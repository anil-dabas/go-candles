package service

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"go-candles/internal/aggregator"
	"go-candles/internal/exchanges"
	"go-candles/internal/proto"
	"go-candles/pkg/models"
)

type Service struct {
	proto.UnimplementedCandleServiceServer
	exchanges   []exchanges.Exchange
	pairs       []string
	interval    time.Duration
	tradeChans  map[string]chan models.Trade
	candleChans map[string]chan models.Candle
	listeners   map[string]*struct {
		mu    sync.Mutex
		chans []chan *proto.CandleResponse
	}
	listenersMu sync.Mutex // Global mutex for listeners map
	builders    map[string]*aggregator.Builder
}

func NewService(interval time.Duration) *Service {
	s := &Service{
		pairs:       []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"},
		interval:    interval,
		tradeChans:  make(map[string]chan models.Trade),
		candleChans: make(map[string]chan models.Candle),
		listeners: make(map[string]*struct {
			mu    sync.Mutex
			chans []chan *proto.CandleResponse
		}),
		builders: make(map[string]*aggregator.Builder),
	}
	s.exchanges = []exchanges.Exchange{
		exchanges.NewBinance(),
		exchanges.NewOKX(),
		exchanges.NewCoinbase(),
	}
	s.init()
	return s
}

func (s *Service) init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	for _, pair := range s.pairs {
		tradeCh := make(chan models.Trade, 10000)
		candleCh := make(chan models.Candle, 10000)
		s.tradeChans[pair] = tradeCh
		s.candleChans[pair] = candleCh
		s.listenersMu.Lock()
		s.listeners[pair] = &struct {
			mu    sync.Mutex
			chans []chan *proto.CandleResponse
		}{
			mu:    sync.Mutex{},
			chans: make([]chan *proto.CandleResponse, 0),
		}
		s.listenersMu.Unlock()

		s.builders[pair] = aggregator.NewBuilder(pair, s.interval, tradeCh, candleCh)
		log.Debug().Str("pair", pair).Int("tradeCh_cap", cap(tradeCh)).Int("candleCh_cap", cap(candleCh)).Msg("Initialized channels for pair")

		go func(p string, ch chan models.Candle) {
			log.Debug().Str("pair", p).Msg("Starting emitLoop goroutine")
			s.emitLoop(p, ch)
		}(pair, candleCh)
	}

	for _, ex := range s.exchanges {
		ex := ex
		go func() {
			log.Debug().Str("exchange", fmt.Sprintf("%T", ex)).Msg("Attempting to connect to exchange")
			if err := ex.Connect(); err != nil {
				log.Error().Err(err).Str("exchange", fmt.Sprintf("%T", ex)).Msg("Exchange connect failed")
				return
			}
			for _, pair := range s.pairs {
				log.Debug().Str("exchange", fmt.Sprintf("%T", ex)).Str("pair", pair).Msg("Attempting to subscribe")
				if err := ex.Subscribe(pair, s.tradeChans[pair]); err != nil {
					log.Error().Err(err).Str("exchange", fmt.Sprintf("%T", ex)).Str("pair", pair).Msg("Subscribe failed")
				}
			}
		}()
	}
}

func (s *Service) emitLoop(pair string, candleCh chan models.Candle) {
	log.Debug().Str("pair", pair).Msg("Entered emitLoop")
	defer log.Debug().Str("pair", pair).Msg("Exiting emitLoop")

	listener, ok := s.listeners[pair]
	if !ok {
		log.Error().Str("pair", pair).Msg("No listener found for pair")
		return
	}

	for candle := range candleCh {
		log.Debug().
			Str("pair", candle.Pair).
			Float64("open", candle.Open).
			Float64("close", candle.Close).
			Float64("volume", candle.Volume).
			Time("ts", candle.Timestamp).
			Int("candleCh_len", len(candleCh)).
			Int("candleCh_cap", cap(candleCh)).
			Msg("Processing candle in emitLoop")
		resp := &proto.CandleResponse{
			Pair:      candle.Pair,
			Timestamp: candle.Timestamp.UnixMilli(),
			Open:      candle.Open,
			High:      candle.High,
			Low:       candle.Low,
			Close:     candle.Close,
			Volume:    candle.Volume,
		}
		listener.mu.Lock()
		log.Debug().Str("pair", pair).Int("subscriber_count", len(listener.chans)).Msg("Checking subscribers")
		for _, ch := range listener.chans {
			select {
			case ch <- resp:
				log.Debug().Str("pair", pair).Msg("Sent candle to subscriber")
			default:
				log.Warn().Str("pair", pair).Int("subscriberCh_len", len(ch)).Int("subscriberCh_cap", cap(ch)).Msg("Dropped candle due to full subscriber channel")
			}
		}
		listener.mu.Unlock()
	}
}

func (s *Service) Subscribe(req *proto.SubscribeRequest, stream proto.CandleService_SubscribeServer) error {
	log.Debug().Strs("pairs", req.Pairs).Msg("Subscribe method called")
	subChans := make(map[string]chan *proto.CandleResponse)

	for _, pair := range req.Pairs {
		log.Debug().Str("pair", pair).Msg("Processing subscription request")
		s.listenersMu.Lock()
		listener, ok := s.listeners[pair]
		if !ok {
			log.Warn().Str("pair", pair).Msg("No listener found for pair, skipping subscription")
			s.listenersMu.Unlock()
			continue
		}
		s.listenersMu.Unlock()

		ch := make(chan *proto.CandleResponse, 10000)
		subChans[pair] = ch

		listener.mu.Lock()
		log.Debug().Str("pair", pair).Int("listener_count", len(listener.chans)).Msg("Before adding subscriber")
		listener.chans = append(listener.chans, ch)
		log.Debug().Str("pair", pair).Int("listener_count", len(listener.chans)).Msg("Added subscriber channel")
		listener.mu.Unlock()
		log.Debug().Str("pair", pair).Int("listener_count", len(listener.chans)).Msg("After adding subscriber")

		go func(p string, ch chan *proto.CandleResponse) {
			defer func() {
				s.listenersMu.Lock()
				listener, ok := s.listeners[p]
				s.listenersMu.Unlock()
				if !ok {
					log.Error().Str("pair", p).Msg("No listener found for pair during cleanup")
					return
				}
				listener.mu.Lock()
				log.Debug().Str("pair", p).Int("listener_count", len(listener.chans)).Msg("Before removing subscriber channel")
				for i, c := range listener.chans {
					if c == ch {
						listener.chans = append(listener.chans[:i], listener.chans[i+1:]...)
						log.Debug().Str("pair", p).Int("listener_count", len(listener.chans)).Msg("Removed subscriber channel")
						break
					}
				}
				listener.mu.Unlock()
				close(ch)
				log.Debug().Str("pair", p).Msg("Closed subscriber channel")
			}()

			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						log.Debug().Str("pair", p).Msg("Subscriber channel closed")
						return
					}
					if err := stream.Send(msg); err != nil {
						log.Error().Err(err).Str("pair", p).Msg("Failed to send candle to stream")
						return
					}
					log.Debug().Str("pair", p).Msg("Sent candle to gRPC stream")
				case <-stream.Context().Done():
					log.Debug().Str("pair", p).Err(stream.Context().Err()).Msg("Stream context done")
					return
				}
			}
		}(pair, ch)
	}

	log.Debug().Strs("pairs", req.Pairs).Msg("Waiting for stream context to close")
	<-stream.Context().Done()
	log.Debug().Strs("pairs", req.Pairs).Msg("Exiting Subscribe")
	return nil
}

func (s *Service) Shutdown() {
	log.Info().Msg("Shutting down service")
	for _, ex := range s.exchanges {
		ex.Close()
	}
	for pair, ch := range s.tradeChans {
		log.Debug().Str("pair", pair).Msg("Closing trade channel")
		close(ch)
	}
	for pair, ch := range s.candleChans {
		log.Debug().Str("pair", pair).Msg("Closing candle channel")
		close(ch)
	}
}
