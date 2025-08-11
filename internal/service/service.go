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
	listeners   map[string]struct {
		mu    sync.Mutex
		chans []chan *proto.CandleResponse
	}
	builders map[string]*aggregator.Builder
}

func NewService(interval time.Duration) *Service {
	s := &Service{
		pairs:       []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"},
		interval:    interval,
		tradeChans:  make(map[string]chan models.Trade),
		candleChans: make(map[string]chan models.Candle),
		listeners: make(map[string]struct {
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
		tradeCh := make(chan models.Trade, 1000)
		candleCh := make(chan models.Candle, 100)
		s.tradeChans[pair] = tradeCh
		s.candleChans[pair] = candleCh
		s.listeners[pair] = struct {
			mu    sync.Mutex
			chans []chan *proto.CandleResponse
		}{}

		s.builders[pair] = aggregator.NewBuilder(pair, s.interval, tradeCh, candleCh)

		go s.emitLoop(pair, candleCh)
	}

	for _, ex := range s.exchanges {
		ex := ex
		go func() {
			if err := ex.Connect(); err != nil {
				log.Error().Err(err).Str("exchange", fmt.Sprintf("%T", ex)).Msg("Exchange connect failed")
			}
			for _, pair := range s.pairs {
				if err := ex.Subscribe(pair, s.tradeChans[pair]); err != nil {
					log.Error().Err(err).Str("exchange", fmt.Sprintf("%T", ex)).Str("pair", pair).Msg("Subscribe failed")
				}
			}
		}()
	}
}

func (s *Service) emitLoop(pair string, candleCh chan models.Candle) {
	for candle := range candleCh {
		resp := &proto.CandleResponse{
			Pair:      candle.Pair,
			Timestamp: candle.Timestamp.UnixMilli(),
			Open:      candle.Open,
			High:      candle.High,
			Low:       candle.Low,
			Close:     candle.Close,
			Volume:    candle.Volume,
		}
		l := s.listeners[pair]
		l.mu.Lock()
		for _, ch := range l.chans {
			select {
			case ch <- resp:
				log.Debug().Str("pair", pair).Msg("Sent candle to subscriber")
			default:
				log.Warn().Str("pair", pair).Msg("Dropped candle due to full subscriber channel")
			}
		}
		l.mu.Unlock()
	}
}

func (s *Service) Subscribe(req *proto.SubscribeRequest, stream proto.CandleService_SubscribeServer) error {
	subChans := make(map[string]chan *proto.CandleResponse)

	for _, pair := range req.Pairs {
		if _, ok := s.tradeChans[pair]; !ok {
			log.Warn().Str("pair", pair).Msg("Invalid pair, skipping subscription")
			continue
		}
		ch := make(chan *proto.CandleResponse, 100)
		subChans[pair] = ch

		l := s.listeners[pair]
		l.mu.Lock()
		l.chans = append(l.chans, ch)
		l.mu.Unlock()

		go func(p string, ch chan *proto.CandleResponse) {
			defer func() {
				l.mu.Lock()
				for i, c := range l.chans {
					if c == ch {
						l.chans = append(l.chans[:i], l.chans[i+1:]...)
						break
					}
				}
				l.mu.Unlock()
				close(ch)
			}()

			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						return
					}
					if err := stream.Send(msg); err != nil {
						log.Error().Err(err).Str("pair", p).Msg("Failed to send candle to stream")
						return
					}
				case <-stream.Context().Done():
					return
				}
			}
		}(pair, ch)
	}

	<-stream.Context().Done()
	return nil
}

func (s *Service) Shutdown() {
	for _, ex := range s.exchanges {
		ex.Close()
	}
	for _, ch := range s.tradeChans {
		close(ch)
	}
	for _, ch := range s.candleChans {
		close(ch)
	}
}
