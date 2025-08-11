package service

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"go-candles/internal/aggregator"
	"go-candles/internal/common"
	"go-candles/internal/config"
	"go-candles/internal/exchanges"
	"go-candles/internal/proto"
	"go-candles/pkg/models"
)

type listenerEntry struct {
	mu    sync.Mutex
	chans []chan *proto.CandleResponse
}

type Service struct {
	proto.UnimplementedCandleServiceServer
	exchanges   []exchanges.Exchange
	config      *config.Config
	pairs       []string
	interval    time.Duration
	tradeChans  map[string]chan models.Trade
	candleChans map[string]chan models.Candle
	listeners   map[string]*listenerEntry
	listenersMu sync.Mutex
	builders    map[string]*aggregator.Builder
}

func NewService(cfg *config.Config) *Service {
	s := &Service{
		config:      cfg,
		pairs:       cfg.Pairs,
		interval:    cfg.GetCandleInterval(),
		tradeChans:  make(map[string]chan models.Trade),
		candleChans: make(map[string]chan models.Candle),
		listeners:   make(map[string]*listenerEntry),
		builders:    make(map[string]*aggregator.Builder),
	}

	// Initialize exchanges
	for exchangeName, exchangeCfg := range cfg.Exchanges {
		switch exchangeName {
		case common.ExchangeBinance:
			s.exchanges = append(s.exchanges, exchanges.NewBinance(
				exchangeCfg.WebsocketURL,
				time.Duration(exchangeCfg.ReconnectIntervalSec)*time.Second,
				time.Duration(exchangeCfg.PingIntervalSec)*time.Second,
				cfg.MaxReconnectAttempts,
			))
		case common.ExchangeCoinbase:
			s.exchanges = append(s.exchanges, exchanges.NewCoinbase(
				exchangeCfg.WebsocketURL,
				time.Duration(exchangeCfg.ReconnectIntervalSec)*time.Second,
				time.Duration(exchangeCfg.PingIntervalSec)*time.Second,
				cfg.MaxReconnectAttempts,
			))
		case common.ExchangeOKX:
			s.exchanges = append(s.exchanges, exchanges.NewOKX(
				exchangeCfg.WebsocketURL,
				time.Duration(exchangeCfg.ReconnectIntervalSec)*time.Second,
				time.Duration(exchangeCfg.PingIntervalSec)*time.Second,
				cfg.MaxReconnectAttempts,
			))
		}
	}

	s.init()
	return s
}

func (s *Service) init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	bufferSize := s.config.GetChannelBufferSize()

	for _, pair := range s.pairs {
		tradeCh := make(chan models.Trade, bufferSize)
		candleCh := make(chan models.Candle, bufferSize)
		s.tradeChans[pair] = tradeCh
		s.candleChans[pair] = candleCh

		s.listenersMu.Lock()
		s.listeners[pair] = &listenerEntry{
			mu:    sync.Mutex{},
			chans: make([]chan *proto.CandleResponse, 0),
		}
		s.listenersMu.Unlock()

		s.builders[pair] = aggregator.NewBuilder(pair, s.interval, tradeCh, candleCh)

		go s.emitLoop(pair, candleCh)
	}

	for _, ex := range s.exchanges {
		ex := ex
		go func() {
			if err := ex.Connect(); err != nil {
				log.Error().
					Err(err).
					Str("error_code", common.ErrCodeExchangeConnectFailed.String()).
					Str("error_message", common.ErrMsgExchangeConnectFailed.String()).
					Str("exchange", fmt.Sprintf("%T", ex)).
					Msg("Exchange connect failed")
				return
			}

			for _, pair := range s.pairs {
				if err := ex.Subscribe(pair, s.tradeChans[pair]); err != nil {
					log.Error().
						Err(err).
						Str("error_code", common.ErrCodeExchangeSubscribeFailed.String()).
						Str("error_message", common.ErrMsgExchangeSubscribeFailed.String()).
						Str("exchange", fmt.Sprintf("%T", ex)).
						Str("pair", pair).
						Msg("Subscribe failed")
				}
			}
		}()
	}
}

func (s *Service) emitLoop(pair string, candleCh chan models.Candle) {
	listener, ok := s.listeners[pair]
	if !ok {
		log.Error().
			Str("error_code", common.ErrCodeInvalidPair.String()).
			Str("error_message", common.ErrMsgInvalidPair.String()).
			Str("pair", pair).
			Msg("No listener found for pair")
		return
	}

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

		listener.mu.Lock()
		for _, ch := range listener.chans {
			select {
			case ch <- resp:
				log.Debug().Str("pair", pair).Msg("Sent candle to subscriber")
			default:
				log.Warn().
					Str("error_code", common.ErrCodeChannelFull.String()).
					Str("error_message", common.ErrMsgChannelFull.String()).
					Str("pair", pair).
					Msg("Dropped candle due to full subscriber channel")
			}
		}
		listener.mu.Unlock()
	}
}

func (s *Service) Subscribe(req *proto.SubscribeRequest, stream proto.CandleService_SubscribeServer) error {
	subChans := make(map[string]chan *proto.CandleResponse)

	for _, pair := range req.Pairs {
		s.listenersMu.Lock()
		listener, ok := s.listeners[pair]
		s.listenersMu.Unlock()

		if !ok {
			log.Warn().
				Str("error_code", common.ErrCodeInvalidPair.String()).
				Str("error_message", common.ErrMsgInvalidPair.String()).
				Str("pair", pair).
				Msg("No listener found for pair, skipping subscription")
			continue
		}

		ch := make(chan *proto.CandleResponse, common.ListenerChannelSize)
		subChans[pair] = ch

		listener.mu.Lock()
		listener.chans = append(listener.chans, ch)
		listener.mu.Unlock()

		go func(p string, ch chan *proto.CandleResponse) {
			defer func() {
				s.listenersMu.Lock()
				listener, ok := s.listeners[p]
				s.listenersMu.Unlock()

				if !ok {
					log.Error().
						Str("error_code", common.ErrCodeInvalidPair.String()).
						Str("error_message", common.ErrMsgInvalidPair.String()).
						Str("pair", p).
						Msg("No listener found for pair during cleanup")
					return
				}

				listener.mu.Lock()
				for i, c := range listener.chans {
					if c == ch {
						listener.chans = append(listener.chans[:i], listener.chans[i+1:]...)
						break
					}
				}
				listener.mu.Unlock()
				close(ch)
			}()

			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						return
					}
					if err := stream.Send(msg); err != nil {
						log.Error().
							Err(err).
							Str("error_code", common.ErrCodeStreamClosed.String()).
							Str("error_message", common.ErrMsgStreamClosed.String()).
							Str("pair", p).
							Msg("Failed to send candle to stream")
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
