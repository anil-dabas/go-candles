package aggregator

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"go-candles/pkg/models"
)

type Builder struct {
	pair         string
	intervalMs   int64
	tradeCh      chan models.Trade
	candleCh     chan models.Candle
	mu           sync.Mutex
	currentStart int64
	open         float64
	high         float64
	low          float64
	close        float64
	volume       float64
	hasData      bool
}

func NewBuilder(pair string, interval time.Duration, tradeCh chan models.Trade, candleCh chan models.Candle) *Builder {
	return &Builder{
		pair:         pair,
		intervalMs:   int64(interval / time.Millisecond),
		tradeCh:      tradeCh,
		candleCh:     candleCh,
		low:          1e9,
		currentStart: time.Now().UnixMilli(),
	}
}

func (b *Builder) Run() {
	go b.run()
}

func (b *Builder) run() {
	for trade := range b.tradeCh {
		log.Debug().Str("pair", trade.Pair).Float64("price", trade.Price).Float64("quantity", trade.Quantity).Time("ts", trade.Timestamp).Msg("Aggregator received trade")

		tsMs := trade.Timestamp.UnixMilli()
		bucketStart := (tsMs / b.intervalMs) * b.intervalMs

		b.mu.Lock()
		if bucketStart != b.currentStart && b.hasData {
			b.candleCh <- models.Candle{
				Pair:      b.pair,
				Timestamp: time.UnixMilli(b.currentStart),
				Open:      b.open,
				High:      b.high,
				Low:       b.low,
				Close:     b.close,
				Volume:    b.volume,
			}
			log.Info().Str("pair", b.pair).Float64("open", b.open).Float64("high", b.high).Float64("low", b.low).Float64("close", b.close).Float64("volume", b.volume).Time("ts", time.UnixMilli(b.currentStart)).Msg("Emitted candle")
			b.reset(bucketStart, trade)
		} else {
			b.update(trade)
		}
		b.mu.Unlock()
	}
}

func (b *Builder) update(trade models.Trade) {
	b.hasData = true
	if b.open == 0 {
		b.open = trade.Price
	}
	b.high = max(b.high, trade.Price)
	b.low = min(b.low, trade.Price)
	b.close = trade.Price
	b.volume += trade.Quantity
}

func (b *Builder) reset(bucketStart int64, trade models.Trade) {
	b.currentStart = bucketStart
	b.open = trade.Price
	b.high = trade.Price
	b.low = trade.Price
	b.close = trade.Price
	b.volume = trade.Quantity
	b.hasData = true
}

func (b *Builder) AddTrade(trade models.Trade) {
	b.tradeCh <- trade
}

func (b *Builder) Close() {
	close(b.tradeCh)
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
