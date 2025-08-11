package aggregator

import (
	"time"

	"github.com/rs/zerolog/log"
	"go-candles/pkg/models"
)

type Builder struct {
	pair     string
	interval time.Duration
	trades   chan models.Trade
	out      chan models.Candle
}

func NewBuilder(pair string, interval time.Duration, trades chan models.Trade, out chan models.Candle) *Builder {
	log.Debug().Str("pair", pair).Dur("interval", interval).Msg("Creating new Builder")
	b := &Builder{
		pair:     pair,
		interval: interval,
		trades:   trades,
		out:      out,
	}
	go func() {
		log.Debug().Str("pair", pair).Msg("Starting buildLoop goroutine")
		b.buildLoop()
	}()
	return b
}

func (b *Builder) buildLoop() {
	log.Debug().Str("pair", b.pair).Dur("interval", b.interval).Msg("Entered buildLoop")
	defer log.Debug().Str("pair", b.pair).Msg("Exiting buildLoop")

	var candle *models.Candle
	start := time.Now().UTC().Truncate(b.interval)
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	for {
		select {
		case trade, ok := <-b.trades:
			if !ok {
				log.Debug().Str("pair", b.pair).Msg("Trade channel closed")
				if candle != nil {
					log.Info().Str("pair", candle.Pair).Float64("open", candle.Open).Float64("close", candle.Close).Float64("high", candle.High).Float64("low", candle.Low).Float64("volume", candle.Volume).Time("ts", candle.Timestamp).Msg("Emitted final candle")
					select {
					case b.out <- *candle:
						log.Debug().Str("pair", candle.Pair).Msg("Sent final candle to output channel")
					default:
						log.Warn().Str("pair", candle.Pair).Int("candleCh_len", len(b.out)).Int("candleCh_cap", cap(b.out)).Msg("Dropped final candle due to full output channel")
					}
				}
				return
			}
			tradeTs := trade.Timestamp.UTC()
			log.Debug().Str("pair", trade.Pair).Float64("price", trade.Price).Float64("quantity", trade.Quantity).Time("ts", tradeTs).Time("start", start).Time("next_interval", start.Add(b.interval)).Int("tradeCh_len", len(b.trades)).Int("tradeCh_cap", cap(b.trades)).Msg("Aggregator received trade")

			if candle == nil || tradeTs.After(start.Add(b.interval)) {
				if candle != nil {
					log.Info().Str("pair", candle.Pair).Float64("open", candle.Open).Float64("close", candle.Close).Float64("high", candle.High).Float64("low", candle.Low).Float64("volume", candle.Volume).Time("ts", candle.Timestamp).Msg("Emitted candle")
					select {
					case b.out <- *candle:
						log.Debug().Str("pair", candle.Pair).Msg("Sent candle to output channel")
					default:
						log.Warn().Str("pair", candle.Pair).Int("candleCh_len", len(b.out)).Int("candleCh_cap", cap(b.out)).Msg("Dropped candle due to full output channel")
					}
				}
				start = tradeTs.Truncate(b.interval)
				candle = &models.Candle{
					Pair:      trade.Pair,
					Timestamp: start,
					Open:      trade.Price,
					High:      trade.Price,
					Low:       trade.Price,
					Close:     trade.Price,
					Volume:    trade.Quantity,
				}
			} else {
				candle.Close = trade.Price
				if trade.Price > candle.High {
					candle.High = trade.Price
				}
				if trade.Price < candle.Low {
					candle.Low = trade.Price
				}
				candle.Volume += trade.Quantity
			}
		case <-ticker.C:
			if candle != nil {
				log.Info().Str("pair", candle.Pair).Float64("open", candle.Open).Float64("close", candle.Close).Float64("high", candle.High).Float64("low", candle.Low).Float64("volume", candle.Volume).Time("ts", candle.Timestamp).Msg("Emitted candle on ticker")
				select {
				case b.out <- *candle:
					log.Debug().Str("pair", candle.Pair).Msg("Sent candle to output channel")
				default:
					log.Warn().Str("pair", candle.Pair).Int("candleCh_len", len(b.out)).Int("candleCh_cap", cap(b.out)).Msg("Dropped candle due to full output channel")
				}
				start = time.Now().UTC().Truncate(b.interval)
				candle = nil
			}
		}
	}
}
