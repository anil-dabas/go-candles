package aggregator

import (
	"time"

	"go-candles/internal/common"
	"go-candles/internal/util"
	"go-candles/pkg/models"
)

type Builder struct {
	pair     string
	interval time.Duration
	trades   chan models.Trade
	out      chan models.Candle
}

func NewBuilder(pair string, interval time.Duration, trades chan models.Trade, out chan models.Candle) *Builder {
	b := &Builder{
		pair:     pair,
		interval: interval,
		trades:   trades,
		out:      out,
	}
	go b.buildLoop()
	return b
}

func (b *Builder) buildLoop() {
	logger := util.NewLogger()
	var candle *models.Candle
	start := time.Now().UTC().Truncate(b.interval)
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	for {
		select {
		case trade, ok := <-b.trades:
			if !ok {
				if candle != nil {
					select {
					case b.out <- *candle:
					default:
						logger.Warn(common.ErrCodeChannelFull, common.ErrMsgChannelFull, "Dropped final candle due to full output channel", "pair", candle.Pair)
					}
				}
				return
			}

			tradeTs := trade.Timestamp.UTC()
			if candle == nil || tradeTs.After(start.Add(b.interval)) {
				if candle != nil {
					select {
					case b.out <- *candle:
					default:
						logger.Warn(common.ErrCodeChannelFull, common.ErrMsgChannelFull, "Dropped candle due to full output channel", "pair", candle.Pair)
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
				select {
				case b.out <- *candle:
				default:
					logger.Warn(common.ErrCodeChannelFull, common.ErrMsgChannelFull, "Dropped candle due to full output channel", "pair", candle.Pair)
				}
				start = time.Now().UTC().Truncate(b.interval)
				candle = nil
			}
		}
	}
}
