package exchanges

import (
	"encoding/json"
	"fmt"
	"go-candles/internal/util"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"go-candles/pkg/models"
)

type Binance struct {
	conn      *websocket.Conn
	mu        sync.Mutex
	subs      map[string]chan models.Trade
	reconnect bool
}

func NewBinance() *Binance {
	return &Binance{
		subs:      make(map[string]chan models.Trade),
		reconnect: true,
	}
}

func (b *Binance) Connect() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	conn, _, err := websocket.DefaultDialer.Dial("wss://stream.binance.com:9443/ws", nil)
	if err != nil {
		return fmt.Errorf("binance connect: %w", err)
	}
	b.conn = conn
	go b.readLoop()
	go b.pingLoop()
	return nil
}

func (b *Binance) Subscribe(pair string, ch chan models.Trade) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.subs[pair] = ch
	if b.conn == nil {
		if err := b.Connect(); err != nil {
			return fmt.Errorf("binance subscribe %s: %w", pair, err)
		}
	}

	params := []string{fmt.Sprintf("%s@trade", util.PairToBinance(pair))}
	sub := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": params,
		"id":     time.Now().UnixNano(),
	}
	if err := b.conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("binance subscribe %s: %w", pair, err)
	}
	log.Info().Str("pair", pair).Msg("Subscribed to Binance trade feed")
	return nil
}

func (b *Binance) readLoop() {
	for b.reconnect {
		if b.conn == nil {
			time.Sleep(5 * time.Second)
			continue
		}
		_, data, err := b.conn.ReadMessage()
		if err != nil {
			log.Error().Err(err).Msg("Binance read error, reconnecting")
			b.mu.Lock()
			if b.conn != nil {
				b.conn.Close()
				b.conn = nil
			}
			b.mu.Unlock()
			time.Sleep(5 * time.Second)
			b.Connect()
			b.resubscribe()
			continue
		}

		log.Debug().Str("raw", string(data)).Msg("Received Binance message")

		// Check for ping response
		var pingResp struct {
			Result interface{} `json:"result"`
			Id     int64       `json:"id"`
		}
		if err := json.Unmarshal(data, &pingResp); err == nil && pingResp.Result == nil && pingResp.Id != 0 {
			log.Debug().Int64("id", pingResp.Id).Msg("Received Binance ping response")
			continue
		}

		// Check for subscription response
		var subResp struct {
			Result []string `json:"result"`
			Id     int64    `json:"id"`
		}
		if err := json.Unmarshal(data, &subResp); err == nil && len(subResp.Result) == 0 && subResp.Id != 0 {
			log.Debug().Int64("id", subResp.Id).Msg("Received Binance subscription response")
			continue
		}

		// Check for trade message
		var tradeResp struct {
			E string `json:"e"`
			S string `json:"s"`
			P string `json:"p"`
			Q string `json:"q"`
			T int64  `json:"T"`
			M bool   `json:"M"`
		}
		if err := json.Unmarshal(data, &tradeResp); err == nil && tradeResp.E == "trade" && tradeResp.M {
			price := util.ParseFloat(tradeResp.P)
			quantity := util.ParseFloat(tradeResp.Q)
			ts := time.UnixMilli(tradeResp.T)
			pair := util.PairFromBinance(tradeResp.S)

			b.mu.Lock()
			ch, ok := b.subs[pair]
			b.mu.Unlock()
			if ok {
				log.Debug().Str("pair", pair).Float64("price", price).Float64("quantity", quantity).Time("ts", ts).Msg("Received Binance trade")
				ch <- models.Trade{Timestamp: ts, Price: price, Quantity: quantity, Pair: pair}
			} else {
				log.Warn().Str("pair", pair).Msg("No subscriber for Binance trade")
			}
			continue
		}

		log.Warn().Str("data", string(data)).Msg("Received unhandled Binance message")
	}
}

func (b *Binance) pingLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for b.reconnect {
		select {
		case <-ticker.C:
			if b.conn != nil {
				b.mu.Lock()
				err := b.conn.WriteJSON(map[string]interface{}{
					"method": "PING",
					"id":     time.Now().UnixNano(),
				})
				b.mu.Unlock()
				if err != nil {
					log.Error().Err(err).Msg("Binance ping error")
				} else {
					log.Debug().Msg("Sent Binance ping")
				}
			}
		}
	}
}

func (b *Binance) resubscribe() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for pair := range b.subs {
		params := []string{fmt.Sprintf("%s@trade", util.PairToBinance(pair))}
		sub := map[string]interface{}{
			"method": "SUBSCRIBE",
			"params": params,
			"id":     time.Now().UnixNano(),
		}
		if b.conn != nil {
			if err := b.conn.WriteJSON(sub); err != nil {
				log.Error().Err(err).Str("pair", pair).Msg("Binance resubscribe failed")
			} else {
				log.Info().Str("pair", pair).Msg("Resubscribed to Binance trade feed")
			}
		}
	}
}

func (b *Binance) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.reconnect = false
	if b.conn != nil {
		b.conn.Close()
		b.conn = nil
	}
}
