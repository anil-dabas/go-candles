package aggregator

import (
	"encoding/json"
	"fmt"
	"go-candles/internal/util"
	"go-candles/pkg/models"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

type Binance struct {
	conn      *websocket.Conn
	subs      map[string]chan<- models.Trade
	mu        sync.Mutex
	reconnect bool
}

func NewBinance() *Binance {
	return &Binance{
		subs:      make(map[string]chan<- models.Trade),
		reconnect: true,
	}
}

func (b *Binance) Connect() error {
	url := "wss://stream.binance.com:9443/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	b.conn = conn
	go b.readLoop()
	go b.pingLoop()
	return nil
}

func (b *Binance) Subscribe(pair string, tradeChan chan<- models.Trade) error {
	b.mu.Lock()
	b.subs[pair] = tradeChan
	b.mu.Unlock()

	if b.conn == nil {
		if err := b.Connect(); err != nil {
			return err
		}
	}

	msg := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": []string{fmt.Sprintf("%s@trade", pairToBinance(pair))},
		"id":     time.Now().UnixNano(),
	}
	err := b.conn.WriteJSON(msg)
	if err == nil {
		log.Info().Str("pair", pair).Msg("Subscribed to Binance trade feed")
	}
	return err
}

func (b *Binance) pingLoop() {
	ticker := time.NewTicker(3 * time.Minute) // Ping every 3 minutes
	defer ticker.Stop()

	for b.reconnect {
		select {
		case <-ticker.C:
			if b.conn != nil {
				err := b.conn.WriteJSON(map[string]interface{}{
					"method": "PING",
					"id":     time.Now().UnixNano(),
				})
				if err != nil {
					log.Error().Err(err).Msg("Binance ping error")
				} else {
					log.Debug().Msg("Sent Binance ping")
				}
			}
		}
	}
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
			b.conn.Close()
			b.conn = nil
			time.Sleep(5 * time.Second)
			b.Connect()
			b.resubscribe()
			continue
		}

		// Handle ping response
		var pingResp struct {
			Result interface{} `json:"result"`
			Id     int64       `json:"id"`
		}
		if json.Unmarshal(data, &pingResp) == nil && pingResp.Result == nil {
			log.Debug().Msg("Received Binance ping response")
			continue
		}

		// Handle trade data
		var tradeResp struct {
			E string `json:"e"` // Event type
			S string `json:"s"` // Symbol
			P string `json:"p"` // Price
			Q string `json:"q"` // Quantity
			T int64  `json:"T"` // Trade time
		}
		if err := json.Unmarshal(data, &tradeResp); err != nil || tradeResp.E != "trade" {
			log.Debug().Str("data", string(data)).Msg("Ignoring non-trade message")
			continue
		}

		price := util.ParseFloat(tradeResp.P)
		quantity := util.ParseFloat(tradeResp.Q)
		ts := time.UnixMilli(tradeResp.T)

		b.mu.Lock()
		ch, ok := b.subs[pairFromBinance(tradeResp.S)]
		b.mu.Unlock()
		if ok {
			log.Debug().Str("pair", pairFromBinance(tradeResp.S)).Float64("price", price).Float64("quantity", quantity).Msg("Received Binance trade")
			ch <- models.Trade{Timestamp: ts, Price: price, Quantity: quantity}
		}
	}
}

func (b *Binance) resubscribe() {
	b.mu.Lock()
	for pair, ch := range b.subs {
		b.Subscribe(pair, ch)
	}
	b.mu.Unlock()
}

func (b *Binance) Close() error {
	b.mu.Lock()
	b.reconnect = false
	var err error
	if b.conn != nil {
		err = b.conn.Close()
		b.conn = nil
	}
	b.mu.Unlock()
	return err
}

func pairToBinance(pair string) string {
	return strings.ToLower(strings.ReplaceAll(pair, "-", ""))
}

func pairFromBinance(symbol string) string {
	return symbol // Binance uses same format (e.g., BTCUSDT)
}
