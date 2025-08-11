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

type Coinbase struct {
	conn      *websocket.Conn
	mu        sync.Mutex
	subs      map[string]chan models.Trade
	reconnect bool
}

func NewCoinbase() *Coinbase {
	return &Coinbase{
		subs:      make(map[string]chan models.Trade),
		reconnect: true,
	}
}

func (c *Coinbase) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, _, err := websocket.DefaultDialer.Dial("wss://advanced-trade-ws.coinbase.com", nil)
	if err != nil {
		return fmt.Errorf("coinbase connect: %w", err)
	}
	c.conn = conn
	go c.readLoop()
	go c.pingLoop()
	return nil
}

func (c *Coinbase) Subscribe(pair string, ch chan models.Trade) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.subs[pair] = ch
	if c.conn == nil {
		if err := c.Connect(); err != nil {
			return fmt.Errorf("coinbase subscribe %s: %w", pair, err)
		}
	}

	msg := map[string]interface{}{
		"type":        "subscribe",
		"product_ids": []string{util.PairToCoinbase(pair)},
		"channel":     "market_trades",
	}
	if err := c.conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("coinbase subscribe %s: %w", pair, err)
	}
	log.Info().Str("pair", pair).Interface("msg", msg).Msg("Sent Coinbase subscription request")
	return nil
}

func (c *Coinbase) readLoop() {
	for c.reconnect {
		if c.conn == nil {
			time.Sleep(5 * time.Second)
			continue
		}
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			log.Error().Err(err).Msg("Coinbase read error, reconnecting")
			c.mu.Lock()
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
			}
			c.mu.Unlock()
			time.Sleep(5 * time.Second)
			c.Connect()
			c.resubscribe()
			continue
		}

		log.Debug().Str("raw", string(data)).Msg("Received Coinbase message")

		// Check for error response
		var errorResp struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(data, &errorResp); err == nil && errorResp.Type == "error" {
			log.Error().Str("message", errorResp.Message).Msg("Coinbase subscription error")
			continue
		}

		// Check for ping response
		var pingResp struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &pingResp); err == nil && pingResp.Type == "heartbeat" {
			log.Debug().Msg("Received Coinbase heartbeat")
			continue
		}

		// Check for subscription response
		var subResp struct {
			Type     string   `json:"type"`
			Channels []string `json:"channels"`
		}
		if err := json.Unmarshal(data, &subResp); err == nil && subResp.Type == "subscriptions" {
			log.Debug().Interface("channels", subResp.Channels).Msg("Received Coinbase subscription confirmation")
			continue
		}

		// Check for trade message (match)
		var tradeResp struct {
			Type      string `json:"type"`
			ProductID string `json:"product_id"`
			Price     string `json:"price"`
			Size      string `json:"size"`
			Time      string `json:"time"`
		}
		if err := json.Unmarshal(data, &tradeResp); err == nil && tradeResp.Type == "match" {
			price := util.ParseFloat(tradeResp.Price)
			quantity := util.ParseFloat(tradeResp.Size)
			ts, err := time.Parse(time.RFC3339Nano, tradeResp.Time)
			if err != nil {
				log.Error().Err(err).Str("time", tradeResp.Time).Msg("Failed to parse Coinbase trade time")
				continue
			}
			pair := util.PairFromCoinbase(tradeResp.ProductID)

			c.mu.Lock()
			ch, ok := c.subs[pair]
			c.mu.Unlock()
			if ok {
				log.Debug().Str("pair", pair).Float64("price", price).Float64("quantity", quantity).Time("ts", ts).Msg("Received Coinbase trade")
				ch <- models.Trade{Timestamp: ts, Price: price, Quantity: quantity, Pair: pair}
			} else {
				log.Warn().Str("pair", pair).Msg("No subscriber for Coinbase trade")
			}
			continue
		}

		// Check for snapshot message
		var snapshotResp struct {
			Channel string `json:"channel"`
			Events  []struct {
				Type   string `json:"type"`
				Trades []struct {
					ProductID string `json:"product_id"`
					Price     string `json:"price"`
					Size      string `json:"size"`
					Time      string `json:"time"`
				} `json:"trades"`
			} `json:"events"`
		}
		if err := json.Unmarshal(data, &snapshotResp); err == nil && snapshotResp.Channel == "market_trades" {
			for _, event := range snapshotResp.Events {
				if event.Type == "snapshot" {
					for _, trade := range event.Trades {
						price := util.ParseFloat(trade.Price)
						quantity := util.ParseFloat(trade.Size)
						ts, err := time.Parse(time.RFC3339Nano, trade.Time)
						if err != nil {
							log.Error().Err(err).Str("time", trade.Time).Msg("Failed to parse Coinbase snapshot trade time")
							continue
						}
						pair := util.PairFromCoinbase(trade.ProductID)

						c.mu.Lock()
						ch, ok := c.subs[pair]
						c.mu.Unlock()
						if ok {
							log.Debug().Str("pair", pair).Float64("price", price).Float64("quantity", quantity).Time("ts", ts).Msg("Received Coinbase snapshot trade")
							ch <- models.Trade{Timestamp: ts, Price: price, Quantity: quantity, Pair: pair}
						} else {
							log.Warn().Str("pair", pair).Msg("No subscriber for Coinbase snapshot trade")
						}
					}
				}
			}
			continue
		}

		log.Warn().Str("data", string(data)).Msg("Received unhandled Coinbase message")
	}
}

func (c *Coinbase) pingLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for c.reconnect {
		select {
		case <-ticker.C:
			if c.conn != nil {
				c.mu.Lock()
				err := c.conn.WriteJSON(map[string]interface{}{
					"type": "heartbeat",
					"on":   true,
				})
				c.mu.Unlock()
				if err != nil {
					log.Error().Err(err).Msg("Coinbase ping error")
				} else {
					log.Debug().Msg("Sent Coinbase ping")
				}
			}
		}
	}
}

func (c *Coinbase) resubscribe() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for pair := range c.subs {
		msg := map[string]interface{}{
			"type":        "subscribe",
			"product_ids": []string{util.PairToCoinbase(pair)},
			"channel":     "market_trades",
		}
		if c.conn != nil {
			if err := c.conn.WriteJSON(msg); err != nil {
				log.Error().Err(err).Str("pair", pair).Msg("Coinbase resubscribe failed")
			} else {
				log.Info().Str("pair", pair).Msg("Resubscribed to Coinbase trade feed")
			}
		}
	}
}

func (c *Coinbase) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reconnect = false
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}
