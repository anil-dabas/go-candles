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

func (c *Binance) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, _, err := websocket.DefaultDialer.Dial("wss://stream.binance.com:9443/ws", nil)
	if err != nil {
		return fmt.Errorf("binance connect: %w", err)
	}
	c.conn = conn
	go c.readLoop()
	go c.pingLoop()
	return nil
}

func (c *Binance) Subscribe(pair string, ch chan models.Trade) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.subs[pair] = ch
	log.Debug().Str("pair", pair).Msg("Registered Binance subscriber channel")
	if c.conn == nil {
		if err := c.Connect(); err != nil {
			return fmt.Errorf("binance subscribe %s: %w", pair, err)
		}
	}

	params := []string{fmt.Sprintf("%s@trade", util.PairToBinance(pair))}
	sub := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": params,
		"id":     time.Now().UnixNano(),
	}
	if err := c.conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("binance subscribe %s: %w", pair, err)
	}
	log.Info().Str("pair", pair).Msg("Subscribed to Binance trade feed")
	return nil
}

func (c *Binance) readLoop() {
	for c.reconnect {
		if c.conn == nil {
			time.Sleep(5 * time.Second)
			continue
		}
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			log.Error().Err(err).Msg("Binance read error, reconnecting")
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
			EventType    string `json:"e"`
			EventTime    int64  `json:"E"`
			Symbol       string `json:"s"`
			TradeID      int64  `json:"t"`
			Price        string `json:"p"`
			Quantity     string `json:"q"`
			TradeTime    int64  `json:"T"`
			IsBuyerMaker bool   `json:"m"`
			IsTradeValid bool   `json:"M"`
		}
		if err := json.Unmarshal(data, &tradeResp); err != nil {
			log.Warn().Err(err).Str("data", string(data)).Msg("Failed to unmarshal Binance message")
			// Attempt to parse as a generic map to identify problematic field
			var raw map[string]interface{}
			if rawErr := json.Unmarshal(data, &raw); rawErr == nil {
				log.Debug().Interface("raw_message", raw).Msg("Parsed raw Binance message for debugging")
			}
			continue
		}
		if tradeResp.EventType != "trade" {
			log.Warn().Str("event", tradeResp.EventType).Str("data", string(data)).Msg("Non-trade Binance message")
			continue
		}

		price := util.ParseFloat(tradeResp.Price)
		quantity := util.ParseFloat(tradeResp.Quantity)
		ts := time.UnixMilli(tradeResp.TradeTime)
		pair := util.PairFromBinance(tradeResp.Symbol)

		c.mu.Lock()
		ch, ok := c.subs[pair]
		if !ok {
			log.Warn().Str("pair", pair).Interface("subs", c.subs).Str("data", string(data)).Msg("No subscriber for Binance trade pair")
			c.mu.Unlock()
			continue
		}
		c.mu.Unlock()

		log.Debug().Str("pair", pair).Float64("price", price).Float64("quantity", quantity).Time("ts", ts).Msg("Received Binance trade")
		select {
		case ch <- models.Trade{Timestamp: ts, Price: price, Quantity: quantity, Pair: pair}:
			log.Debug().Str("pair", pair).Msg("Sent Binance trade to channel")
		default:
			log.Warn().Str("pair", pair).Msg("Dropped Binance trade due to full channel")
		}
	}
}

func (c *Binance) pingLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for c.reconnect {
		select {
		case <-ticker.C:
			if c.conn != nil {
				c.mu.Lock()
				err := c.conn.WriteJSON(map[string]interface{}{
					"method": "PING",
					"id":     time.Now().UnixNano(),
				})
				c.mu.Unlock()
				if err != nil {
					log.Error().Err(err).Msg("Binance ping error")
				} else {
					log.Debug().Msg("Sent Binance ping")
				}
			}
		}
	}
}

func (c *Binance) resubscribe() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for pair := range c.subs {
		params := []string{fmt.Sprintf("%s@trade", util.PairToBinance(pair))}
		sub := map[string]interface{}{
			"method": "SUBSCRIBE",
			"params": params,
			"id":     time.Now().UnixNano(),
		}
		if c.conn != nil {
			if err := c.conn.WriteJSON(sub); err != nil {
				log.Error().Err(err).Str("pair", pair).Msg("Binance resubscribe failed")
			} else {
				log.Info().Str("pair", pair).Msg("Resubscribed to Binance trade feed")
			}
		}
	}
}

func (c *Binance) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reconnect = false
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}
