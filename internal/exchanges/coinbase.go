package exchanges

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go-candles/internal/common"
	"go-candles/internal/util"
	"go-candles/pkg/models"
)

type Coinbase struct {
	conn                 *websocket.Conn
	mu                   sync.Mutex
	subs                 map[string]chan models.Trade
	reconnect            bool
	wsURL                string
	reconnectInterval    time.Duration
	pingInterval         time.Duration
	maxReconnectAttempts int
	reconnectAttempts    int
}

func NewCoinbase(wsURL string, reconnectInterval, pingInterval time.Duration, maxReconnectAttempts int) *Coinbase {
	return &Coinbase{
		subs:                 make(map[string]chan models.Trade),
		reconnect:            true,
		wsURL:                wsURL,
		reconnectInterval:    reconnectInterval,
		pingInterval:         pingInterval,
		maxReconnectAttempts: maxReconnectAttempts,
	}
}

func (c *Coinbase) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, _, err := websocket.DefaultDialer.Dial(c.wsURL, nil)
	if err != nil {
		return fmt.Errorf("%s: %w", common.ErrMsgExchangeConnectFailed.String(), err)
	}
	c.conn = conn
	c.reconnectAttempts = 0
	c.conn.SetReadLimit(1 << 20) // Set 1MB read limit

	go c.readLoop()
	go c.pingLoop()
	return nil
}

func (c *Coinbase) Subscribe(pair string, ch chan models.Trade) error {
	logger := util.NewLogger()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.subs[pair] = ch
	if c.conn == nil {
		if err := c.Connect(); err != nil {
			return fmt.Errorf("%s %s: %w", common.ErrMsgExchangeSubscribeFailed.String(), pair, err)
		}
	}

	msg := map[string]interface{}{
		"type":        "subscribe",
		"product_ids": []string{util.PairToCoinbase(pair)},
		"channel":     "market_trades",
	}
	if err := c.conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("%s %s: %w", common.ErrMsgExchangeSubscribeFailed.String(), pair, err)
	}

	logger.Info("Subscribed to Coinbase trade feed", "pair", pair)
	return nil
}

func (c *Coinbase) readLoop() {
	logger := util.NewLogger()
	defer func() {
		if r := recover(); r != nil {
			logger.Error(nil, common.ErrCodeExchangeReadFailed, common.ErrMsgExchangeReadFailed, "Recovered from panic in Coinbase read loop", "recover", r)

			c.mu.Lock()
			if c.conn != nil {
				if err := c.conn.Close(); err != nil {
					logger.Error(err, common.ErrCodeCoinbaseExchangeConnectionCloseFailed, common.ErrMsgCoinbaseExchangeConnectionCloseFailed, "Failed to close Coinbase WebSocket connection")
				}
				c.conn = nil
			}
			c.mu.Unlock()

			if c.reconnectAttempts < c.maxReconnectAttempts {
				time.Sleep(c.reconnectInterval)
				if err := c.Connect(); err != nil {
					logger.Error(err, common.ErrCodeExchangeConnectFailed, common.ErrMsgExchangeConnectFailed, "Reconnect failed after panic")
				}
			}
		}
	}()

	for c.reconnect {
		if c.conn == nil {
			time.Sleep(c.reconnectInterval)
			continue
		}

		_, data, err := c.conn.ReadMessage()
		if err != nil {
			logger.Error(err, common.ErrCodeExchangeReadFailed, common.ErrMsgExchangeReadFailed, "Coinbase read error, reconnecting")

			c.mu.Lock()
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
			}
			c.mu.Unlock()

			if c.reconnectAttempts >= c.maxReconnectAttempts {
				logger.Error(nil, common.ErrCodeExchangeReadFailed, common.ErrMsgExchangeReadFailed, "Max reconnect attempts reached")
				c.reconnect = false
				return
			}

			c.reconnectAttempts++
			time.Sleep(c.reconnectInterval)
			if err := c.Connect(); err != nil {
				logger.Error(err, common.ErrCodeExchangeConnectFailed, common.ErrMsgExchangeConnectFailed, "Reconnect failed")
				continue
			}
			c.resubscribe()
			continue
		}

		// Check for error response
		var errorResp struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(data, &errorResp); err == nil && errorResp.Type == "error" {
			logger.Error(nil, common.ErrCodeExchangeReadFailed, common.ErrorMessage(errorResp.Message), "Coinbase subscription error")
			continue
		}

		// Check for ping response
		var pingResp struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &pingResp); err == nil && pingResp.Type == "heartbeat" {
			continue
		}

		// Check for subscription response
		var subResp struct {
			Type     string   `json:"type"`
			Channels []string `json:"channels"`
		}
		if err := json.Unmarshal(data, &subResp); err == nil && subResp.Type == "subscriptions" {
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
				logger.Error(err, common.ErrCodeExchangeReadFailed, common.ErrMsgExchangeReadFailed, "Failed to parse Coinbase trade time", "time", tradeResp.Time)
				continue
			}
			pair := util.PairFromCoinbase(tradeResp.ProductID)

			c.mu.Lock()
			ch, ok := c.subs[pair]
			c.mu.Unlock()
			if ok {
				select {
				case ch <- models.Trade{Timestamp: ts, Price: price, Quantity: quantity, Pair: pair}:
					logger.Debug("Sent Coinbase trade to channel", "pair", pair)
				default:
					logger.Warn(common.ErrCodeChannelFull, common.ErrMsgChannelFull, "Dropped Coinbase trade due to full channel", "pair", pair)
				}
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
							logger.Error(err, common.ErrCodeExchangeReadFailed, common.ErrMsgExchangeReadFailed, "Failed to parse Coinbase snapshot trade time", "time", trade.Time)
							continue
						}
						pair := util.PairFromCoinbase(trade.ProductID)

						c.mu.Lock()
						ch, ok := c.subs[pair]
						c.mu.Unlock()
						if ok {
							select {
							case ch <- models.Trade{Timestamp: ts, Price: price, Quantity: quantity, Pair: pair}:
								logger.Debug("Sent Coinbase snapshot trade to channel", "pair", pair)
							default:
								logger.Warn(common.ErrCodeChannelFull, common.ErrMsgChannelFull, "Dropped Coinbase snapshot trade due to full channel", "pair", pair)
							}
						}
					}
				}
			}
			continue
		}
	}
}

func (c *Coinbase) pingLoop() {
	logger := util.NewLogger()
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()

	for c.reconnect {
		<-ticker.C
		if c.conn != nil {
			c.mu.Lock()
			err := c.conn.WriteJSON(map[string]interface{}{
				"type": "heartbeat",
				"on":   true,
			})
			c.mu.Unlock()
			if err != nil {
				logger.Error(err, common.ErrCodeExchangePingFailed, common.ErrMsgExchangePingFailed, "Coinbase ping error")
			}
		}
	}
}

func (c *Coinbase) resubscribe() {
	logger := util.NewLogger()
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
				logger.Error(err, common.ErrCodeExchangeSubscribeFailed, common.ErrMsgExchangeSubscribeFailed, "Coinbase resubscribe failed", "pair", pair)
			} else {
				logger.Info("Resubscribed to Coinbase trade feed", "pair", pair)
			}
		}
	}
}

func (c *Coinbase) Close() {
	logger := util.NewLogger()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reconnect = false
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			logger.Error(err, common.ErrCodeCoinbaseExchangeConnectionCloseFailed, common.ErrMsgCoinbaseExchangeConnectionCloseFailed, "Failed to close Coinbase WebSocket connection during shutdown")
		}
		c.conn = nil
	}
}
