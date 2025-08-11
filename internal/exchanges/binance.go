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

type Binance struct {
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

func NewBinance(wsURL string, reconnectInterval, pingInterval time.Duration, maxReconnectAttempts int) *Binance {
	return &Binance{
		subs:                 make(map[string]chan models.Trade),
		reconnect:            true,
		wsURL:                wsURL,
		reconnectInterval:    reconnectInterval,
		pingInterval:         pingInterval,
		maxReconnectAttempts: maxReconnectAttempts,
	}
}

func (c *Binance) Connect() error {
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

func (c *Binance) Subscribe(pair string, ch chan models.Trade) error {
	logger := util.NewLogger()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.subs[pair] = ch

	if c.conn == nil {
		if err := c.Connect(); err != nil {
			return fmt.Errorf("%s %s: %w", common.ErrMsgExchangeSubscribeFailed.String(), pair, err)
		}
	}

	params := []string{fmt.Sprintf("%s@trade", util.PairToBinance(pair))}
	sub := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": params,
		"id":     time.Now().UnixNano(),
	}

	if err := c.conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("%s %s: %w", common.ErrMsgExchangeSubscribeFailed.String(), pair, err)
	}

	logger.Info("Subscribed to Binance trade feed", "pair", pair)
	return nil
}

func (c *Binance) readLoop() {
	logger := util.NewLogger()
	defer func() {
		if r := recover(); r != nil {
			logger.Error(nil, common.ErrCodeExchangeReadFailed, common.ErrMsgExchangeReadFailed, "Recovered from panic in Binance read loop", "recover", r)

			c.mu.Lock()
			if c.conn != nil {
				if err := c.conn.Close(); err != nil {
					logger.Error(err, common.ErrCodeBinanceExchangeConnectionCloseFailed, common.ErrMsgBinanceExchangeConnectionCloseFailed, "Failed to close Binance WebSocket connection")
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
			logger.Error(err, common.ErrCodeExchangeReadFailed, common.ErrMsgExchangeReadFailed, "Binance read error, reconnecting")

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

		// Check for ping response
		var pingResp struct {
			Result interface{} `json:"result"`
			Id     int64       `json:"id"`
		}
		if err := json.Unmarshal(data, &pingResp); err == nil && pingResp.Result == nil && pingResp.Id != 0 {
			logger.Debug("Received Binance ping response", "id", pingResp.Id)
			continue
		}

		// Check for subscription response
		var subResp struct {
			Result []string `json:"result"`
			Id     int64    `json:"id"`
		}
		if err := json.Unmarshal(data, &subResp); err == nil && len(subResp.Result) == 0 && subResp.Id != 0 {
			logger.Debug("Received Binance subscription response", "id", subResp.Id)
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
			logger.Warn(common.ErrCodeExchangeReadFailed, common.ErrMsgExchangeReadFailed, "Failed to unmarshal Binance message", "data", string(data))
			continue
		}
		if tradeResp.EventType != "trade" {
			continue
		}

		price := util.ParseFloat(tradeResp.Price)
		quantity := util.ParseFloat(tradeResp.Quantity)
		ts := time.UnixMilli(tradeResp.TradeTime)
		pair := util.PairFromBinance(tradeResp.Symbol)

		c.mu.Lock()
		ch, ok := c.subs[pair]
		c.mu.Unlock()
		if ok {
			select {
			case ch <- models.Trade{Timestamp: ts, Price: price, Quantity: quantity, Pair: pair}:
				logger.Debug("Sent Binance trade to channel", "pair", pair)
			default:
				logger.Warn(common.ErrCodeChannelFull, common.ErrMsgChannelFull, "Dropped Binance trade due to full channel", "pair", pair)
			}
		}
	}
}

func (c *Binance) pingLoop() {
	logger := util.NewLogger()
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()

	for c.reconnect {
		<-ticker.C
		if c.conn != nil {
			c.mu.Lock()
			err := c.conn.WriteJSON(map[string]interface{}{
				"method": "PING",
				"id":     time.Now().UnixNano(),
			})
			c.mu.Unlock()
			if err != nil {
				logger.Error(err, common.ErrCodeExchangePingFailed, common.ErrMsgExchangePingFailed, "Binance ping error")
			}
		}
	}
}

func (c *Binance) resubscribe() {
	logger := util.NewLogger()
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
				logger.Error(err, common.ErrCodeExchangeSubscribeFailed, common.ErrMsgExchangeSubscribeFailed, "Binance resubscribe failed", "pair", pair)
			} else {
				logger.Info("Resubscribed to Binance trade feed", "pair", pair)
			}
		}
	}
}

func (c *Binance) Close() {
	logger := util.NewLogger()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reconnect = false
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			logger.Error(err, common.ErrCodeBinanceExchangeConnectionCloseFailed, common.ErrMsgBinanceExchangeConnectionCloseFailed, "Failed to close Binance WebSocket connection during shutdown")
		}
		c.conn = nil
	}
}
