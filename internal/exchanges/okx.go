package exchanges

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go-candles/internal/common"
	"go-candles/internal/util"
	"go-candles/pkg/models"
)

type OKX struct {
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

func NewOKX(wsURL string, reconnectInterval, pingInterval time.Duration, maxReconnectAttempts int) *OKX {
	return &OKX{
		subs:                 make(map[string]chan models.Trade),
		reconnect:            true,
		wsURL:                wsURL,
		reconnectInterval:    reconnectInterval,
		pingInterval:         pingInterval,
		maxReconnectAttempts: maxReconnectAttempts,
	}
}

func (o *OKX) Connect() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	conn, _, err := websocket.DefaultDialer.Dial(o.wsURL, nil)
	if err != nil {
		return fmt.Errorf("%s: %w", common.ErrMsgExchangeConnectFailed.String(), err)
	}
	o.conn = conn
	o.reconnectAttempts = 0

	go o.readLoop()
	go o.pingLoop()
	return nil
}

func (o *OKX) Subscribe(pair string, ch chan models.Trade) error {
	logger := util.NewLogger()
	o.mu.Lock()
	defer o.mu.Unlock()

	o.subs[pair] = ch
	if o.conn == nil {
		return nil
	}

	sub := map[string]interface{}{
		"op": "subscribe",
		"args": []map[string]string{
			{
				"channel": "trades",
				"instId":  util.PairToOKX(pair),
			},
		},
	}
	if err := o.conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("%s %s: %w", common.ErrMsgExchangeSubscribeFailed.String(), pair, err)
	}

	logger.Info("Subscribed to OKX trade feed", "pair", pair)
	return nil
}

func (o *OKX) readLoop() {
	logger := util.NewLogger()
	for o.reconnect {
		if o.conn == nil {
			time.Sleep(o.reconnectInterval)
			continue
		}

		_, data, err := o.conn.ReadMessage()
		if err != nil {
			logger.Error(err, common.ErrCodeExchangeReadFailed, common.ErrMsgExchangeReadFailed, "OKX read error, reconnecting")

			o.mu.Lock()
			if o.conn != nil {
				if err := o.conn.Close(); err != nil {
					logger.Error(err, common.ErrCodeOKXExchangeConnectionCloseFailed, common.ErrMsgOKXExchangeConnectionCloseFailed, "Failed to close OKX WebSocket connection")
				}
				o.conn = nil
			}
			o.mu.Unlock()

			if o.reconnectAttempts >= o.maxReconnectAttempts {
				logger.Error(nil, common.ErrCodeExchangeReadFailed, common.ErrMsgExchangeReadFailed, "Max reconnect attempts reached")
				o.reconnect = false
				return
			}

			o.reconnectAttempts++
			time.Sleep(o.reconnectInterval)
			if err := o.Connect(); err != nil {
				logger.Error(err, common.ErrCodeExchangeConnectFailed, common.ErrMsgExchangeConnectFailed, "Reconnect failed")
				continue
			}
			o.resubscribe()
			continue
		}

		if string(data) == "pong" {
			continue
		}

		var genericResp map[string]interface{}
		if err := json.Unmarshal(data, &genericResp); err == nil {
			if event, ok := genericResp["event"].(string); ok {
				if event == "subscribe" {
					continue
				}
				if event == "error" {
					logger.Error(nil, common.ErrCodeExchangeReadFailed, common.ErrMsgExchangeReadFailed, "Received OKX error", "data", genericResp)
					continue
				}
			}
		}

		var resp struct {
			Arg struct {
				Channel string `json:"channel"`
				InstId  string `json:"instId"`
			} `json:"arg"`
			Data []struct {
				InstId  string `json:"instId"`
				TradeId string `json:"tradeId"`
				Px      string `json:"px"`
				Sz      string `json:"sz"`
				Side    string `json:"side"`
				Ts      string `json:"ts"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &resp); err == nil && resp.Arg.Channel == "trades" {
			for _, d := range resp.Data {
				price := util.ParseFloat(d.Px)
				qty := util.ParseFloat(d.Sz)
				tsMs, _ := strconv.ParseInt(d.Ts, 10, 64)
				ts := time.UnixMilli(tsMs)
				pair := util.PairFromOKX(d.InstId)

				o.mu.Lock()
				ch, ok := o.subs[pair]
				o.mu.Unlock()
				if ok {
					select {
					case ch <- models.Trade{Timestamp: ts, Price: price, Quantity: qty, Pair: pair}:
						logger.Debug("Sent OKX trade to channel", "pair", pair)
					default:
						logger.Warn(common.ErrCodeChannelFull, common.ErrMsgChannelFull, "Dropped OKX trade due to full channel", "pair", pair)
					}
				}
			}
			continue
		}
	}
}

func (o *OKX) pingLoop() {
	logger := util.NewLogger()
	ticker := time.NewTicker(o.pingInterval)
	defer ticker.Stop()

	for o.reconnect {
		<-ticker.C
		if o.conn != nil {
			o.mu.Lock()
			err := o.conn.WriteMessage(websocket.TextMessage, []byte("ping"))
			o.mu.Unlock()
			if err != nil {
				logger.Error(err, common.ErrCodeExchangePingFailed, common.ErrMsgExchangePingFailed, "OKX ping error")
			}
		}
	}
}

func (o *OKX) resubscribe() {
	logger := util.NewLogger()
	o.mu.Lock()
	defer o.mu.Unlock()

	for pair := range o.subs {
		sub := map[string]interface{}{
			"op": "subscribe",
			"args": []map[string]string{
				{
					"channel": "trades",
					"instId":  util.PairToOKX(pair),
				},
			},
		}
		if o.conn != nil {
			if err := o.conn.WriteJSON(sub); err != nil {
				logger.Error(err, common.ErrCodeExchangeSubscribeFailed, common.ErrMsgExchangeSubscribeFailed, "OKX resubscribe failed", "pair", pair)
			} else {
				logger.Info("Resubscribed to OKX trade feed", "pair", pair)
			}
		}
	}
}

func (o *OKX) Close() {
	logger := util.NewLogger()
	o.mu.Lock()
	defer o.mu.Unlock()

	o.reconnect = false
	if o.conn != nil {
		if err := o.conn.Close(); err != nil {
			logger.Error(err, common.ErrCodeOKXExchangeConnectionCloseFailed, common.ErrMsgOKXExchangeConnectionCloseFailed, "Failed to close OKX WebSocket connection during shutdown")
		}
		o.conn = nil
	}
}
