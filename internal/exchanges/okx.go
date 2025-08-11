package exchanges

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
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

	log.Info().Str("pair", pair).Msg("Subscribed to OKX trade feed")
	return nil
}

func (o *OKX) readLoop() {
	for o.reconnect {
		if o.conn == nil {
			time.Sleep(o.reconnectInterval)
			continue
		}

		_, data, err := o.conn.ReadMessage()
		if err != nil {
			log.Error().
				Err(err).
				Str("error_code", common.ErrCodeExchangeReadFailed.String()).
				Str("error_message", common.ErrMsgExchangeReadFailed.String()).
				Msg("OKX read error, reconnecting")

			o.mu.Lock()
			if o.conn != nil {
				if err := o.conn.Close(); err != nil {
					log.Error().
						Err(err).
						Str("error_code", common.ErrCodeOKXExchangeConnectionCloseFailed.String()).
						Str("error_message", common.ErrMsgOKXExchangeConnectionCloseFailed.String()).
						Msg("Failed to close Binance WebSocket connection")
				}
				o.conn = nil
			}
			o.mu.Unlock()

			if o.reconnectAttempts >= o.maxReconnectAttempts {
				log.Error().Msg("Max reconnect attempts reached")
				o.reconnect = false
				return
			}

			o.reconnectAttempts++
			time.Sleep(o.reconnectInterval)
			if err := o.Connect(); err != nil {
				log.Error().Err(err).Msg("Reconnect failed")
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
					log.Error().
						Interface("data", genericResp).
						Str("error_code", common.ErrCodeExchangeReadFailed.String()).
						Msg("Received OKX error")
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
						log.Debug().Str("pair", pair).Msg("Sent OKX trade to channel")
					default:
						log.Warn().
							Str("error_code", common.ErrCodeChannelFull.String()).
							Str("error_message", common.ErrMsgChannelFull.String()).
							Str("pair", pair).
							Msg("Dropped OKX trade due to full channel")
					}
				}
			}
			continue
		}
	}
}

func (o *OKX) pingLoop() {
	ticker := time.NewTicker(o.pingInterval)
	defer ticker.Stop()

	for o.reconnect {
		<-ticker.C
		if o.conn != nil {
			o.mu.Lock()
			err := o.conn.WriteMessage(websocket.TextMessage, []byte("ping"))
			o.mu.Unlock()
			if err != nil {
				log.Error().
					Err(err).
					Str("error_code", common.ErrCodeExchangePingFailed.String()).
					Str("error_message", common.ErrMsgExchangePingFailed.String()).
					Msg("OKX ping error")
			}
		}
	}
}

func (o *OKX) resubscribe() {
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
				log.Error().
					Err(err).
					Str("error_code", common.ErrCodeExchangeSubscribeFailed.String()).
					Str("error_message", common.ErrMsgExchangeSubscribeFailed.String()).
					Str("pair", pair).
					Msg("OKX resubscribe failed")
			} else {
				log.Info().Str("pair", pair).Msg("Resubscribed to OKX trade feed")
			}
		}
	}
}

func (o *OKX) Close() {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.reconnect = false
	if o.conn != nil {
		if err := o.conn.Close(); err != nil {
			log.Error().
				Err(err).
				Str("error_code", common.ErrCodeOKXExchangeConnectionCloseFailed.String()).
				Str("error_message", common.ErrMsgOKXExchangeConnectionCloseFailed.String()).
				Msg("Failed to close Binance WebSocket connection during shutdown")
		}
		o.conn = nil
	}
}
