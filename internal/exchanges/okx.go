package exchanges

import (
	"encoding/json"
	"fmt"
	"go-candles/internal/util"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"go-candles/pkg/models"
)

type OKX struct {
	conn      *websocket.Conn
	mu        sync.Mutex
	subs      map[string]chan models.Trade
	reconnect bool
}

func NewOKX() *OKX {
	return &OKX{
		subs:      make(map[string]chan models.Trade),
		reconnect: true,
	}
}

func (o *OKX) Connect() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	conn, _, err := websocket.DefaultDialer.Dial("wss://ws.okx.com:8443/ws/v5/public", nil)
	if err != nil {
		return fmt.Errorf("okx connect: %w", err)
	}
	o.conn = conn
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
		return fmt.Errorf("okx subscribe %s: %w", pair, err)
	}
	log.Info().Str("pair", pair).Msg("Subscribed to OKX trade feed")
	return nil
}

func (o *OKX) readLoop() {
	for o.reconnect {
		if o.conn == nil {
			time.Sleep(5 * time.Second)
			continue
		}
		_, data, err := o.conn.ReadMessage()
		if err != nil {
			log.Error().Err(err).Msg("OKX read error, reconnecting")
			o.mu.Lock()
			if o.conn != nil {
				o.conn.Close()
				o.conn = nil
			}
			o.mu.Unlock()
			time.Sleep(5 * time.Second)
			o.Connect()
			o.resubscribe()
			continue
		}

		log.Debug().Str("raw", string(data)).Msg("Received OKX message")

		if string(data) == "pong" {
			log.Debug().Msg("Received OKX pong")
			continue
		}

		var genericResp map[string]interface{}
		if err := json.Unmarshal(data, &genericResp); err == nil {
			if event, ok := genericResp["event"].(string); ok {
				if event == "subscribe" {
					log.Debug().Interface("args", genericResp["arg"]).Msg("Received OKX subscription confirmation")
					continue
				}
				if event == "error" {
					log.Error().Interface("data", genericResp).Msg("Received OKX error")
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
				Ts string `json:"ts"`
				Px string `json:"px"`
				Sz string `json:"sz"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &resp); err == nil && resp.Arg.Channel == "trades" {
			for _, d := range resp.Data {
				price := util.ParseFloat(d.Px)
				qty := util.ParseFloat(d.Sz)
				tsMs, _ := strconv.ParseInt(d.Ts, 10, 64)
				ts := time.UnixMilli(tsMs)
				pair := util.PairFromOKX(resp.Arg.InstId)

				o.mu.Lock()
				ch, ok := o.subs[pair]
				o.mu.Unlock()
				if ok {
					log.Debug().Str("pair", pair).Float64("price", price).Float64("quantity", qty).Time("ts", ts).Msg("Received OKX trade")
					ch <- models.Trade{Timestamp: ts, Price: price, Quantity: qty, Pair: pair}
				} else {
					log.Warn().Str("pair", pair).Msg("No subscriber for OKX trade")
				}
			}
			continue
		}

		log.Warn().Str("data", string(data)).Msg("Received unhandled OKX message")
	}
}

func (o *OKX) pingLoop() {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	for o.reconnect {
		select {
		case <-ticker.C:
			if o.conn != nil {
				o.mu.Lock()
				err := o.conn.WriteMessage(websocket.TextMessage, []byte("ping"))
				o.mu.Unlock()
				if err != nil {
					log.Error().Err(err).Msg("OKX ping error")
				} else {
					log.Debug().Msg("Sent OKX ping")
				}
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
				log.Error().Err(err).Str("pair", pair).Msg("OKX resubscribe failed")
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
		o.conn.Close()
		o.conn = nil
	}
}
