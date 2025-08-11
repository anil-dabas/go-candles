package exchanges

import "go-candles/pkg/models"

type Exchange interface {
	Connect() error
	Subscribe(pair string, ch chan models.Trade) error
	Close()
}
