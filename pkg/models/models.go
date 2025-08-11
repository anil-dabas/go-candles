package models

import "time"

type Trade struct {
	Timestamp time.Time
	Price     float64
	Quantity  float64
	Pair      string
}

type Candle struct {
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	Pair      string
}
