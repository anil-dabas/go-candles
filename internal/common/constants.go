package common

const (
	DefaultConfigPath        = "./configs/config.yml"
	DefaultCandleIntervalSec = 5
	DefaultChannelBufferSize = 10000

	ExchangeBinance  = "binance"
	ExchangeCoinbase = "coinbase"
	ExchangeOKX      = "okx"

	ListenerChannelSize = 10000
	MaxGRPCMessageSize  = 1024 * 1024 * 10 // 10MB
)
