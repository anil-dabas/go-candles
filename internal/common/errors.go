package common

type ErrorCode string
type ErrorMessage string

const (
	ErrCodeConfigLoadFailed                      ErrorCode = "CONFIG_LOAD_FAILED"
	ErrCodeGRPCConnectionFailed                  ErrorCode = "GRPC_CONNECTION_FAILED"
	ErrCodeGRPCServeFailed                       ErrorCode = "GRPC_SERVE_FAILED"
	ErrCodeExchangeConnectFailed                 ErrorCode = "EXCHANGE_CONNECT_FAILED"
	ErrCodeExchangeSubscribeFailed               ErrorCode = "EXCHANGE_SUBSCRIBE_FAILED"
	ErrCodeExchangeReadFailed                    ErrorCode = "EXCHANGE_READ_FAILED"
	ErrCodeExchangePingFailed                    ErrorCode = "EXCHANGE_PING_FAILED"
	ErrCodeInvalidPair                           ErrorCode = "INVALID_PAIR"
	ErrCodeChannelFull                           ErrorCode = "CHANNEL_FULL"
	ErrCodeStreamClosed                          ErrorCode = "STREAM_CLOSED"
	ErrCodeGRPCConnectionCloseFailed             ErrorCode = "GRPC_CONNECTION_CLOSE_FAILED"
	ErrCodeBinanceExchangeConnectionCloseFailed  ErrorCode = "BINANCE_EXCHANGE_CONNECTION_CLOSE_FAILED"
	ErrCodeCoinbaseExchangeConnectionCloseFailed ErrorCode = "COINBASE_EXCHANGE_CONNECTION_CLOSE_FAILED"
	ErrCodeOKXExchangeConnectionCloseFailed      ErrorCode = "OKX_EXCHANGE_CONNECTION_CLOSE_FAILED"
)

const (
	ErrMsgConfigLoadFailed                      ErrorMessage = "Failed to load configuration"
	ErrMsgGRPCConnectionFailed                  ErrorMessage = "Failed to connect to gRPC server"
	ErrMsgGRPCServeFailed                       ErrorMessage = "Failed to serve gRPC"
	ErrMsgExchangeConnectFailed                 ErrorMessage = "Failed to connect to exchange"
	ErrMsgExchangeSubscribeFailed               ErrorMessage = "Failed to subscribe to exchange"
	ErrMsgExchangeReadFailed                    ErrorMessage = "Failed to read from exchange"
	ErrMsgExchangePingFailed                    ErrorMessage = "Failed to ping exchange"
	ErrMsgInvalidPair                           ErrorMessage = "Invalid trading pair"
	ErrMsgChannelFull                           ErrorMessage = "Channel is full, message dropped"
	ErrMsgStreamClosed                          ErrorMessage = "Stream closed by server"
	ErrMsgGRPCConnectionCloseFailed             ErrorMessage = "failed to close gRPC connection"
	ErrMsgBinanceExchangeConnectionCloseFailed  ErrorMessage = "failed to close binance exchange WebSocket connection"
	ErrMsgCoinbaseExchangeConnectionCloseFailed ErrorMessage = "failed to close coinbase exchange WebSocket connection"
	ErrMsgOKXExchangeConnectionCloseFailed      ErrorMessage = "failed to close OKX exchange WebSocket connection"
)

func (e ErrorCode) String() string {
	return string(e)
}

func (m ErrorMessage) String() string {
	return string(m)
}
