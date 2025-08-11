package util

import (
	"github.com/rs/zerolog/log"
	"go-candles/internal/common"
)

// Logger provides utility functions for consistent logging.
type Logger struct{}

// NewLogger creates a new Logger instance.
func NewLogger() *Logger {
	return &Logger{}
}

// Error logs an error with the specified error code, message, and optional fields.
func (l *Logger) Error(err error, errorCode common.ErrorCode, errorMsg common.ErrorMessage, msg string, fields ...interface{}) {
	event := log.Error().
		Err(err).
		Str("error_code", errorCode.String()).
		Str("error_message", errorMsg.String())

	// Add optional fields (key-value pairs)
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			event = event.Interface(fields[i].(string), fields[i+1])
		}
	}

	event.Msg(msg)
}

// Warn logs a warning with the specified error code, message, and optional fields.
func (l *Logger) Warn(errorCode common.ErrorCode, errorMsg common.ErrorMessage, msg string, fields ...interface{}) {
	event := log.Warn().
		Str("error_code", errorCode.String()).
		Str("error_message", errorMsg.String())

	// Add optional fields (key-value pairs)
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			event = event.Interface(fields[i].(string), fields[i+1])
		}
	}

	event.Msg(msg)
}

// Info logs an info message with optional fields.
func (l *Logger) Info(msg string, fields ...interface{}) {
	event := log.Info()

	// Add optional fields (key-value pairs)
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			event = event.Interface(fields[i].(string), fields[i+1])
		}
	}

	event.Msg(msg)
}

// Debug logs a debug message with optional fields.
func (l *Logger) Debug(msg string, fields ...interface{}) {
	event := log.Debug()

	// Add optional fields (key-value pairs)
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			event = event.Interface(fields[i].(string), fields[i+1])
		}
	}

	event.Msg(msg)
}
