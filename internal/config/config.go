package config

import (
	"go-candles/internal/common"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type ServerConfig struct {
	Port int    `yaml:"port"`
	Host string `yaml:"host"`
}

type ExchangeConfig struct {
	WebsocketURL         string `yaml:"websocket_url"`
	ReconnectIntervalSec int    `yaml:"reconnect_interval_sec"`
	PingIntervalSec      int    `yaml:"ping_interval_sec"`
}

type Config struct {
	Server               ServerConfig              `yaml:"server"`
	Pairs                []string                  `yaml:"pairs"`
	Exchanges            map[string]ExchangeConfig `yaml:"exchanges"`
	CandleIntervalSec    int                       `yaml:"candle_interval_sec"`
	ChannelBufferSize    int                       `yaml:"channel_buffer_size"`
	MaxReconnectAttempts int                       `yaml:"max_reconnect_attempts"`
	LogLevel             string                    `yaml:"log_level"` // Add this field
}

func LoadConfig(path string) (*Config, error) {
	config := &Config{
		LogLevel: "info", // Default log level
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	d := yaml.NewDecoder(file)
	if err := d.Decode(&config); err != nil {
		return nil, err
	}

	return config, nil
}

func (c *Config) GetCandleInterval() time.Duration {
	if c.CandleIntervalSec <= 0 {
		return time.Duration(common.DefaultCandleIntervalSec) * time.Second
	}
	return time.Duration(c.CandleIntervalSec) * time.Second
}

func (c *Config) GetChannelBufferSize() int {
	if c.ChannelBufferSize <= 0 {
		return common.DefaultChannelBufferSize
	}
	return c.ChannelBufferSize
}

func (c *Config) GetExchangeConfig(exchange string) (ExchangeConfig, bool) {
	ec, ok := c.Exchanges[exchange]
	return ec, ok
}
