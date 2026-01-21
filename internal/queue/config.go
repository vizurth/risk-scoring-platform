package queue

import (
	"time"
)

// Config содержит конфигурацию для Kafka
type Config struct {
	// Connection
	Brokers []string `yaml:"brokers"`

	// Consumer
	GroupID              string `yaml:"group_id" env-default:"arbiter-group"`
	AutoOffsetReset      string `yaml:"auto_offset_reset" env-default:"earliest"`
	EnableAutoCommit     bool   `yaml:"enable_auto_commit" env-default:"false"`
	AutoCommitIntervalMs int    `yaml:"auto_commit_interval_ms" env-default:"5000"`
	SessionTimeoutMs     int    `yaml:"session_timeout_ms" env-default:"10000"`
	MaxPollRecords       int    `yaml:"max_poll_records" env-default:"500"`

	// Producer
	CompressionType     string `yaml:"compression_type" env-default:"snappy"`
	Acks                string `yaml:"acks" env-default:"all"`
	RequestTimeoutMs    int    `yaml:"request_timeout_ms" env-default:"30000"`
	DeliveryTimeoutMs   int    `yaml:"delivery_timeout_ms" env-default:"120000"`
	MaxInFlightRequests int    `yaml:"max_in_flight_requests" env-default:"5"`
	DeliveryChannelSize int    `yaml:"delivery_channel_size" env-default:"10000"`

	// Common
	ApiVersion   string        `yaml:"api_version" env-default:"auto"`
	ClientID     string        `yaml:"client_id" env-default:"arbiter"`
	ReadTimeout  time.Duration `yaml:"read_timeout" env-default:"10s"`
	WriteTimeout time.Duration `yaml:"write_timeout" env-default:"10s"`
}

// NewConfig создаёт конфиг по умолчанию
func NewConfig(brokers []string) *Config {
	return &Config{
		Brokers:             brokers,
		GroupID:             "arbiter-group",
		AutoOffsetReset:     "earliest",
		EnableAutoCommit:    false, // ручной коммит безопаснее
		CompressionType:     "snappy",
		Acks:                "all",
		DeliveryChannelSize: 10000,
		ClientID:            "arbiter",
	}
}
