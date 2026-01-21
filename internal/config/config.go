package config

import (
	"fmt"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/vizurth/risk-scoring-platform/internal/queue"
	"github.com/vizurth/risk-scoring-platform/internal/storage/postgres"
	"github.com/vizurth/risk-scoring-platform/internal/storage/redis"
)

type ScoringConfig struct {
	Host string `yaml:"host"`
	Port string `yaml:"port"`
}

// Config содержит общие настройки приложения
type Config struct {
	Postgres postgres.Config `yaml:"postgres"`
	Kafka    queue.Config    `yaml:"kafka"`
	Redis    redis.Config    `yaml:"redis"`
	Scoring  ScoringConfig   `yaml:"scoring_service"`
}

// New загружает конфигурацию из файла и возвращает Config
func New() (*Config, error) {
	var cfg Config

	// Ищем config.yaml в нескольких местах для совместимости с тестами и Docker
	configPaths := []string{
		"./configs/config.yaml",
		"configs/config.yaml",
		"../configs/config.yaml",
		"../../configs/config.yaml",
	}

	var configPath string
	for _, path := range configPaths {
		if _, err := os.Stat(path); err == nil {
			configPath = path
			break
		}
	}

	if configPath == "" {
		return &Config{}, fmt.Errorf("config file not found")
	}

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		return &Config{}, fmt.Errorf("error reading config: %w", err)
	}

	queueCfg := queue.NewConfig(cfg.Kafka.Brokers)
	cfg.Kafka = *queueCfg

	return &cfg, nil
}
