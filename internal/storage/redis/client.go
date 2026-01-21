package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Config struct {
	Addr        string `yaml:"addr"`
	Password    string `yaml:"password"`
	User        string `yaml:"user"`
	DB          int    `yaml:"db"`
	MaxRetries  int    `yaml:"max_retries"`
	DialTimeout int    `yaml:"dial_timeout"`
	Timeout     int    `yaml:"timeout"`
}

type Client struct {
	client *redis.Client
	log    *zap.Logger
}

func NewClient(ctx context.Context, cfg Config, log *zap.Logger) (*Client, error) {
	db := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		Username:     cfg.User,
		MaxRetries:   cfg.MaxRetries,
		DialTimeout:  time.Duration(cfg.DialTimeout) * time.Second,
		ReadTimeout:  time.Duration(cfg.Timeout) * time.Second,
		WriteTimeout: time.Duration(cfg.Timeout) * time.Second,
	})

	// Retry с правильным backoff
	var err error
	retries := 5
	for i := 0; i < retries; i++ {
		err = db.Ping(ctx).Err()
		if err == nil {
			log.Info("redis connected", zap.Int("attempt", i+1))
			break
		}

		if i < retries-1 {
			backoff := time.Duration(1<<uint(i)) * 100 * time.Millisecond
			log.Warn("failed to ping redis, retrying",
				zap.Error(err),
				zap.Duration("backoff", backoff),
				zap.Int("attempt", i+1))
			time.Sleep(backoff)
		}
	}

	if err != nil {
		log.Error("failed to connect to redis", zap.Error(err))
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	log.Info("successfully connected to redis",
		zap.String("addr", cfg.Addr),
		zap.Int("db", cfg.DB))

	return &Client{client: db, log: log}, nil
}

// Close закрывает подключение
func (c *Client) Close(ctx context.Context) error {
	if err := c.client.Close(); err != nil {
		c.log.Error("failed to close redis", zap.Error(err))
		return fmt.Errorf("redis close failed: %w", err)
	}
	c.log.Info("redis connection closed")
	return nil
}

// GetString получает строку
func (c *Client) GetString(ctx context.Context, key string) (string, error) {
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			c.log.Debug("key not found", zap.String("key", key))
			return "", nil
		}
		c.log.Error("redis get failed", zap.String("key", key), zap.Error(err))
		return "", err
	}
	return val, nil
}

// GetJSON получает JSON и распаковывает в структуру
func (c *Client) GetJSON(ctx context.Context, key string, v interface{}) error {
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			c.log.Debug("key not found", zap.String("key", key))
			return fmt.Errorf("key not found: %s", key)
		}
		c.log.Error("redis get failed", zap.String("key", key), zap.Error(err))
		return err
	}

	// Распаковываем JSON в структуру
	if err := json.Unmarshal([]byte(val), v); err != nil {
		c.log.Error("failed to unmarshal json",
			zap.String("key", key),
			zap.String("value", val),
			zap.Error(err))
		return fmt.Errorf("json unmarshal failed: %w", err)
	}

	return nil
}

// SetJSON устанавливает JSON с TTL
func (c *Client) SetJSON(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	// Упаковываем структуру в JSON
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		c.log.Error("failed to marshal json",
			zap.String("key", key),
			zap.Error(err))
		return fmt.Errorf("json marshal failed:  %w", err)
	}

	if err := c.client.Set(ctx, key, jsonBytes, ttl).Err(); err != nil {
		c.log.Error("redis set failed", zap.String("key", key), zap.Error(err))
		return err
	}

	return nil
}

// GetBytes получает raw bytes
func (c *Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			c.log.Debug("key not found", zap.String("key", key))
			return nil, nil
		}
		c.log.Error("redis get failed", zap.String("key", key), zap.Error(err))
		return nil, err
	}
	return []byte(val), nil
}

// Set устанавливает строковое значение с TTL
func (c *Client) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	if err := c.client.Set(ctx, key, value, ttl).Err(); err != nil {
		c.log.Error("redis set failed", zap.String("key", key), zap.Error(err))
		return err
	}
	return nil
}

// IncrBy увеличивает значение (для счётчиков)
func (c *Client) IncrBy(ctx context.Context, key string, incr int64) (int64, error) {
	val, err := c.client.IncrBy(ctx, key, incr).Result()
	if err != nil {
		c.log.Error("redis incrby failed", zap.String("key", key), zap.Error(err))
		return 0, err
	}
	return val, nil
}

// Delete удаляет ключ
func (c *Client) Delete(ctx context.Context, keys ...string) (int64, error) {
	val, err := c.client.Del(ctx, keys...).Result()
	if err != nil {
		c.log.Error("redis delete failed", zap.Strings("keys", keys), zap.Error(err))
		return 0, err
	}
	return val, nil
}

// Exists проверяет наличие ключа
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	val, err := c.client.Exists(ctx, keys...).Result()
	if err != nil {
		c.log.Error("redis exists failed", zap.Strings("keys", keys), zap.Error(err))
		return 0, err
	}
	return val, nil
}

// ExpireAt устанавливает TTL по времени
func (c *Client) ExpireAt(ctx context.Context, key string, tm time.Time) (bool, error) {
	val, err := c.client.ExpireAt(ctx, key, tm).Result()
	if err != nil {
		c.log.Error("redis expire failed", zap.String("key", key), zap.Error(err))
		return false, err
	}
	return val, nil
}
