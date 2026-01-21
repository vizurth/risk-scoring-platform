package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type kafkaConsumer struct {
	consumer *kafka.Consumer
	config   *Config
	log      *zap.Logger
	topics   []string
}

// NewConsumer создаёт новый Kafka consumer
func NewConsumer(ctx context.Context, cfg *Config, topics []string, log *zap.Logger) (Consumer, error) {
	if len(topics) == 0 {
		return nil, fmt.Errorf("at least one topic is required")
	}

	conf := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(cfg.Brokers, ","),
		"group.id":                 cfg.GroupID,
		"auto.offset.reset":        cfg.AutoOffsetReset,
		"enable.auto.commit":       cfg.EnableAutoCommit,
		"auto.commit.interval. ms": cfg.AutoCommitIntervalMs,
		"session.timeout. ms":      cfg.SessionTimeoutMs,
		"max.poll.records":         cfg.MaxPollRecords,
		"api.version. request":     true,
		"broker.version.fallback":  "auto",
	}

	consumer, err := kafka.NewConsumer(conf)
	if err != nil {
		log.Error("failed to create kafka consumer", zap.Error(err))
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	// Подписываемся на топики
	if err := consumer.SubscribeTopics(topics, nil); err != nil {
		log.Error("failed to subscribe to topics", zap.Strings("topics", topics), zap.Error(err))
		consumer.Close()
		return nil, fmt.Errorf("failed to subscribe:  %w", err)
	}

	log.Info("kafka consumer created",
		zap.Strings("topics", topics),
		zap.String("group_id", cfg.GroupID))

	return &kafkaConsumer{
		consumer: consumer,
		config:   cfg,
		log:      log,
		topics:   topics,
	}, nil
}

// Read читает одно сообщение
func (c *kafkaConsumer) Read(ctx context.Context) (*Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	kafkaMsg, err := c.consumer.ReadMessage(500 * time.Millisecond)
	if err != nil {
		// ignore timeout
		if err.(kafka.Error).Code() == kafka.ErrTimedOut {
			return nil, nil
		}
		c.log.Error("failed to read message", zap.Error(err))
		return nil, fmt.Errorf("read failed: %w", err)
	}

	topic := *kafkaMsg.TopicPartition.Topic
	partition := kafkaMsg.TopicPartition.Partition
	offset := kafkaMsg.TopicPartition.Offset

	return &Message{
		Key:       string(kafkaMsg.Key),
		Topic:     topic,
		Value:     kafkaMsg.Value,
		Partition: partition,
		Offset:    int64(offset),
	}, nil
}

// ReadBatch читает батч сообщений
func (c *kafkaConsumer) ReadBatch(ctx context.Context, maxMessages int) ([]*Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	messages := make([]*Message, 0, maxMessages)

	for i := 0; i < maxMessages; i++ {
		msg, err := c.Read(ctx)
		if err != nil {
			if i > 0 {
				return messages, nil // возвращаем то, что прочитали
			}
			return nil, err
		}

		if msg == nil {
			break // timeout
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

// CommitSync коммитит оффсет синхронно
func (c *kafkaConsumer) CommitSync(ctx context.Context, msg *Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	partition := msg.Partition
	offset := kafka.Offset(msg.Offset + 1)

	topicPartitions := []kafka.TopicPartition{
		{
			Topic:     &msg.Topic,
			Partition: partition,
			Offset:    offset,
		},
	}

	_, err := c.consumer.CommitOffsets(topicPartitions)
	if err != nil {
		c.log.Error("failed to commit offset",
			zap.String("topic", msg.Topic),
			zap.Int32("partition", partition),
			zap.Int64("offset", msg.Offset),
			zap.Error(err))
		return fmt.Errorf("commit failed: %w", err)
	}

	c.log.Debug("offset committed",
		zap.String("topic", msg.Topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", msg.Offset))

	return nil
}

// Close закрывает консьюмер
func (c *kafkaConsumer) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	err := c.consumer.Close()
	if err != nil {
		c.log.Error("failed to close consumer", zap.Error(err))
		return fmt.Errorf("close failed: %w", err)
	}

	c.log.Info("kafka consumer closed")
	return nil
}
