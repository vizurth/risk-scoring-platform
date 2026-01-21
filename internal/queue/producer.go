package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type kafkaProducer struct {
	producer        *kafka.Producer
	config          *Config
	deliveryChannel chan kafka.Event
	stopChan        chan struct{}
	wg              sync.WaitGroup
	log             *zap.Logger
	metrics         ProducerMetrics
}

// ProducerMetrics метрики продюсера
type ProducerMetrics struct {
	Sent      int64
	Failed    int64
	Delivered int64
	mu        sync.RWMutex
}

// NewProducer создаёт новый Kafka producer
func NewProducer(ctx context.Context, cfg *Config, log *zap.Logger) (Producer, error) {
	conf := &kafka.ConfigMap{
		"bootstrap.servers":      strings.Join(cfg.Brokers, ","),
		"compression.type":       cfg.CompressionType,
		"acks":                   cfg.Acks,
		"request.timeout.ms":     cfg.RequestTimeoutMs,
		"delivery.timeout.ms":    cfg.DeliveryTimeoutMs,
		"max.in.flight.requests": cfg.MaxInFlightRequests,
		"api.version.request":    true,
	}

	producer, err := kafka.NewProducer(conf)
	if err != nil {
		log.Error("failed to create kafka producer", zap.Error(err))
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	kp := &kafkaProducer{
		producer:        producer,
		config:          cfg,
		deliveryChannel: make(chan kafka.Event, cfg.DeliveryChannelSize),
		stopChan:        make(chan struct{}),
		log:             log,
	}

	// Запускаем goroutine для обработки доставок
	kp.wg.Add(1)
	go kp.handleDeliveries()

	log.Info("kafka producer created",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("compression", cfg.CompressionType))

	return kp, nil
}

// Send отправляет сообщение асинхронно
func (p *kafkaProducer) Send(ctx context.Context, topic string, key string, value []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(key),
		Value: value,
	}

	if err := p.producer.Produce(kafkaMsg, p.deliveryChannel); err != nil {
		p.log.Error("failed to produce message",
			zap.String("topic", topic),
			zap.String("key", key),
			zap.Error(err))
		p.metrics.incFailed()
		return fmt.Errorf("produce failed: %w", err)
	}

	p.metrics.incSent()
	return nil
}

// SendSync отправляет сообщение синхронно (ждёт подтверждения)
func (p *kafkaProducer) SendSync(ctx context.Context, topic string, key string, value []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(key),
		Value: value,
	}

	if err := p.producer.Produce(kafkaMsg, deliveryChan); err != nil {
		p.log.Error("failed to produce message (sync)",
			zap.String("topic", topic),
			zap.String("key", key),
			zap.Error(err))
		p.metrics.incFailed()
		return fmt.Errorf("produce failed: %w", err)
	}

	// Ждём результата с таймаутом
	select {
	case <-ctx.Done():
		return ctx.Err()
	case event := <-deliveryChan:
		m := event.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			p.log.Error("delivery error",
				zap.String("topic", topic),
				zap.String("key", key),
				zap.Error(m.TopicPartition.Error))
			p.metrics.incFailed()
			return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
		}
		p.metrics.incDelivered()
		return nil
	case <-time.After(30 * time.Second):
		p.metrics.incFailed()
		return fmt.Errorf("delivery timeout")
	}
}

// SendBatch отправляет батч сообщений
func (p *kafkaProducer) SendBatch(ctx context.Context, messages []*ProducerMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	for _, msg := range messages {
		if err := p.Send(ctx, msg.Topic, msg.Key, msg.Value); err != nil {
			p.log.Error("failed to send batch message",
				zap.String("topic", msg.Topic),
				zap.String("key", msg.Key),
				zap.Error(err))
			// продолжаем отправлять остальные
		}
	}

	return nil
}

// handleDeliveries обрабатывает результаты доставки
func (p *kafkaProducer) handleDeliveries() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopChan:
			return
		case event := <-p.deliveryChannel:
			if event == nil {
				return
			}

			m := event.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				p.log.Warn("message delivery failed",
					zap.String("topic", *m.TopicPartition.Topic),
					zap.String("key", string(m.Key)),
					zap.Error(m.TopicPartition.Error))
				p.metrics.incFailed()
			} else {
				p.log.Debug("message delivered",
					zap.String("topic", *m.TopicPartition.Topic),
					zap.Int32("partition", m.TopicPartition.Partition),
					zap.Int64("offset", int64(m.TopicPartition.Offset)))
				p.metrics.incDelivered()
			}
		}
	}
}

// Close закрывает продюсер
func (p *kafkaProducer) Close(ctx context.Context) error {
	close(p.stopChan)

	// Ждём завершения обработчика доставок с таймаутом
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		p.log.Warn("producer close timeout")
	}

	// Флашим оставшиеся сообщения
	remaining := p.producer.Flush(30000)
	if remaining > 0 {
		p.log.Warn("messages remaining in queue", zap.Int("count", remaining))
	}

	p.producer.Close()

	p.log.Info("kafka producer closed",
		zap.Int64("sent", p.metrics.Sent),
		zap.Int64("delivered", p.metrics.Delivered),
		zap.Int64("failed", p.metrics.Failed))

	return nil
}

// Metrics helpers
func (m *ProducerMetrics) incSent() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Sent++
}

func (m *ProducerMetrics) incDelivered() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Delivered++
}

func (m *ProducerMetrics) incFailed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Failed++
}

func (m *ProducerMetrics) GetMetrics() (int64, int64, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Sent, m.Delivered, m.Failed
}
