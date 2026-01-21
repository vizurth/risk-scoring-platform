package queue

import "context"

// Message - обёртка для любого сообщения из Kafka
type Message struct {
	Key       string // партиционирование по ключу
	Topic     string // из какой топики пришло
	Value     []byte // raw JSON/bytes
	Partition int32
	Offset    int64
}

// Consumer интерфейс для чтения из Kafka
type Consumer interface {
	// Read читает одно сообщение (блокирующий вызов)
	Read(ctx context.Context) (*Message, error)

	// ReadBatch читает батч сообщений
	ReadBatch(ctx context.Context, maxMessages int) ([]*Message, error)

	// CommitSync коммитит оффсет синхронно
	CommitSync(ctx context.Context, msg *Message) error

	// Close закрывает консьюмер
	Close(ctx context.Context) error
}

// Producer интерфейс для отправки в Kafka
type Producer interface {
	// Send отправляет сообщение в топику
	Send(ctx context.Context, topic string, key string, value []byte) error

	// SendSync отправляет синхронно (ждёт подтверждения)
	SendSync(ctx context.Context, topic string, key string, value []byte) error

	// SendBatch отправляет батч сообщений
	SendBatch(ctx context.Context, messages []*ProducerMessage) error

	// Close закрывает продюсер
	Close(ctx context.Context) error
}

// ProducerMessage сообщение для отправки
type ProducerMessage struct {
	Topic string
	Key   string
	Value []byte
}
