// language: Go, file: internal/bus/bus.go
// Kontrak message broker. Producer dan consumer hanya kenal interface ini, jadi
// implementasinya bisa RabbitMQ (default) atau broker in-memory (mode demo tanpa Docker).
package bus

import (
	"context"
	"time"
)

// Message adalah satu pesan yang lewat di queue.
type Message struct {
	Body        []byte
	Timestamp   time.Time
	Redelivered bool
	DeliveryTag uint64
}

// Ack menerima pesan (pesan dihapus dari queue oleh broker).
type Ack func() error

// Nack menolak pesan. requeue=true mengembalikannya ke queue, false membuangnya.
type Nack func(requeue bool) error

// Broker adalah abstraksi minimal message broker.
type Broker interface {
	// Publish mengirim satu pesan ke queue.
	Publish(ctx context.Context, queue string, body []byte) error
	// Consume memblokir dan memanggil handler untuk setiap pesan yang masuk.
	// Handler wajib memanggil ack() atau nack().
	Consume(ctx context.Context, queue string, prefetch int, handler func(Message, Ack, Nack)) error
	// QueueDepth mengembalikan jumlah pesan yang masih menunggu di queue.
	QueueDepth(ctx context.Context, queue string) (int, error)
	// Backend memberi nama implementasi untuk ditampilkan di frontend.
	Backend() string
	Close() error
}
