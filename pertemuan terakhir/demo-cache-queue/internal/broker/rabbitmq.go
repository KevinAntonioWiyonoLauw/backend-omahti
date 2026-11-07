// language: Go, file: internal/broker/rabbitmq.go
// Implementasi amqp091-go untuk bus.Broker. Queue dibuat durable supaya pesan
// tidak hilang saat broker restart, dan kita pakai manual ack supaya kegagalan
// proses order bisa dikembalikan ke queue.
package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"demo-cache-queue/internal/bus"
)

// RabbitMQ membungkus satu koneksi + satu channel untuk publish.
type RabbitMQ struct {
	conn    *amqp.Connection
	pubChan *amqp.Channel
}

// NewRabbitMQ melakukan dial dan membuka channel publisher. Connection yang
// sama dipakai ulang untuk consumer di channel terpisah (lihat Consume).
func NewRabbitMQ(url string, queueNames ...string) (*RabbitMQ, error) {
	var conn *amqp.Connection
	var err error

	// Broker belum tentu siap saat container backend start; coba beberapa kali.
	for attempt := 1; attempt <= 15; attempt++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open publisher channel: %w", err)
	}

	for _, name := range queueNames {
		// durable=true: queue bertahan saat broker restart.
		// autoDelete=false: queue tidak dihapus saat consumer terakhir putus.
		if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			conn.Close()
			return nil, fmt.Errorf("declare queue %s: %w", name, err)
		}
	}

	return &RabbitMQ{conn: conn, pubChan: ch}, nil
}

// Publish mengirim pesan persistent ke queue yang sudah dideklarasikan.
func (r *RabbitMQ) Publish(ctx context.Context, queue string, body []byte) error {
	return r.pubChan.PublishWithContext(ctx,
		"",    // exchange default: route langsung ke nama queue
		queue, // routing key = nama queue
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
}

// Consume membaca pesan satu per satu dengan prefetch terbatas, lalu memanggil handler.
// prefetch=1 berarti worker baru ambil pesan berikutnya setelah pesan ini selesai:
// inilah cara RabbitMQ membagi beban adil antar worker yang jumlahnya berbeda.
func (r *RabbitMQ) Consume(ctx context.Context, queue string, prefetch int, handler func(bus.Message, bus.Ack, bus.Nack)) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return fmt.Errorf("open consumer channel: %w", err)
	}
	defer ch.Close()

	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", queue, err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			handler(
				bus.Message{
					Body:        d.Body,
					Timestamp:   d.Timestamp,
					Redelivered: d.Redelivered,
					DeliveryTag: d.DeliveryTag,
				},
				func() error { return d.Ack(false) },
				func(requeue bool) error { return d.Nack(false, requeue) },
			)
		}
	}
}

// QueueDepth membaca jumlah pesan ready langsung dari deklarasi queue (passive).
func (r *RabbitMQ) QueueDepth(_ context.Context, queue string) (int, error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close()

	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}
	return q.Messages, nil
}

// Backend memberi nama implementasi.
func (r *RabbitMQ) Backend() string { return "RabbitMQ" }

// Close menutup publisher channel lalu koneksi.
func (r *RabbitMQ) Close() error {
	_ = r.pubChan.Close()
	return r.conn.Close()
}
