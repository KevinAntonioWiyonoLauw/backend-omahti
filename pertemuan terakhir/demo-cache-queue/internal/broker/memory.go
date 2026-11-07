// language: Go, file: internal/broker/memory.go
// Broker in-memory untuk mode demo tanpa Docker: alur publish → queue → consume
// dan ack/nack identik dengan RabbitMQ. Hanya transportnya yang berbeda.
// Berguna saat kelas tidak punya jaringan stabil; bukan contoh produksi.
package broker

import (
	"context"
	"errors"
	"sync"

	"demo-cache-queue/internal/bus"
)

// ErrClosed dikembalikan Publish setelah broker ditutup.
var ErrClosed = errors.New("broker ditutup")

type memoryJob struct {
	body []byte
}

// decision adalah hasil pemrosesan satu pesan. Dibuat tipe sendiri, bukan bool,
// karena ack dan nack(true) sama-sama "true" kalau dipaksa jadi boolean — dan
// itulah bug yang membuat pesan yang sudah di-ack dikirim ulang selamanya.
type decision int

const (
	decisionAck   decision = iota // pesan selesai, hapus dari queue
	decisionRetry                 // nack(requeue=true): kembalikan ke queue
	decisionDrop                  // nack(requeue=false): buang, jangan diulang
)

// MemoryBroker meniru queue FIFO sederhana.
type MemoryBroker struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []*memoryJob
	closed bool
}

// NewMemory membuat broker in-memory. capacity adalah perkiraan awal antrean.
func NewMemory(capacity int) *MemoryBroker {
	if capacity < 16 {
		capacity = 16
	}
	m := &MemoryBroker{queue: make([]*memoryJob, 0, capacity)}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// Publish selalu berhasil selama broker belum ditutup.
func (m *MemoryBroker) Publish(ctx context.Context, _ string, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrClosed
	}
	m.queue = append(m.queue, &memoryJob{body: body})
	m.cond.Signal()
	return nil
}

// Consume memproses satu pesan sekaligus dan menunggu ack/nack sebelum lanjut,
// meniru prefetch=1. nack(requeue=true) mengembalikan pesan ke ekor antrean.
func (m *MemoryBroker) Consume(ctx context.Context, _ string, _ int, handler func(bus.Message, bus.Ack, bus.Nack)) error {
	// Bangunkan Wait ketika ctx dibatalkan, supaya Consume bisa berhenti.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			m.mu.Lock()
			m.cond.Broadcast()
			m.mu.Unlock()
		case <-stop:
		}
	}()

	for {
		job, err := m.next(ctx)
		if err != nil {
			return err
		}

		// ack/nack hanya mencatat keputusan lalu kembali; tidak menunggu consumer,
		// karena handler baru selesai setelah ack() kembali.
		decided := make(chan decision, 2)
		handler(
			bus.Message{Body: job.body},
			func() error { decided <- decisionAck; return nil },
			func(requeue bool) error {
				if requeue {
					decided <- decisionRetry
				} else {
					decided <- decisionDrop
				}
				return nil
			},
		)

		result := decisionDrop // handler yang tidak memutuskan apa pun = gagal permanen
		select {
		case result = <-decided:
		default:
		}

		if result != decisionRetry {
			continue
		}

		m.mu.Lock()
		if !m.closed {
			m.queue = append(m.queue, job)
			m.cond.Signal()
		}
		m.mu.Unlock()
	}
}

// next menunggu sampai ada pesan, lalu mengambil yang terdepan.
func (m *MemoryBroker) next(ctx context.Context) (*memoryJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for len(m.queue) == 0 {
		if m.closed {
			return nil, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m.cond.Wait()
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	job := m.queue[0]
	// Slot dikosongkan supaya pointer lama tidak tertahan di backing array.
	m.queue[0] = nil
	m.queue = m.queue[1:]
	return job, nil
}

// QueueDepth mengembalikan jumlah pesan yang masih menunggu.
func (m *MemoryBroker) QueueDepth(context.Context, string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queue), nil
}

// Backend memberi nama implementasi untuk frontend.
func (m *MemoryBroker) Backend() string { return "in-memory (mode demo)" }

// Close membangunkan semua consumer dan menolak publish berikutnya.
func (m *MemoryBroker) Close() error {
	m.mu.Lock()
	m.closed = true
	m.cond.Broadcast()
	m.mu.Unlock()
	return nil
}
