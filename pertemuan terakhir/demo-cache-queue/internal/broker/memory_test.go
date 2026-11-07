// language: Go, file: internal/broker/memory_test.go
// Broker in-memory punya satu pekerjaan penting: pesan yang sudah di-ack tidak
// boleh muncul lagi. Kalau ini bocor, order diproses dua kali dan stok terpotong
// dua kali — persis jenis bug yang demo ini ingin dicegah.
package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"demo-cache-queue/internal/bus"
)

func TestMemoryAckDiscardsMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewMemory(10)
	if err := b.Publish(ctx, "q", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	calls := make(chan struct{}, 10)
	done := make(chan struct{})
	go func() {
		_ = b.Consume(ctx, "q", 1, func(m bus.Message, ack bus.Ack, nack bus.Nack) {
			calls <- struct{}{}
			if err := ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
			select {
			case <-done:
			default:
				close(done)
			}
		})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler tidak pernah dipanggil")
	}

	// Beri waktu; pesan yang sudah di-ack tidak boleh dipanggil ulang.
	time.Sleep(300 * time.Millisecond)

	if n := len(calls); n != 1 {
		t.Fatalf("handler dipanggil %d kali, mau 1 (pesan di-ack harus hilang)", n)
	}
	depth, err := b.QueueDepth(ctx, "q")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if depth != 0 {
		t.Fatalf("queue depth = %d setelah ack, mau 0", depth)
	}
}

func TestMemoryNackRequeue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewMemory(10)
	if err := b.Publish(ctx, "q", []byte("retry")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	attempts := 0
	done := make(chan struct{})
	go func() {
		_ = b.Consume(ctx, "q", 1, func(m bus.Message, ack bus.Ack, nack bus.Nack) {
			attempts++
			if attempts == 1 {
				_ = nack(true) // gagal sementara: kembalikan ke queue
				return
			}
			_ = ack()
			close(done)
		})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pesan tidak dikirim ulang setelah nack(true)")
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, mau 2", attempts)
	}
}

func TestMemoryNackWithoutRequeueDiscards(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewMemory(10)
	if err := b.Publish(ctx, "q", []byte("poison")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	first := make(chan struct{})
	go func() {
		_ = b.Consume(ctx, "q", 1, func(m bus.Message, ack bus.Ack, nack bus.Nack) {
			select {
			case <-first:
			default:
				close(first)
			}
			_ = nack(false) // gagal permanen: buang
		})
	}()

	select {
	case <-first:
	case <-time.After(2 * time.Second):
		t.Fatal("handler tidak pernah dipanggil")
	}
	time.Sleep(300 * time.Millisecond)
	depth, _ := b.QueueDepth(ctx, "q")
	if depth != 0 {
		t.Fatalf("nack(false) harus membuang pesan, depth = %d", depth)
	}
}

var _ = errors.New
