// language: Go, file: internal/worker/worker.go
// Consumer "orders_queue". Worker mengambil satu pesan, memprosesnya sekitar 3
// detik (simulasi pembayaran + potong stok), lalu meng-ack pesan. Setiap
// perpindahan status dipancarkan lewat events.Hub supaya frontend bisa
// menggambar animasinya.
package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"demo-cache-queue/internal/bus"
	"demo-cache-queue/internal/events"
	"demo-cache-queue/internal/orders"
)

// ProcessingTime adalah lama simulasi kerja worker per order.
const ProcessingTime = 3 * time.Second

// StockStore adalah potongan data yang dibutuhkan worker. Implementasinya
// internal/store.Store, tapi interface kecil ini membuat worker gampang dites.
type StockStore interface {
	// DecrementStock harus atomik: stok tidak boleh minus saat order bersamaan.
	DecrementStock(productID int) (remaining int, err error)
}

// Broker adalah potongan bus.Broker yang dipakai worker.
type Broker interface {
	Consume(ctx context.Context, queue string, prefetch int, handler func(bus.Message, bus.Ack, bus.Nack)) error
}

// Worker memproses pesan order.
type Worker struct {
	hub       *events.Hub
	store     *orders.Store
	catalog   StockStore
	broker    Broker
	queueName string
	processed atomic.Int64
	failed    atomic.Int64
}

// New membuat worker. Satu Worker sebaiknya dijalankan satu goroutine (Run).
func New(hub *events.Hub, store *orders.Store, cat StockStore, brk Broker, queueName string) *Worker {
	return &Worker{hub: hub, store: store, catalog: cat, broker: brk, queueName: queueName}
}

// Processed mengembalikan jumlah order yang selesai diproses.
func (w *Worker) Processed() int64 { return w.processed.Load() }

// Failed mengembalikan jumlah order yang gagal.
func (w *Worker) Failed() int64 { return w.failed.Load() }

// Run memblokir sampai ctx dibatalkan.
func (w *Worker) Run(ctx context.Context) {
	if err := w.broker.Consume(ctx, w.queueName, 1, w.handle); err != nil && ctx.Err() == nil {
		slog.Error("consumer berhenti", "err", err)
	}
}

// handle menjalankan satu order dari awal sampai selesai. Wajib memanggil ack
// atau nack — kalau tidak, RabbitMQ akan menahan pesan dan queue bocor.
func (w *Worker) handle(msg bus.Message, ack bus.Ack, nack bus.Nack) {
	var m orders.Message
	if err := json.Unmarshal(msg.Body, &m); err != nil {
		slog.Warn("body bukan json valid, pesan dibuang", "err", err)
		_ = nack(false)
		return
	}

	// 1. CONSUMED: worker sudah memegang pesan ini, pesan keluar dari queue.
	consumedAt := time.Now()
	o, ok := w.store.Update(m.OrderID, func(o *orders.Order) {
		o.Status = orders.StatusConsumed
		o.ConsumedAt = consumedAt
		o.Attempts++
	})
	if !ok {
		slog.Warn("order tidak dikenal di store", "order", m.OrderID)
		_ = nack(false)
		return
	}

	w.hub.Publish(events.TypeOrder, map[string]any{
		"order_id":         m.OrderID,
		"product_name":     o.ProductName,
		"status":           orders.StatusConsumed,
		"stage":            "consumer",
		"note":             "worker mengambil pesan dari queue (delivery, belum di-ack)",
		"wait_in_queue_ms": o.ConsumedAt.Sub(o.QueuedAt).Milliseconds(),
	})

	// 2. Simulasi kerja: potong stok dulu (atomik), lalu tunggu 3 detik.
	remaining, err := w.catalog.DecrementStock(m.ProductID)
	if err != nil {
		w.fail(m.OrderID, err)
		_ = nack(false) // gagal permanen: jangan diulang selamanya
		return
	}

	time.Sleep(ProcessingTime)

	// 3. PROCESSED: ubah status dulu, lalu ack, baru pancarkan event.
	// ack() pada RabbitMQ menulis ke socket. Kalau event dipancarkan sebelum
	// ack, broker in-memory (dan broker lain yang memblokir handler) bisa
	// menganggap pesan belum selesai dan mengirimkannya ulang — order yang sama
	// diproses dua kali dan stok terpotong dua kali.
	finishedAt := time.Now()
	done, _ := w.store.Update(m.OrderID, func(o *orders.Order) {
		o.Status = orders.StatusProcessed
		o.FinishedAt = finishedAt
		o.StockLeft = remaining
		o.LatencyMS = finishedAt.Sub(o.CreatedAt).Milliseconds()
	})
	w.processed.Add(1)

	if err := ack(); err != nil {
		slog.Warn("ack gagal", "order", m.OrderID, "err", err)
	}

	w.hub.Publish(events.TypeOrder, map[string]any{
		"order_id":     m.OrderID,
		"product_name": done.ProductName,
		"status":       orders.StatusProcessed,
		"stage":        "consumer",
		"note":         "stok dipotong, status order diperbarui, pesan di-ack",
		"stock_left":   remaining,
		"latency_ms":   done.LatencyMS,
		"process_ms":   ProcessingTime.Milliseconds(),
	})
}

func (w *Worker) fail(id string, cause error) {
	now := time.Now()
	o, _ := w.store.Update(id, func(o *orders.Order) {
		o.Status = orders.StatusFailed
		o.Error = cause.Error()
		o.FinishedAt = now
		o.LatencyMS = now.Sub(o.CreatedAt).Milliseconds()
	})
	w.failed.Add(1)
	w.hub.Publish(events.TypeOrder, map[string]any{
		"order_id":     id,
		"product_name": o.ProductName,
		"status":       orders.StatusFailed,
		"stage":        "consumer",
		"note":         "gagal: " + cause.Error() + " — pesan di-discard (nack requeue=false)",
	})
}

// Pool menjalankan beberapa worker sekaligus dan menunggu semuanya berhenti.
type Pool struct {
	workers []*Worker
	wg      sync.WaitGroup
}

// StartPool menjalankan n worker dengan broker yang sama.
func StartPool(ctx context.Context, n int, build func() *Worker) *Pool {
	p := &Pool{}
	for i := 0; i < n; i++ {
		w := build()
		p.workers = append(p.workers, w)
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			w.Run(ctx)
		}()
	}
	return p
}

// Processed / Failed menjumlahkan counter semua worker di pool.
func (p *Pool) Processed() int64 {
	var total int64
	for _, w := range p.workers {
		total += w.Processed()
	}
	return total
}

// Failed menjumlahkan order gagal di semua worker.
func (p *Pool) Failed() int64 {
	var total int64
	for _, w := range p.workers {
		total += w.Failed()
	}
	return total
}

// Wait menunggu semua worker berhenti (dipanggil saat shutdown).
func (p *Pool) Wait() { p.wg.Wait() }
