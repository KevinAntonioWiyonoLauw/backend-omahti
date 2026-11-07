// language: Go, file: internal/orders/orders.go
// Penyimpanan status order dan model pesan yang lewat di queue.
// Store sengaja in-memory: fokus demo ada di Redis dan RabbitMQ, bukan di database.
package orders

import (
	"fmt"
	"sync"
	"time"
)

// Status adalah posisi order di pipeline.
type Status string

const (
	StatusQueued    Status = "queued"    // sudah di-publish ke broker
	StatusConsumed  Status = "consumed"  // worker sudah mengambil pesan
	StatusProcessed Status = "processed" // stok sudah dipotong, status di DB diperbarui
	StatusFailed    Status = "failed"    // gagal, pesan dibuang
)

// Message adalah body JSON yang dikirim ke queue "orders_queue".
type Message struct {
	OrderID   string    `json:"order_id"`
	ProductID int       `json:"product_id"`
	Qty       int       `json:"qty"`
	TotalIDR  int       `json:"total_idr"`
	CreatedAt time.Time `json:"created_at"`
}

// Order adalah catatan status order.
type Order struct {
	ID          string    `json:"order_id"`
	ProductID   int       `json:"product_id"`
	ProductName string    `json:"product_name"`
	Qty         int       `json:"qty"`
	TotalIDR    int       `json:"total_idr"`
	Status      Status    `json:"status"`
	Attempts    int       `json:"attempts"`
	Error       string    `json:"error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	QueuedAt    time.Time `json:"queued_at"`
	ConsumedAt  time.Time `json:"consumed_at,omitempty"`
	FinishedAt  time.Time `json:"finished_at,omitempty"`
	StockLeft   int       `json:"stock_left"`
	// LatencyMS diukur dari created_at; menunjukkan total waktu async request.
	LatencyMS int64 `json:"latency_ms"`
}

// Store menyimpan order di memori, aman dipakai banyak goroutine.
type Store struct {
	mu        sync.RWMutex
	byID      map[string]*Order
	recentCap int
	recentIDs []string // urutan pembuatan, untuk daftar "recent"
}

// NewStore membuat store dengan batas order yang disimpan.
func NewStore(capacity int) *Store {
	if capacity <= 0 {
		capacity = 200
	}
	return &Store{byID: make(map[string]*Order), recentCap: capacity}
}

// Create menyimpan order baru berstatus queued.
func (s *Store) Create(o Order) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := o
	s.byID[o.ID] = &cp
	s.recentIDs = append(s.recentIDs, o.ID)
	// Buang order terlama supaya memori tidak tumbuh tanpa batas.
	for len(s.recentIDs) > s.recentCap {
		delete(s.byID, s.recentIDs[0])
		s.recentIDs = s.recentIDs[1:]
	}
}

// Update memutasi satu order di bawah lock lalu mengembalikan salinannya.
func (s *Store) Update(id string, fn func(o *Order)) (Order, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.byID[id]
	if !ok {
		return Order{}, false
	}
	fn(o)
	return *o, true
}

// Get mengambil salinan order.
func (s *Store) Get(id string) (Order, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.byID[id]
	if !ok {
		return Order{}, false
	}
	return *o, true
}

// Recent mengembalikan order terbaru lebih dulu, maksimal n.
func (s *Store) Recent(n int) []Order {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if n > len(s.recentIDs) {
		n = len(s.recentIDs)
	}
	out := make([]Order, 0, n)
	for i := len(s.recentIDs) - 1; i >= 0 && len(out) < n; i-- {
		if o, ok := s.byID[s.recentIDs[i]]; ok {
			out = append(out, *o)
		}
	}
	return out
}

// CountByStatus menghitung order per status.
func (s *Store) CountByStatus() map[Status]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[Status]int)
	for _, o := range s.byID {
		out[o.Status]++
	}
	return out
}

// NewID membuat order id ringkas yang mudah dibaca di log: ORD-3F2A91.
func NewID(seq uint64) string {
	return fmt.Sprintf("ORD-%06X", seq)
}
