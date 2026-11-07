// language: Go, file: internal/events/events.go
// Hub untuk SSE: satu channel per klien, broadcast tanpa saling memblokir.
// Kalau klien lambat, event-nya dibuang — demo tidak boleh macet gara-gara tab yang ketutup.
package events

import (
	"sync"
	"time"
)

// Type adalah jenis event yang dipancarkan ke frontend.
type Type string

const (
	TypeConnected Type = "connected"
	TypeCache     Type = "cache" // hasil GET /product/:id
	TypeOrder     Type = "order" // perpindahan status order di pipeline
	TypeQueue     Type = "queue" // snapshot queue broker
	TypeStats     Type = "stats" // counter cache & order
)

// Event adalah satu baris JSON yang dikirim lewat SSE.
type Event struct {
	Type Type   `json:"type"`
	At   int64  `json:"at"` // unix ms, dipakai frontend untuk timeline
	Data any    `json:"data"`
	Seq  uint64 `json:"seq,omitempty"` // urutan publish, membantu debugging
}

// Hub membagikan event ke semua subscriber.
type Hub struct {
	mu     sync.RWMutex
	subs   map[chan Event]struct{}
	seq    uint64
	buffer int
}

// NewHub membuat hub dengan buffer per klien (event di luar buffer dibuang).
func NewHub() *Hub {
	return &Hub{subs: make(map[chan Event]struct{}), buffer: 256}
}

// Subscribe menerima channel yang akan menerima semua event berikutnya.
// Pemanggil wajib memanggil Unsubscribe dan tidak boleh menutup channel-nya sendiri.
func (h *Hub) Subscribe() chan Event {
	ch := make(chan Event, h.buffer)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

// Unsubscribe melepas channel dari hub lalu menutupnya.
func (h *Hub) Unsubscribe(ch chan Event) {
	h.mu.Lock()
	if _, ok := h.subs[ch]; ok {
		delete(h.subs, ch)
		close(ch)
	}
	h.mu.Unlock()
}

// Publish mengirim event ke semua subscriber secara non-blocking.
func (h *Hub) Publish(t Type, data any) {
	h.mu.Lock()
	h.seq++
	ev := Event{Type: t, At: time.Now().UnixMilli(), Data: data, Seq: h.seq}
	for ch := range h.subs {
		select {
		case ch <- ev:
		default: // klien lambat: lewati, koneksi SSE tetap hidup
		}
	}
	h.mu.Unlock()
}

// Subscribers mengembalikan jumlah koneksi SSE aktif.
func (h *Hub) Subscribers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}
