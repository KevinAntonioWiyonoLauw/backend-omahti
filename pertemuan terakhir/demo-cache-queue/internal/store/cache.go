// language: Go, file: internal/store/cache.go
// Layer cache: satu interface kecil, dua implementasi — Redis (utama) dan
// in-memory (fallback saat Redis tidak tersedia). Dengan begitu demo tetap jalan
// di kelas walau container Redis belum naik.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"
)

// ErrCacheMiss artinya key tidak ada di cache (atau sudah kedaluwarsa).
var ErrCacheMiss = errors.New("cache miss")

// Cache adalah kontrak cache yang dipakai oleh API.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) error
	// Backend memberi label "redis" atau "memory" untuk metadata response.
	Backend() string
	Ping(ctx context.Context) error
}

// MemoryCache adalah Cache in-memory dengan TTL, dipakai kalau Redis mati.
type MemoryCache struct {
	mu    sync.RWMutex
	items map[string]memoryItem
}

type memoryItem struct {
	value  []byte
	expire time.Time
}

// NewMemoryCache membuat cache in-memory kosong.
func NewMemoryCache() *MemoryCache {
	return &MemoryCache{items: make(map[string]memoryItem)}
}

// Get mengembalikan salinan value selama key ada dan belum kedaluwarsa.
func (m *MemoryCache) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	it, ok := m.items[key]
	m.mu.RUnlock()

	if !ok {
		return nil, ErrCacheMiss
	}
	if !it.expire.IsZero() && time.Now().After(it.expire) {
		m.mu.Lock()
		delete(m.items, key)
		m.mu.Unlock()
		return nil, ErrCacheMiss
	}
	return it.value, nil
}

// Set menyimpan value dengan TTL. TTL 0 berarti tidak pernah kedaluwarsa.
func (m *MemoryCache) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	m.mu.Lock()
	m.items[key] = memoryItem{value: value, expire: exp}
	m.mu.Unlock()
	return nil
}

// Del menghapus key yang diberikan.
func (m *MemoryCache) Del(_ context.Context, keys ...string) error {
	m.mu.Lock()
	for _, k := range keys {
		delete(m.items, k)
	}
	m.mu.Unlock()
	return nil
}

// Backend memberi label "memory" untuk response.
func (m *MemoryCache) Backend() string { return "memory" }

// Ping selalu berhasil karena tidak ada koneksi jaringan.
func (m *MemoryCache) Ping(context.Context) error { return nil }

// ProductKey / TTLKey membuat nama key yang konsisten antar layer.
// product:3 menyimpan JSON produk, ttl:product:3 menyimpan sisa TTL dalam detik
// (Redis tidak menyimpan TTL di dalam value, jadi UI butuh key pendamping).
func ProductKey(id int) string { return "product:" + strconv.Itoa(id) }

// TTLKey adalah key pendamping untuk menampilkan sisa umur cache di UI.
func TTLKey(id int) string { return "ttl:product:" + strconv.Itoa(id) }

// MarshalProduct menyandikan produk ke JSON untuk disimpan di cache.
func MarshalProduct(p Product) ([]byte, error) { return json.Marshal(p) }

// UnmarshalProduct membaca produk dari nilai cache.
func UnmarshalProduct(b []byte) (Product, error) {
	var p Product
	err := json.Unmarshal(b, &p)
	return p, err
}
