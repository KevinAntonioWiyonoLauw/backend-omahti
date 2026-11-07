// language: Go, file: internal/store/store.go
// "Tabel products" in-memory beserta stok yang bisa berubah.
//
// Sengaja TIDAK menyimulasikan query lambat di sini: jeda buatan milik layer
// API (Config.DBLatency), supaya waktu di response bisa dijelaskan dengan satu
// sumber saja. Store ini hanya menjaga konsistensi data di bawah lock.
package store

import (
	"errors"
	"sort"
	"sync"
)

// ErrOutOfStock dikembalikan kalau stok produk sudah habis.
var ErrOutOfStock = errors.New("stok habis")

// ErrProductNotFound dikembalikan untuk id yang tidak ada di tabel.
var ErrProductNotFound = errors.New("produk tidak ditemukan")

// Product adalah satu baris tabel products.
type Product struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	PriceIDR    int    `json:"price_idr"`
	Stock       int    `json:"stock"`
	Description string `json:"description"`
}

// Store adalah database produk in-memory dengan mutex untuk operasi tulis.
type Store struct {
	mu       sync.RWMutex
	products map[int]*Product
}

// New mengisi store dari seed produk awal.
func New(seed []Product) *Store {
	s := &Store{products: make(map[int]*Product, len(seed))}
	for _, p := range seed {
		cp := p
		s.products[p.ID] = &cp
	}
	return s
}

// Get membaca satu produk (read-only, aman dipanggil paralel).
func (s *Store) Get(id int) (Product, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.products[id]
	if !ok {
		return Product{}, false
	}
	return *p, true
}

// All mengembalikan semua produk, diurutkan menurut ID.
func (s *Store) All() []Product {
	s.mu.RLock()
	out := make([]Product, 0, len(s.products))
	for _, p := range s.products {
		out = append(out, *p)
	}
	s.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Len mengembalikan jumlah produk.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.products)
}

// DecrementStock mengurangi stok secara atomik di bawah write lock.
// Inilah titik yang harus dikunci: tanpa lock, dua worker bisa sama-sama
// membaca stok 1 lalu keduanya menjual unit terakhir.
func (s *Store) DecrementStock(id int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.products[id]
	if !ok {
		return 0, ErrProductNotFound
	}
	if p.Stock <= 0 {
		return 0, ErrOutOfStock
	}
	p.Stock--
	return p.Stock, nil
}

// SetStock menyetel ulang stok (dipakai kalau ingin mengulang demo).
func (s *Store) SetStock(id, stock int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.products[id]
	if !ok {
		return ErrProductNotFound
	}
	p.Stock = stock
	return nil
}
