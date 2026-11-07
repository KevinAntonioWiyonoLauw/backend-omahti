// language: Go, file: internal/api/api_test.go
// Test end-to-end di dalam satu proses: API + broker in-memory + cache in-memory.
// Membuktikan dua klaim utama demo tanpa perlu Docker:
//  1. fetch pertama MISS (lambat), fetch kedua HIT (cepat)
//  2. POST /order membalas 202 lalu worker mengubah status menjadi processed
package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"demo-cache-queue/internal/api"
	"demo-cache-queue/internal/broker"
	"demo-cache-queue/internal/events"
	"demo-cache-queue/internal/orders"
	"demo-cache-queue/internal/store"
	"demo-cache-queue/internal/worker"
)

type fixture struct {
	srv    *httptest.Server
	hub    *events.Hub
	pool   *worker.Pool
	orders *orders.Store
	cancel context.CancelFunc
}

func newFixture(t *testing.T, dbLatency time.Duration) *fixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cache := store.NewMemoryCache()
	db := store.New([]store.Product{
		{ID: 1, Name: "Keyboard", PriceIDR: 5000, Stock: 2},
		{ID: 2, Name: "Habis", PriceIDR: 9000, Stock: 0},
	})
	hub := events.NewHub()
	orderStore := orders.NewStore(50)
	messageBus := broker.NewMemory(100)

	// Waktu proses worker tetap 3 detik (konstanta) — test ini memang menunggunya,
	// supaya yang diuji benar-benar jalur async, bukan versi yang dipercepat.
	pool := worker.StartPool(ctx, 1, func() *worker.Worker {
		return worker.New(hub, orderStore, db, messageBus, "orders_queue")
	})

	s := api.New(ctx, api.Dependencies{
		Cache:  cache,
		DB:     db,
		Hub:    hub,
		Orders: orderStore,
		Broker: messageBus,
		Config: api.Config{QueueName: "orders_queue", CacheTTL: 5 * time.Second, DBLatency: dbLatency},
		Stats:  api.StatsReader{Processed: pool.Processed, Failed: pool.Failed},
	})
	srv := httptest.NewServer(s.Routes(""))
	t.Cleanup(srv.Close)

	return &fixture{srv: srv, hub: hub, pool: pool, orders: orderStore, cancel: cancel}
}

func TestCacheAsideMissThenHit(t *testing.T) {
	f := newFixture(t, 150*time.Millisecond)

	type productResp struct {
		Source     string        `json:"source"`
		Processing int64         `json:"processing_time_ms"`
		CacheTTL   int           `json:"cache_ttl_s"`
		Product    store.Product `json:"product"`
	}

	fetch := func() (productResp, int) {
		res, err := http.Get(f.srv.URL + "/api/product/1")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer res.Body.Close()
		var body productResp
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return body, res.StatusCode
	}

	first, code := fetch()
	if code != http.StatusOK {
		t.Fatalf("status = %d, mau 200", code)
	}
	if first.Source != "database" {
		t.Fatalf("fetch pertama harus dari database, dapat %q", first.Source)
	}
	if first.Processing < 150 {
		t.Fatalf("cache miss harusnya lambat (>=150ms), dapat %dms", first.Processing)
	}
	if first.CacheTTL <= 0 {
		t.Fatalf("TTL harus terisi setelah set cache, dapat %d", first.CacheTTL)
	}

	second, _ := fetch()
	if second.Source != "redis" && second.Source != "memory" {
		t.Fatalf("fetch kedua harus dari cache, dapat %q", second.Source)
	}
	if second.Processing >= first.Processing {
		t.Fatalf("cache hit harus lebih cepat: hit=%dms miss=%dms",
			second.Processing, first.Processing)
	}
	if second.Product.ID != 1 || second.Product.PriceIDR != 5000 {
		t.Fatalf("data produk berubah: %+v", second.Product)
	}
}

func TestOrderIsAsyncThenProcessed(t *testing.T) {
	f := newFixture(t, 0)

	// Dengarkan event supaya test memastikan status berubah lewat event, bukan polling saja.
	sub := f.hub.Subscribe()
	defer f.hub.Unsubscribe(sub)

	body := strings.NewReader(`{"product_id":1,"quantity":1}`)
	res, err := http.Post(f.srv.URL+"/api/order", "application/json", body)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, mau 202 Accepted", res.StatusCode)
	}
	var created struct {
		OrderID string `json:"order_id"`
		Status  string `json:"status"`
	}
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if created.OrderID == "" {
		t.Fatal("order_id kosong")
	}
	if created.Status != string(orders.StatusQueued) {
		t.Fatalf("status awal = %q, mau queued", created.Status)
	}

	// Tunggu event "processed" atau timeout.
	deadline := time.After(worker.ProcessingTime + 3*time.Second)
	sawConsumed := false
	for {
		select {
		case ev := <-sub:
			if ev.Type != events.TypeOrder {
				continue
			}
			payload, _ := ev.Data.(map[string]any)
			switch payload["stage"] {
			case "consumer":
				if st, _ := payload["status"].(orders.Status); st == orders.StatusConsumed {
					sawConsumed = true
				}
			}
			if st, _ := payload["status"].(orders.Status); st == orders.StatusProcessed {
				if !sawConsumed {
					t.Fatal("processed terjadi tanpa didahului consumed")
				}
				goto done
			}
		case <-deadline:
			t.Fatal("timeout menunggu event processed")
		}
	}

done:
	// Event "processed" dipancarkan sebelum ack, jadi store bisa saja belum
	// terlihat diperbarui tepat saat event diterima. Tunggu sebentar sampai
	// store menyusul, dengan batas waktu supaya test tidak menggantung.
	var o orders.Order
	var ok bool
	for i := 0; i < 100; i++ {
		o, ok = f.orders.Get(created.OrderID)
		if ok && o.Status == orders.StatusProcessed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ok {
		t.Fatal("order tidak ada di store")
	}
	if o.Status != orders.StatusProcessed {
		t.Fatalf("status akhir = %q, mau processed", o.Status)
	}
	if o.StockLeft != 1 {
		t.Fatalf("stok sisa = %d, mau 1", o.StockLeft)
	}
	if o.LatencyMS < worker.ProcessingTime.Milliseconds() {
		t.Fatalf("latency terlalu kecil: %d ms", o.LatencyMS)
	}
}

func TestOrderRejectedWhenOutOfStock(t *testing.T) {
	f := newFixture(t, 0)

	res, err := http.Post(f.srv.URL+"/api/order", "application/json",
		strings.NewReader(`{"product_id":2,"quantity":1}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, mau 409 untuk stok habis", res.StatusCode)
	}
}

func TestProductNotFound(t *testing.T) {
	f := newFixture(t, 0)

	res, err := http.Get(f.srv.URL + "/api/product/99")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, mau 404", res.StatusCode)
	}
}
