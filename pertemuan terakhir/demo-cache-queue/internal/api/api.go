// language: Go, file: internal/api/api.go
// Seluruh HTTP handler demo. Dua endpoint inti:
//
//	GET  /api/product/:id -> cache-aside (Redis dulu, database belakangan)
//	POST /api/order       -> publish ke queue lalu langsung balas 202
//
// Sisanya endpoint bantu untuk dashboard: SSE, stats, reset.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"demo-cache-queue/internal/bus"
	"demo-cache-queue/internal/events"
	"demo-cache-queue/internal/orders"
	"demo-cache-queue/internal/store"
)

// Config berisi nilai yang bisa diubah lewat environment variable.
type Config struct {
	QueueName   string        // "orders_queue"
	CacheTTL    time.Duration // umur cache produk
	DBLatency   time.Duration // simulasi query lambat
	CachePrefix string        // prefix key di Redis, memudahkan reset
}

// Publisher adalah potongan bus.Broker yang dipakai handler order.
type Publisher interface {
	Publish(ctx context.Context, queue string, body []byte) error
	QueueDepth(ctx context.Context, queue string) (int, error)
	Backend() string
}

// StatsReader dipenuhi oleh worker.Pool.
type StatsReader struct {
	Processed func() int64
	Failed    func() int64
	Backend   func() string
}

// Dependencies dikumpulkan supaya Server mudah dirakit di main.go maupun di test.
type Dependencies struct {
	Cache   store.Cache
	DB      *store.Store
	Hub     *events.Hub
	Orders  *orders.Store
	Broker  Publisher
	Config  Config
	Stats   StatsReader
	Started time.Time
}

// Server memegang semua dependensi handler.
type Server struct {
	cfg    Config
	cache  store.Cache
	db     *store.Store
	hub    *events.Hub
	orders *orders.Store
	broker Publisher
	stats  StatsReader

	started  time.Time
	orderSeq atomic.Uint64

	// baseCtx dipakai untuk kerja yang harus tetap jalan setelah response
	// dikirim (publish ke queue, ack). r.Context() mati saat handler selesai.
	baseCtx context.Context

	// Counter untuk panel statistik di dashboard.
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
	dbQueries   atomic.Int64
	published   atomic.Int64

	recentMu   sync.Mutex
	recentHits []hitRecord // riwayat fetch produk untuk grafik latensi
}

// hitRecord adalah satu titik data di grafik latensi frontend.
type hitRecord struct {
	ProductID  int    `json:"product_id"`
	Source     string `json:"source"` // "redis" atau "database"
	Processing int64  `json:"processing_time_ms"`
	At         int64  `json:"at"`
}

const recentLimit = 60

// New membuat Server dan menyiapkan router. ctx dipakai untuk operasi yang
// berjalan setelah response dikirim (misalnya publish ke queue).
func New(ctx context.Context, deps Dependencies) *Server {
	if deps.Config.QueueName == "" {
		deps.Config.QueueName = "orders_queue"
	}
	if deps.Config.CacheTTL <= 0 {
		deps.Config.CacheTTL = 15 * time.Second
	}
	if deps.Started.IsZero() {
		deps.Started = time.Now()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &Server{
		cfg:     deps.Config,
		cache:   deps.Cache,
		db:      deps.DB,
		hub:     deps.Hub,
		orders:  deps.Orders,
		broker:  deps.Broker,
		stats:   deps.Stats,
		started: deps.Started,
		baseCtx: ctx,
	}
}

// Routes mengembalikan handler lengkap dengan middleware CORS dan logging.
func (s *Server) Routes(staticDir string) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/products", s.handleProducts)
	mux.HandleFunc("GET /api/product/{id}", s.handleProduct)
	mux.HandleFunc("DELETE /api/product/{id}/cache", s.handleInvalidate)
	mux.HandleFunc("POST /api/order", s.handleOrder)
	mux.HandleFunc("GET /api/orders", s.handleOrders)
	mux.HandleFunc("GET /api/orders/{id}", s.handleOrderByID)
	mux.HandleFunc("GET /api/events", s.handleSSE)
	mux.HandleFunc("GET /api/stats", s.handleStats)
	mux.HandleFunc("POST /api/reset", s.handleReset)

	// Frontend statis. Diletakkan paling akhir (pola "/") supaya tidak menutupi /api.
	if staticDir != "" {
		fs := http.FileServer(http.Dir(staticDir))
		mux.Handle("GET /", noCache(fs))
	}

	return withLogging(withCORS(mux))
}

// --- Redis demo -------------------------------------------------------------

// productResponse adalah bentuk JSON GET /api/product/:id.
type productResponse struct {
	Product    store.Product `json:"product"`
	Source     string        `json:"source"`             // "database" | "redis"
	Cache      string        `json:"cache"`              // "redis" | "memory"
	Processing int64         `json:"processing_time_ms"` // waktu total handler
	DBTime     int64         `json:"db_time_ms"`         // waktu bagian query DB
	CacheTTL   int           `json:"cache_ttl_s"`        // sisa umur cache
	At         int64         `json:"at"`
}

// handleProduct menerapkan pola cache-aside:
//
//  1. cek cache         -> hit  : balas segera
//  2. cache miss + DB   -> ambil dari database (lambat), simpan ke cache
//  3. balas dengan metadata supaya frontend bisa membandingkan
func (s *Server) handleProduct(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id produk harus angka"})
		return
	}

	// Langkah 1: cek cache lebih dulu.
	cached, err := s.cache.Get(r.Context(), store.ProductKey(id))
	if err == nil {
		p, decErr := store.UnmarshalProduct(cached)
		if decErr == nil {
			s.cacheHits.Add(1)
			proc := time.Since(start).Milliseconds()
			ttl := s.remainingTTL(r.Context(), id)
			s.recordHit(hitRecord{ProductID: id, Source: "redis", Processing: proc, At: start.UnixMilli()})
			s.hub.Publish(events.TypeCache, map[string]any{
				"product_id": id, "source": "redis", "processing_time_ms": proc,
			})
			writeJSON(w, http.StatusOK, productResponse{
				Product: p, Source: "redis", Cache: s.cache.Backend(),
				Processing: proc, CacheTTL: ttl, At: start.UnixMilli(),
			})
			return
		}
		// Data rusak di cache: diperlakukan sebagai miss dan dihapus.
		_ = s.cache.Del(r.Context(), store.ProductKey(id))
	}

	// Langkah 2: cache miss, pukul "database" yang lambat.
	s.cacheMisses.Add(1)
	dbStart := time.Now()
	p, ok := s.db.Get(id)
	dbTime := time.Since(dbStart).Milliseconds()
	s.dbQueries.Add(1)

	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "produk tidak ditemukan"})
		return
	}

	// Tambahan jeda supaya total bagian database selalu sekitar DBLatency,
	// berapa pun waktu yang sudah dihabiskan di dalam store.
	dbTime = s.simulateSlowQuery(dbTime)

	// Langkah 3: isi cache supaya request berikutnya cepat. TTL disimpan sebagai
	// key pendamping (ttl:product:N) karena GET Redis tidak mengembalikan TTL.
	if body, mErr := store.MarshalProduct(p); mErr == nil {
		if err := s.cache.Set(r.Context(), store.ProductKey(id), body, s.cfg.CacheTTL); err == nil {
			_ = s.cache.Set(r.Context(), store.TTLKey(id),
				[]byte(strconv.Itoa(int(s.cfg.CacheTTL.Seconds()))), s.cfg.CacheTTL)
		}
	}

	proc := time.Since(start).Milliseconds()
	s.recordHit(hitRecord{ProductID: id, Source: "database", Processing: proc, At: start.UnixMilli()})
	s.hub.Publish(events.TypeCache, map[string]any{
		"product_id": id, "source": "database", "processing_time_ms": proc,
	})

	writeJSON(w, http.StatusOK, productResponse{
		Product: p, Source: "database", Cache: s.cache.Backend(),
		Processing: proc, DBTime: dbTime, CacheTTL: int(s.cfg.CacheTTL.Seconds()),
		At: start.UnixMilli(),
	})
}

// simulateSlowQuery menambahkan jeda buatan, meniru query berat ke database
// sungguhan. Waktu akses yang sudah terpakai dihitung, jadi kalau store sudah
// lambat sendiri handler tidak menidurkan dirinya dua kali.
// Mengembalikan total waktu bagian ini dalam milidetik.
func (s *Server) simulateSlowQuery(alreadyMS int64) int64 {
	want := s.cfg.DBLatency.Milliseconds()
	if want <= alreadyMS {
		return alreadyMS
	}
	time.Sleep(time.Duration(want-alreadyMS) * time.Millisecond)
	return want
}

// remainingTTL mengembalikan sisa umur cache dalam detik. Redis menyimpan TTL
// di metadata key, bukan di value, jadi kita perlu key pendamping.
func (s *Server) remainingTTL(ctx context.Context, id int) int {
	raw, err := s.cache.Get(ctx, store.TTLKey(id))
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(string(raw))
	if n < 0 {
		return 0
	}
	return n
}

func (s *Server) recordHit(h hitRecord) {
	s.recentMu.Lock()
	s.recentHits = append(s.recentHits, h)
	if len(s.recentHits) > recentLimit {
		s.recentHits = s.recentHits[len(s.recentHits)-recentLimit:]
	}
	s.recentMu.Unlock()
}

// handleInvalidate menghapus produk dari cache — tombol "kosongkan cache".
func (s *Server) handleInvalidate(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id produk harus angka"})
		return
	}
	if err := s.cache.Del(r.Context(), store.ProductKey(id), store.TTLKey(id)); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.hub.Publish(events.TypeCache, map[string]any{"product_id": id, "source": "invalidated"})
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "product_id": id})
}

// handleProducts mengembalikan seluruh katalog beserta stok terkini.
func (s *Server) handleProducts(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"products": s.db.All()})
}

// --- RabbitMQ demo ----------------------------------------------------------

// orderRequest adalah body POST /api/order.
type orderRequest struct {
	ProductID int `json:"product_id"`
	Quantity  int `json:"quantity"`
}

// handleOrder sengaja TIDAK memproses order. Handler hanya:
//
//  1. validasi stok (query cepat, supaya user dapat feedback awal)
//  2. publish pesan ke queue
//  3. balas 202 Accepted dengan order_id
//
// Semua kerja berat (potong stok, update status) dilakukan worker.
func (s *Server) handleOrder(w http.ResponseWriter, r *http.Request) {
	var req orderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body JSON tidak valid"})
		return
	}
	if req.ProductID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "product_id wajib diisi"})
		return
	}
	if req.Quantity <= 0 {
		req.Quantity = 1
	}

	p, ok := s.db.Get(req.ProductID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "produk tidak ditemukan"})
		return
	}
	if p.Stock < req.Quantity {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "stok tidak cukup", "stock": p.Stock,
		})
		return
	}

	now := time.Now()
	orderID := orders.NewID(s.orderSeq.Add(1))
	msg := orders.Message{
		OrderID:   orderID,
		ProductID: p.ID,
		Qty:       req.Quantity,
		TotalIDR:  p.PriceIDR * req.Quantity,
		CreatedAt: now,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Status awal disimpan SEBELUM publish, supaya event "consumed" dari worker
	// tidak pernah mendahului record yang belum ada.
	s.orders.Create(orders.Order{
		ID:          orderID,
		ProductID:   p.ID,
		ProductName: p.Name,
		Qty:         req.Quantity,
		TotalIDR:    msg.TotalIDR,
		Status:      orders.StatusQueued,
		CreatedAt:   now,
		QueuedAt:    now,
	})

	// Event "queued" dipancarkan SEBELUM publish. Kalau setelahnya, worker yang
	// cepat bisa sudah mengirim "consumed" lebih dulu dan animasi pipeline di
	// frontend akan melompat dari Broker ke Consumer tanpa melewati Producer.
	s.hub.Publish(events.TypeOrder, map[string]any{
		"order_id":     orderID,
		"product_name": p.Name,
		"qty":          req.Quantity,
		"total_idr":    msg.TotalIDR,
		"status":       orders.StatusQueued,
		"stage":        "producer",
		"note":         "pesan dikirim ke " + s.cfg.QueueName + " (" + s.broker.Backend() + ")",
	})

	if err := s.broker.Publish(s.baseCtx, s.cfg.QueueName, body); err != nil {
		s.orders.Update(orderID, func(o *orders.Order) {
			o.Status = orders.StatusFailed
			o.Error = "gagal publish ke queue: " + err.Error()
		})
		s.hub.Publish(events.TypeOrder, map[string]any{
			"order_id": orderID, "status": orders.StatusFailed, "stage": "broker",
			"note": "publish gagal: " + err.Error(),
		})
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "broker tidak tersedia"})
		return
	}
	s.published.Add(1)

	// Depth dibaca setelah publish supaya angkanya mencerminkan pesan ini.
	depth, _ := s.broker.QueueDepth(s.baseCtx, s.cfg.QueueName)
	s.hub.Publish(events.TypeQueue, map[string]any{"queue_size": depth})

	// 202 = sudah diterima, belum diproses. Ini inti asynchronous processing.
	writeJSON(w, http.StatusAccepted, map[string]any{
		"order_id": orderID,
		"status":   orders.StatusQueued,
		"queue":    s.cfg.QueueName,
		"broker":   s.broker.Backend(),
		"note":     "order diterima. Pemrosesan berjalan di background worker.",
	})
}

// handleOrders mengembalikan order terbaru untuk tabel di UI.
func (s *Server) handleOrders(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"orders": s.orders.Recent(20)})
}

// handleOrderByID dipakai untuk polling status satu order.
func (s *Server) handleOrderByID(w http.ResponseWriter, r *http.Request) {
	o, ok := s.orders.Get(r.PathValue("id"))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "order tidak ditemukan"})
		return
	}
	writeJSON(w, http.StatusOK, o)
}

// --- Streaming & status -----------------------------------------------------

// handleSSE membuka stream text/event-stream. Setiap event dari hub dikirim
// sebagai blok "event: <type>\ndata: <json>\n\n". Heartbeat tiap 20 detik
// mencegah proxy memutus koneksi yang diam.
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming tidak didukung", http.StatusInternalServerError)
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	ch := s.hub.Subscribe()
	defer s.hub.Unsubscribe(ch)

	// Salam pertama: kirim daftar order terbaru supaya UI langsung sinkron.
	initial, _ := json.Marshal(map[string]any{
		"orders":      s.orders.Recent(20),
		"queue_size":  s.queueDepth(r.Context()),
		"queue":       s.cfg.QueueName,
		"broker":      s.broker.Backend(),
		"cache":       s.cache.Backend(),
		"subscribers": s.hub.Subscribers(),
	})
	writeSSE(w, "connected", initial)
	flusher.Flush()

	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			_, _ = w.Write([]byte(": ping\n\n"))
			flusher.Flush()
		case ev, open := <-ch:
			if !open {
				return
			}
			payload, err := json.Marshal(ev.Data)
			if err != nil {
				continue
			}
			writeSSE(w, string(ev.Type), payload)
			flusher.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, event string, data []byte) {
	// data JSON tidak mengandung newline ganda, jadi aman ditulis satu baris.
	_, _ = w.Write([]byte("event: " + event + "\ndata: " + string(data) + "\n\n"))
}

// handleStats mengembalikan counter untuk panel statistik.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	s.recentMu.Lock()
	recent := make([]hitRecord, len(s.recentHits))
	copy(recent, s.recentHits)
	s.recentMu.Unlock()

	hits, misses := s.cacheHits.Load(), s.cacheMisses.Load()
	var ratio float64
	if total := hits + misses; total > 0 {
		ratio = float64(hits) / float64(total) * 100
	}

	byStatus := s.orders.CountByStatus()
	processed, failed := int64(0), int64(0)
	if s.stats.Processed != nil {
		processed = s.stats.Processed()
	}
	if s.stats.Failed != nil {
		failed = s.stats.Failed()
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"cache": map[string]any{
			"hits":      hits,
			"misses":    misses,
			"hit_ratio": ratio,
			"backend":   s.cache.Backend(),
			"ttl_s":     int(s.cfg.CacheTTL.Seconds()),
			"recent":    recent,
		},
		"orders": map[string]any{
			"published":  s.published.Load(),
			"processed":  processed,
			"failed":     failed,
			"by_status":  byStatus,
			"queue_size": s.queueDepth(r.Context()),
			"queue":      s.cfg.QueueName,
			"broker":     s.broker.Backend(),
		},
		"uptime_s": int(time.Since(s.started).Seconds()),
	})
}

func (s *Server) queueDepth(ctx context.Context) int {
	n, err := s.broker.QueueDepth(ctx, s.cfg.QueueName)
	if err != nil {
		return 0
	}
	return n
}

// handleReset mengembalikan keadaan demo ke awal: semua cache dihapus, semua
// order baru ditandai selesai. Stok tidak direset supaya perilaku "stok habis"
// tetap bisa dilihat.
func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	for _, p := range s.db.All() {
		_ = s.cache.Del(r.Context(), store.ProductKey(p.ID), store.TTLKey(p.ID))
	}
	s.cacheHits.Store(0)
	s.cacheMisses.Store(0)
	s.dbQueries.Store(0)
	s.published.Store(0)
	s.recentMu.Lock()
	s.recentHits = nil
	s.recentMu.Unlock()
	s.hub.Publish(events.TypeStats, map[string]any{"reset": true})
	writeJSON(w, http.StatusOK, map[string]bool{"reset": true})
}

// handleHealth dipakai docker healthcheck.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	cacheErr := s.cache.Ping(r.Context())
	status := "ok"
	if cacheErr != nil {
		status = "degraded"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":             status,
		"cache":              s.cache.Backend(),
		"broker":             s.broker.Backend(),
		"orders_queue_depth": s.queueDepth(r.Context()),
		"uptime_s":           int(time.Since(s.started).Seconds()),
	})
}

// --- helper -----------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
		slog.Debug("gagal menulis response", "err", err)
	}
}

// withCORS membuka akses dari localhost mana pun — kelas biasanya membuka
// index.html lewat file:// atau port Live Server.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && (strings.HasPrefix(origin, "http://localhost") || strings.HasPrefix(origin, "http://127.0.0.1") || origin == "null") {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// withLogging mencatat durasi tiap request; berguna saat demo error di kelas.
func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		slog.Info("http", "method", r.Method, "path", r.URL.Path,
			"dur_ms", time.Since(start).Milliseconds())
	})
}

// noCache menambahkan header supaya browser tidak menyajikan index.html lama.
func noCache(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".html") || r.URL.Path == "/" {
			w.Header().Set("Cache-Control", "no-store")
		}
		h.ServeHTTP(w, r)
	})
}

var _ bus.Message // dokumentasi: Publisher & Consumer memakai tipe bus.Message
