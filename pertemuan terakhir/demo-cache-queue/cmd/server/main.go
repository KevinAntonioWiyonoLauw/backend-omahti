// language: Go, file: cmd/server/main.go
// Entry point demo: sambungkan Redis, broker, worker, lalu HTTP server.
// Semua dependensi eksternal punya fallback supaya demo tetap bisa jalan di
// laptop yang belum punya Docker — cukup dengan MODE=demo.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"demo-cache-queue/internal/api"
	"demo-cache-queue/internal/broker"
	"demo-cache-queue/internal/bus"
	"demo-cache-queue/internal/events"
	"demo-cache-queue/internal/orders"
	"demo-cache-queue/internal/store"
	"demo-cache-queue/internal/worker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		addr       = env("ADDR", ":18000")
		mode       = env("MODE", "demo") // demo = mode tahan banting, strict = semua wajib tersedia
		redisURL   = env("REDIS_URL", "redis://localhost:16380/0")
		amqpURL    = env("RABBITMQ_URL", "amqp://guest:guest@localhost:15673/")
		queueName  = env("QUEUE_NAME", "orders_queue")
		staticDir  = env("STATIC_DIR", "./web")
		cacheTTL   = durationEnv("CACHE_TTL", 15*time.Second)
		dbLatency  = durationEnv("DB_LATENCY", 2*time.Second)
		nWorkers   = intEnv("WORKERS", 1)
		strictMode = mode == "strict"
	)

	// 1. Cache: Redis kalau tersedia, kalau tidak pakai in-memory.
	var cache store.Cache = store.NewMemoryCache()
	if rc, err := store.NewRedisCache(ctx, redisURL); err == nil {
		cache = rc
		slog.Info("cache siap", "backend", "redis", "url", redact(redisURL))
		defer rc.Close()
	} else if strictMode {
		slog.Error("redis wajib tersedia di MODE=strict", "err", err)
		os.Exit(1)
	} else {
		slog.Warn("redis tidak tersedia, memakai cache in-memory", "err", err)
	}

	// 2. Broker: RabbitMQ kalau tersedia, kalau tidak broker in-memory.
	var messageBus bus.Broker
	if rq, err := broker.NewRabbitMQ(amqpURL, queueName); err == nil {
		messageBus = rq
		slog.Info("broker siap", "backend", "rabbitmq")
		defer rq.Close()
	} else if strictMode {
		slog.Error("rabbitmq wajib tersedia di MODE=strict", "err", err)
		os.Exit(1)
	} else {
		slog.Warn("rabbitmq tidak tersedia, memakai broker in-memory", "err", err)
		messageBus = broker.NewMemory(1000)
	}

	// 3. Data & state.
	db := store.New(seedProducts())
	hub := events.NewHub()
	orderStore := orders.NewStore(200)

	// 4. Worker pool: satu goroutine per worker, semua mendengarkan queue yang sama.
	pool := worker.StartPool(ctx, nWorkers, func() *worker.Worker {
		return worker.New(hub, orderStore, db, messageBus, queueName)
	})
	slog.Info("worker berjalan", "jumlah", nWorkers, "queue", queueName)

	// 5. HTTP server.
	server := api.New(ctx, api.Dependencies{
		Cache:  cache,
		DB:     db,
		Hub:    hub,
		Orders: orderStore,
		Broker: messageBus,
		Config: api.Config{QueueName: queueName, CacheTTL: cacheTTL, DBLatency: dbLatency},
		Stats: api.StatsReader{
			Processed: pool.Processed,
			Failed:    pool.Failed,
		},
	})

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server.Routes(staticDir),
		ReadHeaderTimeout: 5 * time.Second,
		// Tidak ada WriteTimeout: koneksi SSE hidup terus selama tab terbuka.
	}

	go func() {
		slog.Info("server mendengarkan", "addr", addr, "mode", mode)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server berhenti", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("mematikan server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Warn("shutdown http tidak bersih", "err", err)
	}
	pool.Wait()
	slog.Info("selesai")
}

// seedProducts adalah data awal "tabel products".
func seedProducts() []store.Product {
	return []store.Product{
		{ID: 1, Name: "Mechanical Keyboard 75%", PriceIDR: 899000, Stock: 42,
			Description: "Hot-swap, gasket mount, PBT double-shot. Contoh read berat yang cocok di-cache."},
		{ID: 2, Name: "Wireless Mouse 8K Polling", PriceIDR: 649000, Stock: 87,
			Description: "Sensor 30K DPI, 8000Hz. Produk dengan stok terbanyak."},
		{ID: 3, Name: "Monitor 27\" QHD 180Hz", PriceIDR: 3450000, Stock: 12,
			Description: "IPS QHD 180Hz, HDR400. Harga tinggi, stok menengah."},
		{ID: 4, Name: "USB-C Dock 12-in-1", PriceIDR: 1250000, Stock: 23,
			Description: "PD 100W, dual HDMI, gigabit ethernet. Stok terbatas saat flash sale."},
		{ID: 5, Name: "SSD NVMe 2TB Gen4", PriceIDR: 2150000, Stock: 5,
			Description: "7400MB/s read. Sisa 5 unit — cepat habis kalau di-spam order."},
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func intEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// redact menyembunyikan password kalau ada di URL redis.
func redact(raw string) string {
	if j := strings.Index(raw, "://"); j >= 0 {
		if at := strings.Index(raw[j+3:], "@"); at >= 0 {
			return raw[:j+3] + "***" + raw[j+3+at:]
		}
	}
	return raw
}
