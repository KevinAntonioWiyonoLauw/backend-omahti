// language: Go, file: internal/store/redis.go
// Implementasi Cache di atas go-redis/v9. Semua operasi membawa context supaya
// bisa dibatasi timeout — kalau Redis lambat, request tidak menggantung selamanya.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache membungkus redis.Client.
type RedisCache struct {
	client *redis.Client
}

// NewRedisCache membuat client dari URL redis://host:port/0 dan mencoba PING.
// Kalau PING gagal, error dikembalikan supaya pemanggil bisa fallback.
func NewRedisCache(ctx context.Context, redisURL string) (*RedisCache, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	opts.DialTimeout = 3 * time.Second
	opts.ReadTimeout = 2 * time.Second
	opts.WriteTimeout = 2 * time.Second

	client := redis.NewClient(opts)

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &RedisCache{client: client}, nil
}

// Get membaca key; redis.Nil diterjemahkan menjadi ErrCacheMiss.
func (c *RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
	body, err := c.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrCacheMiss
	}
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Set menyimpan value dengan TTL; TTL 0 berarti tanpa kedaluwarsa.
func (c *RedisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}

// Del menghapus satu atau beberapa key sekaligus.
func (c *RedisCache) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return c.client.Del(ctx, keys...).Err()
}

// Backend memberi label "redis" untuk metadata response.
func (c *RedisCache) Backend() string { return "redis" }

// Ping memeriksa koneksi; dipakai endpoint /health.
func (c *RedisCache) Ping(ctx context.Context) error { return c.client.Ping(ctx).Err() }

// Client memberi akses langsung untuk keperluan demo, misalnya DEL semua key
// saat tombol "kosongkan cache" ditekan.
func (c *RedisCache) Client() *redis.Client { return c.client }

// Close menutup koneksi pool.
func (c *RedisCache) Close() error { return c.client.Close() }
