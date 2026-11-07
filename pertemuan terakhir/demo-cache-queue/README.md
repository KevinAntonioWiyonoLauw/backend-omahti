# Demo Cache &amp; Queue — Modul Backend Terakhir

Satu dashboard yang memperagakan dua konsep yang paling sering ditanya di kelas
backend: **cache dengan Redis** dan **pemrosesan asynchronous dengan RabbitMQ**.

Semua angka di layar berasal dari backend sungguhan, bukan animasi palsu.
Animasi pipeline digerakkan event SSE dari server.

Stack: Go 1.24 (`net/http`), `redis/go-redis/v9`, `rabbitmq/amqp091-go`,
frontend HTML/CSS/JS tanpa build step.

## Yang dibuktikan

| Demo | Tanpa optimasi | Dengan optimasi |
|---|---|---|
| Baca detail produk | ~2.000 ms setiap request (query berat) | ~2 ms setelah cache terisi (TTL 15 s) |
| Checkout / order | request menunggu 3 detik pemrosesan | balas `202 Accepted` dalam <10 ms, worker memproses di belakang |

## Cara menjalankan

Butuh Docker Desktop. Satu perintah:

```bash
cd "pertemuan terakhir/demo-cache-queue"
docker compose up --build
```

Lalu buka:

- Dashboard: <http://localhost:18000>
- RabbitMQ Management: <http://localhost:25673> — user `guest`, password `guest`
- Health check: <http://localhost:18000/api/health>

Mematikan: `docker compose down` (tambahkan `-v` kalau ingin bersih total).

### Peta port

Semua port host sengaja di rentang belasan ribu supaya tidak bentrok dengan
Redis/RabbitMQ/web server yang biasanya sudah jalan di laptop.

| Service | Port host | Port di container |
|---|---|---|
| Dashboard / backend | `18000` | `8000` |
| Redis | `16380` | `6379` |
| RabbitMQ AMQP | `15673` | `5672` |
| RabbitMQ Management | `25673` | `15672` |

Nilainya disimpan di file `.env` (sudah ada di folder ini), jadi bisa diubah
tanpa menyentuh `docker-compose.yml`:

```bash
BACKEND_HOST_PORT=18000
REDIS_HOST_PORT=16380
AMQP_HOST_PORT=15673
MGMT_HOST_PORT=25673
```

Setelah mengubah `.env`, jalankan ulang `docker compose up -d` dan buka
<http://localhost:18000>. Backend tetap memakai port internal 8000; hanya port
di host yang digeser.

### Tanpa Docker (mode demo)

Backend bisa jalan sendiri dengan cache in-memory dan broker in-memory.
Alurnya identik, hanya backend penyimpanannya yang berbeda.

```bash
go run ./cmd/server            # MODE=demo adalah default, listen di :18000
```

Buka <http://localhost:18000>. Di mode ini badge `cache`/`broker` akan
menampilkan `memory` / `in-memory (mode demo)`. Ini sekaligus contoh pola
**fallback** yang wajar dipakai di aplikasi nyata.

### Menjalankan tanpa Docker tetapi dengan Redis/RabbitMQ asli

```bash
docker compose up -d redis rabbitmq
MODE=strict go run ./cmd/server
```

## Skenario presentasi di kelas (10 menit)

**Bagian 1 — Redis (panel kiri)**

1. Pilih produk, tekan **Fetch produk**. Perhatikan: badge kuning
   `CACHE MISS · database`, waktu ~2000 ms, bar grafik tinggi.
2. Tekan **Fetch produk** sekali lagi. Badge jadi hijau `CACHE HIT · redis`,
   waktu ~2 ms. Grafik langsung memperlihatkan jurangnya.
3. Tunggu 15 detik sampai TTL habis (atau tekan **Hapus dari cache**), lalu
   fetch lagi. Kembali lambat. Ini yang disebut *cache invalidation*.

Poin yang ditekankan: cache tidak mengubah hasil query, hanya memindahkan biaya
baca dari database ke memori. Yang harus dipikirkan adalah TTL dan kapan cache
harus dibuang saat data berubah.

**Bagian 2 — RabbitMQ (panel kanan)**

1. Tekan **Place order**. Request langsung selesai (`202 Accepted`), stage
   Producer menyala, lalu pesan masuk ke Broker.
2. Amati stage Consumer menyala 3 detik, lalu Completed.
3. Tekan **Burst 5** dan lihat angka `queue depth` naik dulu sebelum turun satu
   per satu. Ini antrean yang sesungguhnya.
4. Buka RabbitMQ Management, lihat queue `orders_queue` saat burst berjalan.

Poin yang ditekankan: request tidak menunggu pekerjaan selesai. Queue memberi
ketahanan (pesan tidak hilang saat consumer mati) dan penskalaan horizontal
(tinggal naikkan `WORKERS`).

### Naikkan jumlah worker

```bash
WORKERS=3 docker compose up -d --force-recreate backend
```

Dengan prefetch 1, RabbitMQ membagi pesan ke worker yang menganggur. Burst 5
order akan selesai jauh lebih cepat, dan log menunjukkan pesan dikerjakan
goroutine yang berbeda.

## Endpoint

| Method | Path | Keterangan |
|---|---|---|
| GET | `/api/product/{id}` | Cache-aside. Metadata: `source`, `processing_time_ms`, `db_time_ms`, `cache_ttl_s` |
| DELETE | `/api/product/{id}/cache` | Hapus key produk dari cache |
| POST | `/api/order` | Publish ke `orders_queue`, balas `202` + `order_id` |
| GET | `/api/orders` | 20 order terbaru beserta status |
| GET | `/api/order/{id}` | Status satu order |
| GET | `/api/events` | SSE: `connected`, `cache`, `order`, `queue` |
| GET | `/api/stats` | Counter cache hit/miss, queue depth, order per status |
| POST | `/api/reset` | Kosongkan cache dan reset counter |
| GET | `/api/health` | Status Redis &amp; broker, dipakai docker healthcheck |

Contoh cepat:

```bash
curl -s localhost:18000/api/product/1
# {"product":{...},"source":"database","processing_time_ms":2001,...}

curl -s localhost:18000/api/product/1
# {"product":{...},"source":"redis","processing_time_ms":1,...}

curl -s -X POST localhost:18000/api/order -H 'Content-Type: application/json' \
     -d '{"product_id":5,"quantity":1}'
# {"order_id":"ORD-000001","status":"queued","queue":"orders_queue",...}
```

## Struktur

```
demo-cache-queue/
├── docker-compose.yml        # Redis, RabbitMQ (+UI), Backend
├── Dockerfile                # build multi-stage, image akhir kecil
├── go.mod
├── cmd/server/main.go        # wiring: cache → broker → worker → HTTP
├── web/index.html            # dashboard (tanpa build step)
└── internal/
    ├── api/                  # handler HTTP + SSE + middleware
    │   └── api_test.go       # bukti miss→hit dan async order
    ├── bus/                  # kontrak Message/Broker
    ├── broker/
    │   ├── rabbitmq.go       # implementasi amqp091-go
    │   ├── memory.go         # implementasi in-memory untuk mode demo
    │   └── memory_test.go    # semantik ack/nack
    ├── events/               # hub SSE (broadcast non-blocking)
    ├── orders/               # model & status order
    ├── store/
    │   ├── store.go          # "tabel products" + decrement stok atomik
    │   ├── cache.go          # interface Cache + fallback in-memory
    │   └── redis.go          # implementasi go-redis/v9
    └── worker/worker.go      # consumer orders_queue
```

## Bagian kode yang layak dibaca di kelas

**Cache-aside** — `internal/api/api.go`, fungsi `handleProduct`.
Urutannya: cek cache → miss → query database → isi cache dengan TTL.

**Handler yang sadar asynchronous** — `handleOrder`.
Handler hanya memvalidasi stok, menyimpan status `queued`, publish pesan,
lalu `202 Accepted`. Tidak ada pekerjaan berat di request.

**Manual ack** — `internal/worker/worker.go`, fungsi `handle`.
Pesan baru di-ack setelah status order diperbarui. Kalau worker mati di tengah
jalan, pesan kembali ke queue dan dikerjakan ulang (`redelivered`).
Itu sebabnya RabbitMQ tidak boleh di-ack otomatis untuk pekerjaan yang punya
efek samping seperti potong stok.

**Decrement stok atomik** — `internal/store/store.go`.
Dua worker tidak boleh menjual unit terakhir dua kali. Perhatikan write lock.

**Urutan ack vs event** — `internal/worker/worker.go` dan `internal/broker/memory.go`.
Pesan baru dilepas dari queue setelah status diperbarui. Bug klasik yang sengaja
ditutup di sini: menyamakan ack dengan `nack(true)`. Keduanya "benar" secara
boolean, tetapi artinya berlawanan — satu menghapus pesan, satu mengirim ulang.
Karena itu keputusan ack/nack memakai tipe sendiri (`decision`), bukan `bool`.
Ada tesnya di `internal/broker/memory_test.go`.

## Konfigurasi (environment variable)

| Variabel | Default | Keterangan |
|---|---|---|
| `ADDR` | `:18000` | alamat listen |
| `MODE` | `demo` | `demo`: fallback in-memory; `strict`: gagal start kalau Redis/RabbitMQ mati |
| `REDIS_URL` | `redis://localhost:16380/0` | koneksi Redis |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:15673/` | koneksi AMQP |
| `QUEUE_NAME` | `orders_queue` | nama queue |
| `CACHE_TTL` | `15s` | umur cache produk (pakai `1m`, `500ms`, dst.) |
| `DB_LATENCY` | `2s` | simulasi query database lambat |
| `WORKERS` | `1` | jumlah consumer goroutine |
| `STATIC_DIR` | `./web` | folder frontend |

## Tes

```bash
go test ./... -v
```

Tes `TestCacheAsideMissThenHit` membuktikan fetch kedua lebih cepat dari yang
pertama; `TestOrderIsAsyncThenProcessed` membuktikan `202` datang duluan dan
status berubah menjadi `processed` lewat event.
`internal/broker/memory_test.go` mengunci semantik ack/nack: pesan yang sudah
di-ack tidak boleh muncul lagi.

## Masalah yang sering muncul

| Gejala | Penyebab | Solusi |
|---|---|---|
| `broker tidak tersedia` saat Place order | RabbitMQ belum siap | `docker compose ps`, tunggu healthcheck hijau |
| Badge cache menampilkan `memory` padahal pakai Docker | Redis gagal connect, backend jatuh ke fallback | cek `docker compose logs backend` |
| Fetch selalu lambat | TTL 15 detik keburu habis, atau ada tab lain yang melakukan **Reset** | fetch dua kali berturut-turut |
| Order macet di `queued` | consumer mati | `docker compose logs backend` |
| Port 18000/16380/15673/25673 dipakai | konflik dengan service lain | ubah nilai di `.env` lalu `docker compose up -d` |
