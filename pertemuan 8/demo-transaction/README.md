# Demo Transaksi & Quality — Modul Backend #8

Studi kasus yang membuktikan **bahayanya operasi database tanpa transaksi**, lalu
menunjukkan versi amannya. Semua bisa dijalankan langsung di komputer masing-masing.

Stack: Go + PostgreSQL (docker) + `database/sql` + `testify`/`testify/mock`.

## Yang dibuktikan

| Skenario | Tanpa transaksi | Dengan transaksi |
|---|---|---|
| Transfer | uang pengirim terpotong, penerima tidak menerima (uang hilang) | rollback total, saldo utuh |
| Voting 1000 klik | lost update, hanya sebagian vote tercatat | 1 klik = 1 vote |
| Klik "Bayar" 2x | saldo terpotong 2x | request kedua diabaikan (idempoten) |

## Struktur

```
demo-transaction/
├── docker-compose.yml        # PostgreSQL 16, port host 55432
├── schema.sql                # accounts (CHECK limit), contests, ledger (idempotency)
├── cmd/demo/main.go          # runner 4 skenario + output pembuktian
├── visualizer.html           # visualizer animasi (buka di browser)
└── internal/
    ├── wallet/               # logic murni: unsafe vs safe
    │   ├── wallet.go         # interface Store & Tx (titik mocking)
    │   ├── transfer.go       # TransferUnsafe / TransferSafe
    │   ├── vote.go           # VoteUnsafe / VoteSafe
    │   ├── payment.go        # PayUnsafe / PaySafe (idempotency)
    │   └── wallet_test.go    # unit test + behavioral verification
    └── store/store.go        # implementasi PostgreSQL (BEGIN/COMMIT/ROLLBACK)
```

Ada dua cara memakai demo ini: **visualizer HTML** (tanpa install apa pun, buat presentasi di kelas) atau **program Go** (bukti angka asli dari PostgreSQL).

## Visualizer HTML (tanpa setup)

Buka `visualizer.html` di browser. Tiga skenario, tiap langkah muncul satu per satu dengan jeda yang bisa diatur. Ada tombol **Lanjut** untuk maju manual saat presentasi. Tidak perlu Docker, Go, atau internet.

## Cara menjalankan program Go

Pastikan Docker dan Go sudah terpasang.

```bash
# 1. nyalakan database
docker compose up -d

# 2. jalankan demo
go run ./cmd/demo
```

Contoh output (bagian inti):

```
[A] TRANSFER TANPA TRANSAKSI
Saldo akhir:  Siti = 50000   Budi = 900000
TOTAL UANG  :  sebelum = 1900000   sesudah = 950000
>>> UANG HILANG: 950000

[B] TRANSFER DENGAN TRANSAKSI
Saldo akhir:  Siti = 1000000   Budi = 900000
>>> ROLLBACK: tidak ada uang hilang.

[C] VOTING 1000 KLIK
Tanpa locking : tercatat 40 vote   (HILANG 960)
Dengan locking: tercatat 1000 vote (hilang 0)

[D] DOUBLE PAYMENT
Tanpa idempotency: saldo = 40000
Dengan idempotency: saldo = 70000  (request kedua ditolak)
```

## Menjalankan test & coverage

```bash
go test -v ./...                              # semua unit test
go test -coverprofile=coverage.out ./...      # generate coverage
go tool cover -func=coverage.out              # ringkasan per fungsi
go tool cover -html=coverage.out              # laporan visual
```

Contoh ringkasan coverage:

```
internal/wallet/transfer.go:TransferSafe    79.2%
internal/wallet/payment.go:PaySafe          74.1%
internal/wallet/vote.go:VoteSafe            68.8%
total: (statements)                          ~70%
```

## Poin ngajar untuk meet 8

1. **Atomicity itu nyata.** `TransferUnsafe` sengaja dibuat gagal di langkah kedua.
   Karena langkah pertama sudah commit, uang benar-benar hilang — bukan teori.
2. **Transaksi = unit kerja.** `TransferSafe` membungkus semua query, dan rollback
   otomatis lewat `committed` flag + `defer`.
3. **Transaksi saja tidak cukup untuk concurrency.** `VoteSafe` butuh
   `SELECT ... FOR UPDATE`; tanpa lock, dua transaksi tetap baca nilai yang sama.
   Ini jembatan ke Training 10 (Race Condition & Locking).
4. **Idempotency mencegah double charge.** Klik ganda ditangani lewat key unik +
   `ON CONFLICT DO NOTHING`, bukan validasi di sisi klien.
5. **Behavioral verification.** Test membuktikan *perilaku* (Commit vs Rollback),
   bukan cuma output — persis pesan modul. Mock bikin test cepat & repeatable.

## Catatan

- `TransferUnsafe`/`VoteUnsafe`/`PayUnsafe` **hanya untuk demonstrasi**. Jangan
  dipakai sebagai contoh produksi.
- Limit saldo (1.000.000) ada di `schema.sql` sebagai representasi aturan
  Consistency; skenario A memanfaatkannya untuk memaksa kegagalan di tengah alur.
- Ganti password/port di `docker-compose.yml` dan `DATABASE_URL` kalau bentrok.