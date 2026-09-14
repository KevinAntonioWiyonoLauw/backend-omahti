// Demo Transaksi & Quality — Modul Backend #8.
//
// Program ini menjalankan empat skenario yang membuktikan bahayanya operasi
// tanpa transaksi:
//
//	A. Transfer tanpa transaksi   -> uang hilang (Atomicity dilanggar)
//	B. Transfer dengan transaksi  -> gagal total, saldo utuh (rollback)
//	C. Voting tanpa lock          -> lost update, suara hilang
//	D. Bayar dobel tanpa idempotency -> saldo terpotong berkali-kali
//
// Jalankan:
//
//	go run ./cmd/demo
//
// Database:
//
//	docker compose up -d
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"demo-transaction/internal/store"
	"demo-transaction/internal/wallet"
)

const (
	budiID  = 1 // penerima
	sitiID  = 2 // pengirim
	contest = 1
)

func main() {
	dsn := envOr("DATABASE_URL", "postgres://omahti:omahti@127.0.0.1:55432/wallet?sslmode=disable")
	voteCount := envInt("VOTE_COUNT", 1000)

	st, err := store.NewPgStore(dsn)
	if err != nil {
		fmt.Println("gagal konek database:", err)
		fmt.Println("jalankan dulu: docker compose up -d")
		os.Exit(1)
	}
	defer st.Close()

	svc := wallet.New(st)
	ctx := context.Background()

	fmt.Println("======================================================")
	fmt.Println(" DEMO TRANSAKSI & ACID — Modul Backend #8")
	fmt.Println("======================================================")

	scenarioA(ctx, st, svc)
	scenarioB(ctx, st, svc)
	scenarioC(ctx, st, svc, voteCount)
	scenarioD(ctx, st, svc)
}

// A. Transfer TANPA transaksi: potong saldo pengirim sukses, tambah saldo
// penerima gagal (melanggar CHECK limit). Langkah pertama sudah commit,
// jadi tidak bisa dibatalkan. Uang hilang.
func scenarioA(ctx context.Context, st *store.PgStore, svc *wallet.Service) {
	fmt.Println("\n[A] TRANSFER TANPA TRANSAKSI  (bakal rugi)")
	fmt.Println("------------------------------------------------------")

	must(st.SetBalance(ctx, sitiID, 1_000_000))
	must(st.SetBalance(ctx, budiID, 900_000))
	fmt.Println("Saldo awal:  Siti = 1.000.000   Budi = 900.000")
	fmt.Println("Transfer    :  Siti -> Budi  sebesar 950.000")
	fmt.Println("               Budi akan jadi 1.850.000, melebihi limit 1.000.000")

	err := svc.TransferUnsafe(ctx, sitiID, budiID, 950_000)
	fmt.Println("\nhasil error :", err)

	siti, _ := st.GetBalance(ctx, sitiID)
	budi, _ := st.GetBalance(ctx, budiID)
	fmt.Printf("Saldo akhir:  Siti = %d   Budi = %d\n", siti, budi)
	fmt.Printf("TOTAL UANG  :  sebelum = 1900000   sesudah = %d\n", siti+budi)
	fmt.Println(">>> UANG HILANG: 1900000 -", siti+budi, "=", 1_900_000-(siti+budi))
	fmt.Println(">>> Saldo Siti terpotong walau Budi tidak menerima apa pun.")
}

// B. Transfer DENGAN transaksi: kegagalan yang sama, tapi seluruh perubahan
// di-rollback. Saldo kembali seperti sebelum transfer.
func scenarioB(ctx context.Context, st *store.PgStore, svc *wallet.Service) {
	fmt.Println("\n[B] TRANSFER DENGAN TRANSAKSI  (aman)")
	fmt.Println("------------------------------------------------------")

	must(st.SetBalance(ctx, sitiID, 1_000_000))
	must(st.SetBalance(ctx, budiID, 900_000))
	fmt.Println("Saldo awal:  Siti = 1.000.000   Budi = 900.000")
	fmt.Println("Transfer    :  Siti -> Budi  sebesar 950.000  (kondisi gagal yang sama)")

	err := svc.TransferSafe(ctx, sitiID, budiID, 950_000)
	fmt.Println("\nhasil error :", err)

	siti, _ := st.GetBalance(ctx, sitiID)
	budi, _ := st.GetBalance(ctx, budiID)
	fmt.Printf("Saldo akhir:  Siti = %d   Budi = %d\n", siti, budi)
	fmt.Printf("TOTAL UANG  :  sebelum = 1900000   sesudah = %d\n", siti+budi)
	fmt.Println(">>> ROLLBACK: tidak ada yang berubah, tidak ada uang hilang.")
}

// C. Voting: 1000 klik bersamaan. Tanpa lock, read-modify-write saling menimpa.
func scenarioC(ctx context.Context, st *store.PgStore, svc *wallet.Service, n int) {
	fmt.Println("\n[C] RACE CONDITION — VOTING", n, "KLIK BERSAMAAN")
	fmt.Println("------------------------------------------------------")

	fmt.Println("(tiap klik dijalankan dari goroutine terpisah; pool koneksi dibatasi 25)")

	must(st.SetVotes(ctx, contest, 0))
	unsafeVotes, unsafeFails := hitVotes(ctx, st, svc, n, false)
	fmt.Printf("Tanpa locking : %d klik -> tercatat %d vote, request gagal %d  (HILANG %d)\n",
		n, unsafeVotes, unsafeFails, int64(n)-unsafeVotes)

	must(st.SetVotes(ctx, contest, 0))
	safeVotes, safeFails := hitVotes(ctx, st, svc, n, true)
	fmt.Printf("Dengan locking: %d klik -> tercatat %d vote, request gagal %d  (hilang %d)\n",
		n, safeVotes, safeFails, int64(n)-safeVotes)
	fmt.Println(">>> Tanpa lock, banyak goroutine membaca nilai lama yang sama lalu saling menimpa.")
	fmt.Println(">>> Dengan SELECT ... FOR UPDATE, tiap kenaikan menunggu lock & pakai nilai terbaru.")
}

// D. Idempotency: user klik "Bayar" dua kali karena jaringan lambat.
func scenarioD(ctx context.Context, st *store.PgStore, svc *wallet.Service) {
	fmt.Println("\n[D] DOUBLE PAYMENT — KLIK 'BAYAR' DUA KALI")
	fmt.Println("------------------------------------------------------")

	// Tanpa idempotency: dua request = dua pemotongan.
	must(st.SetBalance(ctx, budiID, 100_000))
	e1 := svc.PayUnsafe(ctx, budiID, 30_000)
	e2 := svc.PayUnsafe(ctx, budiID, 30_000)
	budi, _ := st.GetBalance(ctx, budiID)
	fmt.Printf("Tanpa idempotency: dua klik -> saldo = %d  (err1=%v err2=%v)\n", budi, e1, e2)

	// Dengan idempotency key yang sama: klik kedua diabaikan.
	must(st.SetBalance(ctx, budiID, 100_000))
	key := "order-12345-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	f1 := svc.PaySafe(ctx, budiID, 30_000, key)
	f2 := svc.PaySafe(ctx, budiID, 30_000, key)
	budi, _ = st.GetBalance(ctx, budiID)
	fmt.Printf("Dengan idempotency: dua klik -> saldo = %d  (err1=%v err2=%v)\n", budi, f1, f2)
	fmt.Println(">>> Key yang sama membuat request kedua mengembalikan ErrDuplicate tanpa potong saldo.")
}

// --- util ---

// hitVotes menjalankan n klik bersamaan, lalu mengembalikan jumlah vote yang
// tercatat dan jumlah request yang error. Memakai pool goroutine terbatas supaya
// tidak menabrak max_connections database.
func hitVotes(ctx context.Context, st *store.PgStore, svc *wallet.Service, n int, safe bool) (votes int64, fails int64) {
	const workers = 25

	var wg sync.WaitGroup
	var failed int64
	start := make(chan struct{})
	jobs := make(chan struct{}, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range jobs {
				var err error
				if safe {
					err = svc.VoteSafe(ctx, contest)
				} else {
					err = svc.VoteUnsafe(ctx, contest)
				}
				if err != nil {
					atomic.AddInt64(&failed, 1)
				}
			}
		}()
	}

	close(start)
	for i := 0; i < n; i++ {
		jobs <- struct{}{}
	}
	close(jobs)
	wg.Wait()

	v, _ := st.GetVotes(ctx, contest)
	return v, failed
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}