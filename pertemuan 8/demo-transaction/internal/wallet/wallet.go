// Package wallet berisi logic transfer, voting, dan pembayaran untuk demo
// Transaksi & Quality (Modul Backend #8).
//
// Inti pelajarannya ada di dua jalur yang berdampingan:
//   - *Unsafe : setiap query langsung commit (autocommit). Tidak ada unit kerja.
//   - *Safe   : semua query dibungkus satu transaksi, Commit di akhir,
//     Rollback otomatis lewat defer kalau ada yang gagal.
package wallet

import (
	"context"
	"errors"
)

var (
	// ErrInsufficientFunds dipakai saat saldo tidak cukup.
	ErrInsufficientFunds = errors.New("saldo tidak cukup")

	// ErrDuplicate dipakai saat idempotency key sudah pernah diproses.
	ErrDuplicate = errors.New("request duplikat: idempotency key sudah dipakai")

	// ErrExternal mensimulasikan kegagalan pihak ketiga / server crash di tengah alur.
	ErrExternal = errors.New("downstream gagal")
)

// Tx adalah satu unit kerja. Semua operasi di dalamnya berbagi transaksi yang sama.
type Tx interface {
	GetBalance(ctx context.Context, accountID int64) (int64, error)
	AddBalance(ctx context.Context, accountID int64, delta int64) error

	// GetVotesLocked membaca counter dengan SELECT ... FOR UPDATE, sehingga baris
	// terkunci sampai Commit/Rollback. Ini yang mencegah lost update.
	GetVotesLocked(ctx context.Context, contestID int64) (int64, error)
	SetVotes(ctx context.Context, contestID int64, value int64) error

	// MarkKey mencatat idempotency key. Return false kalau key sudah ada.
	MarkKey(ctx context.Context, key string, amount int64) (bool, error)

	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Store membuka transaksi dan menyediakan jalur autocommit (tanpa transaksi).
type Store interface {
	Begin(ctx context.Context) (Tx, error)

	// --- jalur autocommit: tiap operasi langsung tersimpan ---
	GetBalance(ctx context.Context, accountID int64) (int64, error)
	SetBalance(ctx context.Context, accountID int64, value int64) error

	// GetVotes/SetVotes adalah read-modify-write di sisi aplikasi (tanpa lock).
	GetVotes(ctx context.Context, contestID int64) (int64, error)
	SetVotes(ctx context.Context, contestID int64, value int64) error
}

// Service berisi semua use case. Bergantung pada interface Store, bukan DB konkret,
// supaya bisa di-mock di unit test.
type Service struct {
	store Store
}

func New(store Store) *Service {
	return &Service{store: store}
}