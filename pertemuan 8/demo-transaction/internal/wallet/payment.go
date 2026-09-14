package wallet

import (
	"context"
	"errors"
	"fmt"
)

// PayUnsafe adalah VERSI BURUK saat user klik tombol "Bayar" berkali-kali.
//
// Tanpa idempotency key, setiap request adalah transaksi baru. Dua klik = saldo
// terpotong dua kali untuk produk yang sama.
func (s *Service) PayUnsafe(ctx context.Context, accountID, amount int64) error {
	if amount <= 0 {
		return errors.New("amount harus > 0")
	}

	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	balance, err := tx.GetBalance(ctx, accountID)
	if err != nil {
		return fmt.Errorf("baca saldo: %w", err)
	}
	if balance < amount {
		return ErrInsufficientFunds
	}
	if err := tx.AddBalance(ctx, accountID, -amount); err != nil {
		return fmt.Errorf("potong saldo: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	return nil
}

// PaySafe menambahkan idempotency key. Request dengan key yang sama hanya
// diproses sekali; sisanya diabaikan.
//
// Perhatikan urutannya: MarkKey dipanggil DI DALAM transaksi dan hasilnya ikut
// di-commit bersama pemotongan saldo. Jadi kalau pemotongan gagal, key-nya juga
// di-rollback dan user masih bisa mencoba lagi (bukan terkunci permanen).
// Kalau key sudah ada (rows affected = 0), kita kembalikan ErrDuplicate dan
// tidak ada saldo yang berubah.
func (s *Service) PaySafe(ctx context.Context, accountID, amount int64, idempotencyKey string) error {
	if amount <= 0 {
		return errors.New("amount harus > 0")
	}
	if idempotencyKey == "" {
		return errors.New("idempotency key wajib")
	}

	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	inserted, err := tx.MarkKey(ctx, idempotencyKey, amount)
	if err != nil {
		return fmt.Errorf("catat idempotency key: %w", err)
	}
	if !inserted {
		return ErrDuplicate
	}

	balance, err := tx.GetBalance(ctx, accountID)
	if err != nil {
		return fmt.Errorf("baca saldo: %w", err)
	}
	if balance < amount {
		return ErrInsufficientFunds
	}
	if err := tx.AddBalance(ctx, accountID, -amount); err != nil {
		return fmt.Errorf("potong saldo: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	return nil
}