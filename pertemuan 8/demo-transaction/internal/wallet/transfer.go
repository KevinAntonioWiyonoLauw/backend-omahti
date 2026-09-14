package wallet

import (
	"context"
	"errors"
	"fmt"
)

// TransferUnsafe adalah VERSI BURUK: tidak ada transaksi.
//
// Setiap langkah langsung tersimpan ke database (autocommit). Kalau langkah kedua
// gagal — di sini disimulasikan lewat saldo penerima yang melewati CHECK limit —
// maka langkah pertama sudah kadung commit dan tidak bisa dibatalkan.
//
// Hasilnya: uang pengirim berkurang, tapi penerima tidak menerima apa-apa.
// Uang raib. Ini contoh paling telak kenapa Atomicity itu wajib.
func (s *Service) TransferUnsafe(ctx context.Context, fromID, toID, amount int64) error {
	if amount <= 0 {
		return errors.New("amount harus > 0")
	}

	sender, err := s.store.GetBalance(ctx, fromID)
	if err != nil {
		return fmt.Errorf("baca saldo pengirim: %w", err)
	}
	if sender < amount {
		return ErrInsufficientFunds
	}
	if err := s.store.SetBalance(ctx, fromID, sender-amount); err != nil {
		return fmt.Errorf("potong saldo pengirim: %w", err)
	}

	receiver, err := s.store.GetBalance(ctx, toID)
	if err != nil {
		return fmt.Errorf("baca saldo penerima: %w", err)
	}
	// Langkah ini yang sengaja dibuat bisa gagal. Karena langkah sebelumnya sudah
	// commit, kegagalan di sini tidak me-rollback apa pun.
	if err := s.store.SetBalance(ctx, toID, receiver+amount); err != nil {
		return fmt.Errorf("tambah saldo penerima: %w", err)
	}
	return nil
}

// TransferSafe adalah VERSI BENAR: satu transaksi, all-or-nothing.
//
// defer Rollback adalah jaring pengaman: kalau ada error/panic sebelum Commit,
// seluruh perubahan dibatalkan. Kalau Commit sudah jalan, Rollback berikutnya
// tidak berpengaruh (idempoten).
func (s *Service) TransferSafe(ctx context.Context, fromID, toID, amount int64) error {
	if amount <= 0 {
		return errors.New("amount harus > 0")
	}

	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		// Jaring pengaman: hanya rollback kalau belum commit. Pola ini membuat
		// bisa dibuktikan lewat test bahwa pada sukses Commit jalan & Rollback tidak.
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	sender, err := tx.GetBalance(ctx, fromID)
	if err != nil {
		return fmt.Errorf("baca saldo pengirim: %w", err)
	}
	if sender < amount {
		return ErrInsufficientFunds
	}
	if err := tx.AddBalance(ctx, fromID, -amount); err != nil {
		return fmt.Errorf("potong saldo pengirim: %w", err)
	}

	if _, err := tx.GetBalance(ctx, toID); err != nil {
		return fmt.Errorf("baca saldo penerima: %w", err)
	}
	if err := tx.AddBalance(ctx, toID, amount); err != nil {
		return fmt.Errorf("tambah saldo penerima: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	return nil
}