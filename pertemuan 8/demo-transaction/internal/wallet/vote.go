package wallet

import (
	"context"
	"fmt"
)

// VoteUnsafe adalah VERSI BURUK untuk kasus race condition.
//
// Pola read-modify-write dilakukan di aplikasi: baca dulu, baru tulis nilai baru.
// Ketika N goroutine melakukannya bersamaan, banyak yang membaca nilai lama yang
// sama, lalu saling menimpa. Inilah "lost update": dari 1000 klik, yang tercatat
// jauh lebih sedikit.
func (s *Service) VoteUnsafe(ctx context.Context, contestID int64) error {
	current, err := s.store.GetVotes(ctx, contestID)
	if err != nil {
		return fmt.Errorf("baca votes: %w", err)
	}
	if err := s.store.SetVotes(ctx, contestID, current+1); err != nil {
		return fmt.Errorf("tulis votes: %w", err)
	}
	return nil
}

// VoteSafe melakukan read-modify-write di dalam transaksi.
//
// Kuncinya bukan sekadar BEGIN/COMMIT: barisnya harus dikunci dengan
// SELECT ... FOR UPDATE (di sini GetVotesLocked). Tanpa lock, dua transaksi tetap
// bisa membaca nilai yang sama. Dengan lock, transaksi kedua menunggu sampai yang
// pertama commit, sehingga setiap kenaikan berbasis nilai terbaru.
func (s *Service) VoteSafe(ctx context.Context, contestID int64) error {
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

	current, err := tx.GetVotesLocked(ctx, contestID)
	if err != nil {
		return fmt.Errorf("baca votes (locked): %w", err)
	}
	if err := tx.SetVotes(ctx, contestID, current+1); err != nil {
		return fmt.Errorf("tulis votes: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	return nil
}