// Package store mengimplementasikan wallet.Store di atas PostgreSQL.
//
// Di sini kita memakai database/sql polos (bukan GORM) supaya siklus
// BEGIN / COMMIT / ROLLBACK terlihat eksplisit — persis seperti yang dibahas
// di Modul Backend #8. Versi GORM ada di bagian komentar untuk perbandingan.
package store

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"

	"demo-transaction/internal/wallet"
)

// PgStore membuka koneksi ke PostgreSQL dan mengimplementasikan wallet.Store.
type PgStore struct {
	db *sql.DB
}

// NewPgStore membuka koneksi dan mengetesnya dengan Ping.
func NewPgStore(dsn string) (*PgStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	// Batasi pool supaya demo concurrency tidak menabrak max_connections
	// PostgreSQL. Ini juga meniru pool production yang selalu terbatas.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	return &PgStore{db: db}, nil
}

func (s *PgStore) Close() error { return s.db.Close() }

// pgTx mengimplementasikan wallet.Tx.
type pgTx struct {
	tx *sql.Tx
}

// Begin memulai transaksi database.
//
// Padanan GORM:
//
//	return tx := db.Begin() // atau dipakai lewat db.Transaction(func(tx *gorm.DB) error { ... })
func (s *PgStore) Begin(ctx context.Context) (wallet.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &pgTx{tx: tx}, nil
}

func (t *pgTx) GetBalance(ctx context.Context, accountID int64) (int64, error) {
	var balance int64
	err := t.tx.QueryRowContext(ctx,
		`SELECT balance FROM accounts WHERE id = $1`, accountID).Scan(&balance)
	if err != nil {
		return 0, fmt.Errorf("get balance: %w", err)
	}
	return balance, nil
}

func (t *pgTx) AddBalance(ctx context.Context, accountID int64, delta int64) error {
	// UPDATE relatif (balance = balance + delta) lebih tahan race dibanding
	// membaca lalu menulis nilai absolut.
	_, err := t.tx.ExecContext(ctx,
		`UPDATE accounts SET balance = balance + $1 WHERE id = $2`, delta, accountID)
	return err
}

// GetVotesLocked mengunci baris dengan FOR UPDATE. Inilah kunci anti-lost-update.
func (t *pgTx) GetVotesLocked(ctx context.Context, contestID int64) (int64, error) {
	var votes int64
	err := t.tx.QueryRowContext(ctx,
		`SELECT votes FROM contests WHERE id = $1 FOR UPDATE`, contestID).Scan(&votes)
	if err != nil {
		return 0, fmt.Errorf("get votes locked: %w", err)
	}
	return votes, nil
}

func (t *pgTx) SetVotes(ctx context.Context, contestID int64, value int64) error {
	_, err := t.tx.ExecContext(ctx,
		`UPDATE contests SET votes = $1 WHERE id = $2`, value, contestID)
	return err
}

// MarkKey menyisipkan idempotency key. Kalau key sudah ada, ON CONFLICT DO NOTHING
// membuat rows affected = 0, yang kita artikan sebagai "sudah pernah diproses".
func (t *pgTx) MarkKey(ctx context.Context, key string, amount int64) (bool, error) {
	res, err := t.tx.ExecContext(ctx,
		`INSERT INTO ledger (idempotency_key, amount) VALUES ($1, $2)
		 ON CONFLICT (idempotency_key) DO NOTHING`, key, amount)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (t *pgTx) Commit(ctx context.Context) error   { return t.tx.Commit() }
func (t *pgTx) Rollback(ctx context.Context) error { return t.tx.Rollback() }

// --- jalur autocommit: sengaja tanpa transaksi ---

func (s *PgStore) GetBalance(ctx context.Context, accountID int64) (int64, error) {
	var balance int64
	err := s.db.QueryRowContext(ctx,
		`SELECT balance FROM accounts WHERE id = $1`, accountID).Scan(&balance)
	if err != nil {
		return 0, fmt.Errorf("get balance: %w", err)
	}
	return balance, nil
}

func (s *PgStore) SetBalance(ctx context.Context, accountID int64, value int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE accounts SET balance = $1 WHERE id = $2`, value, accountID)
	return err
}

func (s *PgStore) GetVotes(ctx context.Context, contestID int64) (int64, error) {
	var votes int64
	err := s.db.QueryRowContext(ctx,
		`SELECT votes FROM contests WHERE id = $1`, contestID).Scan(&votes)
	if err != nil {
		return 0, fmt.Errorf("get votes: %w", err)
	}
	return votes, nil
}

func (s *PgStore) SetVotes(ctx context.Context, contestID int64, value int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE contests SET votes = $1 WHERE id = $2`, value, contestID)
	return err
}

