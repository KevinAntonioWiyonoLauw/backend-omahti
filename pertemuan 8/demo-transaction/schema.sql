-- Skema untuk demo Transaksi & ACID (Modul Backend #8)

-- accounts: dua CHECK merepresentasikan aturan "Consistency".
--   - balance >= 0      : saldo tidak boleh minus.
--   - balance <= 1000000 : batas maksimum saldo (dipakai skenario "penerima mentok limit").
--   Tanpa transaksi, transfer bisa melanggar aturan ini atau membuat uang hilang.
CREATE TABLE IF NOT EXISTS accounts (
    id      BIGINT PRIMARY KEY,
    owner   TEXT   NOT NULL,
    balance BIGINT NOT NULL CHECK (balance >= 0 AND balance <= 1000000)
);

INSERT INTO accounts (id, owner, balance) VALUES
    (1, 'Budi (penerima)', 100000),
    (2, 'Siti (pengirim)', 100000)
ON CONFLICT (id) DO NOTHING;

-- contests: untuk demo race condition / lost update ala "1000 klik vote tercatat 200".
CREATE TABLE IF NOT EXISTS contests (
    id    BIGINT PRIMARY KEY,
    title TEXT   NOT NULL,
    votes BIGINT NOT NULL DEFAULT 0
);

INSERT INTO contests (id, title, votes) VALUES
    (1, 'Kontes Foto Kampus', 0)
ON CONFLICT (id) DO NOTHING;

-- ledger: kunci idempotency untuk mencegah double charge saat user klik "Bayar" berkali-kali.
--   PRIMARY KEY pada idempotency_key membuat INSERT kedua otomatis gagal (ON CONFLICT DO NOTHING).
CREATE TABLE IF NOT EXISTS ledger (
    idempotency_key TEXT PRIMARY KEY,
    amount          BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);