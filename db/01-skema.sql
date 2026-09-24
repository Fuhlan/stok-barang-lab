-- Skema Database untuk Kasus A10: Penerimaan Stok Barang

-- 1. Tabel Saldo Stok Barang per SKU
CREATE TABLE IF NOT EXISTS stok_barang (
    sku             TEXT PRIMARY KEY,
    saldo           INTEGER NOT NULL DEFAULT 0 CHECK (saldo >= 0),
    diperbarui_pada TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Tabel Ledger Mutasi Penerimaan Stok & Deduplikasi Atomik
CREATE TABLE IF NOT EXISTS ledger_penerimaan (
    event_id        TEXT PRIMARY KEY,
    receipt_id      TEXT NOT NULL,
    sku             TEXT NOT NULL,
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    saldo_sebelum   INTEGER NOT NULL,
    saldo_sesudah   INTEGER NOT NULL,
    dicatat_pada    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. Tabel Catatan Pesan Tidak Valid (DLQ / Terminal Failure U4)
CREATE TABLE IF NOT EXISTS rejected_events (
    id              SERIAL PRIMARY KEY,
    event_id        TEXT,
    alasan          TEXT NOT NULL,
    raw_payload     JSONB,
    ditolak_pada    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Inisialisasi saldo awal deterministik untuk pengujian (SKU-001 dengan stok 100)
INSERT INTO stok_barang (sku, saldo) VALUES ('SKU-001', 100)
ON CONFLICT (sku) DO NOTHING;
