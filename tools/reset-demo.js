// Reset demo database: Kosongkan ledger dan reset saldo SKU-001 ke 100
const { Pool } = require('pg');

async function main() {
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgres://simpel:simpel123@localhost:5432/simpel',
    max: 1
  });

  try {
    await pool.query('TRUNCATE TABLE ledger_penerimaan CASCADE;');
    await pool.query('TRUNCATE TABLE rejected_events CASCADE;');
    await pool.query('INSERT INTO stok_barang (sku, saldo, diperbarui_pada) VALUES (\'SKU-001\', 100, now()) ON CONFLICT (sku) DO UPDATE SET saldo = 100, diperbarui_pada = now();');
    console.log('PASS: Data demo berhasil direset. Saldo awal SKU-001 = 100, ledger kosong.');
  } finally {
    await pool.end();
  }
}

main().catch(err => {
  console.error('Reset database failed:', err.message);
  process.exit(1);
});
