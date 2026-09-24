// Inspeksi hasil ledger mutasi dan saldo stok Kasus A10
const { Pool } = require('pg');

async function main() {
  const runId = process.argv[2];
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgres://simpel:simpel123@localhost:5432/simpel',
    max: 1
  });

  try {
    const stockQuery = await pool.query('SELECT sku, saldo, diperbarui_pada FROM stok_barang WHERE sku = \'SKU-001\'');
    const currentStock = stockQuery.rows[0] ? stockQuery.rows[0].saldo : 0;

    let ledgerQuery;
    if (runId) {
      ledgerQuery = await pool.query(
        'SELECT count(*)::int AS count, coalesce(sum(quantity), 0)::int AS total_qty FROM ledger_penerimaan WHERE event_id LIKE $1',
        [`${runId}-%`]
      );
    } else {
      ledgerQuery = await pool.query(
        'SELECT count(*)::int AS count, coalesce(sum(quantity), 0)::int AS total_qty FROM ledger_penerimaan'
      );
    }

    const rejectedQuery = await pool.query('SELECT count(*)::int AS count FROM rejected_events');

    const output = {
      runId: runId || 'ALL',
      saldo_sku001: currentStock,
      total_ledger_rows: ledgerQuery.rows[0].count,
      total_quantity_masuk: ledgerQuery.rows[0].total_qty,
      total_rejected_events: rejectedQuery.rows[0].count
    };

    console.log(JSON.stringify(output, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch(err => {
  console.error('Gagal mengambil hasil:', err.message);
  process.exit(1);
});
