// Migrasi DDL Kasus A10
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { Pool } = require('pg');

async function main() {
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgres://simpel:simpel123@localhost:5432/simpel',
    max: 1
  });

  try {
    const sqlPath = join(__dirname, '../db/01-skema.sql');
    const sql = readFileSync(sqlPath, 'utf8');
    await pool.query(sql);
    console.log('PASS: Tabel skema stok-barang-lab siap digunakan di PostgreSQL.');
  } finally {
    await pool.end();
  }
}

main().catch(err => {
  console.error('Database setup failed:', err.message);
  process.exit(1);
});
