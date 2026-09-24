// Skenario Pengujian U2: Recovery 5 Event Baru (G01-G05) Saat/Pasca Simulasi Downtime Worker
// Halaman 7 Panduan Capstone Project: Antrean Tahan Saat Worker Pulih
const { Pool } = require("pg");
const { writeFileSync, mkdirSync } = require("node:fs");
const { setTimeout: delay } = require("node:timers/promises");
const { openPublisher } = require("../layanan/messaging");

async function jalankanU2(options = {}) {
  const runId = options.runId || process.argv[2] || `run02`;
  const evidenceDir = options.evidenceDir || ".evidence";
  mkdirSync(evidenceDir, { recursive: true });

  const pool =
    options.pool ||
    new Pool({
      connectionString:
        process.env.DATABASE_URL ||
        "postgres://simpel:simpel123@localhost:5432/simpel",
      max: 2,
    });

  const publisher = options.publisher || (await openPublisher());

  console.log(
    `\n==============================================================`,
  );
  console.log(
    `= SKENARIO U2: Recovery Downtime (5 Event Baru G01-G05) [${runId}] =`,
  );
  console.log(`==============================================================`);

  console.log("\n[U2] Mengirim 5 event baru G01-G05 (qty=4)...");
  const u2Events = [];
  for (let i = 1; i <= 5; i++) {
    const idStr = String(i).padStart(2, "0");
    const event = {
      event_id: `${runId}-G${idStr}`,
      event_type: "stock.received",
      occurred_at: new Date().toISOString(),
      payload: {
        receipt_id: `RCV-G${idStr}`,
        sku: "SKU-001",
        quantity: 4,
      },
    };
    await publisher.publish(event);
    u2Events.push(event);
  }

  console.log(
    "🚀 5 event G01-G05 dipublikasikan. Menunggu konsumsi worker (3 detik)...",
  );
  await delay(3000);

  const u2Db = await pool.query(
    "SELECT count(*)::int AS count, sum(quantity)::int AS total_qty FROM ledger_penerimaan WHERE event_id LIKE $1",
    [`${runId}-G%`],
  );
  const totalLedgerRes = await pool.query(
    "SELECT count(*)::int AS count FROM ledger_penerimaan",
  );
  const totalLedger = totalLedgerRes.rows[0].count;
  const u2SaldoRes = await pool.query(
    "SELECT saldo FROM stok_barang WHERE sku = 'SKU-001'",
  );
  const u2Saldo = u2SaldoRes.rows[0]?.saldo;

  console.log(
    `[U2 Hasil] Event G tercatat: ${u2Db.rows[0].count}/5. Total akumulasi ledger: ${totalLedger}, Saldo: ${u2Saldo} (Ekspektasi: 180 jika setelah U1)`,
  );

  const pass =
    u2Db.rows[0].count === 5 &&
    (options.standalone ? true : totalLedger === 25 && u2Saldo === 180);
  const result = {
    scenario: "U2",
    runId,
    pass,
    ledgerGCount: u2Db.rows[0].count,
    totalLedger,
    saldo: u2Saldo,
    expectedSaldo: options.standalone ? undefined : 180,
    events: u2Events,
  };

  writeFileSync(
    `${evidenceDir}/${runId}-u2.json`,
    JSON.stringify(result, null, 2),
  );

  console.log(`[U2 Status] ${pass ? "LULUS [✅ PASS]" : "GAGAL [❌ FAIL]"}`);

  if (!options.pool) await pool.end();
  if (!options.publisher) await publisher.close();

  return result;
}

if (require.main === module) {
  jalankanU2({ standalone: true }).catch((err) => {
    console.error("Eksekusi Skenario U2 gagal:", err);
    process.exit(1);
  });
}

module.exports = { jalankanU2 };
