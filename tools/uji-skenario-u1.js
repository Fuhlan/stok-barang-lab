// Skenario Pengujian U1: Pengiriman 20 Event Valid Normal (N01-N20)
// Halaman 7 Panduan Capstone Project: Beban Awal Normal
const amqp = require("amqplib");
const { Pool } = require("pg");
const { writeFileSync, mkdirSync } = require("node:fs");
const { setTimeout: delay } = require("node:timers/promises");
const { openPublisher } = require("../layanan/messaging");

async function jalankanU1(options = {}) {
  const runId = options.runId || process.argv[2] || "run01";
  const evidenceDir = options.evidenceDir || ".evidence";
  const autoReset = options.autoReset !== undefined ? options.autoReset : true;
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
    `== SKENARIO U1: Beban Awal Normal (20 Event Valid) [${runId}] ===`,
  );
  console.log(`==============================================================`);

  if (autoReset) {
    console.log(
      "-> Inisialisasi: Reset Saldo Awal SKU-001 = 100 dan pembersihan ledger...",
    );
    await pool.query("TRUNCATE TABLE ledger_penerimaan CASCADE;");
    await pool.query("TRUNCATE TABLE rejected_events CASCADE;");
    await pool.query(
      "INSERT INTO stok_barang (sku, saldo) VALUES ('SKU-001', 100) ON CONFLICT (sku) DO UPDATE SET saldo = 100;",
    );
    console.log("-> Saldo awal SKU-001 berhasil diset: 100");
  }

  console.log("\n[U1] Mengirim 20 event valid (N01-N20, qty=3)...");
  const u1Events = [];
  const startPublish = performance.now();
  for (let i = 1; i <= 20; i++) {
    const idStr = String(i).padStart(2, "0");
    const event = {
      event_id: `${runId}-N${idStr}`,
      event_type: "stock.received",
      occurred_at: new Date().toISOString(),
      payload: {
        receipt_id: `RCV-N${idStr}`,
        sku: "SKU-001",
        quantity: 3,
      },
    };
    await publisher.publish(event);
    u1Events.push(event);
  }
  const publishDurasiMs = Number((performance.now() - startPublish).toFixed(2));

  console.log(
    `🚀 20 event U1 terkirim ke broker (${publishDurasiMs} ms). Menunggu pemrosesan worker...`,
  );
  
  // Polling hingga seluruh 20 pesan tercatat di database atau timeout (max 10s)
  const startProcessing = performance.now();
  const maxWaitMs = 10000;
  let u1Db = await pool.query(
    "SELECT count(*)::int AS count, sum(quantity)::int AS total_qty FROM ledger_penerimaan WHERE event_id LIKE $1",
    [`${runId}-N%`],
  );
  while (u1Db.rows[0].count < 20 && performance.now() - startProcessing < maxWaitMs) {
    await delay(100);
    u1Db = await pool.query(
      "SELECT count(*)::int AS count, sum(quantity)::int AS total_qty FROM ledger_penerimaan WHERE event_id LIKE $1",
      [`${runId}-N%`],
    );
  }
  const pemrosesanDurasiMs = Number((performance.now() - startProcessing).toFixed(2));
  const totalDurasiMs = Number((publishDurasiMs + pemrosesanDurasiMs).toFixed(2));

  const u1SaldoRes = await pool.query(
    "SELECT saldo FROM stok_barang WHERE sku = 'SKU-001'",
  );
  const u1Saldo = u1SaldoRes.rows[0]?.saldo;

  console.log(
    `[U1 Hasil] Ledger N tercatat: ${u1Db.rows[0].count}/20, Saldo saat ini: ${u1Saldo} (Target: 160)`,
  );
  console.log(
    `[U1 Metrik Waktu] Publish: ${publishDurasiMs} ms, Pemrosesan: ${pemrosesanDurasiMs} ms, Total: ${totalDurasiMs} ms`,
  );

  const pass = u1Db.rows[0].count === 20 && u1Saldo === 160;
  const result = {
    scenario: "U1",
    runId,
    timestamp: new Date().toISOString(),
    pass,
    durasi: {
      publishMs: publishDurasiMs,
      pemrosesanMs: pemrosesanDurasiMs,
      totalMs: totalDurasiMs,
    },
    ledgerCount: u1Db.rows[0].count,
    totalQty: u1Db.rows[0].total_qty,
    saldo: u1Saldo,
    expectedSaldo: 160,
    events: u1Events,
  };

  writeFileSync(
    `${evidenceDir}/${runId}-u1.json`,
    JSON.stringify(result, null, 2),
  );

  console.log(`[U1 Status] ${pass ? "LULUS [✅ PASS]" : "GAGAL [❌ FAIL]"}`);

  if (!options.pool) await pool.end();
  if (!options.publisher) await publisher.close();

  return result;
}

if (require.main === module) {
  jalankanU1().catch((err) => {
    console.error("Eksekusi Skenario U1 gagal:", err);
    process.exit(1);
  });
}

module.exports = { jalankanU1 };
