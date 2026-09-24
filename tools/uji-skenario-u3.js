// Skenario Pengujian U3: Replay 5 Event Lama N01-N05 (Pengujian Idempotensi & Anti Double Mutasi)
// Halaman 7 Panduan Capstone Project: Replay Idempotensi Tanpa Penambahan Ganda
const { Pool } = require("pg");
const { writeFileSync, mkdirSync } = require("node:fs");
const { setTimeout: delay } = require("node:timers/promises");
const { openPublisher } = require("../layanan/messaging");

async function jalankanU3(options = {}) {
  const runId = options.runId || process.argv[2] || "run03";
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
    `SKENARIO U3: Uji Replay Idempotensi (5 Event N01-N05) [${runId}]`,
  );
  console.log(`==============================================================`);

  // Catat saldo dan ledger sebelum replay
  const ledgerSebelumRes = await pool.query(
    "SELECT count(*)::int AS count FROM ledger_penerimaan",
  );
  const saldoSebelumRes = await pool.query(
    "SELECT saldo FROM stok_barang WHERE sku = 'SKU-001'",
  );
  const ledgerSebelum = ledgerSebelumRes.rows[0]?.count || 0;
  const saldoSebelum = saldoSebelumRes.rows[0]?.saldo || 0;

  console.log(
    `[U3 Pre-check] Ledger saat ini: ${ledgerSebelum}, Saldo saat ini: ${saldoSebelum}`,
  );

  // Dapatkan atau bentuk event yang akan di-replay dari database (5 data awal)
  const eventsToReplay = options.u1Events ? options.u1Events.slice(0, 5) : [];
  if (eventsToReplay.length === 0) {
    const dbEventsRes = await pool.query(
      `SELECT event_id, receipt_id, sku, quantity 
       FROM ledger_penerimaan 
       ORDER BY dicatat_pada ASC 
       LIMIT 5`,
    );

    if (dbEventsRes.rows.length === 0) {
      throw new Error(
        "[U3 Error] Tidak ada data di ledger_penerimaan untuk di-replay. Pastikan U1 telah dijalankan terlebih dahulu.",
      );
    }

    for (const row of dbEventsRes.rows) {
      eventsToReplay.push({
        event_id: row.event_id,
        event_type: "stock.received",
        occurred_at: new Date().toISOString(),
        payload: {
          receipt_id: row.receipt_id,
          sku: row.sku,
          quantity: row.quantity,
        },
      });
    }
  }

  console.log(
    `\n[U3] Mengirim ulang (replay) ${eventsToReplay.length} event dari ledger dengan ID & payload persis sama...`,
  );
  for (let i = 0; i < eventsToReplay.length; i++) {
    await publisher.publish(eventsToReplay[i]);
  }

  console.log(
    "🚀 5 event replay terkirim. Menunggu pemeriksaan idempotensi worker (2 detik)...",
  );
  await delay(2000);

  const ledgerSetelahRes = await pool.query(
    "SELECT count(*)::int AS count FROM ledger_penerimaan",
  );
  const saldoSetelahRes = await pool.query(
    "SELECT saldo FROM stok_barang WHERE sku = 'SKU-001'",
  );
  const ledgerSetelah = ledgerSetelahRes.rows[0].count;
  const saldoSetelah = saldoSetelahRes.rows[0].saldo;

  console.log(
    `[U3 Hasil] Total ledger setelah replay: ${ledgerSetelah} (Sebelum: ${ledgerSebelum})`,
  );
  console.log(
    `[U3 Hasil] Saldo setelah replay: ${saldoSetelah} (Sebelum: ${saldoSebelum})`,
  );

  // Idempotensi terpenuhi jika tidak ada penambahan ledger baru dan saldo tidak berubah
  const pass = ledgerSetelah === ledgerSebelum && saldoSetelah === saldoSebelum;
  const result = {
    scenario: "U3",
    runId,
    pass,
    ledgerSebelum,
    ledgerSetelah,
    saldoSebelum,
    saldoSetelah,
    isIdempotent: pass,
    replayedEvents: eventsToReplay,
  };

  writeFileSync(
    `${evidenceDir}/${runId}-u3.json`,
    JSON.stringify(result, null, 2),
  );

  console.log(
    `[U3 Status] ${pass ? "LULUS (Tidak ada mutasi ganda) [✅ PASS]" : "GAGAL (Terjadi mutasi ganda) [❌ FAIL]"}`,
  );

  if (!options.pool) await pool.end();
  if (!options.publisher) await publisher.close();

  return result;
}

if (require.main === module) {
  jalankanU3().catch((err) => {
    console.error("Eksekusi Skenario U3 gagal:", err);
    process.exit(1);
  });
}

module.exports = { jalankanU3 };
