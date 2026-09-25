// Skenario Pengujian U4: Dead Letter (Pesan Cacat X01) & Event Valid V01
// Halaman 7 Panduan Capstone Project: Pemisahan Pesan Cacat ke DLQ/Tabel Rejected Tanpa Mengganggu Pesan Valid
const amqp = require("amqplib");
const { Pool } = require("pg");
const { writeFileSync, mkdirSync } = require("node:fs");
const { setTimeout: delay } = require("node:timers/promises");
const { declareTopology, openPublisher } = require("../layanan/messaging");

async function jalankanU4(options = {}) {
  const runId = options.runId || process.argv[2] || "run04";
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

  const amqpUrl =
    process.env.AMQP_URL || "amqp://simpel:simpel123@localhost:5672";
  let conn = options.conn;
  let ch = options.ch;
  let closeAmqpLocal = false;

  if (!ch) {
    conn = await amqp.connect(amqpUrl);
    ch = await conn.createChannel();
    closeAmqpLocal = true;
  }
  const spec = await declareTopology(ch);
  const publisher = options.publisher || (await openPublisher());

  console.log(
    `\n==============================================================`,
  );
  console.log(
    `= SKENARIO U4: Dead Letter (X01 Cacat) & V01 Valid [${runId}] =`,
  );
  console.log(`==============================================================`);

  const ledgerSebelumRes = await pool.query(
    "SELECT count(*)::int AS count FROM ledger_penerimaan",
  );
  const saldoSebelumRes = await pool.query(
    "SELECT saldo FROM stok_barang WHERE sku = 'SKU-001'",
  );
  const ledgerSebelum = ledgerSebelumRes.rows[0]?.count || 0;
  const saldoSebelum = saldoSebelumRes.rows[0]?.saldo || 0;

  console.log(
    "\n[U4] Mengirim pesan tidak valid X01 (tanpa field SKU) dan event valid V01 (qty=10)...",
  );
  const invalidEventX01 = {
    event_id: `${runId}-X01`,
    event_type: "stock.received",
    occurred_at: new Date().toISOString(),
    payload: {
      receipt_id: "RCV-X01",
      // Sengaja tanpa SKU
      quantity: 5,
    },
  };

  const validEventV01 = {
    event_id: `${runId}-V01`,
    event_type: "stock.received",
    occurred_at: new Date().toISOString(),
    payload: {
      receipt_id: "RCV-V01",
      sku: "SKU-001",
      quantity: 10,
    },
  };

  // Kirim X01 langsung via AMQP channel agar tidak dicegat HTTP express
  const startPublish = performance.now();
  ch.publish(
    spec.exchange,
    spec.routingKey,
    Buffer.from(JSON.stringify(invalidEventX01)),
    {
      persistent: true,
      messageId: invalidEventX01.event_id,
    },
  );

  // Kirim V01
  await publisher.publish(validEventV01);
  const publishDurasiMs = Number((performance.now() - startPublish).toFixed(2));

  console.log(
    `🚀 X01 dan V01 terkirim (${publishDurasiMs} ms). Menunggu verifikasi worker...`,
  );
  
  // Polling hingga X01 ada di rejected_events dan V01 ada di ledger atau timeout (max 10s)
  const startProcessing = performance.now();
  const maxWaitMs = 10000;
  let rejectedCountRes = { rows: [{ count: 0 }] };
  let v01RecordedRes = { rows: [{ count: 0 }] };
  while (performance.now() - startProcessing < maxWaitMs) {
    rejectedCountRes = await pool.query(
      "SELECT count(*)::int AS count FROM rejected_events WHERE event_id = $1",
      [`${runId}-X01`],
    );
    v01RecordedRes = await pool.query(
      "SELECT count(*)::int AS count FROM ledger_penerimaan WHERE event_id = $1",
      [`${runId}-V01`],
    );
    if (rejectedCountRes.rows[0].count >= 1 && v01RecordedRes.rows[0].count >= 1) {
      break;
    }
    await delay(100);
  }
  const pemrosesanDurasiMs = Number((performance.now() - startProcessing).toFixed(2));
  const totalDurasiMs = Number((publishDurasiMs + pemrosesanDurasiMs).toFixed(2));

  const totalLedgerAkhirRes = await pool.query(
    "SELECT count(*)::int AS count FROM ledger_penerimaan",
  );
  const saldoAkhirRes = await pool.query(
    "SELECT saldo FROM stok_barang WHERE sku = 'SKU-001'",
  );

  const totalLedgerAkhir = totalLedgerAkhirRes.rows[0].count;
  const saldoAkhir = saldoAkhirRes.rows[0].saldo;
  const rejectedCount = rejectedCountRes.rows[0].count;
  const v01Recorded = v01RecordedRes.rows[0].count === 1;

  console.log(
    `[U4 Hasil] Total Ledger Akhir: ${totalLedgerAkhir} (Ledger bertambah ${totalLedgerAkhir - ledgerSebelum}, V01 tercatat: ${v01Recorded})`,
  );
  console.log(
    `[U4 Hasil] Saldo Akhir: ${saldoAkhir} (Saldo bertambah ${saldoAkhir - saldoSebelum})`,
  );
  console.log(
    `[U4 Hasil] X01 tercatat di tabel rejected_events: ${rejectedCount}`,
  );
  console.log(
    `[U4 Metrik Waktu] Publish: ${publishDurasiMs} ms, Pemrosesan/DLQ: ${pemrosesanDurasiMs} ms, Total: ${totalDurasiMs} ms`,
  );

  // Bila dijalankan berurutan (full suite), totalLedgerAkhir = 26, saldoAkhir = 190.
  // Bila standalone, pastikan V01 bertambah 1 ledger & saldo naik +10, serta X01 ada di rejected.
  const pass =
    rejectedCount >= 1 &&
    v01Recorded &&
    (options.standalone
      ? totalLedgerAkhir === ledgerSebelum + 1 &&
        saldoAkhir === saldoSebelum + 10
      : totalLedgerAkhir === 26 && saldoAkhir === 190);

  const result = {
    scenario: "U4",
    runId,
    timestamp: new Date().toISOString(),
    pass,
    durasi: {
      publishMs: publishDurasiMs,
      pemrosesanMs: pemrosesanDurasiMs,
      totalMs: totalDurasiMs,
    },
    totalLedgerAkhir,
    saldoAkhir,
    rejectedRecorded: rejectedCount >= 1,
    v01Recorded,
    expectedSaldo: options.standalone ? saldoSebelum + 10 : 190,
  };

  writeFileSync(
    `${evidenceDir}/${runId}-u4.json`,
    JSON.stringify(result, null, 2),
  );

  console.log(`[U4 Status] ${pass ? "LULUS [✅ PASS]" : "GAGAL [❌ FAIL]"}`);

  if (!options.pool) await pool.end();
  if (!options.publisher) await publisher.close();
  if (closeAmqpLocal) {
    await ch.close();
    await conn.close();
  }

  return result;
}

if (require.main === module) {
  jalankanU4({ standalone: true }).catch((err) => {
    console.error("Eksekusi Skenario U4 gagal:", err);
    process.exit(1);
  });
}

module.exports = { jalankanU4 };
