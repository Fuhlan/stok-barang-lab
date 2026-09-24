// Orchestrator Pengujian Terotomatisasi Skenario U1, U2, U3, U4 Kasus A10
// Mengacu ke Halaman 7 Panduan Capstone Project
const amqp = require("amqplib");
const { Pool } = require("pg");
const { mkdirSync } = require("node:fs");
const { declareTopology, openPublisher } = require("../layanan/messaging");

const { jalankanU1 } = require("./uji-skenario-u1");
const { jalankanU2 } = require("./uji-skenario-u2");
const { jalankanU3 } = require("./uji-skenario-u3");
const { jalankanU4 } = require("./uji-skenario-u4");

async function main() {
  const runId = process.argv[2] || "run00";
  const evidenceDir = ".evidence";
  mkdirSync(evidenceDir, { recursive: true });

  const pool = new Pool({
    connectionString:
      process.env.DATABASE_URL ||
      "postgres://simpel:simpel123@localhost:5432/simpel",
    max: 2,
  });

  const amqpUrl =
    process.env.AMQP_URL || "amqp://simpel:simpel123@localhost:5672";
  const conn = await amqp.connect(amqpUrl);
  const ch = await conn.createChannel();
  await declareTopology(ch);

  console.log(
    `\n##############################################################`,
  );
  console.log(
    `### MEMULAI RANGKAIAN LENGKAP PENGUJIAN KASUS A10 [${runId}] ##`,
  );
  console.log(`##############################################################`);

  const publisher = await openPublisher();

  const sharedContext = {
    runId,
    evidenceDir,
    pool,
    publisher,
    conn,
    ch,
  };

  // 1. Eksekusi Skenario U1
  const hasilU1 = await jalankanU1({ ...sharedContext, autoReset: true });

  // 2. Eksekusi Skenario U2
  const hasilU2 = await jalankanU2({ ...sharedContext, standalone: false });

  // 3. Eksekusi Skenario U3 (mengirim ulang event dari U1)
  const hasilU3 = await jalankanU3({
    ...sharedContext,
    u1Events: hasilU1.events,
  });

  // 4. Eksekusi Skenario U4
  const hasilU4 = await jalankanU4({ ...sharedContext, standalone: false });

  // Rekapitulasi Akhir
  console.log(
    "\n================ REKAPITULASI HASIL PENGUJIAN ================",
  );
  console.log(
    `U1 (Normal 20 Event)       : ${hasilU1.pass ? "LULUS [✅ PASS]" : "GAGAL [❌ FAIL]"}`,
  );
  console.log(
    `U2 (Recovery 5 Event)      : ${hasilU2.pass ? "LULUS [✅ PASS]" : "GAGAL [❌ FAIL]"}`,
  );
  console.log(
    `U3 (Replay 5 Event Idemp)  : ${hasilU3.pass ? "LULUS [✅ PASS]" : "GAGAL [❌ FAIL]"}`,
  );
  console.log(
    `U4 (Dead Letter & V01)     : ${hasilU4.pass ? "LULUS [✅ PASS]" : "GAGAL [❌ FAIL]"}`,
  );
  const allPass = hasilU1.pass && hasilU2.pass && hasilU3.pass && hasilU4.pass;
  console.log(
    `Status Keseluruhan         : ${allPass ? "SEMUA SKENARIO LULUS 100%" : "ADA SKENARIO GAGAL"}`,
  );
  console.log("==============================================================");

  await publisher.close();
  await ch.close();
  await conn.close();
  await pool.end();

  if (!allPass) {
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error("Eksekusi uji gagal:", err);
    process.exit(1);
  });
}

module.exports = { main };
