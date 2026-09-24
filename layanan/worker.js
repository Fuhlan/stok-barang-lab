// Consumer Worker Kasus A10: Penerimaan Stok Barang
// Menerapkan Idempotency Persisten Atomik & Manual Acknowledgment
const amqp = require('amqplib');
const { Pool } = require('pg');
const { setTimeout: delay } = require('node:timers/promises');
const { integer, declareTopology, validateStockEvent } = require('./messaging');

async function main() {
  const workerId = process.env.WORKER_ID || `worker-stok-${process.pid}`;
  const prefetch = integer('WORKER_PREFETCH', 1, 1, 100);
  const workMs = integer('WORKER_KERJA_MS', 20, 0, 5000);
  const ackDelay = integer('WORKER_ACK_DELAY_MS', 0, 0, 10000);

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgres://simpel:simpel123@localhost:5432/simpel',
    max: integer('DB_POOL_MAX', 4, 1, 16),
    connectionTimeoutMillis: 3000,
    statement_timeout: 10000,
    application_name: `stok-${workerId}`
  });

  await pool.query('SELECT 1');

  const connection = await amqp.connect(process.env.AMQP_URL || 'amqp://simpel:simpel123@localhost:5672');
  connection.on('error', () => {});
  const channel = await connection.createChannel();
  const spec = await declareTopology(channel);

  await channel.prefetch(prefetch);

  let active = 0;
  let stopping = false;
  let channelOpen = true;
  let consumerTag;

  const stats = { received: 0, stored: 0, duplicates: 0, rejected: 0, errors: 0 };
  const log = data => console.log(JSON.stringify({ service: 'consumer-stok', worker: workerId, ...data }));

  async function stop(code = 0) {
    if (stopping) return;
    stopping = true;
    process.exitCode = code;
    if (consumerTag && channelOpen) await channel.cancel(consumerTag).catch(() => {});
    const deadline = Date.now() + 10000;
    while (active && Date.now() < deadline) await delay(25);
    await channel.close().catch(() => {});
    await connection.close().catch(() => {});
    await pool.end().catch(() => {});
    log({ stopping: true, code, stats });
  }

  channel.on('error', () => { channelOpen = false; void stop(1); });
  channel.on('close', () => { channelOpen = false; if (!stopping) void stop(1); });
  connection.on('close', () => { channelOpen = false; if (!stopping) void stop(1); });
  process.once('SIGINT', () => void stop());
  process.once('SIGTERM', () => void stop());

  const consumed = await channel.consume(spec.queue, async message => {
    if (!message) return;
    if (stopping) {
      if (channelOpen) channel.nack(message, false, true);
      return;
    }

    active++;
    stats.received++;

    let rawData;
    let event;
    try {
      rawData = JSON.parse(message.content.toString('utf8'));
      event = validateStockEvent(rawData);
    } catch (error) {
      stats.rejected++;
      log({ rejected: true, reason: error.message, messageId: message.properties.messageId });
      
      // Simpan catatan penolakan ke tabel rejected_events secara transparan
      try {
        await pool.query(
          'INSERT INTO rejected_events (event_id, alasan, raw_payload) VALUES ($1, $2, $3)',
          [message.properties.messageId || 'UNKNOWN', error.message, JSON.stringify(rawData || {})]
        );
      } catch (dbErr) {
        log({ error: 'Failed to record rejected event in DB', details: dbErr.message });
      }

      // Reject tanpa requeue -> mengarah ke DLQ (stock_updates.dlq) via Dead Letter Exchange
      if (channelOpen) channel.nack(message, false, false);
      active--;
      return;
    }

    // Pemrosesan Idempotensi & Mutasi Stok dalam Satu Transaksi Database Atomik
    let client;
    try {
      client = await pool.connect();
      await client.query('BEGIN');

      // 1. Cek apakah event_id sudah pernah diproses di ledger
      const checkLedger = await client.query(
        'SELECT event_id, sku, quantity, saldo_sesudah FROM ledger_penerimaan WHERE event_id = $1 FOR UPDATE',
        [event.event_id]
      );

      let isDuplicate = false;
      let finalSaldo = 0;

      if (checkLedger.rowCount > 0) {
        // Event sudah pernah diproses -> Idempotent Replay terdeteksi
        isDuplicate = true;
        stats.duplicates++;
        finalSaldo = checkLedger.rows[0].saldo_sesudah;
        await client.query('COMMIT');
        log({
          action: 'DEDUPLICATED',
          event_id: event.event_id,
          receipt_id: event.payload.receipt_id,
          sku: event.payload.sku,
          saldo: finalSaldo,
          pesan: 'Event duplicate detected. Replay did not increase stock.'
        });
      } else {
        // Event baru -> Amankan baris stok_barang dengan FOR UPDATE (pesimistik lock per SKU)
        const stockRow = await client.query(
          'SELECT saldo FROM stok_barang WHERE sku = $1 FOR UPDATE',
          [event.payload.sku]
        );

        let saldoSebelum = 0;
        if (stockRow.rowCount === 0) {
          // SKU belum ada, inisialisasi 0
          await client.query('INSERT INTO stok_barang (sku, saldo) VALUES ($1, 0)', [event.payload.sku]);
          saldoSebelum = 0;
        } else {
          saldoSebelum = stockRow.rows[0].saldo;
        }

        const saldoSesudah = saldoSebelum + event.payload.quantity;

        // Update saldo stok barang
        await client.query(
          'UPDATE stok_barang SET saldo = $1, diperbarui_pada = now() WHERE sku = $2',
          [saldoSesudah, event.payload.sku]
        );

        // Catat ke ledger mutasi
        await client.query(
          `INSERT INTO ledger_penerimaan (event_id, receipt_id, sku, quantity, saldo_sebelum, saldo_sesudah, dicatat_pada)
           VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [
            event.event_id,
            event.payload.receipt_id,
            event.payload.sku,
            event.payload.quantity,
            saldoSebelum,
            saldoSesudah,
            event.occurred_at || new Date().toISOString()
          ]
        );

        await client.query('COMMIT');
        stats.stored++;
        finalSaldo = saldoSesudah;

        if (workMs) await delay(workMs);

        log({
          action: 'MUTATION_COMMITTED',
          event_id: event.event_id,
          receipt_id: event.payload.receipt_id,
          sku: event.payload.sku,
          quantity: event.payload.quantity,
          saldoSebelum,
          saldoSesudah,
          redelivered: message.fields.redelivered
        });
      }

      client.release();
      client = null;

      if (ackDelay) await delay(ackDelay);

      // Acknowledgment hanya dilakukan setelah efek bisnis dan ledger aman tersimpan
      if (channelOpen) channel.ack(message);

    } catch (err) {
      if (client) {
        await client.query('ROLLBACK').catch(() => {});
        client.release();
      }
      stats.errors++;
      log({ error: 'Database transaction failed', event_id: event.event_id, message: err.message });
      // Hentikan worker untuk menghindari requeue loop tanpa jeda
      void stop(1);
    } finally {
      active--;
    }
  }, { noAck: false });

  consumerTag = consumed.consumerTag;
  log({ status: 'READY', queue: spec.queue, prefetch, workerId });
}

main().catch(err => {
  console.error(JSON.stringify({ error: err.message, stack: err.stack }));
  process.exit(1);
});
