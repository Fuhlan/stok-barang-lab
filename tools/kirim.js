// Pengirim event sintetis penerimaan stok
const { writeFileSync, mkdirSync } = require('node:fs');
const { dirname } = require('node:path');
const { openPublisher } = require('../layanan/messaging');

const options = Object.fromEntries(process.argv.slice(2).map(arg => arg.replace(/^--/, '').split('=')));

async function main() {
  const count = Number(options.count || 1);
  const runId = options.run || `r${Date.now()}`;
  const prefix = options.prefix || 'N';
  const sku = options.sku || 'SKU-001';
  const qty = Number(options.qty || 3);

  const publisher = await openPublisher();
  const sent = [];
  const start = performance.now();

  try {
    for (let i = 1; i <= count; i++) {
      const padNum = String(i).padStart(2, '0');
      const eventId = `${runId}-${prefix}${padNum}`;
      const receiptId = `RCV-${prefix}${padNum}`;
      const event = {
        event_id: eventId,
        event_type: 'stock.received',
        occurred_at: new Date().toISOString(),
        payload: {
          receipt_id: receiptId,
          sku,
          quantity: qty
        }
      };

      await publisher.publish(event);
      sent.push(event);
    }
  } finally {
    await publisher.close();
  }

  const duration = Math.round(performance.now() - start);
  const result = {
    runId,
    count: sent.length,
    durationMs: duration,
    events: sent.map(e => ({ event_id: e.event_id, receipt_id: e.payload.receipt_id, qty: e.payload.quantity }))
  };

  const output = options.output || `.evidence/${runId}-${prefix}.json`;
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(output, JSON.stringify(result, null, 2) + '\n');
  console.log(JSON.stringify({ runId, sentCount: sent.length, durationMs: duration, output }));
}

main().catch(err => {
  console.error('Kirim gagal:', err.message);
  process.exit(1);
});
