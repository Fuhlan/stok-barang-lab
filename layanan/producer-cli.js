// Producer CLI Kasus A10: Publikasi Event Tunggal / Batch
const { openPublisher } = require('./messaging');

async function main() {
  const args = process.argv.slice(2);
  const options = Object.fromEntries(args.map(arg => arg.replace(/^--/, '').split('=')));

  const eventId = options.eventId || options.event_id || `run-cli-${Date.now()}`;
  const receiptId = options.receiptId || options.receipt_id || 'RCV-CLI-001';
  const sku = options.sku || 'SKU-001';
  const quantity = Number(options.quantity || options.qty || 5);

  const event = {
    event_id: eventId,
    event_type: options.eventType || 'stock.received',
    occurred_at: new Date().toISOString(),
    payload: {
      receipt_id: receiptId,
      sku,
      quantity
    }
  };

  const publisher = await openPublisher();
  try {
    console.log('Mengirim event stok:', JSON.stringify(event));
    await publisher.publish(event);
    console.log('SUKSES: Publisher confirm diterima oleh broker.');
  } finally {
    await publisher.close();
  }
}

main().catch(err => {
  console.error('CLI Producer Error:', err.message);
  process.exit(1);
});
