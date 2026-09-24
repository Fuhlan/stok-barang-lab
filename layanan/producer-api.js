// Producer API Kasus A10: HTTP Ingress Gateway
const express = require('express');
const { randomUUID } = require('node:crypto');
const { integer, openPublisher, validateStockEvent } = require('./messaging');

async function main() {
  const publisher = await openPublisher();
  const app = express();
  app.use(express.json({ limit: '16kb' }));

  app.get('/health', (_req, res) => {
    res.status(publisher.isReady() ? 200 : 503).json({
      service: 'producer-api',
      ready: publisher.isReady()
    });
  });

  app.post('/penerimaan', async (req, res) => {
    const started = performance.now();
    const { receipt_id, sku, quantity, run_id, event_id } = req.body || {};

    const runId = run_id || 'manual';
    const eventId = event_id || `${runId}-${randomUUID()}`;
    const receiptId = receipt_id || `RCV-${randomUUID().slice(0, 8)}`;

    const event = {
      event_id: eventId,
      event_type: 'stock.received',
      occurred_at: new Date().toISOString(),
      payload: {
        receipt_id: receiptId,
        sku: sku || 'SKU-001',
        quantity: quantity !== undefined ? Number(quantity) : 1
      }
    };

    try {
      validateStockEvent(event);
    } catch (valErr) {
      return res.status(400).json({ error: valErr.message, event_id: eventId });
    }

    try {
      await publisher.publish(event);
      res.status(202).json({
        status: 'DITERIMA_BROKER',
        event_id: event.event_id,
        receipt_id: event.payload.receipt_id,
        sku: event.payload.sku,
        quantity: event.payload.quantity,
        durasiMs: Number((performance.now() - started).toFixed(2))
      });
    } catch (pubErr) {
      res.status(503).json({
        error: pubErr.message,
        event_id: event.event_id
      });
    }
  });

  const port = integer('PORT_PRODUCER', 3010, 1024, 65535);
  const server = app.listen(port, '127.0.0.1', () => {
    console.log(JSON.stringify({ service: 'producer-api', ready: true, port }));
  });

  async function stop() {
    server.close();
    await publisher.close();
  }
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  publisher.connection.once('close', () => { server.close(); });
}

main().catch(err => {
  console.error('Producer API failed to start:', err.message);
  process.exit(1);
});
