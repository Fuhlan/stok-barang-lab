// Modul AMQP: Topologi, Validasi Kontrak, dan Publisher Confirm Kasus A10
const amqp = require('amqplib');

function integer(name, fallback, min, max) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer from ${min} to ${max}`);
  }
  return value;
}

function topologySpec() {
  return {
    exchange: 'inventory',
    type: 'direct',
    queue: 'stock_updates',
    routingKey: 'stock.received',
    dlxExchange: 'inventory.dlx',
    dlqQueue: 'stock_updates.dlq',
    dlqRoutingKey: 'stock.rejected'
  };
}

async function declareTopology(channel, spec = topologySpec()) {
  // 1. Deklarasi Dead Letter Exchange & Queue untuk menampung pesan cacat (U4)
  await channel.assertExchange(spec.dlxExchange, 'direct', { durable: true });
  await channel.assertQueue(spec.dlqQueue, { durable: true });
  await channel.bindQueue(spec.dlqQueue, spec.dlxExchange, spec.dlqRoutingKey);

  // 2. Deklarasi Main Exchange dan Work Queue dengan binding dead-letter
  await channel.assertExchange(spec.exchange, spec.type, { durable: true });
  const queueOptions = {
    durable: true,
    deadLetterExchange: spec.dlxExchange,
    deadLetterRoutingKey: spec.dlqRoutingKey
  };
  await channel.assertQueue(spec.queue, queueOptions);
  await channel.bindQueue(spec.queue, spec.exchange, spec.routingKey);

  return spec;
}

function validateStockEvent(event) {
  if (!event || typeof event !== 'object') throw new Error('Payload is not a valid JSON object');
  if (event.event_type !== 'stock.received') throw new Error(`Unsupported event_type: ${event.event_type}`);
  if (typeof event.event_id !== 'string' || !event.event_id.trim()) throw new Error('Missing or invalid event_id');
  if (typeof event.occurred_at !== 'string' || !Number.isFinite(Date.parse(event.occurred_at))) throw new Error('Invalid occurred_at timestamp');
  
  const payload = event.payload;
  if (!payload || typeof payload !== 'object') throw new Error('Missing or invalid payload object');
  if (typeof payload.receipt_id !== 'string' || !payload.receipt_id.trim()) throw new Error('Field receipt_id is required');
  if (typeof payload.sku !== 'string' || !payload.sku.trim()) throw new Error('Field sku is required');
  if (!Number.isInteger(payload.quantity) || payload.quantity <= 0) throw new Error('Field quantity must be a positive integer');

  return event;
}

async function openPublisher(declare = declareTopology, url = process.env.AMQP_URL || 'amqp://simpel:simpel123@localhost:5672') {
  const connection = await amqp.connect(url);
  connection.on('error', () => {});
  let channel, spec;
  try {
    channel = await connection.createConfirmChannel();
    channel.on('error', () => {});
    spec = await declare(channel);
  } catch (error) {
    await connection.close().catch(() => {});
    throw error;
  }

  const pending = new Map();
  let ready = true;
  let buffered = false;

  function failPending(reason) {
    ready = false;
    for (const job of pending.values()) job.finish(new Error(reason));
  }

  channel.on('error', () => failPending('Publisher channel failed'));
  channel.on('close', () => {
    failPending('Publisher channel closed');
    void connection.close().catch(() => {});
  });
  connection.on('close', () => failPending('Broker connection closed'));
  channel.on('drain', () => { buffered = false; });
  channel.on('return', message => {
    const job = pending.get(message.properties.messageId);
    if (job) job.returned = true;
  });

  return {
    channel,
    connection,
    spec,
    isReady: () => ready && !buffered,
    publish(event, routingKey = spec.routingKey, exchange = spec.exchange) {
      if (!ready || buffered) return Promise.reject(new Error('Publisher is not ready'));
      const messageId = event.event_id;
      if (pending.has(messageId)) return Promise.reject(new Error(`The same event_id (${messageId}) is already in flight`));
      return new Promise((resolve, reject) => {
        const job = {
          returned: false,
          timer: null,
          finish(error) {
            if (!pending.has(messageId)) return;
            pending.delete(messageId);
            clearTimeout(job.timer);
            error ? reject(error) : resolve();
          }
        };
        pending.set(messageId, job);
        job.timer = setTimeout(() => job.finish(new Error('Confirm timed out')), 10000);
        try {
          const payloadBuffer = Buffer.from(JSON.stringify(event));
          const writable = channel.publish(exchange, routingKey, payloadBuffer, {
            persistent: true,
            mandatory: true,
            contentType: 'application/json',
            messageId,
            headers: { event_type: event.event_type }
          }, error => job.finish(error || (job.returned ? new Error('Unroutable publication') : null)));
          if (!writable) buffered = true;
        } catch (error) {
          job.finish(error);
        }
      });
    },
    async close() {
      ready = false;
      await channel.close().catch(() => {});
      await connection.close().catch(() => {});
    }
  };
}

module.exports = {
  integer,
  topologySpec,
  declareTopology,
  validateStockEvent,
  openPublisher
};
