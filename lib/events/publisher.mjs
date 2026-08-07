/**
 * RabbitMQ publisher — two flavors:
 *
 * publishEvent(routingKey, data)   — domain events (event.incident.created, etc.)
 * publishMessage(routingKey, data) — direct agent messages (ask.prod-ops, direct.<sid>)
 */

import amqplib from "amqplib";

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";
const EXCHANGE = process.env.EXCHANGE_NAME || "agents";

async function publish(routingKey, payload) {
  try {
    const conn = await amqplib.connect(RABBITMQ_URL);
    const ch = await conn.createConfirmChannel();
    await ch.assertExchange(EXCHANGE, "topic", { durable: true });
    ch.publish(EXCHANGE, routingKey,
      Buffer.from(JSON.stringify(payload)),
      { contentType: "application/json" }
    );
    await ch.waitForConfirms().catch(() => {});
    await conn.close();
  } catch (err) {
    console.error(`Publish failed (${routingKey}):`, err.message);
  }
}

/**
 * Publish a domain event — adds event name and timestamp to payload.
 */
export async function publishEvent(routingKey, data) {
  await publish(routingKey, {
    event: routingKey,
    timestamp: new Date().toISOString(),
    ...data,
  });
}

/**
 * Publish a direct message to an agent — used for replies, nudges, approval follow-ups.
 */
export async function publishMessage(routingKey, data) {
  await publish(routingKey, data);
}
