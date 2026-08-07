/**
 * Event system — dispatcher + RabbitMQ publisher.
 */

export { emit, on } from "./dispatcher.mjs";
export { publishEvent, publishMessage } from "./publisher.mjs";
