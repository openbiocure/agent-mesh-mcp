/**
 * Domain event dispatcher — decouples tool handlers from side effects.
 *
 * Tools emit domain events after DB writes. Listeners handle side effects
 * (notifications, RabbitMQ events, deploy queue, subscriber alerts).
 *
 * Usage:
 *   import { emit, on } from "./dispatcher.mjs";
 *
 *   // Register listener (in lib/listeners.mjs)
 *   on("incident.created", async (data) => { ... });
 *
 *   // Emit from tool handler
 *   await emit("incident.created", { id, severity, title, repo });
 */

const listeners = new Map();

/**
 * Register a listener for a domain event.
 * Supports exact match and wildcard (e.g. "incident.*" matches "incident.created").
 */
export function on(event, handler) {
  if (!listeners.has(event)) listeners.set(event, []);
  listeners.get(event).push(handler);
}

/**
 * Emit a domain event — runs all matching listeners concurrently.
 * Errors in listeners are logged but never bubble up to the tool handler.
 */
export async function emit(event, data) {
  const handlers = [];

  for (const [pattern, fns] of listeners) {
    if (pattern === event || (pattern.endsWith(".*") && event.startsWith(pattern.slice(0, -1)))) {
      handlers.push(...fns);
    }
  }

  if (handlers.length === 0) return;

  await Promise.allSettled(
    handlers.map((fn) =>
      fn({ event, ...data }).catch((err) =>
        console.error(`Listener error (${event}):`, err.message)
      )
    )
  );
}
