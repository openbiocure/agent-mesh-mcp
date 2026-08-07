/**
 * Telegram callback adapter — renders callback results from shared command handlers.
 */

import { sendMessage, editMessage, answerCallback } from "./client.mjs";
import { formatActivityTail } from "../channels/telegram.mjs";
import {
  getIncidentTail, nudgeIncident, respondToApproval,
  subscribeToRelease, unsubscribeEntity,
} from "../commands/callbacks.mjs";

export async function handleCallback(callback) {
  const data = callback.data;
  const [action, callbackId] = data.split(":");
  const chatId = String(callback.from.id);

  if (action === "tail") return handleTail(callback, callbackId);
  if (action === "nudge") return handleNudge(callback, callbackId);
  if (action === "unsub") return handleUnsub(callback, callbackId, chatId);
  if (action === "sub") return handleSub(callback, callbackId, chatId);
  if (action === "approve" || action === "reject") return handleApproval(callback, action, callbackId, chatId);
  return false;
}

async function handleTail(callback, incidentId) {
  const result = await getIncidentTail(incidentId);
  if (!result) return true;

  const text = formatActivityTail(result.agent, result.activities);
  await answerCallback(callback.id, "Tailing...");
  await sendMessage(callback.message.chat.id, text);
  return true;
}

async function handleNudge(callback, incidentId) {
  const topic = await nudgeIncident(incidentId);
  await answerCallback(callback.id, topic ? `Nudged ${topic}` : "Incident not found");
  return true;
}

async function handleUnsub(callback, entityId, chatId) {
  const count = await unsubscribeEntity(entityId, chatId);
  await answerCallback(callback.id, count > 0 ? "Unsubscribed" : "Already unsubscribed");
  await editMessage(callback.message.chat.id, callback.message.message_id,
    entityId === "all" ? "🔕 All subscriptions removed." : "🔕 Unsubscribed.");
  return true;
}

async function handleSub(callback, releaseId, chatId) {
  const releaseName = await subscribeToRelease(releaseId, chatId);
  await answerCallback(callback.id, "Subscribed!");
  await editMessage(callback.message.chat.id, callback.message.message_id,
    `🔔 Subscribed to ${releaseName}`);
  return true;
}

async function handleApproval(callback, action, approvalId, chatId) {
  const approverName = `${callback.from.first_name || callback.from.username || "unknown"} (${chatId})`;
  const result = await respondToApproval(approvalId, action, approverName);

  await answerCallback(callback.id,
    result.updated ? `${result.status.charAt(0).toUpperCase() + result.status.slice(1)}!` : "Already responded");

  if (result.updated) {
    const icon = result.status === "approved" ? "✅" : "❌";
    await editMessage(callback.message.chat.id, callback.message.message_id,
      callback.message.text + `\n\n${icon} ${result.status.toUpperCase()} at ${new Date().toISOString().slice(0, 19)}`);
  }
  return true;
}
