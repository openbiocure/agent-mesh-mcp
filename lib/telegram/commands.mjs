/**
 * Telegram command adapter — calls shared command handlers, renders via Telegram channel.
 */

import { sendMessage } from "./client.mjs";
import {
  getActiveReleases, getOnlineAgents, getOpenIncidents,
  getIncidentStatuses, subscribe, unsubscribe, toggleQuiet,
} from "../commands/index.mjs";
import { renderReleaseList, renderAgentList, renderIncidentList, renderIncidentStatus, renderIncidentStatusList } from "../templates/index.mjs";
import { formatReleaseList, formatAgentList, formatIncidentList, formatIncidentStatusList, formatIncidentStatusLine } from "../channels/telegram.mjs";

export async function handleCommand(message) {
  const cmd = message.text.split(" ")[0].toLowerCase();
  const chatId = message.chat.id;
  const arg = message.text.split(" ")[1];

  if (cmd === "/releases") return handleReleases(chatId);
  if (cmd === "/agents") return handleAgents(chatId);
  if (cmd === "/incidents") return handleIncidents(chatId);
  if (cmd === "/status") return handleStatus(chatId);
  if (cmd === "/subscribe") return handleSubscribe(chatId, arg);
  if (cmd === "/unsubscribe") return handleUnsubscribe(chatId, arg);
  if (cmd === "/quiet") return handleQuiet(chatId);
  if (cmd === "/help") return handleHelp(chatId);
  return false;
}

async function handleReleases(chatId) {
  const releases = await getActiveReleases();
  if (releases.length === 0) {
    await sendMessage(chatId, "No active releases.");
  } else {
    await sendMessage(chatId, formatReleaseList(renderReleaseList(releases)));
  }
  return true;
}

async function handleAgents(chatId) {
  const agents = await getOnlineAgents();
  await sendMessage(chatId, formatAgentList(renderAgentList(agents)));
  return true;
}

async function handleIncidents(chatId) {
  const incidents = await getOpenIncidents();
  if (incidents.length === 0) {
    await sendMessage(chatId, "No open incidents.");
  } else {
    await sendMessage(chatId, formatIncidentList(renderIncidentList(incidents)));
  }
  return true;
}

async function handleStatus(chatId) {
  const statuses = await getIncidentStatuses();
  if (statuses.length === 0) {
    await sendMessage(chatId, "No active incidents.");
    return true;
  }

  const lines = statuses.map(({ incident, agentStatus }) =>
    formatIncidentStatusLine(renderIncidentStatus(incident, agentStatus))
  );

  const buttons = statuses
    .filter(({ incident }) => incident.assignedAgent)
    .map(({ incident }) => [
      { text: `🔍 Tail ${incident.id.slice(0, 8)}`, callback_data: `tail:${incident.id}` },
      { text: `⚡ Nudge ${incident.id.slice(0, 8)}`, callback_data: `nudge:${incident.id}` },
    ]);

  const statusList = renderIncidentStatusList(statuses.map(s => s.incident));
  const text = formatIncidentStatusList(statusList, lines);
  await sendMessage(chatId, text, {
    reply_markup: buttons.length ? { inline_keyboard: buttons } : undefined,
  });
  return true;
}

async function handleSubscribe(chatId, shortId) {
  const result = await subscribe(chatId, shortId);

  if (result.action === "pick") {
    if (result.releases.length === 0) {
      await sendMessage(chatId, "No active releases to subscribe to.");
    } else {
      const buttons = result.releases.map(r => [{ text: `🔔 ${r.name}`, callback_data: `sub:${r.id}` }]);
      await sendMessage(chatId, "Subscribe to a release:", { reply_markup: { inline_keyboard: buttons } });
    }
  } else if (result.action === "not_found") {
    await sendMessage(chatId, `No release found matching '${result.shortId}'`);
  } else if (result.action === "already_subscribed") {
    await sendMessage(chatId, `Already subscribed to ${result.release.name}`);
  } else if (result.action === "subscribed") {
    await sendMessage(chatId, `🔔 Subscribed to: ${result.release.name} (${result.release.id.slice(0, 8)})\nStatus: ${result.release.status}`);
  }
  return true;
}

async function handleUnsubscribe(chatId, shortId) {
  const result = await unsubscribe(chatId, shortId);

  if (result.action === "unsubscribed_all") {
    await sendMessage(chatId, result.count > 0 ? `Unsubscribed from all (${result.count} removed).` : "No active subscriptions.");
  } else if (result.action === "not_found") {
    await sendMessage(chatId, `No release found matching '${result.shortId}'`);
  } else if (result.action === "unsubscribed") {
    await sendMessage(chatId, "Unsubscribed.");
  } else if (result.action === "not_subscribed") {
    await sendMessage(chatId, `No subscription found for '${result.shortId}'`);
  } else if (result.action === "no_subscriptions") {
    await sendMessage(chatId, "No active subscriptions.");
  } else if (result.action === "pick") {
    const buttons = result.subscriptions.map(s => [
      { text: `❌ ${result.nameMap[s.entityId] || s.entityId.slice(0, 8)}`, callback_data: `unsub:${s.entityId}` },
    ]);
    buttons.push([{ text: "🗑 Unsubscribe All", callback_data: "unsub:all" }]);
    await sendMessage(chatId, `🔔 Your subscriptions (${result.subscriptions.length}):`, { reply_markup: { inline_keyboard: buttons } });
  }
  return true;
}

async function handleQuiet(chatId) {
  const isQuiet = await toggleQuiet();
  await sendMessage(chatId, isQuiet ? "🔇 Deploy schedule silenced" : "🔊 Deploy schedule verbose");
  return true;
}

async function handleHelp(chatId) {
  await sendMessage(chatId, "/releases — active releases\n/agents — online agents\n/incidents — open incidents\n/status — who's working on what\n/subscribe — subscribe to release updates\n/unsubscribe — manage subscriptions\n/quiet — toggle deploy notifications\n/help — this message");
  return true;
}
