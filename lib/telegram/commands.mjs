/**
 * Telegram bot commands — /releases, /agents, /incidents, /status, /subscribe, /unsubscribe, /quiet, /help
 */

import prisma from "../db.mjs";
import { resolveId } from "../helpers.mjs";
import { sendMessage } from "./client.mjs";
import { renderReleaseList, renderAgentList, renderIncidentList, renderIncidentStatus, renderIncidentStatusList } from "../templates/index.mjs";
import { renderActivityShort } from "../templates/activity.mjs";
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
  const releases = await prisma.release.findMany({
    where: { status: { in: ["building", "ready", "deploying", "failed"] } },
    orderBy: { createdAt: "desc" },
    take: 10,
  });
  if (releases.length === 0) {
    await sendMessage(chatId, "No active releases.");
  } else {
    const rendered = renderReleaseList(releases);
    const text = formatReleaseList(rendered);
    await sendMessage(chatId, text);
  }
  return true;
}

async function handleAgents(chatId) {
  const workers = await prisma.worker.findMany({
    where: { status: "online" },
    orderBy: { lastHeartbeat: "desc" },
  });
  const seen = new Set();
  const unique = workers.filter(w => { if (seen.has(w.name)) return false; seen.add(w.name); return true; });
  const rendered = renderAgentList(unique);
  const text = formatAgentList(rendered);
  await sendMessage(chatId, text);
  return true;
}

async function handleIncidents(chatId) {
  const incidents = await prisma.incident.findMany({
    where: { status: { not: "resolved" } },
    orderBy: { createdAt: "desc" },
    take: 10,
  });
  // Sort by severity in JS since Prisma can't do CASE ORDER
  const order = { p1: 1, p2: 2, p3: 3 };
  incidents.sort((a, b) => (order[a.severity] || 4) - (order[b.severity] || 4));

  if (incidents.length === 0) {
    await sendMessage(chatId, "No open incidents.");
  } else {
    const rendered = renderIncidentList(incidents);
    const text = formatIncidentList(rendered);
    await sendMessage(chatId, text);
  }
  return true;
}

async function handleStatus(chatId) {
  const incidents = await prisma.incident.findMany({
    where: { status: { in: ["investigating", "open"] } },
    orderBy: { createdAt: "desc" },
    take: 10,
  });
  const order = { p1: 1, p2: 2, p3: 3 };
  incidents.sort((a, b) => (order[a.severity] || 4) - (order[b.severity] || 4));

  if (incidents.length === 0) {
    await sendMessage(chatId, "No active incidents.");
    return true;
  }

  const lines = [];
  for (const inc of incidents) {
    let agentStatus = "unassigned";
    if (inc.assignedAgent) {
      const act = await prisma.activity.findFirst({
        where: { workerName: { contains: inc.assignedAgent.replace("backend-engineer", "backend") } },
        orderBy: { timestamp: "desc" },
        select: { content: true, timestamp: true },
      });
      if (act) {
        const activity = renderActivityShort(act);
        agentStatus = `${inc.assignedAgent} (${activity.timeAgo})\n   ${activity.text}`;
      } else {
        agentStatus = `${inc.assignedAgent} (no activity)`;
      }
    }
    const rendered = renderIncidentStatus(inc, agentStatus);
    lines.push(formatIncidentStatusLine(rendered));
  }

  const buttons = incidents
    .filter(i => i.assignedAgent)
    .map(i => [
      { text: `🔍 Tail ${i.id.slice(0, 8)}`, callback_data: `tail:${i.id}` },
      { text: `⚡ Nudge ${i.id.slice(0, 8)}`, callback_data: `nudge:${i.id}` },
    ]);

  const statusList = renderIncidentStatusList(incidents);
  const text = formatIncidentStatusList(statusList, lines);
  await sendMessage(chatId, text, {
    reply_markup: buttons.length ? { inline_keyboard: buttons } : undefined,
  });
  return true;
}

async function handleSubscribe(chatId, shortId) {
  if (!shortId) {
    const releases = await prisma.release.findMany({
      where: { status: { in: ["building", "ready", "deploying"] } },
      orderBy: { createdAt: "desc" },
      take: 10,
    });
    if (releases.length === 0) {
      await sendMessage(chatId, "No active releases to subscribe to.");
    } else {
      const buttons = releases.map(r => [{ text: `🔔 ${r.name}`, callback_data: `sub:${r.id}` }]);
      await sendMessage(chatId, "Subscribe to a release:", { reply_markup: { inline_keyboard: buttons } });
    }
    return true;
  }

  const releaseId = await resolveId("releases", shortId);
  const rel = await prisma.release.findUnique({ where: { id: releaseId }, select: { id: true, name: true, status: true } });
  if (!rel) { await sendMessage(chatId, `No release found matching '${shortId}'`); return true; }

  const existing = await prisma.subscription.findFirst({
    where: { entityType: "release", entityId: rel.id, chatId: String(chatId) },
  });
  if (existing) { await sendMessage(chatId, `Already subscribed to ${rel.name}`); return true; }

  await prisma.subscription.create({
    data: { entityType: "release", entityId: rel.id, chatId: String(chatId) },
  });
  await sendMessage(chatId, `🔔 Subscribed to: ${rel.name} (${rel.id.slice(0, 8)})\nStatus: ${rel.status}`);
  return true;
}

async function handleUnsubscribe(chatId, shortId) {
  const chatIdStr = String(chatId);
  if (shortId === "all") {
    const result = await prisma.subscription.deleteMany({ where: { chatId: chatIdStr } });
    await sendMessage(chatId, result.count > 0 ? `Unsubscribed from all (${result.count} removed).` : "No active subscriptions.");
    return true;
  }
  if (shortId) {
    const releaseId = await resolveId("releases", shortId).catch(() => null);
    if (!releaseId) { await sendMessage(chatId, `No release found matching '${shortId}'`); return true; }
    const result = await prisma.subscription.deleteMany({ where: { entityType: "release", entityId: releaseId, chatId: chatIdStr } });
    await sendMessage(chatId, result.count > 0 ? "Unsubscribed." : `No subscription found for '${shortId}'`);
    return true;
  }

  const subs = await prisma.subscription.findMany({
    where: { chatId: chatIdStr },
    orderBy: { createdAt: "desc" },
  });
  if (subs.length === 0) { await sendMessage(chatId, "No active subscriptions."); return true; }

  // Get release names
  const releases = await prisma.release.findMany({
    where: { id: { in: subs.map(s => s.entityId) } },
    select: { id: true, name: true },
  });
  const nameMap = Object.fromEntries(releases.map(r => [r.id, r.name]));

  const buttons = subs.map(s => [{ text: `❌ ${nameMap[s.entityId] || s.entityId.slice(0, 8)}`, callback_data: `unsub:${s.entityId}` }]);
  buttons.push([{ text: "🗑 Unsubscribe All", callback_data: "unsub:all" }]);
  await sendMessage(chatId, `🔔 Your subscriptions (${subs.length}):`, { reply_markup: { inline_keyboard: buttons } });
  return true;
}

async function handleQuiet(chatId) {
  const current = await prisma.meshSetting.findUnique({ where: { key: "DEPLOY_SCHEDULE_QUIET" } });
  const newVal = current?.value === "true" ? "false" : "true";
  await prisma.meshSetting.upsert({ where: { key: "DEPLOY_SCHEDULE_QUIET" }, update: { value: newVal }, create: { key: "DEPLOY_SCHEDULE_QUIET", value: newVal } });
  await sendMessage(chatId, newVal === "true" ? "🔇 Deploy schedule silenced" : "🔊 Deploy schedule verbose");
  return true;
}

async function handleHelp(chatId) {
  await sendMessage(chatId, "/releases — active releases\n/agents — online agents\n/incidents — open incidents\n/status — who's working on what\n/subscribe — subscribe to release updates\n/unsubscribe — manage subscriptions\n/quiet — toggle deploy notifications\n/help — this message");
  return true;
}
