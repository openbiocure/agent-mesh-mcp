/**
 * Shared command handlers — channel-agnostic data fetching and business logic.
 *
 * Each function returns structured data. Channel adapters (telegram, slack)
 * render and send. No formatting, no sending, no channel-specific logic here.
 */

import prisma, { resolveId } from "../db/index.mjs";
import { renderActivityShort } from "../templates/activity.mjs";

export async function getActiveReleases() {
  return prisma.release.findMany({
    where: { status: { in: ["building", "ready", "deploying", "failed"] } },
    orderBy: { createdAt: "desc" },
    take: 10,
  });
}

export async function getOnlineAgents() {
  const workers = await prisma.worker.findMany({
    where: { status: "online" },
    orderBy: { lastHeartbeat: "desc" },
  });
  const seen = new Set();
  return workers.filter(w => { if (seen.has(w.name)) return false; seen.add(w.name); return true; });
}

export async function getOpenIncidents() {
  const incidents = await prisma.incident.findMany({
    where: { status: { not: "resolved" } },
    orderBy: { createdAt: "desc" },
    take: 10,
  });
  const order = { p1: 1, p2: 2, p3: 3 };
  incidents.sort((a, b) => (order[a.severity] || 4) - (order[b.severity] || 4));
  return incidents;
}

export async function getIncidentStatuses() {
  const incidents = await prisma.incident.findMany({
    where: { status: { in: ["investigating", "open"] } },
    orderBy: { createdAt: "desc" },
    take: 10,
  });
  const order = { p1: 1, p2: 2, p3: 3 };
  incidents.sort((a, b) => (order[a.severity] || 4) - (order[b.severity] || 4));

  const statuses = [];
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
    statuses.push({ incident: inc, agentStatus });
  }
  return statuses;
}

export async function subscribe(subscriberId, shortId) {
  if (!shortId) {
    const releases = await prisma.release.findMany({
      where: { status: { in: ["building", "ready", "deploying"] } },
      orderBy: { createdAt: "desc" },
      take: 10,
    });
    return { action: "pick", releases };
  }

  const releaseId = await resolveId("releases", shortId);
  const rel = await prisma.release.findUnique({ where: { id: releaseId }, select: { id: true, name: true, status: true } });
  if (!rel) return { action: "not_found", shortId };

  const existing = await prisma.subscription.findFirst({
    where: { entityType: "release", entityId: rel.id, chatId: String(subscriberId) },
  });
  if (existing) return { action: "already_subscribed", release: rel };

  await prisma.subscription.create({
    data: { entityType: "release", entityId: rel.id, chatId: String(subscriberId) },
  });
  return { action: "subscribed", release: rel };
}

export async function unsubscribe(subscriberId, shortId) {
  const subId = String(subscriberId);

  if (shortId === "all") {
    const result = await prisma.subscription.deleteMany({ where: { chatId: subId } });
    return { action: "unsubscribed_all", count: result.count };
  }

  if (shortId) {
    const releaseId = await resolveId("releases", shortId).catch(() => null);
    if (!releaseId) return { action: "not_found", shortId };
    const result = await prisma.subscription.deleteMany({
      where: { entityType: "release", entityId: releaseId, chatId: subId },
    });
    return { action: result.count > 0 ? "unsubscribed" : "not_subscribed", shortId };
  }

  const subs = await prisma.subscription.findMany({
    where: { chatId: subId },
    orderBy: { createdAt: "desc" },
  });
  if (subs.length === 0) return { action: "no_subscriptions" };

  const releases = await prisma.release.findMany({
    where: { id: { in: subs.map(s => s.entityId) } },
    select: { id: true, name: true },
  });
  const nameMap = Object.fromEntries(releases.map(r => [r.id, r.name]));
  return { action: "pick", subscriptions: subs, nameMap };
}

export async function toggleQuiet() {
  const current = await prisma.meshSetting.findUnique({ where: { key: "DEPLOY_SCHEDULE_QUIET" } });
  const newVal = current?.value === "true" ? "false" : "true";
  await prisma.meshSetting.upsert({
    where: { key: "DEPLOY_SCHEDULE_QUIET" },
    update: { value: newVal },
    create: { key: "DEPLOY_SCHEDULE_QUIET", value: newVal },
  });
  return newVal === "true";
}
