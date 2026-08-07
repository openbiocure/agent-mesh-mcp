/**
 * Callback command handlers — channel-agnostic business logic for button actions.
 */

import prisma, { resolveId } from "../db/index.mjs";
import { publishMessage } from "../events/index.mjs";
import { renderActivity } from "../templates/activity.mjs";

/**
 * Get recent activity tail for an incident's assigned agent.
 * Returns { agent, activities[] } or null.
 */
export async function getIncidentTail(incidentId) {
  const inc = await prisma.incident.findFirst({
    where: { id: incidentId },
    select: { assignedAgent: true },
  });
  if (!inc?.assignedAgent) return null;

  const activities = await prisma.activity.findMany({
    where: { workerName: { contains: inc.assignedAgent.replace("backend-engineer", "backend") } },
    orderBy: { timestamp: "desc" },
    take: 5,
    select: { content: true, timestamp: true },
  });

  return { agent: inc.assignedAgent, activities: activities.map(renderActivity) };
}

/**
 * Nudge an incident — dispatch an urgent message to the responsible agent.
 * Returns the topic that was nudged.
 */
export async function nudgeIncident(incidentId) {
  const inc = await prisma.incident.findFirst({
    where: { id: incidentId },
    select: { id: true, title: true, severity: true, repo: true },
  });
  if (!inc) return null;

  const setting = await prisma.meshSetting.findUnique({ where: { key: "HOTFIX_TOPIC_MAP" } });
  const topicMap = setting?.value ? JSON.parse(setting.value) : {};
  const topic = topicMap[inc.repo] || "prod-ops";

  await publishMessage(`ask.${topic}`, {
    from: "telegram-nudge",
    message: `NUDGE: ${inc.severity.toUpperCase()} incident ${inc.id.slice(0, 8)}: ${inc.title}. This has been stale. Investigate and fix NOW.`,
    async: true,
  });

  return topic;
}

/**
 * Handle approval response (approve/reject).
 * Returns { updated, status, approval, routingKey } or { updated: false }.
 */
export async function respondToApproval(approvalId, action, approverName) {
  const status = action === "approve" ? "approved" : "rejected";

  const { count: rowsAffected } = await prisma.approval.updateMany({
    where: { id: approvalId, status: "pending" },
    data: { status, respondedAt: new Date(), approvedBy: approverName },
  });

  if (rowsAffected === 0) return { updated: false };

  const appr = await prisma.approval.findUnique({
    where: { id: approvalId },
    select: { title: true, description: true, requestedBy: true, workerSid: true, responseNote: true },
  });

  if (appr) {
    const worker = await prisma.worker.findFirst({
      where: { name: appr.requestedBy, status: "online" },
      orderBy: { lastHeartbeat: "desc" },
      select: { topics: true },
    });
    const topic = worker?.topics?.length > 0 ? worker.topics[0] : `ask.${appr.requestedBy}`;
    const note = appr.responseNote ? `\n\nNote from human: ${appr.responseNote}` : "";
    const followUpMessage = status === "approved"
      ? `Approval APPROVED: "${appr.title}". Proceed with the action you requested. Description: ${appr.description || "n/a"}${note}`
      : `Approval REJECTED: "${appr.title}". Do NOT proceed. ${appr.description || ""}${note}`;

    const routingKey = appr.workerSid ? `direct.${appr.workerSid}` : topic;
    await publishMessage(routingKey, { from: "approval-webhook", message: followUpMessage });
  }

  return { updated: true, status, approval: appr };
}

/**
 * Subscribe to a release via button press.
 * Returns the release name.
 */
export async function subscribeToRelease(releaseId, subscriberId) {
  const existing = await prisma.subscription.findFirst({
    where: { entityType: "release", entityId: releaseId, chatId: subscriberId },
  });
  if (!existing) {
    await prisma.subscription.create({
      data: { entityType: "release", entityId: releaseId, chatId: subscriberId },
    });
  }
  const rel = await prisma.release.findFirst({ where: { id: releaseId }, select: { name: true } });
  return rel?.name || releaseId.slice(0, 8);
}

/**
 * Unsubscribe via button press.
 * Returns count of removed subscriptions.
 */
export async function unsubscribeEntity(entityId, subscriberId) {
  if (entityId === "all") {
    const result = await prisma.subscription.deleteMany({ where: { chatId: subscriberId } });
    return result.count;
  }
  const result = await prisma.subscription.deleteMany({ where: { entityId, chatId: subscriberId } });
  return result.count;
}
