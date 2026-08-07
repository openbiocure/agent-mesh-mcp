/**
 * Reply command handlers — channel-agnostic business logic for message replies.
 *
 * All reply identification goes through the notifications table (messageId lookup).
 * No regex on rendered message text — the notification type and title are the source of truth.
 */

import crypto from "crypto";
import prisma, { resolveId } from "../db/index.mjs";
import { publishMessage } from "../events/index.mjs";

/**
 * Handle a reply to a previously sent message.
 * Looks up the notification by messageId and routes based on type.
 *
 * Returns { type, data } or null if not a known notification.
 */
export async function handleReplyMessage(originalMessageId, replyText) {
  const notif = await prisma.notification.findFirst({
    where: { messageId: originalMessageId },
  });
  if (!notif) return null;

  // Deploy notification — return release status
  if (notif.type === "deploy" || notif.type === "warning") {
    const shortId = extractShortId(notif.title);
    if (shortId) {
      const data = await getReleaseNudge(shortId);
      if (data) return { type: "deploy_nudge", data };
    }
  }

  // Approval notification — add comment and notify agent
  if (notif.type === "approval") {
    const shortId = extractShortId(notif.title);
    if (shortId && replyText) {
      const data = await addApprovalComment(shortId, replyText);
      if (data) return { type: "approval_comment", data };
    }
  }

  // Default — route reply back to the agent
  const routingKey = notif.workerSid ? `direct.${notif.workerSid}` : `ask.${notif.workerName}`;
  await publishMessage(routingKey, {
    from: "telegram-reply",
    message: `Human replied to "${notif.title}": ${replyText}`,
  });
  return { type: "routed", data: { workerName: notif.workerName } };
}

/**
 * Extract an 8-char hex short ID from a notification title.
 * Titles follow the pattern "... (abc12345)" or "... abc12345".
 */
function extractShortId(title) {
  const match = title?.match(/([a-f0-9]{8})/);
  return match ? match[1] : null;
}

/**
 * Get release status for a deploy nudge.
 */
async function getReleaseNudge(releaseShortId) {
  const releaseId = await resolveId("releases", releaseShortId);
  const r = await prisma.release.findUnique({
    where: { id: releaseId },
    select: { name: true, status: true, summary: true, startedAt: true },
  });
  if (!r) return null;

  const result = { shortId: releaseShortId, name: r.name, status: r.status };

  if (r.status === "deploying" && r.startedAt) {
    result.elapsedMinutes = Math.round((Date.now() - new Date(r.startedAt).getTime()) / 60000);
  }
  if (r.summary) result.summary = r.summary;

  const activity = await prisma.activity.findFirst({
    where: { workerName: "devops-engineer" },
    orderBy: { timestamp: "desc" },
    select: { content: true, timestamp: true },
  });
  if (activity) {
    result.lastActivity = {
      content: activity.content,
      agoSeconds: Math.round((Date.now() - new Date(activity.timestamp).getTime()) / 1000),
    };
  }

  return result;
}

/**
 * Add a comment to an approval and notify the agent if pending.
 */
async function addApprovalComment(approvalShortId, commentText) {
  const approvalId = await resolveId("approvals", approvalShortId);
  const appr = await prisma.approval.findUnique({
    where: { id: approvalId },
    select: { id: true, workerSid: true, title: true, status: true },
  });
  if (!appr) return null;

  await prisma.approvalComment.create({
    data: { id: crypto.randomUUID(), approvalId: appr.id, author: "human", content: commentText },
  });
  await prisma.approval.update({
    where: { id: appr.id },
    data: { responseNote: commentText },
  });

  let agentNotified = false;
  if (appr.status === "pending" && appr.workerSid) {
    await publishMessage(`direct.${appr.workerSid}`, {
      from: "approval-webhook",
      message: `Refinement feedback on approval "${appr.title}": ${commentText}. Adjust your approach and request_approval again if needed.`,
    });
    agentNotified = true;
  }

  return { approvalShortId, agentNotified };
}
