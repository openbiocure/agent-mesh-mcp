/**
 * Telegram callback handlers — button presses (approve, reject, sub, unsub, tail, nudge)
 */

import amqplib from "amqplib";
import prisma from "../db.mjs";
import { sendMessage, editMessage, answerCallback } from "./client.mjs";
import { resolveId } from "../helpers.mjs";

const EXCHANGE = process.env.EXCHANGE_NAME || "agents";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";

async function getTopicMap() {
  const setting = await prisma.meshSetting.findUnique({ where: { key: "HOTFIX_TOPIC_MAP" } });
  return setting?.value ? JSON.parse(setting.value) : {};
}

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
  const inc = await prisma.incident.findFirst({
    where: { id: incidentId },
    select: { assignedAgent: true },
  });
  if (!inc?.assignedAgent) return true;

  const activities = await prisma.activity.findMany({
    where: { workerName: { contains: inc.assignedAgent.replace("backend-engineer", "backend") } },
    orderBy: { timestamp: "desc" },
    take: 5,
    select: { content: true, timestamp: true },
  });

  let text;
  if (activities.length === 0) {
    text = `No recent activity from ${inc.assignedAgent}`;
  } else {
    text = `🔍 ${inc.assignedAgent} — last ${activities.length} actions:\n\n` +
      activities.map(a => {
        const ago = Math.round((Date.now() - new Date(a.timestamp).getTime()) / 1000);
        const agoStr = ago < 60 ? `${ago}s` : ago < 3600 ? `${Math.round(ago/60)}m` : `${Math.round(ago/3600)}h`;
        return `${agoStr} ago: ${(a.content || "").slice(0, 120)}`;
      }).join("\n\n");
  }

  await answerCallback(callback.id, "Tailing...");
  await sendMessage(callback.message.chat.id, text);
  return true;
}

async function handleNudge(callback, incidentId) {
  const inc = await prisma.incident.findFirst({
    where: { id: incidentId },
    select: { id: true, title: true, severity: true, repo: true },
  });
  if (!inc) return true;

  const topicMap = await getTopicMap();
  const topic = topicMap[inc.repo] || "prod-ops";

  try {
    const conn = await amqplib.connect(RABBITMQ_URL);
    const ch = await conn.createConfirmChannel();
    await ch.assertExchange(EXCHANGE, "topic", { durable: true });
    ch.publish(EXCHANGE, `ask.${topic}`,
      Buffer.from(JSON.stringify({
        from: "telegram-nudge",
        message: `NUDGE: ${inc.severity.toUpperCase()} incident ${inc.id.slice(0, 8)}: ${inc.title}. This has been stale. Investigate and fix NOW.`,
        async: true,
      })),
      { contentType: "application/json" }
    );
    await ch.waitForConfirms().catch(() => {});
    await conn.close();
  } catch (e) {
    console.error("Nudge dispatch failed:", e.message);
  }

  await answerCallback(callback.id, `Nudged ${topic}`);
  return true;
}

async function handleUnsub(callback, entityId, chatId) {
  let count;
  if (entityId === "all") {
    const result = await prisma.subscription.deleteMany({ where: { chatId } });
    count = result.count;
  } else {
    const result = await prisma.subscription.deleteMany({ where: { entityId, chatId } });
    count = result.count;
  }
  await answerCallback(callback.id, count > 0 ? "Unsubscribed" : "Already unsubscribed");
  await editMessage(callback.message.chat.id, callback.message.message_id,
    entityId === "all" ? "🔕 All subscriptions removed." : "🔕 Unsubscribed.");
  return true;
}

async function handleSub(callback, releaseId, chatId) {
  const existing = await prisma.subscription.findFirst({
    where: { entityType: "release", entityId: releaseId, chatId },
  });
  if (!existing) {
    await prisma.subscription.create({
      data: { entityType: "release", entityId: releaseId, chatId },
    });
  }
  const rel = await prisma.release.findFirst({ where: { id: releaseId }, select: { name: true } });
  await answerCallback(callback.id, "Subscribed!");
  await editMessage(callback.message.chat.id, callback.message.message_id,
    `🔔 Subscribed to ${rel?.name || releaseId.slice(0, 8)}`);
  return true;
}

async function handleApproval(callback, action, approvalId, chatId) {
  const status = action === "approve" ? "approved" : "rejected";
  const approver = `${callback.from.first_name || callback.from.username || "unknown"} (${chatId})`;

  const { count: rowsAffected } = await prisma.approval.updateMany({
    where: { id: approvalId, status: "pending" },
    data: { status, respondedAt: new Date(), approvedBy: approver },
  });

  await answerCallback(callback.id,
    rowsAffected > 0 ? `${status.charAt(0).toUpperCase() + status.slice(1)}!` : "Already responded");

  if (rowsAffected > 0) {
    await editMessage(callback.message.chat.id, callback.message.message_id,
      callback.message.text + `\n\n${status === "approved" ? "✅" : "❌"} ${status.toUpperCase()} at ${new Date().toISOString().slice(0, 19)}`);

    try {
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
        const conn = await amqplib.connect(RABBITMQ_URL);
        const ch = await conn.createConfirmChannel();
        await ch.assertExchange(EXCHANGE, "topic", { durable: true });
        ch.publish(EXCHANGE, routingKey,
          Buffer.from(JSON.stringify({ from: "approval-webhook", message: followUpMessage })),
          { contentType: "application/json" }
        );
        await ch.waitForConfirms().catch(() => {});
        await conn.close();
        console.log(`Approval ${status}: dispatched follow-up to ${routingKey}`);
      }
    } catch (mqErr) {
      console.error("Failed to dispatch approval follow-up:", mqErr.message);
    }
  }
  return true;
}
