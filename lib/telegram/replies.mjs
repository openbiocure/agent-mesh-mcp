/**
 * Telegram reply handlers — notification routing, deploy nudge, approval comments
 */

import crypto from "crypto";
import amqplib from "amqplib";
import prisma from "../db.mjs";
import { resolveId } from "../helpers.mjs";
import { sendMessage, TELEGRAM_CHAT_ID } from "./client.mjs";

const EXCHANGE = process.env.EXCHANGE_NAME || "agents";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";

export async function handleReply(message) {
  const origText = message.reply_to_message.text || "";
  const origMsgId = message.reply_to_message.message_id;

  // 1. Check if reply is to a notification — route back to agent
  if (origMsgId && message.text) {
    const notif = await prisma.notification.findFirst({
      where: { messageId: origMsgId },
    });
    if (notif) {
      try {
        const routingKey = notif.workerSid ? `direct.${notif.workerSid}` : `ask.${notif.workerName}`;
        const conn = await amqplib.connect(RABBITMQ_URL);
        const ch = await conn.createConfirmChannel();
        await ch.assertExchange(EXCHANGE, "topic", { durable: true });
        ch.publish(EXCHANGE, routingKey,
          Buffer.from(JSON.stringify({
            from: "telegram-reply",
            message: `Human replied to "${notif.title}": ${message.text}`,
          })),
          { contentType: "application/json" }
        );
        await ch.waitForConfirms().catch(() => {});
        await conn.close();
        await sendMessage(message.chat.id, `↩️ Reply sent to ${notif.workerName}`, { reply_to_message_id: message.message_id });
      } catch (e) {
        console.error("Reply routing failed:", e.message);
      }
      return true;
    }
  }

  // 2. Deploy nudge — reply to deploy notification for status
  const deployMatch = origText.match(/(?:Deploying|DEPLOYED|FAILED|HOTFIX|Release)[:\s]*([^\n]*?)\(([a-f0-9]{8})\)/i);
  if (deployMatch) {
    const releaseShortId = deployMatch[2];
    try {
      const releaseId = await resolveId("releases", releaseShortId);
      const r = await prisma.release.findUnique({
        where: { id: releaseId },
        select: { name: true, status: true, summary: true, startedAt: true },
      });
      if (r) {
        let statusText = `📋 Release ${releaseShortId}: ${r.status}\n${r.name}`;
        if (r.status === "deploying" && r.startedAt) {
          const elapsed = Math.round((Date.now() - new Date(r.startedAt).getTime()) / 60000);
          statusText += `\nDeploying for ${elapsed}min`;
        }
        if (r.summary) statusText += `\n\n${r.summary.slice(0, 500)}`;
        const activity = await prisma.activity.findFirst({
          where: { workerName: "devops-engineer" },
          orderBy: { timestamp: "desc" },
          select: { content: true, timestamp: true },
        });
        if (activity) {
          const ago = Math.round((Date.now() - new Date(activity.timestamp).getTime()) / 1000);
          statusText += `\n\nLast devops activity (${ago}s ago): ${activity.content?.slice(0, 200) || "unknown"}`;
        }
        await sendMessage(message.chat.id, statusText, { reply_to_message_id: message.message_id });
      }
    } catch {}
    return true;
  }

  // 3. Approval comment — reply to approval notification
  const match = origText.match(/(?:ID:|Refine)\s*([a-f0-9]{8})/);
  if (match && message.text) {
    try {
      const approvalId = await resolveId("approvals", match[1]);
      const appr = await prisma.approval.findUnique({
        where: { id: approvalId },
        select: { id: true, workerSid: true, requestedBy: true, title: true, status: true },
      });
      if (appr) {
        await prisma.approvalComment.create({
          data: { id: crypto.randomUUID(), approvalId: appr.id, author: "human", content: message.text },
        });
        await prisma.approval.update({
          where: { id: appr.id },
          data: { responseNote: message.text },
        });

        if (appr.status === "pending" && appr.workerSid) {
          try {
            const conn = await amqplib.connect(RABBITMQ_URL);
            const ch = await conn.createConfirmChannel();
            await ch.assertExchange(EXCHANGE, "topic", { durable: true });
            ch.publish(EXCHANGE, `direct.${appr.workerSid}`,
              Buffer.from(JSON.stringify({
                from: "approval-webhook",
                message: `Refinement feedback on approval "${appr.title}": ${message.text}. Adjust your approach and request_approval again if needed.`,
              })),
              { contentType: "application/json" }
            );
            await ch.waitForConfirms().catch(() => {});
            await conn.close();
          } catch (e) {
            console.error("Failed to notify agent of refinement:", e.message);
          }
        }

        await sendMessage(TELEGRAM_CHAT_ID, `💬 Comment added to ${match[1]}${appr.status === "pending" ? " — agent notified" : ""}`);
      }
    } catch {}
    return true;
  }

  return false;
}
