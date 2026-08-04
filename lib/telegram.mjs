/**
 * Telegram webhook handler — extracted from server.mjs.
 * Handles approval approve/reject button callbacks and reply-based comments.
 */

import crypto from "crypto";
import amqplib from "amqplib";
import prisma from "./db.mjs";

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "8808069971:AAHGimNxHN7GssOterrZjU-pHSvZ78IsoCo";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "1329256217";
const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || "d341662f8d119512eef10545dc51a4b8ad233f5af1d5ed4a977741dcf4aab26c";
const EXCHANGE = process.env.EXCHANGE_NAME || "agents";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";

/**
 * Express handler for POST /telegram/webhook
 */
export async function telegramWebhookHandler(req, res) {
  // Verify Telegram secret token
  if (req.headers["x-telegram-bot-api-secret-token"] !== TELEGRAM_WEBHOOK_SECRET) {
    res.status(403).json({ error: "forbidden" });
    return;
  }
  try {
    const callback = req.body?.callback_query;
    if (!callback) {
      // Regular message — could be a comment reply
      const message = req.body?.message;
      if (message?.reply_to_message && message?.text) {
        // Extract approval ID from the original message
        // Match "ID: abc12345" or "Refine abc12345"
        const match = message.reply_to_message.text?.match(/(?:ID:|Refine)\s*([a-f0-9]{8})/);
        if (match) {
          const approvals = await prisma.$queryRaw`SELECT id::text as id, worker_sid as "workerSid", requested_by as "requestedBy", title, status FROM approvals WHERE id::text LIKE ${match[1] + "%"}`;
          if (approvals.length === 1) {
            const appr = approvals[0];
            await prisma.approvalComment.create({
              data: {
                id: crypto.randomUUID(),
                approvalId: appr.id,
                author: "human",
                content: message.text,
              },
            });

            // Store as response_note on the approval
            await prisma.$executeRaw`UPDATE approvals SET response_note = ${message.text} WHERE id::text = ${appr.id}`;

            // If approval is still pending, notify the agent about the feedback
            if (appr.status === "pending" && appr.workerSid) {
              try {
                const conn = await amqplib.connect(RABBITMQ_URL);
                const ch = await conn.createConfirmChannel();
                await ch.assertExchange(EXCHANGE, "topic", { durable: true });
                ch.publish(
                  EXCHANGE,
                  `direct.${appr.workerSid}`,
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

            await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: `\u{1F4AC} Comment added to ${match[1]}${appr.status === "pending" ? " — agent notified" : ""}` }),
            });
          }
        }
      }
      res.json({ ok: true });
      return;
    }

    // Check allowed users from DB
    const chatId = String(callback.from.id);
    try {
      const setting = await prisma.meshSetting.findUnique({ where: { key: "TELEGRAM_ALLOWED_USERS" } });
      const allowed = setting?.value ? setting.value.split(",").map((s) => s.trim()) : [TELEGRAM_CHAT_ID];
      if (!allowed.includes(chatId)) {
        await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/answerCallbackQuery`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ callback_query_id: callback.id, text: "Not authorized", show_alert: true }),
        });
        res.json({ ok: true });
        return;
      }
    } catch {
      /* if DB fails, fall through with default check */
    }

    // Button callback
    const data = callback.data;
    const [action, approvalId] = data.split(":");

    if (!approvalId || !["approve", "reject"].includes(action)) {
      res.json({ ok: true });
      return;
    }

    const status = action === "approve" ? "approved" : "rejected";

    const updated = await prisma.approval.updateMany({
      where: { id: approvalId, status: "pending" },
      data: { status, respondedAt: new Date() },
    });

    // Answer the callback (removes loading spinner on button)
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/answerCallbackQuery`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        callback_query_id: callback.id,
        text: updated.count > 0 ? `${status.charAt(0).toUpperCase() + status.slice(1)}!` : "Already responded",
      }),
    });

    // Update the message to show the result
    if (updated.count > 0) {
      await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/editMessageText`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: callback.message.chat.id,
          message_id: callback.message.message_id,
          text: callback.message.text + `\n\n${status === "approved" ? "\u2705" : "\u274C"} ${status.toUpperCase()} at ${new Date().toISOString().slice(0, 19)}`,
        }),
      });

      // Dispatch follow-up task to the requesting agent via RabbitMQ
      try {
        const appr = await prisma.approval.findUnique({
          where: { id: approvalId },
          select: { title: true, description: true, requestedBy: true, workerSid: true },
        });
        if (appr) {
          // Find which topic the requesting agent listens on
          const worker = await prisma.worker.findFirst({
            where: { name: appr.requestedBy, status: "online" },
            orderBy: { lastHeartbeat: "desc" },
            select: { topics: true },
          });
          const topic =
            worker && worker.topics?.length > 0 ? worker.topics[0] : `ask.${appr.requestedBy}`;

          const followUpMessage =
            status === "approved"
              ? `Approval APPROVED: "${appr.title}". Proceed with the action you requested. Description: ${appr.description || "n/a"}`
              : `Approval REJECTED: "${appr.title}". Do NOT proceed. ${appr.description || ""}`;

          const conn = await amqplib.connect(RABBITMQ_URL);
          const ch = await conn.createConfirmChannel();
          await ch.assertExchange(EXCHANGE, "topic", { durable: true });

          const routingKey = appr.workerSid ? `direct.${appr.workerSid}` : topic;
          ch.publish(
            EXCHANGE,
            routingKey,
            Buffer.from(
              JSON.stringify({
                from: "approval-webhook",
                message: followUpMessage,
              })
            ),
            { contentType: "application/json" }
          );

          // Wait for publish to flush before closing
          await ch.waitForConfirms().catch(() => {});
          await conn.close();
          console.log(`Approval ${status}: dispatched follow-up to ${routingKey}`);
        }
      } catch (mqErr) {
        console.error("Failed to dispatch approval follow-up:", mqErr.message);
      }
    }

    res.json({ ok: true });
  } catch (err) {
    console.error("Telegram webhook error:", err.message);
    res.json({ ok: true }); // Always 200 to Telegram
  }
}
