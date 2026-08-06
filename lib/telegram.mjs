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
      const message = req.body?.message;

      // Telegram bot commands
      if (message?.text?.startsWith("/")) {
        const cmd = message.text.split(" ")[0].toLowerCase();
        let reply = "";

        if (cmd === "/releases") {
          const releases = await prisma.release.findMany({
            where: { status: { in: ["building", "ready", "deploying"] } },
            orderBy: { createdAt: "desc" },
            take: 10,
          });
          if (releases.length === 0) {
            reply = "No active releases.";
          } else {
            reply = releases.map(r =>
              `${r.status === "deploying" ? "🔄" : r.status === "ready" ? "✅" : "🔨"} ${r.name} (${r.id.slice(0, 8)}) — ${r.status}`
            ).join("\n");
          }
        } else if (cmd === "/bugs") {
          const bugs = await prisma.meshBug.findMany({
            where: { status: { notIn: ["fixed", "wontfix"] } },
            orderBy: { createdAt: "desc" },
            take: 10,
          });
          reply = bugs.length === 0 ? "No open bugs." : bugs.map(b =>
            `[${b.severity}] ${b.title} (${b.id.slice(0, 8)}) — ${b.status}`
          ).join("\n");
        } else if (cmd === "/agents") {
          const workers = await prisma.worker.findMany({
            where: { status: "online" },
            orderBy: { lastHeartbeat: "desc" },
          });
          // Deduplicate by name, keep latest
          const seen = new Set();
          const unique = workers.filter(w => { if (seen.has(w.name)) return false; seen.add(w.name); return true; });
          reply = unique.map(w => `${w.name} — ${w.host} — ${w.topics?.join(", ")}`).join("\n");
        } else if (cmd === "/incidents") {
          const incidents = await prisma.$queryRaw`
            SELECT id::text, title, severity, status FROM incidents
            WHERE status != 'resolved'
            ORDER BY CASE severity WHEN 'p1' THEN 1 WHEN 'p2' THEN 2 WHEN 'p3' THEN 3 ELSE 4 END, created_at DESC
            LIMIT 10`;
          reply = incidents.length === 0 ? "No open incidents." : incidents.map(i =>
            `[${i.severity}] ${i.title} (${i.id.slice(0, 8)}) — ${i.status}`
          ).join("\n");
        } else if (cmd === "/help") {
          reply = "/releases — active releases\n/bugs — open mesh bugs\n/agents — online agents\n/incidents — open incidents\n/help — this message";
        }

        if (reply) {
          await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ chat_id: message.chat.id, text: reply }),
          });
          res.json({ ok: true });
          return;
        }
      }

      // Reply to a message — nudge or comment
      if (message?.reply_to_message) {
        const origText = message.reply_to_message.text || "";

        // Nudge: reply to a deploy notification to get status update
        const deployMatch = origText.match(/(?:Deploying|DEPLOYED|FAILED|HOTFIX|Release)[:\s]*([^\n]*?)\(([a-f0-9]{8})\)/i);
        if (deployMatch) {
          const releaseShortId = deployMatch[2];
          const releases = await prisma.$queryRaw`SELECT id::text as id, name, status, summary, started_at as "startedAt", deployed_at as "deployedAt" FROM releases WHERE id::text LIKE ${releaseShortId + "%"}`;
          if (releases.length === 1) {
            const r = releases[0];
            let statusText = `📋 Release ${releaseShortId}: **${r.status}**\n${r.name}`;
            if (r.status === "deploying" && r.startedAt) {
              const elapsed = Math.round((Date.now() - new Date(r.startedAt).getTime()) / 60000);
              statusText += `\nDeploying for ${elapsed}min`;
            }
            if (r.summary) statusText += `\n\n${r.summary.slice(0, 500)}`;
            // Check latest activity from devops
            const activity = await prisma.activity.findFirst({
              where: { workerName: "devops-engineer" },
              orderBy: { timestamp: "desc" },
              select: { content: true, timestamp: true },
            });
            if (activity) {
              const ago = Math.round((Date.now() - new Date(activity.timestamp).getTime()) / 1000);
              statusText += `\n\nLast devops activity (${ago}s ago): ${activity.content?.slice(0, 200) || "unknown"}`;
            }
            await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ chat_id: message.chat.id, text: statusText, reply_to_message_id: message.message_id }),
            });
          }
          res.json({ ok: true });
          return;
        }

        // Extract approval ID from the original message
        // Match "ID: abc12345" or "Refine abc12345"
        const match = origText.match(/(?:ID:|Refine)\s*([a-f0-9]{8})/);
        if (match && message.text) {
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
    const approver = `${callback.from.first_name || callback.from.username || "unknown"} (${chatId})`;

    const rowsAffected = await prisma.$executeRaw`
      UPDATE approvals SET status = ${status}, responded_at = now(), approved_by = ${approver}
      WHERE id = ${approvalId}::uuid AND status = 'pending'`;

    // Answer the callback (removes loading spinner on button)
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/answerCallbackQuery`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        callback_query_id: callback.id,
        text: rowsAffected > 0 ? `${status.charAt(0).toUpperCase() + status.slice(1)}!` : "Already responded",
      }),
    });

    // Update the message to show the result
    if (rowsAffected > 0) {
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
          select: { title: true, description: true, requestedBy: true, workerSid: true, responseNote: true },
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

          const note = appr.responseNote ? `\n\nNote from human: ${appr.responseNote}` : "";
          const followUpMessage =
            status === "approved"
              ? `Approval APPROVED: "${appr.title}". Proceed with the action you requested. Description: ${appr.description || "n/a"}${note}`
              : `Approval REJECTED: "${appr.title}". Do NOT proceed. ${appr.description || ""}${note}`;

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
