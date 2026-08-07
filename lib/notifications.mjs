/**
 * Notification dispatcher — sends typed notifications through the configured channel.
 * Currently Telegram. The notification is stored in the DB for reply routing.
 */

import prisma from "./db.mjs";

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "8808069971:AAHGimNxHN7GssOterrZjU-pHSvZ78IsoCo";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "1329256217";

const TEMPLATES = {
  incident: { icon: "🔴" },
  deploy:   { icon: "🚀" },
  info:     { icon: "📋" },
  warning:  { icon: "⚠️" },
  success:  { icon: "✅" },
  error:    { icon: "❌" },
};

/**
 * Send a notification through the configured channel.
 *
 * @param {Object} opts
 * @param {string} opts.type - incident, deploy, info, warning, success, error
 * @param {string} opts.title - short title
 * @param {string} [opts.message] - body text
 * @param {string} [opts.workerName] - agent that sent it (for reply routing)
 * @param {string} [opts.workerSid] - worker sid (for sticky reply routing)
 */
export async function sendNotification({ type = "info", title, message, workerName, workerSid }) {
  const t = TEMPLATES[type] || TEMPLATES.info;

  let text = `${t.icon} ${title}`;
  if (workerName) text += `\nAgent: ${workerName}`;
  if (message) text += `\n\n${message}`;
  text += "\n\nReply to respond to the agent.";

  try {
    const resp = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text }),
    });
    const result = await resp.json();
    const messageId = result?.result?.message_id;

    // Store for reply routing
    if (messageId && workerName) {
      await prisma.$executeRaw`
        INSERT INTO notifications (worker_name, worker_sid, message_id, chat_id, type, title)
        VALUES (${workerName}, ${workerSid || null}, ${messageId}, ${TELEGRAM_CHAT_ID}, ${type}, ${title})`;
    }
  } catch (err) {
    console.error("Notification send failed:", err.message);
  }
}
