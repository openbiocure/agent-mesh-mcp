/**
 * Notification tools: notify
 */

import { z } from "zod";
import prisma from "../../lib/db.mjs";

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "8808069971:AAHGimNxHN7GssOterrZjU-pHSvZ78IsoCo";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "1329256217";
const AGENT_NAME = process.env.AGENT_NAME || "unknown";
const WORKER_SID = process.env.WORKER_SID || null;

const TEMPLATES = {
  incident: { icon: "🔴", footer: "Reply to respond to the agent." },
  deploy:   { icon: "🚀", footer: "Reply to respond to the agent." },
  info:     { icon: "📋", footer: "Reply to respond to the agent." },
  warning:  { icon: "⚠️", footer: "Reply to respond to the agent." },
  success:  { icon: "✅", footer: "" },
  error:    { icon: "❌", footer: "Reply to respond to the agent." },
};

export function registerNotificationTools(server) {
  server.tool(
    "notify",
    `Send a Telegram notification. The human can reply directly and the message routes back to you.

Types: incident, deploy, info, warning, success, error

Examples:
  notify(type="incident", title="P1 curated crash", message="Workers crash-looping after PR #405")
  notify(type="deploy", title="Deploying ff77ff7e", message="Starting backup...")
  notify(type="success", title="Release deployed", message="All containers healthy")
  notify(type="info", title="3 P2s need your input", message="RAG queue, clinicaltrials, watermark")`,
    {
      type: z.enum(["incident", "deploy", "info", "warning", "success", "error"]).optional().describe("Notification type (default: info)"),
      title: z.string().describe("Short title"),
      message: z.string().optional().describe("Body text"),
    },
    async ({ type, title, message }) => {
      try {
        const t = TEMPLATES[type || "info"];
        const agentSlug = AGENT_NAME.toLowerCase().replace(/\s+/g, "-");

        let text = `${t.icon} ${title}\nAgent: ${agentSlug}`;
        if (message) text += `\n\n${message}`;
        if (t.footer) text += `\n\n${t.footer}`;

        const resp = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text }),
        });
        const result = await resp.json();
        const messageId = result?.result?.message_id;

        // Store notification for reply routing
        if (messageId) {
          await prisma.$executeRaw`
            INSERT INTO notifications (worker_name, worker_sid, message_id, chat_id, type, title)
            VALUES (${agentSlug}, ${WORKER_SID}, ${messageId}, ${TELEGRAM_CHAT_ID}, ${type || "info"}, ${title})`;
        }

        return { content: [{ type: "text", text: `Notification sent.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
