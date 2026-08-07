/**
 * Notification tools: notify
 */

import { z } from "zod";
import { sendNotification } from "../../lib/notifications/index.mjs";

const AGENT_NAME = process.env.AGENT_NAME || "unknown";
const WORKER_SID = process.env.WORKER_SID || null;

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
        await sendNotification({
          type: type || "info",
          title,
          message,
          workerName: AGENT_NAME.toLowerCase().replace(/\s+/g, "-"),
          workerSid: WORKER_SID,
        });
        return { content: [{ type: "text", text: `Notification sent.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
