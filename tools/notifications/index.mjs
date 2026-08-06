/**
 * Notification tools: notify
 */

import { z } from "zod";
import { sendTelegramNotification } from "../../lib/helpers.mjs";

export function registerNotificationTools(server) {
  server.tool(
    "notify",
    `Send a Telegram notification to the human. Use for status updates, completion reports, or anything that needs attention.

Examples:
  notify(message="Feature 'LLM picker' — 3 GitHub issues created in platform-ui")
  notify(message="All PRs merged for feature abc123. Ready to create release.")
  notify(message="Backend-engineer is stuck on issue #50 — needs input")`,
    {
      message: z.string().describe("Notification message"),
    },
    async ({ message }) => {
      try {
        await sendTelegramNotification(message);
        return { content: [{ type: "text", text: `Notification sent.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
