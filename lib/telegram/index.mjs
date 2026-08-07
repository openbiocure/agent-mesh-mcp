/**
 * Telegram webhook handler — routes to commands, callbacks, or replies.
 */

import prisma from "../db/index.mjs";
import { handleCommand } from "./commands.mjs";
import { handleCallback } from "./callbacks.mjs";
import { handleReply } from "./replies.mjs";
import { answerCallback, TELEGRAM_CHAT_ID } from "./client.mjs";

const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || "d341662f8d119512eef10545dc51a4b8ad233f5af1d5ed4a977741dcf4aab26c";

/**
 * Express handler for POST /telegram/webhook
 */
export async function telegramWebhookHandler(req, res) {
  if (req.headers["x-telegram-bot-api-secret-token"] !== TELEGRAM_WEBHOOK_SECRET) {
    res.status(403).json({ error: "forbidden" });
    return;
  }

  try {
    const callback = req.body?.callback_query;

    if (!callback) {
      const message = req.body?.message;

      if (message?.text?.startsWith("/")) {
        const handled = await handleCommand(message);
        if (handled) { res.json({ ok: true }); return; }
      }

      if (message?.reply_to_message) {
        const handled = await handleReply(message);
        if (handled) { res.json({ ok: true }); return; }
      }

      res.json({ ok: true });
      return;
    }

    // Auth check for callbacks
    const chatId = String(callback.from.id);
    try {
      const setting = await prisma.meshSetting.findUnique({ where: { key: "TELEGRAM_ALLOWED_USERS" } });
      const allowed = setting?.value ? setting.value.split(",").map(s => s.trim()) : [TELEGRAM_CHAT_ID];
      if (!allowed.includes(chatId)) {
        await answerCallback(callback.id, "Not authorized", true);
        res.json({ ok: true });
        return;
      }
    } catch { /* fall through */ }

    await handleCallback(callback);
    res.json({ ok: true });

  } catch (err) {
    console.error("Telegram webhook error:", err.message);
    res.json({ ok: true });
  }
}
