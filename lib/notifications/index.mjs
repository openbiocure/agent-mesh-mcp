/**
 * Notification dispatcher — sends typed notifications through the configured channel.
 * Currently Telegram. The notification is stored in the DB for reply routing.
 */

import prisma from "../db/index.mjs";
import { sendMessage } from "../telegram/client.mjs";
import { notificationIcon } from "../templates/shared.mjs";
import { formatNotification } from "../channels/telegram.mjs";

const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "1329256217";

/**
 * Send a notification through the configured channel.
 */
export async function sendNotification({ type = "info", title, message, workerName, workerSid }) {
  const icon = notificationIcon(type);
  const text = formatNotification({ icon, title, workerName, message });

  try {
    const result = await sendMessage(TELEGRAM_CHAT_ID, text);
    const messageId = result?.result?.message_id;

    if (messageId && workerName) {
      await prisma.notification.create({
        data: {
          workerName,
          workerSid: workerSid || null,
          messageId,
          chatId: TELEGRAM_CHAT_ID,
          type,
          title,
        },
      });
    }
  } catch (err) {
    console.error("Notification send failed:", err.message);
  }
}

/**
 * Notify all subscribers of a release status change.
 * Cleans up subscriptions when release reaches a terminal state.
 */
export async function notifySubscribers(releaseId, releaseName, status, detail = "") {
  try {
    const subs = await prisma.subscription.findMany({
      where: { entityType: "release", entityId: releaseId },
    });
    if (subs.length === 0) return;

    const icon = status === "deployed" ? "✅" : status === "failed" ? "❌" : status === "deploying" ? "🔄" : status === "ready" ? "📦" : "📋";
    const text = `${icon} Release ${releaseName} (${releaseId.slice(0, 8)})\nStatus: ${status}${detail ? "\n\n" + detail : ""}`;

    for (const sub of subs) {
      await sendMessage(sub.chatId, text).catch(() => {});
    }

    if (["deployed", "failed", "rolled_back"].includes(status)) {
      await prisma.subscription.deleteMany({
        where: { entityType: "release", entityId: releaseId },
      });
    }
  } catch (err) {
    console.error("notifySubscribers error:", err.message);
  }
}
