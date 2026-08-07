/**
 * Notification dispatcher — sends typed notifications through the configured channel.
 * Currently Telegram. The notification is stored in the DB for reply routing.
 */

import prisma from "./db.mjs";
import { sendMessage } from "./telegram/client.mjs";
import { notificationIcon } from "./templates/shared.mjs";
import { formatNotification } from "./channels/telegram.mjs";

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
