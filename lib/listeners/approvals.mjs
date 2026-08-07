/**
 * Approval listeners — side effects for approval domain events.
 */

import prisma from "../db/index.mjs";
import { on, publishEvent } from "../events/index.mjs";
import { sendMessage, TELEGRAM_CHAT_ID } from "../telegram/client.mjs";

on("approval.created", async ({ id, title, description, requested_by, worker_sid }) => {
  await publishEvent("event.approval.created", { id, title, requested_by, worker_sid });

  const shortId = id.slice(0, 8);
  const text = `\u{1F514} Approval Request\n\n${requested_by} wants to:\n\n${title}${description ? "\n\n" + description : ""}\n\nID: ${shortId}`;
  const result = await sendMessage(TELEGRAM_CHAT_ID, text, {
    reply_markup: { inline_keyboard: [
      [
        { text: "\u2705 Approve", callback_data: `approve:${id}` },
        { text: "\u274C Reject", callback_data: `reject:${id}` },
      ],
    ]},
  });

  // Store notification so replies route back via notification lookup (not regex)
  const messageId = result?.result?.message_id;
  if (messageId) {
    await prisma.notification.create({
      data: {
        workerName: requested_by,
        workerSid: worker_sid || null,
        messageId,
        chatId: TELEGRAM_CHAT_ID,
        type: "approval",
        title: `Approval: ${title} (${shortId})`,
      },
    });
  }
});

on("approval.responded", async ({ id, status, note }) => {
  await publishEvent("event.approval.responded", { id, status, note });
});
