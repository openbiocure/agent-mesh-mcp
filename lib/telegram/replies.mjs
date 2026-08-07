/**
 * Telegram reply adapter — renders reply results from shared command handlers.
 */

import { sendMessage, TELEGRAM_CHAT_ID } from "./client.mjs";
import { formatDeployNudge, formatApprovalComment, formatReplyRouted } from "../channels/telegram.mjs";
import { handleReplyMessage } from "../commands/replies.mjs";

export async function handleReply(message) {
  const origMsgId = message.reply_to_message?.message_id;
  if (!origMsgId || !message.text) return false;

  const result = await handleReplyMessage(origMsgId, message.text);
  if (!result) return false;

  const formatters = {
    deploy_nudge: (d) => sendMessage(message.chat.id, formatDeployNudge(d), { reply_to_message_id: message.message_id }),
    approval_comment: (d) => sendMessage(TELEGRAM_CHAT_ID, formatApprovalComment(d)),
    routed: (d) => sendMessage(message.chat.id, formatReplyRouted(d), { reply_to_message_id: message.message_id }),
  };

  const send = formatters[result.type];
  if (send) await send(result.data);

  return true;
}
