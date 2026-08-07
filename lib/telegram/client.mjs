/**
 * Telegram API client — wraps fetch calls so nobody else touches the API directly.
 */

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "8808069971:AAHGimNxHN7GssOterrZjU-pHSvZ78IsoCo";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "1329256217";

export { TELEGRAM_CHAT_ID };

export async function sendMessage(chatId, text, opts = {}) {
  const body = { chat_id: chatId, text, ...opts };
  const resp = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return resp.json();
}

export async function editMessage(chatId, messageId, text) {
  await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/editMessageText`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, message_id: messageId, text }),
  });
}

export async function answerCallback(callbackQueryId, text, showAlert = false) {
  await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/answerCallbackQuery`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ callback_query_id: callbackQueryId, text, show_alert: showAlert }),
  });
}
