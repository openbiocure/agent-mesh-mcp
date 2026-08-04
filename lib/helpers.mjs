import { Prisma } from "@prisma/client";
import prisma from "./db.mjs";

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "8808069971:AAHGimNxHN7GssOterrZjU-pHSvZ78IsoCo";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "1329256217";

/**
 * Resolve a short ID prefix to a full UUID.
 * Uses raw SQL because Prisma doesn't support casting + LIKE on UUIDs.
 */
export async function resolveId(table, shortId) {
  if (shortId.length >= 32) return shortId;
  const rows = await prisma.$queryRaw`SELECT id::text as id FROM ${Prisma.raw(table)} WHERE id::text LIKE ${shortId + "%"} LIMIT 2`;
  if (rows.length === 1) return rows[0].id;
  if (rows.length === 0) throw new Error(`No ${table} found matching '${shortId}'`);
  throw new Error(`Ambiguous: ${rows.length} ${table} match '${shortId}'`);
}

/**
 * Send a Telegram notification, optionally with inline keyboard buttons.
 */
export async function sendTelegramNotification(text, buttons = null) {
  const body = { chat_id: TELEGRAM_CHAT_ID, text };
  if (buttons) {
    body.reply_markup = { inline_keyboard: buttons };
  }
  try {
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (err) {
    console.error("Telegram notification failed:", err.message);
  }
}
