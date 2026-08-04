/**
 * Settings tools: set_setting, get_setting, list_settings
 */

import { z } from "zod";
import prisma from "../../lib/db.mjs";

export function registerSettingsTools(server) {
  // --- set_setting ---
  server.tool(
    "set_setting",
    `Set a mesh configuration value. Stored in postgres, no restart needed.

Examples:
  set_setting(key="TELEGRAM_ALLOWED_USERS", value="1329256217,987654321")
  set_setting(key="DEPLOY_FREEZE", value="true")`,
    {
      key: z.string().describe("Setting key"),
      value: z.string().describe("Setting value"),
    },
    async ({ key, value }) => {
      try {
        await prisma.meshSetting.upsert({
          where: { key },
          update: { value, updatedAt: new Date() },
          create: { key, value, updatedAt: new Date() },
        });
        return { content: [{ type: "text", text: `Setting \`${key}\` = \`${value}\`` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- get_setting ---
  server.tool(
    "get_setting",
    "Get a mesh configuration value.",
    {
      key: z.string().describe("Setting key"),
    },
    async ({ key }) => {
      try {
        const setting = await prisma.meshSetting.findUnique({ where: { key } });
        if (!setting) {
          return { content: [{ type: "text", text: `Setting \`${key}\` not found.` }] };
        }
        return { content: [{ type: "text", text: `\`${key}\` = \`${setting.value}\`` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- list_settings ---
  server.tool(
    "list_settings",
    "List all mesh configuration settings.",
    {},
    async () => {
      try {
        const rows = await prisma.meshSetting.findMany({
          orderBy: { key: "asc" },
        });
        if (rows.length === 0) {
          return { content: [{ type: "text", text: "No settings configured." }] };
        }
        const lines = rows.map((r) => `- \`${r.key}\` = \`${r.value}\``);
        return { content: [{ type: "text", text: `**Settings (${rows.length}):**\n\n${lines.join("\n")}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
