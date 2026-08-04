/**
 * Prompt tools: sync_prompt, get_prompt, list_prompts
 */

import { z } from "zod";
import crypto from "crypto";
import prisma from "../../lib/db.mjs";

const AGENT_NAME = process.env.AGENT_NAME || "unknown";

export function registerPromptTools(server) {
  // --- sync_prompt ---
  server.tool(
    "sync_prompt",
    `Sync an agent's prompt into the database. Workers pick it up on next session rotation — no deploy needed.

Examples:
  sync_prompt(agent_name="backend-engineer", content="You are a backend engineer...", source_file=".claude/agents/backend-engineer.md")

Use get_prompt(agent_name) to see current prompt. Use list_prompts() to see all synced prompts.`,
    {
      agent_name: z.string().describe("Agent name (e.g. 'backend-engineer', 'prod-bug-hunter')"),
      content: z.string().describe("The full prompt content to sync"),
      source_file: z.string().optional().describe("Original file path for reference"),
    },
    async ({ agent_name, content, source_file }) => {
      try {
        const existing = await prisma.prompt.findUnique({
          where: { agentName: agent_name },
          select: { version: true },
        });

        if (existing) {
          const newVersion = (existing.version || 0) + 1;
          await prisma.prompt.update({
            where: { agentName: agent_name },
            data: {
              content,
              version: newVersion,
              sourceFile: source_file || null,
              updatedBy: AGENT_NAME,
              updatedAt: new Date(),
            },
          });
          return { content: [{ type: "text", text: `Prompt for **${agent_name}** updated to v${newVersion} (${content.length} chars). Workers will use it on next session.` }] };
        } else {
          await prisma.prompt.create({
            data: {
              id: crypto.randomUUID(),
              agentName: agent_name,
              content,
              version: 1,
              sourceFile: source_file || null,
              updatedBy: AGENT_NAME,
            },
          });
          return { content: [{ type: "text", text: `Prompt for **${agent_name}** created (v1, ${content.length} chars). Workers will use it on next session.` }] };
        }
      } catch (err) {
        return { content: [{ type: "text", text: `(error syncing prompt: ${err.message})` }], isError: true };
      }
    }
  );

  // --- get_prompt ---
  server.tool(
    "get_prompt",
    "Get the current prompt for an agent from the database.",
    {
      agent_name: z.string().describe("Agent name"),
    },
    async ({ agent_name }) => {
      try {
        const r = await prisma.prompt.findUnique({
          where: { agentName: agent_name },
        });
        if (!r) {
          return { content: [{ type: "text", text: `No prompt in DB for **${agent_name}**. Worker is using file-based prompt.` }] };
        }
        return { content: [{ type: "text", text: `**${agent_name}** prompt (v${r.version}, ${r.content.length} chars)\nSource: ${r.sourceFile || "n/a"} | Updated by: ${r.updatedBy} | ${r.updatedAt}\n\n---\n\n${r.content}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- list_prompts ---
  server.tool(
    "list_prompts",
    "List all agent prompts stored in the database with version info.",
    {},
    async () => {
      try {
        const rows = await prisma.prompt.findMany({
          orderBy: { agentName: "asc" },
        });
        if (rows.length === 0) {
          return { content: [{ type: "text", text: "No prompts in DB. All workers are using file-based prompts." }] };
        }
        const lines = rows.map((r) =>
          `- **${r.agentName}** v${r.version} (${r.content.length} chars) \u2014 ${r.sourceFile || "no source"} \u2014 updated by ${r.updatedBy} at ${new Date(r.updatedAt).toISOString().slice(0, 19)}`
        );
        return { content: [{ type: "text", text: `**Prompts (${rows.length}):**\n\n${lines.join("\n")}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
