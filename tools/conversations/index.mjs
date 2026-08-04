/**
 * Conversation tools: search_conversations
 */

import { z } from "zod";
import prisma from "../../lib/db.mjs";

export function registerConversationTools(server) {
  server.tool(
    "search_conversations",
    `Search past conversations across all agents. Returns matching turns with context.

Examples:
  search_conversations(query="migration", worker_name="backend-engineer")
  search_conversations(query="deploy failed", limit=5)
  search_conversations(query="citation-ingestion")

Searches all agents by default. Filter by worker_name to narrow results.`,
    {
      query: z.string().describe("Search query — keywords or phrases to find in past conversations"),
      worker_name: z.string().optional().describe("Filter to a specific agent (e.g. 'backend-engineer')"),
      limit: z.number().optional().describe("Max results to return (default: 10)"),
    },
    async ({ query, worker_name, limit: maxResults }) => {
      try {
        const n = Math.min(maxResults || 10, 50);
        const where = {
          content: { contains: query, mode: "insensitive" },
        };
        if (worker_name) where.workerName = worker_name;

        const rows = await prisma.conversation.findMany({
          where,
          orderBy: { timestamp: "desc" },
          take: n,
          select: {
            id: true,
            workerName: true,
            callerId: true,
            role: true,
            content: true,
            timestamp: true,
          },
        });

        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No conversations found matching "${query}".` }] };
        }
        const lines = rows.map((r) => {
          const ts = r.timestamp ? new Date(r.timestamp).toISOString().replace("T", " ").slice(0, 19) : "?";
          return `**${ts}** | ${r.workerName} | ${r.callerId} (${r.role}):\n${r.content}`;
        });
        return { content: [{ type: "text", text: `**Conversations matching "${query}"** (${rows.length} results):\n\n${lines.join("\n\n---\n\n")}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error searching conversations: ${err.message})` }], isError: true };
      }
    }
  );
}
