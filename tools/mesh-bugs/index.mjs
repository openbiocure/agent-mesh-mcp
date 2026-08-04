/**
 * Mesh bug tools: report_mesh_bug, list_mesh_bugs, update_mesh_bug
 */

import { z } from "zod";
import crypto from "crypto";
import prisma from "../../lib/db.mjs";
import { resolveId } from "../../lib/helpers.mjs";

const AGENT_NAME = process.env.AGENT_NAME || "unknown";

export function registerMeshBugTools(server) {
  // --- report_mesh_bug ---
  server.tool(
    "report_mesh_bug",
    `Report a bug in the mesh infrastructure. Call this when you encounter mesh issues.

Examples:
  report_mesh_bug(title="Tasks dropping on async dispatch", component="scheduler", severity="high")
  report_mesh_bug(title="get_task_result returns unknown ID", component="mcp", description="Task was dispatched but result lost after MCP session expired")

Components: worker, mcp, scheduler, rabbitmq, postgres, valkey. Use list_mesh_bugs() to see reported bugs.`,
    {
      title: z.string().describe("Short bug title"),
      description: z.string().optional().describe("Detailed description of the bug"),
      component: z.string().optional().describe("Affected component (worker, mcp, scheduler, rabbitmq, postgres, valkey)"),
      severity: z.enum(["low", "medium", "high", "critical"]).optional().describe("Bug severity (default: medium)"),
    },
    async ({ title, description, component, severity }) => {
      try {
        const id = crypto.randomUUID();
        await prisma.meshBug.create({
          data: {
            id,
            title,
            description: description || null,
            reportedBy: AGENT_NAME,
            component: component || null,
            severity: severity || "medium",
            status: "open",
          },
        });
        return { content: [{ type: "text", text: `Bug reported: \`${id.slice(0, 8)}\`\n\n- **${title}**\n- Component: ${component || "unknown"}\n- Severity: ${severity || "medium"}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- list_mesh_bugs ---
  server.tool(
    "list_mesh_bugs",
    "List all reported mesh bugs, optionally filtered by status (open, acknowledged, fixed, wontfix).",
    {
      status: z.string().optional().describe("Filter by status"),
    },
    async ({ status }) => {
      try {
        const where = {};
        if (status) where.status = status;

        const rows = await prisma.meshBug.findMany({
          where,
          orderBy: [
            { severity: "asc" }, // critical < high < low < medium alphabetically — use raw for exact order
            { createdAt: "desc" },
          ],
        });
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No mesh bugs${status ? ` with status '${status}'` : ""}.` }] };
        }
        const lines = rows.map((r) =>
          `- **[${r.severity}]** ${r.title} (\`${r.id.slice(0, 8)}\`) \u2014 ${r.status}\n    Component: ${r.component || "?"} | Reported by: ${r.reportedBy} | ${new Date(r.createdAt).toISOString().slice(0, 19)}`
        );
        return { content: [{ type: "text", text: `**Mesh Bugs (${rows.length}):**\n\n${lines.join("\n\n")}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- update_mesh_bug ---
  server.tool(
    "update_mesh_bug",
    "Update a mesh bug \u2014 change status, add fix notes.",
    {
      bug_id: z.string().describe("Bug UUID"),
      status: z.string().optional().describe("New status (open, acknowledged, fixed, wontfix)"),
      fix_notes: z.string().optional().describe("Notes on the fix"),
    },
    async ({ bug_id, status, fix_notes }) => {
      try {
        bug_id = await resolveId("mesh_bugs", bug_id);
        const data = {};
        if (status) data.status = status;
        if (fix_notes) data.fixNotes = fix_notes;
        if (status === "fixed") data.resolvedAt = new Date();

        if (Object.keys(data).length === 0) {
          return { content: [{ type: "text", text: "(nothing to update)" }] };
        }

        const updated = await prisma.meshBug.updateMany({
          where: { id: bug_id },
          data,
        });
        if (updated.count === 0) {
          return { content: [{ type: "text", text: `(error: bug \`${bug_id}\` not found)` }], isError: true };
        }
        return { content: [{ type: "text", text: `Bug \`${bug_id.slice(0, 8)}\` updated.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
