/**
 * Release tools: create_release, list_releases, get_release, update_release, close_release
 */

import { z } from "zod";
import crypto from "crypto";
import prisma from "../../lib/db.mjs";
import { resolveId } from "../../lib/helpers.mjs";

const AGENT_NAME = process.env.AGENT_NAME || "unknown";

export function registerReleaseTools(server) {
  // --- create_release ---
  server.tool(
    "create_release",
    `Create a release — a deployment runbook you build up during development.

Examples:
  create_release(name="LLM model picker", prs=["openbiocure/obc-connectors-core#45", "openbiocure/platform-ui#12"], requires_migration=true)
  create_release(name="Dead code cleanup", prs=["openbiocure/obc-connectors-core#391"])

Workflow: create (building) \u2192 add PRs/steps with update_release \u2192 mark ready \u2192 tell devops "deploy release <id>" \u2192 close_release when done.`,
    {
      name: z.string().describe("Release name (e.g. 'LLM model picker')"),
      prs: z.array(z.string()).optional().describe("PR references (e.g. ['openbiocure/obc-connectors-core#45', 'openbiocure/platform-ui#12'])"),
      steps: z.string().optional().describe("Deployment steps as markdown checklist"),
      requires_migration: z.boolean().optional().describe("Does this release require a DB migration?"),
      requires_downtime: z.boolean().optional().describe("Does this release require downtime?"),
      notes: z.string().optional().describe("Dev notes"),
    },
    async ({ name, prs, steps, requires_migration, requires_downtime, notes }) => {
      try {
        const id = crypto.randomUUID();
        await prisma.release.create({
          data: {
            id,
            name,
            prs: prs || [],
            steps: steps || null,
            requiresMigration: requires_migration || false,
            requiresDowntime: requires_downtime || false,
            notes: notes || null,
            status: "building",
            createdBy: AGENT_NAME,
          },
        });
        const flags = [requires_migration && "migration", requires_downtime && "downtime"].filter(Boolean);
        return { content: [{ type: "text", text: `Release created: \`${id.slice(0, 8)}\`\n\n- **${name}**\n- PRs: ${(prs || []).join(", ") || "none yet"}\n- Status: building${flags.length ? `\n- Flags: ${flags.join(", ")}` : ""}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- list_releases ---
  server.tool(
    "list_releases",
    "List all releases, optionally filtered by status (building, ready, deploying, deployed, failed, rolled_back).",
    {
      status: z.string().optional().describe("Filter by status"),
    },
    async ({ status }) => {
      try {
        const where = {};
        if (status) where.status = status;

        const rows = await prisma.release.findMany({
          where,
          orderBy: { createdAt: "desc" },
        });
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No releases${status ? ` with status '${status}'` : ""}.` }] };
        }
        const lines = rows.map((r) => {
          const flags = [r.requiresMigration && "migration", r.requiresDowntime && "downtime"].filter(Boolean);
          const prs = (r.prs || []).join(", ") || "no PRs";
          const dur = r.durationSeconds ? ` (${Math.round(r.durationSeconds)}s)` : "";
          return `- **${r.name}** (\`${r.id.slice(0, 8)}\`) \u2014 ${r.status}${dur}\n    PRs: ${prs}${flags.length ? ` | Flags: ${flags.join(", ")}` : ""}${r.summary ? `\n    Summary: ${r.summary}` : ""}`;
        });
        return { content: [{ type: "text", text: `**Releases (${rows.length}):**\n\n${lines.join("\n\n")}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- get_release ---
  server.tool(
    "get_release",
    "Get full details of a release including steps, rollback plan, and notes.",
    {
      release_id: z.string().describe("Release UUID"),
    },
    async ({ release_id }) => {
      try {
        release_id = await resolveId("releases", release_id);
        const r = await prisma.release.findUnique({ where: { id: release_id } });
        if (!r) {
          return { content: [{ type: "text", text: `(error: release \`${release_id}\` not found)` }], isError: true };
        }
        const flags = [r.requiresMigration && "requires migration", r.requiresDowntime && "requires downtime"].filter(Boolean);
        let text = `# ${r.name}\n\n**Status:** ${r.status} | **Created:** ${new Date(r.createdAt).toISOString().slice(0, 19)}\n`;
        if (flags.length) text += `**Flags:** ${flags.join(", ")}\n`;
        text += `**PRs:** ${(r.prs || []).join(", ") || "none"}\n`;
        if (r.steps) text += `\n## Deploy Steps\n${r.steps}\n`;
        if (r.rollbackSteps) text += `\n## Rollback\n${r.rollbackSteps}\n`;
        if (r.notes) text += `\n## Notes\n${r.notes}\n`;
        if (r.summary) text += `\n## Summary\n${r.summary}\n`;
        if (r.durationSeconds) text += `\n**Duration:** ${Math.round(r.durationSeconds)}s\n`;
        return { content: [{ type: "text", text }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- update_release ---
  server.tool(
    "update_release",
    "Update a release \u2014 add PRs, edit steps, change status, add notes. Use this as you build the feature.",
    {
      release_id: z.string().describe("Release UUID"),
      prs: z.array(z.string()).optional().describe("Replace PR list"),
      steps: z.string().optional().describe("Replace deployment steps"),
      rollback_steps: z.string().optional().describe("Replace rollback steps"),
      requires_migration: z.boolean().optional(),
      requires_downtime: z.boolean().optional(),
      notes: z.string().optional().describe("Replace notes"),
      status: z.string().optional().describe("Change status (building, ready, deploying, deployed, failed, rolled_back)"),
      summary: z.string().optional().describe("Post-deploy summary"),
    },
    async ({ release_id, prs, steps, rollback_steps, requires_migration, requires_downtime, notes, status, summary }) => {
      try {
        release_id = await resolveId("releases", release_id);
        const data = {};
        if (prs !== undefined) data.prs = prs;
        if (steps !== undefined) data.steps = steps;
        if (rollback_steps !== undefined) data.rollbackSteps = rollback_steps;
        if (requires_migration !== undefined) data.requiresMigration = requires_migration;
        if (requires_downtime !== undefined) data.requiresDowntime = requires_downtime;
        if (notes !== undefined) data.notes = notes;
        if (status) data.status = status;
        if (summary) data.summary = summary;
        if (status === "deploying") data.startedAt = new Date();
        if (status === "deployed") data.deployedAt = new Date();

        if (Object.keys(data).length === 0) {
          return { content: [{ type: "text", text: "(nothing to update)" }] };
        }

        const updated = await prisma.release.updateMany({
          where: { id: release_id },
          data,
        });
        if (updated.count === 0) {
          return { content: [{ type: "text", text: `(error: release \`${release_id}\` not found)` }], isError: true };
        }
        return { content: [{ type: "text", text: `Release \`${release_id.slice(0, 8)}\` updated.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- close_release ---
  server.tool(
    "close_release",
    "Close a release as deployed, failed, or rolled back. Include a summary of what happened.",
    {
      release_id: z.string().describe("Release UUID"),
      status: z.enum(["deployed", "failed", "rolled_back"]).describe("Final status"),
      summary: z.string().optional().describe("What happened during deployment"),
      duration_seconds: z.number().optional().describe("How long the deployment took"),
    },
    async ({ release_id, status, summary, duration_seconds }) => {
      try {
        release_id = await resolveId("releases", release_id);
        const data = { status };
        if (summary) data.summary = summary;
        if (duration_seconds !== undefined) data.durationSeconds = duration_seconds;
        if (status === "deployed") data.deployedAt = new Date();

        const updated = await prisma.release.updateMany({
          where: { id: release_id },
          data,
        });
        if (updated.count === 0) {
          return { content: [{ type: "text", text: `(error: release \`${release_id}\` not found)` }], isError: true };
        }
        return { content: [{ type: "text", text: `Release \`${release_id.slice(0, 8)}\` closed as **${status}**.${summary ? `\n\n${summary}` : ""}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
