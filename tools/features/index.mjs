/**
 * Feature tools: create_feature, list_features, get_feature, update_feature
 */

import { z } from "zod";
import crypto from "crypto";
import prisma, { resolveId } from "../../lib/db/index.mjs";
import { emit } from "../../lib/events/index.mjs";

export function registerFeatureTools(server) {
  // --- create_feature ---
  server.tool(
    "create_feature",
    `Create a new feature to track cross-repo work.

Examples:
  create_feature(name="LLM model picker", spec="Users can select their preferred model...", repos=["obc-connectors-core", "platform-ui"])
  create_feature(name="Dark mode", spec="Add theme toggle to shell header")`,
    {
      name: z.string().describe("Feature name"),
      spec: z.string().optional().describe("Feature spec (markdown)"),
      repos: z.array(z.string()).optional().describe("Repos involved (e.g. ['obc-connectors-core', 'platform-ui'])"),
      status: z.string().optional().describe("Initial status (default: draft)"),
      priority: z.enum(["low", "medium", "high", "critical"]).optional().describe("Priority (default: medium)"),
    },
    async ({ name, spec, repos, status, priority }) => {
      try {
        const id = crypto.randomUUID();
        await prisma.feature.create({
          data: {
            id,
            name,
            spec: spec || null,
            status: status || "draft",
            priority: priority || "medium",
            repos: repos || [],
            issues: [],
            prs: [],
            createdBy: process.env.AGENT_NAME || "unknown",
          },
        });
        return { content: [{ type: "text", text: `Feature created: \`${id.slice(0, 8)}\`\n\n- **${name}**\n- Priority: ${priority || "medium"}\n- Status: ${status || "draft"}\n- Repos: ${(repos || []).join(", ") || "none yet"}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- list_features ---
  server.tool(
    "list_features",
    `List all features, optionally filtered by status (draft, review, ready, in_progress, done).

Examples:
  list_features()
  list_features(status="ready")`,
    {
      status: z.string().optional().describe("Filter by status"),
    },
    async ({ status }) => {
      try {
        const where = {};
        if (status) where.status = status;

        const rows = await prisma.feature.findMany({
          where,
          orderBy: { createdAt: "desc" },
        });

        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No features${status ? ` with status '${status}'` : ""}.` }] };
        }

        const lines = rows.map((r) => {
          const repos = (r.repos || []).join(", ") || "no repos";
          const issues = (r.issues || []).length;
          const prs = (r.prs || []).length;
          return `- **[${r.priority}]** ${r.name} (\`${r.id.slice(0, 8)}\`) — ${r.status}\n    Repos: ${repos} | Issues: ${issues} | PRs: ${prs}`;
        });

        return { content: [{ type: "text", text: `**Features (${rows.length}):**\n\n${lines.join("\n\n")}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- get_feature ---
  server.tool(
    "get_feature",
    "Get full details of a feature including spec, issues, and PRs.",
    {
      feature_id: z.string().describe("Feature UUID or short ID"),
    },
    async ({ feature_id }) => {
      try {
        feature_id = await resolveId("features", feature_id);
        const r = await prisma.feature.findUnique({ where: { id: feature_id } });
        if (!r) {
          return { content: [{ type: "text", text: `(error: feature not found)` }], isError: true };
        }

        let text = `# ${r.name}\n\n**Status:** ${r.status} | **Created:** ${new Date(r.createdAt).toISOString().slice(0, 19)}\n`;
        text += `**Repos:** ${(r.repos || []).join(", ") || "none"}\n`;
        text += `**Issues:** ${(r.issues || []).join(", ") || "none"}\n`;
        text += `**PRs:** ${(r.prs || []).join(", ") || "none"}\n`;
        if (r.releaseId) text += `**Release:** ${r.releaseId}\n`;
        if (r.spec) text += `\n## Spec\n${r.spec}\n`;

        return { content: [{ type: "text", text }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- update_feature ---
  server.tool(
    "update_feature",
    `Update a feature — change spec, status, add issues/PRs, link release.

Examples:
  update_feature(feature_id="abc123", status="ready")
  update_feature(feature_id="abc123", issues=["obc-connectors-core#50", "platform-ui#15"])
  update_feature(feature_id="abc123", spec="Updated spec with API details...")`,
    {
      feature_id: z.string().describe("Feature UUID or short ID"),
      name: z.string().optional(),
      spec: z.string().optional(),
      status: z.string().optional().describe("draft, review, ready, in_progress, done"),
      priority: z.enum(["low", "medium", "high", "critical"]).optional(),
      repos: z.array(z.string()).optional(),
      issues: z.array(z.string()).optional(),
      prs: z.array(z.string()).optional(),
      release_id: z.string().optional().describe("Link to a release"),
    },
    async ({ feature_id, name, spec, status, priority, repos, issues, prs, release_id }) => {
      try {
        feature_id = await resolveId("features", feature_id);
        const data = {};
        if (name !== undefined) data.name = name;
        if (spec !== undefined) data.spec = spec;
        if (status !== undefined) data.status = status;
        if (priority !== undefined) data.priority = priority;
        if (repos !== undefined) data.repos = repos;
        if (issues !== undefined) data.issues = issues;
        if (prs !== undefined) data.prs = prs;
        if (release_id !== undefined) data.releaseId = release_id;

        if (Object.keys(data).length === 0) {
          return { content: [{ type: "text", text: "(nothing to update)" }] };
        }

        await prisma.feature.update({ where: { id: feature_id }, data });

        if (status !== undefined) {
          await emit("feature.status_changed", { id: feature_id, status, name: name || undefined });
        }

        return { content: [{ type: "text", text: `Feature \`${feature_id.slice(0, 8)}\` updated.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
