/**
 * Release tools: create_release, list_releases, get_release, update_release, close_release
 */

import { z } from "zod";
import crypto from "crypto";
import amqplib from "amqplib";
import prisma from "../../lib/db.mjs";
import { resolveId, sendTelegramNotification } from "../../lib/helpers.mjs";
import { notifySubscribers } from "../../lib/telegram.mjs";

const AGENT_NAME = process.env.AGENT_NAME || "unknown";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";
const EXCHANGE = process.env.EXCHANGE_NAME || "agents";

/**
 * Deploy queue: dispatch one release at a time.
 * P1/P2 hotfixes skip the queue and deploy immediately.
 */
async function triggerDeployIfReady() {
  try {
    // Check if anything is already deploying
    const deploying = await prisma.release.findFirst({
      where: { status: "deploying" },
      select: { id: true, name: true, startedAt: true },
    });

    // Find next ready release (by creation date)
    const next = await prisma.release.findFirst({
      where: { status: "ready" },
      orderBy: { createdAt: "asc" },
      select: { id: true, name: true, incidentId: true },
    });

    if (!next) return; // nothing to deploy

    // Check if this release is a hotfix (linked to a P1/P2 incident)
    let isHotfix = false;
    if (next.incidentId) {
      const incident = await prisma.incident.findUnique({
        where: { id: next.incidentId },
        select: { severity: true },
      });
      isHotfix = incident && ["p1", "p2"].includes(incident.severity);
    }

    // If something is deploying and this is NOT a hotfix, wait or reclaim
    if (deploying && !isHotfix) {
      const elapsed = deploying.startedAt
        ? (Date.now() - new Date(deploying.startedAt).getTime()) / 60000
        : 999;
      if (elapsed < 30) return; // still active, wait

      // Stuck — reclaim
      await prisma.release.update({
        where: { id: deploying.id },
        data: { status: "ready" },
      });
      await sendTelegramNotification(`⚠️ Release ${deploying.name} stuck deploying for ${Math.round(elapsed)}min — reclaimed to ready`);
    }

    // Hotfix with something deploying — notify but deploy anyway
    if (deploying && isHotfix) {
      await sendTelegramNotification(`🚨 HOTFIX ${next.name} — jumping deploy queue (linked to P1/P2 incident)`);
    }

    // Dispatch deploy
    const conn = await amqplib.connect(RABBITMQ_URL);
    const ch = await conn.createConfirmChannel();
    await ch.assertExchange(EXCHANGE, "topic", { durable: true });
    ch.publish(
      EXCHANGE,
      "ask.prod-ops",
      Buffer.from(JSON.stringify({
        from: "deploy-queue",
        message: `Deploy release ${next.id.slice(0, 8)} "${next.name}". Run get_release("${next.id.slice(0, 8)}"), follow the steps, close_release when done.`,
      })),
      { contentType: "application/json" }
    );
    await ch.waitForConfirms().catch(() => {});
    await conn.close();

    await sendTelegramNotification(`🚀 Deploying: ${next.name} (${next.id.slice(0, 8)})${isHotfix ? " [HOTFIX]" : ""}`);
  } catch (err) {
    console.error("Deploy queue error:", err.message);
  }
}

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
      incident_id: z.string().optional().describe("Linked incident UUID — P1/P2 incidents make this a hotfix that jumps the deploy queue"),
      notes: z.string().optional().describe("Dev notes"),
    },
    async ({ name, prs, steps, requires_migration, requires_downtime, incident_id, notes }) => {
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
            incidentId: incident_id || null,
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

        // Notify subscribers on status change
        if (status) {
          const rel = await prisma.release.findUnique({ where: { id: release_id }, select: { name: true } });
          await notifySubscribers(release_id, rel?.name || "unknown", status);
        }

        // Auto-trigger deploy queue when status changes to ready
        if (status === "ready") {
          await triggerDeployIfReady();
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

        // Notify subscribers
        const rel = await prisma.release.findUnique({ where: { id: release_id }, select: { name: true } });
        await notifySubscribers(release_id, rel?.name || "unknown", status, summary);

        // Deploy finished — trigger next in queue
        await triggerDeployIfReady();

        return { content: [{ type: "text", text: `Release \`${release_id.slice(0, 8)}\` closed as **${status}**.${summary ? `\n\n${summary}` : ""}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
