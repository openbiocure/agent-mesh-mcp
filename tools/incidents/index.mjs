/**
 * Incident tools: create_incident, list_incidents, update_incident, resolve_incident
 */

import { z } from "zod";
import crypto from "crypto";
import prisma from "../../lib/db.mjs";
import { resolveId } from "../../lib/helpers.mjs";
import { sendNotification } from "../../lib/notifications.mjs";

const AGENT_NAME = process.env.AGENT_NAME || "unknown";

export function registerIncidentTools(server) {
  // --- create_incident ---
  server.tool(
    "create_incident",
    `Track a new P1/P2/P3 incident. Links to a GitHub issue and assigns an agent.

Examples:
  create_incident(repo="openbiocure/obc-connectors-core", title="API 500 on /models", severity="p1", gh_issue=380, assigned_agent="backend-engineer")
  create_incident(repo="openbiocure/platform-ui", title="Login redirect broken", severity="p2")

Use list_incidents() to see tracked incidents. Use update_incident() to change status/assignment. Use resolve_incident() when fixed.`,
    {
      repo: z.string().describe("GitHub repo (e.g. 'openbiocure/obc-connectors-core')"),
      title: z.string().describe("Short incident title"),
      severity: z.enum(["p1", "p2", "p3"]).describe("Severity level"),
      gh_issue: z.number().optional().describe("GitHub issue number"),
      summary: z.string().optional().describe("Incident summary/description"),
      assigned_agent: z.string().optional().describe("Agent to investigate (e.g. 'backend-engineer')"),
    },
    async ({ repo, title, severity, gh_issue, summary, assigned_agent }) => {
      try {
        const id = crypto.randomUUID();
        await prisma.incident.create({
          data: {
            id,
            repo,
            title,
            severity,
            ghIssue: gh_issue || null,
            summary: summary || null,
            assignedAgent: assigned_agent || null,
            status: "open",
            createdBy: AGENT_NAME,
          },
        });
        const ghRef = gh_issue ? ` (${repo}#${gh_issue})` : "";

        // Notify on Telegram for P1/P2
        if (["p1", "p2"].includes(severity)) {
          await sendNotification({
            type: "incident",
            title: `NEW ${severity.toUpperCase()}: ${title}${ghRef}`,
            message: `Repo: ${repo}${summary ? "\n" + summary : ""}`,
            workerName: AGENT_NAME.toLowerCase().replace(/\s+/g, "-"),
            workerSid: process.env.WORKER_SID || null,
          });
        }

        return { content: [{ type: "text", text: `Incident created: \`${id.slice(0, 8)}\`\n\n- **[${severity.toUpperCase()}]** ${title}${ghRef}\n- Status: open${assigned_agent ? `\n- Assigned: ${assigned_agent}` : ""}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error creating incident: ${err.message})` }], isError: true };
      }
    }
  );

  // --- list_incidents ---
  server.tool(
    "list_incidents",
    "List tracked incidents, optionally filtered by severity or status.",
    {
      severity: z.enum(["p1", "p2", "p3"]).optional().describe("Filter by severity"),
      status: z.string().optional().describe("Filter by status (open, investigating, fix_submitted, resolved)"),
    },
    async ({ severity, status }) => {
      try {
        const where = {};
        if (severity) where.severity = severity;
        if (status) where.status = status;

        const rows = await prisma.incident.findMany({
          where,
          orderBy: [
            { severity: "asc" }, // p1 < p2 < p3 alphabetically — works
            { createdAt: "desc" },
          ],
        });
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No incidents found${severity ? ` with severity ${severity}` : ""}${status ? ` with status ${status}` : ""}.` }] };
        }
        const lines = rows.map((r) => {
          const gh = r.ghIssue ? `${r.repo}#${r.ghIssue}` : r.repo;
          const pr = r.ghPr ? ` \u2192 PR #${r.ghPr}` : "";
          const agent = r.assignedAgent ? ` \u2192 ${r.assignedAgent}` : "";
          return `- **[${r.severity.toUpperCase()}]** ${r.title} (\`${r.id.slice(0, 8)}\`)\n    ${gh}${pr} | ${r.status}${agent}`;
        });
        return { content: [{ type: "text", text: `**Incidents (${rows.length}):**\n\n${lines.join("\n\n")}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- update_incident ---
  server.tool(
    "update_incident",
    "Update an incident's status, severity, assigned agent, or link a PR.",
    {
      incident_id: z.string().describe("Incident UUID (use list_incidents to find)"),
      status: z.string().optional().describe("New status (open, investigating, fix_submitted, resolved)"),
      severity: z.enum(["p1", "p2", "p3"]).optional().describe("New severity"),
      assigned_agent: z.string().optional().describe("Assign to an agent"),
      gh_pr: z.number().optional().describe("Link a fix PR"),
      summary: z.string().optional().describe("Update summary"),
    },
    async ({ incident_id, status, severity, assigned_agent, gh_pr, summary }) => {
      try {
        incident_id = await resolveId("incidents", incident_id);
        const data = {};
        if (status) data.status = status;
        if (severity) data.severity = severity;
        if (assigned_agent) data.assignedAgent = assigned_agent;
        if (gh_pr) data.ghPr = gh_pr;
        if (summary) data.summary = summary;
        if (status === "resolved") data.resolvedAt = new Date();
        data.updatedAt = new Date();

        if (Object.keys(data).length <= 1) {
          return { content: [{ type: "text", text: "(error: nothing to update)" }], isError: true };
        }

        const updated = await prisma.incident.updateMany({
          where: { id: incident_id },
          data,
        });
        if (updated.count === 0) {
          return { content: [{ type: "text", text: `(error: incident \`${incident_id}\` not found)` }], isError: true };
        }
        return { content: [{ type: "text", text: `Incident \`${incident_id.slice(0, 8)}\` updated.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- resolve_incident ---
  server.tool(
    "resolve_incident",
    "Mark an incident as resolved, optionally linking the fix PR.",
    {
      incident_id: z.string().describe("Incident UUID"),
      gh_pr: z.number().optional().describe("GitHub PR number that fixes this"),
    },
    async ({ incident_id, gh_pr }) => {
      try {
        incident_id = await resolveId("incidents", incident_id);
        const data = {
          status: "resolved",
          resolvedAt: new Date(),
          updatedAt: new Date(),
        };
        if (gh_pr) data.ghPr = gh_pr;

        const updated = await prisma.incident.updateMany({
          where: { id: incident_id },
          data,
        });
        if (updated.count === 0) {
          return { content: [{ type: "text", text: `(error: incident \`${incident_id}\` not found)` }], isError: true };
        }
        return { content: [{ type: "text", text: `Incident \`${incident_id.slice(0, 8)}\` resolved.${gh_pr ? ` Fix: PR #${gh_pr}` : ""}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
