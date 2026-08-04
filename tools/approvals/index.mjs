/**
 * Approval tools: request_approval, get_approval, list_approvals, approve, reject, comment_approval
 */

import { z } from "zod";
import crypto from "crypto";
import prisma from "../../lib/db.mjs";
import { resolveId, sendTelegramNotification } from "../../lib/helpers.mjs";

const AGENT_NAME = process.env.AGENT_NAME || "unknown";

export function registerApprovalTools(server) {
  // --- request_approval ---
  server.tool(
    "request_approval",
    `Request human approval before taking an irreversible action. Sends a Telegram notification with approve/reject buttons.

Examples:
  request_approval(title="Create PR for #364 fix", description="Fix LLMMapper signature in connector.py:108-110")
  request_approval(title="Deploy release abc123", description="Requires migration, 30s downtime expected")

The agent should poll get_approval(id) to check if approved/rejected. Read comments for refinement feedback.`,
    {
      title: z.string().describe("Short title for the approval request"),
      description: z.string().optional().describe("Detailed description of what the agent wants to do"),
      requested_by: z.string().optional().describe("Your agent name slug (e.g. 'devops-engineer'). Defaults to AGENT_NAME env var."),
    },
    async ({ title, description, requested_by }) => {
      try {
        const id = crypto.randomUUID();
        const requester = requested_by || AGENT_NAME;
        const workerSid = process.env.WORKER_SID || null;
        const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000);

        await prisma.approval.create({
          data: {
            id,
            title,
            description: description || null,
            requestedBy: requester,
            workerSid,
            status: "pending",
            expiresAt,
          },
        });

        // Send Telegram notification with buttons
        const shortId = id.slice(0, 8);
        const text = `\u{1F514} Approval Request\n\n${requester} wants to:\n\n${title}${description ? "\n\n" + description : ""}\n\nID: ${shortId}`;
        await sendTelegramNotification(text, [
          [
            { text: "\u2705 Approve", callback_data: `approve:${id}` },
            { text: "\u274C Reject", callback_data: `reject:${id}` },
          ],
        ]);

        return { content: [{ type: "text", text: `Approval requested: \`${shortId}\`\n\nTelegram notification sent. Poll with get_approval("${shortId}") to check status.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- get_approval ---
  server.tool(
    "get_approval",
    "Check the status of an approval request. Returns status, comments, and any refinement feedback.",
    {
      approval_id: z.string().describe("Approval UUID or short ID"),
    },
    async ({ approval_id }) => {
      try {
        approval_id = await resolveId("approvals", approval_id);
        const a = await prisma.approval.findUnique({
          where: { id: approval_id },
          include: {
            comments: {
              orderBy: { createdAt: "asc" },
            },
          },
        });
        if (!a) {
          return { content: [{ type: "text", text: `(error: approval not found)` }], isError: true };
        }
        let text = `**${a.title}** (\`${a.id.slice(0, 8)}\`)\n\nStatus: **${a.status}**\nRequested by: ${a.requestedBy}\nCreated: ${new Date(a.createdAt).toISOString().slice(0, 19)}`;
        if (a.responseNote) text += `\nResponse: ${a.responseNote}`;
        if (a.comments.length > 0) {
          text += `\n\n**Comments (${a.comments.length}):**\n`;
          text += a.comments.map((c) => `- **${c.author}** (${new Date(c.createdAt).toISOString().slice(0, 19)}): ${c.content}`).join("\n");
        }
        return { content: [{ type: "text", text }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- list_approvals ---
  server.tool(
    "list_approvals",
    "List all approval requests, optionally filtered by status (pending, approved, rejected, expired).",
    {
      status: z.string().optional().describe("Filter by status"),
    },
    async ({ status }) => {
      try {
        const where = {};
        if (status) where.status = status;

        const rows = await prisma.approval.findMany({
          where,
          orderBy: { createdAt: "desc" },
        });
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No approvals${status ? ` with status '${status}'` : ""}.` }] };
        }
        const lines = rows.map((r) =>
          `- **${r.title}** (\`${r.id.slice(0, 8)}\`) \u2014 ${r.status}\n    By: ${r.requestedBy} | ${new Date(r.createdAt).toISOString().slice(0, 19)}`
        );
        return { content: [{ type: "text", text: `**Approvals (${rows.length}):**\n\n${lines.join("\n\n")}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- approve ---
  server.tool(
    "approve",
    "Approve a pending approval request.",
    {
      approval_id: z.string().describe("Approval UUID or short ID"),
      note: z.string().optional().describe("Optional note for the agent"),
    },
    async ({ approval_id, note }) => {
      try {
        approval_id = await resolveId("approvals", approval_id);
        const updated = await prisma.approval.updateMany({
          where: { id: approval_id, status: "pending" },
          data: {
            status: "approved",
            responseNote: note || null,
            respondedAt: new Date(),
          },
        });
        if (updated.count === 0) {
          return { content: [{ type: "text", text: `(error: approval not found or not pending)` }], isError: true };
        }
        return { content: [{ type: "text", text: `Approval \`${approval_id.slice(0, 8)}\` approved.${note ? ` Note: ${note}` : ""}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- reject ---
  server.tool(
    "reject",
    "Reject an approval request with feedback. The agent will read the note to adjust its approach.",
    {
      approval_id: z.string().describe("Approval UUID or short ID"),
      note: z.string().optional().describe("Feedback for the agent \u2014 why rejected, what to change"),
    },
    async ({ approval_id, note }) => {
      try {
        approval_id = await resolveId("approvals", approval_id);
        const updated = await prisma.approval.updateMany({
          where: { id: approval_id, status: "pending" },
          data: {
            status: "rejected",
            responseNote: note || null,
            respondedAt: new Date(),
          },
        });
        if (updated.count === 0) {
          return { content: [{ type: "text", text: `(error: approval not found or not pending)` }], isError: true };
        }
        return { content: [{ type: "text", text: `Approval \`${approval_id.slice(0, 8)}\` rejected.${note ? ` Feedback: ${note}` : ""}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- comment_approval ---
  server.tool(
    "comment_approval",
    "Add a comment to an approval thread. Comments are append-only. Use for refinement feedback or review notes.",
    {
      approval_id: z.string().describe("Approval UUID or short ID"),
      content: z.string().describe("Comment content"),
    },
    async ({ approval_id, content }) => {
      try {
        approval_id = await resolveId("approvals", approval_id);
        await prisma.approvalComment.create({
          data: {
            id: crypto.randomUUID(),
            approvalId: approval_id,
            author: AGENT_NAME,
            content,
          },
        });
        return { content: [{ type: "text", text: `Comment added to approval \`${approval_id.slice(0, 8)}\`.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
