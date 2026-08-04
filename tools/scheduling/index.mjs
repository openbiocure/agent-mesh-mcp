/**
 * Scheduling tools: create_schedule, list_schedules, run_schedule, delete_schedule
 */

import { z } from "zod";
import crypto from "crypto";
import amqplib from "amqplib";
import prisma from "../../lib/db.mjs";
import { resolveId } from "../../lib/helpers.mjs";

const EXCHANGE = process.env.EXCHANGE_NAME || "agents";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";
const AGENT_NAME = process.env.AGENT_NAME || "unknown";

export function registerSchedulingTools(server) {
  // --- create_schedule ---
  server.tool(
    "create_schedule",
    `Create a recurring scheduled task. A mesh worker will pick it up and arm a timer.

Examples:
  create_schedule(name="daily-bug-check", topic="datalake", message="Run the P1 bug sweep", cron="0 9 * * *")
  create_schedule(name="deploy-watcher", topic="prod-ops", message="Check for ready releases", interval_seconds=300)
  create_schedule(name="health-ping", topic="agent-ui", message="Reply with: pong", interval_seconds=60)

Use list_schedules() to see existing ones. Use run_schedule(id) to fire one immediately. Use delete_schedule(id) to remove.`,
    {
      name: z.string().describe("Human-readable name for this schedule (e.g. 'daily-bug-check')"),
      topic: z.string().describe("Topic to dispatch to (e.g. 'datalake', 'prod')"),
      message: z.string().describe("The message to send to the agent on each run"),
      cron: z.string().optional().describe("Cron expression (e.g. '0 9 * * *' for daily at 9am UTC). Mutually exclusive with interval_seconds."),
      interval_seconds: z.number().optional().describe("Run every N seconds. Mutually exclusive with cron."),
      caller_id: z.string().optional().describe("Caller identity for the scheduled task (default: 'scheduler')"),
    },
    async ({ name, topic, message, cron, interval_seconds, caller_id }) => {
      try {
        const id = crypto.randomUUID();
        const now = new Date();
        const resolvedTopic = topic.startsWith("ask.") ? topic : `ask.${topic}`;
        await prisma.schedule.create({
          data: {
            id,
            name,
            topic: resolvedTopic,
            message,
            cron: cron || null,
            intervalSeconds: interval_seconds || null,
            callerId: caller_id || "scheduler",
            enabled: true,
            status: "idle",
            runCount: 0,
            failCount: 0,
            nextRun: now,
            createdBy: AGENT_NAME,
            createdAt: now,
          },
        });

        // Publish event to RabbitMQ — a worker will pick it up and arm the timer
        const conn = await amqplib.connect(RABBITMQ_URL);
        const ch = await conn.createChannel();
        await ch.assertExchange(EXCHANGE, "topic", { durable: true });
        ch.publish(EXCHANGE, "schedule.create", Buffer.from(JSON.stringify({ action: "create", schedule_id: id })), {
          contentType: "application/json",
        });
        await conn.close();

        return { content: [{ type: "text", text: `Schedule created: \`${id}\`\n\n- **${name}** \u2192 \`${topic}\`\n- ${cron ? `Cron: \`${cron}\`` : `Every ${interval_seconds}s`}\n- Status: idle, next run: now` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error creating schedule: ${err.message})` }], isError: true };
      }
    }
  );

  // --- list_schedules ---
  server.tool(
    "list_schedules",
    "List all scheduled tasks and their current state (status, last result, run/fail counts, next run time).",
    {},
    async () => {
      try {
        const rows = await prisma.schedule.findMany({
          orderBy: { createdAt: "desc" },
        });
        if (rows.length === 0) {
          return { content: [{ type: "text", text: "No schedules configured." }] };
        }
        const lines = rows.map((r) => {
          const freq = r.cron ? `cron: \`${r.cron}\`` : `every ${r.intervalSeconds}s`;
          const status = r.enabled ? r.status : "disabled";
          const lastRun = r.lastRun ? new Date(r.lastRun).toISOString().replace("T", " ").slice(0, 19) : "never";
          const nextRun = r.nextRun ? new Date(r.nextRun).toISOString().replace("T", " ").slice(0, 19) : "\u2014";
          let state = `${status} | runs: ${r.runCount} | fails: ${r.failCount}`;
          if (r.lastResult) state += `\n    Last result: ${r.lastResult.slice(0, 100)}`;
          if (r.lastError) state += `\n    Last error: ${r.lastError}`;
          const msgPreview = r.message.length > 80 ? r.message.slice(0, 80) + "..." : r.message;
          return `- **${r.name}** (\`${r.id.slice(0, 8)}\`) \u2192 \`${r.topic}\` | ${freq}\n    Message: ${msgPreview}\n    ${state}\n    Last: ${lastRun} | Next: ${nextRun}`;
        });
        return { content: [{ type: "text", text: `**Schedules (${rows.length}):**\n\n${lines.join("\n\n")}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error listing schedules: ${err.message})` }], isError: true };
      }
    }
  );

  // --- delete_schedule ---
  server.tool(
    "delete_schedule",
    "Delete a scheduled task by its ID (use list_schedules to find IDs).",
    {
      schedule_id: z.string().describe("Schedule UUID to delete"),
    },
    async ({ schedule_id }) => {
      try {
        schedule_id = await resolveId("schedules", schedule_id);
        const deleted = await prisma.schedule.delete({
          where: { id: schedule_id },
        }).catch(() => null);
        if (!deleted) {
          return { content: [{ type: "text", text: `(error: schedule \`${schedule_id}\` not found)` }], isError: true };
        }

        // Notify workers to cancel the timer
        const conn = await amqplib.connect(RABBITMQ_URL);
        const ch = await conn.createChannel();
        await ch.assertExchange(EXCHANGE, "topic", { durable: true });
        ch.publish(EXCHANGE, "schedule.delete", Buffer.from(JSON.stringify({ action: "delete", schedule_id })), {
          contentType: "application/json",
        });
        await conn.close();

        return { content: [{ type: "text", text: `Schedule \`${schedule_id}\` deleted.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error deleting schedule: ${err.message})` }], isError: true };
      }
    }
  );

  // --- run_schedule ---
  server.tool(
    "run_schedule",
    "Fire a schedule immediately, regardless of its next_run time. The owning worker will dispatch the task right away.",
    {
      schedule_id: z.string().describe("Schedule UUID to fire now (use list_schedules to find IDs)"),
    },
    async ({ schedule_id }) => {
      try {
        schedule_id = await resolveId("schedules", schedule_id);
        const schedule = await prisma.schedule.findUnique({
          where: { id: schedule_id },
          select: { name: true },
        });
        if (!schedule) {
          return { content: [{ type: "text", text: `(error: schedule \`${schedule_id}\` not found)` }], isError: true };
        }

        const conn = await amqplib.connect(RABBITMQ_URL);
        const ch = await conn.createChannel();
        await ch.assertExchange(EXCHANGE, "topic", { durable: true });
        ch.publish(EXCHANGE, "schedule.run_now", Buffer.from(JSON.stringify({ action: "run_now", schedule_id })), {
          contentType: "application/json",
        });
        await conn.close();

        return { content: [{ type: "text", text: `Fired schedule \`${schedule.name}\` (\`${schedule_id.slice(0, 8)}\`). A worker will dispatch it now.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error firing schedule: ${err.message})` }], isError: true };
      }
    }
  );
}
