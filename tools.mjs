/**
 * Shared MCP tool definitions for agent-mesh.
 * Used by both index.mjs (stdio) and server.mjs (HTTP).
 */

import { z } from "zod";
import amqplib from "amqplib";
import crypto from "crypto";
import pg from "pg";

const EXCHANGE = process.env.EXCHANGE_NAME || "agents";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";
const RABBITMQ_MGMT_URL = process.env.RABBITMQ_MGMT_URL || "http://localhost:15672";
const AGENT_NAME = process.env.AGENT_NAME || "unknown";
const TIMEOUT = parseInt(process.env.ASK_TIMEOUT || "900", 10) * 1000;
const STATUS_EXCHANGE = "agent.status";
const MESH_DB_URL = process.env.MESH_DB_URL || "postgresql://postgres:postgres@postgres.lab/obc_mesh";

// Postgres pool for task result fallback
let _pgPool = null;
function getPgPool() {
  if (!_pgPool) {
    _pgPool = new pg.Pool({ connectionString: MESH_DB_URL, max: 3 });
    _pgPool.on("error", () => { _pgPool = null; });
  }
  return _pgPool;
}

// In-memory store for async task results
const taskResults = new Map();

// Persistent AMQP connection for async result listeners
let _persistentConn = null;
let _persistentCh = null;

async function getPersistentChannel() {
  if (_persistentCh) return _persistentCh;
  _persistentConn = await amqplib.connect(RABBITMQ_URL);
  _persistentConn.on("close", () => { _persistentConn = null; _persistentCh = null; });
  _persistentConn.on("error", () => { _persistentConn = null; _persistentCh = null; });
  _persistentCh = await _persistentConn.createChannel();
  return _persistentCh;
}

function getMgmtAuth() {
  try {
    const url = new URL(RABBITMQ_URL);
    return btoa(`${url.username}:${url.password}`);
  } catch {
    return btoa("guest:guest");
  }
}

/**
 * Register all mesh tools on an McpServer instance.
 */
export function registerTools(server) {
  // --- list_agents ---
  server.tool(
    "list_agents",
    "List all online agents and their topics. Call this first to discover available agents before using ask_agent.",
    {},
    async () => {
      try {
        const auth = getMgmtAuth();
        const resp = await fetch(`${RABBITMQ_MGMT_URL}/api/queues/%2f`, {
          headers: { Authorization: `Basic ${auth}` },
        });

        if (!resp.ok) {
          return {
            content: [{ type: "text", text: `(error: RabbitMQ management API returned ${resp.status})` }],
            isError: true,
          };
        }

        const queues = await resp.json();
        const onlineQueues = queues.filter(
          (q) => q.name.startsWith("agent.") && q.consumers > 0
        );

        const agents = [];
        for (const q of onlineQueues) {
          const bindResp = await fetch(
            `${RABBITMQ_MGMT_URL}/api/queues/%2f/${encodeURIComponent(q.name)}/bindings`,
            { headers: { Authorization: `Basic ${auth}` } }
          );
          const bindings = bindResp.ok ? await bindResp.json() : [];
          const topics = bindings
            .filter((b) => b.source && b.routing_key.startsWith("ask."))
            .map((b) => b.routing_key.replace("ask.", ""));

          agents.push({
            name: q.name.replace("agent.", ""),
            topics,
            consumers: q.consumers,
            pending_messages: q.messages,
          });
        }

        if (agents.length === 0) {
          return { content: [{ type: "text", text: "No agents are currently online." }] };
        }

        const lines = agents.map(
          (a) =>
            `- **${a.name}** — topic${a.topics.length > 1 ? "s" : ""}: ${a.topics.map((t) => `\`${t}\``).join(", ")} (${a.consumers} consumer${a.consumers > 1 ? "s" : ""}, ${a.pending_messages} pending)`
        );

        return {
          content: [{ type: "text", text: `**Online agents (${agents.length}):**\n${lines.join("\n")}` }],
        };
      } catch (err) {
        return {
          content: [{ type: "text", text: `(error listing agents: ${err.message})` }],
          isError: true,
        };
      }
    }
  );

  // --- ask_agent ---
  server.tool(
    "ask_agent",
    "Ask another agent a question via RabbitMQ. Use list_agents() first to see available topics. For long tasks, set async=true to get a task_id back immediately and poll with get_task_result. Use sid to route to a specific worker instance for multi-turn conversations.",
    {
      topic: z.string().describe("The topic to route the question to."),
      message: z.string().describe("The message to send to the other agent"),
      timeout: z.number().optional().describe("Timeout in seconds (default: 900). Ignored when async=true."),
      async: z.boolean().optional().describe("If true, returns a task_id immediately instead of waiting. Poll with get_task_result(task_id). Use for long-running tasks."),
      sid: z.string().optional().describe("Worker session ID for sticky routing. Pass the sid from a previous response to route to the same worker instance. Enables multi-turn conversations with context."),
    },
    async ({ topic, message, timeout: timeoutSec, async: isAsync, sid }) => {
      // If sid provided, route directly to that worker; otherwise use topic
      const routingKey = sid ? `direct.${sid}` : (topic.startsWith("ask.") ? topic : `ask.${topic}`);
      const correlationId = crypto.randomUUID();
      const timeoutMs = timeoutSec ? Math.max(1, Math.floor(Number(timeoutSec))) * 1000 : TIMEOUT;

      try {
        if (isAsync) {
          // Async: use persistent connection + named durable reply queue
          const ch = await getPersistentChannel();
          await ch.assertExchange(EXCHANGE, "topic", { durable: true });

          // Named durable queue that survives connection drops
          const replyQueue = `reply.${correlationId}`;
          await ch.assertQueue(replyQueue, { durable: true, autoDelete: true, arguments: { "x-expires": 3600000 } });

          const bodyObj = { from: AGENT_NAME, message, async: true };
          ch.publish(EXCHANGE, routingKey, Buffer.from(JSON.stringify(bodyObj)), {
            replyTo: replyQueue,
            correlationId,
            contentType: "application/json",
          });

          // Wait for the immediate ack (task_id)
          const ack = await new Promise((resolve) => {
            const timer = setTimeout(() => resolve(null), 10000);
            ch.consume(replyQueue, (msg) => {
              if (msg?.properties.correlationId === correlationId) {
                clearTimeout(timer);
                ch.ack(msg);
                resolve(msg.content.toString());
              }
            }).then(({ consumerTag }) => {
              // Store consumer tag for cleanup
              setTimeout(() => ch.cancel(consumerTag).catch(() => {}), 10000);
            });
          });

          if (!ack) {
            return { content: [{ type: "text", text: "(error: no ack received for async task)" }], isError: true };
          }

          let taskId, ackSid;
          try { const d = JSON.parse(ack); taskId = d.task_id; ackSid = d.sid; } catch { taskId = null; }

          if (!taskId) {
            return { content: [{ type: "text", text: ack }] };
          }

          // Start persistent listener for the final result
          taskResults.set(taskId, { status: "processing", result: null, replyQueue });

          ch.consume(replyQueue, (msg) => {
            if (msg?.properties.correlationId === correlationId) {
              ch.ack(msg);
              const content = msg.content.toString();
              try {
                const data = JSON.parse(content);
                taskResults.set(taskId, { status: data.status || "completed", result: data.result || content, raw: data });
              } catch {
                taskResults.set(taskId, { status: "completed", result: content });
              }
              // Delete the reply queue after consuming the result
              ch.deleteQueue(replyQueue).catch(() => {});
            }
          });

          return {
            content: [{ type: "text", text: `Task accepted. ID: \`${taskId}\`${ackSid ? ` | sid: \`${ackSid}\`` : ""}\n\nUse \`get_task_result("${taskId}")\` to poll for the result.${ackSid ? ` Use \`tail_agent(sid="${ackSid}")\` to watch live.` : ""}` }],
          };
        }

        // Synchronous: use a fresh connection with exclusive queue (original behavior)
        const conn = await amqplib.connect(RABBITMQ_URL);
        const ch = await conn.createChannel();

        await ch.assertExchange(EXCHANGE, "topic", { durable: true });
        const { queue: replyQueue } = await ch.assertQueue("", { exclusive: true });

        ch.publish(EXCHANGE, routingKey, Buffer.from(JSON.stringify({ from: AGENT_NAME, message })), {
          replyTo: replyQueue,
          correlationId,
          contentType: "application/json",
        });

        const reply = await new Promise((resolve) => {
          const timer = setTimeout(() => {
            resolve(`(timeout: no agent replied on ${routingKey} within ${timeoutMs / 1000}s)`);
          }, timeoutMs);

          ch.consume(replyQueue, (msg) => {
            if (msg?.properties.correlationId === correlationId) {
              clearTimeout(timer);
              const replySid = msg.properties.headers?.["x-worker-sid"] || null;
              resolve({ text: msg.content.toString(), sid: replySid });
            }
          }, { noAck: true });
        });

        await conn.close();
        const replyText = typeof reply === "string" ? reply : reply.text;
        const replySid = typeof reply === "string" ? null : reply.sid;
        const sidNote = replySid ? `\n\n---\n_sid: \`${replySid}\`_ (pass this to ask_agent to continue with the same worker)` : "";
        return { content: [{ type: "text", text: replyText + sidNote }] };
      } catch (err) {
        return {
          content: [{ type: "text", text: `(error: ${err.message})` }],
          isError: true,
        };
      }
    }
  );

  // --- get_task_result ---
  server.tool(
    "get_task_result",
    "Poll for the result of an async task started with ask_agent(async=true). Returns the result if ready, or 'processing' if still running.",
    {
      task_id: z.string().describe("The task ID returned by ask_agent when async=true."),
    },
    async ({ task_id }) => {
      // Try in-memory first
      const task = taskResults.get(task_id);
      if (task) {
        if (task.status === "processing") {
          return {
            content: [{ type: "text", text: `Task \`${task_id}\` is still processing. Try again in a few seconds.` }],
          };
        }
        taskResults.delete(task_id);
        return { content: [{ type: "text", text: task.result }] };
      }

      // Fallback to postgres
      try {
        const pool = getPgPool();
        const { rows } = await pool.query(
          "SELECT status, result FROM tasks WHERE task_id = $1", [task_id]
        );
        if (rows.length === 0) {
          return {
            content: [{ type: "text", text: `(error: unknown task_id "${task_id}")` }],
            isError: true,
          };
        }
        const row = rows[0];
        if (row.status === "accepted" || row.status === "processing") {
          return {
            content: [{ type: "text", text: `Task \`${task_id}\` is still ${row.status}. Try again in a few seconds.` }],
          };
        }
        return { content: [{ type: "text", text: row.result || `(task ${row.status} with no result)` }] };
      } catch (dbErr) {
        return {
          content: [{ type: "text", text: `(error: task_id "${task_id}" not found in memory, DB fallback failed: ${dbErr.message})` }],
          isError: true,
        };
      }
    }
  );

  // --- cancel_agent ---
  server.tool(
    "cancel_agent",
    "Cancel a running task on a specific worker instance. Requires the worker's sid from a previous ask_agent response. The worker will abort its current Claude session and return whatever partial result it has.",
    {
      sid: z.string().describe("Worker session ID (sid) from a previous ask_agent response."),
    },
    async ({ sid }) => {
      let conn;
      try {
        conn = await amqplib.connect(RABBITMQ_URL);
        const ch = await conn.createChannel();
        await ch.assertExchange(EXCHANGE, "topic", { durable: true });
        ch.publish(EXCHANGE, `cancel.${sid}`, Buffer.from(JSON.stringify({ from: AGENT_NAME, action: "cancel" })), {
          contentType: "application/json",
        });
        await conn.close();
        return { content: [{ type: "text", text: `Cancel sent to worker \`${sid}\`. The worker will abort and return its partial result.` }] };
      } catch (err) {
        if (conn) await conn.close().catch(() => {});
        return { content: [{ type: "text", text: `(error cancelling: ${err.message})` }], isError: true };
      }
    }
  );

  // --- tail_agent ---
  server.tool(
    "tail_agent",
    "Watch a running agent's live activity (tool calls, file reads, reasoning). Use after ask_agent(async=true) to monitor progress. Pass sid to filter to a specific worker instance.",
    {
      name: z.string().describe("Agent name to tail (e.g. 'js-engineer', 'backend-engineer')"),
      duration: z.number().optional().describe("Seconds to tail for (default: 30, max: 120)"),
      sid: z.string().optional().describe("Worker session ID to filter events. Only shows activity from this specific worker instance."),
    },
    async ({ name, duration, sid }) => {
      const seconds = Math.min(Math.max(1, duration || 30), 120);
      let conn;
      try {
        conn = await amqplib.connect(RABBITMQ_URL);
        const ch = await conn.createChannel();

        await ch.assertExchange(STATUS_EXCHANGE, "topic", { durable: true });
        const { queue } = await ch.assertQueue("", { exclusive: true, autoDelete: true });
        await ch.bindQueue(queue, STATUS_EXCHANGE, `activity.${name}`);

        const events = [];

        await new Promise((resolve) => {
          setTimeout(() => resolve(), seconds * 1000);

          ch.consume(queue, (msg) => {
            if (!msg) return;
            ch.ack(msg);
            try {
              const evt = JSON.parse(msg.content.toString());
              // Filter by sid if provided
              if (sid && evt.sid !== sid) return;
              events.push(evt);
            } catch {
              events.push({ type: "raw", content: msg.content.toString(), timestamp: new Date().toISOString() });
            }
          });
        });

        await conn.close();
        conn = null;

        if (events.length === 0) {
          const target = sid ? `**${name}** (sid: ${sid})` : `**${name}**`;
          return {
            content: [{ type: "text", text: `No activity from ${target} in ${seconds}s. The agent may be idle or not running.` }],
          };
        }

        const lines = events.map((e) => {
          const ts = e.timestamp ? e.timestamp.split("T")[1]?.replace("Z", "").slice(0, 8) : "??:??:??";
          const icon = e.type === "tool_call" ? "[tool]"
            : e.type === "error" ? "[error]"
            : e.type === "status" ? "[status]"
            : "[text]";
          return `${ts} ${icon} ${e.content || ""}`;
        });

        return {
          content: [{ type: "text", text: `**Activity from ${name}** (${events.length} events in ${seconds}s):\n\n\`\`\`\n${lines.join("\n")}\n\`\`\`` }],
        };
      } catch (err) {
        if (conn) await conn.close().catch(() => {});
        return {
          content: [{ type: "text", text: `(error tailing ${name}: ${err.message})` }],
          isError: true,
        };
      }
    }
  );

  // --- create_schedule ---
  server.tool(
    "create_schedule",
    "Create a recurring scheduled task. The scheduler dispatches it to the specified topic on the cron or interval you set. Use list_schedules() to see existing ones.",
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
        const pool = getPgPool();
        const id = crypto.randomUUID();
        const now = new Date().toISOString();
        const resolvedTopic = topic.startsWith("ask.") ? topic : `ask.${topic}`;
        await pool.query(
          `INSERT INTO schedules (id, name, topic, message, cron, interval_seconds, caller_id, enabled, status, run_count, fail_count, next_run, created_by, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, true, 'idle', 0, 0, $8, $9, $8)`,
          [id, name, resolvedTopic, message, cron || null, interval_seconds || null, caller_id || "scheduler", now, AGENT_NAME]
        );

        // Publish event to RabbitMQ — a worker will pick it up and arm the timer
        const conn = await amqplib.connect(RABBITMQ_URL);
        const ch = await conn.createChannel();
        await ch.assertExchange(EXCHANGE, "topic", { durable: true });
        ch.publish(EXCHANGE, "schedule.create", Buffer.from(JSON.stringify({ action: "create", schedule_id: id })), {
          contentType: "application/json",
        });
        await conn.close();

        return { content: [{ type: "text", text: `Schedule created: \`${id}\`\n\n- **${name}** → \`${topic}\`\n- ${cron ? `Cron: \`${cron}\`` : `Every ${interval_seconds}s`}\n- Status: idle, next run: now` }] };
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
        const pool = getPgPool();
        const { rows } = await pool.query(
          `SELECT id, name, topic, substring(message from 1 for 80) as message, cron, interval_seconds,
                  enabled, status, last_task_id, substring(last_result from 1 for 100) as last_result,
                  last_error, run_count, fail_count, last_run, next_run
           FROM schedules ORDER BY created_at DESC`
        );
        if (rows.length === 0) {
          return { content: [{ type: "text", text: "No schedules configured." }] };
        }
        const lines = rows.map((r) => {
          const freq = r.cron ? `cron: \`${r.cron}\`` : `every ${r.interval_seconds}s`;
          const status = r.enabled ? r.status : "disabled";
          const lastRun = r.last_run ? new Date(r.last_run).toISOString().replace("T", " ").slice(0, 19) : "never";
          const nextRun = r.next_run ? new Date(r.next_run).toISOString().replace("T", " ").slice(0, 19) : "—";
          let state = `${status} | runs: ${r.run_count} | fails: ${r.fail_count}`;
          if (r.last_result) state += `\n    Last result: ${r.last_result}`;
          if (r.last_error) state += `\n    Last error: ${r.last_error}`;
          return `- **${r.name}** (\`${r.id.slice(0, 8)}\`) → \`${r.topic}\` | ${freq}\n    ${state}\n    Last: ${lastRun} | Next: ${nextRun}`;
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
        const pool = getPgPool();
        const { rowCount } = await pool.query("DELETE FROM schedules WHERE id = $1", [schedule_id]);
        if (rowCount === 0) {
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

  // --- search_conversations ---
  server.tool(
    "search_conversations",
    "Search past conversations across all agents using full-text search. Returns matching turns with context (who said what, when, to which agent).",
    {
      query: z.string().describe("Search query — keywords or phrases to find in past conversations"),
      worker_name: z.string().optional().describe("Filter to a specific agent (e.g. 'backend-engineer')"),
      limit: z.number().optional().describe("Max results to return (default: 10)"),
    },
    async ({ query, worker_name, limit: maxResults }) => {
      try {
        const pool = getPgPool();
        const n = Math.min(maxResults || 10, 50);
        let sql = `SELECT id, worker_name, caller_id, role, substring(content from 1 for 300) as content, timestamp
                    FROM conversations
                    WHERE content ILIKE $1`;
        const params = [`%${query}%`];
        if (worker_name) {
          sql += ` AND worker_name = $${params.length + 1}`;
          params.push(worker_name);
        }
        sql += ` ORDER BY timestamp DESC LIMIT $${params.length + 1}`;
        params.push(n);

        const { rows } = await pool.query(sql, params);
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No conversations found matching "${query}".` }] };
        }
        const lines = rows.map((r) => {
          const ts = r.timestamp ? new Date(r.timestamp).toISOString().replace("T", " ").slice(0, 19) : "?";
          return `**${ts}** | ${r.worker_name} | ${r.caller_id} (${r.role}):\n${r.content}`;
        });
        return { content: [{ type: "text", text: `**Conversations matching "${query}"** (${rows.length} results):\n\n${lines.join("\n\n---\n\n")}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error searching conversations: ${err.message})` }], isError: true };
      }
    }
  );
}
