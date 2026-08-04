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
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "8808069971:AAHGimNxHN7GssOterrZjU-pHSvZ78IsoCo";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "1329256217";

async function sendTelegramNotification(text, buttons = null) {
  const body = { chat_id: TELEGRAM_CHAT_ID, text };
  if (buttons) {
    body.reply_markup = { inline_keyboard: buttons };
  }
  try {
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (err) {
    console.error("Telegram notification failed:", err.message);
  }
}

// Postgres pool for task result fallback
let _pgPool = null;
function getPgPool() {
  if (!_pgPool) {
    _pgPool = new pg.Pool({ connectionString: MESH_DB_URL, max: 3 });
    _pgPool.on("error", () => { _pgPool = null; });
  }
  return _pgPool;
}

// Resolve short ID prefix to full UUID
async function resolveId(table, shortId) {
  if (shortId.length >= 32) return shortId; // already full
  const pool = getPgPool();
  const { rows } = await pool.query(`SELECT id::text FROM ${table} WHERE id::text LIKE $1 LIMIT 2`, [shortId + "%"]);
  if (rows.length === 1) return rows[0].id;
  if (rows.length === 0) throw new Error(`No ${table} found matching '${shortId}'`);
  throw new Error(`Ambiguous: ${rows.length} ${table} match '${shortId}'`);
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
    `Ask another agent a question via RabbitMQ.

Examples:
  ask_agent(topic="datalake", message="What tables have failing rows?")
  ask_agent(topic="prod-ops", message="Check disk usage on prod", async=true)
  ask_agent(topic="platform", message="Fix the login bug", sid="w-abc123")

Use list_agents() first to see available topics. For long tasks, set async=true to get a task_id immediately — poll with get_task_result(task_id). Pass sid from a previous response for multi-turn conversations with the same worker.`,
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
      limit: z.number().optional().describe("Number of recent events to return (default: 20, max: 50)"),
      sid: z.string().optional().describe("Worker session ID to filter events. Only shows activity from this specific worker instance."),
    },
    async ({ name, limit, sid }) => {
      const maxRows = Math.min(Math.max(1, limit || 20), 50);
      try {
        const pool = getPgPool();
        let query, params;
        if (sid) {
          query = `SELECT worker_sid, event_type, content, timestamp
                   FROM activity WHERE worker_name = $1 AND worker_sid = $2
                   ORDER BY timestamp DESC LIMIT $3`;
          params = [name, sid, maxRows];
        } else {
          query = `SELECT worker_sid, event_type, content, timestamp
                   FROM activity WHERE worker_name = $1
                   ORDER BY timestamp DESC LIMIT $2`;
          params = [name, maxRows];
        }
        const { rows } = await pool.query(query, params);
        rows.reverse(); // chronological order

        if (rows.length === 0) {
          const target = sid ? `**${name}** (sid: ${sid})` : `**${name}**`;
          return {
            content: [{ type: "text", text: `No activity from ${target}. The agent may be idle or not running.` }],
          };
        }

        const lines = rows.map((r) => {
          const ts = r.timestamp ? new Date(r.timestamp).toISOString().split("T")[1]?.replace("Z", "").slice(0, 8) : "??:??:??";
          const icon = r.event_type === "tool_call" ? "[tool]"
            : r.event_type === "error" ? "[error]"
            : r.event_type === "status" ? "[status]"
            : "[text]";
          return `${ts} ${icon} ${r.content || ""}`;
        });

        return {
          content: [{ type: "text", text: `**Activity from ${name}** (${rows.length} events):\n\n\`\`\`\n${lines.join("\n")}\n\`\`\`` }],
        };
      } catch (err) {
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
        schedule_id = await resolveId("schedules", schedule_id);
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
        const pool = getPgPool();
        const { rows } = await pool.query("SELECT name FROM schedules WHERE id = $1", [schedule_id]);
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `(error: schedule \`${schedule_id}\` not found)` }], isError: true };
        }

        const conn = await amqplib.connect(RABBITMQ_URL);
        const ch = await conn.createChannel();
        await ch.assertExchange(EXCHANGE, "topic", { durable: true });
        ch.publish(EXCHANGE, "schedule.run_now", Buffer.from(JSON.stringify({ action: "run_now", schedule_id })), {
          contentType: "application/json",
        });
        await conn.close();

        return { content: [{ type: "text", text: `Fired schedule \`${rows[0].name}\` (\`${schedule_id.slice(0, 8)}\`). A worker will dispatch it now.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error firing schedule: ${err.message})` }], isError: true };
      }
    }
  );

  // --- search_conversations ---
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
        const pool = getPgPool();
        const { rows } = await pool.query("SELECT version FROM prompts WHERE agent_name = $1", [agent_name]);
        if (rows.length > 0) {
          const newVersion = (rows[0].version || 0) + 1;
          await pool.query(
            "UPDATE prompts SET content = $1, version = $2, source_file = $3, updated_by = $4, updated_at = now() WHERE agent_name = $5",
            [content, newVersion, source_file || null, AGENT_NAME, agent_name]
          );
          return { content: [{ type: "text", text: `Prompt for **${agent_name}** updated to v${newVersion} (${content.length} chars). Workers will use it on next session.` }] };
        } else {
          await pool.query(
            "INSERT INTO prompts (id, agent_name, content, version, source_file, updated_by) VALUES ($1, $2, $3, 1, $4, $5)",
            [crypto.randomUUID(), agent_name, content, source_file || null, AGENT_NAME]
          );
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
        const pool = getPgPool();
        const { rows } = await pool.query("SELECT content, version, source_file, updated_by, updated_at FROM prompts WHERE agent_name = $1", [agent_name]);
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No prompt in DB for **${agent_name}**. Worker is using file-based prompt.` }] };
        }
        const r = rows[0];
        return { content: [{ type: "text", text: `**${agent_name}** prompt (v${r.version}, ${r.content.length} chars)\nSource: ${r.source_file || "n/a"} | Updated by: ${r.updated_by} | ${r.updated_at}\n\n---\n\n${r.content}` }] };
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
        const pool = getPgPool();
        const { rows } = await pool.query("SELECT agent_name, version, length(content) as chars, source_file, updated_by, updated_at FROM prompts ORDER BY agent_name");
        if (rows.length === 0) {
          return { content: [{ type: "text", text: "No prompts in DB. All workers are using file-based prompts." }] };
        }
        const lines = rows.map((r) => `- **${r.agent_name}** v${r.version} (${r.chars} chars) — ${r.source_file || "no source"} — updated by ${r.updated_by} at ${new Date(r.updated_at).toISOString().slice(0, 19)}`);
        return { content: [{ type: "text", text: `**Prompts (${rows.length}):**\n\n${lines.join("\n")}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

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
        const pool = getPgPool();
        const id = crypto.randomUUID();
        await pool.query(
          `INSERT INTO incidents (id, repo, title, severity, gh_issue, summary, assigned_agent, status, created_by)
           VALUES ($1, $2, $3, $4, $5, $6, $7, 'open', $8)`,
          [id, repo, title, severity, gh_issue || null, summary || null, assigned_agent || null, AGENT_NAME]
        );
        const ghRef = gh_issue ? ` (${repo}#${gh_issue})` : "";
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
        const pool = getPgPool();
        let sql = "SELECT id, repo, gh_issue, gh_pr, severity, status, title, assigned_agent, created_at, updated_at FROM incidents WHERE 1=1";
        const params = [];
        if (severity) { params.push(severity); sql += ` AND severity = $${params.length}`; }
        if (status) { params.push(status); sql += ` AND status = $${params.length}`; }
        sql += " ORDER BY CASE severity WHEN 'p1' THEN 1 WHEN 'p2' THEN 2 ELSE 3 END, created_at DESC";
        const { rows } = await pool.query(sql, params);
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No incidents found${severity ? ` with severity ${severity}` : ""}${status ? ` with status ${status}` : ""}.` }] };
        }
        const lines = rows.map((r) => {
          const gh = r.gh_issue ? `${r.repo}#${r.gh_issue}` : r.repo;
          const pr = r.gh_pr ? ` → PR #${r.gh_pr}` : "";
          const agent = r.assigned_agent ? ` → ${r.assigned_agent}` : "";
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
        const pool = getPgPool();
        const sets = []; const vals = [];
        if (status) { vals.push(status); sets.push(`status = $${vals.length}`); }
        if (severity) { vals.push(severity); sets.push(`severity = $${vals.length}`); }
        if (assigned_agent) { vals.push(assigned_agent); sets.push(`assigned_agent = $${vals.length}`); }
        if (gh_pr) { vals.push(gh_pr); sets.push(`gh_pr = $${vals.length}`); }
        if (summary) { vals.push(summary); sets.push(`summary = $${vals.length}`); }
        if (status === "resolved") { sets.push("resolved_at = now()"); }
        sets.push("updated_at = now()");
        if (sets.length === 1) {
          return { content: [{ type: "text", text: "(error: nothing to update)" }], isError: true };
        }
        vals.push(incident_id);
        const { rowCount } = await pool.query(`UPDATE incidents SET ${sets.join(", ")} WHERE id = $${vals.length}`, vals);
        if (rowCount === 0) {
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
        const pool = getPgPool();
        const vals = [incident_id];
        let prSet = "";
        if (gh_pr) { vals.push(gh_pr); prSet = `, gh_pr = $${vals.length}`; }
        const { rowCount } = await pool.query(`UPDATE incidents SET status = 'resolved', resolved_at = now()${prSet}, updated_at = now() WHERE id = $1`, vals);
        if (rowCount === 0) {
          return { content: [{ type: "text", text: `(error: incident \`${incident_id}\` not found)` }], isError: true };
        }
        return { content: [{ type: "text", text: `Incident \`${incident_id.slice(0, 8)}\` resolved.${gh_pr ? ` Fix: PR #${gh_pr}` : ""}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- create_release ---
  server.tool(
    "create_release",
    `Create a release — a deployment runbook you build up during development.

Examples:
  create_release(name="LLM model picker", prs=["openbiocure/obc-connectors-core#45", "openbiocure/platform-ui#12"], requires_migration=true)
  create_release(name="Dead code cleanup", prs=["openbiocure/obc-connectors-core#391"])

Workflow: create (building) → add PRs/steps with update_release → mark ready → tell devops "deploy release <id>" → close_release when done.`,
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
        const pool = getPgPool();
        const id = crypto.randomUUID();
        await pool.query(
          `INSERT INTO releases (id, name, prs, steps, requires_migration, requires_downtime, notes, status, created_by)
           VALUES ($1, $2, $3, $4, $5, $6, $7, 'building', $8)`,
          [id, name, prs || [], steps || null, requires_migration || false, requires_downtime || false, notes || null, AGENT_NAME]
        );
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
        const pool = getPgPool();
        let sql = "SELECT id, name, prs, status, requires_migration, requires_downtime, summary, duration_seconds, created_at, deployed_at FROM releases";
        const params = [];
        if (status) { params.push(status); sql += ` WHERE status = $1`; }
        sql += " ORDER BY created_at DESC";
        const { rows } = await pool.query(sql, params);
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No releases${status ? ` with status '${status}'` : ""}.` }] };
        }
        const lines = rows.map((r) => {
          const flags = [r.requires_migration && "migration", r.requires_downtime && "downtime"].filter(Boolean);
          const prs = (r.prs || []).join(", ") || "no PRs";
          const dur = r.duration_seconds ? ` (${Math.round(r.duration_seconds)}s)` : "";
          return `- **${r.name}** (\`${r.id.slice(0, 8)}\`) — ${r.status}${dur}\n    PRs: ${prs}${flags.length ? ` | Flags: ${flags.join(", ")}` : ""}${r.summary ? `\n    Summary: ${r.summary.slice(0, 100)}` : ""}`;
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
        const pool = getPgPool();
        const { rows } = await pool.query("SELECT * FROM releases WHERE id = $1", [release_id]);
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `(error: release \`${release_id}\` not found)` }], isError: true };
        }
        const r = rows[0];
        const flags = [r.requires_migration && "requires migration", r.requires_downtime && "requires downtime"].filter(Boolean);
        let text = `# ${r.name}\n\n**Status:** ${r.status} | **Created:** ${new Date(r.created_at).toISOString().slice(0, 19)}\n`;
        if (flags.length) text += `**Flags:** ${flags.join(", ")}\n`;
        text += `**PRs:** ${(r.prs || []).join(", ") || "none"}\n`;
        if (r.steps) text += `\n## Deploy Steps\n${r.steps}\n`;
        if (r.rollback_steps) text += `\n## Rollback\n${r.rollback_steps}\n`;
        if (r.notes) text += `\n## Notes\n${r.notes}\n`;
        if (r.summary) text += `\n## Summary\n${r.summary}\n`;
        if (r.duration_seconds) text += `\n**Duration:** ${Math.round(r.duration_seconds)}s\n`;
        return { content: [{ type: "text", text }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

  // --- update_release ---
  server.tool(
    "update_release",
    "Update a release — add PRs, edit steps, change status, add notes. Use this as you build the feature.",
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
        const pool = getPgPool();
        const sets = []; const vals = [];
        if (prs !== undefined) { vals.push(prs); sets.push(`prs = $${vals.length}`); }
        if (steps !== undefined) { vals.push(steps); sets.push(`steps = $${vals.length}`); }
        if (rollback_steps !== undefined) { vals.push(rollback_steps); sets.push(`rollback_steps = $${vals.length}`); }
        if (requires_migration !== undefined) { vals.push(requires_migration); sets.push(`requires_migration = $${vals.length}`); }
        if (requires_downtime !== undefined) { vals.push(requires_downtime); sets.push(`requires_downtime = $${vals.length}`); }
        if (notes !== undefined) { vals.push(notes); sets.push(`notes = $${vals.length}`); }
        if (status) { vals.push(status); sets.push(`status = $${vals.length}`); }
        if (summary) { vals.push(summary); sets.push(`summary = $${vals.length}`); }
        if (status === "deploying") { sets.push("started_at = now()"); }
        if (status === "deployed") { sets.push("deployed_at = now()"); }
        if (sets.length === 0) {
          return { content: [{ type: "text", text: "(nothing to update)" }] };
        }
        vals.push(release_id);
        const { rowCount } = await pool.query(`UPDATE releases SET ${sets.join(", ")} WHERE id = $${vals.length}`, vals);
        if (rowCount === 0) {
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
        const pool = getPgPool();
        const sets = [`status = $1`]; const vals = [status];
        if (summary) { vals.push(summary); sets.push(`summary = $${vals.length}`); }
        if (duration_seconds !== undefined) { vals.push(duration_seconds); sets.push(`duration_seconds = $${vals.length}`); }
        if (status === "deployed") { sets.push("deployed_at = now()"); }
        vals.push(release_id);
        const { rowCount } = await pool.query(`UPDATE releases SET ${sets.join(", ")} WHERE id = $${vals.length}`, vals);
        if (rowCount === 0) {
          return { content: [{ type: "text", text: `(error: release \`${release_id}\` not found)` }], isError: true };
        }
        return { content: [{ type: "text", text: `Release \`${release_id.slice(0, 8)}\` closed as **${status}**.${summary ? `\n\n${summary}` : ""}` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

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
        const pool = getPgPool();
        const id = crypto.randomUUID();
        await pool.query(
          "INSERT INTO mesh_bugs (id, title, description, reported_by, component, severity, status) VALUES ($1, $2, $3, $4, $5, $6, 'open')",
          [id, title, description || null, AGENT_NAME, component || null, severity || "medium"]
        );
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
        const pool = getPgPool();
        let sql = "SELECT id, title, component, severity, status, reported_by, created_at FROM mesh_bugs";
        const params = [];
        if (status) { params.push(status); sql += " WHERE status = $1"; }
        sql += " ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END, created_at DESC";
        const { rows } = await pool.query(sql, params);
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No mesh bugs${status ? ` with status '${status}'` : ""}.` }] };
        }
        const lines = rows.map((r) =>
          `- **[${r.severity}]** ${r.title} (\`${r.id.slice(0, 8)}\`) — ${r.status}\n    Component: ${r.component || "?"} | Reported by: ${r.reported_by} | ${new Date(r.created_at).toISOString().slice(0, 19)}`
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
    "Update a mesh bug — change status, add fix notes.",
    {
      bug_id: z.string().describe("Bug UUID"),
      status: z.string().optional().describe("New status (open, acknowledged, fixed, wontfix)"),
      fix_notes: z.string().optional().describe("Notes on the fix"),
    },
    async ({ bug_id, status, fix_notes }) => {
      try {
        bug_id = await resolveId("mesh_bugs", bug_id);
        const pool = getPgPool();
        const sets = []; const vals = [];
        if (status) { vals.push(status); sets.push(`status = $${vals.length}`); }
        if (fix_notes) { vals.push(fix_notes); sets.push(`fix_notes = $${vals.length}`); }
        if (status === "fixed") { sets.push("resolved_at = now()"); }
        if (sets.length === 0) {
          return { content: [{ type: "text", text: "(nothing to update)" }] };
        }
        vals.push(bug_id);
        const { rowCount } = await pool.query(`UPDATE mesh_bugs SET ${sets.join(", ")} WHERE id = $${vals.length}`, vals);
        if (rowCount === 0) {
          return { content: [{ type: "text", text: `(error: bug \`${bug_id}\` not found)` }], isError: true };
        }
        return { content: [{ type: "text", text: `Bug \`${bug_id.slice(0, 8)}\` updated.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );

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
    },
    async ({ title, description }) => {
      try {
        const pool = getPgPool();
        const id = crypto.randomUUID();
        const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
        await pool.query(
          `INSERT INTO approvals (id, title, description, requested_by, status, expires_at)
           VALUES ($1, $2, $3, $4, 'pending', $5)`,
          [id, title, description || null, AGENT_NAME, expiresAt]
        );

        // Send Telegram notification with buttons
        const shortId = id.slice(0, 8);
        const text = `🔔 Approval Request\n\n${AGENT_NAME} wants to:\n\n${title}${description ? "\n\n" + description.slice(0, 500) : ""}\n\nID: ${shortId}`;
        await sendTelegramNotification(text, [
          [
            { text: "✅ Approve", callback_data: `approve:${id}` },
            { text: "❌ Reject", callback_data: `reject:${id}` },
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
        const pool = getPgPool();
        const { rows } = await pool.query("SELECT * FROM approvals WHERE id = $1", [approval_id]);
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `(error: approval not found)` }], isError: true };
        }
        const a = rows[0];
        const { rows: comments } = await pool.query(
          "SELECT author, content, created_at FROM approval_comments WHERE approval_id = $1 ORDER BY created_at ASC", [approval_id]
        );
        let text = `**${a.title}** (\`${a.id.slice(0, 8)}\`)\n\nStatus: **${a.status}**\nRequested by: ${a.requested_by}\nCreated: ${new Date(a.created_at).toISOString().slice(0, 19)}`;
        if (a.response_note) text += `\nResponse: ${a.response_note}`;
        if (comments.length > 0) {
          text += `\n\n**Comments (${comments.length}):**\n`;
          text += comments.map(c => `- **${c.author}** (${new Date(c.created_at).toISOString().slice(0, 19)}): ${c.content}`).join("\n");
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
        const pool = getPgPool();
        let sql = "SELECT id, title, requested_by, status, created_at FROM approvals";
        const params = [];
        if (status) { params.push(status); sql += " WHERE status = $1"; }
        sql += " ORDER BY created_at DESC";
        const { rows } = await pool.query(sql, params);
        if (rows.length === 0) {
          return { content: [{ type: "text", text: `No approvals${status ? ` with status '${status}'` : ""}.` }] };
        }
        const lines = rows.map(r =>
          `- **${r.title}** (\`${r.id.slice(0, 8)}\`) — ${r.status}\n    By: ${r.requested_by} | ${new Date(r.created_at).toISOString().slice(0, 19)}`
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
        const pool = getPgPool();
        const { rowCount } = await pool.query(
          "UPDATE approvals SET status = 'approved', response_note = $1, responded_at = now() WHERE id = $2 AND status = 'pending'",
          [note || null, approval_id]
        );
        if (rowCount === 0) {
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
      note: z.string().optional().describe("Feedback for the agent — why rejected, what to change"),
    },
    async ({ approval_id, note }) => {
      try {
        approval_id = await resolveId("approvals", approval_id);
        const pool = getPgPool();
        const { rowCount } = await pool.query(
          "UPDATE approvals SET status = 'rejected', response_note = $1, responded_at = now() WHERE id = $2 AND status = 'pending'",
          [note || null, approval_id]
        );
        if (rowCount === 0) {
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
        const pool = getPgPool();
        await pool.query(
          "INSERT INTO approval_comments (id, approval_id, author, content) VALUES ($1, $2, $3, $4)",
          [crypto.randomUUID(), approval_id, AGENT_NAME, content]
        );
        return { content: [{ type: "text", text: `Comment added to approval \`${approval_id.slice(0, 8)}\`.` }] };
      } catch (err) {
        return { content: [{ type: "text", text: `(error: ${err.message})` }], isError: true };
      }
    }
  );
}
