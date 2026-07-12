/**
 * Shared MCP tool definitions for agent-mesh.
 * Used by both index.mjs (stdio) and server.mjs (HTTP).
 */

import { z } from "zod";
import amqplib from "amqplib";
import crypto from "crypto";

const EXCHANGE = process.env.EXCHANGE_NAME || "agents";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";
const RABBITMQ_MGMT_URL = process.env.RABBITMQ_MGMT_URL || "http://localhost:15672";
const AGENT_NAME = process.env.AGENT_NAME || "unknown";
const TIMEOUT = parseInt(process.env.ASK_TIMEOUT || "900", 10) * 1000;

// In-memory store for async task results
const taskResults = new Map();

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
    "Ask another agent a question via RabbitMQ. Use list_agents() first to see available topics. For long tasks, set async=true to get a task_id back immediately and poll with get_task_result.",
    {
      topic: z.string().describe("The topic to route the question to."),
      message: z.string().describe("The message to send to the other agent"),
      timeout: z.number().optional().describe("Timeout in seconds (default: 900). Ignored when async=true."),
      async: z.boolean().optional().describe("If true, returns a task_id immediately instead of waiting. Poll with get_task_result(task_id). Use for long-running tasks."),
    },
    async ({ topic, message, timeout: timeoutSec, async: isAsync }) => {
      const routingKey = topic.startsWith("ask.") ? topic : `ask.${topic}`;
      const correlationId = crypto.randomUUID();
      const timeoutMs = (timeoutSec || TIMEOUT / 1000) * 1000;

      let conn;
      try {
        conn = await amqplib.connect(RABBITMQ_URL);
        const ch = await conn.createChannel();

        await ch.assertExchange(EXCHANGE, "topic", { durable: true });
        const { queue: replyQueue } = await ch.assertQueue("", { exclusive: true });

        const bodyObj = { from: AGENT_NAME, message };
        if (isAsync) bodyObj.async = true;

        ch.publish(EXCHANGE, routingKey, Buffer.from(JSON.stringify(bodyObj)), {
          replyTo: replyQueue,
          correlationId,
          contentType: "application/json",
        });

        if (isAsync) {
          // Wait for the immediate ack (task_id) — should arrive within a few seconds
          const ack = await new Promise((resolve) => {
            const timer = setTimeout(() => {
              resolve(null);
            }, 10000);

            ch.consume(replyQueue, (msg) => {
              if (msg?.properties.correlationId === correlationId) {
                clearTimeout(timer);
                resolve(msg.content.toString());
              }
            }, { noAck: true });
          });

          if (!ack) {
            await conn.close();
            return { content: [{ type: "text", text: "(error: no ack received for async task)" }], isError: true };
          }

          // Parse the ack to get task_id
          let taskId;
          try {
            const ackData = JSON.parse(ack);
            taskId = ackData.task_id;
          } catch {
            taskId = null;
          }

          if (!taskId) {
            await conn.close();
            return { content: [{ type: "text", text: ack }] };
          }

          // Start background listener for the result
          taskResults.set(taskId, { status: "processing", result: null });

          ch.consume(replyQueue, (msg) => {
            if (msg?.properties.correlationId === correlationId) {
              const content = msg.content.toString();
              try {
                const data = JSON.parse(content);
                taskResults.set(taskId, { status: data.status || "completed", result: data.result || content, raw: data });
              } catch {
                taskResults.set(taskId, { status: "completed", result: content });
              }
              conn.close().catch(() => {});
            }
          }, { noAck: true });

          return {
            content: [{ type: "text", text: `Task accepted. ID: \`${taskId}\`\n\nUse \`get_task_result("${taskId}")\` to poll for the result.` }],
          };
        }

        // Synchronous — wait for reply
        const reply = await new Promise((resolve) => {
          const timer = setTimeout(() => {
            resolve(`(timeout: no agent replied on ${routingKey} within ${timeoutMs / 1000}s)`);
          }, timeoutMs);

          ch.consume(replyQueue, (msg) => {
            if (msg?.properties.correlationId === correlationId) {
              clearTimeout(timer);
              resolve(msg.content.toString());
            }
          }, { noAck: true });
        });

        await conn.close();
        return { content: [{ type: "text", text: reply }] };
      } catch (err) {
        if (conn) await conn.close().catch(() => {});
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
      const task = taskResults.get(task_id);
      if (!task) {
        return {
          content: [{ type: "text", text: `(error: unknown task_id "${task_id}")` }],
          isError: true,
        };
      }

      if (task.status === "processing") {
        return {
          content: [{ type: "text", text: `Task \`${task_id}\` is still processing. Try again in a few seconds.` }],
        };
      }

      // Clean up after retrieval
      taskResults.delete(task_id);

      return {
        content: [{ type: "text", text: task.result }],
      };
    }
  );
}
