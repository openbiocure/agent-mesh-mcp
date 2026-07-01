#!/usr/bin/env node
/**
 * MCP server for inter-agent communication via RabbitMQ.
 *
 * Tools:
 *   list_agents()          — list online agents and their topics
 *   ask_agent(topic, question) — ask an agent a question by topic
 *
 * Env: RABBITMQ_URL, RABBITMQ_MGMT_URL, AGENT_NAME, ASK_TIMEOUT, EXCHANGE_NAME
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import amqplib from "amqplib";

const EXCHANGE = process.env.EXCHANGE_NAME || "agents";
const RABBITMQ_URL =
  process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";
const RABBITMQ_MGMT_URL =
  process.env.RABBITMQ_MGMT_URL || "http://localhost:15672";
const AGENT_NAME = process.env.AGENT_NAME || "unknown";
const TIMEOUT = parseInt(process.env.ASK_TIMEOUT || "900", 10) * 1000;

// Extract credentials from AMQP URL for management API auth
function getMgmtAuth() {
  try {
    const url = new URL(RABBITMQ_URL);
    return btoa(`${url.username}:${url.password}`);
  } catch {
    return btoa("guest:guest");
  }
}

const server = new McpServer({
  name: "agent-mesh",
  version: "0.3.0",
});

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
          content: [
            {
              type: "text",
              text: `(error: RabbitMQ management API returned ${resp.status}. Is the management plugin enabled?)`,
            },
          ],
          isError: true,
        };
      }

      const queues = await resp.json();
      const onlineQueues = queues.filter(
        (q) => q.name.startsWith("agent.") && q.consumers > 0
      );

      // Fetch bindings for each online queue to get actual routing keys
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
          topics: topics,
          queue: q.name,
          consumers: q.consumers,
          pending_messages: q.messages,
        });
      }

      if (agents.length === 0) {
        return {
          content: [
            { type: "text", text: "No agents are currently online." },
          ],
        };
      }

      const lines = agents.map(
        (a) =>
          `- **${a.name}** — topic${a.topics.length > 1 ? "s" : ""}: ${a.topics.map((t) => `\`${t}\``).join(", ")} (${a.consumers} consumer${a.consumers > 1 ? "s" : ""}, ${a.pending_messages} pending)`
      );

      return {
        content: [
          {
            type: "text",
            text: `**Online agents (${agents.length}):**\n${lines.join("\n")}`,
          },
        ],
      };
    } catch (err) {
      return {
        content: [
          {
            type: "text",
            text: `(error listing agents: ${err.message})`,
          },
        ],
        isError: true,
      };
    }
  }
);

server.tool(
  "ask_agent",
  "Ask another agent a question via RabbitMQ. Use list_agents() first to see available topics.",
  {
    topic: z
      .string()
      .describe(
        "The topic to route the question to. Use list_agents() to see available topics."
      ),
    message: z.string().describe("The message to send to the other agent"),
    timeout: z
      .number()
      .optional()
      .describe(
        "Timeout in seconds to wait for a reply. Default: 900 (15 min). Use shorter for simple questions (e.g. 30), longer for complex tasks."
      ),
  },
  async ({ topic, message, timeout: timeoutSec }) => {
    const routingKey = topic.startsWith("ask.") ? topic : `ask.${topic}`;
    const correlationId = crypto.randomUUID();
    const timeoutMs = (timeoutSec || TIMEOUT / 1000) * 1000;

    let conn;
    try {
      conn = await amqplib.connect(RABBITMQ_URL);
      const ch = await conn.createChannel();

      await ch.assertExchange(EXCHANGE, "topic", { durable: true });
      const { queue: replyQueue } = await ch.assertQueue("", {
        exclusive: true,
      });

      const body = JSON.stringify({ from: AGENT_NAME, message });

      ch.publish(EXCHANGE, routingKey, Buffer.from(body), {
        replyTo: replyQueue,
        correlationId,
        contentType: "application/json",
      });

      const reply = await new Promise((resolve) => {
        const timer = setTimeout(() => {
          resolve(
            `(timeout: no agent replied on ${routingKey} within ${timeoutMs / 1000}s)`
          );
        }, timeoutMs);

        ch.consume(
          replyQueue,
          (msg) => {
            if (msg?.properties.correlationId === correlationId) {
              clearTimeout(timer);
              resolve(msg.content.toString());
            }
          },
          { noAck: true }
        );
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

const transport = new StdioServerTransport();
await server.connect(transport);
