#!/usr/bin/env node
/**
 * MCP server for inter-agent communication via RabbitMQ.
 *
 * Exposes ask_agent(topic, question) as an MCP tool.
 * Agents publish questions to a topic exchange, other agents consume and reply.
 *
 * Env: RABBITMQ_URL, AGENT_NAME, ASK_TIMEOUT, EXCHANGE_NAME
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import amqplib from "amqplib";

const EXCHANGE = process.env.EXCHANGE_NAME || "agents";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";
const AGENT_NAME = process.env.AGENT_NAME || "unknown";
const TIMEOUT = parseInt(process.env.ASK_TIMEOUT || "900", 10) * 1000;

const server = new McpServer({
  name: "agent-mesh",
  version: "0.2.0",
});

server.tool(
  "ask_agent",
  "Ask another agent a question via RabbitMQ. The question is routed by topic to the responsible agent.",
  {
    topic: z
      .string()
      .describe("The topic to route the question to (e.g. backend, frontend, ops, qa)"),
    question: z.string().describe("The question to ask the other agent"),
  },
  async ({ topic, question }) => {
    const routingKey = topic.startsWith("ask.") ? topic : `ask.${topic}`;
    const correlationId = crypto.randomUUID();

    let conn;
    try {
      conn = await amqplib.connect(RABBITMQ_URL);
      const ch = await conn.createChannel();

      await ch.assertExchange(EXCHANGE, "topic", { durable: true });
      const { queue: replyQueue } = await ch.assertQueue("", {
        exclusive: true,
      });

      const body = JSON.stringify({ from: AGENT_NAME, question });

      ch.publish(EXCHANGE, routingKey, Buffer.from(body), {
        replyTo: replyQueue,
        correlationId,
        contentType: "application/json",
      });

      const reply = await new Promise((resolve) => {
        const timer = setTimeout(() => {
          resolve(
            `(timeout: no agent replied on ${routingKey} within ${TIMEOUT / 1000}s)`
          );
        }, TIMEOUT);

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
