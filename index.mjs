#!/usr/bin/env node
/**
 * MCP server for inter-agent communication via RabbitMQ (stdio transport).
 *
 * Tools:
 *   list_agents()                    — list online agents and their topics
 *   ask_agent(topic, message)        — ask an agent a question by topic
 *   get_task_result(task_id)         — poll async task result
 *
 * Env: RABBITMQ_URL, RABBITMQ_MGMT_URL, AGENT_NAME, ASK_TIMEOUT, EXCHANGE_NAME
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { registerTools } from "./tools.mjs";

const server = new McpServer({
  name: "agent-mesh",
  version: "0.5.0",
});

registerTools(server);

const transport = new StdioServerTransport();
await server.connect(transport);
