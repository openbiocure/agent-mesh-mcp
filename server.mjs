#!/usr/bin/env node
/**
 * Remote MCP server for agent-mesh — Streamable HTTP transport with Keycloak auth.
 *
 * Exposes the same list_agents + ask_agent tools as the stdio transport (index.mjs),
 * but over HTTP so Claude.ai / Claude Desktop can connect as a custom connector.
 *
 * Env:
 *   MCP_PORT          — HTTP port (default: 3100)
 *   KEYCLOAK_URL      — Keycloak base URL (e.g. https://identity.openbiocure.ai)
 *   KEYCLOAK_REALM    — Keycloak realm (default: openbiocure)
 *   RABBITMQ_URL      — RabbitMQ connection
 *   RABBITMQ_MGMT_URL — RabbitMQ management API
 *   AGENT_NAME        — Identity for inter-agent communication
 *   EXCHANGE_NAME     — RabbitMQ exchange
 *   ASK_TIMEOUT       — Default ask timeout in seconds
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { z } from "zod";
import amqplib from "amqplib";
import crypto from "crypto";
import express from "express";

// --- Config ---

const PORT = parseInt(process.env.MCP_PORT || "3100", 10);
const KEYCLOAK_URL = process.env.KEYCLOAK_URL || "https://identity.openbiocure.ai";
const KEYCLOAK_REALM = process.env.KEYCLOAK_REALM || "openbiocure";
const EXCHANGE = process.env.EXCHANGE_NAME || "agents";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672/";
const RABBITMQ_MGMT_URL = process.env.RABBITMQ_MGMT_URL || "http://localhost:15672";
const AGENT_NAME = process.env.AGENT_NAME || "remote";
const TIMEOUT = parseInt(process.env.ASK_TIMEOUT || "900", 10) * 1000;

// --- Keycloak token verification ---

async function verifyKeycloakToken(token) {
  const resp = await fetch(
    `${KEYCLOAK_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/userinfo`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  if (!resp.ok) {
    throw new Error(`Token verification failed: ${resp.status}`);
  }
  return await resp.json();
}

// --- Auth middleware ---

function bearerAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    res.status(401).json({ error: "Missing or invalid Authorization header" });
    return;
  }
  const token = authHeader.slice(7);
  verifyKeycloakToken(token)
    .then((userInfo) => {
      req.auth = { token, clientId: userInfo.sub, scopes: [] };
      next();
    })
    .catch((err) => {
      console.error("Auth failed:", err.message);
      res.status(401).json({ error: "Invalid token" });
    });
}

// --- RabbitMQ helpers ---

function getMgmtAuth() {
  try {
    const url = new URL(RABBITMQ_URL);
    return btoa(`${url.username}:${url.password}`);
  } catch {
    return btoa("guest:guest");
  }
}

// --- MCP Server ---

function createServer() {
  const server = new McpServer({
    name: "agent-mesh",
    version: "0.4.0",
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

  server.tool(
    "ask_agent",
    "Ask another agent a question via RabbitMQ. Use list_agents() first to see available topics.",
    {
      topic: z.string().describe("The topic to route the question to."),
      message: z.string().describe("The message to send to the other agent"),
      timeout: z.number().optional().describe("Timeout in seconds (default: 900)."),
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
        const { queue: replyQueue } = await ch.assertQueue("", { exclusive: true });

        const body = JSON.stringify({ from: AGENT_NAME, message });

        ch.publish(EXCHANGE, routingKey, Buffer.from(body), {
          replyTo: replyQueue,
          correlationId,
          contentType: "application/json",
        });

        const reply = await new Promise((resolve) => {
          const timer = setTimeout(() => {
            resolve(`(timeout: no agent replied on ${routingKey} within ${timeoutMs / 1000}s)`);
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

  return server;
}

// --- OAuth endpoints (proxy to Keycloak) ---

const KC_BASE = `${KEYCLOAK_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect`;
const KC_CLIENT_ID = process.env.KEYCLOAK_CLIENT_ID || "agent-mesh";
const KC_CLIENT_SECRET = process.env.KEYCLOAK_CLIENT_SECRET || "";

// --- HTTP Server ---

const app = createMcpExpressApp({
  host: "0.0.0.0",
  allowedHosts: ["mesh.openbiocure.ai", "localhost"],
});

// Parse URL-encoded bodies for /token endpoint
app.use(express.urlencoded({ extended: true }));

// OAuth discovery — tells Claude.ai where the OAuth endpoints are
app.get("/.well-known/oauth-authorization-server", (req, res) => {
  res.json({
    issuer: `https://mesh.openbiocure.ai`,
    authorization_endpoint: `https://mesh.openbiocure.ai/authorize`,
    token_endpoint: `https://mesh.openbiocure.ai/token`,
    registration_endpoint: `https://mesh.openbiocure.ai/register`,
    response_types_supported: ["code"],
    grant_types_supported: ["authorization_code", "refresh_token"],
    code_challenge_methods_supported: ["S256"],
    token_endpoint_auth_methods_supported: ["client_secret_post", "client_secret_basic"],
  });
});

// OAuth authorize — redirect to Keycloak with all params
app.get("/authorize", (req, res) => {
  const params = new URLSearchParams({
    response_type: req.query.response_type || "code",
    client_id: KC_CLIENT_ID,
    redirect_uri: req.query.redirect_uri,
    state: req.query.state || "",
    code_challenge: req.query.code_challenge || "",
    code_challenge_method: req.query.code_challenge_method || "S256",
    scope: req.query.scope || "openid",
  });
  res.redirect(`${KC_BASE}/auth?${params}`);
});

// OAuth token exchange — proxy to Keycloak
app.post("/token", async (req, res) => {
  try {
    const body = new URLSearchParams(req.body);
    // Replace the client_id with our Keycloak client
    body.set("client_id", KC_CLIENT_ID);
    body.set("client_secret", KC_CLIENT_SECRET);
    // Fix redirect_uri if needed
    const resp = await fetch(`${KC_BASE}/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: body.toString(),
    });
    const data = await resp.json();
    res.status(resp.status).json(data);
  } catch (err) {
    console.error("Token exchange error:", err.message);
    res.status(500).json({ error: "token_exchange_failed" });
  }
});

// Dynamic client registration (Claude.ai may call this)
app.post("/register", (req, res) => {
  res.status(201).json({
    client_id: KC_CLIENT_ID,
    client_secret: KC_CLIENT_SECRET,
    redirect_uris: req.body.redirect_uris || [],
    grant_types: ["authorization_code", "refresh_token"],
    response_types: ["code"],
    token_endpoint_auth_method: "client_secret_post",
  });
});

// Auth on the MCP endpoint
app.use("/mcp", bearerAuth);

// Session management
const transports = new Map();

app.post("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"];

  if (sessionId && transports.has(sessionId)) {
    // Existing session
    const transport = transports.get(sessionId);
    await transport.handleRequest(req, res, req.body);
    return;
  }

  // New session
  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: () => crypto.randomUUID(),
  });

  const server = createServer();
  await server.connect(transport);

  // Store session after connect so the session ID is set
  transport.onclose = () => {
    const sid = transport.sessionId;
    if (sid) transports.delete(sid);
  };

  await transport.handleRequest(req, res, req.body);

  if (transport.sessionId) {
    transports.set(transport.sessionId, transport);
  }
});

app.get("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"];
  if (!sessionId || !transports.has(sessionId)) {
    res.status(400).json({ error: "Invalid or missing session ID" });
    return;
  }
  const transport = transports.get(sessionId);
  await transport.handleRequest(req, res);
});

app.delete("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"];
  if (sessionId && transports.has(sessionId)) {
    const transport = transports.get(sessionId);
    await transport.handleRequest(req, res);
    transports.delete(sessionId);
  } else {
    res.status(404).json({ error: "Session not found" });
  }
});

// Health check (no auth)
app.get("/health", (req, res) => {
  res.json({ status: "ok", sessions: transports.size });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Agent Mesh MCP (HTTP) listening on :${PORT}`);
  console.log(`Keycloak: ${KEYCLOAK_URL}/realms/${KEYCLOAK_REALM}`);
  console.log(`RabbitMQ: ${RABBITMQ_URL}`);
});
