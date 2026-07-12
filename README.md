# agent-mesh-mcp

MCP server for inter-agent communication via RabbitMQ. Two transports:

- **stdio** (`index.mjs`) — for local Claude Code / Cursor sessions
- **HTTP** (`server.mjs`) — for remote access from Claude.ai, Claude Desktop, or mobile via Streamable HTTP + Keycloak OAuth

## Install

```bash
npm install -g agent-mesh-mcp
```

## Local Usage (stdio)

### Claude Code (`~/.claude/settings.local.json`)

```json
{
  "mcpServers": {
    "agent-mesh": {
      "command": "agent-mesh-mcp",
      "env": {
        "RABBITMQ_URL": "amqp://user:pass@your-rabbitmq:5672/",
        "RABBITMQ_MGMT_URL": "http://your-rabbitmq:15672",
        "EXCHANGE_NAME": "agents",
        "AGENT_NAME": "human"
      }
    }
  }
}
```

### Cursor (`.cursor/mcp.json`)

Same format as above.

## Remote Usage (HTTP)

Run `server.mjs` as a standalone HTTP server with Keycloak OAuth:

```bash
MCP_PORT=3100 \
KEYCLOAK_URL=https://your-keycloak \
KEYCLOAK_REALM=master \
KEYCLOAK_CLIENT_ID=agent-mesh \
KEYCLOAK_CLIENT_SECRET=your-secret \
RABBITMQ_URL=amqp://user:pass@your-rabbitmq:5672/ \
RABBITMQ_MGMT_URL=http://your-rabbitmq:15672 \
EXCHANGE_NAME=agents \
node server.mjs
```

Then add as a custom connector in Claude.ai:
- **URL**: `https://your-domain/mcp`
- Claude.ai handles OAuth automatically via `/.well-known/oauth-authorization-server`

### Architecture

```
Claude.ai / Phone / Desktop
        |
        v  HTTPS
   your-domain/mcp (Cloudflare Tunnel or reverse proxy)
        |
        v
   server.mjs (Streamable HTTP + OAuth)
        |
        v  AMQP
   RabbitMQ --> All workers (any machine)
```

### OAuth Flow

The server proxies OAuth to Keycloak:
- `GET /authorize` — redirects to Keycloak login
- `POST /token` — exchanges auth code for access token via Keycloak
- `POST /register` — dynamic client registration
- `GET /.well-known/oauth-authorization-server` — OAuth metadata
- `GET /health` — health check (no auth)

All `/mcp` requests require a valid Keycloak bearer token.

## Tools

### `list_agents()`

List all online agents and their topics.

### `ask_agent(topic, message, timeout?)`

Ask another agent a question via RabbitMQ.

**Parameters:**
- `topic` — routing key (e.g. `datalake`, `platform`, `ops`, `prod`)
- `message` — the message to send
- `timeout` — seconds to wait for reply (default: 900)

**Returns:** the agent's reply, or a timeout message.

## How It Works

```
Caller --> ask_agent(topic, message)
              |
              v
         RabbitMQ (topic exchange)
              |
              v
         Worker on ask.<topic> queue
              |
              v
         Agent processes message (Claude Agent SDK)
              |
              v
         Reply --> RabbitMQ --> Caller
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection URL |
| `RABBITMQ_MGMT_URL` | `http://localhost:15672` | RabbitMQ management API |
| `AGENT_NAME` | `unknown` | Name of the calling agent |
| `ASK_TIMEOUT` | `900` | Reply timeout in seconds |
| `EXCHANGE_NAME` | `agents` | RabbitMQ exchange name |
| `MCP_PORT` | `3100` | HTTP server port (server.mjs only) |
| `KEYCLOAK_URL` | `https://identity.openbiocure.ai` | Keycloak base URL (server.mjs only) |
| `KEYCLOAK_REALM` | `openbiocure` | Keycloak realm (server.mjs only) |
| `KEYCLOAK_CLIENT_ID` | `agent-mesh` | OAuth client ID (server.mjs only) |
| `KEYCLOAK_CLIENT_SECRET` | — | OAuth client secret (server.mjs only) |

## License

MIT
