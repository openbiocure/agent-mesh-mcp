# agent-mesh-mcp

MCP server for inter-agent communication via RabbitMQ. Agents ask each other questions routed by topic.

## Install

```bash
npm install -g agent-mesh-mcp
```

## Usage

### Claude Code (`~/.claude.json`)

```json
{
  "mcpServers": {
    "agent-mesh": {
      "command": "agent-mesh-mcp",
      "env": {
        "RABBITMQ_URL": "amqp://user:pass@your-rabbitmq:5672/",
        "AGENT_NAME": "human"
      }
    }
  }
}
```

### Cursor (`.cursor/mcp.json`)

```json
{
  "mcpServers": {
    "agent-mesh": {
      "command": "agent-mesh-mcp",
      "env": {
        "RABBITMQ_URL": "amqp://user:pass@your-rabbitmq:5672/",
        "AGENT_NAME": "cursor"
      }
    }
  }
}
```

### npx (no install)

```json
{
  "mcpServers": {
    "agent-mesh": {
      "command": "npx",
      "args": ["-y", "agent-mesh-mcp"],
      "env": {
        "RABBITMQ_URL": "amqp://user:pass@your-rabbitmq:5672/"
      }
    }
  }
}
```

## Tool

### `ask_agent(topic, question)`

Ask another agent a question. The question is published to a RabbitMQ topic exchange and routed to the agent listening on that topic.

**Parameters:**
- `topic` — routing key (e.g. `backend`, `frontend`, `ops`, `qa`, `devops`)
- `question` — the question to ask

**Returns:** the agent's reply, or a timeout message.

## How It Works

```
Caller ──► ask_agent(topic, question)
              │
              ▼
         RabbitMQ (topic exchange)
              │
              ▼
         Worker on ask.<topic> queue
              │
              ▼
         Agent processes question
              │
              ▼
         Reply ──► RabbitMQ ──► Caller
```

Each agent is a worker process listening on its topic queue. When a question arrives, the worker runs an AI model (e.g. Claude) to answer it, then publishes the reply back.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection URL |
| `AGENT_NAME` | `unknown` | Name of the calling agent |
| `ASK_TIMEOUT` | `900` | Reply timeout in seconds |
| `EXCHANGE_NAME` | `agents` | RabbitMQ exchange name |

## License

MIT
