# acp-to-ws

`acp-to-ws` is an ACP (Agent Client Protocol) proxy. It starts an ACP Agent over standard input/output and exposes it to ACP Clients over WebSocket.

```text
ACP Client <-- WebSocket / JSON-RPC 2.0 --> acp-to-ws <-- stdio / NDJSON --> ACP Agent
```

## Features

- Starts exactly one stdio ACP Agent when the proxy starts; the proxy exits when the Agent exits.
- Supports multiple WebSocket client connections, while keeping only the latest connection active.
- Manages JSON-RPC request ID mappings between the Client and Agent automatically.
- Caches the first `initialize` response and the latest session setup (`cwd`, `mcpServers`, and `sessionId`).
- Allows only one `session/prompt` Prompt Turn at a time; a newer client can take over subsequent Prompt messages.
- Caches Agent requests when no client is connected and sends them when a client connects.

## Requirements

- Node.js 19 or later
- npm
- An Agent that runs through the ACP stdio interface

If you are sticking to Node 18, just run `index.js` with `--experimental-global-webcrypto`.

## Installation

Install from npm:

```sh
npm install -g acp-to-ws
```

Install from source:

```sh
git clone https://github.com/qwertycxz/acp-to-ws.git
cd acp-to-ws
npm ci
npm run build
```

## Usage

```text
acp-to-ws [--host <host>] [--port <port>] -- <stdio-agent-command> [args...]
```

For example:

```sh
acp-to-ws --host 127.0.0.1 --port 8080 -- node ./dist/agent.js
```

To run the built entry point directly:

```sh
npm run start -- --port 8080 -- npx tsx ./agent.ts
```

After startup, connect the WebSocket client to:

```text
ws://127.0.0.1:8080
```

Command-line options:

| Option            | Default     | Description                 |
| ----------------- | ----------- | --------------------------- |
| `--host`          | `127.0.0.1` | WebSocket listening address |
| `--port`          | `80`        | WebSocket listening port    |
| `-h`, `--help`    | -           | Display usage and exit      |
| `-v`, `--version` | -           | Display version and exit    |

Use `--` to separate proxy options from the Agent command and its arguments. The default port is `80`; listening on this port may require additional privileges on some operating systems. In most cases, explicitly using a high port such as `8080` is recommended.

## Runtime Behavior

### Connection Management

Each proxy process owns one Agent. When a client establishes a new connection, it becomes the latest active connection. The previous WebSocket connection remains open, but its subsequent requests receive the following error:

```json
{
	"code": -32010,
	"message": "A newer connection has been established. Please close this connection and reconnect."
}
```

Clients should close the old connection and reconnect after receiving this error.

### Initialization and Session Setup

- The first client's `initialize` request is forwarded to the Agent unchanged.
- The Agent's initialization response is cached. Later `initialize` requests return the cached response directly, regardless of their contents.
- When no Prompt Turn is active, `session/load`, `session/resume`, and `session/new` are forwarded transparently, and the latest `cwd`, `mcpServers`, and `sessionId` are recorded.
- The `sessionId` returned by a successful `session/new` becomes the proxy's cached session ID.
- The cache is cleared whenever the Agent returns an error for one of these session setup requests.
- During a Prompt Turn, the contents of client `session/load`, `session/resume`, and `session/new` requests are ignored; the proxy sends `session/load` using the cached session setup.

### Prompt Turns

A Prompt Turn starts with a `session/prompt` request and ends when its final response has been sent:

- A new `session/prompt` receives an error if no session setup is cached or another Prompt Turn is already active.
- Messages produced by the Agent during the Prompt Turn, along with the final response, are sent to the latest active client at that time.
- Completion responses for session setup requests accepted during the Prompt Turn are held until the Prompt Turn ends.
- If the client associated with a request is replaced by a newer connection during this time, that request receives the reconnect-required error.

### Messages and Cancellation

Ordinary JSON-RPC messages and notifications are forwarded between the Client and Agent. The proxy assigns new IDs to forwarded requests and restores the client's original ID when the response returns.

`$/cancel_request` is handled specially so that it can be mapped correctly between the Client's and Agent's ID spaces. `session/cancel` ignores the `sessionId` provided by the client and uses the proxy's cached session ID instead. The notification is ignored when no session setup is cached.

## Development

```sh
npm i
npm run build
npm run dev -- --port 8080 -- node ./dist/agent.js
```

Common commands:

| Command                | Description                         |
| ---------------------- | ----------------------------------- |
| `npm run build`        | Compile TypeScript into `dist/`     |
| `npm run start --`     | Run the built proxy                 |
| `npm run dev --`       | Run the proxy directly              |
| `npm run clean`        | Remove TypeScript build output      |
| `npm run format`       | Format the project with Prettier    |
| `npm run format:check` | Check formatting                    |
| `npm run lint`         | Check the code with Biome           |
| `npm run lint:fix`     | Automatically fix applicable issues |

## Contributor

[@qwertycxz](https://github.com/qwertycxz)

## How could I contribute?

[Issue](https://github.com/qwertycxz/MetadataWildcard4fabric-permissions-api/issues/new) and [Pull-requests](https://github.com/qwertycxz/MetadataWildcard4fabric-permissions-api/compare) are both welcomed.

## License

[Apache 2.0](LICENSE) © qwertycxz
