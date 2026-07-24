#!/usr/bin/env node
import { AGENT_METHODS, ndJsonStream, PROTOCOL_METHODS, RequestError, type AnyMessage, type AnyRequest, type AnyResponse, type JsonRpcId, type LoadSessionRequest, type NewSessionRequest, type SessionId } from '@agentclientprotocol/sdk'
import { spawn } from 'node:child_process'
import { constants } from 'node:os'
import { Readable, Writable } from 'node:stream'
import { parseArgs } from 'node:util'
import { WebSocket, WebSocketServer } from 'ws'

function messageClient(client: WebSocket, message: Omit<AnyMessage, 'jsonrpc'>) {
	client.send(
		JSON.stringify({
			...message,
			jsonrpc: '2.0',
		}),
	)
}

const AGENT2CLIENT_ID = new Map<WebSocket, Map<JsonRpcId, JsonRpcId>>()
interface Agent2ClientRequest {
	client?: WebSocket
	id?: JsonRpcId
	request: AnyRequest
}
let ws_client: WebSocket | undefined = undefined

function requestClient(request: Agent2ClientRequest) {
	if (!ws_client) return
	request.client = ws_client
	request.id = crypto.randomUUID()

	let requests = AGENT2CLIENT_ID.get(ws_client)
	if (requests) {
		requests.set(request.id, request.request.id)
	} else {
		AGENT2CLIENT_ID.set(ws_client, new Map([[request.id, request.request.id]]))
	}

	messageClient(ws_client, {
		...request.request,
		id: request.id,
	})
}

const DEFAULT_HOST = '127.0.0.1'
const DEFAULT_PORT = '80'

const {
	positionals: [spawn_command, ...spawn_arguments],
	values: { help, host, port },
} = parseArgs({
	allowPositionals: true,
	options: {
		help: {
			short: 'h',
			type: 'boolean',
		},
		host: {
			default: DEFAULT_HOST,
			type: 'string',
		},
		port: {
			default: DEFAULT_PORT,
			type: 'string',
		},
	},
})

if (help || !spawn_command) {
	console.error(`Usage:
	acp-to-ws [--host <host>] [--port <port>] -- <stdio-agent-command> [args...]

Examples:
	acp-to-ws --port 80 -- npx tsx ./agent.ts
	node dist/index.js --host 0.0.0.0 --port 80 -- node ./dist/agent.js

Args:
	<host>  Defaults to ${DEFAULT_HOST}
	<port>  Defaults to ${DEFAULT_PORT}`)
	process.exit(constants.errno.EINVAL)
}

const STDIO_AGENT = spawn(spawn_command, spawn_arguments, {
	stdio: ['overlapped', 'overlapped', 'inherit'],
})

STDIO_AGENT.on('error', error => {
	console.error('STDIO agent error:', error)
	process.exit(constants.errno.ENOENT)
})

STDIO_AGENT.on('exit', (code, signal) => {
	console.error('STDIO agent signal:', signal)
	code ??= constants.errno.ENOENT
	process.exit(code)
})

const WS_SERVER = new WebSocketServer({
	host,
	perMessageDeflate: true,
	port: parseInt(port),
})

WS_SERVER.on('error', error => {
	console.error('WebSocket server error:', error)
	process.exit(constants.errno.EIO)
})

const CLIENT2AGENT_ID = new Map<WebSocket, Map<JsonRpcId, JsonRpcId>>()
type Client2AgentRequest = {
	client: WebSocket
	id: JsonRpcId
} & (
		| {
			kind: 'initialize-request' | 'other-request' | 'session-load' | 'session-prompt' | 'session-resume'
		}
		| {
			kind: 'prompt-load' | 'prompt-new' | 'prompt-resume'
			session: SessionId
		}
		| {
			kind: 'session-new'
			request: NewSessionRequest
		}
	)
const CLIENT2AGENT_REQUESTS = new Map<JsonRpcId, Client2AgentRequest>()
const CONNECTION_REPLACED = new RequestError(-32010, 'A newer connection has been established. Please close this connection and reconnect.').toErrorResponse()

let initialize_id: JsonRpcId | undefined
let prompt_id: JsonRpcId | undefined
let prompt_responses: {
	client: WebSocket
	response: AnyResponse
}[] = []

function removeClient(client: WebSocket) {
	AGENT2CLIENT_ID.delete(client)
	CLIENT2AGENT_ID.delete(client)

	for (const [id, request] of CLIENT2AGENT_REQUESTS) {
		if (request.client != client) continue
		messageClient(client, {
			error: CONNECTION_REPLACED,
			id: request.id,
		})
		if (id === prompt_id || id === initialize_id || request.kind == 'session-new') continue
		CLIENT2AGENT_REQUESTS.delete(id)
	}

	for (const response of prompt_responses) {
		if (response.client != client) continue
		messageClient(client, {
			error: CONNECTION_REPLACED,
			id: response.response.id,
		})
	}
	prompt_responses = prompt_responses.filter(response => response.client != client)
}

const AGENT2CLIENT_REQUESTS = new Map<JsonRpcId, Agent2ClientRequest>()
const IDLE_NEEDED = RequestError.invalidRequest(undefined, 'An idle session is needed to perform this action.').toErrorResponse()
const INITIALIZE_RUNNING = RequestError.invalidRequest(undefined, 'Initialization is already in progress.').toErrorResponse()
const SESSION_CREATING = RequestError.invalidRequest(undefined, 'The session is creating').toErrorResponse()

let initialize_response: AnyResponse | undefined
let session_load: LoadSessionRequest | undefined

const { readable, writable } = ndJsonStream(Writable.toWeb(STDIO_AGENT.stdin), Readable.toWeb(STDIO_AGENT.stdout))
const AGENT_STDIN = writable.getWriter()

WS_SERVER.on('connection', client => {
	if (ws_client) {
		removeClient(ws_client)
	}

	client.on('close', () => {
		if (client == ws_client) {
			ws_client = undefined
		}
		removeClient(client)
	})

	client.on('message', data => {
		let id: JsonRpcId = null
		try {
			const message = JSON.parse(data.toString())
			if ('id' in message) {
				id = message.id
				if ('method' in message) {
					if (client != ws_client) {
						messageClient(client, {
							error: CONNECTION_REPLACED,
							id,
						})
						return
					}

					message.id = crypto.randomUUID()
					let request: Client2AgentRequest = {
						client,
						id,
						kind: 'other-request',
					}

					switch (message.method) {
						case AGENT_METHODS.initialize:
							if (initialize_response) {
								messageClient(client, {
									...initialize_response,
									id,
								})
								return
							}

							if (initialize_id !== undefined) {
								const initialize = CLIENT2AGENT_REQUESTS.get(initialize_id)
								if (initialize) {
									if (initialize.client == client) {
										messageClient(client, {
											error: INITIALIZE_RUNNING,
											id,
										})
										return
									}

									initialize.client = client
									initialize.id = id
									return
								}
							}

							initialize_id = message.id
							request.kind = 'initialize-request'
							break
						case AGENT_METHODS.session_load:
							if (prompt_id === undefined) {
								request.kind = 'session-load'
								session_load = message.params
								break
							}

							if (!session_load) return
							message.params = session_load
							request = {
								client,
								id,
								kind: 'prompt-load',
								session: session_load.sessionId,
							}
							break
						case AGENT_METHODS.session_resume:
							if (prompt_id === undefined) {
								request.kind = 'session-resume'
								session_load = message.params
								break
							}

							if (!session_load) return
							message.method = AGENT_METHODS.session_load
							message.params = session_load
							request = {
								client,
								id,
								kind: 'prompt-resume',
								session: session_load.sessionId,
							}
							break
						case AGENT_METHODS.session_new:
							if (prompt_id === undefined) {
								request = {
									client,
									id,
									kind: 'session-new',
									request: message.params,
								}
								break
							}

							if (!session_load) {
								messageClient(client, {
									error: SESSION_CREATING,
									id,
								})
								return
							}

							message.method = AGENT_METHODS.session_load
							message.params = session_load
							request = {
								client,
								id,
								kind: 'prompt-new',
								session: session_load.sessionId,
							}
							break
						case AGENT_METHODS.session_prompt:
							if (!session_load || prompt_id !== undefined) {
								messageClient(client, {
									error: IDLE_NEEDED,
									id,
								})
								return
							}

							prompt_id = message.id
							prompt_responses = []
							request.kind = 'session-prompt'
							break
					}

					CLIENT2AGENT_REQUESTS.set(message.id, request)
					let requests = CLIENT2AGENT_ID.get(client)
					if (requests) {
						requests.set(id, message.id)
					} else {
						CLIENT2AGENT_ID.set(client, new Map([[id, message.id]]))
					}
				} else {
					const requests = AGENT2CLIENT_ID.get(client)
					if (!requests) return

					const agent2client = requests.get(message.id)
					if (agent2client === undefined || !AGENT2CLIENT_REQUESTS.delete(agent2client)) return
					requests.delete(message.id)
					message.id = agent2client
				}
			} else {
				if (client != ws_client) return
				if (message.method == PROTOCOL_METHODS.cancel_request) {
					const agent2client = AGENT2CLIENT_ID.get(client)?.get(message.params.requestId)
					if (agent2client !== undefined) {
						message.params.requestId = agent2client
					} else {
						const client2agent = CLIENT2AGENT_ID.get(client)?.get(message.params.requestId)
						if (client2agent === undefined) return
						message.params.requestId = client2agent
					}
				}
			}
			void AGENT_STDIN.write(message)
		} catch (error) {
			messageClient(client, {
				error: RequestError.parseError(error, String(error)).toErrorResponse(),
				id,
			})
		}
	})

	ws_client = client
	for (const [_, request] of AGENT2CLIENT_REQUESTS) {
		requestClient(request)
	}
})

for await (const message of readable) {
	if (!('id' in message)) {
		if (!ws_client) continue
		if (message.method == PROTOCOL_METHODS.cancel_request) {
			if (!message.params || typeof message.params != 'object' || !('requestId' in message.params) || (message.params.requestId !== null && !(typeof message.params.requestId == 'number' || typeof message.params.requestId == 'string'))) continue
			const agent2client = AGENT2CLIENT_REQUESTS.get(message.params.requestId)
			if (agent2client && agent2client.client == ws_client) {
				message.params.requestId = agent2client.id
			} else {
				const client2agent = CLIENT2AGENT_REQUESTS.get(message.params.requestId)
				if (!client2agent || client2agent.client != ws_client) continue
				message.params.requestId = client2agent.id
			}
		}

		messageClient(ws_client, message)
		continue
	}

	if (!('method' in message)) {
		const request = CLIENT2AGENT_REQUESTS.get(message.id)
		if (!request) continue
		CLIENT2AGENT_REQUESTS.delete(message.id)
		CLIENT2AGENT_ID.get(request.client)?.delete(request.id)

		const response = {
			...message,
			id: request.id,
		}
		if ('error' in message) {
			switch (request.kind) {
				case 'session-load':
				case 'session-resume':
				case 'prompt-load':
				case 'prompt-resume':
				case 'prompt-new':
				case 'session-new':
					session_load = undefined
					break
			}
		}

		switch (request.kind) {
			case 'initialize-request':
				initialize_id = undefined
				initialize_response = message
				break
			case 'prompt-new':
				if ('result' in response) {
					if (typeof response.result == 'object') {
						response.result = {
							...response.result,
							sessionId: request.session,
						}
					} else {
						response.result = {
							sessionId: request.session,
						}
					}
				}
			case 'prompt-load':
			case 'prompt-resume':
				if (prompt_id !== undefined) {
					prompt_responses = [
						...prompt_responses,
						{
							client: request.client,
							response,
						},
					]
					continue
				}
				break
			case 'session-new':
				if ('result' in message && message.result && typeof message.result == 'object' && 'sessionId' in message.result && typeof message.result.sessionId == 'string') {
					session_load = {
						...request.request,
						sessionId: message.result.sessionId,
					}
				}
				break
			case 'session-prompt':
				for (const prompt of prompt_responses) {
					if (prompt.client == ws_client) {
						messageClient(ws_client, prompt.response)
					}
				}

				if (request.client == ws_client) {
					messageClient(ws_client, response)
				}

				prompt_id = undefined
				prompt_responses = []
				continue
		}

		if (request.client == ws_client) {
			messageClient(request.client, response)
		}
		continue
	}

	const request = {
		request: message,
	}
	AGENT2CLIENT_REQUESTS.set(message.id, request)
	requestClient(request)
}
