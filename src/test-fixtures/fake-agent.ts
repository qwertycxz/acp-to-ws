import process from 'node:process'
import { AGENT_METHODS, CLIENT_METHODS } from '@agentclientprotocol/sdk'

type JsonRpcId = string | number | null

type RequestMessage = {
	jsonrpc: '2.0'
	id: JsonRpcId
	method: string
	params?: Record<string, unknown>
}

type ResponseMessage = {
	jsonrpc: '2.0'
	id: JsonRpcId
	result?: unknown
	error?: unknown
}

type NotificationMessage = {
	jsonrpc: '2.0'
	method: string
	params?: unknown
}

type Message = RequestMessage | ResponseMessage | NotificationMessage

const pendingPrompts = new Map<string, RequestMessage>()
const pendingPermissionByAgentRequestId = new Map<JsonRpcId, RequestMessage>()
let nextPermissionId = 1

process.stdin.setEncoding('utf8')

let buffer = ''
process.stdin.on('data', chunk => {
	buffer += chunk
	let newlineIndex = buffer.indexOf('\n')
	while (newlineIndex >= 0) {
		const line = buffer.slice(0, newlineIndex)
		buffer = buffer.slice(newlineIndex + 1)
		handleLine(line)
		newlineIndex = buffer.indexOf('\n')
	}
})

function handleLine(line: string): void {
	if (!line.trim()) {
		return
	}
	const message = JSON.parse(line) as Message
	if ('method' in message && 'id' in message) {
		handleRequest(message)
		return
	}
	if ('method' in message) {
		handleNotification(message)
		return
	}
	handleResponse(message)
}

function handleRequest(request: RequestMessage): void {
	const params = request.params ?? {}
	if (request.method === AGENT_METHODS.initialize) {
		send({
			id: request.id,
			jsonrpc: '2.0',
			result: {
				_meta: { receivedClientCapabilities: params['clientCapabilities'] ?? null },
				agentCapabilities: { loadSession: true },
				protocolVersion: 1,
			},
		})
		return
	}
	if (request.method === AGENT_METHODS.session_new) {
		send({
			id: request.id,
			jsonrpc: '2.0',
			result: {
				_meta: {
					receivedWorkspace: workspaceFromParams(params),
				},
				sessionId: 'session-1',
			},
		})
		return
	}
	if (request.method === AGENT_METHODS.session_load) {
		const sessionId = stringParam(params, 'sessionId') ?? 'session-1'
		notifyUpdate(sessionId, 'load-history')
		send({
			id: request.id,
			jsonrpc: '2.0',
			result: {
				_meta: {
					receivedWorkspace: workspaceFromParams(params),
				},
			},
		})
		return
	}
	if (request.method === AGENT_METHODS.session_prompt) {
		const sessionId = stringParam(params, 'sessionId') ?? 'session-1'
		const text = promptText(params)
		notifyUpdate(sessionId, `prompt-start:${text}`)
		if (text.includes('hold')) {
			pendingPrompts.set(sessionId, request)
			return
		}
		if (text.includes('permission')) {
			const permissionId = `perm-${nextPermissionId++}`
			pendingPermissionByAgentRequestId.set(permissionId, request)
			send({
				id: permissionId,
				jsonrpc: '2.0',
				method: CLIENT_METHODS.session_request_permission,
				params: {
					options: [{ kind: 'allow_once', name: 'Allow', optionId: 'allow' }],
					sessionId,
					toolCall: { status: 'pending', toolCallId: 'tool-1' },
				},
			})
			return
		}
		sendPromptEnd(request.id, 'end_turn')
		return
	}
	send({ id: request.id, jsonrpc: '2.0', result: {} })
}

function handleNotification(notification: NotificationMessage): void {
	if (notification.method !== AGENT_METHODS.session_cancel || !isRecord(notification.params)) {
		return
	}
	const sessionId = stringParam(notification.params, 'sessionId')
	if (!sessionId) {
		return
	}
	const prompt = pendingPrompts.get(sessionId)
	if (prompt) {
		pendingPrompts.delete(sessionId)
		sendPromptEnd(prompt.id, 'cancelled')
	}
}

function handleResponse(response: ResponseMessage): void {
	const prompt = pendingPermissionByAgentRequestId.get(response.id)
	if (!prompt) {
		return
	}
	pendingPermissionByAgentRequestId.delete(response.id)
	sendPromptEnd(prompt.id, 'end_turn')
}

function notifyUpdate(sessionId: string, text: string): void {
	send({
		jsonrpc: '2.0',
		method: CLIENT_METHODS.session_update,
		params: {
			sessionId,
			update: {
				content: { text, type: 'text' },
				sessionUpdate: 'agent_message_chunk',
			},
		},
	})
}

function sendPromptEnd(id: JsonRpcId, stopReason: string): void {
	send({
		id,
		jsonrpc: '2.0',
		result: { stopReason },
	})
}

function send(message: Message): void {
	process.stdout.write(`${JSON.stringify(message)}\n`)
}

function promptText(params: Record<string, unknown>): string {
	const prompt = params['prompt']
	if (!Array.isArray(prompt)) {
		return ''
	}
	return prompt
		.map(block => {
			if (isRecord(block) && typeof block['text'] === 'string') {
				return block['text']
			}
			return ''
		})
		.join('')
}

function workspaceFromParams(params: Record<string, unknown>): Record<string, unknown> {
	return {
		additionalDirectories: params['additionalDirectories'] ?? null,
		cwd: params['cwd'] ?? null,
		mcpServers: params['mcpServers'] ?? null,
	}
}

function stringParam(params: Record<string, unknown>, key: string): string | undefined {
	const value = params[key]
	return typeof value === 'string' ? value : undefined
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value)
}
