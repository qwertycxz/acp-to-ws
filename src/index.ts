#!/usr/bin/env node
import { spawn } from 'node:child_process'
import { join } from 'node:path/posix'
import { Readable, Writable } from 'node:stream'
import { parseArgs } from 'node:util'
import { methods, ndJsonStream, RequestError, type AnyMessage, type AnyNotification, type AnyRequest, type AnyResponse, type ErrorResponse, type JsonRpcId } from '@agentclientprotocol/sdk'
import { WebSocket, WebSocketServer, type RawData } from 'ws'

type JsonRpcPayload = { result: unknown } | { error: ErrorResponse }

type InitializePendingResponse = {
	kind: 'initialize'
	waiters: Array<{
		client: WebSocket
		clientRequestId: JsonRpcId
	}>
}

type ClientPendingResponse = {
	kind: 'client'
	client: WebSocket
	clientRequestId: JsonRpcId
	method: string
	params: unknown
}

type PromptPendingResponse = {
	kind: 'prompt'
	client: WebSocket
	clientRequestId: JsonRpcId
}

type PromptSetupPendingResponse = {
	kind: 'promptSetup'
	client: WebSocket
	clientRequestId: JsonRpcId
	originalMethod: SessionSetupMethod
	sessionId: string
}

type PendingAgentResponse = InitializePendingResponse | ClientPendingResponse | PromptPendingResponse | PromptSetupPendingResponse

type PendingAgentRequest = {
	stateId: string
	agentRequestId: JsonRpcId
	request: AnyRequest
	deliveredClient: WebSocket | undefined
	deliveredClientRequestId: JsonRpcId | undefined
	settled: boolean
}

type CachedLoadParams = Record<string, unknown> & {
	cwd: string
	mcpServers: unknown[]
	sessionId: string
}

type DeferredPromptSetupResponse = {
	client: WebSocket
	response: AnyResponse
}

type ActivePromptTurn = {
	agentPromptRequestId: JsonRpcId
	sessionId: string | undefined
	deferredSetupResponses: DeferredPromptSetupResponse[]
}

const METHOD_CANCEL_REQUEST = methods.protocol.cancelRequest
const METHOD_INITIALIZE = methods.agent.initialize
const METHOD_SESSION_LOAD = methods.agent.session.load
const METHOD_SESSION_NEW = methods.agent.session.new
const METHOD_SESSION_PROMPT = methods.agent.session.prompt
const METHOD_SESSION_RESUME = methods.agent.session.resume

type SessionSetupMethod = typeof METHOD_SESSION_LOAD | typeof METHOD_SESSION_NEW | typeof METHOD_SESSION_RESUME

const ERROR_CODE_CONFLICT = -32099

function rawDataToString(data: RawData) {
	if (Array.isArray(data)) {
		return Buffer.concat(data).toString('utf8')
	}

	if (data instanceof ArrayBuffer) {
		return Buffer.from(data).toString('utf8')
	}

	if (ArrayBuffer.isView(data)) {
		return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString('utf8')
	}

	return String(data)
}

function sendSocketMessage(socket: WebSocket, message: AnyMessage) {
	return new Promise<void>((resolve, reject) => {
		if (socket.readyState !== WebSocket.OPEN) {
			reject(new Error('WebSocket is not open'))
			return
		}

		socket.send(JSON.stringify(message), error => {
			if (error) {
				reject(error)
				return
			}

			resolve()
		})
	})
}

function isSessionSetupMethod(method: string): method is SessionSetupMethod {
	return method === METHOD_SESSION_LOAD || method === METHOD_SESSION_NEW || method === METHOD_SESSION_RESUME
}

function cachedLoadRequest(params: CachedLoadParams) {
	return {
		...params,
		cwd: params.cwd,
		mcpServers: params.mcpServers,
		sessionId: params.sessionId,
	}
}

function sessionNewCacheParams(params: unknown, result: unknown) {
	if (!isRecord(params) || !isRecord(result) || typeof result.sessionId !== 'string') {
		return undefined
	}

	return {
		...params,
		sessionId: result.sessionId,
	}
}

function normalizePromptSetupResponse(response: AnyResponse, originalMethod: SessionSetupMethod, sessionId: string) {
	if ('error' in response || originalMethod !== METHOD_SESSION_NEW) {
		return response
	}

	const result = isRecord(response.result) ? { ...response.result, sessionId } : { sessionId }
	return makeResultResponse(response.id, result)
}

function makeRequest(id: JsonRpcId, method: string, params: unknown) {
	return {
		jsonrpc: '2.0',
		id,
		method,
		params,
	} satisfies AnyRequest
}

function requestWithId(request: AnyRequest, id: JsonRpcId) {
	const nextRequest: AnyRequest = {
		jsonrpc: '2.0',
		id,
		method: request.method,
	}

	if ('params' in request) {
		nextRequest.params = request.params
	}

	return nextRequest
}

function responseWithId(response: AnyResponse, id: JsonRpcId) {
	if ('result' in response) {
		return makeResultResponse(id, response.result)
	}

	return makeResponseWithError(id, response.error)
}

function makeResultResponse(id: JsonRpcId, result: unknown): AnyResponse {
	return {
		jsonrpc: '2.0',
		id,
		result: result === undefined ? null : result,
	}
}

function makeResponseWithError(id: JsonRpcId, error: ErrorResponse): AnyResponse {
	return {
		jsonrpc: '2.0',
		id,
		error,
	}
}

function makeErrorResponse(id: JsonRpcId, error: RequestError | ErrorResponse) {
	return makeResponseWithError(id, error instanceof RequestError ? error.toErrorResponse() : error)
}

function responsePayload(response: AnyResponse): JsonRpcPayload {
	if ('result' in response) {
		return { result: response.result }
	}

	return { error: response.error }
}

function cancelRequestNotification(requestId: JsonRpcId, originalParams: unknown) {
	const params = isRecord(originalParams) ? { ...originalParams, requestId } : { requestId }

	return {
		jsonrpc: '2.0',
		method: METHOD_CANCEL_REQUEST,
		params,
	} satisfies AnyNotification
}

function requestIdFromParams(params: unknown) {
	if (!isRecord(params) || !isJsonRpcId(params.requestId)) {
		return undefined
	}

	return params.requestId
}

function stringProperty(value: unknown, key: string) {
	if (!isRecord(value)) {
		return undefined
	}

	const property = value[key]
	return typeof property === 'string' ? property : undefined
}

function isJsonRpcMessage(value: unknown): value is AnyMessage {
	return isRequestMessage(value) || isResponseMessage(value) || isNotificationMessage(value)
}

function isRequestMessage(value: unknown): value is AnyRequest {
	return isRecord(value) && value.jsonrpc === '2.0' && typeof value.method === 'string' && 'id' in value && isJsonRpcId(value.id)
}

function isResponseMessage(value: unknown): value is AnyResponse {
	if (!isRecord(value) || value.jsonrpc !== '2.0' || !('id' in value) || !isJsonRpcId(value.id) || 'method' in value) {
		return false
	}

	const hasResult = 'result' in value
	const hasError = 'error' in value
	return hasResult !== hasError && (!hasError || isJsonRpcError(value.error))
}

function isNotificationMessage(value: unknown): value is AnyNotification {
	return isRecord(value) && value.jsonrpc === '2.0' && typeof value.method === 'string' && !('id' in value)
}

function isJsonRpcError(value: unknown): value is ErrorResponse {
	return isRecord(value) && typeof value.code === 'number' && Number.isInteger(value.code) && typeof value.message === 'string'
}

function isJsonRpcId(value: unknown): value is JsonRpcId {
	return value === null || typeof value === 'string' || (typeof value === 'number' && Number.isFinite(value))
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function extractResponseId(value: unknown) {
	return isRecord(value) && isJsonRpcId(value.id) ? value.id : null
}

function idKey(id: JsonRpcId) {
	return `${typeof id}:${JSON.stringify(id)}`
}

function idEquals(left: JsonRpcId, right: JsonRpcId) {
	return idKey(left) === idKey(right)
}

const DEFAULT_HOST = '127.0.0.1'
const DEFAULT_PORT = '80'
const DEFAULT_PATH = '/acp'

const {
	positionals: [command, ...args],
	values: { help, host, path, port },
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
		path: {
			default: DEFAULT_PATH,
			type: 'string',
		},
		port: {
			default: DEFAULT_PORT,
			type: 'string',
		},
	},
})

if (help || !(command && host && path && port)) {
	console.error(`Usage:
	acp-to-ws [--host <host>] [--port <port>] [--path <path>] -- <stdio-agent-command> [args...]

Examples:
	acp-to-ws --port 80 -- npx tsx ./agent.ts
	node dist/index.js --host 0.0.0.0 --port 80 -- node ./dist/agent.js

Args:
	<host>  Defaults to ${DEFAULT_HOST}
	<path>  Defaults to ${DEFAULT_PATH}
	<port>  Defaults to ${DEFAULT_PORT}`)
	process.exit(1)
}

const STDIO_AGENT = spawn(command, args, {
	stdio: ['overlapped', 'overlapped', 'inherit'],
})

STDIO_AGENT.on('error', error => {
	console.error('Failed to start ACP agent:', error)
	process.exit(1)
})

STDIO_AGENT.on('exit', (code, signal) => {
	if (signal) {
		console.error(`ACP agent exited after signal ${signal}`)
	}

	if (code == null) {
		code = 1
	}
	process.exit(code)
})

const { readable, writable } = ndJsonStream(Writable.toWeb(STDIO_AGENT.stdin), Readable.toWeb(STDIO_AGENT.stdout))

async function readStdout() {
	for await (const message of readable) {
		await receiveAgentMessage(message)
	}
	process.exit()
}
void readStdout()

const AGENT_STDIN = writable.getWriter()

const WS_SERVER = new WebSocketServer({
	host,
	path: join('/', path),
	port: parseInt(port),
})

WS_SERVER.on('error', error => {
	console.error('ACP WebSocket server error:', error)
	process.exit(1)
})

let activePromptTurn: ActivePromptTurn | undefined
let cachedInitializeResponse: JsonRpcPayload | undefined
let cachedSessionLoad: CachedLoadParams | undefined
let ws_client: WebSocket | undefined
let initializeAgentRequestId: JsonRpcId | undefined
let nextMessageId = 1
let nextRequestStateId = 1

const agentRequestStateByAgentId = new Map<string, string>()
const clientAgentRequestLookup = new Map<WebSocket, Map<string, string>>()
const clientRequestToAgentId = new Map<WebSocket, Map<string, JsonRpcId>>()
const pendingAgentRequests = new Map<string, PendingAgentRequest>()
const pendingAgentResponses = new Map<string, PendingAgentResponse>()

function clientRequestToAgentMap(client: WebSocket) {
	let requests = clientRequestToAgentId.get(client)

	if (!requests) {
		requests = new Map()
		clientRequestToAgentId.set(client, requests)
	}

	return requests
}

function clientAgentRequestLookupMap(client: WebSocket) {
	let requests = clientAgentRequestLookup.get(client)

	if (!requests) {
		requests = new Map()
		clientAgentRequestLookup.set(client, requests)
	}

	return requests
}

function removeClient(client: WebSocket) {
	if (ws_client === client) {
		ws_client = undefined
	}

	clientRequestToAgentId.delete(client)
	clientAgentRequestLookup.delete(client)

	for (const pending of pendingAgentResponses.values()) {
		if (pending.kind === 'initialize') {
			pending.waiters = pending.waiters.filter(waiter => waiter.client !== client)
		}
	}

	if (activePromptTurn) {
		activePromptTurn.deferredSetupResponses = activePromptTurn.deferredSetupResponses.filter(response => response.client !== client)
	}

	for (const state of pendingAgentRequests.values()) {
		if (state.deliveredClient === client) {
			clearDeliveredAgentRequest(state)
		}
	}
}

async function receiveClientMessage(client: WebSocket, message: AnyMessage) {
	if (isRequestMessage(message)) {
		await receiveClientRequest(client, message)
		return
	}

	if (isResponseMessage(message)) {
		await receiveClientResponse(client, message)
		return
	}

	await receiveClientNotification(client, message)
}

async function receiveAgentMessage(message: AnyMessage) {
	if (isRequestMessage(message)) {
		await receiveAgentRequest(message)
		return
	}

	if (isResponseMessage(message)) {
		await receiveAgentResponse(message)
		return
	}

	await receiveAgentNotification(message)
}

async function receiveClientRequest(client: WebSocket, request: AnyRequest) {
	if (client != ws_client) {
		return
	}

	if (request.method === METHOD_INITIALIZE) {
		await receiveInitializeRequest(client, request)
		return
	}

	if (activePromptTurn) {
		if (request.method === METHOD_SESSION_PROMPT) {
			await sendToClient(client, makeErrorResponse(request.id, new RequestError(ERROR_CODE_CONFLICT, 'A prompt turn is already running.', { activeSessionId: activePromptTurn.sessionId ?? null })))
			return
		}

		if (isSessionSetupMethod(request.method)) {
			await receiveSessionSetupDuringPrompt(client, request, request.method)
			return
		}
	}

	if (request.method === METHOD_SESSION_LOAD || request.method === METHOD_SESSION_RESUME) {
		mergeSessionCache(request.params)
	}

	if (request.method === METHOD_SESSION_PROMPT) {
		await forwardPromptRequest(client, request)
		return
	}

	await forwardClientRequest(client, request)
}

async function receiveInitializeRequest(client: WebSocket, request: AnyRequest) {
	if (cachedInitializeResponse) {
		await sendResponsePayload(client, request.id, cachedInitializeResponse)
		return
	}

	if (initializeAgentRequestId !== undefined) {
		const pending = pendingAgentResponses.get(idKey(initializeAgentRequestId))

		if (pending?.kind === 'initialize') {
			pending.waiters.push({ client, clientRequestId: request.id })
			return
		}
	}

	const agentRequestId = nextProxyRequestId('initialize')
	initializeAgentRequestId = agentRequestId
	pendingAgentResponses.set(idKey(agentRequestId), {
		kind: 'initialize',
		waiters: [{ client, clientRequestId: request.id }],
	})
	await AGENT_STDIN.write(requestWithId(request, agentRequestId))
}

async function receiveSessionSetupDuringPrompt(client: WebSocket, request: AnyRequest, originalMethod: SessionSetupMethod) {
	const cachedLoadParams = cachedSessionLoad

	if (!cachedLoadParams) {
		await sendToClient(client, makeErrorResponse(request.id, RequestError.internalError(undefined, 'No cached session/load parameters are available for prompt-turn recovery.')))
		return
	}

	const agentRequestId = nextProxyRequestId('session-load')
	pendingAgentResponses.set(idKey(agentRequestId), {
		kind: 'promptSetup',
		client,
		clientRequestId: request.id,
		originalMethod,
		sessionId: cachedLoadParams.sessionId,
	})
	clientRequestToAgentMap(client).set(idKey(request.id), agentRequestId)
	await AGENT_STDIN.write(makeRequest(agentRequestId, METHOD_SESSION_LOAD, cachedLoadRequest(cachedLoadParams)))
}

async function forwardPromptRequest(client: WebSocket, request: AnyRequest) {
	const agentRequestId = nextProxyRequestId('prompt')
	pendingAgentResponses.set(idKey(agentRequestId), {
		kind: 'prompt',
		client,
		clientRequestId: request.id,
	})
	clientRequestToAgentMap(client).set(idKey(request.id), agentRequestId)
	activePromptTurn = {
		agentPromptRequestId: agentRequestId,
		sessionId: stringProperty(request.params, 'sessionId') ?? cachedSessionLoad?.sessionId,
		deferredSetupResponses: [],
	}
	await AGENT_STDIN.write(requestWithId(request, agentRequestId))
}

async function forwardClientRequest(client: WebSocket, request: AnyRequest) {
	const agentRequestId = nextProxyRequestId('client')
	pendingAgentResponses.set(idKey(agentRequestId), {
		kind: 'client',
		client,
		clientRequestId: request.id,
		method: request.method,
		params: request.params,
	})
	clientRequestToAgentMap(client).set(idKey(request.id), agentRequestId)
	await AGENT_STDIN.write(requestWithId(request, agentRequestId))
}

async function receiveClientResponse(client: WebSocket, response: AnyResponse) {
	const stateId = clientAgentRequestLookup.get(client)?.get(idKey(response.id))

	if (!stateId) {
		return
	}

	const state = pendingAgentRequests.get(stateId)

	if (!state || state.settled) {
		return
	}

	state.settled = true
	clearAgentRequestState(state)
	await AGENT_STDIN.write(responseWithId(response, state.agentRequestId))
}

async function receiveClientNotification(client: WebSocket, notification: AnyNotification) {
	if (client != ws_client) {
		return
	}

	if (notification.method === METHOD_CANCEL_REQUEST) {
		const requestId = requestIdFromParams(notification.params)

		if (requestId === undefined) {
			return
		}

		const requestKey = idKey(requestId)
		const agentRequestId = clientRequestToAgentId.get(client)?.get(requestKey)

		if (agentRequestId !== undefined) {
			await AGENT_STDIN.write(cancelRequestNotification(agentRequestId, notification.params))
			return
		}

		const stateId = clientAgentRequestLookup.get(client)?.get(requestKey)
		const state = stateId ? pendingAgentRequests.get(stateId) : undefined

		if (state) {
			await AGENT_STDIN.write(cancelRequestNotification(state.agentRequestId, notification.params))
		}

		return
	}

	await AGENT_STDIN.write(notification)
}

async function receiveAgentRequest(request: AnyRequest) {
	const stateId = `agent-request:${nextRequestStateId++}`
	const state: PendingAgentRequest = {
		stateId,
		agentRequestId: request.id,
		request,
		deliveredClient: undefined,
		deliveredClientRequestId: undefined,
		settled: false,
	}
	pendingAgentRequests.set(stateId, state)
	agentRequestStateByAgentId.set(idKey(request.id), stateId)
	await deliverAgentRequest(state)
}

async function receiveAgentResponse(response: AnyResponse) {
	const pending = pendingAgentResponses.get(idKey(response.id))

	if (!pending) {
		return
	}

	pendingAgentResponses.delete(idKey(response.id))

	if (initializeAgentRequestId !== undefined && idEquals(initializeAgentRequestId, response.id)) {
		initializeAgentRequestId = undefined
	}

	if (pending.kind === 'initialize') {
		const payload = responsePayload(response)
		cachedInitializeResponse = payload

		for (const waiter of pending.waiters) {
			await sendResponsePayload(waiter.client, waiter.clientRequestId, payload)
		}

		return
	}

	clientRequestToAgentId.get(pending.client)?.delete(idKey(pending.clientRequestId))

	if (pending.kind === 'client') {
		if (pending.method === METHOD_SESSION_NEW && 'result' in response) {
			mergeSessionCache(sessionNewCacheParams(pending.params, response.result))
		}

		await sendToClient(pending.client, responseWithId(response, pending.clientRequestId))
		return
	}

	if (pending.kind === 'prompt') {
		await sendToClient(pending.client, responseWithId(response, pending.clientRequestId))

		if (activePromptTurn && idEquals(activePromptTurn.agentPromptRequestId, response.id)) {
			await finishActivePromptTurn()
		}

		return
	}

	const setupResponse = normalizePromptSetupResponse(responseWithId(response, pending.clientRequestId), pending.originalMethod, pending.sessionId)

	if (activePromptTurn) {
		activePromptTurn.deferredSetupResponses.push({
			client: pending.client,
			response: setupResponse,
		})
		return
	}

	await sendToClient(pending.client, setupResponse)
}

async function receiveAgentNotification(notification: AnyNotification) {
	if (notification.method === METHOD_CANCEL_REQUEST) {
		const requestId = requestIdFromParams(notification.params)

		if (requestId === undefined) {
			return
		}

		const stateId = agentRequestStateByAgentId.get(idKey(requestId))
		const state = stateId ? pendingAgentRequests.get(stateId) : undefined

		if (!state || state.deliveredClient === undefined || state.deliveredClientRequestId === undefined) {
			return
		}

		await sendToClient(state.deliveredClient, cancelRequestNotification(state.deliveredClientRequestId, notification.params))
		return
	}

	await sendToCurrentClient(notification)
}

async function finishActivePromptTurn() {
	const promptTurn = activePromptTurn

	if (!promptTurn) {
		return
	}

	activePromptTurn = undefined

	for (const deferredResponse of promptTurn.deferredSetupResponses) {
		await sendToClient(deferredResponse.client, deferredResponse.response)
	}
}

async function flushPendingAgentRequests() {
	for (const state of pendingAgentRequests.values()) {
		await deliverAgentRequest(state)
	}
}

async function deliverAgentRequest(state: PendingAgentRequest) {
	if (state.settled || !ws_client) {
		return
	}

	if (state.deliveredClient === ws_client) {
		return
	}

	clearDeliveredAgentRequest(state)

	const clientRequestId = nextProxyRequestId('agent')
	state.deliveredClient = ws_client
	state.deliveredClientRequestId = clientRequestId
	clientAgentRequestLookupMap(ws_client).set(idKey(clientRequestId), state.stateId)
	await sendToClient(ws_client, requestWithId(state.request, clientRequestId))
}

function clearDeliveredAgentRequest(state: PendingAgentRequest) {
	if (state.deliveredClient !== undefined && state.deliveredClientRequestId !== undefined) {
		clientAgentRequestLookup.get(state.deliveredClient)?.delete(idKey(state.deliveredClientRequestId))
	}

	state.deliveredClient = undefined
	state.deliveredClientRequestId = undefined
}

function clearAgentRequestState(state: PendingAgentRequest) {
	clearDeliveredAgentRequest(state)
	pendingAgentRequests.delete(state.stateId)
	agentRequestStateByAgentId.delete(idKey(state.agentRequestId))
}

function mergeSessionCache(params: unknown) {
	if (!isRecord(params)) {
		return
	}

	const base: Record<string, unknown> = cachedSessionLoad ? { ...cachedSessionLoad } : {}
	const merged = { ...base, ...params }
	const cwd = typeof merged.cwd === 'string' ? merged.cwd : undefined
	const sessionId = typeof merged.sessionId === 'string' ? merged.sessionId : undefined
	const mcpServers = Array.isArray(merged.mcpServers) ? merged.mcpServers : undefined

	if (!cwd || !sessionId || !mcpServers) {
		return
	}

	cachedSessionLoad = {
		...merged,
		cwd,
		mcpServers,
		sessionId,
	}
}

async function sendResponsePayload(client: WebSocket, requestId: JsonRpcId, payload: JsonRpcPayload) {
	if ('result' in payload) {
		await sendToClient(client, makeResultResponse(requestId, payload.result))
		return
	}

	await sendToClient(client, makeResponseWithError(requestId, payload.error))
}

async function sendToCurrentClient(message: AnyMessage) {
	if (!ws_client) {
		return false
	}

	return sendToClient(ws_client, message)
}

async function sendToClient(client: WebSocket, message: AnyMessage) {
	if (client != ws_client) {
		return false
	}

	try {
		await sendSocketMessage(client, message)
		return true
	} catch {
		removeClient(client)
		return false
	}
}

function nextProxyRequestId(prefix: string) {
	return `acp-to-ws:${prefix}:${nextMessageId++}`
}

async function handleClientSocketMessage(client: WebSocket, data: RawData) {
	const text = rawDataToString(data)
	let parsed: unknown

	try {
		parsed = JSON.parse(text)
	} catch {
		await sendSocketMessage(client, makeErrorResponse(null, RequestError.parseError()))
		return
	}

	if (!isJsonRpcMessage(parsed)) {
		await sendSocketMessage(client, makeErrorResponse(extractResponseId(parsed), RequestError.invalidRequest(parsed)))
		return
	}

	await receiveClientMessage(client, parsed)
}

WS_SERVER.on('connection', client => {
	if (ws_client) {
		ws_client.close(1000, 'Replaced by a newer ACP client')
		removeClient(ws_client)
	}

	ws_client = client
	void flushPendingAgentRequests()

	client.on('message', data => {
		void handleClientSocketMessage(client, data).catch(error => {
			console.error('ACP client message failed:', error)
			removeClient(client)

			if (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CONNECTING) {
				client.close()
			}
		})
	})
	client.on('close', () => {
		removeClient(client)
	})
	client.on('error', () => {
		removeClient(client)
	})
})

console.error(`ACP WebSocket proxy listening at ws://${host}:${port}${path}`)
