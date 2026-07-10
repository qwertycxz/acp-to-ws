#!/usr/bin/env node
import { spawn } from 'node:child_process'
import { join } from 'node:path/posix'
import { Readable, Writable } from 'node:stream'
import { parseArgs } from 'node:util'
import { methods, ndJsonStream, RequestError, type AnyMessage, type AnyNotification, type AnyRequest, type AnyResponse, type ErrorResponse, type JsonRpcId } from '@agentclientprotocol/sdk'
import { WebSocket, WebSocketServer, type RawData } from 'ws'

type JsonRpcPayload = { result: unknown } | { error: ErrorResponse }

type ConnectedClient = {
	id: number
	close: () => void
	sendMessage: (message: AnyMessage) => Promise<void>
}

type InitializePendingResponse = {
	kind: 'initialize'
	waiters: Array<{
		clientId: number
		clientRequestId: JsonRpcId
	}>
}

type ClientPendingResponse = {
	kind: 'client'
	clientId: number
	clientRequestId: JsonRpcId
	method: string
	params: unknown
}

type PromptPendingResponse = {
	kind: 'prompt'
	clientId: number
	clientRequestId: JsonRpcId
}

type PromptSetupPendingResponse = {
	kind: 'promptSetup'
	clientId: number
	clientRequestId: JsonRpcId
	originalMethod: SessionSetupMethod
	sessionId: string
}

type PendingAgentResponse = InitializePendingResponse | ClientPendingResponse | PromptPendingResponse | PromptSetupPendingResponse

type PendingAgentRequest = {
	stateId: string
	agentRequestId: JsonRpcId
	request: AnyRequest
	deliveredClientId: number | undefined
	deliveredClientRequestId: JsonRpcId | undefined
	settled: boolean
}

type CachedLoadParams = Record<string, unknown> & {
	cwd: string
	mcpServers: unknown[]
	sessionId: string
}

type DeferredPromptSetupResponse = {
	clientId: number
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

function waitForWebSocketServer(server: WebSocketServer) {
	return new Promise<void>((resolve, reject) => {
		const onError = (error: Error) => {
			server.off('listening', onListening)
			reject(error)
		}

		const onListening = () => {
			server.off('error', onError)
			resolve()
		}

		server.once('error', onError)
		server.once('listening', onListening)
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

function clientRequestKey(clientId: number, requestId: JsonRpcId) {
	return `${clientId}:${idKey(requestId)}`
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

const stdioAgent = spawn(command, args, {
	stdio: ['overlapped', 'overlapped', 'inherit'],
})

const wsServer = new WebSocketServer({
	host,
	path: join('/', path),
	port: parseInt(port),
})

function gracefulStop() {
	stdioAgent.kill()
	wsServer.close()
}

stdioAgent.on('error', error => {
	console.error('Failed to start ACP agent:', error)
	process.exitCode = 1
	gracefulStop()
})

stdioAgent.on('exit', (code, signal) => {
	if (typeof code === 'number' && code !== 0) {
		process.exitCode = code
	}

	if (signal) {
		console.error(`ACP agent exited after signal ${signal}`)
	}

	gracefulStop()
})

const { readable, writable } = ndJsonStream(Writable.toWeb(stdioAgent.stdin), Readable.toWeb(stdioAgent.stdout))
const agentWriter = writable.getWriter()
const webSocketServerListening = waitForWebSocketServer(wsServer)

let activePromptTurn: ActivePromptTurn | undefined
let cachedInitializeResponse: JsonRpcPayload | undefined
let cachedSessionLoad: CachedLoadParams | undefined
let connectedClient: ConnectedClient | undefined
let initializeAgentRequestId: JsonRpcId | undefined
let nextClientId = 1
let nextMessageId = 1
let nextRequestStateId = 1

const agentRequestStateByAgentId = new Map<string, string>()
const clientAgentRequestLookup = new Map<string, string>()
const clientRequestToAgentId = new Map<string, JsonRpcId>()
const pendingAgentRequests = new Map<string, PendingAgentRequest>()
const pendingAgentResponses = new Map<string, PendingAgentResponse>()

function sendAgentMessage(message: AnyMessage) {
	return agentWriter.write(message as AnyMessage)
}

function addClient(sendMessage: (message: AnyMessage) => Promise<void>, close: () => void) {
	const previousClient = connectedClient

	if (previousClient) {
		removeClient(previousClient.id)
		previousClient.close()
	}

	const clientId = nextClientId++
	connectedClient = { id: clientId, close, sendMessage }
	void flushPendingAgentRequests()
	return clientId
}

function removeClient(clientId: number) {
	if (connectedClient?.id === clientId) {
		connectedClient = undefined
	}

	for (const key of Array.from(clientRequestToAgentId.keys())) {
		if (key.startsWith(`${clientId}:`)) {
			clientRequestToAgentId.delete(key)
		}
	}

	for (const key of Array.from(clientAgentRequestLookup.keys())) {
		if (key.startsWith(`${clientId}:`)) {
			clientAgentRequestLookup.delete(key)
		}
	}

	for (const pending of pendingAgentResponses.values()) {
		if (pending.kind === 'initialize') {
			pending.waiters = pending.waiters.filter(waiter => waiter.clientId !== clientId)
		}
	}

	if (activePromptTurn) {
		activePromptTurn.deferredSetupResponses = activePromptTurn.deferredSetupResponses.filter(response => response.clientId !== clientId)
	}

	for (const state of pendingAgentRequests.values()) {
		if (state.deliveredClientId === clientId) {
			clearDeliveredAgentRequest(state)
		}
	}
}

async function receiveClientMessage(clientId: number, message: AnyMessage) {
	if (isRequestMessage(message)) {
		await receiveClientRequest(clientId, message)
		return
	}

	if (isResponseMessage(message)) {
		await receiveClientResponse(clientId, message)
		return
	}

	await receiveClientNotification(clientId, message)
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

async function receiveClientRequest(clientId: number, request: AnyRequest) {
	if (!isCurrentClient(clientId)) {
		return
	}

	if (request.method === METHOD_INITIALIZE) {
		await receiveInitializeRequest(clientId, request)
		return
	}

	if (activePromptTurn) {
		if (request.method === METHOD_SESSION_PROMPT) {
			await sendToClient(clientId, makeErrorResponse(request.id, new RequestError(ERROR_CODE_CONFLICT, 'A prompt turn is already running.', { activeSessionId: activePromptTurn.sessionId ?? null })))
			return
		}

		if (isSessionSetupMethod(request.method)) {
			await receiveSessionSetupDuringPrompt(clientId, request, request.method)
			return
		}
	}

	if (request.method === METHOD_SESSION_LOAD || request.method === METHOD_SESSION_RESUME) {
		mergeSessionCache(request.params)
	}

	if (request.method === METHOD_SESSION_PROMPT) {
		await forwardPromptRequest(clientId, request)
		return
	}

	await forwardClientRequest(clientId, request)
}

async function receiveInitializeRequest(clientId: number, request: AnyRequest) {
	if (cachedInitializeResponse) {
		await sendResponsePayload(clientId, request.id, cachedInitializeResponse)
		return
	}

	if (initializeAgentRequestId !== undefined) {
		const pending = pendingAgentResponses.get(idKey(initializeAgentRequestId))

		if (pending?.kind === 'initialize') {
			pending.waiters.push({ clientId, clientRequestId: request.id })
			return
		}
	}

	const agentRequestId = nextProxyRequestId('initialize')
	initializeAgentRequestId = agentRequestId
	pendingAgentResponses.set(idKey(agentRequestId), {
		kind: 'initialize',
		waiters: [{ clientId, clientRequestId: request.id }],
	})
	await sendAgentMessage(requestWithId(request, agentRequestId))
}

async function receiveSessionSetupDuringPrompt(clientId: number, request: AnyRequest, originalMethod: SessionSetupMethod) {
	const cachedLoadParams = cachedSessionLoad

	if (!cachedLoadParams) {
		await sendToClient(clientId, makeErrorResponse(request.id, RequestError.internalError(undefined, 'No cached session/load parameters are available for prompt-turn recovery.')))
		return
	}

	const agentRequestId = nextProxyRequestId('session-load')
	pendingAgentResponses.set(idKey(agentRequestId), {
		kind: 'promptSetup',
		clientId,
		clientRequestId: request.id,
		originalMethod,
		sessionId: cachedLoadParams.sessionId,
	})
	clientRequestToAgentId.set(clientRequestKey(clientId, request.id), agentRequestId)
	await sendAgentMessage(makeRequest(agentRequestId, METHOD_SESSION_LOAD, cachedLoadRequest(cachedLoadParams)))
}

async function forwardPromptRequest(clientId: number, request: AnyRequest) {
	const agentRequestId = nextProxyRequestId('prompt')
	pendingAgentResponses.set(idKey(agentRequestId), {
		kind: 'prompt',
		clientId,
		clientRequestId: request.id,
	})
	clientRequestToAgentId.set(clientRequestKey(clientId, request.id), agentRequestId)
	activePromptTurn = {
		agentPromptRequestId: agentRequestId,
		sessionId: stringProperty(request.params, 'sessionId') ?? cachedSessionLoad?.sessionId,
		deferredSetupResponses: [],
	}
	await sendAgentMessage(requestWithId(request, agentRequestId))
}

async function forwardClientRequest(clientId: number, request: AnyRequest) {
	const agentRequestId = nextProxyRequestId('client')
	pendingAgentResponses.set(idKey(agentRequestId), {
		kind: 'client',
		clientId,
		clientRequestId: request.id,
		method: request.method,
		params: request.params,
	})
	clientRequestToAgentId.set(clientRequestKey(clientId, request.id), agentRequestId)
	await sendAgentMessage(requestWithId(request, agentRequestId))
}

async function receiveClientResponse(clientId: number, response: AnyResponse) {
	const stateId = clientAgentRequestLookup.get(clientRequestKey(clientId, response.id))

	if (!stateId) {
		return
	}

	const state = pendingAgentRequests.get(stateId)

	if (!state || state.settled) {
		return
	}

	state.settled = true
	clearAgentRequestState(state)
	await sendAgentMessage(responseWithId(response, state.agentRequestId))
}

async function receiveClientNotification(clientId: number, notification: AnyNotification) {
	if (!isCurrentClient(clientId)) {
		return
	}

	if (notification.method === METHOD_CANCEL_REQUEST) {
		const requestId = requestIdFromParams(notification.params)

		if (requestId === undefined) {
			return
		}

		const clientRequestKeyValue = clientRequestKey(clientId, requestId)
		const agentRequestId = clientRequestToAgentId.get(clientRequestKeyValue)

		if (agentRequestId !== undefined) {
			await sendAgentMessage(cancelRequestNotification(agentRequestId, notification.params))
			return
		}

		const stateId = clientAgentRequestLookup.get(clientRequestKeyValue)
		const state = stateId ? pendingAgentRequests.get(stateId) : undefined

		if (state) {
			await sendAgentMessage(cancelRequestNotification(state.agentRequestId, notification.params))
		}

		return
	}

	await sendAgentMessage(notification)
}

async function receiveAgentRequest(request: AnyRequest) {
	const stateId = `agent-request:${nextRequestStateId++}`
	const state: PendingAgentRequest = {
		stateId,
		agentRequestId: request.id,
		request,
		deliveredClientId: undefined,
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
			await sendResponsePayload(waiter.clientId, waiter.clientRequestId, payload)
		}

		return
	}

	clientRequestToAgentId.delete(clientRequestKey(pending.clientId, pending.clientRequestId))

	if (pending.kind === 'client') {
		if (pending.method === METHOD_SESSION_NEW && 'result' in response) {
			mergeSessionCache(sessionNewCacheParams(pending.params, response.result))
		}

		await sendToClient(pending.clientId, responseWithId(response, pending.clientRequestId))
		return
	}

	if (pending.kind === 'prompt') {
		await sendToClient(pending.clientId, responseWithId(response, pending.clientRequestId))

		if (activePromptTurn && idEquals(activePromptTurn.agentPromptRequestId, response.id)) {
			await finishActivePromptTurn()
		}

		return
	}

	const setupResponse = normalizePromptSetupResponse(responseWithId(response, pending.clientRequestId), pending.originalMethod, pending.sessionId)

	if (activePromptTurn) {
		activePromptTurn.deferredSetupResponses.push({
			clientId: pending.clientId,
			response: setupResponse,
		})
		return
	}

	await sendToClient(pending.clientId, setupResponse)
}

async function receiveAgentNotification(notification: AnyNotification) {
	if (notification.method === METHOD_CANCEL_REQUEST) {
		const requestId = requestIdFromParams(notification.params)

		if (requestId === undefined) {
			return
		}

		const stateId = agentRequestStateByAgentId.get(idKey(requestId))
		const state = stateId ? pendingAgentRequests.get(stateId) : undefined

		if (!state || state.deliveredClientId === undefined || state.deliveredClientRequestId === undefined) {
			return
		}

		await sendToClient(state.deliveredClientId, cancelRequestNotification(state.deliveredClientRequestId, notification.params))
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
		await sendToClient(deferredResponse.clientId, deferredResponse.response)
	}
}

async function flushPendingAgentRequests() {
	for (const state of pendingAgentRequests.values()) {
		await deliverAgentRequest(state)
	}
}

async function deliverAgentRequest(state: PendingAgentRequest) {
	if (state.settled || !connectedClient) {
		return
	}

	if (state.deliveredClientId === connectedClient.id) {
		return
	}

	clearDeliveredAgentRequest(state)

	const clientRequestId = nextProxyRequestId('agent')
	state.deliveredClientId = connectedClient.id
	state.deliveredClientRequestId = clientRequestId
	clientAgentRequestLookup.set(clientRequestKey(connectedClient.id, clientRequestId), state.stateId)
	await sendToClient(connectedClient.id, requestWithId(state.request, clientRequestId))
}

function clearDeliveredAgentRequest(state: PendingAgentRequest) {
	if (state.deliveredClientId !== undefined && state.deliveredClientRequestId !== undefined) {
		clientAgentRequestLookup.delete(clientRequestKey(state.deliveredClientId, state.deliveredClientRequestId))
	}

	state.deliveredClientId = undefined
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

async function sendResponsePayload(clientId: number, requestId: JsonRpcId, payload: JsonRpcPayload) {
	if ('result' in payload) {
		await sendToClient(clientId, makeResultResponse(requestId, payload.result))
		return
	}

	await sendToClient(clientId, makeResponseWithError(requestId, payload.error))
}

async function sendToCurrentClient(message: AnyMessage) {
	if (!connectedClient) {
		return false
	}

	return sendToClient(connectedClient.id, message)
}

async function sendToClient(clientId: number, message: AnyMessage) {
	const targetClient = connectedClient

	if (!targetClient || targetClient.id !== clientId) {
		return false
	}

	try {
		await targetClient.sendMessage(message)
		return true
	} catch {
		removeClient(clientId)
		return false
	}
}

function isCurrentClient(clientId: number) {
	return connectedClient?.id === clientId
}

function nextProxyRequestId(prefix: string) {
	return `acp-to-ws:${prefix}:${nextMessageId++}`
}

async function handleClientSocketMessage(clientId: number, socket: WebSocket, data: RawData) {
	const text = rawDataToString(data)
	let parsed: unknown

	try {
		parsed = JSON.parse(text)
	} catch {
		await sendSocketMessage(socket, makeErrorResponse(null, RequestError.parseError()))
		return
	}

	if (!isJsonRpcMessage(parsed)) {
		await sendSocketMessage(socket, makeErrorResponse(extractResponseId(parsed), RequestError.invalidRequest(parsed)))
		return
	}

	await receiveClientMessage(clientId, parsed)
}

wsServer.on('connection', socket => {
	const clientId = addClient(
		message => sendSocketMessage(socket, message),
		() => {
			if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
				socket.close(1000, 'Replaced by a newer ACP client')
			}
		},
	)

	socket.on('message', data => {
		void handleClientSocketMessage(clientId, socket, data).catch(error => {
			console.error('ACP client message failed:', error)
			removeClient(clientId)

			if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
				socket.close()
			}
		})
	})
	socket.on('close', () => {
		removeClient(clientId)
	})
	socket.on('error', () => {
		removeClient(clientId)
	})
})

async function readStdout() {
	for await (const message of readable) {
		await receiveAgentMessage(message)
	}
	gracefulStop()
}

void readStdout()

try {
	await webSocketServerListening
	console.error(`ACP WebSocket proxy listening at ws://${host}:${port}${path}`)
} catch (error) {
	gracefulStop()
	throw error
}
