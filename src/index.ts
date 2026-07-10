#!/usr/bin/env node
import { spawn } from 'node:child_process'
import { createServer, type Server } from 'node:http'
import { join } from 'node:path/posix'
import { exit } from 'node:process'
import { Readable, Writable } from 'node:stream'
import { parseArgs } from 'node:util'
import { ndJsonStream, type AnyMessage } from '@agentclientprotocol/sdk'
import { WebSocket, WebSocketServer, type RawData } from 'ws'

type JsonRpcId = string | number | null

type JsonRpcError = {
	code: number
	message: string
	data?: unknown
}

type JsonRpcRequest = {
	jsonrpc: '2.0'
	id: JsonRpcId
	method: string
	params?: unknown
}

type JsonRpcResponse = {
	jsonrpc: '2.0'
	id: JsonRpcId
} & ({ result: unknown } | { error: JsonRpcError })

type JsonRpcNotification = {
	jsonrpc: '2.0'
	method: string
	params?: unknown
}

type JsonRpcMessage = JsonRpcRequest | JsonRpcResponse | JsonRpcNotification
type JsonRpcPayload = { result: unknown } | { error: JsonRpcError }

type ConnectedClient = {
	id: number
	close: () => void
	sendMessage: (message: JsonRpcMessage) => Promise<void>
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
	request: JsonRpcRequest
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
	response: JsonRpcResponse
}

type ActivePromptTurn = {
	agentPromptRequestId: JsonRpcId
	sessionId: string | undefined
	deferredSetupResponses: DeferredPromptSetupResponse[]
}

const METHOD_CANCEL_REQUEST = '$/cancel_request'
const METHOD_INITIALIZE = 'initialize'
const METHOD_SESSION_LOAD = 'session/load'
const METHOD_SESSION_NEW = 'session/new'
const METHOD_SESSION_PROMPT = 'session/prompt'
const METHOD_SESSION_RESUME = 'session/resume'

type SessionSetupMethod = typeof METHOD_SESSION_LOAD | typeof METHOD_SESSION_NEW | typeof METHOD_SESSION_RESUME

const ERROR_CODE_CONFLICT = -32099
const ERROR_CODE_INTERNAL = -32603
const ERROR_CODE_INVALID_REQUEST = -32600
const ERROR_CODE_PARSE = -32700

class AcpRecoveringProxy {
	private activePromptTurn: ActivePromptTurn | undefined
	private cachedInitializeResponse?: JsonRpcPayload
	private cachedSessionLoad?: CachedLoadParams
	private client: ConnectedClient | undefined
	private initializeAgentRequestId: JsonRpcId | undefined
	private nextClientId = 1
	private nextMessageId = 1
	private nextRequestStateId = 1
	private readonly agentRequestStateByAgentId = new Map<string, string>()
	private readonly clientAgentRequestLookup = new Map<string, string>()
	private readonly clientRequestToAgentId = new Map<string, JsonRpcId>()
	private readonly pendingAgentRequests = new Map<string, PendingAgentRequest>()
	private readonly pendingAgentResponses = new Map<string, PendingAgentResponse>()
	private readonly sendAgentMessage: (message: JsonRpcMessage) => Promise<void>

	constructor(options: { sendAgentMessage: (message: JsonRpcMessage) => Promise<void> }) {
		this.sendAgentMessage = options.sendAgentMessage
	}

	addClient(sendMessage: (message: JsonRpcMessage) => Promise<void>, close: () => void) {
		const previousClient = this.client

		if (previousClient) {
			this.removeClient(previousClient.id)
			previousClient.close()
		}

		const clientId = this.nextClientId++
		this.client = { id: clientId, close, sendMessage }
		void this.flushPendingAgentRequests()
		return clientId
	}

	removeClient(clientId: number) {
		if (this.client?.id === clientId) {
			this.client = undefined
		}

		for (const key of Array.from(this.clientRequestToAgentId.keys())) {
			if (key.startsWith(`${clientId}:`)) {
				this.clientRequestToAgentId.delete(key)
			}
		}

		for (const key of Array.from(this.clientAgentRequestLookup.keys())) {
			if (key.startsWith(`${clientId}:`)) {
				this.clientAgentRequestLookup.delete(key)
			}
		}

		for (const pending of this.pendingAgentResponses.values()) {
			if (pending.kind === 'initialize') {
				pending.waiters = pending.waiters.filter(waiter => waiter.clientId !== clientId)
			}
		}

		if (this.activePromptTurn) {
			this.activePromptTurn.deferredSetupResponses = this.activePromptTurn.deferredSetupResponses.filter(response => response.clientId !== clientId)
		}

		for (const state of this.pendingAgentRequests.values()) {
			if (state.deliveredClientId === clientId) {
				this.clearDeliveredAgentRequest(state)
			}
		}
	}

	async receiveClientMessage(clientId: number, message: JsonRpcMessage) {
		if (isRequestMessage(message)) {
			await this.receiveClientRequest(clientId, message)
			return
		}

		if (isResponseMessage(message)) {
			await this.receiveClientResponse(clientId, message)
			return
		}

		await this.receiveClientNotification(clientId, message)
	}

	async receiveAgentMessage(message: JsonRpcMessage) {
		if (isRequestMessage(message)) {
			await this.receiveAgentRequest(message)
			return
		}

		if (isResponseMessage(message)) {
			await this.receiveAgentResponse(message)
			return
		}

		await this.receiveAgentNotification(message)
	}

	private async receiveClientRequest(clientId: number, request: JsonRpcRequest) {
		if (!this.isCurrentClient(clientId)) {
			return
		}

		if (request.method === METHOD_INITIALIZE) {
			await this.receiveInitializeRequest(clientId, request)
			return
		}

		if (this.activePromptTurn) {
			if (request.method === METHOD_SESSION_PROMPT) {
				await this.sendToClient(clientId, makeErrorResponse(request.id, ERROR_CODE_CONFLICT, 'A prompt turn is already running.', { activeSessionId: this.activePromptTurn.sessionId ?? null }))
				return
			}

			if (isSessionSetupMethod(request.method)) {
				await this.receiveSessionSetupDuringPrompt(clientId, request, request.method)
				return
			}
		}

		if (request.method === METHOD_SESSION_LOAD || request.method === METHOD_SESSION_RESUME) {
			this.mergeSessionCache(request.params)
		}

		if (request.method === METHOD_SESSION_PROMPT) {
			await this.forwardPromptRequest(clientId, request)
			return
		}

		await this.forwardClientRequest(clientId, request)
	}

	private async receiveInitializeRequest(clientId: number, request: JsonRpcRequest) {
		if (this.cachedInitializeResponse) {
			await this.sendResponsePayload(clientId, request.id, this.cachedInitializeResponse)
			return
		}

		if (this.initializeAgentRequestId !== undefined) {
			const pending = this.pendingAgentResponses.get(idKey(this.initializeAgentRequestId))

			if (pending?.kind === 'initialize') {
				pending.waiters.push({ clientId, clientRequestId: request.id })
				return
			}
		}

		const agentRequestId = this.nextProxyRequestId('initialize')
		this.initializeAgentRequestId = agentRequestId
		this.pendingAgentResponses.set(idKey(agentRequestId), {
			kind: 'initialize',
			waiters: [{ clientId, clientRequestId: request.id }],
		})
		await this.sendAgentMessage(requestWithId(request, agentRequestId))
	}

	private async receiveSessionSetupDuringPrompt(clientId: number, request: JsonRpcRequest, originalMethod: SessionSetupMethod) {
		const cachedLoadParams = this.cachedSessionLoad

		if (!cachedLoadParams) {
			await this.sendToClient(clientId, makeErrorResponse(request.id, ERROR_CODE_INTERNAL, 'No cached session/load parameters are available for prompt-turn recovery.'))
			return
		}

		const agentRequestId = this.nextProxyRequestId('session-load')
		this.pendingAgentResponses.set(idKey(agentRequestId), {
			kind: 'promptSetup',
			clientId,
			clientRequestId: request.id,
			originalMethod,
			sessionId: cachedLoadParams.sessionId,
		})
		this.clientRequestToAgentId.set(clientRequestKey(clientId, request.id), agentRequestId)
		await this.sendAgentMessage(makeRequest(agentRequestId, METHOD_SESSION_LOAD, cachedLoadRequest(cachedLoadParams)))
	}

	private async forwardPromptRequest(clientId: number, request: JsonRpcRequest) {
		const agentRequestId = this.nextProxyRequestId('prompt')
		this.pendingAgentResponses.set(idKey(agentRequestId), {
			kind: 'prompt',
			clientId,
			clientRequestId: request.id,
		})
		this.clientRequestToAgentId.set(clientRequestKey(clientId, request.id), agentRequestId)
		this.activePromptTurn = {
			agentPromptRequestId: agentRequestId,
			sessionId: stringProperty(request.params, 'sessionId') ?? this.cachedSessionLoad?.sessionId,
			deferredSetupResponses: [],
		}
		await this.sendAgentMessage(requestWithId(request, agentRequestId))
	}

	private async forwardClientRequest(clientId: number, request: JsonRpcRequest) {
		const agentRequestId = this.nextProxyRequestId('client')
		this.pendingAgentResponses.set(idKey(agentRequestId), {
			kind: 'client',
			clientId,
			clientRequestId: request.id,
			method: request.method,
			params: request.params,
		})
		this.clientRequestToAgentId.set(clientRequestKey(clientId, request.id), agentRequestId)
		await this.sendAgentMessage(requestWithId(request, agentRequestId))
	}

	private async receiveClientResponse(clientId: number, response: JsonRpcResponse) {
		const stateId = this.clientAgentRequestLookup.get(clientRequestKey(clientId, response.id))

		if (!stateId) {
			return
		}

		const state = this.pendingAgentRequests.get(stateId)

		if (!state || state.settled) {
			return
		}

		state.settled = true
		this.clearAgentRequestState(state)
		await this.sendAgentMessage(responseWithId(response, state.agentRequestId))
	}

	private async receiveClientNotification(clientId: number, notification: JsonRpcNotification) {
		if (!this.isCurrentClient(clientId)) {
			return
		}

		if (notification.method === METHOD_CANCEL_REQUEST) {
			const requestId = requestIdFromParams(notification.params)

			if (requestId === undefined) {
				return
			}

			const clientRequestKeyValue = clientRequestKey(clientId, requestId)
			const agentRequestId = this.clientRequestToAgentId.get(clientRequestKeyValue)

			if (agentRequestId !== undefined) {
				await this.sendAgentMessage(cancelRequestNotification(agentRequestId, notification.params))
				return
			}

			const stateId = this.clientAgentRequestLookup.get(clientRequestKeyValue)
			const state = stateId ? this.pendingAgentRequests.get(stateId) : undefined

			if (state) {
				await this.sendAgentMessage(cancelRequestNotification(state.agentRequestId, notification.params))
			}

			return
		}

		await this.sendAgentMessage(notification)
	}

	private async receiveAgentRequest(request: JsonRpcRequest) {
		const stateId = `agent-request:${this.nextRequestStateId++}`
		const state: PendingAgentRequest = {
			stateId,
			agentRequestId: request.id,
			request,
			deliveredClientId: undefined,
			deliveredClientRequestId: undefined,
			settled: false,
		}
		this.pendingAgentRequests.set(stateId, state)
		this.agentRequestStateByAgentId.set(idKey(request.id), stateId)
		await this.deliverAgentRequest(state)
	}

	private async receiveAgentResponse(response: JsonRpcResponse) {
		const pending = this.pendingAgentResponses.get(idKey(response.id))

		if (!pending) {
			return
		}

		this.pendingAgentResponses.delete(idKey(response.id))

		if (this.initializeAgentRequestId !== undefined && idEquals(this.initializeAgentRequestId, response.id)) {
			this.initializeAgentRequestId = undefined
		}

		if (pending.kind === 'initialize') {
			const payload = responsePayload(response)
			this.cachedInitializeResponse = payload

			for (const waiter of pending.waiters) {
				await this.sendResponsePayload(waiter.clientId, waiter.clientRequestId, payload)
			}

			return
		}

		this.clientRequestToAgentId.delete(clientRequestKey(pending.clientId, pending.clientRequestId))

		if (pending.kind === 'client') {
			if (pending.method === METHOD_SESSION_NEW && 'result' in response) {
				this.mergeSessionCache(sessionNewCacheParams(pending.params, response.result))
			}

			await this.sendToClient(pending.clientId, responseWithId(response, pending.clientRequestId))
			return
		}

		if (pending.kind === 'prompt') {
			await this.sendToClient(pending.clientId, responseWithId(response, pending.clientRequestId))

			if (this.activePromptTurn && idEquals(this.activePromptTurn.agentPromptRequestId, response.id)) {
				await this.finishActivePromptTurn()
			}

			return
		}

		const setupResponse = normalizePromptSetupResponse(responseWithId(response, pending.clientRequestId), pending.originalMethod, pending.sessionId)

		if (this.activePromptTurn) {
			this.activePromptTurn.deferredSetupResponses.push({
				clientId: pending.clientId,
				response: setupResponse,
			})
			return
		}

		await this.sendToClient(pending.clientId, setupResponse)
	}

	private async receiveAgentNotification(notification: JsonRpcNotification) {
		if (notification.method === METHOD_CANCEL_REQUEST) {
			const requestId = requestIdFromParams(notification.params)

			if (requestId === undefined) {
				return
			}

			const stateId = this.agentRequestStateByAgentId.get(idKey(requestId))
			const state = stateId ? this.pendingAgentRequests.get(stateId) : undefined

			if (!state || state.deliveredClientId === undefined || state.deliveredClientRequestId === undefined) {
				return
			}

			await this.sendToClient(state.deliveredClientId, cancelRequestNotification(state.deliveredClientRequestId, notification.params))
			return
		}

		await this.sendToCurrentClient(notification)
	}

	private async finishActivePromptTurn() {
		const activePromptTurn = this.activePromptTurn

		if (!activePromptTurn) {
			return
		}

		this.activePromptTurn = undefined

		for (const deferredResponse of activePromptTurn.deferredSetupResponses) {
			await this.sendToClient(deferredResponse.clientId, deferredResponse.response)
		}
	}

	private async flushPendingAgentRequests() {
		for (const state of this.pendingAgentRequests.values()) {
			await this.deliverAgentRequest(state)
		}
	}

	private async deliverAgentRequest(state: PendingAgentRequest) {
		if (state.settled || !this.client) {
			return
		}

		if (state.deliveredClientId === this.client.id) {
			return
		}

		this.clearDeliveredAgentRequest(state)

		const clientRequestId = this.nextProxyRequestId('agent')
		state.deliveredClientId = this.client.id
		state.deliveredClientRequestId = clientRequestId
		this.clientAgentRequestLookup.set(clientRequestKey(this.client.id, clientRequestId), state.stateId)
		await this.sendToClient(this.client.id, requestWithId(state.request, clientRequestId))
	}

	private clearDeliveredAgentRequest(state: PendingAgentRequest) {
		if (state.deliveredClientId !== undefined && state.deliveredClientRequestId !== undefined) {
			this.clientAgentRequestLookup.delete(clientRequestKey(state.deliveredClientId, state.deliveredClientRequestId))
		}

		state.deliveredClientId = undefined
		state.deliveredClientRequestId = undefined
	}

	private clearAgentRequestState(state: PendingAgentRequest) {
		this.clearDeliveredAgentRequest(state)
		this.pendingAgentRequests.delete(state.stateId)
		this.agentRequestStateByAgentId.delete(idKey(state.agentRequestId))
	}

	private mergeSessionCache(params: unknown) {
		if (!isRecord(params)) {
			return
		}

		const base: Record<string, unknown> = this.cachedSessionLoad ? { ...this.cachedSessionLoad } : {}
		const merged = { ...base, ...params }
		const cwd = typeof merged.cwd === 'string' ? merged.cwd : undefined
		const sessionId = typeof merged.sessionId === 'string' ? merged.sessionId : undefined
		const mcpServers = Array.isArray(merged.mcpServers) ? merged.mcpServers : undefined

		if (!cwd || !sessionId || !mcpServers) {
			return
		}

		this.cachedSessionLoad = {
			...merged,
			cwd,
			mcpServers,
			sessionId,
		}
	}

	private async sendResponsePayload(clientId: number, requestId: JsonRpcId, payload: JsonRpcPayload) {
		if ('result' in payload) {
			await this.sendToClient(clientId, makeResultResponse(requestId, payload.result))
			return
		}

		await this.sendToClient(clientId, makeResponseWithError(requestId, payload.error))
	}

	private async sendToCurrentClient(message: JsonRpcMessage) {
		if (!this.client) {
			return false
		}

		return this.sendToClient(this.client.id, message)
	}

	private async sendToClient(clientId: number, message: JsonRpcMessage) {
		const client = this.client

		if (!client || client.id !== clientId) {
			return false
		}

		try {
			await client.sendMessage(message)
			return true
		} catch {
			this.removeClient(clientId)
			return false
		}
	}

	private isCurrentClient(clientId: number) {
		return this.client?.id === clientId
	}

	private nextProxyRequestId(prefix: string) {
		return `acp-to-ws:${prefix}:${this.nextMessageId++}`
	}
}

async function startProxy(args: string[], command: string, host: string, path: string, port: number) {
	const agentProcess = spawn(command, args, {
		shell: process.platform === 'win32',
		stdio: ['pipe', 'pipe', 'inherit'],
	})
	const agentInput = Writable.toWeb(agentProcess.stdin) as WritableStream<Uint8Array>
	const agentOutput = Readable.toWeb(agentProcess.stdout) as ReadableStream<Uint8Array>
	const agentStream = ndJsonStream(agentInput, agentOutput)
	const agentReader = agentStream.readable.getReader()
	const agentWriter = agentStream.writable.getWriter()
	const sendAgentMessage = queuedSender<JsonRpcMessage>(message => agentWriter.write(message as AnyMessage))
	const proxy = new AcpRecoveringProxy({ sendAgentMessage })
	const webSocketServer = new WebSocketServer({ noServer: true })
	const httpServer = createServer((request, response) => {
		if (!isMatchingPath(request.url, path)) {
			response.writeHead(404, { 'Content-Type': 'text/plain' })
			response.end('Not Found')
			return
		}

		response.writeHead(426, { 'Content-Type': 'text/plain' })
		response.end('Upgrade Required')
	})
	let stopPromise: Promise<void> | undefined
	let resolveClosed: () => void = () => {}
	const closed = new Promise<void>(resolve => {
		resolveClosed = resolve
	})

	const stop = (killAgent: boolean) => {
		if (stopPromise) {
			return stopPromise
		}

		stopPromise = (async () => {
			if (killAgent && !agentProcess.killed) {
				agentProcess.kill()
			}

			await Promise.all([closeWebSocketServer(webSocketServer), closeHttpServer(httpServer)])
			try {
				agentWriter.releaseLock()
			} catch {
				// The process is already stopping; pending writes may have closed the writer.
			}
			resolveClosed()
		})()
		return stopPromise
	}

	webSocketServer.on('connection', socket => {
		const sendMessage = queuedSender<JsonRpcMessage>(message => sendSocketMessage(socket, message))
		const clientId = proxy.addClient(sendMessage, () => {
			if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
				socket.close(1000, 'Replaced by a newer ACP client')
			}
		})

		socket.on('message', data => {
			void handleClientSocketMessage(proxy, clientId, socket, data).catch(error => {
				console.error('ACP client message failed:', error)
				proxy.removeClient(clientId)

				if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
					socket.close()
				}
			})
		})
		socket.on('close', () => {
			proxy.removeClient(clientId)
		})
		socket.on('error', () => {
			proxy.removeClient(clientId)
		})
	})

	httpServer.on('upgrade', (request, socket, head) => {
		if (!isMatchingPath(request.url, path)) {
			socket.destroy()
			return
		}

		webSocketServer.handleUpgrade(request, socket, head, webSocket => {
			webSocketServer.emit('connection', webSocket, request)
		})
	})

	agentProcess.once('error', error => {
		console.error('Failed to start ACP agent:', error)
		process.exitCode = 1
		void stop(false)
	})

	agentProcess.once('exit', (code, signal) => {
		if (typeof code === 'number' && code !== 0) {
			process.exitCode = code
		}

		if (signal) {
			console.error(`ACP agent exited after signal ${signal}`)
		}

		void stop(false)
	})

	void (async () => {
		try {
			for (;;) {
				const { done, value } = await agentReader.read()

				if (done) {
					break
				}

				if (isJsonRpcMessage(value)) {
					await proxy.receiveAgentMessage(value)
				} else {
					console.error('Ignoring invalid ACP agent message:', value)
				}
			}
		} catch (error) {
			console.error('ACP agent stream failed:', error)
			process.exitCode = 1
		} finally {
			agentReader.releaseLock()
			void stop(true)
		}
	})()

	try {
		await listen(httpServer, port, host)
		console.error(`ACP WebSocket proxy listening at ws://${host}:${port}${path}`)
		await closed
	} catch (error) {
		await stop(true)
		throw error
	}
}

async function handleClientSocketMessage(proxy: AcpRecoveringProxy, clientId: number, socket: WebSocket, data: RawData) {
	const text = rawDataToString(data)
	let parsed: unknown

	try {
		parsed = JSON.parse(text)
	} catch {
		await sendSocketMessage(socket, makeErrorResponse(null, ERROR_CODE_PARSE, 'Parse error'))
		return
	}

	if (!isJsonRpcMessage(parsed)) {
		await sendSocketMessage(socket, makeErrorResponse(extractResponseId(parsed), ERROR_CODE_INVALID_REQUEST, 'Invalid request'))
		return
	}

	await proxy.receiveClientMessage(clientId, parsed)
}

function queuedSender<T>(send: (message: T) => Promise<void>) {
	let queue = Promise.resolve()

	return (message: T) => {
		const next = queue.then(() => send(message))
		queue = next.catch(() => undefined)
		return next
	}
}

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

function sendSocketMessage(socket: WebSocket, message: JsonRpcMessage) {
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

function listen(server: Server, port: number, host: string) {
	return new Promise<void>((resolve, reject) => {
		const onError = (error: Error) => {
			reject(error)
		}

		server.once('error', onError)
		server.listen(port, host, () => {
			server.off('error', onError)
			resolve()
		})
	})
}

function closeHttpServer(server: Server) {
	if (!server.listening) {
		return Promise.resolve()
	}

	return new Promise<void>(resolve => {
		server.close(() => {
			resolve()
		})
	})
}

function closeWebSocketServer(server: WebSocketServer) {
	for (const socket of server.clients) {
		if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
			socket.close()
		}
	}

	return new Promise<void>(resolve => {
		server.close(() => {
			resolve()
		})
	})
}

function isMatchingPath(url: string | undefined, expectedPath: string) {
	return new URL(url ?? '/', 'http://127.0.0.1').pathname === expectedPath
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

function normalizePromptSetupResponse(response: JsonRpcResponse, originalMethod: SessionSetupMethod, sessionId: string) {
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
	} satisfies JsonRpcRequest
}

function requestWithId(request: JsonRpcRequest, id: JsonRpcId) {
	const nextRequest: JsonRpcRequest = {
		jsonrpc: '2.0',
		id,
		method: request.method,
	}

	if ('params' in request) {
		nextRequest.params = request.params
	}

	return nextRequest
}

function responseWithId(response: JsonRpcResponse, id: JsonRpcId) {
	if ('result' in response) {
		return makeResultResponse(id, response.result)
	}

	return makeResponseWithError(id, response.error)
}

function makeResultResponse(id: JsonRpcId, result: unknown): JsonRpcResponse {
	return {
		jsonrpc: '2.0',
		id,
		result: result === undefined ? null : result,
	}
}

function makeResponseWithError(id: JsonRpcId, error: JsonRpcError): JsonRpcResponse {
	return {
		jsonrpc: '2.0',
		id,
		error,
	}
}

function makeErrorResponse(id: JsonRpcId, code: number, message: string, data?: unknown) {
	return makeResponseWithError(id, makeError(code, message, data))
}

function makeError(code: number, message: string, data?: unknown) {
	const error: JsonRpcError = { code, message }

	if (data !== undefined) {
		error.data = data
	}

	return error
}

function responsePayload(response: JsonRpcResponse): JsonRpcPayload {
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
	} satisfies JsonRpcNotification
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

function isJsonRpcMessage(value: unknown): value is JsonRpcMessage {
	return isRequestMessage(value) || isResponseMessage(value) || isNotificationMessage(value)
}

function isRequestMessage(value: unknown): value is JsonRpcRequest {
	return isRecord(value) && value.jsonrpc === '2.0' && typeof value.method === 'string' && 'id' in value && isJsonRpcId(value.id)
}

function isResponseMessage(value: unknown): value is JsonRpcResponse {
	if (!isRecord(value) || value.jsonrpc !== '2.0' || !('id' in value) || !isJsonRpcId(value.id) || 'method' in value) {
		return false
	}

	const hasResult = 'result' in value
	const hasError = 'error' in value
	return hasResult !== hasError && (!hasError || isJsonRpcError(value.error))
}

function isNotificationMessage(value: unknown): value is JsonRpcNotification {
	return isRecord(value) && value.jsonrpc === '2.0' && typeof value.method === 'string' && !('id' in value)
}

function isJsonRpcError(value: unknown): value is JsonRpcError {
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
	exit(1)
}

await startProxy(args, command, host, join('/', path), parseInt(port))
