#!/usr/bin/env node
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { join } from 'node:path/posix'
import process, { exit } from 'node:process'
import { parseArgs } from 'node:util'
import { AGENT_METHODS, CLIENT_METHODS, PROTOCOL_METHODS } from '@agentclientprotocol/sdk'
import { WebSocket, WebSocketServer } from 'ws'

type JsonRpcId = string | number | null

type JsonRpcRequest = {
	jsonrpc: '2.0'
	id: JsonRpcId
	method: string
	params?: unknown
}

type JsonRpcNotification = {
	jsonrpc: '2.0'
	method: string
	params?: unknown
}

type JsonRpcError = {
	code: number
	message: string
	data?: unknown
}

type JsonRpcResponse = {
	jsonrpc: '2.0'
	id: JsonRpcId
} & ({ result: unknown } | { error: JsonRpcError })

type JsonRpcMessage = JsonRpcRequest | JsonRpcNotification | JsonRpcResponse

export type ProxyOptions = {
	host: string
	port: number
	path: string
	command: string
	args: string[]
}

type WorkspaceConfig = {
	cwd: string
	additionalDirectories?: unknown
	mcpServers: unknown
}

type PendingClientRequestKind = 'initialize' | 'prompt' | 'load' | 'session-new' | 'other'

type PendingClientRequest = {
	client: ClientConnection
	originalId: JsonRpcId
	proxyId: JsonRpcId
	method: string
	kind: PendingClientRequestKind
	sessionId?: string | undefined
	assignedSessionId?: string | undefined
	previousSessionId?: string | undefined
	workspaceCandidate?: WorkspaceConfig | undefined
}

type DeferredResponse = {
	request: PendingClientRequest
	response: JsonRpcResponse
}

type ActivePrompt = {
	client: ClientConnection
	sessionId: string
	proxyId: JsonRpcId
	deferredLoadResponses: DeferredResponse[]
}

type InitializeWaiter = {
	client: ClientConnection
	id: JsonRpcId
}

type PermissionBroadcast = {
	agentRequestId: JsonRpcId
	sessionId: string
	clientRequestIds: Map<ClientConnection, JsonRpcId>
	resolved: boolean
}

const JSON_RPC_VERSION = '2.0'
const INTERNAL_ERROR = -32603
const METHOD_NOT_FOUND = -32601
const PROXY_BUSY = -32000
const PERMISSION_ANSWERED_CLOSE_CODE = 4000

export class AcpToWsProxy {
	readonly options: ProxyOptions
	private webSocketServer: WebSocketServer | undefined
	private child: ChildProcessWithoutNullStreams | undefined
	private clients = new Set<ClientConnection>()
	private clientsBySession = new Map<string, Set<ClientConnection>>()
	private pendingClientRequests = new Map<string, PendingClientRequest>()
	private pendingPermissionRequests = new Map<string, PermissionBroadcast>()
	private initializeResponse: JsonRpcResponse | undefined
	private initializePending: PendingClientRequest | undefined
	private initializeWaiters: InitializeWaiter[] = []
	private activePrompt: ActivePrompt | undefined
	private agentRequestCounter = 0
	private clientRequestCounter = 0
	private agentWriteChain = Promise.resolve()
	private closed = false
	private workspaceConfig: WorkspaceConfig | undefined
	private pendingWorkspaceConfig: WorkspaceConfig | undefined

	constructor(options: ProxyOptions) {
		this.options = options
	}

	async start(): Promise<void> {
		if (this.child || this.webSocketServer) {
			throw new Error('proxy already started')
		}
		this.child = this.spawnStdioAgent()
		this.attachAgent(this.child)
		this.webSocketServer = new WebSocketServer({
			host: this.options.host,
			path: this.options.path,
			port: this.options.port,
		})
		this.webSocketServer.on('connection', webSocket => this.addClient(webSocket))
		await new Promise<void>((resolve, reject) => {
			const server = this.requireWebSocketServer()
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

	get url(): string {
		const server = this.requireWebSocketServer()
		const address = server.address()
		const port = typeof address === 'object' && address ? address.port : this.options.port
		return `ws://${this.options.host}:${port}${this.options.path}`
	}

	async close(reason = 'proxy closed'): Promise<void> {
		if (this.closed) {
			return
		}
		this.closed = true
		for (const client of [...this.clients]) {
			client.close(1001, reason)
		}
		this.clients.clear()
		this.clientsBySession.clear()
		this.pendingClientRequests.clear()
		this.pendingPermissionRequests.clear()
		this.closeChild()
		await this.closeWebSocketServer()
	}

	private spawnStdioAgent(): ChildProcessWithoutNullStreams {
		return spawn(this.options.command, this.options.args, {
			cwd: process.cwd(),
			env: process.env,
			stdio: ['pipe', 'pipe', 'pipe'],
			windowsHide: true,
		})
	}

	private attachAgent(child: ChildProcessWithoutNullStreams): void {
		child.stderr.setEncoding('utf8')
		child.stderr.on('data', chunk => {
			for (const line of splitLogLines(chunk)) {
				console.log(`[agent:stderr] ${line}`)
			}
		})
		child.once('error', error => this.failProxy(`stdio process error: ${error.message}`))
		child.once('exit', (code, signal) => {
			if (this.closed) {
				return
			}
			const reason = signal ? `stdio process exited with signal ${signal}` : `stdio process exited with code ${code ?? 'unknown'}`
			this.failProxy(reason)
		})
		readNdJson(
			child,
			message => this.handleAgentMessage(message),
			error => this.failProxy(`failed to read stdio message: ${formatError(error)}`),
		)
	}

	private addClient(webSocket: WebSocket): void {
		const client = new ClientConnection(this, webSocket, `client-${this.clients.size + 1}`)
		this.clients.add(client)
	}

	removeClient(client: ClientConnection): void {
		this.clients.delete(client)
		this.removeClientFromSession(client)
		for (const request of [...this.pendingClientRequests.values()]) {
			if (request.client === client && request.kind !== 'initialize' && request.kind !== 'prompt') {
				this.pendingClientRequests.delete(idKey(request.proxyId))
			}
		}
		for (const broadcast of new Set(this.pendingPermissionRequests.values())) {
			if (broadcast.clientRequestIds.has(client)) {
				const requestId = broadcast.clientRequestIds.get(client)
				if (requestId !== undefined) {
					this.pendingPermissionRequests.delete(permissionKey(client, requestId))
				}
				broadcast.clientRequestIds.delete(client)
				if (!broadcast.resolved && broadcast.clientRequestIds.size === 0) {
					void this.cancelPermissionRequest(broadcast)
				}
			}
		}
	}

	handleClientMessage(client: ClientConnection, message: JsonRpcMessage): void {
		if (isRequestMessage(message)) {
			void this.handleClientRequest(client, message).catch(error => {
				void client.send(internalErrorResponse(message.id, error))
			})
			return
		}
		if (isResponseMessage(message)) {
			this.handleClientResponse(client, message)
			return
		}
		if (isNotificationMessage(message)) {
			void this.handleClientNotification(client, message).catch(error => client.fail(`failed to handle notification: ${formatError(error)}`))
			return
		}
	}

	private async handleClientRequest(client: ClientConnection, request: JsonRpcRequest): Promise<void> {
		if (request.method === AGENT_METHODS.initialize) {
			await this.handleInitializeRequest(client, request)
			return
		}
		if (!this.initializeResponse || 'error' in this.initializeResponse) {
			await client.send(errorResponse(request.id, PROXY_BUSY, 'ACP agent is not initialized'))
			return
		}
		if (request.method === AGENT_METHODS.session_prompt) {
			await this.handlePromptRequest(client, request)
			return
		}
		if (request.method === AGENT_METHODS.session_load) {
			await this.handleLoadRequest(client, request)
			return
		}
		await this.forwardClientRequest(client, request, this.classifyRequest(request))
	}

	private async handleInitializeRequest(client: ClientConnection, request: JsonRpcRequest): Promise<void> {
		if (this.initializeResponse) {
			await client.send(rewriteResponseId(this.initializeResponse, request.id))
			return
		}
		if (this.initializePending) {
			this.initializeWaiters.push({ client, id: request.id })
			return
		}
		const proxyRequest = this.rewriteClientRequest(request, sanitizeInitializeParams(request.params))
		const pending: PendingClientRequest = {
			client,
			kind: 'initialize',
			method: request.method,
			originalId: request.id,
			proxyId: proxyRequest.id,
		}
		this.initializePending = pending
		this.pendingClientRequests.set(idKey(proxyRequest.id), pending)
		await this.sendAgent(proxyRequest)
	}

	private async handlePromptRequest(client: ClientConnection, request: JsonRpcRequest): Promise<void> {
		const sessionId = sessionIdFromParams(request.params)
		if (!sessionId) {
			await client.send(errorResponse(request.id, -32602, 'session/prompt requires params.sessionId'))
			return
		}
		if (this.activePrompt) {
			await client.send(errorResponse(request.id, PROXY_BUSY, `A prompt is already running in session ${this.activePrompt.sessionId}; load that session and wait for it to finish before sending another prompt.`))
			return
		}
		this.setClientSession(client, sessionId)
		const proxyRequest = this.rewriteClientRequest(request)
		const pending: PendingClientRequest = {
			client,
			kind: 'prompt',
			method: request.method,
			originalId: request.id,
			proxyId: proxyRequest.id,
			sessionId,
		}
		this.activePrompt = {
			client,
			deferredLoadResponses: [],
			proxyId: proxyRequest.id,
			sessionId,
		}
		this.pendingClientRequests.set(idKey(proxyRequest.id), pending)
		await this.sendAgent(proxyRequest)
	}

	private async handleLoadRequest(client: ClientConnection, request: JsonRpcRequest): Promise<void> {
		const sessionId = sessionIdFromParams(request.params)
		const previousSessionId = client.currentSessionId
		if (sessionId) {
			this.setClientSession(client, sessionId)
		}
		await this.forwardClientRequest(client, request, 'load', {
			assignedSessionId: sessionId,
			previousSessionId,
		})
	}

	private async handleClientNotification(client: ClientConnection, notification: JsonRpcNotification): Promise<void> {
		const sessionId = sessionIdFromParams(notification.params)
		if (sessionId) {
			this.setClientSession(client, sessionId)
		}
		const normalizedParams = this.normalizeWorkspaceParams(notification.method, notification.params)
		await this.sendAgent({
			...notification,
			...(normalizedParams === undefined ? {} : { params: normalizedParams }),
		})
	}

	private handleClientResponse(client: ClientConnection, response: JsonRpcResponse): void {
		const broadcast = this.pendingPermissionRequests.get(permissionKey(client, response.id))
		if (!broadcast || broadcast.resolved) {
			return
		}
		broadcast.resolved = true
		for (const [candidate, requestId] of broadcast.clientRequestIds) {
			this.pendingPermissionRequests.delete(permissionKey(candidate, requestId))
			if (candidate !== client) {
				candidate.close(PERMISSION_ANSWERED_CLOSE_CODE, 'permission answered by another client')
			}
		}
		void this.sendAgent(rewriteResponseId(response, broadcast.agentRequestId)).catch(error => this.failProxy(`failed to forward permission response: ${formatError(error)}`))
	}

	private async forwardClientRequest(client: ClientConnection, request: JsonRpcRequest, kind: PendingClientRequestKind, overrides: Partial<PendingClientRequest> = {}): Promise<void> {
		const normalizedParams = this.normalizeWorkspaceParams(request.method, request.params)
		const proxyRequest = this.rewriteClientRequest(request, normalizedParams)
		const pending: PendingClientRequest = {
			client,
			kind,
			method: request.method,
			originalId: request.id,
			proxyId: proxyRequest.id,
			sessionId: sessionIdFromParams(normalizedParams),
			workspaceCandidate: workspaceConfigFromParams(request.method, normalizedParams),
			...overrides,
		}
		this.pendingClientRequests.set(idKey(proxyRequest.id), pending)
		await this.sendAgent(proxyRequest)
	}

	private classifyRequest(request: JsonRpcRequest): PendingClientRequestKind {
		if (request.method === AGENT_METHODS.session_new) {
			return 'session-new'
		}
		return 'other'
	}

	private rewriteClientRequest(request: JsonRpcRequest, params = request.params): JsonRpcRequest {
		const proxyId = this.nextAgentRequestId()
		return {
			jsonrpc: JSON_RPC_VERSION,
			id: proxyId,
			method: request.method,
			...(params === undefined ? {} : { params }),
		}
	}

	private normalizeWorkspaceParams(method: string, params: unknown): unknown {
		if (!isRecord(params)) {
			return params
		}
		const candidate = workspaceConfigFromParams(method, params)
		if (!this.workspaceConfig && !this.pendingWorkspaceConfig && candidate) {
			this.pendingWorkspaceConfig = candidate
			return params
		}
		const config = this.workspaceConfig ?? this.pendingWorkspaceConfig
		if (!config || !methodAcceptsWorkspace(method)) {
			return params
		}
		const normalized: Record<string, unknown> = { ...params }
		normalized['cwd'] = config.cwd
		normalized['mcpServers'] = deepClone(config.mcpServers)
		if (config.additionalDirectories === undefined) {
			delete normalized['additionalDirectories']
		} else {
			normalized['additionalDirectories'] = deepClone(config.additionalDirectories)
		}
		return normalized
	}

	private handleAgentMessage(message: JsonRpcMessage): void {
		if (isRequestMessage(message)) {
			void this.handleAgentRequest(message).catch(error => {
				void this.sendAgent(internalErrorResponse(message.id, error))
			})
			return
		}
		if (isResponseMessage(message)) {
			this.handleAgentResponse(message)
			return
		}
		if (isNotificationMessage(message)) {
			void this.handleAgentNotification(message).catch(error => this.failProxy(`failed to handle agent notification: ${formatError(error)}`))
			return
		}
	}

	private handleAgentResponse(response: JsonRpcResponse): void {
		const pending = this.pendingClientRequests.get(idKey(response.id))
		if (!pending) {
			return
		}
		this.pendingClientRequests.delete(idKey(response.id))
		if (pending.kind === 'initialize') {
			this.applyResponseSideEffects(pending, response)
			this.initializeResponse = response
			this.initializePending = undefined
			void pending.client.send(rewriteResponseId(response, pending.originalId)).catch(error => pending.client.fail(`failed to send initialize response: ${formatError(error)}`))
			for (const waiter of this.initializeWaiters.splice(0)) {
				void waiter.client.send(rewriteResponseId(response, waiter.id)).catch(error => waiter.client.fail(`failed to send cached initialize response: ${formatError(error)}`))
			}
			return
		}
		if (pending.kind === 'load' && this.activePrompt && pending.sessionId === this.activePrompt.sessionId) {
			this.activePrompt.deferredLoadResponses.push({ request: pending, response })
			return
		}
		this.applyResponseSideEffects(pending, response)
		if (pending.kind === 'prompt') {
			this.sendClientResponseSafely(pending, response)
			this.finishPrompt(pending)
			return
		}
		this.sendClientResponseSafely(pending, response)
	}

	private applyResponseSideEffects(pending: PendingClientRequest, response: JsonRpcResponse): void {
		if ('error' in response) {
			if (pending.kind === 'load' && pending.assignedSessionId && pending.previousSessionId !== pending.assignedSessionId && pending.client.currentSessionId === pending.assignedSessionId) {
				if (pending.previousSessionId) {
					this.setClientSession(pending.client, pending.previousSessionId)
				} else {
					this.removeClientFromSession(pending.client)
				}
			}
			if (!this.workspaceConfig && this.pendingWorkspaceConfig && pending.workspaceCandidate === this.pendingWorkspaceConfig) {
				this.pendingWorkspaceConfig = undefined
			}
			return
		}
		const responseSessionId = sessionIdFromResult(response.result)
		if (responseSessionId && pending.method === AGENT_METHODS.session_new) {
			this.setClientSession(pending.client, responseSessionId)
		}
		if (!this.workspaceConfig && pending.workspaceCandidate) {
			this.workspaceConfig = pending.workspaceCandidate
			if (this.pendingWorkspaceConfig === pending.workspaceCandidate) {
				this.pendingWorkspaceConfig = undefined
			}
		}
	}

	private async sendClientResponse(pending: PendingClientRequest, response: JsonRpcResponse): Promise<void> {
		await pending.client.send(rewriteResponseId(response, pending.originalId))
	}

	private sendClientResponseSafely(pending: PendingClientRequest, response: JsonRpcResponse): void {
		void this.sendClientResponse(pending, response).catch(error => {
			if (pending.client.isOpen) {
				pending.client.fail(`failed to send ${pending.method} response: ${formatError(error)}`)
			}
		})
	}

	private finishPrompt(pending: PendingClientRequest): void {
		const prompt = this.activePrompt
		if (!prompt || prompt.proxyId !== pending.proxyId) {
			return
		}
		this.activePrompt = undefined
		for (const deferred of prompt.deferredLoadResponses) {
			this.applyResponseSideEffects(deferred.request, deferred.response)
			this.sendClientResponseSafely(deferred.request, deferred.response)
		}
	}

	private async handleAgentNotification(notification: JsonRpcNotification): Promise<void> {
		if (notification.method === CLIENT_METHODS.session_update) {
			const sessionId = sessionIdFromParams(notification.params)
			if (!sessionId) {
				return
			}
			await this.sendToSession(sessionId, notification)
			return
		}
		if (notification.method === PROTOCOL_METHODS.cancel_request) {
			this.handleAgentCancelRequest(notification)
		}
	}

	private handleAgentCancelRequest(notification: JsonRpcNotification): void {
		if (!isRecord(notification.params)) {
			return
		}
		const requestId = notification.params['requestId']
		for (const broadcast of new Set(this.pendingPermissionRequests.values())) {
			if (sameId(broadcast.agentRequestId, requestId)) {
				for (const [client, clientRequestId] of broadcast.clientRequestIds) {
					void client.send({
						jsonrpc: JSON_RPC_VERSION,
						method: PROTOCOL_METHODS.cancel_request,
						params: { requestId: clientRequestId },
					})
					this.pendingPermissionRequests.delete(permissionKey(client, clientRequestId))
				}
				broadcast.clientRequestIds.clear()
				broadcast.resolved = true
			}
		}
	}

	private async handleAgentRequest(request: JsonRpcRequest): Promise<void> {
		if (request.method === CLIENT_METHODS.session_request_permission) {
			await this.handlePermissionRequest(request)
			return
		}
		await this.sendAgent(errorResponse(request.id, METHOD_NOT_FOUND, `"Method not found": ${request.method}`, { method: request.method }))
	}

	private async handlePermissionRequest(request: JsonRpcRequest): Promise<void> {
		const sessionId = sessionIdFromParams(request.params)
		if (!sessionId) {
			await this.sendAgent(errorResponse(request.id, -32602, 'session/request_permission requires params.sessionId'))
			return
		}
		const candidates = [...(this.clientsBySession.get(sessionId) ?? [])].filter(client => client.isOpen)
		if (candidates.length === 0) {
			await this.cancelPermissionRequest({ agentRequestId: request.id, clientRequestIds: new Map(), resolved: false, sessionId })
			return
		}
		const broadcast: PermissionBroadcast = {
			agentRequestId: request.id,
			clientRequestIds: new Map(),
			resolved: false,
			sessionId,
		}
		for (const client of candidates) {
			const clientRequestId = this.nextClientRequestId()
			broadcast.clientRequestIds.set(client, clientRequestId)
			this.pendingPermissionRequests.set(permissionKey(client, clientRequestId), broadcast)
			await client.send({
				jsonrpc: JSON_RPC_VERSION,
				id: clientRequestId,
				method: request.method,
				...(request.params === undefined ? {} : { params: request.params }),
			})
		}
	}

	private async cancelPermissionRequest(broadcast: PermissionBroadcast): Promise<void> {
		if (broadcast.resolved) {
			return
		}
		broadcast.resolved = true
		for (const [client, requestId] of broadcast.clientRequestIds) {
			this.pendingPermissionRequests.delete(permissionKey(client, requestId))
		}
		await this.sendAgent({
			jsonrpc: JSON_RPC_VERSION,
			method: AGENT_METHODS.session_cancel,
			params: { sessionId: broadcast.sessionId },
		})
		await this.sendAgent({
			jsonrpc: JSON_RPC_VERSION,
			id: broadcast.agentRequestId,
			result: { outcome: { outcome: 'cancelled' } },
		})
	}

	private async sendToSession(sessionId: string, message: JsonRpcMessage): Promise<void> {
		const clients = [...(this.clientsBySession.get(sessionId) ?? [])]
		await Promise.all(clients.map(client => client.send(message).catch(error => client.fail(`failed to send session message: ${formatError(error)}`))))
	}

	private setClientSession(client: ClientConnection, sessionId: string): void {
		if (client.currentSessionId === sessionId) {
			return
		}
		this.removeClientFromSession(client)
		client.currentSessionId = sessionId
		let clients = this.clientsBySession.get(sessionId)
		if (!clients) {
			clients = new Set()
			this.clientsBySession.set(sessionId, clients)
		}
		clients.add(client)
	}

	private removeClientFromSession(client: ClientConnection): void {
		const sessionId = client.currentSessionId
		if (!sessionId) {
			return
		}
		const clients = this.clientsBySession.get(sessionId)
		clients?.delete(client)
		if (clients?.size === 0) {
			this.clientsBySession.delete(sessionId)
		}
		client.currentSessionId = undefined
	}

	private nextAgentRequestId(): string {
		this.agentRequestCounter += 1
		return `proxy-${this.agentRequestCounter}`
	}

	private nextClientRequestId(): string {
		this.clientRequestCounter += 1
		return `agent-${this.clientRequestCounter}`
	}

	private sendAgent(message: JsonRpcMessage): Promise<void> {
		const child = this.requireChild()
		if (child.stdin.destroyed) {
			return Promise.reject(new Error('stdio stdin is closed'))
		}
		this.agentWriteChain = this.agentWriteChain.then(
			() =>
				new Promise<void>((resolve, reject) => {
					child.stdin.write(`${JSON.stringify(message)}\n`, 'utf8', error => {
						if (error) {
							reject(error)
							return
						}
						resolve()
					})
				}),
		)
		return this.agentWriteChain
	}

	private failProxy(reason: string): void {
		if (this.closed) {
			return
		}
		console.log(`[proxy] ${reason}`)
		for (const client of [...this.clients]) {
			client.close(1011, reason)
		}
		void this.close(reason)
	}

	private closeChild(): void {
		const child = this.child
		if (!child) {
			return
		}
		child.stdin.destroy()
		if (child.exitCode !== null || child.signalCode !== null) {
			return
		}
		child.kill()
		const killTimer = setTimeout(() => {
			if (child.exitCode === null && child.signalCode === null) {
				child.kill('SIGKILL')
			}
		}, 5000)
		killTimer.unref()
	}

	private async closeWebSocketServer(): Promise<void> {
		const server = this.webSocketServer
		if (!server) {
			return
		}
		await new Promise<void>((resolve, reject) => {
			server.close(error => {
				if (error) {
					reject(error)
					return
				}
				resolve()
			})
		})
	}

	private requireChild(): ChildProcessWithoutNullStreams {
		if (!this.child) {
			throw new Error('stdio agent is not started')
		}
		return this.child
	}

	private requireWebSocketServer(): WebSocketServer {
		if (!this.webSocketServer) {
			throw new Error('websocket server is not started')
		}
		return this.webSocketServer
	}
}

class ClientConnection {
	currentSessionId: string | undefined
	private writeChain = Promise.resolve()
	private closed = false

	constructor(
		proxy: AcpToWsProxy,
		private readonly webSocket: WebSocket,
		readonly label: string,
	) {
		webSocket.on('message', (data, isBinary) => {
			try {
				const message = parseWebSocketMessage(data, isBinary)
				proxy.handleClientMessage(this, message)
			} catch (error) {
				this.fail(`invalid websocket message: ${formatError(error)}`)
			}
		})
		webSocket.once('close', () => {
			this.closed = true
			proxy.removeClient(this)
		})
		webSocket.once('error', error => this.fail(`websocket error: ${error.message}`))
	}

	get isOpen(): boolean {
		return !this.closed && this.webSocket.readyState === WebSocket.OPEN
	}

	send(message: JsonRpcMessage): Promise<void> {
		if (!this.isOpen) {
			return Promise.reject(new Error(`${this.label} websocket is not open`))
		}
		this.writeChain = this.writeChain.then(
			() =>
				new Promise<void>((resolve, reject) => {
					this.webSocket.send(JSON.stringify(message), error => {
						if (error) {
							reject(error)
							return
						}
						resolve()
					})
				}),
		)
		return this.writeChain
	}

	close(code: number, reason: string): void {
		if (this.webSocket.readyState !== WebSocket.OPEN && this.webSocket.readyState !== WebSocket.CONNECTING) {
			return
		}
		this.webSocket.close(code, reason.slice(0, 123))
	}

	fail(reason: string): void {
		console.log(`[${this.label}] ${reason}`)
		this.close(1011, reason)
	}
}

export async function startProxy(options: ProxyOptions): Promise<AcpToWsProxy> {
	const proxy = new AcpToWsProxy(options)
	await proxy.start()
	return proxy
}

function readNdJson(child: ChildProcessWithoutNullStreams, onMessage: (message: JsonRpcMessage) => void, onError: (error: unknown) => void): void {
	let buffer = ''
	let stopped = false
	const processLine = (line: string) => {
		if (stopped) {
			return
		}
		try {
			const trimmedLine = line.trim()
			if (trimmedLine.length === 0) {
				return
			}
			const value = JSON.parse(trimmedLine) as unknown
			if (!isJsonRpcMessage(value)) {
				throw new Error(`non-JSON-RPC message from stdio: ${safeJson(value)}`)
			}
			onMessage(value)
		} catch (error) {
			stopped = true
			onError(error)
		}
	}
	child.stdout.setEncoding('utf8')
	child.stdout.on('data', chunk => {
		if (stopped) {
			return
		}
		buffer += chunk
		let newlineIndex = buffer.indexOf('\n')
		while (newlineIndex >= 0) {
			const line = buffer.slice(0, newlineIndex)
			buffer = buffer.slice(newlineIndex + 1)
			processLine(line)
			newlineIndex = buffer.indexOf('\n')
		}
	})
	child.stdout.once('end', () => {
		if (!stopped && buffer.trim().length > 0) {
			processLine(buffer)
		}
	})
	child.stdout.once('error', error => {
		if (!stopped) {
			stopped = true
			onError(error)
		}
	})
}

function parseWebSocketMessage(data: WebSocket.RawData, isBinary: boolean): JsonRpcMessage {
	if (isBinary) {
		throw new Error('ACP WebSocket messages must be text frames')
	}
	const text = rawDataToString(data)
	const value = JSON.parse(text) as unknown
	if (Array.isArray(value)) {
		throw new Error('JSON-RPC batch messages are not supported')
	}
	if (!isJsonRpcMessage(value)) {
		throw new Error(`non-JSON-RPC message from websocket: ${safeJson(value)}`)
	}
	return value
}

function isJsonRpcMessage(value: unknown): value is JsonRpcMessage {
	return isRequestMessage(value) || isNotificationMessage(value) || isResponseMessage(value)
}

function isRequestMessage(value: unknown): value is JsonRpcRequest {
	return isRecord(value) && value['jsonrpc'] === JSON_RPC_VERSION && typeof value['method'] === 'string' && hasOwn(value, 'id') && isJsonRpcId(value['id'])
}

function isNotificationMessage(value: unknown): value is JsonRpcNotification {
	return isRecord(value) && value['jsonrpc'] === JSON_RPC_VERSION && typeof value['method'] === 'string' && !hasOwn(value, 'id')
}

function isResponseMessage(value: unknown): value is JsonRpcResponse {
	return isRecord(value) && value['jsonrpc'] === JSON_RPC_VERSION && hasOwn(value, 'id') && isJsonRpcId(value['id']) && hasOwn(value, 'result') !== hasOwn(value, 'error') && typeof value['method'] !== 'string'
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function hasOwn(value: Record<string, unknown>, key: string): boolean {
	return Object.prototype.hasOwnProperty.call(value, key)
}

function isJsonRpcId(value: unknown): value is JsonRpcId {
	return typeof value === 'string' || typeof value === 'number' || value === null
}

function idKey(id: JsonRpcId): string {
	return `${id === null ? 'null' : typeof id}:${String(id)}`
}

function permissionKey(client: ClientConnection, id: JsonRpcId): string {
	return `${client.label}:${idKey(id)}`
}

function sameId(left: JsonRpcId, right: unknown): boolean {
	return isJsonRpcId(right) && idKey(left) === idKey(right)
}

function sessionIdFromParams(params: unknown): string | undefined {
	if (!isRecord(params)) {
		return undefined
	}
	const sessionId = params['sessionId']
	return typeof sessionId === 'string' ? sessionId : undefined
}

function sessionIdFromResult(result: unknown): string | undefined {
	if (!isRecord(result)) {
		return undefined
	}
	const sessionId = result['sessionId']
	return typeof sessionId === 'string' ? sessionId : undefined
}

function sanitizeInitializeParams(params: unknown): unknown {
	if (!isRecord(params)) {
		return params
	}
	const sanitized: Record<string, unknown> = { ...params }
	if (isRecord(params['clientCapabilities'])) {
		const capabilities: Record<string, unknown> = { ...params['clientCapabilities'] }
		delete capabilities['fs']
		delete capabilities['terminal']
		delete capabilities['elicitation']
		delete capabilities['nes']
		sanitized['clientCapabilities'] = capabilities
	}
	return sanitized
}

function methodAcceptsWorkspace(method: string): boolean {
	return method === AGENT_METHODS.session_new || method === AGENT_METHODS.session_load || method === AGENT_METHODS.session_fork || method === AGENT_METHODS.session_resume
}

function workspaceConfigFromParams(method: string, params: unknown): WorkspaceConfig | undefined {
	if (!methodAcceptsWorkspace(method) || !isRecord(params)) {
		return undefined
	}
	const cwd = params['cwd']
	if (typeof cwd !== 'string' || !hasOwn(params, 'mcpServers')) {
		return undefined
	}
	const config: WorkspaceConfig = {
		cwd,
		mcpServers: deepClone(params['mcpServers']),
	}
	if (hasOwn(params, 'additionalDirectories')) {
		config.additionalDirectories = deepClone(params['additionalDirectories'])
	}
	return config
}

function rewriteResponseId(response: JsonRpcResponse, id: JsonRpcId): JsonRpcResponse {
	return { ...response, id }
}

function errorResponse(id: JsonRpcId, code: number, message: string, data?: unknown): JsonRpcResponse {
	const error: JsonRpcError = data === undefined ? { code, message } : { code, data, message }
	return {
		error,
		id,
		jsonrpc: JSON_RPC_VERSION,
	}
}

function internalErrorResponse(id: JsonRpcId, error: unknown): JsonRpcResponse {
	return errorResponse(id, INTERNAL_ERROR, 'Internal error', { details: formatError(error) })
}

function deepClone<T>(value: T): T {
	return value === undefined ? value : (JSON.parse(JSON.stringify(value)) as T)
}

function rawDataToString(data: WebSocket.RawData): string {
	if (typeof data === 'string') {
		return data
	}
	if (Buffer.isBuffer(data)) {
		return data.toString('utf8')
	}
	if (Array.isArray(data)) {
		return Buffer.concat(data).toString('utf8')
	}
	return Buffer.from(data).toString('utf8')
}

function splitLogLines(chunk: string): string[] {
	return chunk
		.split(/\r?\n/)
		.map(line => line.trimEnd())
		.filter(line => line.length > 0)
}

function safeJson(value: unknown): string {
	try {
		return JSON.stringify(value)
	} catch {
		return '[unserializable]'
	}
}

function formatError(error: unknown): string {
	if (error instanceof Error) {
		return error.stack ?? error.message
	}
	return safeJson(error)
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

const proxy = await startProxy({
	args,
	command,
	host,
	path: join('/', path),
	port: parseInt(port),
})
console.log(`[proxy] ACP WebSocket endpoint listening at ${proxy.url}`)
console.log(`[proxy] stdio agent command: ${command} ${args}`)
