import assert from 'node:assert/strict'
import { once } from 'node:events'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { AGENT_METHODS, CLIENT_METHODS } from '@agentclientprotocol/sdk'
import { WebSocket } from 'ws'
import { startProxy, type ProxyOptions } from './index.js'

type JsonRpcId = string | number | null

type RequestMessage = {
	jsonrpc: '2.0'
	id: JsonRpcId
	method: string
	params?: unknown
}

type NotificationMessage = {
	jsonrpc: '2.0'
	method: string
	params?: unknown
}

type ResponseMessage = {
	jsonrpc: '2.0'
	id: JsonRpcId
	result?: unknown
	error?: { code: number; message: string; data?: unknown }
}

type Message = RequestMessage | NotificationMessage | ResponseMessage

const distDir = dirname(fileURLToPath(import.meta.url))
const fakeAgentPath = join(distDir, 'test-fixtures', 'fake-agent.js')

test('caches initialize and strips unsupported client capabilities', async () => {
	const harness = await createHarness()
	try {
		const first = await harness.client()
		const firstInit = await first.request(AGENT_METHODS.initialize, {
			clientCapabilities: {
				fs: { readTextFile: true, writeTextFile: true },
				terminal: true,
			},
			protocolVersion: 1,
		})
		const second = await harness.client()
		const secondInit = await second.request(AGENT_METHODS.initialize, {
			clientCapabilities: {
				fs: { readTextFile: true },
			},
			protocolVersion: 1,
		})

		assert.deepEqual(firstInit.result, secondInit.result)
		assert.deepEqual(readMeta(firstInit, 'receivedClientCapabilities'), {})
	} finally {
		await harness.close()
	}
})

test('normalizes later session workspace to the first session request', async () => {
	const harness = await createHarness()
	try {
		const first = await harness.client()
		await initialize(first)
		const newSession = await first.request(AGENT_METHODS.session_new, {
			additionalDirectories: ['C:/first-extra'],
			cwd: 'C:/first',
			mcpServers: [{ args: [], env: [], name: 'first', command: 'first-mcp' }],
		})
		assert.equal(readResultField(newSession, 'sessionId'), 'session-1')

		const second = await harness.client()
		await initialize(second)
		const load = await second.request(AGENT_METHODS.session_load, {
			additionalDirectories: ['C:/second-extra'],
			cwd: 'C:/second',
			mcpServers: [{ args: [], env: [], name: 'second', command: 'second-mcp' }],
			sessionId: 'session-1',
		})

		assert.deepEqual(readMeta(load, 'receivedWorkspace'), {
			additionalDirectories: ['C:/first-extra'],
			cwd: 'C:/first',
			mcpServers: [{ args: [], env: [], name: 'first', command: 'first-mcp' }],
		})
	} finally {
		await harness.close()
	}
})

test('defers same-session load response while prompt is active but forwards updates', async () => {
	const harness = await createHarness()
	try {
		const promptClient = await harness.client()
		await initialize(promptClient)
		await promptClient.request(AGENT_METHODS.session_new, {
			cwd: 'C:/repo',
			mcpServers: [],
		})
		const promptPromise = promptClient.request(AGENT_METHODS.session_prompt, {
			prompt: [{ text: 'hold', type: 'text' }],
			sessionId: 'session-1',
		})
		await promptClient.waitForNotification(CLIENT_METHODS.session_update)

		const loadingClient = await harness.client()
		await initialize(loadingClient)
		const loadPromise = loadingClient.request(AGENT_METHODS.session_load, {
			cwd: 'C:/repo',
			mcpServers: [],
			sessionId: 'session-1',
		})
		const loadUpdate = await loadingClient.waitForNotification(CLIENT_METHODS.session_update)
		assert.equal(readUpdateText(loadUpdate), 'load-history')
		await assert.rejects(loadingClient.expectNoMessage(50), /no message/)

		promptClient.notify(AGENT_METHODS.session_cancel, { sessionId: 'session-1' })
		const promptResponse = await promptPromise
		const loadResponse = await loadPromise
		assert.equal(readResultField(promptResponse, 'stopReason'), 'cancelled')
		assert.ok('result' in loadResponse)
	} finally {
		await harness.close()
	}
})

test('flushes deferred load response after original prompt client disconnects', async () => {
	const harness = await createHarness()
	try {
		const promptClient = await harness.client()
		await initialize(promptClient)
		await promptClient.request(AGENT_METHODS.session_new, {
			cwd: 'C:/repo',
			mcpServers: [],
		})
		void promptClient.request(AGENT_METHODS.session_prompt, {
			prompt: [{ text: 'hold', type: 'text' }],
			sessionId: 'session-1',
		})
		await promptClient.waitForNotification(CLIENT_METHODS.session_update)
		promptClient.socket.close()
		await once(promptClient.socket, 'close')

		const loadingClient = await harness.client()
		await initialize(loadingClient)
		const loadPromise = loadingClient.request(AGENT_METHODS.session_load, {
			cwd: 'C:/repo',
			mcpServers: [],
			sessionId: 'session-1',
		})
		await loadingClient.waitForNotification(CLIENT_METHODS.session_update)
		await assert.rejects(loadingClient.expectNoMessage(50), /no message/)

		loadingClient.notify(AGENT_METHODS.session_cancel, { sessionId: 'session-1' })
		const loadResponse = await loadPromise
		assert.ok('result' in loadResponse)
	} finally {
		await harness.close()
	}
})

test('rejects concurrent prompts', async () => {
	const harness = await createHarness()
	try {
		const first = await harness.client()
		await initialize(first)
		await first.request(AGENT_METHODS.session_new, {
			cwd: 'C:/repo',
			mcpServers: [],
		})
		const promptPromise = first.request(AGENT_METHODS.session_prompt, {
			prompt: [{ text: 'hold', type: 'text' }],
			sessionId: 'session-1',
		})
		await first.waitForNotification(CLIENT_METHODS.session_update)

		const second = await harness.client()
		await initialize(second)
		const rejected = await second.request(AGENT_METHODS.session_prompt, {
			prompt: [{ text: 'second', type: 'text' }],
			sessionId: 'session-1',
		})
		assert.equal(rejected.error?.code, -32000)

		first.notify(AGENT_METHODS.session_cancel, { sessionId: 'session-1' })
		await promptPromise
	} finally {
		await harness.close()
	}
})

test('permission request broadcasts to session clients and first response wins', async () => {
	const harness = await createHarness()
	try {
		const first = await harness.client()
		await initialize(first)
		await first.request(AGENT_METHODS.session_new, {
			cwd: 'C:/repo',
			mcpServers: [],
		})
		const second = await harness.client()
		await initialize(second)
		await second.request(AGENT_METHODS.session_load, {
			cwd: 'C:/repo',
			mcpServers: [],
			sessionId: 'session-1',
		})

		const promptPromise = first.request(AGENT_METHODS.session_prompt, {
			prompt: [{ text: 'permission', type: 'text' }],
			sessionId: 'session-1',
		})
		await first.waitForNotification(CLIENT_METHODS.session_update)
		const firstPermission = await first.waitForRequest(CLIENT_METHODS.session_request_permission)
		const secondPermission = await second.waitForRequest(CLIENT_METHODS.session_request_permission)
		first.respond(firstPermission.id, { outcome: { outcome: 'selected', optionId: 'allow' } })
		await once(second.socket, 'close')
		assert.equal(firstPermission.method, CLIENT_METHODS.session_request_permission)
		assert.equal(secondPermission.method, CLIENT_METHODS.session_request_permission)

		const promptResponse = await promptPromise
		assert.equal(readResultField(promptResponse, 'stopReason'), 'end_turn')
	} finally {
		await harness.close()
	}
})

async function createHarness(): Promise<{ client: () => Promise<TestClient>; close: () => Promise<void> }> {
	const proxy = await startProxy({
		args: [fakeAgentPath],
		command: process.execPath,
		host: '127.0.0.1',
		path: '/acp',
		port: 0,
	} satisfies ProxyOptions)
	return {
		client: () => TestClient.connect(proxy.url),
		close: () => proxy.close(),
	}
}

async function initialize(client: TestClient): Promise<ResponseMessage> {
	return client.request(AGENT_METHODS.initialize, {
		clientCapabilities: {},
		protocolVersion: 1,
	})
}

class TestClient {
	private nextId = 1
	private messages: Message[] = []
	private waiters: Array<(message: Message) => boolean> = []

	private constructor(readonly socket: WebSocket) {
		socket.on('message', data => {
			const message = JSON.parse(data.toString()) as Message
			const waiter = this.waiters.find(wait => wait(message))
			if (waiter) {
				this.waiters = this.waiters.filter(wait => wait !== waiter)
				return
			}
			this.messages.push(message)
		})
	}

	static async connect(url: string): Promise<TestClient> {
		const socket = new WebSocket(url)
		await once(socket, 'open')
		return new TestClient(socket)
	}

	async request(method: string, params?: unknown): Promise<ResponseMessage> {
		const id = this.nextId++
		this.socket.send(JSON.stringify({ id, jsonrpc: '2.0', method, params }))
		return this.waitForResponse(id)
	}

	notify(method: string, params?: unknown): void {
		this.socket.send(JSON.stringify({ jsonrpc: '2.0', method, params }))
	}

	respond(id: JsonRpcId, result: unknown): void {
		this.socket.send(JSON.stringify({ id, jsonrpc: '2.0', result }))
	}

	waitForResponse(id: JsonRpcId): Promise<ResponseMessage> {
		return this.waitFor(message => isResponse(message) && message.id === id) as Promise<ResponseMessage>
	}

	waitForNotification(method: string): Promise<NotificationMessage> {
		return this.waitFor(message => isNotification(message) && message.method === method) as Promise<NotificationMessage>
	}

	waitForRequest(method: string): Promise<RequestMessage> {
		return this.waitFor(message => isRequest(message) && message.method === method) as Promise<RequestMessage>
	}

	async expectNoMessage(timeoutMs: number): Promise<void> {
		await new Promise<void>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.waiters = this.waiters.filter(waiter => waiter !== onMessage)
				resolve()
			}, timeoutMs)
			const onMessage = (message: Message) => {
				clearTimeout(timer)
				reject(new Error(`expected no message, got ${JSON.stringify(message)}`))
				return true
			}
			this.waiters.push(onMessage)
		})
		throw new Error('no message')
	}

	private waitFor(predicate: (message: Message) => boolean): Promise<Message> {
		const existing = this.messages.find(predicate)
		if (existing) {
			this.messages = this.messages.filter(message => message !== existing)
			return Promise.resolve(existing)
		}
		return new Promise(resolve => {
			this.waiters.push(message => {
				if (!predicate(message)) {
					return false
				}
				resolve(message)
				return true
			})
		})
	}
}

function isRequest(message: Message): message is RequestMessage {
	return 'method' in message && 'id' in message
}

function isNotification(message: Message): message is NotificationMessage {
	return 'method' in message && !('id' in message)
}

function isResponse(message: Message): message is ResponseMessage {
	return 'id' in message && !('method' in message)
}

function readMeta(response: ResponseMessage, key: string): unknown {
	assert.ok(response.result && typeof response.result === 'object')
	const meta = (response.result as Record<string, unknown>)['_meta']
	assert.ok(meta && typeof meta === 'object')
	return (meta as Record<string, unknown>)[key]
}

function readResultField(response: ResponseMessage, key: string): unknown {
	assert.ok(response.result && typeof response.result === 'object')
	return (response.result as Record<string, unknown>)[key]
}

function readUpdateText(notification: NotificationMessage): unknown {
	assert.ok(notification.params && typeof notification.params === 'object')
	const update = (notification.params as Record<string, unknown>)['update']
	assert.ok(update && typeof update === 'object')
	const content = (update as Record<string, unknown>)['content']
	assert.ok(content && typeof content === 'object')
	return (content as Record<string, unknown>)['text']
}
