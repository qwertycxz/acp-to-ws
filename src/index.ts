#!/usr/bin/env node
import { join } from 'node:path/posix'
import { exit } from 'node:process'
import { parseArgs } from 'node:util'

async function startProxy(args: string[], command: string, host: string, path: string, port: number) {
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
