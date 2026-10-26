import { MAX_SECURE_MESSAGE_BYTES } from '@dsh-remote/protocol'
import type { PluginRouteCallParams, PluginRouteCallResult } from '@dsh-remote/protocol'
import { z } from 'zod'
import type { SafeLogger } from './logging.js'
import { RpcError } from './safe-error.js'

/** Local plugin web routes forwardable over the authenticated channel (POST only). */
export const PLUGIN_ROUTE_ALLOWLIST = ['api/bot'] as const

/** Reviewed DS Bot operations; new upstream operations stay local by default. */
export const BOT_REMOTE_OPERATIONS = [
  'state', 'models', 'efforts', 'exchange', 'earlier', 'chat', 'usage', 'room-progress', 'schedules',
  'update-check',
  'create-bot', 'duplicate-bot', 'update-bot', 'delete-bot', 'set-main', 'set-flags',
  'create-room', 'update-room', 'delete-room',
  'add-agent', 'agent-session', 'agent-adopt', 'agent-archive', 'agent-rename', 'remove-agent',
  'send', 'answer-question', 'dismiss-question', 'view-reply', 'approval-answer', 'stop-member-work',
] as const

export function isAllowedPluginRoute(route: string): route is typeof PLUGIN_ROUTE_ALLOWLIST[number] {
  return PLUGIN_ROUTE_ALLOWLIST.some(allowed => allowed === route)
}

export type PluginRouteDispatcher = (
  route: string,
  body: unknown,
  signal: AbortSignal,
) => Promise<{ status: number; json: unknown }>

const callSchema = z.object({
  route: z.enum(PLUGIN_ROUTE_ALLOWLIST),
  body: z.object({
    endpoint: z.enum(BOT_REMOTE_OPERATIONS),
    payload: z.record(z.string(), z.unknown()).nullable().optional(),
  }).strict().superRefine((body, ctx) => {
    if (body.endpoint === 'approval-answer' && body.payload?.choice !== 'allow' && body.payload?.choice !== 'reject') {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'Only one-time approval or rejection is supported.' })
    }
  }),
}).strict()

/** Local plugin route call timeout, mirroring the Harness API bridge budget. */
const PLUGIN_ROUTE_TIMEOUT_MS = 30_000

/**
 * Forwards bounded POST calls to local plugin web routes (first case: ds-bot's
 * `api/bot`) through the shared fetch handler of the Host connection service.
 * The route allowlist and both size bounds keep the tunnel fail-closed.
 */
export class PluginRouteBridge {
  constructor(
    private readonly dispatch: PluginRouteDispatcher,
    private readonly logger?: SafeLogger,
  ) {}

  async call(input: unknown): Promise<PluginRouteCallResult> {
    let params: PluginRouteCallParams
    try {
      params = callSchema.parse(input) as PluginRouteCallParams
    } catch (error) {
      if (error instanceof z.ZodError) {
        throw new RpcError('INVALID_MESSAGE', 'The plugin route call is invalid for the remote channel.')
      }
      throw error
    }
    if (serializedBytes(params) > MAX_SECURE_MESSAGE_BYTES) {
      throw new RpcError('INVALID_MESSAGE', `The plugin route call exceeds the ${MAX_SECURE_MESSAGE_BYTES}-byte remote limit.`)
    }
    const startedAt = performance.now()
    let result: { status: number; json: unknown }
    try {
      result = await this.dispatch(params.route, params.body, AbortSignal.timeout(PLUGIN_ROUTE_TIMEOUT_MS))
    } catch (error) {
      this.logger?.warn('plugin route call failed', {
        route: params.route,
        durationMs: Math.round(performance.now() - startedAt),
      })
      throw error
    }
    if (serializedBytes(result) > MAX_SECURE_MESSAGE_BYTES) {
      throw new RpcError('RESPONSE_TOO_LARGE', 'The plugin route response exceeds the bounded remote limit.')
    }
    this.logger?.debug('plugin route call ok', {
      route: params.route,
      status: result.status,
      durationMs: Math.round(performance.now() - startedAt),
    })
    return { status: result.status, json: result.json }
  }
}

function serializedBytes(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength
}

/** Local plugin web routes forwardable as authenticated event streams (GET only). */
export const PLUGIN_ROUTE_STREAM_ALLOWLIST = ['api/bot/feed'] as const

/** Opens one allowlisted local plugin route and returns its raw HTTP response. */
export type PluginRouteStreamDispatcher = (route: string, signal: AbortSignal) => Promise<Response>

const streamSchema = z.object({ route: z.enum(PLUGIN_ROUTE_STREAM_ALLOWLIST) }).strict()

/** Largest single buffered Server-Sent Events record accepted from a local route. */
const MAX_SSE_EVENT_BYTES = 512 * 1024
/**
 * How long a local route may take to start streaming before the open fails.
 * Keep it below the Client's 30s RPC budget so a slow Host fails the open before
 * the Client times out and retires a stream that would otherwise open later.
 */
const PLUGIN_STREAM_OPEN_TIMEOUT_MS = 15_000
const SSE_BOUNDARIES = ['\r\n\r\n', '\n\n', '\r\r'] as const

export interface SseEvent {
  event: string
  data: unknown
}

/**
 * Bridges one allowlisted local plugin event stream into the authenticated
 * Remote stream lifecycle. The DS Bot feed is a Server-Sent Events route on
 * the Host (`GET api/bot/feed`); DS Bot does not register a Typert Remote
 * provider for it, so the official Gateway cannot open `dsBot/feed`. This
 * bridge keeps the wire contract (`{ event, data }` frames) identical while
 * routing the stream through the bounded local plugin-route carrier.
 */
export class PluginRouteStreamBridge {
  constructor(
    private readonly dispatch: PluginRouteStreamDispatcher,
    private readonly logger?: SafeLogger,
  ) {}

  async open(input: unknown, signal: AbortSignal): Promise<AsyncIterable<unknown>> {
    let params: z.infer<typeof streamSchema>
    try {
      params = streamSchema.parse(input) as z.infer<typeof streamSchema>
    } catch (error) {
      if (error instanceof z.ZodError) throw new RpcError('INVALID_MESSAGE', 'The plugin route stream is invalid for the remote channel.')
      throw error
    }

    const startedAt = performance.now()
    const openController = new AbortController()
    const onAbort = () => openController.abort(signal.reason)
    signal.addEventListener('abort', onAbort, { once: true })
    const openTimer = setTimeout(
      () => openController.abort(new Error('The plugin route stream open timed out.')),
      PLUGIN_STREAM_OPEN_TIMEOUT_MS,
    )
    openTimer.unref?.()
    let response: Response
    try {
      response = await this.dispatch(params.route, openController.signal)
    } catch (error) {
      this.logger?.warn('plugin route stream failed', {
        route: params.route,
        durationMs: Math.round(performance.now() - startedAt),
      })
      throw error instanceof RpcError ? error : new RpcError('INTERNAL_ERROR', 'The local plugin route stream is unavailable.')
    } finally {
      clearTimeout(openTimer)
      signal.removeEventListener('abort', onAbort)
    }

    if (response.status === 401 || response.status === 403) {
      await cancelBody(response)
      throw new RpcError('PERMISSION_DENIED', 'The Host local plugin route refused the stream.')
    }
    if (response.status === 404) {
      await cancelBody(response)
      throw new RpcError('METHOD_NOT_ALLOWED', 'The Host has no DS Bot feed route.')
    }
    if (response.status === 503) {
      await cancelBody(response)
      throw new RpcError('FEATURE_NOT_SUPPORTED', 'The Host local plugin route carrier is unavailable.')
    }
    const contentType = (response.headers.get('content-type') ?? '').toLowerCase()
    if (!response.ok || response.body === null || !contentType.startsWith('text/event-stream')) {
      await cancelBody(response)
      throw new RpcError('FAILED', 'The Host local plugin route did not open a DS Bot feed stream.')
    }

    this.logger?.debug('plugin route stream open', {
      route: params.route,
      durationMs: Math.round(performance.now() - startedAt),
    })
    return this.frames(response, signal)
  }

  private async *frames(response: Response, signal: AbortSignal): AsyncGenerator<SseEvent> {
    const body = response.body
    if (body === null) throw new RpcError('FAILED', 'The Host local plugin route returned no feed body.')
    const reader = body.getReader()
    const decoder = new TextDecoder()
    const onAbort = () => { void reader.cancel().catch(() => undefined) }
    signal.addEventListener('abort', onAbort, { once: true })
    let buffer = ''
    try {
      while (!signal.aborted) {
        const { value, done } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        for (;;) {
          const boundary = nextBoundary(buffer)
          if (boundary === undefined) break
          const raw = buffer.slice(0, boundary.index)
          buffer = buffer.slice(boundary.index + boundary.length)
          if (raw.length > MAX_SSE_EVENT_BYTES) {
            throw new RpcError('RESPONSE_TOO_LARGE', 'A local DS Bot feed event exceeds the remote limit.')
          }
          const event = parseSseEvent(raw)
          if (event !== undefined) yield event
        }
        if (buffer.length > MAX_SSE_EVENT_BYTES) {
          throw new RpcError('RESPONSE_TOO_LARGE', 'A local DS Bot feed event exceeds the remote limit.')
        }
      }
    } finally {
      signal.removeEventListener('abort', onAbort)
      await reader.cancel().catch(() => undefined)
    }
  }
}

async function cancelBody(response: Response): Promise<void> {
  await response.body?.cancel().catch(() => undefined)
}

function nextBoundary(buffer: string): { index: number; length: number } | undefined {
  let found: { index: number; length: number } | undefined
  for (const boundary of SSE_BOUNDARIES) {
    const index = buffer.indexOf(boundary)
    if (index < 0) continue
    if (found === undefined || index < found.index) found = { index, length: boundary.length }
  }
  return found
}

/** Parses one Server-Sent Events record; comments and non-data fields are ignored. */
function parseSseEvent(raw: string): SseEvent | undefined {
  let event: string | undefined
  const dataLines: string[] = []
  for (const line of raw.split(/\r\n|\r|\n/u)) {
    if (line === '' || line.startsWith(':')) continue
    const colon = line.indexOf(':')
    const field = colon < 0 ? line : line.slice(0, colon)
    let value = colon < 0 ? '' : line.slice(colon + 1)
    if (value.startsWith(' ')) value = value.slice(1)
    if (field === 'event') event = value
    else if (field === 'data') dataLines.push(value)
  }
  if (dataLines.length === 0) return undefined
  let data: unknown
  try {
    data = JSON.parse(dataLines.join('\n'))
  } catch {
    throw new RpcError('FAILED', 'The local DS Bot feed emitted a malformed event.')
  }
  return { event: event === undefined || event === '' ? 'message' : event, data }
}
