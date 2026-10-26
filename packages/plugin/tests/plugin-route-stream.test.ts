import { describe, expect, it, vi } from 'vitest'
import {
  PLUGIN_ROUTE_STREAM_ALLOWLIST,
  PluginRouteStreamBridge,
  type PluginRouteStreamDispatcher,
} from '../src/plugin-route-bridge.js'
import { RpcError } from '../src/safe-error.js'
import { runScenarios } from '../../../scripts/test-scenarios.mjs'

const encoder = new TextEncoder()

/** Builds a streaming SSE response whose body yields the given chunks in order. */
function sseResponse(chunks: string[], init: ResponseInit = {}): Response {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk))
      controller.close()
    },
  })
  return new Response(body, {
    status: 200,
    headers: { 'content-type': 'text/event-stream; charset=utf-8' },
    ...init,
  })
}

function dispatcher(response: Response | (() => Promise<Response>)): PluginRouteStreamDispatcher {
  return vi.fn(async () => (typeof response === 'function' ? await response() : response))
}

async function collect(stream: AsyncIterable<unknown>): Promise<unknown[]> {
  const values: unknown[] = []
  for await (const value of stream) values.push(value)
  return values
}

describe('PluginRouteStreamBridge', () => {
  it('forwards the fixed DS Bot feed route and yields {event,data} frames', async () => {
    const dispatch = dispatcher(sseResponse([
      'event: hello\ndata: {"revision":743}\n\n',
      'event: stream\ndata: {"botId":"b1","sessionId":"s1","attemptId":"a1","phase":"start"}\n\n',
      'event: stream\ndata: {"botId":"b1","attemptId":"a1","text":"你好"}\n\n',
      'event: stream\ndata: {"botId":"b1","attemptId":"a1","phase":"end"}\n\n',
    ]))
    const bridge = new PluginRouteStreamBridge(dispatch)
    const events = await collect(await bridge.open({ route: 'api/bot/feed' }, new AbortController().signal))

    expect(dispatch).toHaveBeenCalledWith('api/bot/feed', expect.any(AbortSignal))
    expect(events).toEqual([
      { event: 'hello', data: { revision: 743 } },
      { event: 'stream', data: { botId: 'b1', sessionId: 's1', attemptId: 'a1', phase: 'start' } },
      { event: 'stream', data: { botId: 'b1', attemptId: 'a1', text: '你好' } },
      { event: 'stream', data: { botId: 'b1', attemptId: 'a1', phase: 'end' } },
    ])
  })

  it('keeps frames whole across arbitrary chunk boundaries, CRLF and keepalive comments', async () => {
    const bridge = new PluginRouteStreamBridge(dispatcher(sseResponse([
      ': keepalive\r\n\r\n',
      'event: hello\r',
      '\ndata: {"revision":1}\r\n\r',
      '\nevent: stream\ndata: {"bo',
      'tId":"b1","text":"ab"}\n\n',
      'event: stream\ndata: {"botId":"b1","text":"c"}\n',
      '\n',
    ])))
    const events = await collect(await bridge.open({ route: 'api/bot/feed' }, new AbortController().signal))
    expect(events).toEqual([
      { event: 'hello', data: { revision: 1 } },
      { event: 'stream', data: { botId: 'b1', text: 'ab' } },
      { event: 'stream', data: { botId: 'b1', text: 'c' } },
    ])
  })

  it('rejects routes outside the exact stream allowlist before dispatch', async () => {
    await runScenarios([
      'api/bot', 'api/bot/feed/', 'api/bot/feed?x=1', '/api/bot/feed', 'api/../api/bot/feed',
      'api/bot/feedback', 'api/%62ot/feed', 'api/bot/feed\n', '', 'http://evil/api/bot/feed',
    ].map(route => ({
      name: JSON.stringify(route),
      run: async () => {
        const dispatch = dispatcher(sseResponse(['event: hello\ndata: {}\n\n']))
        const bridge = new PluginRouteStreamBridge(dispatch)
        await expect(bridge.open({ route }, new AbortController().signal)).rejects.toMatchObject({ code: 'INVALID_MESSAGE' })
        expect(dispatch).not.toHaveBeenCalled()
      },
    })))
    expect(PLUGIN_ROUTE_STREAM_ALLOWLIST).toEqual(['api/bot/feed'])
  })

  it('maps local route failures to bounded Remote errors', async () => {
    await runScenarios([
      { name: 'unauthorized', status: 401, code: 'PERMISSION_DENIED' },
      { name: 'forbidden', status: 403, code: 'PERMISSION_DENIED' },
      { name: 'route missing', status: 404, code: 'METHOD_NOT_ALLOWED' },
      { name: 'carrier missing', status: 503, code: 'FEATURE_NOT_SUPPORTED' },
      { name: 'server error', status: 500, code: 'FAILED' },
    ].map(({ name, status, code }) => ({
      name,
      run: async () => {
        const response = new Response('nope', { status, headers: { 'content-type': 'text/plain' } })
        const bridge = new PluginRouteStreamBridge(dispatcher(response))
        await expect(bridge.open({ route: 'api/bot/feed' }, new AbortController().signal))
          .rejects.toMatchObject({ code })
      },
    })))
  })

  it('refuses a non-event-stream success body', async () => {
    const response = new Response('{"ok":true}', { status: 200, headers: { 'content-type': 'application/json' } })
    const bridge = new PluginRouteStreamBridge(dispatcher(response))
    await expect(bridge.open({ route: 'api/bot/feed' }, new AbortController().signal))
      .rejects.toMatchObject({ code: 'FAILED' })
  })

  it('fails a malformed frame instead of forwarding it', async () => {
    const bridge = new PluginRouteStreamBridge(dispatcher(sseResponse(['event: hello\ndata: {not json}\n\n'])))
    await expect(collect(await bridge.open({ route: 'api/bot/feed' }, new AbortController().signal)))
      .rejects.toMatchObject({ code: 'FAILED' })
  })

  it('bounds a single buffered SSE event', async () => {
    const huge = `event: stream\ndata: {"text":"${'x'.repeat(600 * 1024)}"}\n\n`
    const bridge = new PluginRouteStreamBridge(dispatcher(sseResponse([huge])))
    await expect(collect(await bridge.open({ route: 'api/bot/feed' }, new AbortController().signal)))
      .rejects.toMatchObject({ code: 'RESPONSE_TOO_LARGE' })
  })

  it('stops reading and cancels the local body when the Remote stream aborts', async () => {
    const controller = new AbortController()
    let cancelled = false
    const body = new ReadableStream<Uint8Array>({
      start(stream) {
        stream.enqueue(encoder.encode('event: hello\ndata: {"revision":1}\n\n'))
        // Keeps the stream open until the consumer cancels.
      },
      cancel() { cancelled = true },
    })
    const response = new Response(body, { status: 200, headers: { 'content-type': 'text/event-stream' } })
    const bridge = new PluginRouteStreamBridge(dispatcher(response))
    const events = await bridge.open({ route: 'api/bot/feed' }, controller.signal)
    const iterator = events[Symbol.asyncIterator]()
    await expect(iterator.next()).resolves.toEqual({ value: { event: 'hello', data: { revision: 1 } }, done: false })
    controller.abort('test-abort')
    await vi.waitFor(() => expect(cancelled).toBe(true))
    await iterator.return?.(undefined)
  })

  it('wraps a dispatcher failure as an internal error', async () => {
    const bridge = new PluginRouteStreamBridge(vi.fn(async () => { throw new Error('carrier exploded') }))
    await expect(bridge.open({ route: 'api/bot/feed' }, new AbortController().signal))
      .rejects.toMatchObject({ code: 'INTERNAL_ERROR' })
  })

  it('keeps RpcError codes raised by the dispatcher', async () => {
    const bridge = new PluginRouteStreamBridge(vi.fn(async () => {
      throw new RpcError('FEATURE_NOT_SUPPORTED', 'carrier missing')
    }))
    await expect(bridge.open({ route: 'api/bot/feed' }, new AbortController().signal))
      .rejects.toMatchObject({ code: 'FEATURE_NOT_SUPPORTED' })
  })
})
