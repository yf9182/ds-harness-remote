import { RpcError } from './safe-error.js'
import type { PluginRouteDispatcher, PluginRouteStreamDispatcher } from './plugin-route-bridge.js'

export interface PluginRouteConnectionLike {
  createSharedFetchHandler?(channel: '/api'): { fetch(request: Request, peer?: unknown): Promise<Response> }
  authenticatedUrl?(baseUrl: string): string
}

/** Resolve lazily: Connection may activate after the Remote plugin. */
export function createPluginRouteDispatcher(
  readConnection: () => PluginRouteConnectionLike | undefined,
  readPort: () => number | undefined,
  peer?: unknown,
): PluginRouteDispatcher {
  let cookie: string | undefined
  return async (route, body, signal) => {
    signal.throwIfAborted()
    const connection = readConnection()
    const handler = connection?.createSharedFetchHandler?.('/api')
    let response: Response
    if (handler !== undefined) {
      // The authenticated Remote channel has already admitted this peer. Invoke
      // the official local Fetch carrier, including on Desktop without HTTP.
      response = await withSignal(handler.fetch(new Request(`http://127.0.0.1/${route}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body ?? {}),
        signal,
      }), peer), signal)
    } else {
      const port = readPort()
      if (connection?.authenticatedUrl === undefined || typeof port !== 'number') {
        return { status: 503, json: { ok: false, error: { code: 'gateway/unavailable', message: 'The local plugin route carrier is unavailable.' } } }
      }
      const origin = `http://127.0.0.1:${port}`
      if (cookie === undefined) {
        const launch = await fetch(connection.authenticatedUrl(`${origin}/`), { redirect: 'manual', signal })
        const setCookie = typeof launch.headers.getSetCookie === 'function' ? launch.headers.getSetCookie() : []
        cookie = setCookie.map(item => item.split(';', 1)[0]).join('; ') || undefined
      }
      response = await fetch(`${origin}/${route}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...(cookie === undefined ? {} : { cookie }) },
        body: JSON.stringify(body ?? {}),
        signal,
      })
      if (response.status === 401) cookie = undefined
    }
    const text = await response.text()
    let json: unknown = null
    if (text !== '') {
      try { json = JSON.parse(text) } catch { json = text }
    }
    return { status: response.status, json }
  }
}

function withSignal<T>(pending: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(signal.reason)
    signal.addEventListener('abort', onAbort, { once: true })
    if (signal.aborted) onAbort()
    void pending.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort))
  })
}

/**
 * Opens one allowlisted local plugin event stream (`GET api/bot/feed`) through
 * the same authenticated carrier as `createPluginRouteDispatcher`. Streams
 * cannot go through the official Gateway because DS Bot registers the feed as
 * a local Fetch route rather than a Typert Remote method.
 */
export function createPluginRouteStreamDispatcher(
  readConnection: () => PluginRouteConnectionLike | undefined,
  readPort: () => number | undefined,
  peer?: unknown,
): PluginRouteStreamDispatcher {
  let cookie: string | undefined
  return async (route, signal) => {
    signal.throwIfAborted()
    const connection = readConnection()
    const handler = connection?.createSharedFetchHandler?.('/api')
    if (handler !== undefined) {
      return handler.fetch(new Request(`http://127.0.0.1/${route}`, {
        method: 'GET',
        headers: { accept: 'text/event-stream' },
        signal,
      }), peer)
    }
    const port = readPort()
    if (connection?.authenticatedUrl === undefined || typeof port !== 'number') {
      throw new RpcError('FEATURE_NOT_SUPPORTED', 'The local plugin route carrier is unavailable.')
    }
    const origin = `http://127.0.0.1:${port}`
    const open = async (): Promise<Response> => fetch(`${origin}/${route}`, {
      method: 'GET',
      headers: { accept: 'text/event-stream', ...(cookie === undefined ? {} : { cookie }) },
      signal,
      redirect: 'manual',
    })
    if (cookie === undefined) {
      const launch = await fetch(connection.authenticatedUrl(`${origin}/`), { redirect: 'manual', signal })
      const setCookie = typeof launch.headers.getSetCookie === 'function' ? launch.headers.getSetCookie() : []
      cookie = setCookie.map(item => item.split(';', 1)[0]).join('; ') || undefined
    }
    let response = await open()
    if (response.status === 401) {
      await response.body?.cancel().catch(() => undefined)
      cookie = undefined
      const launch = await fetch(connection.authenticatedUrl(`${origin}/`), { redirect: 'manual', signal })
      const setCookie = typeof launch.headers.getSetCookie === 'function' ? launch.headers.getSetCookie() : []
      cookie = setCookie.map(item => item.split(';', 1)[0]).join('; ') || undefined
      response = await open()
    }
    return response
  }
}
