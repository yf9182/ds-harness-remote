import { compactHistory } from './compact-history.js'
import { createHash } from 'node:crypto'
import { TerminalCarrierCompat, usesDuplexTerminal } from './terminal-carrier-compat.js'
import type {
  HarnessApiTransferChunkParams,
  HarnessApiTransferCommitResult,
  HarnessApiTransferOpenParams,
  HarnessApiTransferReadParams,
  HarnessApiTransferReadResult,
  HarnessRemoteCallParams,
  HarnessRemoteFrameData,
  HarnessRemoteStreamClosedData,
  HarnessRemoteStreamOpenParams,
} from '@dsh-remote/protocol'
import {
  createEvent,
  encodeMessage,
  HARNESS_API_TRANSFER_CHUNK_BYTES,
  MAX_ACTIVE_TRANSFERS_PER_DIRECTION,
  MAX_ALPHA_STREAMS_PER_CONNECTION,
  MAX_HARNESS_API_TRANSFER_BYTES,
  MAX_SECURE_MESSAGE_BYTES,
  TRANSFER_IDLE_MS,
} from '@dsh-remote/protocol'
import { z } from 'zod'
import { TerminalPolicy, TERMINAL_CALLS, TERMINAL_STREAMS } from './terminal-policy.js'
import { harnessSessionGeneration } from './harness-version.js'
import type { SafeLogger } from './logging.js'
import { ensureHostWorkspaceDirectory, listRemoteDirectory } from './remote-directory-browser.js'
import { RpcError } from './rpc-router.js'
import type { LocalTypertGateway, TypertRpcResult } from './typert-gateway-contract.js'
import { CodexWorkspaceBridge } from './codex-workspace-bridge.js'
import type { PluginRouteStreamBridge } from './plugin-route-bridge.js'

type PublishRemoteFrame = (
  event: 'harness.remote.frame' | 'harness.remote.stream.closed',
  data: HarnessRemoteFrameData | HarnessRemoteStreamClosedData,
) => Promise<void>

interface ActiveRemoteStream {
  controller: AbortController
}

interface IncomingTransfer {
  totalBytes: number
  totalChunks: number
  chunks: Uint8Array[]
  receivedBytes: number
  touchedAt: number
}

interface OutgoingTransfer {
  bytes: Uint8Array
  totalChunks: number
  nextIndex: number
  touchedAt: number
}

interface DsBotSendIdempotencyEntry {
  expiresAt: number
  result?: TypertRpcResult
  pending?: Promise<TypertRpcResult>
}

const endpointSchema = z.string().min(1).max(128).regex(/^(?:\$events(?:\/result)?|[A-Za-z0-9_$.-]+\/[A-Za-z0-9_$.-]+)$/)
const callSchema = z.object({ endpoint: endpointSchema, payload: z.unknown() }).strict()
const streamOpenSchema = z.object({
  streamId: z.string().min(1).max(128),
  endpoint: endpointSchema,
  payload: z.unknown(),
}).strict()
const streamCloseSchema = z.object({ streamId: z.string().min(1).max(128) }).strict()
const dsBotCallPayloadSchema = z.object({ args: z.object({
  endpoint: z.string().min(1).max(128),
  payload: z.record(z.unknown()).nullable().optional(),
}).strict() }).strict()
const dsBotAnswerQuestionPayloadSchema = z.object({
  sessionId: z.string().min(1).max(256).refine(value => value.trim().length > 0),
  questionId: z.string().min(1).max(256).refine(value => value.trim().length > 0),
  answer: z.object({
    selected: z.array(z.string().min(1).max(256).refine(value => value.trim().length > 0)).max(6),
    custom: z.string().min(1).max(8_000).refine(value => value.trim().length > 0).optional(),
  }).strict(),
}).strict()
const dsBotApprovalAnswerPayloadSchema = z.object({
  id: z.string().min(1).max(256).refine(value => value.trim().length > 0),
  choice: z.enum(['allow', 'reject']),
}).strict()
const dsBotDismissQuestionPayloadSchema = z.object({
  sessionId: z.string().min(1).max(256).refine(value => value.trim().length > 0),
}).strict()
const dsBotRoomProgressPayloadSchema = z.object({
  roomId: z.string().min(1).max(256).refine(value => value.trim().length > 0),
}).strict()
const dsBotIdSchema = z.string().min(1).max(256).refine(value => value.trim().length > 0)
const dsBotOptionalTextSchema = z.string().max(12_000)
const dsBotMemberIdsSchema = z.array(dsBotIdSchema).max(64)
const dsBotCreateBotPayloadSchema = z.object({
  name: z.string().trim().min(1).max(200),
  role: z.string().max(4_000).optional(),
  color: z.string().max(64).optional(),
  instructions: dsBotOptionalTextSchema.optional(),
  brief: z.string().max(4_000).optional(),
  model: z.string().max(256).nullable().optional(),
  effort: z.string().max(128).optional(),
  workDir: z.string().max(4_096).optional(),
}).strict()
const dsBotUpdateBotPayloadSchema = z.object({
  id: dsBotIdSchema,
  name: z.string().trim().min(1).max(200).optional(),
  role: z.string().max(4_000).optional(),
  color: z.string().max(64).optional(),
  instructions: dsBotOptionalTextSchema.optional(),
  brief: z.string().max(4_000).optional(),
  model: z.string().max(256).nullable().optional(),
  effort: z.string().max(128).optional(),
  workDir: z.string().max(4_096).optional(),
}).strict()
const dsBotCreateRoomPayloadSchema = z.object({
  name: z.string().trim().min(1).max(200),
  members: dsBotMemberIdsSchema.min(2),
  admin: dsBotIdSchema.optional(),
  greet: z.boolean().optional(),
}).strict()
const dsBotUpdateRoomPayloadSchema = z.object({
  id: dsBotIdSchema,
  name: z.string().trim().min(1).max(200).optional(),
  notice: z.string().max(12_000).optional(),
  mode: z.string().max(64).optional(),
  admin: dsBotIdSchema.optional(),
  members: dsBotMemberIdsSchema.optional(),
  add: dsBotMemberIdsSchema.optional(),
  remove: dsBotMemberIdsSchema.optional(),
  workDir: z.string().max(4_096).optional(),
  permission: z.string().max(128).optional(),
  relayLimit: z.number().int().min(0).max(100).optional(),
}).strict()
const dsBotDeletePayloadSchema = z.object({ id: dsBotIdSchema }).strict()
const dsBotSetMainPayloadSchema = z.object({
  id: dsBotIdSchema,
  main: z.boolean().optional(),
  primary: z.boolean().optional(),
  replace: dsBotIdSchema.optional(),
}).strict()
const dsBotSetFlagsPayloadSchema = z.object({
  id: dsBotIdSchema,
  pinned: z.boolean().optional(),
  archived: z.boolean().optional(),
  hidden: z.boolean().optional(),
}).strict()
const dsBotStopMemberPayloadSchema = z.object({ roomId: dsBotIdSchema, botId: dsBotIdSchema }).strict()
const dsBotDuplicatePayloadSchema = z.object({ id: dsBotIdSchema }).strict()
const DS_BOT_INLINE_IMAGE_BYTES = 120 * 1024
const DS_BOT_INLINE_IMAGES_BYTES = 4 * 1024 * 1024
const DS_BOT_SEND_IMAGES = 4
const DS_BOT_SEND_IDEMPOTENCY_ENTRIES = 128
const DS_BOT_SEND_IDEMPOTENCY_TTL_MS = 5 * 60_000
const dsBotImageSchema = z.object({
  mime: z.enum(['image/png', 'image/jpeg', 'image/webp', 'image/gif']),
  data: z.string().min(4).max(Math.ceil(DS_BOT_INLINE_IMAGE_BYTES / 3) * 4),
}).strict()
const dsBotFileReferenceSchema = z.union([
  z.object({ transferId: z.string().uuid(), name: z.string().min(1).max(220), mime: z.string().min(1).max(128) }).strict(),
  z.object({ uploadId: z.string().uuid(), name: z.string().min(1).max(220), mime: z.string().min(1).max(128) }).strict(),
])
const dsBotSendPayloadSchema = z.object({
  sessionId: dsBotIdSchema,
  text: z.string().max(32_000),
  images: z.array(dsBotImageSchema).max(DS_BOT_SEND_IMAGES).optional(),
  file: dsBotFileReferenceSchema.optional(),
  opId: z.string().uuid().optional(),
}).strict().refine(value => value.text.trim().length > 0 || (value.images?.length ?? 0) > 0 || value.file !== undefined)
const dsBotViewReplyPayloadSchema = z.object({
  sessionId: dsBotIdSchema,
  viewId: dsBotIdSchema,
  action: z.string().min(1).max(128),
  values: z.record(z.unknown()).optional(),
}).strict()
const dsBotSimpleReadPayloadSchemas: Record<string, z.ZodTypeAny> = {
  hello: z.object({ api: z.array(z.number().int().min(1).max(64)).max(16).optional() }).strict(),
  state: z.object({
    locale: z.string().max(64).optional(), since: z.number().int().nonnegative().optional(),
    models: z.string().max(16_384).optional(), avatars: z.enum(['ref']).optional(),
  }).strict(),
  notices: z.object({ boot: z.string().max(256).optional(), since: z.number().int().nonnegative().optional() }).strict(),
  avatar: z.object({ botId: dsBotIdSchema }).strict(),
  chat: z.object({ botId: dsBotIdSchema, cursor: z.object({ part: z.number().int().nonnegative().optional(), beforeSeq: z.number().int().nonnegative().optional() }).strict().optional() }).strict(),
  'room-chat': z.object({ roomId: dsBotIdSchema, cursor: z.object({ beforeSeq: z.number().int().nonnegative() }).strict().optional() }).strict(),
  'room-progress': dsBotRoomProgressPayloadSchema,
  'usage-summary': z.object({ botId: dsBotIdSchema, timeZone: z.string().max(64).optional() }).strict(),
  'run-stats': z.object({ botId: dsBotIdSchema }).strict(),
  models: z.object({}).strict(),
  efforts: z.object({ ref: z.string().max(256) }).strict(),
  earlier: z.object({ sessionId: dsBotIdSchema, cursor: z.object({ part: z.number().int().nonnegative().optional(), beforeSeq: z.number().int().nonnegative().optional() }).strict().optional() }).strict(),
  exchange: z.object({ a: dsBotIdSchema, b: dsBotIdSchema }).strict(),
  usage: z.object({ timeZone: z.string().max(64).optional() }).strict(),
  schedules: z.object({ botId: dsBotIdSchema.optional(), roomId: dsBotIdSchema.optional() }).strict(),
  'create-bot': dsBotCreateBotPayloadSchema,
  'duplicate-bot': dsBotDuplicatePayloadSchema,
  'update-bot': dsBotUpdateBotPayloadSchema,
  'delete-bot': dsBotDeletePayloadSchema,
  'set-main': dsBotSetMainPayloadSchema,
  'set-flags': dsBotSetFlagsPayloadSchema,
  'create-room': dsBotCreateRoomPayloadSchema,
  'update-room': dsBotUpdateRoomPayloadSchema,
  'delete-room': dsBotDeletePayloadSchema,
  'stop-member-work': dsBotStopMemberPayloadSchema,
  send: dsBotSendPayloadSchema,
  'view-reply': dsBotViewReplyPayloadSchema,
  'answer-question': dsBotAnswerQuestionPayloadSchema,
  'dismiss-question': dsBotDismissQuestionPayloadSchema,
  'approval-answer': dsBotApprovalAnswerPayloadSchema,
}
const dsBotFeedPayloadSchema = z.object({ args: z.object({}).strict() }).strict()
const commandExecuteArgsSchema = z.object({
  agentId: z.string().min(1).max(128),
  line: z.string().min(1).max(2048),
  // dsh-commands <= 0.1.2 calls this field `images`; dsh-commands 0.1.5
  // renamed it to `submittedAttachments`. Only empty arrays are exposed by
  // the Remote bridge; command attachments are deliberately out of scope.
  images: z.array(z.never()).length(0).optional(),
  submittedAttachments: z.array(z.never()).length(0).optional(),
}).strict().superRefine((value, context) => {
  if (value.images !== undefined && value.submittedAttachments !== undefined) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'Use either images or submittedAttachments, not both.',
      path: ['submittedAttachments'],
    })
  }
})
const commandExecutePayloadSchema = z.object({ args: commandExecuteArgsSchema }).strict()
const transferOpenSchema = z.object({
  transferId: z.string().uuid(),
  totalBytes: z.number().int().positive().max(MAX_HARNESS_API_TRANSFER_BYTES),
  totalChunks: z.number().int().positive(),
}).strict()
const transferChunkSchema = z.object({
  transferId: z.string().uuid(),
  index: z.number().int().nonnegative(),
  data: z.string().min(1),
}).strict()
const transferIdSchema = z.object({ transferId: z.string().uuid() }).strict()
const transferReadSchema = z.object({
  transferId: z.string().uuid(),
  index: z.number().int().nonnegative(),
}).strict()
const directoryListSchema = z.object({
  path: z.string().min(1).max(4096).optional(),
}).strict()

const MAX_ACTIVE_STREAMS = MAX_ALPHA_STREAMS_PER_CONNECTION
const MAX_ACTIVE_TRANSFERS = MAX_ACTIVE_TRANSFERS_PER_DIRECTION
const INLINE_TRANSFER_RESPONSE_BYTES = 2 * 1024 * 1024
const DS_BOT_MOBILE_CALL_ENDPOINTS = new Set([
  'hello',
  'state',
  'models',
  'efforts',
  'earlier',
  'exchange',
  'usage',
  'run-stats',
  'schedules',
  'chat',
  'room-chat',
  'avatar',
  'notices',
  'send',
  'create-bot',
  'duplicate-bot',
  'update-bot',
  'delete-bot',
  'set-main',
  'set-flags',
  'create-room',
  'update-room',
  'delete-room',
  'stop-member-work',
  'view-reply',
  'answer-question',
  'dismiss-question',
  'approval-answer',
  'room-progress',
])

/** Fixed Typert Remote endpoints exposed to authenticated peers. */
export const HARNESS_REMOTE_ALLOWLIST = [
  '$events',
  '$events/result',
  'agentPresets/list',
  'agentPresets/read',
  'agentPresets/select',
  'commands/execute',
  'commands/list',
  'credentials/describe',
  'credentials/set',
  'credentials/unset',
  'directoryPicker/list',
  'dsBot/call',
  'dsBot/feed',
  'fileReferences/list',
  'goals/clear',
  'goals/complete',
  'goals/create',
  'goals/edit',
  'goals/pause',
  'goals/resume',
  'job/list',
  'job/follow',
  'llm/discoverModels',
  'llm/listConfigurableProviders',
  'llm/listProviders',
  'messageFeedback/delete',
  'messageFeedback/list',
  'messageFeedback/put',
  'pluginInventory/list',
  'pluginManager/listBundles',
  'pluginManager/listPlugins',
  'pluginManager/setBundleEnabled',
  'pluginManager/setPluginEnabled',
  'permissionPresets/catalog',
  'schedule/list',
  'schedule/catalog',
  'schedule/history',
  'schedule/update',
  'schedule/delete',
  'session/attachment',
  'session/cancel',
  'session/canOpenWorkspacePath',
  'session/control',
  'session/create',
  'session/follow',
  'session/fork',
  'session/list',
  'session/modelCatalog',
  'session/page',
  'session/prompt',
  'remoteAccess/follow',
  'session/projections',
  'session/initializeDefaultModel',
  'session/workspacePathApplications',
  'session/rename',
  'session/search',
  'session/selectModel',
  'session/updateQueue',
  'sessionReferenceResolver/candidates',
  'settings/describe',
  'settings/mutate',
  'settings/replace',
  'settings/update',
  'skills/list',
  'subagents/interruptByParent',
  'subagents/list',
  'subagents/prompt',
  'userQuestions/answer',
  'userQuestions/attachWait',
  'claudeCodeMods/watchBand',
  'claudeCodeMods/pressBand',
  'workspaceFiles/list',
  'workspaceFiles/stat',
  'workspaceFiles/read',
  'workspaceFiles/readBytes',
  'workspaceFiles/readAll',
  'workspaceFiles/readRelated',
  'workspaceFiles/changes',
  'officeToPdf/render',
  'officeToPdf/generation',
  'workspace/archiveSession',
  'workspace/create',
  'workspace/delete',
  'workspace/follow',
  'workspace/insertBefore',
  'workspace/insertSessionBefore',
  'workspace/rename',
  'workspace/unarchiveSession',
  'workspace/initializeDefault',
  'workspace/pinSession',
  'workspace/unpinSession',
] as const

const allowedEndpoints = new Set<string>(HARNESS_REMOTE_ALLOWLIST)

/** Host-side adapter from the encrypted peer channel to the official alpha Gateway carrier. */
export class HarnessRemoteBridge {
  compactHistoryEnabled = false
  private readonly streams = new Map<string, ActiveRemoteStream>()
  private readonly incomingTransfers = new Map<string, IncomingTransfer>()
  private readonly outgoingTransfers = new Map<string, OutgoingTransfer>()
  private readonly dsBotSendIdempotency = new Map<string, DsBotSendIdempotencyEntry>()

  private readonly terminalCarrier: TerminalCarrierCompat | undefined

  constructor(
    private readonly gateway: LocalTypertGateway,
    private readonly publish: PublishRemoteFrame,
    private readonly logger?: SafeLogger,
    private readonly harnessVersion?: string,
    private readonly terminal = new TerminalPolicy(() => false, "", new Map()),
    private readonly codexWorkspace?: CodexWorkspaceBridge,
    /** Local DS Bot event-stream carrier; `dsBot/feed` has no Gateway provider. */
    private readonly pluginRouteStream?: PluginRouteStreamBridge,
  ) {
    this.terminalCarrier = usesDuplexTerminal(harnessVersion) ? new TerminalCarrierCompat(gateway) : undefined
  }

  async call(input: unknown): Promise<TypertRpcResult> {
    const params = callSchema.parse(input) as HarnessRemoteCallParams
    if (params.endpoint === 'dsBot/call') {
      const dsBotCall = dsBotCallPayloadSchema.safeParse(params.payload)
      if (dsBotCall.success && dsBotCall.data.args.endpoint === 'send') {
        this.assertDsBotCallPayload(params.payload)
        const send = dsBotSendPayloadSchema.parse(dsBotCall.data.args.payload ?? {})
        if (send.opId !== undefined) return this.callIdempotent(send.opId, () => this.callCore(input))
      }
    }
    return this.callCore(input)
  }

  private async callCore(input: unknown): Promise<TypertRpcResult> {
    const params = callSchema.parse(input) as HarnessRemoteCallParams
    this.assertAllowed(params.endpoint)
    if (params.endpoint === 'dsBot/feed') throw new RpcError('METHOD_NOT_ALLOWED', 'Use a stream for this DS Bot endpoint.')
    if (params.endpoint === 'dsBot/call') this.assertDsBotCallPayload(params.payload)
    if (TERMINAL_STREAMS.has(params.endpoint)) throw new RpcError('METHOD_NOT_ALLOWED', 'Use a stream for this terminal endpoint.')
    if (['settings/update', 'settings/replace', 'settings/mutate'].includes(params.endpoint)) {
      const args = requestArgs(params.payload)
      if (args.ns === 'ds-harness-remote' || args.ns === 'dsh-remote') {
        throw new RpcError('PERMISSION_DENIED', 'Remote access settings can only be changed locally on the Host.')
      }
    }
    await this.checkPluginManagement(params.endpoint, params.payload)
    const reservation = params.endpoint.startsWith('terminal/') ? this.terminal.check(params.endpoint, params.payload) : undefined
    let codexResult: TypertRpcResult | undefined
    if (this.codexWorkspace !== undefined) {
      try {
        codexResult = await this.codexWorkspace.call(params.endpoint, params.payload, AbortSignal.timeout(60_000))
      } catch (error) {
        // Release failed allocations only after their process cleanup succeeds.
        if (reservation !== undefined) {
          this.terminal.result(params.endpoint, params.payload, { ok: false, error: { code: error instanceof RpcError ? error.code : 'FAILED', message: '', details: {} } }, reservation)
        }
        throw error
      }
    }
    if (codexResult !== undefined) return reservation === undefined ? codexResult : this.terminal.result(params.endpoint, params.payload, codexResult, reservation)
    if (params.endpoint === 'session/canOpenWorkspacePath') {
      return { ok: true, value: true }
    }
    if (params.endpoint === 'workspace/create') {
      const request = requestArgs(params.payload)
      const folder = typeof request.path === 'string' ? request.path : ''
      await ensureHostWorkspaceDirectory(folder)
    }
    const startedAt = performance.now()
    const signal = AbortSignal.timeout(60_000)
    try {
      const nativePayload = normalizeWorkspaceRequestPayload(params.endpoint, params.payload, this.harnessVersion)
      const nativeResult = params.endpoint === 'commands/execute'
        ? await dispatchCommandForHost(this.gateway, params.payload, signal, this.harnessVersion)
        : this.terminalCarrier !== undefined && (params.endpoint === 'terminal/write' || params.endpoint === 'terminal/resize')
          ? await this.terminalCarrier.call(params.endpoint, nativePayload, signal)
          : await this.gateway.dispatch(params.endpoint, nativePayload, signal)
      const result = params.endpoint === 'directoryPicker/list' && needsDirectoryFallback(nativeResult)
        ? await this.directoryList(params.payload, signal)
        : filterRemotePluginInventory(params.endpoint, nativeResult)
      this.logger?.debug('harness remote call ok', {
        endpoint: params.endpoint,
        durationMs: Math.round(performance.now() - startedAt),
      })
      // The native 0.2.x workspace APIs return binary payloads as Uint8Array.
      // Remote calls cross a JSON-only encrypted frame, so keep the wire
      // contract stable by converting only the two bounded preview responses
      // to canonical base64. (Codex/ACP workspace bridges already return the
      // wire shape and are left unchanged.)
      const wireResult = normalizeWorkspaceBinaryResult(params.endpoint,
        this.compactHistoryEnabled && ['session/page', 'session/history', 'subagent/history'].includes(params.endpoint)
          ? compactHistory(result) : result)
      return reservation === undefined ? wireResult : this.terminal.result(params.endpoint, params.payload, wireResult, reservation)
    } catch (error) {
      if (reservation !== undefined) this.terminal.result(params.endpoint, params.payload, { ok: false, error: { code: 'FAILED', message: '', details: {} } }, reservation)
      if (params.endpoint === 'directoryPicker/list') {
        const result = await this.directoryList(params.payload, signal)
        this.logger?.debug('harness remote call ok', {
          endpoint: params.endpoint,
          durationMs: Math.round(performance.now() - startedAt),
          fallback: 'directory-browser',
        })
        return result
      }
      this.logger?.warn('harness remote call failed', {
        endpoint: params.endpoint,
        durationMs: Math.round(performance.now() - startedAt),
        code: this.gateway.failure(error).code,
      })
      throw error
    }
  }

  private async callIdempotent(opId: string, execute: () => Promise<TypertRpcResult>): Promise<TypertRpcResult> {
    const now = Date.now()
    for (const [key, entry] of this.dsBotSendIdempotency) {
      if (entry.expiresAt <= now) this.dsBotSendIdempotency.delete(key)
    }
    const existing = this.dsBotSendIdempotency.get(opId)
    if (existing !== undefined) {
      this.dsBotSendIdempotency.delete(opId)
      this.dsBotSendIdempotency.set(opId, existing)
      if (existing.result !== undefined) return existing.result
      if (existing.pending !== undefined) return existing.pending
    }

    const entry: DsBotSendIdempotencyEntry = { expiresAt: now + DS_BOT_SEND_IDEMPOTENCY_TTL_MS }
    const pending = execute().then(result => {
      entry.result = result
      entry.pending = undefined
      entry.expiresAt = Date.now() + DS_BOT_SEND_IDEMPOTENCY_TTL_MS
      return result
    }, error => {
      if (this.dsBotSendIdempotency.get(opId) === entry) this.dsBotSendIdempotency.delete(opId)
      throw error
    })
    entry.pending = pending
    this.dsBotSendIdempotency.set(opId, entry)
    while (this.dsBotSendIdempotency.size > DS_BOT_SEND_IDEMPOTENCY_ENTRIES) {
      const oldest = this.dsBotSendIdempotency.keys().next().value as string | undefined
      if (oldest === undefined) break
      this.dsBotSendIdempotency.delete(oldest)
    }
    return pending
  }

  private async directoryList(payload: unknown, signal: AbortSignal): Promise<TypertRpcResult> {
    const args = requestArgs(payload)
    const params = directoryListSchema.parse(args)
    return { ok: true, value: await listRemoteDirectory(params.path, signal) }
  }

  openTransfer(input: unknown): { opened: true; transferId: string } {
    this.pruneTransfers()
    const params = transferOpenSchema.parse(input) as HarnessApiTransferOpenParams
    if (params.totalChunks !== Math.ceil(params.totalBytes / HARNESS_API_TRANSFER_CHUNK_BYTES)) {
      throw new RpcError('INVALID_MESSAGE', 'The Harness Remote transfer chunk count is invalid.')
    }
    if (this.incomingTransfers.has(params.transferId) || this.outgoingTransfers.has(params.transferId)) {
      throw new RpcError('REQUEST_CONFLICT', 'The Harness Remote transfer id is already active.')
    }
    if (this.incomingTransfers.size >= MAX_ACTIVE_TRANSFERS) {
      throw new RpcError('RATE_LIMITED', 'Too many Harness Remote transfers are active.', undefined, true)
    }
    this.incomingTransfers.set(params.transferId, {
      totalBytes: params.totalBytes,
      totalChunks: params.totalChunks,
      chunks: [],
      receivedBytes: 0,
      touchedAt: Date.now(),
    })
    return { opened: true, transferId: params.transferId }
  }

  appendTransfer(input: unknown): { accepted: true; transferId: string; index: number } {
    this.pruneTransfers()
    const params = transferChunkSchema.parse(input) as HarnessApiTransferChunkParams
    const transfer = this.incomingTransfers.get(params.transferId)
    if (transfer === undefined) throw new RpcError('TRANSFER_NOT_FOUND', 'The Harness Remote transfer is not active.')
    if (params.index !== transfer.chunks.length || params.index >= transfer.totalChunks) {
      this.incomingTransfers.delete(params.transferId)
      throw new RpcError('INVALID_MESSAGE', 'Harness Remote transfer chunks must arrive exactly once and in order.')
    }
    const chunk = decodeCanonicalBase64(params.data)
    const expectedBytes = Math.min(
      HARNESS_API_TRANSFER_CHUNK_BYTES,
      transfer.totalBytes - params.index * HARNESS_API_TRANSFER_CHUNK_BYTES,
    )
    if (chunk.byteLength !== expectedBytes) {
      this.incomingTransfers.delete(params.transferId)
      throw new RpcError('INVALID_MESSAGE', 'The Harness Remote transfer chunk size is invalid.')
    }
    transfer.chunks.push(chunk)
    transfer.receivedBytes += chunk.byteLength
    transfer.touchedAt = Date.now()
    return { accepted: true, transferId: params.transferId, index: params.index }
  }

  async commitTransfer(input: unknown): Promise<HarnessApiTransferCommitResult> {
    this.pruneTransfers()
    const params = transferIdSchema.parse(input)
    const transfer = this.incomingTransfers.get(params.transferId)
    if (transfer === undefined) throw new RpcError('TRANSFER_NOT_FOUND', 'The Harness Remote transfer is not active.')
    this.incomingTransfers.delete(params.transferId)
    if (transfer.chunks.length !== transfer.totalChunks || transfer.receivedBytes !== transfer.totalBytes) {
      throw new RpcError('INVALID_MESSAGE', 'The Harness Remote transfer is incomplete.')
    }
    let request: unknown
    try {
      request = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(concatChunks(transfer.chunks, transfer.totalBytes)))
    } catch {
      throw new RpcError('INVALID_MESSAGE', 'The Harness Remote transfer does not contain a valid request.')
    }
    const response = await this.call(callSchema.parse(request))
    const responseBytes = new TextEncoder().encode(JSON.stringify(response))
    if (responseBytes.byteLength <= INLINE_TRANSFER_RESPONSE_BYTES) return { kind: 'inline', response }
    if (responseBytes.byteLength > MAX_HARNESS_API_TRANSFER_BYTES) {
      throw new RpcError('RESPONSE_TOO_LARGE', 'The Harness Remote response exceeds the bounded transfer limit.')
    }
    if (this.outgoingTransfers.size >= MAX_ACTIVE_TRANSFERS) {
      throw new RpcError('RATE_LIMITED', 'Too many Harness Remote response transfers are active.', undefined, true)
    }
    const totalChunks = Math.ceil(responseBytes.byteLength / HARNESS_API_TRANSFER_CHUNK_BYTES)
    this.outgoingTransfers.set(params.transferId, {
      bytes: responseBytes,
      totalChunks,
      nextIndex: 0,
      touchedAt: Date.now(),
    })
    return { kind: 'chunked', transferId: params.transferId, totalBytes: responseBytes.byteLength, totalChunks }
  }

  readTransfer(input: unknown): HarnessApiTransferReadResult {
    this.pruneTransfers()
    const params = transferReadSchema.parse(input) as HarnessApiTransferReadParams
    const transfer = this.outgoingTransfers.get(params.transferId)
    if (transfer === undefined) throw new RpcError('TRANSFER_NOT_FOUND', 'The Harness Remote response transfer is not active.')
    if (params.index !== transfer.nextIndex || params.index >= transfer.totalChunks) {
      this.outgoingTransfers.delete(params.transferId)
      throw new RpcError('INVALID_MESSAGE', 'Harness Remote response chunks must be read exactly once and in order.')
    }
    const start = params.index * HARNESS_API_TRANSFER_CHUNK_BYTES
    const end = Math.min(start + HARNESS_API_TRANSFER_CHUNK_BYTES, transfer.bytes.byteLength)
    transfer.nextIndex += 1
    transfer.touchedAt = Date.now()
    return {
      transferId: params.transferId,
      index: params.index,
      data: Buffer.from(transfer.bytes.subarray(start, end)).toString('base64'),
    }
  }

  closeTransfer(input: unknown): { closed: boolean; transferId: string } {
    const params = transferIdSchema.parse(input)
    const closed = this.incomingTransfers.delete(params.transferId) || this.outgoingTransfers.delete(params.transferId)
    return { closed, transferId: params.transferId }
  }

  async openStream(input: unknown): Promise<{ opened: true; streamId: string }> {
    const params = streamOpenSchema.parse(input) as HarnessRemoteStreamOpenParams
    this.assertAllowed(params.endpoint)
    if (params.endpoint === 'dsBot/call') throw new RpcError('METHOD_NOT_ALLOWED', 'This DS Bot endpoint is unary.')
    if (params.endpoint === 'dsBot/feed') this.assertDsBotFeedPayload(params.payload)
    if (params.endpoint.startsWith('pluginManager/')) throw new RpcError('METHOD_NOT_ALLOWED', 'Plugin management endpoints are not streams.')
    if (params.endpoint.startsWith('settings/')) throw new RpcError('METHOD_NOT_ALLOWED', 'Settings endpoints are not streams.')
    if (TERMINAL_CALLS.has(params.endpoint)) throw new RpcError('METHOD_NOT_ALLOWED', 'This terminal endpoint is not a stream.')
    if (TERMINAL_STREAMS.has(params.endpoint)) this.terminal.check(params.endpoint, params.payload)
    if (this.streams.has(params.streamId)) throw new RpcError('REQUEST_CONFLICT', 'The Harness Remote stream is already open.')
    if (this.streams.size >= MAX_ACTIVE_STREAMS) {
      throw new RpcError('RATE_LIMITED', 'Too many Harness Remote streams are open.', undefined, true)
    }
    const controller = new AbortController()
    this.streams.set(params.streamId, { controller })
    let source: AsyncIterable<unknown>
    try {
      // DS Bot exposes its live feed as the local Fetch route `GET api/bot/feed`
      // and registers no `dsBot` Typert Remote provider, so the official Gateway
      // cannot open `dsBot/feed`. Route it through the bounded plugin carrier and
      // keep the Gateway path as the fallback for Hosts that do register one.
      source = (this.pluginRouteStream !== undefined && params.endpoint === 'dsBot/feed'
        ? await this.pluginRouteStream.open({ route: 'api/bot/feed' }, controller.signal)
        : undefined)
        ?? (this.codexWorkspace === undefined ? undefined : await this.codexWorkspace.open(params.endpoint, params.payload, controller.signal))
        ?? await (params.endpoint === 'terminal/follow' && this.terminalCarrier !== undefined
          ? this.terminalCarrier.open(params.payload, controller.signal)
          : this.gateway.open(params.endpoint, normalizeWorkspaceChangesPayload(params.endpoint, params.payload, this.harnessVersion), controller.signal))
      controller.signal.throwIfAborted()
    } catch (error) {
      this.streams.delete(params.streamId)
      controller.abort()
      throw error
    }
    void this.pump(params.streamId, source, controller.signal, params.endpoint === 'session/follow')
    return { opened: true, streamId: params.streamId }
  }

  closeStream(input: unknown): { closed: boolean; streamId: string } {
    const params = streamCloseSchema.parse(input)
    const active = this.streams.get(params.streamId)
    if (active !== undefined) {
      this.streams.delete(params.streamId)
      active.controller.abort('client-closed')
    }
    return { closed: active !== undefined, streamId: params.streamId }
  }

  async closeAll(reason: HarnessRemoteStreamClosedData['reason'] = 'peer-disconnected'): Promise<void> {
    const streams = [...this.streams.entries()]
    this.streams.clear()
    this.incomingTransfers.clear()
    this.outgoingTransfers.clear()
    for (const [, stream] of streams) stream.controller.abort(reason)
    await Promise.all([this.gateway.dispose?.(), this.codexWorkspace?.closeAll()])
  }

  private async checkPluginManagement(endpoint: string, payload: unknown): Promise<void> {
    if (endpoint !== 'pluginManager/setPluginEnabled' && endpoint !== 'pluginManager/setBundleEnabled') return
    const plugin = endpoint === 'pluginManager/setPluginEnabled'
    const key = plugin ? 'id' : 'name'
    // Validate the actual Typert argument envelope; never authorize a nested alias
    // while forwarding different top-level arguments to the official Gateway.
    const parsed = z.object({ args: z.object({
      [key]: z.string().min(1).max(4096),
      enabled: z.boolean(),
    }).strict() }).strict().safeParse(payload)
    if (!parsed.success) throw new RpcError('INVALID_MESSAGE', 'Invalid plugin enablement request.')
    const args = parsed.data.args
    const result = await this.gateway.dispatch(plugin ? 'pluginManager/listPlugins' : 'pluginManager/listBundles', { args: {} }, AbortSignal.timeout(60_000))
    if (!result.ok || !Array.isArray(result.value)) throw new RpcError('PERMISSION_DENIED', 'Cannot verify the Host plugin inventory.')
    const row = result.value.find(value => isRecord(value) && value[plugin ? 'entryId' : 'name'] === args[key])
    if (!isRecord(row) || row.readOnlyReason !== undefined) throw new RpcError('PERMISSION_DENIED', 'The Host plugin cannot be changed remotely.')
    const remoteModule = (value: unknown): boolean => typeof value === 'string'
      && /^(?:ds-harness-remote|dsh-remote)(?:\/|$)/.test(value)
    if (remoteModule(plugin ? row.moduleName : row.name)
      || (!plugin && (!Array.isArray(row.rows) || row.rows.some(value => !isRecord(value) || remoteModule(value.moduleName))
        || (Array.isArray(row.overrides) && row.overrides.some(value => remoteModule(value) || value === 'ds-harness-remote-tui'))))) {
      throw new RpcError('PERMISSION_DENIED', 'The Remote plugin can only be enabled or disabled locally on the Host.')
    }
  }

  private assertDsBotCallPayload(payload: unknown): void {
    const parsed = dsBotCallPayloadSchema.safeParse(payload)
    if (!parsed.success) throw new RpcError('INVALID_MESSAGE', 'Invalid DS Bot Remote call.')
    if (!DS_BOT_MOBILE_CALL_ENDPOINTS.has(parsed.data.args.endpoint)) {
      throw new RpcError('METHOD_NOT_ALLOWED', 'The requested DS Bot mobile endpoint is not allowed.')
    }
    const schema = dsBotSimpleReadPayloadSchemas[parsed.data.args.endpoint]
    const innerPayload = parsed.data.args.payload ?? {}
    const validated = schema?.safeParse(innerPayload)
    if (schema === undefined || validated === undefined || !validated.success) {
      throw new RpcError('INVALID_MESSAGE', `Invalid DS Bot ${parsed.data.args.endpoint} request.`)
    }
    if (parsed.data.args.endpoint === 'send') {
      const send = validated.data as z.infer<typeof dsBotSendPayloadSchema>
      let totalBytes = 0
      const images = (send.images ?? []).map(image => {
        const bytes = Buffer.from(image.data, 'base64')
        if (bytes.byteLength === 0 || bytes.byteLength > DS_BOT_INLINE_IMAGE_BYTES || bytes.toString('base64') !== image.data) {
          throw new RpcError('INVALID_MESSAGE', 'Invalid DS Bot inline image.')
        }
        totalBytes += bytes.byteLength
        return { mime: image.mime, size: bytes.byteLength, sha: createHash('sha256').update(bytes).digest('hex') }
      })
      if (totalBytes > DS_BOT_INLINE_IMAGES_BYTES) throw new RpcError('INVALID_MESSAGE', 'DS Bot inline images exceed the combined size limit.')
      if (images.length > 0) this.logger?.debug('DS Bot inline images validated', { images })
    }
  }

  private assertDsBotFeedPayload(payload: unknown): void {
    if (!dsBotFeedPayloadSchema.safeParse(payload).success) {
      throw new RpcError('INVALID_MESSAGE', 'Invalid DS Bot Remote feed request.')
    }
  }

  private assertAllowed(endpoint: string): void {
    if (!allowedEndpoints.has(endpoint) && !TERMINAL_CALLS.has(endpoint) && !TERMINAL_STREAMS.has(endpoint)) {
      throw new RpcError('METHOD_NOT_ALLOWED', 'The requested Harness Remote endpoint is not allowed.')
    }
    if (endpoint === '$events' && !this.gateway.supportsCarrier) {
      throw new RpcError('FEATURE_NOT_SUPPORTED', 'This Harness version does not support Remote event transport.')
    }
  }

  private async pump(streamId: string, source: AsyncIterable<unknown>, signal: AbortSignal, historyFollow = false): Promise<void> {
    let reason: HarnessRemoteStreamClosedData['reason'] = 'completed'
    let failure: HarnessRemoteStreamClosedData['failure']
    try {
      for await (const value of source) {
        if (signal.aborted) break
        const frame: HarnessRemoteFrameData = {
          streamId,
          hasValue: true,
          ...(value === undefined ? {} : { value: historyFollow && this.compactHistoryEnabled && isCompactSnapshot(value) ? compactHistory(value) : value }),
        }
        if (encodeMessage(createEvent('harness.remote.frame', frame)).byteLength > MAX_SECURE_MESSAGE_BYTES) {
          throw new Error('Oversized Harness Remote frame')
        }
        await this.publish('harness.remote.frame', frame)
      }
      if (signal.aborted) reason = 'cancelled'
    } catch (error) {
      reason = signal.aborted ? 'cancelled' : 'failed'
      if (!signal.aborted) failure = this.gateway.failure(error)
    } finally {
      this.streams.delete(streamId)
      await this.publish('harness.remote.stream.closed', {
        streamId,
        reason,
        ...(failure === undefined ? {} : { failure }),
      }).catch(() => undefined)
    }
  }

  private pruneTransfers(): void {
    const cutoff = Date.now() - TRANSFER_IDLE_MS
    for (const [id, transfer] of this.incomingTransfers) if (transfer.touchedAt < cutoff) this.incomingTransfers.delete(id)
    for (const [id, transfer] of this.outgoingTransfers) if (transfer.touchedAt < cutoff) this.outgoingTransfers.delete(id)
  }
}

/**
 * DSH 0.1.7 changed workspaceFiles/changes from a Session-wide feed to a
 * target-scoped feed that requires `path`. Older Web/Client bundles still
 * open the Session-wide form. Keep that request working on a 0.1.7 Host by
 * watching the workspace root; the legacy client continues filtering frames
 * by the file it has opened. Do not add the field to older Hosts, whose
 * generated Typert schema rejects unknown arguments.
 */
function normalizeWorkspaceChangesPayload(endpoint: string, payload: unknown, harnessVersion?: string): unknown {
  if (endpoint !== 'workspaceFiles/changes' || !requiresWorkspaceChangePath(harnessVersion)) return payload
  if (!isRecord(payload) || !isRecord(payload.args) || Object.hasOwn(payload.args, 'path')) return payload
  return { ...payload, args: { ...payload.args, path: '.' } }
}

/**
 * DSH 0.1.7 nests the byte range under `options`, while older clients send
 * it directly on the request args. Keep the translation Host-version gated:
 * older generated descriptors reject the new `options` field.
 */
function normalizeWorkspaceRequestPayload(endpoint: string, payload: unknown, harnessVersion?: string): unknown {
  if (!isRecord(payload) || !isRecord(payload.args)) return payload
  const args = payload.args
  if (endpoint === 'workspaceFiles/readBytes' && requiresWorkspaceChangePath(harnessVersion)) {
    if (!Object.hasOwn(args, 'range') || Object.hasOwn(args, 'options')) return payload
    const { range, ...rest } = args
    return { ...payload, args: { ...rest, options: { range } } }
  }
  // DSH 0.2 removes the first-use naming request; accept a legacy client
  // payload while keeping the generated 0.2 endpoint's zero-argument shape.
  if (endpoint === 'workspace/initializeDefault' && isDshV02OrNewer(harnessVersion) && Object.hasOwn(args, 'request')) {
    const { request: _request, ...rest } = args
    return { ...payload, args: rest }
  }
  return payload
}

function isDshV02OrNewer(version: string | undefined): boolean {
  const match = /^(?:dsh-)?v?(\d+)\.(\d+)\.(\d+)(?:-|$)/u.exec(version?.trim() ?? '')
  if (match === null) return false
  return Number(match[1]) === 0 && Number(match[2]) >= 2
}

function requiresWorkspaceChangePath(version: string | undefined): boolean {
  const match = /^(?:dsh-)?v?(\d+)\.(\d+)\.(\d+)(?:-|$)/u.exec(version?.trim() ?? '')
  if (match === null) return false
  return Number(match[1]) === 0
    && ((Number(match[2]) === 1 && Number(match[3]) >= 7) || Number(match[2]) === 2)
}

async function dispatchCommandForHost(
  gateway: LocalTypertGateway,
  payload: unknown,
  signal: AbortSignal,
  harnessVersion?: string,
): Promise<TypertRpcResult> {
  const parsed = commandExecutePayloadSchema.parse(payload)
  const legacyPayload = {
    args: {
      agentId: parsed.args.agentId,
      line: parsed.args.line,
      images: parsed.args.images ?? [],
    },
  }
  const currentPayload = {
    args: {
      agentId: parsed.args.agentId,
      line: parsed.args.line,
      submittedAttachments: parsed.args.submittedAttachments ?? [],
    },
  }
  const args = harnessSessionGeneration(harnessVersion) === 'v3' ? currentPayload : legacyPayload
  return gateway.dispatch(
    'commands/execute',
    args,
    signal,
  )
}

function decodeCanonicalBase64(value: string): Uint8Array {
  const bytes = Buffer.from(value, 'base64')
  if (bytes.toString('base64') !== value) throw new RpcError('INVALID_MESSAGE', 'The Harness Remote transfer chunk is invalid.')
  return bytes
}

function normalizeWorkspaceBinaryResult(endpoint: string, result: TypertRpcResult): TypertRpcResult {
  if (result.ok !== true || (endpoint !== 'workspaceFiles/readBytes' && endpoint !== 'officeToPdf/render')) return result
  if (!isJsonRecord(result.value)) return result
  const directBytes = isBinary(result.value.data) ? result.value.data : undefined
  const attachmentBytes = directBytes === undefined ? attachmentAt(result, ['data']) : undefined
  if (directBytes === undefined && attachmentBytes === undefined) return result
  const bytes = directBytes ?? attachmentBytes!
  return {
    ok: true,
    value: {
      ...result.value,
      data: Buffer.from(bytes).toString('base64'),
    },
  }
}

function attachmentAt(result: TypertRpcResult, path: readonly string[]): Uint8Array | undefined {
  const attachments = (result as TypertRpcResult & { attachments?: unknown }).attachments
  if (!Array.isArray(attachments)) return undefined
  const attachment = attachments.find(item => {
    if (!isJsonRecord(item) || !Array.isArray(item.path) || item.path.length !== path.length) return false
    return item.path.every((part, index) => part === path[index])
  })
  return isJsonRecord(attachment) && isBinary(attachment.bytes) ? attachment.bytes : undefined
}

function isBinary(value: unknown): value is Uint8Array {
  return value instanceof Uint8Array
}

function isJsonRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function concatChunks(chunks: readonly Uint8Array[], totalBytes: number): Uint8Array {
  const result = new Uint8Array(totalBytes)
  let offset = 0
  for (const chunk of chunks) {
    result.set(chunk, offset)
    offset += chunk.byteLength
  }
  return result
}

function needsDirectoryFallback(result: TypertRpcResult): boolean {
  if (result.ok) return false
  const code = result.error.code.toLocaleLowerCase()
  const message = result.error.message.toLocaleLowerCase()
  return code.includes('capability')
    || code === 'directory-picker/unavailable'
    || code === 'directory-picker-unavailable'
    || message.includes('browse capability')
    || message.includes('browser capability')
    || message.includes('brower capability')
    || message.includes('directory-picker-unavailable')
}

/** Hide this transport's own controls only in the remote inventory projection. */
function filterRemotePluginInventory(endpoint: string, result: TypertRpcResult): TypertRpcResult {
  if (!result.ok) return result
  const isRemote = (value: unknown): boolean => typeof value === 'string'
    && /^(?:ds-harness-remote|dsh-remote)(?:\/|$)/.test(value)
  const filterEntries = (entries: unknown[]): unknown[] => entries.filter(entry =>
    !isRecord(entry) || !isRemote(entry.moduleName))
  if (endpoint === 'pluginManager/listPlugins' && Array.isArray(result.value)) {
    return { ...result, value: filterEntries(result.value) }
  }
  if (endpoint === 'pluginManager/listBundles' && Array.isArray(result.value)) {
    return { ...result, value: result.value
      .filter(bundle => !isRecord(bundle) || !isRemote(bundle.name))
      .map(bundle => isRecord(bundle) && Array.isArray(bundle.rows)
        ? { ...bundle, rows: filterEntries(bundle.rows) } : bundle) }
  }
  if (endpoint === 'pluginInventory/list' && isRecord(result.value) && Array.isArray(result.value.entries)) {
    return { ...result, value: { ...result.value, entries: filterEntries(result.value.entries) } }
  }
  return result
}

function requestArgs(payload: unknown): Record<string, unknown> {
  const root = record(payload)
  const args = isRecord(root.args) ? root.args : root
  return record(args.request ?? args._request ?? args)
}

function record(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {}
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isCompactSnapshot(value: unknown): boolean {
  return value !== null && typeof value === 'object' && (value as { type?: unknown }).type === 'snapshot'
}
