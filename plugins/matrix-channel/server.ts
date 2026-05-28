#!/usr/bin/env bun

import { existsSync, readFileSync, mkdirSync, writeFileSync, unlinkSync, rmdirSync } from 'node:fs'
import { join, basename } from 'node:path'
import { homedir } from 'node:os'
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from '@modelcontextprotocol/sdk/types.js'
import { z } from 'zod'
import {
  tryAcquireLock, MuxServer, MuxClient,
  eventToFrame, frameToEvent,
  reactionToFrame, frameToReaction,
  loadSyncToken, saveSyncToken,
  type WireFrame,
} from './mux'

// ── Types ──────────────────────────────────────────────

export interface Config {
  homeserverUrl: string
  accessToken: string
  botUserId: string
  roomIds: string[] | null
  threadProject: string | null
  threadRootRoomId: string | null
}

export type ReplyToMode = 'first' | 'all' | 'off'

export interface Access {
  allowedUsers: string[]
  ackReaction: string | null
  maxImageSize: number
  replyToMode: ReplyToMode
}

// ── Config ─────────────────────────────────────────────

export const CHANNELS_DIR = process.env.MATRIX_CHANNELS_DIR
  ?? join(process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'), 'channels', 'matrix')
export const DEFAULT_MAX_IMAGE_SIZE = parseMaxImageSize(process.env.MATRIX_MAX_IMAGE_SIZE)

// ── Cleanup Registry ──────────────────────────────────
const cleanupFns: (() => void)[] = []

function registerCleanup(fn: () => void): void {
  cleanupFns.push(fn)
}

function runAllCleanup(): void {
  for (const fn of cleanupFns) {
    try { fn() } catch {}
  }
}

function parseMaxImageSize(raw: string | undefined): number {
  if (!raw) return 10 * 1024 * 1024
  const parsed = Number(raw)
  if (!Number.isFinite(parsed) || parsed <= 0) return 10 * 1024 * 1024
  return parsed
}

function requireEnv(name: string): string {
  const value = process.env[name]
  if (!value) {
    throw new Error(
      `${name} is not set. Run /matrix:configure to set up credentials, ` +
      `or set ${name} in ~/.claude/channels/matrix/.env`
    )
  }
  return value
}

export function loadConfig(envDir?: string): Config {
  // Load .env file if it exists (env vars take precedence)
  const envPath = join(envDir ?? CHANNELS_DIR, '.env')
  if (existsSync(envPath)) {
    const lines = readFileSync(envPath, 'utf-8').split('\n')
    for (const line of lines) {
      const trimmed = line.trim()
      if (!trimmed || trimmed.startsWith('#')) continue
      const eqIdx = trimmed.indexOf('=')
      if (eqIdx === -1) continue
      const key = trimmed.slice(0, eqIdx)
      const val = trimmed.slice(eqIdx + 1)
      if (!process.env[key]) process.env[key] = val
    }
  }

  let homeserverUrl = requireEnv('MATRIX_HOMESERVER_URL').replace(/\/+$/, '')
  if (!homeserverUrl.startsWith('https://') && !homeserverUrl.startsWith('http://')) {
    homeserverUrl = `https://${homeserverUrl}`
  }

  const rawRoomIds = process.env.MATRIX_ROOM_IDS?.trim()
  const roomIds = rawRoomIds ? rawRoomIds.split(',').map((id) => id.trim()).filter(Boolean) : null

  const threadsEnabled = process.env.MATRIX_THREADS === 'true'
  const threadRootRoomId = process.env.MATRIX_THREAD_ROOT_ROOM_ID?.trim() || null

  // Strict mode enforcement
  if (threadRootRoomId && !threadsEnabled) {
    throw new Error('MATRIX_THREAD_ROOT_ROOM_ID requires MATRIX_THREADS=true')
  }
  if (threadsEnabled && !threadRootRoomId) {
    throw new Error('MATRIX_THREADS=true requires MATRIX_THREAD_ROOT_ROOM_ID')
  }
  if (roomIds && threadRootRoomId) {
    throw new Error('MATRIX_ROOM_IDS and MATRIX_THREAD_ROOT_ROOM_ID are mutually exclusive')
  }

  const threadProject = threadsEnabled
    ? (process.env.MATRIX_THREAD_PROJECT || basename(process.cwd()))
    : null

  return {
    homeserverUrl,
    accessToken: requireEnv('MATRIX_ACCESS_TOKEN'),
    botUserId: requireEnv('MATRIX_BOT_USER_ID'),
    roomIds: threadRootRoomId ? [threadRootRoomId] : roomIds,
    threadProject,
    threadRootRoomId,
  }
}

// ── Access config ─────────────────────────────────────────────────

export function parseAccessJson(raw: string): Access {
  const data = JSON.parse(raw) as {
    allowedUsers?: unknown
    ackReaction?:  unknown
    maxImageSize?:  unknown
    replyToMode?:  unknown
  }

  const allowedUsers = Array.isArray(data.allowedUsers)
    ? data.allowedUsers.filter((u): u is string => typeof u === 'string')
    : []

  const ackReaction = typeof data.ackReaction === 'string' ? data.ackReaction : null

  const maxImageSize = typeof data.maxImageSize === 'number' ? data.maxImageSize : 10 * 1024 * 1024

  let replyToMode: ReplyToMode = 'first'
  if (data.replyToMode !== undefined) {
    if (data.replyToMode === 'first' || data.replyToMode === 'all' || data.replyToMode === 'off') {
      replyToMode = data.replyToMode
    } else {
      throw new Error(`invalid replyToMode: ${String(data.replyToMode)} (expected first|all|off)`)
    }
  }

  return { allowedUsers, ackReaction, maxImageSize, replyToMode }
}

export function loadAccess(path?: string): Access {
  const filePath = path ?? join(CHANNELS_DIR, 'access.json')
  if (!existsSync(filePath)) {
    return { allowedUsers: [], ackReaction: null, maxImageSize: DEFAULT_MAX_IMAGE_SIZE, replyToMode: 'first' }
  }
  try {
    return parseAccessJson(readFileSync(filePath, 'utf-8'))
  } catch (err) {
    console.error(`Failed to parse ${filePath}: ${err instanceof Error ? err.message : err}`)
    console.error('Falling back to default access config (no allowed users)')
    return { allowedUsers: [], ackReaction: null, maxImageSize: DEFAULT_MAX_IMAGE_SIZE, replyToMode: 'first' }
  }
}

// ── Thread Root Persistence ──────────────────────────────

/** Thread roots are stored as { "roomId:project": "$eventId" }. */
function threadKey(roomId: string, project: string): string {
  return `${roomId}:${project}`
}

export function loadThreadRoots(path?: string): Map<string, string> {
  const filePath = path ?? join(CHANNELS_DIR, 'threads.json')
  if (!existsSync(filePath)) return new Map()
  try {
    const raw = JSON.parse(readFileSync(filePath, 'utf-8'))
    return new Map(Object.entries(raw))
  } catch {
    return new Map()
  }
}

export function saveThreadRoot(
  pathOrUndefined: string | undefined,
  roomId: string,
  project: string,
  eventId: string,
): void {
  const filePath = pathOrUndefined ?? join(CHANNELS_DIR, 'threads.json')
  const dir = filePath.substring(0, filePath.lastIndexOf('/'))
  mkdirSync(dir, { recursive: true })
  const existing = loadThreadRoots(filePath)
  existing.set(threadKey(roomId, project), eventId)
  writeFileSync(filePath, JSON.stringify(Object.fromEntries(existing), null, 2))
}

// ── Permission Relay State ────────────────────────────

export const PERMISSION_TTL_MS = 60 * 60 * 1000 // 1 hour
export const ALLOW_EMOJI = '✅'
export const DENY_EMOJI = '❌'

export interface PendingPermission {
  requestId: string
  expiresAt: number
}

/** Maps the prompt-message eventId (the message the user reacts to) to the request_id. */
export const pendingPermissions = new Map<string, PendingPermission>()

/** Tracks the last room that produced a forwarded message — used as the default prompt room in room mode. */
export const lastActiveRoomState: { roomId: string | null } = { roomId: null }

export function expirePendingPermissions(now: number = Date.now()): void {
  for (const [eventId, entry] of pendingPermissions) {
    if (entry.expiresAt <= now) pendingPermissions.delete(eventId)
  }
}

export function classifyVerdict(emoji: string): 'allow' | 'deny' | null {
  if (emoji === ALLOW_EMOJI) return 'allow'
  if (emoji === DENY_EMOJI) return 'deny'
  return null
}

// ── Matrix CS API Helpers ──────────────────────────────

const SYNC_FILTER = JSON.stringify({
  room: {
    timeline: { types: ['m.room.message', 'm.reaction'], limit: 50 },
    state: { types: ['m.room.name', 'm.room.member'], lazy_load_members: true },
  },
  presence: { types: [] },
  account_data: { types: [] },
})

export interface BaseEvent {
  roomId: string
  roomName: string
  sender: string
  eventId: string
  threadRootId: string | null
}

export interface TextEvent extends BaseEvent {
  type: 'text'
  body: string
}

export interface ImageEvent extends BaseEvent {
  type: 'image'
  body: string
  mxcUrl: string
  mimeType: string
  size: number | null
  filename: string | null
}

export type SyncEvent = TextEvent | ImageEvent

export interface ReactionEvent {
  roomId: string
  roomName: string
  sender: string
  eventId: string
  targetEventId: string
  emoji: string
}

export interface SyncInvite {
  roomId: string
  inviter: string
}

let txnCounter = 0
export function nextTxnId(): string {
  return `m${Date.now()}.${txnCounter++}`
}

export function buildSyncUrl(homeserverUrl: string, since: string | null): string {
  const params = new URLSearchParams({ filter: SYNC_FILTER })
  if (since) {
    params.set('since', since)
    params.set('timeout', '30000')
  } else {
    params.set('timeout', '0')
  }
  return `${homeserverUrl}/_matrix/client/v3/sync?${params}`
}

/** Cache of room ID → human-readable name, persisted across sync batches. */
export const roomNameCache = new Map<string, string>()

export function parseSyncEvents(data: any): SyncEvent[] {
  const events: SyncEvent[] = []
  const joined = data.rooms?.join ?? {}

  for (const [roomId, room] of Object.entries<any>(joined)) {
    // Update cache if this batch carries a room name state event
    const nameEvent = (room.state?.events ?? []).find(
      (e: any) => e.type === 'm.room.name'
    )
    if (nameEvent?.content?.name) {
      roomNameCache.set(roomId, nameEvent.content.name)
    }
    const roomName: string = roomNameCache.get(roomId) ?? roomId

    for (const event of room.timeline?.events ?? []) {
      if (event.type !== 'm.room.message') continue

      const relatesTo = event.content?.['m.relates_to']
      const threadRootId = relatesTo?.rel_type === 'm.thread' ? relatesTo.event_id : null

      const msgtype = event.content?.msgtype
      if (msgtype === 'm.text') {
        events.push({
          type: 'text',
          roomId,
          roomName,
          sender: event.sender,
          eventId: event.event_id,
          threadRootId,
          body: event.content.body,
        })
      } else if (msgtype === 'm.image') {
        if (!event.content.url) {
          if (event.content.file) {
            console.error(`Skipping encrypted image (E2EE not supported) in ${roomId}`)
            events.push({
              type: 'text',
              roomId,
              roomName,
              sender: event.sender,
              eventId: event.event_id,
              threadRootId,
              body: `[Encrypted image not supported: ${event.content.body ?? 'image'}] This room uses E2EE — the plugin cannot decrypt media.`,
            })
          }
          continue
        }
        events.push({
          type: 'image',
          roomId,
          roomName,
          sender: event.sender,
          eventId: event.event_id,
          threadRootId,
          body: event.content.body ?? event.content.filename ?? 'image',
          mxcUrl: event.content.url,
          mimeType: event.content.info?.mimetype ?? 'application/octet-stream',
          size: event.content.info?.size ?? null,
          filename: event.content.filename ?? null,
        })
      }
    }
  }

  return events
}

export function parseSyncReactions(data: any): ReactionEvent[] {
  const reactions: ReactionEvent[] = []
  const joined = data.rooms?.join ?? {}

  for (const [roomId, room] of Object.entries<any>(joined)) {
    const roomName: string = roomNameCache.get(roomId) ?? roomId

    for (const event of room.timeline?.events ?? []) {
      if (event.type !== 'm.reaction') continue
      const relates = event.content?.['m.relates_to']
      if (!relates || relates.rel_type !== 'm.annotation') continue
      if (typeof relates.event_id !== 'string' || typeof relates.key !== 'string') continue

      reactions.push({
        roomId,
        roomName,
        sender: event.sender,
        eventId: event.event_id,
        targetEventId: relates.event_id,
        emoji: relates.key,
      })
    }
  }

  return reactions
}

export function parseSyncInvites(data: any): SyncInvite[] {
  const invites: SyncInvite[] = []
  const invited = data.rooms?.invite ?? {}

  for (const [roomId, room] of Object.entries<any>(invited)) {
    const memberEvent = (room.invite_state?.events ?? []).find(
      (e: any) => e.type === 'm.room.member' && e.content?.membership === 'invite'
    )
    invites.push({
      roomId,
      inviter: memberEvent?.sender ?? 'unknown',
    })
  }

  return invites
}

export function buildMessageBody(
  text: string,
  html: string | undefined,
  threadRootId?: string,
): Record<string, any> {
  // m.text, not m.notice — some Matrix clients filter or hide notice events,
  // which made bot replies "disappear" even though the homeserver accepted
  // them. Bot loop prevention is already handled by shouldForwardEvent
  // skipping events whose sender == botUserId, so the spec's rationale for
  // m.notice doesn't apply.
  const body: Record<string, any> = { msgtype: 'm.text', body: text }
  if (html) {
    body.format = 'org.matrix.custom.html'
    body.formatted_body = html
  }
  if (threadRootId) {
    body['m.relates_to'] = {
      rel_type: 'm.thread',
      event_id: threadRootId,
      is_falling_back: true,
      'm.in_reply_to': { event_id: threadRootId },
    }
  }
  return body
}

// ── Edit message body ─────────────────────────────────────────────
//
// Emits an m.replace event with m.new_content for in-place edits.
// When the original event was threaded, the replacement also carries
// an m.thread nested inside m.relates_to so older Element clients
// render the edit in the correct thread pane.

export interface BuildEditMessageBodyArgs {
  text:                  string
  html?:                 string
  eventId:               string
  originalThreadRootId?: string
}

export function buildEditMessageBody(args: BuildEditMessageBodyArgs): Record<string, unknown> {
  // Edits use m.notice so they do not push-notify the recipient — per the
  // matrix:threading skill, edits are for in-place progress updates and the
  // operator wakes the user with a fresh reply at the end. m.text on edits
  // made every interim progress edit ring a phone.
  const newContent: Record<string, unknown> = {
    msgtype: 'm.notice',
    body:    args.text,
  }
  if (args.html !== undefined) {
    newContent.format         = 'org.matrix.custom.html'
    newContent.formatted_body = args.html
  }

  const relatesTo: Record<string, unknown> = {
    rel_type: 'm.replace',
    event_id: args.eventId,
  }
  if (args.originalThreadRootId !== undefined) {
    relatesTo['m.thread'] = {
      event_id:        args.originalThreadRootId,
      is_falling_back: true,
      'm.in_reply_to': { event_id: args.originalThreadRootId },
    }
  }

  const body: Record<string, unknown> = {
    msgtype: 'm.notice',
    body:    '* ' + args.text,
    'm.new_content': newContent,
    'm.relates_to':  relatesTo,
  }
  if (args.html !== undefined) {
    body.format         = 'org.matrix.custom.html'
    body.formatted_body = '* ' + args.html
  }

  return body
}

// ── Edit message tool definition ──────────────────────────────────────────────

export const editMessageToolDefinition = {
  name: 'edit_message',
  description:
    'Edit a prior message authored by this bot. Use for in-place ' +
    'progress updates that should NOT push-notify the recipient. ' +
    'Always follow up with a final `reply` to wake the user when done. ' +
    'Carries both m.replace (the edit) and m.thread (the original ' +
    "thread membership, if any) so older clients render the edited " +
    'message in the same thread.',
  inputSchema: {
    type: 'object' as const,
    properties: {
      room_id:  { type: 'string', description: 'Matrix room ID' },
      event_id: { type: 'string', description: 'Event ID to edit. Must have been sent by this bot.' },
      text:     { type: 'string', description: 'New plain-text body.' },
      html:     { type: 'string', description: 'Optional new HTML body.' },
    },
    required: ['room_id', 'event_id', 'text'],
  },
} as const

// ── fetchEventForEdit ────────────────────────────────────────────────────────

export interface FetchEventForEditArgs {
  fetch:         typeof globalThis.fetch
  homeserverUrl: string
  accessToken:   string
  roomId:        string
  eventId:       string
}

export interface EventEditInfo {
  sender:        string
  threadRootId?: string
}

export async function fetchEventForEdit(args: FetchEventForEditArgs): Promise<EventEditInfo> {
  // Room IDs use ! and : which are valid unencoded in URI path segments;
  // encodeURIComponent would encode the colon, breaking Matrix convention.
  const encodedRoomId = args.roomId.replace(/[^!:.@a-zA-Z0-9_-]/g, encodeURIComponent)
  const url =
    args.homeserverUrl.replace(/\/+$/, '') +
    `/_matrix/client/v3/rooms/${encodedRoomId}` +
    `/event/${encodeURIComponent(args.eventId)}`

  const res = await args.fetch(url, {
    method:  'GET',
    headers: { Authorization: `Bearer ${args.accessToken}` },
  })
  if (!res.ok) {
    throw new Error(`fetchEventForEdit: HTTP ${res.status} on GET ${url}`)
  }

  const data = await res.json() as {
    sender: string
    content?: { 'm.relates_to'?: { rel_type?: string; event_id?: string } }
  }

  let threadRootId: string | undefined
  const relates = data.content?.['m.relates_to']
  if (relates?.rel_type === 'm.thread' && typeof relates.event_id === 'string') {
    threadRootId = relates.event_id
  }

  return { sender: data.sender, threadRootId }
}

export function buildReactionBody(eventId: string, emoji: string) {
  return {
    'm.relates_to': {
      rel_type: 'm.annotation',
      event_id: eventId,
      key: emoji,
    },
  }
}

export function buildThreadRootBody(project: string): Record<string, any> {
  return {
    msgtype: 'm.notice',
    body: `Thread: ${project}`,
    format: 'org.matrix.custom.html',
    formatted_body: `<b>Thread:</b> ${project}`,
  }
}

export function mxcToHttpUrl(homeserverUrl: string, mxcUrl: string): string {
  if (!mxcUrl.startsWith('mxc://')) {
    throw new Error(`Invalid MXC URL: ${mxcUrl}`)
  }
  const withoutScheme = mxcUrl.slice('mxc://'.length)
  const slashIdx = withoutScheme.indexOf('/')
  if (slashIdx <= 0 || slashIdx === withoutScheme.length - 1) {
    throw new Error(`Invalid MXC URL (missing server or media ID): ${mxcUrl}`)
  }
  const serverName = withoutScheme.slice(0, slashIdx)
  const mediaId = withoutScheme.slice(slashIdx + 1)
  return `${homeserverUrl}/_matrix/client/v1/media/download/${encodeURIComponent(serverName)}/${encodeURIComponent(mediaId)}`
}

const IMAGE_DIR = '/tmp/claude-matrix-images'
const IMAGE_CLEANUP_DELAY_MS = 5 * 60 * 1000 // 5 minutes

/** Tracks all image files written during this session for exit cleanup. */
export const trackedImages = new Set<string>()

export function scheduleImageCleanup(filePath: string, delayMs = IMAGE_CLEANUP_DELAY_MS): void {
  setTimeout(() => {
    try {
      unlinkSync(filePath)
      trackedImages.delete(filePath)
      console.error(`Cleaned up image: ${filePath}`)
    } catch {
      trackedImages.delete(filePath)
    }
  }, delayMs).unref()
}

export function cleanupAllImages(): void {
  for (const filePath of trackedImages) {
    try {
      unlinkSync(filePath)
    } catch {
      // already gone
    }
  }
  const count = trackedImages.size
  trackedImages.clear()
  if (count > 0) {
    console.error(`Exit cleanup: removed ${count} image(s)`)
  }
  // Remove the directory itself if empty
  try {
    rmdirSync(IMAGE_DIR)
  } catch {
    // not empty or doesn't exist
  }
}

const MIME_TO_EXT: Record<string, string> = {
  'image/png': '.png',
  'image/jpeg': '.jpg',
  'image/gif': '.gif',
  'image/webp': '.webp',
  'image/svg+xml': '.svg',
  'image/bmp': '.bmp',
  'image/tiff': '.tiff',
}

export function sanitizeForFilename(
  input: string,
  allowDots = false,
  maxLength = 100,
): string {
  const pattern = allowDots ? /[^a-zA-Z0-9._-]/g : /[^a-zA-Z0-9_-]/g
  return input.replace(pattern, '_').slice(0, maxLength)
}

export function buildImagePath(
  eventId: string,
  filename: string | null,
  mimeType = 'application/octet-stream',
): string {
  const safeName = filename
    ? sanitizeForFilename(filename, true)
    : `image${MIME_TO_EXT[mimeType] ?? '.bin'}`
  const safeEventId = sanitizeForFilename(eventId)
  return join(IMAGE_DIR, `${safeEventId}-${safeName}`)
}

function formatSize(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(0)}KB`
  return `${bytes}B`
}

export async function downloadImage(
  config: Config,
  access: Access,
  event: ImageEvent,
): Promise<{ content: string; imagePath: string | null }> {
  const displayName = event.filename ?? event.body

  // Early skip if event metadata already indicates oversized image
  if (event.size && event.size > access.maxImageSize) {
    return {
      content: `[Image skipped: exceeds size limit of ${formatSize(access.maxImageSize)}] ${displayName}`,
      imagePath: null,
    }
  }

  try {
    const url = mxcToHttpUrl(config.homeserverUrl, event.mxcUrl)

    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${config.accessToken}` },
      signal: AbortSignal.timeout(30000),
    })

    if (!res.ok) {
      return {
        content: `[Image download failed: HTTP ${res.status}] ${displayName}`,
        imagePath: null,
      }
    }

    // Early reject via Content-Length
    const contentLength = Number(res.headers.get('Content-Length'))
    if (contentLength && contentLength > access.maxImageSize) {
      return {
        content: `[Image skipped: exceeds size limit of ${formatSize(access.maxImageSize)}] ${displayName}`,
        imagePath: null,
      }
    }

    // Stream body and enforce size limit
    const chunks: Uint8Array[] = []
    let totalBytes = 0
    const reader = res.body!.getReader()

    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      totalBytes += value.byteLength
      if (totalBytes > access.maxImageSize) {
        reader.cancel()
        return {
          content: `[Image skipped: exceeds size limit of ${formatSize(access.maxImageSize)}] ${displayName}`,
          imagePath: null,
        }
      }
      chunks.push(value)
    }

    // Write to disk
    const filePath = buildImagePath(event.eventId, event.filename, event.mimeType)
    mkdirSync(IMAGE_DIR, { recursive: true, mode: 0o700 })
    const buffer = Buffer.concat(chunks)
    writeFileSync(filePath, buffer, { mode: 0o600 })
    trackedImages.add(filePath)

    return {
      content: `[Image: ${displayName} (${event.mimeType}, ${formatSize(totalBytes)})]\nUse the Read tool to view the image at ${filePath}`,
      imagePath: filePath,
    }
  } catch (err: any) {
    if (err.name === 'TimeoutError') {
      return {
        content: `[Image download failed: timed out] ${displayName}`,
        imagePath: null,
      }
    }
    return {
      content: `[Image download failed: ${err.message ?? err}] ${displayName}`,
      imagePath: null,
    }
  }
}

// ── Matrix HTTP Client ─────────────────────────────────

function matrixHeaders(accessToken: string): HeadersInit {
  return {
    Authorization: `Bearer ${accessToken}`,
    'Content-Type': 'application/json',
  }
}

async function matrixSync(
  config: Config,
  since: string | null,
): Promise<any> {
  const url = buildSyncUrl(config.homeserverUrl, since)
  const res = await fetch(url, { headers: matrixHeaders(config.accessToken) })

  if (res.status === 429) {
    const body = await res.json().catch(() => ({}))
    const retryMs = body.retry_after_ms ?? 5000
    throw Object.assign(new Error('Rate limited'), { retryMs })
  }

  if (!res.ok) {
    throw new Error(`Sync failed: ${res.status} ${await res.text()}`)
  }

  return res.json()
}

async function matrixSend(
  config: Config,
  roomId: string,
  eventType: string,
  content: Record<string, any>,
): Promise<string> {
  const txnId = nextTxnId()
  const url = `${config.homeserverUrl}/_matrix/client/v3/rooms/${encodeURIComponent(roomId)}/send/${encodeURIComponent(eventType)}/${encodeURIComponent(txnId)}`
  const res = await fetch(url, {
    method: 'PUT',
    headers: matrixHeaders(config.accessToken),
    body: JSON.stringify(content),
  })

  if (!res.ok) {
    throw new Error(`Send failed: ${res.status} ${await res.text()}`)
  }

  const data = await res.json()
  return data.event_id
}

// ── Typing indicator ──────────────────────────────────────────────
//
// Fired once per inbound message, fire-and-forget. Matrix's server-side
// timeout (30s) is enough to cover most Claude turn durations without a
// renewal loop — same pattern as the Discord plugin.
//
// MATRIX_TYPING=false disables. Default: on.

// messageTargetsBot returns true if `body` looks addressed to the bot
// identified by `botUserId`. Matches the full Matrix ID (e.g.
// `@devbot:host.tld`) or the localpart as a delimited token
// (`devbot`, `DevBot`, `@devbot`, `[devbot 💕](...)`, `devbot,` etc.).
// Used to gate the typing indicator so we don't broadcast "thinking..."
// for messages aimed at a different agent in the same room.
export function messageTargetsBot(body: string, botUserId: string): boolean {
  if (!body || !botUserId) return false
  if (body.includes(botUserId)) return true
  const localpart = botUserId.split(':')[0].replace(/^@/, '')
  if (!localpart) return false
  const re = new RegExp(
    `(^|[^A-Za-z0-9_])${escapeRegExp(localpart)}([^A-Za-z0-9_]|$)`,
    'i',
  )
  return re.test(body)
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

export interface FireTypingArgs {
  fetch:         typeof globalThis.fetch
  homeserverUrl: string
  accessToken:   string
  userId:        string
  roomId:        string
}

export async function fireTypingIndicator(args: FireTypingArgs): Promise<void> {
  if (process.env.MATRIX_TYPING === 'false') return

  const url =
    args.homeserverUrl.replace(/\/+$/, '') +
    `/_matrix/client/v3/rooms/${args.roomId}` +
    `/typing/${args.userId}`

  try {
    await args.fetch(url, {
      method: 'PUT',
      headers: {
        Authorization: `Bearer ${args.accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ typing: true, timeout: 30000 }),
    })
  } catch (err) {
    console.error('[matrix] typing indicator failed (non-fatal):', err)
  }
}

async function matrixJoin(config: Config, roomId: string): Promise<void> {
  const url = `${config.homeserverUrl}/_matrix/client/v3/join/${encodeURIComponent(roomId)}`
  const res = await fetch(url, {
    method: 'POST',
    headers: matrixHeaders(config.accessToken),
    body: '{}',
  })

  if (!res.ok) {
    console.error(`Join failed for ${roomId}: ${res.status} ${await res.text()}`)
  }
}

async function matrixReply(
  config: Config,
  roomId: string,
  text: string,
  html?: string,
  threadRootId?: string,
): Promise<string> {
  return matrixSend(config, roomId, 'm.room.message', buildMessageBody(text, html, threadRootId))
}

async function matrixReact(
  config: Config,
  roomId: string,
  eventId: string,
  emoji: string,
): Promise<string> {
  return matrixSend(config, roomId, 'm.reaction', buildReactionBody(eventId, emoji))
}

async function ensureThreadRoot(
  config: Config,
  roomId: string,
  project: string,
  threadsPath?: string,
): Promise<string> {
  const roots = loadThreadRoots(threadsPath)
  const key = `${roomId}:${project}`
  const existing = roots.get(key)
  if (existing) return existing

  const eventId = await matrixSend(
    config,
    roomId,
    'm.room.message',
    buildThreadRootBody(project),
  )
  saveThreadRoot(threadsPath, roomId, project, eventId)
  console.error(`Created thread root for "${project}" in ${roomId}: ${eventId}`)
  return eventId
}

// ── Permission Relay ──────────────────────────────────

export interface PermissionRequestParams {
  request_id: string
  tool_name: string
  description: string
  input_preview: string
}

export function buildPermissionPromptText(params: PermissionRequestParams): string {
  const lines = [
    `🔐 Claude wants to use ${params.tool_name} (id: ${params.request_id})`,
    params.description,
  ]
  if (params.input_preview) {
    lines.push('', '```', params.input_preview, '```')
  }
  lines.push('', `React ${ALLOW_EMOJI} to allow or ${DENY_EMOJI} to deny.`)
  return lines.join('\n')
}

export function buildPermissionPromptHtml(params: PermissionRequestParams): string {
  const esc = (s: string) =>
    s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  const parts = [
    `<p>🔐 <b>Claude wants to use ${esc(params.tool_name)}</b> <code>id: ${esc(params.request_id)}</code></p>`,
    `<p>${esc(params.description)}</p>`,
  ]
  if (params.input_preview) {
    parts.push(`<pre><code>${esc(params.input_preview)}</code></pre>`)
  }
  parts.push(`<p>React ${ALLOW_EMOJI} to allow or ${DENY_EMOJI} to deny.</p>`)
  return parts.join('')
}

export function pickPermissionRoom(
  config: Config,
  lastActive: { roomId: string | null },
): string | null {
  if (config.threadRootRoomId) return config.threadRootRoomId
  if (lastActive.roomId) return lastActive.roomId
  if (config.roomIds && config.roomIds.length > 0) return config.roomIds[0]!
  return null
}

async function relayPermissionRequest(
  config: Config,
  threadRootByRoom: Map<string, string>,
  params: PermissionRequestParams,
): Promise<void> {
  expirePendingPermissions()

  const roomId = pickPermissionRoom(config, lastActiveRoomState)
  if (!roomId) {
    console.error(
      `Permission request ${params.request_id}: no target room available — drop. ` +
      'Send a message first or configure MATRIX_ROOM_IDS / MATRIX_THREAD_ROOT_ROOM_ID.',
    )
    return
  }

  const threadRootId = threadRootByRoom.get(roomId)
  const text = buildPermissionPromptText(params)
  const html = buildPermissionPromptHtml(params)

  const eventId = await matrixReply(config, roomId, text, html, threadRootId)

  pendingPermissions.set(eventId, {
    requestId: params.request_id,
    expiresAt: Date.now() + PERMISSION_TTL_MS,
  })

  console.error(
    `Permission request ${params.request_id} relayed to ${roomId} as ${eventId} (tool=${params.tool_name})`,
  )

  // Pre-react so the user can one-tap. Fire-and-forget; failure is non-fatal.
  matrixReact(config, roomId, eventId, ALLOW_EMOJI).catch((err) =>
    console.error(`Pre-react ${ALLOW_EMOJI} failed:`, err),
  )
  matrixReact(config, roomId, eventId, DENY_EMOJI).catch((err) =>
    console.error(`Pre-react ${DENY_EMOJI} failed:`, err),
  )
}

// ── Reply tool ────────────────────────────────────────
//
// `reply_to_event_id` (new in this PR) routes the message into a
// thread rooted under that event. When set, the outgoing message
// carries rel_type: m.thread + is_falling_back: true +
// m.in_reply_to so unthreaded clients (FluffyChat) still see it.
// Omit to post top-level.

export const replyToolDefinition = {
  name: 'reply',
  description: 'Send a message to a Matrix room',
  inputSchema: {
    type: 'object' as const,
    properties: {
      room_id: { type: 'string', description: 'Matrix room ID, e.g. !abc:example.com' },
      text:    { type: 'string', description: 'Plain-text body' },
      html:    { type: 'string', description: 'Optional HTML body' },
      reply_to_event_id: {
        type: 'string',
        description:
          'Event ID to thread under. When set, the message is sent as a ' +
          'threaded reply (rel_type: m.thread) with proper m.in_reply_to ' +
          'fallback so unthreaded clients still see it. Use this for ' +
          'follow-ups, intermediate progress posts, and anything that ' +
          'should land under the originating user message rather than ' +
          'cluttering the room top-level. Omit to post top-level.',
      },
    },
    required: ['room_id', 'text'],
  },
} as const

// ── MCP Server ─────────────────────────────────────────

export const mcpInstructions = [
  'Messages arrive as <channel source="matrix" room_id="..." event_id="..."',
  'sender="..." room_name="...">. To respond:',
  '',
  '  • Call `reply(room_id, text)` for a top-level message in the room.',
  '  • Call `reply(room_id, text, reply_to_event_id=<inbound event_id>)` to',
  "    thread the response under the user's message. Use this for any",
  '    follow-up after the initial response — keeps the room uncluttered.',
  '  • Call `edit_message(room_id, event_id, text)` to update a prior',
  '    message you sent in-place, e.g. interim "still working — step 3 of',
  '    5" status. Edits do NOT generate push notifications, so ALWAYS',
  '    follow up with a final new `reply` to wake the user when work',
  '    completes. The bot can only edit its own messages.',
  '  • Call `react(room_id, event_id, emoji)` for lightweight status',
  '    signals (👀 received, ✅ done, ❌ failed).',
  '',
  'Threading is per-call: pass `reply_to_event_id` on each reply that',
  'should land in the thread. The plugin does NOT auto-thread; if you omit',
  'the parameter, the message posts top-level. Read the inbound',
  '`event_id` from the <channel> tag and use it as the thread root for',
  'any narration that follows.',
].join('\n')

function createMcpServer(config: Config, threadRootByRoom: Map<string, string>): Server {
  const mcp = new Server(
    { name: 'matrix', version: '0.6.0' },
    {
      capabilities: {
        experimental: {
          'claude/channel': {},
          'claude/channel/permission': {},
        },
        tools: {},
      },
      instructions: mcpInstructions,
    },
  )

  const PermissionRequestSchema = z.object({
    method: z.literal('notifications/claude/channel/permission_request'),
    params: z.object({
      request_id: z.string(),
      tool_name: z.string(),
      description: z.string(),
      input_preview: z.string(),
    }),
  })

  mcp.setNotificationHandler(PermissionRequestSchema, async ({ params }) => {
    try {
      await relayPermissionRequest(config, threadRootByRoom, params)
    } catch (err) {
      console.error('Failed to relay permission request:', err)
    }
  })

  mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [
      replyToolDefinition,
      {
        name: 'react',
        description: 'React to a message with an emoji',
        inputSchema: {
          type: 'object' as const,
          properties: {
            room_id: { type: 'string', description: 'The room the message is in' },
            event_id: { type: 'string', description: 'The event to react to' },
            emoji: { type: 'string', description: 'Emoji to react with' },
          },
          required: ['room_id', 'event_id', 'emoji'],
        },
      },
      editMessageToolDefinition,
    ],
  }))

  mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
    const args = (req.params.arguments ?? {}) as Record<string, string>

    switch (req.params.name) {
      case 'reply': {
        if (!args.room_id || !args.text) {
          return { content: [{ type: 'text', text: 'Missing required arguments: room_id and text' }], isError: true }
        }
        // Per-call reply_to_event_id wins over the static MATRIX_THREADS root.
        // See docs/superpowers/specs/2026-05-27-matrix-channel-threading-tools-design.md
        const threadRootId =
          (args.reply_to_event_id as string | undefined) ??
          threadRootByRoom.get(args.room_id as string)
        const eventId = await matrixReply(config, args.room_id, args.text as string, args.html as string | undefined, threadRootId)
        // Echo the message body in the result so transcript / UI surfaces show
        // what was actually sent — a bare "sent" leaves the caller blind.
        return { content: [{ type: 'text', text: JSON.stringify({ ok: true, event_id: eventId, text: args.text }) }] }
      }
      case 'react': {
        if (!args.room_id || !args.event_id || !args.emoji) {
          return { content: [{ type: 'text', text: 'Missing required arguments: room_id, event_id, and emoji' }], isError: true }
        }
        const eventId = await matrixReact(config, args.room_id, args.event_id, args.emoji)
        return { content: [{ type: 'text', text: JSON.stringify({ ok: true, event_id: eventId, emoji: args.emoji, target_event_id: args.event_id }) }] }
      }
      case 'edit_message': {
        const editArgs = req.params.arguments as {
          room_id: string
          event_id: string
          text: string
          html?: string
        }

        // Ownership + thread-state fetch (one round trip, two facts).
        const info = await fetchEventForEdit({
          fetch: globalThis.fetch,
          homeserverUrl: config.homeserverUrl,
          accessToken:   config.accessToken,
          roomId:        editArgs.room_id,
          eventId:       editArgs.event_id,
        })

        if (info.sender !== config.botUserId) {
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify({
                  ok: false,
                  error: 'not_owned_by_bot',
                  detail: `event ${editArgs.event_id} was sent by ${info.sender}, not ${config.botUserId}`,
                }),
              },
            ],
          }
        }

        const body = buildEditMessageBody({
          text:                 editArgs.text,
          html:                 editArgs.html,
          eventId:              editArgs.event_id,
          originalThreadRootId: info.threadRootId,
        })

        await matrixSend(config, editArgs.room_id, 'm.room.message', body as Record<string, any>)

        return { content: [{ type: 'text', text: JSON.stringify({ ok: true }) }] }
      }
      default:
        throw new Error(`Unknown tool: ${req.params.name}`)
    }
  })

  return mcp
}

// ── Event Gating ───────────────────────────────────────

export function shouldForwardEvent(
  event: SyncEvent,
  access: Access,
  botUserId: string,
  roomIds: string[] | null = null,
): boolean {
  if (event.sender === botUserId) return false
  if (!access.allowedUsers.includes(event.sender)) return false
  if (roomIds && !roomIds.includes(event.roomId)) return false
  return true
}

export function shouldAutoJoin(
  invite: SyncInvite,
  access: Access,
  roomIds: string[] | null = null,
): boolean {
  if (!access.allowedUsers.includes(invite.inviter)) return false
  if (roomIds && !roomIds.includes(invite.roomId)) return false
  return true
}

// ── Thread Root Setup ─────────────────────────────────

export async function setupThreadRoots(
  config: Config,
  threadRootByRoom: Map<string, string>,
): Promise<void> {
  if (!config.threadProject || !config.threadRootRoomId) return
  const roots = loadThreadRoots()
  const key = `${config.threadRootRoomId}:${config.threadProject}`
  const existing = roots.get(key)
  if (existing) {
    threadRootByRoom.set(config.threadRootRoomId, existing)
    console.error(`Loaded persisted thread root for "${config.threadProject}" in ${config.threadRootRoomId}: ${existing}`)
  } else {
    const rootId = await ensureThreadRoot(config, config.threadRootRoomId, config.threadProject)
    threadRootByRoom.set(config.threadRootRoomId, rootId)
  }
}

// ── Event Processing ──────────────────────────────────

export async function processEvents(
  events: SyncEvent[],
  config: Config,
  access: Access,
  threadRootByRoom: Map<string, string>,
  mcp: Server,
): Promise<void> {
  for (const event of events) {
    if (!shouldForwardEvent(event, access, config.botUserId, config.roomIds)) continue

    if (config.threadProject) {
      if (event.threadRootId) {
        const ourRoot = threadRootByRoom.get(event.roomId)
        if (event.threadRootId !== ourRoot) continue
      } else {
        continue
      }
    }

    let content: string
    const meta: Record<string, string> = {
      room_id: event.roomId,
      room_name: event.roomName,
      sender: event.sender,
      event_id: event.eventId,
    }
    if (config.threadProject) {
      meta.thread_project = config.threadProject
    }

    if (event.type === 'text') {
      content = event.body
    } else {
      const result = await downloadImage(config, access, event)
      content = result.content
      if (result.imagePath) {
        meta.image_path = result.imagePath
      }
    }

    lastActiveRoomState.roomId = event.roomId

    // Fire typing indicator only when the message looks addressed to us —
    // showing "thinking..." for messages aimed at other agents in the room
    // is misleading. Best-effort; gated by MATRIX_TYPING env as well.
    if (messageTargetsBot(content, config.botUserId)) {
      void fireTypingIndicator({
        fetch: globalThis.fetch,
        homeserverUrl: config.homeserverUrl,
        accessToken:   config.accessToken,
        userId:        config.botUserId,
        roomId:        event.roomId,
      })
    }

    await mcp.notification({
      method: 'notifications/claude/channel',
      params: { content, meta },
    })

    if (meta.image_path) {
      scheduleImageCleanup(meta.image_path)
    }

    if (access.ackReaction) {
      matrixReact(config, event.roomId, event.eventId, access.ackReaction).catch((err) =>
        console.error(`Ack reaction failed for ${event.eventId}:`, err)
      )
    }
  }
}

export function shouldHonorReaction(
  reaction: ReactionEvent,
  access: Access,
  botUserId: string,
  roomIds: string[] | null = null,
): boolean {
  if (reaction.sender === botUserId) return false
  if (!access.allowedUsers.includes(reaction.sender)) return false
  if (roomIds && !roomIds.includes(reaction.roomId)) return false
  return true
}

export async function processReactions(
  reactions: ReactionEvent[],
  config: Config,
  access: Access,
  mcp: Server,
): Promise<void> {
  expirePendingPermissions()

  for (const reaction of reactions) {
    if (!shouldHonorReaction(reaction, access, config.botUserId, config.roomIds)) continue

    const entry = pendingPermissions.get(reaction.targetEventId)
    if (!entry) continue

    const behavior = classifyVerdict(reaction.emoji)
    if (!behavior) continue

    pendingPermissions.delete(reaction.targetEventId)

    console.error(
      `Permission ${entry.requestId}: ${behavior} (by ${reaction.sender} via ${reaction.emoji})`,
    )

    await mcp.notification({
      method: 'notifications/claude/channel/permission',
      params: { request_id: entry.requestId, behavior },
    })
  }
}

// ── Sync Loop ──────────────────────────────────────────

async function runSyncLoop(
  config: Config,
  access: Access,
  mcp: Server,
  threadRootByRoom: Map<string, string>,
): Promise<never> {
  let since: string | null = null
  let backoffMs = 5000

  // Initial sync — grab state, process pending invites, discard message history
  try {
    console.error('Starting initial sync...')
    const data = await matrixSync(config, null)
    since = data.next_batch
    console.error(`Initial sync complete. Token: ${since}`)

    // Process any pending invites from before the plugin started
    const pendingInvites = parseSyncInvites(data)
    for (const invite of pendingInvites) {
      if (shouldAutoJoin(invite, access, config.roomIds)) {
        console.error(`Auto-joining room ${invite.roomId} (pending invite from ${invite.inviter})`)
        matrixJoin(config, invite.roomId).catch((err) =>
          console.error(`Failed to join ${invite.roomId}:`, err)
        )
      }
    }
  } catch (err) {
    console.error('Initial sync failed:', err)
    throw err
  }

  // Incremental sync loop
  while (true) {
    try {
      const data = await matrixSync(config, since)

      // Process invites
      const invites = parseSyncInvites(data)
      for (const invite of invites) {
        if (shouldAutoJoin(invite, access, config.roomIds)) {
          console.error(`Auto-joining room ${invite.roomId} (invited by ${invite.inviter})`)
          matrixJoin(config, invite.roomId).catch((err) =>
            console.error(`Failed to join ${invite.roomId}:`, err)
          )
        } else {
          console.error(`Ignoring invite to ${invite.roomId} from ${invite.inviter} (not in allowlist)`)
        }
      }

      // Process messages
      const events = parseSyncEvents(data)
      await processEvents(events, config, access, threadRootByRoom, mcp)

      // Process reactions (used for permission relay verdicts)
      const reactions = parseSyncReactions(data)
      await processReactions(reactions, config, access, mcp)

      since = data.next_batch
      backoffMs = 5000 // reset on success
    } catch (err: any) {
      const waitMs = err.retryMs ?? backoffMs
      console.error(`Sync error, retrying in ${waitMs}ms:`, err.message ?? err)
      await new Promise((r) => setTimeout(r, waitMs))
      backoffMs = Math.min(backoffMs * 2, 60000)
    }
  }
}

// ── Multiplexer Sync Loop ─────────────────────────────

async function runMultiplexerSyncLoop(
  config: Config,
  access: Access,
  muxServer: MuxServer,
  channelsDir: string,
  onEvents: (events: SyncEvent[]) => Promise<void>,
  onReactions: (reactions: ReactionEvent[]) => Promise<void>,
): Promise<never> {
  let since = loadSyncToken(channelsDir)
  let backoffMs = 5000

  if (!since) {
    try {
      console.error('Multiplexer: starting initial sync...')
      const data = await matrixSync(config, null)
      since = data.next_batch
      saveSyncToken(channelsDir, since)
      console.error(`Multiplexer: initial sync complete. Token: ${since}`)

      const pendingInvites = parseSyncInvites(data)
      for (const invite of pendingInvites) {
        if (shouldAutoJoin(invite, access, config.roomIds)) {
          console.error(`Auto-joining room ${invite.roomId}`)
          matrixJoin(config, invite.roomId).catch((err) =>
            console.error(`Failed to join ${invite.roomId}:`, err)
          )
        }
      }
    } catch (err) {
      console.error('Multiplexer: initial sync failed:', err)
      throw err
    }
  } else {
    console.error(`Multiplexer: resuming from persisted token: ${since}`)
  }

  while (true) {
    try {
      const data = await matrixSync(config, since)

      const invites = parseSyncInvites(data)
      for (const invite of invites) {
        if (shouldAutoJoin(invite, access, config.roomIds)) {
          matrixJoin(config, invite.roomId).catch((err) =>
            console.error(`Failed to join ${invite.roomId}:`, err)
          )
        }
      }

      const events = parseSyncEvents(data)
      for (const event of events) {
        muxServer.broadcast(eventToFrame(event))
      }
      await onEvents(events)

      const reactions = parseSyncReactions(data)
      for (const reaction of reactions) {
        muxServer.broadcast(reactionToFrame(reaction))
      }
      await onReactions(reactions)

      since = data.next_batch
      saveSyncToken(channelsDir, since)
      backoffMs = 5000
    } catch (err: any) {
      const waitMs = err.retryMs ?? backoffMs
      console.error(`Multiplexer sync error, retrying in ${waitMs}ms:`, err.message ?? err)
      await new Promise((r) => setTimeout(r, waitMs))
      backoffMs = Math.min(backoffMs * 2, 60000)
    }
  }
}

// ── Multiplexer Role Helpers ──────────────────────────

async function startAsMultiplexer(
  config: Config,
  access: Access,
  mcp: Server,
  threadRootByRoom: Map<string, string>,
  channelsDir: string,
  lock: { release: () => void },
): Promise<void> {
  const muxServer = new MuxServer(channelsDir)
  await muxServer.start()

  registerCleanup(() => {
    muxServer.stop().catch(() => {})
    lock.release()
  })

  runMultiplexerSyncLoop(
    config,
    access,
    muxServer,
    channelsDir,
    (events) => processEvents(events, config, access, threadRootByRoom, mcp),
    (reactions) => processReactions(reactions, config, access, mcp),
  ).catch((err) => {
    console.error('Fatal multiplexer sync error:', err)
    process.exit(1)
  })
}

async function runAsClient(
  config: Config,
  access: Access,
  mcp: Server,
  threadRootByRoom: Map<string, string>,
  channelsDir: string,
): Promise<never> {
  const client = new MuxClient(channelsDir)

  // Sequential event processing queue
  let processing = Promise.resolve()
  client.onFrame = (frame) => {
    if (frame.type === 'heartbeat') return
    if (frame.type === 'reaction') {
      const reaction = frameToReaction(frame)
      processing = processing.then(() =>
        processReactions([reaction], config, access, mcp)
      ).catch((err) => console.error('Error processing reaction from multiplexer:', err))
      return
    }
    const event = frameToEvent(frame)
    processing = processing.then(() =>
      processEvents([event], config, access, threadRootByRoom, mcp)
    ).catch((err) => console.error('Error processing event from multiplexer:', err))
  }

  client.onDisconnect = async () => {
    console.error('Multiplexer disconnected — attempting takeover')
    const jitter = Math.random() * 2000
    await new Promise((r) => setTimeout(r, jitter))

    const lock = tryAcquireLock(channelsDir)
    if (lock) {
      console.error('Takeover: acquired lock — becoming multiplexer')
      await startAsMultiplexer(config, access, mcp, threadRootByRoom, channelsDir, lock)
    } else {
      console.error('Takeover: lock not acquired — reconnecting as client')
      try {
        await client.connect()
        console.error('Reconnected to new multiplexer as client')
      } catch {
        console.error('Failed to reconnect — falling back to direct sync')
        runSyncLoop(config, access, mcp, threadRootByRoom).catch((err) => {
          console.error('Fatal sync loop error:', err)
          process.exit(1)
        })
      }
    }
  }

  try {
    await client.connect()
    console.error('Connected to multiplexer as client')
  } catch {
    console.error('Failed to connect to multiplexer — falling back to direct sync')
    runSyncLoop(config, access, mcp, threadRootByRoom).catch((err) => {
      console.error('Fatal sync loop error:', err)
      process.exit(1)
    })
  }

  return new Promise(() => {})
}

// ── Main ───────────────────────────────────────────────

if (import.meta.main) {
  const config = loadConfig()
  const access = loadAccess()
  const threadRootByRoom = new Map<string, string>()

  // Consolidated cleanup — register once
  registerCleanup(cleanupAllImages)
  process.on('SIGINT', () => { runAllCleanup(); process.exit(0) })
  process.on('SIGTERM', () => { runAllCleanup(); process.exit(0) })
  process.on('exit', runAllCleanup)

  console.error(`Matrix channel starting for ${config.botUserId}`)
  console.error(`Homeserver: ${config.homeserverUrl}`)
  console.error(`Allowed users: ${access.allowedUsers.join(', ') || '(none)'}`)
  console.error(`Room filter: ${config.roomIds ? config.roomIds.join(', ') : '(all rooms)'}`)
  if (config.threadProject) {
    console.error(`Threading enabled for project: ${config.threadProject}`)
  }

  const mcp = createMcpServer(config, threadRootByRoom)
  await mcp.connect(new StdioServerTransport())
  await setupThreadRoots(config, threadRootByRoom)

  // Role selection
  if (!MuxServer.validateSocketPath(CHANNELS_DIR)) {
    console.error('Role: DIRECT (socket path too long)')
    runSyncLoop(config, access, mcp, threadRootByRoom).catch((err) => {
      console.error('Fatal sync loop error:', err)
      process.exit(1)
    })
  } else {
    const lock = tryAcquireLock(CHANNELS_DIR)
    if (lock) {
      console.error('Role: MULTIPLEXER')
      await startAsMultiplexer(config, access, mcp, threadRootByRoom, CHANNELS_DIR, lock)
    } else {
      console.error('Role: CLIENT')
      await runAsClient(config, access, mcp, threadRootByRoom, CHANNELS_DIR)
    }
  }
}
