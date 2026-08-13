// Importer for Claude data exports (the ZIP you download from claude.ai →
// Settings → Privacy → "Export data"). It mirrors the ChatGPT export importer
// (`chatgpt-export.ts`) — preflight, resumable-ish progress job, dedup ledger —
// but adapts to Claude's format:
//   * a single `conversations.json` array (not sharded)
//   * flat, linear `chat_messages` (no branch tree)
//   * ISO-8601 timestamps, `sender: human|assistant`
//   * text-only attachments (`extracted_content`); no binary asset files
//   * an additional `design_chats/*.json` set that uses a different, richer
//     message shape and is imported through the same pipeline.
//
// The generic zip/folder reader and helpers live in `export-archive.ts`.

import * as crypto from 'crypto'
import * as fs from 'fs'
import * as path from 'path'
import { STATE_DIR } from './config'
import {
  ExportArchiveReader,
  ExportEntry,
  nowIso,
  openExportReader,
  readJsonFile,
  safeError,
  sha256Hex,
  writePrivateJson,
} from './export-archive'
import {
  appendMessages,
  appendRawRecords,
  ensureDirs,
  hasVaultSession,
  loadSessions,
  makeVaultPath,
  replaceSessionMessages,
  saveSessions,
} from './store'
import type { ContentBlock, Message, RawRecord, Session } from './types'

const CLAUDE_EXPORT_SOURCE = 'claude-export' as const
const CLAUDE_IMPORTS_VERSION = 1
const CLAUDE_IMPORT_JOB_VERSION = 1
const CLAUDE_PARSER_VERSION = 1
const CLAUDE_IMPORTS_FILE = path.join(STATE_DIR, 'claude-export-imports.json')
const CLAUDE_IMPORT_JOB_FILE = path.join(STATE_DIR, 'claude-export-import-job.json')
const STRONG_DUPLICATE_MIN_TEXT_CHARS = 200
const STRONG_DUPLICATE_MIN_MESSAGES = 3

// ---------------------------------------------------------------------------
// Raw export shapes (conversations.json)
// ---------------------------------------------------------------------------

type ClaudeContentBlock = {
  type?: string
  text?: string
  thinking?: string
  name?: string
  input?: unknown
  content?: unknown
  title?: string
  [key: string]: unknown
}

type ClaudeChatMessage = {
  uuid?: string
  text?: string
  content?: ClaudeContentBlock[]
  sender?: string
  created_at?: string
  updated_at?: string
  attachments?: Array<Record<string, unknown>>
  files?: Array<Record<string, unknown>>
  parent_message_uuid?: string
  [key: string]: unknown
}

type ClaudeConversation = {
  uuid?: string
  name?: string
  summary?: string
  created_at?: string
  updated_at?: string
  model?: string | null
  account?: { uuid?: string }
  chat_messages?: ClaudeChatMessage[]
}

// ---------------------------------------------------------------------------
// Raw export shapes (design_chats/*.json)
// ---------------------------------------------------------------------------

type ClaudeDesignInnerBlock = {
  type?: string
  text?: string
  message?: unknown
  toolCall?: { name?: string; type?: string; input?: unknown; output?: unknown }
  [key: string]: unknown
}

type ClaudeDesignInner = {
  role?: string
  content?: string
  attachments?: Array<Record<string, unknown>>
  contentBlocks?: ClaudeDesignInnerBlock[]
  timestamp?: string
  [key: string]: unknown
}

type ClaudeDesignMessage = {
  uuid?: string
  role?: string
  content?: ClaudeDesignInner
  created_at?: string
}

type ClaudeDesignChat = {
  uuid?: string
  title?: string
  project?: { uuid?: string; name?: string }
  created_at?: string
  updated_at?: string
  messages?: ClaudeDesignMessage[]
}

// A normalized thread — the common shape both parsers reduce to before the
// shared prepare/dedup/import pipeline runs.
type ClaudeThread = {
  kind: 'conversation' | 'design'
  file: string
  id: string
  title: string | undefined
  createdAt: string | undefined
  updatedAt: string | undefined
  messages: Message[]
  raw: unknown
}

// ---------------------------------------------------------------------------
// Public result / job types
// ---------------------------------------------------------------------------

export type ClaudeExportCounts = {
  files: number
  totalBytes: number
  conversations: number
  designChats: number
  projects: number
  threads: number
  messages: number
  attachments: number
  thoughts: number
  toolUses: number
  unknownBlockTypes: Record<string, number>
}

export type ClaudeExportPreflightResult = {
  ok: boolean
  sourcePath: string
  sourceKind: 'folder' | 'zip'
  status: 'ready' | 'failed'
  format: 'claude-export'
  counts: ClaudeExportCounts
  warnings: string[]
  errors: string[]
  files: {
    conversations: boolean
    users: boolean
    projects: number
    designChats: number
  }
}

export type ClaudeImportPhase =
  | 'idle'
  | 'preflight'
  | 'reading-export'
  | 'importing-conversations'
  | 'completed'
  | 'failed'

export type ClaudeImportJob = {
  version: typeof CLAUDE_IMPORT_JOB_VERSION
  id: string
  sourcePath: string
  sourceKind: 'folder' | 'zip'
  phase: ClaudeImportPhase
  startedAt: string
  updatedAt: string
  completedAt?: string
  currentConversation?: string
  lastError?: string
  counts: ClaudeExportCounts
  imported: { sessions: number; messages: number; rawRecords: number; attachments: number }
  updated: { sessions: number; messages: number }
  skipped: { sessions: number; messages: number; duplicates: number }
  failed: { sessions: number; attachments: number }
  cursor: { conversationIndex: number; attachmentIndex: number }
  warnings: string[]
  done: boolean
}

type ClaudeImportedConversationRecord = {
  parserVersion?: number
  sourceAccount?: string
  conversationId: string
  destinationUid: string
  currentPathFingerprint: string
  rawConversationHash: string
  strongFingerprint: boolean
  firstImportedAt: string
  lastImportedAt: string
  lastAction: 'imported' | 'updated' | 'skipped'
}

type ClaudeImportsState = {
  version: typeof CLAUDE_IMPORTS_VERSION
  updatedAt: string
  conversations: Record<string, ClaudeImportedConversationRecord>
  currentPathFingerprints: Record<string, ClaudeImportedConversationRecord>
  rawConversationHashes: Record<string, ClaudeImportedConversationRecord>
}

type PreparedThread = {
  session: Session
  messages: Message[]
  rawRecord: RawRecord
  currentPathFingerprint: string
  rawConversationHash: string
  strongFingerprint: boolean
}

// ---------------------------------------------------------------------------
// State files (dedup ledger + progress job)
// ---------------------------------------------------------------------------

function defaultImportsState(): ClaudeImportsState {
  return {
    version: CLAUDE_IMPORTS_VERSION,
    updatedAt: nowIso(),
    conversations: {},
    currentPathFingerprints: {},
    rawConversationHashes: {},
  }
}

function readImportsState(): ClaudeImportsState {
  const raw = readJsonFile<Partial<ClaudeImportsState>>(CLAUDE_IMPORTS_FILE)
  if (!raw || typeof raw !== 'object') return defaultImportsState()
  return {
    version: CLAUDE_IMPORTS_VERSION,
    updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : nowIso(),
    conversations: raw.conversations && typeof raw.conversations === 'object' ? raw.conversations : {},
    currentPathFingerprints: raw.currentPathFingerprints && typeof raw.currentPathFingerprints === 'object' ? raw.currentPathFingerprints : {},
    rawConversationHashes: raw.rawConversationHashes && typeof raw.rawConversationHashes === 'object' ? raw.rawConversationHashes : {},
  }
}

function writeImportsState(state: ClaudeImportsState): void {
  writePrivateJson(CLAUDE_IMPORTS_FILE, { ...state, version: CLAUDE_IMPORTS_VERSION, updatedAt: nowIso() })
}

function readJob(): ClaudeImportJob | null {
  return readJsonFile<ClaudeImportJob>(CLAUDE_IMPORT_JOB_FILE)
}

function writeJob(job: ClaudeImportJob): void {
  writePrivateJson(CLAUDE_IMPORT_JOB_FILE, job)
}

function updateJob(job: ClaudeImportJob, patch: Partial<ClaudeImportJob>): ClaudeImportJob {
  const next = { ...job, ...patch, updatedAt: nowIso() }
  writeJob(next)
  return next
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

function isoFromClaudeTime(value: unknown, fallback?: string): string {
  if (typeof value === 'string' && value.trim()) {
    const parsed = Date.parse(value)
    if (!Number.isNaN(parsed)) return new Date(parsed).toISOString()
  }
  return fallback ?? new Date(0).toISOString()
}

function stringifyCompact(value: unknown): string {
  if (value === undefined || value === null) return ''
  if (typeof value === 'string') return value
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

function pushText(blocks: ContentBlock[], text: unknown): void {
  if (typeof text !== 'string') return
  const trimmed = text.trim()
  if (!trimmed) return
  const previous = blocks[blocks.length - 1]
  if (previous?.type === 'text' && typeof previous.text === 'string') {
    previous.text = `${previous.text}\n${trimmed}`
  } else {
    blocks.push({ type: 'text', text: trimmed })
  }
}

function claudeRole(sender: string | undefined): Message['role'] {
  if (sender === 'human' || sender === 'user') return 'user'
  if (sender === 'assistant' || sender === 'system' || sender === 'tool') return sender === 'system' || sender === 'tool' ? sender : 'assistant'
  return 'assistant'
}

function mediaTypeFromClaudeFileType(fileType: unknown, name = ''): string | undefined {
  const raw = typeof fileType === 'string' ? fileType.trim().toLowerCase() : ''
  const map: Record<string, string> = {
    txt: 'text/plain',
    md: 'text/markdown',
    csv: 'text/csv',
    json: 'application/json',
    pdf: 'application/pdf',
    png: 'image/png',
    jpg: 'image/jpeg',
    jpeg: 'image/jpeg',
    gif: 'image/gif',
    webp: 'image/webp',
    image: 'image/*',
  }
  if (raw && map[raw]) return map[raw]
  if (raw.includes('/')) return raw
  const lower = name.toLowerCase()
  if (/\.pdf$/.test(lower)) return 'application/pdf'
  if (/\.(png|jpe?g|gif|webp)$/.test(lower)) return 'image/*'
  if (/\.(md|markdown)$/.test(lower)) return 'text/markdown'
  if (/\.csv$/.test(lower)) return 'text/csv'
  if (/\.json$/.test(lower)) return 'application/json'
  if (/\.txt$/.test(lower)) return 'text/plain'
  return undefined
}

function toolResultText(content: unknown): string {
  if (typeof content === 'string') return content
  if (Array.isArray(content)) {
    return content
      .map(item => {
        if (typeof item === 'string') return item
        if (item && typeof item === 'object') {
          const record = item as Record<string, unknown>
          const parts = [record.title, record.text, record.url].filter((value): value is string => typeof value === 'string' && value.trim() !== '')
          return parts.join(' — ')
        }
        return ''
      })
      .filter(Boolean)
      .join('\n')
  }
  return stringifyCompact(content)
}

// ---------------------------------------------------------------------------
// Content-block translation
// ---------------------------------------------------------------------------

// conversations.json message content[] → ContentBlock[]
function collectConversationBlocks(content: ClaudeContentBlock[] | undefined, blocks: ContentBlock[]): void {
  if (!Array.isArray(content)) return
  for (const block of content) {
    if (!block || typeof block !== 'object') continue
    const type = typeof block.type === 'string' ? block.type : ''
    switch (type) {
      case 'text':
        pushText(blocks, block.text)
        break
      case 'thinking':
        if (typeof block.thinking === 'string' && block.thinking.trim()) {
          blocks.push({ type: 'thinking', thinking: block.thinking.trim() })
        }
        break
      case 'tool_use':
        blocks.push({
          type: 'tool_use',
          name: typeof block.name === 'string' ? block.name : 'claude_tool',
          input: block.input,
          text: typeof block.message === 'string' ? block.message : undefined,
          content: block,
        })
        break
      case 'tool_result':
        blocks.push({
          type: 'tool_result',
          name: typeof block.name === 'string' ? block.name : undefined,
          content: block.content,
          text: toolResultText(block.content),
        })
        break
      case 'voice_note':
        pushText(blocks, [block.title, block.text].filter(v => typeof v === 'string').join('\n'))
        break
      case 'token_budget':
        // Pure metadata (remaining token budget) — nothing to render.
        break
      default: {
        const fallback = typeof block.text === 'string' ? block.text : stringifyCompact(block)
        if (fallback && fallback !== '{}') blocks.push({ type: 'other', content: block, text: fallback })
      }
    }
  }
}

// design_chats inner.contentBlocks[] → ContentBlock[]
function collectDesignBlocks(inner: ClaudeDesignInner, blocks: ContentBlock[]): void {
  const contentBlocks = Array.isArray(inner.contentBlocks) ? inner.contentBlocks : []
  if (contentBlocks.length === 0) {
    pushText(blocks, inner.content)
    return
  }
  for (const block of contentBlocks) {
    if (!block || typeof block !== 'object') continue
    const type = typeof block.type === 'string' ? block.type : ''
    switch (type) {
      case 'text':
        pushText(blocks, block.text)
        break
      case 'thinking':
        if (typeof block.text === 'string' && block.text.trim()) {
          blocks.push({ type: 'thinking', thinking: block.text.trim() })
        }
        break
      case 'tool_call': {
        const call = block.toolCall || {}
        blocks.push({
          type: 'tool_use',
          name: typeof call.name === 'string' ? call.name : 'design_tool',
          input: call.input,
          text: typeof call.type === 'string' ? call.type : undefined,
          content: call,
        })
        break
      }
      case 'error':
        if (typeof block.message === 'string') pushText(blocks, block.message)
        break
      case 'user_interjection': {
        const msg = block.message as Record<string, unknown> | undefined
        const text = msg && typeof msg.content === 'string' ? msg.content : stringifyCompact(block.message)
        pushText(blocks, text)
        break
      }
      default: {
        const fallback = typeof block.text === 'string' ? block.text : stringifyCompact(block)
        if (fallback && fallback !== '{}') blocks.push({ type: 'other', content: block, text: fallback })
      }
    }
  }
}

// Text-only attachments and file references (both export shapes) → ContentBlock[]
function appendAttachmentBlocks(
  blocks: ContentBlock[],
  attachments: Array<Record<string, unknown>> | undefined,
  files: Array<Record<string, unknown>> | undefined,
): number {
  let count = 0
  for (const attachment of attachments ?? []) {
    if (!attachment || typeof attachment !== 'object') continue
    const fileName = typeof attachment.file_name === 'string' ? attachment.file_name
      : typeof attachment.name === 'string' ? attachment.name : 'attachment'
    const fileType = attachment.file_type ?? attachment.type
    const extracted = typeof attachment.extracted_content === 'string' ? attachment.extracted_content
      : typeof attachment.content === 'string' ? attachment.content : ''
    blocks.push({
      type: 'file',
      attachmentName: fileName,
      mediaType: mediaTypeFromClaudeFileType(fileType, fileName),
      text: extracted ? `${fileName}\n${extracted}` : fileName,
      content: attachment,
    })
    count += 1
  }
  for (const file of files ?? []) {
    if (!file || typeof file !== 'object') continue
    const fileName = typeof file.file_name === 'string' ? file.file_name
      : typeof file.name === 'string' ? file.name : 'file'
    blocks.push({
      type: 'file',
      attachmentName: fileName,
      mediaType: mediaTypeFromClaudeFileType(file.type, fileName),
      text: fileName,
      content: file,
    })
    count += 1
  }
  return count
}

// ---------------------------------------------------------------------------
// Message construction
// ---------------------------------------------------------------------------

function messageFromChatMessage(message: ClaudeChatMessage, conversation: ClaudeConversation): Message | null {
  const role = claudeRole(message.sender)
  const blocks: ContentBlock[] = []
  collectConversationBlocks(message.content, blocks)
  if (blocks.length === 0) pushText(blocks, message.text)
  appendAttachmentBlocks(blocks, message.attachments, message.files)
  if (blocks.length === 0) return null
  const timestamp = isoFromClaudeTime(message.created_at, isoFromClaudeTime(conversation.created_at))
  return {
    id: message.uuid || sha256Hex(stringifyCompact(message)).slice(0, 24),
    role,
    timestamp,
    content: blocks,
    hasThinking: blocks.some(block => block.type === 'thinking'),
    sourceEventType: 'claude.message',
    unknownAttrs: {
      claudeMessageUuid: message.uuid ?? null,
      claudeParentMessageUuid: message.parent_message_uuid ?? null,
      claudeSender: message.sender ?? null,
    },
  }
}

function messageFromDesignMessage(message: ClaudeDesignMessage, chat: ClaudeDesignChat): Message | null {
  const inner: ClaudeDesignInner = message.content && typeof message.content === 'object' ? message.content : {}
  const role = claudeRole(message.role || inner.role)
  const blocks: ContentBlock[] = []
  collectDesignBlocks(inner, blocks)
  appendAttachmentBlocks(blocks, inner.attachments, undefined)
  if (blocks.length === 0) return null
  const timestamp = isoFromClaudeTime(inner.timestamp || message.created_at, isoFromClaudeTime(chat.created_at))
  return {
    id: message.uuid || sha256Hex(stringifyCompact(message)).slice(0, 24),
    role,
    timestamp,
    content: blocks,
    hasThinking: blocks.some(block => block.type === 'thinking'),
    sourceEventType: 'claude.design-message',
    unknownAttrs: {
      claudeDesignMessageUuid: message.uuid ?? null,
      claudeDesignAuthorName: inner.authorName ?? null,
    },
  }
}

// ---------------------------------------------------------------------------
// Loading / normalizing threads
// ---------------------------------------------------------------------------

function loadConversationThreads(reader: ExportArchiveReader): ClaudeThread[] {
  const files = reader.listEntries()
    .map(entry => entry.path)
    .filter(file => file === 'conversations.json' || /^conversations-\d+\.json$/.test(file))
    .sort()
  const threads: ClaudeThread[] = []
  for (const file of files) {
    const parsed = JSON.parse(reader.readBuffer(file).toString('utf8')) as unknown
    if (!Array.isArray(parsed)) throw new Error(`${file} is not a Claude conversations array`)
    for (const raw of parsed) {
      if (!raw || typeof raw !== 'object') continue
      const conversation = raw as ClaudeConversation
      const messages = (conversation.chat_messages ?? [])
        .map(message => messageFromChatMessage(message, conversation))
        .filter((message): message is Message => message !== null)
      threads.push({
        kind: 'conversation',
        file,
        id: String(conversation.uuid || '').trim(),
        title: conversation.name || undefined,
        createdAt: conversation.created_at,
        updatedAt: conversation.updated_at,
        messages,
        raw: conversation,
      })
    }
  }
  return threads
}

function loadDesignThreads(reader: ExportArchiveReader): ClaudeThread[] {
  const files = reader.listEntries()
    .map(entry => entry.path)
    .filter(file => /^design_chats\/.+\.json$/.test(file))
    .sort()
  const threads: ClaudeThread[] = []
  for (const file of files) {
    const parsed = JSON.parse(reader.readBuffer(file).toString('utf8')) as unknown
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) continue
    const chat = parsed as ClaudeDesignChat
    const messages = (chat.messages ?? [])
      .map(message => messageFromDesignMessage(message, chat))
      .filter((message): message is Message => message !== null)
    threads.push({
      kind: 'design',
      file,
      id: String(chat.uuid || '').trim(),
      title: chat.title || chat.project?.name || undefined,
      createdAt: chat.created_at,
      updatedAt: chat.updated_at,
      messages,
      raw: chat,
    })
  }
  return threads
}

function loadThreads(reader: ExportArchiveReader): ClaudeThread[] {
  return [...loadConversationThreads(reader), ...loadDesignThreads(reader)]
}

// ---------------------------------------------------------------------------
// Account / identity / dedup
// ---------------------------------------------------------------------------

function userHash(reader: ExportArchiveReader): string | undefined {
  if (!reader.has('users.json')) return undefined
  try {
    const raw = reader.readBuffer('users.json').toString('utf8')
    const parsed = JSON.parse(raw) as unknown
    const first = Array.isArray(parsed) ? parsed[0] : parsed
    const record = (first && typeof first === 'object' ? first : {}) as Record<string, unknown>
    const candidate = String(record.uuid || record.email_address || record.email || raw).trim()
    return `claude:${sha256Hex(candidate).slice(0, 12)}`
  } catch {
    return undefined
  }
}

// A thread's stable identity namespace — includes kind so a conversation and a
// design chat can never collide even if uuids were ever shared.
function threadIdentity(thread: ClaudeThread, rawHash: string): string {
  const explicit = thread.id
  if (explicit) return `${thread.kind}:${explicit}`
  const title = String(thread.title || '').trim()
  const created = String(thread.createdAt || '').trim()
  return `${thread.kind}:missing-${sha256Hex(`${title}\0${created}\0${rawHash}`).slice(0, 16)}`
}

function stableSessionUid(sourceAccount: string | undefined, identity: string): string {
  return sha256Hex(`${CLAUDE_EXPORT_SOURCE}\0${sourceAccount ?? ''}\0${identity}`).slice(0, 24)
}

function conversationRecordKey(sourceAccount: string | undefined, identity: string): string {
  return `${sourceAccount ?? ''}\0${identity}`
}

function scopedImportIndexKey(sourceAccount: string | undefined, value: string): string {
  return `${sourceAccount ?? ''}\0${value}`
}

function sameAccountRecord(
  record: ClaudeImportedConversationRecord | undefined,
  sourceAccount: string | undefined,
): ClaudeImportedConversationRecord | undefined {
  if (!record) return undefined
  return (record.sourceAccount ?? '') === (sourceAccount ?? '') ? record : undefined
}

function currentPathFingerprint(messages: Message[]): { hash: string; strong: boolean; textChars: number } {
  let textChars = 0
  const normalized = messages.map(message => {
    const content = message.content.map(block => {
      if (block.type === 'text') {
        const text = String(block.text || '').normalize('NFKC').replace(/\s+/g, ' ').trim()
        textChars += text.length
        return { type: block.type, text }
      }
      if (block.type === 'thinking') {
        const text = String(block.thinking || '').normalize('NFKC').replace(/\s+/g, ' ').trim()
        textChars += text.length
        return { type: block.type, thinking: text }
      }
      return {
        type: block.type,
        name: block.name,
        attachmentName: block.attachmentName,
        text: String(block.text || '').normalize('NFKC').replace(/\s+/g, ' ').trim(),
      }
    })
    return { role: message.role, content }
  })
  return {
    hash: sha256Hex(JSON.stringify(normalized)),
    strong: messages.length >= STRONG_DUPLICATE_MIN_MESSAGES || textChars >= STRONG_DUPLICATE_MIN_TEXT_CHARS,
    textChars,
  }
}

// ---------------------------------------------------------------------------
// Thread → Session
// ---------------------------------------------------------------------------

function prepareThread(thread: ClaudeThread, sourceAccount: string | undefined): PreparedThread {
  const rawConversationHash = sha256Hex(JSON.stringify(thread.raw))
  const identity = threadIdentity(thread, rawConversationHash)
  const messages = thread.messages
  const fallbackTime = isoFromClaudeTime(thread.createdAt)
  const firstTimestamp = messages[0]?.timestamp || fallbackTime
  const lastTimestamp = messages[messages.length - 1]?.timestamp || isoFromClaudeTime(thread.updatedAt, firstTimestamp)
  const uid = stableSessionUid(sourceAccount, identity)
  const originalPath = `claude-export://${thread.kind}/${sourceAccount ?? 'unknown'}/${thread.id || identity}`
  const fingerprint = currentPathFingerprint(messages)
  const defaultTitle = thread.kind === 'design' ? 'Untitled Claude design chat' : 'Untitled Claude conversation'
  const session: Session = {
    uid,
    id: thread.id || identity,
    source: CLAUDE_EXPORT_SOURCE,
    sourceClient: thread.kind === 'design' ? 'Claude design chat' : 'Claude export',
    sourceAccount,
    appVersion: `export parser-v${CLAUDE_PARSER_VERSION}`,
    model: 'Claude',
    modelProvider: 'anthropic',
    title: thread.title || undefined,
    firstTimestamp,
    lastTimestamp,
    cwd: thread.title || defaultTitle,
    messageCount: messages.length,
    hasThinking: messages.some(message => message.hasThinking),
    vaultPath: makeVaultPath(CLAUDE_EXPORT_SOURCE, uid),
    originalPath,
  }
  const rawRecord: RawRecord = {
    v: 1,
    source: CLAUDE_EXPORT_SOURCE,
    sourcePath: originalPath,
    sourceByteOffset: 0,
    capturedAt: nowIso(),
    rawHash: rawConversationHash,
    raw: {
      type: thread.kind === 'design' ? 'claude-design-snapshot' : 'claude-conversation-snapshot',
      thread: thread.raw,
    },
  }
  return {
    session,
    messages,
    rawRecord,
    currentPathFingerprint: fingerprint.hash,
    rawConversationHash,
    strongFingerprint: fingerprint.strong,
  }
}

// ---------------------------------------------------------------------------
// Counting / preflight
// ---------------------------------------------------------------------------

function emptyCounts(entries: ExportEntry[]): ClaudeExportCounts {
  return {
    files: entries.length,
    totalBytes: entries.reduce((sum, entry) => sum + entry.size, 0),
    conversations: 0,
    designChats: 0,
    projects: 0,
    threads: 0,
    messages: 0,
    attachments: 0,
    thoughts: 0,
    toolUses: 0,
    unknownBlockTypes: {},
  }
}

const KNOWN_CONVERSATION_BLOCK_TYPES = new Set(['text', 'thinking', 'tool_use', 'tool_result', 'voice_note', 'token_budget'])

function summarizeExport(reader: ExportArchiveReader, threads: ClaudeThread[]): ClaudeExportCounts {
  const entries = reader.listEntries()
  const counts = emptyCounts(entries)
  counts.projects = entries.filter(entry => /^projects\/.+\.json$/.test(entry.path)).length
  for (const thread of threads) {
    if (thread.kind === 'design') counts.designChats += 1
    else counts.conversations += 1
    if (thread.messages.length === 0) continue
    counts.threads += 1
    counts.messages += thread.messages.length
    for (const message of thread.messages) {
      for (const block of message.content) {
        if (block.type === 'thinking') counts.thoughts += 1
        if (block.type === 'tool_use') counts.toolUses += 1
        if (block.type === 'file') counts.attachments += 1
      }
    }
    // Track unknown raw block types for visibility.
    if (thread.kind === 'conversation') {
      const conversation = thread.raw as ClaudeConversation
      for (const message of conversation.chat_messages ?? []) {
        for (const block of message.content ?? []) {
          const type = typeof block?.type === 'string' ? block.type : ''
          if (type && !KNOWN_CONVERSATION_BLOCK_TYPES.has(type)) {
            counts.unknownBlockTypes[type] = (counts.unknownBlockTypes[type] || 0) + 1
          }
        }
      }
    }
  }
  return counts
}

export async function preflightClaudeExport(sourcePath: string): Promise<ClaudeExportPreflightResult> {
  const warnings: string[] = []
  const errors: string[] = []
  let reader: ExportArchiveReader | null = null
  try {
    reader = openExportReader(sourcePath)
    const entries = reader.listEntries()
    const hasConversations = reader.has('conversations.json') || entries.some(entry => /^conversations-\d+\.json$/.test(entry.path))
    const designChatCount = entries.filter(entry => /^design_chats\/.+\.json$/.test(entry.path)).length
    if (!hasConversations && designChatCount === 0) {
      errors.push('No conversations.json or design_chats were found in this export')
    }
    const threads = loadThreads(reader)
    const counts = summarizeExport(reader, threads)
    if (!reader.has('users.json')) warnings.push('users.json is missing; imported sessions will not be grouped by account')
    const emptyThreads = threads.filter(thread => thread.messages.length === 0).length
    if (emptyThreads > 0) warnings.push(`${emptyThreads} conversations/design chats have no messages and will be skipped`)
    if (counts.projects > 0) warnings.push(`${counts.projects} project files were found; project metadata is preserved but not imported as chat sessions`)
    if (Object.keys(counts.unknownBlockTypes).length > 0) {
      warnings.push(`Unrecognized content block types will be preserved as raw: ${Object.keys(counts.unknownBlockTypes).join(', ')}`)
    }
    return {
      ok: errors.length === 0,
      sourcePath: path.resolve(sourcePath),
      sourceKind: reader.kind,
      status: errors.length === 0 ? 'ready' : 'failed',
      format: 'claude-export',
      counts,
      warnings,
      errors,
      files: {
        conversations: hasConversations,
        users: reader.has('users.json'),
        projects: counts.projects,
        designChats: designChatCount,
      },
    }
  } catch (error) {
    errors.push(safeError(error))
    const entries = reader?.listEntries() ?? []
    return {
      ok: false,
      sourcePath: path.resolve(sourcePath),
      sourceKind: reader?.kind ?? (fs.existsSync(sourcePath) && fs.statSync(sourcePath).isDirectory() ? 'folder' : 'zip'),
      status: 'failed',
      format: 'claude-export',
      counts: emptyCounts(entries),
      warnings,
      errors,
      files: {
        conversations: !!reader?.has('conversations.json'),
        users: !!reader?.has('users.json'),
        projects: entries.filter(entry => /^projects\/.+\.json$/.test(entry.path)).length,
        designChats: entries.filter(entry => /^design_chats\/.+\.json$/.test(entry.path)).length,
      },
    }
  } finally {
    reader?.close?.()
  }
}

// ---------------------------------------------------------------------------
// Import job
// ---------------------------------------------------------------------------

function newJob(sourcePath: string, sourceKind: 'folder' | 'zip', counts: ClaudeExportCounts, warnings: string[]): ClaudeImportJob {
  const startedAt = nowIso()
  return {
    version: CLAUDE_IMPORT_JOB_VERSION,
    id: crypto.randomUUID(),
    sourcePath: path.resolve(sourcePath),
    sourceKind,
    phase: 'preflight',
    startedAt,
    updatedAt: startedAt,
    counts,
    imported: { sessions: 0, messages: 0, rawRecords: 0, attachments: 0 },
    updated: { sessions: 0, messages: 0 },
    skipped: { sessions: 0, messages: 0, duplicates: 0 },
    failed: { sessions: 0, attachments: 0 },
    cursor: { conversationIndex: 0, attachmentIndex: 0 },
    warnings,
    done: false,
  }
}

export function currentClaudeImportJob(): ClaudeImportJob | null {
  return readJob()
}

export async function runClaudeExportImport(sourcePath: string): Promise<ClaudeImportJob> {
  if (!hasVaultSession()) throw new Error('current vault must be unlocked before importing Claude exports')
  ensureDirs()
  const preflight = await preflightClaudeExport(sourcePath)
  if (!preflight.ok) throw new Error(preflight.errors.join(' · ') || 'Claude export preflight failed')
  let job = newJob(sourcePath, preflight.sourceKind, preflight.counts, preflight.warnings)
  writeJob(job)
  let reader: ExportArchiveReader | null = null
  try {
    reader = openExportReader(sourcePath)
    job = updateJob(job, { phase: 'reading-export' })
    const sourceAccount = userHash(reader)
    const allThreads = loadThreads(reader)
    const threads = allThreads.filter(thread => thread.messages.length > 0)
    const importsState = readImportsState()

    job = updateJob(job, { phase: 'importing-conversations' })
    const sessions = await loadSessions()
    const sessionByUid = new Map(sessions.map(session => [session.uid, session]))
    const merged = [...sessions]
    const indexByUid = new Map(merged.map((session, index) => [session.uid, index]))
    let sessionsDirty = false
    const flushSessions = async (): Promise<void> => {
      if (!sessionsDirty) return
      await saveSessions(merged)
      sessionsDirty = false
    }

    for (let index = 0; index < threads.length; index += 1) {
      const thread = threads[index]
      job.cursor.conversationIndex = index
      job.currentConversation = thread.title || thread.id || thread.file
      writeJob(job)
      try {
        const prepared = prepareThread(thread, sourceAccount)
        const identity = threadIdentity(thread, prepared.rawConversationHash)
        const key = conversationRecordKey(sourceAccount, identity)
        const existingRecord = importsState.conversations[key]
        const sameRaw = importsState.rawConversationHashes[scopedImportIndexKey(sourceAccount, prepared.rawConversationHash)]
          || sameAccountRecord(importsState.rawConversationHashes[prepared.rawConversationHash], sourceAccount)
        const sameStrongFingerprint = prepared.strongFingerprint
          ? importsState.currentPathFingerprints[scopedImportIndexKey(sourceAccount, prepared.currentPathFingerprint)]
            || sameAccountRecord(importsState.currentPathFingerprints[prepared.currentPathFingerprint], sourceAccount)
          : undefined
        const destinationUid = existingRecord?.destinationUid || sameRaw?.destinationUid || sameStrongFingerprint?.destinationUid || prepared.session.uid
        const existingSession = sessionByUid.get(destinationUid) || sessionByUid.get(prepared.session.uid)
        const rawAlreadyStored = !!sameRaw

        const existingCurrentParser = existingRecord?.parserVersion === CLAUDE_PARSER_VERSION
        const sameRawCurrentParser = sameRaw?.parserVersion === CLAUDE_PARSER_VERSION
        const sameStrongCurrentParser = sameStrongFingerprint?.parserVersion === CLAUDE_PARSER_VERSION
        if (
          (existingRecord && existingCurrentParser && existingRecord.currentPathFingerprint === prepared.currentPathFingerprint)
          || (sameRaw && sameRawCurrentParser)
          || (sameStrongFingerprint && sameStrongCurrentParser && sameStrongFingerprint.conversationId !== identity)
        ) {
          job.skipped.sessions += 1
          job.skipped.duplicates += 1
          job.skipped.messages += prepared.messages.length
          const record = existingRecord || sameRaw || sameStrongFingerprint!
          record.lastImportedAt = nowIso()
          record.lastAction = 'skipped'
          importsState.conversations[key] = record
          writeImportsState(importsState)
          writeJob(job)
          continue
        }

        const destinationSession: Session = {
          ...prepared.session,
          uid: destinationUid,
          vaultPath: makeVaultPath(CLAUDE_EXPORT_SOURCE, destinationUid),
        }

        if (existingSession || existingRecord) {
          await replaceSessionMessages(destinationSession, prepared.messages)
          const existingIndex = indexByUid.get(destinationUid)
          if (existingIndex !== undefined) merged[existingIndex] = destinationSession
          else {
            merged.push(destinationSession)
            indexByUid.set(destinationUid, merged.length - 1)
          }
          job.updated.sessions += 1
          job.updated.messages += prepared.messages.length
        } else {
          await appendMessages(destinationSession, prepared.messages)
          merged.push(destinationSession)
          indexByUid.set(destinationUid, merged.length - 1)
          sessionByUid.set(destinationUid, destinationSession)
          job.imported.sessions += 1
          job.imported.messages += prepared.messages.length
        }
        sessionsDirty = true

        if (!rawAlreadyStored) {
          await appendRawRecords(CLAUDE_EXPORT_SOURCE, destinationUid, [{
            ...prepared.rawRecord,
            sourcePath: destinationSession.originalPath,
          }])
          job.imported.rawRecords += 1
        }
        await saveSessions(merged)
        sessionsDirty = false
        const importedAt = nowIso()
        const record: ClaudeImportedConversationRecord = {
          parserVersion: CLAUDE_PARSER_VERSION,
          sourceAccount,
          conversationId: identity,
          destinationUid,
          currentPathFingerprint: prepared.currentPathFingerprint,
          rawConversationHash: prepared.rawConversationHash,
          strongFingerprint: prepared.strongFingerprint,
          firstImportedAt: existingRecord?.firstImportedAt ?? importedAt,
          lastImportedAt: importedAt,
          lastAction: existingSession || existingRecord ? 'updated' : 'imported',
        }
        importsState.conversations[key] = record
        importsState.rawConversationHashes[scopedImportIndexKey(sourceAccount, prepared.rawConversationHash)] = record
        if (prepared.strongFingerprint) importsState.currentPathFingerprints[scopedImportIndexKey(sourceAccount, prepared.currentPathFingerprint)] = record
        writeImportsState(importsState)
        writeJob(job)
      } catch (error) {
        job.failed.sessions += 1
        job.lastError = safeError(error)
        writeJob(job)
      }
    }
    await flushSessions()
    job.currentConversation = undefined
    job = updateJob(job, {
      phase: 'completed',
      completedAt: nowIso(),
      lastError: undefined,
      done: true,
    })
    return job
  } catch (error) {
    job = updateJob(job, {
      phase: 'failed',
      lastError: safeError(error),
      done: true,
    })
    return job
  } finally {
    reader?.close?.()
  }
}
