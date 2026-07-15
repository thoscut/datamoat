#!/usr/bin/env node
// Smoke test for the Claude export importer (src/claude-export.ts).
//
// Builds a small synthetic Claude export (an extracted folder — openExportReader
// accepts folders as well as zips), then exercises preflight and the full import
// pipeline with the store's persistence layer stubbed in-memory, and asserts that
// a second run deduplicates everything.
//
// Run: node scripts/claude-import-smoke.js   (or: npm run smoke:claude-import)

const fs = require('fs')
const os = require('os')
const path = require('path')

require('ts-node').register({
  transpileOnly: true,
  compilerOptions: { module: 'commonjs', moduleResolution: 'node' },
})

function assert(cond, message) {
  if (!cond) {
    console.error(`FAIL: ${message}`)
    process.exit(1)
  }
  console.log(`ok: ${message}`)
}

function buildFixture(root) {
  fs.mkdirSync(root, { recursive: true })
  fs.mkdirSync(path.join(root, 'design_chats'), { recursive: true })
  fs.mkdirSync(path.join(root, 'projects'), { recursive: true })

  fs.writeFileSync(path.join(root, 'users.json'), JSON.stringify([
    { uuid: 'user-1', full_name: 'Tester', email_address: 'tester@example.com' },
  ]))

  const conversations = [
    {
      uuid: 'conv-1',
      name: 'First conversation',
      summary: '',
      created_at: '2025-01-01T10:00:00.000000Z',
      updated_at: '2025-01-01T10:05:00.000000Z',
      account: { uuid: 'user-1' },
      chat_messages: [
        {
          uuid: 'm1', sender: 'human', created_at: '2025-01-01T10:00:00.000000Z',
          text: 'Please analyze the attached file.',
          content: [{ type: 'text', text: 'Please analyze the attached file.' }],
          attachments: [{ file_name: 'notes.txt', file_size: 12, file_type: 'txt', extracted_content: 'hello world!' }],
          files: [{ file_name: 'logo.png', file_uuid: 'f-1' }],
          parent_message_uuid: '00000000-0000-4000-8000-000000000000',
        },
        {
          uuid: 'm2', sender: 'assistant', created_at: '2025-01-01T10:01:00.000000Z',
          text: 'Here is my analysis.',
          content: [
            { type: 'thinking', thinking: 'Let me reason about this carefully.' },
            { type: 'tool_use', name: 'web_search', input: { query: 'analysis' }, message: 'Searching' },
            { type: 'tool_result', name: 'web_search', content: [{ type: 'knowledge', title: 'Result', text: 'a fact', url: 'https://x' }] },
            { type: 'token_budget', remaining: null },
            { type: 'text', text: 'Here is my analysis.' },
          ],
          attachments: [], files: [], parent_message_uuid: 'm1',
        },
      ],
    },
    {
      // Empty conversation — should be skipped (no messages).
      uuid: 'conv-empty', name: 'Empty', created_at: '2025-01-02T00:00:00Z', updated_at: '2025-01-02T00:00:00Z',
      account: { uuid: 'user-1' }, chat_messages: [],
    },
  ]
  fs.writeFileSync(path.join(root, 'conversations.json'), JSON.stringify(conversations))

  const designChat = {
    uuid: 'design-1',
    title: 'Design chat',
    project: { uuid: 'p-1', name: 'Project' },
    created_at: '2025-03-01T09:00:00.000Z',
    updated_at: '2025-03-01T09:10:00.000Z',
    messages: [
      {
        uuid: 'dm1', role: 'user', created_at: '2025-03-01T09:00:00.000Z',
        content: {
          role: 'user', content: 'What do you think of this mockup?',
          attachments: [{ id: 'a1', name: 'mock.png', path: 'uploads/mock.png', type: 'image' }],
          timestamp: '2025-03-01T09:00:00.000Z',
        },
      },
      {
        uuid: 'dm2', role: 'assistant', created_at: '2025-03-01T09:01:00.000Z',
        content: {
          role: 'assistant', content: 'It looks great.',
          contentBlocks: [
            { type: 'thinking', text: 'Consider the layout.' },
            { type: 'tool_call', toolCall: { name: 'view_image', type: 'edit', input: { path: 'uploads/mock.png' } } },
            { type: 'text', text: 'It looks great.' },
          ],
          timestamp: '2025-03-01T09:01:00.000Z',
        },
      },
    ],
  }
  fs.writeFileSync(path.join(root, 'design_chats', 'design-1.json'), JSON.stringify(designChat))

  // A project file (metadata only — not imported as a chat).
  fs.writeFileSync(path.join(root, 'projects', 'p-1.json'), JSON.stringify({ uuid: 'p-1', name: 'Project', docs: [] }))
}

async function main() {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'claude-import-smoke-'))
  const fixture = path.join(tmp, 'export')
  process.env.DATAMOAT_HOME = path.join(tmp, 'home')
  buildFixture(fixture)

  // Stub the store's persistence so no encrypted vault is required.
  const store = require('../src/store')
  let sessionsStore = []
  const msgStore = new Map()
  store.hasVaultSession = () => true
  store.ensureDirs = () => {}
  store.loadSessions = async () => sessionsStore
  store.saveSessions = async s => { sessionsStore = s.slice() }
  store.appendMessages = async (session, msgs) => { msgStore.set(session.uid, msgs) }
  store.replaceSessionMessages = async (session, msgs) => { msgStore.set(session.uid, msgs) }
  store.appendRawRecords = async () => {}

  const { preflightClaudeExport, runClaudeExportImport } = require('../src/claude-export')

  const pre = await preflightClaudeExport(fixture)
  assert(pre.ok, 'preflight succeeds')
  assert(pre.counts.conversations === 2, `preflight counts 2 conversations (got ${pre.counts.conversations})`)
  assert(pre.counts.designChats === 1, `preflight counts 1 design chat (got ${pre.counts.designChats})`)
  assert(pre.counts.threads === 2, `preflight counts 2 non-empty threads (got ${pre.counts.threads})`)
  assert(pre.counts.thoughts === 2, `preflight counts 2 thinking blocks (got ${pre.counts.thoughts})`)
  assert(pre.counts.toolUses === 2, `preflight counts 2 tool uses (got ${pre.counts.toolUses})`)
  assert(Object.keys(pre.counts.unknownBlockTypes).length === 0, 'no unknown block types')

  const job1 = await runClaudeExportImport(fixture)
  assert(job1.phase === 'completed', `import completes (phase=${job1.phase})`)
  assert(job1.imported.sessions === 2, `imports 2 sessions (got ${job1.imported.sessions})`)
  assert(sessionsStore.length === 2, `2 sessions in store (got ${sessionsStore.length})`)

  const conv = sessionsStore.find(s => s.id === 'conv-1')
  assert(conv && conv.modelProvider === 'anthropic', 'conversation session has anthropic provider')
  assert(conv && conv.source === 'claude-export', 'session source is claude-export')
  const convMsgs = msgStore.get(conv.uid)
  assert(convMsgs[0].role === 'user', 'human maps to user role')
  assert(convMsgs[1].role === 'assistant', 'assistant role preserved')
  assert(convMsgs[1].hasThinking === true, 'thinking detected on assistant message')
  const types = convMsgs[1].content.map(b => b.type)
  assert(types.includes('thinking') && types.includes('tool_use') && types.includes('tool_result') && types.includes('text'),
    `assistant blocks mapped (${types.join(',')})`)
  assert(!types.includes('token_budget'), 'token_budget dropped')
  assert(convMsgs[0].content.some(b => b.type === 'file'), 'attachment mapped to file block')

  const design = sessionsStore.find(s => s.sourceClient === 'Claude design chat')
  assert(design, 'design chat imported as a session')
  const designMsgs = msgStore.get(design.uid)
  assert(designMsgs.length === 2 && designMsgs[1].hasThinking, 'design chat assistant thinking mapped')

  // Second run — everything deduplicates.
  const job2 = await runClaudeExportImport(fixture)
  assert(job2.imported.sessions === 0, `re-run imports nothing (got ${job2.imported.sessions})`)
  assert(job2.skipped.duplicates === 2, `re-run skips 2 duplicates (got ${job2.skipped.duplicates})`)
  assert(sessionsStore.length === 2, 'session count stable after re-run')

  fs.rmSync(tmp, { recursive: true, force: true })
  console.log('\nAll Claude import smoke checks passed.')
}

main().catch(err => { console.error(err); process.exit(1) })
