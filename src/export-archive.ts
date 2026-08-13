// Shared archive-reading utilities for provider export importers (ChatGPT, Claude, …).
//
// This module owns the provider-agnostic pieces: reading a ZIP file or an
// extracted folder as a uniform entry list, magic-byte media-type sniffing,
// atomic private JSON writes, and small hashing/time helpers. It was factored
// out of `chatgpt-export.ts` so a second importer can reuse the exact same,
// battle-tested zip/zip64 reader instead of duplicating it.

import * as crypto from 'crypto'
import * as fs from 'fs'
import * as path from 'path'
import * as zlib from 'zlib'
import { Readable } from 'stream'

export const MAX_ZIP_ENTRY_COUNT = 100_000
export const MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES = 12 * 1024 * 1024 * 1024
export const MAX_ZIP_SINGLE_ENTRY_BYTES = MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES

export function positiveByteLimit(value: string | undefined, fallback: number): number {
  const parsed = Number(value)
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback
}

type ZipEntry = {
  path: string
  compressedSize: number
  uncompressedSize: number
  method: number
  flags: number
  localHeaderOffset: number
}

export type ExportEntry = {
  path: string
  size: number
}

export type ExportArchiveReader = {
  kind: 'folder' | 'zip'
  rootPath: string
  listEntries(): ExportEntry[]
  has(relativePath: string): boolean
  readBuffer(relativePath: string): Buffer
  createReadStream(relativePath: string): NodeJS.ReadableStream
  close?(): void
}

export function nowIso(): string {
  return new Date().toISOString()
}

export function safeError(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

export function sha256Hex(value: string | Buffer): string {
  return crypto.createHash('sha256').update(value).digest('hex')
}

export function writePrivateJson(filePath: string, value: unknown): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true, mode: 0o700 })
  const tmpPath = `${filePath}.${process.pid}.${crypto.randomBytes(6).toString('hex')}.tmp`
  fs.writeFileSync(tmpPath, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', mode: 0o600 })
  const fd = fs.openSync(tmpPath, 'r')
  try {
    fs.fsyncSync(fd)
  } catch {
    /* non-fatal */
  } finally {
    fs.closeSync(fd)
  }
  fs.renameSync(tmpPath, filePath)
  try { fs.chmodSync(filePath, 0o600) } catch { /* non-fatal */ }
}

export function readJsonFile<T>(filePath: string): T | null {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8')) as T
  } catch {
    return null
  }
}

export function toPosixPath(value: string): string {
  return value.replace(/\\/g, '/')
}

function safeZipPath(name: string): string {
  if (name.includes('\0')) throw new Error('zip entry path contains NUL byte')
  const normalized = toPosixPath(name)
  if (!normalized || normalized.startsWith('/') || normalized.startsWith('~')) {
    throw new Error(`unsafe zip entry path: ${name}`)
  }
  if (/^[A-Za-z]:\//.test(normalized)) throw new Error(`unsafe zip entry path: ${name}`)
  const parts = normalized.split('/').filter(Boolean)
  if (parts.some(part => part === '..')) throw new Error(`unsafe zip entry path: ${name}`)
  return parts.join('/')
}

function listFolderEntries(root: string): ExportEntry[] {
  const entries: ExportEntry[] = []
  const walk = (dirPath: string): void => {
    for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
      if (entry.name === '.DS_Store' || entry.name.startsWith('._')) continue
      const absolute = path.join(dirPath, entry.name)
      if (entry.isDirectory()) {
        walk(absolute)
        continue
      }
      if (!entry.isFile()) continue
      const relative = toPosixPath(path.relative(root, absolute))
      entries.push({ path: relative, size: fs.statSync(absolute).size })
    }
  }
  walk(root)
  return entries.sort((a, b) => a.path.localeCompare(b.path))
}

function folderReader(rootPath: string): ExportArchiveReader {
  const resolved = path.resolve(rootPath)
  const entries = listFolderEntries(resolved)
  const entrySet = new Set(entries.map(entry => entry.path))
  const resolveEntryPath = (relativePath: string): string => {
    const safe = safeZipPath(relativePath)
    if (!entrySet.has(safe)) throw new Error(`export file not found: ${relativePath}`)
    const absolute = path.join(resolved, ...safe.split('/'))
    const normalized = path.resolve(absolute)
    if (!normalized.startsWith(`${resolved}${path.sep}`) && normalized !== resolved) {
      throw new Error(`unsafe export file path: ${relativePath}`)
    }
    return normalized
  }
  return {
    kind: 'folder',
    rootPath: resolved,
    listEntries: () => entries,
    has: relativePath => entrySet.has(toPosixPath(relativePath)),
    readBuffer(relativePath: string): Buffer {
      return fs.readFileSync(resolveEntryPath(relativePath))
    },
    createReadStream(relativePath: string): NodeJS.ReadableStream {
      return fs.createReadStream(resolveEntryPath(relativePath))
    },
  }
}

function readAt(fd: number, offset: number, length: number): Buffer {
  const buffer = Buffer.alloc(length)
  const bytesRead = fs.readSync(fd, buffer, 0, length, offset)
  if (bytesRead !== length) throw new Error('unexpected end of zip file')
  return buffer
}

function uint64ToSafeNumber(value: bigint, label: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error(`zip64 ${label} is too large`)
  return Number(value)
}

function zip64ExtraValues(
  extra: Buffer,
  fields: { uncompressed: number; compressed: number; localHeaderOffset: number },
): { uncompressedSize: number; compressedSize: number; localHeaderOffset: number } {
  let uncompressedSize = fields.uncompressed
  let compressedSize = fields.compressed
  let localHeaderOffset = fields.localHeaderOffset
  let cursor = 0
  while (cursor + 4 <= extra.length) {
    const headerId = extra.readUInt16LE(cursor)
    const dataSize = extra.readUInt16LE(cursor + 2)
    const dataStart = cursor + 4
    const dataEnd = dataStart + dataSize
    if (dataEnd > extra.length) break
    if (headerId === 0x0001) {
      let valueCursor = dataStart
      const readZip64Value = (label: string): number => {
        if (valueCursor + 8 > dataEnd) throw new Error(`zip64 extra field is truncated: ${label}`)
        const value = uint64ToSafeNumber(extra.readBigUInt64LE(valueCursor), label)
        valueCursor += 8
        return value
      }
      if (fields.uncompressed === 0xffffffff) uncompressedSize = readZip64Value('uncompressed size')
      if (fields.compressed === 0xffffffff) compressedSize = readZip64Value('compressed size')
      if (fields.localHeaderOffset === 0xffffffff) localHeaderOffset = readZip64Value('local header offset')
      return { uncompressedSize, compressedSize, localHeaderOffset }
    }
    cursor = dataEnd
  }
  if (
    fields.uncompressed === 0xffffffff
    || fields.compressed === 0xffffffff
    || fields.localHeaderOffset === 0xffffffff
  ) {
    throw new Error('zip64 extra field is missing')
  }
  return { uncompressedSize, compressedSize, localHeaderOffset }
}

function zipCentralDirectoryLocation(
  fd: number,
  stat: fs.Stats,
  eocdOffset: number,
  eocd: Buffer,
): { entriesTotal: number; centralSize: number; centralOffset: number } {
  const diskNumber = eocd.readUInt16LE(4)
  const centralDisk = eocd.readUInt16LE(6)
  const entriesThisDisk = eocd.readUInt16LE(8)
  const entriesTotal32 = eocd.readUInt16LE(10)
  const centralSize32 = eocd.readUInt32LE(12)
  const centralOffset32 = eocd.readUInt32LE(16)
  if (diskNumber !== 0 || centralDisk !== 0 || entriesThisDisk !== entriesTotal32) {
    throw new Error('multi-disk zip files are not supported')
  }

  const needsZip64 = entriesTotal32 === 0xffff || centralSize32 === 0xffffffff || centralOffset32 === 0xffffffff
  if (!needsZip64) {
    return {
      entriesTotal: entriesTotal32,
      centralSize: centralSize32,
      centralOffset: centralOffset32,
    }
  }

  if (eocdOffset < 20) throw new Error('zip64 locator is missing')
  const locator = readAt(fd, eocdOffset - 20, 20)
  if (locator.readUInt32LE(0) !== 0x07064b50) throw new Error('zip64 locator is missing')
  const locatorDisk = locator.readUInt32LE(4)
  const zip64EocdOffset = uint64ToSafeNumber(locator.readBigUInt64LE(8), 'end-of-central-directory offset')
  const totalDisks = locator.readUInt32LE(16)
  if (locatorDisk !== 0 || totalDisks !== 1) throw new Error('multi-disk zip files are not supported')
  if (zip64EocdOffset < 0 || zip64EocdOffset + 56 > stat.size) throw new Error('invalid zip64 end-of-central-directory offset')

  const zip64Header = readAt(fd, zip64EocdOffset, 56)
  if (zip64Header.readUInt32LE(0) !== 0x06064b50) throw new Error('zip64 end-of-central-directory record not found')
  const zip64Disk = zip64Header.readUInt32LE(16)
  const zip64CentralDisk = zip64Header.readUInt32LE(20)
  const zip64EntriesThisDisk = uint64ToSafeNumber(zip64Header.readBigUInt64LE(24), 'entry count')
  const zip64EntriesTotal = uint64ToSafeNumber(zip64Header.readBigUInt64LE(32), 'entry count')
  if (zip64Disk !== 0 || zip64CentralDisk !== 0 || zip64EntriesThisDisk !== zip64EntriesTotal) {
    throw new Error('multi-disk zip files are not supported')
  }
  return {
    entriesTotal: zip64EntriesTotal,
    centralSize: uint64ToSafeNumber(zip64Header.readBigUInt64LE(40), 'central directory size'),
    centralOffset: uint64ToSafeNumber(zip64Header.readBigUInt64LE(48), 'central directory offset'),
  }
}

function parseZipEntries(zipPath: string, fd: number): Map<string, ZipEntry> {
  const stat = fs.fstatSync(fd)
  const tailLength = Math.min(stat.size, 66_000)
  const tail = readAt(fd, stat.size - tailLength, tailLength)
  let eocdOffset = -1
  for (let i = tail.length - 22; i >= 0; i -= 1) {
    if (tail.readUInt32LE(i) === 0x06054b50) {
      eocdOffset = stat.size - tailLength + i
      break
    }
  }
  if (eocdOffset < 0) throw new Error('zip end-of-central-directory record not found')
  const eocd = readAt(fd, eocdOffset, 22)
  const { entriesTotal, centralSize, centralOffset } = zipCentralDirectoryLocation(fd, stat, eocdOffset, eocd)
  if (entriesTotal > MAX_ZIP_ENTRY_COUNT) throw new Error('zip has too many files')
  const central = readAt(fd, centralOffset, centralSize)
  const entries = new Map<string, ZipEntry>()
  let cursor = 0
  let totalUncompressed = 0
  for (let index = 0; index < entriesTotal; index += 1) {
    if (central.readUInt32LE(cursor) !== 0x02014b50) throw new Error(`invalid zip central directory in ${path.basename(zipPath)}`)
    const flags = central.readUInt16LE(cursor + 8)
    const method = central.readUInt16LE(cursor + 10)
    const compressedSize32 = central.readUInt32LE(cursor + 20)
    const uncompressedSize32 = central.readUInt32LE(cursor + 24)
    const nameLength = central.readUInt16LE(cursor + 28)
    const extraLength = central.readUInt16LE(cursor + 30)
    const commentLength = central.readUInt16LE(cursor + 32)
    const localHeaderOffset32 = central.readUInt32LE(cursor + 42)
    const name = central.subarray(cursor + 46, cursor + 46 + nameLength).toString('utf8')
    const extra = central.subarray(cursor + 46 + nameLength, cursor + 46 + nameLength + extraLength)
    cursor += 46 + nameLength + extraLength + commentLength
    if (name.endsWith('/')) continue
    const { compressedSize, uncompressedSize, localHeaderOffset } = zip64ExtraValues(extra, {
      compressed: compressedSize32,
      uncompressed: uncompressedSize32,
      localHeaderOffset: localHeaderOffset32,
    })
    if (flags & 0x1) throw new Error(`encrypted zip entry is not supported: ${name}`)
    if (method !== 0 && method !== 8) throw new Error(`unsupported zip compression method ${method}: ${name}`)
    if (uncompressedSize > MAX_ZIP_SINGLE_ENTRY_BYTES) throw new Error(`zip entry is too large: ${name}`)
    totalUncompressed += uncompressedSize
    if (totalUncompressed > MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES) throw new Error('zip uncompressed size is too large')
    const safe = safeZipPath(name)
    entries.set(safe, { path: safe, compressedSize, uncompressedSize, method, flags, localHeaderOffset })
  }
  return entries
}

function zipEntryDataOffset(fd: number, entry: ZipEntry, relativePath: string): number {
  const local = readAt(fd, entry.localHeaderOffset, 30)
  if (local.readUInt32LE(0) !== 0x04034b50) throw new Error(`invalid zip local header: ${relativePath}`)
  const nameLength = local.readUInt16LE(26)
  const extraLength = local.readUInt16LE(28)
  return entry.localHeaderOffset + 30 + nameLength + extraLength
}

function zipReader(zipPath: string): ExportArchiveReader {
  const resolved = path.resolve(zipPath)
  const fd = fs.openSync(resolved, 'r')
  let closed = false
  const entries = parseZipEntries(resolved, fd)
  const list = Array.from(entries.values()).map(entry => ({ path: entry.path, size: entry.uncompressedSize }))
    .sort((a, b) => a.path.localeCompare(b.path))
  return {
    kind: 'zip',
    rootPath: resolved,
    listEntries: () => list,
    has: relativePath => entries.has(toPosixPath(relativePath)),
    readBuffer(relativePath: string): Buffer {
      const safe = safeZipPath(relativePath)
      const entry = entries.get(safe)
      if (!entry) throw new Error(`export file not found: ${relativePath}`)
      const dataOffset = zipEntryDataOffset(fd, entry, relativePath)
      const compressed = readAt(fd, dataOffset, entry.compressedSize)
      const data = entry.method === 0 ? compressed : zlib.inflateRawSync(compressed)
      if (data.length !== entry.uncompressedSize) throw new Error(`zip entry size mismatch: ${relativePath}`)
      return data
    },
    createReadStream(relativePath: string): NodeJS.ReadableStream {
      const safe = safeZipPath(relativePath)
      const entry = entries.get(safe)
      if (!entry) throw new Error(`export file not found: ${relativePath}`)
      if (entry.compressedSize === 0) return Readable.from([])
      const dataOffset = zipEntryDataOffset(fd, entry, relativePath)
      const compressed = fs.createReadStream(resolved, {
        start: dataOffset,
        end: dataOffset + entry.compressedSize - 1,
      })
      return entry.method === 0 ? compressed : compressed.pipe(zlib.createInflateRaw())
    },
    close() {
      if (closed) return
      closed = true
      fs.closeSync(fd)
    },
  }
}

// Opens a provider export located at `sourcePath`, which may be either a `.zip`
// archive or an already-extracted folder. Throws for anything else.
export function openExportReader(sourcePath: string): ExportArchiveReader {
  const resolved = path.resolve(sourcePath)
  const stat = fs.statSync(resolved)
  if (stat.isDirectory()) return folderReader(resolved)
  if (!stat.isFile()) throw new Error('export path must be a zip file or extracted folder')
  const fd = fs.openSync(resolved, 'r')
  try {
    const signature = readAt(fd, 0, Math.min(4, stat.size))
    if (signature.length < 4 || signature.readUInt32LE(0) !== 0x04034b50) {
      throw new Error('export file must be a .zip file')
    }
  } finally {
    fs.closeSync(fd)
  }
  return zipReader(resolved)
}

export function readJsonFromExport<T>(reader: ExportArchiveReader, relativePath: string, fallback: T): T {
  if (!reader.has(relativePath)) return fallback
  return JSON.parse(reader.readBuffer(relativePath).toString('utf8')) as T
}

function mediaTypeFromName(name: string): string | null {
  const lower = String(name || '').toLowerCase()
  if (/\.(mp4|m4v)$/i.test(lower)) return 'video/mp4'
  if (/\.(mov|qt)$/i.test(lower)) return 'video/quicktime'
  if (/\.webm$/i.test(lower)) return 'video/webm'
  if (/\.mp3$/i.test(lower)) return 'audio/mpeg'
  if (/\.wav$/i.test(lower)) return 'audio/wav'
  if (/\.zip$/i.test(lower)) return 'application/zip'
  if (/\.md$/i.test(lower)) return 'text/markdown'
  if (/\.csv$/i.test(lower)) return 'text/csv'
  if (/\.json$/i.test(lower)) return 'application/json'
  if (/\.tex$/i.test(lower)) return 'text/x-tex'
  return null
}

export function sniffMediaType(buffer: Buffer, fallbackName = ''): string {
  if (buffer.length >= 8 && buffer.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))) return 'image/png'
  if (buffer.length >= 3 && buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) return 'image/jpeg'
  if (buffer.length >= 6 && (buffer.subarray(0, 6).toString('ascii') === 'GIF87a' || buffer.subarray(0, 6).toString('ascii') === 'GIF89a')) return 'image/gif'
  if (buffer.length >= 12 && buffer.subarray(0, 4).toString('ascii') === 'RIFF' && buffer.subarray(8, 12).toString('ascii') === 'WEBP') return 'image/webp'
  if (buffer.length >= 5 && buffer.subarray(0, 5).toString('ascii') === '%PDF-') return 'application/pdf'
  if (buffer.length >= 12 && buffer.subarray(4, 8).toString('ascii') === 'ftyp') return /\.mov$/i.test(fallbackName) ? 'video/quicktime' : 'video/mp4'
  if (buffer.length >= 4 && buffer[0] === 0x1a && buffer[1] === 0x45 && buffer[2] === 0xdf && buffer[3] === 0xa3) return 'video/webm'
  const named = mediaTypeFromName(fallbackName)
  if (named) return named
  const prefix = buffer.subarray(0, Math.min(buffer.length, 512)).toString('utf8').trimStart().toLowerCase()
  if (prefix.startsWith('<!doctype html') || prefix.startsWith('<html')) return 'text/html'
  const sample = buffer.subarray(0, Math.min(buffer.length, 4096))
  if (sample.length > 0) {
    let printable = 0
    for (const byte of sample) {
      if (byte === 9 || byte === 10 || byte === 13 || (byte >= 32 && byte < 127) || byte >= 0xc2) printable += 1
    }
    if (printable / sample.length > 0.85) return 'text/plain'
  }
  return 'application/octet-stream'
}

export async function readStreamSample(stream: NodeJS.ReadableStream, maxBytes: number): Promise<Buffer> {
  const chunks: Buffer[] = []
  let bytes = 0
  for await (const rawChunk of stream as AsyncIterable<Buffer | string>) {
    const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk)
    if (bytes < maxBytes) {
      const needed = Math.min(maxBytes - bytes, chunk.length)
      chunks.push(chunk.subarray(0, needed))
      bytes += needed
    }
    if (bytes >= maxBytes) break
  }
  const destroyable = stream as unknown as { destroy?: () => void }
  if (typeof destroyable.destroy === 'function') {
    destroyable.destroy()
  }
  return Buffer.concat(chunks)
}
