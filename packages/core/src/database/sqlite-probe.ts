import { createHash } from "node:crypto"
import { appendFileSync, readFileSync } from "node:fs"
import { isAbsolute, relative, resolve, sep } from "node:path"

const PROBE_IDS = new Set([
  "run-process-success",
  "run-process-thinking",
  "run-process-thinking-plain",
  "run-process-unknown-finish",
  "run-process-permission-ask",
  "run-process-permission-allow",
  "run-process-permission-deny",
  "run-process-json-output",
  "run-process-command-effort",
])
// Nine fixed child IDs × (256 records + 8 error records + two truncation markers) × 768 chars stays below 2 MiB.
const MAX_EVENTS = 256
const MAX_ERROR_EVENTS = 8
const MAX_RECORD_CHARS = 768
const MAX_STATEMENT_CHARS = 192

export interface DatabaseInfo {
  readonly connectionID: number
  readonly databaseKey: string
  readonly databasePath: string
  readonly databaseKind: "memory" | "file"
}

export interface Client {
  readonly clientID: number
  readonly succeeded: (query: string) => void
  readonly failed: (query: string, cause: unknown) => void
}

export interface Probe {
  readonly open: (filename: string) => DatabaseInfo
  readonly client: (database: DatabaseInfo) => Client
  readonly failed: (database: DatabaseInfo, query: string, cause: unknown) => void
}

export function isAllowedProbeID(id: string) {
  return PROBE_IDS.has(id)
}

export function sanitizeStatement(query: string) {
  return query
    .replace(/'(?:''|[^'])*'/g, "?")
    .replace(/"(?:""|[^"])*"/g, (quoted) => safeIdentifier(quoted.slice(1, -1).replaceAll('""', '"'), '"'))
    .replace(/`(?:``|[^`])*`/g, (quoted) => safeIdentifier(quoted.slice(1, -1).replaceAll("``", "`"), "`"))
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/--[^\r\n]*/g, " ")
    .replace(/\b0x[0-9a-f]+\b/gi, "?")
    .replace(/(?<!\?)\b\d+(?:\.\d+)?\b/g, "?")
    .replace(/[^\x20-\x7e]/g, "?")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, MAX_STATEMENT_CHARS)
}

export function sqliteErrorCode(cause: unknown) {
  if (typeof cause !== "object" || cause === null) return
  if ("code" in cause && typeof cause.code === "string" && cause.code.startsWith("SQLITE_")) return cause.code

  const code =
    "errcode" in cause && typeof cause.errcode === "number"
      ? cause.errcode
      : "code" in cause && typeof cause.code === "number"
        ? cause.code
        : undefined
  if (code === undefined) return
  if ((code & 0xff) === 5) return "SQLITE_BUSY"
  if ((code & 0xff) === 6) return "SQLITE_LOCKED"
}

export function create(): Probe | undefined {
  const probeID = process.env["OPENCODE_SQLITE_PROBE_ID"]
  const logPath = process.env["OPENCODE_SQLITE_PROBE_LOG"]
  const runnerTemp = process.env["RUNNER_TEMP"]
  if (process.env["OPENCODE_SQLITE_PROBE"] !== "1" || !probeID || !isAllowedProbeID(probeID) || !logPath || !runnerTemp)
    return

  const relativeLogPath = relative(resolve(runnerTemp), resolve(logPath))
  if (
    !relativeLogPath ||
    relativeLogPath === ".." ||
    relativeLogPath.startsWith(`..${sep}`) ||
    isAbsolute(relativeLogPath)
  )
    return

  let nextConnectionID = 0
  let nextClientID = 0
  let eventCount = 0
  let errorCount = 0
  let truncationWritten = false
  let errorTruncationWritten = false

  const appendLine = (fields: Record<string, unknown>) => {
    const line = JSON.stringify({
      version: 1,
      timestamp_ms: Date.now(),
      driver: "bun:sqlite",
      pid: process.pid,
      ppid: process.ppid,
      probe_id: probeID,
      ...fields,
    })
    if (line.length > MAX_RECORD_CHARS) return
    try {
      appendFileSync(logPath, `${line}\n`, { mode: 0o600 })
    } catch {}
  }

  const append = (fields: Record<string, unknown>, error = false) => {
    if (error) {
      if (errorCount >= MAX_ERROR_EVENTS) {
        if (!errorTruncationWritten) {
          errorTruncationWritten = true
          appendLine({ event: "error_trace_truncated", max_error_events: MAX_ERROR_EVENTS })
        }
        return
      }
      errorCount++
    } else {
      if (eventCount >= MAX_EVENTS) {
        if (!truncationWritten) {
          truncationWritten = true
          appendLine({ event: "trace_truncated", max_events: MAX_EVENTS })
        }
        return
      }
      eventCount++
    }

    appendLine(fields)
  }

  const owners = (databaseKey: string, connectionID: number, clientID: number) => {
    const open = new Map<string, Record<string, unknown>>()
    try {
      for (const line of readFileSync(logPath, "utf8").split("\n")) {
        if (!line.startsWith("{")) continue
        const record = parseRecord(line)
        if (!record || record.database_key !== databaseKey) continue
        if (record.event === "transaction_begin") {
          const key = `${record.pid}:${record.connection_id}:${record.client_id}:${record.transaction_id}`
          open.set(key, record)
        }
        if (record.event === "transaction_end" && Array.isArray(record.transaction_ids)) {
          for (const transactionID of record.transaction_ids) {
            open.delete(`${record.pid}:${record.connection_id}:${record.client_id}:${transactionID}`)
          }
        }
      }
    } catch {
      return []
    }

    return [...open.values()]
      .filter(
        (record) =>
          record.pid !== process.pid || record.connection_id !== connectionID || record.client_id !== clientID,
      )
      .slice(0, 2)
      .map((record) => ({
        pid: record.pid,
        connection_id: record.connection_id,
        client_id: record.client_id,
        transaction_id: record.transaction_id,
      }))
  }

  const statementError = (
    database: DatabaseInfo,
    query: string,
    cause: unknown,
    clientID?: number,
    activeTransactions: readonly string[] = [],
  ) => {
    const errorCode = sqliteErrorCode(cause)
    if (!errorCode || (!errorCode.startsWith("SQLITE_BUSY") && !errorCode.startsWith("SQLITE_LOCKED"))) return
    append(
      {
        event: "statement_error",
        database_key: database.databaseKey,
        database_path: database.databasePath,
        database_kind: database.databaseKind,
        connection_id: database.connectionID,
        client_id: clientID,
        operation: operation(query),
        statement: sanitizeStatement(query),
        error_code: errorCode,
        active_transactions: activeTransactions.slice(-4),
        lock_candidates: owners(database.databaseKey, database.connectionID, clientID ?? -1),
      },
      true,
    )
  }

  return {
    open(filename) {
      const connectionID = ++nextConnectionID
      const database = describeDatabase(filename, connectionID)
      append({
        event: "database_open",
        connection_id: connectionID,
        database_key: database.databaseKey,
        database_path: database.databasePath,
        database_kind: database.databaseKind,
      })
      return { ...database, connectionID }
    },
    client(database) {
      const clientID = ++nextClientID
      let transactionSequence = 0
      const activeTransactions: string[] = []
      append({
        event: "client_open",
        connection_id: database.connectionID,
        client_id: clientID,
        database_key: database.databaseKey,
        database_path: database.databasePath,
      })

      return {
        clientID,
        succeeded(query) {
          const control = transactionControl(query)
          if (control === "BEGIN" || control === "SAVEPOINT") {
            const transactionID = `${process.pid}:${clientID}:${++transactionSequence}`
            activeTransactions.push(transactionID)
            append({
              event: "transaction_begin",
              connection_id: database.connectionID,
              client_id: clientID,
              database_key: database.databaseKey,
              transaction_id: transactionID,
              operation: control,
              statement: sanitizeStatement(query),
            })
            return
          }
          if (control === "ROLLBACK_TO") {
            append({
              event: "transaction_rollback_to",
              connection_id: database.connectionID,
              client_id: clientID,
              database_key: database.databaseKey,
              transaction_id: activeTransactions.at(-1),
              statement: sanitizeStatement(query),
            })
            return
          }
          if (!control) return

          const transactionIDs = control === "RELEASE" ? activeTransactions.splice(-1) : activeTransactions.splice(0)
          append({
            event: "transaction_end",
            connection_id: database.connectionID,
            client_id: clientID,
            database_key: database.databaseKey,
            transaction_ids: transactionIDs,
            operation: control,
            statement: sanitizeStatement(query),
          })
        },
        failed(query, cause) {
          statementError(database, query, cause, clientID, activeTransactions)
        },
      }
    },
    failed(database, query, cause) {
      statementError(database, query, cause)
    },
  }
}

function safeIdentifier(value: string, quote: string) {
  if (!/^[A-Za-z_][A-Za-z0-9_$]*$/.test(value)) return "?"
  return `${quote}${value}${quote}`
}

function operation(query: string) {
  return (
    query
      .trim()
      .match(/^[A-Za-z]+/)?.[0]
      .toUpperCase() ?? "UNKNOWN"
  )
}

function transactionControl(query: string) {
  const value = query.trim().replace(/\s+/g, " ")
  if (/^ROLLBACK\s+TO\b/i.test(value)) return "ROLLBACK_TO"
  return /^(BEGIN|SAVEPOINT|COMMIT|END|ROLLBACK|RELEASE)\b/i.exec(value)?.[1]?.toUpperCase()
}

function describeDatabase(filename: string, connectionID: number) {
  const memory = filename === ":memory:" || (filename.startsWith("file:") && /[?&]mode=memory\b/i.test(filename))
  const identity = memory
    ? `memory:${process.pid}:${connectionID}:${filename}`
    : filename.startsWith("file:")
      ? filename
      : resolve(filename)
  const databaseKey = createHash("sha256").update(identity).digest("hex").slice(0, 16)
  return {
    databaseKey,
    databasePath: memory ? ":memory:" : `<file:${databaseKey}>`,
    databaseKind: memory ? ("memory" as const) : ("file" as const),
  }
}

function parseRecord(line: string) {
  try {
    const value: unknown = JSON.parse(line)
    if (!value || typeof value !== "object" || Array.isArray(value)) return
    return value as Record<string, unknown>
  } catch {
    return
  }
}

export * as SqliteProbe from "./sqlite-probe"
