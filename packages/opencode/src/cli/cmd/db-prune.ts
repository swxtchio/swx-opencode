import { Database } from "@opencode-ai/core/database/database"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { Durable } from "@opencode-ai/schema/durable-event-manifest"
import type { Argv } from "yargs"
import { lstat, realpath, stat } from "node:fs/promises"
import path from "node:path"
import { cmd } from "./cmd"
import { scanDatabaseHolders } from "./db-retention"

type SqliteAccess = import("bun:sqlite").Database

type MeasurementStatus = "measured" | "partial" | "unavailable" | "failed"
type LedgerStatus = "available" | "unavailable" | "failed"

type PruneArgs = {
  before?: string
  database?: string
  directory?: string
  session?: string[]
  apply?: boolean
  firstmateRanIt?: boolean
  measurementStatus?: MeasurementStatus
  usageLedgerStatus?: LedgerStatus
}

type SessionRow = {
  id: string
  directory: string
  parent_id: string | null
  time_created: number
  time_updated: number
}
type SessionReport = {
  sessionID: string
  directory: string | null
  directoryState: string
  ownedRows: Record<string, number>
  parentSessionID: string | null
  createdAt: string | null
  lastActivity: string | null
  reasons: string[]
}

// Keep the relationship snapshot available for revalidation without exposing it in CLI JSON.
const parentSnapshot = Symbol("parentSnapshot")

const ownedSessionTables = new Set([
  "session",
  "message",
  "part",
  "todo",
  "session_message",
  "session_input",
  "session_context_epoch",
  "session_prompt_queue",
  "session_prompt_queue_sequence",
  "session_prompt_queue_alias",
  "session_prompt_provenance",
  "session_prompt_receipt",
  "session_ask",
  "session_share",
  "event_sequence",
  "event",
  "event_retention",
  "event_aggregate_writer",
])

const aggregateTables = ["event_sequence", "event", "event_retention", "event_aggregate_writer"]

const requiredColumns: Record<string, string[]> = {
  session: ["id", "directory", "parent_id", "time_created", "time_updated", "time_compacting"],
  message: ["id", "session_id", "time_created", "time_updated", "data"],
  part: ["id", "message_id", "session_id", "time_created", "time_updated", "data"],
  todo: ["session_id", "time_created", "time_updated"],
  session_message: ["id", "session_id", "type", "time_created", "time_updated", "data"],
  session_input: ["id", "session_id", "delivery", "promoted_seq", "time_created"],
  session_context_epoch: ["session_id"],
  session_prompt_queue: [
    "id",
    "session_id",
    "delivery",
    "time_created",
    "time_promoted",
    "time_withdrawn",
    "time_preparing",
  ],
  session_prompt_queue_sequence: ["session_id"],
  session_prompt_queue_alias: ["id", "item_id"],
  session_prompt_provenance: ["message_id", "session_id"],
  session_prompt_receipt: ["session_id", "state", "time_created", "time_updated"],
  session_ask: ["id", "session_id", "time_created", "time_settled"],
  session_share: ["session_id", "time_created", "time_updated"],
  event_sequence: ["aggregate_id", "seq"],
  event: ["aggregate_id", "type"],
  event_retention: ["aggregate_id", "state", "time_started", "time_updated"],
  event_aggregate_writer: ["aggregate_id", "writer_id", "time_last_append"],
  event_writer_process: ["writer_id", "pid", "boot_id", "start_ticks"],
}

const activitySources: {
  table: string
  scope: string
  columns: { name: string; optional: boolean }[]
}[] = [
  {
    table: "session",
    scope: "id",
    columns: [
      { name: "time_created", optional: false },
      { name: "time_updated", optional: false },
    ],
  },
  {
    table: "message",
    scope: "session_id",
    columns: [
      { name: "time_created", optional: false },
      { name: "time_updated", optional: false },
    ],
  },
  {
    table: "part",
    scope: "session_id",
    columns: [
      { name: "time_created", optional: false },
      { name: "time_updated", optional: false },
    ],
  },
  {
    table: "todo",
    scope: "session_id",
    columns: [
      { name: "time_created", optional: false },
      { name: "time_updated", optional: false },
    ],
  },
  {
    table: "session_message",
    scope: "session_id",
    columns: [
      { name: "time_created", optional: false },
      { name: "time_updated", optional: false },
    ],
  },
  { table: "session_input", scope: "session_id", columns: [{ name: "time_created", optional: false }] },
  {
    table: "session_prompt_queue",
    scope: "session_id",
    columns: [
      { name: "time_created", optional: false },
      { name: "time_promoted", optional: true },
      { name: "time_withdrawn", optional: true },
      { name: "time_preparing", optional: true },
    ],
  },
  {
    table: "session_prompt_receipt",
    scope: "session_id",
    columns: [
      { name: "time_created", optional: false },
      { name: "time_updated", optional: false },
    ],
  },
  {
    table: "session_ask",
    scope: "session_id",
    columns: [
      { name: "time_created", optional: false },
      { name: "time_settled", optional: true },
    ],
  },
  {
    table: "session_share",
    scope: "session_id",
    columns: [
      { name: "time_created", optional: false },
      { name: "time_updated", optional: false },
    ],
  },
  {
    table: "event_retention",
    scope: "aggregate_id",
    columns: [
      { name: "time_started", optional: false },
      { name: "time_updated", optional: false },
    ],
  },
  { table: "event_aggregate_writer", scope: "aggregate_id", columns: [{ name: "time_last_append", optional: false }] },
]

const sidecars = ["-wal", "-shm", "-journal"]

const storageScopes: readonly (readonly [string, string])[] = [
  ["session_prompt_queue_alias", "item_id IN (SELECT id FROM session_prompt_queue WHERE session_id = ?)"],
  ...[
    "session_prompt_queue",
    "session_prompt_queue_sequence",
    "session_prompt_receipt",
    "session_prompt_provenance",
    "session_ask",
    "session_input",
    "session_context_epoch",
    "todo",
    "session_share",
    "part",
    "message",
    "session_message",
  ].map((table) => [table, "session_id = ?"] as const),
  ...["event", "event_aggregate_writer", "event_retention", "event_sequence"].map(
    (table) => [table, "aggregate_id = ?"] as const,
  ),
  ["session", "id = ?"],
]

const pruneOptions = (yargs: Argv) =>
  yargs
    .option("before", {
      type: "string",
      describe: "optional UTC or offset cutoff narrowing missing-directory Sessions by activity",
    })
    .option("database", {
      type: "string",
      describe: "target local database file (defaults to opencode db path); never creates a database",
    })
    .option("directory", {
      type: "string",
      describe: "limit retirement to this absolute recorded directory, after it has been removed",
    })
    .option("session", {
      type: "array",
      string: true,
      describe: "limit inventory and apply to these Session IDs; may be repeated",
    })
    .option("apply", {
      type: "boolean",
      default: false,
      describe: "delete the selected whole Sessions and reclaim allocated database bytes",
    })
    .option("firstmate-ran-it", {
      type: "boolean",
      default: false,
      describe: "report Firstmate's actual-run confirmation; this does not gate pruning",
    })
    .option("measurement-status", {
      type: "string",
      choices: ["measured", "partial", "unavailable", "failed"],
      default: "unavailable",
      describe: "report the best-effort Firstmate measurement state without requiring completeness",
    })
    .option("usage-ledger-status", {
      type: "string",
      choices: ["available", "unavailable", "failed"],
      default: "unavailable",
      describe: "report Firstmate's durable usage-ledger state without using it as a deletion gate",
    })

export const PruneCommand = cmd<{}, PruneArgs>({
  command: "prune",
  describe: "inventory missing-directory local Sessions; --apply deletes them offline and vacuums the database",
  builder: pruneOptions,
  async handler(args) {
    const filename = args.database ?? Database.path()
    const cutoff = parseCutoff(args.before)
    const requested = normalizeSessionIDs(args.session)
    const usage = measurementReport(args)
    if (
      (args.before !== undefined && !cutoff) ||
      requested === null ||
      (args.directory !== undefined && !validDirectory(args.directory))
    ) {
      console.log(
        JSON.stringify(
          {
            action: "refused",
            database: filename,
            cutoff: args.before ?? null,
            selectedSessions: [],
            excludedSessions: [],
            deletionScope: { sessionIDs: [], wholeSessions: true },
            reclamation: {
              ready: false,
              reasons: [
                args.before !== undefined && !cutoff
                  ? "invalid-or-missing-cutoff"
                  : requested === null
                    ? "invalid-session-selection"
                    : "invalid-directory-selection",
              ],
            },
            usageMeasurement: usage,
            rawTranscriptWarning: "Applying this command permanently deletes the raw transcripts in selected Sessions.",
            changedRows: 0,
          },
          null,
          2,
        ),
      )
      process.exitCode = 1
      return
    }

    const resolved = await resolveDatabaseFile(filename)
    if ("reason" in resolved) {
      console.log(
        JSON.stringify(
          refusedReport(filename, cutoff, requested, usage, [resolved.reason ?? "database-file-unreadable"]),
          null,
          2,
        ),
      )
      process.exitCode = 1
      return
    }
    if (args.apply) {
      const holders = scanDatabaseHolders(resolved.filename)
      const reasons =
        holders.state === "unreadable"
          ? [`database-liveness-unreadable:${holders.reason}`]
          : holders.pids.length > 0
            ? [`database-in-use-by-process:${holders.pids.join(",")}`]
            : []
      if (reasons.length > 0) {
        console.log(JSON.stringify(refusedReport(filename, cutoff, requested, usage, reasons), null, 2))
        process.exitCode = 1
        return
      }
    }

    const sqlite = await import("bun:sqlite")
    let db: SqliteAccess | undefined
    try {
      db = new sqlite.Database(resolved.filename, args.apply ? { readwrite: true, create: false } : { readonly: true })
      db.exec("PRAGMA busy_timeout = 5; PRAGMA foreign_keys = ON")
      // Retain SQLite exclusion across the committed delete and the out-of-transaction vacuum.
      if (args.apply) db.exec("PRAGMA locking_mode = EXCLUSIVE")
      const inventory = await inventorySnapshot(db, resolved.filename, cutoff, requested, args.directory)
      if (!args.apply) {
        console.log(JSON.stringify({ ...inventory, action: "inventory", usageMeasurement: usage }, null, 2))
        return
      }

      const result = await applyPrune(db, resolved.filename, cutoff, requested, inventory, args.directory)
      console.log(JSON.stringify({ ...result, usageMeasurement: usage }, null, 2))
      if (result.action !== "applied" && result.action !== "noop") process.exitCode = 1
    } catch (error) {
      console.log(
        JSON.stringify(
          refusedReport(filename, cutoff, requested, usage, [
            error instanceof Error ? error.message : "database-operation-failed",
          ]),
          null,
          2,
        ),
      )
      process.exitCode = 1
    } finally {
      db?.close()
    }
  },
})

export const RetireCommand = cmd<{}, PruneArgs>({
  command: "retire",
  describe: "retire a removed worktree's local Sessions; inventory unless --apply is specified",
  builder: (yargs: Argv) => pruneOptions(yargs).demandOption("directory"),
  handler: PruneCommand.handler,
})

async function inventorySnapshot(
  db: SqliteAccess,
  filename: string,
  cutoff: number | undefined,
  requested: readonly string[] | undefined,
  directory?: string,
) {
  const schema = inspectSchema(db)
  const sessions = schema.tables.has("session")
    ? db
        .query<SessionRow, []>("SELECT id, directory, parent_id, time_created, time_updated FROM session ORDER BY id")
        .all()
    : []
  const parents = sessions.map((row) => [row.id, row.parent_id] as const)
  const ids = new Set(sessions.map((row) => row.id))
  const activities = activityBySession(db, schema.tables, schema.columnMap)
  const ownership = aggregateOwnershipFailures(db, ids, schema.tables)
  const unknownStorage = schema.unclassifiedTables.map((table) => `unclassified-session-storage:${table}`)
  const integrity = integrityResult(db)
  const foreignKeys = foreignKeyResult(db)
  const holders = scanDatabaseHolders(filename)
  const holderReasons =
    holders.state === "unreadable"
      ? [`database-liveness-unreadable:${holders.reason}`]
      : holders.pids.length > 0
        ? [`database-in-use-by-process:${holders.pids.join(",")}`]
        : []
  const autoVacuum = db.query<{ auto_vacuum: number }, []>("PRAGMA auto_vacuum").get()?.auto_vacuum
  const allocated = await allocatedBytes(filename)
  const globalReasons = [...schema.failures, ...unknownStorage, ...ownership]
  if (integrity.state !== "ok") globalReasons.push(`integrity-check-${integrity.state}`)
  if (foreignKeys.state !== "ok") globalReasons.push(`foreign-key-check-${foreignKeys.state}`)
  const candidates = new Map<string, SessionReport>()
  const excluded = new Map<string, SessionReport>()

  for (const row of sessions) {
    const reasons = [...globalReasons, ...holderReasons]
    const directoryState = await inspectDirectory(row.directory)
    if (directoryState !== "missing") reasons.push(`session-directory-${directoryState}`)
    if (directory !== undefined && path.normalize(row.directory) !== path.normalize(directory)) {
      reasons.push("outside-explicit-directory-selection")
    }
    if (requested && !requested.includes(row.id)) reasons.push("outside-explicit-session-selection")
    if (row.parent_id !== null && !ids.has(row.parent_id)) reasons.push("parent-session-missing")
    if (hasParentCycle(row.id, new Map(sessions.map((item) => [item.id, item.parent_id])))) {
      reasons.push("parent-cycle")
    }
    const activity = activities.get(row.id)
    if (!activity || activity.unreadable) reasons.push("activity-evidence-unreadable")
    else if (cutoff !== undefined && activity.lastActivity >= cutoff) reasons.push("activity-at-or-after-cutoff")
    if (schema.failures.length === 0) {
      reasons.push(...writerFailures(db, row.id))
      reasons.push(...sessionWorkFailures(db, row.id))
    }
    const report: SessionReport = {
      sessionID: row.id,
      directory: row.directory,
      directoryState,
      ownedRows: schema.failures.length === 0 ? ownedRows(db, row.id) : {},
      parentSessionID: row.parent_id,
      createdAt: Number.isFinite(row.time_created) ? new Date(row.time_created).toISOString() : null,
      lastActivity: activity ? new Date(activity.lastActivity).toISOString() : null,
      reasons: [...new Set(reasons)].toSorted(),
    }
    if (report.reasons.length === 0) candidates.set(row.id, report)
    else excluded.set(row.id, report)
  }

  for (const row of sessions) {
    if (!candidates.has(row.id)) continue
    const protectedDescendant = descendants(row.id, sessions)
      .filter((id) => !candidates.has(id))
      .toSorted()
    if (protectedDescendant.length === 0) continue
    const current = candidates.get(row.id)!
    candidates.delete(row.id)
    excluded.set(row.id, {
      ...current,
      reasons: [`protected-descendant:${protectedDescendant.join(",")}`],
    })
  }

  for (const id of requested ?? []) {
    if (ids.has(id)) continue
    excluded.set(id, {
      sessionID: id,
      directory: null,
      directoryState: "unknown",
      ownedRows: {},
      parentSessionID: null,
      createdAt: null,
      lastActivity: null,
      reasons: ["session-not-found"],
    })
  }

  const crossSessionOwnership = crossSessionCascadeFailures(db, [...candidates.keys()])
  const readinessReasons = [
    ...schema.failures,
    ...unknownStorage,
    ...ownership,
    ...crossSessionOwnership,
    ...holderReasons,
  ]
  if (integrity.state !== "ok") readinessReasons.push(`integrity-check-${integrity.state}`)
  if (foreignKeys.state !== "ok") readinessReasons.push(`foreign-key-check-${foreignKeys.state}`)
  if (allocated.status !== "measured") readinessReasons.push(`allocated-byte-measurement-${allocated.status}`)
  return {
    database: filename,
    cutoff: cutoff === undefined ? null : new Date(cutoff).toISOString(),
    explicitDirectory: directory ?? null,
    explicitSessionIDs: requested ?? null,
    selectedSessions: [...candidates.values()].toSorted((left, right) => left.sessionID.localeCompare(right.sessionID)),
    excludedSessions: [...excluded.values()].toSorted((left, right) => left.sessionID.localeCompare(right.sessionID)),
    deletionScope: { sessionIDs: [...candidates.keys()].toSorted(), wholeSessions: true },
    selectedCounts: {
      sessions: candidates.size,
      ownedRows: [...candidates.values()].reduce(
        (sum, item) => sum + Object.values(item.ownedRows).reduce((sum, n) => sum + n, 0),
        0,
      ),
    },
    reclamation: {
      ready: readinessReasons.length === 0,
      method: "PRAGMA wal_checkpoint(TRUNCATE) followed by VACUUM",
      autoVacuum: autoVacuum === 2 ? "incremental" : (autoVacuum ?? "unreadable"),
      allocatedBytes: allocated,
      integrity,
      foreignKeys,
      reasons: [...new Set(readinessReasons)].toSorted(),
    },
    rawTranscriptWarning: "Applying this command permanently deletes the raw transcripts in selected Sessions.",
    changedRows: 0,
    [parentSnapshot]: parents,
  }
}

async function applyPrune(
  db: SqliteAccess,
  filename: string,
  cutoff: number | undefined,
  requested: readonly string[] | undefined,
  inventory: Awaited<ReturnType<typeof inventorySnapshot>>,
  directory?: string,
) {
  const refusalReasons = [...inventory.reclamation.reasons]
  if (inventory.selectedSessions.length === 0 && directory === undefined) refusalReasons.push("no-eligible-sessions")
  if (refusalReasons.length > 0) return { ...inventory, action: "refused", refusalReasons, changedRows: 0 }
  if (inventory.selectedSessions.length === 0) {
    const state = await inspectDirectory(directory!)
    if (
      state !== "missing" ||
      inventory.excludedSessions.some(
        (item) => item.directory !== null && path.normalize(item.directory) === path.normalize(directory!),
      )
    ) {
      return {
        ...inventory,
        action: "refused",
        refusalReasons: [`directory-retirement-not-eligible:${state}`],
        changedRows: 0,
      }
    }
  }
  if (inventory.reclamation.allocatedBytes.status !== "measured") {
    return {
      ...inventory,
      action: "refused",
      refusalReasons: ["allocated-byte-measurement-unavailable"],
      changedRows: 0,
    }
  }

  let changedRows = 0
  const deletedByTable: Record<string, number> = {}
  db.exec("BEGIN IMMEDIATE")
  try {
    const current = await inventorySnapshot(db, filename, cutoff, requested, directory)
    if (!current.reclamation.ready) throw new Error(`revalidation-failed:${current.reclamation.reasons.join(",")}`)
    if (!sameIDs(inventory.deletionScope.sessionIDs, current.deletionScope.sessionIDs)) {
      throw new Error("selection-changed-after-inventory")
    }
    if (!sameParents(inventory[parentSnapshot], current[parentSnapshot])) {
      throw new Error("session-relationships-changed-after-inventory")
    }
    if (
      !sameIDs(
        current.deletionScope.sessionIDs,
        inventory.selectedSessions.map((item) => item.sessionID),
      )
    ) {
      throw new Error("inventory-scope-inconsistent")
    }

    const beforeIntegrity = integrityResult(db)
    const beforeForeignKeys = foreignKeyResult(db)
    if (beforeIntegrity.state !== "ok") throw new Error(`integrity-check-${beforeIntegrity.state}`)
    if (beforeForeignKeys.state !== "ok") throw new Error(`foreign-key-check-${beforeForeignKeys.state}`)

    for (const session of current.selectedSessions) {
      if ((await inspectDirectory(session.directory!)) !== "missing")
        throw new Error("session-directory-restored-before-delete")
    }
    Object.assign(deletedByTable, deleteSessions(db, current.deletionScope.sessionIDs))
    changedRows = Object.values(deletedByTable).reduce((sum, count) => sum + count, 0)
    const afterIntegrity = integrityResult(db)
    const afterForeignKeys = foreignKeyResult(db)
    if (afterIntegrity.state !== "ok") throw new Error(`integrity-check-after-delete-${afterIntegrity.state}`)
    if (afterForeignKeys.state !== "ok") throw new Error(`foreign-key-check-after-delete-${afterForeignKeys.state}`)

    const holdersBeforeCommit = scanDatabaseHolders(filename)
    if (holdersBeforeCommit.state === "unreadable") {
      throw new Error(`database-liveness-unreadable:${holdersBeforeCommit.reason}`)
    }
    if (holdersBeforeCommit.pids.length > 0) {
      throw new Error(`database-in-use-by-process:${holdersBeforeCommit.pids.join(",")}`)
    }
    db.exec("COMMIT")
  } catch (error) {
    if (db.inTransaction) db.exec("ROLLBACK")
    return {
      ...inventory,
      action: "refused",
      refusalReasons: [error instanceof Error ? error.message : "apply-failed"],
      changedRows: 0,
    }
  }

  const checkpoint = checkpointDatabase(db)
  const vacuum = (() => {
    if ("error" in checkpoint || checkpoint.busy !== 0) return { state: "skipped", reason: "checkpoint-incomplete" }
    try {
      const holders = scanDatabaseHolders(filename)
      if (holders.state === "unreadable") throw new Error(`database-liveness-unreadable:${holders.reason}`)
      if (holders.pids.length > 0) throw new Error(`database-in-use-by-process:${holders.pids.join(",")}`)
      db.exec("VACUUM")
      return { state: "complete" }
    } catch (error) {
      return { state: "failed", reason: error instanceof Error ? error.message : "unknown" }
    }
  })()
  // VACUUM can populate a WAL even after shrinking the main file.
  const vacuumCheckpoint = vacuum.state === "complete" ? checkpointDatabase(db) : undefined
  const afterAllocated = await allocatedBytes(filename)
  const beforeAllocated = inventory.reclamation.allocatedBytes
  const reclaimedBytes =
    beforeAllocated.status === "measured" && afterAllocated.status === "measured"
      ? beforeAllocated.totalBytes - afterAllocated.totalBytes
      : undefined
  const allocatedBytesByPath = beforeAllocated.files.map((before) => {
    const after = afterAllocated.files.find((item) => item.path === before.path)
    const allocatedBytesBefore = before.status === "measured" ? before.allocatedBytes : null
    const allocatedBytesAfter = after?.status === "measured" ? after.allocatedBytes : null
    return {
      path: before.path,
      allocatedBytesBefore,
      allocatedBytesAfter,
      reclaimedBytes:
        allocatedBytesBefore !== null && allocatedBytesAfter !== null
          ? allocatedBytesBefore - allocatedBytesAfter
          : null,
    }
  })
  const postIntegrity = integrityResult(db)
  const postForeignKeys = foreignKeyResult(db)
  const reclamationReasons = []
  if ("error" in checkpoint) reclamationReasons.push(`wal-checkpoint-failed:${checkpoint.error}`)
  else if (checkpoint.busy !== 0) reclamationReasons.push("wal-checkpoint-did-not-complete")
  if (vacuum.state !== "complete") reclamationReasons.push(`vacuum-${vacuum.state}:${vacuum.reason}`)
  if (vacuumCheckpoint && "error" in vacuumCheckpoint)
    reclamationReasons.push(`vacuum-checkpoint-failed:${vacuumCheckpoint.error}`)
  else if (vacuumCheckpoint && vacuumCheckpoint.busy !== 0)
    reclamationReasons.push("vacuum-checkpoint-did-not-complete")
  if (afterAllocated.status !== "measured")
    reclamationReasons.push("allocated-byte-measurement-after-apply-unavailable")
  if (reclaimedBytes === undefined || (changedRows > 0 && reclaimedBytes <= 0))
    reclamationReasons.push("allocated-bytes-not-reclaimed")
  if (postIntegrity.state !== "ok") reclamationReasons.push(`integrity-check-after-checkpoint-${postIntegrity.state}`)
  if (postForeignKeys.state !== "ok")
    reclamationReasons.push(`foreign-key-check-after-checkpoint-${postForeignKeys.state}`)
  const action = reclamationReasons.length === 0 ? (changedRows > 0 ? "applied" : "noop") : "failed"
  return {
    ...inventory,
    action,
    committed: true,
    changedRows,
    deletedSessionIDs: inventory.deletionScope.sessionIDs,
    deletedCounts: { sessions: deletedByTable.session ?? 0, ownedRows: changedRows, byTable: deletedByTable },
    reclamation: {
      ...inventory.reclamation,
      allocatedBytesBefore: beforeAllocated,
      allocatedBytesAfter: afterAllocated,
      reclaimedBytes,
      allocatedBytesByPath,
      checkpoint,
      vacuum,
      vacuumCheckpoint,
      integrityAfter: postIntegrity,
      foreignKeysAfter: postForeignKeys,
      reasons: reclamationReasons,
    },
    ...(reclamationReasons.length === 0 ? {} : { refusalReasons: reclamationReasons }),
  }
}

function checkpointDatabase(db: SqliteAccess) {
  try {
    return (
      db.query<{ busy: number }, []>("PRAGMA wal_checkpoint(TRUNCATE)").get() ?? {
        error: "wal-checkpoint-returned-no-result",
      }
    )
  } catch (error) {
    return { error: error instanceof Error ? error.message : "unknown" }
  }
}

function deleteSessions(db: SqliteAccess, sessionIDs: readonly string[]) {
  return Object.fromEntries(
    storageScopes.map(([table, scope]) => [
      table,
      sessionIDs.reduce((sum, id) => sum + db.query(`DELETE FROM ${quote(table)} WHERE ${scope}`).run(id).changes, 0),
    ]),
  )
}

function ownedRows(db: SqliteAccess, sessionID: string) {
  return Object.fromEntries(
    storageScopes.map(([table, scope]) => [
      table,
      db
        .query<{ rows: number }, [string]>(`SELECT count(*) AS rows FROM ${quote(table)} WHERE ${scope}`)
        .get(sessionID)!.rows,
    ]),
  )
}

function validDirectory(directory: string) {
  return (
    typeof directory === "string" &&
    directory.length > 0 &&
    !directory.includes("\0") &&
    path.isAbsolute(directory) &&
    !directory.split(path.sep).includes("..")
  )
}

async function inspectDirectory(directory: string) {
  if (!validDirectory(directory)) return "unknown"
  const root = path.parse(directory).root
  let current = root
  for (const segment of directory.slice(root.length).split(path.sep).filter(Boolean)) {
    current = path.join(current, segment)
    try {
      const entry = await lstat(current)
      // Do not infer disappearance through an inaccessible or dangling symlink ancestor.
      if (entry.isSymbolicLink()) return "unknown"
      if (!entry.isDirectory()) return "exists"
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT") return "missing"
      return "unknown"
    }
  }
  return "exists"
}

function inspectSchema(db: SqliteAccess) {
  const tables = new Set(
    db
      .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
      .all()
      .map((row) => row.name),
  )
  const failures = new Array<string>()
  const columnMap = new Map<string, Set<string>>()
  for (const [table, columns] of Object.entries(requiredColumns)) {
    if (!tables.has(table)) {
      failures.push(`required-table-missing:${table}`)
      continue
    }
    const present = new Set(
      db
        .query<{ name: string }, []>(`PRAGMA table_info(${quote(table)})`)
        .all()
        .map((row) => row.name),
    )
    columnMap.set(table, present)
    for (const column of columns) {
      if (!present.has(column)) failures.push(`required-column-missing:${table}.${column}`)
    }
  }

  const unclassifiedTables = [...tables].filter((table) => {
    if (ownedSessionTables.has(table) || table === "event_writer_process") return false
    const columns = db
      .query<{ name: string }, []>(`PRAGMA table_info(${quote(table)})`)
      .all()
      .map((row) => row.name)
    if (columns.includes("session_id") || columns.includes("aggregate_id")) return true
    return db
      .query<{ table: string }, []>(`PRAGMA foreign_key_list(${quote(table)})`)
      .all()
      .some((foreignKey) => ownedSessionTables.has(foreignKey.table))
  })
  return { tables, columnMap, failures: failures.toSorted(), unclassifiedTables: unclassifiedTables.toSorted() }
}

function aggregateOwnershipFailures(db: SqliteAccess, sessionIDs: ReadonlySet<string>, tables: ReadonlySet<string>) {
  const failures = new Set<string>()
  for (const table of aggregateTables) {
    if (!tables.has(table)) continue
    const rows = db.query<{ aggregate_id: string }, []>(`SELECT DISTINCT aggregate_id FROM ${quote(table)}`).all()
    for (const row of rows) {
      if (!sessionIDs.has(row.aggregate_id)) failures.add(`unknown-aggregate-owner:${table}:${row.aggregate_id}`)
    }
  }
  if (tables.has("event")) {
    const types = db.query<{ type: string }, []>("SELECT DISTINCT type FROM event").all()
    for (const row of types) {
      const definition = Durable.get(row.type)
      if (!definition?.durable || definition.durable.aggregate !== "sessionID") {
        failures.add(`unknown-durable-event-owner:${row.type}`)
      }
    }
  }
  return [...failures].toSorted()
}

function crossSessionCascadeFailures(db: SqliteAccess, sessionIDs: readonly string[]) {
  if (sessionIDs.length === 0) return []
  const cascadesParts = db
    .query<{ table: string; from: string; to: string; on_delete: string }, []>("PRAGMA foreign_key_list(part)")
    .all()
    .some(
      (foreignKey) =>
        foreignKey.table === "message" &&
        foreignKey.from === "message_id" &&
        foreignKey.to === "id" &&
        foreignKey.on_delete.toUpperCase() === "CASCADE",
    )
  if (!cascadesParts) return []

  const selected = new Set(sessionIDs)
  return db
    .query<{ parentSessionID: string; childSessionID: string; messageID: string }, []>(
      `SELECT message.session_id AS parentSessionID, part.session_id AS childSessionID, message.id AS messageID
       FROM message
       JOIN part ON part.message_id = message.id
       WHERE message.session_id <> part.session_id
       ORDER BY message.session_id, part.session_id, message.id`,
    )
    .all()
    .filter((row) => selected.has(row.parentSessionID) !== selected.has(row.childSessionID))
    .map(
      (row) => `cross-session-cascade:part.message_id:${row.messageID}:${row.parentSessionID}->${row.childSessionID}`,
    )
    .toSorted()
}

function activityBySession(
  db: SqliteAccess,
  tables: ReadonlySet<string>,
  columnMap: ReadonlyMap<string, ReadonlySet<string>>,
) {
  const result = new Map<string, { lastActivity: number; unreadable: boolean }>()
  for (const source of activitySources) {
    if (!tables.has(source.table) || source.columns.some((column) => !columnMap.get(source.table)?.has(column.name))) {
      continue
    }
    const values = source.columns
      .map(
        (column) =>
          `SELECT ${quote(source.scope)} AS session_id, ${quote(column.name)} AS activity, ${column.optional ? 1 : 0} AS optional FROM ${quote(source.table)}`,
      )
      .join(" UNION ALL ")
    const rows = db
      .query<{ session_id: string; newest: number | null; unreadable: number }, []>(
        `
        SELECT session_id,
          max(CASE WHEN typeof(activity) = 'integer' THEN activity END) AS newest,
          sum(CASE WHEN typeof(activity) = 'integer' OR (activity IS NULL AND optional = 1) THEN 0 ELSE 1 END) AS unreadable
        FROM (${values})
        GROUP BY session_id
      `,
      )
      .all()
    for (const row of rows) {
      const current = result.get(row.session_id)
      result.set(row.session_id, {
        lastActivity: Math.max(
          current?.lastActivity ?? Number.NEGATIVE_INFINITY,
          row.newest ?? Number.NEGATIVE_INFINITY,
        ),
        unreadable: Boolean(current?.unreadable || row.unreadable),
      })
    }
  }
  return result
}

function writerFailures(db: SqliteAccess, sessionID: string) {
  const rows = db
    .query<{ pid: number | null; boot_id: string | null; start_ticks: string | null }, [string]>(
      `SELECT process.pid, process.boot_id, process.start_ticks
       FROM event_aggregate_writer writer
       LEFT JOIN event_writer_process process ON process.writer_id = writer.writer_id
       WHERE writer.aggregate_id = ?`,
    )
    .all(sessionID)
  const failures = new Set<string>()
  for (const row of rows) {
    if (row.pid === null || !row.boot_id || !row.start_ticks) {
      failures.add("writer-liveness-unreadable")
      continue
    }
    const current = EventWriter.readIdentity(row.pid)
    if (!current) continue
    if (!current.bootID || !current.startTicks) {
      failures.add("writer-liveness-unreadable")
      continue
    }
    if (current.bootID === row.boot_id && current.startTicks === row.start_ticks) {
      failures.add("session-written-by-live-process")
    }
  }
  return [...failures]
}

function sessionWorkFailures(db: SqliteAccess, sessionID: string) {
  const row = db
    .query<
      {
        compacting: number
        unreadableV1Message: number
        unfinishedV1: number
        unreadableV2Message: number
        unfinishedV2: number
        unreadablePart: number
        runningPart: number
        pendingV1: number
        preparingV1: number
        malformedV1Queue: number
        pendingV2: number
        invalidV2Delivery: number
        pendingReceipt: number
        invalidReceipt: number
        pendingAsk: number
        activeRetention: number
      },
      [string]
    >(
      `SELECT
        (SELECT count(*) FROM session WHERE id = ?1 AND time_compacting IS NOT NULL) AS compacting,
        (SELECT count(*) FROM message WHERE session_id = ?1 AND json_valid(data) = 0) AS unreadableV1Message,
        (SELECT count(*) FROM message WHERE session_id = ?1 AND json_valid(data) = 1 AND json_extract(data, '$.role') = 'assistant' AND coalesce(json_type(data, '$.time.completed'), 'missing') NOT IN ('integer', 'real')) AS unfinishedV1,
        (SELECT count(*) FROM session_message WHERE session_id = ?1 AND json_valid(data) = 0) AS unreadableV2Message,
        (SELECT count(*) FROM session_message WHERE session_id = ?1 AND type = 'assistant' AND json_valid(data) = 1 AND coalesce(json_type(data, '$.time.completed'), 'missing') NOT IN ('integer', 'real')) AS unfinishedV2,
        (SELECT count(*) FROM part WHERE session_id = ?1 AND json_valid(data) = 0) AS unreadablePart,
        (SELECT count(*) FROM part WHERE session_id = ?1 AND json_valid(data) = 1 AND json_extract(data, '$.state.status') IN ('pending', 'running')) AS runningPart,
        (SELECT count(*) FROM session_prompt_queue WHERE session_id = ?1 AND time_promoted IS NULL AND time_withdrawn IS NULL) AS pendingV1,
        (SELECT count(*) FROM session_prompt_queue WHERE session_id = ?1 AND time_preparing IS NOT NULL) AS preparingV1,
        (SELECT count(*) FROM session_prompt_queue WHERE session_id = ?1 AND time_promoted IS NOT NULL AND time_withdrawn IS NOT NULL) AS malformedV1Queue,
        (SELECT count(*) FROM session_input WHERE session_id = ?1 AND promoted_seq IS NULL) AS pendingV2,
        (SELECT count(*) FROM session_input WHERE session_id = ?1 AND delivery NOT IN ('steer', 'queue')) AS invalidV2Delivery,
        (SELECT count(*) FROM session_prompt_receipt WHERE session_id = ?1 AND state = 'pending') AS pendingReceipt,
        (SELECT count(*) FROM session_prompt_receipt WHERE session_id = ?1 AND state NOT IN ('pending', 'withdrawn', 'delivered', 'merged', 'superseded', 'deleted')) AS invalidReceipt,
        (SELECT count(*) FROM session_ask WHERE session_id = ?1 AND time_settled IS NULL) AS pendingAsk,
        (SELECT count(*) FROM event_retention WHERE aggregate_id = ?1 AND state IN ('scanning', 'redacting')) AS activeRetention`,
    )
    .get(sessionID)
  if (!row) return ["session-work-state-unreadable"]
  const failures = new Set<string>()
  if (row.compacting) failures.add("session-compaction-in-flight")
  if (row.unreadableV1Message || row.unreadableV2Message || row.unreadablePart)
    failures.add("session-work-state-unreadable")
  if (row.unfinishedV1 || row.unfinishedV2 || row.runningPart) failures.add("assistant-or-tool-work-in-flight")
  if (row.pendingV1 || row.preparingV1 || row.pendingReceipt) failures.add("v1-prompt-pending-or-in-custody")
  if (row.malformedV1Queue) failures.add("v1-queue-state-unreadable")
  if (row.pendingV2) failures.add("v2-input-pending")
  if (row.invalidV2Delivery || row.invalidReceipt) failures.add("session-input-state-unreadable")
  if (row.pendingAsk) failures.add("supervised-ask-pending")
  if (row.activeRetention) failures.add("session-retention-in-flight")
  return [...failures]
}

function descendants(sessionID: string, sessions: readonly SessionRow[]) {
  const children = new Map<string, string[]>()
  for (const row of sessions) {
    if (!row.parent_id) continue
    const current = children.get(row.parent_id) ?? []
    current.push(row.id)
    children.set(row.parent_id, current)
  }
  const result = new Set<string>()
  const pending = [...(children.get(sessionID) ?? [])]
  while (pending.length > 0) {
    const current = pending.pop()!
    if (result.has(current) || current === sessionID) continue
    result.add(current)
    pending.push(...(children.get(current) ?? []))
  }
  return [...result]
}

function hasParentCycle(sessionID: string, parents: ReadonlyMap<string, string | null>) {
  const seen = new Set<string>()
  let current: string | null | undefined = sessionID
  while (current) {
    if (seen.has(current)) return true
    seen.add(current)
    current = parents.get(current)
  }
  return false
}

function integrityResult(db: SqliteAccess) {
  try {
    const rows = db.query<{ integrity_check: string }, []>("PRAGMA integrity_check").all()
    return rows.length === 1 && rows[0]?.integrity_check === "ok"
      ? { state: "ok" as const, details: ["ok"] }
      : { state: "failed" as const, details: rows.map((row) => row.integrity_check) }
  } catch {
    return { state: "unreadable" as const, details: [] }
  }
}

function foreignKeyResult(db: SqliteAccess) {
  try {
    const rows = db
      .query<{ table: string; rowid: number | null; parent: string; fkid: number }, []>("PRAGMA foreign_key_check")
      .all()
    return rows.length === 0 ? { state: "ok" as const, violations: [] } : { state: "failed" as const, violations: rows }
  } catch {
    return { state: "unreadable" as const, violations: [] }
  }
}

async function allocatedBytes(filename: string) {
  const paths = [filename, ...sidecars.map((suffix) => filename + suffix)]
  const files = await Promise.all(
    paths.map(async (item) => {
      try {
        const current = await stat(item)
        if (!Number.isFinite(current.blocks)) return { path: item, status: "unavailable" as const }
        return {
          path: item,
          status: "measured" as const,
          allocatedBytes: current.blocks * 512,
          sizeBytes: current.size,
        }
      } catch (error) {
        if (error instanceof Error && "code" in error && error.code === "ENOENT") {
          return { path: item, status: "measured" as const, allocatedBytes: 0, sizeBytes: 0 }
        }
        return { path: item, status: "unavailable" as const }
      }
    }),
  )
  if (files.some((file) => file.status !== "measured")) {
    return { status: "unavailable" as const, reason: "filesystem-allocation-unreadable", files }
  }
  return {
    status: "measured" as const,
    totalBytes: files.reduce((sum, file) => sum + (file.status === "measured" ? file.allocatedBytes : 0), 0),
    files,
  }
}

async function resolveDatabaseFile(filename: string) {
  if (filename === ":memory:") return { reason: "in-memory-database-has-no-allocated-file" }
  const absolute = path.resolve(filename)
  try {
    const [canonical, entry] = await Promise.all([realpath(absolute), lstat(absolute)])
    if (canonical !== absolute || !entry.isFile() || entry.isSymbolicLink()) {
      return { reason: "database-path-is-not-a-canonical-regular-file" }
    }
    return { filename: canonical }
  } catch {
    return { reason: "database-file-unreadable" }
  }
}

function measurementReport(args: PruneArgs) {
  return {
    firstmateRun: args.firstmateRanIt ? "confirmed" : "not-confirmed",
    measurementStatus: args.measurementStatus ?? "unavailable",
    durableLedgerStatus: args.usageLedgerStatus ?? "unavailable",
    source: "reported by the caller; measurement and ledger state do not gate pruning",
  }
}

function parseCutoff(input: string | undefined) {
  if (!input) return undefined
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.exec(input)
  if (!match) return undefined
  const year = Number(match[1])
  const month = Number(match[2])
  const day = Number(match[3])
  const hour = Number(match[4])
  const daysInMonth = [
    31,
    year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0) ? 29 : 28,
    31,
    30,
    31,
    30,
    31,
    31,
    30,
    31,
    30,
    31,
  ][month - 1]
  if (!daysInMonth || day < 1 || day > daysInMonth || hour > 23) return undefined
  const result = Date.parse(input)
  return Number.isFinite(result) && result <= Date.now() ? result : undefined
}

function normalizeSessionIDs(input: readonly string[] | undefined) {
  if (input === undefined) return undefined
  if (input.length === 0 || input.some((item) => !item.trim())) return null
  return [...new Set(input.map((item) => item.trim()))].toSorted()
}

function sameIDs(left: readonly string[], right: readonly string[]) {
  const sorted = right.toSorted()
  return left.length === sorted.length && left.toSorted().every((id, index) => id === sorted[index])
}

function sameParents(
  left: readonly (readonly [string, string | null])[],
  right: readonly (readonly [string, string | null])[],
) {
  const sorted = right.toSorted(([leftID], [rightID]) => leftID.localeCompare(rightID))
  return (
    left.length === sorted.length &&
    left
      .toSorted(([leftID], [rightID]) => leftID.localeCompare(rightID))
      .every(([id, parent], index) => {
        const other = sorted[index]
        return id === other?.[0] && parent === other[1]
      })
  )
}

function refusedReport(
  filename: string,
  cutoff: number | undefined,
  requested: readonly string[] | undefined,
  usage: ReturnType<typeof measurementReport>,
  reasons: string[],
) {
  return {
    action: "refused",
    database: filename,
    cutoff: cutoff === undefined ? null : new Date(cutoff).toISOString(),
    explicitSessionIDs: requested ?? null,
    selectedSessions: [],
    excludedSessions: [],
    deletionScope: { sessionIDs: [], wholeSessions: true },
    reclamation: { ready: false, reasons },
    usageMeasurement: usage,
    rawTranscriptWarning: "Applying this command permanently deletes the raw transcripts in selected Sessions.",
    changedRows: 0,
  }
}

function quote(identifier: string) {
  return `"${identifier.replaceAll('"', '""')}"`
}
