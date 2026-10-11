import { createHash } from "node:crypto"
import { spawnSync } from "node:child_process"
import { chmodSync, constants, lstatSync, readdirSync, readFileSync, realpathSync, statSync } from "node:fs"
import { chmod, copyFile, lstat, mkdtemp, open, realpath, rename, rm, stat, statfs, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { Database } from "@opencode-ai/core/database/database"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { Durable } from "@opencode-ai/schema/durable-event-manifest"
import { cmd } from "./cmd"

type SessionRow = { id: string; parent_id: string | null; time_created: number }
type AggregateRow = { aggregate_id: string; seq: number; owner_id: string | null }
type EventTypeRow = { type: string; rows: number }
type MarkerRow = { state: "scanning" | "redacting" | "complete"; evidence: string }
type TableRow = { name: string }
type ColumnRow = { name: string }
type CopyInventory = { copy: string; rows: number; bytes: number }

export interface SqliteAccess {
  readonly filename: string
  query<Row = unknown>(
    statement: string,
  ): {
    all(...bindings: Array<string | number>): Row[]
    get(...bindings: Array<string | number>): Row | undefined
    run(...bindings: Array<string | number>): { changes: number }
  }
  transaction<Result>(callback: () => Result): { (): Result; deferred(): Result; immediate(): Result }
  readonly inTransaction: boolean
  serialize(): Uint8Array
  exec(source: string): void
  close(): void
}

export type RetentionEvidence = {
  readonly customerBinding?: {
    readonly proofID: string
    readonly durable: boolean
    readonly sessionIDs: readonly string[]
    readonly customerBoundSessionIDs: readonly string[]
    readonly nonCustomerSessionIDs: readonly string[]
  }
  // The mixed /sync/history reader contract (swxtchio/swx-opencode#133 item 1) is a public
  // decision; until one is recorded, production evidence carries none and every tree refuses.
  readonly readerContract?: {
    readonly reviewed: boolean
    readonly id: string
  }
  // Produced by scanDatabaseHolders for the exact database file being inventoried or changed.
  readonly holders?: DatabaseHolderScan
  readonly handoff?: {
    readonly receiptID: string
    readonly durable: boolean
    readonly sessionIDs: readonly string[]
    readonly finalSequence: Readonly<Record<string, number>>
    readonly axes: Readonly<
      Record<
        | "billing"
        | "provider"
        | "servingModel"
        | "routeAttribution"
        | "reportedCost"
        | "inputTokens"
        | "outputTokens"
        | "reasoningTokens"
        | "cacheReadTokens"
        | "cacheWriteTokens"
        | "correctness"
        | "performance",
        { readonly status: "retained" } | { readonly status: "unavailable"; readonly cause: string }
      >
    >
    readonly report: {
      readonly windowStart: string
      readonly windowEnd: string
      readonly resultDigest: string
      readonly denominators: Readonly<Record<string, number>>
      readonly unavailableCauses: Readonly<Record<string, string>>
      readonly rawHistoryInaccessible: boolean
    }
  }
  readonly evidenceError?: string
}

export type DatabaseHolderScan =
  | {
      readonly state: "complete"
      // "memory" for an in-memory database, which no other process can open.
      readonly target: { readonly device: string; readonly inode: string } | "memory"
      // Other processes with an open handle on the database, WAL, or shared-memory file.
      readonly pids: readonly number[]
    }
  | { readonly state: "unreadable"; readonly reason: string }

// The captain-approved boundary (swxtchio/swx-firstmate#2052, 2026-10-11): a Session tree inactive for
// more than seven days is eligible; one inactive for seven days or less is never touched.
export const inactivityPolicy = {
  windowMs: 7 * 24 * 60 * 60 * 1000,
  reference: "swxtchio/swx-firstmate#2052",
} as const

export type RetentionTree = {
  readonly rootSessionID: string
  readonly sessionIDs: readonly string[]
  readonly aggregates: readonly {
    aggregateID: string
    seq: number
    ownerID: string | null
    rows: number
    bytes: number
    eventTypes: readonly string[]
  }[]
  readonly copies: readonly CopyInventory[]
  readonly retainedMetricFields: readonly string[]
  readonly reasons: readonly string[]
  readonly eligible: boolean
  readonly progressScope: string
}

export type RetentionInventory = {
  readonly generatedAt: number
  readonly evidenceSnapshot: RetentionEvidence
  readonly trees: readonly RetentionTree[]
  readonly unknownAggregates: readonly string[]
  readonly refusals: readonly string[]
}

type InventoryOptions = { readonly transactional?: boolean }

export type RetentionApplyResult = {
  readonly state: "complete" | "in-progress" | "refused"
  readonly changedRows: number
  readonly changedBytes: number
  readonly completedSessionIDs: readonly string[]
  readonly reasons: readonly string[]
  // Write-lock hold per batch, from the moment BEGIN IMMEDIATE holds the lock (including in-lock
  // revalidation) until COMMIT or ROLLBACK returns.
  readonly lockHold: LockHold
}

export type LockHold = { readonly batches: number; readonly maxMs: number; readonly totalMs: number }

export type CompactFixtureResult = {
  readonly state: "complete" | "refused"
  readonly changedFiles: number
  readonly reasons: readonly string[]
  readonly sourceDeviceID?: string
  readonly backupDeviceID?: string
  readonly stagingDeviceID?: string
  readonly measurements?: {
    readonly before: PhysicalMetrics
    readonly backup: PhysicalMetrics
    readonly staging: PhysicalMetrics
    readonly restored: PhysicalMetrics
  }
}

export type PhysicalMetrics = {
  readonly size: number
  readonly blocks: number
  readonly pageCount: number
  readonly freelistCount: number
  readonly integrityCheck: string
  readonly retainedSession:
    | {
        readonly id: string
        readonly cost: number
        readonly tokens_input: number
        readonly tokens_output: number
        readonly tokens_reasoning: number
        readonly tokens_cache_read: number
        readonly tokens_cache_write: number
      }
    | undefined
}

type CompactFixtureInput = {
  readonly sourcePath: string
  readonly sourceFixture?: FixtureDirectory
  readonly backupPath: string
  readonly stagingPath: string
  readonly expectedSourceDeviceID: string
  readonly expectedBackupDeviceID: string
  readonly expectedStagingDeviceID: string
  readonly retainedSessionID: string
  readonly capacityFloorBytes?: number
  readonly afterExclusiveLock?: () => Promise<void>
  readonly afterDestinationPreflight?: () => Promise<void>
}

type ApplyInput = {
  readonly tree: RetentionTree
  readonly evidence: () => RetentionEvidence
  readonly now?: () => number
  readonly batchSize?: number
  readonly maxBatches?: number
}

type RedactionRow = { cursor: string | number; value: string | number | null }
type FixtureIdentity = { readonly filename: string; readonly device: string; readonly inode: string }
type NativeSqliteDatabase = InstanceType<typeof import("bun:sqlite").Database>
type FixtureControl = {
  identity: FixtureIdentity
  database: NativeSqliteDatabase
  compacting: boolean
  removed: boolean
  lockPath: string
}
type FixtureDirectory = {
  readonly filename: string
  readonly db: NativeSqliteDatabase
  readonly reopen: () => Promise<NativeSqliteDatabase>
  readonly remove: () => Promise<void>
  readonly [Symbol.asyncDispose]: () => Promise<void>
}

const fixtureHandles = new WeakMap<object, FixtureIdentity>()
const fixtureControls = new WeakMap<object, FixtureControl>()

const redactionPlan = [
  { table: "event", scope: "aggregate_id", cursor: "seq", value: "data", kind: "json" },
  { table: "message", scope: "session_id", cursor: "id", value: "data", kind: "json" },
  { table: "part", scope: "session_id", cursor: "id", value: "data", kind: "json" },
  { table: "session_message", scope: "session_id", cursor: "seq", value: "data", kind: "json" },
  { table: "session_input", scope: "session_id", cursor: "id", value: "prompt", kind: "json" },
  { table: "session_context_epoch", scope: "session_id", cursor: "session_id", value: "snapshot", kind: "context" },
  { table: "todo", scope: "session_id", cursor: "position", value: "content", kind: "text" },
  { table: "session_share", scope: "session_id", cursor: "session_id", value: "url", kind: "delete" },
  { table: "session", scope: "id", cursor: "id", value: "title", kind: "session" },
] as const

const metricScalarKeys = new Set([
  "agent",
  "cost",
  "createdAt",
  "finish",
  "id",
  "messageID",
  "modelID",
  "partID",
  "providerID",
  "responseModelID",
  "responseModelIDs",
  "role",
  "seq",
  "sessionID",
  "status",
  "time_created",
  "time_updated",
  "timestamp",
  "type",
  "updatedAt",
  "variant",
])

const tokenMetricKeys = new Set(["input", "output", "reasoning", "total"])
const cacheMetricKeys = new Set(["read", "write"])
const timeMetricKeys = new Set(["created", "completed"])
const timingMetricKeys = new Set(["duration", "durationMs", "startedAt", "endedAt", "wallTime", "wallTimeMs"])

const metricObjectKeys = new Set([
  "cache",
  "correctness",
  "info",
  "message",
  "metrics",
  "model",
  "part",
  "payload",
  "performance",
  "quality",
  "tokens",
  "timing",
  "time",
  "usage",
])

function allRows<Row>(db: SqliteAccess, statement: string, ...bindings: Array<string | number>) {
  return db.query<Row>(statement).all(...bindings)
}

function oneRow<Row>(db: SqliteAccess, statement: string, ...bindings: Array<string | number>) {
  return db.query<Row>(statement).get(...bindings)
}

const retainedMetricFields = [
  "providerID",
  "modelID (requested model or route)",
  "model.id (requested route)",
  "responseModelID",
  "responseModelIDs (serving models)",
  "time.created",
  "time.completed",
  "tokens.input",
  "tokens.output",
  "tokens.reasoning",
  "tokens.cache.read",
  "tokens.cache.write",
  "cost",
] as const

const handoffAxes = [
  "billing",
  "provider",
  "servingModel",
  "routeAttribution",
  "reportedCost",
  "inputTokens",
  "outputTokens",
  "reasoningTokens",
  "cacheReadTokens",
  "cacheWriteTokens",
  "correctness",
  "performance",
] as const

const copyTables = [
  { copy: "session.title", table: "session", scope: "id", value: "title" },
  { copy: "session.slug", table: "session", scope: "id", value: "slug" },
  { copy: "session.directory", table: "session", scope: "id", value: "directory" },
  { copy: "session.path", table: "session", scope: "id", value: "path" },
  { copy: "session.share_url", table: "session", scope: "id", value: "share_url" },
  { copy: "session.summary_diffs", table: "session", scope: "id", value: "summary_diffs" },
  { copy: "session.metadata", table: "session", scope: "id", value: "metadata" },
  { copy: "session.revert", table: "session", scope: "id", value: "revert" },
  { copy: "session.permission", table: "session", scope: "id", value: "permission" },
  { copy: "message.data", table: "message", scope: "session_id", value: "data" },
  { copy: "part.data", table: "part", scope: "session_id", value: "data" },
  { copy: "session_message.data", table: "session_message", scope: "session_id", value: "data" },
  { copy: "session_input.prompt", table: "session_input", scope: "session_id", value: "prompt" },
  { copy: "session_context_epoch.snapshot", table: "session_context_epoch", scope: "session_id", value: "snapshot" },
  { copy: "session_context_epoch.baseline", table: "session_context_epoch", scope: "session_id", value: "baseline" },
  { copy: "session_prompt_queue.input", table: "session_prompt_queue", scope: "session_id", value: "input" },
  { copy: "todo.content", table: "todo", scope: "session_id", value: "content" },
  { copy: "session_share.id", table: "session_share", scope: "session_id", value: "id" },
  { copy: "session_share.url", table: "session_share", scope: "session_id", value: "url" },
  { copy: "session_share.secret", table: "session_share", scope: "session_id", value: "secret" },
] as const

const requiredRetentionTables = new Set([
  ...copyTables.map((item) => item.table),
  ...redactionPlan.map((item) => item.table),
  "event_sequence",
  "event_retention",
  "event_writer_process",
  "event_aggregate_writer",
  "session_prompt_queue_sequence",
])
const classifiedSessionTables = new Set([...requiredRetentionTables])

// Every producer timestamp a Session tree carries. Event rows have no timestamp, so appends are
// timed by event_aggregate_writer, which the EventV2 commit writes with each append.
const activityColumns = [
  { table: "session", scope: "id", required: ["time_created", "time_updated"], optional: ["time_archived"] },
  { table: "message", scope: "session_id", required: ["time_created", "time_updated"], optional: [] },
  { table: "part", scope: "session_id", required: ["time_created", "time_updated"], optional: [] },
  { table: "session_message", scope: "session_id", required: ["time_created", "time_updated"], optional: [] },
  { table: "session_input", scope: "session_id", required: ["time_created"], optional: [] },
  {
    table: "session_prompt_queue",
    scope: "session_id",
    required: ["time_created"],
    optional: ["time_promoted", "time_withdrawn"],
  },
  { table: "todo", scope: "session_id", required: ["time_created", "time_updated"], optional: [] },
  { table: "session_share", scope: "session_id", required: ["time_created", "time_updated"], optional: [] },
  { table: "event_aggregate_writer", scope: "aggregate_id", required: ["time_last_append"], optional: [] },
] as const

const jsonCopies = [
  { table: "message", scope: "session_id", value: "data" },
  { table: "part", scope: "session_id", value: "data" },
  { table: "session_message", scope: "session_id", value: "data" },
  { table: "session_input", scope: "session_id", value: "prompt" },
  { table: "session_context_epoch", scope: "session_id", value: "snapshot" },
  { table: "session_prompt_queue", scope: "session_id", value: "input" },
] as const

function retentionSchema(db: SqliteAccess) {
  const tables = new Set(
    allRows<TableRow>(db, "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").map(
      (row) => row.name,
    ),
  )
  const missingTables = Array.from(requiredRetentionTables).filter((table) => !tables.has(table))
  const unclassifiedSessionTables = Array.from(tables).flatMap((table) => {
    if (classifiedSessionTables.has(table)) return []
    const columns = allRows<ColumnRow>(db, `PRAGMA table_info(${quote(table)})`)
    return columns.some((column) => column.name === "session_id" || column.name === "aggregate_id") ? [table] : []
  })
  return { tables, missingTables, unclassifiedSessionTables }
}

function requiredTableReason(table: string) {
  return table === "event_retention"
    ? "unreplayable-marker-store-unavailable"
    : `${table.replaceAll("_", "-")}-table-unreadable`
}

function copyOwnershipFailures(db: SqliteAccess) {
  return copyTables.flatMap(({ table, scope }) => {
    const columns = allRows<ColumnRow>(db, `PRAGMA table_info(${quote(table)})`)
    if (columns.length === 0) return []
    return columns.some((column) => column.name === scope) ? [] : [`source-ownership-unreadable:${table}`]
  })
}

function eventAggregateFailures(
  db: SqliteAccess,
  sessionID: string,
  aggregate: AggregateRow | undefined,
  eventTypes: readonly string[],
  verifyPayloadOwnership = true,
) {
  const reasons = new Set<string>()
  const rowCount =
    oneRow<{ rows: number }>(db, "SELECT count(*) AS rows FROM event WHERE aggregate_id = ?", sessionID)?.rows ?? 0
  if (rowCount > 0 && !aggregate) reasons.add(`event-aggregate-sequence-missing:${sessionID}`)
  if (!verifyPayloadOwnership && rowCount > 0) {
    reasons.add(`event-payload-ownership-proof-unavailable:${sessionID}`)
  }
  const invalidJSON = verifyPayloadOwnership
    ? (oneRow<{ rows: number }>(
        db,
        "SELECT count(*) AS rows FROM event WHERE aggregate_id = ? AND json_valid(data) = 0",
        sessionID,
      )?.rows ?? 0)
    : 0
  if (verifyPayloadOwnership && invalidJSON > 0) reasons.add(`event-data-unreadable:${sessionID}`)
  for (const type of eventTypes) {
    const definition = Durable.get(type)
    if (!definition?.durable || definition.durable.aggregate !== "sessionID") {
      reasons.add(`event-aggregate-owner-unknown:${sessionID}:${type}`)
      continue
    }
    if (verifyPayloadOwnership && invalidJSON === 0) {
      const invalidOwner = oneRow<{ rows: number }>(
        db,
        "SELECT count(*) AS rows FROM event WHERE aggregate_id = ? AND type = ? AND json_extract(data, ?) IS NOT aggregate_id",
        sessionID,
        type,
        `$.${definition.durable.aggregate}`,
      )?.rows
      if (invalidOwner) reasons.add(`event-aggregate-owner-conflict:${sessionID}:${type}`)
    }
  }
  const duplicateSequence = oneRow<{ rows: number }>(
    db,
    "SELECT count(*) AS rows FROM (SELECT seq FROM event WHERE aggregate_id = ? GROUP BY seq HAVING count(*) > 1)",
    sessionID,
  )?.rows
  if (duplicateSequence) reasons.add(`event-sequence-conflict:${sessionID}`)
  return Array.from(reasons).toSorted()
}

export async function createFixtureDatabase(snapshot?: Uint8Array): Promise<FixtureDirectory> {
  const directory = await mkdtemp(path.join(tmpdir(), "opencode-retention-fixture-"))
  const filename = path.join(directory, "retention.sqlite")
  if (snapshot) await writeFile(filename, snapshot, { flag: "wx" })
  const sqlite = await import("bun:sqlite")
  const db = new sqlite.Database(filename)
  const identity = await readFixtureIdentity(db as unknown as SqliteAccess, filename)
  const control: FixtureControl = {
    identity,
    database: db,
    compacting: false,
    removed: false,
    lockPath: path.join(directory, ".retention-compaction.lock"),
  }
  fixtureHandles.set(db, identity)

  const remove = async () => {
    if (control.compacting) throw new Error("retention fixture cannot be removed during compaction")
    if (control.removed) return
    control.removed = true
    try {
      control.database.close()
    } catch {}
    await rm(directory, { recursive: true, force: true })
  }
  const fixture: FixtureDirectory = {
    filename,
    get db() {
      return control.database
    },
    reopen: async () => {
      if (control.compacting) throw new Error("retention fixture writer admission is closed during compaction")
      if (control.removed) throw new Error("retention fixture was already removed")
      if (!fixturePathMatches(control.identity)) throw new Error("retention fixture identity changed before reopen")
      const reopened = new sqlite.Database(filename) as unknown as SqliteAccess
      if (!(await fixtureHandleMatches(reopened, control.identity))) {
        reopened.close()
        throw new Error("retention fixture identity changed while reopening")
      }
      fixtureHandles.set(reopened, control.identity)
      control.database = reopened as unknown as NativeSqliteDatabase
      return control.database
    },
    remove,
    [Symbol.asyncDispose]: remove,
  }
  fixtureControls.set(fixture, control)
  return fixture
}

function makeProgressScope(
  rootSessionID: string,
  sessionIDs: readonly string[],
  aggregates: readonly { aggregateID: string; eventTypes: readonly string[] }[],
  evidence: RetentionEvidence,
) {
  const scopeEvidence = {
    inactivityWindowMs: inactivityPolicy.windowMs,
    inactivityReference: inactivityPolicy.reference,
    readerContractID: evidence.readerContract?.id ?? null,
    // Only this tree's projection of a store-wide proof, so classifying other Sessions later does not
    // invalidate this tree's restart-safe progress.
    customerBinding: evidence.customerBinding
      ? {
          proofID: evidence.customerBinding.proofID,
          durable: evidence.customerBinding.durable,
          customerBoundSessionIDs: evidence.customerBinding.customerBoundSessionIDs.filter((id) =>
            sessionIDs.includes(id),
          ),
          nonCustomerSessionIDs: evidence.customerBinding.nonCustomerSessionIDs.filter((id) =>
            sessionIDs.includes(id),
          ),
        }
      : null,
    receiptID: evidence.handoff?.receiptID ?? null,
  }
  return createHash("sha256")
    .update(
      JSON.stringify({
        rootSessionID,
        sessionIDs,
        aggregates: aggregates.map((item) => ({ aggregateID: item.aggregateID, eventTypes: item.eventTypes })),
        scopeEvidence,
      }),
    )
    .digest("hex")
}

function customerBindingFailures(evidence: RetentionEvidence, sessionIDs: readonly string[]) {
  const reasons = new Set<string>()
  const binding = evidence.customerBinding
  if (!binding) {
    reasons.add("customer-session-classification-unavailable")
    return Array.from(reasons)
  }
  if (!binding.proofID || binding.durable !== true) reasons.add("customer-session-classification-proof-unreadable")
  // A classification may cover a whole store; it must name every Session of this tree.
  const classified = new Set(binding.sessionIDs)
  if (sessionIDs.some((sessionID) => !classified.has(sessionID))) {
    reasons.add("customer-session-classification-tree-scope-mismatch")
  }
  const customerBound = new Set(binding.customerBoundSessionIDs)
  const nonCustomer = new Set(binding.nonCustomerSessionIDs)
  for (const sessionID of sessionIDs) {
    if (customerBound.has(sessionID) && nonCustomer.has(sessionID)) {
      reasons.add(`customer-session-classification-conflict:${sessionID}`)
      continue
    }
    if (customerBound.has(sessionID)) {
      reasons.add(`customer-bound-session-retention-workflow-unapproved:${sessionID}`)
      continue
    }
    if (!nonCustomer.has(sessionID)) reasons.add(`customer-session-binding-unknown:${sessionID}`)
  }
  return Array.from(reasons).toSorted()
}

export function inventory(
  db: SqliteAccess,
  evidence: RetentionEvidence = {},
  now = Date.now(),
  options?: InventoryOptions,
): RetentionInventory {
  if (db.inTransaction || options?.transactional === false) return inventorySnapshot(db, evidence, now)
  return db.transaction(() => inventorySnapshot(db, evidence, now)).deferred()
}

function inventorySnapshot(db: SqliteAccess, evidence: RetentionEvidence, now: number): RetentionInventory {
  const schema = retentionSchema(db)
  const tables = schema.tables
  const requiredRefusals = schema.missingTables.map(requiredTableReason)
  if (requiredRefusals.length > 0) {
    return {
      generatedAt: now,
      evidenceSnapshot: evidence,
      trees: [],
      unknownAggregates: [],
      refusals: requiredRefusals,
    }
  }
  const sessions = allRows<SessionRow>(db, "SELECT id, parent_id, time_created FROM session ORDER BY id")
  const sessionsByID = new Map(sessions.map((row) => [row.id, row]))
  const children = new Map<string, string[]>()
  for (const row of sessions) {
    if (row.parent_id === null) continue
    const ids = children.get(row.parent_id) ?? []
    ids.push(row.id)
    children.set(row.parent_id, ids)
  }

  const unknownAggregates = allRows<{ aggregate_id: string }>(
    db,
    "SELECT aggregate_id FROM event_sequence WHERE NOT EXISTS (SELECT 1 FROM session WHERE session.id = event_sequence.aggregate_id) ORDER BY aggregate_id",
  ).map((row) => row.aggregate_id)
  const components = new Array<{ rootSessionID: string; sessionIDs: string[]; unresolvedParent: boolean }>()
  const visited = new Set<string>()
  const visit = (rootSessionID: string, unresolvedParent = false) => {
    const sessionIDs = new Array<string>()
    const pending = [rootSessionID]
    while (pending.length > 0) {
      const sessionID = pending.pop()!
      if (visited.has(sessionID)) continue
      visited.add(sessionID)
      sessionIDs.push(sessionID)
      pending.push(...(children.get(sessionID) ?? []).toReversed())
    }
    components.push({ rootSessionID, sessionIDs: sessionIDs.toSorted(), unresolvedParent })
  }
  for (const row of sessions) {
    if (row.parent_id === null && !visited.has(row.id)) visit(row.id)
  }
  for (const row of sessions) {
    if (visited.has(row.id)) continue
    visit(row.id, row.parent_id !== null && !sessionsByID.has(row.parent_id))
  }
  const globalRefusals = new Set<string>()
  for (const table of schema.unclassifiedSessionTables) globalRefusals.add(`unclassified-session-owned-table:${table}`)
  for (const reason of copyOwnershipFailures(db)) globalRefusals.add(reason)

  const trees = components.map((component) => {
    const sessionIDs = component.sessionIDs
    const reasons = new Set<string>(globalRefusals)
    if (component.unresolvedParent) reasons.add("session-tree-parent-unreadable")
    if (
      sessionIDs.some((sessionID) => {
        const parentID = sessionsByID.get(sessionID)?.parent_id
        return parentID !== null && (!parentID || !sessionIDs.includes(parentID))
      })
    )
      reasons.add("session-tree-parent-unreadable")
    if (sessionIDs.some((sessionID) => parentCycle(sessionID, sessionsByID))) reasons.add("session-tree-cycle")
    for (const sessionID of sessionIDs) {
      if (!sessionsByID.has(sessionID)) reasons.add(`session-row-missing:${sessionID}`)
    }
    for (const reason of candidateFailures(db, sessionIDs, evidence, now)) reasons.add(reason)

    // Payload ownership is proved by a JSON scan of every event row, the dominant cost of a dry run on a
    // large store. A tree that is already refused for another reason skips it and stays refused.
    const verifyEventPayloadOwnership = reasons.size === 0
    for (const sessionID of sessionIDs) {
      const aggregate = oneRow<AggregateRow>(
        db,
        "SELECT aggregate_id, seq, owner_id FROM event_sequence WHERE aggregate_id = ?",
        sessionID,
      )
      const eventTypes = allRows<EventTypeRow>(
        db,
        "SELECT type, count(*) AS rows FROM event WHERE aggregate_id = ? GROUP BY type ORDER BY type",
        sessionID,
      )
      for (const reason of eventAggregateFailures(
        db,
        sessionID,
        aggregate,
        eventTypes.map((row) => row.type),
        verifyEventPayloadOwnership,
      )) {
        reasons.add(reason)
      }
      for (const { table, scope, value } of jsonCopies) {
        if (!tables.has(table)) continue
        const invalidJSON = oneRow<{ rows: number }>(
          db,
          `SELECT count(*) AS rows FROM ${quote(table)} WHERE ${quote(scope)} = ? AND json_valid(${quote(value)}) = 0`,
          sessionID,
        )?.rows
        if (invalidJSON) reasons.add(`session-copy-unreadable:${table}:${sessionID}`)
      }
    }

    const aggregates = sessionIDs.map((sessionID) => {
      const row = oneRow<AggregateRow>(
        db,
        "SELECT aggregate_id, seq, owner_id FROM event_sequence WHERE aggregate_id = ?",
        sessionID,
      )
      const rows = oneRow<{ rows: number; bytes: number }>(
        db,
        "SELECT count(*) AS rows, coalesce(sum(length(CAST(data AS BLOB))), 0) AS bytes FROM event WHERE aggregate_id = ?",
        sessionID,
      )
      const eventTypes = allRows<{ type: string }>(
        db,
        "SELECT DISTINCT type FROM event WHERE aggregate_id = ? ORDER BY type",
        sessionID,
      ).map((eventType) => eventType.type)
      return {
        aggregateID: sessionID,
        seq: row?.seq ?? -1,
        ownerID: row?.owner_id ?? null,
        rows: rows?.rows ?? 0,
        bytes: rows?.bytes ?? 0,
        eventTypes,
      }
    })
    const copies = copyTables.flatMap(({ copy, table, scope, value }) => {
      if (!tables.has(table)) return []
      const rows = oneRow<{ rows: number; bytes: number }>(
        db,
        `SELECT count(*) AS rows, coalesce(sum(length(CAST(${quote(value)} AS BLOB))), 0) AS bytes FROM ${quote(table)} WHERE ${quote(scope)} IN (${placeholders(sessionIDs.length)})`,
        ...sessionIDs,
      )
      return [{ copy, rows: rows?.rows ?? 0, bytes: rows?.bytes ?? 0 }]
    })
    const progressScope = makeProgressScope(component.rootSessionID, sessionIDs, aggregates, evidence)
    for (const sessionID of sessionIDs) {
      const marker = oneRow<MarkerRow>(
        db,
        "SELECT state, evidence FROM event_retention WHERE aggregate_id = ?",
        sessionID,
      )
      const storedScope = marker ? parseJson(marker.evidence)?.progressScope : undefined
      if (marker && storedScope === undefined) reasons.add(`retention-progress-unreadable:${sessionID}`)
      if (marker && marker.state !== "complete" && storedScope !== progressScope) {
        reasons.add(`retention-progress-scope-conflict:${sessionID}`)
      }
    }
    return {
      rootSessionID: component.rootSessionID,
      sessionIDs,
      aggregates,
      copies,
      retainedMetricFields,
      reasons: Array.from(reasons).toSorted(),
      eligible: reasons.size === 0,
      progressScope,
    }
  })

  return {
    generatedAt: now,
    evidenceSnapshot: evidence,
    trees,
    unknownAggregates,
    refusals: Array.from(globalRefusals).toSorted(),
  }
}

/**
 * The eligibility policy shared by the dry run and by every apply batch, which calls it again inside
 * its write transaction. Each refusal names what is unproved; an empty result means every gate passed.
 */
function candidateFailures(db: SqliteAccess, sessionIDs: readonly string[], evidence: RetentionEvidence, now: number) {
  const reasons = new Set<string>()
  if (evidence.evidenceError) reasons.add(`evidence-unreadable:${evidence.evidenceError}`)
  if (!evidence.readerContract?.reviewed || !evidence.readerContract.id) reasons.add("reader-contract-unreviewed")
  for (const reason of customerBindingFailures(evidence, sessionIDs)) reasons.add(reason)
  for (const reason of holderFailures(db, evidence.holders)) reasons.add(reason)
  for (const reason of handoffFailures(db, evidence, sessionIDs)) reasons.add(reason)
  for (const reason of inactivityFailures(db, sessionIDs, now)) reasons.add(reason)
  for (const reason of sessionWorkFailures(db, sessionIDs)) reasons.add(reason)
  return Array.from(reasons).toSorted()
}

/**
 * A tree is inactive only when its newest producer timestamp, across every Session in it, is more than
 * the policy window before now. A missing or non-numeric timestamp column cannot prove inactivity.
 */
function inactivityFailures(db: SqliteAccess, sessionIDs: readonly string[], now: number) {
  const reasons = new Set<string>()
  const ids = placeholders(sessionIDs.length)
  let lastActivity = Number.NEGATIVE_INFINITY
  for (const source of activityColumns) {
    const columns = new Set(allRows<ColumnRow>(db, `PRAGMA table_info(${quote(source.table)})`).map((row) => row.name))
    const missing = [source.scope, ...source.required].filter((column) => !columns.has(column))
    if (missing.length > 0) {
      reasons.add(`session-activity-evidence-unavailable:${source.table}.${missing.join(",")}`)
      continue
    }
    const present = [
      ...source.required.map((column) => ({ column, nullable: false })),
      ...source.optional.filter((column) => columns.has(column)).map((column) => ({ column, nullable: true })),
    ]
    for (const { column, nullable } of present) {
      const row = oneRow<{ newest: number | null; unreadable: number | null }>(
        db,
        `SELECT max(${quote(column)}) AS newest, sum(CASE WHEN typeof(${quote(column)}) = 'integer' THEN 0 WHEN ${quote(column)} IS NULL AND ${nullable ? 1 : 0} THEN 0 ELSE 1 END) AS unreadable FROM ${quote(source.table)} WHERE ${quote(source.scope)} IN (${ids})`,
        ...sessionIDs,
      )
      if (row?.unreadable) reasons.add(`session-activity-unreadable:${source.table}.${column}`)
      if (typeof row?.newest === "number") lastActivity = Math.max(lastActivity, row.newest)
    }
  }
  if (lastActivity === Number.NEGATIVE_INFINITY) reasons.add("session-activity-unreadable:no-timestamp")
  if (!Number.isFinite(now)) reasons.add("inactivity-clock-unreadable")
  if (lastActivity !== Number.NEGATIVE_INFINITY && !(now - lastActivity > inactivityPolicy.windowMs)) {
    reasons.add("session-tree-active-within-inactivity-window")
  }
  return Array.from(reasons).toSorted()
}

/**
 * Cross-process liveness. Opening the shared database is not ownership: a registered process holds a
 * Session only by having appended to it. Every process holding the file must be registered and still be
 * the same process, otherwise its ownership is unprovable and every tree refuses.
 */
function holderFailures(db: SqliteAccess, holders: DatabaseHolderScan | undefined) {
  if (!holders) return ["cross-process-liveness-proof-unavailable"]
  if (holders.state === "unreadable") return [`database-holder-scan-unreadable:${holders.reason}`]
  const reasons = new Set<string>()
  const target = fileIdentity(db.filename)
  if (
    target === undefined ||
    (holders.target === "memory" ? target !== "memory" : target === "memory" || target.device !== holders.target.device || target.inode !== holders.target.inode)
  ) {
    reasons.add("database-holder-scan-target-mismatch")
  }
  const self = EventWriter.self()
  if (!self.bootID || !self.startTicks) reasons.add("process-identity-unavailable")
  for (const pid of holders.pids) {
    const current = EventWriter.readIdentity(pid)
    if (!current) continue
    const registered = allRows<{ boot_id: string | null; start_ticks: string | null }>(
      db,
      "SELECT boot_id, start_ticks FROM event_writer_process WHERE pid = ?",
      pid,
    )
    if (!registered.some((row) => sameProcess(row, current))) reasons.add(`unregistered-database-holder:${pid}`)
  }
  return Array.from(reasons).toSorted()
}

/**
 * The evidence scan runs before the write lock, so a process that opens the database between that scan
 * and BEGIN IMMEDIATE is missing from it. Called while the lock is held, this rescans the full holder set:
 * every current holder must pass the same registration check, and any holder the evidence did not name
 * refuses the batch.
 */
function lockedHolderFailures(db: SqliteAccess, scanned: DatabaseHolderScan | undefined) {
  const current = scanDatabaseHolders(db.filename)
  const reasons = new Set(holderFailures(db, current))
  if (scanned?.state === "complete" && current.state === "complete") {
    for (const pid of current.pids) {
      if (!scanned.pids.includes(pid)) reasons.add(`database-holder-appeared-after-scan:${pid}`)
    }
  }
  return Array.from(reasons).toSorted()
}

function sessionWorkFailures(db: SqliteAccess, sessionIDs: readonly string[]) {
  const reasons = new Set<string>()
  const ids = placeholders(sessionIDs.length)
  const writers = allRows<{
    aggregate_id: string
    pid: number | null
    boot_id: string | null
    start_ticks: string | null
  }>(
    db,
    `SELECT writer.aggregate_id, process.pid, process.boot_id, process.start_ticks FROM event_aggregate_writer writer LEFT JOIN event_writer_process process ON process.writer_id = writer.writer_id WHERE writer.aggregate_id IN (${ids}) ORDER BY writer.aggregate_id`,
    ...sessionIDs,
  )
  for (const writer of writers) {
    if (writer.pid === null) {
      reasons.add(`session-writer-unregistered:${writer.aggregate_id}`)
      continue
    }
    if (!writer.boot_id || !writer.start_ticks) {
      reasons.add(`session-writer-liveness-unreadable:${writer.aggregate_id}`)
      continue
    }
    const current = EventWriter.readIdentity(writer.pid)
    if (!current) continue
    if (current.bootID === null || current.startTicks === null) {
      reasons.add(`session-writer-liveness-unreadable:${writer.aggregate_id}`)
      continue
    }
    if (sameProcess(writer, current)) reasons.add(`session-held-by-live-process:${writer.aggregate_id}`)
  }
  for (const sessionID of sessionIDs) {
    const owner = oneRow<{ owner_id: string | null }>(
      db,
      "SELECT owner_id FROM event_sequence WHERE aggregate_id = ?",
      sessionID,
    )
    if (owner?.owner_id) reasons.add(`session-owned-by-workspace:${sessionID}`)
    const unfinished = oneRow<{ v1: number; v2: number }>(
      db,
      `SELECT
        (SELECT count(*) FROM message WHERE session_id = ?1 AND (json_valid(data) = 0 OR (json_extract(data, '$.role') = 'assistant' AND coalesce(json_type(data, '$.time.completed'), 'missing') NOT IN ('integer', 'real')))) AS v1,
        (SELECT count(*) FROM session_message WHERE session_id = ?1 AND type = 'assistant' AND (json_valid(data) = 0 OR coalesce(json_type(data, '$.time.completed'), 'missing') NOT IN ('integer', 'real'))) AS v2`,
      sessionID,
    )
    if (unfinished?.v1 || unfinished?.v2) reasons.add(`unfinished-assistant-message:${sessionID}`)
  }
  const queueRows = oneRow<{ rows: number }>(
    db,
    `SELECT count(*) AS rows FROM session_prompt_queue WHERE session_id IN (${ids})`,
    ...sessionIDs,
  )?.rows
  if (queueRows) reasons.add("v1-prompt-queue-row-present")
  const inputs = oneRow<{ rows: number; pending: number }>(
    db,
    `SELECT count(*) AS rows, coalesce(sum(promoted_seq IS NULL), 0) AS pending FROM session_input WHERE session_id IN (${ids})`,
    ...sessionIDs,
  )
  if (inputs?.pending) reasons.add("v2-input-pending")
  else if (inputs?.rows) reasons.add("v2-input-promoted-row-present")
  return Array.from(reasons).toSorted()
}

function handoffFailures(db: SqliteAccess, evidence: RetentionEvidence, sessionIDs: readonly string[]) {
  const handoff = evidence.handoff
  if (!handoff) return ["measurement-handoff-receipt-unavailable"]
  const reasons = new Set<string>()
  if (!handoff.receiptID || handoff.durable !== true) reasons.add("measurement-handoff-receipt-unreadable")
  if (
    handoff.report.rawHistoryInaccessible !== true ||
    !handoff.report.resultDigest ||
    !validUtcWindow(handoff.report.windowStart, handoff.report.windowEnd)
  )
    reasons.add("report-not-reproduced-without-raw-history")
  for (const [axis, state] of Object.entries(handoff.axes)) {
    if (state.status === "unavailable" && (!state.cause || handoff.report.unavailableCauses[axis] !== state.cause)) {
      reasons.add(`measurement-axis-unavailable-cause-missing:${axis}`)
    }
    if (state.status === "retained" && axis in handoff.report.unavailableCauses) {
      reasons.add(`measurement-axis-retained-but-reported-unavailable:${axis}`)
    }
  }
  if (handoffAxes.some((axis) => !handoff.axes[axis])) reasons.add("measurement-handoff-axes-incomplete")
  if (Object.values(handoff.report.denominators).some((value) => !Number.isFinite(value) || value < 0)) {
    reasons.add("report-denominators-unreadable")
  }
  // A receipt may cover a whole store; it must cover every Session of this tree at its final write.
  const covered = new Set(handoff.sessionIDs)
  if (sessionIDs.some((sessionID) => !covered.has(sessionID))) reasons.add("handoff-tree-scope-mismatch")
  for (const sessionID of sessionIDs) {
    const seq = oneRow<{ seq: number }>(db, "SELECT seq FROM event_sequence WHERE aggregate_id = ?", sessionID)?.seq
    if (handoff.finalSequence[sessionID] !== (seq ?? -1)) {
      reasons.add(`measurement-handoff-does-not-cover-final-write:${sessionID}`)
    }
  }
  return Array.from(reasons).toSorted()
}

function sameProcess(
  registered: { readonly boot_id: string | null; readonly start_ticks: string | null },
  current: EventWriter.ProcessIdentity,
) {
  return (
    registered.boot_id !== null &&
    registered.start_ticks !== null &&
    registered.boot_id === current.bootID &&
    registered.start_ticks === current.startTicks
  )
}

function fileIdentity(filename: string) {
  if (filename === ":memory:" || filename === "") return "memory" as const
  try {
    const file = statSync(filename)
    return { device: String(file.dev), inode: String(file.ino) }
  } catch {
    return undefined
  }
}

/**
 * Lists every other process holding the database file, its WAL, or its shared-memory file, matched by
 * device and inode so a renamed or linked path cannot hide a holder. Two kernel sources are combined:
 * the open descriptors of this user's processes, and every process holding a POSIX lock on those inodes
 * in /proc/locks, which covers all users and processes whose descriptors are not inspectable. Every
 * SQLite connection to a WAL database holds such a lock on the shared-memory file while it is open.
 * The residual blind spot is a process of another user, or a non-inspectable one, that has the file open
 * without a SQLite lock.
 */
export function scanDatabaseHolders(filename: string): DatabaseHolderScan {
  const target = fileIdentity(filename)
  if (target === "memory") return { state: "complete", target, pids: [] }
  if (!target) return { state: "unreadable", reason: "database-file-unreadable" }
  const userID = process.getuid?.()
  if (userID === undefined) return { state: "unreadable", reason: "process-user-unavailable" }
  try {
    const directory = statSync(path.dirname(realpathSync(filename)))
    if (directory.uid !== userID || (directory.mode & 0o077) !== 0) {
      return { state: "unreadable", reason: "database-directory-admits-other-users" }
    }
    const files = [filename, `${filename}-wal`, `${filename}-shm`].flatMap((file) => {
      try {
        const stat = statSync(file, { bigint: true })
        return [{ dev: stat.dev, ino: stat.ino }]
      } catch {
        return []
      }
    })
    const inodes = new Set(files.map((file) => `${file.dev}:${file.ino}`))
    const opened = readdirSync("/proc").flatMap((entry) => {
      if (!/^\d+$/.test(entry) || Number(entry) === process.pid) return []
      return processDescriptors(entry, userID).some((descriptor) => {
        try {
          const file = statSync(`/proc/${entry}/fd/${descriptor}`, { bigint: true })
          return inodes.has(`${file.dev}:${file.ino}`)
        } catch {
          return false
        }
      })
        ? [Number(entry)]
        : []
    })
    const locked = lockHolders(files)
    if (locked === "unattributable") return { state: "unreadable", reason: "database-lock-holder-unattributable" }
    const pids = new Set([...opened, ...locked.filter((pid) => pid !== process.pid)])
    return { state: "complete", target, pids: Array.from(pids).toSorted((left, right) => left - right) }
  } catch (error) {
    return { state: "unreadable", reason: error instanceof Error ? error.message : "unknown" }
  }
}

function processDescriptors(pid: string, userID: number) {
  try {
    if (statSync(`/proc/${pid}`).uid !== userID) return []
    return readdirSync(`/proc/${pid}/fd`)
  } catch {
    // Exited, or not inspectable (for example a non-dumpable process); lockHolders still covers its
    // SQLite connections.
    return []
  }
}

function lockHolders(files: readonly { dev: bigint; ino: bigint }[]) {
  const keys = new Set(
    files.map((file) => {
      const major = ((file.dev >> 8n) & 0xfffn) | ((file.dev >> 32n) & ~0xfffn)
      const minor = (file.dev & 0xffn) | ((file.dev >> 12n) & ~0xffn)
      return `${major}:${minor}:${file.ino}`
    }),
  )
  const pids = new Array<number>()
  for (const line of readFileSync("/proc/locks", "utf8").split("\n")) {
    const match = /\s(-?\d+)\s+([0-9a-f]+):([0-9a-f]+):(\d+)\s/i.exec(line)
    if (!match) continue
    if (!keys.has(`${BigInt(`0x${match[2]}`)}:${BigInt(`0x${match[3]}`)}:${match[4]}`)) continue
    // An open-file-description lock reports pid -1 and cannot be attributed to a process.
    if (match[1] === "-1") return "unattributable" as const
    pids.push(Number(match[1]))
  }
  return pids
}

function revalidateCandidate(db: SqliteAccess, expected: RetentionTree, evidence: RetentionEvidence, now: number) {
  const reasons = new Set<string>()
  const schema = retentionSchema(db)
  for (const table of schema.missingTables) reasons.add(requiredTableReason(table))
  for (const table of schema.unclassifiedSessionTables) reasons.add(`unclassified-session-owned-table:${table}`)
  for (const reason of copyOwnershipFailures(db)) reasons.add(reason)
  if (expected.rootSessionID === "" || expected.sessionIDs.length === 0) reasons.add("session-tree-unreadable")
  if (reasons.size > 0) return Array.from(reasons).toSorted()
  for (const reason of candidateFailures(db, expected.sessionIDs, evidence, now)) reasons.add(reason)
  for (const reason of lockedHolderFailures(db, evidence.holders)) reasons.add(reason)

  const rows = allRows<SessionRow>(
    db,
    `WITH RECURSIVE tree(id, parent_id, time_created) AS (
      SELECT id, parent_id, time_created FROM session WHERE id = ?
      UNION
      SELECT child.id, child.parent_id, child.time_created FROM session child JOIN tree parent ON child.parent_id = parent.id
    )
    SELECT id, parent_id, time_created FROM tree ORDER BY id`,
    expected.rootSessionID,
  )
  const sessionIDs = rows.map((row) => row.id)
  if (!sameIDs(sessionIDs, expected.sessionIDs)) reasons.add("session-tree-changed")
  if (rows.find((row) => row.id === expected.rootSessionID)?.parent_id !== null) {
    reasons.add("session-tree-root-changed")
  }
  if (rows.some((row) => parentCycle(row.id, new Map(rows.map((item) => [item.id, item]))))) {
    reasons.add("session-tree-cycle")
  }

  const aggregates = new Array<{ aggregateID: string; eventTypes: string[] }>()
  for (const sessionID of expected.sessionIDs) {
    if (!rows.some((row) => row.id === sessionID)) {
      reasons.add(`session-row-missing:${sessionID}`)
      continue
    }
    const aggregate = oneRow<AggregateRow>(
      db,
      "SELECT aggregate_id, seq, owner_id FROM event_sequence WHERE aggregate_id = ?",
      sessionID,
    )
    const inventoried = expected.aggregates.find((item) => item.aggregateID === sessionID)
    if ((aggregate?.seq ?? -1) !== (inventoried?.seq ?? -1)) reasons.add(`event-appended-after-inventory:${sessionID}`)
    if ((aggregate?.owner_id ?? null) !== (inventoried?.ownerID ?? null)) {
      reasons.add(`cross-process-aggregate-owner-snapshot-mismatch:${sessionID}`)
    }
    const eventTypes = allRows<{ type: string }>(
      db,
      "SELECT DISTINCT type FROM event WHERE aggregate_id = ? ORDER BY type",
      sessionID,
    ).map((row) => row.type)
    aggregates.push({ aggregateID: sessionID, eventTypes })
    for (const reason of eventAggregateFailures(db, sessionID, aggregate, eventTypes)) reasons.add(reason)
    for (const { table, scope, value } of jsonCopies) {
      const invalidJSON = oneRow<{ rows: number }>(
        db,
        `SELECT count(*) AS rows FROM ${quote(table)} WHERE ${quote(scope)} = ? AND json_valid(${quote(value)}) = 0`,
        sessionID,
      )?.rows
      if (invalidJSON) reasons.add(`session-copy-unreadable:${table}:${sessionID}`)
    }
    const maxEvent = oneRow<{ seq: number | null }>(
      db,
      "SELECT max(seq) AS seq FROM event WHERE aggregate_id = ?",
      sessionID,
    )?.seq
    if ((aggregate?.seq ?? -1) !== (maxEvent ?? -1)) reasons.add(`event-sequence-changed:${sessionID}`)
    const marker = oneRow<MarkerRow>(
      db,
      "SELECT state, evidence FROM event_retention WHERE aggregate_id = ?",
      sessionID,
    )
    const markerEvidence = marker ? parseJson(marker.evidence) : undefined
    if (marker && marker.state !== "complete" && markerEvidence?.progressScope !== expected.progressScope) {
      reasons.add(`retention-progress-scope-conflict:${sessionID}`)
    }
  }
  if (
    !sameIDs(
      aggregates.flatMap((item) => item.eventTypes),
      expected.aggregates.flatMap((item) => item.eventTypes),
    )
  ) {
    reasons.add("event-type-set-changed")
  }
  if (makeProgressScope(expected.rootSessionID, sessionIDs, aggregates, evidence) !== expected.progressScope) {
    reasons.add("candidate-progress-scope-changed")
  }
  return Array.from(reasons).toSorted()
}

export function apply(db: SqliteAccess, input: ApplyInput): RetentionApplyResult {
  const now = input.now ?? Date.now
  const batchSize = input.batchSize ?? 32
  const maxBatches = input.maxBatches ?? 10_000
  let changedRows = 0
  let changedBytes = 0
  let batches = 0
  const completedSessionIDs = new Set<string>()
  const reasons = new Set<string>()
  const holds = new Array<number>()
  const lockHold = (): LockHold => ({
    batches: holds.length,
    maxMs: holds.reduce((max, value) => Math.max(max, value), 0),
    totalMs: holds.reduce((total, value) => total + value, 0),
  })

  if (!input.tree.eligible) {
    return {
      state: "refused",
      changedRows,
      changedBytes,
      completedSessionIDs: [],
      reasons: input.tree.reasons,
      lockHold: lockHold(),
    }
  }
  while (batches < maxBatches) {
    let batch: {
      readonly state: "progress" | "complete" | "refused"
      readonly sessionID?: string
      readonly changedRows: number
      readonly changedBytes: number
      readonly refusals?: readonly string[]
    }
    // Evidence is produced before the write lock and revalidated against the database inside it, where
    // the holders are also rescanned so none can arrive unchecked between the two.
    let evidence: RetentionEvidence
    try {
      evidence = input.evidence()
    } catch (error) {
      evidence = { evidenceError: error instanceof Error ? error.message : "unknown" }
    }
    let acquired: number | undefined
    try {
      batch = db
        .transaction(() => {
          acquired = performance.now()
          const proofFailures = revalidateCandidate(db, input.tree, evidence, now())
          if (proofFailures.length > 0) {
            return {
              state: "refused" as const,
              changedRows: 0,
              changedBytes: 0,
              refusals: proofFailures.length > 0 ? proofFailures : ["candidate-proof-changed"],
            }
          }
          const tree = input.tree
          for (const sessionID of tree.sessionIDs) {
            if (completedSessionIDs.has(sessionID)) continue
            const marker = oneRow<MarkerRow>(
              db,
              "SELECT state, evidence FROM event_retention WHERE aggregate_id = ?",
              sessionID,
            )
            if (marker?.state === "complete") {
              completedSessionIDs.add(sessionID)
              continue
            }
            const storedEvidence = marker ? parseJson(marker.evidence) : undefined
            const startIndex = marker
              ? Math.max(
                  0,
                  redactionPlan.findIndex((item) => item.table === storedEvidence?.progressTable),
                )
              : 0
            let cursor = marker ? storedEvidence?.progressID : undefined
            for (let index = startIndex; index < redactionPlan.length; index++) {
              const plan = redactionPlan[index]!
              const query = `SELECT ${quote(plan.cursor)} AS cursor, ${quote(plan.value)} AS value FROM ${quote(plan.table)} WHERE ${quote(plan.scope)} = ?${cursor === undefined ? "" : ` AND ${quote(plan.cursor)} > ?`} ORDER BY ${quote(plan.cursor)} LIMIT ?`
              const rows = allRows<RedactionRow>(
                db,
                query,
                sessionID,
                ...(cursor === undefined
                  ? []
                  : [plan.cursor === "seq" || plan.cursor === "position" ? Number(cursor) : String(cursor)]),
                batchSize,
              )
              if (rows.length === 0) {
                cursor = undefined
                continue
              }
              let batchChangedRows = 0
              let batchChangedBytes = 0
              for (const row of rows) {
                const result = redactRow(db, plan, sessionID, row)
                batchChangedRows += result.changedRows
                batchChangedBytes += result.changedBytes
              }
              const nextState = batchChangedRows > 0 ? "redacting" : (marker?.state ?? "scanning")
              const progress = JSON.stringify({
                progressScope: tree.progressScope,
                progressTable: plan.table,
                progressID: rows.at(-1)?.cursor,
                evidence,
              })
              if (marker) {
                db.query(
                  "UPDATE event_retention SET state = ?, progress_table = ?, progress_id = ?, evidence = ?, time_updated = ? WHERE aggregate_id = ?",
                ).run(nextState, plan.table, String(rows.at(-1)?.cursor), progress, now(), sessionID)
              } else {
                db.query(
                  "INSERT INTO event_retention (aggregate_id, state, progress_table, progress_id, evidence, time_started, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ).run(sessionID, nextState, plan.table, String(rows.at(-1)?.cursor), progress, now(), now())
              }
              return {
                state: "progress" as const,
                sessionID,
                changedRows: batchChangedRows,
                changedBytes: batchChangedBytes,
              }
            }
            if (marker?.state === "scanning") {
              db.query("DELETE FROM event_retention WHERE aggregate_id = ?").run(sessionID)
            } else if (marker?.state === "redacting") {
              db.query(
                "UPDATE event_retention SET state = 'complete', progress_table = NULL, progress_id = NULL, time_updated = ? WHERE aggregate_id = ?",
              ).run(now(), sessionID)
            }
            completedSessionIDs.add(sessionID)
            return { state: "complete" as const, sessionID, changedRows: 0, changedBytes: 0 }
          }
          return { state: "complete" as const, changedRows: 0, changedBytes: 0 }
        })
        .immediate()
      if (acquired !== undefined) holds.push(performance.now() - acquired)
    } catch (error) {
      if (acquired !== undefined) holds.push(performance.now() - acquired)
      reasons.add(`apply-transaction-failed:${error instanceof Error ? error.message : "unknown"}`)
      return {
        state: batches > 0 ? "in-progress" : "refused",
        changedRows,
        changedBytes,
        completedSessionIDs: Array.from(completedSessionIDs).toSorted(),
        reasons: Array.from(reasons).toSorted(),
        lockHold: lockHold(),
      }
    }
    if (batch.state === "refused") {
      for (const reason of batch.refusals ?? ["candidate-proof-changed"]) reasons.add(reason)
      return {
        state: batches > 0 ? "in-progress" : "refused",
        changedRows,
        changedBytes,
        completedSessionIDs: Array.from(completedSessionIDs).toSorted(),
        reasons: Array.from(reasons).toSorted(),
        lockHold: lockHold(),
      }
    }
    changedRows += batch.changedRows
    changedBytes += batch.changedBytes
    batches += 1
    if (batch.state === "complete" && completedSessionIDs.size === input.tree.sessionIDs.length) {
      return {
        state: "complete",
        changedRows,
        changedBytes,
        completedSessionIDs: Array.from(completedSessionIDs).toSorted(),
        reasons: [],
        lockHold: lockHold(),
      }
    }
  }
  reasons.add("apply-batch-backstop-reached")
  return {
    state: "in-progress",
    changedRows,
    changedBytes,
    completedSessionIDs: Array.from(completedSessionIDs).toSorted(),
    reasons: Array.from(reasons).toSorted(),
    lockHold: lockHold(),
  }
}

function redactRow(db: SqliteAccess, plan: (typeof redactionPlan)[number], sessionID: string, row: RedactionRow) {
  if (plan.kind === "delete") {
    const result = db.query(`DELETE FROM ${quote(plan.table)} WHERE ${quote(plan.scope)} = ?`).run(sessionID)
    return { changedRows: result.changes, changedBytes: byteLength(row.value) }
  }
  if (plan.kind === "session") {
    const before = oneRow<Record<string, string | null>>(
      db,
      "SELECT title, slug, directory, path, share_url, summary_diffs, metadata, revert, permission FROM session WHERE id = ?",
      sessionID,
    )
    if (!before) return { changedRows: 0, changedBytes: 0 }
    const after = {
      title: "",
      slug: "",
      directory: "",
      path: null,
      share_url: null,
      summary_diffs: null,
      metadata: null,
      revert: null,
      permission: null,
    }
    if (Object.entries(after).every(([key, value]) => before[key] === value)) return { changedRows: 0, changedBytes: 0 }
    db.query(
      "UPDATE session SET title = '', slug = '', directory = '', path = NULL, share_url = NULL, summary_diffs = NULL, metadata = NULL, revert = NULL, permission = NULL WHERE id = ?",
    ).run(sessionID)
    return {
      changedRows: 1,
      changedBytes: Object.entries(before).reduce(
        (total, [key, value]) => total + byteLength(value) - byteLength(after[key as keyof typeof after]),
        0,
      ),
    }
  }
  if (plan.kind === "context") {
    const before = oneRow<{ snapshot: string; baseline: string }>(
      db,
      "SELECT snapshot, baseline FROM session_context_epoch WHERE session_id = ?",
      sessionID,
    )
    if (!before) return { changedRows: 0, changedBytes: 0 }
    const snapshot = retainedJSON(before.snapshot)
    if (snapshot === before.snapshot && before.baseline === "") return { changedRows: 0, changedBytes: 0 }
    db.query("UPDATE session_context_epoch SET snapshot = ?, baseline = '' WHERE session_id = ?").run(
      snapshot,
      sessionID,
    )
    return {
      changedRows: 1,
      changedBytes: byteLength(before.snapshot) - byteLength(snapshot) + byteLength(before.baseline),
    }
  }
  const next = retainedValue(plan.kind, row.value)
  if (next === row.value) return { changedRows: 0, changedBytes: 0 }
  db.query(
    `UPDATE ${quote(plan.table)} SET ${quote(plan.value)} = ? WHERE ${quote(plan.scope)} = ? AND ${quote(plan.cursor)} = ?`,
  ).run(next, sessionID, row.cursor)
  return { changedRows: 1, changedBytes: byteLength(row.value) - byteLength(next) }
}

function retainedJSON(value: string) {
  return JSON.stringify(retainMetrics(JSON.parse(value))) ?? "{}"
}

function retainedValue(kind: "json" | "text", value: string | number | null) {
  return kind === "text" ? "" : retainedJSON(String(value))
}

function retainMetrics(value: unknown, key?: string): unknown {
  if (key === "responseModelIDs" && Array.isArray(value)) return value.filter((item) => typeof item === "string")
  if (key === "tokens") {
    const retained = retainNamedMetrics(value, tokenMetricKeys, (item) => typeof item === "number")
    if (retained && value !== null && typeof value === "object" && !Array.isArray(value)) {
      const cache = retainNamedMetrics(
        (value as Record<string, unknown>).cache,
        cacheMetricKeys,
        (item) => typeof item === "number",
      )
      if (cache) retained.cache = cache
    }
    return retained
  }
  if (key === "time")
    return retainNamedMetrics(value, timeMetricKeys, (item) => typeof item === "number" && Number.isFinite(item))
  if (key === "timing" || key === "performance") {
    return retainNamedMetrics(value, timingMetricKeys, (item) => typeof item === "number")
  }
  if (Array.isArray(value)) return value.map((item) => retainMetrics(item)).filter((item) => item !== undefined)
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).flatMap(([childKey, child]) => {
        if (metricScalarKeys.has(childKey)) return [[childKey, child]]
        if (!metricObjectKeys.has(childKey)) return []
        const retained = retainMetrics(child, childKey)
        return retained === undefined ? [] : [[childKey, retained]]
      }),
    )
  }
  return value
}

function retainNamedMetrics(
  value: unknown,
  keys: ReadonlySet<string>,
  keep: (value: unknown) => boolean,
): Record<string, unknown> | undefined {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return undefined
  return Object.fromEntries(Object.entries(value).filter(([key, child]) => keys.has(key) && keep(child)))
}

function sameRetainedRead(left: PhysicalMetrics, right: PhysicalMetrics) {
  return JSON.stringify(left.retainedSession) === JSON.stringify(right.retainedSession)
}

function byteLength(value: unknown) {
  return Buffer.byteLength(typeof value === "string" ? value : value === null ? "" : String(value))
}

async function physicalMetrics(db: SqliteAccess, filename: string, sessionID: string): Promise<PhysicalMetrics> {
  const file = await stat(filename)
  return {
    size: file.size,
    blocks: file.blocks,
    pageCount: oneRow<{ page_count: number }>(db, "PRAGMA page_count")?.page_count ?? 0,
    freelistCount: oneRow<{ freelist_count: number }>(db, "PRAGMA freelist_count")?.freelist_count ?? 0,
    integrityCheck: oneRow<{ integrity_check: string }>(db, "PRAGMA integrity_check")?.integrity_check ?? "unreadable",
    retainedSession: oneRow<PhysicalMetrics["retainedSession"]>(
      db,
      "SELECT id, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write FROM session WHERE id = ?",
      sessionID,
    ),
  }
}

export async function compactRestoreFixture(input: CompactFixtureInput): Promise<CompactFixtureResult> {
  let sourceDeviceID: string | undefined
  let backupDeviceID: string | undefined
  let stagingDeviceID: string | undefined
  let sourcePath: string | undefined
  let databaseReopened = false
  let control: FixtureControl | undefined
  let lockFile: Awaited<ReturnType<typeof open>> | undefined
  let lockCreated = false
  let sourceLocked = false
  let sourceClosed = false
  let oldSourceDB: NativeSqliteDatabase | undefined
  let restoredLocked = false
  let replaced = false
  let completed = false
  let originalMode: number | undefined
  let restoreDirectory: string | undefined
  let sourceDirectory: string | undefined
  let backupPath: string | undefined
  let stagingPath: string | undefined
  let backupCreated = false
  let stagingCreated = false
  let restorePath: string | undefined
  let stageDB: NativeSqliteDatabase | undefined
  const refused = (reasons: Iterable<string>, measurements?: CompactFixtureResult["measurements"]): CompactFixtureResult => ({
    state: "refused",
    changedFiles: replaced ? 3 : 0,
    reasons: Array.from(reasons).toSorted(),
    sourceDeviceID,
    backupDeviceID,
    stagingDeviceID,
    measurements,
  })
  try {
    control = input.sourceFixture ? fixtureControls.get(input.sourceFixture) : undefined
    const sourceIdentity = control?.identity
    if (
      !control ||
      !sourceIdentity ||
      sourceIdentity.filename !== input.sourceFixture?.filename ||
      path.resolve(input.sourcePath) !== sourceIdentity.filename ||
      !fixturePathMatches(sourceIdentity) ||
      fixtureHandles.get(control.database) !== sourceIdentity
    ) {
      return refused(["source-fixture-identity-unverified"])
    }
    if (![input.expectedSourceDeviceID, input.expectedBackupDeviceID, input.expectedStagingDeviceID].every(Boolean)) {
      return refused(["filesystem-identity-unverified"])
    }
    sourcePath = await realpath(input.sourcePath)
    if (sourcePath !== sourceIdentity.filename || !isTemporaryFixturePath(sourcePath)) {
      return refused(["compaction-only-supported-for-isolated-fixtures"])
    }
    sourceDirectory = await realpath(path.dirname(sourcePath))
    const backupDirectory = await realpath(path.dirname(input.backupPath))
    const stagingDirectory = await realpath(path.dirname(input.stagingPath))
    if (
      path.resolve(path.dirname(input.backupPath)) !== backupDirectory ||
      path.resolve(path.dirname(input.stagingPath)) !== stagingDirectory
    ) {
      return refused(["destination-directory-identity-unverified"])
    }
    const sourceStat = await stat(sourcePath)
    const sourceDirectoryStat = await stat(sourceDirectory)
    const sourceFilesystem = await statfs(sourceDirectory)
    const backupStat = await stat(backupDirectory)
    const backupFilesystem = await statfs(backupDirectory)
    const stagingStat = await stat(stagingDirectory)
    const stagingFilesystem = await statfs(stagingDirectory)
    sourceDeviceID = String(sourceStat.dev)
    backupDeviceID = String(backupStat.dev)
    stagingDeviceID = String(stagingStat.dev)
    const rootDeviceID = String((await stat("/")).dev)
    const currentUserID = process.getuid?.()
    backupPath = path.join(backupDirectory, path.basename(input.backupPath))
    stagingPath = path.join(stagingDirectory, path.basename(input.stagingPath))
    const sourceSize = sourceStat.size + (await fileSize(`${sourcePath}-wal`)) + (await fileSize(`${sourcePath}-shm`))
    const destinationUse = new Map<string, number>()
    destinationUse.set(backupDeviceID, sourceSize)
    destinationUse.set(stagingDeviceID, (destinationUse.get(stagingDeviceID) ?? 0) + sourceSize)
    const availableByDevice = new Map([
      [backupDeviceID, backupFilesystem.bavail * backupFilesystem.bsize],
      [stagingDeviceID, stagingFilesystem.bavail * stagingFilesystem.bsize],
    ])
    const reasons = new Set<string>()

    if (sourceDeviceID !== input.expectedSourceDeviceID) reasons.add("source-filesystem-identity-mismatch")
    if (backupDeviceID !== input.expectedBackupDeviceID) reasons.add("backup-filesystem-identity-mismatch")
    if (stagingDeviceID !== input.expectedStagingDeviceID) reasons.add("staging-filesystem-identity-mismatch")
    if (backupDeviceID === rootDeviceID) reasons.add("backup-destination-is-root")
    if (stagingDeviceID === rootDeviceID) reasons.add("staging-destination-is-root")
    if (backupDeviceID === sourceDeviceID) reasons.add("backup-filesystem-not-distinct")
    if (stagingDeviceID === sourceDeviceID) reasons.add("staging-filesystem-not-distinct")
    if (currentUserID === 0) reasons.add("fixture-compaction-requires-non-root-owner")
    if (currentUserID !== undefined) {
      if (sourceDirectoryStat.uid !== currentUserID || (sourceDirectoryStat.mode & 0o077) !== 0) {
        reasons.add("source-fixture-owner-or-mode-unverified")
      }
      if (
        [backupStat, stagingStat].some(
          (directoryStat) => directoryStat.uid !== currentUserID || (directoryStat.mode & 0o077) !== 0,
        )
      ) {
        reasons.add("destination-fixture-owner-or-mode-unverified")
      }
    }
    if (sourceFilesystem.bavail * sourceFilesystem.bsize < sourceSize) {
      reasons.add("source-restore-capacity-insufficient")
    }
    for (const [deviceID, required] of destinationUse) {
      if ((availableByDevice.get(deviceID) ?? 0) < Math.max(required, input.capacityFloorBytes ?? 0)) {
        reasons.add(`destination-capacity-insufficient:${deviceID}`)
      }
    }
    if (backupPath === sourcePath || stagingPath === sourcePath || backupPath === stagingPath) {
      reasons.add("compaction-paths-conflict")
    }
    for (const destination of [backupPath, stagingPath]) {
      try {
        await lstat(destination)
        reasons.add(`destination-already-exists:${destination}`)
      } catch (error) {
        if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") {
          reasons.add(`destination-identity-unreadable:${destination}`)
        }
      }
    }
    for (const filename of [sourcePath, `${sourcePath}-wal`, `${sourcePath}-shm`]) {
      if (filename !== sourcePath && !(await fileSize(filename))) continue
      const openSource = openPath(filename)
      if (!openSource.readable) reasons.add("source-inode-liveness-unavailable")
      if (!fixtureOwnerIsOnlyOpen(openSource)) reasons.add("source-inode-still-open")
    }
    if (reasons.size > 0) return refused(reasons)

    const identityBeforeFence = await stat(sourcePath)
    if (
      String(identityBeforeFence.dev) !== sourceIdentity.device ||
      String(identityBeforeFence.ino) !== sourceIdentity.inode
    ) {
      return refused(["source-fixture-identity-changed-before-fence"])
    }
    control.compacting = true
    lockFile = await open(control.lockPath, constants.O_CREAT | constants.O_EXCL | constants.O_RDWR, 0o600)
    lockCreated = true
    await lockFile.writeFile(JSON.stringify({ pid: process.pid, fixture: sourceIdentity.inode }))
    await lockFile.sync()

    const sourceDB = control.database
    const sourceWithQueryCache = sourceDB as NativeSqliteDatabase & { clearQueryCache: () => void }
    oldSourceDB = sourceDB
    const sourceAccess = sourceDB as unknown as SqliteAccess
    sourceDB.exec("PRAGMA busy_timeout=0")
    if (oneRow<{ journal_mode: string }>(sourceAccess, "PRAGMA journal_mode")?.journal_mode !== "delete") {
      return refused(["source-journal-mode-not-delete"])
    }
    if ((await fileSize(`${sourcePath}-wal`)) || (await fileSize(`${sourcePath}-shm`))) {
      return refused(["source-wal-sidecar-present"])
    }
    sourceDB.exec("PRAGMA locking_mode=EXCLUSIVE")
    if (oneRow<{ locking_mode: string }>(sourceAccess, "PRAGMA locking_mode")?.locking_mode !== "exclusive") {
      return refused(["sqlite-exclusive-lock-unavailable"])
    }
    sourceLocked = true
    sourceDB.exec("BEGIN EXCLUSIVE")
    try {
      const acquired = sourceDB
        .query("UPDATE event_sequence SET seq = seq + 1 WHERE aggregate_id = ?")
        .run(input.retainedSessionID)
      sourceDB.exec("ROLLBACK")
      if (acquired.changes !== 1) return refused(["retained-session-aggregate-lock-unavailable"])
    } catch (error) {
      try {
        sourceDB.exec("ROLLBACK")
      } catch {}
      return refused([`sqlite-exclusive-lock-unavailable:${error instanceof Error ? error.message : "unknown"}`])
    }
    // Release pre-existing cached statements before later compactor queries can evict them from Bun's LRU cache.
    sourceWithQueryCache.clearQueryCache()
    for (const filename of [sourcePath, `${sourcePath}-wal`, `${sourcePath}-shm`]) {
      if (filename !== sourcePath && !(await fileSize(filename))) continue
      const openSource = openPath(filename)
      if (!openSource.readable) return refused(["source-inode-liveness-unavailable-under-fence"])
      if (!fixtureOwnerIsOnlyOpen(openSource)) return refused(["source-inode-still-open-under-fence"])
    }
    if (input.afterExclusiveLock) {
      await input.afterExclusiveLock()
      return refused(["writer-arrival-blocked-under-exclusive-fence"])
    }
    if (input.afterDestinationPreflight) await input.afterDestinationPreflight()

    originalMode = sourceStat.mode & 0o777
    const before = await physicalMetrics(sourceAccess, sourcePath, input.retainedSessionID)
    if (before.integrityCheck !== "ok" || !before.retainedSession) {
      return refused(["source-integrity-or-retained-read-failed"], { before, backup: before, staging: before, restored: before })
    }
    await copyFile(sourcePath, backupPath, constants.COPYFILE_EXCL)
    backupCreated = true
    const sqlite = await import("bun:sqlite")
    const backupDB = new sqlite.Database(backupPath, { readonly: true }) as unknown as SqliteAccess
    let backup: PhysicalMetrics
    try {
      backup = await physicalMetrics(backupDB, backupPath, input.retainedSessionID)
    } finally {
      backupDB.close()
    }
    if (!sameRetainedRead(before, backup) || backup.integrityCheck !== "ok") {
      return refused(["backup-integrity-or-retained-read-mismatch"], { before, backup, staging: backup, restored: backup })
    }

    await copyFile(backupPath, stagingPath, constants.COPYFILE_EXCL)
    stagingCreated = true
    stageDB = new sqlite.Database(stagingPath)
    stageDB.exec("VACUUM")
    const staging = await physicalMetrics(stageDB as unknown as SqliteAccess, stagingPath, input.retainedSessionID)
    if (
      staging.integrityCheck !== "ok" ||
      staging.freelistCount !== 0 ||
      !sameRetainedRead(before, staging) ||
      staging.size >= before.size ||
      staging.blocks >= before.blocks
    ) {
      return refused(["compaction-did-not-preserve-metrics-and-reduce-physical-file"], {
        before,
        backup,
        staging,
        restored: staging,
      })
    }
    stageDB.close()
    stageDB = undefined

    restoreDirectory = await mkdtemp(path.join(sourceDirectory, ".retention-restore-"))
    const restoreDirectoryStat = await stat(restoreDirectory)
    if (String(restoreDirectoryStat.dev) !== sourceDeviceID || (restoreDirectoryStat.mode & 0o077) !== 0) {
      return refused(["restore-filesystem-or-directory-identity-unverified"], {
        before,
        backup,
        staging,
        restored: staging,
      })
    }
    restorePath = path.join(restoreDirectory, "retention.sqlite")
    await copyFile(stagingPath, restorePath, constants.COPYFILE_EXCL)
    await chmod(restorePath, originalMode)
    const restoreDB = new sqlite.Database(restorePath, { readonly: true }) as unknown as SqliteAccess
    let restored: PhysicalMetrics
    try {
      restored = await physicalMetrics(restoreDB, restorePath, input.retainedSessionID)
    } finally {
      restoreDB.close()
    }
    if (
      restored.integrityCheck !== "ok" ||
      restored.size >= before.size ||
      restored.blocks >= before.blocks ||
      !sameRetainedRead(before, restored) ||
      restored.size !== staging.size ||
      restored.blocks !== staging.blocks
    ) {
      return refused(["restore-integrity-or-physical-measurement-failed"], { before, backup, staging, restored })
    }

    // Finalize compactor-created statements while the exclusive writer fence is still held.
    sourceWithQueryCache.clearQueryCache()
    await chmod(restorePath, 0)
    await chmod(sourcePath, 0)
    await rename(restorePath, sourcePath)
    replaced = true
    // A deferred SQLite close can leave a statement-backed descriptor on the replaced inode.
    sourceDB.close(true)
    sourceClosed = true
    const oldInode = deletedFixtureInodeOpen(sourceIdentity)
    if (!oldInode.readable) {
      return refused(["old-source-inode-liveness-unavailable-after-replacement"], { before, backup, staging, restored })
    }
    if (oldInode.pids.length > 0) {
      return refused(["old-source-inode-still-open-after-replacement"], { before, backup, staging, restored })
    }
    chmodSync(sourcePath, originalMode)
    const restoredDB = new sqlite.Database(sourcePath)
    const restoredIdentity = await readFixtureIdentity(restoredDB as unknown as SqliteAccess, sourcePath)
    control.identity = restoredIdentity
    control.database = restoredDB
    databaseReopened = true
    fixtureHandles.set(restoredDB, restoredIdentity)
    restoredDB.exec("PRAGMA busy_timeout=0")
    restoredDB.exec("PRAGMA locking_mode=EXCLUSIVE")
    const restoredAccess = restoredDB as unknown as SqliteAccess
    if (oneRow<{ locking_mode: string }>(restoredAccess, "PRAGMA locking_mode")?.locking_mode !== "exclusive") {
      return refused(["restored-sqlite-exclusive-lock-unavailable"], { before, backup, staging, restored })
    }
    restoredLocked = true
    restoredDB.exec("BEGIN EXCLUSIVE")
    try {
      const acquired = restoredDB
        .query("UPDATE event_sequence SET seq = seq + 1 WHERE aggregate_id = ?")
        .run(input.retainedSessionID)
      restoredDB.exec("ROLLBACK")
      if (acquired.changes !== 1) {
        return refused(["restored-session-aggregate-lock-unavailable"], { before, backup, staging, restored })
      }
    } catch (error) {
      try {
        restoredDB.exec("ROLLBACK")
      } catch {}
      return refused([`restored-sqlite-exclusive-lock-unavailable:${error instanceof Error ? error.message : "unknown"}`], {
        before,
        backup,
        staging,
        restored,
      })
    }
    const replacementOpen = openPath(sourcePath)
    if (!replacementOpen.readable) {
      return refused(["restored-source-inode-liveness-unavailable-under-fence"], { before, backup, staging, restored })
    }
    if (!fixtureOwnerIsOnlyOpen(replacementOpen)) {
      return refused(["restored-source-inode-still-open-under-fence"], { before, backup, staging, restored })
    }
    const finalMetrics = await physicalMetrics(restoredAccess, sourcePath, input.retainedSessionID)
    if (
      finalMetrics.integrityCheck !== "ok" ||
      !sameRetainedRead(before, finalMetrics) ||
      finalMetrics.size >= before.size ||
      finalMetrics.blocks >= before.blocks
    ) {
      return refused(["restored-source-integrity-or-retained-read-failed"], {
        before,
        backup,
        staging,
        restored: finalMetrics,
      })
    }
    restoredDB.exec("PRAGMA locking_mode=NORMAL")
    restoredDB.query("SELECT 1").get()
    restoredLocked = false
    originalMode = undefined
    completed = true
    return {
      state: "complete",
      changedFiles: 3,
      reasons: [],
      sourceDeviceID,
      backupDeviceID,
      stagingDeviceID,
      measurements: { before, backup, staging, restored: finalMetrics },
    }
  } catch (error) {
    return refused([`fixture-compaction-failed:${error instanceof Error ? error.message : "unknown"}`])
  } finally {
    if (stageDB) {
      try {
        stageDB.close()
      } catch {}
    }
    if (replaced && oldSourceDB && !sourceClosed) {
      try {
        oldSourceDB.close()
        sourceClosed = true
      } catch {}
    }
    if (control && sourceLocked && !sourceClosed) {
      try {
        control.database.exec("PRAGMA locking_mode=NORMAL")
        control.database.query("SELECT 1").get()
      } catch {}
    }
    if (control && restoredLocked && databaseReopened) {
      try {
        control.database.exec("PRAGMA locking_mode=NORMAL")
        control.database.query("SELECT 1").get()
        restoredLocked = false
      } catch {}
    }
    if (control && sourceClosed && !databaseReopened && !control.removed && sourcePath) {
      try {
        if (originalMode !== undefined && (await Bun.file(sourcePath).exists())) chmodSync(sourcePath, originalMode)
        const sqlite = await import("bun:sqlite")
        const reopened = new sqlite.Database(sourcePath) as unknown as NativeSqliteDatabase
        const identity = await readFixtureIdentity(reopened as unknown as SqliteAccess, sourcePath)
        control.database = reopened
        control.identity = identity
        fixtureHandles.set(reopened, identity)
        databaseReopened = true
      } catch {}
    }
    if (lockFile) await lockFile.close().catch(() => undefined)
    if (lockCreated && control) await rm(control.lockPath, { force: true }).catch(() => undefined)
    if (restoreDirectory) await rm(restoreDirectory, { recursive: true, force: true }).catch(() => undefined)
    if (!completed && !replaced) {
      if (backupCreated && backupPath && backupPath !== sourcePath) await rm(backupPath, { force: true }).catch(() => undefined)
      if (stagingCreated && stagingPath && stagingPath !== sourcePath) await rm(stagingPath, { force: true }).catch(() => undefined)
    }
    if (control) control.compacting = false
  }
}

function fixturePathMatches(identity: FixtureIdentity) {
  try {
    const link = lstatSync(identity.filename)
    const file = statSync(identity.filename)
    return (
      link.isFile() &&
      !link.isSymbolicLink() &&
      realpathSync(identity.filename) === identity.filename &&
      String(file.dev) === identity.device &&
      String(file.ino) === identity.inode
    )
  } catch {
    return false
  }
}

async function readFixtureIdentity(db: SqliteAccess, filename: string): Promise<FixtureIdentity> {
  const absolute = path.resolve(filename)
  const [canonical, link, file] = await Promise.all([realpath(absolute), lstat(absolute), stat(absolute)])
  if (db.filename !== absolute || canonical !== absolute || !link.isFile() || link.isSymbolicLink()) {
    db.close()
    throw new Error("retention fixture database is not a regular file at its canonical path")
  }
  return { filename: absolute, device: String(file.dev), inode: String(file.ino) }
}

async function fixtureHandleMatches(db: SqliteAccess, identity: FixtureIdentity) {
  if (db.filename !== identity.filename || !fixturePathMatches(identity)) return false
  const current = await readFixtureIdentity(db, identity.filename)
  return current.device === identity.device && current.inode === identity.inode
}

function isTemporaryFixturePath(filename: string) {
  if (filename === ":memory:") return true
  const temporaryRoot = path.resolve(tmpdir())
  const absolute = path.resolve(filename)
  const relative = path.relative(temporaryRoot, absolute)
  return (
    relative !== "" &&
    !relative.startsWith("..") &&
    !path.isAbsolute(relative) &&
    absolute !== path.resolve(Database.path())
  )
}

async function fileSize(filename: string) {
  try {
    return (await stat(filename)).size
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") return 0
    throw error
  }
}

function openPath(filename: string) {
  const result = spawnSync("lsof", ["-F0pfn", filename], { encoding: "utf8", timeout: 15_000 })
  if (result.error || result.status === null || ![0, 1].includes(result.status))
    return { readable: false, pids: [], handles: 0 }
  const fields = result.stdout.split(/[\0\n]+/).filter(Boolean)
  return {
    readable: true,
    pids: Array.from(new Set(fields.filter((field) => field.startsWith("p")).map((field) => field.slice(1)))),
    handles: fields.filter((field) => field.startsWith("f")).length,
  }
}

function deletedFixtureInodeOpen(identity: FixtureIdentity) {
  const result = spawnSync("lsof", ["+L1", "-F0pfnDi"], { encoding: "utf8", timeout: 15_000, maxBuffer: 5_000_000 })
  if (result.error || result.status === null || ![0, 1].includes(result.status)) return { readable: false, pids: [] }
  let processID: string | undefined
  let inode: string | undefined
  let name: string | undefined
  const fields = result.stdout.split(/[\0\n]+/).filter(Boolean)
  for (const field of fields) {
    if (field.startsWith("p")) processID = field.slice(1)
    if (field.startsWith("f")) {
      inode = undefined
      name = undefined
    }
    if (field.startsWith("i")) inode = field.slice(1)
    if (field.startsWith("n")) {
      name = field.slice(1)
      if (name === `${identity.filename} (deleted)` && inode === identity.inode) {
        return { readable: true, pids: processID ? [processID] : [] }
      }
    }
  }
  return { readable: true, pids: [] }
}

function fixtureOwnerIsOnlyOpen(opened: ReturnType<typeof openPath>) {
  return opened.readable && opened.pids.length === 1 && opened.pids[0] === String(process.pid) && opened.handles === 1
}

function parentCycle(sessionID: string, sessions: ReadonlyMap<string, SessionRow>) {
  const path = new Set<string>()
  let current = sessions.get(sessionID)
  while (current?.parent_id) {
    if (path.has(current.id)) return true
    path.add(current.id)
    current = sessions.get(current.parent_id)
  }
  return false
}

function validUtcWindow(start: string, end: string) {
  if (!start.endsWith("Z") || !end.endsWith("Z")) return false
  const from = Date.parse(start)
  const to = Date.parse(end)
  return Number.isFinite(from) && Number.isFinite(to) && from < to
}

function parseJson(value: string) {
  try {
    const parsed = JSON.parse(value) as unknown
    return parsed !== null && typeof parsed === "object" ? (parsed as Record<string, unknown>) : undefined
  } catch {
    return undefined
  }
}

function sameIDs(left: readonly string[], right: readonly string[]) {
  return left.length === right.length && left.toSorted().every((id, index) => id === right[index])
}

function placeholders(count: number) {
  return new Array(count).fill("?").join(",")
}

function quote(identifier: string) {
  return `"${identifier.replaceAll('"', '""')}"`
}

type RetentionArgs = { apply?: boolean }

export const RetentionCommand = cmd<{}, RetentionArgs>({
  command: "retention",
  describe: "inventory Session retention candidates; --apply redacts only trees that pass every gate",
  builder: (yargs) =>
    yargs.option("apply", { type: "boolean", default: false, describe: "redact every tree that passes every gate" }),
  async handler(args) {
    const sqlite = await import("bun:sqlite")
    const filename = Database.path()
    try {
      if (!args.apply) {
        const db = new sqlite.Database(filename, { readonly: true }) as unknown as SqliteAccess
        try {
          console.log(
            JSON.stringify(inventory(db, productionEvidence(filename), Date.now(), { transactional: false }), null, 2),
          )
        } finally {
          db.close()
        }
        return
      }
      const db = new sqlite.Database(filename, { readwrite: true, create: false }) as unknown as SqliteAccess
      try {
        const result = applyGated(db, () => productionEvidence(filename))
        console.log(JSON.stringify(result, null, 2))
        if (result.action !== "applied") process.exitCode = 1
      } finally {
        db.close()
      }
    } catch (error) {
      console.log(
        JSON.stringify({
          action: "refused",
          changedRows: 0,
          reasons: [`database-unreadable:${error instanceof Error ? error.message : "unknown"}`],
        }),
      )
      process.exitCode = 1
    }
  },
})

/**
 * The production evidence this build can produce. The measurement receipt, customer classification and
 * reader contract have no approved producer yet, so their gates refuse every tree.
 */
function productionEvidence(filename: string): RetentionEvidence {
  return { holders: scanDatabaseHolders(filename) }
}

export type GatedApplyResult = {
  readonly action: "applied" | "partial" | "refused"
  readonly lockHold: LockHold
  readonly changedRows: number
  readonly changedBytes: number
  readonly completedSessionIDs: readonly string[]
  readonly eligibleTrees: number
  readonly refusedTrees: number
  readonly reasons: readonly string[]
}

/**
 * Redacts every tree whose inventory passes every gate, revalidating inside each batch. A refusal before
 * the first committed batch writes nothing; a refusal after one stops all further writes and reports the
 * committed progress.
 */
export function applyGated(db: SqliteAccess, evidence: () => RetentionEvidence, now = Date.now): GatedApplyResult {
  db.exec("PRAGMA busy_timeout = 5000")
  const dryRun = inventory(db, evidence(), now(), { transactional: false })
  const eligible = dryRun.trees.filter((tree) => tree.eligible)
  const refusedTrees = dryRun.trees.length - eligible.length
  if (eligible.length === 0) {
    const kinds = new Set([...dryRun.refusals, ...dryRun.trees.flatMap((tree) => tree.reasons.map(reasonKind))])
    if (dryRun.trees.length === 0) kinds.add("no-retention-candidate")
    return {
      action: "refused",
      lockHold: { batches: 0, maxMs: 0, totalMs: 0 },
      changedRows: 0,
      changedBytes: 0,
      completedSessionIDs: [],
      eligibleTrees: 0,
      refusedTrees,
      reasons: [...Array.from(kinds).toSorted(), "production-apply-disabled"],
    }
  }
  const completed = new Array<string>()
  const holds = new Array<LockHold>()
  const lockHold = (): LockHold => ({
    batches: holds.reduce((total, hold) => total + hold.batches, 0),
    maxMs: holds.reduce((max, hold) => Math.max(max, hold.maxMs), 0),
    totalMs: holds.reduce((total, hold) => total + hold.totalMs, 0),
  })
  let changedRows = 0
  let changedBytes = 0
  for (const tree of eligible) {
    const result = apply(db, { tree, evidence, now })
    holds.push(result.lockHold)
    changedRows += result.changedRows
    changedBytes += result.changedBytes
    completed.push(...result.completedSessionIDs)
    if (result.state === "complete") continue
    return {
      action: changedRows > 0 || completed.length > 0 ? "partial" : "refused",
      lockHold: lockHold(),
      changedRows,
      changedBytes,
      completedSessionIDs: completed.toSorted(),
      eligibleTrees: eligible.length,
      refusedTrees,
      reasons: [...result.reasons, "production-apply-disabled"],
    }
  }
  return {
    action: "applied",
    lockHold: lockHold(),
    changedRows,
    changedBytes,
    completedSessionIDs: completed.toSorted(),
    eligibleTrees: eligible.length,
    refusedTrees,
    reasons: [],
  }
}

function reasonKind(reason: string) {
  const separator = reason.indexOf(":")
  return separator === -1 ? reason : reason.slice(0, separator)
}

export * as DbRetention from "./db-retention"
