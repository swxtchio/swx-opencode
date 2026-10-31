import { Database } from "@opencode-ai/core/database/database"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { Durable } from "@opencode-ai/schema/durable-event-manifest"
import type { Argv } from "yargs"
import { lstat, realpath, stat, statfs } from "node:fs/promises"
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
  online?: boolean
  reclaimOnly?: boolean
  reserveBytes?: number
  batchRows?: number
  batchBytes?: number
  maxBatches?: number
  vacuumPages?: number
  maxVacuumPages?: number
  firstmateRanIt?: boolean
  measurementStatus?: MeasurementStatus
  usageLedgerStatus?: LedgerStatus
}

const onlineBounds = {
  sessions: 256,
  validationRows: 10_000,
  batchRows: 256,
  batchBytes: 16_777_216,
  maxBatchBytes: 67_108_864,
  maxBatches: 256,
  vacuumPages: 128,
  maxVacuumPages: 1_048_576,
  busyTimeout: 5,
  busyAttempts: 3,
} as const

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
  directoryIdentity: string | null
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
      describe: "target database file; online mode requires this or OPENCODE_DB; never creates a database",
    })
    .option("directory", {
      type: "string",
      describe: "limit offline retirement to this absolute recorded directory, after it has been removed",
    })
    .option("session", {
      type: "array",
      string: true,
      describe: "limit inventory and apply to these Session IDs; repeat for each (online maximum: 256)",
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
  describe: "inventory missing-directory local Sessions; --apply deletes them offline unless --online is selected",
  builder: (yargs: Argv) =>
    pruneOptions(yargs)
      .option("online", {
        type: "boolean",
        default: false,
        describe:
          "use bounded WAL-safe Session deletion and incremental reclamation (validation cap: 10,000 rows per Session)",
      })
      .option("reclaim-only", {
        type: "boolean",
        default: false,
        describe:
          "continue online incremental reclamation without Sessions; requires --online --apply and no --session",
      })
      .option("reserve-bytes", {
        type: "number",
        describe: "minimum free bytes to preserve before every online mutation; positive and required with --apply",
      })
      .option("batch-rows", {
        type: "number",
        defaultDescription: String(onlineBounds.batchRows),
        describe: "maximum owned rows changed by one online delete transaction (1–256)",
      })
      .option("batch-bytes", {
        type: "number",
        defaultDescription: String(onlineBounds.batchBytes),
        describe: "maximum JSON payload bytes changed by one transaction (1–67,108,864)",
      })
      .option("max-batches", {
        type: "number",
        defaultDescription: String(onlineBounds.maxBatches),
        describe: "maximum online delete transactions in one invocation (1–256)",
      })
      .option("vacuum-pages", {
        type: "number",
        defaultDescription: String(onlineBounds.vacuumPages),
        describe: "maximum pages in one incremental-vacuum step (1–128)",
      })
      .option("max-vacuum-pages", {
        type: "number",
        defaultDescription: String(onlineBounds.maxVacuumPages),
        describe: "maximum incremental-vacuum pages in one invocation (1–1,048,576)",
      }),
  async handler(args) {
    const filename = args.database ?? Database.path()
    const cutoff = parseCutoff(args.before)
    const requested = normalizeSessionIDs(args.session)
    const usage = measurementReport(args)
    if (args.online) {
      const onlineUsageReasons = [
        ...(args.before !== undefined && cutoff === undefined ? ["invalid-or-missing-cutoff"] : []),
        ...(requested === null ? ["invalid-session-selection"] : []),
        ...(args.directory !== undefined ? ["online-directory-selection-unsupported"] : []),
        ...(args.reclaimOnly && !args.apply ? ["reclaim-only-requires-apply"] : []),
        ...(args.reclaimOnly && (requested?.length ?? 0) > 0 ? ["reclaim-only-disallows-session-selection"] : []),
        ...(!args.reclaimOnly && (!requested || requested.length === 0) ? ["online-session-selection-required"] : []),
        ...(requested && requested.length > onlineBounds.sessions ? ["online-session-selection-limit-exceeded"] : []),
        ...(args.reserveBytes !== undefined && (!Number.isSafeInteger(args.reserveBytes) || args.reserveBytes < 0)
          ? ["reserve-bytes-invalid"]
          : []),
        ...(args.apply &&
        (args.reserveBytes === undefined || !Number.isSafeInteger(args.reserveBytes) || args.reserveBytes <= 0)
          ? ["reserve-bytes-required"]
          : []),
        ...onlineOptions(args).reasons,
      ]
      if (onlineUsageReasons.length > 0) {
        process.stderr.write(`opencode db prune: invalid online usage (${onlineUsageReasons.join(", ")})\n`)
        process.stderr.write("Run 'opencode db prune --help' for usage.\n")
        process.exitCode = 2
        return
      }
      const report = await applyOnlinePrune(filename, args, cutoff, requested, usage)
      console.log(JSON.stringify(report))
      process.exitCode = report.action === "inventory" || report.action === "applied" ? 0 : 1
      return
    }
    if (
      args.reclaimOnly ||
      args.reserveBytes !== undefined ||
      args.batchRows !== undefined ||
      args.batchBytes !== undefined ||
      args.maxBatches !== undefined ||
      args.vacuumPages !== undefined ||
      args.maxVacuumPages !== undefined
    ) {
      console.log(
        JSON.stringify(
          refusedReport(filename, cutoff, requested === null ? undefined : requested, usage, [
            "online-options-require---online",
          ]),
        ),
      )
      process.exitCode = 1
      return
    }
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

type OnlineOptions = {
  batchRows: number
  batchBytes: number
  maxBatches: number
  vacuumPages: number
  maxVacuumPages: number
  reserveBytes: number | null
}

type OnlineValidationBudget = { remainingRows: number }

const onlineFence = Symbol("onlineFence")
const onlineDescendantFence = Symbol("onlineDescendantFence")

type OnlineSessionSummary = {
  sessionID: string
  directory: string | null
  directoryIdentity: string | null
  directoryState: string
  parentSessionID: string | null
  lastActivity: string | null
  reasons: string[]
  [onlineFence]?: OnlineSessionFence
  [onlineDescendantFence]?: string[]
}

type OnlineSessionFence = {
  readonly data: string
  readonly eventSequence: number | null
  readonly eventOwnerID: string | null
}

type OnlineReclamation = {
  ready: boolean
  method: string
  autoVacuum: string
  reasons: string[]
  pageSizeBytes: number | null
  pageCountBefore: number | null
  pageCountAfter: number | null
  freelistBefore: number | null
  freelistAfter: number | null
  reserveBytes: number | null
  availableBytes: number | null
  plannedPeakBytes: number | null
  bounds: Record<string, number>
  allocatedBytesBefore: Awaited<ReturnType<typeof allocatedBytes>>
  allocatedBytesAfter?: Awaited<ReturnType<typeof allocatedBytes>>
  netAllocatedBytes?: number | null
  remainingFreelistPages?: number | null
  remainingWalFrames?: number | null
  steps: Record<string, unknown>[]
}

type OnlinePruneReport = {
  action: "inventory" | "applied" | "refused" | "failed"
  database: string
  cutoff: string | null
  explicitSessionIDs: string[] | null
  selectedSessions: OnlineSessionSummary[]
  excludedSessions: OnlineSessionSummary[]
  deletionScope: { sessionIDs: string[]; wholeSessions: true }
  reclamation: OnlineReclamation
  usageMeasurement: ReturnType<typeof measurementReport>
  rawTranscriptWarning: string
  committed: boolean
  changedRows: number
  changedRowsByTable: Record<string, number>
  deletedSessionIDs: string[]
  partialSessionIDs: string[]
  remainingSessionIDs: string[]
  skippedSessions: { sessionID: string; reasons: string[] }[]
  refusalReasons: string[]
  partial: boolean
  batches: Record<string, unknown>[]
  sessionProgress: Record<string, unknown>[]
}

const onlineReclaimMethod = "PRAGMA incremental_vacuum(N) with PRAGMA wal_checkpoint(PASSIVE)"

async function applyOnlinePrune(
  filename: string,
  args: PruneArgs,
  cutoff: number | undefined,
  requested: readonly string[] | undefined | null,
  usage: ReturnType<typeof measurementReport>,
): Promise<OnlinePruneReport> {
  const options = onlineOptions(args)
  const invalid = []
  if (args.before !== undefined && cutoff === undefined) invalid.push("invalid-or-missing-cutoff")
  if (requested === null) invalid.push("invalid-session-selection")
  if (args.directory !== undefined) invalid.push("online-directory-selection-unsupported")
  if (!args.database && !process.env.OPENCODE_DB) invalid.push("online-database-target-required")
  if (args.reclaimOnly && !args.apply) invalid.push("reclaim-only-requires-apply")
  if (args.reclaimOnly && (requested?.length ?? 0) > 0) invalid.push("reclaim-only-disallows-session-selection")
  if (!args.reclaimOnly && (!requested || requested.length === 0)) invalid.push("online-session-selection-required")
  if (requested && requested.length > onlineBounds.sessions) invalid.push("online-session-selection-limit-exceeded")
  if (args.reserveBytes !== undefined && (!Number.isSafeInteger(args.reserveBytes) || args.reserveBytes < 0))
    invalid.push("reserve-bytes-invalid")
  if (
    args.apply &&
    (args.reserveBytes === undefined || !Number.isSafeInteger(args.reserveBytes) || args.reserveBytes <= 0)
  )
    invalid.push("reserve-bytes-required")
  if (options.reasons.length > 0) invalid.push(...options.reasons)
  if (invalid.length > 0) return onlineRefusedReport(filename, cutoff, requested, usage, invalid)

  const resolved = await resolveDatabaseFile(filename)
  if ("reason" in resolved)
    return onlineRefusedReport(filename, cutoff, requested, usage, [resolved.reason ?? "database-file-unreadable"])

  const sqlite = await import("bun:sqlite")
  let db: SqliteAccess | undefined
  try {
    db = new sqlite.Database(resolved.filename, args.apply ? { readwrite: true, create: false } : { readonly: true })
    db.exec(`PRAGMA busy_timeout = ${onlineBounds.busyTimeout}; PRAGMA foreign_keys = ON`)
    if (args.reclaimOnly) return await onlineReclaimOnly(db, resolved.filename, options.value, usage)

    const inventory = await onlineInventorySnapshot(db, resolved.filename, requested!, cutoff, options.value, usage)
    if (!args.apply) return inventory
    if (!inventory.reclamation.ready)
      return { ...inventory, action: "refused", refusalReasons: inventory.reclamation.reasons }
    if (inventory.selectedSessions.length === 0) {
      return {
        ...inventory,
        action: "refused",
        refusalReasons: ["no-eligible-sessions", ...inventory.excludedSessions.flatMap((item) => item.reasons)],
      }
    }
    return await applyOnlineScope(db, resolved.filename, cutoff, options.value, inventory)
  } catch (error) {
    const refused = onlineRefusedReport(filename, cutoff, requested, usage, [
      error instanceof Error ? error.message : "online-database-operation-failed",
    ])
    return args.apply ? refused : { ...refused, action: "inventory" }
  } finally {
    db?.close()
  }
}

function onlineOptions(args: PruneArgs) {
  const value: OnlineOptions = {
    batchRows: args.batchRows ?? onlineBounds.batchRows,
    batchBytes: args.batchBytes ?? onlineBounds.batchBytes,
    maxBatches: args.maxBatches ?? onlineBounds.maxBatches,
    vacuumPages: args.vacuumPages ?? onlineBounds.vacuumPages,
    maxVacuumPages: args.maxVacuumPages ?? onlineBounds.maxVacuumPages,
    reserveBytes: args.reserveBytes ?? null,
  }
  const reasons = [
    ...(Number.isSafeInteger(value.batchRows) && value.batchRows > 0 && value.batchRows <= onlineBounds.batchRows
      ? []
      : ["online-batch-rows-out-of-range"]),
    ...(Number.isSafeInteger(value.batchBytes) && value.batchBytes > 0 && value.batchBytes <= onlineBounds.maxBatchBytes
      ? []
      : ["online-batch-bytes-out-of-range"]),
    ...(Number.isSafeInteger(value.maxBatches) && value.maxBatches > 0 && value.maxBatches <= onlineBounds.maxBatches
      ? []
      : ["online-max-batches-out-of-range"]),
    ...(Number.isSafeInteger(value.vacuumPages) &&
    value.vacuumPages > 0 &&
    value.vacuumPages <= onlineBounds.vacuumPages
      ? []
      : ["online-vacuum-pages-out-of-range"]),
    ...(Number.isSafeInteger(value.maxVacuumPages) &&
    value.maxVacuumPages > 0 &&
    value.maxVacuumPages <= onlineBounds.maxVacuumPages
      ? []
      : ["online-max-vacuum-pages-out-of-range"]),
  ]
  return { value, reasons }
}

function onlineRefusedReport(
  filename: string,
  cutoff: number | undefined,
  requested: readonly string[] | undefined | null,
  usage: ReturnType<typeof measurementReport>,
  reasons: string[],
): OnlinePruneReport {
  return {
    action: "refused",
    database: filename,
    cutoff: cutoff === undefined ? null : new Date(cutoff).toISOString(),
    explicitSessionIDs: requested === undefined || requested === null ? null : [...requested],
    selectedSessions: [],
    excludedSessions: [],
    deletionScope: { sessionIDs: [], wholeSessions: true },
    reclamation: {
      ready: false,
      method: onlineReclaimMethod,
      autoVacuum: "unreadable",
      reasons,
      pageSizeBytes: null,
      pageCountBefore: null,
      pageCountAfter: null,
      freelistBefore: null,
      freelistAfter: null,
      reserveBytes: null,
      availableBytes: null,
      plannedPeakBytes: null,
      bounds: onlineBoundsReport(),
      allocatedBytesBefore: { status: "unavailable", reason: "inventory-unavailable", files: [] },
      steps: [],
    },
    usageMeasurement: usage,
    rawTranscriptWarning: "Applying this command permanently deletes the raw transcripts in selected Sessions.",
    committed: false,
    changedRows: 0,
    changedRowsByTable: {},
    deletedSessionIDs: [],
    partialSessionIDs: [],
    remainingSessionIDs: requested ? [...requested] : [],
    skippedSessions: [],
    refusalReasons: reasons,
    partial: false,
    batches: [],
    sessionProgress: [],
  }
}

async function onlineInventorySnapshot(
  db: SqliteAccess,
  filename: string,
  requested: readonly string[],
  cutoff: number | undefined,
  options: OnlineOptions,
  usage: ReturnType<typeof measurementReport>,
): Promise<OnlinePruneReport> {
  const schema = inspectSchema(db)
  const schemaReasons = [
    ...schema.failures,
    ...schema.unclassifiedTables.map((table) => `unclassified-session-storage:${table}`),
    ...onlineScopeIndexFailures(db, schema.tables),
  ]
  const sessionColumns = schema.columnMap.get("session")
  const canReadSessionScope = ["id", "directory", "parent_id", "time_created", "time_updated"].every((column) =>
    sessionColumns?.has(column),
  )
  const holderScan = scanDatabaseHolders(filename)
  const holderReasons = onlineHolderFailures(db, holderScan)
  const autoVacuum = db.query<{ auto_vacuum: number }, []>("PRAGMA auto_vacuum").get()?.auto_vacuum
  const pageSize = db.query<{ page_size: number }, []>("PRAGMA page_size").get()?.page_size
  const pageCount = db.query<{ page_count: number }, []>("PRAGMA page_count").get()?.page_count
  const freelist = db.query<{ freelist_count: number }, []>("PRAGMA freelist_count").get()?.freelist_count
  const allocated = await allocatedBytes(filename)
  const availableBytes = await onlineAvailableBytes(filename)
  const globalReasons = [
    ...schemaReasons,
    ...holderReasons,
    ...(autoVacuum === 2 ? [] : [`incremental-vacuum-unavailable:${autoVacuum ?? "unreadable"}`]),
    ...(pageSize === undefined ? ["page-size-unreadable"] : []),
    ...(pageCount === undefined ? ["page-count-unreadable"] : []),
    ...(freelist === undefined ? ["freelist-count-unreadable"] : []),
    ...(allocated.status === "measured" ? [] : [`allocated-byte-measurement-${allocated.status}`]),
    ...(availableBytes === undefined ? ["free-space-measurement-unavailable"] : []),
  ]
  const selectedSessions = new Array<OnlineSessionSummary>()
  const excludedSessions = new Array<OnlineSessionSummary>()
  const sessionRows = new Map<string, SessionRow>()
  const validationBudgets = new Map<string, OnlineValidationBudget>()
  for (const sessionID of requested) {
    const budget = onlineValidationBudget()
    validationBudgets.set(sessionID, budget)
    const row = canReadSessionScope
      ? db
          .query<
            SessionRow,
            [string]
          >("SELECT id, directory, parent_id, time_created, time_updated FROM session WHERE id = ?")
          .get(sessionID)
      : undefined
    if (!row) {
      excludedSessions.push(onlineExcludedSession(sessionID, null, null, "unknown", null, ["session-not-found"]))
      continue
    }
    onlineConsumeValidationRows(1, budget)
    sessionRows.set(sessionID, row)
    const identity = await directoryIdentity(row.directory)
    const directoryState = identity === undefined ? "unknown" : await inspectDirectory(identity)
    const reasons = [...globalReasons]
    if (directoryState !== "missing") reasons.push(`session-directory-${directoryState}`)
    const evidence = schemaReasons.length === 0 ? onlineSessionEvidence(db, schema, sessionID, budget) : undefined
    if (evidence) reasons.push(...evidence.reasons)
    else if (schemaReasons.length > 0) reasons.push("session-evidence-unavailable")
    if (cutoff !== undefined && evidence?.lastActivity !== undefined && evidence.lastActivity >= cutoff)
      reasons.push("activity-at-or-after-cutoff")
    const summary = onlineExcludedSession(
      sessionID,
      row.directory,
      identity ?? null,
      directoryState,
      row.parent_id,
      [...new Set(reasons)].toSorted(),
      evidence?.lastActivity,
      evidence?.fence,
    )
    if (summary.reasons.length === 0) selectedSessions.push(summary)
    else excludedSessions.push(summary)
  }

  const selectedIDs = new Set(selectedSessions.map((item) => item.sessionID))
  for (const item of selectedSessions) {
    const row = sessionRows.get(item.sessionID)!
    const budget = validationBudgets.get(item.sessionID)!
    if (row.parent_id !== null) {
      const parent = db.query<{ id: string }, [string]>("SELECT id FROM session WHERE id = ?").get(row.parent_id)
      if (!parent) item.reasons.push("parent-session-missing")
      else if (!onlineConsumeValidationRows(1, budget)) item.reasons.push("session-validation-bound-reached")
    }
    const cycle = onlineParentCycle(db, item.sessionID, budget)
    if (cycle.cycle) item.reasons.push("parent-cycle")
    if (cycle.boundReached) item.reasons.push("session-validation-bound-reached")
    const tree = onlineDescendants(db, item.sessionID, budget)
    if (tree.boundReached) {
      item.reasons.push("session-validation-bound-reached")
      continue
    }
    item[onlineDescendantFence] = tree.sessionIDs.filter((id) => id !== item.sessionID).toSorted()
    const protectedDescendant = tree.sessionIDs.filter((id) => id !== item.sessionID && !selectedIDs.has(id)).toSorted()
    if (protectedDescendant.length > 0) item.reasons.push(`protected-descendant:${protectedDescendant.join(",")}`)
  }
  for (const session of selectedSessions.filter((item) => item.reasons.length > 0)) {
    selectedSessions.splice(selectedSessions.indexOf(session), 1)
    excludedSessions.push(session)
  }

  let dependenciesChanged = true
  while (dependenciesChanged) {
    dependenciesChanged = false
    const eligibleIDs = selectedSessions.map((item) => item.sessionID)
    const cascadeReasons =
      schemaReasons.length === 0
        ? onlineCrossSessionCascadeFailures(db, eligibleIDs, new Set(eligibleIDs), validationBudgets)
        : []
    for (const result of cascadeReasons) {
      const candidate = selectedSessions.find((item) => item.sessionID === result.sessionID)
      if (!candidate) continue
      selectedSessions.splice(selectedSessions.indexOf(candidate), 1)
      excludedSessions.push({ ...candidate, reasons: [result.reason] })
      dependenciesChanged = true
    }
    const remaining = new Set(selectedSessions.map((item) => item.sessionID))
    for (const candidate of selectedSessions) {
      const protectedDescendant = (candidate[onlineDescendantFence] ?? []).filter((id) => !remaining.has(id))
      if (protectedDescendant.length === 0) continue
      selectedSessions.splice(selectedSessions.indexOf(candidate), 1)
      excludedSessions.push({
        ...candidate,
        reasons: [`protected-descendant:${protectedDescendant.toSorted().join(",")}`],
      })
      dependenciesChanged = true
    }
  }
  const reasons = [...new Set(globalReasons)].toSorted()
  const plannedPeakBytes = onlinePlannedPeakBytes(options, pageSize ?? null, pageCount ?? null, filename)
  const capacityReason =
    options.reserveBytes === null
      ? "reserve-bytes-unavailable"
      : availableBytes === undefined
        ? "free-space-measurement-unavailable"
        : availableBytes < options.reserveBytes + plannedPeakBytes
          ? "free-space-reserve-would-be-violated"
          : undefined
  const readinessReasons = [...reasons, ...(capacityReason ? [capacityReason] : [])]
  const cutoffText = cutoff === undefined ? null : new Date(cutoff).toISOString()
  return {
    action: "inventory",
    database: filename,
    cutoff: cutoffText,
    explicitSessionIDs: [...requested],
    selectedSessions: selectedSessions.toSorted((left, right) => left.sessionID.localeCompare(right.sessionID)),
    excludedSessions: excludedSessions.toSorted((left, right) => left.sessionID.localeCompare(right.sessionID)),
    deletionScope: { sessionIDs: selectedSessions.map((item) => item.sessionID).toSorted(), wholeSessions: true },
    reclamation: {
      ready: readinessReasons.length === 0,
      method: onlineReclaimMethod,
      autoVacuum:
        autoVacuum === 2 ? "incremental" : autoVacuum === 1 ? "full" : autoVacuum === 0 ? "none" : "unreadable",
      reasons: readinessReasons,
      pageSizeBytes: pageSize ?? null,
      pageCountBefore: pageCount ?? null,
      pageCountAfter: pageCount ?? null,
      freelistBefore: freelist ?? null,
      freelistAfter: freelist ?? null,
      reserveBytes: options.reserveBytes,
      availableBytes: availableBytes ?? null,
      plannedPeakBytes,
      bounds: onlineBoundsReport(options),
      allocatedBytesBefore: allocated,
      steps: [],
    },
    usageMeasurement: usage,
    rawTranscriptWarning: "Applying this command permanently deletes the raw transcripts in selected Sessions.",
    committed: false,
    changedRows: 0,
    changedRowsByTable: {},
    deletedSessionIDs: [],
    partialSessionIDs: [],
    remainingSessionIDs: [...requested].toSorted(),
    skippedSessions: excludedSessions.map((item) => ({ sessionID: item.sessionID, reasons: item.reasons })),
    refusalReasons: readinessReasons,
    partial: false,
    batches: [],
    sessionProgress: [
      ...selectedSessions.map((item) => ({ sessionID: item.sessionID, state: "selected", changedRows: 0 })),
      ...excludedSessions.map((item) => ({
        sessionID: item.sessionID,
        state: "skipped",
        changedRows: 0,
        reasons: item.reasons,
      })),
    ],
  }
}

function onlineExcludedSession(
  sessionID: string,
  directory: string | null,
  directoryIdentity: string | null,
  directoryState: string,
  parentSessionID: string | null,
  reasons: string[],
  lastActivity?: number,
  fence?: OnlineSessionFence,
): OnlineSessionSummary {
  return {
    sessionID,
    directory,
    directoryIdentity,
    directoryState,
    parentSessionID,
    lastActivity: lastActivity === undefined ? null : new Date(lastActivity).toISOString(),
    reasons,
    ...(fence ? { [onlineFence]: fence } : {}),
  }
}

function onlineValidationBudget(): OnlineValidationBudget {
  return { remainingRows: onlineBounds.validationRows }
}

function onlineConsumeValidationRows(rows: number, budget: OnlineValidationBudget) {
  if (rows > budget.remainingRows) {
    budget.remainingRows = 0
    return false
  }
  budget.remainingRows -= rows
  return true
}

const onlineWorkExpressions: Record<string, string[]> = {
  session: ["time_compacting AS work_compacting"],
  message: [
    "json_valid(data) AS work_json_valid",
    "CASE WHEN json_valid(data) THEN json_extract(data, '$.role') END AS work_role",
    "CASE WHEN json_valid(data) THEN json_type(data, '$.time.completed') END AS work_completed_type",
  ],
  part: [
    "json_valid(data) AS work_json_valid",
    "CASE WHEN json_valid(data) THEN json_extract(data, '$.state.status') END AS work_part_status",
  ],
  session_message: [
    "type AS work_type",
    "json_valid(data) AS work_json_valid",
    "CASE WHEN json_valid(data) THEN json_type(data, '$.time.completed') END AS work_completed_type",
  ],
  session_input: ["promoted_seq AS work_promoted_seq", "delivery AS work_delivery"],
  session_prompt_queue: [
    "time_promoted AS work_time_promoted",
    "time_withdrawn AS work_time_withdrawn",
    "time_preparing AS work_time_preparing",
  ],
  session_prompt_receipt: ["state AS work_state"],
  session_ask: ["time_settled AS work_time_settled"],
  event_retention: ["state AS work_state"],
  event_aggregate_writer: ["writer_id AS work_writer_id"],
}

const onlineFenceOnlyTables = [
  {
    table: "session_context_epoch",
    scope: "session_id",
    fields: [
      "baseline_seq",
      "length(CAST(baseline AS BLOB)) AS baseline_bytes",
      "length(CAST(snapshot AS BLOB)) AS snapshot_bytes",
    ],
  },
  { table: "session_prompt_queue_sequence", scope: "session_id", fields: ["seq", "aliases_indexed"] },
  {
    table: "session_prompt_provenance",
    scope: "session_id",
    fields: ["message_id", "length(CAST(origins AS BLOB)) AS origins_bytes"],
  },
]

const onlinePayloadColumns: Record<string, string[]> = {
  session: ["title", "path", "summary_diffs", "metadata", "permission", "agent", "model", "small_model"],
  message: ["data"],
  part: ["data"],
  todo: ["content"],
  session_message: ["data"],
  session_input: ["prompt"],
  session_context_epoch: ["baseline", "snapshot"],
  session_prompt_queue: ["input", "origins"],
  session_prompt_provenance: ["origins"],
  session_prompt_receipt: ["fingerprint"],
  session_ask: ["directory", "tool", "request", "decision"],
  session_share: ["secret", "url"],
  event: ["data"],
  event_retention: ["evidence"],
}

function onlinePayloadExpression(table: string, schema: ReturnType<typeof inspectSchema>) {
  const columns = onlinePayloadColumns[table]?.filter((column) => schema.columnMap.get(table)?.has(column)) ?? []
  if (columns.length === 0) return "0"
  return columns.map((column) => `coalesce(length(CAST(${quote(column)} AS BLOB)), 0)`).join(" + ")
}

function onlineSelectScope(table: string, scope: string, sessionID: string, schema: ReturnType<typeof inspectSchema>) {
  const payload = onlinePayloadExpression(table, schema)
  const dependency =
    table === "message"
      ? ` AND NOT EXISTS (SELECT 1 FROM part child WHERE child.message_id = ${quote(table)}.id)`
      : table === "session_prompt_queue"
        ? ` AND NOT EXISTS (SELECT 1 FROM session_prompt_queue_alias alias WHERE alias.item_id = ${quote(table)}.id)`
        : table === "event_sequence"
          ? ` AND NOT EXISTS (SELECT 1 FROM event child WHERE child.aggregate_id = ${quote(table)}.aggregate_id)`
          : ""
  return {
    query: `SELECT rowid, ${payload} AS payload_bytes FROM ${quote(table)} WHERE ${scope}${dependency} ORDER BY rowid LIMIT ?`,
    sessionID,
  }
}

function onlinePlanSessionBatch(
  db: SqliteAccess,
  sessionID: string,
  options: OnlineOptions,
  schema: ReturnType<typeof inspectSchema>,
) {
  let remainingRows = options.batchRows
  let remainingBytes = options.batchBytes
  let payloadBytes = 0
  let selectedRows = 0
  const plan = new Array<{ table: string; rowids: number[]; payloadBytes: number }>()
  let blockedReason: string | undefined
  for (const [table, scope] of storageScopes) {
    if (table === "session") continue
    if (remainingRows <= 0 || remainingBytes <= 0) break
    const selected = onlineSelectScope(table, scope, sessionID, schema)
    const rows = db
      .query<{ rowid: number; payload_bytes: number }, [string, number]>(selected.query)
      .all(selected.sessionID, remainingRows + 1)
    if (rows.length > remainingRows) rows.length = remainingRows
    const rowids = new Array<number>()
    let selectedBytes = 0
    for (const row of rows) {
      if (!Number.isSafeInteger(row.payload_bytes) || row.payload_bytes < 0) {
        blockedReason = `session-payload-size-unreadable:${table}:${sessionID}`
        break
      }
      if (row.payload_bytes > remainingBytes - selectedBytes) {
        if (row.payload_bytes > options.batchBytes)
          blockedReason = `session-payload-exceeds-batch-byte-bound:${table}:${sessionID}`
        else if (rowids.length === 0 && selectedRows === 0)
          blockedReason = `session-payload-exceeds-batch-byte-bound:${table}:${sessionID}`
        break
      }
      rowids.push(row.rowid)
      selectedBytes += row.payload_bytes
    }
    if (rowids.length === 0) {
      if (blockedReason) break
      continue
    }
    plan.push({ table, rowids, payloadBytes: selectedBytes })
    selectedRows += rowids.length
    remainingRows -= rowids.length
    remainingBytes -= selectedBytes
    payloadBytes += selectedBytes
    if (blockedReason) break
  }
  return { plan, changedRows: selectedRows, payloadBytes, blockedReason }
}

function onlineCommitDeletePlan(db: SqliteAccess, sessionID: string, plan: ReturnType<typeof onlinePlanSessionBatch>) {
  const changedRowsByTable: Record<string, number> = {}
  for (const item of plan.plan) {
    const changes = db
      .query(`DELETE FROM ${quote(item.table)} WHERE rowid IN (${item.rowids.map(() => "?").join(",")})`)
      .run(...item.rowids).changes
    if (changes !== item.rowids.length) throw new Error(`online-delete-count-mismatch:${item.table}:${sessionID}`)
    changedRowsByTable[item.table] = (changedRowsByTable[item.table] ?? 0) + changes
  }
  return changedRowsByTable
}

function onlineHasRows(db: SqliteAccess, table: string, scope: string, sessionID: string) {
  return Boolean(db.query(`SELECT rowid FROM ${quote(table)} WHERE ${scope} LIMIT 1`).get(sessionID))
}

function onlineHasSessionStorage(db: SqliteAccess, sessionID: string) {
  return storageScopes.some(([table, scope]) => table !== "session" && onlineHasRows(db, table, scope, sessionID))
}

function onlineRemainingStorageTables(db: SqliteAccess, sessionID: string) {
  return storageScopes
    .filter(([table, scope]) => table !== "session" && onlineHasRows(db, table, scope, sessionID))
    .map(([table]) => table)
}

function onlineDeletePeakBytes(rows: number, payloadBytes: number, pageSize: number) {
  return Math.max(pageSize, payloadBytes * 2 + rows * pageSize * 32)
}

type OnlineDeleteBatchResult = {
  state: "committed" | "empty" | "skipped" | "global-refusal" | "failed"
  batch?: number
  sessionID: string
  changedRows: number
  changedRowsByTable: Record<string, number>
  payloadBytes: number
  plannedPeakBytes?: number
  availableBytes?: number
  fence?: OnlineSessionFence
  blockedReason?: string
  reasons?: string[]
  reason?: string
  attempts?: number
  hasRemainingRows?: boolean
  remainingStorageTables?: string[]
}

async function onlineDeleteOneBatch(
  db: SqliteAccess,
  filename: string,
  selected: OnlineSessionSummary,
  expectedFence: OnlineSessionFence | undefined,
  cutoff: number | undefined,
  eligibleIDs: ReadonlySet<string>,
  skippedIDs: ReadonlySet<string>,
  completedIDs: ReadonlySet<string>,
  options: OnlineOptions,
  batchNumber: number,
): Promise<OnlineDeleteBatchResult> {
  const sessionID = selected.sessionID
  const acquired = onlineBeginImmediate(db)
  if (!acquired.started)
    return {
      state: "failed",
      sessionID,
      changedRows: 0,
      changedRowsByTable: {},
      payloadBytes: 0,
      reason: "database-busy-after-retries",
      attempts: acquired.attempts,
    }
  try {
    const validation = await onlineRevalidateSession(
      db,
      filename,
      selected,
      expectedFence,
      cutoff,
      eligibleIDs,
      skippedIDs,
      completedIDs,
    )
    if (validation.globalReasons.length > 0)
      return {
        state: "global-refusal",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        reasons: validation.globalReasons,
      }
    if (validation.reasons.length > 0)
      return {
        state: "skipped",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        reasons: validation.reasons,
      }

    const schema = inspectSchema(db)
    const plan = onlinePlanSessionBatch(db, sessionID, options, schema)
    if (plan.blockedReason && plan.changedRows === 0)
      return {
        state: "skipped",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        reasons: [plan.blockedReason],
      }
    if (plan.changedRows === 0) {
      const remainingStorageTables = onlineRemainingStorageTables(db, sessionID)
      return {
        state: "empty",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        ...(plan.blockedReason ? { blockedReason: plan.blockedReason } : {}),
        hasRemainingRows: remainingStorageTables.length > 0,
        remainingStorageTables,
      }
    }

    const state = onlinePageState(db)
    const availableBytes = await onlineAvailableBytes(filename)
    if (!state || availableBytes === undefined)
      return {
        state: "global-refusal",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        reasons: ["free-space-or-page-measurement-unavailable"],
      }
    const plannedPeakBytes = onlineDeletePeakBytes(plan.changedRows, plan.payloadBytes, state.pageSize)
    if (availableBytes < options.reserveBytes! + plannedPeakBytes)
      return {
        state: "global-refusal",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        reasons: ["capacity-reserve-reached"],
        availableBytes,
        plannedPeakBytes,
      }

    const changedRowsByTable = onlineCommitDeletePlan(db, sessionID, plan)
    const remainingStorageTables = onlineRemainingStorageTables(db, sessionID)
    const fence = expectedFence ? onlineFenceAfterDelete(expectedFence, plan) : undefined
    db.exec("COMMIT")
    return {
      state: "committed",
      batch: batchNumber,
      sessionID,
      changedRows: plan.changedRows,
      changedRowsByTable,
      payloadBytes: plan.payloadBytes,
      plannedPeakBytes,
      availableBytes,
      fence,
      remainingStorageTables,
      ...(plan.blockedReason ? { blockedReason: plan.blockedReason } : {}),
    }
  } catch (error) {
    if (db.inTransaction) db.exec("ROLLBACK")
    return {
      state: "failed",
      sessionID,
      changedRows: 0,
      changedRowsByTable: {},
      payloadBytes: 0,
      reason: error instanceof Error ? error.message : "online-delete-batch-failed",
    }
  } finally {
    if (db.inTransaction) db.exec("ROLLBACK")
  }
}

async function onlineDeleteSessionRow(
  db: SqliteAccess,
  filename: string,
  selected: OnlineSessionSummary,
  expectedFence: OnlineSessionFence | undefined,
  cutoff: number | undefined,
  eligibleIDs: ReadonlySet<string>,
  skippedIDs: ReadonlySet<string>,
  completedIDs: ReadonlySet<string>,
  options: OnlineOptions,
  batchNumber: number,
): Promise<OnlineDeleteBatchResult> {
  const sessionID = selected.sessionID
  const acquired = onlineBeginImmediate(db)
  if (!acquired.started)
    return {
      state: "failed",
      sessionID,
      changedRows: 0,
      changedRowsByTable: {},
      payloadBytes: 0,
      reason: "database-busy-after-retries",
      attempts: acquired.attempts,
    }
  try {
    const validation = await onlineRevalidateSession(
      db,
      filename,
      selected,
      expectedFence,
      cutoff,
      eligibleIDs,
      skippedIDs,
      completedIDs,
    )
    if (validation.globalReasons.length > 0)
      return {
        state: "global-refusal",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        reasons: validation.globalReasons,
      }
    if (validation.reasons.length > 0)
      return {
        state: "skipped",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        reasons: validation.reasons,
      }
    const remainingStorageTables = onlineRemainingStorageTables(db, sessionID)
    if (remainingStorageTables.length > 0)
      return {
        state: "skipped",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        reasons: [`session-storage-remains:${remainingStorageTables.join(",")}`],
        remainingStorageTables,
      }
    const survivingDescendants = validation.descendantIDs
    if (survivingDescendants.length > 0)
      return {
        state: "skipped",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        reasons: [`protected-descendant:${survivingDescendants.toSorted().join(",")}`],
      }

    const state = onlinePageState(db)
    const availableBytes = await onlineAvailableBytes(filename)
    if (!state || availableBytes === undefined)
      return {
        state: "global-refusal",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        reasons: ["free-space-or-page-measurement-unavailable"],
      }
    const plannedPeakBytes = onlineDeletePeakBytes(1, 0, state.pageSize)
    if (availableBytes < options.reserveBytes! + plannedPeakBytes)
      return {
        state: "global-refusal",
        sessionID,
        changedRows: 0,
        changedRowsByTable: {},
        payloadBytes: 0,
        reasons: ["capacity-reserve-reached"],
        availableBytes,
        plannedPeakBytes,
      }
    const changes = db.query("DELETE FROM session WHERE id = ?").run(sessionID).changes
    if (changes !== 1) throw new Error(`session-row-delete-count-mismatch:${sessionID}`)
    db.exec("COMMIT")
    return {
      state: "committed",
      batch: batchNumber,
      sessionID,
      changedRows: 1,
      changedRowsByTable: { session: 1 },
      payloadBytes: 0,
      plannedPeakBytes,
      availableBytes,
      remainingStorageTables: [],
    }
  } catch (error) {
    if (db.inTransaction) db.exec("ROLLBACK")
    return {
      state: "failed",
      sessionID,
      changedRows: 0,
      changedRowsByTable: {},
      payloadBytes: 0,
      reason: error instanceof Error ? error.message : "online-session-delete-failed",
    }
  } finally {
    if (db.inTransaction) db.exec("ROLLBACK")
  }
}

async function applyOnlineScope(
  db: SqliteAccess,
  filename: string,
  cutoff: number | undefined,
  options: OnlineOptions,
  inventory: OnlinePruneReport,
): Promise<OnlinePruneReport> {
  const requested = inventory.explicitSessionIDs ?? []
  const candidates = inventory.selectedSessions
  const eligibleIDs = new Set(candidates.map((item) => item.sessionID))
  const skipped = new Map(inventory.excludedSessions.map((item) => [item.sessionID, item.reasons]))
  const completed = new Set<string>()
  const expectedFences = new Map(
    candidates.flatMap((item) => (item[onlineFence] ? [[item.sessionID, item[onlineFence]!] as const] : [])),
  )
  const progress = new Map(
    requested.map((sessionID) => [sessionID, { changedRows: 0, changedRowsByTable: {} as Record<string, number> }]),
  )
  const batches = new Array<Record<string, unknown>>()
  const steps = new Array<Record<string, unknown>>()
  const changedRowsByTable: Record<string, number> = {}
  let changedRows = 0
  let pagesRequested = 0
  let batchNumber = 0
  let committed = false
  let failureReason: string | undefined

  try {
    while (!failureReason) {
      const pending = candidates.filter(
        (item) =>
          !skipped.has(item.sessionID) && !completed.has(item.sessionID) && onlineHasSessionStorage(db, item.sessionID),
      )
      if (pending.length === 0) break
      let roundProgress = false
      const stalled = new Array<string>()
      for (const item of pending) {
        if (batchNumber >= options.maxBatches) {
          failureReason = "delete-batch-limit-reached"
          break
        }
        batchNumber += 1
        const currentProgress = progress.get(item.sessionID)!
        const result = await onlineDeleteOneBatch(
          db,
          filename,
          item,
          expectedFences.get(item.sessionID),
          cutoff,
          eligibleIDs,
          new Set(skipped.keys()),
          completed,
          options,
          batchNumber,
        )
        if (result.state === "empty") {
          if (result.hasRemainingRows) stalled.push(item.sessionID)
          continue
        }
        if (result.state === "skipped") {
          skipped.set(item.sessionID, result.reasons ?? ["session-not-eligible"])
          continue
        }
        if (result.state === "global-refusal" || result.state === "failed") {
          failureReason = result.reasons?.[0] ?? result.reason ?? "online-delete-batch-failed"
          break
        }

        committed = true
        roundProgress = true
        changedRows += result.changedRows
        currentProgress.changedRows += result.changedRows
        for (const [table, count] of Object.entries(result.changedRowsByTable)) {
          currentProgress.changedRowsByTable[table] = (currentProgress.changedRowsByTable[table] ?? 0) + count
          changedRowsByTable[table] = (changedRowsByTable[table] ?? 0) + count
        }
        if (result.fence) expectedFences.set(item.sessionID, result.fence)
        else expectedFences.delete(item.sessionID)
        batches.push({
          batch: batchNumber,
          sessionID: item.sessionID,
          changedRows: result.changedRows,
          changedRowsByTable: result.changedRowsByTable,
          payloadBytes: result.payloadBytes,
          plannedPeakBytes: result.plannedPeakBytes,
          availableBytes: result.availableBytes,
          remainingStorageTables: result.remainingStorageTables,
          deletedSessionIDs: [...completed].toSorted(),
          remainingSessionIDs: requested.filter((id) => !completed.has(id)).toSorted(),
        })

        const step = await onlineReclaimStep(db, filename, options, pagesRequested)
        pagesRequested += step.pagesRequested
        committed ||= step.committed
        steps.push(...step.steps.map((record) => ({ batch: batchNumber, sessionID: item.sessionID, ...record })))
        if (step.reason) {
          failureReason = step.reason
          break
        }
        if (result.blockedReason) {
          skipped.set(item.sessionID, [result.blockedReason])
        }
      }
      if (failureReason) break
      const remaining = candidates.filter(
        (item) =>
          !skipped.has(item.sessionID) && !completed.has(item.sessionID) && onlineHasSessionStorage(db, item.sessionID),
      )
      if (remaining.length === 0) break
      if (!roundProgress) {
        for (const item of remaining) {
          if (stalled.includes(item.sessionID)) skipped.set(item.sessionID, ["session-storage-dependency-unresolved"])
        }
        if (remaining.every((item) => skipped.has(item.sessionID))) break
        failureReason = "online-delete-made-no-progress"
      }
    }

    const remainingPayload = candidates.filter(
      (item) =>
        !skipped.has(item.sessionID) && !completed.has(item.sessionID) && onlineHasSessionStorage(db, item.sessionID),
    )
    if (!failureReason && committed && remainingPayload.length === 0) {
      const drained = await onlineReclaimUntilDone(db, filename, options, pagesRequested)
      pagesRequested = drained.pagesRequested
      committed ||= drained.committed
      steps.push(...drained.steps)
      if (!drained.complete) failureReason = drained.reason ?? "reclamation-incomplete"
    }

    if (!failureReason && remainingPayload.length === 0) {
      const sessionsToDelete = candidates
        .filter((item) => !skipped.has(item.sessionID) && !completed.has(item.sessionID))
        .toSorted(
          (left, right) =>
            (left[onlineDescendantFence]?.length ?? 0) - (right[onlineDescendantFence]?.length ?? 0) ||
            left.sessionID.localeCompare(right.sessionID),
        )
      for (const item of sessionsToDelete) {
        if (batchNumber >= options.maxBatches) {
          failureReason = "delete-batch-limit-reached"
          break
        }
        batchNumber += 1
        const currentProgress = progress.get(item.sessionID)!
        const result = await onlineDeleteSessionRow(
          db,
          filename,
          item,
          expectedFences.get(item.sessionID),
          cutoff,
          eligibleIDs,
          new Set(skipped.keys()),
          completed,
          options,
          batchNumber,
        )
        if (result.state === "skipped") {
          skipped.set(item.sessionID, result.reasons ?? ["session-not-eligible"])
          continue
        }
        if (result.state !== "committed") {
          failureReason = result.reasons?.[0] ?? result.reason ?? "online-session-delete-failed"
          break
        }
        committed = true
        completed.add(item.sessionID)
        changedRows += result.changedRows
        currentProgress.changedRows += result.changedRows
        currentProgress.changedRowsByTable.session = (currentProgress.changedRowsByTable.session ?? 0) + 1
        changedRowsByTable.session = (changedRowsByTable.session ?? 0) + 1
        batches.push({
          batch: batchNumber,
          sessionID: item.sessionID,
          changedRows: result.changedRows,
          changedRowsByTable: result.changedRowsByTable,
          payloadBytes: 0,
          plannedPeakBytes: result.plannedPeakBytes,
          availableBytes: result.availableBytes,
          completedSessionIDs: [item.sessionID],
          deletedSessionIDs: [item.sessionID],
          remainingSessionIDs: requested.filter((id) => !completed.has(id)).toSorted(),
        })
        const step = await onlineReclaimStep(db, filename, options, pagesRequested)
        pagesRequested += step.pagesRequested
        committed ||= step.committed
        steps.push(...step.steps)
        if (step.reason) {
          failureReason = step.reason
          break
        }
      }
    }

    if (!failureReason && completed.size > 0) {
      const drained = await onlineReclaimUntilDone(db, filename, options, pagesRequested)
      pagesRequested = drained.pagesRequested
      committed ||= drained.committed
      steps.push(...drained.steps)
      if (!drained.complete) failureReason = drained.reason ?? "reclamation-incomplete"
    }
  } catch (error) {
    failureReason ??= error instanceof Error ? error.message : "online-apply-failed"
  }

  const pageState = (() => {
    try {
      return onlinePageState(db)
    } catch (error) {
      failureReason ??= error instanceof Error ? error.message : "database-page-measurement-after-apply-failed"
      return undefined
    }
  })()
  const allocatedAfter = await allocatedBytes(filename)
  const netAllocatedBytes =
    inventory.reclamation.allocatedBytesBefore.status === "measured" && allocatedAfter.status === "measured"
      ? inventory.reclamation.allocatedBytesBefore.totalBytes - allocatedAfter.totalBytes
      : null
  const finalCheckpoint = [...steps].toReversed().find((step) => "checkpoint" in step)?.checkpoint as
    | { busy?: number; logFrames?: number; checkpointedFrames?: number }
    | undefined
  const partialSessionIDs = [...progress]
    .filter(([sessionID, item]) => item.changedRows > 0 && !completed.has(sessionID))
    .map(([sessionID]) => sessionID)
    .toSorted()
  const sessionProgress = requested.map((sessionID) => {
    const current = progress.get(sessionID)!
    const reason = skipped.get(sessionID)
    return {
      sessionID,
      state: completed.has(sessionID)
        ? "completed"
        : reason
          ? "skipped"
          : current.changedRows > 0
            ? "partial"
            : "remaining",
      changedRows: current.changedRows,
      changedRowsByTable: current.changedRowsByTable,
      remainingStorageTables: completed.has(sessionID)
        ? []
        : (() => {
            try {
              return onlineRemainingStorageTables(db, sessionID)
            } catch (error) {
              failureReason ??= error instanceof Error ? error.message : "remaining-storage-measurement-failed"
              return null
            }
          })(),
      reasons: reason ?? [],
    }
  })
  const remainingSessionIDs = requested.filter((id) => !completed.has(id)).toSorted()
  const skippedSessions = [...skipped]
    .map(([sessionID, reasons]) => ({ sessionID, reasons }))
    .toSorted((a, b) => a.sessionID.localeCompare(b.sessionID))
  const refusalReasons = [
    ...new Set([...(failureReason ? [failureReason] : []), ...skippedSessions.flatMap((item) => item.reasons)]),
  ]
  if (remainingSessionIDs.length > 0 && refusalReasons.length === 0)
    refusalReasons.push("requested-session-work-incomplete")
  const action =
    remainingSessionIDs.length === 0 && failureReason === undefined ? "applied" : committed ? "failed" : "refused"
  return {
    ...inventory,
    action,
    committed,
    changedRows,
    changedRowsByTable,
    deletedSessionIDs: [...completed].toSorted(),
    partialSessionIDs,
    remainingSessionIDs,
    skippedSessions,
    refusalReasons,
    partial: committed && remainingSessionIDs.length > 0,
    batches,
    sessionProgress,
    reclamation: {
      ...inventory.reclamation,
      ready: failureReason === undefined,
      reasons: failureReason ? [...inventory.reclamation.reasons, failureReason] : inventory.reclamation.reasons,
      pageCountAfter: pageState?.pageCount ?? null,
      freelistAfter: pageState?.freelist ?? null,
      allocatedBytesAfter: allocatedAfter,
      netAllocatedBytes,
      remainingFreelistPages: pageState?.freelist ?? null,
      remainingWalFrames:
        finalCheckpoint?.logFrames === undefined || finalCheckpoint?.checkpointedFrames === undefined
          ? null
          : finalCheckpoint.logFrames - finalCheckpoint.checkpointedFrames,
      steps: [...inventory.reclamation.steps, ...steps],
    },
  }
}

function onlineBeginImmediate(db: SqliteAccess) {
  for (const attempt of Array.from({ length: onlineBounds.busyAttempts }, (_, index) => index + 1)) {
    try {
      db.exec("BEGIN IMMEDIATE")
      return { started: true as const, attempts: attempt }
    } catch (error) {
      if (!onlineBusy(error)) throw error
    }
  }
  return { started: false as const, attempts: onlineBounds.busyAttempts }
}

function onlineBusy(error: unknown) {
  if (!(error instanceof Error)) return false
  return error.message.includes("SQLITE_BUSY") || error.message.includes("database is locked")
}

async function onlineRevalidateSession(
  db: SqliteAccess,
  filename: string,
  selected: OnlineSessionSummary,
  expectedFence: OnlineSessionFence | undefined,
  cutoff: number | undefined,
  eligibleIDs: ReadonlySet<string>,
  skippedIDs: ReadonlySet<string>,
  completedIDs: ReadonlySet<string>,
) {
  const budget = onlineValidationBudget()
  const schema = inspectSchema(db)
  const globalReasons = [
    ...schema.failures,
    ...schema.unclassifiedTables.map((table) => `unclassified-session-storage:${table}`),
    ...onlineScopeIndexFailures(db, schema.tables),
    ...(db.query<{ auto_vacuum: number }, []>("PRAGMA auto_vacuum").get()?.auto_vacuum === 2
      ? []
      : ["incremental-vacuum-unavailable"]),
  ]
  const currentHolders = scanDatabaseHolders(filename)
  globalReasons.push(...onlineHolderFailures(db, currentHolders))
  if (globalReasons.length > 0)
    return {
      globalReasons: [...new Set(globalReasons)].toSorted(),
      reasons: [],
      fence: undefined,
      descendantIDs: [],
    }

  const reasons = new Set<string>()
  const row = db
    .query<
      SessionRow,
      [string]
    >("SELECT id, directory, parent_id, time_created, time_updated FROM session WHERE id = ?")
    .get(selected.sessionID)
  if (!row) reasons.add(`session-row-missing:${selected.sessionID}`)
  if (row && !onlineConsumeValidationRows(1, budget)) reasons.add("session-validation-bound-reached")
  if (row && (row.directory !== selected.directory || row.parent_id !== selected.parentSessionID))
    reasons.add("selection-changed-after-inventory")
  if (row) {
    const identity = await directoryIdentity(row.directory)
    const state = identity === undefined ? "unknown" : await inspectDirectory(identity)
    if (state !== "missing") reasons.add(`session-directory-${state}`)
    if (identity !== selected.directoryIdentity) reasons.add("session-directory-identity-changed")
  }

  const evidence = onlineSessionEvidence(db, schema, selected.sessionID, budget)
  for (const reason of evidence.reasons) reasons.add(reason)
  if (cutoff !== undefined && evidence.lastActivity !== undefined && evidence.lastActivity >= cutoff)
    reasons.add("activity-at-or-after-cutoff")
  if (expectedFence && evidence.complete && evidence.fence.data !== expectedFence.data)
    reasons.add("session-changed-after-inventory")

  const cycle = onlineParentCycle(db, selected.sessionID, budget)
  if (cycle.cycle) reasons.add("parent-cycle")
  if (cycle.boundReached) reasons.add("session-validation-bound-reached")
  const tree = onlineDescendants(db, selected.sessionID, budget)
  if (tree.boundReached) reasons.add("session-validation-bound-reached")
  if (!tree.boundReached) {
    const expectedDescendants = (selected[onlineDescendantFence] ?? []).filter((id) => !completedIDs.has(id))
    const currentDescendants = tree.sessionIDs.filter((id) => id !== selected.sessionID)
    if (!sameIDs(currentDescendants, expectedDescendants)) reasons.add("session-tree-changed")
    const protectedDescendants = currentDescendants.filter((id) => !eligibleIDs.has(id) || skippedIDs.has(id))
    if (protectedDescendants.length > 0)
      reasons.add(`protected-descendant:${protectedDescendants.toSorted().join(",")}`)
  }

  const activeEligibleIDs = new Set([...eligibleIDs].filter((id) => !skippedIDs.has(id) && !completedIDs.has(id)))
  const cascade = onlineCrossSessionCascadeFailures(
    db,
    [selected.sessionID],
    activeEligibleIDs,
    new Map([[selected.sessionID, budget]]),
  ).find((item) => item.sessionID === selected.sessionID)
  if (cascade) reasons.add(cascade.reason)

  return {
    globalReasons: [],
    reasons: [...reasons].toSorted(),
    fence: evidence.fence,
    descendantIDs: tree.sessionIDs.filter((id) => id !== selected.sessionID),
  }
}

function onlineSessionEvidence(
  db: SqliteAccess,
  schema: ReturnType<typeof inspectSchema>,
  sessionID: string,
  budget: OnlineValidationBudget,
) {
  const reasons = new Set<string>()
  const fence = new Array<unknown>()
  let lastActivity = Number.NEGATIVE_INFINITY
  const bounded = () => {
    reasons.add("session-validation-bound-reached")
    return {
      complete: false,
      lastActivity: undefined,
      reasons: [...reasons].toSorted(),
      fence: { data: "", eventSequence: null, eventOwnerID: null } satisfies OnlineSessionFence,
    }
  }

  for (const source of activitySources) {
    if (!schema.tables.has(source.table)) {
      reasons.add(`session-activity-evidence-unavailable:${source.table}`)
      continue
    }
    const columns = schema.columnMap.get(source.table)
    const missing = source.columns.filter((column) => !column.optional && !columns?.has(column.name))
    if (missing.length > 0 || !columns?.has(source.scope)) {
      reasons.add(
        `session-activity-evidence-unavailable:${source.table}.${missing.map((column) => column.name).join(",")}`,
      )
      continue
    }
    const timeColumns = source.columns.filter((column) => columns.has(column.name))
    const expressions = [
      "rowid AS work_rowid",
      ...timeColumns.map((column) => `${quote(column.name)} AS ${quote(column.name)}`),
      ...(onlineWorkExpressions[source.table] ?? []),
    ]
    const rows = db
      .query<
        Record<string, unknown>,
        [string, number]
      >(`SELECT ${expressions.join(", ")} FROM ${quote(source.table)} WHERE ${quote(source.scope)} = ? ORDER BY rowid LIMIT ?`)
      .all(sessionID, budget.remainingRows + 1)
    if (!onlineConsumeValidationRows(rows.length, budget)) return bounded()
    fence.push([
      source.table,
      rows.map((row) => expressions.map((expression) => resultAlias(expression)).map((key) => row[key])),
    ])
    for (const row of rows) {
      for (const column of timeColumns) {
        const value = row[column.name]
        if (typeof value === "number" && Number.isFinite(value)) {
          lastActivity = Math.max(lastActivity, value)
          continue
        }
        if (value === null && column.optional) continue
        reasons.add(`session-activity-unreadable:${source.table}.${column.name}`)
      }
      if (source.table === "session" && row.work_compacting !== null) reasons.add("session-compaction-in-flight")
      if (source.table === "message") {
        if (row.work_json_valid !== 1) reasons.add("session-work-state-unreadable")
        else if (
          row.work_role === "assistant" &&
          row.work_completed_type !== "integer" &&
          row.work_completed_type !== "real"
        )
          reasons.add("assistant-or-tool-work-in-flight")
      }
      if (source.table === "part") {
        if (row.work_json_valid !== 1) reasons.add("session-work-state-unreadable")
        if (row.work_part_status === "pending" || row.work_part_status === "running")
          reasons.add("assistant-or-tool-work-in-flight")
      }
      if (source.table === "session_message") {
        if (row.work_json_valid !== 1) reasons.add("session-work-state-unreadable")
        else if (
          row.work_type === "assistant" &&
          row.work_completed_type !== "integer" &&
          row.work_completed_type !== "real"
        )
          reasons.add("assistant-or-tool-work-in-flight")
      }
      if (source.table === "session_input") {
        if (row.work_promoted_seq === null) reasons.add("v2-input-pending")
        if (row.work_delivery !== "steer" && row.work_delivery !== "queue")
          reasons.add("session-input-state-unreadable")
      }
      if (source.table === "session_prompt_queue") {
        if (row.work_time_promoted === null && row.work_time_withdrawn === null)
          reasons.add("v1-prompt-queue-row-present")
        if (row.work_time_preparing !== null) reasons.add("v1-prompt-queue-row-present")
        if (row.work_time_promoted !== null && row.work_time_withdrawn !== null)
          reasons.add("v1-queue-state-unreadable")
      }
      if (source.table === "session_prompt_receipt") {
        if (row.work_state === "pending") reasons.add("v1-prompt-receipt-pending")
        if (
          row.work_state !== "pending" &&
          row.work_state !== "withdrawn" &&
          row.work_state !== "delivered" &&
          row.work_state !== "merged" &&
          row.work_state !== "superseded" &&
          row.work_state !== "deleted"
        )
          reasons.add("session-input-state-unreadable")
      }
      if (source.table === "session_ask" && row.work_time_settled === null) reasons.add("supervised-ask-pending")
      if (source.table === "event_retention" && (row.work_state === "scanning" || row.work_state === "redacting"))
        reasons.add("session-retention-in-flight")
    }
  }

  for (const source of onlineFenceOnlyTables) {
    if (!schema.tables.has(source.table)) continue
    const rows = db
      .query<
        Record<string, unknown>,
        [string, number]
      >(`SELECT rowid AS work_rowid, ${source.fields.join(", ")} FROM ${quote(source.table)} WHERE ${quote(source.scope)} = ? ORDER BY rowid LIMIT ?`)
      .all(sessionID, budget.remainingRows + 1)
    if (!onlineConsumeValidationRows(rows.length, budget)) return bounded()
    fence.push([source.table, rows])
  }

  const aliasRows = db
    .query<{ work_rowid: number; id: string; item_id: string }, [string, number]>(
      `SELECT alias.rowid AS work_rowid, alias.id, alias.item_id
       FROM session_prompt_queue_alias alias
       JOIN session_prompt_queue queue ON queue.id = alias.item_id
       WHERE queue.session_id = ? ORDER BY alias.rowid LIMIT ?`,
    )
    .all(sessionID, budget.remainingRows + 1)
  if (!onlineConsumeValidationRows(aliasRows.length, budget)) return bounded()
  fence.push(["session_prompt_queue_alias", aliasRows])

  const sequenceRows = db
    .query<
      { rowid: number; seq: number; owner_id: string | null },
      [string, number]
    >("SELECT rowid, seq, owner_id FROM event_sequence WHERE aggregate_id = ? LIMIT ?")
    .all(sessionID, budget.remainingRows + 1)
  if (!onlineConsumeValidationRows(sequenceRows.length, budget)) return bounded()
  const sequence = sequenceRows[0]
  const events = db
    .query<
      { rowid: number; seq: number; type: string },
      [string, number]
    >("SELECT rowid, seq, type FROM event WHERE aggregate_id = ? ORDER BY seq LIMIT ?")
    .all(sessionID, budget.remainingRows + 1)
  if (!onlineConsumeValidationRows(events.length, budget)) return bounded()
  const validSequence =
    sequence === undefined ||
    (Number.isSafeInteger(sequence.rowid) &&
      sequence.rowid > 0 &&
      Number.isSafeInteger(sequence.seq) &&
      sequence.seq >= 0 &&
      (sequence.owner_id === null || typeof sequence.owner_id === "string"))
  const validEvents = events.every(
    (row) =>
      Number.isSafeInteger(row.rowid) &&
      row.rowid > 0 &&
      Number.isSafeInteger(row.seq) &&
      row.seq >= 0 &&
      typeof row.type === "string",
  )
  if (!validSequence || !validEvents) reasons.add(`event-sequence-unreadable:${sessionID}`)
  const maxEvent = validEvents ? (events.at(-1)?.seq ?? null) : null
  if (sequence?.owner_id) reasons.add(`event-aggregate-owner-claimed:${sessionID}`)
  if (sequence && (maxEvent ?? -1) > sequence.seq) reasons.add(`event-sequence-unreadable:${sessionID}`)
  if (!sequence && maxEvent !== null) reasons.add(`event-sequence-unreadable:${sessionID}`)
  const eventTypes = validEvents ? [...new Set(events.map((row) => row.type))].toSorted() : []
  for (const type of eventTypes) {
    const definition = Durable.get(type)
    if (!definition?.durable || definition.durable.aggregate !== "sessionID")
      reasons.add(`unknown-durable-event-owner:${type}`)
  }
  fence.push(["event", events.map((row) => [row.rowid, row.seq, row.type])])
  fence.push([
    "event_sequence",
    [sequence?.rowid ?? null, sequence?.seq ?? null, sequence?.owner_id ?? null, maxEvent, eventTypes],
  ])
  for (const reason of onlineWriterFailures(db, sessionID, budget)) reasons.add(reason)
  if (Number.isFinite(lastActivity) === false) reasons.add("activity-evidence-unreadable:no-timestamp")

  return {
    complete: true,
    lastActivity: Number.isFinite(lastActivity) ? lastActivity : undefined,
    reasons: [...reasons].toSorted(),
    fence: {
      data: JSON.stringify(fence),
      eventSequence: sequence?.seq ?? null,
      eventOwnerID: sequence?.owner_id ?? null,
    } satisfies OnlineSessionFence,
  }
}

function resultAlias(expression: string) {
  const alias = /\s+AS\s+"?([A-Za-z_][A-Za-z0-9_]*)"?$/i.exec(expression)
  return alias?.[1] ?? expression.replaceAll('"', "")
}

function onlineFenceAfterDelete(fence: OnlineSessionFence, plan: ReturnType<typeof onlinePlanSessionBatch>) {
  const deletedRows = new Map(plan.plan.map((item) => [item.table, new Set(item.rowids)]))
  const data = JSON.parse(fence.data) as [string, unknown][]
  const sequenceDeletes = deletedRows.get("event_sequence")
  const entries: [string, unknown][] = data.map(([table, value]) => {
    const deleted = deletedRows.get(table)
    if (!deleted) return [table, value]
    if (table === "event_sequence" && Array.isArray(value)) {
      if (typeof value[0] === "number" && sequenceDeletes?.has(value[0])) return [table, [null, null, null, null, []]]
      return [table, value]
    }
    if (!Array.isArray(value)) return [table, value]
    return [
      table,
      value.filter((row: unknown) => {
        if (Array.isArray(row)) return typeof row[0] !== "number" || !deleted.has(row[0])
        if (row && typeof row === "object" && "work_rowid" in row)
          return typeof row.work_rowid !== "number" || !deleted.has(row.work_rowid)
        return true
      }),
    ]
  })
  const eventRows = entries.find(([table]) => table === "event")?.[1]
  const sequence = entries.find(([table]) => table === "event_sequence")?.[1]
  if (Array.isArray(eventRows) && Array.isArray(sequence) && sequence[0] !== null) {
    const rows = (eventRows as unknown[]).flatMap((row): [number, number, string][] => {
      if (!Array.isArray(row)) return []
      const values = row as unknown[]
      return typeof values[0] === "number" && typeof values[1] === "number" && typeof values[2] === "string"
        ? [[values[0], values[1], values[2]]]
        : []
    })
    sequence[3] = rows.at(-1)?.[1] ?? null
    sequence[4] = [...new Set(rows.map((row) => row[2]))].toSorted()
  }
  const sequenceDeleted =
    sequenceDeletes !== undefined &&
    data.some(
      ([table, value]) =>
        table === "event_sequence" &&
        Array.isArray(value) &&
        typeof value[0] === "number" &&
        sequenceDeletes.has(value[0]),
    )
  return {
    data: JSON.stringify(entries),
    eventSequence: sequenceDeleted ? null : fence.eventSequence,
    eventOwnerID: sequenceDeleted ? null : fence.eventOwnerID,
  } satisfies OnlineSessionFence
}

function onlineHolderFailures(db: SqliteAccess, current: ReturnType<typeof scanDatabaseHolders>) {
  if (current.state === "unreadable") return [`database-liveness-unreadable:${current.reason}`]
  const reasons = new Set<string>()
  if (current.target === "memory") reasons.add("database-holder-scan-target-mismatch")
  try {
    const registrations = db.query<{ pid: number; boot_id: string | null; start_ticks: string | null }, [number]>(
      "SELECT pid, boot_id, start_ticks FROM event_writer_process WHERE pid = ?",
    )
    for (const pid of current.pids) {
      const identity = EventWriter.readIdentity(pid)
      if (!identity) continue
      if (!identity.bootID || !identity.startTicks) {
        reasons.add(`database-holder-identity-unreadable:${pid}`)
        continue
      }
      if (!registrations.all(pid).some((row) => onlineSameProcess(row, identity))) {
        reasons.add(`unregistered-database-holder:${pid}`)
      }
    }
  } catch (error) {
    reasons.add(`database-writer-registration-unreadable:${error instanceof Error ? error.message : "unknown"}`)
  }
  return [...reasons].toSorted()
}

function onlineWriterFailures(db: SqliteAccess, sessionID: string, budget: OnlineValidationBudget) {
  const rows = db
    .query<{ pid: number | null; boot_id: string | null; start_ticks: string | null }, [string, number]>(
      `SELECT process.pid, process.boot_id, process.start_ticks
       FROM event_aggregate_writer writer
       LEFT JOIN event_writer_process process ON process.writer_id = writer.writer_id
       WHERE writer.aggregate_id = ?
       LIMIT ?`,
    )
    .all(sessionID, budget.remainingRows + 1)
  if (!onlineConsumeValidationRows(rows.length, budget)) return ["session-validation-bound-reached"]
  const reasons = new Set<string>()
  for (const row of rows) {
    if (
      typeof row.pid !== "number" ||
      !Number.isSafeInteger(row.pid) ||
      row.pid <= 0 ||
      typeof row.boot_id !== "string" ||
      !row.boot_id ||
      typeof row.start_ticks !== "string" ||
      !row.start_ticks
    ) {
      reasons.add("writer-liveness-unreadable")
      continue
    }
    const current = EventWriter.readIdentity(row.pid)
    if (!current) continue
    if (!current.bootID || !current.startTicks) {
      reasons.add("writer-liveness-unreadable")
      continue
    }
    if (onlineSameProcess(row, current)) reasons.add("session-written-by-live-process")
  }
  return [...reasons]
}

function onlineSameProcess(
  registered: { boot_id: string | null; start_ticks: string | null },
  current: EventWriter.ProcessIdentity,
) {
  return (
    registered.boot_id !== null &&
    registered.start_ticks !== null &&
    registered.boot_id === current.bootID &&
    registered.start_ticks === current.startTicks
  )
}

function onlineScopeIndexFailures(db: SqliteAccess, tables: ReadonlySet<string>) {
  const required: { table: string; columns: string[] }[] = [
    { table: "session", columns: ["id"] },
    { table: "session", columns: ["parent_id"] },
    { table: "message", columns: ["session_id"] },
    { table: "part", columns: ["session_id"] },
    { table: "part", columns: ["message_id"] },
    { table: "todo", columns: ["session_id"] },
    { table: "session_message", columns: ["session_id"] },
    { table: "session_input", columns: ["session_id"] },
    { table: "session_context_epoch", columns: ["session_id"] },
    { table: "session_prompt_queue", columns: ["session_id"] },
    { table: "session_prompt_queue_sequence", columns: ["session_id"] },
    { table: "session_prompt_queue_alias", columns: ["item_id"] },
    { table: "session_prompt_provenance", columns: ["session_id"] },
    { table: "session_prompt_receipt", columns: ["session_id"] },
    { table: "session_ask", columns: ["session_id"] },
    { table: "session_share", columns: ["session_id"] },
    { table: "event_sequence", columns: ["aggregate_id"] },
    { table: "event", columns: ["aggregate_id", "seq"] },
    { table: "event_retention", columns: ["aggregate_id"] },
    { table: "event_aggregate_writer", columns: ["aggregate_id"] },
  ]
  const failures = new Array<string>()
  for (const { table, columns } of required) {
    if (!tables.has(table)) continue
    const indexes = db.query<{ name: string }, []>(`PRAGMA index_list(${quote(table)})`).all()
    const indexed = indexes.some((index) => {
      const indexColumns = db
        .query<{ seqno: number; name: string | null }, []>(`PRAGMA index_info(${quote(index.name)})`)
        .all()
        .toSorted((left, right) => left.seqno - right.seqno)
      return columns.every((column, position) => indexColumns[position]?.name === column)
    })
    if (!indexed) failures.push(`online-scope-index-unavailable:${table}.${columns.join(".")}`)
  }
  return failures
}

function onlineDescendants(db: SqliteAccess, sessionID: string, budget: OnlineValidationBudget) {
  const rows = db
    .query<{ id: string; parent_id: string | null }, [string, number]>(
      `WITH RECURSIVE tree(id, parent_id) AS (
        SELECT id, parent_id FROM session WHERE id = ?
        UNION
        SELECT child.id, child.parent_id FROM session child JOIN tree parent ON child.parent_id = parent.id
      )
      SELECT id, parent_id FROM tree LIMIT ?`,
    )
    .all(sessionID, budget.remainingRows + 1)
  if (!onlineConsumeValidationRows(rows.length, budget)) return { sessionIDs: [], boundReached: true }
  return {
    sessionIDs: rows.map((row) => row.id),
    boundReached: false,
  }
}

function onlineParentCycle(db: SqliteAccess, sessionID: string, budget: OnlineValidationBudget) {
  const limit = budget.remainingRows + 1
  const rows = db
    .query<{ id: string; parent_id: string | null; depth: number }, [string, number, number]>(
      `WITH RECURSIVE ancestors(id, parent_id, depth) AS (
        SELECT id, parent_id, 1 FROM session WHERE id = ?
        UNION ALL
        SELECT parent.id, parent.parent_id, ancestors.depth + 1
        FROM session parent JOIN ancestors ON parent.id = ancestors.parent_id
        WHERE ancestors.depth < ?
      )
      SELECT id, parent_id, depth FROM ancestors LIMIT ?`,
    )
    .all(sessionID, limit, limit)
  const seen = new Set<string>()
  const cycle = rows.some((row) => {
    if (seen.has(row.id)) return true
    seen.add(row.id)
    return false
  })
  if (!onlineConsumeValidationRows(rows.length, budget)) return { cycle, boundReached: true }
  return {
    cycle,
    boundReached: false,
  }
}

function onlineCrossSessionCascadeFailures(
  db: SqliteAccess,
  sessionIDs: readonly string[],
  selectedSessionIDs: ReadonlySet<string>,
  validationBudgets: ReadonlyMap<string, OnlineValidationBudget>,
) {
  const failures = new Map<string, string>()
  for (const sessionID of sessionIDs) {
    const budget = validationBudgets.get(sessionID) ?? onlineValidationBudget()
    const outgoing = db
      .query<{ parentSessionID: string; childSessionID: string; messageID: string }, [string, string, number]>(
        `SELECT message.session_id AS parentSessionID, part.session_id AS childSessionID, message.id AS messageID
         FROM message JOIN part ON part.message_id = message.id
         WHERE message.session_id = ? AND part.session_id <> ?
         ORDER BY message.id LIMIT ?`,
      )
      .all(sessionID, sessionID, budget.remainingRows + 1)
    if (!onlineConsumeValidationRows(outgoing.length, budget)) {
      failures.set(sessionID, "session-validation-bound-reached")
      continue
    }
    for (const row of outgoing) {
      if (selectedSessionIDs.has(row.childSessionID)) continue
      failures.set(
        sessionID,
        `cross-session-cascade:part.message_id:${row.messageID}:${row.parentSessionID}->${row.childSessionID}`,
      )
      break
    }
    if (failures.has(sessionID)) continue
    const incoming = db
      .query<{ parentSessionID: string; childSessionID: string; messageID: string }, [string, string, number]>(
        `SELECT message.session_id AS parentSessionID, part.session_id AS childSessionID, message.id AS messageID
         FROM part JOIN message ON message.id = part.message_id
         WHERE part.session_id = ? AND message.session_id <> ?
         ORDER BY message.id LIMIT ?`,
      )
      .all(sessionID, sessionID, budget.remainingRows + 1)
    if (!onlineConsumeValidationRows(incoming.length, budget)) {
      failures.set(sessionID, "session-validation-bound-reached")
      continue
    }
    for (const row of incoming) {
      if (selectedSessionIDs.has(row.parentSessionID)) continue
      failures.set(
        sessionID,
        `cross-session-cascade:part.message_id:${row.messageID}:${row.parentSessionID}->${row.childSessionID}`,
      )
      break
    }
  }
  return [...failures].map(([sessionID, reason]) => ({ sessionID, reason }))
}

async function onlineAvailableBytes(filename: string) {
  try {
    const filesystem = await statfs(path.dirname(filename))
    return filesystem.bavail * filesystem.bsize
  } catch {
    return undefined
  }
}

function onlinePlannedPeakBytes(
  options: OnlineOptions,
  pageSize: number | null,
  _pageCount: number | null,
  _filename: string,
) {
  if (pageSize === null) return 0
  const deletePeak = options.batchBytes * 2 + options.batchRows * pageSize * 32
  const vacuumPeak = options.vacuumPages * pageSize * 2
  return Math.max(deletePeak, vacuumPeak)
}

function onlineBoundsReport(options?: OnlineOptions) {
  return {
    sessionIDs: onlineBounds.sessions,
    validationRows: onlineBounds.validationRows,
    batchRows: options?.batchRows ?? onlineBounds.batchRows,
    batchBytes: options?.batchBytes ?? onlineBounds.batchBytes,
    maxBatchBytes: onlineBounds.maxBatchBytes,
    maxBatches: options?.maxBatches ?? onlineBounds.maxBatches,
    vacuumPages: options?.vacuumPages ?? onlineBounds.vacuumPages,
    maxVacuumPages: options?.maxVacuumPages ?? onlineBounds.maxVacuumPages,
    busyTimeoutMs: onlineBounds.busyTimeout,
    busyAttempts: onlineBounds.busyAttempts,
  }
}

function onlinePageState(db: SqliteAccess) {
  const pageSize = db.query<{ page_size: number }, []>("PRAGMA page_size").get()?.page_size
  const pageCount = db.query<{ page_count: number }, []>("PRAGMA page_count").get()?.page_count
  const freelist = db.query<{ freelist_count: number }, []>("PRAGMA freelist_count").get()?.freelist_count
  if (pageSize === undefined || pageCount === undefined || freelist === undefined) return undefined
  return { pageSize, pageCount, freelist }
}

function onlinePassiveCheckpoint(db: SqliteAccess) {
  try {
    const row = db
      .query<{ busy: number; log: number; checkpointed: number }, []>("PRAGMA wal_checkpoint(PASSIVE)")
      .get()
    if (!row) return { error: "wal-checkpoint-returned-no-result" }
    return { busy: row.busy, logFrames: row.log, checkpointedFrames: row.checkpointed }
  } catch (error) {
    return { error: error instanceof Error ? error.message : "wal-checkpoint-failed" }
  }
}

async function onlineReclaimStep(db: SqliteAccess, filename: string, options: OnlineOptions, pagesRequested: number) {
  const before = onlinePageState(db)
  const allocationBefore = await allocatedBytes(filename)
  if (!before)
    return {
      reason: "database-page-measurement-unavailable",
      pagesRequested: 0,
      steps: [],
      complete: false,
      committed: false,
    }
  if (db.inTransaction)
    return {
      reason: "reclamation-inside-delete-transaction",
      pagesRequested: 0,
      steps: [],
      complete: false,
      committed: false,
    }

  const steps = new Array<Record<string, unknown>>()
  let pages = 0
  let vacuumExecuted = false
  let committed = false
  let reason: string | undefined
  if (before.freelist > 0 && pagesRequested < options.maxVacuumPages) {
    pages = Math.min(options.vacuumPages, before.freelist, options.maxVacuumPages - pagesRequested)
    const available = await onlineAvailableBytes(filename)
    const required = options.reserveBytes! + pages * before.pageSize * 2
    if (available === undefined) reason = "free-space-measurement-unavailable"
    else if (available < required) reason = "capacity-reserve-reached"
    if (!reason) {
      try {
        const holderReasons = onlineHolderFailures(db, scanDatabaseHolders(filename))
        if (holderReasons.length > 0) reason = holderReasons[0]
      } catch (error) {
        reason = error instanceof Error ? error.message : "database-liveness-unreadable"
      }
    }
    if (!reason) {
      try {
        db.exec(`PRAGMA incremental_vacuum(${pages})`)
        vacuumExecuted = true
        committed = true
      } catch (error) {
        reason = error instanceof Error ? error.message : "incremental-vacuum-failed"
      }
    }
  }

  const afterVacuum = (() => {
    try {
      return onlinePageState(db)
    } catch (error) {
      reason ??= error instanceof Error ? error.message : "database-page-measurement-after-vacuum-failed"
      return undefined
    }
  })()
  if (!afterVacuum) reason ??= "database-page-measurement-after-vacuum-unavailable"
  const currentAvailable = await onlineAvailableBytes(filename)
  const databaseFile = await stat(filename).catch(() => undefined)
  const checkpointPeak =
    afterVacuum && databaseFile
      ? Math.max(0, afterVacuum.pageCount * afterVacuum.pageSize - databaseFile.size) + afterVacuum.pageSize
      : undefined
  if (currentAvailable === undefined) reason ??= "free-space-measurement-unavailable"
  else if (checkpointPeak === undefined) reason ??= "checkpoint-peak-unavailable"
  else if (currentAvailable < options.reserveBytes! + checkpointPeak) reason ??= "capacity-reserve-reached"
  if (!reason) {
    try {
      const holderReasons = onlineHolderFailures(db, scanDatabaseHolders(filename))
      if (holderReasons.length > 0) reason = holderReasons[0]
    } catch (error) {
      reason = error instanceof Error ? error.message : "database-liveness-unreadable"
    }
  }

  const checkpoint = reason ? { error: reason } : onlinePassiveCheckpoint(db)
  if ("error" in checkpoint) reason ??= checkpoint.error
  else {
    if (checkpoint.checkpointedFrames > 0) committed = true
    if (checkpoint.busy !== 0 || checkpoint.checkpointedFrames < checkpoint.logFrames)
      reason ??= "wal-checkpoint-incomplete"
  }

  const after = (() => {
    try {
      return onlinePageState(db)
    } catch (error) {
      reason ??= error instanceof Error ? error.message : "database-page-measurement-after-checkpoint-failed"
      return undefined
    }
  })()
  const allocationAfter = await allocatedBytes(filename)
  const netAllocatedBytes =
    allocationBefore.status === "measured" && allocationAfter.status === "measured"
      ? allocationBefore.totalBytes - allocationAfter.totalBytes
      : null
  steps.push({
    vacuumPages: vacuumExecuted ? pages : 0,
    vacuumPagesRequested: pages,
    freelistBefore: before.freelist,
    freelistAfter: after?.freelist ?? null,
    pageCountBefore: before.pageCount,
    pageCountAfter: after?.pageCount ?? null,
    pageSizeBytes: before.pageSize,
    checkpoint,
    allocatedBytesBefore: allocationBefore.status === "measured" ? allocationBefore.totalBytes : null,
    allocatedBytesAfter: allocationAfter.status === "measured" ? allocationAfter.totalBytes : null,
    netAllocatedBytes,
    allocatedFilesBefore: allocationBefore.files,
    allocatedFilesAfter: allocationAfter.files,
  })
  const checkpointComplete =
    !("error" in checkpoint) && checkpoint.busy === 0 && checkpoint.checkpointedFrames === checkpoint.logFrames
  const complete = after?.freelist === 0 && checkpointComplete
  if (after && after.freelist > 0 && pagesRequested + pages >= options.maxVacuumPages)
    reason ??= "vacuum-page-bound-reached"
  return { complete, committed, pagesRequested: vacuumExecuted ? pages : 0, reason, steps }
}

async function onlineReclaimUntilDone(
  db: SqliteAccess,
  filename: string,
  options: OnlineOptions,
  initialPagesRequested = 0,
) {
  const steps = new Array<Record<string, unknown>>()
  let pagesRequested = initialPagesRequested
  let committed = false
  while (true) {
    let step
    try {
      step = await onlineReclaimStep(db, filename, options, pagesRequested)
    } catch (error) {
      return {
        complete: false,
        committed,
        pagesRequested,
        steps,
        reason: error instanceof Error ? error.message : "incremental-reclamation-failed",
      }
    }
    steps.push(...step.steps)
    committed ||= step.committed
    pagesRequested += step.pagesRequested
    if (step.complete) return { complete: true, committed, pagesRequested, steps }
    if (step.reason) return { complete: false, committed, pagesRequested, steps, reason: step.reason }
    if (step.pagesRequested === 0)
      return { complete: false, committed, pagesRequested, steps, reason: "reclamation-made-no-progress" }
  }
}

async function onlineReclaimOnly(
  db: SqliteAccess,
  filename: string,
  options: OnlineOptions,
  usage: ReturnType<typeof measurementReport>,
): Promise<OnlinePruneReport> {
  const schema = inspectSchema(db)
  const reasons = [
    ...schema.failures,
    ...schema.unclassifiedTables.map((table) => `unclassified-session-storage:${table}`),
    ...onlineScopeIndexFailures(db, schema.tables),
  ]
  const autoVacuum = db.query<{ auto_vacuum: number }, []>("PRAGMA auto_vacuum").get()?.auto_vacuum
  if (autoVacuum !== 2) reasons.push(`incremental-vacuum-unavailable:${autoVacuum ?? "unreadable"}`)
  const holderReasons = onlineHolderFailures(db, scanDatabaseHolders(filename))
  reasons.push(...holderReasons)
  const state = onlinePageState(db)
  if (!state) reasons.push("database-page-measurement-unavailable")
  const allocated = await allocatedBytes(filename)
  if (allocated.status !== "measured") reasons.push(`allocated-byte-measurement-${allocated.status}`)
  const available = await onlineAvailableBytes(filename)
  if (available === undefined) reasons.push("free-space-measurement-unavailable")
  const plannedPeakBytes = onlinePlannedPeakBytes(options, state?.pageSize ?? null, state?.pageCount ?? null, filename)
  if (available !== undefined && available < options.reserveBytes! + plannedPeakBytes)
    reasons.push("free-space-reserve-would-be-violated")

  const base = onlineRefusedReport(filename, undefined, undefined, usage, reasons)
  base.reclamation = {
    ...base.reclamation,
    ready: reasons.length === 0,
    autoVacuum: autoVacuum === 2 ? "incremental" : autoVacuum === 1 ? "full" : autoVacuum === 0 ? "none" : "unreadable",
    pageSizeBytes: state?.pageSize ?? null,
    pageCountBefore: state?.pageCount ?? null,
    pageCountAfter: state?.pageCount ?? null,
    freelistBefore: state?.freelist ?? null,
    freelistAfter: state?.freelist ?? null,
    reserveBytes: options.reserveBytes,
    availableBytes: available ?? null,
    plannedPeakBytes,
    bounds: onlineBoundsReport(options),
    allocatedBytesBefore: allocated,
  }
  base.rawTranscriptWarning = "This reclaim-only invocation does not delete Session history."
  if (reasons.length > 0) return base

  const reclaimed = await onlineReclaimUntilDone(db, filename, options)
  const measurement = (() => {
    try {
      return { state: onlinePageState(db), reason: undefined }
    } catch (error) {
      return {
        state: undefined,
        reason: error instanceof Error ? error.message : "database-page-measurement-after-reclaim-failed",
      }
    }
  })()
  const allocationAfter = await allocatedBytes(filename)
  const netAllocatedBytes =
    allocated.status === "measured" && allocationAfter.status === "measured"
      ? allocated.totalBytes - allocationAfter.totalBytes
      : null
  const finalReason = reclaimed.reason ?? measurement.reason
  const lastCheckpoint = [...reclaimed.steps].toReversed().find((step) => "checkpoint" in step)?.checkpoint as
    | { logFrames?: number; checkpointedFrames?: number }
    | undefined
  base.action = reclaimed.complete && finalReason === undefined ? "applied" : reclaimed.committed ? "failed" : "refused"
  base.committed = reclaimed.committed
  base.partial = reclaimed.committed && !reclaimed.complete
  base.refusalReasons = finalReason ? [finalReason] : []
  base.reclamation = {
    ...base.reclamation,
    ready: reclaimed.complete && finalReason === undefined,
    reasons: finalReason ? [finalReason] : [],
    pageCountAfter: measurement.state?.pageCount ?? null,
    freelistAfter: measurement.state?.freelist ?? null,
    allocatedBytesAfter: allocationAfter,
    netAllocatedBytes,
    remainingFreelistPages: measurement.state?.freelist ?? null,
    remainingWalFrames:
      lastCheckpoint?.logFrames === undefined || lastCheckpoint?.checkpointedFrames === undefined
        ? null
        : lastCheckpoint.logFrames - lastCheckpoint.checkpointedFrames,
    steps: reclaimed.steps,
  }
  base.batches = reclaimed.steps.map((step, index) => ({ batch: index + 1, kind: "reclaim", ...step }))
  return base
}

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
  const targetDirectory = directory === undefined ? undefined : await directoryIdentity(directory)
  const targetState = directory === undefined ? undefined : await inspectDirectory(directory)
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
  if (directory !== undefined && targetDirectory === undefined) globalReasons.push("directory-identity-unreadable")
  if (targetState !== undefined && targetState !== "missing") globalReasons.push(`retirement-target-${targetState}`)
  if (integrity.state !== "ok") globalReasons.push(`integrity-check-${integrity.state}`)
  if (foreignKeys.state !== "ok") globalReasons.push(`foreign-key-check-${foreignKeys.state}`)
  const candidates = new Map<string, SessionReport>()
  const excluded = new Map<string, SessionReport>()

  for (const row of sessions) {
    const reasons = [...globalReasons, ...holderReasons]
    const identity = await directoryIdentity(row.directory)
    const directoryState = identity === undefined ? "unknown" : await inspectDirectory(identity)
    if (directoryState !== "missing") reasons.push(`session-directory-${directoryState}`)
    if (directory !== undefined && !retirementScope(row.directory, identity, directory, targetDirectory)) {
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
      directoryIdentity: identity ?? null,
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
      directoryIdentity: null,
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
  if (directory !== undefined && targetDirectory === undefined) readinessReasons.push("directory-identity-unreadable")
  if (integrity.state !== "ok") readinessReasons.push(`integrity-check-${integrity.state}`)
  if (foreignKeys.state !== "ok") readinessReasons.push(`foreign-key-check-${foreignKeys.state}`)
  if (allocated.status !== "measured") readinessReasons.push(`allocated-byte-measurement-${allocated.status}`)
  const retainedSessions = [...excluded.values()].filter(
    (item) =>
      (!requested || requested.includes(item.sessionID)) &&
      (directory !== undefined
        ? retirementScope(item.directory, item.directoryIdentity ?? undefined, directory, targetDirectory)
        : requested !== undefined || item.directoryState === "missing") &&
      !item.reasons.includes("activity-at-or-after-cutoff"),
  )
  return {
    database: filename,
    cutoff: cutoff === undefined ? null : new Date(cutoff).toISOString(),
    explicitDirectory: directory ?? null,
    directoryIdentity: targetDirectory ?? null,
    retirementTarget:
      directory === undefined ? null : { directory, identity: targetDirectory ?? null, state: targetState },
    explicitSessionIDs: requested ?? null,
    selectedSessions: [...candidates.values()].toSorted((left, right) => left.sessionID.localeCompare(right.sessionID)),
    excludedSessions: [...excluded.values()].toSorted((left, right) => left.sessionID.localeCompare(right.sessionID)),
    retainedSessions,
    retainedCounts: {
      sessions: retainedSessions.length,
      ownedRows: retainedSessions.reduce(
        (sum, item) => sum + Object.values(item.ownedRows).reduce((sum, n) => sum + n, 0),
        0,
      ),
    },
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
  if (directory !== undefined) {
    const state = await inspectDirectory(directory)
    if (state === "exists") return retirementNoop(inventory, "target-exists")
    if (state !== "missing")
      return { ...inventory, action: "refused", refusalReasons: [`retirement-target-${state}`], changedRows: 0 }
  }
  const refusalReasons = [...inventory.reclamation.reasons]
  if (inventory.selectedSessions.length === 0 && directory === undefined) refusalReasons.push("no-eligible-sessions")
  if (refusalReasons.length > 0) return { ...inventory, action: "refused", refusalReasons, changedRows: 0 }
  if (inventory.selectedSessions.length === 0) {
    const targetDirectory = await directoryIdentity(directory!)
    const state = targetDirectory === undefined ? "unknown" : await inspectDirectory(targetDirectory)
    if (
      state !== "missing" ||
      inventory.excludedSessions.some((item) =>
        retirementScope(item.directory, item.directoryIdentity ?? undefined, directory!, targetDirectory),
      )
    ) {
      return {
        ...inventory,
        action: "refused",
        refusalReasons: [`directory-retirement-not-eligible:${state}`],
        changedRows: 0,
      }
    }
    return retirementNoop(inventory, "no-matching-sessions")
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
    await requireMissingTarget(directory)
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
    await requireMissingTarget(directory)
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
    if (error instanceof Error && error.message === "retirement-target-exists")
      return retirementNoop(inventory, "target-exists")
    return {
      ...inventory,
      action: "refused",
      refusalReasons: [error instanceof Error ? error.message : "apply-failed"],
      changedRows: 0,
    }
  }

  const checkpoint = checkpointDatabase(db)
  const vacuum = (() => {
    if (changedRows === 0) return { state: "skipped", reason: "no-deletions" }
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
  if (vacuum.state !== "complete" && vacuum.reason !== "no-deletions")
    reclamationReasons.push(`vacuum-${vacuum.state}:${vacuum.reason}`)
  if (vacuumCheckpoint && "error" in vacuumCheckpoint)
    reclamationReasons.push(`vacuum-checkpoint-failed:${vacuumCheckpoint.error}`)
  else if (vacuumCheckpoint && vacuumCheckpoint.busy !== 0)
    reclamationReasons.push("vacuum-checkpoint-did-not-complete")
  if (afterAllocated.status !== "measured")
    reclamationReasons.push("allocated-byte-measurement-after-apply-unavailable")
  if (postIntegrity.state !== "ok") reclamationReasons.push(`integrity-check-after-checkpoint-${postIntegrity.state}`)
  if (postForeignKeys.state !== "ok")
    reclamationReasons.push(`foreign-key-check-after-checkpoint-${postForeignKeys.state}`)
  const action = reclamationReasons.length === 0 ? (changedRows > 0 ? "applied" : "noop") : "failed"
  return {
    ...inventory,
    action,
    committed: true,
    partial: inventory.retainedSessions.length > 0,
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

async function requireMissingTarget(directory: string | undefined) {
  if (directory === undefined) return
  const state = await inspectDirectory(directory)
  if (state !== "missing") throw new Error(`retirement-target-${state}`)
}

function retirementNoop(inventory: Awaited<ReturnType<typeof inventorySnapshot>>, reason: string) {
  const retainedSessions =
    reason === "target-exists"
      ? [...inventory.selectedSessions, ...inventory.retainedSessions].map((item) => ({
          ...item,
          reasons: [...new Set([...item.reasons, "retirement-target-exists"])],
        }))
      : inventory.retainedSessions
  return {
    ...inventory,
    action: "noop",
    noopReason: reason,
    retirementTarget:
      reason === "target-exists" && inventory.retirementTarget
        ? { ...inventory.retirementTarget, state: "exists" }
        : inventory.retirementTarget,
    committed: false,
    selectedSessions: [],
    selectedCounts: { sessions: 0, ownedRows: 0 },
    deletionScope: { ...inventory.deletionScope, sessionIDs: [] },
    retainedSessions,
    retainedCounts: {
      sessions: retainedSessions.length,
      ownedRows: retainedSessions.reduce(
        (sum, item) => sum + Object.values(item.ownedRows).reduce((sum, n) => sum + n, 0),
        0,
      ),
    },
    changedRows: 0,
    deletedSessionIDs: [],
    deletedCounts: { sessions: 0, ownedRows: 0, byTable: {} },
    reclamation: { ...inventory.reclamation, vacuum: { state: "skipped", reason: "no-deletions" } },
  }
}

function retirementScope(
  recorded: string | null,
  identity: string | undefined,
  target: string,
  targetIdentity: string | undefined,
) {
  if (identity !== undefined && targetIdentity !== undefined) return withinDirectory(identity, targetIdentity)
  if (recorded === null || !validDirectory(recorded)) return false
  return (
    withinDirectory(path.resolve(recorded), path.resolve(target)) ||
    withinDirectory(path.resolve(recorded), targetIdentity)
  )
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
    typeof directory === "string" && directory.length > 0 && !directory.includes("\0") && path.isAbsolute(directory)
  )
}

async function directoryIdentity(directory: string) {
  if (!validDirectory(directory)) return undefined
  const root = path.parse(directory).root
  const segments = directory
    .slice(root.length)
    .split(path.sep)
    .filter((segment) => segment && segment !== ".")
  let current = path.resolve(root)
  for (const [index, segment] of segments.entries()) {
    if (segment === "..") {
      current = path.dirname(current)
      continue
    }
    const candidate = path.join(current, segment)
    try {
      const entry = await lstat(candidate)
      // Resolve each existing link before parent segments can hide a dangling one.
      current = entry.isSymbolicLink() ? await realpath(candidate) : candidate
      if (index < segments.length - 1 && !(entry.isSymbolicLink() ? await stat(current) : entry).isDirectory()) {
        return undefined
      }
    } catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) return undefined
      // ENOENT from lstat is absence; ENOENT resolving a present symlink is uncertainty.
      try {
        await lstat(candidate)
        return undefined
      } catch (error) {
        if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) return undefined
      }
      current = candidate
    }
  }
  return current
}

function withinDirectory(candidate: string | undefined, directory: string | undefined) {
  if (candidate === undefined || directory === undefined) return false
  return (
    candidate === directory || candidate.startsWith(directory.endsWith(path.sep) ? directory : directory + path.sep)
  )
}

async function inspectDirectory(directory: string) {
  const identity = await directoryIdentity(directory)
  if (identity === undefined) return "unknown"
  const root = path.parse(identity).root
  let current = root
  for (const segment of identity.slice(root.length).split(path.sep).filter(Boolean)) {
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
