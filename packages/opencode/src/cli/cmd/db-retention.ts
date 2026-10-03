import { createHash, randomUUID } from "node:crypto"
import { spawnSync } from "node:child_process"
import { copyFile, lstat, open, realpath, rename, stat, statfs, unlink } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { Database } from "@opencode-ai/core/database/database"
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
  close(): void
}

export type RetentionEvidence = {
  readonly policy?: {
    readonly reviewed: boolean
    readonly cutoffEpochMs: number
    readonly reviewedReference: string
    readonly policyDigest: string
    readonly readerContractReviewed: boolean
    readonly readerContractID: string
  }
  readonly liveness?: {
    readonly proofID: string
    readonly observedAtEpochMs: number
    readonly sessionIDs: readonly string[]
    readonly aggregateOwners: Readonly<Record<string, string | null>>
    readonly servingProcesses: readonly string[]
    readonly canResume: boolean
    readonly unfinishedOwnedWork: boolean
    readonly validThroughEpochMs: number
  }
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

export type RetentionApplyResult = {
  readonly state: "complete" | "in-progress" | "refused"
  readonly changedRows: number
  readonly changedBytes: number
  readonly completedSessionIDs: readonly string[]
  readonly reasons: readonly string[]
}

export type PhysicalMetrics = {
  readonly size: number
  readonly blocks: number
  readonly allocatedBytes: number
  readonly pageCount: number
  readonly freelistCount: number
  readonly autoVacuum: number
  readonly integrityCheck: string
  readonly retainedSession: Record<string, unknown>
}

export type CompactFixtureResult = {
  readonly state: "complete" | "refused" | "failed"
  readonly changedFiles: number
  readonly reasons: readonly string[]
  readonly sourceDeviceID?: string
  readonly backupDeviceID?: string
  readonly stagingDeviceID?: string
  readonly measurements?: {
    readonly before: PhysicalMetrics
    readonly backup?: PhysicalMetrics
    readonly staging?: PhysicalMetrics
    readonly restored?: PhysicalMetrics
  }
}

type CompactFixtureInput = {
  readonly sourcePath: string
  readonly backupPath: string
  readonly stagingPath: string
  readonly expectedSourceDeviceID: string
  readonly expectedBackupDeviceID: string
  readonly expectedStagingDeviceID: string
  readonly retainedSessionID: string
  readonly capacityFloorBytes?: number
}

type ApplyInput = {
  readonly tree: RetentionTree
  readonly evidence: () => RetentionEvidence
  readonly now?: () => number
  readonly batchSize?: number
  readonly maxBatches?: number
}

type RedactionRow = { cursor: string | number; value: string | number | null }

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
  "cacheRead",
  "cacheWrite",
  "cost",
  "createdAt",
  "duration",
  "durationMs",
  "finish",
  "id",
  "input",
  "messageID",
  "modelID",
  "output",
  "partID",
  "providerID",
  "read",
  "reasoning",
  "responseModelID",
  "responseModelIDs",
  "role",
  "routeID",
  "routeName",
  "seq",
  "sessionID",
  "startedAt",
  "status",
  "time",
  "time_created",
  "time_updated",
  "timestamp",
  "type",
  "updatedAt",
  "variant",
  "wallTime",
  "wallTimeMs",
  "write",
])

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
  "route",
  "tokens",
  "timing",
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
  "modelID",
  "responseModelID",
  "responseModelIDs",
  "route",
  "routeID",
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

const jsonCopies = [
  { table: "message", scope: "session_id", value: "data" },
  { table: "part", scope: "session_id", value: "data" },
  { table: "session_message", scope: "session_id", value: "data" },
  { table: "session_input", scope: "session_id", value: "prompt" },
  { table: "session_context_epoch", scope: "session_id", value: "snapshot" },
  { table: "session_prompt_queue", scope: "session_id", value: "input" },
  { table: "event", scope: "aggregate_id", value: "data" },
] as const

function makeProgressScope(
  rootSessionID: string,
  sessionIDs: readonly string[],
  aggregates: readonly { aggregateID: string; eventTypes: readonly string[] }[],
  evidence: RetentionEvidence,
) {
  const scopeEvidence = {
    cutoffEpochMs: evidence.policy?.cutoffEpochMs ?? null,
    reviewedReference: evidence.policy?.reviewedReference ?? null,
    policyDigest: evidence.policy?.policyDigest ?? null,
    readerContractID: evidence.policy?.readerContractID ?? null,
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

export function inventory(db: SqliteAccess, evidence: RetentionEvidence = {}, now = Date.now()): RetentionInventory {
  if (db.inTransaction) return inventorySnapshot(db, evidence, now)
  return db.transaction(() => inventorySnapshot(db, evidence, now)).deferred()
}

function inventorySnapshot(db: SqliteAccess, evidence: RetentionEvidence, now: number): RetentionInventory {
  const tables = new Set(
    allRows<TableRow>(db, "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").map(
      (row) => row.name,
    ),
  )
  const requiredTables = ["session", "event_sequence", "event"]
  const requiredRefusals = requiredTables
    .filter((table) => !tables.has(table))
    .map((table) => `${table.replaceAll("_", "-")}-table-unreadable`)
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
  const classifiedSessionTables = new Set([
    "message",
    "part",
    "session_message",
    "session_input",
    "session_context_epoch",
    "session_prompt_queue",
    "session_prompt_queue_sequence",
    "todo",
    "session_share",
  ])
  const unclassifiedSessionTables = Array.from(tables).flatMap((table) => {
    if (classifiedSessionTables.has(table) || table === "session") return []
    const columns = allRows<ColumnRow>(db, `PRAGMA table_info(${quote(table)})`)
    return columns.some((column) => column.name === "session_id") ? [table] : []
  })
  const globalRefusals = new Set<string>()
  if (!tables.has("session")) globalRefusals.add("session-table-unreadable")
  if (!tables.has("event_sequence")) globalRefusals.add("event-sequence-table-unreadable")
  if (!tables.has("event")) globalRefusals.add("event-table-unreadable")
  for (const table of unclassifiedSessionTables) globalRefusals.add(`unclassified-session-owned-table:${table}`)

  const trees = components.map((component) => {
    const sessionIDs = component.sessionIDs
    const reasons = new Set<string>()
    if (evidence.evidenceError) reasons.add(`evidence-unreadable:${evidence.evidenceError}`)
    if (
      !evidence.policy?.reviewed ||
      !Number.isFinite(evidence.policy.cutoffEpochMs) ||
      !evidence.policy.reviewedReference ||
      !evidence.policy.policyDigest
    ) {
      reasons.add("reviewed-age-boundary-unavailable")
    } else if (!evidence.policy.readerContractReviewed || !evidence.policy.readerContractID) {
      reasons.add("reader-contract-unreviewed")
    }
    if (!evidence.liveness) reasons.add("cross-process-liveness-proof-unavailable")
    if (!evidence.handoff) reasons.add("measurement-handoff-receipt-unavailable")
    if (component.unresolvedParent) reasons.add("session-tree-parent-unreadable")
    if (
      sessionIDs.some((sessionID) => {
        const parentID = sessionsByID.get(sessionID)?.parent_id
        return parentID !== null && (!parentID || !sessionIDs.includes(parentID))
      })
    )
      reasons.add("session-tree-parent-unreadable")
    if (sessionIDs.some((sessionID) => parentCycle(sessionID, sessionsByID))) reasons.add("session-tree-cycle")
    for (const refusal of globalRefusals) reasons.add(refusal)
    if (!tables.has("event_retention")) reasons.add("unreplayable-marker-store-unavailable")

    const liveness = evidence.liveness
    if (liveness) {
      if (!liveness.proofID) reasons.add("cross-process-liveness-proof-unreadable")
      if (!Number.isFinite(liveness.observedAtEpochMs) || liveness.observedAtEpochMs > now) {
        reasons.add("cross-process-liveness-proof-time-unreadable")
      }
      if (!Number.isFinite(liveness.validThroughEpochMs)) reasons.add("cross-process-liveness-proof-time-unreadable")
      if (liveness.validThroughEpochMs <= now) reasons.add("cross-process-liveness-proof-stale")
      if (liveness.canResume) reasons.add("session-can-resume")
      if (liveness.unfinishedOwnedWork) reasons.add("unfinished-owned-work")
      if (liveness.servingProcesses.length > 0) reasons.add("serving-process-owns-session")
      if (!sameIDs(liveness.sessionIDs, sessionIDs)) reasons.add("liveness-tree-scope-mismatch")
    }
    const handoff = evidence.handoff
    if (handoff) {
      if (!handoff.receiptID || handoff.durable !== true) reasons.add("measurement-handoff-receipt-unreadable")
      if (
        handoff.report.rawHistoryInaccessible !== true ||
        !handoff.report.resultDigest ||
        !validUtcWindow(handoff.report.windowStart, handoff.report.windowEnd)
      )
        reasons.add("report-not-reproduced-without-raw-history")
      for (const [axis, state] of Object.entries(handoff.axes)) {
        if (
          state.status === "unavailable" &&
          (!state.cause || handoff.report.unavailableCauses[axis] !== state.cause)
        ) {
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
      if (!sameIDs(handoff.sessionIDs, sessionIDs)) reasons.add("handoff-tree-scope-mismatch")
    }

    for (const sessionID of sessionIDs) {
      const session = sessionsByID.get(sessionID)
      if (!session) {
        reasons.add(`session-row-missing:${sessionID}`)
        continue
      }
      if (evidence.policy?.reviewed && session.time_created > evidence.policy.cutoffEpochMs) {
        reasons.add(`session-after-reviewed-cutoff:${sessionID}`)
      }
      const aggregate = oneRow<AggregateRow>(
        db,
        "SELECT aggregate_id, seq, owner_id FROM event_sequence WHERE aggregate_id = ?",
        sessionID,
      )
      if (liveness && liveness.aggregateOwners[sessionID] !== (aggregate?.owner_id ?? null)) {
        reasons.add(`cross-process-aggregate-owner-snapshot-mismatch:${sessionID}`)
      }
      const eventTypes = tables.has("event")
        ? allRows<EventTypeRow>(
            db,
            "SELECT type, count(*) AS rows FROM event WHERE aggregate_id = ? GROUP BY type ORDER BY type",
            sessionID,
          )
        : []
      const rowCount = eventTypes.reduce((total, row) => total + row.rows, 0)
      if (rowCount > 0 && !aggregate) reasons.add(`event-aggregate-sequence-missing:${sessionID}`)
      if (aggregate && eventTypes.length > 0) {
        const invalidJSON = oneRow<{ rows: number }>(
          db,
          "SELECT count(*) AS rows FROM event WHERE aggregate_id = ? AND json_valid(data) = 0",
          sessionID,
        )?.rows
        if (invalidJSON) reasons.add(`event-data-unreadable:${sessionID}`)
        for (const eventType of eventTypes) {
          const definition = Durable.get(eventType.type)
          if (!definition?.durable || definition.durable.aggregate !== "sessionID") {
            reasons.add(`event-aggregate-owner-unknown:${sessionID}:${eventType.type}`)
            continue
          }
          if (!invalidJSON) {
            const invalidOwner = oneRow<{ rows: number }>(
              db,
              "SELECT count(*) AS rows FROM event WHERE aggregate_id = ? AND json_extract(data, ?) IS NOT aggregate_id",
              sessionID,
              `$.${definition.durable.aggregate}`,
            )?.rows
            if (invalidOwner) reasons.add(`event-aggregate-owner-conflict:${sessionID}:${eventType.type}`)
          }
        }
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
      if (handoff && handoff.finalSequence[sessionID] !== (aggregate?.seq ?? -1)) {
        reasons.add(`measurement-handoff-does-not-cover-final-write:${sessionID}`)
      }
    }

    for (const { table, scope } of copyTables) {
      if (!tables.has(table)) {
        reasons.add(`source-table-unreadable:${table}`)
        continue
      }
      if (scope === "session_id") {
        const columns = allRows<ColumnRow>(db, `PRAGMA table_info(${quote(table)})`)
        if (!columns.some((column) => column.name === "session_id")) {
          reasons.add(`source-ownership-unreadable:${table}`)
          continue
        }
      }
    }

    if (tables.has("session_prompt_queue")) {
      const queueRows = oneRow<{ rows: number }>(
        db,
        `SELECT count(*) AS rows FROM session_prompt_queue WHERE session_id IN (${placeholders(sessionIDs.length)})`,
        ...sessionIDs,
      )?.rows
      if (queueRows) reasons.add("v1-prompt-queue-row-present")
    }
    if (tables.has("session_input")) {
      const pending = oneRow<{ rows: number }>(
        db,
        `SELECT count(*) AS rows FROM session_input WHERE session_id IN (${placeholders(sessionIDs.length)}) AND promoted_seq IS NULL`,
        ...sessionIDs,
      )?.rows
      if (pending) reasons.add("v2-input-pending")
    }

    const aggregates = sessionIDs.map((sessionID) => {
      const row = oneRow<AggregateRow>(
        db,
        "SELECT aggregate_id, seq, owner_id FROM event_sequence WHERE aggregate_id = ?",
        sessionID,
      )
      const rows = tables.has("event")
        ? oneRow<{ rows: number; bytes: number }>(
            db,
            "SELECT count(*) AS rows, coalesce(sum(length(CAST(data AS BLOB))), 0) AS bytes FROM event WHERE aggregate_id = ?",
            sessionID,
          )
        : undefined
      const eventTypes = tables.has("event")
        ? allRows<{ type: string }>(
            db,
            "SELECT DISTINCT type FROM event WHERE aggregate_id = ? ORDER BY type",
            sessionID,
          ).map((eventType) => eventType.type)
        : []
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
      if (!tables.has("event_retention")) continue
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

function revalidateCandidate(db: SqliteAccess, expected: RetentionTree, evidence: RetentionEvidence, now: number) {
  const reasons = new Set<string>()
  if (evidence.evidenceError) reasons.add(`evidence-unreadable:${evidence.evidenceError}`)
  if (
    !evidence.policy?.reviewed ||
    !Number.isFinite(evidence.policy.cutoffEpochMs) ||
    !evidence.policy.reviewedReference ||
    !evidence.policy.policyDigest
  ) {
    reasons.add("reviewed-age-boundary-unavailable")
  }
  if (!evidence.policy?.readerContractReviewed || !evidence.policy.readerContractID) {
    reasons.add("reader-contract-unreviewed")
  }
  if (!evidence.liveness) reasons.add("cross-process-liveness-proof-unavailable")
  if (!evidence.handoff) reasons.add("measurement-handoff-receipt-unavailable")
  if (expected.rootSessionID === "" || expected.sessionIDs.length === 0) reasons.add("session-tree-unreadable")

  const liveness = evidence.liveness
  if (liveness) {
    if (!liveness.proofID) reasons.add("cross-process-liveness-proof-unreadable")
    if (!Number.isFinite(liveness.observedAtEpochMs) || liveness.observedAtEpochMs > now) {
      reasons.add("cross-process-liveness-proof-time-unreadable")
    }
    if (!Number.isFinite(liveness.validThroughEpochMs)) reasons.add("cross-process-liveness-proof-time-unreadable")
    if (liveness.validThroughEpochMs <= now) reasons.add("cross-process-liveness-proof-stale")
    if (liveness.canResume) reasons.add("session-can-resume")
    if (liveness.unfinishedOwnedWork) reasons.add("unfinished-owned-work")
    if (liveness.servingProcesses.length > 0) reasons.add("serving-process-owns-session")
    if (!sameIDs(liveness.sessionIDs, expected.sessionIDs)) reasons.add("liveness-tree-scope-mismatch")
  }

  const handoff = evidence.handoff
  if (handoff) {
    if (!handoff.receiptID || !handoff.durable) reasons.add("measurement-handoff-receipt-unreadable")
    if (
      !handoff.report.rawHistoryInaccessible ||
      !handoff.report.resultDigest ||
      !validUtcWindow(handoff.report.windowStart, handoff.report.windowEnd)
    ) {
      reasons.add("report-not-reproduced-without-raw-history")
    }
    if (!sameIDs(handoff.sessionIDs, expected.sessionIDs)) reasons.add("handoff-tree-scope-mismatch")
    if (handoffAxes.some((axis) => !handoff.axes[axis])) reasons.add("measurement-handoff-axes-incomplete")
    for (const [axis, state] of Object.entries(handoff.axes)) {
      if (state.status === "unavailable" && (!state.cause || handoff.report.unavailableCauses[axis] !== state.cause)) {
        reasons.add(`measurement-axis-unavailable-cause-missing:${axis}`)
      }
      if (state.status === "retained" && axis in handoff.report.unavailableCauses) {
        reasons.add(`measurement-axis-retained-but-reported-unavailable:${axis}`)
      }
    }
    if (Object.values(handoff.report.denominators).some((value) => !Number.isFinite(value) || value < 0)) {
      reasons.add("report-denominators-unreadable")
    }
  }

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
    const session = rows.find((row) => row.id === sessionID)
    if (!session) {
      reasons.add(`session-row-missing:${sessionID}`)
      continue
    }
    if (evidence.policy?.reviewed && session.time_created > evidence.policy.cutoffEpochMs) {
      reasons.add(`session-after-reviewed-cutoff:${sessionID}`)
    }
    const aggregate = oneRow<AggregateRow>(
      db,
      "SELECT aggregate_id, seq, owner_id FROM event_sequence WHERE aggregate_id = ?",
      sessionID,
    )
    const eventTypes = allRows<{ type: string }>(
      db,
      "SELECT DISTINCT type FROM event WHERE aggregate_id = ? ORDER BY type",
      sessionID,
    ).map((row) => row.type)
    aggregates.push({ aggregateID: sessionID, eventTypes })
    if (liveness && liveness.aggregateOwners[sessionID] !== (aggregate?.owner_id ?? null)) {
      reasons.add(`cross-process-aggregate-owner-snapshot-mismatch:${sessionID}`)
    }
    const maxEvent = oneRow<{ seq: number | null }>(
      db,
      "SELECT max(seq) AS seq FROM event WHERE aggregate_id = ?",
      sessionID,
    )?.seq
    if ((aggregate?.seq ?? -1) !== (maxEvent ?? -1)) reasons.add(`event-sequence-changed:${sessionID}`)
    if (handoff && handoff.finalSequence[sessionID] !== (aggregate?.seq ?? -1)) {
      reasons.add(`measurement-handoff-does-not-cover-final-write:${sessionID}`)
    }
    if (
      eventTypes.some((type) => {
        const definition = Durable.get(type)
        return !definition?.durable || definition.durable.aggregate !== "sessionID"
      })
    ) {
      reasons.add(`event-aggregate-owner-unknown:${sessionID}`)
    }
    const queueRows = oneRow<{ rows: number }>(
      db,
      "SELECT count(*) AS rows FROM session_prompt_queue WHERE session_id = ?",
      sessionID,
    )?.rows
    if (queueRows) reasons.add("v1-prompt-queue-row-present")
    const pendingInput = oneRow<{ rows: number }>(
      db,
      "SELECT count(*) AS rows FROM session_input WHERE session_id = ? AND promoted_seq IS NULL",
      sessionID,
    )?.rows
    if (pendingInput) reasons.add("v2-input-pending")
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

  if (!isFixtureDatabase(db.filename)) {
    return {
      state: "refused",
      changedRows,
      changedBytes,
      completedSessionIDs: [],
      reasons: ["apply-only-supported-for-isolated-fixtures"],
    }
  }
  if (!input.tree.eligible) {
    return {
      state: "refused",
      changedRows,
      changedBytes,
      completedSessionIDs: [],
      reasons: input.tree.reasons,
    }
  }
  while (batches < maxBatches) {
    let batch: {
      readonly state: "progress" | "complete" | "refused"
      readonly sessionID?: string
      readonly changedRows: number
      readonly changedBytes: number
      readonly reason?: string
    }
    try {
      batch = db
        .transaction(() => {
          let evidence: RetentionEvidence
          try {
            evidence = input.evidence()
          } catch (error) {
            return {
              state: "refused" as const,
              changedRows: 0,
              changedBytes: 0,
              reason: `evidence-unreadable:${error instanceof Error ? error.message : "unknown"}`,
            }
          }
          const proofFailures = revalidateCandidate(db, input.tree, evidence, now())
          if (proofFailures.length > 0) {
            return {
              state: "refused" as const,
              changedRows: 0,
              changedBytes: 0,
              reason: proofFailures.join(",") || "candidate-proof-changed",
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
    } catch (error) {
      reasons.add(`apply-transaction-failed:${error instanceof Error ? error.message : "unknown"}`)
      return {
        state: batches > 0 ? "in-progress" : "refused",
        changedRows,
        changedBytes,
        completedSessionIDs: Array.from(completedSessionIDs).toSorted(),
        reasons: Array.from(reasons).toSorted(),
      }
    }
    if (batch.state === "refused") {
      reasons.add(batch.reason ?? "candidate-proof-changed")
      return {
        state: batches > 0 ? "in-progress" : "refused",
        changedRows,
        changedBytes,
        completedSessionIDs: Array.from(completedSessionIDs).toSorted(),
        reasons: Array.from(reasons).toSorted(),
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
    const snapshot = JSON.stringify(retainMetrics(JSON.parse(before.snapshot))) ?? "{}"
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
  const next = plan.kind === "text" ? "" : (JSON.stringify(retainMetrics(JSON.parse(String(row.value)))) ?? "{}")
  if (next === row.value) return { changedRows: 0, changedBytes: 0 }
  db.query(
    `UPDATE ${quote(plan.table)} SET ${quote(plan.value)} = ? WHERE ${quote(plan.scope)} = ? AND ${quote(plan.cursor)} = ?`,
  ).run(next, sessionID, row.cursor)
  return { changedRows: 1, changedBytes: byteLength(row.value) - byteLength(next) }
}

function retainMetrics(value: unknown, key?: string): unknown {
  if (key === "responseModelIDs" && Array.isArray(value)) return value.filter((item) => typeof item === "string")
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

function byteLength(value: unknown) {
  return Buffer.byteLength(typeof value === "string" ? value : value === null ? "" : String(value))
}

export async function compactRestoreFixture(input: CompactFixtureInput): Promise<CompactFixtureResult> {
  let changedFiles = 0
  const reasons = new Set<string>()
  let sourceDeviceID: string | undefined
  let backupDeviceID: string | undefined
  let stagingDeviceID: string | undefined
  let restoreTemp: string | undefined
  try {
    if (![input.expectedSourceDeviceID, input.expectedBackupDeviceID, input.expectedStagingDeviceID].every(Boolean)) {
      return { state: "refused", changedFiles, reasons: ["filesystem-identity-unverified"] }
    }
    const sourcePath = await realpath(input.sourcePath)
    if (!isFixtureDatabase(sourcePath)) {
      return { state: "refused", changedFiles, reasons: ["compaction-only-supported-for-isolated-fixtures"] }
    }
    const sourceDirectory = await realpath(path.dirname(sourcePath))
    const backupDirectory = await realpath(path.dirname(input.backupPath))
    const stagingDirectory = await realpath(path.dirname(input.stagingPath))
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
    const backupPath = path.join(backupDirectory, path.basename(input.backupPath))
    const stagingPath = path.join(stagingDirectory, path.basename(input.stagingPath))
    const sourceSize = sourceStat.size + (await fileSize(`${sourcePath}-wal`)) + (await fileSize(`${sourcePath}-shm`))
    const destinationUse = new Map<string, number>()
    destinationUse.set(backupDeviceID, sourceSize)
    destinationUse.set(stagingDeviceID, (destinationUse.get(stagingDeviceID) ?? 0) + sourceSize)
    const availableByDevice = new Map([
      [backupDeviceID, backupFilesystem.bavail * backupFilesystem.bsize],
      [stagingDeviceID, stagingFilesystem.bavail * stagingFilesystem.bsize],
    ])

    if (sourceDeviceID !== input.expectedSourceDeviceID) reasons.add("source-filesystem-identity-mismatch")
    if (backupDeviceID !== input.expectedBackupDeviceID) reasons.add("backup-filesystem-identity-mismatch")
    if (stagingDeviceID !== input.expectedStagingDeviceID) reasons.add("staging-filesystem-identity-mismatch")
    if (backupDeviceID === rootDeviceID) reasons.add("backup-destination-is-root")
    if (stagingDeviceID === rootDeviceID) reasons.add("staging-destination-is-root")
    if (backupDeviceID === sourceDeviceID) reasons.add("backup-filesystem-not-distinct")
    if (stagingDeviceID === sourceDeviceID) reasons.add("staging-filesystem-not-distinct")
    if (currentUserID !== undefined) {
      if (sourceDirectoryStat.uid !== currentUserID) reasons.add("source-fixture-owner-unverified")
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
      if (openSource.pids.length > 0) reasons.add("source-inode-still-open")
    }
    if (reasons.size > 0) {
      return {
        state: "refused",
        changedFiles,
        reasons: Array.from(reasons).toSorted(),
        sourceDeviceID,
        backupDeviceID,
        stagingDeviceID,
      }
    }

    const sqlite = await import("bun:sqlite")
    const checkpointDB = new sqlite.Database(sourcePath) as unknown as SqliteAccess
    const checkpoint = oneRow<{ busy: number; log: number; checkpointed: number }>(
      checkpointDB,
      "PRAGMA wal_checkpoint(TRUNCATE)",
    )
    checkpointDB.close()
    if (!checkpoint || checkpoint.busy !== 0 || checkpoint.log !== checkpoint.checkpointed) {
      reasons.add("fixture-checkpoint-incomplete")
      return {
        state: "refused",
        changedFiles,
        reasons: Array.from(reasons),
        sourceDeviceID,
        backupDeviceID,
        stagingDeviceID,
      }
    }
    const before = await physicalMetrics(sourcePath, input.retainedSessionID)
    const readinessDB = new sqlite.Database(sourcePath, { readonly: true }) as unknown as SqliteAccess
    const markers = allRows<{ state: string }>(readinessDB, "SELECT state FROM event_retention")
    readinessDB.close()
    if (markers.length === 0) reasons.add("redacted-session-marker-unavailable")
    if (markers.some((marker) => marker.state !== "complete")) reasons.add("redaction-progress-incomplete")
    if (before.integrityCheck !== "ok") reasons.add("source-integrity-check-failed")
    if (reasons.size > 0) {
      return {
        state: "refused",
        changedFiles,
        reasons: Array.from(reasons),
        sourceDeviceID,
        backupDeviceID,
        stagingDeviceID,
        measurements: { before },
      }
    }
    await copyFile(sourcePath, backupPath, 1)
    changedFiles += 1
    const backup = await physicalMetrics(backupPath, input.retainedSessionID)
    if (backup.integrityCheck !== "ok" || !sameRetainedMetrics(before.retainedSession, backup.retainedSession)) {
      reasons.add("backup-integrity-or-retained-read-failed")
      return {
        state: "failed",
        changedFiles,
        reasons: Array.from(reasons),
        sourceDeviceID,
        backupDeviceID,
        stagingDeviceID,
        measurements: { before, backup },
      }
    }

    const compactDB = new sqlite.Database(sourcePath) as unknown as SqliteAccess
    compactDB.query("PRAGMA auto_vacuum = INCREMENTAL").run()
    compactDB.query(`VACUUM INTO ${sqlLiteral(stagingPath)}`).run()
    compactDB.close()
    changedFiles += 1
    const staging = await physicalMetrics(stagingPath, input.retainedSessionID)
    if (staging.integrityCheck !== "ok") reasons.add("staging-integrity-check-failed")
    if (staging.autoVacuum !== 2) reasons.add("staging-auto-vacuum-not-incremental")
    if (staging.size >= before.size) reasons.add("compacted-file-not-smaller")
    if (!sameRetainedMetrics(before.retainedSession, backup.retainedSession, staging.retainedSession)) {
      reasons.add("retained-read-diverged-before-restore")
    }
    if (reasons.size > 0) {
      return {
        state: "failed",
        changedFiles,
        reasons: Array.from(reasons).toSorted(),
        sourceDeviceID,
        backupDeviceID,
        stagingDeviceID,
        measurements: { before, backup, staging },
      }
    }

    restoreTemp = path.join(path.dirname(sourcePath), `.${path.basename(sourcePath)}.${randomUUID()}.restore`)
    const restoreFree = await statfs(path.dirname(sourcePath))
    if (restoreFree.bavail * restoreFree.bsize < staging.size) {
      reasons.add("source-restore-capacity-insufficient")
      return {
        state: "refused",
        changedFiles,
        reasons: Array.from(reasons),
        sourceDeviceID,
        backupDeviceID,
        stagingDeviceID,
        measurements: { before, backup, staging },
      }
    }
    await copyFile(stagingPath, restoreTemp, 1)
    changedFiles += 1
    const stagedRestore = await physicalMetrics(restoreTemp, input.retainedSessionID)
    if (!sameRetainedMetrics(before.retainedSession, stagedRestore.retainedSession)) {
      reasons.add("restored-read-diverged-before-replacement")
      return {
        state: "failed",
        changedFiles,
        reasons: Array.from(reasons),
        sourceDeviceID,
        backupDeviceID,
        stagingDeviceID,
        measurements: { before, backup, staging, restored: stagedRestore },
      }
    }
    const openBeforeReplace = openPath(sourcePath)
    if (!openBeforeReplace.readable || openBeforeReplace.pids.length > 0) {
      reasons.add(
        openBeforeReplace.readable ? "source-inode-still-open-before-replacement" : "source-inode-liveness-unavailable",
      )
      return {
        state: "refused",
        changedFiles,
        reasons: Array.from(reasons).toSorted(),
        sourceDeviceID,
        backupDeviceID,
        stagingDeviceID,
        measurements: { before, backup, staging, restored: stagedRestore },
      }
    }
    await rename(restoreTemp, sourcePath)
    restoreTemp = undefined
    changedFiles += 1
    const deleted = deletedPathOpen(sourcePath)
    if (!deleted.readable || deleted.paths.length > 0) {
      reasons.add(deleted.readable ? "deleted-source-inode-still-open" : "deleted-inode-scan-unavailable")
    }
    const restored = await physicalMetrics(sourcePath, input.retainedSessionID)
    if (restored.size >= before.size) reasons.add("restored-file-not-smaller")
    if (restored.allocatedBytes >= before.allocatedBytes) reasons.add("restored-allocation-not-smaller")
    if (!sameRetainedMetrics(before.retainedSession, restored.retainedSession))
      reasons.add("retained-read-diverged-after-restore")
    if (restored.integrityCheck !== "ok") reasons.add("restored-integrity-check-failed")
    return {
      state: reasons.size === 0 ? "complete" : "failed",
      changedFiles,
      reasons: Array.from(reasons).toSorted(),
      sourceDeviceID,
      backupDeviceID,
      stagingDeviceID,
      measurements: { before, backup, staging, restored },
    }
  } catch (error) {
    if (restoreTemp) await unlink(restoreTemp).catch(() => undefined)
    reasons.add(`fixture-compaction-failed:${error instanceof Error ? error.message : "unknown"}`)
    return {
      state: changedFiles === 0 ? "refused" : "failed",
      changedFiles,
      reasons: Array.from(reasons).toSorted(),
      sourceDeviceID,
      backupDeviceID,
      stagingDeviceID,
    }
  }
}

async function physicalMetrics(filename: string, sessionID: string): Promise<PhysicalMetrics> {
  const sqlite = await import("bun:sqlite")
  const db = new sqlite.Database(filename, { readonly: true }) as unknown as SqliteAccess
  try {
    const integrityCheck =
      oneRow<{ integrity_check: string }>(db, "PRAGMA integrity_check")?.integrity_check ?? "unreadable"
    const pageCount = oneRow<{ page_count: number }>(db, "PRAGMA page_count")?.page_count ?? -1
    const freelistCount = oneRow<{ freelist_count: number }>(db, "PRAGMA freelist_count")?.freelist_count ?? -1
    const autoVacuum = oneRow<{ auto_vacuum: number }>(db, "PRAGMA auto_vacuum")?.auto_vacuum ?? -1
    const retainedSession = oneRow<Record<string, unknown>>(
      db,
      "SELECT id, parent_id, project_id, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write, agent, model, time_created, time_updated FROM session WHERE id = ?",
      sessionID,
    )
    if (!retainedSession) throw new Error(`retained session missing: ${sessionID}`)
    const file = await stat(filename)
    return {
      size: file.size,
      blocks: file.blocks,
      allocatedBytes: file.blocks * 512,
      pageCount,
      freelistCount,
      autoVacuum,
      integrityCheck,
      retainedSession,
    }
  } finally {
    db.close()
  }
}

function sameRetainedMetrics(...values: Record<string, unknown>[]) {
  return values.every((value) => JSON.stringify(value) === JSON.stringify(values[0]))
}

function isFixtureDatabase(filename: string) {
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

function sqlLiteral(value: string) {
  return `'${value.replaceAll("'", "''")}'`
}

function openPath(filename: string) {
  const result = spawnSync("lsof", ["-t", filename], { encoding: "utf8", timeout: 15_000 })
  if (result.error || result.status === null || ![0, 1].includes(result.status)) return { readable: false, pids: [] }
  return { readable: true, pids: result.stdout.trim() ? result.stdout.trim().split(/\s+/) : [] }
}

function deletedPathOpen(filename: string) {
  const result = spawnSync("lsof", ["+L1", "-Fn"], { encoding: "utf8", timeout: 15_000 })
  if (result.error || result.status === null || ![0, 1].includes(result.status)) return { readable: false, paths: [] }
  return {
    readable: true,
    paths: result.stdout
      .split(/\r?\n/)
      .filter((line) => line.startsWith("n"))
      .map((line) => line.slice(1))
      .filter((name) => name === `${filename} (deleted)` || name === filename),
  }
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
  describe: "inventory Session retention candidates without writing the database",
  builder: (yargs) => yargs.option("apply", { type: "boolean", default: false, describe: "apply redaction" }),
  async handler(args) {
    const sqlite = await import("bun:sqlite")
    if (args.apply) {
      console.log(
        JSON.stringify({
          action: "refused",
          changedRows: 0,
          reasons: [
            "reviewed-age-boundary-unavailable",
            "cross-process-liveness-proof-unavailable",
            "measurement-handoff-receipt-unavailable",
            "reader-contract-unreviewed",
            "production-apply-disabled",
          ],
        }),
      )
      process.exitCode = 1
      return
    }
    try {
      const db = new sqlite.Database(Database.path(), { readonly: true }) as unknown as SqliteAccess
      try {
        console.log(JSON.stringify(inventory(db), null, 2))
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

export * as DbRetention from "./db-retention"
