#!/usr/bin/env bun
// Measures the write-lock hold of real retention apply batches on an isolated copy of one Session tree.
// The source database is only read (opened read-only); every write goes to a fresh private copy built by
// the production Database layer. Usage:
//   bun script/retention-lock-measure.ts --source <opencode.db> --session <root-session-id> [--batches 5] [--batch-size 32]
import { Database as Sqlite } from "bun:sqlite"
import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { parseArgs } from "node:util"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import {
  apply,
  inactivityPolicy,
  inventory,
  scanDatabaseHolders,
  type RetentionEvidence,
  type SqliteAccess,
} from "../src/cli/cmd/db-retention"

const args = parseArgs({
  args: process.argv.slice(2),
  options: {
    source: { type: "string" },
    session: { type: "string" },
    batches: { type: "string", default: "5" },
    "batch-size": { type: "string", default: "32" },
    keep: { type: "boolean", default: false },
  },
})
const source = args.values.source
const root = args.values.session
if (!source || !root) throw new Error("--source and --session are required")
const batches = Number(args.values.batches)
const batchSize = Number(args.values["batch-size"])

const directory = await mkdtemp(path.join(os.tmpdir(), "opencode-retention-lock-"))
const filename = path.join(directory, "opencode.db")
try {
  await Effect.runPromise(Effect.scoped(Layer.build(Database.layerFromPath(filename))))
  const live = new Sqlite(source, { readonly: true })
  const copy = new Sqlite(filename)
  const copyStarted = performance.now()
  const sessionIDs = live
    .query<{ id: string }, [string]>(
      "WITH RECURSIVE tree(id) AS (SELECT ? UNION SELECT session.id FROM session JOIN tree ON session.parent_id = tree.id) SELECT id FROM tree",
    )
    .all(root)
    .map((row) => row.id)
  if (sessionIDs.length === 0) throw new Error(`session ${root} not found`)
  const scale: Record<string, { rows: number; bytes: number }> = {}
  const scopes = [
    ["session", "id"],
    ["event_sequence", "aggregate_id"],
    ["event", "aggregate_id"],
    ["message", "session_id"],
    ["part", "session_id"],
    ["session_message", "session_id"],
    ["session_input", "session_id"],
    ["session_context_epoch", "session_id"],
    ["session_prompt_queue", "session_id"],
    ["session_prompt_queue_sequence", "session_id"],
    ["todo", "session_id"],
    ["session_share", "session_id"],
  ] as const
  const ids = sessionIDs.map(() => "?").join(",")
  for (const [table, scope] of scopes) {
    const columns = copy
      .query<{ name: string }, []>(`PRAGMA table_info(${table})`)
      .all()
      .map((row) => row.name)
    const insert = copy.query(
      `INSERT INTO ${table} (${columns.join(",")}) VALUES (${columns.map(() => "?").join(",")})`,
    )
    let rows = 0
    let bytes = 0
    copy.exec("BEGIN")
    for (const row of live
      .query<Record<string, string | number | null>, string[]>(`SELECT ${columns.join(",")} FROM ${table} WHERE ${scope} IN (${ids})`)
      .iterate(...sessionIDs)) {
      insert.run(...columns.map((column) => row[column] ?? null))
      rows += 1
      bytes += Object.values(row).reduce<number>((total, value) => total + Buffer.byteLength(String(value ?? "")), 0)
    }
    copy.exec("COMMIT")
    scale[table] = { rows, bytes }
  }
  live.close()
  const copySeconds = (performance.now() - copyStarted) / 1000

  const db = copy as unknown as SqliteAccess
  const lastActivity = Math.max(
    ...["session", "message", "part", "session_message", "todo"].map(
      (table) =>
        copy
          .query<{ newest: number | null }, string[]>(
            `SELECT max(time_updated) AS newest FROM ${table} WHERE ${table === "session" ? "id" : "session_id"} IN (${ids})`,
          )
          .get(...sessionIDs)?.newest ?? 0,
    ),
  )
  // The copied tree is younger than the policy window, so the clock is set one minute past it; the
  // inactivity gate still runs inside every measured batch.
  const now = lastActivity + inactivityPolicy.windowMs + 60_000
  const finalSequence = Object.fromEntries(
    sessionIDs.map((id) => [
      id,
      copy.query<{ seq: number }, [string]>("SELECT seq FROM event_sequence WHERE aggregate_id = ?").get(id)?.seq ?? -1,
    ]),
  )
  // Stand-ins for the gates that have no approved producer yet (receipt, customer classification, reader
  // contract); the holder scan is the production producer.
  const evidence = (): RetentionEvidence => ({
    customerBinding: {
      proofID: "lock-measurement",
      durable: true,
      sessionIDs,
      customerBoundSessionIDs: [],
      nonCustomerSessionIDs: sessionIDs,
    },
    readerContract: { reviewed: true, id: "lock-measurement" },
    holders: scanDatabaseHolders(filename),
    handoff: {
      receiptID: "lock-measurement",
      durable: true,
      sessionIDs,
      finalSequence,
      axes: Object.fromEntries(
        [
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
        ].map((axis) => [axis, { status: "retained" as const }]),
      ) as NonNullable<RetentionEvidence["handoff"]>["axes"],
      report: {
        windowStart: "2026-10-01T00:00:00Z",
        windowEnd: "2026-10-02T00:00:00Z",
        resultDigest: "lock-measurement",
        denominators: {},
        unavailableCauses: {},
        rawHistoryInaccessible: true,
      },
    },
  })
  const inventoryStarted = performance.now()
  const tree = inventory(db, evidence(), now, { transactional: false }).trees.find((item) => item.rootSessionID === root)
  const inventorySeconds = (performance.now() - inventoryStarted) / 1000
  if (!tree?.eligible) throw new Error(`copied tree is not eligible: ${tree?.reasons.join(",")}`)
  const results = Array.from({ length: batches }, () =>
    apply(db, { tree, evidence, now: () => now, batchSize, maxBatches: 1 }),
  )
  console.log(
    JSON.stringify(
      {
        source: { path: source, rootSessionID: root, sessionIDs: sessionIDs.length },
        copy: { path: filename, seconds: Number(copySeconds.toFixed(1)), scale },
        inventorySeconds: Number(inventorySeconds.toFixed(2)),
        batchSize,
        batches: results.map((result) => ({
          state: result.state,
          changedRows: result.changedRows,
          lockHoldMs: Number(result.lockHold.maxMs.toFixed(1)),
          reasons: result.reasons,
        })),
      },
      null,
      2,
    ),
  )
  copy.close()
} finally {
  if (!args.values.keep) await rm(directory, { recursive: true, force: true })
}
