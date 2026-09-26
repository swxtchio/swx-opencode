import { Database as Sqlite } from "bun:sqlite"
import { Database } from "@opencode-ai/core/database/database"
import { Effect } from "effect"
import type { Argv } from "yargs"
import { effectCmd } from "../effect-cmd"

/**
 * Export the pricing MEASUREMENT from a session database, so the database can
 * be reset without losing cost history.
 *
 * Deliberately not opencode's own cost figure alone. swx-firstmate's
 * bin/fm-usage-lib.sh ignores the `cost` column and re-derives dollars from
 * tokens against a dated per-provider rate table, because a harness's
 * self-reported total cannot be re-priced when rates change or turn out
 * wrong. Record totals use step-finish usage when present and fall back to
 * message usage for legacy rows; per-served-model splits carry the same token
 * components and reported cost alongside opencode's figure for cross-checking.
 * The summary's `reportedCostTotal` is the sum of exported record costs, not the
 * session cost rollups.
 *
 * Read-only by construction: the database is opened with `readonly`, so this
 * is safe to run against a live instance and can be re-run as often as
 * wanted before anything destructive happens.
 */

const SCHEMA_VERSION = 1

type SessionRow = {
  id: string
  parent_id: string | null
  project_id: string
  directory: string
  title: string
  agent: string | null
  model: string | null
  time_created: number
  time_updated: number
  cost: number
  tokens_input: number
  tokens_output: number
  tokens_reasoning: number
  tokens_cache_read: number
  tokens_cache_write: number
}

type ModelRow = {
  message_id?: string
  session_id: string
  provider_id: string | null
  model_id: string | null
  messages: number
  tokens_input: number
  tokens_output: number
  tokens_reasoning: number
  tokens_cache_read: number
  tokens_cache_write: number
  cost_reported: number
}

type ServedRow = { session_id: string; provider_id: string | null; model_id: string | null; served: string }
type StepUsageRow = {
  message_id: string
  served_model_id: string | null
  tokens_input: number
  tokens_output: number
  tokens_reasoning: number
  tokens_cache_read: number
  tokens_cache_write: number
  cost_reported: number
}

type UsageTokens = { input: number; output: number; reasoning: number; cacheRead: number; cacheWrite: number }
type ServedModelUsage = { modelID: string | null; tokens: UsageTokens; reportedCost: number }
type UsageRecord = Record<string, unknown> & {
  messages: number
  tokens: UsageTokens
  servedModelIDs: string[]
  servedModelUsage: ServedModelUsage[]
  reportedCost: number
}

/** Keep assistant messages separate so step parts can supply totals without mixing legacy rows. */
const MODEL_SQL = `
  SELECT
    m.id                                                       AS message_id,
    m.session_id,
    json_extract(m.data, '$.providerID')                       AS provider_id,
    json_extract(m.data, '$.modelID')                          AS model_id,
    1                                                          AS messages,
    COALESCE(json_extract(m.data, '$.tokens.input'), 0)        AS tokens_input,
    COALESCE(json_extract(m.data, '$.tokens.output'), 0)       AS tokens_output,
    COALESCE(json_extract(m.data, '$.tokens.reasoning'), 0)    AS tokens_reasoning,
    COALESCE(json_extract(m.data, '$.tokens.cache.read'), 0)   AS tokens_cache_read,
    COALESCE(json_extract(m.data, '$.tokens.cache.write'), 0)  AS tokens_cache_write,
    COALESCE(json_extract(m.data, '$.cost'), 0)                AS cost_reported
  FROM message m
  WHERE json_extract(m.data, '$.role') = 'assistant'
`

/**
 * The models that actually served each turn, where recorded.
 *
 * `responseModelIDs` is the field #12 added: on a routed turn the configured
 * model and the served model differ, and only this records which answered.
 * Absent on older rows, which is why it is a separate query rather than a
 * column - a session with none simply has no served entry.
 */
const SERVED_SQL = `
  SELECT DISTINCT
    m.session_id,
    json_extract(m.data, '$.providerID') AS provider_id,
    json_extract(m.data, '$.modelID')    AS model_id,
    served.value                          AS served
  FROM message m, json_each(json_extract(m.data, '$.responseModelIDs')) AS served
  WHERE json_extract(m.data, '$.role') = 'assistant'
`

/** A step without a reported serving ID stays attributed to its requested model. */
const STEP_USAGE_SQL = `
  SELECT
    p.message_id,
    COALESCE(NULLIF(json_extract(p.data, '$.responseModelID'), ''), json_extract(m.data, '$.modelID')) AS served_model_id,
    SUM(COALESCE(json_extract(p.data, '$.tokens.input'), 0))       AS tokens_input,
    SUM(COALESCE(json_extract(p.data, '$.tokens.output'), 0))      AS tokens_output,
    SUM(COALESCE(json_extract(p.data, '$.tokens.reasoning'), 0))   AS tokens_reasoning,
    SUM(COALESCE(json_extract(p.data, '$.tokens.cache.read'), 0))  AS tokens_cache_read,
    SUM(COALESCE(json_extract(p.data, '$.tokens.cache.write'), 0)) AS tokens_cache_write,
    SUM(COALESCE(json_extract(p.data, '$.cost'), 0))               AS cost_reported
  FROM part p
  JOIN message m ON m.id = p.message_id AND m.session_id = p.session_id
  WHERE json_extract(m.data, '$.role') = 'assistant'
    AND json_extract(p.data, '$.type') = 'step-finish'
  GROUP BY p.message_id, served_model_id
`

/** Usage whose session row is gone - the only way this export can lose data. */
const ORPHAN_SQL = `
  SELECT COUNT(*) AS n
  FROM message m LEFT JOIN session s ON s.id = m.session_id
  WHERE s.id IS NULL AND json_extract(m.data, '$.role') = 'assistant'
`

const SESSION_SQL = `
  SELECT id, parent_id, project_id, directory, title, agent, model,
         time_created, time_updated, cost,
         tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write
  FROM session
`

/** session.model is JSON text; anything unparseable is preserved verbatim. */
export function parseModel(value: string | null | undefined): unknown {
  if (!value) return null
  try {
    return JSON.parse(value)
  } catch {
    return value
  }
}

export function buildRecords(input: {
  sessions: SessionRow[]
  models: ModelRow[]
  served: ServedRow[]
  steps?: StepUsageRow[]
}): Record<string, unknown>[] {
  const servedBy = new Map<string, Set<string>>()
  for (const row of input.served) {
    const key = `${row.session_id}\u0000${row.provider_id}\u0000${row.model_id}`
    const set = servedBy.get(key) ?? new Set<string>()
    set.add(row.served)
    servedBy.set(key, set)
  }

  const stepsByMessage = new Map<string, StepUsageRow[]>()
  for (const row of input.steps ?? []) {
    const rows = stepsByMessage.get(row.message_id) ?? []
    rows.push(row)
    stepsByMessage.set(row.message_id, rows)
  }

  const sessions = new Map(input.sessions.map((session) => [session.id, session]))
  const records = new Map<string, { record: UsageRecord; servedModelUsage: Map<string | null, ServedModelUsage> }>()

  for (const row of input.models) {
    const session = sessions.get(row.session_id)
    const key = `${row.session_id}\u0000${row.provider_id}\u0000${row.model_id}`
    let group = records.get(key)
    if (!group) {
      group = {
        record: {
          type: "usage",
          sessionID: row.session_id,
          parentID: session?.parent_id ?? null,
          projectID: session?.project_id ?? null,
          directory: session?.directory ?? null,
          title: session?.title ?? null,
          agent: session?.agent ?? null,
          // Parsed, not passed through. The session.model column holds JSON text,
          // so emitting it raw gives the archive a string like
          // "{\"id\":\"gpt-5.6-luna\",\"providerID\":\"swx-azure\",...}" -
          // which anyone re-deriving cost would have to parse again, from a field
          // whose shape is not obvious. An archive should not export its own
          // serialisation accident.
          configuredModel: parseModel(session?.model),
          timeCreated: session?.time_created ?? null,
          timeUpdated: session?.time_updated ?? null,
          providerID: row.provider_id,
          modelID: row.model_id,
          messages: 0,
          tokens: { input: 0, output: 0, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
          servedModelIDs: [],
          servedModelUsage: [],
          reportedCost: 0,
        },
        servedModelUsage: new Map(),
      }
      records.set(key, group)
    }

    group.record.messages += row.messages
    const stepRows = row.message_id ? stepsByMessage.get(row.message_id) : undefined
    // Legacy messages lack step ownership, so preserve their totals under the requested model.
    const usageRows = stepRows?.length ? stepRows : [undefined]
    for (const step of usageRows) {
      const modelID = step?.served_model_id ?? row.model_id
      const tokens = step
        ? {
            input: step.tokens_input,
            output: step.tokens_output,
            reasoning: step.tokens_reasoning,
            cacheRead: step.tokens_cache_read,
            cacheWrite: step.tokens_cache_write,
          }
        : {
            input: row.tokens_input,
            output: row.tokens_output,
            reasoning: row.tokens_reasoning,
            cacheRead: row.tokens_cache_read,
            cacheWrite: row.tokens_cache_write,
          }
      const cost = step?.cost_reported ?? row.cost_reported
      const served = group.servedModelUsage.get(modelID) ?? {
        modelID,
        tokens: { input: 0, output: 0, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
        reportedCost: 0,
      }
      served.tokens.input += tokens.input
      served.tokens.output += tokens.output
      served.tokens.reasoning += tokens.reasoning
      served.tokens.cacheRead += tokens.cacheRead
      served.tokens.cacheWrite += tokens.cacheWrite
      served.reportedCost += cost
      group.servedModelUsage.set(modelID, served)
      group.record.tokens.input += tokens.input
      group.record.tokens.output += tokens.output
      group.record.tokens.reasoning += tokens.reasoning
      group.record.tokens.cacheRead += tokens.cacheRead
      group.record.tokens.cacheWrite += tokens.cacheWrite
      group.record.reportedCost += cost
    }
  }

  return [...records].map(([key, group]) => ({
    ...group.record,
    servedModelIDs: [...(servedBy.get(key) ?? [])].sort(),
    servedModelUsage: [...group.servedModelUsage.values()].sort((a, b) =>
      (a.modelID ?? "").localeCompare(b.modelID ?? ""),
    ),
  }))
}

/**
 * Reconcile exported token totals against the session table's own columns.
 *
 * #14 makes this non-optional, and the reason is the sequencing: this is the
 * only thing standing between a database reset and the silent loss of the
 * cost history, so it gets an explicit pass/fail rather than a log line.
 */
export function verify(input: { sessions: SessionRow[]; records: Record<string, unknown>[]; orphanMessages: number }): {
  ok: boolean
  lines: string[]
} {
  const tokens = (record: Record<string, unknown>) => record["tokens"] as Record<string, number>

  // Per-session comparison identifies which rollup differs from exported usage.
  const exported = new Map<string, number>()
  const exportedCost = new Map<string, number>()
  for (const record of input.records) {
    const id = String(record["sessionID"])
    const t = tokens(record)
    exported.set(
      id,
      (exported.get(id) ?? 0) + t["input"]! + t["output"]! + t["reasoning"]! + t["cacheRead"]! + t["cacheWrite"]!,
    )
    const cost = record["reportedCost"]
    if (typeof cost === "number") exportedCost.set(id, (exportedCost.get(id) ?? 0) + cost)
  }

  const divergent: string[] = []
  const divergentCost: string[] = []
  for (const session of input.sessions) {
    const stored =
      session.tokens_input +
      session.tokens_output +
      session.tokens_reasoning +
      session.tokens_cache_read +
      session.tokens_cache_write
    const fromExport = exported.get(session.id) ?? 0
    if (fromExport !== stored) divergent.push(`${session.id} export=${fromExport} session=${stored}`)
    const fromExportCost = exportedCost.get(session.id) ?? 0
    const costTolerance = 1e-9 * Math.max(1, Math.abs(fromExportCost), Math.abs(session.cost))
    if (Math.abs(fromExportCost - session.cost) > costTolerance)
      divergentCost.push(`${session.id} export=${fromExportCost} session=${session.cost}`)
  }

  const lines: string[] = []

  // The only condition that can LOSE data, and therefore the only one that
  // fails. A message whose session row is gone is usage this export would
  // silently drop, which is exactly what must not happen before a reset.
  const ok = input.orphanMessages === 0
  lines.push(
    `  ${ok ? "ok  " : "FAIL"} orphan messages   ${input.orphanMessages}` +
      (ok ? "" : "  <- usage with no session row; it would be dropped"),
  )

  // Divergence is not a failure: the export prefers step-finish usage, then
  // falls back to message fields for legacy messages. Session token columns
  // are aggregate caches, so a difference is reported without blocking an
  // otherwise complete archive.
  lines.push(
    `  ${divergent.length === 0 ? "ok  " : "note"} session rollups   ${divergent.length} of ${input.sessions.length} disagree with exported usage`,
  )
  for (const entry of divergent.slice(0, 10)) lines.push(`         ${entry}`)
  if (divergent.length > 10) lines.push(`         ... and ${divergent.length - 10} more`)
  lines.push(
    `  ${divergentCost.length === 0 ? "ok  " : "note"} session cost rollups   ${divergentCost.length} of ${input.sessions.length} disagree with exported costs`,
  )
  for (const entry of divergentCost.slice(0, 10)) lines.push(`         ${entry}`)
  if (divergentCost.length > 10) lines.push(`         ... and ${divergentCost.length - 10} more`)

  return { ok, lines }
}

export function readExport(db: Sqlite) {
  // One read transaction gives every exported value the same database snapshot.
  const read = db.transaction(() => ({
    sessions: db.query(SESSION_SQL).all() as SessionRow[],
    models: db.query(MODEL_SQL).all() as ModelRow[],
    served: db.query(SERVED_SQL).all() as ServedRow[],
    steps: db.query(STEP_USAGE_SQL).all() as StepUsageRow[],
    orphans: (db.query(ORPHAN_SQL).get() as { n: number }).n,
  }))
  const { sessions, models, served, steps, orphans } = read()
  const records = buildRecords({ sessions, models, served, steps })
  const check = verify({ sessions, records, orphanMessages: orphans })
  const reportedCostTotal = records.reduce((total, record) => {
    const cost = record["reportedCost"]
    return total + (typeof cost === "number" ? cost : 0)
  }, 0)
  return { sessions, records, orphanMessages: orphans, reportedCostTotal, check }
}

export const ExportUsageCommand = effectCmd({
  command: "export-usage",
  describe:
    "export per-session usage as JSONL for archiving; step-finish totals take precedence, with message totals for legacy rows",
  instance: false,
  builder: (yargs: Argv) =>
    yargs
      .option("out", { type: "string", describe: "file to write (default: stdout)" })
      .option("db", {
        type: "string",
        describe: "database file to read (default: this build's database)",
      })
      .option("verify-only", {
        type: "boolean",
        default: false,
        describe: "reconcile totals without writing any records",
      }),
  handler: Effect.fn("Cli.db.exportUsage")(function* (args: { out?: string; db?: string; "verify-only": boolean }) {
    // Defaulting to Database.path() is deliberate but dangerous on its own:
    // the filename is CHANNEL-SUFFIXED, so a dev build reads
    // opencode-local.db while the installed binary's data lives in
    // opencode.db. Measured on this machine: 59.86 GB in one, 0 in the other.
    // The path is therefore always printed.
    const file = args.db ?? Database.path()
    const db = new Sqlite(file, { readonly: true })

    try {
      const { sessions, records, reportedCostTotal, check } = readExport(db)

      const summary = {
        type: "summary",
        schemaVersion: SCHEMA_VERSION,
        database: file,
        exportedAt: new Date().toISOString(),
        sessions: sessions.length,
        records: records.length,
        reportedCostTotal,
        verified: check.ok,
      }

      if (!args["verify-only"]) {
        const body = [...records, summary].map((record) => JSON.stringify(record)).join("\n") + "\n"
        if (args.out) yield* Effect.promise(() => Bun.write(args.out!, body))
        else process.stdout.write(body)
      }

      const report = [
        `database   ${file}`,
        `sessions   ${sessions.length}`,
        `records    ${records.length}`,
        `reconcile  ${check.ok ? "PASS" : "FAIL"}`,
        ...check.lines,
      ].join("\n")
      console.error(report)

      // Non-zero on mismatch: this runs before a destructive reset, and a
      // silent partial export is the one outcome that cannot be recovered
      // from.
      if (!check.ok) process.exitCode = 1
    } finally {
      db.close()
    }
  }),
})
