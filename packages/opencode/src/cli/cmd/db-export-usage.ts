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
 * wrong. So the export preserves tokens per session per model, and carries
 * opencode's cost alongside, clearly labelled as its figure, for
 * cross-checking only.
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

/**
 * Per session and model. Only assistant messages carry usage, and a session's
 * own row holds totals that this must reconcile against - see `verify`.
 */
const MODEL_SQL = `
  SELECT
    session_id,
    json_extract(data, '$.providerID') AS provider_id,
    json_extract(data, '$.modelID')    AS model_id,
    COUNT(*)                                                     AS messages,
    SUM(COALESCE(json_extract(data, '$.tokens.input'), 0))       AS tokens_input,
    SUM(COALESCE(json_extract(data, '$.tokens.output'), 0))      AS tokens_output,
    SUM(COALESCE(json_extract(data, '$.tokens.reasoning'), 0))   AS tokens_reasoning,
    SUM(COALESCE(json_extract(data, '$.tokens.cache.read'), 0))  AS tokens_cache_read,
    SUM(COALESCE(json_extract(data, '$.tokens.cache.write'), 0)) AS tokens_cache_write,
    SUM(COALESCE(json_extract(data, '$.cost'), 0))               AS cost_reported
  FROM message
  WHERE json_extract(data, '$.role') = 'assistant'
  GROUP BY session_id, provider_id, model_id
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

const SESSION_SQL = `
  SELECT id, parent_id, project_id, directory, title, agent, model,
         time_created, time_updated, cost,
         tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write
  FROM session
`

export function buildRecords(input: {
  sessions: SessionRow[]
  models: ModelRow[]
  served: ServedRow[]
}): Record<string, unknown>[] {
  const servedBy = new Map<string, Set<string>>()
  for (const row of input.served) {
    const key = `${row.session_id}\u0000${row.provider_id}\u0000${row.model_id}`
    const set = servedBy.get(key) ?? new Set<string>()
    set.add(row.served)
    servedBy.set(key, set)
  }

  const sessions = new Map(input.sessions.map((session) => [session.id, session]))

  return input.models.map((row) => {
    const session = sessions.get(row.session_id)
    const key = `${row.session_id}\u0000${row.provider_id}\u0000${row.model_id}`
    return {
      type: "usage",
      sessionID: row.session_id,
      parentID: session?.parent_id ?? null,
      projectID: session?.project_id ?? null,
      directory: session?.directory ?? null,
      title: session?.title ?? null,
      agent: session?.agent ?? null,
      configuredModel: session?.model ?? null,
      timeCreated: session?.time_created ?? null,
      timeUpdated: session?.time_updated ?? null,
      providerID: row.provider_id,
      modelID: row.model_id,
      messages: row.messages,
      tokens: {
        input: row.tokens_input,
        output: row.tokens_output,
        reasoning: row.tokens_reasoning,
        cacheRead: row.tokens_cache_read,
        cacheWrite: row.tokens_cache_write,
      },
      servedModelIDs: [...(servedBy.get(key) ?? [])].sort(),
      // opencode's own figure. Kept for cross-checking, NOT as the basis for
      // billing - re-derive from the tokens above against a dated rate table.
      reportedCost: row.cost_reported,
    }
  })
}

/**
 * Reconcile the per-model totals against the session table's own columns.
 *
 * #14 makes this non-optional, and the reason is the sequencing: this is the
 * only thing standing between a database reset and the silent loss of the
 * cost history, so it gets an explicit pass/fail rather than a log line.
 */
export function verify(input: { sessions: SessionRow[]; records: Record<string, unknown>[] }): {
  ok: boolean
  lines: string[]
} {
  const field = (record: Record<string, unknown>, name: string) =>
    Number((record["tokens"] as Record<string, number>)[name] ?? 0)

  const exported = input.records.reduce(
    (acc, record) => ({
      input: acc.input + field(record, "input"),
      output: acc.output + field(record, "output"),
      reasoning: acc.reasoning + field(record, "reasoning"),
      cacheRead: acc.cacheRead + field(record, "cacheRead"),
      cacheWrite: acc.cacheWrite + field(record, "cacheWrite"),
    }),
    { input: 0, output: 0, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
  )

  const stored = input.sessions.reduce(
    (acc, session) => ({
      input: acc.input + session.tokens_input,
      output: acc.output + session.tokens_output,
      reasoning: acc.reasoning + session.tokens_reasoning,
      cacheRead: acc.cacheRead + session.tokens_cache_read,
      cacheWrite: acc.cacheWrite + session.tokens_cache_write,
    }),
    { input: 0, output: 0, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
  )

  const lines: string[] = []
  let ok = true
  for (const key of ["input", "output", "reasoning", "cacheRead", "cacheWrite"] as const) {
    const match = exported[key] === stored[key]
    if (!match) ok = false
    lines.push(`  ${match ? "ok  " : "MISMATCH"} ${key.padEnd(10)} exported=${exported[key]} session=${stored[key]}`)
  }
  return { ok, lines }
}

export const ExportUsageCommand = effectCmd({
  command: "export-usage",
  describe: "export per-session token usage as JSONL, for archiving before a reset",
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
      // One read transaction across all three queries.
      //
      // Without it they see different snapshots, and on a live database that
      // is not theoretical: the first run of this against the real 59.86 GB
      // file took 4m30s with crews actively writing, and reconciliation
      // failed by +3 input, +583 output and +307,620 cache-read tokens -
      // purely because the session table had moved on between queries.
      //
      // WAL gives a read transaction a stable snapshot without blocking
      // writers, so this stays safe to run against a live instance.
      const read = db.transaction(() => ({
        sessions: db.query(SESSION_SQL).all() as SessionRow[],
        models: db.query(MODEL_SQL).all() as ModelRow[],
        served: db.query(SERVED_SQL).all() as ServedRow[],
      }))
      const { sessions, models, served } = read()
      const records = buildRecords({ sessions, models, served })
      const check = verify({ sessions, records })

      const summary = {
        type: "summary",
        schemaVersion: SCHEMA_VERSION,
        database: file,
        exportedAt: new Date().toISOString(),
        sessions: sessions.length,
        records: records.length,
        reportedCostTotal: sessions.reduce((total, session) => total + session.cost, 0),
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
