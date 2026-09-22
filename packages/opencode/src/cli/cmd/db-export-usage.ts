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
export function verify(input: { sessions: SessionRow[]; records: Record<string, unknown>[]; orphanMessages: number }): {
  ok: boolean
  lines: string[]
} {
  const tokens = (record: Record<string, unknown>) => record["tokens"] as Record<string, number>

  // Per session, because a global comparison says only that SOMETHING
  // disagrees. Measured on the real database: one session out of 7,196
  // accounted for the entire gap, and a global total could not show that.
  const exported = new Map<string, number>()
  for (const record of input.records) {
    const id = String(record["sessionID"])
    const t = tokens(record)
    exported.set(
      id,
      (exported.get(id) ?? 0) + t["input"]! + t["output"]! + t["reasoning"]! + t["cacheRead"]! + t["cacheWrite"]!,
    )
  }

  const divergent: string[] = []
  for (const session of input.sessions) {
    const stored =
      session.tokens_input +
      session.tokens_output +
      session.tokens_reasoning +
      session.tokens_cache_read +
      session.tokens_cache_write
    const fromMessages = exported.get(session.id) ?? 0
    if (fromMessages !== stored) divergent.push(`${session.id} messages=${fromMessages} session=${stored}`)
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

  // Divergence between the two is NOT a failure, and the reason is what this
  // export is for. #14 asks to preserve the MEASUREMENT - tokens per session
  // per model, read from the messages - because a harness's own rollup cannot
  // be re-priced when rates change. The session table's token columns are
  // that rollup: a derived cache. When the two disagree the messages are the
  // source of truth and the export already carries them, so this is reported
  // rather than treated as a blocker.
  lines.push(
    `  ${divergent.length === 0 ? "ok  " : "note"} session rollups   ${divergent.length} of ${input.sessions.length} disagree with their own messages`,
  )
  for (const entry of divergent.slice(0, 10)) lines.push(`         ${entry}`)
  if (divergent.length > 10) lines.push(`         ... and ${divergent.length - 10} more`)

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
        orphans: (db.query(ORPHAN_SQL).get() as { n: number }).n,
      }))
      const { sessions, models, served, orphans } = read()
      const records = buildRecords({ sessions, models, served })
      const check = verify({ sessions, records, orphanMessages: orphans })

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
