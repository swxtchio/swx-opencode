export * as EventWriter from "./writer"

import { randomUUID } from "node:crypto"
import { readFileSync } from "node:fs"
import { eq } from "drizzle-orm"
import { Effect } from "effect"
import type { Database } from "../database/database"
import { EventAggregateWriterTable, EventWriterProcessTable } from "./writer.sql"

type DatabaseTransaction = Parameters<Parameters<Database.Interface["db"]["transaction"]>[0]>[0]
type DatabaseAccess = Database.Interface["db"] | DatabaseTransaction

export type ProcessIdentity = {
  readonly pid: number
  readonly bootID: string | null
  readonly startTicks: string | null
}

// One writer ID per OS process: two Database layers opened by the same process share it.
const writerID = randomUUID()
let current: ProcessIdentity | undefined

export function id() {
  return writerID
}

export function self() {
  current ??= readIdentity(process.pid) ?? { pid: process.pid, bootID: null, startTicks: null }
  return current
}

/**
 * Reads a process's kernel identity from procfs. A PID alone can be reused; the boot ID and the
 * process start time (field 22 of /proc/<pid>/stat) together name exactly one process lifetime.
 * Returns undefined when the process does not exist, and null fields when the platform or a
 * permission boundary hides them.
 */
export function readIdentity(pid: number): ProcessIdentity | undefined {
  const stat = readProc(`/proc/${pid}/stat`)
  if (stat === "missing") return undefined
  return { pid, bootID: bootID(), startTicks: stat === "unreadable" ? null : startTicks(stat) }
}

/** Records this process as a holder of the database. Idempotent per process and database. */
export function register(db: DatabaseAccess) {
  const identity = self()
  return db
    .insert(EventWriterProcessTable)
    .values({
      writer_id: writerID,
      pid: identity.pid,
      boot_id: identity.bootID,
      start_ticks: identity.startTicks,
      time_registered: Date.now(),
    })
    .onConflictDoNothing()
    .run()
    .pipe(Effect.orDie, Effect.asVoid)
}

/**
 * Whether the process registered as `writer` still runs. "unknown" means its record, the platform or a
 * permission boundary cannot prove it gone, and callers must treat it as alive.
 */
export function liveness(db: DatabaseAccess, writer: string) {
  if (writer === writerID) return Effect.succeed("self" as const)
  return db
    .select()
    .from(EventWriterProcessTable)
    .where(eq(EventWriterProcessTable.writer_id, writer))
    .get()
    .pipe(
      Effect.orDie,
      Effect.map((row) => {
        if (!row?.boot_id || !row.start_ticks) return "unknown" as const
        const current = readIdentity(row.pid)
        if (!current) return "gone" as const
        if (!current.bootID || !current.startTicks) return "unknown" as const
        return current.bootID === row.boot_id && current.startTicks === row.start_ticks
          ? ("alive" as const)
          : ("gone" as const)
      }),
    )
}

/** Records, inside the caller's write transaction, that this process appended to the aggregate. */
export function recordAppend(db: DatabaseAccess, aggregateID: string) {
  const now = Date.now()
  return db
    .insert(EventAggregateWriterTable)
    .values({ aggregate_id: aggregateID, writer_id: writerID, time_last_append: now })
    .onConflictDoUpdate({
      target: [EventAggregateWriterTable.aggregate_id, EventAggregateWriterTable.writer_id],
      set: { time_last_append: now },
    })
    .run()
    .pipe(Effect.orDie, Effect.asVoid)
}

function bootID() {
  const value = readProc("/proc/sys/kernel/random/boot_id")
  return value === "missing" || value === "unreadable" ? null : value.trim() || null
}

function startTicks(stat: string) {
  // The command name in field 2 may contain spaces and parentheses, so fields are counted
  // from the last closing parenthesis: the field after it is field 3.
  const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ")
  const value = fields[22 - 3]
  return value && /^\d+$/.test(value) ? value : null
}

function readProc(filename: string) {
  try {
    return readFileSync(filename, "utf8")
  } catch (error) {
    if (error instanceof Error && "code" in error && (error.code === "ENOENT" || error.code === "ESRCH")) {
      return "missing" as const
    }
    return "unreadable" as const
  }
}
