import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"
import type { SqlError } from "effect/unstable/sql/SqlError"

const sqliteBusySnapshot = 517

// Restore the lock tolerance the old busy_timeout gave, but wait asynchronously between short native attempts
// so a contended statement never blocks the event loop. The window runs from the first failed retry, and the
// backoff cap matches SQLite's own busy-handler sleep.
const retrySchedule = Schedule.exponential("10 millis").pipe(
  Schedule.modifyDelay((_output, delay) => Effect.succeed(Duration.millis(Math.min(Duration.toMillis(delay), 100)))),
  Schedule.jittered,
  Schedule.both(Schedule.during("5 seconds")),
)

// A stale read snapshot fails every retry of the same statement, and only restarting its transaction can recover,
// so that class fails at once. bun:sqlite reports it by code and node:sqlite by its SQLite result code.
const isStaleSnapshot = (cause: unknown) =>
  typeof cause === "object" &&
  cause !== null &&
  (("code" in cause && cause.code === "SQLITE_BUSY_SNAPSHOT") ||
    ("errcode" in cause && cause.errcode === sqliteBusySnapshot))

// Every SQLite client routes its statements through this one gate. A lock failure that leaves it logs one summary,
// never one line per attempt, so a contended statement cannot flood the log.
export const retryLocked = <A>(execute: Effect.Effect<A, SqlError>, sql: string) =>
  Effect.suspend(() => {
    const startedAt = performance.now()
    let attempts = 0
    const attempt = Effect.suspend(() => {
      attempts++
      return execute
    })
    const retryable = (error: SqlError) => error.reason.isRetryable && !isStaleSnapshot(error.reason.cause)
    // Only a statement that met the lock builds the retry schedule, which re-attempts it at once before backing off.
    return attempt.pipe(
      Effect.catchIf(retryable, () => Effect.retry(attempt, { schedule: retrySchedule, while: retryable })),
      Effect.tapError((error) => {
        if (error.reason._tag !== "LockTimeoutError") return Effect.void
        const cause: { code?: unknown; errcode?: unknown; errno?: unknown } =
          typeof error.reason.cause === "object" && error.reason.cause !== null ? error.reason.cause : {}
        return Effect.logWarning(retryable(error) ? "sqlite lock retries exhausted" : "sqlite lock failed without retry", {
          pid: process.pid,
          statement: statementOperation(sql),
          attempts,
          elapsedMs: Math.round(performance.now() - startedAt),
          // node:sqlite's code is generic, so its full extended result code tells BUSY from BUSY_SNAPSHOT.
          "sqlite.code": cause.code,
          "sqlite.errcode": cause.errcode ?? cause.errno,
        })
      }),
    )
  })

// Names a statement by its operation alone: the leading keyword, and a BEGIN's transaction behaviour. Leading comments
// are skipped and nothing after the operation is read, so comments, literals, identifiers and parameters never reach
// the log.
const statementOperation = (sql: string) => {
  const statement = sql.replace(/^(?:\s|--[^\n]*|\/\*[\s\S]*?\*\/)*/, "")
  const operation = statement.match(/^[a-z]+\b/i)?.[0].toLowerCase()
  if (operation !== "begin") return operation ?? "unknown"
  const behavior = statement.match(/^begin\s+(deferred|immediate|exclusive)\b/i)?.[1]
  return behavior ? `begin ${behavior.toLowerCase()}` : "begin"
}
