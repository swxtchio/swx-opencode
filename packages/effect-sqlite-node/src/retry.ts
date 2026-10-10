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

// Every SQLite client routes its statements through this one gate.
export const retryLocked = <A>(execute: Effect.Effect<A, SqlError>) => {
  const retryable = (error: SqlError) => error.reason.isRetryable && !isStaleSnapshot(error.reason.cause)
  // Only a statement that met the lock builds the retry schedule, which re-attempts it at once before backing off.
  return execute.pipe(
    Effect.catchIf(retryable, () => Effect.retry(execute, { schedule: retrySchedule, while: retryable })),
  )
}
