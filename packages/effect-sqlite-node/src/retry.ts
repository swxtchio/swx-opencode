import { Context, Deferred, Duration, Effect, Exit, Option, Schedule, Scope, Semaphore } from "effect"
import { isSqlError, LockTimeoutError, SqlError } from "effect/unstable/sql/SqlError"

const sqliteBusySnapshot = 517
const defaultRetryWindow = Duration.seconds(10)
const sessionRetryWindow = Duration.minutes(1)
const lockWaits = new WeakMap<object, number>()

type RetryWindowState = {
  readonly windowMs: number
  startedAt: number | undefined
  sawLock: boolean
  sawPermit: boolean
}

export class RetryWindow extends Context.Service<RetryWindow, RetryWindowState>()(
  "@opencode-ai/effect-sqlite-node/RetryWindow",
) {}

class UnboundedPermitWait extends Context.Service<UnboundedPermitWait, true>()(
  "@opencode-ai/effect-sqlite-node/UnboundedPermitWait",
) {}

export type RetryOperationState = { sawContention: boolean }

class RetryOperation extends Context.Service<RetryOperation, RetryOperationState>()(
  "@opencode-ai/effect-sqlite-node/RetryOperation",
) {}

export const withRetryWindow = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.withFiber((fiber) => {
    if (Option.isSome(Context.getOption(fiber.context, RetryWindow))) return effect
    return effect.pipe(
      Effect.provideService(RetryWindow, {
        windowMs: Duration.toMillis(sessionRetryWindow),
        startedAt: undefined,
        sawLock: false,
        sawPermit: false,
      }),
    )
  })

export const withFreshRetryWindow = <A, E, R>(effect: Effect.Effect<A, E, R>, window = sessionRetryWindow) =>
  effect.pipe(
    Effect.provideService(RetryWindow, {
      windowMs: Duration.toMillis(window),
      startedAt: undefined,
      sawLock: false,
      sawPermit: false,
    }),
  )

// An abandoned consume attempt may finish BEGIN and roll back after its caller's backstop; the body guard forbids deletion.
export const withUnboundedPermitWait = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provideService(UnboundedPermitWait, true))

export const resetRetryWindow = Effect.gen(function* () {
  const window = yield* Effect.serviceOption(RetryWindow)
  if (Option.isNone(window)) return
  window.value.startedAt = undefined
  window.value.sawLock = false
  window.value.sawPermit = false
})

const permitWaitBudget = Effect.withFiber((fiber) => {
  if (Option.isSome(Context.getOption(fiber.context, UnboundedPermitWait))) return Effect.succeed(undefined)
  const operation = Context.getOption(fiber.context, RetryOperation)
  if (Option.isSome(operation)) operation.value.sawContention = true
  const window = Context.getOption(fiber.context, RetryWindow)
  if (Option.isNone(window)) return Effect.succeed(undefined)
  return Effect.sync(() => {
    window.value.startedAt ??= performance.now()
    window.value.sawPermit = true
    return Math.max(0, window.value.windowMs - (performance.now() - window.value.startedAt))
  })
})

const permitWaitTimeout = Effect.withFiber((fiber) =>
  Effect.sync(() => {
    const window = Context.getOption(fiber.context, RetryWindow)
    const elapsedMs =
      Option.isSome(window) && window.value.startedAt !== undefined
        ? Math.round(performance.now() - window.value.startedAt)
        : 0
    const cause = Object.assign(new Error("SQLite connection permit wait exceeded the session retry window"), {
      code: "SQLITE_BUSY",
      sqlitePermitWait: true,
    })
    const error = new SqlError({
      reason: new LockTimeoutError({
        cause,
        message: "Failed to execute statement: database is locked (SQLITE_BUSY)",
        operation: "execute",
      }),
    })
    lockWaits.set(error, elapsedMs)
    return error
  }),
)

// The caller owns the returned scope and closes it after one statement attempt or transfers it to a transaction.
export const acquirePermitLease = (
  semaphore: Semaphore.Semaphore,
  permits = 1,
  operation?: RetryOperationState,
) =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const lease = yield* Scope.make()
      const acquired = yield* Deferred.make<void>()
      const hold = semaphore
        .withPermits(permits)(Effect.andThen(Deferred.succeed(acquired, undefined), Effect.never))
        .pipe(Effect.forkScoped({ startImmediately: true }), Effect.asVoid)
      const forked = yield* Effect.exit(Scope.provide(hold, lease))
      if (Exit.isFailure(forked)) {
        yield* Scope.close(lease, forked)
        return yield* Effect.failCause(forked.cause)
      }
      if (Deferred.isDoneUnsafe(acquired)) return lease

      if (operation) operation.sawContention = true
      const remainingMs = yield* permitWaitBudget
      if (remainingMs === undefined) {
        const waited = yield* restore(Deferred.await(acquired)).pipe(Effect.exit)
        if (Exit.isFailure(waited)) {
          yield* Scope.close(lease, waited)
          return yield* Effect.failCause(waited.cause)
        }
        return lease
      }

      const waited = yield* restore(
        Deferred.await(acquired).pipe(Effect.timeoutOption(Duration.millis(remainingMs))),
      ).pipe(Effect.exit)
      if (Exit.isFailure(waited)) {
        yield* Scope.close(lease, waited)
        return yield* Effect.failCause(waited.cause)
      }
      if (Option.isNone(waited.value)) {
        yield* Scope.close(lease, Exit.void)
        return yield* Effect.fail(yield* permitWaitTimeout)
      }
      return lease
    }),
  )

// Retry gates wrap one permit lease per native statement, so the backoff never monopolizes the shared connection.
export const withPermit = <A, E, R>(
  semaphore: Semaphore.Semaphore,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | SqlError, R> =>
  Effect.scoped(
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const immediate = yield* semaphore.withPermitsIfAvailable(1)(restore(effect))
        if (immediate._tag === "Some") return immediate.value
        const lease = yield* restore(acquirePermitLease(semaphore))
        const result = yield* Effect.exit(restore(effect))
        yield* Scope.close(lease, result)
        return yield* result
      }),
    ),
  )

export const retryWindowStatus = Effect.withFiber((fiber) =>
  Effect.sync(() => {
    const window = Context.getOption(fiber.context, RetryWindow)
    if (Option.isNone(window)) {
      const windowMs = Duration.toMillis(sessionRetryWindow)
      return { windowMs, elapsedMs: 0, remainingMs: windowMs, sawLock: false, sawPermit: false }
    }
    const elapsedMs = window.value.startedAt === undefined ? 0 : performance.now() - window.value.startedAt
    return {
      windowMs: window.value.windowMs,
      elapsedMs: Math.round(elapsedMs),
      remainingMs: Math.max(0, window.value.windowMs - elapsedMs),
      sawLock: window.value.sawLock,
      sawPermit: window.value.sawPermit,
    }
  }),
)

export const makeStatementGate = Effect.sync(() => {
  let active = 0
  let exclusive = false
  const waiters = new Set<Deferred.Deferred<void>>()
  const wake = Effect.gen(function* () {
    const pending = yield* Effect.sync(() => {
      const result = Array.from(waiters)
      waiters.clear()
      return result
    })
    yield* Effect.forEach(pending, (waiter) => Deferred.succeed(waiter, undefined), { discard: true })
  })
  const withStatement = <A, E, R>(effect: Effect.Effect<A, E, R>, operation?: RetryOperationState) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        while (true) {
          const waiter = yield* Deferred.make<void>()
          const admitted = yield* Effect.sync(() => {
            if (exclusive) {
              waiters.add(waiter)
              return false
            }
            active++
            return true
          })
          if (admitted) break
          if (operation) operation.sawContention = true
          const remainingMs = yield* permitWaitBudget
          const wait =
            remainingMs === undefined
              ? Deferred.await(waiter).pipe(Effect.map(Option.some))
              : Deferred.await(waiter).pipe(Effect.timeoutOption(Duration.millis(remainingMs)))
          const result = yield* restore(wait).pipe(Effect.exit)
          yield* Effect.sync(() => waiters.delete(waiter))
          if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause)
          if (Option.isNone(result.value)) return yield* Effect.fail(yield* permitWaitTimeout)
        }

        const result = yield* Effect.exit(restore(effect))
        const idle = yield* Effect.sync(() => --active === 0)
        if (idle) yield* wake
        return yield* result
      }),
    )
  const withExclusive = <A, E, R>(effect: Effect.Effect<A, E, R>, operation?: RetryOperationState) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        while (true) {
          const waiter = yield* Deferred.make<void>()
          const admitted = yield* Effect.sync(() => {
            if (exclusive || active > 0) {
              waiters.add(waiter)
              return false
            }
            exclusive = true
            return true
          })
          if (admitted) break
          if (operation) operation.sawContention = true
          const remainingMs = yield* permitWaitBudget
          const wait =
            remainingMs === undefined
              ? Deferred.await(waiter).pipe(Effect.map(Option.some))
              : Deferred.await(waiter).pipe(Effect.timeoutOption(Duration.millis(remainingMs)))
          const result = yield* restore(wait).pipe(Effect.exit)
          yield* Effect.sync(() => waiters.delete(waiter))
          if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause)
          if (Option.isNone(result.value)) return yield* Effect.fail(yield* permitWaitTimeout)
          // The driver's active count drops just before its caller resumes with the completed statement.
          yield* Effect.yieldNow
        }

        const result = yield* Effect.exit(restore(effect))
        yield* Effect.sync(() => (exclusive = false))
        yield* wake
        return yield* result
      }),
    )
  return { withStatement, withExclusive }
})

export const lockWaitMs = (error: unknown) => {
  if (!isSqlError(error)) return undefined
  return lockWaits.get(error)
}

// Keep the default client window short; SessionPrompt supplies one shared recovery window for its dependent writes.
const retrySchedule = (windowMs: number) =>
  Schedule.exponential("10 millis").pipe(
    Schedule.modifyDelay((_output, delay) => Effect.succeed(Duration.millis(Math.min(Duration.toMillis(delay), 100)))),
    Schedule.jittered,
    Schedule.both(Schedule.during(Duration.millis(windowMs))),
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
export const retryLocked = <A>(
  execute: Effect.Effect<A, SqlError>,
  sql: string,
  resetOnSuccess = true,
  operation: RetryOperationState = { sawContention: false },
) =>
  Effect.withFiber((fiber) => {
    const window = Option.getOrUndefined(Context.getOption(fiber.context, RetryWindow))
    return Effect.suspend(() => {
      const startedAt = performance.now()
      let attempts = 0
      let episodeStartedAt: number | undefined
      const attempt = Effect.suspend(() => {
        attempts++
        return execute
      }).pipe(Effect.provideService(RetryOperation, operation))
      const retryable = (error: SqlError) => error.reason.isRetryable && !isStaleSnapshot(error.reason.cause)
      const withinWindow = (error: SqlError) => {
        if (!retryable(error)) return false
        operation.sawContention = true
        if (error.reason._tag !== "LockTimeoutError" || !window) return true
        if (isPermitWait(error.reason.cause)) window.sawPermit = true
        else window.sawLock = true
        window.startedAt ??= performance.now()
        episodeStartedAt ??= window.startedAt
        return performance.now() - window.startedAt < window.windowMs
      }
      // Only a statement that met the lock builds the retry schedule, which re-attempts it at once before backing off.
      return attempt.pipe(
        Effect.catchIf(withinWindow, (error) =>
          Effect.retry(attempt, {
            schedule: retrySchedule(
              error.reason._tag === "LockTimeoutError" && window
                ? window.windowMs
                : Duration.toMillis(defaultRetryWindow),
            ),
            while: withinWindow,
          }),
        ),
        Effect.tap(() => {
          if (!resetOnSuccess || !operation.sawContention) return Effect.void
          return resetRetryWindow.pipe(Effect.andThen(Effect.sync(() => (operation.sawContention = false))))
        }),
        Effect.tapError((error) => {
          if (error.reason._tag !== "LockTimeoutError") return Effect.void
          const cause: { code?: unknown; errcode?: unknown; errno?: unknown } =
            typeof error.reason.cause === "object" && error.reason.cause !== null ? error.reason.cause : {}
          const elapsedMs = Math.round(performance.now() - (episodeStartedAt ?? startedAt))
          if (!isStaleSnapshot(error.reason.cause) && (attempts > 1 || window?.sawLock || window?.sawPermit))
            lockWaits.set(error, elapsedMs)
          return Effect.logWarning(attempts > 1 ? "sqlite lock retries exhausted" : "sqlite lock failed without retry", {
            pid: process.pid,
            statement: statementOperation(sql),
            attempts,
            elapsedMs,
            // node:sqlite's code is generic, so its full extended result code tells BUSY from BUSY_SNAPSHOT.
            "sqlite.code": cause.code,
            "sqlite.errcode": cause.errcode ?? cause.errno,
          })
        }),
      )
    })
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

export const isCommitStatement = (sql: string) => {
  const operation = statementOperation(sql)
  return operation === "commit" || operation === "end"
}

const isPermitWait = (cause: unknown) =>
  typeof cause === "object" && cause !== null && "sqlitePermitWait" in cause && cause.sqlitePermitWait === true

export const isBeginStatement = (sql: string) => {
  const operation = statementOperation(sql)
  return operation === "begin" || operation.startsWith("begin ")
}
