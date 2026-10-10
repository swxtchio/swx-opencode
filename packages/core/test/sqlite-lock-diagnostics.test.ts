import { describe, expect, test } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { TransactionPurpose } from "@opencode-ai/effect-drizzle-sqlite"
import { Cause, Context, Effect, Exit, Fiber, Layer, Logger } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { isSqlError } from "effect/unstable/sql/SqlError"
import { Sqlite } from "../src/database/sqlite"
import {
  countStatement,
  newStatementAttempts,
  patchBunQuery,
  type StatementAttempts,
} from "../src/database/sqlite-attempts"
import path from "path"
import { tmpdir } from "./fixture/tmpdir"

const nodeSqliteDrivers = await import("node:sqlite").then(
  async (nodeSqlite) => ({
    core: await import("../src/database/sqlite.node"),
    standalone: await import("@opencode-ai/effect-sqlite-node"),
    DatabaseSync: nodeSqlite.DatabaseSync,
  }),
  () => undefined,
)
// Above the lock-hold threshold by a margin that scheduling noise cannot close.
const longHoldMs = 400
// Outlasts the driver's own retry window, so it only catches a retry that never stops.
const lockBackstop = "10 seconds"

type LogLine = { readonly text: unknown; readonly fields: Record<string, unknown> }

// Collects what the instrumented code sends to the Effect logger, in place of the file sink.
const captured = <A, E, R>(effect: (lines: LogLine[]) => Effect.Effect<A, E, R>) =>
  Effect.suspend(() => {
    const lines: LogLine[] = []
    const capture = Logger.make((options) => {
      const [text, fields] = Array.isArray(options.message) ? options.message : [options.message]
      lines.push({ text, fields: typeof fields === "object" && fields !== null ? fields : {} })
    })
    return effect(lines).pipe(Effect.provideService(Logger.CurrentLoggers, new Set([capture])))
  })

const holds = (lines: LogLine[]) => lines.filter((line) => line.text === "sqlite write lock held")
const exhausted = (lines: LogLine[]) => lines.filter((line) => line.text === "sqlite lock retries exhausted")
const unretried = (lines: LogLine[]) => lines.filter((line) => line.text === "sqlite lock failed without retry")

const run = async <A, E>(effect: Effect.Effect<A, E, import("effect").Scope.Scope>) => {
  const exit = await Effect.runPromiseExit(Effect.scoped(effect))
  if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
  return exit.value
}

const productionDatabase = (filename: string) =>
  Layer.build(Database.layerFromPath(filename)).pipe(Effect.map((context) => Context.get(context, Database.Service).db))

// Holds the write lock from a second connection in this process until the returned release runs.
const inProcessHolder = Effect.fn("inProcessHolder")(function* (filename: string) {
  const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
  const holder = new sqlite.Database(filename)
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      if (holder.inTransaction) holder.run("ROLLBACK")
      holder.close()
    }),
  )
  holder.run("BEGIN IMMEDIATE")
  return () => {
    if (holder.inTransaction) holder.run("COMMIT")
  }
})

// A holder in another process takes a real file lock, which every SQLite library in this process must respect.
const childHolder = Effect.fn("childHolder")(function* (filename: string) {
  const child = Bun.spawn(
    [
      "bun",
      "-e",
      `import { Database } from "bun:sqlite"
const native = new Database(process.env.SQLITE_LOCK_DB)
native.run("BEGIN IMMEDIATE")
console.log("ready")
await new Promise((resolve) => process.stdin.once("data", resolve))
native.run("COMMIT")
native.close()`,
    ],
    { env: { ...process.env, SQLITE_LOCK_DB: filename }, stdin: "pipe", stdout: "pipe", stderr: "inherit" },
  )
  yield* Effect.addFinalizer(() =>
    Effect.promise(async () => {
      child.stdin.write("release\n")
      child.stdin.end()
      await child.exited
    }),
  )
  yield* Effect.promise(async () => {
    const reader = child.stdout.getReader()
    const decoder = new TextDecoder()
    let output = ""
    while (!output.includes("ready\n")) {
      const chunk = await reader.read()
      if (chunk.done) throw new Error(`SQLite holder exited before ready: ${output}`)
      output += decoder.decode(chunk.value)
    }
    reader.releaseLock()
  })
})

const nodeClientWithAttemptCount = (input: {
  clientType: "core-node" | "standalone-node"
  filename: string
  statement: string
  attempts: StatementAttempts
}) =>
  Effect.gen(function* () {
    const drivers = nodeSqliteDrivers
    if (!drivers) return yield* Effect.die(new Error("node:sqlite tests ran without node:sqlite"))
    const counting = (prepare: (sql: string) => object) =>
      function (this: unknown, sql: string) {
        const statement = prepare.call(this, sql)
        return sql === input.statement ? countStatement(statement, input.attempts) : statement
      }
    if (input.clientType === "core-node") {
      const context = yield* Layer.build(drivers.core.layer({ filename: input.filename }))
      const native = Context.get(context, Sqlite.Native) as InstanceType<typeof drivers.DatabaseSync>
      Object.defineProperty(native, "prepare", { configurable: true, writable: true, value: counting(native.prepare) })
      return Context.get(context, SqlClient)
    }
    const context = yield* Layer.build(drivers.standalone.NodeSqliteClient.layer({ filename: input.filename }))
    const prototype = drivers.DatabaseSync.prototype
    const descriptor = Object.getOwnPropertyDescriptor(prototype, "prepare")
    if (!descriptor) return yield* Effect.die(new Error("node:sqlite prepare method was not found"))
    Object.defineProperty(prototype, "prepare", { ...descriptor, value: counting(prototype.prepare) })
    yield* Effect.addFinalizer(() => Effect.sync(() => Object.defineProperty(prototype, "prepare", descriptor)))
    return Context.get(context, SqlClient)
  })

describe("SQLite write-lock hold diagnostics", () => {
  test("logs an immediate transaction held past the threshold, by commit and by rollback, and not a short one", () =>
    run(
      captured((lines) =>
        Effect.gen(function* () {
          const tmp = yield* Effect.acquireRelease(
            Effect.promise(() => tmpdir()),
            (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
          )
          const db = yield* productionDatabase(path.join(tmp.path, "holds.sqlite"))
          yield* db.run("CREATE TABLE hold_test (id INTEGER PRIMARY KEY)")

          const committedStartedAt = performance.now()
          yield* db
            .transaction(
              (tx) =>
                tx.run("INSERT INTO hold_test (id) VALUES (1)").pipe(Effect.andThen(Effect.sleep(`${longHoldMs} millis`))),
              { behavior: "immediate" },
            )
            .pipe(Effect.provideService(TransactionPurpose, "test.commit"))
          const committedElapsedMs = performance.now() - committedStartedAt

          const rolledBack = yield* db
            .transaction(
              (tx) =>
                tx.run("INSERT INTO hold_test (id) VALUES (2)").pipe(
                  Effect.andThen(Effect.sleep(`${longHoldMs} millis`)),
                  Effect.andThen(Effect.fail("abandoned" as const)),
                ),
              { behavior: "immediate" },
            )
            .pipe(Effect.provideService(TransactionPurpose, "test.rollback"), Effect.flip)
          expect(rolledBack).toBe("abandoned")

          yield* db.transaction((tx) => tx.run("INSERT INTO hold_test (id) VALUES (3)"), { behavior: "immediate" }).pipe(
            Effect.provideService(TransactionPurpose, "test.short"),
          )
          // A deferred transaction does not take the write lock at BEGIN, so it is not a hold.
          yield* db.transaction(() => Effect.sleep(`${longHoldMs} millis`))

          const logged = holds(lines)
          expect(logged.map((line) => [line.fields.purpose, line.fields.outcome])).toEqual([
            ["test.commit", "commit"],
            ["test.rollback", "rollback"],
          ])
          for (const line of logged) {
            expect(line.fields.pid).toBe(process.pid)
            expect(line.fields.durationMs).toBeGreaterThanOrEqual(longHoldMs)
          }
          expect(logged[0].fields.durationMs).toBeLessThanOrEqual(Math.ceil(committedElapsedMs))
          expect(yield* db.values("SELECT id FROM hold_test ORDER BY id")).toEqual([[1], [3]])
        }),
      ),
    ))

  test("names an unlabelled hold by its span and keeps nested savepoints inside the outer hold", () =>
    run(
      captured((lines) =>
        Effect.gen(function* () {
          const tmp = yield* Effect.acquireRelease(
            Effect.promise(() => tmpdir()),
            (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
          )
          const db = yield* productionDatabase(path.join(tmp.path, "nested.sqlite"))
          yield* db.run("CREATE TABLE hold_test (id INTEGER PRIMARY KEY)")

          // The savepoint alone outlasts the threshold, so only its exclusion keeps it from logging a hold of its own.
          const stepMs = 150
          yield* db
            .transaction(
              (tx) =>
                Effect.gen(function* () {
                  yield* Effect.sleep(`${stepMs} millis`)
                  // A nested immediate transaction, as a durable-event commit inside an open one makes, is a savepoint.
                  yield* db.transaction(
                    (inner) =>
                      inner
                        .run("INSERT INTO hold_test (id) VALUES (1)")
                        .pipe(Effect.andThen(Effect.sleep(`${longHoldMs} millis`))),
                    { behavior: "immediate" },
                  )
                  yield* Effect.sleep(`${stepMs} millis`)
                }),
              { behavior: "immediate" },
            )
            .pipe(Effect.withSpan("test.nestedCaller"))

          const logged = holds(lines)
          expect(logged).toHaveLength(1)
          expect(logged[0].fields).toMatchObject({ purpose: "test.nestedCaller", span: "test.nestedCaller", outcome: "commit" })
          expect(logged[0].fields.durationMs).toBeGreaterThanOrEqual(stepMs * 2 + longHoldMs)
        }),
      ),
    ))

  test("attributes a failed commit and its rollback to the same hold", () =>
    run(
      captured((lines) =>
        Effect.gen(function* () {
          const tmp = yield* Effect.acquireRelease(
            Effect.promise(() => tmpdir()),
            (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
          )
          const db = yield* productionDatabase(path.join(tmp.path, "commit-failure.sqlite"))
          yield* db.run("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
          yield* db.run(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
          )

          // The deferred foreign key is checked at COMMIT, which fails and is followed by the rollback.
          const failure = yield* db
            .transaction(
              (tx) =>
                tx
                  .run("INSERT INTO child (id, parent_id) VALUES (1, 99)")
                  .pipe(Effect.andThen(Effect.sleep(`${longHoldMs} millis`))),
              { behavior: "immediate" },
            )
            .pipe(Effect.provideService(TransactionPurpose, "test.failedCommit"), Effect.flip)
          expect(isSqlError(failure) && failure.reason.cause).toMatchObject({ code: "SQLITE_CONSTRAINT_FOREIGNKEY" })

          const logged = holds(lines)
          expect(logged).toHaveLength(1)
          expect(logged[0].fields).toMatchObject({ purpose: "test.failedCommit", outcome: "rollback" })
          expect(logged[0].fields.durationMs).toBeGreaterThanOrEqual(longHoldMs)
          expect(yield* db.values("SELECT COUNT(*) FROM child")).toEqual([[0]])
        }),
      ),
    ))

  test("does not count a long wait for the lock as a hold, nor log a statement that recovers", () =>
    run(
      captured((lines) =>
        Effect.gen(function* () {
          const tmp = yield* Effect.acquireRelease(
            Effect.promise(() => tmpdir()),
            (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
          )
          const filename = path.join(tmp.path, "wait.sqlite")
          const attempts = newStatementAttempts()
          yield* patchBunQuery((sql) => sql === "begin immediate", attempts)
          const db = yield* productionDatabase(filename)
          yield* db.run("CREATE TABLE hold_test (id INTEGER PRIMARY KEY)")
          const release = yield* inProcessHolder(filename)

          const waitStartedAt = performance.now()
          const writer = yield* db
            .transaction((tx) => tx.run("INSERT INTO hold_test (id) VALUES (1)"), { behavior: "immediate" })
            .pipe(Effect.provideService(TransactionPurpose, "test.waiter"), Effect.forkChild({ startImmediately: true }))
          // The BEGIN has met the held lock; keep holding well past the hold threshold before releasing it.
          while (attempts.busy.length === 0 || performance.now() - waitStartedAt < longHoldMs)
            yield* Effect.sleep("10 millis")
          release()
          yield* Fiber.join(writer).pipe(
            Effect.timeoutOrElse({ duration: lockBackstop, orElse: () => Effect.die(new Error("writer never finished")) }),
          )
          const waitedMs = performance.now() - waitStartedAt

          expect(waitedMs).toBeGreaterThan(longHoldMs)
          expect(attempts.busy.length).toBeGreaterThan(0)
          expect(attempts.count).toBeGreaterThan(attempts.busy.length)
          expect(holds(lines)).toEqual([])
          expect(exhausted(lines)).toEqual([])
          expect(unretried(lines)).toEqual([])
          expect(yield* db.values("SELECT id FROM hold_test")).toEqual([[1]])
        }),
      ),
    ))
})

describe("SQLite lock retry exhaustion diagnostics", () => {
  test(
    "summarizes an exhausted BEGIN IMMEDIATE once, with its observed attempts and native code, and logs no hold",
    () =>
      run(
        captured((lines) =>
          Effect.gen(function* () {
            const tmp = yield* Effect.acquireRelease(
              Effect.promise(() => tmpdir()),
              (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
            )
            const filename = path.join(tmp.path, "begin-exhausted.sqlite")
            const attempts = newStatementAttempts()
            yield* patchBunQuery((sql) => sql === "begin immediate", attempts)
            const db = yield* productionDatabase(filename)
            yield* db.run("CREATE TABLE hold_test (id INTEGER PRIMARY KEY)")
            yield* inProcessHolder(filename)

            const startedAt = performance.now()
            yield* db
              .transaction((tx) => tx.run("INSERT INTO hold_test (id) VALUES (1)"), { behavior: "immediate" })
              .pipe(
                Effect.provideService(TransactionPurpose, "test.exhausted"),
                Effect.timeoutOrElse({ duration: lockBackstop, orElse: () => Effect.die(new Error("retries never stopped")) }),
                Effect.flip,
              )
            const elapsedMs = performance.now() - startedAt

            const summaries = exhausted(lines)
            expect(summaries).toHaveLength(1)
            expect(summaries[0].fields).toMatchObject({
              pid: process.pid,
              statement: "begin immediate",
              attempts: attempts.count,
              "sqlite.code": "SQLITE_BUSY",
            })
            expect(attempts.count).toBeGreaterThan(1)
            expect(summaries[0].fields.elapsedMs).toBeGreaterThanOrEqual(5_000)
            expect(summaries[0].fields.elapsedMs).toBeLessThanOrEqual(Math.ceil(elapsedMs))
            expect(holds(lines)).toEqual([])
            expect(unretried(lines)).toEqual([])
          }),
        ),
      ),
    20_000,
  )

  test(
    "keeps attempt counts local to each execution of one reused statement effect",
    () =>
      run(
        captured((lines) =>
          Effect.gen(function* () {
            const tmp = yield* Effect.acquireRelease(
              Effect.promise(() => tmpdir()),
              (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
            )
            const filename = path.join(tmp.path, "reuse.sqlite")
            const writerSql = "INSERT INTO hold_test (id) VALUES (1)"
            const attempts = newStatementAttempts()
            yield* patchBunQuery((sql) => sql === writerSql, attempts)
            const db = yield* productionDatabase(filename)
            yield* db.run("CREATE TABLE hold_test (id INTEGER PRIMARY KEY)")
            yield* inProcessHolder(filename)

            // One statement effect, run twice at once; the connection serializes them, so each meets its own window.
            const write = db.$client.unsafe(writerSql).raw.pipe(Effect.flip)
            yield* Effect.all([write, write], { concurrency: 2 }).pipe(
              Effect.timeoutOrElse({ duration: "20 seconds", orElse: () => Effect.die(new Error("retries never stopped")) }),
            )

            const summaries = exhausted(lines)
            expect(summaries).toHaveLength(2)
            const counts = summaries.map((line) => line.fields.attempts as number)
            expect(counts.every((count) => count > 1)).toBe(true)
            expect(counts[0] + counts[1]).toBe(attempts.count)
            for (const line of summaries) {
              expect(line.fields.statement).toBe("INSERT INTO hold_test")
              expect(line.fields.elapsedMs).toBeLessThan(10_000)
            }
          }),
        ),
      ),
    30_000,
  )

  for (const clientType of ["core-node", "standalone-node"] as const) {
    test.skipIf(!nodeSqliteDrivers)(
      `${clientType} summarizes an exhausted statement once with its full native result code`,
      () =>
        run(
          captured((lines) =>
            Effect.gen(function* () {
              const tmp = yield* Effect.acquireRelease(
                Effect.promise(() => tmpdir()),
                (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
              )
              const filename = path.join(tmp.path, `${clientType}-exhausted.sqlite`)
              const writerSql = "INSERT INTO hold_test (id) VALUES (1)"
              const attempts = newStatementAttempts()
              const client = yield* nodeClientWithAttemptCount({ clientType, filename, statement: writerSql, attempts })
              yield* client.unsafe("CREATE TABLE hold_test (id INTEGER PRIMARY KEY)").raw
              yield* childHolder(filename)

              yield* client.unsafe(writerSql).raw.pipe(
                Effect.timeoutOrElse({ duration: lockBackstop, orElse: () => Effect.die(new Error("retries never stopped")) }),
                Effect.flip,
              )

              const summaries = exhausted(lines)
              expect(summaries).toHaveLength(1)
              expect(summaries[0].fields).toMatchObject({
                statement: "INSERT INTO hold_test",
                attempts: attempts.count,
                "sqlite.code": "ERR_SQLITE_ERROR",
                "sqlite.errcode": 5,
              })
              expect(attempts.count).toBeGreaterThan(1)
              expect(unretried(lines)).toEqual([])
            }),
          ),
        ),
      20_000,
    )
  }

  for (const clientType of ["bun", "core-node", "standalone-node"] as const) {
    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} logs a stale-snapshot write once by its own native code, apart from plain SQLITE_BUSY`,
      () =>
        run(
          captured((lines) =>
            Effect.gen(function* () {
              const tmp = yield* Effect.acquireRelease(
                Effect.promise(() => tmpdir()),
                (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
              )
              const filename = path.join(tmp.path, `${clientType}-snapshot.sqlite`)
              const writerSql = "INSERT INTO hold_test (id) VALUES (1)"
              const attempts = newStatementAttempts()
              const client: SqlClient =
                clientType === "bun"
                  ? yield* patchBunQuery((sql) => sql === writerSql, attempts).pipe(
                      Effect.andThen(productionDatabase(filename)),
                      Effect.map((db) => db.$client),
                    )
                  : yield* nodeClientWithAttemptCount({ clientType, filename, statement: writerSql, attempts })
              yield* client.unsafe("CREATE TABLE hold_test (id INTEGER PRIMARY KEY)").raw
              yield* client.unsafe("PRAGMA journal_mode = WAL").raw
              const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
              const other = new sqlite.Database(filename)
              yield* Effect.addFinalizer(() => Effect.sync(() => other.close()))

              yield* client
                .withTransaction(
                  Effect.gen(function* () {
                    yield* client.unsafe("SELECT COUNT(*) FROM hold_test").values
                    // Another connection commits after this transaction's read, so its snapshot is stale.
                    other.run("INSERT INTO hold_test (id) VALUES (2)")
                    yield* client.unsafe(writerSql).raw
                  }),
                )
                .pipe(
                  Effect.timeoutOrElse({ duration: lockBackstop, orElse: () => Effect.die(new Error("retries never stopped")) }),
                  Effect.flip,
                )

              const summaries = unretried(lines)
              expect(summaries).toHaveLength(1)
              expect(summaries[0].fields).toMatchObject({ statement: "INSERT INTO hold_test", attempts: 1 })
              expect(attempts.count).toBe(1)
              expect(summaries[0].fields).toMatchObject(
                clientType === "bun"
                  ? { "sqlite.code": "SQLITE_BUSY_SNAPSHOT" }
                  : { "sqlite.code": "ERR_SQLITE_ERROR", "sqlite.errcode": 517 },
              )
              expect(exhausted(lines)).toEqual([])
            }),
          ),
        ),
      20_000,
    )
  }
})
