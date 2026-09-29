import { describe, expect, test } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { NodeSqliteClient } from "@opencode-ai/effect-sqlite-node"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, Option } from "effect"
import { isSqlError } from "effect/unstable/sql/SqlError"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { layer } from "../src/database/sqlite.node"
import { Sqlite } from "../src/database/sqlite"
import path from "path"
import { tmpdir } from "./fixture/tmpdir"

const sqliteError = (error: unknown) => {
  if (isSqlError(error)) return error
  if (!(error instanceof EffectDrizzleQueryError) || !Cause.isCause(error.cause)) return
  const failure = Option.getOrUndefined(Cause.findErrorOption(error.cause))
  return isSqlError(failure) ? failure : undefined
}

const holderScript = `
import { Context, Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"

await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const context = yield* Layer.build(Database.layerFromPath(process.env.SQLITE_BUSY_DB!))
  const client = Context.get(context, Database.Service).db.$client
  yield* client.unsafe("CREATE TABLE IF NOT EXISTS busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
  yield* client.unsafe("BEGIN IMMEDIATE").raw
  yield* client.unsafe("INSERT INTO busy_retry_test (id, value) VALUES (100, 'holder')").raw
  console.log("ready")
  yield* Effect.promise(() => new Promise((resolve) => process.stdin.once("data", resolve)))
  yield* client.unsafe("COMMIT").raw
  console.log("released")
})))
`

const bunHolderScript = `
import { Database } from "bun:sqlite"

const native = new Database(process.env.SQLITE_BUSY_DB!)
native.run("PRAGMA journal_mode = WAL")
native.run("BEGIN IMMEDIATE")
native.run("INSERT INTO busy_retry_test (id, value) VALUES (100, 'holder')")
console.log("ready")
await new Promise((resolve) => process.stdin.once("data", resolve))
native.run("COMMIT")
console.log("released")
native.close()
`

const nodeHolderScript = `
import { Context, Effect, Layer } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { layer } from "../src/database/sqlite.node"

await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const context = yield* Layer.build(layer({ filename: process.env.SQLITE_BUSY_DB! }))
  const client = Context.get(context, SqlClient)
  yield* client.unsafe("BEGIN IMMEDIATE").raw
  yield* client.unsafe("INSERT INTO busy_retry_test (id, value) VALUES (100, 'holder')").raw
  console.log("ready")
  yield* Effect.promise(() => new Promise((resolve) => process.stdin.once("data", resolve)))
  yield* client.unsafe("COMMIT").raw
  console.log("released")
})))
`

async function startLockHolder(filename: string, script = holderScript) {
  const child = Bun.spawn(["bun", "-e", script], {
    cwd: import.meta.dir,
    env: { ...process.env, SQLITE_BUSY_DB: filename },
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  })
  const reader = child.stdout.getReader()
  let released = false
  const readLine = async (expected: string) => {
    const decoder = new TextDecoder()
    let output = ""
    while (!output.includes(`${expected}\n`)) {
      const chunk = await reader.read()
      if (chunk.done) throw new Error(`SQLite holder exited before ${expected}: ${output}`)
      output += decoder.decode(chunk.value)
    }
  }
  await readLine("ready")
  return {
    release: async () => {
      if (released) return
      released = true
      child.stdin.write("release\n")
      child.stdin.end()
      await readLine("released")
      await child.exited
    },
  }
}

describe("SQLite busy timeout and statement retries", () => {
  test("configures the production connection busy timeout", async () => {
    await using tmp = await tmpdir()
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(Database.layerFromPath(path.join(tmp.path, "busy.sqlite")))
          const client = Context.get(context, Database.Service).db.$client

          expect(yield* client.unsafe("PRAGMA busy_timeout").values).toEqual([[30_000]])
        }),
      ),
    )
  })

  test("configures the native busy timeout in both Node SQLite clients", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const core = yield* Layer.build(layer({ filename: ":memory:" }))
          const standalone = yield* Layer.build(NodeSqliteClient.layer({ filename: ":memory:" }))
          expect(yield* Context.get(core, SqlClient).unsafe("PRAGMA busy_timeout").values).toEqual([[30_000]])
          expect(yield* Context.get(standalone, SqlClient).unsafe("PRAGMA busy_timeout").values).toEqual([[30_000]])
        }),
      ),
    )
  })

  for (const clientType of ["core", "standalone"] as const) {
    for (const method of ["run", "values"] as const) {
      test(`node:sqlite ${clientType} retries ${method} after a second process releases its write lock`, async () => {
        await using tmp = await tmpdir()
        const filename = path.join(tmp.path, "node-busy.sqlite")
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Effect.gen(function* () {
              const context =
                clientType === "core"
                  ? yield* Layer.build(layer({ filename }))
                  : yield* Layer.build(NodeSqliteClient.layer({ filename }))
              const client = Context.get(context, SqlClient)
              yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
              yield* client.unsafe("PRAGMA busy_timeout = 0").raw

              const holder = yield* Effect.promise(() => startLockHolder(filename, nodeHolderScript))
              yield* Effect.gen(function* () {
                const started = yield* Deferred.make<void>()
                const statement =
                  method === "run"
                    ? client.unsafe("INSERT INTO busy_retry_test (id, value) VALUES (1, 'node-writer')").raw
                    : client.unsafe("INSERT INTO busy_retry_test (id, value) VALUES (1, 'node-writer') RETURNING id")
                        .values
                const writer = yield* Deferred.succeed(started, undefined).pipe(
                  Effect.andThen(statement),
                  Effect.forkChild({ startImmediately: true }),
                )

                yield* Deferred.await(started)
                yield* Effect.promise(() => holder.release())
                const result = yield* Fiber.await(writer)
                expect(Exit.isSuccess(result)).toBe(true)
                if (!Exit.isSuccess(result)) return
                if (method === "values") expect(result.value).toEqual([[1]])
                expect(
                  yield* client.unsafe("SELECT COUNT(*) FROM busy_retry_test WHERE value = 'node-writer'").values,
                ).toEqual([[1]])
              }).pipe(Effect.ensuring(Effect.promise(() => holder.release())))
            }),
          ),
        )
        expect(Exit.isSuccess(exit)).toBe(true)
      }, 10_000)
    }
  }

  for (const clientType of ["core", "standalone"] as const) {
    test(`classifies ${clientType} node:sqlite errcode 5 as retryable SQLITE_BUSY`, async () => {
      await using tmp = await tmpdir()
      const filename = path.join(tmp.path, "node-busy.sqlite")
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const context =
              clientType === "core"
                ? yield* Layer.build(layer({ filename }))
                : yield* Layer.build(NodeSqliteClient.layer({ filename }))
            const client = Context.get(context, SqlClient)
            yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
            yield* client.unsafe("PRAGMA busy_timeout = 0").raw
            const holder = yield* Effect.promise(() => startLockHolder(filename, nodeHolderScript))

            yield* Effect.gen(function* () {
              let failure: unknown
              const write = client.unsafe("INSERT INTO busy_retry_test (id, value) VALUES (1, 'node-writer')").raw.pipe(
                Effect.timeoutOrElse({
                  duration: "5 seconds",
                  orElse: () => Effect.fail(new Error("node retry did not stop")),
                }),
              )
              yield* write.pipe(Effect.catch((error) => Effect.sync(() => (failure = error))))
              expect(isSqlError(failure)).toBe(true)
              if (!isSqlError(failure)) return
              expect(failure.reason._tag).toBe("LockTimeoutError")
              expect(failure.reason.cause).toMatchObject({ code: "ERR_SQLITE_ERROR", errcode: 5 })
              expect(failure.message).toContain("SQLITE_BUSY")
            }).pipe(Effect.ensuring(Effect.promise(() => holder.release())))
          }),
        ),
      )
      expect(Exit.isSuccess(exit)).toBe(true)
    }, 10_000)
  }

  for (const method of ["run", "values"] as const) {
    test(`retries ${method} after a second process releases its write lock`, async () => {
      await using tmp = await tmpdir()
      const filename = path.join(tmp.path, "busy.sqlite")
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(Database.layerFromPath(filename))
            const database = Context.get(context, Database.Service).db
            const client = database.$client
            yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
            yield* client.unsafe("PRAGMA busy_timeout = 0").raw

            const holder = yield* Effect.promise(() => startLockHolder(filename))
            yield* Effect.gen(function* () {
              const started = yield* Deferred.make<void>()
              const writer = yield* Effect.gen(function* () {
                yield* Deferred.succeed(started, undefined)
                if (method === "run")
                  return yield* database.run("INSERT INTO busy_retry_test (id, value) VALUES (1, 'writer')")
                return yield* database.values(
                  "INSERT INTO busy_retry_test (id, value) VALUES (1, 'writer') RETURNING id",
                )
              }).pipe(Effect.forkChild)

              yield* Deferred.await(started)
              yield* Effect.promise(() => holder.release())
              const result = yield* Fiber.await(writer)
              expect(Exit.isSuccess(result)).toBe(true)
              if (!Exit.isSuccess(result)) return
              if (method === "values") expect(result.value).toEqual([[1]])
              expect(yield* database.values("SELECT COUNT(*) FROM busy_retry_test WHERE value = 'writer'")).toEqual([
                [1],
              ])
            }).pipe(Effect.ensuring(Effect.promise(() => holder.release())))
          }),
        ),
      )
      expect(Exit.isSuccess(exit)).toBe(true)
    }, 10_000)
  }

  for (const clientType of ["bun", "core-node", "standalone-node"] as const) {
    test(`${clientType} holds the connection permit across retry backoff before a transaction commits`, async () => {
      await using tmp = await tmpdir()
      const filename = path.join(tmp.path, "busy.sqlite")
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const client: SqlClient = yield* Effect.gen(function* () {
              if (clientType === "bun") {
                const context = yield* Layer.build(Database.layerFromPath(filename))
                return Context.get(context, Database.Service).db.$client
              }
              if (clientType === "core-node") {
                const context = yield* Layer.build(layer({ filename }))
                return Context.get(context, SqlClient)
              }
              const context = yield* Layer.build(NodeSqliteClient.layer({ filename }))
              return Context.get(context, SqlClient)
            })
            yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
            yield* client.unsafe("PRAGMA busy_timeout = 0").raw
            const holder = yield* Effect.promise(() =>
              startLockHolder(filename, clientType === "bun" ? holderScript : nodeHolderScript),
            )
            const order: string[] = []
            const transactionRelease = yield* Deferred.make<void>()

            yield* Effect.gen(function* () {
              const writerStarted = yield* Deferred.make<void>()
              const writer = yield* Effect.gen(function* () {
                yield* Deferred.succeed(writerStarted, undefined)
                yield* client.unsafe("INSERT INTO busy_retry_test (id, value) VALUES (1, 'writer')").raw
                order.push("writer-finished")
              }).pipe(Effect.forkChild({ startImmediately: true }))
              yield* Deferred.await(writerStarted)

              const transaction = yield* client
                .withTransaction(
                  Effect.gen(function* () {
                    order.push("transaction-started")
                    yield* Deferred.await(transactionRelease)
                    yield* client.unsafe("INSERT INTO busy_retry_test (id, value) VALUES (2, 'transaction')").raw
                    order.push("transaction-committed")
                  }),
                )
                .pipe(Effect.forkChild({ startImmediately: true }))

              yield* Effect.promise(() => holder.release())
              const writerExit = yield* Fiber.await(writer)
              expect(Exit.isSuccess(writerExit)).toBe(true)
              yield* Deferred.succeed(transactionRelease, undefined)
              const transactionExit = yield* Fiber.await(transaction)
              expect(Exit.isSuccess(transactionExit)).toBe(true)
              expect(order).toEqual(["writer-finished", "transaction-started", "transaction-committed"])
              expect(yield* client.unsafe("SELECT COUNT(*) FROM busy_retry_test").values).toEqual([[3]])
            }).pipe(
              Effect.ensuring(Deferred.succeed(transactionRelease, undefined)),
              Effect.ensuring(Effect.promise(() => holder.release())),
            )
          }),
        ),
      )
    }, 10_000)
  }

  for (const method of ["run", "values"] as const) {
    test(`exhausts retryable ${method} locks and preserves a nonretryable partial write cause`, async () => {
      await using tmp = await tmpdir()
      const sqlite = await import("bun:sqlite")
      const bun = await import("../src/database/sqlite.bun")
      const filename = path.join(tmp.path, "busy.sqlite")
      const exit = await Effect.runPromiseExit(
        Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(bun.layer({ filename }))
            const database = yield* EffectDrizzleSqlite.makeWithDefaults().pipe(Effect.provide(context))
            const client = database.$client
            yield* database.run(
              "CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL CHECK (value <> 'bad'))",
            )
            yield* client.unsafe("PRAGMA busy_timeout = 0").raw
            const native = Context.get(context, Sqlite.Native) as InstanceType<typeof sqlite.Database>
            const attempts = { lock: 0, partial: 0 }
            const query = native.query.bind(native)
            Object.defineProperty(native, "query", {
              configurable: true,
              value: (sql: string) => {
                const statement = query(sql)
                const counted =
                  sql.includes("busy_retry_test") && sql.includes("'writer'")
                    ? "lock"
                    : sql.includes("busy_retry_test") && sql.includes("'good'")
                      ? "partial"
                      : undefined
                if (!counted) return statement
                if (method === "run") {
                  const all = statement.native.all
                  statement.native.all = (...params: Parameters<typeof all>) => {
                    attempts[counted] += 1
                    return all.apply(statement.native, params)
                  }
                } else {
                  const values = statement.native.values
                  statement.native.values = (...params: Parameters<typeof values>) => {
                    attempts[counted] += 1
                    return values.apply(statement.native, params)
                  }
                }
                return statement
              },
            })
            const holder = yield* Effect.promise(() => startLockHolder(filename, bunHolderScript))

            yield* Effect.gen(function* () {
              let failure: unknown
              if (method === "run") {
                yield* database.run("INSERT INTO busy_retry_test (id, value) VALUES (1, 'writer')").pipe(
                  Effect.timeoutOrElse({
                    duration: "5 seconds",
                    orElse: () => Effect.fail(new Error("retry did not stop")),
                  }),
                  Effect.catch((error) => Effect.sync(() => (failure = error))),
                )
              } else {
                yield* database
                  .values("INSERT INTO busy_retry_test (id, value) VALUES (1, 'writer') RETURNING id")
                  .pipe(
                    Effect.timeoutOrElse({
                      duration: "5 seconds",
                      orElse: () => Effect.fail(new Error("retry did not stop")),
                    }),
                    Effect.catch((error) => Effect.sync(() => (failure = error))),
                  )
              }
              expect(failure).toBeInstanceOf(EffectDrizzleQueryError)
              if (!(failure instanceof EffectDrizzleQueryError)) return
              expect(attempts.lock).toBe(5)
              expect(failure.query).toBe("Database is locked (SQLITE_BUSY)")
              expect(failure.params).toEqual([])
              expect(failure.message).toContain("Database is locked (SQLITE_BUSY)")
              expect(failure.message).not.toContain("INSERT INTO")
              const error = sqliteError(failure)
              expect(isSqlError(error)).toBe(true)
              if (!isSqlError(error)) return
              expect(error.reason._tag).toBe("LockTimeoutError")
              expect(error.reason.cause).toMatchObject({ code: "SQLITE_BUSY", message: "database is locked" })
              expect(error.message).toContain("database is locked")
              expect(error.message).toContain("SQLITE_BUSY")

              yield* Effect.promise(() => holder.release())
              const constraintFailure = yield* Effect.gen(function* () {
                if (method === "run")
                  return yield* Effect.flip(
                    database.run("INSERT OR FAIL INTO busy_retry_test (id, value) VALUES (1, 'good'), (2, 'bad')"),
                  )
                return yield* Effect.flip(
                  database.values(
                    "INSERT OR FAIL INTO busy_retry_test (id, value) VALUES (1, 'good'), (2, 'bad') RETURNING id",
                  ),
                )
              })
              expect(constraintFailure).toBeInstanceOf(EffectDrizzleQueryError)
              const constraint = sqliteError(constraintFailure)
              expect(isSqlError(constraint)).toBe(true)
              if (!isSqlError(constraint)) return
              expect(constraint.reason._tag).toBe("ConstraintError")
              expect(constraint.reason.isRetryable).toBe(false)
              expect(constraint.reason.cause).toMatchObject({ code: "SQLITE_CONSTRAINT_CHECK" })
              expect(attempts.partial).toBe(1)
              expect(yield* database.values("SELECT id, value FROM busy_retry_test ORDER BY id")).toEqual([
                [1, "good"],
                [100, "holder"],
              ])
            }).pipe(Effect.ensuring(Effect.promise(() => holder.release())))
          }),
        ),
      )
      if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
    }, 10_000)
  }

  for (const method of ["run", "values"] as const) {
    test(`does not retry a nonretryable partial-write failure through ${method}`, async () => {
      await using tmp = await tmpdir()
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(Database.layerFromPath(path.join(tmp.path, "busy.sqlite")))
            const database = Context.get(context, Database.Service).db
            yield* database.run(
              "CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL CHECK (value <> 'bad'))",
            )

            const failure = yield* Effect.gen(function* () {
              if (method === "run")
                return yield* Effect.flip(
                  database.run("INSERT OR FAIL INTO busy_retry_test VALUES (1, 'good'), (2, 'bad')"),
                )
              return yield* Effect.flip(
                database.values("INSERT OR FAIL INTO busy_retry_test VALUES (1, 'good'), (2, 'bad') RETURNING id"),
              )
            })
            expect(failure).toBeInstanceOf(EffectDrizzleQueryError)
            const error = sqliteError(failure)
            expect(isSqlError(error)).toBe(true)
            if (!isSqlError(error)) return
            expect(error.reason._tag).toBe("ConstraintError")
            expect(error.reason.isRetryable).toBe(false)
            expect(error.reason.cause).toMatchObject({ code: "SQLITE_CONSTRAINT_CHECK" })
            expect(yield* database.values("SELECT id, value FROM busy_retry_test ORDER BY id")).toEqual([[1, "good"]])
          }),
        ),
      )
    })
  }
})
