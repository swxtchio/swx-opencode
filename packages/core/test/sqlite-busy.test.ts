import { describe, expect, test } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, Option } from "effect"
import { isSqlError } from "effect/unstable/sql/SqlError"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { Sqlite } from "../src/database/sqlite"
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
const nodeSqliteTest = test.skipIf(!nodeSqliteDrivers)
// The retry window each statement must give a competing writer: the tolerance the old native busy_timeout gave.
const lockWindowMs = 5_000
// Exhaustion may overrun the window by one capped backoff, one native attempt and scheduling delay.
const lockWindowSlackMs = 1_500
// Backstops sit above window plus slack so a statement that never stops fails with its own message.
const lockBackstop = "10 seconds"
// Well past the point where the old four-attempt schedule gave up, and well inside the restored window.
const heldLockMs = 1_500
// The event loop must keep turning while a statement waits; a long synchronous native wait stalls it for seconds.
const stallBoundMs = 1_000

// Ticks continually from now on and returns a reader for the longest gap between ticks. The reader includes the gap
// still open when it is called, so a synchronous call the ticker has not yet woken from still counts.
const eventLoopStalls = Effect.gen(function* () {
  let lastTick = performance.now()
  let longestMs = 0
  yield* Effect.sleep("5 millis").pipe(
    Effect.andThen(
      Effect.sync(() => {
        const now = performance.now()
        longestMs = Math.max(longestMs, now - lastTick)
        lastTick = now
      }),
    ),
    Effect.forever,
    Effect.forkScoped,
  )
  return () => Math.max(longestMs, performance.now() - lastTick)
})

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
  const readCommand = () => Effect.promise(() => new Promise((resolve) => process.stdin.once("data", resolve)))
  const command = String(yield* readCommand()).trim()
  if (command.startsWith("probe ")) {
    yield* Effect.promise(async () => {
      const response = await fetch(command.slice("probe ".length))
      const body = await response.text()
      if (response.status !== 200 || body !== "server-progress") throw new Error("server probe failed")
    })
    const release = new Promise((resolve) => process.stdin.once("data", resolve))
    console.log("probed")
    yield* Effect.promise(() => release)
  }
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
const readCommand = () => new Promise((resolve) => process.stdin.once("data", resolve))
const command = String(await readCommand()).trim()
if (command.startsWith("probe ")) {
  const response = await fetch(command.slice("probe ".length))
  const body = await response.text()
  if (response.status !== 200 || body !== "server-progress") throw new Error("server probe failed")
  const release = new Promise((resolve) => process.stdin.once("data", resolve))
  console.log("probed")
  await release
}
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
  const readCommand = () => Effect.promise(() => new Promise((resolve) => process.stdin.once("data", resolve)))
  const command = String(yield* readCommand()).trim()
  if (command.startsWith("probe ")) {
    yield* Effect.promise(async () => {
      const response = await fetch(command.slice("probe ".length))
      const body = await response.text()
      if (response.status !== 200 || body !== "server-progress") throw new Error("server probe failed")
    })
    const release = new Promise((resolve) => process.stdin.once("data", resolve))
    console.log("probed")
    yield* Effect.promise(() => release)
  }
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
    stderr: "inherit",
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
    probe: async (url: string) => {
      child.stdin.write(`probe ${url}\n`)
      await child.stdin.flush()
      await readLine("probed")
    },
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

const clientWithAttemptCount = (input: {
  clientType: "bun" | "core-node" | "standalone-node"
  filename: string
  statement: string
  attempts: { count: number }
}) =>
  Effect.gen(function* () {
    if (input.clientType === "bun") {
      const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
      const prototype = sqlite.Database.prototype
      const queryDescriptor = Object.getOwnPropertyDescriptor(prototype, "query")
      if (!queryDescriptor) return yield* Effect.die(new Error("bun:sqlite query method was not found"))
      const query = prototype.query
      Object.defineProperty(prototype, "query", {
        ...queryDescriptor,
        value: function (this: InstanceType<typeof sqlite.Database>, sql: string) {
          if (sql === input.statement) input.attempts.count++
          return query.call(this, sql)
        },
      })
      yield* Effect.addFinalizer(() => Effect.sync(() => Object.defineProperty(prototype, "query", queryDescriptor)))
      const context = yield* Layer.build(Database.layerFromPath(input.filename))
      return Context.get(context, Database.Service).db.$client
    }

    const drivers = nodeSqliteDrivers
    if (!drivers) return yield* Effect.fail(new Error("node:sqlite tests ran without node:sqlite"))
    if (input.clientType === "core-node") {
      const context = yield* Layer.build(drivers.core.layer({ filename: input.filename }))
      const native = Context.get(context, Sqlite.Native) as InstanceType<typeof drivers.DatabaseSync>
      const prepare = native.prepare
      Object.defineProperty(native, "prepare", {
        configurable: true,
        writable: true,
        value: function (this: InstanceType<typeof drivers.DatabaseSync>, sql: string) {
          if (sql === input.statement) input.attempts.count++
          return prepare.call(this, sql)
        },
      })
      return Context.get(context, SqlClient)
    }

    const context = yield* Layer.build(drivers.standalone.NodeSqliteClient.layer({ filename: input.filename }))
    const prototype = drivers.DatabaseSync.prototype
    const prepareDescriptor = Object.getOwnPropertyDescriptor(prototype, "prepare")
    if (!prepareDescriptor) return yield* Effect.die(new Error("node:sqlite prepare method was not found"))
    const prepare = prototype.prepare
    Object.defineProperty(prototype, "prepare", {
      ...prepareDescriptor,
      value: function (this: InstanceType<typeof drivers.DatabaseSync>, sql: string) {
        if (sql === input.statement) input.attempts.count++
        return prepare.call(this, sql)
      },
    })
    yield* Effect.addFinalizer(() => Effect.sync(() => Object.defineProperty(prototype, "prepare", prepareDescriptor)))
    return Context.get(context, SqlClient)
  })

describe("SQLite busy timeout and statement retries", () => {
  test("configures the production connection busy timeout", async () => {
    await using tmp = await tmpdir()
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(Database.layerFromPath(path.join(tmp.path, "busy.sqlite")))
          const client = Context.get(context, Database.Service).db.$client

          expect(yield* client.unsafe("PRAGMA busy_timeout").values).toEqual([[5]])
        }),
      ),
    )
  })

  nodeSqliteTest("configures the native busy timeout in both Node SQLite clients", async () => {
    if (!nodeSqliteDrivers) return
    const drivers = nodeSqliteDrivers
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const core = yield* Layer.build(drivers.core.layer({ filename: ":memory:" }))
          const standalone = yield* Layer.build(drivers.standalone.NodeSqliteClient.layer({ filename: ":memory:" }))
          expect(yield* Context.get(core, SqlClient).unsafe("PRAGMA busy_timeout").values).toEqual([[5]])
          expect(yield* Context.get(standalone, SqlClient).unsafe("PRAGMA busy_timeout").values).toEqual([[5]])
        }),
      ),
    )
  })

  for (const clientType of ["bun", "core-node", "standalone-node"] as const) {
    for (const method of ["run", "values"] as const) {
      test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
        `${clientType} ${method} waits out a write lock held past the old 250ms window`,
        async () => {
          await using tmp = await tmpdir()
          const filename = path.join(tmp.path, `${clientType}-held.sqlite`)
          const writerValue = `${clientType}-${method}-writer`
          const writerSql =
            method === "run"
              ? `INSERT INTO busy_retry_test (id, value) VALUES (1, '${writerValue}')`
              : `INSERT INTO busy_retry_test (id, value) VALUES (1, '${writerValue}') RETURNING id`
          const exit = await Effect.runPromiseExit(
            Effect.scoped(
              Effect.gen(function* () {
                const attempts = { count: 0 }
                const client = yield* clientWithAttemptCount({ clientType, filename, statement: writerSql, attempts })
                yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
                expect(yield* client.unsafe("PRAGMA busy_timeout").values).toEqual([[5]])
                const server = Bun.serve({ port: 0, fetch: () => new Response("server-progress") })
                yield* Effect.addFinalizer(() => Effect.sync(() => server.stop(true)))
                const holder = yield* Effect.promise(() =>
                  startLockHolder(filename, clientType === "bun" ? holderScript : nodeHolderScript),
                )
                yield* Effect.addFinalizer(() => Effect.promise(() => holder.release()))

                const longestStallMs = yield* eventLoopStalls
                let writerFinished = false
                const startedAt = performance.now()
                const writer = yield* (
                  method === "run" ? client.unsafe(writerSql).raw : client.unsafe(writerSql).values
                ).pipe(
                  Effect.onExit(() => Effect.sync(() => void (writerFinished = true))),
                  Effect.forkChild({ startImmediately: true }),
                )
                yield* Effect.gen(function* () {
                  while (attempts.count === 0) yield* Effect.sleep("1 millis")
                }).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error("writer did not attempt the held-lock statement")),
                  }),
                )
                // The holder owns the write lock and the writer has met it; keep holding well past the old schedule.
                while (performance.now() - startedAt < heldLockMs) yield* Effect.sleep("10 millis")
                expect(writerFinished).toBe(false)

                yield* Effect.promise(() => holder.probe(server.url.href)).pipe(
                  Effect.timeoutOrElse({
                    duration: "2 seconds",
                    orElse: () =>
                      Effect.fail(new Error("unrelated request did not progress while SQLite was contended")),
                  }),
                )
                expect(longestStallMs()).toBeLessThan(stallBoundMs)
                expect(writerFinished).toBe(false)

                yield* Effect.promise(() => holder.release())
                const result = yield* Fiber.await(writer).pipe(
                  Effect.timeoutOrElse({
                    duration: "2 seconds",
                    orElse: () => Effect.fail(new Error("writer did not finish after the lock was released")),
                  }),
                )
                if (Exit.isFailure(result)) return yield* Effect.fail(new Error(Cause.pretty(result.cause)))
                if (method === "values") expect(result.value).toEqual([[1]])
                expect(
                  yield* client.unsafe(`SELECT COUNT(*) FROM busy_retry_test WHERE value = '${writerValue}'`).values,
                ).toEqual([[1]])
              }),
            ),
          )
          if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
        },
        10_000,
      )
    }
  }

  for (const clientType of ["core", "standalone"] as const) {
    nodeSqliteTest(
      `classifies ${clientType} node:sqlite errcode 5 as retryable SQLITE_BUSY`,
      async () => {
        if (!nodeSqliteDrivers) return
        const drivers = nodeSqliteDrivers
        await using tmp = await tmpdir()
        const filename = path.join(tmp.path, "node-busy.sqlite")
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Effect.gen(function* () {
              const context =
                clientType === "core"
                  ? yield* Layer.build(drivers.core.layer({ filename }))
                  : yield* Layer.build(drivers.standalone.NodeSqliteClient.layer({ filename }))
              const client = Context.get(context, SqlClient)
              yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
              yield* client.unsafe("PRAGMA busy_timeout = 0").raw
              const holder = yield* Effect.promise(() => startLockHolder(filename, nodeHolderScript))

              yield* Effect.gen(function* () {
                let failure: unknown
                const write = client
                  .unsafe("INSERT INTO busy_retry_test (id, value) VALUES (1, 'node-writer')")
                  .raw.pipe(
                    Effect.timeoutOrElse({
                      duration: lockBackstop,
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
      },
      20_000,
    )
  }

  test("exhausts an immediate transaction begin into a direct SqlError", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "immediate-busy.sqlite")
    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(Database.layerFromPath(filename))
          const database = Context.get(context, Database.Service).db
          yield* database.run("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
          yield* database.$client.unsafe("PRAGMA busy_timeout = 0").raw
          const holder = yield* Effect.promise(() => startLockHolder(filename))

          yield* Effect.gen(function* () {
            let failure: unknown
            const write = database
              .transaction((tx) => tx.run("INSERT INTO busy_retry_test (id, value) VALUES (1, 'transaction-writer')"), {
                behavior: "immediate",
              })
              .pipe(
                Effect.timeoutOrElse({
                  duration: lockBackstop,
                  orElse: () => Effect.fail(new Error("immediate transaction retries did not stop")),
                }),
              )
            yield* write.pipe(Effect.catch((error) => Effect.sync(() => (failure = error))))
            expect(isSqlError(failure)).toBe(true)
            if (!isSqlError(failure)) return
            expect(failure).not.toBeInstanceOf(EffectDrizzleQueryError)
            expect(failure.reason._tag).toBe("LockTimeoutError")
            expect(failure.reason.cause).toMatchObject({ code: "SQLITE_BUSY", message: "database is locked" })
            expect(failure.message).toContain("SQLITE_BUSY")
          }).pipe(Effect.ensuring(Effect.promise(() => holder.release())))
        }),
      ),
    )
    if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
  }, 20_000)

  for (const clientType of ["bun", "core-node", "standalone-node"] as const) {
    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} serves unrelated requests and exhausts a persistent lock after the retry window`,
      async () => {
        const drivers = nodeSqliteDrivers
        if (clientType !== "bun" && !drivers) return
        await using tmp = await tmpdir()
        const filename = path.join(tmp.path, `${clientType}-persistent.sqlite`)
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Effect.gen(function* () {
              const client: SqlClient = yield* Effect.gen(function* () {
                if (clientType === "bun") {
                  const context = yield* Layer.build(Database.layerFromPath(filename))
                  return Context.get(context, Database.Service).db.$client
                }
                if (!drivers) return yield* Effect.fail(new Error("node:sqlite tests ran without node:sqlite"))
                if (clientType === "core-node") {
                  const context = yield* Layer.build(drivers.core.layer({ filename }))
                  return Context.get(context, SqlClient)
                }
                const context = yield* Layer.build(drivers.standalone.NodeSqliteClient.layer({ filename }))
                return Context.get(context, SqlClient)
              })
              yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
              expect(yield* client.unsafe("PRAGMA busy_timeout").values).toEqual([[5]])
              const server = Bun.serve({ port: 0, fetch: () => new Response("server-progress") })
              yield* Effect.addFinalizer(() => Effect.sync(() => server.stop(true)))
              const holder = yield* Effect.promise(() =>
                startLockHolder(filename, clientType === "bun" ? holderScript : nodeHolderScript),
              )
              yield* Effect.addFinalizer(() => Effect.promise(() => holder.release()))

              const longestStallMs = yield* eventLoopStalls
              let writerFinished = false
              let elapsedMs = 0
              const startedAt = performance.now()
              const writer = yield* client
                .unsafe("INSERT INTO busy_retry_test (id, value) VALUES (1, 'persistent-writer')")
                .raw.pipe(
                  Effect.onExit(() =>
                    Effect.sync(() => {
                      writerFinished = true
                      elapsedMs = performance.now() - startedAt
                    }),
                  ),
                  Effect.forkChild({ startImmediately: true }),
                )
              yield* Effect.promise(() => holder.probe(server.url.href)).pipe(
                Effect.timeoutOrElse({
                  duration: "2 seconds",
                  orElse: () => Effect.fail(new Error("unrelated request did not progress while SQLite was contended")),
                }),
              )
              expect(writerFinished).toBe(false)
              const result = yield* Fiber.await(writer).pipe(
                Effect.timeoutOrElse({
                  duration: lockBackstop,
                  orElse: () => Effect.fail(new Error("persistent-lock retries did not stop")),
                }),
              )
              expect(writerFinished).toBe(true)
              expect(Exit.isFailure(result)).toBe(true)
              expect(elapsedMs).toBeGreaterThanOrEqual(lockWindowMs)
              expect(elapsedMs).toBeLessThan(lockWindowMs + lockWindowSlackMs)
              expect(longestStallMs()).toBeLessThan(stallBoundMs)
              if (!Exit.isFailure(result)) return
              const error = Option.getOrUndefined(Cause.findErrorOption(result.cause))
              expect(isSqlError(error)).toBe(true)
              if (isSqlError(error)) {
                expect(error.reason._tag).toBe("LockTimeoutError")
                expect(error.message).toContain("database is locked")
              }
              yield* Effect.promise(() => holder.release())
              expect(
                yield* client.unsafe("SELECT COUNT(*) FROM busy_retry_test WHERE value = 'persistent-writer'").values,
              ).toEqual([[0]])
              yield* client.unsafe("INSERT INTO busy_retry_test (id, value) VALUES (2, 'independent-writer')").raw
              expect(yield* client.unsafe("SELECT id, value FROM busy_retry_test ORDER BY id").values).toEqual([
                [2, "independent-writer"],
                [100, "holder"],
              ])
            }),
          ),
        )
        if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
      },
      20_000,
    )
  }

  for (const clientType of ["bun", "core-node"] as const) {
    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} holds the connection permit across retry backoff before a transaction commits`,
      async () => {
        const drivers = nodeSqliteDrivers
        if (clientType !== "bun" && !drivers) return
        await using tmp = await tmpdir()
        const filename = path.join(tmp.path, "busy.sqlite")
        await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              // The main fiber sees this counter only after the synchronous native call returns,
              // so the holder is released after an attempt rather than an elapsed-time guess.
              const writerSql = "INSERT INTO busy_retry_test (id, value) VALUES (1, 'writer')"
              let statementAttempts = 0
              const client: SqlClient = yield* Effect.gen(function* () {
                if (clientType === "bun") {
                  const bun = yield* Effect.promise(() => import("../src/database/sqlite.bun"))
                  const context = yield* Layer.build(bun.layer({ filename }))
                  const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
                  const native = Context.get(context, Sqlite.Native) as InstanceType<typeof sqlite.Database>
                  const query = native.query
                  Object.defineProperty(native, "query", {
                    configurable: true,
                    writable: true,
                    value: (sql: string) => {
                      if (sql === writerSql) statementAttempts++
                      return query.call(native, sql)
                    },
                  })
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => Object.defineProperty(native, "query", { configurable: true, value: query })),
                  )
                  return Context.get(context, SqlClient)
                }
                if (!drivers) return yield* Effect.fail(new Error("node:sqlite tests ran without node:sqlite"))
                if (clientType === "core-node") {
                  const context = yield* Layer.build(drivers.core.layer({ filename }))
                  const native = Context.get(context, Sqlite.Native) as InstanceType<typeof drivers.DatabaseSync>
                  const prepare = native.prepare
                  Object.defineProperty(native, "prepare", {
                    configurable: true,
                    writable: true,
                    value: function (this: InstanceType<typeof drivers.DatabaseSync>, sql: string) {
                      if (sql === writerSql) statementAttempts++
                      return prepare.call(this, sql)
                    },
                  })
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => Object.defineProperty(native, "prepare", { configurable: true, value: prepare })),
                  )
                  return Context.get(context, SqlClient)
                }
                return yield* Effect.fail(new Error("unsupported SQLite client type"))
              })
              yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
              yield* client.unsafe("PRAGMA busy_timeout = 0").raw
              const holder = yield* Effect.promise(() =>
                startLockHolder(filename, clientType === "bun" ? bunHolderScript : nodeHolderScript),
              )
              const order: string[] = []
              const transactionRelease = yield* Deferred.make<void>()

              yield* Effect.gen(function* () {
                let writerFinished = false
                const writer = yield* client.unsafe(writerSql).raw.pipe(
                  Effect.tap(() => Effect.sync(() => order.push("writer-finished"))),
                  Effect.onExit(() => Effect.sync(() => void (writerFinished = true))),
                  Effect.forkChild({ startImmediately: true }),
                )
                yield* Effect.gen(function* () {
                  while (statementAttempts === 0) yield* Effect.sleep("1 millis")
                }).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error("writer did not attempt the held-lock statement")),
                  }),
                )
                expect(writerFinished).toBe(false)

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
                const writerExit = yield* Fiber.await(writer).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () =>
                      Effect.fail(
                        new Error(
                          `writer stayed blocked behind the waiting transaction; attempts=${statementAttempts}; order=${order.join(",")}`,
                        ),
                      ),
                  }),
                )
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
      },
      10_000,
    )
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
              const startedAt = performance.now()
              if (method === "run") {
                yield* database.run("INSERT INTO busy_retry_test (id, value) VALUES (1, 'writer')").pipe(
                  Effect.timeoutOrElse({
                    duration: lockBackstop,
                    orElse: () => Effect.fail(new Error("retry did not stop")),
                  }),
                  Effect.catch((error) => Effect.sync(() => (failure = error))),
                )
              } else {
                yield* database
                  .values("INSERT INTO busy_retry_test (id, value) VALUES (1, 'writer') RETURNING id")
                  .pipe(
                    Effect.timeoutOrElse({
                      duration: lockBackstop,
                      orElse: () => Effect.fail(new Error("retry did not stop")),
                    }),
                    Effect.catch((error) => Effect.sync(() => (failure = error))),
                  )
              }
              expect(failure).toBeInstanceOf(EffectDrizzleQueryError)
              if (!(failure instanceof EffectDrizzleQueryError)) return
              const elapsedMs = performance.now() - startedAt
              expect(elapsedMs).toBeGreaterThanOrEqual(lockWindowMs)
              expect(elapsedMs).toBeLessThan(lockWindowMs + lockWindowSlackMs)
              expect(attempts.lock).toBeGreaterThan(1)
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
    }, 20_000)
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
