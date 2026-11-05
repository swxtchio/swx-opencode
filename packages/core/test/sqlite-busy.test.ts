import { describe, expect, test } from "bun:test"
import { Database } from "../src/database/database"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { Cause, Context, Deferred, Duration, Effect, Exit, Fiber, Layer, Option, References, Scope } from "effect"
import { isSqlError } from "effect/unstable/sql/SqlError"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { Sqlite } from "../src/database/sqlite"
import { layer } from "../src/database/sqlite.bun"
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
    standalone: await import("../../effect-sqlite-node/src/index"),
    DatabaseSync: nodeSqlite.DatabaseSync,
  }),
  () => undefined,
)
const nodeSqliteTest = test.skipIf(!nodeSqliteDrivers)

test(
  "a retry-priority grant race never loses the connection permit",
  async () => {
    const bun = Bun.which("bun")
    if (!bun) throw new Error("Bun is required for the connection-lease handoff regression")
    const script = path.join(import.meta.dir, "fixture/sqlite-lease-grant-race.mjs")
    const child = Bun.spawn([bun, script, "bun"], {
      cwd: path.resolve(import.meta.dir, ".."),
      stdout: "pipe",
      stderr: "pipe",
    })
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ])
    if (exitCode !== 0) throw new Error(`Bun connection-lease handoff sweep failed (${exitCode})\n${stdout}\n${stderr}`)
    for (const maxOps of [3, 4, 8, 16, 64]) expect(stdout).toContain(`PASS bun lease grant race maxOps=${maxOps}`)
  },
  30_000,
)

// The retry window each statement must give a competing writer.
const lockWindowMs = 10_000
// Exhaustion may overrun the window by one capped backoff, one native attempt and scheduling delay.
const lockWindowSlackMs = 1_500
// Backstops sit above window plus slack so a statement that never stops fails with its own message.
const lockBackstop = "15 seconds"
// Bun test timeouts for a test that exhausts the window, above the backstop.
const exhaustionTestMs = 30_000
// Well past the point where the old four-attempt schedule gave up, and well inside the window.
const heldLockMs = 1_500
// Past the old 5 second window and inside the current one, with room for the release to reach the writer.
const longHeldLockMs = 9_500
// The event loop must keep turning while a statement waits; a long synchronous native wait stalls it for seconds.
const stallBoundMs = 1_000
// A stale snapshot can never succeed on retry, so its write must fail far inside the retry window.
const snapshotBoundMs = 1_000

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
  attempts: StatementAttempts
}) =>
  Effect.gen(function* () {
    if (input.clientType === "bun") {
      yield* patchBunQuery((sql) => sql === input.statement, input.attempts)
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
          const statement = prepare.call(this, sql)
          return sql === input.statement ? countStatement(statement, input.attempts) : statement
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
        const statement = prepare.call(this, sql)
        return sql === input.statement ? countStatement(statement, input.attempts) : statement
      },
    })
    yield* Effect.addFinalizer(() => Effect.sync(() => Object.defineProperty(prototype, "prepare", prepareDescriptor)))
    return Context.get(context, SqlClient)
  })

type SqliteClientType = "bun" | "core-node" | "standalone-node"

const sqliteClientForPermitTest = (clientType: SqliteClientType) =>
  Effect.gen(function* () {
    if (clientType === "bun") {
      const context = yield* Layer.build(layer({ filename: ":memory:", disableWAL: true }))
      return Context.get(context, SqlClient)
    }

    const drivers = nodeSqliteDrivers
    if (!drivers) return yield* Effect.fail(new Error("node:sqlite tests ran without node:sqlite"))
    if (clientType === "core-node") {
      const context = yield* Layer.build(drivers.core.layer({ filename: ":memory:" }))
      return Context.get(context, SqlClient)
    }

    const context = yield* Layer.build(drivers.standalone.NodeSqliteClient.layer({ filename: ":memory:" }))
    return Context.get(context, SqlClient)
  })

const hasLoadExtension = (
  client: SqlClient,
): client is SqlClient & { readonly loadExtension: (path: string) => Effect.Effect<void, unknown> } =>
  "loadExtension" in client && typeof client.loadExtension === "function"

const hasExport = (
  client: SqlClient,
): client is SqlClient & { readonly export: Effect.Effect<Uint8Array, unknown> } =>
  "export" in client && Effect.isEffect(client.export)

const queuedAcquisition = (client: SqlClient, kind: "reserve" | "transaction") =>
  (kind === "reserve"
    ? Effect.scoped(Effect.andThen(client.reserve, Effect.yieldNow))
    : client.withTransaction(client.unsafe("SELECT 2").raw)
  ).pipe(Effect.provideService(References.MaxOpsBeforeYield, 3))

describe("SQLite permit acquisition interruption", () => {
  for (const clientType of ["bun", "core-node", "standalone-node"] as const) {
    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} proves reserve handoff and preserves withTransaction scope on interruption`,
      async () => {
        await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const client: SqlClient = yield* sqliteClientForPermitTest(clientType)

              // Reserve is the acquisition proof; the transaction arm preserves SqlClient's masked acquireConnection path.
              for (const kind of ["reserve", "transaction"] as const) {
                for (let delay = 0; delay < 32; delay++) {
                  const holderReady = yield* Deferred.make<void>()
                  const holderRelease = yield* Deferred.make<void>()
                  const holder = yield* Effect.scoped(
                    Effect.gen(function* () {
                      yield* client.reserve
                      yield* Deferred.succeed(holderReady, undefined)
                      yield* Deferred.await(holderRelease)
                    }),
                  ).pipe(Effect.forkChild({ startImmediately: true }))

                  yield* Effect.gen(function* () {
                    yield* Deferred.await(holderReady).pipe(
                      Effect.timeoutOrElse({
                        duration: "1 second",
                        orElse: () => Effect.fail(new Error(`${clientType} holder did not acquire the permit`)),
                      }),
                    )
                    const queued = yield* queuedAcquisition(client, kind).pipe(
                      Effect.forkChild({ startImmediately: true }),
                    )
                    yield* Effect.yieldNow
                    expect(queued.pollUnsafe()).toBeUndefined()

                    yield* Deferred.succeed(holderRelease, undefined)
                    for (let step = 0; step < delay; step++) yield* Effect.yieldNow
                    yield* Fiber.interrupt(queued).pipe(
                      Effect.timeoutOrElse({
                        duration: "1 second",
                        orElse: () =>
                          Effect.fail(new Error(`${clientType} queued ${kind} did not settle after interruption`)),
                      }),
                    )
                    expect(
                      yield* client.unsafe("SELECT 3").values.pipe(
                        Effect.timeoutOrElse({
                          duration: "1 second",
                          orElse: () =>
                            Effect.fail(
                              new Error(`${clientType} SQL did not proceed after interrupting queued ${kind}`),
                            ),
                        }),
                      ),
                    ).toEqual([[3]])
                    yield* Fiber.join(holder).pipe(
                      Effect.timeoutOrElse({
                        duration: "1 second",
                        orElse: () => Effect.fail(new Error(`${clientType} holder did not release the permit`)),
                      }),
                    )
                  }).pipe(
                    Effect.ensuring(
                      Deferred.succeed(holderRelease, undefined).pipe(Effect.andThen(Fiber.interrupt(holder))),
                    ),
                  )
                }
              }
            }),
          ),
        )
      },
      20_000,
    )

    for (const kind of ["execute", "raw", "values", "unprepared", "loadExtension", "export"] as const) {
      if (kind === "export" && clientType !== "bun") continue
      test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
        `${clientType} cancels queued ${kind} acquisition without leaking permits`,
        async () => {
          await Effect.runPromise(
            Effect.scoped(
              Effect.gen(function* () {
                const client: SqlClient = yield* sqliteClientForPermitTest(clientType)
                const statement = client.unsafe("SELECT 2")
                const operation =
                  kind === "execute"
                    ? Effect.asVoid(statement)
                    : kind === "raw"
                      ? Effect.asVoid(statement.raw)
                      : kind === "values"
                        ? Effect.asVoid(statement.values)
                        : kind === "unprepared"
                          ? Effect.scoped(
                              Effect.flatMap(client.reserve, (connection) =>
                                connection.executeUnprepared("SELECT 2", [], undefined),
                              ),
                            ).pipe(Effect.asVoid)
                          : kind === "loadExtension"
                            ? hasLoadExtension(client)
                              ? Effect.asVoid(client.loadExtension("/missing/sqlite-permit-test-extension")).pipe(
                                  Effect.catchIf(isSqlError, () => Effect.void),
                                )
                              : Effect.fail(new Error(`${clientType} layer does not expose loadExtension`))
                            : hasExport(client)
                              ? Effect.asVoid(client.export)
                              : Effect.fail(new Error(`${clientType} layer does not expose export`))

                for (let delay = 0; delay < 32; delay++) {
                  const holderReady = yield* Deferred.make<void>()
                  const holderRelease = yield* Deferred.make<void>()
                  const holder = yield* Effect.scoped(
                    Effect.gen(function* () {
                      yield* client.reserve
                      yield* Deferred.succeed(holderReady, undefined)
                      yield* Deferred.await(holderRelease)
                    }),
                  ).pipe(Effect.forkChild({ startImmediately: true }))

                  yield* Effect.gen(function* () {
                    yield* Deferred.await(holderReady).pipe(
                      Effect.timeoutOrElse({
                        duration: "1 second",
                        orElse: () => Effect.fail(new Error(`${clientType} holder did not acquire the permit`)),
                      }),
                    )
                    const queued = yield* Effect.scoped(Effect.andThen(operation, Effect.yieldNow)).pipe(
                      Effect.provideService(References.MaxOpsBeforeYield, 3),
                      Effect.forkChild({ startImmediately: true }),
                    )
                    yield* Effect.yieldNow
                    expect(queued.pollUnsafe()).toBeUndefined()

                    yield* Deferred.succeed(holderRelease, undefined)
                    for (let step = 0; step < delay; step++) yield* Effect.yieldNow
                    yield* Fiber.interrupt(queued).pipe(
                      Effect.timeoutOrElse({
                        duration: "1 second",
                        orElse: () => Effect.fail(new Error(`${clientType} queued ${kind} did not settle after interrupt`)),
                      }),
                    )
                    expect(
                      yield* client.unsafe("SELECT 3").values.pipe(
                        Effect.timeoutOrElse({
                          duration: "1 second",
                          orElse: () => Effect.fail(new Error(`${clientType} SQL did not proceed after interrupting ${kind}`)),
                        }),
                      ),
                    ).toEqual([[3]])
                    yield* Fiber.join(holder).pipe(
                      Effect.timeoutOrElse({
                        duration: "1 second",
                        orElse: () => Effect.fail(new Error(`${clientType} holder did not release the permit`)),
                      }),
                    )
                  }).pipe(
                    Effect.ensuring(
                      Deferred.succeed(holderRelease, undefined).pipe(
                        Effect.andThen(
                          Effect.asVoid(Fiber.interrupt(holder)).pipe(
                            Effect.timeoutOrElse({ duration: "1 second", orElse: () => Effect.void }),
                          ),
                        ),
                      ),
                    ),
                  )
                }
              }),
            ),
          )
        },
        20_000,
      )
    }

    // Waiting cancellation is a preservation control; the reserve sweep above owns the acquisition-handoff proof.
    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} cancels a queued reserve while its permit holder remains active`,
      async () => {
        await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const client: SqlClient = yield* sqliteClientForPermitTest(clientType)
              const holderReady = yield* Deferred.make<void>()
              const holderRelease = yield* Deferred.make<void>()
              const holder = yield* Effect.scoped(
                Effect.gen(function* () {
                  yield* client.reserve
                  yield* Deferred.succeed(holderReady, undefined)
                  yield* Deferred.await(holderRelease)
                }),
              ).pipe(Effect.forkChild({ startImmediately: true }))

              yield* Effect.gen(function* () {
                yield* Deferred.await(holderReady).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error(`${clientType} holder did not acquire the permit`)),
                  }),
                )
                const queued = yield* Effect.scoped(Effect.andThen(client.reserve, Effect.void)).pipe(
                  Effect.forkChild({ startImmediately: true }),
                )
                yield* Effect.yieldNow
                expect(queued.pollUnsafe()).toBeUndefined()
                yield* Fiber.interrupt(queued).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error(`${clientType} reserve wait did not cancel`)),
                  }),
                )
                expect(queued.pollUnsafe()).not.toBeUndefined()
                expect(holder.pollUnsafe()).toBeUndefined()

                yield* Deferred.succeed(holderRelease, undefined)
                yield* Fiber.join(holder).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error(`${clientType} holder did not release the permit`)),
                  }),
                )
                expect(
                  yield* client.unsafe("SELECT 3").values.pipe(
                    Effect.timeoutOrElse({
                      duration: "1 second",
                      orElse: () => Effect.fail(new Error(`${clientType} SQL did not proceed after reserve cancel`)),
                    }),
                  ),
                ).toEqual([[3]])
              }).pipe(
                Effect.ensuring(
                  Deferred.succeed(holderRelease, undefined).pipe(Effect.andThen(Fiber.interrupt(holder))),
                ),
              )
            }),
          ),
        )
      },
      10_000,
    )

    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} lets a deferred read transaction finish while BEGIN IMMEDIATE retries a real lock`,
      async () => {
        await using tmp = await tmpdir()
        const filename = path.join(tmp.path, `${clientType}-deferred-read-during-begin.sqlite`)
        const writerSql = "BEGIN IMMEDIATE"
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Sqlite.withFreshRetryWindow(
              Effect.gen(function* () {
                const attempts = newStatementAttempts()
                const client = yield* clientWithAttemptCount({
                  clientType,
                  filename,
                  statement: writerSql,
                  attempts,
                })
                yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
                const holder = yield* Effect.promise(() =>
                  startLockHolder(filename, clientType === "bun" ? bunHolderScript : nodeHolderScript),
                )
                yield* Effect.addFinalizer(() => Effect.promise(() => holder.release()))

                const transactionScope = yield* Scope.make()
                yield* Effect.addFinalizer(() => Scope.close(transactionScope, Exit.void))
                const connection = yield* Scope.provide(client.reserve, transactionScope)
                const writer = yield* connection
                  .executeUnprepared(writerSql, [], undefined)
                  .pipe(Effect.forkChild({ startImmediately: true }))
                yield* Effect.gen(function* () {
                  while (attempts.count < 2) yield* Effect.sleep("1 millis")
                }).pipe(
                  Effect.timeoutOrElse({
                    duration: "2 seconds",
                    orElse: () => Effect.fail(new Error(`${clientType} writer did not reach BEGIN IMMEDIATE retry`)),
                  }),
                )
                expect(writer.pollUnsafe()).toBeUndefined()

                const deferredRead = yield* client
                  .withTransaction(client.unsafe("SELECT COUNT(*) FROM busy_retry_test").values)
                  .pipe(Effect.forkChild({ startImmediately: true }))
                const read = yield* Fiber.await(deferredRead).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () =>
                      Effect.fail(
                        new Error(`${clientType} deferred read waited behind a writer whose BEGIN has not succeeded`),
                      ),
                  }),
                )
                expect(Exit.isSuccess(read)).toBe(true)
                if (Exit.isSuccess(read)) expect(read.value).toEqual([[0]])
                expect(writer.pollUnsafe()).toBeUndefined()

                yield* Effect.promise(() => holder.release())
                const writerExit = yield* Fiber.await(writer).pipe(
                  Effect.timeoutOrElse({
                    duration: "2 seconds",
                    orElse: () => Effect.fail(new Error(`${clientType} writer did not finish after lock release`)),
                  }),
                )
                expect(Exit.isSuccess(writerExit)).toBe(true)
                if (!Exit.isSuccess(writerExit)) return
                yield* connection.executeUnprepared("ROLLBACK", [], undefined)
              }),
              Duration.seconds(5),
            ),
          ),
        )
        if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
      },
      15_000,
    )

    for (const ownership of ["ordinary", "reserved"] as const) {
      test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
        `${clientType} lets a deferred read transaction finish during ${ownership} statement retry backoff`,
        async () => {
          await using tmp = await tmpdir()
          const filename = path.join(tmp.path, `${clientType}-${ownership}-statement-backoff.sqlite`)
          const writerSql = "INSERT INTO busy_retry_test (id, value) VALUES (1, 'retrying-writer')"
          const exit = await Effect.runPromiseExit(
            Effect.scoped(
              Sqlite.withFreshRetryWindow(
                Effect.gen(function* () {
                  const attempts = newStatementAttempts()
                  const client = yield* clientWithAttemptCount({ clientType, filename, statement: writerSql, attempts })
                  yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
                  const holder = yield* Effect.promise(() =>
                    startLockHolder(filename, clientType === "bun" ? bunHolderScript : nodeHolderScript),
                  )
                  yield* Effect.addFinalizer(() => Effect.promise(() => holder.release()))

                  const reservationScope = ownership === "reserved" ? yield* Scope.make() : undefined
                  if (reservationScope)
                    yield* Effect.addFinalizer(() => Scope.close(reservationScope, Exit.void))
                  const reserved = reservationScope ? yield* Scope.provide(client.reserve, reservationScope) : undefined
                  const writerEffect = reserved
                    ? reserved.executeUnprepared(writerSql, [], undefined)
                    : client.unsafe(writerSql).raw
                  const writer = yield* writerEffect.pipe(Effect.forkChild({ startImmediately: true }))
                  yield* Effect.gen(function* () {
                    while (attempts.count < 2) yield* Effect.sleep("1 millis")
                  }).pipe(
                    Effect.timeoutOrElse({
                      duration: "2 seconds",
                      orElse: () => Effect.fail(new Error(`${clientType} ${ownership} writer did not retry its INSERT`)),
                    }),
                  )
                  expect(writer.pollUnsafe()).toBeUndefined()

                  const deferredRead = yield* client
                    .withTransaction(client.unsafe("SELECT COUNT(*) FROM busy_retry_test").values)
                    .pipe(Effect.forkChild({ startImmediately: true }))
                  const read = yield* Fiber.await(deferredRead).pipe(
                    Effect.timeoutOrElse({
                      duration: "1 second",
                      orElse: () =>
                        Effect.fail(
                          new Error(`${clientType} deferred read waited through ${ownership} statement backoff`),
                        ),
                    }),
                  )
                  expect(Exit.isSuccess(read)).toBe(true)
                  if (Exit.isSuccess(read)) expect(read.value).toEqual([[0]])
                  expect(writer.pollUnsafe()).toBeUndefined()

                  yield* Effect.promise(() => holder.release())
                  const writerExit = yield* Fiber.await(writer).pipe(
                    Effect.timeoutOrElse({
                      duration: "2 seconds",
                      orElse: () =>
                        Effect.fail(new Error(`${clientType} ${ownership} writer did not finish after lock release`)),
                    }),
                  )
                  expect(Exit.isSuccess(writerExit)).toBe(true)
                  if (!Exit.isSuccess(writerExit)) return
                  if (reservationScope) yield* Scope.close(reservationScope, Exit.void)
                  expect(
                    yield* client
                      .unsafe("SELECT COUNT(*) FROM busy_retry_test WHERE id = 1")
                      .values,
                  ).toEqual([[1]])
                }),
                Duration.seconds(5),
              ),
            ),
          )
          if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
        },
        15_000,
      )
    }
  }
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
                const attempts = newStatementAttempts()
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

  for (const clientType of ["bun", "core-node", "standalone-node"] as const) {
    for (const operation of ["statement", "transaction-begin"] as const) {
      test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
        `${clientType} lets an unrelated SQL statement progress while ${operation} retries a held lock`,
        async () => {
          await using tmp = await tmpdir()
          const filename = path.join(tmp.path, `${clientType}-${operation}-fairness.sqlite`)
          const writerSql =
            operation === "statement"
              ? "INSERT INTO busy_retry_test (id, value) VALUES (1, 'fairness-writer')"
              : "BEGIN IMMEDIATE"
          const exit = await Effect.runPromiseExit(
            Effect.scoped(
              Effect.gen(function* () {
                const attempts = newStatementAttempts()
                const client = yield* clientWithAttemptCount({ clientType, filename, statement: writerSql, attempts })
                yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
                const holder = yield* Effect.promise(() =>
                  startLockHolder(filename, clientType === "bun" ? bunHolderScript : nodeHolderScript),
                )
                yield* Effect.addFinalizer(() => Effect.promise(() => holder.release()))

                const transactionScope = operation === "transaction-begin" ? yield* Scope.make() : undefined
                const transactionConnection = transactionScope
                  ? yield* Scope.provide(client.reserve, transactionScope)
                  : undefined
                if (transactionScope) yield* Effect.addFinalizer(() => Scope.close(transactionScope, Exit.void))

                let writerFinished = false
                const writerEffect =
                  operation === "statement"
                    ? client.unsafe(writerSql).raw
                    : Effect.gen(function* () {
                        if (!transactionConnection) return yield* Effect.die(new Error("missing reserved connection"))
                        yield* transactionConnection.executeUnprepared("BEGIN IMMEDIATE", [], undefined)
                        yield* transactionConnection.executeUnprepared(
                          "INSERT INTO busy_retry_test (id, value) VALUES (1, 'fairness-writer')",
                          [],
                          undefined,
                        )
                        yield* transactionConnection.executeUnprepared("COMMIT", [], undefined)
                      })
                const writer = yield* writerEffect.pipe(
                  Effect.onExit(() => Effect.sync(() => void (writerFinished = true))),
                  Effect.forkChild({ startImmediately: true }),
                )
                yield* Effect.gen(function* () {
                  while (attempts.count === 0) yield* Effect.sleep("1 millis")
                }).pipe(
                  Effect.timeoutOrElse({
                    duration: "2 seconds",
                    orElse: () => Effect.fail(new Error("writer did not attempt the held-lock statement")),
                  }),
                )
                const attemptsBeforeRead = attempts.count
                const unrelated = yield* client.unsafe("SELECT 42").values.pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error("unrelated SQL stalled behind the retrying writer")),
                  }),
                )
                expect(unrelated).toEqual([[42]])
                expect(writerFinished).toBe(false)
                yield* Effect.gen(function* () {
                  while (attempts.count <= attemptsBeforeRead) yield* Effect.sleep("1 millis")
                }).pipe(
                  Effect.timeoutOrElse({
                    duration: "2 seconds",
                    orElse: () => Effect.fail(new Error("writer stopped retrying before the lock was released")),
                  }),
                )
                expect(writerFinished).toBe(false)

                yield* Effect.promise(() => holder.release())
                const result = yield* Fiber.await(writer).pipe(
                  Effect.timeoutOrElse({
                    duration: "2 seconds",
                    orElse: () => Effect.fail(new Error("writer did not finish after the lock was released")),
                  }),
                )
                if (Exit.isFailure(result)) return yield* Effect.fail(new Error(Cause.pretty(result.cause)))
                if (transactionScope) yield* Scope.close(transactionScope, Exit.void)
                expect(
                  yield* client.unsafe("SELECT COUNT(*) FROM busy_retry_test WHERE value = 'fairness-writer'").values,
                ).toEqual([[1]])
              }),
            ),
          )
          if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
        },
        10_000,
      )
    }

    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} counts waiting for the connection permit against the session retry window`,
      async () => {
        await using tmp = await tmpdir()
        const filename = path.join(tmp.path, `${clientType}-permit-window.sqlite`)
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Effect.gen(function* () {
              const attempts = newStatementAttempts()
              const client = yield* clientWithAttemptCount({
                clientType,
                filename,
                statement: "SELECT 7",
                attempts,
              })
              const entered = yield* Deferred.make<void>()
              const release = yield* Deferred.make<void>()
              const owner = yield* client
                .withTransaction(
                  Effect.gen(function* () {
                    yield* Deferred.succeed(entered, undefined)
                    yield* Deferred.await(release)
                  }),
                )
                .pipe(Effect.forkChild({ startImmediately: true }))
              yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
              yield* Deferred.await(entered)

              const startedAt = performance.now()
              const failure = yield* Sqlite.withFreshRetryWindow(
                client.unsafe("SELECT 7").raw.pipe(Effect.flip),
                Duration.millis(500),
              ).pipe(
                Effect.timeoutOrElse({
                  duration: "2 seconds",
                  orElse: () => Effect.fail(new Error("connection-permit wait escaped the retry window")),
                }),
              )
              expect(isSqlError(failure)).toBe(true)
              if (!isSqlError(failure)) return
              expect(failure.reason._tag).toBe("LockTimeoutError")
              expect(Sqlite.lockWaitMs(failure)).toBeGreaterThanOrEqual(450)
              expect(performance.now() - startedAt).toBeLessThan(1_500)

              yield* Deferred.succeed(release, undefined)
              const result = yield* Fiber.await(owner).pipe(
                Effect.timeoutOrElse({
                  duration: "2 seconds",
                  orElse: () => Effect.fail(new Error("permit owner did not release its connection")),
                }),
              )
              if (Exit.isFailure(result)) return yield* Effect.fail(new Error(Cause.pretty(result.cause)))
              expect(yield* client.unsafe("SELECT 9").values).toEqual([[9]])
            }),
          ),
        )
        if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
      },
      10_000,
    )

    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} bounds transaction BEGIN while it waits for the connection permit`,
      async () => {
        await using tmp = await tmpdir()
        const filename = path.join(tmp.path, `${clientType}-transaction-permit-window.sqlite`)
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Effect.gen(function* () {
              const attempts = newStatementAttempts()
              const client = yield* clientWithAttemptCount({
                clientType,
                filename,
                statement: "BEGIN IMMEDIATE",
                attempts,
              })
              const entered = yield* Deferred.make<void>()
              const release = yield* Deferred.make<void>()
              const owner = yield* client
                .withTransaction(
                  Effect.gen(function* () {
                    yield* Deferred.succeed(entered, undefined)
                    yield* Deferred.await(release)
                  }),
                )
                .pipe(Effect.forkChild({ startImmediately: true }))
              yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
              yield* Deferred.await(entered)

              const transactionScope = yield* Scope.make()
              yield* Effect.addFinalizer(() => Scope.close(transactionScope, Exit.void))
              const startedAt = performance.now()
              const failure = yield* Sqlite.withFreshRetryWindow(
                Effect.gen(function* () {
                  const connection = yield* Scope.provide(client.reserve, transactionScope)
                  yield* connection.executeUnprepared("BEGIN IMMEDIATE", [], undefined)
                }).pipe(Effect.flip),
                Duration.millis(500),
              ).pipe(
                Effect.timeoutOrElse({
                  duration: "2 seconds",
                  orElse: () => Effect.fail(new Error("transaction permit wait escaped the session retry window")),
                }),
              )
              expect(isSqlError(failure)).toBe(true)
              if (!isSqlError(failure)) return
              expect(failure.reason._tag).toBe("LockTimeoutError")
              expect(Sqlite.lockWaitMs(failure)).toBeGreaterThanOrEqual(450)
              expect(performance.now() - startedAt).toBeLessThan(1_500)

              yield* Scope.close(transactionScope, Exit.void)
              yield* Deferred.succeed(release, undefined)
              const result = yield* Fiber.await(owner).pipe(
                Effect.timeoutOrElse({
                  duration: "2 seconds",
                  orElse: () => Effect.fail(new Error("permit owner did not release its connection")),
                }),
              )
              if (Exit.isFailure(result)) return yield* Effect.fail(new Error(Cause.pretty(result.cause)))
              expect(yield* client.unsafe("SELECT 9").values).toEqual([[9]])
            }),
          ),
        )
        if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
      },
      10_000,
    )
  }

  for (const clientType of ["bun", "core-node", "standalone-node"] as const) {
    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} retires a recovered permit-contention window before a later contention`,
      async () => {
        const windowMs = 500
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Sqlite.withFreshRetryWindow(
              Effect.gen(function* () {
                const client = yield* sqliteClientForPermitTest(clientType)
                const holdConnection = (entered: Deferred.Deferred<void>, release: Deferred.Deferred<void>) =>
                  client
                    .withTransaction(
                      Effect.gen(function* () {
                        yield* Deferred.succeed(entered, undefined)
                        yield* Deferred.await(release)
                        return yield* Effect.fail("intentional rollback" as const)
                      }),
                    )
                    .pipe(Effect.catch(() => Effect.void))
                const awaitPermitWait = Effect.gen(function* () {
                  while (!(yield* Sqlite.retryWindowStatus).sawPermit) yield* Effect.sleep("1 millis")
                }).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error(`${clientType} read never waited for the connection permit`)),
                  }),
                )

                const firstEntered = yield* Deferred.make<void>()
                const firstRelease = yield* Deferred.make<void>()
                yield* Effect.addFinalizer(() => Deferred.succeed(firstRelease, undefined))
                const firstOwner = yield* holdConnection(firstEntered, firstRelease).pipe(
                  Effect.forkChild({ startImmediately: true }),
                )
                yield* Deferred.await(firstEntered)
                const firstRead = yield* client.unsafe("SELECT 7").values.pipe(
                  Effect.forkChild({ startImmediately: true }),
                )
                yield* awaitPermitWait
                yield* Deferred.succeed(firstRelease, undefined)
                expect(
                  yield* Fiber.join(firstRead).pipe(
                    Effect.timeoutOrElse({
                      duration: "1 second",
                      orElse: () => Effect.fail(new Error(`${clientType} first read did not recover`)),
                    }),
                  ),
                ).toEqual([[7]])
                yield* Fiber.join(firstOwner)
                const afterRecovery = yield* Sqlite.retryWindowStatus

                yield* Effect.sleep(Duration.millis(windowMs + 100))

                const secondEntered = yield* Deferred.make<void>()
                const secondRelease = yield* Deferred.make<void>()
                yield* Effect.addFinalizer(() => Deferred.succeed(secondRelease, undefined))
                const secondOwner = yield* holdConnection(secondEntered, secondRelease).pipe(
                  Effect.forkChild({ startImmediately: true }),
                )
                yield* Deferred.await(secondEntered)
                const secondRead = yield* client.unsafe("SELECT 7").values.pipe(
                  Effect.forkChild({ startImmediately: true }),
                )
                yield* Effect.yieldNow
                const beforeRelease = secondRead.pollUnsafe()
                yield* Deferred.succeed(secondRelease, undefined)
                const secondResult = yield* Fiber.await(secondRead).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error(`${clientType} second read did not settle after release`)),
                  }),
                )
                yield* Fiber.join(secondOwner)

                expect(beforeRelease).toBeUndefined()
                expect(Exit.isSuccess(secondResult)).toBe(true)
                if (Exit.isSuccess(secondResult)) expect(secondResult.value).toEqual([[7]])
                expect(afterRecovery).toMatchObject({ elapsedMs: 0, sawPermit: false })
              }),
              Duration.millis(windowMs),
            ),
          ),
        )
        if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
      },
      10_000,
    )
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
      exhaustionTestMs,
    )
  }

  for (const clientType of ["bun", "core-node", "standalone-node"] as const) {
    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} fails a stale-snapshot transaction write without retrying it`,
      async () => {
        await using tmp = await tmpdir()
        const filename = path.join(tmp.path, `${clientType}-snapshot.sqlite`)
        const writerSql = "INSERT INTO busy_retry_test (id, value) VALUES (1, 'stale-writer')"
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Effect.gen(function* () {
              const attempts = newStatementAttempts()
              const client = yield* clientWithAttemptCount({ clientType, filename, statement: writerSql, attempts })
              yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
              expect(yield* client.unsafe("PRAGMA journal_mode").values).toEqual([["wal"]])
              const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
              const other = new sqlite.Database(filename)
              yield* Effect.addFinalizer(() => Effect.sync(() => other.close()))

              let elapsedMs = 0
              const failure = yield* client
                .withTransaction(
                  Effect.gen(function* () {
                    expect(yield* client.unsafe("SELECT COUNT(*) FROM busy_retry_test").values).toEqual([[0]])
                    // Another connection commits after this transaction's read, so its snapshot is stale.
                    other.run("INSERT INTO busy_retry_test (id, value) VALUES (2, 'other-writer')")
                    const startedAt = performance.now()
                    yield* client
                      .unsafe(writerSql)
                      .raw.pipe(
                        Effect.onExit(() => Effect.sync(() => void (elapsedMs = performance.now() - startedAt))),
                      )
                  }),
                )
                .pipe(
                  Effect.timeoutOrElse({
                    duration: lockBackstop,
                    orElse: () => Effect.fail(new Error("stale-snapshot retries did not stop")),
                  }),
                  Effect.flip,
                )
              expect(isSqlError(failure)).toBe(true)
              if (!isSqlError(failure)) return
              expect(failure.reason._tag).toBe("LockTimeoutError")
              expect(failure.reason.cause).toMatchObject(
                clientType === "bun" ? { code: "SQLITE_BUSY_SNAPSHOT" } : { errcode: 517 },
              )
              expect(elapsedMs).toBeLessThan(snapshotBoundMs)
              expect(attempts.count).toBe(1)
              expect(yield* client.unsafe("SELECT id, value FROM busy_retry_test ORDER BY id").values).toEqual([
                [2, "other-writer"],
              ])
            }),
          ),
        )
        if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
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
  }, exhaustionTestMs)

  test("commits an immediate transaction after a competing connection holds the write lock for 9.5 seconds", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "immediate-long-hold.sqlite")
    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const attempts = newStatementAttempts()
          yield* patchBunQuery((sql) => sql === "begin immediate", attempts)
          const context = yield* Layer.build(Database.layerFromPath(filename))
          const database = Context.get(context, Database.Service).db
          yield* database.run("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
          const holder = yield* Effect.promise(() => startLockHolder(filename))
          yield* Effect.addFinalizer(() => Effect.promise(() => holder.release()))

          let writerFinished = false
          const writer = yield* database
            .transaction((tx) => tx.run("INSERT INTO busy_retry_test (id, value) VALUES (1, 'long-hold-writer')"), {
              behavior: "immediate",
            })
            .pipe(
              Effect.onExit(() => Effect.sync(() => void (writerFinished = true))),
              Effect.forkChild({ startImmediately: true }),
            )
          yield* Effect.gen(function* () {
            while (attempts.busy.length === 0) yield* Effect.sleep("1 millis")
          }).pipe(
            Effect.timeoutOrElse({
              duration: "1 second",
              orElse: () => Effect.fail(new Error("writer did not meet the held write lock")),
            }),
          )
          // The writer has met the lock; hold it past the old window and almost to the end of the current one.
          const metAt = performance.now()
          while (performance.now() - metAt < longHeldLockMs) yield* Effect.sleep("10 millis")
          expect(writerFinished).toBe(false)

          yield* Effect.promise(() => holder.release())
          const result = yield* Fiber.await(writer).pipe(
            Effect.timeoutOrElse({
              duration: "2 seconds",
              orElse: () => Effect.fail(new Error("writer did not finish after the lock was released")),
            }),
          )
          if (Exit.isFailure(result)) return yield* Effect.fail(new Error(Cause.pretty(result.cause)))
          expect(attempts.count).toBeGreaterThan(1)
          expect(
            yield* database.$client.unsafe("SELECT COUNT(*) FROM busy_retry_test WHERE value = 'long-hold-writer'")
              .values,
          ).toEqual([[1]])
        }),
      ),
    )
    if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
  }, exhaustionTestMs)

  for (const clientType of ["bun", "core-node", "standalone-node"] as const) {
    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} shares one session retry window across consecutive locked statements`,
      async () => {
        await using tmp = await tmpdir()
        const filename = path.join(tmp.path, `${clientType}-session-window.sqlite`)
        const writerSql = "INSERT INTO busy_retry_test (id, value) VALUES (1, 'session-window-writer')"
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Effect.gen(function* () {
              const attempts = newStatementAttempts()
              const client = yield* clientWithAttemptCount({ clientType, filename, statement: writerSql, attempts })
              yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
              yield* client.unsafe("PRAGMA busy_timeout = 0").raw
              const holder = yield* Effect.promise(() =>
                startLockHolder(filename, clientType === "bun" ? holderScript : nodeHolderScript),
              )
              yield* Effect.addFinalizer(() => Effect.promise(() => holder.release()))

              const result = yield* Sqlite.withFreshRetryWindow(
                Effect.gen(function* () {
                  const firstStartedAt = performance.now()
                  const first = yield* client.unsafe(writerSql).raw.pipe(Effect.flip)
                  const firstElapsedMs = performance.now() - firstStartedAt
                  const secondStartedAt = performance.now()
                  const second = yield* client
                    .unsafe("INSERT INTO busy_retry_test (id, value) VALUES (2, 'session-window-followup')")
                    .raw.pipe(Effect.flip)
                  return { first, firstElapsedMs, second, secondElapsedMs: performance.now() - secondStartedAt }
                }),
                Duration.millis(500),
              ).pipe(
                Effect.timeoutOrElse({
                  duration: "3 seconds",
                  orElse: () => Effect.fail(new Error("the shared SQLite retry window did not stop")),
                }),
              )

              expect(isSqlError(result.first)).toBe(true)
              expect(isSqlError(result.second)).toBe(true)
              expect(attempts.count).toBeGreaterThan(1)
              expect(result.firstElapsedMs).toBeGreaterThan(300)
              expect(result.secondElapsedMs).toBeLessThan(400)
              if (!isSqlError(result.first) || !isSqlError(result.second)) return
              expect(result.first.reason._tag).toBe("LockTimeoutError")
              expect(result.second.reason._tag).toBe("LockTimeoutError")
              expect(Sqlite.lockWaitMs(result.second)).toBeGreaterThan(300)
              if (clientType === "bun") expect(result.first.reason.cause).toMatchObject({ code: "SQLITE_BUSY" })
              else expect(result.first.reason.cause).toMatchObject({ errcode: 5 })
              yield* Effect.promise(() => holder.release())
            }),
          ),
        )
        if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
      },
      10_000,
    )
  }

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
      exhaustionTestMs,
    )
  }

  for (const clientType of ["bun", "core-node", "standalone-node"] as const) {
    test.skipIf(clientType !== "bun" && !nodeSqliteDrivers)(
      `${clientType} lets a deferred transaction finish during backoff and serves later arrivals after the writer`,
      async () => {
        await using tmp = await tmpdir()
        const filename = path.join(tmp.path, `${clientType}-writer-fairness.sqlite`)
        const writerSql = "INSERT INTO busy_retry_test (id, value) VALUES (1, 'writer')"
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Sqlite.withFreshRetryWindow(
              Effect.gen(function* () {
                const attempts = newStatementAttempts()
                const client = yield* clientWithAttemptCount({ clientType, filename, statement: writerSql, attempts })
                const scope = yield* Scope.Scope
                yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
                yield* client.unsafe("PRAGMA busy_timeout = 0").raw
                const holder = yield* Effect.promise(() =>
                  startLockHolder(filename, clientType === "bun" ? bunHolderScript : nodeHolderScript),
                )
                yield* Effect.addFinalizer(() => Effect.promise(() => holder.release()))
                const order: string[] = []
                let writerFinished = false
                const writer = yield* client.unsafe(writerSql).raw.pipe(
                  Effect.tap(() => Effect.sync(() => order.push("writer-finished"))),
                  Effect.onExit(() => Effect.sync(() => void (writerFinished = true))),
                  Effect.forkChild({ startImmediately: true }),
                )
                yield* Effect.gen(function* () {
                  while (!(yield* Sqlite.retryWindowStatus).sawLock) yield* Effect.sleep("1 millis")
                }).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error(`${clientType} writer did not meet the real write lock`)),
                  }),
                )
                expect(writerFinished).toBe(false)

                const backoffRead = yield* Sqlite.withFreshRetryWindow(
                  client.withTransaction(
                    Effect.gen(function* () {
                      order.push("backoff-read-started")
                      const rows = yield* client.unsafe("SELECT COUNT(*) FROM busy_retry_test").values
                      order.push("backoff-read-completed")
                      return rows
                    }),
                  ),
                  Duration.seconds(5),
                ).pipe(Effect.forkChild({ startImmediately: true }))
                const backoffReadExit = yield* Fiber.await(backoffRead).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error(`${clientType} deferred transaction did not run in backoff`)),
                  }),
                )
                expect(Exit.isSuccess(backoffReadExit)).toBe(true)
                if (Exit.isSuccess(backoffReadExit)) expect(backoffReadExit.value).toEqual([[0]])
                expect(writerFinished).toBe(false)

                const ownerEntered = yield* Deferred.make<void>()
                const ownerRelease = yield* Deferred.make<void>()
                yield* Effect.addFinalizer(() => Deferred.succeed(ownerRelease, undefined))
                const owner = yield* Sqlite.withFreshRetryWindow(
                  client.withTransaction(
                    Effect.gen(function* () {
                      order.push("backoff-owner-started")
                      yield* Deferred.succeed(ownerEntered, undefined)
                      yield* Deferred.await(ownerRelease)
                      yield* client.unsafe("INSERT INTO busy_retry_test (id, value) VALUES (2, 'transaction')").raw
                      order.push("backoff-owner-committed")
                    }),
                  ),
                  Duration.seconds(5),
                ).pipe(Effect.forkChild({ startImmediately: true }))
                yield* Deferred.await(ownerEntered).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error(`${clientType} deferred transaction did not begin in backoff`)),
                  }),
                )
                // Queue arrivals during backoff, then keep admitting more while the writer waits for the open transaction.
                const laterTransaction = (index: number) =>
                  Effect.gen(function* () {
                    const requested = yield* Deferred.make<void>()
                    const rows = yield* Deferred.make<unknown>()
                    const release = yield* Deferred.make<void>()
                    yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
                    const acquisition = yield* Sqlite.withFreshRetryWindow(
                      Effect.gen(function* () {
                        const transaction = yield* client
                          .withTransaction(
                            Effect.gen(function* () {
                              const result = yield* client
                                .unsafe("SELECT COUNT(*) FROM busy_retry_test WHERE id = 1")
                                .values
                              order.push(`later-${index}-started`)
                              yield* Deferred.succeed(rows, result)
                              yield* Deferred.await(release)
                            }),
                          )
                          .pipe(Effect.forkIn(scope, { startImmediately: true }))
                        yield* Effect.gen(function* () {
                          while (!(yield* Sqlite.retryWindowStatus).sawPermit) yield* Effect.sleep("1 millis")
                        }).pipe(
                          Effect.timeoutOrElse({
                            duration: "2 seconds",
                            orElse: () =>
                              Effect.fail(new Error(`${clientType} later transaction ${index} did not wait for a permit`)),
                          }),
                        )
                        yield* Deferred.succeed(requested, undefined)
                        return transaction
                      }),
                      Duration.seconds(20),
                    ).pipe(Effect.forkIn(scope, { startImmediately: true }))
                    yield* Deferred.await(requested).pipe(
                      Effect.timeoutOrElse({
                        duration: "2 seconds",
                        orElse: () => Effect.fail(new Error(`${clientType} later transaction ${index} did not arrive`)),
                      }),
                    )
                    return { index, requested, rows, release, acquisition }
                  })
                const queuedTransactions = yield* Effect.forEach(
                  Array.from({ length: 4 }, (_, index) => index),
                  laterTransaction,
                  { concurrency: "unbounded" },
                )
                yield* Effect.yieldNow
                yield* Effect.promise(() => holder.release())
                yield* Effect.gen(function* () {
                  while (!(yield* Sqlite.retryWindowStatus).sawPermit) yield* Effect.sleep("1 millis")
                }).pipe(
                  Effect.timeoutOrElse({
                    duration: "2 seconds",
                    orElse: () => Effect.fail(new Error(`${clientType} writer never waited behind the open transaction`)),
                  }),
                )
                const continuingArrivals = yield* Effect.forEach(
                  Array.from({ length: 8 }, (_, index) => index + 4),
                  laterTransaction,
                  { concurrency: 1 },
                )
                const laterTransactions = [...queuedTransactions, ...continuingArrivals]
                const writerBudget = yield* Sqlite.retryWindowStatus
                expect(writerBudget.remainingMs).toBeGreaterThan(0)

                if (writerFinished)
                  yield* Effect.fail(new Error(`${clientType} writer exhausted its budget before the owner released`))
                expect(writerFinished).toBe(false)

                yield* Deferred.succeed(ownerRelease, undefined)
                const ownerExit = yield* Fiber.await(owner).pipe(
                  Effect.timeoutOrElse({
                    duration: "2 seconds",
                    orElse: () => Effect.fail(new Error(`${clientType} open transaction did not commit`)),
                  }),
                )
                expect(Exit.isSuccess(ownerExit)).toBe(true)
                expect(order.indexOf("backoff-owner-committed")).toBeGreaterThanOrEqual(0)

                const writerExit = yield* Fiber.await(writer).pipe(
                  Effect.timeoutOrElse({
                    duration: "3 seconds",
                    orElse: () => Effect.fail(new Error(`${clientType} writer was starved by later transactions`)),
                  }),
                )
                expect(Exit.isSuccess(writerExit)).toBe(true)
                expect(writerFinished).toBe(true)
                const laterFibers = yield* Effect.forEach(
                  laterTransactions,
                  (item) => Fiber.join(item.acquisition),
                  { concurrency: "unbounded" },
                )
                yield* Effect.forEach(laterTransactions, (item) => Deferred.succeed(item.release, undefined), {
                  concurrency: "unbounded",
                })
                const laterExits = yield* Effect.forEach(
                  laterFibers,
                  (fiber) =>
                    Fiber.await(fiber).pipe(
                      Effect.timeoutOrElse({
                        duration: "5 seconds",
                        orElse: () => Effect.fail(new Error(`${clientType} queued transaction did not finish`)),
                      }),
                    ),
                  { concurrency: "unbounded" },
                )
                expect(laterExits.every(Exit.isSuccess)).toBe(true)
                const laterRows = yield* Effect.forEach(
                  laterTransactions,
                  (item) =>
                    Deferred.await(item.rows).pipe(
                      Effect.timeoutOrElse({
                        duration: "5 seconds",
                        orElse: () => Effect.fail(new Error(`${clientType} queued transaction did not read`)),
                      }),
                    ),
                  { concurrency: "unbounded" },
                )
                expect(laterRows).toEqual(Array.from({ length: 12 }, () => [[1]]))
                expect(order.indexOf("backoff-owner-committed")).toBeLessThan(order.indexOf("writer-finished"))
                expect(
                  yield* client.unsafe("SELECT COUNT(*) FROM busy_retry_test WHERE id IN (1, 2)").values,
                ).toEqual([[2]])
              }),
              Duration.millis(lockWindowMs),
            ),
          ),
        )
        if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
      },
      30_000,
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
    }, exhaustionTestMs)
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

test(
  "native Node runs Core Node and standalone SQLite permit handoff regressions",
  async () => {
    const node = Bun.which("node")
    if (!node) throw new Error("Node is required for the native Node SQLite acquisition tests")
    const loader = path.join(import.meta.dir, "fixture/node-typescript-loader.mjs")
    const script = path.join(import.meta.dir, "fixture/sqlite-acquisition-node.mjs")
    const child = Bun.spawn([node, "--experimental-strip-types", "--loader", loader, script], {
      cwd: path.resolve(import.meta.dir, ".."),
      stdout: "pipe",
      stderr: "pipe",
    })
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ])
    if (exitCode !== 0) throw new Error(`Native Node acquisition tests failed (${exitCode})\n${stdout}\n${stderr}`)
    expect(stdout).toMatch(/^runtime=node node=\d+\.\d+\.\d+$/m)
    expect(stdout).toContain("PASS core-node")
    expect(stdout).toContain("PASS standalone-node")
    expect(stdout).toContain("PASS core-node lease grant race maxOps=64")
    expect(stdout).toContain("PASS standalone-node lease grant race maxOps=64")
  },
  120_000,
)
