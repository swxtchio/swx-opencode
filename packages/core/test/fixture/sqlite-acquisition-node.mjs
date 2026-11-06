import assert from "node:assert/strict"
import { mkdtemp, rm } from "node:fs/promises"
import { DatabaseSync } from "node:sqlite"
import os from "node:os"
import path from "node:path"
import { Context, Deferred, Duration, Effect, Exit, Fiber, Layer, References, Scope } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { countStatement, newStatementAttempts } from "../../src/database/sqlite-attempts.ts"
import { retryWindowStatus, withFreshRetryWindow } from "../../src/database/sqlite.ts"
import { layer as coreNodeLayer } from "../../src/database/sqlite.node.ts"
import { verifyLeaseGrantRace } from "./sqlite-lease-grant-race.mjs"
import { NodeSqliteClient } from "../../../effect-sqlite-node/src/index.ts"

const nodeMajor = Number(process.versions.node.split(".")[0])
assert.equal(process.release.name, "node", "SQLite permit tests must run under native Node.js")
assert.equal(process.versions.bun, undefined, "SQLite permit tests must not fall back to Bun")
assert.ok(nodeMajor >= 22, `SQLite permit tests require Node.js 22+, got ${process.versions.node}`)

const operations = (client) => {
  const statement = client.unsafe("SELECT 2")
  const result = [
    { kind: "execute", run: () => Effect.asVoid(statement) },
    { kind: "raw", run: () => Effect.asVoid(statement.raw) },
    { kind: "values", run: () => Effect.asVoid(statement.values) },
    {
      kind: "transaction/unprepared",
      run: () => Effect.asVoid(client.withTransaction(client.unsafe("SELECT 2").raw)),
    },
  ]
  if (typeof client.loadExtension === "function") {
    result.push({
      kind: "loadExtension",
      run: () => Effect.catch(Effect.asVoid(client.loadExtension("/missing/sqlite-permit-test-extension")), () => Effect.void),
    })
  }
  return result
}

const verify = async (name, layer) => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(layer)
        const client = Context.get(context, SqlClient)
        const operationsForClient = operations(client)
        assert.equal(typeof client.loadExtension, "function", `${name} layer must expose loadExtension`)

        for (const operation of operationsForClient) {
          for (let delay = 0; delay < 128; delay++) {
            const holderReady = yield* Deferred.make()
            const holderRelease = yield* Deferred.make()
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
                  orElse: () =>
                    Effect.fail(
                      new Error(`${name} holder did not acquire the permit for ${operation.kind} at delay ${delay}`),
                    ),
                }),
              )
              const queued = yield* Effect.scoped(Effect.andThen(operation.run(), Effect.yieldNow)).pipe(
                Effect.provideService(References.MaxOpsBeforeYield, 3),
                Effect.forkChild({ startImmediately: true }),
              )
              yield* Effect.yieldNow
              assert.equal(queued.pollUnsafe(), undefined, `${name} ${operation.kind} was not queued`)

              yield* Deferred.succeed(holderRelease, undefined)
              for (let step = 0; step < delay; step++) yield* Effect.yieldNow
              yield* Fiber.interrupt(queued).pipe(
                Effect.timeoutOrElse({
                  duration: "1 second",
                  orElse: () => Effect.fail(new Error(`${name} ${operation.kind} did not settle after interrupt`)),
                }),
              )
              yield* Fiber.join(holder).pipe(
                Effect.timeoutOrElse({
                  duration: "1 second",
                  orElse: () => Effect.fail(new Error(`${name} holder did not release the permit`)),
                }),
              )
              assert.deepEqual(
                yield* client.unsafe("SELECT 3").values.pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () => Effect.fail(new Error(`${name} leaked the permit after ${operation.kind}`)),
                  }),
                ),
                [[3]],
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
  process.stdout.write(`PASS ${name}: execute, raw, values, transaction/unprepared, loadExtension\n`)
}

const verifyDeferredReadDuringBegin = async (name, layerForFilename) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "opencode-sqlite-deferred-read-"))
  const filename = path.join(directory, "session.sqlite")
  const holder = new DatabaseSync(filename, { timeout: 0 })
  const descriptor = Object.getOwnPropertyDescriptor(DatabaseSync.prototype, "prepare")
  assert.ok(descriptor, "node:sqlite prepare descriptor must exist")
  const prepare = DatabaseSync.prototype.prepare
  let held = false
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layerForFilename({ filename }))
          const client = Context.get(context, SqlClient)
          yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
          holder.exec("PRAGMA journal_mode = WAL")
          holder.exec("BEGIN IMMEDIATE")
          holder.prepare("INSERT INTO busy_retry_test (id, value) VALUES (100, 'holder')").run()
          held = true

          const attempts = newStatementAttempts()
          Object.defineProperty(DatabaseSync.prototype, "prepare", {
            ...descriptor,
            value: function (sql) {
              const statement = prepare.call(this, sql)
              return String(sql).trim().toLowerCase() === "begin immediate"
                ? countStatement(statement, attempts)
                : statement
            },
          })
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => Object.defineProperty(DatabaseSync.prototype, "prepare", descriptor)),
          )

          const transactionScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(transactionScope, Exit.void))
          const connection = yield* Scope.provide(client.reserve, transactionScope)
          const writer = yield* connection.executeUnprepared("BEGIN IMMEDIATE", [], undefined).pipe(
            Effect.forkChild({ startImmediately: true }),
          )
          yield* Effect.gen(function* () {
            while (attempts.count < 2) yield* Effect.sleep("1 millis")
          }).pipe(
            Effect.timeoutOrElse({
              duration: "2 seconds",
              orElse: () => Effect.fail(new Error(`${name} writer did not retry BEGIN IMMEDIATE`)),
            }),
          )

          const deferredRead = yield* client
            .withTransaction(client.unsafe("SELECT COUNT(*) FROM busy_retry_test").values)
            .pipe(Effect.forkChild({ startImmediately: true }))
          const read = yield* Fiber.await(deferredRead).pipe(
            Effect.timeoutOrElse({
              duration: "1 second",
              orElse: () => Effect.fail(new Error(`${name} deferred read stalled behind an unstarted writer`)),
            }),
          )
          assert.equal(Exit.isSuccess(read), true, `${name} deferred read must succeed`)
          if (Exit.isSuccess(read)) assert.deepEqual(read.value, [[0]])
          assert.equal(writer.pollUnsafe(), undefined, `${name} writer must still be waiting on the external lock`)
          assert.equal(held, true)

          holder.exec("COMMIT")
          held = false
          const writerExit = yield* Fiber.await(writer).pipe(
            Effect.timeoutOrElse({
              duration: "2 seconds",
              orElse: () => Effect.fail(new Error(`${name} writer did not finish after lock release`)),
            }),
          )
          assert.equal(Exit.isSuccess(writerExit), true, `${name} BEGIN IMMEDIATE must recover after release`)
          if (Exit.isSuccess(writerExit)) yield* connection.executeUnprepared("ROLLBACK", [], undefined)
        }),
      ),
    )
    process.stdout.write(`PASS ${name}: deferred read completes between locked BEGIN attempts\n`)
  } finally {
    if (held) holder.exec("ROLLBACK")
    holder.close()
    await rm(directory, { recursive: true, force: true })
  }
}

const verifyStatementRetryDuringBackoff = async (name, layerForFilename, ownership) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "opencode-sqlite-statement-backoff-"))
  const filename = path.join(directory, "session.sqlite")
  const holder = new DatabaseSync(filename, { timeout: 0 })
  const descriptor = Object.getOwnPropertyDescriptor(DatabaseSync.prototype, "prepare")
  assert.ok(descriptor, "node:sqlite prepare descriptor must exist")
  const prepare = DatabaseSync.prototype.prepare
  const writerSql = "INSERT INTO busy_retry_test (id, value) VALUES (1, 'retrying-writer')"
  let held = false
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layerForFilename({ filename }))
          const client = Context.get(context, SqlClient)
          yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
          holder.exec("PRAGMA journal_mode = WAL")
          holder.exec("BEGIN IMMEDIATE")
          holder.prepare("INSERT INTO busy_retry_test (id, value) VALUES (100, 'holder')").run()
          held = true

          const attempts = newStatementAttempts()
          Object.defineProperty(DatabaseSync.prototype, "prepare", {
            ...descriptor,
            value: function (sql) {
              const statement = prepare.call(this, sql)
              return String(sql).trim() === writerSql ? countStatement(statement, attempts) : statement
            },
          })
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => Object.defineProperty(DatabaseSync.prototype, "prepare", descriptor)),
          )

          const reservationScope = ownership === "reserved" ? yield* Scope.make() : undefined
          if (reservationScope) yield* Effect.addFinalizer(() => Scope.close(reservationScope, Exit.void))
          const connection = reservationScope ? yield* Scope.provide(client.reserve, reservationScope) : undefined
          const writerEffect = connection ? connection.executeUnprepared(writerSql, [], undefined) : client.unsafe(writerSql).raw
          const writer = yield* writerEffect.pipe(Effect.forkChild({ startImmediately: true }))
          yield* Effect.gen(function* () {
            while (attempts.count < 2) yield* Effect.sleep("1 millis")
          }).pipe(
            Effect.timeoutOrElse({
              duration: "2 seconds",
              orElse: () => Effect.fail(new Error(`${name} ${ownership} writer did not retry its statement`)),
            }),
          )

          const deferredRead = yield* client
            .withTransaction(client.unsafe("SELECT COUNT(*) FROM busy_retry_test").values)
            .pipe(Effect.forkChild({ startImmediately: true }))
          const read = yield* Fiber.await(deferredRead).pipe(
            Effect.timeoutOrElse({
              duration: "1 second",
              orElse: () => Effect.fail(new Error(`${name} deferred read waited through ${ownership} backoff`)),
            }),
          )
          assert.equal(Exit.isSuccess(read), true, `${name} deferred read must succeed`)
          if (Exit.isSuccess(read)) assert.deepEqual(read.value, [[0]])
          assert.equal(writer.pollUnsafe(), undefined, `${name} statement writer must still be waiting`)

          holder.exec("COMMIT")
          held = false
          const writerExit = yield* Fiber.await(writer).pipe(
            Effect.timeoutOrElse({
              duration: "2 seconds",
              orElse: () => Effect.fail(new Error(`${name} ${ownership} writer did not recover after release`)),
            }),
          )
          assert.equal(Exit.isSuccess(writerExit), true, `${name} ${ownership} statement must recover`)
          if (reservationScope) yield* Scope.close(reservationScope, Exit.void)
          assert.deepEqual(
            yield* client.unsafe("SELECT COUNT(*) FROM busy_retry_test WHERE id = 1").values,
            [[1]],
          )
        }),
      ),
    )
    process.stdout.write(`PASS ${name}: ${ownership} statement releases ownership during retry backoff\n`)
  } finally {
    if (held) holder.exec("ROLLBACK")
    holder.close()
    await rm(directory, { recursive: true, force: true })
  }
}

const verifyWriterFairness = async (name, layerForFilename) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "opencode-sqlite-writer-fairness-"))
  const filename = path.join(directory, "session.sqlite")
  const holder = new DatabaseSync(filename, { timeout: 0 })
  const writerSql = "INSERT INTO busy_retry_test (id, value) VALUES (1, 'writer')"
  let held = false
  try {
    await Effect.runPromise(
      Effect.scoped(
        withFreshRetryWindow(
          Effect.gen(function* () {
            const context = yield* Layer.build(layerForFilename({ filename }))
            const client = Context.get(context, SqlClient)
            const scope = yield* Scope.Scope
            yield* client.unsafe("CREATE TABLE busy_retry_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
            yield* client.unsafe("PRAGMA busy_timeout = 0").raw
            holder.exec("PRAGMA journal_mode = WAL")
            holder.exec("BEGIN IMMEDIATE")
            holder.prepare("INSERT INTO busy_retry_test (id, value) VALUES (100, 'holder')").run()
            held = true

            const order = []
            let writerFinished = false
            const writer = yield* client.unsafe(writerSql).raw.pipe(
              Effect.tap(() => Effect.sync(() => order.push("writer-finished"))),
              Effect.onExit(() => Effect.sync(() => void (writerFinished = true))),
              Effect.forkChild({ startImmediately: true }),
            )
            yield* Effect.gen(function* () {
              while (!(yield* retryWindowStatus).sawLock) yield* Effect.sleep("1 millis")
            }).pipe(
              Effect.timeoutOrElse({
                duration: "2 seconds",
                orElse: () => Effect.fail(new Error(`${name} writer did not meet the real write lock`)),
              }),
            )

            const readTransaction = yield* withFreshRetryWindow(
              client.withTransaction(client.unsafe("SELECT COUNT(*) FROM busy_retry_test").values),
              Duration.seconds(5),
            ).pipe(Effect.forkChild({ startImmediately: true }))
            const read = yield* Fiber.await(readTransaction).pipe(
              Effect.timeoutOrElse({
                duration: "1 second",
                orElse: () => Effect.fail(new Error(`${name} deferred read did not finish during writer backoff`)),
              }),
            )
            assert.deepEqual(Exit.isSuccess(read) ? read.value : undefined, [[0]])
            assert.equal(writerFinished, false)

            const ownerStarted = yield* Deferred.make()
            const ownerRelease = yield* Deferred.make()
            yield* Effect.addFinalizer(() => Deferred.succeed(ownerRelease, undefined))
            const owner = yield* withFreshRetryWindow(
              client.withTransaction(
                Effect.gen(function* () {
                  order.push("owner-started")
                  yield* Deferred.succeed(ownerStarted, undefined)
                  yield* Deferred.await(ownerRelease)
                  yield* client.unsafe("INSERT INTO busy_retry_test (id, value) VALUES (2, 'owner')").raw
                  order.push("owner-committed")
                }),
              ),
              Duration.seconds(5),
            ).pipe(Effect.forkChild({ startImmediately: true }))
            yield* Deferred.await(ownerStarted).pipe(
              Effect.timeoutOrElse({
                duration: "2 seconds",
                orElse: () => Effect.fail(new Error(`${name} deferred owner did not begin during backoff`)),
              }),
            )

            const laterTransaction = (index) =>
              Effect.gen(function* () {
                const requested = yield* Deferred.make()
                const rows = yield* Deferred.make()
                const release = yield* Deferred.make()
                yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
                const acquisition = yield* withFreshRetryWindow(
                  Effect.gen(function* () {
                    const transaction = yield* client
                      .withTransaction(
                        Effect.gen(function* () {
                          const result = yield* client.unsafe("SELECT COUNT(*) FROM busy_retry_test WHERE id = 1").values
                          order.push(`later-${index}-started`)
                          yield* Deferred.succeed(rows, result)
                          yield* Deferred.await(release)
                        }),
                      )
                      .pipe(Effect.forkIn(scope, { startImmediately: true }))
                    yield* Effect.gen(function* () {
                      while (!(yield* retryWindowStatus).sawPermit) yield* Effect.sleep("1 millis")
                    }).pipe(
                      Effect.timeoutOrElse({
                        duration: "2 seconds",
                        orElse: () => Effect.fail(new Error(`${name} late transaction ${index} did not wait`)),
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
                    orElse: () => Effect.fail(new Error(`${name} late transaction ${index} did not arrive`)),
                  }),
                )
                return { requested, rows, release, acquisition }
              })
            const queuedTransactions = yield* Effect.forEach(
              Array.from({ length: 4 }, (_, index) => index),
              laterTransaction,
              { concurrency: "unbounded" },
            )
            holder.exec("COMMIT")
            held = false
            yield* Effect.gen(function* () {
              while (!(yield* retryWindowStatus).sawPermit) yield* Effect.sleep("1 millis")
            }).pipe(
              Effect.timeoutOrElse({
                duration: "2 seconds",
                orElse: () => Effect.fail(new Error(`${name} writer did not request its post-backoff connection turn`)),
              }),
            )
            const writerBudget = yield* retryWindowStatus
            assert.ok(writerBudget.remainingMs > 0, `${name} writer must still have retry budget`)
            assert.equal(writerFinished, false)
            const continuingArrivals = yield* Effect.forEach(
              Array.from({ length: 8 }, (_, index) => index + 4),
              laterTransaction,
              { concurrency: 1 },
            )
            const later = [...queuedTransactions, ...continuingArrivals]
            yield* Deferred.succeed(ownerRelease, undefined)
            const ownerExit = yield* Fiber.await(owner).pipe(
              Effect.timeoutOrElse({
                duration: "2 seconds",
                orElse: () => Effect.fail(new Error(`${name} deferred owner did not commit`)),
              }),
            )
            assert.equal(Exit.isSuccess(ownerExit), true)

            const writerExit = yield* Fiber.await(writer).pipe(
              Effect.timeoutOrElse({
                duration: "2 seconds",
                orElse: () => Effect.fail(new Error(`${name} writer was starved by later transaction arrivals`)),
              }),
            )
            assert.equal(Exit.isSuccess(writerExit), true)
            assert.equal(writerFinished, true)

            const late = yield* Effect.forEach(later, (item) => Fiber.join(item.acquisition), {
              concurrency: "unbounded",
            })
            yield* Effect.forEach(later, (item) => Deferred.succeed(item.release, undefined), {
              concurrency: "unbounded",
            })
            const lateExits = yield* Effect.forEach(
              late,
              (fiber) =>
                Fiber.await(fiber).pipe(
                  Effect.timeoutOrElse({
                    duration: "5 seconds",
                    orElse: () => Effect.fail(new Error(`${name} later deferred transaction did not finish`)),
                  }),
                ),
              { concurrency: "unbounded" },
            )
            assert.ok(lateExits.every(Exit.isSuccess))
            const rows = yield* Effect.forEach(later, (item) => Deferred.await(item.rows), {
              concurrency: "unbounded",
            })
            assert.deepEqual(rows, Array.from({ length: 12 }, () => [[1]]))
            assert.ok(order.indexOf("owner-committed") < order.indexOf("writer-finished"))
          }),
          Duration.seconds(10),
        ),
      ),
    )
    process.stdout.write(`PASS ${name}: retrying writer precedes queued post-backoff transactions\n`)
  } finally {
    if (held) holder.exec("ROLLBACK")
    holder.close()
    await rm(directory, { recursive: true, force: true })
  }
}

const failures = []
process.stdout.write(`runtime=${process.release.name} node=${process.versions.node}\n`)
for (const [name, layer, layerForFilename] of [
  ["core-node", coreNodeLayer({ filename: ":memory:" }), coreNodeLayer],
  ["standalone-node", NodeSqliteClient.layer({ filename: ":memory:" }), NodeSqliteClient.layer],
]) {
  try {
    await verifyLeaseGrantRace(name)
    await verify(name, layer)
    await verifyDeferredReadDuringBegin(name, layerForFilename)
    await verifyStatementRetryDuringBackoff(name, layerForFilename, "ordinary")
    await verifyStatementRetryDuringBackoff(name, layerForFilename, "reserved")
    await verifyWriterFairness(name, layerForFilename)
  } catch (error) {
    failures.push(name)
    process.stderr.write(`FAIL ${name}: ${error instanceof Error ? error.stack : String(error)}\n`)
  }
}
if (failures.length > 0) process.exitCode = 1
