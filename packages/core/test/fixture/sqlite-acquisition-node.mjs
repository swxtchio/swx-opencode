import assert from "node:assert/strict"
import { Context, Deferred, Effect, Fiber, Layer, References } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { layer as coreNodeLayer } from "../../src/database/sqlite.node.ts"
import { NodeSqliteClient } from "../../../effect-sqlite-node/src/index.ts"

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
                  orElse: () => Effect.fail(new Error(`${name} holder did not acquire the permit`)),
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

const failures = []
process.stdout.write(`node=${process.versions.node}\n`)
for (const [name, layer] of [
  ["core-node", coreNodeLayer({ filename: ":memory:" })],
  ["standalone-node", NodeSqliteClient.layer({ filename: ":memory:" })],
]) {
  try {
    await verify(name, layer)
  } catch (error) {
    failures.push(name)
    process.stderr.write(`FAIL ${name}: ${error instanceof Error ? error.stack : String(error)}\n`)
  }
}
if (failures.length > 0) process.exitCode = 1
