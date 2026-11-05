import assert from "node:assert/strict"
import { Deferred, Effect, Exit, Fiber, References, Scope, Semaphore } from "effect"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { acquirePermitLease, makeStatementGate } from "../../../effect-sqlite-node/src/retry.ts"

export const verifyLeaseGrantRace = async (name) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        for (const maxOps of [3, 4, 8, 16, 64]) {
          for (let offset = 0; offset < 64; offset++) {
            yield* Effect.scoped(
              Effect.gen(function* () {
                const semaphore = yield* Semaphore.make(1)
                const gate = yield* makeStatementGate
                const held = yield* Deferred.make()
                const release = yield* Deferred.make()
                yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
                const holder = yield* semaphore
                  .withPermits(1)(Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))))
                  .pipe(Effect.forkChild({ startImmediately: true }))
                yield* Deferred.await(held)

                const scope = yield* Scope.make()
                yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
                const operation = { sawContention: false }
                // Preserve the d61 retry-priority call on the old implementation; the fixed gate ignores these removed flags.
                const acquisition = yield* Effect.uninterruptibleMask((restore) =>
                  Effect.gen(function* () {
                    const lease = yield* restore(
                      gate.withExclusive(acquirePermitLease(semaphore, 1, operation), operation, true),
                    )
                    yield* Scope.addFinalizer(scope, Scope.close(lease, Exit.void))
                    return lease
                  }),
                ).pipe(
                  Effect.provideService(References.MaxOpsBeforeYield, maxOps),
                  Effect.forkChild({ startImmediately: true }),
                )
                while (!operation.sawContention) yield* Effect.yieldNow

                const contender = yield* gate
                  .withStatement(
                    Effect.scoped(
                      acquirePermitLease(semaphore).pipe(Effect.flatMap((lease) => Scope.close(lease, Exit.void))),
                    ),
                    { sawContention: false },
                    true,
                  )
                  .pipe(
                    Effect.provideService(References.MaxOpsBeforeYield, maxOps),
                    Effect.forkChild({ startImmediately: true }),
                  )
                yield* Deferred.succeed(release, undefined)
                for (let step = 0; step < offset; step++) yield* Effect.yieldNow

                const acquired = yield* Fiber.await(acquisition).pipe(Effect.timeoutOption("300 millis"))
                if (acquired._tag === "None") {
                  const permit = yield* semaphore.withPermitsIfAvailable(1)(Effect.succeed("available"))
                  throw new Error(
                    `${name} lost a granted permit at maxOps=${maxOps} offset=${offset}; contender=${contender.pollUnsafe()?._tag ?? "waiting"}; holder=${holder.pollUnsafe()?._tag ?? "waiting"}; permit=${permit._tag}`,
                  )
                }
                assert.equal(Exit.isSuccess(acquired.value), true, `${name} acquisition failed at maxOps=${maxOps} offset=${offset}`)

                yield* Scope.close(scope, Exit.void)
                const contended = yield* Fiber.await(contender).pipe(Effect.timeoutOption("300 millis"))
                assert.notEqual(contended._tag, "None", `${name} contender stalled at maxOps=${maxOps} offset=${offset}`)
                if (contended._tag === "Some") assert.equal(Exit.isSuccess(contended.value), true)
                yield* Fiber.join(holder)
                const available = yield* semaphore.withPermitsIfAvailable(1)(Effect.succeed("available"))
                assert.equal(available._tag, "Some", `${name} permit remains held at maxOps=${maxOps} offset=${offset}`)
              }),
            ).pipe(
              Effect.timeoutOrElse({
                duration: "1 second",
                orElse: () => Effect.fail(new Error(`${name} lease-race case timed out at maxOps=${maxOps} offset=${offset}`)),
              }),
            )
          }
          process.stdout.write(`PASS ${name} lease grant race maxOps=${maxOps}: 64 scheduler offsets\n`)
        }
      }),
    ),
  )

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  await verifyLeaseGrantRace(process.argv[2] ?? "standalone")
