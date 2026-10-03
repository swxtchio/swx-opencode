import { describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { InstancePromise } from "../../src/effect/instance-promise"
import { InstanceRef } from "../../src/effect/instance-ref"
import { hasInstancePromises, registerDisposer } from "../../src/effect/instance-registry"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import type { InstanceContext } from "../../src/project/instance-context"
import { TestInstance, tmpdirScoped } from "../fixture/fixture"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"

let bootstrapRun: Effect.Effect<void> = Effect.void
const noopBootstrap = Layer.succeed(
  InstanceBootstrap.Service,
  InstanceBootstrap.Service.of({ run: Effect.suspend(() => bootstrapRun) }),
)

const it = testEffect(
  LayerNode.compile(LayerNode.group([InstanceStore.node, CrossSpawnSpawner.node]), [
    [InstanceStore.bootstrapNode, noopBootstrap],
  ]),
)

const setBootstrap = (run: Effect.Effect<void>) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      bootstrapRun = run
    }),
    () =>
      Effect.sync(() => {
        bootstrapRun = Effect.void
      }),
  )

const registerDisposerScoped = (disposer: (directory: string) => Promise<void>) =>
  Effect.acquireRelease(
    Effect.sync(() => registerDisposer(disposer)),
    (off) => Effect.sync(off),
  )

describe("InstanceStore", () => {
  it.live("loads instance context", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const ctx = yield* store.load({ directory: dir })

      expect(ctx.directory).toBe(dir)
      expect(ctx.worktree).toBe(dir)
    }),
  )

  it.live("runs bootstrap with InstanceRef provided", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      let initializedDirectory: string | undefined

      yield* setBootstrap(
        Effect.gen(function* () {
          initializedDirectory = (yield* InstanceRef)?.directory
        }),
      )
      yield* store.load({ directory: dir })

      expect(initializedDirectory).toBe(dir)
    }),
  )

  it.live("stops bootstrap Promise ownership when awaited bootstrap work completes", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const startChild = yield* Deferred.make<void>()
      const childOwnerStarted = yield* Deferred.make<void>()
      let releasePromise = () => {}
      let child: Fiber.Fiber<void, never> | undefined

      yield* setBootstrap(
        Effect.gen(function* () {
          child = yield* Effect.gen(function* () {
            yield* Deferred.await(startChild)
            yield* InstancePromise.from(
              () =>
                new Promise<void>((resolve) => {
                  releasePromise = resolve
                  queueMicrotask(() => Deferred.doneUnsafe(childOwnerStarted, Effect.void))
                }),
            )
          }).pipe(Effect.forkDetach({ startImmediately: true }))
        }),
      )
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(startChild, undefined)
          releasePromise()
          if (child) yield* Fiber.await(child).pipe(Effect.asVoid)
        }),
      )

      const ready = yield* store.load({ directory: dir })
      yield* Deferred.succeed(startChild, undefined)
      yield* awaitWithTimeout(Deferred.await(childOwnerStarted), "delayed child Promise did not start")
      expect(hasInstancePromises(dir)).toBe(false)
      expect(yield* store.load({ directory: dir })).toBe(ready)
      releasePromise()
      if (child) expect(Exit.isSuccess(yield* Fiber.await(child))).toBe(true)
    }),
  )

  it.live("caches loaded instance context by directory", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      let initialized = 0

      yield* setBootstrap(
        Effect.sync(() => {
          initialized++
        }),
      )
      const first = yield* store.load({ directory: dir })
      const second = yield* store.load({ directory: dir })

      expect(second).toBe(first)
      expect(initialized).toBe(1)
    }),
  )

  it.live("dedupes concurrent loads while init is in flight", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let initialized = 0

      yield* setBootstrap(
        Effect.gen(function* () {
          initialized++
          yield* Deferred.succeed(started, undefined)
          yield* Deferred.await(release)
        }),
      )
      const first = yield* store.load({ directory: dir }).pipe(Effect.forkScoped)

      yield* Deferred.await(started)

      yield* setBootstrap(
        Effect.sync(() => {
          initialized++
        }),
      )
      const second = yield* store.load({ directory: dir }).pipe(Effect.forkScoped)

      expect(initialized).toBe(1)
      yield* Deferred.succeed(release, undefined)

      const [firstCtx, secondCtx] = yield* Effect.all([Fiber.join(first), Fiber.join(second)])
      expect(secondCtx).toBe(firstCtx)
      expect(initialized).toBe(1)
    }),
  )

  it.live("disposeDirectory stops and settles a pending instance load", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()

      yield* setBootstrap(
        Effect.gen(function* () {
          yield* Deferred.succeed(started, undefined)
          yield* Deferred.await(release)
        }),
      )
      yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))

      const loading = yield* store.load({ directory: dir }).pipe(Effect.forkScoped)
      yield* awaitWithTimeout(Deferred.await(started), "instance bootstrap did not start")
      yield* awaitWithTimeout(
        store.disposeDirectory(dir),
        "disposeDirectory waited for a pending instance load",
        "8 seconds",
      )

      const exit = yield* Fiber.await(loading)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
    { timeout: 15_000 },
  )

  it.live("disposeAll interrupts a pending instance load", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()

      yield* setBootstrap(
        Effect.gen(function* () {
          yield* Deferred.succeed(started, undefined)
          yield* Deferred.await(release)
        }),
      )
      yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))

      const loading = yield* store.load({ directory: dir }).pipe(Effect.forkScoped)
      yield* awaitWithTimeout(Deferred.await(started), "instance bootstrap did not start")
      yield* awaitWithTimeout(store.disposeAll(), "disposeAll waited for a pending instance load", "8 seconds")

      const exit = yield* Fiber.await(loading)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
    { timeout: 15_000 },
  )

  it.live(
    "continues disposing healthy directories around an unsettled bootstrap owner and recovers after release",
    () =>
      Effect.gen(function* () {
        const store = yield* InstanceStore.Service
        const badFirst = yield* tmpdirScoped({ git: true })
        const healthyLater = yield* tmpdirScoped({ git: true })
        const healthyFirst = yield* tmpdirScoped({ git: true })
        const badLater = yield* tmpdirScoped({ git: true })
        const healthyLaterDisposed = yield* Deferred.make<void>()
        const healthyFirstDisposed = yield* Deferred.make<void>()
        const signals = new Map<string, Deferred.Deferred<void>>([
          [healthyLater, healthyLaterDisposed],
          [healthyFirst, healthyFirstDisposed],
        ])
        const disposed: string[] = []
        const makeOwner = (directory: string, started: Deferred.Deferred<void>) => {
          let release = () => {}
          return {
            directory,
            started,
            promise: new Promise<void>((resolve) => (release = resolve)),
            release: () => release(),
          }
        }
        let heldOwner: ReturnType<typeof makeOwner> | undefined
        let firstLoading: Fiber.Fiber<InstanceContext, never> | undefined
        let secondLoading: Fiber.Fiber<InstanceContext, never> | undefined
        let firstDisposal: Fiber.Fiber<void, never> | undefined
        let secondDisposal: Fiber.Fiber<void, never> | undefined
        yield* setBootstrap(
          Effect.gen(function* () {
            const owner = heldOwner
            if (!owner || (yield* InstanceRef)?.directory !== owner.directory) return
            yield* InstancePromise.from(() => {
              Deferred.doneUnsafe(owner.started, Effect.void)
              return owner.promise
            })
          }),
        )
        yield* registerDisposerScoped(async (directory) => {
          disposed.push(directory)
          const signal = signals.get(directory)
          if (!signal) return
          Deferred.doneUnsafe(signal, Effect.void)
        })
        yield* Effect.addFinalizer(() => Effect.sync(() => heldOwner?.release()))

        const firstStarted = yield* Deferred.make<void>()
        heldOwner = makeOwner(badFirst, firstStarted)
        firstLoading = yield* store.load({ directory: badFirst }).pipe(Effect.forkScoped({ startImmediately: true }))
        yield* awaitWithTimeout(Deferred.await(firstStarted), "first bootstrap owner did not start")
        const firstOwnerTracked = yield* pollWithTimeout(
          Effect.sync(() => (hasInstancePromises(badFirst) ? true : undefined)),
          "first bootstrap Promise was not tracked",
        )
        expect(firstOwnerTracked).toBe(true)
        yield* store.load({ directory: healthyLater })

        firstDisposal = yield* store.disposeAll().pipe(Effect.forkScoped({ startImmediately: true }))
        yield* awaitWithTimeout(
          Deferred.await(healthyLaterDisposed),
          "disposeAll skipped the healthy directory inserted after the unsettled owner",
          "20 seconds",
        )
        const firstFailure = yield* awaitWithTimeout(
          Fiber.await(firstDisposal),
          "disposeAll did not report the first directory's incomplete cleanup",
          "5 seconds",
        )
        expect(Exit.isFailure(firstFailure)).toBe(true)
        if (Exit.isFailure(firstFailure)) expect(Cause.pretty(firstFailure.cause)).toContain("failed to dispose 1 instance(s)")
        expect(disposed).toEqual([healthyLater])
        expect(hasInstancePromises(badFirst)).toBe(true)

        heldOwner.release()
        expect(Exit.isSuccess(yield* Fiber.await(firstLoading))).toBe(true)
        firstLoading = undefined
        const firstRecovery = yield* Effect.exit(
          awaitWithTimeout(store.disposeAll(), "disposeAll did not recover after the first owner settled", "15 seconds"),
        )
        expect(Exit.isSuccess(firstRecovery)).toBe(true)
        expect(disposed).toEqual([healthyLater, badFirst])
        firstDisposal = undefined

        yield* store.load({ directory: healthyFirst })
        const secondStarted = yield* Deferred.make<void>()
        heldOwner = makeOwner(badLater, secondStarted)
        secondLoading = yield* store.load({ directory: badLater }).pipe(Effect.forkScoped({ startImmediately: true }))
        yield* awaitWithTimeout(Deferred.await(secondStarted), "second bootstrap owner did not start")
        const secondOwnerTracked = yield* pollWithTimeout(
          Effect.sync(() => (hasInstancePromises(badLater) ? true : undefined)),
          "second bootstrap Promise was not tracked",
        )
        expect(secondOwnerTracked).toBe(true)

        secondDisposal = yield* store.disposeAll().pipe(Effect.forkScoped({ startImmediately: true }))
        yield* awaitWithTimeout(
          Deferred.await(healthyFirstDisposed),
          "disposeAll skipped the healthy directory inserted before the unsettled owner",
          "20 seconds",
        )
        const secondFailure = yield* awaitWithTimeout(
          Fiber.await(secondDisposal),
          "disposeAll did not report the later directory's incomplete cleanup",
          "20 seconds",
        )
        expect(Exit.isFailure(secondFailure)).toBe(true)
        if (Exit.isFailure(secondFailure)) expect(Cause.pretty(secondFailure.cause)).toContain("failed to dispose 1 instance(s)")
        expect(disposed).toEqual([healthyLater, badFirst, healthyFirst])
        expect(hasInstancePromises(badLater)).toBe(true)

        heldOwner.release()
        expect(Exit.isSuccess(yield* Fiber.await(secondLoading))).toBe(true)
        secondLoading = undefined
        const secondRecovery = yield* Effect.exit(
          awaitWithTimeout(store.disposeAll(), "disposeAll did not recover after the later owner settled", "15 seconds"),
        )
        expect(Exit.isSuccess(secondRecovery)).toBe(true)
        expect(disposed).toEqual([healthyLater, badFirst, healthyFirst, badLater])
        secondDisposal = undefined
      }),
    { timeout: 90_000 },
  )

  it.live(
    "lets a healthy load finish inside the disposal grace before cleaning it",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        const store = yield* InstanceStore.Service
        const started = yield* Deferred.make<void>()
        const releaseBootstrap = yield* Deferred.make<void>()
        const disposing = yield* Deferred.make<void>()
        const releaseDispose = yield* Deferred.make<() => void>()
        const disposeFinished = yield* Deferred.make<void>()
        const disposed: Array<string> = []
        let unregister: (() => void) | undefined

        yield* setBootstrap(
          Effect.gen(function* () {
            yield* Deferred.succeed(started, undefined)
            yield* Deferred.await(releaseBootstrap)
          }),
        )
        yield* Effect.sync(() => {
          unregister = registerDisposer((directory) => {
            if (directory !== dir) return Promise.resolve()
            disposed.push(directory)
            return new Promise<void>((resolve) => {
              Deferred.doneUnsafe(disposing, Effect.void)
              Deferred.doneUnsafe(releaseDispose, Effect.succeed(resolve))
            }).then(() => {
              Deferred.doneUnsafe(disposeFinished, Effect.void)
            })
          })
        })
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            yield* Deferred.succeed(releaseBootstrap, undefined)
            if (yield* Deferred.isDone(releaseDispose)) {
              const release = yield* Deferred.await(releaseDispose)
              yield* Effect.sync(release)
            }
            if (unregister) yield* Effect.sync(unregister)
          }),
        )

        const loading = yield* store.load({ directory: dir }).pipe(Effect.forkScoped)
        yield* awaitWithTimeout(Deferred.await(started), "healthy bootstrap did not start")
        const removing = yield* store.disposeDirectory(dir).pipe(Effect.forkDetach({ startImmediately: true }))
        // Give the concurrent remover a scheduler turn while bootstrap is still held.
        yield* Effect.sleep(250)
        expect(yield* Deferred.isDone(disposing)).toBe(false)
        expect(loading.pollUnsafe()).toBeUndefined()
        yield* Deferred.succeed(releaseBootstrap, undefined)

        const loaded = yield* awaitWithTimeout(
          Fiber.await(loading),
          "load waiter did not complete successfully during the disposal grace",
          "5 seconds",
        )
        expect(Exit.isSuccess(loaded)).toBe(true)
        if (Exit.isSuccess(loaded)) expect(loaded.value.directory).toBe(dir)
        yield* awaitWithTimeout(Deferred.await(disposing), "dispose did not run after the healthy load completed")

        const release = yield* Deferred.await(releaseDispose)
        yield* Effect.sync(release)
        yield* awaitWithTimeout(Deferred.await(disposeFinished), "healthy-load disposer did not finish")
        const removed = yield* awaitWithTimeout(Fiber.await(removing), "disposeDirectory did not finish cleanup")
        expect(Exit.isSuccess(removed)).toBe(true)
        expect(disposed).toEqual([dir])
        if (unregister) {
          yield* Effect.sync(unregister)
          unregister = undefined
        }
      }),
    { timeout: 15_000 },
  )

  it.instance(
    "bounds disposal of a bootstrap that ignores interruption and quarantines later loads",
    () =>
      Effect.gen(function* () {
        const root = yield* TestInstance
        const instance = yield* InstanceRef
        if (!instance) return yield* Effect.die(new Error("test instance context missing"))
        const store = yield* InstanceStore.Service
        const directory = `${root.directory}/non-cooperative-load`
        const input = { directory, project: instance.project, worktree: instance.worktree }
        const started = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const finished = yield* Deferred.make<void>()

        yield* setBootstrap(
          Effect.gen(function* () {
            if ((yield* InstanceRef)?.directory !== directory) return
            yield* Deferred.succeed(started, undefined)
            yield* Effect.uninterruptible(
              Effect.gen(function* () {
                yield* Deferred.await(release)
                yield* Deferred.succeed(finished, undefined)
              }),
            )
          }),
        )
        yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))

        const loading = yield* store.load(input).pipe(Effect.forkScoped)
        yield* awaitWithTimeout(Deferred.await(started), "non-cooperative bootstrap did not start")
        const removing = yield* store.disposeDirectory(directory).pipe(Effect.forkDetach)
        const removal = yield* Effect.exit(
          awaitWithTimeout(
            Fiber.await(removing),
            "disposal did not return after its stop-confirmation bound",
            "20 seconds",
          ),
        )
        expect(Exit.isSuccess(removal)).toBe(true)
        const loadExit = yield* Effect.exit(
          awaitWithTimeout(Fiber.await(loading), "load waiter did not settle after disposal", "2 seconds"),
        )

        const blocked = yield* Effect.exit(
          awaitWithTimeout(
            Effect.exit(store.load(input)),
            "orphaned load retry did not fail promptly",
            "2 seconds",
          ),
        )

        expect(Exit.isSuccess(loadExit)).toBe(true)
        if (Exit.isSuccess(loadExit)) expect(Exit.isFailure(loadExit.value)).toBe(true)
        expect(Exit.isSuccess(blocked)).toBe(true)
        if (Exit.isSuccess(blocked)) {
          expect(Exit.isFailure(blocked.value)).toBe(true)
          if (Exit.isFailure(blocked.value))
            expect(Cause.pretty(blocked.value.cause)).toContain("instance load is still running")
        }

        yield* Deferred.succeed(release, undefined)
        yield* awaitWithTimeout(Deferred.await(finished), "non-cooperative bootstrap did not leave its hold")
        const recovered = yield* pollWithTimeout(
          store.load(input).pipe(
            Effect.as(true),
            Effect.catchCause(() => Effect.succeed(undefined)),
          ),
          "the completed orphan was not cleared for a later load",
        )
        expect(recovered).toBe(true)
        yield* store.disposeDirectory(directory)

        if (Exit.isSuccess(removal)) {
          expect(Exit.isFailure(removal.value)).toBe(true)
          if (Exit.isFailure(removal.value))
            expect(Cause.pretty(removal.value.cause)).toContain("instance load did not stop")
        }
      }),
      { git: true },
      { timeout: 30_000 },
  )

  it.live(
    "keeps a Promise-backed bootstrap quarantined until its Promise settles",
    () =>
      Effect.gen(function* () {
        const directory = yield* tmpdirScoped({ git: true })
        const store = yield* InstanceStore.Service
        const started = yield* Deferred.make<void>()
        const finished = yield* Deferred.make<void>()
        let releasePromise: (() => void) | undefined

        yield* setBootstrap(
          Effect.gen(function* () {
            if ((yield* InstanceRef)?.directory !== directory) return
            yield* Effect.uninterruptible(
              Effect.promise(
                () =>
                  new Promise<void>((resolve) => {
                    releasePromise = resolve
                    Deferred.doneUnsafe(started, Effect.void)
                  }).then(() => {
                    Deferred.doneUnsafe(finished, Effect.void)
                  }),
              ),
            )
          }),
        )
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => releasePromise?.()),
        )

        const loading = yield* store.load({ directory }).pipe(Effect.forkScoped)
        yield* awaitWithTimeout(Deferred.await(started), "Promise-backed bootstrap did not start")
        const removing = yield* store.disposeDirectory(directory).pipe(Effect.forkDetach)
        const removal = yield* awaitWithTimeout(
          Fiber.await(removing),
          "disposeDirectory did not refuse the unsettled bootstrap Promise",
          "15 seconds",
        )
        expect(Exit.isFailure(removal)).toBe(true)
        if (Exit.isFailure(removal))
          expect(Cause.pretty(removal.cause)).toContain("instance load did not stop")
        const loadExit = yield* Fiber.await(loading)
        expect(Exit.isFailure(loadExit)).toBe(true)
        const blocked = yield* Effect.exit(
          awaitWithTimeout(
            Effect.exit(store.load({ directory })),
            "load retried while the bootstrap Promise was still running",
            "2 seconds",
          ),
        )
        expect(Exit.isSuccess(blocked)).toBe(true)
        if (Exit.isSuccess(blocked)) expect(Exit.isFailure(blocked.value)).toBe(true)

        if (!releasePromise) return yield* Effect.die(new Error("bootstrap Promise did not publish its release handle"))
        yield* Effect.sync(releasePromise)
        releasePromise = undefined
        yield* awaitWithTimeout(Deferred.await(finished), "Promise-backed bootstrap did not settle")
        yield* setBootstrap(Effect.void)
        const recovered = yield* pollWithTimeout(
          store.load({ directory }).pipe(
            Effect.as(true),
            Effect.catchCause(() => Effect.succeed(undefined)),
          ),
          "settled bootstrap owner did not complete cleanup",
          "10 seconds",
        )
        expect(recovered).toBe(true)
        yield* store.disposeDirectory(directory)
      }),
    { timeout: 30_000 },
  )

  it.live(
    "bounds new loads until an owned bootstrap Promise and its continuation settle",
    () =>
      Effect.gen(function* () {
        const directory = yield* tmpdirScoped({ git: true })
        const store = yield* InstanceStore.Service
        const promiseStarted = yield* Deferred.make<void>()
        const promiseFinished = yield* Deferred.make<void>()
        const continuationStarted = yield* Deferred.make<void>()
        const releaseContinuation = yield* Deferred.make<void>()
        const continuationFinished = yield* Deferred.make<void>()
        let releasePromise: (() => void) | undefined

        yield* setBootstrap(
          Effect.gen(function* () {
            if ((yield* InstanceRef)?.directory !== directory) return
            yield* InstancePromise.from(
              () =>
                new Promise<void>((resolve) => {
                  releasePromise = resolve
                  Deferred.doneUnsafe(promiseStarted, Effect.void)
                }).then(() => {
                  Deferred.doneUnsafe(promiseFinished, Effect.void)
                }),
            )
            yield* Deferred.succeed(continuationStarted, undefined)
            yield* Deferred.await(releaseContinuation)
            yield* Deferred.succeed(continuationFinished, undefined)
          }),
        )
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            if (releasePromise) yield* Effect.sync(releasePromise)
            yield* Deferred.succeed(releaseContinuation, undefined)
          }),
        )

        const loading = yield* store.load({ directory }).pipe(Effect.forkScoped)
        yield* awaitWithTimeout(Deferred.await(promiseStarted), "tracked config Promise did not start")
        const removing = yield* store.disposeDirectory(directory).pipe(Effect.forkScoped({ startImmediately: true }))
        const removal = yield* awaitWithTimeout(
          Fiber.await(removing),
          "disposal did not refuse while the owned Promise remained active",
          "20 seconds",
        )
        expect(Exit.isFailure(removal)).toBe(true)
        if (Exit.isFailure(removal))
          expect(Cause.pretty(removal.cause)).toContain("instance bootstrap Promise did not settle")

        const blocked = yield* Effect.exit(
          awaitWithTimeout(
            Effect.exit(store.load({ directory })),
            "new load did not reject the still-active owner Promise",
            "3 seconds",
          ),
        )
        expect(Exit.isSuccess(blocked)).toBe(true)
        if (Exit.isSuccess(blocked)) {
          expect(Exit.isFailure(blocked.value)).toBe(true)
          if (Exit.isFailure(blocked.value))
            expect(Cause.pretty(blocked.value.cause)).toContain("instance bootstrap Promise is still running")
        }

        if (!releasePromise) return yield* Effect.die(new Error("tracked Promise did not publish its release handle"))
        yield* Effect.sync(releasePromise)
        releasePromise = undefined
        yield* awaitWithTimeout(Deferred.await(promiseFinished), "tracked Promise did not settle")
        yield* awaitWithTimeout(Deferred.await(continuationStarted), "bootstrap did not resume after Promise settlement")
        const reloadBlocked = yield* Effect.exit(
          awaitWithTimeout(
            Effect.exit(store.reload({ directory })),
            "reload replaced a quarantined predecessor while its continuation was active",
            "3 seconds",
          ),
        )
        expect(Exit.isSuccess(reloadBlocked)).toBe(true)
        if (Exit.isSuccess(reloadBlocked)) {
          expect(Exit.isFailure(reloadBlocked.value)).toBe(true)
          if (Exit.isFailure(reloadBlocked.value))
            expect(Cause.pretty(reloadBlocked.value.cause)).toContain("instance load is still recovering")
        }
        const recovering = yield* Effect.exit(
          awaitWithTimeout(
            Effect.exit(store.load({ directory })),
            "new load joined an indefinitely pending cached deferred",
            "10 seconds",
          ),
        )
        expect(Exit.isSuccess(recovering)).toBe(true)
        if (Exit.isSuccess(recovering)) {
          expect(Exit.isFailure(recovering.value)).toBe(true)
          if (Exit.isFailure(recovering.value))
            expect(Cause.pretty(recovering.value.cause)).toContain("instance load is still recovering")
        }

        yield* Deferred.succeed(releaseContinuation, undefined)
        yield* awaitWithTimeout(Deferred.await(continuationFinished), "bootstrap continuation did not finish")
        const recovered = yield* pollWithTimeout(
          store.load({ directory }).pipe(
            Effect.as(true),
            Effect.catchCause(() => Effect.succeed(undefined)),
          ),
          "new load did not recover after the continuation settled",
          "10 seconds",
        )
        expect(recovered).toBe(true)
        const loaded = yield* Fiber.await(loading)
        expect(Exit.isSuccess(loaded)).toBe(true)
        yield* store.disposeDirectory(directory)
      }),
    { timeout: 40_000 },
  )

  it.live("removes failed loads from the cache", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      let attempts = 0

      yield* setBootstrap(
        Effect.sync(() => {
          attempts++
          throw new Error("init failed")
        }),
      )
      const failed = yield* store.load({ directory: dir }).pipe(
        Effect.as(false),
        Effect.catchCause(() => Effect.succeed(true)),
      )

      expect(failed).toBe(true)

      yield* setBootstrap(
        Effect.sync(() => {
          attempts++
        }),
      )
      const ctx = yield* store.load({ directory: dir })

      expect(ctx.directory).toBe(dir)
      expect(attempts).toBe(2)
    }),
  )

  it.live("reload replaces the cached context", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service

      const first = yield* store.load({ directory: dir })
      const second = yield* store.reload({ directory: dir })
      const cached = yield* store.load({ directory: dir })

      expect(second).not.toBe(first)
      expect(cached).toBe(second)
    }),
  )

  it.live(
    "releases predecessor contexts after successful reloads",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        const store = yield* InstanceStore.Service
        const predecessors = yield* Effect.gen(function* () {
          const reloadAndWeaken = Effect.fnUntraced(function* (previous: InstanceContext) {
            const next = yield* store.reload({ directory: dir })
            return { next, previous: new WeakRef(previous) }
          })
          let current = yield* store.load({ directory: dir })
          const refs: WeakRef<InstanceContext>[] = []

          for (const _ of Array.from({ length: 8 })) {
            const reloaded = yield* reloadAndWeaken(current)
            refs.push(reloaded.previous)
            current = reloaded.next
          }
          return refs
        })

        const collected = yield* Effect.exit(
          pollWithTimeout(
            Effect.sync(() => {
              Bun.gc(true)
              return predecessors.every((previous) => previous.deref() === undefined) ? true : undefined
            }),
            "successful reloads retained predecessor contexts",
            "5 seconds",
          ),
        )
        expect(Exit.isSuccess(collected)).toBe(true)
        if (Exit.isSuccess(collected)) expect(collected.value).toBe(true)
        yield* store.disposeDirectory(dir)
      }),
    { timeout: 15_000 },
  )

  it.live(
    "does not lose an active predecessor when disposing overlapping reloads",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        const store = yield* InstanceStore.Service
        const firstStarted = yield* Deferred.make<void>()
        const releaseFirst = yield* Deferred.make<void>()
        const firstFinished = yield* Deferred.make<void>()
        yield* store.load({ directory: dir })
        yield* setBootstrap(
          Effect.gen(function* () {
            if ((yield* InstanceRef)?.directory !== dir) return
            yield* Deferred.succeed(firstStarted, undefined)
            yield* Effect.uninterruptible(
              Effect.gen(function* () {
                yield* Deferred.await(releaseFirst)
                yield* Deferred.succeed(firstFinished, undefined)
              }),
            )
          }),
        )
        yield* Effect.addFinalizer(() => Deferred.succeed(releaseFirst, undefined).pipe(Effect.asVoid))

        const firstReload = yield* store.reload({ directory: dir }).pipe(Effect.forkScoped)
        yield* awaitWithTimeout(Deferred.await(firstStarted), "first reload did not reach its bootstrap hold")
        const secondReload = yield* store.reload({ directory: dir }).pipe(Effect.forkScoped({ startImmediately: true }))
        const removing = yield* store.disposeDirectory(dir).pipe(Effect.forkScoped({ startImmediately: true }))
        const removal = yield* awaitWithTimeout(
          Fiber.await(removing),
          "disposeDirectory did not refuse an active predecessor before its hold was released",
          "12 seconds",
        )
        expect(Exit.isFailure(removal)).toBe(true)
        if (Exit.isFailure(removal))
          expect(Cause.pretty(removal.cause)).toContain("instance load did not stop")
        expect(yield* Deferred.isDone(firstFinished)).toBe(false)

        yield* Deferred.succeed(releaseFirst, undefined)
        yield* awaitWithTimeout(Deferred.await(firstFinished), "held predecessor did not finish")
        const [firstExit, secondExit] = yield* Effect.all(
          [
            awaitWithTimeout(Fiber.await(firstReload), "first reload did not settle after release"),
            awaitWithTimeout(Fiber.await(secondReload), "second reload did not settle after release"),
          ],
          { concurrency: "unbounded" },
        )
        expect(Exit.isFailure(firstExit)).toBe(true)
        expect(Exit.isFailure(secondExit)).toBe(true)
        const recovered = yield* pollWithTimeout(
          store.load({ directory: dir }).pipe(
            Effect.as(true),
            Effect.catchCause(() => Effect.succeed(undefined)),
          ),
          "reload cleanup left its predecessor cached",
          "5 seconds",
        )
        expect(recovered).toBe(true)
        yield* store.disposeDirectory(dir)
      }),
    { timeout: 25_000 },
  )

  it.live("dispose preserves the ready context's normal cleanup", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const disposed: Array<string> = []
      yield* registerDisposerScoped(async (directory) => {
        disposed.push(directory)
      })

      const first = yield* store.load({ directory: dir })
      yield* store.dispose(first)
      expect(disposed).toEqual([dir])

      const second = yield* store.load({ directory: dir })
      expect(second).not.toBe(first)
    }),
  )

  it.live("keeps overlapping reloads healthy while refusing disposal until the disposer settles", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const disposing = yield* Deferred.make<void>()
      const releaseDispose = yield* Deferred.make<() => void>()
      const disposeFinished = yield* Deferred.make<void>()
      const disposed: Array<string> = []
      let disposeCalls = 0
      let unregister: (() => void) | undefined

      yield* Effect.sync(() => {
        unregister = registerDisposer((directory) => {
          if (directory !== dir) return Promise.resolve()
          disposed.push(directory)
          disposeCalls++
          if (disposeCalls > 1) return Promise.resolve()
          return new Promise<void>((resolve) => {
            Deferred.doneUnsafe(disposing, Effect.void)
            Deferred.doneUnsafe(releaseDispose, Effect.succeed(resolve))
          }).then(() => {
            Deferred.doneUnsafe(disposeFinished, Effect.void)
          })
        })
      })
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          if (yield* Deferred.isDone(releaseDispose)) {
            const release = yield* Deferred.await(releaseDispose)
            yield* Effect.sync(release)
          }
          if (unregister) yield* Effect.sync(unregister)
        }),
      )

      const first = yield* store.load({ directory: dir })
      yield* setBootstrap(Effect.void)
      const reload = yield* store.reload({ directory: dir }).pipe(Effect.forkScoped({ startImmediately: true }))
      yield* awaitWithTimeout(Deferred.await(disposing), "reload did not reach its held disposer")
      const successor = yield* store.reload({ directory: dir }).pipe(Effect.forkScoped({ startImmediately: true }))
      expect(successor.pollUnsafe()).toBeUndefined()

      const removing = yield* store.disposeDirectory(dir).pipe(Effect.forkScoped({ startImmediately: true }))
      const removal = yield* awaitWithTimeout(
        Fiber.await(removing),
        "disposeDirectory did not refuse a held disposer",
        "12 seconds",
      )
      expect(Exit.isFailure(removal)).toBe(true)
      if (Exit.isFailure(removal))
        expect(Cause.pretty(removal.cause)).toContain("instance disposer did not settle")
      expect(reload.pollUnsafe()).toBeUndefined()
      expect(successor.pollUnsafe()).toBeUndefined()
      const joined = yield* store.load({ directory: dir }).pipe(Effect.forkScoped({ startImmediately: true }))
      expect(joined.pollUnsafe()).toBeUndefined()

      const release = yield* Deferred.await(releaseDispose)
      yield* setBootstrap(Effect.void)
      yield* Effect.sync(release)
      yield* awaitWithTimeout(Deferred.await(disposeFinished), "held disposer did not finish")
      const [reloaded, next, loaded] = yield* Effect.all(
        [
          awaitWithTimeout(Fiber.await(reload), "reload owner did not finish after its disposer completed"),
          awaitWithTimeout(Fiber.await(successor), "successor reload did not finish after its predecessor"),
          awaitWithTimeout(Fiber.await(joined), "concurrent load did not join the healthy reload"),
        ],
        { concurrency: "unbounded" },
      )
      expect(Exit.isSuccess(reloaded)).toBe(true)
      expect(Exit.isSuccess(next)).toBe(true)
      expect(Exit.isSuccess(loaded)).toBe(true)
      if (Exit.isSuccess(reloaded) && Exit.isSuccess(next) && Exit.isSuccess(loaded)) {
        expect(next.value).not.toBe(reloaded.value)
        expect(loaded.value).toBe(next.value)
      }
      if (unregister) {
        yield* Effect.sync(unregister)
        unregister = undefined
      }
      yield* store.disposeDirectory(dir)
      expect(first.directory).toBe(dir)
      expect(disposed).toEqual([dir, dir])
    }),
    { timeout: 25_000 },
  )

  it.live("stale dispose does not delete an in-flight reload", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const disposing = yield* Deferred.make<void>()
      const releaseDispose = yield* Deferred.make<() => void>()
      const disposeFinished = yield* Deferred.make<void>()
      const reloading = yield* Deferred.make<void>()
      const releaseReload = yield* Deferred.make<void>()
      const disposed: Array<string> = []
      yield* registerDisposerScoped((directory) => {
        disposed.push(directory)
        return new Promise<void>((resolve) => {
          Deferred.doneUnsafe(disposing, Effect.void)
          Deferred.doneUnsafe(releaseDispose, Effect.succeed(resolve))
        }).then(() => {
          Deferred.doneUnsafe(disposeFinished, Effect.void)
        })
      })
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          if (!(yield* Deferred.isDone(releaseDispose))) return
          const release = yield* Deferred.await(releaseDispose)
          yield* Effect.sync(release)
        }),
      )

      const first = yield* store.load({ directory: dir })
      yield* setBootstrap(
        Effect.gen(function* () {
          yield* Deferred.succeed(reloading, undefined)
          yield* Deferred.await(releaseReload)
        }),
      )
      const reload = yield* store.reload({ directory: dir }).pipe(Effect.forkScoped)

      yield* awaitWithTimeout(Deferred.await(disposing), "reload did not reach its held disposer")
      const staleDispose = yield* store.dispose(first).pipe(Effect.forkScoped)
      const stale = yield* awaitWithTimeout(
        Fiber.await(staleDispose),
        "stale dispose waited on the newer pending entry",
        "2 seconds",
      )
      expect(Exit.isSuccess(stale)).toBe(true)

      const release = yield* Deferred.await(releaseDispose)
      yield* Effect.sync(release)
      yield* awaitWithTimeout(Deferred.await(disposeFinished), "held disposer did not finish")
      yield* awaitWithTimeout(Deferred.await(reloading), "reload did not resume after its disposer")
      yield* Deferred.succeed(releaseReload, undefined)

      const second = yield* Fiber.join(reload)
      yield* Fiber.join(staleDispose)

      expect(disposed).toEqual([dir])
      expect(yield* store.load({ directory: dir })).toBe(second)
    }),
  )

  it.live("dedupes concurrent disposeAll calls", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const disposing = yield* Deferred.make<void>()
      const releaseDispose = yield* Deferred.make<() => void>()
      const disposed: Array<string> = []
      yield* registerDisposerScoped((directory) => {
        disposed.push(directory)
        Deferred.doneUnsafe(disposing, Effect.void)
        return new Promise<void>((resolve) => {
          Deferred.doneUnsafe(releaseDispose, Effect.succeed(resolve))
        })
      })

      yield* store.load({ directory: dir })
      const first = yield* store.disposeAll().pipe(Effect.forkScoped)
      yield* Deferred.await(disposing)
      const release = yield* Deferred.await(releaseDispose)
      const second = yield* store.disposeAll().pipe(Effect.forkScoped)

      expect(disposed).toEqual([dir])
      yield* Effect.sync(release)
      yield* Effect.all([Fiber.join(first), Fiber.join(second)])
      expect(disposed).toEqual([dir])
    }),
  )

  it.live("re-arms disposeAll after completion", () =>
    Effect.gen(function* () {
      const dir1 = yield* tmpdirScoped({ git: true })
      const dir2 = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const disposed: Array<string> = []
      yield* registerDisposerScoped(async (directory) => {
        disposed.push(directory)
      })

      yield* store.load({ directory: dir1 })
      yield* store.disposeAll()
      expect(disposed).toEqual([dir1])

      yield* store.load({ directory: dir2 })
      yield* store.disposeAll()
      expect(disposed).toEqual([dir1, dir2])
    }),
  )
})
