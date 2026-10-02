import { describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { InstanceRef } from "../../src/effect/instance-ref"
import { registerDisposer } from "../../src/effect/instance-registry"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
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

  it.live("disposeDirectory settles a reload interrupted in its held disposer", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const store = yield* InstanceStore.Service
      const disposing = yield* Deferred.make<void>()
      const releaseDispose = yield* Deferred.make<() => void>()
      const disposeFinished = yield* Deferred.make<void>()
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
      const reload = yield* store.reload({ directory: dir }).pipe(Effect.forkScoped)
      yield* awaitWithTimeout(Deferred.await(disposing), "reload did not reach its held disposer")

      const removing = yield* store.disposeDirectory(dir).pipe(Effect.forkScoped)
      const removal = yield* Effect.exit(
        awaitWithTimeout(Fiber.join(removing), "disposeDirectory did not finish the interrupted reload", "8 seconds"),
      )
      const reloaded = yield* Effect.exit(
        awaitWithTimeout(Fiber.await(reload), "reload caller remained blocked after its worker exited", "2 seconds"),
      )

      const release = yield* Deferred.await(releaseDispose)
      yield* Effect.sync(release)
      yield* awaitWithTimeout(Deferred.await(disposeFinished), "held disposer did not finish")
      const next = yield* awaitWithTimeout(store.load({ directory: dir }), "reload left a poisoned cache entry")

      expect(Exit.isSuccess(removal)).toBe(true)
      expect(Exit.isSuccess(reloaded)).toBe(true)
      if (Exit.isSuccess(reloaded)) expect(Exit.isFailure(reloaded.value)).toBe(true)
      expect(next.directory).toBe(dir)
      expect(first.directory).toBe(dir)
      expect(disposed).toEqual([dir])
    }),
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
