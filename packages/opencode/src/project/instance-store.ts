import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { makeGlobalNode, Node } from "@opencode-ai/core/effect/app-node"
import { GlobalBus } from "@/bus/global"
import { serviceUse } from "@opencode-ai/core/effect/service-use"
import { WorkspaceContext } from "@/control-plane/workspace-context"
import { InstanceRef } from "@/effect/instance-ref"
import {
  awaitInstancePromises,
  disposeInstance as runDisposers,
  hasInstancePromises,
} from "@/effect/instance-registry"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Cause, Context, Deferred, Duration, Effect, Exit, Fiber, Layer } from "effect"
import { type InstanceContext } from "./instance-context"
import { InstanceBootstrap } from "./bootstrap-service"
import * as Project from "./project"

export interface LoadInput {
  directory: string
  worktree?: string
  project?: Project.Info
}

export interface Interface {
  readonly load: (input: LoadInput) => Effect.Effect<InstanceContext>
  readonly reload: (input: LoadInput) => Effect.Effect<InstanceContext>
  readonly dispose: (ctx: InstanceContext) => Effect.Effect<void>
  readonly disposeDirectory: (directory: string) => Effect.Effect<void>
  readonly disposeAll: () => Effect.Effect<void>
  readonly provide: <A, E, R>(input: LoadInput, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/InstanceStore") {}

export const use = serviceUse(Service)

interface Entry {
  readonly deferred: Deferred.Deferred<InstanceContext>
  readonly loadFiber: Deferred.Deferred<Fiber.Fiber<void>>
  reloadGroup?: ReloadGroup
  context?: InstanceContext
  disposersDone?: boolean
  disposedEventEmitted?: boolean
  previous?: Entry
}

interface ReloadGroup {
  current: Entry
  completed: boolean
}

type PendingLoad = { entry: Entry; fiber: Fiber.Fiber<void> }

type DisposerResult = { success: true } | { success: false; error: unknown }

interface DisposerRun {
  readonly completion: Deferred.Deferred<DisposerResult>
  readonly entries: Set<Entry>
  readonly observers: Set<() => void>
  readonly autoFinalize: Set<Entry>
  reloadGroup?: ReloadGroup
}

const layer: Layer.Layer<Service, never, Project.Service | InstanceBootstrap.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const project = yield* Project.Service
    const bootstrap = yield* InstanceBootstrap.Service
    const cache = new Map<string, Entry>()
    const orphaned = new Map<string, Set<Entry>>()
    const disposerRuns = new Map<string, DisposerRun>()

    const boot = (input: LoadInput & { directory: string }) =>
      Effect.gen(function* () {
        const ctx: InstanceContext =
          input.project && input.worktree
            ? {
                directory: input.directory,
                worktree: input.worktree,
                project: input.project,
              }
            : yield* project.fromDirectory(input.directory).pipe(
                Effect.map((result) => ({
                  directory: input.directory,
                  worktree: result.sandbox,
                  project: result.project,
                })),
              )
        // Bootstrap stays cooperative; a non-cancellable Promise must be owned at its Promise boundary.
        yield* bootstrap.run.pipe(Effect.provideService(InstanceRef, ctx))
        return ctx
      }).pipe(Effect.withSpan("InstanceStore.boot"))

    const removeEntry = (directory: string, entry: Entry) =>
      Effect.sync(() => {
        if (cache.get(directory) !== entry) return false
        cache.delete(directory)
        return true
      })

    const emitDisposedSync = (input: { directory: string; project?: string }) =>
      GlobalBus.emit("event", {
        directory: input.directory,
        project: input.project,
        workspace: WorkspaceContext.workspaceID,
        payload: {
          type: "server.instance.disposed",
          properties: {
            directory: input.directory,
          },
        },
      })

    const completeDisposerRun = (directory: string, run: DisposerRun, result: DisposerResult) => {
      if (disposerRuns.get(directory) === run) disposerRuns.delete(directory)
      if (result.success) {
        run.entries.forEach((entry) => (entry.disposersDone = true))
        run.autoFinalize.forEach((entry) => {
          if (!entry.context || entry.disposedEventEmitted) return
          if (cache.get(directory) === entry) cache.delete(directory)
          entry.disposedEventEmitted = true
          emitDisposedSync({ directory, project: entry.context.project.id })
        })
      }
      Deferred.doneUnsafe(run.completion, Effect.succeed(result))
      run.observers.forEach((observer) => observer())
      run.observers.clear()
    }

    const awaitDisposerRunCompletion = (run: DisposerRun) =>
      Deferred.await(run.completion).pipe(
        Effect.flatMap((result) => (result.success ? Effect.void : Effect.die(result.error))),
      )

    const awaitDisposerRun = (directory: string, run: DisposerRun, duration: Duration.Input = "5 seconds") =>
      awaitDisposerRunCompletion(run).pipe(
        Effect.timeoutOrElse({
          duration,
          orElse: () => Effect.die(new Error(`instance disposer did not settle: ${directory}`)),
        }),
      )

    const startDisposerRun = (directory: string, entry?: Entry, reloadOwner?: Entry) => {
      const current = disposerRuns.get(directory)
      if (entry?.disposersDone && !current) return undefined
      const run = current ?? {
        completion: Deferred.makeUnsafe<DisposerResult>(),
        entries: new Set<Entry>(),
        observers: new Set<() => void>(),
        autoFinalize: new Set<Entry>(),
      }
      if (entry) run.entries.add(entry)
      if (reloadOwner?.reloadGroup) run.reloadGroup = reloadOwner.reloadGroup
      if (!current) {
        disposerRuns.set(directory, run)
        // The JS Promise is the cleanup owner; interrupting an Effect waiter does not stop it.
        void runDisposers(directory).then(
          () => completeDisposerRun(directory, run, { success: true }),
          (error: unknown) => completeDisposerRun(directory, run, { success: false, error }),
        )
      }
      return run
    }

    const trackDisposerWait = (directory: string, run: DisposerRun, entry: Entry | undefined, wait: Effect.Effect<void>) =>
      wait.pipe(
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            if (entry && disposerRuns.get(directory) === run) run.autoFinalize.add(entry)
          }),
        ),
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            if (entry && disposerRuns.get(directory) === run) {
              yield* Effect.sync(() => run.autoFinalize.add(entry))
            }
            return yield* Effect.failCause(cause)
          }),
        ),
      )

    const awaitDisposerRunTracked = (directory: string, run: DisposerRun, entry?: Entry) =>
      trackDisposerWait(directory, run, entry, awaitDisposerRun(directory, run))

    const awaitDisposerRunOwned = (directory: string, run: DisposerRun, entry?: Entry) =>
      trackDisposerWait(directory, run, entry, awaitDisposerRunCompletion(run))

    const runDisposersTracked = Effect.fnUntraced(function* (directory: string, entry?: Entry) {
      const run = yield* Effect.sync(() => startDisposerRun(directory, entry))
      if (!run) return
      yield* awaitDisposerRunTracked(directory, run, entry)
    })

    const runDisposersOwned = Effect.fnUntraced(function* (directory: string, entry: Entry, reloadOwner: Entry) {
      const run = yield* Effect.sync(() => startDisposerRun(directory, entry, reloadOwner))
      if (!run) return
      yield* awaitDisposerRunOwned(directory, run, entry)
    })

    const waitForDisposers = Effect.fnUntraced(function* (directory: string) {
      const run = disposerRuns.get(directory)
      if (!run) return
      yield* awaitDisposerRun(directory, run, "8 seconds")
    })

    const settleTrackedPromises = (
      directory: string,
      pending: PendingLoad[],
    ): Effect.Effect<PendingLoad[]> =>
      Effect.gen(function* () {
        if (!hasInstancePromises(directory)) return pending
        yield* Effect.promise(() => awaitInstancePromises(directory)).pipe(
          Effect.timeoutOrElse({
            duration: "8 seconds",
            orElse: () => Effect.die(new Error(`instance bootstrap Promise did not settle: ${directory}`)),
          }),
        )
        if (pending.length === 0) return pending
        const settled = yield* Effect.forEach(
          pending,
          (item) =>
            Fiber.await(item.fiber).pipe(
              Effect.timeoutOrElse({ duration: "5 seconds", orElse: () => Effect.succeed(undefined) }),
            ),
          { concurrency: "unbounded" },
        )
        const remaining = pending.filter((_, index) => settled[index] === undefined)
        if (hasInstancePromises(directory)) return yield* settleTrackedPromises(directory, remaining)
        return remaining
      })

    const completeEntry = (directory: string, entry: Entry, work: Effect.Effect<InstanceContext>) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const exit = yield* Effect.exit(restore(work))
          if (yield* Deferred.isDone(entry.deferred)) {
            if (Exit.isSuccess(exit)) entry.context = exit.value
            const run = yield* Effect.sync(() => startDisposerRun(directory, entry))
            if (run) yield* Effect.sync(() => run.autoFinalize.add(entry)).pipe(Effect.asVoid)
            if (entry.reloadGroup?.current === entry) entry.reloadGroup.completed = true
            return
          }
          if (Exit.isFailure(exit)) yield* removeEntry(directory, entry)
          else entry.context = exit.value
          yield* Deferred.done(entry.deferred, exit).pipe(Effect.asVoid)
          if (Exit.isSuccess(exit)) entry.previous = undefined
          if (entry.reloadGroup?.current === entry) entry.reloadGroup.completed = true
        }),
      )

    const emitDisposed = (input: { directory: string; project?: string }, entry?: Entry) =>
      Effect.sync(() => {
        if (entry?.disposedEventEmitted) return
        emitDisposedSync(input)
        if (entry) entry.disposedEventEmitted = true
      })

    const disposeContext = Effect.fn("InstanceStore.disposeContext")(
      function* (ctx: InstanceContext, entry?: Entry, run?: DisposerRun) {
        yield* Effect.logInfo("disposing instance", { directory: ctx.directory })
        if (run) yield* awaitDisposerRunTracked(ctx.directory, run, entry)
        else yield* runDisposersTracked(ctx.directory, entry)
        yield* emitDisposed({ directory: ctx.directory, project: ctx.project.id }, entry)
      },
    )

    const disposeEntry = Effect.fnUntraced(function* (directory: string, entry: Entry, ctx: InstanceContext) {
      if (cache.get(directory) !== entry) return false
      yield* disposeContext(ctx, entry)
      if (cache.get(directory) !== entry) return false
      cache.delete(directory)
      return true
    })

    const abandonLoads = Effect.fnUntraced(function* (
      directory: string,
      loads: Array<{ entry: Entry; fiber?: Fiber.Fiber<void> }>,
      reason: string,
    ) {
      const error = new Error(`instance load ${reason}: ${directory}`)
      yield* Effect.sync(() => {
        const blocked = orphaned.get(directory) ?? new Set<Entry>()
        const disposal = disposerRuns.get(directory)
        const interruptor = Fiber.getCurrent()?.id
        for (const load of loads) {
          const fiberPending = !load.fiber || load.fiber.pollUnsafe() === undefined
          if (fiberPending || disposal) {
            blocked.add(load.entry)
            const removeOrphan = () => {
              const current = orphaned.get(directory)
              if (!current) return
              current.delete(load.entry)
              if (current.size === 0) orphaned.delete(directory)
            }
            if (load.fiber && fiberPending) load.fiber.addObserver(removeOrphan)
            disposal?.observers.add(removeOrphan)
            if (load.fiber && fiberPending) load.fiber.interruptUnsafe(interruptor)
          }
          if (cache.get(directory) === load.entry) cache.delete(directory)
          if (!Deferred.isDoneUnsafe(load.entry.deferred)) {
            Deferred.doneUnsafe(load.entry.deferred, Effect.die(error))
          }
        }
        if (blocked.size > 0) orphaned.set(directory, blocked)
      })
      return yield* Effect.die(error)
    })

    const settleLoad = Effect.fnUntraced(function* (directory: string, entry: Entry) {
      if (orphaned.has(directory))
        return yield* Effect.die(new Error(`instance load is still running for ${directory}`))
      const entries: Entry[] = []
      for (let current: Entry | undefined = entry; current; current = current.previous) entries.push(current)
      yield* waitForDisposers(directory)
      if (yield* Deferred.isDone(entry.deferred)) return yield* Deferred.await(entry.deferred).pipe(Effect.exit)
      const fibers = yield* Effect.forEach(
        entries,
        (item) =>
          Deferred.await(item.loadFiber).pipe(
            Effect.timeoutOrElse({ duration: "5 seconds", orElse: () => Effect.succeed(undefined) }),
          ),
        { concurrency: "unbounded" },
      )
      const unregistered = entries.flatMap((item, index) => (fibers[index] ? [] : [{ entry: item }]))
      if (unregistered.length > 0) return yield* abandonLoads(directory, unregistered, "did not register")

      const registered = entries.flatMap((item, index) => {
        const fiber = fibers[index]
        return fiber ? [{ entry: item, fiber }] : []
      })
      const settled = yield* Effect.forEach(
        registered,
        (item) =>
          Fiber.await(item.fiber).pipe(
            Effect.timeoutOrElse({ duration: "5 seconds", orElse: () => Effect.succeed(undefined) }),
          ),
        { concurrency: "unbounded" },
      )
      const pending = registered.filter((_, index) => settled[index] === undefined)
      const pendingAfterPromises = yield* settleTrackedPromises(directory, pending)
      if (pendingAfterPromises.length > 0) {
        yield* Effect.sync(() => {
          const interruptor = Fiber.getCurrent()?.id
          pendingAfterPromises.forEach((item) => item.fiber.interruptUnsafe(interruptor))
        })
        const stopped = yield* Effect.forEach(
          pendingAfterPromises,
          (item) =>
            Fiber.await(item.fiber).pipe(
              Effect.timeoutOrElse({ duration: "5 seconds", orElse: () => Effect.succeed(undefined) }),
            ),
          { concurrency: "unbounded" },
        )
        const unconfirmed = pendingAfterPromises.filter((_, index) => stopped[index] === undefined)
        if (unconfirmed.length > 0) return yield* abandonLoads(directory, unconfirmed, "did not stop")
      }

      yield* waitForDisposers(directory)
      const unresolved = entries.filter((item) => !Deferred.isDoneUnsafe(item.deferred))
      if (unresolved.length > 0) {
        return yield* abandonLoads(
          directory,
          unresolved.map((item) => ({
            entry: item,
            fiber: fibers[entries.indexOf(item)],
          })),
          "stopped without publishing its exit",
        )
      }
      return yield* Deferred.await(entry.deferred).pipe(Effect.exit)
    })

    const load = (input: LoadInput): Effect.Effect<InstanceContext> => {
      const directory = FSUtil.resolve(input.directory)
      return Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (orphaned.has(directory)) {
            return yield* Effect.die(new Error(`instance load is still running for ${directory}`))
          }
          const existing = cache.get(directory)
          const disposal = disposerRuns.get(directory)
          const reloadGroup = existing?.reloadGroup
          if (
            disposal &&
            (!existing ||
              !reloadGroup ||
              reloadGroup.completed ||
              reloadGroup.current !== existing ||
              disposal.reloadGroup !== reloadGroup)
          ) {
            return yield* Effect.die(new Error(`instance disposal is still running for ${directory}`))
          }
          if (existing) return yield* restore(Deferred.await(existing.deferred))

          const entry: Entry = {
            deferred: Deferred.makeUnsafe<InstanceContext>(),
            loadFiber: Deferred.makeUnsafe<Fiber.Fiber<void>>(),
          }
          cache.set(directory, entry)
          // Admission is masked; the detached producer must still observe later stop requests.
          const fiber = yield* completeEntry(
            directory,
            entry,
            Effect.gen(function* () {
              yield* Effect.logInfo("creating instance", { directory: directory })
              return yield* boot({ ...input, directory })
            }),
          ).pipe(Effect.interruptible, Effect.forkDetach({ startImmediately: true }))
          yield* Deferred.succeed(entry.loadFiber, fiber)
          return yield* restore(Deferred.await(entry.deferred))
        }),
      ).pipe(Effect.withSpan("InstanceStore.load"))
    }

    const reload = (input: LoadInput): Effect.Effect<InstanceContext> => {
      const directory = FSUtil.resolve(input.directory)
      return Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (orphaned.has(directory)) {
            return yield* Effect.die(new Error(`instance load is still running for ${directory}`))
          }
          const previous = cache.get(directory)
          const continuation =
            previous?.reloadGroup &&
            !previous.reloadGroup.completed &&
            previous.reloadGroup.current === previous
              ? previous.reloadGroup
              : undefined
          const disposal = disposerRuns.get(directory)
          if (disposal && (!continuation || disposal.reloadGroup !== continuation)) {
            return yield* Effect.die(new Error(`instance disposal is still running for ${directory}`))
          }
          const entry: Entry = {
            deferred: Deferred.makeUnsafe<InstanceContext>(),
            loadFiber: Deferred.makeUnsafe<Fiber.Fiber<void>>(),
            ...(previous ? { previous } : {}),
          }
          const reloadGroup = continuation ?? { current: entry, completed: false }
          entry.reloadGroup = reloadGroup
          reloadGroup.current = entry
          cache.set(directory, entry)
          // Keep reload producers interruptible after the cache handoff, just like load producers.
          const fiber = yield* completeEntry(
            directory,
            entry,
            Effect.gen(function* () {
              yield* Effect.logInfo("reloading instance", { directory: directory })
              if (entry.previous) {
                yield* Deferred.await(entry.previous.deferred).pipe(Effect.ignore)
                yield* runDisposersOwned(directory, entry.previous, entry)
                yield* emitDisposed({ directory, project: input.project?.id }, entry.previous)
              }
              return yield* boot({ ...input, directory })
            }),
          ).pipe(Effect.interruptible, Effect.forkDetach({ startImmediately: true }))
          yield* Deferred.succeed(entry.loadFiber, fiber)
          return yield* restore(Deferred.await(entry.deferred))
        }),
      ).pipe(Effect.withSpan("InstanceStore.reload"))
    }

    const dispose = Effect.fn("InstanceStore.dispose")(function* (ctx: InstanceContext) {
      if (orphaned.has(ctx.directory))
        return yield* Effect.die(new Error(`instance load is still running for ${ctx.directory}`))
      const entry = cache.get(ctx.directory)
      if (!entry) return yield* disposeContext(ctx)
      if (entry.context !== ctx) return

      const exit = yield* settleLoad(ctx.directory, entry)
      if (Exit.isFailure(exit)) return yield* removeEntry(ctx.directory, entry).pipe(Effect.asVoid)
      if (exit.value !== ctx) return
      yield* disposeEntry(ctx.directory, entry, ctx).pipe(Effect.asVoid)
    })

    const disposeDirectory = Effect.fn("InstanceStore.disposeDirectory")(function* (input: string) {
      const directory = FSUtil.resolve(input)
      if (orphaned.has(directory))
        return yield* Effect.die(new Error(`instance load is still running for ${directory}`))
      yield* waitForDisposers(directory)
      const entry = cache.get(directory)
      if (!entry) return
      const exit = yield* settleLoad(directory, entry)
      if (Exit.isFailure(exit)) return yield* removeEntry(directory, entry).pipe(Effect.asVoid)
      yield* disposeEntry(directory, entry, exit.value).pipe(Effect.asVoid)
    })

    const disposeAllOnce = Effect.fnUntraced(function* () {
      yield* Effect.logInfo("disposing all instances")
      yield* Effect.forEach(
        [...cache.entries()],
        (item) =>
          Effect.gen(function* () {
            const exit = yield* settleLoad(item[0], item[1]).pipe(
              Effect.catchCause((cause) =>
                Effect.gen(function* () {
                  yield* Effect.logWarning("instance dispose failed", { key: item[0], cause })
                  return undefined
                }),
              ),
            )
            if (!exit) return
            if (Exit.isFailure(exit)) {
              yield* Effect.logWarning("instance dispose failed", { key: item[0], cause: exit.cause })
              yield* removeEntry(item[0], item[1])
              return
            }
            yield* disposeEntry(item[0], item[1], exit.value)
          }),
        { discard: true },
      )
    })

    const cachedDisposeAll = yield* Effect.cachedWithTTL(disposeAllOnce(), Duration.zero)
    const disposeAll = Effect.fn("InstanceStore.disposeAll")(function* () {
      return yield* cachedDisposeAll
    })

    const provide = <A, E, R>(input: LoadInput, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
      load(input).pipe(Effect.flatMap((ctx) => effect.pipe(Effect.provideService(InstanceRef, ctx))))

    yield* Effect.addFinalizer(() => disposeAll().pipe(Effect.ignore))

    return Service.of({
      load,
      reload,
      dispose,
      disposeDirectory,
      disposeAll,
      provide,
    })
  }),
)

export const bootstrapNode = LayerNode.unbound(InstanceBootstrap.Service, Node.tags.values.global)

export const node = makeGlobalNode({
  service: Service,
  layer: layer,
  deps: [Project.node, bootstrapNode],
})

export * as InstanceStore from "./instance-store"
