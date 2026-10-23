export * as SessionRunCoordinator from "./run-coordinator"

import { Deferred, Effect, Exit, Fiber, FiberSet, Scope } from "effect"

export type WaitRegistration = {
  readonly waitID: string
  readonly sessionID: string
  readonly admittedSeq?: number
  readonly messageID?: string
}

/** Serializes execution for each key while allowing different keys to run concurrently. */
export interface Coordinator<Key, E> {
  /** Snapshots keys with an execution owned by this coordinator. */
  readonly active: Effect.Effect<ReadonlySet<Key>>
  /** Starts execution while idle or joins the active execution. */
  readonly run: (key: Key) => Effect.Effect<void, E>
  /** Joins one active drain without starting execution. */
  readonly join: (key: Key, registration?: WaitRegistration) => Effect.Effect<Exit.Exit<void, E> | undefined>
  /** Registers one coalesced follow-up after newly recorded work. */
  readonly wake: (key: Key) => Effect.Effect<void>
  /**
   * Runs `operation` as the key's execution once no drain owns it. A drain never overlaps it, and work recorded while
   * it runs drains after it.
   */
  readonly exclusive: <A, E2>(key: Key, operation: Effect.Effect<A, E2>) => Effect.Effect<A, E2>
  /** Stops active execution and waits for its cleanup. */
  readonly interrupt: (key: Key) => Effect.Effect<void>
}

type Entry<E> = {
  readonly done: Deferred.Deferred<void, E>
  settled?: Deferred.Deferred<Exit.Exit<void, E>>
  owner?: Fiber.Fiber<void, never>
  pendingWake: boolean
  stopping: boolean
}

export const make = <Key, E>(options: {
  readonly drain: (key: Key, force: boolean) => Effect.Effect<void, E>
}): Effect.Effect<Coordinator<Key, E>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const active = new Map<Key, Entry<E>>()
    const waiters = new Map<Key, Map<string, WaitRegistration>>()
    const fork = yield* FiberSet.makeRuntime<never, void, never>()

    const makeEntry = (): Entry<E> => ({
      done: Deferred.makeUnsafe<void, E>(),
      pendingWake: false,
      stopping: false,
    })

    const start = (
      key: Key,
      entry: Entry<E>,
      force: boolean,
      successor = false,
      work: Effect.Effect<void, E> = Effect.suspend(() => options.drain(key, force)),
    ) => {
      const ready = Deferred.makeUnsafe<void>()
      const settled = Deferred.makeUnsafe<Exit.Exit<void, E>>()
      entry.settled = settled
      const owner = fork(
        (successor ? Effect.yieldNow : Deferred.await(ready)).pipe(
          Effect.andThen(work),
          Effect.onExit((exit) => Effect.sync(() => settle(key, entry, settled, exit))),
          Effect.exit,
          Effect.asVoid,
        ),
      )
      entry.owner = owner
      if (!successor) Deferred.doneUnsafe(ready, Effect.void)
    }

    const settle = (
      key: Key,
      entry: Entry<E>,
      settled: Deferred.Deferred<Exit.Exit<void, E>>,
      exit: Exit.Exit<void, E>,
    ) => {
      Deferred.doneUnsafe(settled, Effect.succeed(exit))
      if (Exit.isSuccess(exit) && !entry.stopping && entry.pendingWake) {
        entry.pendingWake = false
        start(key, entry, false, true)
        return
      }

      const successor = entry.pendingWake ? makeEntry() : undefined
      if (successor === undefined) active.delete(key)
      else {
        active.set(key, successor)
        start(key, successor, false, true)
      }
      Deferred.doneUnsafe(entry.done, exit)
    }

    const run = (key: Key): Effect.Effect<void, E> =>
      Effect.uninterruptibleMask((restore) => {
        const entry = active.get(key)
        if (entry !== undefined) {
          if (entry.stopping) return restore(Deferred.await(entry.done).pipe(Effect.andThen(run(key))))
          return restore(Deferred.await(entry.done))
        }

        const next = makeEntry()
        active.set(key, next)
        start(key, next, true)
        return restore(Deferred.await(next.done))
      })

    const join = (key: Key, registration?: WaitRegistration) =>
      Effect.suspend(() => {
        const settled = active.get(key)?.settled
        if (settled === undefined) return Effect.succeed(undefined)
        if (registration === undefined) return Deferred.await(settled)

        const current = waiters.get(key) ?? new Map<string, WaitRegistration>()
        current.set(registration.waitID, registration)
        waiters.set(key, current)
        return Effect.gen(function* () {
          yield* Effect.logInfo("Session wait registered", {
            event: "session.wait.registered",
            wait_id: registration.waitID,
            session_id: registration.sessionID,
            admitted_seq: registration.admittedSeq,
            message_id: registration.messageID,
            waiter_count: current.size,
          })
          const exit = yield* Deferred.await(settled)
          yield* Effect.logInfo("Session wait join resolved", {
            event: "session.wait.join_resolved",
            wait_id: registration.waitID,
            session_id: registration.sessionID,
            admitted_seq: registration.admittedSeq,
            message_id: registration.messageID,
            outcome: Exit.isSuccess(exit) ? "success" : "failure",
          })
          return exit
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              current.delete(registration.waitID)
              if (current.size === 0) waiters.delete(key)
            }),
          ),
        )
      })

    const wake = (key: Key) =>
      Effect.sync(() => {
        const entry = active.get(key)
        if (entry !== undefined) {
          entry.pendingWake = true
          return
        }

        const next = makeEntry()
        active.set(key, next)
        start(key, next, false)
      })

    const exclusive = <A, E2>(key: Key, operation: Effect.Effect<A, E2>): Effect.Effect<A, E2> =>
      Effect.uninterruptibleMask((restore) => {
        const entry = active.get(key)
        if (entry !== undefined)
          return restore(Deferred.await(entry.done).pipe(Effect.exit, Effect.andThen(exclusive(key, operation))))

        const next = makeEntry()
        active.set(key, next)
        const result = Deferred.makeUnsafe<A, E2>()
        // The operation's own outcome goes to its caller; the entry settles as a successful execution either way.
        start(
          key,
          next,
          false,
          false,
          operation.pipe(
            Effect.onExit((exit) => Deferred.done(result, exit)),
            Effect.ignore,
          ),
        )
        return restore(Deferred.await(result))
      })

    const interrupt = (key: Key): Effect.Effect<void> =>
      Effect.suspend(() => {
        const entry = active.get(key)
        if (entry?.owner === undefined) return Effect.void
        entry.stopping = true
        entry.pendingWake = false
        return Fiber.interrupt(entry.owner)
      })

    return { active: Effect.sync(() => new Set(active.keys())), run, join, wake, exclusive, interrupt }
  })
