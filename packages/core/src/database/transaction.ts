export * as DatabaseTransaction from "./transaction"

import { Cause, Context, Effect, Exit, Fiber, Option, Scope } from "effect"
import type { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
type Transaction = Parameters<Parameters<Database["transaction"]>[0]>[0]

class Notifications extends Context.Service<
  Notifications,
  { owner: object; effects?: Effect.Effect<void>[]; completions?: Effect.Effect<void>[] }
>()("@opencode/Database/TransactionNotifications") {}

// Notification custody follows actual Core database/savepoint outcomes, without domain hooks in the generic adapter.
export function afterCommit(effect: Effect.Effect<void>, beforeObservers = false) {
  return Effect.gen(function* () {
    const frame = yield* Effect.serviceOption(Notifications)
    if (Option.isNone(frame) || !frame.value.effects) return yield* effect
    if (beforeObservers) frame.value.completions?.push(effect)
    else frame.value.effects.push(effect)
  })
}

export function wrap<T extends Database>(db: T, scope: Scope.Scope) {
  const owner = {}
  const decorate = (target: Database | Transaction) => {
    const transaction = target.transaction.bind(target) as Database["transaction"]
    target.transaction = ((operation, config) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const parent = yield* Effect.serviceOption(Notifications)
          const frame = { owner, effects: [] as Effect.Effect<void>[], completions: [] as Effect.Effect<void>[] }
          const result = yield* transaction((tx) => {
            decorate(tx)
            return restore(operation(tx)).pipe(Effect.provideService(Notifications, frame))
          }, config).pipe(Effect.exit)
          if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause)
          if (Option.isSome(parent) && parent.value.owner === owner && parent.value.effects) {
            parent.value.effects.push(...frame.effects)
            parent.value.completions?.push(...frame.completions)
            return result.value
          }
          // The connection has been released. Bind committed state before any observer can yield.
          const completed = yield* Effect.forEach(frame.completions, (effect) => Effect.exit(effect)).pipe(
            Effect.provideService(Notifications, { owner }),
          )
          const notify = Effect.gen(function* () {
            const exits = yield* Effect.forEach(frame.effects, (effect) => Effect.exit(effect))
            const causes = [...completed, ...exits].flatMap((exit) => (Exit.isFailure(exit) ? [exit.cause] : []))
            if (causes.length) return yield* Effect.failCause(causes.reduce(Cause.combine, Cause.empty))
          }).pipe(Effect.provideService(Notifications, { owner }), Effect.scoped)
          if (frame.effects.length) {
            // Caller cancellation can stop waiting, but committed publication belongs to the Database lifetime.
            const publication = yield* notify.pipe(
              Effect.interruptible,
              Effect.forkIn(scope, { startImmediately: true }),
            )
            yield* restore(Fiber.join(publication))
          } else yield* notify
          return result.value
        }),
      )) as Database["transaction"]
  }
  decorate(db)
  return db
}
