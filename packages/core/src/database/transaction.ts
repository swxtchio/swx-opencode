export * as DatabaseTransaction from "./transaction"

import { Cause, Context, Effect, Exit, Option } from "effect"
import type { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
type Transaction = Parameters<Parameters<Database["transaction"]>[0]>[0]

class Notifications extends Context.Service<Notifications, { owner: object; effects?: Effect.Effect<void>[] }>()(
  "@opencode/Database/TransactionNotifications",
) {}

// Notification custody follows actual Core database/savepoint outcomes, without domain hooks in the generic adapter.
export function afterCommit(effect: Effect.Effect<void>) {
  return Effect.gen(function* () {
    const frame = yield* Effect.serviceOption(Notifications)
    if (Option.isNone(frame) || !frame.value.effects) return yield* effect
    frame.value.effects.push(effect)
  })
}

export function wrap<T extends Database>(db: T) {
  const owner = {}
  const decorate = (target: Database | Transaction) => {
    const transaction = target.transaction.bind(target) as Database["transaction"]
    target.transaction = ((operation, config) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const parent = yield* Effect.serviceOption(Notifications)
          const frame = { owner, effects: [] as Effect.Effect<void>[] }
          const result = yield* transaction((tx) => {
            decorate(tx)
            return restore(operation(tx)).pipe(Effect.provideService(Notifications, frame))
          }, config).pipe(Effect.exit)
          if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause)
          if (Option.isSome(parent) && parent.value.owner === owner && parent.value.effects) {
            parent.value.effects.push(...frame.effects)
            return result.value
          }
          // The adapter has committed and released its connection before callbacks can read the database again.
          // An explicitly interrupting listener remains a failure, but cannot discard later committed notifications.
          const exits = yield* Effect.forEach(frame.effects, (effect) => Effect.exit(effect)).pipe(
            Effect.provideService(Notifications, { owner }),
          )
          const causes = exits.flatMap((exit) => (Exit.isFailure(exit) ? [exit.cause] : []))
          if (causes.length) return yield* Effect.failCause(causes.reduce(Cause.combine, Cause.empty))
          return result.value
        }),
      )) as Database["transaction"]
  }
  decorate(db)
  return db
}
