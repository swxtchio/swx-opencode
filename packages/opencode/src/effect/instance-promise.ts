import { Effect } from "effect"
import { trackInstancePromise } from "./instance-registry"
import { InstanceBootstrapRef, InstanceRef } from "./instance-ref"

export function from<A>(make: () => Promise<A>) {
  return Effect.gen(function* () {
    const instance = yield* InstanceRef
    const owner = yield* InstanceBootstrapRef
    const promise = make()
    if (instance && owner?.active) trackInstancePromise(instance.directory, promise)
    // The Effect waiter stays interruptible; the registry retains the Promise owner until it settles.
    return yield* Effect.promise(() => promise)
  })
}

export function tryPromise<A, E>(options: { try: () => Promise<A>; catch: (error: unknown) => E }) {
  return Effect.gen(function* () {
    const instance = yield* InstanceRef
    const owner = yield* InstanceBootstrapRef
    return yield* Effect.tryPromise({
      try: () => {
        const promise = options.try()
        if (instance && owner?.active) trackInstancePromise(instance.directory, promise)
        return promise
      },
      catch: options.catch,
    })
  })
}

export * as InstancePromise from "./instance-promise"
