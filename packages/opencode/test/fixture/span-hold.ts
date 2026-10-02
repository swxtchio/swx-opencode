import { Deferred, Effect, Fiber, Option, Tracer } from "effect"

// Effect's run loop evaluates each primitive through a tracer's `context` hook under this key.
const evaluate = "~effect/Effect/evaluate"

type Hold = {
  readonly name: string
  readonly parent: string
  readonly reached: Deferred.Deferred<void>
  readonly release: Deferred.Deferred<void>
}

/**
 * Holds the fiber that ends span `name` under parent span `parent` before its next step, until released.
 *
 * A Session write such as `Session.patch` reads the Session and then commits in one fiber with no observable step
 * between them, so this is how a test lets a removal land after that read without adding a hook to production code.
 * Run the producer with `tracer`, `arm` a hold, await its `reached`, act, then `release` it.
 */
export function spanHold() {
  const state = {
    armed: undefined as Hold | undefined,
    held: undefined as { readonly fiber: Fiber.Fiber<unknown, unknown>; readonly hold: Hold } | undefined,
  }
  const tracer = Tracer.make({
    span(options) {
      const span = new Tracer.NativeSpan(options)
      const hold = state.armed
      if (
        hold === undefined ||
        options.name !== hold.name ||
        !Option.exists(options.parent, (parent) => parent._tag === "Span" && parent.name === hold.parent)
      )
        return span
      const end = span.end.bind(span)
      span.end = (endTime, exit) => {
        end(endTime, exit)
        const fiber = Fiber.getCurrent()
        if (state.armed !== hold || fiber === undefined) return
        state.armed = undefined
        state.held = { fiber, hold }
      }
      return span
    },
    context(primitive, fiber) {
      const held = state.held
      const step = primitive as unknown as Effect.Effect<unknown> & Record<typeof evaluate, (fiber: unknown) => never>
      if (held === undefined || held.fiber !== fiber) return step[evaluate](fiber)
      state.held = undefined
      Deferred.doneUnsafe(held.hold.reached, Effect.void)
      // Returned as the fiber's next step: wait for release, then continue with the step it was about to take.
      return Deferred.await(held.hold.release).pipe(Effect.andThen(step)) as never
    },
  })
  const arm = (input: { readonly name: string; readonly parent: string }) =>
    Effect.gen(function* () {
      const hold = { ...input, reached: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      state.armed = hold
      return {
        reached: Deferred.await(hold.reached),
        release: Deferred.succeed(hold.release, undefined),
      }
    })
  return { tracer, arm }
}
