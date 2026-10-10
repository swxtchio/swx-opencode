import { Cause, Context, Effect, Tracer } from "effect"

// Where a failure came from, for diagnostics only. Keyed by the failure object, so the attribution follows that
// failure through Cause squashing and later work cannot overwrite it; the failure itself is never changed.
export type Origin = {
  readonly event?: string
  readonly stage?: "handler" | "stream"
  readonly case?: string
  readonly callSite?: string
}

const origins = new WeakMap<object, Origin>()

// The innermost recording wins each field, and the call site is the span chain where the failure was first seen.
export const record = (cause: Cause.Cause<unknown>, origin: Omit<Origin, "callSite">) =>
  Effect.withFiber((fiber) => {
    if (Cause.hasInterruptsOnly(cause)) return Effect.void
    const failure = Cause.squash(cause)
    if (typeof failure !== "object" || failure === null) return Effect.void
    const callSite = spanChain(fiber.context)
    origins.set(failure, { ...origin, ...(callSite && { callSite }), ...origins.get(failure) })
    return Effect.void
  })

export const get = (failure: unknown) =>
  typeof failure === "object" && failure !== null ? origins.get(failure) : undefined

function spanChain(context: Context.Context<never>) {
  const names: string[] = []
  const parent = Context.getOption(context, Tracer.ParentSpan)
  let span = parent._tag === "Some" ? parent.value : undefined
  while (span?._tag === "Span" && names.length < 6) {
    names.push(span.name)
    span = span.parent._tag === "Some" ? span.parent.value : undefined
  }
  return names.length ? names.join(" < ") : undefined
}

export * as FailureOrigin from "./failure-origin"
