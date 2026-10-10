import { Cause, Context, Effect } from "effect"

// Where a failure came from, for diagnostics only. Keyed by the failure object, so the attribution follows that
// failure through Cause squashing and later work cannot overwrite it; the failure itself is never changed.
export type Origin = {
  readonly event?: string
  readonly stage?: "handler" | "stream"
  readonly case?: string
  readonly callSite?: string
}

const origins = new WeakMap<object, Origin>()

// The innermost recording wins each field. The call site is the stack frame chain the runtime captured where the
// failure was raised, so it names the failing callable even when it is recorded after those scopes have unwound or
// across a stream boundary.
export const record = (cause: Cause.Cause<unknown>, origin: Omit<Origin, "callSite">) =>
  Effect.sync(() => {
    if (Cause.hasInterruptsOnly(cause)) return
    const failure = Cause.squash(cause)
    if (typeof failure !== "object" || failure === null) return
    const callSite = frameChain(cause)
    origins.set(failure, { ...origin, ...(callSite && { callSite }), ...origins.get(failure) })
  })

export const get = (failure: unknown) =>
  typeof failure === "object" && failure !== null ? origins.get(failure) : undefined

function frameChain(cause: Cause.Cause<unknown>) {
  const names: string[] = []
  const captured = Context.getOption(Cause.annotations(cause), Cause.StackTrace)
  let frame = captured._tag === "Some" ? captured.value : undefined
  while (frame && names.length < 8) {
    // Effect.fn records a definition-site frame beside each call frame; the call frames name the path.
    if (!frame.name.endsWith(" (definition)")) names.push(frame.name)
    frame = frame.parent
  }
  return names.length ? names.join(" < ") : undefined
}

export * as FailureOrigin from "./failure-origin"
