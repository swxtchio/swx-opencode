export * as AskEmit from "./emit"

import { Cause, Effect, Exit } from "effect"
import type { SessionAsk } from "@opencode-ai/schema/session-ask"

// `--questions-command` (swxtchio/swx-opencode#286). Only `serve` sets this, from
// the operator's own argv, so neither project config nor anything a model
// writes can enable emission or choose where it goes.
export type Settings = {
  // An executable run directly, never through a shell, with one ask's JSON on stdin.
  readonly command: string
  // The listening server's URL, so a receiver knows where to answer.
  readonly server?: string
}

let settings: Settings | undefined

export function configure(next: Settings | undefined) {
  settings = next
}

export function current() {
  return settings
}

// Delivers one ask to the operator's command. Its exit is a delivery report
// only: it never answers, refuses or blocks the ask, so a failure is logged and
// the request stays pending and answerable.
export const send = Effect.fn("AskEmit.send")(function* (command: string, payload: SessionAsk.Emitted) {
  const exit = yield* Effect.tryPromise(async () => {
    const child = Bun.spawn([command], {
      stdin: new Blob([JSON.stringify(payload) + "\n"]),
      stdout: "ignore",
      stderr: "pipe",
    })
    const stderr = await new Response(child.stderr).text()
    return { code: await child.exited, stderr }
  }).pipe(Effect.exit)
  if (Exit.isFailure(exit)) {
    yield* Effect.logWarning("ask emit failed", { id: payload.id, command, error: Cause.pretty(exit.cause) })
    return
  }
  if (exit.value.code === 0) {
    yield* Effect.logInfo("ask emitted", { id: payload.id })
    return
  }
  yield* Effect.logWarning("ask emit failed", {
    id: payload.id,
    command,
    code: exit.value.code,
    stderr: exit.value.stderr.slice(-2000),
  })
})
