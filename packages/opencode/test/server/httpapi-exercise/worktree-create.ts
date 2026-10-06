import { Effect, Exit } from "effect"
import { WorktreeEvent } from "@opencode-ai/schema/worktree-event"
import { check, object } from "./assertions"
import { http } from "./dsl"

export const worktreeCreateScenario = http.protected
  .post("/experimental/worktree", "worktree.create")
  .mutating()
  .at((ctx) => ({ path: "/experimental/worktree", headers: ctx.headers(), body: { name: "api-dsl" } }))
  .jsonEffect(
    200,
    (body, ctx) =>
      Effect.gen(function* () {
        object(body)
        if (typeof body.directory !== "string") throw new Error("created worktree should include directory")
        const directory = body.directory
        const terminal = yield* ctx.worktreeTerminal(directory).pipe(
          Effect.onInterrupt(() =>
            ctx.worktreeRemove(directory).pipe(
              Effect.flatMap((removed) =>
                removed
                  ? Effect.void
                  : Effect.logWarning("worktree cleanup found no worktree after scenario interruption", {
                      directory,
                    }),
              ),
              Effect.catchCause((cause) =>
                Effect.logError("worktree cleanup failed after scenario interruption", { cause }),
              ),
            ),
          ),
          Effect.exit,
        )
        const removed = yield* ctx.worktreeRemove(directory).pipe(Effect.exit)
        if (Exit.isFailure(terminal)) {
          if (Exit.isFailure(removed)) {
            yield* Effect.logError("worktree removal failed after boot did not settle", { cause: removed.cause })
          }
          return yield* Effect.failCause(terminal.cause)
        }
        if (terminal.value.payload.type === WorktreeEvent.Failed.type) {
          if (Exit.isFailure(removed)) return yield* Effect.failCause(removed.cause)
          check(removed.value, "failed worktree should be removed after boot failure")
          return yield* Effect.die(
            new Error(`worktree boot failed: ${terminal.value.payload.properties.message}; cleanup confirmed`),
          )
        }
        if (Exit.isFailure(removed)) return yield* Effect.failCause(removed.cause)
        check(removed.value, "created worktree should be removed")
        check(terminal.value.payload.type === WorktreeEvent.Ready.type, "worktree boot should reach ready")
      }),
    "status",
  )
