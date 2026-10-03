import { expect, test } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppProcess } from "@opencode-ai/core/process"
import { Effect, Layer } from "effect"
import { TestLLMServer } from "../../lib/llm-server"
import { cleanupExercisePaths } from "./environment"
import { disposeApps } from "./backend"
import { parseOptions } from "./routing"
import { runScenario } from "./runner"
import { runtime } from "./runtime"
import { worktreeCreateScenario } from "./worktree-create"

let failNextWorktreeReset = false

const failedResetAppProcess = Layer.effect(
  AppProcess.Service,
  Effect.gen(function* () {
    const appProcess = yield* AppProcess.Service
    return AppProcess.Service.of({
      ...appProcess,
      run: (command, options) => {
        if (
          failNextWorktreeReset &&
          command._tag === "StandardCommand" &&
          command.command === "git" &&
          command.args[0] === "reset" &&
          command.args[1] === "--hard"
        ) {
          failNextWorktreeReset = false
          return Effect.succeed({
            command: "git reset --hard",
            exitCode: 1,
            stdout: Buffer.alloc(0),
            stderr: Buffer.from("simulated failed worktree boot in exerciser"),
            stdoutTruncated: false,
            stderrTruncated: false,
          } satisfies AppProcess.RunResult)
        }
        return appProcess.run(command, options)
      },
    })
  }),
).pipe(Layer.provide(LayerNode.compile(AppProcess.node)))

test(
  "worktree.create exerciser reports a Failed boot and confirms cleanup while retaining Ready control",
  async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() =>
            Effect.promise(() => disposeApps()).pipe(Effect.andThen(cleanupExercisePaths)),
          )
          yield* Effect.addFinalizer(() => Effect.sync(() => (failNextWorktreeReset = false)).pipe(Effect.asVoid))

          const modules = yield* Effect.promise(() => runtime())
          const options = parseOptions([
            "--mode",
            "effect",
            "--include",
            "worktree.create",
            "--scenario-timeout",
            "20 seconds",
          ])
          const ready = yield* runScenario(options, modules)(worktreeCreateScenario)
          if (ready.status === "fail") return yield* Effect.die(new Error(`Ready control failed: ${ready.message}`))
          expect(ready.status).toBe("pass")

          failNextWorktreeReset = true
          const failedModules = {
            ...modules,
            HttpApiApp: {
              ...modules.HttpApiApp,
              routes: modules.HttpApiApp.createRoutes(undefined, [[AppProcess.node, failedResetAppProcess]]),
            },
            routeMemoMap: Layer.makeMemoMapUnsafe(),
          }
          const failed = yield* runScenario(options, failedModules)(worktreeCreateScenario)
          expect(failNextWorktreeReset).toBe(false)
          expect(failed.status).toBe("fail")
          if (failed.status === "fail") {
            expect(failed.message).toContain("worktree boot failed")
            expect(failed.message).toContain("cleanup confirmed")
          }
        }),
      ).pipe(Effect.provide(TestLLMServer.layer)),
    )
  },
  { timeout: 45_000 },
)
