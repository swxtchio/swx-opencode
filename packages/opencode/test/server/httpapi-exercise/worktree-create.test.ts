import { expect, test } from "bun:test"
import { existsSync } from "node:fs"
import path from "node:path"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppProcess } from "@opencode-ai/core/process"
import { Deferred, Effect, Fiber, Layer } from "effect"
import { awaitWithTimeout } from "../../lib/effect"
import { TestLLMServer } from "../../lib/llm-server"
import { cleanupExercisePaths } from "./environment"
import { disposeApps } from "./backend"
import { parseOptions } from "./routing"
import { runScenario } from "./runner"
import { runtime } from "./runtime"
import type { ActiveScenario } from "./types"
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

type ResetStarted = { directory: string; branch: string }
type ResetGate = {
  started: Deferred.Deferred<ResetStarted>
  release: Deferred.Deferred<void>
  interrupted: Deferred.Deferred<void>
  terminated: Deferred.Deferred<void>
}
type RemovalObservation = {
  directory: string
  removed: boolean
  checkoutExists: boolean
  branchExists: boolean
  worktreeListed: boolean
}

const heldResetAppProcess = (gate: ResetGate) =>
  Layer.effect(
    AppProcess.Service,
    Effect.gen(function* () {
      const appProcess = yield* AppProcess.Service
      return AppProcess.Service.of({
        ...appProcess,
        run: (command, options) => {
          const directory = command._tag === "StandardCommand" ? command.options.cwd : undefined
          if (
            command._tag !== "StandardCommand" ||
            command.command !== "git" ||
            command.args[0] !== "reset" ||
            command.args[1] !== "--hard" ||
            !directory
          )
            return appProcess.run(command, options)

          return Effect.gen(function* () {
            const currentBranch = git(directory, ["branch", "--show-current"])
            const branch = currentBranch.stdout.toString().trim()
            if (currentBranch.exitCode !== 0 || !branch)
              return yield* Effect.die(new Error("held worktree reset did not have a current branch"))
            const branchRef = git(directory, ["show-ref", "--verify", "--quiet", `refs/heads/${branch}`])
            if (branchRef.exitCode !== 0)
              return yield* Effect.die(new Error("held worktree reset branch was not created"))

            yield* Deferred.succeed(gate.started, { directory, branch })
            yield* Deferred.await(gate.release).pipe(
              Effect.onInterrupt(() => Deferred.succeed(gate.interrupted, undefined).pipe(Effect.asVoid)),
            )
            return yield* appProcess.run(command, options)
          }).pipe(Effect.ensuring(Deferred.succeed(gate.terminated, undefined).pipe(Effect.asVoid)))
        },
      })
    }),
  ).pipe(Layer.provide(LayerNode.compile(AppProcess.node)))

function git(cwd: string, args: string[]) {
  return Bun.spawnSync(["git", "-C", cwd, ...args], { stdout: "pipe", stderr: "pipe" })
}

function observeRemovals(gate: ResetGate, observations: RemovalObservation[]): ActiveScenario {
  return {
    ...worktreeCreateScenario,
    expect: (ctx, state, result) =>
      worktreeCreateScenario.expect(
        {
          ...ctx,
          worktreeRemove: (directory) =>
            Effect.gen(function* () {
              const removed = yield* ctx.worktreeRemove(directory)
              if (!ctx.directory) return yield* Effect.die(new Error("worktree scenario had no project directory"))
              const started = yield* Deferred.await(gate.started)
              const branch = git(ctx.directory, ["show-ref", "--verify", "--quiet", `refs/heads/${started.branch}`])
              if (branch.exitCode !== 0 && branch.exitCode !== 1)
                return yield* Effect.die(new Error("could not verify the worktree branch after removal"))
              const worktrees = git(ctx.directory, ["worktree", "list", "--porcelain"])
              if (worktrees.exitCode !== 0)
                return yield* Effect.die(new Error("could not list worktrees after removal"))
              const worktreeListed = worktrees.stdout
                .toString()
                .split(/\r?\n/)
                .filter((line) => line.startsWith("worktree "))
                .some((line) => path.resolve(line.slice("worktree ".length)) === path.resolve(directory))
              observations.push({
                directory,
                removed,
                checkoutExists: existsSync(directory),
                branchExists: branch.exitCode === 0,
                worktreeListed,
              })
              return removed
            }),
        },
        state,
        result,
      ),
  }
}

async function runHeldResetScenario(scenarioTimeout: string) {
  return Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const gate: ResetGate = {
          started: yield* Deferred.make<ResetStarted>(),
          release: yield* Deferred.make<void>(),
          interrupted: yield* Deferred.make<void>(),
          terminated: yield* Deferred.make<void>(),
        }
        const removals: RemovalObservation[] = []
        yield* Effect.addFinalizer(() => Effect.promise(() => disposeApps()).pipe(Effect.andThen(cleanupExercisePaths)))
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            yield* Deferred.succeed(gate.release, undefined)
            if (!Deferred.isDoneUnsafe(gate.started)) return
            yield* Deferred.await(gate.terminated).pipe(
              Effect.timeoutOrElse({ duration: "5 seconds", orElse: () => Effect.void }),
              Effect.asVoid,
            )
          }),
        )

        const modules = yield* Effect.promise(() => runtime())
        const options = parseOptions([
          "--mode",
          "effect",
          "--include",
          "worktree.create",
          "--scenario-timeout",
          scenarioTimeout,
        ])
        const heldModules = {
          ...modules,
          HttpApiApp: {
            ...modules.HttpApiApp,
            routes: modules.HttpApiApp.createRoutes(undefined, [[AppProcess.node, heldResetAppProcess(gate)]]),
          },
          routeMemoMap: Layer.makeMemoMapUnsafe(),
        }
        const fiber = yield* runScenario(
          options,
          heldModules,
        )(observeRemovals(gate, removals)).pipe(Effect.forkScoped({ startImmediately: true }))
        const started = yield* awaitWithTimeout(
          Deferred.await(gate.started),
          "worktree boot did not reach the real git reset --hard command",
          "10 seconds",
        )
        const result = yield* awaitWithTimeout(
          Fiber.join(fiber),
          "worktree scenario did not settle after its configured bound",
          scenarioTimeout === "4 seconds" ? "15 seconds" : "35 seconds",
        )
        yield* awaitWithTimeout(
          Deferred.await(gate.interrupted),
          "worktree boot was not interrupted by cleanup",
          "5 seconds",
        )
        yield* awaitWithTimeout(
          Deferred.await(gate.terminated),
          "worktree boot did not terminate after cleanup",
          "5 seconds",
        )
        return { result, started, removals }
      }).pipe(Effect.provide(TestLLMServer.layer)),
    ),
  )
}

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

test(
  "worktree.create scenario deadline interrupts boot and removes checkout and branch",
  async () => {
    const outcome = await runHeldResetScenario("4 seconds")
    expect(outcome.result.status).toBe("fail")
    if (outcome.result.status === "fail") expect(outcome.result.message).toContain("scenario timed out after 4s")
    expect(outcome.started.branch).toBe("opencode/api-dsl")
    expect(
      outcome.removals.some(
        (item) =>
          item.directory === outcome.started.directory &&
          item.removed &&
          !item.checkoutExists &&
          !item.branchExists &&
          !item.worktreeListed,
      ),
    ).toBe(true)
  },
  { timeout: 25_000 },
)

test(
  "worktree.create preserves inner terminal timeout cause and removes checkout and branch",
  async () => {
    const outcome = await runHeldResetScenario("30 seconds")
    expect(outcome.result.status).toBe("fail")
    if (outcome.result.status === "fail")
      expect(outcome.result.message).toContain("worktree boot did not reach a terminal state")
    expect(outcome.started.branch).toBe("opencode/api-dsl")
    expect(
      outcome.removals.some(
        (item) =>
          item.directory === outcome.started.directory &&
          item.removed &&
          !item.checkoutExists &&
          !item.branchExists &&
          !item.worktreeListed,
      ),
    ).toBe(true)
  },
  { timeout: 50_000 },
)
