import { afterAll, afterEach, describe, expect, mock, test } from "bun:test"
import path from "path"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Cause, Deferred, Effect, Exit, Fiber } from "effect"

if (process.env.OPENCODE_INTERNAL_PLUGIN_OWNER_CHILD === "1") {
  let internalPluginGate:
    | {
        directory: string
        start: () => void
        wait: Promise<void>
        release: () => void
        finish: () => void
      }
    | undefined

  void mock.module(path.resolve(import.meta.dir, "../../src/plugin/openai/codex.ts"), () => ({
    CodexAuthPlugin: async () => {
      const gate = internalPluginGate
      if (!gate) return {}
      gate.start()
      await gate.wait
      gate.finish()
      return {}
    },
  }))

  const { Worktree } = await import("../../src/worktree")
  const { InstanceBootstrap } = await import("../../src/project/bootstrap")
  const { InstanceStore } = await import("../../src/project/instance-store")
  const { RuntimeFlags } = await import("../../src/effect/runtime-flags")
  const { Git } = await import("../../src/git")
  const { TestInstance, disposeAllInstances } = await import("../fixture/fixture")
  const { awaitWithTimeout, pollWithTimeout, testEffect } = await import("../lib/effect")

  const it = testEffect(
    LayerNode.compile(LayerNode.group([Worktree.node, FSUtil.node, Git.node, CrossSpawnSpawner.node]), [
      [InstanceStore.bootstrapNode, InstanceBootstrap.node],
      [RuntimeFlags.node, RuntimeFlags.layer({ disableDefaultPlugins: false, pure: false })],
    ]),
  )

  afterEach(() => disposeAllInstances())
  afterAll(() => mock.restore())

  describe("Worktree internal plugin boot ownership", () => {
    it.instance(
      "refuses worktree removal while an internal plugin initializer Promise is active",
      () =>
        Effect.gen(function* () {
          const test = yield* TestInstance
          const fs = yield* FSUtil.Service
          const svc = yield* Worktree.Service
          const store = yield* InstanceStore.Service
          const expected = yield* svc.makeWorktreeInfo({ name: "internal-plugin-owner" })
          const started = yield* Deferred.make<void>()
          const finished = yield* Deferred.make<void>()
          let release: (() => void) | undefined
          let directory: string | undefined
          let removing: Fiber.Fiber<boolean, unknown> | undefined
          internalPluginGate = {
            directory: expected.directory,
            start: () => Deferred.doneUnsafe(started, Effect.void),
            wait: new Promise<void>((resolve) => {
              release = resolve
            }),
            release: () => release?.(),
            finish: () => Deferred.doneUnsafe(finished, Effect.void),
          }
          yield* Effect.addFinalizer(() =>
            Effect.gen(function* () {
              internalPluginGate?.release()
              if (removing) {
                yield* Fiber.await(removing).pipe(
                  Effect.timeoutOrElse({ duration: "20 seconds", orElse: () => Effect.succeed(undefined) }),
                  Effect.asVoid,
                )
              }
              internalPluginGate = undefined
              if (directory) yield* svc.remove({ directory }).pipe(Effect.ignore)
            }),
          )

          const info = yield* svc.create({ name: expected.name })
          directory = info.directory
          yield* awaitWithTimeout(Deferred.await(started), "internal plugin initializer did not start", "15 seconds")
          removing = yield* svc
            .remove({ directory: info.directory })
            .pipe(Effect.forkScoped({ startImmediately: true }))
          const refusal = yield* awaitWithTimeout(
            Fiber.await(removing),
            "Worktree.remove did not refuse the pending internal plugin initializer",
            "20 seconds",
          )
          expect(Exit.isFailure(refusal)).toBe(true)
          if (Exit.isFailure(refusal)) {
            expect(Cause.squash(refusal.cause)).toBeInstanceOf(Worktree.RemoveFailedError)
            expect(Cause.pretty(refusal.cause)).toContain("instance bootstrap Promise did not settle")
          }
          expect(yield* fs.exists(info.directory)).toBe(true)

          internalPluginGate.release()
          yield* awaitWithTimeout(Deferred.await(finished), "internal plugin initializer Promise did not settle")
          const recovered = yield* pollWithTimeout(
            store.load({ directory: info.directory }).pipe(
              Effect.as(true),
              Effect.catchCause(() => Effect.succeed(undefined)),
            ),
            "worktree load did not recover after the internal plugin initializer settled",
            "15 seconds",
          )
          expect(recovered).toBe(true)
          expect(yield* svc.remove({ directory: info.directory })).toBe(true)
          expect(yield* fs.exists(info.directory)).toBe(false)
          removing = undefined
        }),
      { git: true },
      { timeout: 45_000 },
    )
  })
} else {
  test("Codex auth and provider hooks remain real in the shared test process", async () => {
    const codex = await import("../../src/plugin/openai/codex")
    const hooks = await codex.CodexAuthPlugin({} as never)
    expect(hooks.auth?.provider).toBe("openai")
    expect(hooks.auth?.loader).toBeFunction()
    expect(hooks.provider?.models).toBeFunction()
  })

  test("runs the held internal plugin initializer in an isolated Bun test process", async () => {
    const child = Bun.spawn(
      [
        process.execPath,
        "test",
        "--timeout",
        "30000",
        "--only-failures",
        "test/project/internal-plugin-owner.test.ts",
        "-t",
        "refuses worktree removal while an internal plugin initializer Promise is active",
      ],
      {
        cwd: path.resolve(import.meta.dir, "../.."),
        env: { ...process.env, OPENCODE_INTERNAL_PLUGIN_OWNER_CHILD: "1" },
        stdout: "pipe",
        stderr: "pipe",
      },
    )
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ])
    if (exitCode !== 0) throw new Error(`isolated internal plugin test failed:\n${stdout}\n${stderr}`)
    expect(exitCode).toBe(0)
  }, 120_000)
}
