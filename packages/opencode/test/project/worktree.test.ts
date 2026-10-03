import { afterEach, describe, expect } from "bun:test"
import { createWriteStream } from "node:fs"
import path from "path"
import { pathToFileURL } from "url"
import { finished } from "node:stream/promises"
import { mkdir, symlink } from "node:fs/promises"
import { AppProcess } from "@opencode-ai/core/process"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import { GlobalBus, type GlobalEvent } from "../../src/bus/global"
import { hasInstancePromises, registerDisposer } from "../../src/effect/instance-registry"
import { InstanceRef } from "../../src/effect/instance-ref"
import { Git } from "../../src/git"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import type { InstanceContext } from "../../src/project/instance-context"
import { InstanceStore } from "../../src/project/instance-store"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Plugin } from "../../src/plugin"
import { Worktree } from "../../src/worktree"
import { TestConfig } from "../fixture/config"
import { disposeAllInstances, provideInstance, TestInstance, tmpdirScoped } from "../fixture/fixture"
import { PluginGate } from "../fixture/plugin-gate"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"

const it = testEffect(
  LayerNode.compile(LayerNode.group([Worktree.node, FSUtil.node, Git.node, CrossSpawnSpawner.node]), [
    [InstanceStore.bootstrapNode, InstanceBootstrap.node],
  ]),
)
const wintest = process.platform !== "win32" ? it.instance : it.instance.skip

type BootControl =
  | {
      mode: "hold"
      directory: string
      started: Deferred.Deferred<void>
      release: Deferred.Deferred<void>
      interrupted: Deferred.Deferred<void>
    }
  | { mode: "fail"; directory: string }

let bootControl: BootControl | undefined

const gatedAppProcess = Layer.effect(
  AppProcess.Service,
  Effect.gen(function* () {
    const appProcess = yield* AppProcess.Service
    return AppProcess.Service.of({
      ...appProcess,
      run: (command, options) => {
        const control = bootControl
        if (
          !control ||
          command._tag !== "StandardCommand" ||
          command.command !== "git" ||
          command.args[0] !== "reset" ||
          command.args[1] !== "--hard" ||
          command.options.cwd !== control.directory
        ) {
          return appProcess.run(command, options)
        }
        if (control.mode === "fail") {
          return Effect.succeed({
            command: "git reset --hard",
            exitCode: 1,
            stdout: Buffer.alloc(0),
            stderr: Buffer.from("simulated worktree checkout failure"),
            stdoutTruncated: false,
            stderrTruncated: false,
          } satisfies AppProcess.RunResult)
        }
        return Effect.gen(function* () {
          yield* Deferred.succeed(control.started, undefined)
          yield* Deferred.await(control.release).pipe(
            Effect.onInterrupt(() => Deferred.succeed(control.interrupted, undefined).pipe(Effect.asVoid)),
          )
          return yield* appProcess.run(command, options)
        })
      },
    })
  }),
).pipe(Layer.provide(LayerNode.compile(AppProcess.node)))

const raceIt = testEffect(
  LayerNode.compile(LayerNode.group([Worktree.node, FSUtil.node, Git.node]), [
    [InstanceStore.bootstrapNode, InstanceBootstrap.node],
    [AppProcess.node, gatedAppProcess],
  ]),
)
let failedBootstrapDirectory: string | undefined
const failedBootstrap = Layer.succeed(
  InstanceBootstrap.Service,
  InstanceBootstrap.Service.of({
    run: Effect.gen(function* () {
      if ((yield* InstanceRef)?.directory === failedBootstrapDirectory) {
        return yield* Effect.die(new Error("simulated instance bootstrap failure"))
      }
    }),
  }),
)
const failedBootstrapIt = testEffect(
  LayerNode.compile(LayerNode.group([Worktree.node, FSUtil.node, Git.node]), [
    [InstanceStore.bootstrapNode, failedBootstrap],
  ]),
)
let controlledBootstrapRun: Effect.Effect<void> = Effect.void
const controlledBootstrap = Layer.succeed(
  InstanceBootstrap.Service,
  InstanceBootstrap.Service.of({ run: Effect.suspend(() => controlledBootstrapRun) }),
)
const nonCooperativeBootstrapIt = testEffect(
  LayerNode.compile(LayerNode.group([Worktree.node, FSUtil.node, Git.node]), [
    [InstanceStore.bootstrapNode, controlledBootstrap],
  ]),
)
let pluginSpec: string | undefined
let pluginSource: string | undefined
const pluginTestConfig = TestConfig.layer({
  get: () =>
    Effect.succeed({
      plugin: pluginSpec ? [pluginSpec] : [],
      plugin_origins:
        pluginSpec && pluginSource ? [{ spec: pluginSpec, source: pluginSource, scope: "local" as const }] : [],
    }),
  directories: () => Effect.succeed([]),
})
const pluginBootstrapIt = testEffect(
  LayerNode.compile(LayerNode.group([Worktree.node, FSUtil.node, Git.node, Plugin.node]), [
    [Config.node, pluginTestConfig],
    [RuntimeFlags.node, RuntimeFlags.layer({ disableDefaultPlugins: false, pure: false })],
    [InstanceStore.bootstrapNode, InstanceBootstrap.node],
  ]),
)

function normalize(input: string) {
  return input.replace(/\\/g, "/").toLowerCase()
}

const waitReady = Effect.fn("WorktreeTest.waitReady")(function* () {
  const ready = yield* Deferred.make<{ name: string; branch?: string }>()
  const on = (evt: GlobalEvent) => {
    if (evt.payload.type !== Worktree.Event.Ready.type) return
    Deferred.doneUnsafe(ready, Effect.succeed(evt.payload.properties))
  }

  GlobalBus.on("event", on)
  yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", on)))

  return yield* Deferred.await(ready).pipe(
    Effect.timeoutOrElse({
      duration: "10 seconds",
      orElse: () => Effect.fail(new Error("timed out waiting for worktree.ready")),
    }),
  )
})

const subscribeTerminal = (directory: string) =>
  Effect.gen(function* () {
    const terminal = yield* Deferred.make<GlobalEvent>()
    const on = (evt: GlobalEvent) => {
      if (evt.directory !== directory) return
      if (evt.payload.type !== Worktree.Event.Ready.type && evt.payload.type !== Worktree.Event.Failed.type) return
      Deferred.doneUnsafe(terminal, Effect.succeed(evt))
    }

    GlobalBus.on("event", on)
    yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", on)))
    return terminal
  })

const removeCreatedWorktree = (directory: string) =>
  Effect.gen(function* () {
    const svc = yield* Worktree.Service
    const ok = yield* svc.remove({ directory })
    if (!ok) return yield* Effect.fail(new Error(`failed to remove worktree ${directory}`))
  })

const withCreatedWorktree = <A, E, R>(
  input: Parameters<Worktree.Interface["create"]>[0],
  use: (created: { info: Worktree.Info; ready: { name: string; branch?: string } }) => Effect.Effect<A, E, R>,
) =>
  Effect.acquireUseRelease(
    Effect.gen(function* () {
      const svc = yield* Worktree.Service
      const ready = yield* waitReady().pipe(Effect.forkScoped)
      const info = yield* svc.create(input)
      const props = yield* Fiber.join(ready)
      return { info, ready: props }
    }),
    use,
    ({ info }) => removeCreatedWorktree(info.directory),
  )

const git = Effect.fn("WorktreeTest.git")(function* (cwd: string, args: string[]) {
  const service = yield* Git.Service
  const result = yield* service.run(args, { cwd })
  if (result.exitCode !== 0) throw new Error(`git ${args.join(" ")} failed: ${result.stderr.toString("utf8")}`)
  return result.text()
})

const gitResult = Effect.fn("WorktreeTest.gitResult")(function* (cwd: string, args: string[]) {
  const service = yield* Git.Service
  return yield* service.run(args, { cwd })
})

describe("Worktree", () => {
  afterEach(() => disposeAllInstances())

  describe("makeWorktreeInfo", () => {
    it.instance(
      "returns info with name, branch, and directory",
      () =>
        Effect.gen(function* () {
          const svc = yield* Worktree.Service
          const info = yield* svc.makeWorktreeInfo()

          expect(info.name).toBeDefined()
          expect(typeof info.name).toBe("string")
          expect(info.branch).toBe(`opencode/${info.name}`)
          expect(info.directory).toContain(info.name)
        }),
      { git: true },
    )

    it.instance(
      "uses provided name as base",
      () =>
        Effect.gen(function* () {
          const svc = yield* Worktree.Service
          const info = yield* svc.makeWorktreeInfo({ name: "my-feature" })

          expect(info.name).toBe("my-feature")
          expect(info.branch).toBe("opencode/my-feature")
        }),
      { git: true },
    )

    it.instance(
      "slugifies the provided name",
      () =>
        Effect.gen(function* () {
          const svc = yield* Worktree.Service
          const info = yield* svc.makeWorktreeInfo({ name: "My Feature Branch!" })

          expect(info.name).toBe("my-feature-branch")
        }),
      { git: true },
    )

    it.instance(
      "omits branch for detached info",
      () =>
        Effect.gen(function* () {
          const test = yield* TestInstance
          const svc = yield* Worktree.Service
          yield* git(test.directory, ["branch", "opencode/my-feature"])

          const info = yield* svc.makeWorktreeInfo({ name: "my-feature", detached: true })

          expect(info.name).toBe("my-feature")
          expect(info.branch).toBeUndefined()
        }),
      { git: true },
    )

    it.instance("fails with NotGitError for non-git directories", () =>
      Effect.gen(function* () {
        const svc = yield* Worktree.Service
        const exit = yield* Effect.exit(svc.makeWorktreeInfo())

        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause)
          expect(error).toBeInstanceOf(Worktree.NotGitError)
          if (error instanceof Worktree.NotGitError) expect(error._tag).toBe("WorktreeNotGitError")
        }
      }),
    )

    wintest(
      "creates detached git worktree when info has no branch",
      () =>
        Effect.gen(function* () {
          const test = yield* TestInstance
          const svc = yield* Worktree.Service
          const info = yield* svc.makeWorktreeInfo({ name: "detached-test", detached: true })
          const ready = yield* waitReady().pipe(Effect.forkScoped)
          yield* svc.createFromInfo(info)

          const list = yield* git(test.directory, ["worktree", "list", "--porcelain"])
          const normalizedList = normalize(list)
          const normalizedDir = normalize(info.directory)
          expect(normalizedList).toContain(normalizedDir)

          const branch = yield* gitResult(info.directory, ["symbolic-ref", "-q", "--short", "HEAD"])
          expect(branch.exitCode).not.toBe(0)

          const props = yield* Fiber.join(ready)
          expect(props.name).toBe(info.name)
          expect(props.branch).toBeUndefined()

          yield* svc.remove({ directory: info.directory })
        }),
      { git: true },
    )
  })

  describe("create + remove lifecycle", () => {
    it.instance(
      "create returns worktree info and remove cleans up",
      () =>
        withCreatedWorktree(undefined, ({ info }) =>
          Effect.gen(function* () {
            const test = yield* TestInstance
            const fs = yield* FSUtil.Service
            const svc = yield* Worktree.Service
            expect(info.name).toBeDefined()
            expect(info.branch ?? "").toStartWith("opencode/")
            expect(info.directory).toBeDefined()

            expect(yield* svc.remove({ directory: info.directory })).toBe(true)
            expect(yield* fs.exists(info.directory)).toBe(false)
            const branch = yield* gitResult(test.directory, [
              "show-ref",
              "--verify",
              "--quiet",
              `refs/heads/${info.branch}`,
            ])
            expect(branch.exitCode).not.toBe(0)
          }),
        ),
      { git: true },
    )

    nonCooperativeBootstrapIt.instance(
      "refuses Worktree.remove while a reload disposer Promise is still running",
      () =>
        Effect.gen(function* () {
          const test = yield* TestInstance
          const fs = yield* FSUtil.Service
          const svc = yield* Worktree.Service
          const store = yield* InstanceStore.Service
          const ready = yield* waitReady().pipe(Effect.forkScoped)
          controlledBootstrapRun = Effect.void
          const info = yield* svc.create({ name: "held-disposer-remove" })
          yield* Fiber.join(ready)
          const disposing = yield* Deferred.make<void>()
          const releaseDispose = yield* Deferred.make<() => void>()
          const disposeFinished = yield* Deferred.make<void>()
          let disposeCalls = 0
          let releasePromise: (() => void) | undefined
          let unregister: (() => void) | undefined
          let reload: Fiber.Fiber<InstanceContext, never> | undefined
          let removing: Fiber.Fiber<boolean, Worktree.Error> | undefined

          yield* Effect.addFinalizer(() =>
            Effect.gen(function* () {
              controlledBootstrapRun = Effect.void
              if (releasePromise) yield* Effect.sync(releasePromise)
              if (reload) {
                yield* Fiber.await(reload).pipe(
                  Effect.timeoutOrElse({ duration: "15 seconds", orElse: () => Effect.succeed(undefined) }),
                  Effect.asVoid,
                )
              }
              if (removing) {
                yield* Fiber.await(removing).pipe(
                  Effect.timeoutOrElse({ duration: "15 seconds", orElse: () => Effect.succeed(undefined) }),
                  Effect.asVoid,
                )
              }
              if (unregister) yield* Effect.sync(unregister)
              yield* removeCreatedWorktree(info.directory).pipe(Effect.ignore)
            }),
          )

          unregister = yield* Effect.sync(() =>
            registerDisposer((directory) => {
              if (directory !== info.directory) return Promise.resolve()
              disposeCalls++
              if (disposeCalls > 1) return Promise.resolve()
              return new Promise<void>((resolve) => {
                releasePromise = resolve
                Deferred.doneUnsafe(disposing, Effect.void)
                Deferred.doneUnsafe(releaseDispose, Effect.succeed(resolve))
              }).then(() => {
                Deferred.doneUnsafe(disposeFinished, Effect.void)
              })
            }),
          )

          reload = yield* store.reload({ directory: info.directory }).pipe(Effect.forkScoped)
          yield* awaitWithTimeout(Deferred.await(disposing), "reload did not reach the held disposer")
          removing = yield* svc.remove({ directory: info.directory }).pipe(Effect.forkScoped)
          const removal = yield* awaitWithTimeout(
            Fiber.await(removing),
            "Worktree.remove did not report its bounded disposer refusal",
            "12 seconds",
          )
          expect(Exit.isFailure(removal)).toBe(true)
          if (Exit.isFailure(removal)) {
            expect(Cause.squash(removal.cause)).toBeInstanceOf(Worktree.RemoveFailedError)
            expect(Cause.pretty(removal.cause)).toContain("instance disposer did not settle")
          }
          expect(reload.pollUnsafe()).toBeUndefined()
          expect(yield* fs.exists(info.directory)).toBe(true)
          expect(normalize(yield* git(test.directory, ["worktree", "list", "--porcelain"]))).toContain(
            normalize(info.directory),
          )
          const joined = yield* store.load({ directory: info.directory }).pipe(Effect.forkScoped({ startImmediately: true }))
          expect(joined.pollUnsafe()).toBeUndefined()

          const release = yield* Deferred.await(releaseDispose)
          yield* Effect.sync(release)
          releasePromise = undefined
          yield* awaitWithTimeout(Deferred.await(disposeFinished), "held disposer did not finish")
          const reloaded = yield* awaitWithTimeout(Fiber.await(reload), "reload did not finish after disposer completion")
          const loaded = yield* awaitWithTimeout(Fiber.await(joined), "concurrent load did not join the reload owner")
          expect(Exit.isSuccess(reloaded)).toBe(true)
          expect(Exit.isSuccess(loaded)).toBe(true)
          if (Exit.isSuccess(reloaded) && Exit.isSuccess(loaded)) expect(loaded.value).toBe(reloaded.value)
          const recovered = yield* pollWithTimeout(
            store.load({ directory: info.directory }).pipe(
              Effect.as(true),
              Effect.catchCause(() => Effect.succeed(undefined)),
            ),
            "completed disposer did not release the worktree cache quarantine",
          )
          expect(recovered).toBe(true)
          if (unregister) {
            yield* Effect.sync(unregister)
            unregister = undefined
          }
          expect(yield* svc.remove({ directory: info.directory })).toBe(true)
          expect(yield* fs.exists(info.directory)).toBe(false)
          removing = undefined
        }),
      { git: true },
      { timeout: 60_000 },
    )

    raceIt.instance(
      "stops an unregistered worktree boot before removal deletes its directory",
      () =>
        Effect.gen(function* () {
          const test = yield* TestInstance
          const fs = yield* FSUtil.Service
          const svc = yield* Worktree.Service
          const expected = yield* svc.makeWorktreeInfo({ name: "boot-before-registration" })
          const control: BootControl = {
            mode: "hold",
            directory: expected.directory,
            started: yield* Deferred.make<void>(),
            release: yield* Deferred.make<void>(),
            interrupted: yield* Deferred.make<void>(),
          }
          bootControl = control
          yield* Effect.addFinalizer(() =>
            Effect.gen(function* () {
              bootControl = undefined
              yield* Deferred.succeed(control.release, undefined).pipe(Effect.asVoid)
            }),
          )

          const info = yield* svc.create({ name: expected.name })
          expect(info.directory).toBe(expected.directory)
          yield* awaitWithTimeout(Deferred.await(control.started), "worktree boot did not reach checkout", "5 seconds")
          yield* Effect.addFinalizer(() => removeCreatedWorktree(info.directory).pipe(Effect.ignore))

          const removing = yield* svc.remove({ directory: info.directory }).pipe(Effect.forkScoped)
          const stopped = yield* Deferred.await(control.interrupted).pipe(
            Effect.as(true),
            Effect.timeoutOrElse({ duration: "2 seconds", orElse: () => Effect.succeed(false) }),
          )
          expect(stopped).toBe(true)

          expect(yield* awaitWithTimeout(Fiber.join(removing), "worktree removal did not finish", "5 seconds")).toBe(
            true,
          )
          expect(yield* fs.exists(info.directory)).toBe(false)
          const branch = yield* gitResult(test.directory, [
            "show-ref",
            "--verify",
            "--quiet",
            `refs/heads/${info.branch ?? ""}`,
          ])
          expect(branch.exitCode).not.toBe(0)
        }),
      { git: true },
      { timeout: 25_000 },
    )

    raceIt.instance(
      "publishes a failed checkout before removing the failed worktree",
      () =>
        Effect.gen(function* () {
          const fs = yield* FSUtil.Service
          const svc = yield* Worktree.Service
          const expected = yield* svc.makeWorktreeInfo({ name: "failed-worktree-checkout" })
          const terminal = yield* subscribeTerminal(expected.directory)
          bootControl = { mode: "fail", directory: expected.directory }
          yield* Effect.addFinalizer(() => Effect.sync(() => (bootControl = undefined)).pipe(Effect.asVoid))

          const info = yield* svc.create({ name: expected.name })
          yield* Effect.addFinalizer(() => removeCreatedWorktree(info.directory).pipe(Effect.ignore))
          expect(info.directory).toBe(expected.directory)
          const event = yield* awaitWithTimeout(
            Deferred.await(terminal),
            "worktree checkout did not publish its failure",
            "5 seconds",
          )
          expect(event.payload.type).toBe(Worktree.Event.Failed.type)
          expect(event.payload.properties.message).toContain("simulated worktree checkout failure")

          expect(yield* svc.remove({ directory: info.directory })).toBe(true)
          expect(yield* fs.exists(info.directory)).toBe(false)
        }),
      { git: true },
    )

    failedBootstrapIt.instance(
      "publishes bootstrap defects as failed worktree events before removal",
      () =>
        Effect.gen(function* () {
          const fs = yield* FSUtil.Service
          const svc = yield* Worktree.Service
          const expected = yield* svc.makeWorktreeInfo({ name: "failed-instance-bootstrap" })
          const terminal = yield* subscribeTerminal(expected.directory)
          failedBootstrapDirectory = expected.directory
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => (failedBootstrapDirectory = undefined)).pipe(Effect.asVoid),
          )

          const info = yield* svc.create({ name: expected.name })
          yield* Effect.addFinalizer(() => removeCreatedWorktree(info.directory).pipe(Effect.ignore))
          const event = yield* awaitWithTimeout(
            Deferred.await(terminal),
            "worktree bootstrap defect did not publish a terminal event",
            "5 seconds",
          )
          expect(event.payload.type).toBe(Worktree.Event.Failed.type)
          expect(event.payload.properties.message).toContain("simulated instance bootstrap failure")

          expect(yield* svc.remove({ directory: info.directory })).toBe(true)
          expect(yield* fs.exists(info.directory)).toBe(false)
        }),
      { git: true },
    )

    wintest(
      "refuses worktree deletion until real ConfigCommand.load settles its FIFO Promise",
      () =>
        Effect.gen(function* () {
          const test = yield* TestInstance
          const fs = yield* FSUtil.Service
          const svc = yield* Worktree.Service
          const store = yield* InstanceStore.Service
          const fifoDirectory = yield* tmpdirScoped()
          const fifo = path.join(fifoDirectory, "held-command.md")
          const commandDirectory = path.join(test.directory, ".opencode", "command")
          yield* Effect.promise(() => mkdir(commandDirectory, { recursive: true }))
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
          const fifoMaker = yield* spawner.spawn(ChildProcess.make("mkfifo", [fifo]))
          expect(yield* fifoMaker.exitCode).toBe(ChildProcessSpawner.ExitCode(0))
          yield* Effect.promise(() => symlink(fifo, path.join(commandDirectory, "held.md")))
          yield* git(test.directory, ["add", "--force", ".opencode/command/held.md"])
          yield* git(test.directory, ["commit", "-m", "add a held ConfigCommand fixture"])
          const expected = yield* svc.makeWorktreeInfo({ name: "held-config-command" })
          let createdDirectory: string | undefined
          let removalFiber: Fiber.Fiber<boolean, Worktree.Error> | undefined

          const info = yield* svc.create({ name: expected.name })
          createdDirectory = info.directory
          expect(info.directory).toBe(expected.directory)
          yield* Effect.addFinalizer(() => removeCreatedWorktree(info.directory).pipe(Effect.ignore))

          const writer = createWriteStream(fifo)
          writer.on("error", () => {})
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              if (!writer.destroyed) writer.destroy()
            }),
          )
          const readerOpened = new Promise<void>((resolve, reject) => {
            writer.once("open", () => resolve())
            writer.once("error", reject)
          })
          yield* awaitWithTimeout(
            Effect.promise(() => readerOpened),
            "ConfigCommand.load did not open its held FIFO",
            "20 seconds",
          )
          expect(hasInstancePromises(info.directory)).toBe(true)

          removalFiber = yield* svc.remove({ directory: info.directory }).pipe(Effect.forkDetach({ startImmediately: true }))
          const removalResult = yield* Effect.exit(
            awaitWithTimeout(
              Fiber.await(removalFiber),
              "worktree removal did not refuse its unsettled config Promise",
              "30 seconds",
            ),
          )
          expect(Exit.isSuccess(removalResult)).toBe(true)
          if (Exit.isFailure(removalResult)) return
          const removal = removalResult.value
          expect(Exit.isFailure(removal)).toBe(true)
          if (Exit.isFailure(removal)) {
            expect(Cause.squash(removal.cause)).toBeInstanceOf(Worktree.RemoveFailedError)
            expect(Cause.pretty(removal.cause)).toContain("instance bootstrap Promise did not settle")
          }
          expect(yield* fs.exists(info.directory)).toBe(true)
          expect(normalize(yield* git(test.directory, ["worktree", "list", "--porcelain"]))).toContain(
            normalize(info.directory),
          )
          const reloadBlocked = yield* Effect.exit(
            awaitWithTimeout(
              Effect.exit(store.reload({ directory: info.directory })),
              "reload did not refuse the quarantined ConfigCommand owner",
              "5 seconds",
            ),
          )
          expect(Exit.isSuccess(reloadBlocked)).toBe(true)
          if (Exit.isSuccess(reloadBlocked)) {
            expect(Exit.isFailure(reloadBlocked.value)).toBe(true)
            if (Exit.isFailure(reloadBlocked.value))
              expect(Cause.pretty(reloadBlocked.value.cause)).toContain("instance bootstrap Promise is still running")
          }
          const blocked = yield* Effect.exit(
            awaitWithTimeout(
              Effect.exit(store.load({ directory: info.directory })),
              "new load did not report the pending config Promise owner",
              "5 seconds",
            ),
          )
          expect(Exit.isSuccess(blocked)).toBe(true)
          if (Exit.isSuccess(blocked)) {
            expect(Exit.isFailure(blocked.value)).toBe(true)
            if (Exit.isFailure(blocked.value))
              expect(Cause.pretty(blocked.value.cause)).toContain("instance bootstrap Promise is still running")
          }
          writer.end("---\ndescription: held config command\n---\necho ready\n")
          yield* awaitWithTimeout(Effect.promise(() => finished(writer)), "held ConfigCommand reader did not finish", "15 seconds")
          const loaded = yield* pollWithTimeout(
            store.load({ directory: info.directory }).pipe(
              Effect.as(true),
              Effect.catchCause(() => Effect.succeed(undefined)),
            ),
            "load did not recover after ConfigCommand.load settled",
            "25 seconds",
          )
          expect(loaded).toBe(true)

          expect(yield* svc.remove({ directory: info.directory })).toBe(true)
          expect(yield* fs.exists(info.directory)).toBe(false)
          removalFiber = undefined
        }),
      { git: true },
      { timeout: 120_000 },
    )

    pluginBootstrapIt.instance(
      "serves ready loads and reloads while a runtime plugin hook is active",
      () =>
        Effect.gen(function* () {
          const test = yield* TestInstance
          const fs = yield* FSUtil.Service
          const svc = yield* Worktree.Service
          const store = yield* InstanceStore.Service
          const plugin = yield* Plugin.Service
          const gate = PluginGate.install()
          let createdDirectory: string | undefined
          let triggering: Fiber.Fiber<{ env: Record<string, string> }, never> | undefined
          let reloading: Fiber.Fiber<InstanceContext, never> | undefined
          yield* Effect.addFinalizer(() =>
            Effect.gen(function* () {
              gate.reset()
              pluginSpec = undefined
              pluginSource = undefined
              if (triggering) {
                yield* Fiber.await(triggering).pipe(
                  Effect.timeoutOrElse({ duration: "30 seconds", orElse: () => Effect.succeed(undefined) }),
                  Effect.asVoid,
                )
              }
              if (reloading) {
                yield* Fiber.await(reloading).pipe(
                  Effect.timeoutOrElse({ duration: "30 seconds", orElse: () => Effect.succeed(undefined) }),
                  Effect.asVoid,
                )
              }
              if (createdDirectory) yield* svc.remove({ directory: createdDirectory }).pipe(Effect.ignore)
            }),
          )

          const pluginPath = path.join(test.directory, "round8-runtime-plugin.ts")
          const gateModule = pathToFileURL(path.join(import.meta.dir, "../fixture/plugin-gate.ts")).href
          yield* fs.writeFileString(
            pluginPath,
            [
              `import { PluginGate } from ${JSON.stringify(gateModule)}`,
              "export default {",
              '  id: "test.round-eight-runtime-hook",',
              "  server: async () => ({",
              '    "shell.env": async () => PluginGate.waitForTrigger(),',
              "  }),",
              "}",
            ].join("\n"),
          )
          yield* Effect.sync(() => {
            pluginSpec = pathToFileURL(pluginPath).href
            pluginSource = pluginPath
          })

          const readySignal = yield* waitReady().pipe(Effect.forkScoped)
          const info = yield* svc.create({ name: "round8-runtime-hook" })
          createdDirectory = info.directory
          yield* Fiber.join(readySignal)
          const ready = yield* store.load({ directory: info.directory })
          triggering = yield* store
            .provide(
              { directory: info.directory },
              plugin.trigger("shell.env", { cwd: info.directory }, { env: {} as Record<string, string> }),
            )
            .pipe(Effect.forkScoped({ startImmediately: true }))
          yield* awaitWithTimeout(Effect.promise(() => gate.triggerStarted), "runtime plugin hook did not start", "10 seconds")
          expect(hasInstancePromises(info.directory)).toBe(false)
          expect(yield* store.load({ directory: info.directory })).toBe(ready)

          reloading = yield* store.reload({ directory: info.directory }).pipe(Effect.forkScoped({ startImmediately: true }))
          const reloadExit = yield* awaitWithTimeout(
            Fiber.await(reloading),
            "ordinary reload was blocked by a post-boot plugin hook",
            "15 seconds",
          )
          expect(Exit.isSuccess(reloadExit)).toBe(true)
          if (Exit.isSuccess(reloadExit)) expect(yield* store.load({ directory: info.directory })).toBe(reloadExit.value)
          expect(hasInstancePromises(info.directory)).toBe(false)
          reloading = undefined

          gate.releaseTrigger()
          expect(Exit.isSuccess(yield* Fiber.await(triggering))).toBe(true)
          triggering = undefined
          expect(yield* svc.remove({ directory: info.directory })).toBe(true)
          createdDirectory = undefined
        }),
      { git: true },
      { timeout: 90_000 },
    )

    pluginBootstrapIt.instance(
      "refuses worktree deletion during external plugin loading and initialization",
      () =>
        Effect.gen(function* () {
          const test = yield* TestInstance
          const fs = yield* FSUtil.Service
          const svc = yield* Worktree.Service
          const store = yield* InstanceStore.Service
          const gate = PluginGate.install()
          let createdDirectory: string | undefined
          let loadingRemoval: Fiber.Fiber<boolean, Worktree.Error> | undefined
          let initializingRemoval: Fiber.Fiber<boolean, Worktree.Error> | undefined
          let configuringRemoval: Fiber.Fiber<boolean, Worktree.Error> | undefined
          let disposingRemoval: Fiber.Fiber<boolean, Worktree.Error> | undefined
          yield* Effect.addFinalizer(() =>
            Effect.gen(function* () {
              gate.reset()
              pluginSpec = undefined
              pluginSource = undefined
              for (const fiber of [
                loadingRemoval,
                initializingRemoval,
                configuringRemoval,
                disposingRemoval,
              ]) {
                if (!fiber) continue
                yield* Fiber.await(fiber).pipe(
                  Effect.timeoutOrElse({ duration: "45 seconds", orElse: () => Effect.succeed(undefined) }),
                  Effect.asVoid,
                )
              }
              if (createdDirectory) yield* removeCreatedWorktree(createdDirectory).pipe(Effect.ignore)
            }),
          )

          const pluginPath = path.join(test.directory, "round5-worktree-plugin.ts")
          const gateModule = pathToFileURL(path.join(import.meta.dir, "../fixture/plugin-gate.ts")).href
          yield* fs.writeFileString(
            pluginPath,
            [
              `import { PluginGate } from ${JSON.stringify(gateModule)}`,
              "await PluginGate.waitForLoad()",
              "export default {",
              '  id: "test.round-five-worktree",',
              "  server: async () => {",
              "    await PluginGate.waitForInit()",
              "    return {",
              "      config: async () => PluginGate.waitForConfig(),",
              "      dispose: async () => PluginGate.waitForDispose(),",
              "    }",
              "  },",
              "}",
            ].join("\n"),
          )
          yield* Effect.sync(() => {
            pluginSpec = pathToFileURL(pluginPath).href
            pluginSource = pluginPath
          })

          const expected = yield* svc.makeWorktreeInfo({ name: "plugin-promise-owner" })
          const info = yield* svc.create({ name: expected.name })
          createdDirectory = info.directory
          expect(info.directory).toBe(expected.directory)
          yield* awaitWithTimeout(Effect.promise(() => gate.loadStarted), "external plugin load did not start", "20 seconds")
          expect(hasInstancePromises(info.directory)).toBe(true)

          loadingRemoval = yield* svc.remove({ directory: info.directory }).pipe(Effect.forkDetach({ startImmediately: true }))
          const loadRefusal = yield* awaitWithTimeout(
            Fiber.await(loadingRemoval),
            "removal did not refuse a pending external plugin load",
            "35 seconds",
          )
          expect(Exit.isFailure(loadRefusal)).toBe(true)
          if (Exit.isFailure(loadRefusal)) {
            expect(Cause.squash(loadRefusal.cause)).toBeInstanceOf(Worktree.RemoveFailedError)
            expect(Cause.pretty(loadRefusal.cause)).toContain("instance bootstrap Promise did not settle")
          }
          expect(yield* fs.exists(info.directory)).toBe(true)

          gate.releaseLoad()
          yield* awaitWithTimeout(Effect.promise(() => gate.initStarted), "external plugin initialization did not start", "20 seconds")
          expect(hasInstancePromises(info.directory)).toBe(true)
          initializingRemoval = yield* svc.remove({ directory: info.directory }).pipe(Effect.forkDetach({ startImmediately: true }))
          const initRefusal = yield* awaitWithTimeout(
            Fiber.await(initializingRemoval),
            "removal did not refuse a pending plugin initializer",
            "35 seconds",
          )
          expect(Exit.isFailure(initRefusal)).toBe(true)
          if (Exit.isFailure(initRefusal)) {
            expect(Cause.squash(initRefusal.cause)).toBeInstanceOf(Worktree.RemoveFailedError)
            expect(Cause.pretty(initRefusal.cause)).toContain("instance bootstrap Promise did not settle")
          }
          expect(yield* fs.exists(info.directory)).toBe(true)
          gate.releaseInit()
          yield* awaitWithTimeout(Effect.promise(() => gate.configStarted), "plugin config callback did not start", "20 seconds")
          expect(hasInstancePromises(info.directory)).toBe(true)
          configuringRemoval = yield* svc.remove({ directory: info.directory }).pipe(Effect.forkDetach({ startImmediately: true }))
          const configRefusal = yield* awaitWithTimeout(
            Fiber.await(configuringRemoval),
            "removal did not refuse a pending plugin config callback",
            "35 seconds",
          )
          expect(Exit.isFailure(configRefusal)).toBe(true)
          if (Exit.isFailure(configRefusal)) {
            expect(Cause.squash(configRefusal.cause)).toBeInstanceOf(Worktree.RemoveFailedError)
            expect(Cause.pretty(configRefusal.cause)).toContain("instance bootstrap Promise did not settle")
          }
          expect(yield* fs.exists(info.directory)).toBe(true)
          const blocked = yield* Effect.exit(
            awaitWithTimeout(
              Effect.exit(store.load({ directory: info.directory })),
              "load did not report the active plugin initializer",
              "5 seconds",
            ),
          )
          expect(Exit.isSuccess(blocked)).toBe(true)
          if (Exit.isSuccess(blocked)) {
            expect(Exit.isFailure(blocked.value)).toBe(true)
            if (Exit.isFailure(blocked.value))
              expect(Cause.pretty(blocked.value.cause)).toContain("instance bootstrap Promise is still running")
          }

          gate.releaseConfig()
          const recovered = yield* pollWithTimeout(
            store.load({ directory: info.directory }).pipe(
              Effect.as(true),
              Effect.catchCause(() => Effect.succeed(undefined)),
            ),
            "plugin bootstrap did not recover after its awaited Promises settled",
            "30 seconds",
          )
          expect(recovered).toBe(true)

          gate.holdDispose()
          disposingRemoval = yield* svc.remove({ directory: info.directory }).pipe(Effect.forkDetach({ startImmediately: true }))
          yield* awaitWithTimeout(Effect.promise(() => gate.disposeStarted), "plugin dispose hook did not start", "20 seconds")
          expect(hasInstancePromises(info.directory)).toBe(false)
          const disposeRefusal = yield* awaitWithTimeout(
            Fiber.await(disposingRemoval),
            "worktree removal did not refuse a pending plugin dispose hook",
            "15 seconds",
          )
          expect(Exit.isFailure(disposeRefusal)).toBe(true)
          if (Exit.isFailure(disposeRefusal)) {
            expect(Cause.squash(disposeRefusal.cause)).toBeInstanceOf(Worktree.RemoveFailedError)
            expect(Cause.pretty(disposeRefusal.cause)).toContain("instance disposer did not settle")
          }
          expect(yield* fs.exists(info.directory)).toBe(true)
          gate.releaseDispose()
          expect(yield* awaitWithTimeout(svc.remove({ directory: info.directory }), "worktree cleanup did not resume after plugin disposal", "20 seconds")).toBe(
            true,
          )
          expect(yield* fs.exists(info.directory)).toBe(false)
          loadingRemoval = undefined
          initializingRemoval = undefined
          configuringRemoval = undefined
          disposingRemoval = undefined
        }),
      { git: true },
      { timeout: 240_000 },
    )

    it.instance(
      "create returns after setup and fires Event.Ready after bootstrap",
      () =>
        withCreatedWorktree(undefined, ({ info, ready }) =>
          Effect.gen(function* () {
            const svc = yield* Worktree.Service

            expect(info.name).toBeDefined()
            expect(info.branch ?? "").toStartWith("opencode/")

            expect(ready.name).toBe(info.name)
            expect(ready.branch).toBe(info.branch)

            const list = yield* svc.list()
            expect(list).toContainEqual(expect.objectContaining({ name: info.name, branch: info.branch }))
          }),
        ),
      { git: true },
    )

    it.instance(
      "lists the active linked worktree but not the project checkout",
      () =>
        withCreatedWorktree(undefined, ({ info }) =>
          Effect.gen(function* () {
            const test = yield* TestInstance
            const svc = yield* Worktree.Service
            const list = yield* svc.list().pipe(provideInstance(info.directory))

            expect(list.map((item) => item.name)).toContain(info.name)
            expect(list.map((item) => item.name)).not.toContain(path.basename(test.directory).toLowerCase())
          }),
        ),
      { git: true },
    )

    it.instance(
      "create with custom name",
      () =>
        withCreatedWorktree({ name: "test-workspace" }, ({ info }) =>
          Effect.gen(function* () {
            expect(info.name).toBe("test-workspace")
            expect(info.branch).toBe("opencode/test-workspace")
          }),
        ),
      { git: true },
    )
  })

  describe("createFromInfo", () => {
    wintest(
      "creates git worktree and boots asynchronously",
      () =>
        Effect.gen(function* () {
          const test = yield* TestInstance
          const svc = yield* Worktree.Service
          const info = yield* svc.makeWorktreeInfo({ name: "from-info-test" })
          const ready = yield* waitReady().pipe(Effect.forkScoped)
          yield* svc.createFromInfo(info)

          const list = yield* git(test.directory, ["worktree", "list", "--porcelain"])
          const normalizedList = list.replace(/\\/g, "/")
          const normalizedDir = info.directory.replace(/\\/g, "/")
          expect(normalizedList).toContain(normalizedDir)

          yield* Fiber.join(ready)
          yield* removeCreatedWorktree(info.directory)
        }),
      { git: true },
    )
  })

  describe("list", () => {
    it.instance(
      "uses parent folder name when worktree basename matches the primary worktree",
      () =>
        Effect.gen(function* () {
          const test = yield* TestInstance
          const fs = yield* FSUtil.Service
          const svc = yield* Worktree.Service
          const parent = path.join(path.dirname(test.directory), `${path.basename(test.directory)}-parent`)
          const target = path.join(parent, path.basename(test.directory))
          const branch = `same-basename-list-${Date.now()}`

          yield* fs.ensureDir(parent)
          yield* git(test.directory, ["worktree", "add", "-b", branch, target])

          const list = yield* svc.list()
          const directory = yield* fs.realPath(target).pipe(Effect.catch(() => Effect.succeed(target)))

          expect(list.map((item) => ({ ...item, directory: normalize(item.directory) }))).toContainEqual({
            name: path.basename(parent),
            branch,
            directory: normalize(directory),
          })

          yield* svc.remove({ directory: target })
        }),
      { git: true },
    )
  })

  describe("remove edge cases", () => {
    it.instance(
      "remove non-existent directory succeeds silently",
      () =>
        Effect.gen(function* () {
          const test = yield* TestInstance
          const svc = yield* Worktree.Service
          const ok = yield* svc.remove({ directory: path.join(test.directory, "does-not-exist") })
          expect(ok).toBe(true)
        }),
      { git: true },
    )

    it.instance("fails with NotGitError for non-git directories", () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const svc = yield* Worktree.Service
        const exit = yield* Effect.exit(svc.remove({ directory: path.join(test.directory, "fake") }))

        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause)
          expect(error).toBeInstanceOf(Worktree.NotGitError)
          if (error instanceof Worktree.NotGitError) expect(error._tag).toBe("WorktreeNotGitError")
        }
      }),
    )
  })
})
