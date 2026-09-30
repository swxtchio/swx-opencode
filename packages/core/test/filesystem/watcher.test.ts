import { $ } from "bun"
import { describe, expect, test } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { ConfigProvider, Deferred, Duration, Effect, Exit, Fiber, Layer, Option, Scope, Stream } from "effect"
import { Config } from "@opencode-ai/core/config"
import { ConfigWatcher } from "@opencode-ai/core/config/watcher"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Watcher } from "@opencode-ai/core/filesystem/watcher"
import { Location } from "@opencode-ai/core/location"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { location } from "../fixture/location"
import { tmpdir } from "../fixture/tmpdir"
import { testEffect } from "../lib/effect"

const describeWatcher = Watcher.hasNativeBinding() && !process.env.CI ? describe : describe.skip

type WatcherEvent = { file: string; event: "add" | "change" | "unlink" }

const it = testEffect(AppNodeBuilder.build(LayerNode.group([FSUtil.node, EventV2.node])))

type Options = { root?: boolean; ignore?: string[]; events?: EventV2.Interface }

function provide(directory: string, vcs?: Location.Interface["vcs"], options?: Options) {
  return Effect.provide(watcherLayer(directory, vcs, options))
}

function watcherLayer(directory: string, vcs?: Location.Interface["vcs"], options?: Options) {
  const configLayer = Layer.succeed(
    Config.Service,
    Config.Service.of({
      entries: () =>
        Effect.succeed(
          options?.ignore
            ? [
                new Config.Document({
                  type: "document",
                  info: new Config.Info({ watcher: new ConfigWatcher.Info({ ignore: options.ignore }) }),
                }),
              ]
            : [],
        ),
    }),
  )
  const flagsLayer = ConfigProvider.layer(
    ConfigProvider.fromUnknown({
      OPENCODE_EXPERIMENTAL_FILEWATCHER: options?.root === false ? "false" : "true",
      OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER: "false",
    }),
  )
  const locationLayer = Layer.succeed(
    Location.Service,
    Location.Service.of(location({ directory: AbsolutePath.make(directory) }, { vcs })),
  )
  return AppNodeBuilder.build(Watcher.node, [
    [Config.node, configLayer],
    [Location.node, locationLayer],
    ...(options?.events ? [[EventV2.node, Layer.succeed(EventV2.Service, options.events)] as const] : []),
  ]).pipe(Layer.provide(flagsLayer))
}

function withTmp<A, E, R>(
  f: (directory: string, vcs?: Location.Interface["vcs"]) => Effect.Effect<A, E, R>,
  options?: { git?: boolean; init?: (directory: string) => Promise<void> } & Options,
) {
  return Effect.acquireRelease(
    Effect.promise(async () => {
      const tmp = await tmpdir()
      if (!options?.git) return { tmp, vcs: undefined }
      await gitInit(tmp.path)
      await options.init?.(tmp.path)
      return { tmp, vcs: { type: "git" as const, store: AbsolutePath.make(path.join(tmp.path, ".git")) } }
    }),
    ({ tmp }) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
  ).pipe(Effect.flatMap(({ tmp, vcs }) => f(tmp.path, vcs).pipe(provide(tmp.path, vcs, options))))
}

async function gitInit(directory: string) {
  await $`git init`.cwd(directory).quiet()
  await $`git config core.fsmonitor false`.cwd(directory).quiet()
  await $`git config commit.gpgsign false`.cwd(directory).quiet()
  await $`git config user.email test@opencode.test`.cwd(directory).quiet()
  await $`git config user.name Test`.cwd(directory).quiet()
  await $`git commit --allow-empty -m root`.cwd(directory).quiet()
}

function wait(check: (event: WatcherEvent) => boolean) {
  return Effect.gen(function* () {
    const events = yield* EventV2.Service
    const deferred = yield* Deferred.make<WatcherEvent>()
    const fiber = yield* events.subscribe(Watcher.Event.Updated).pipe(
      Stream.runForEach((event) => {
        if (!check(event.data)) return Effect.void
        return Deferred.succeed(deferred, event.data).pipe(Effect.asVoid)
      }),
      Effect.forkScoped,
    )
    yield* Effect.yieldNow
    return { deferred, fiber }
  })
}

function maybeNextUpdate<E>(
  check: (event: WatcherEvent) => boolean,
  trigger: Effect.Effect<void, E>,
  timeout: Duration.Input = "5 seconds",
) {
  return Effect.acquireUseRelease(
    wait(check),
    ({ deferred }) => trigger.pipe(Effect.andThen(Deferred.await(deferred)), Effect.timeoutOption(timeout)),
    ({ fiber }) => Fiber.interrupt(fiber),
  )
}

function nextUpdate<E>(check: (event: WatcherEvent) => boolean, trigger: Effect.Effect<void, E>) {
  return Effect.gen(function* () {
    const result = yield* maybeNextUpdate(check, trigger)
    if (Option.isSome(result)) return result.value
    return yield* Effect.fail(new Error("timed out waiting for file watcher update"))
  })
}

function eventuallyUpdate<E>(check: (event: WatcherEvent) => boolean, trigger: () => Effect.Effect<void, E>) {
  return Effect.gen(function* () {
    while (true) {
      const result = yield* maybeNextUpdate(check, trigger(), "250 millis")
      if (Option.isSome(result)) return result.value
    }
  }).pipe(
    Effect.timeoutOrElse({
      duration: "5 seconds",
      orElse: () => Effect.fail(new Error("timed out waiting for file watcher readiness")),
    }),
  )
}

function noUpdate<E>(check: (event: WatcherEvent) => boolean, trigger: Effect.Effect<void, E>, timeout = 500) {
  return Effect.acquireUseRelease(
    wait(check),
    ({ deferred }) =>
      trigger.pipe(
        Effect.andThen(Deferred.await(deferred)),
        Effect.timeoutOption(`${timeout} millis`),
        Effect.tap((result) => Effect.sync(() => expect(result).toEqual(Option.none()))),
      ),
    ({ fiber }) => Fiber.interrupt(fiber),
  )
}

function ready(directory: string) {
  const file = path.join(directory, `.watcher-${Math.random().toString(36).slice(2)}`)
  return Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    yield* eventuallyUpdate(
      (event) => event.file === file,
      () => fs.writeFileString(file, `ready-${Math.random()}`),
    ).pipe(Effect.ensuring(fs.remove(file, { force: true }).pipe(Effect.ignore)), Effect.asVoid)
  })
}

describeWatcher("Watcher", () => {
  it.live("publishes root create, update, and delete events", () =>
    withTmp(
      (directory) =>
        Effect.gen(function* () {
          const fs = yield* FSUtil.Service
          const file = path.join(directory, "watch.txt")
          yield* ready(directory)
          for (const item of [
            { event: "add" as const, trigger: fs.writeFileString(file, "a") },
            { event: "change" as const, trigger: fs.writeFileString(file, "b") },
            { event: "unlink" as const, trigger: fs.remove(file) },
          ]) {
            expect(
              yield* nextUpdate((event) => event.file === file && event.event === item.event, item.trigger),
            ).toEqual({
              file,
              event: item.event,
            })
          }
        }),
      { git: true },
    ),
  )

  it.live("skips non-git roots", () =>
    withTmp((directory) =>
      Effect.gen(function* () {
        const fs = yield* FSUtil.Service
        const file = path.join(directory, "plain.txt")
        yield* noUpdate((event) => event.file === file, fs.writeFileString(file, "plain"))
      }),
    ),
  )

  it.live("cleanup stops publishing events", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const fs = yield* FSUtil.Service
      const tmp = yield* Effect.acquireRelease(
        Effect.promise(() => tmpdir()),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      )
      yield* ready(tmp.path).pipe(
        provide(tmp.path, { type: "git", store: AbsolutePath.make(path.join(tmp.path, ".git")) }),
        Effect.scoped,
      )
      const file = path.join(tmp.path, "after-dispose.txt")
      yield* noUpdate((event) => event.file === file, fs.writeFileString(file, "gone")).pipe(
        Effect.provideService(EventV2.Service, events),
      )
    }).pipe(Effect.provide(AppNodeBuilder.build(LayerNode.group([FSUtil.node, EventV2.node])))),
  )

  it.live("ignores .git/index changes", () =>
    withTmp(
      (directory) =>
        Effect.gen(function* () {
          const fs = yield* FSUtil.Service
          const index = path.join(directory, ".git", "index")
          yield* ready(directory)
          yield* noUpdate(
            (event) => event.file === index,
            fs
              .writeFileString(path.join(directory, "tracked.txt"), "a")
              .pipe(Effect.andThen(Effect.promise(() => $`git add .`.cwd(directory).quiet())), Effect.asVoid),
          )
        }),
      { git: true },
    ),
  )

  it.live("publishes .git/HEAD events", () =>
    withTmp(
      (directory) =>
        Effect.gen(function* () {
          const fs = yield* FSUtil.Service
          const head = path.join(directory, ".git", "HEAD")
          const branch = `watch-${Math.random().toString(36).slice(2)}`
          yield* ready(directory)
          yield* Effect.promise(() => $`git branch ${branch}`.cwd(directory).quiet())
          expect(
            yield* nextUpdate((event) => event.file === head, fs.writeFileString(head, `ref: refs/heads/${branch}\n`)),
          ).toMatchObject({ file: head })
        }),
      { git: true },
    ),
  )

  it.live("publishes .git/HEAD events when git switches branches", () =>
    withTmp(
      (directory) =>
        Effect.gen(function* () {
          const head = path.join(yield* Effect.promise(() => fs.realpath(path.join(directory, ".git"))), "HEAD")
          const branch = `switch-${Math.random().toString(36).slice(2)}`
          expect(
            yield* nextUpdate(
              (event) => event.file === head,
              Effect.promise(() => $`git switch -q -c ${branch}`.cwd(directory).quiet()),
            ),
          ).toMatchObject({ file: head })
        }),
      { git: true, root: false },
    ),
  )

  it.live("publishes .git/HEAD events for a linked worktree's git directory", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.acquireRelease(
        Effect.promise(() => tmpdir()),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      )
      const worktree = path.join(tmp.path, "..", `wt_${path.basename(tmp.path)}`)
      yield* Effect.addFinalizer(() => Effect.promise(() => fs.rm(worktree, { recursive: true, force: true })))
      const head = yield* Effect.promise(async () => {
        await gitInit(tmp.path)
        await $`git worktree add -q -b ${path.basename(worktree)} ${worktree}`.cwd(tmp.path).quiet()
        const gitDirectory = (await $`git rev-parse --absolute-git-dir`.cwd(worktree).text()).trim()
        return path.join(await fs.realpath(gitDirectory), "HEAD")
      })
      const branch = `switch-${Math.random().toString(36).slice(2)}`
      expect(
        yield* nextUpdate(
          (event) => event.file === head,
          Effect.promise(() => $`git switch -q -c ${branch}`.cwd(worktree).quiet()),
        ).pipe(
          provide(worktree, { type: "git", store: AbsolutePath.make(path.join(tmp.path, ".git")) }, { root: false }),
        ),
      ).toMatchObject({ file: head })
    }),
  )

  it.live("reports each started watch active", () =>
    withTmp(
      (directory) =>
        Effect.gen(function* () {
          const git = yield* Effect.promise(() => fs.realpath(path.join(directory, ".git")))
          yield* ready(directory)
          const watcher = yield* Watcher.Service
          expect(yield* watcher.status).toEqual([
            { watch: "git", directory: git, state: "active" },
            { watch: "root", directory, state: "active" },
          ])
        }),
      { git: true },
    ),
  )

  it.live("keeps the root watch inactive when it is not enabled", () =>
    withTmp(
      (directory) =>
        Effect.gen(function* () {
          const afs = yield* FSUtil.Service
          const git = yield* Effect.promise(() => fs.realpath(path.join(directory, ".git")))
          const watcher = yield* Watcher.Service
          expect(yield* watcher.status).toEqual([{ watch: "git", directory: git, state: "active" }])
          const file = path.join(directory, "root-off.txt")
          yield* noUpdate((event) => event.file === file, afs.writeFileString(file, "off"))
          const branch = `switch-${Math.random().toString(36).slice(2)}`
          expect(
            yield* nextUpdate(
              (event) => event.file === path.join(git, "HEAD"),
              Effect.promise(() => $`git switch -q -c ${branch}`.cwd(directory).quiet()),
            ),
          ).toMatchObject({ file: path.join(git, "HEAD") })
        }),
      { git: true, root: false },
    ),
  )

  it.live("keeps the git watch inactive when .git is ignored", () =>
    withTmp(
      (directory) =>
        Effect.gen(function* () {
          const afs = yield* FSUtil.Service
          yield* ready(directory)
          const watcher = yield* Watcher.Service
          expect(yield* watcher.status).toEqual([{ watch: "root", directory, state: "active" }])
          const head = path.join(directory, ".git", "HEAD")
          yield* noUpdate((event) => event.file === head, afs.writeFileString(head, "ref: refs/heads/ignored\n"))
        }),
      { git: true, ignore: [".git"] },
    ),
  )

  it.live(
    "keeps concurrent root watches independent when one is disposed",
    () =>
      Effect.gen(function* () {
        const [first, second] = yield* Effect.all(
          [0, 1].map(() =>
            Effect.acquireRelease(
              Effect.promise(() => tmpdir()),
              (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
            ),
          ),
        )
        const vcs = (directory: string) => ({
          type: "git" as const,
          store: AbsolutePath.make(path.join(directory, ".git")),
        })
        // Fresh layers, so each directory gets its own watcher instead of a memoized
        // one, publishing to this test's event bus.
        const events = yield* EventV2.Service
        const firstScope = yield* Scope.make()
        const firstContext = yield* Layer.buildWithScope(
          Layer.fresh(watcherLayer(first.path, vcs(first.path), { events })),
          firstScope,
        )
        const secondContext = yield* Layer.build(Layer.fresh(watcherLayer(second.path, vcs(second.path), { events })))
        yield* ready(first.path).pipe(Effect.provide(firstContext))
        yield* ready(second.path).pipe(Effect.provide(secondContext))
        yield* Scope.close(firstScope, Exit.void)
        const afs = yield* FSUtil.Service
        const file = path.join(second.path, "after-first-disposed.txt")
        expect(
          yield* nextUpdate((event) => event.file === file, afs.writeFileString(file, "still")).pipe(
            Effect.provide(secondContext),
          ),
        ).toEqual({ file, event: "add" })
      }),
    20_000,
  )

  const describeSymlink = process.platform !== "win32" ? describe : describe.skip
  describeSymlink("symlinked .git", () => {
    it.live("publishes .git/HEAD events through a symlinked .git directory", () =>
      withTmp(
        (directory) =>
          Effect.gen(function* () {
            const afs = yield* FSUtil.Service
            const actual = path.join(directory, "..", `actual_${path.basename(directory)}`)
            yield* Effect.addFinalizer(() => Effect.promise(() => fs.rm(actual, { recursive: true, force: true })))
            yield* ready(directory)
            const head = path.join(directory, ".git", "HEAD")
            const branch = `watch-${Math.random().toString(36).slice(2)}`
            yield* Effect.promise(() => $`git branch ${branch}`.cwd(directory).quiet())
            expect(
              yield* nextUpdate(
                (event) => event.file === path.join(actual, "HEAD"),
                afs.writeFileString(head, `ref: refs/heads/${branch}\n`),
              ),
            ).toEqual({ file: path.join(actual, "HEAD"), event: "change" })
          }),
        {
          git: true,
          init: async (directory) => {
            const actual = path.join(directory, "..", `actual_${path.basename(directory)}`)
            await fs.rename(path.join(directory, ".git"), actual)
            await fs.symlink(actual, path.join(directory, ".git"))
          },
        },
      ),
    )
  })
})

// Runs the production watcher in a child whose rootless user namespace caps
// inotify instances, so the kernel really refuses them without touching the
// shared per-user pool beyond that cap.
const canCapInotify =
  process.platform === "linux" &&
  Bun.spawnSync(["unshare", "-U", "-r", "sh", "-c", "echo 1 > /proc/sys/user/max_inotify_instances"]).exitCode === 0
const describeRefusal = canCapInotify && Watcher.hasNativeBinding() && !process.env.CI ? describe : describe.skip

type ChildReport = {
  booted: Watcher.WatchStatus[]
  instances: number
  progressed: boolean
  headEvent?: string
  rootEvent?: string
  settled?: Watcher.WatchStatus[]
}

async function underInotifyLimit(limit: number, options: { waitRoot?: boolean } = {}) {
  await using tmp = await tmpdir()
  await gitInit(tmp.path)
  const child = Bun.spawn(
    [
      "unshare",
      "-U",
      "-r",
      "sh",
      "-c",
      `echo ${limit} > /proc/sys/user/max_inotify_instances && exec "$0" "$@"`,
      process.execPath,
      path.join(import.meta.dir, "../fixture/watcher-child.ts"),
      tmp.path,
      options.waitRoot ? "wait-root" : "",
    ],
    { stdout: "pipe", stderr: "pipe" },
  )
  // A parked JavaScript thread never finishes on its own; the bound turns that
  // into a failure of this test instead of a hang.
  const exited = await Promise.race([child.exited, Bun.sleep(25_000).then(() => undefined)])
  if (exited === undefined) {
    child.kill("SIGKILL")
    await child.exited
    throw new Error(`watcher child under inotify limit ${limit} made no progress: its JavaScript thread is parked`)
  }
  const stdout = await new Response(child.stdout).text()
  if (exited !== 0)
    throw new Error(`watcher child exited ${exited}: ${stdout}${await new Response(child.stderr).text()}`)
  return {
    report: JSON.parse(stdout.trim().split("\n").at(-1)!) as ChildReport,
    directory: await fs.realpath(tmp.path),
  }
}

describeRefusal("Watcher under refused inotify instances", () => {
  test("keeps its thread running and reports every refused watch as not active", async () => {
    const { report, directory } = await underInotifyLimit(0, { waitRoot: true })
    expect(report.progressed).toBe(true)
    expect(report.instances).toBe(0)
    expect(report.booted).toEqual([
      {
        watch: "git",
        directory: path.join(directory, ".git"),
        state: "unavailable",
        reason: expect.stringContaining("EMFILE"),
      },
      { watch: "root", directory, state: "starting" },
    ])
    expect(report.headEvent).toBeUndefined()
    expect(report.settled?.find((item) => item.watch === "root")).toMatchObject({ state: "unconfirmed" })
    expect(report.rootEvent).toBeUndefined()
  }, 40_000)

  test("keeps a started git watch delivering when the root watch is refused", async () => {
    const { report, directory } = await underInotifyLimit(1, { waitRoot: true })
    expect(report.progressed).toBe(true)
    // The one permitted instance belongs to the git watch, so the root
    // watch's native subscription was refused.
    expect(report.instances).toBe(1)
    expect(report.booted).toEqual([
      { watch: "git", directory: path.join(directory, ".git"), state: "active" },
      { watch: "root", directory, state: "starting" },
    ])
    expect(report.headEvent).toBe("change")
    // The refused root watch settles as unconfirmed, never active, and delivers nothing.
    expect(report.settled).toEqual([
      { watch: "git", directory: path.join(directory, ".git"), state: "active" },
      {
        watch: "root",
        directory,
        state: "unconfirmed",
        reason: expect.stringContaining("no subscription acknowledgement"),
      },
    ])
    expect(report.rootEvent).toBeUndefined()
  }, 40_000)

  test("starts every watch under ordinary capacity", async () => {
    const { report, directory } = await underInotifyLimit(64, { waitRoot: true })
    expect(report.progressed).toBe(true)
    expect(report.headEvent).toBe("change")
    expect(report.settled).toEqual([
      { watch: "git", directory: path.join(directory, ".git"), state: "active" },
      { watch: "root", directory, state: "active" },
    ])
    expect(report.rootEvent).toBe("add")
  }, 40_000)
})
