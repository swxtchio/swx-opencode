// Boots the production Watcher layer for a git repository in this process and
// prints one JSON report. The watcher test runs it under a rootless user
// namespace whose inotify-instance ceiling makes the kernel refuse instances.
import { $ } from "bun"
import fs from "fs/promises"
import path from "path"
import { ConfigProvider, Deferred, Effect, Layer, Stream } from "effect"
import { Config } from "@opencode-ai/core/config"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Watcher } from "@opencode-ai/core/filesystem/watcher"
import { Location } from "@opencode-ai/core/location"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { announce, result } from "./child-liveness"
import { location } from "./location"

announce()

const directory = process.argv[2]
const waitRoot = process.argv[3] === "wait-root"
const rootFile = path.join(await fs.realpath(directory), "root-file.txt")
const head = await fs.realpath(path.join(directory, ".git")).then((git) => path.join(git, "HEAD"))

const inotifyInstances = async () =>
  (
    await Promise.all(
      (await fs.readdir("/proc/self/fd")).map((fd) => fs.readlink(`/proc/self/fd/${fd}`).catch(() => "")),
    )
  ).filter((target) => target === "anon_inode:inotify").length

const program = Effect.gen(function* () {
  const events = yield* EventV2.Service
  const watcher = yield* Watcher.Service
  const booted = yield* watcher.status
  const instances = yield* Effect.promise(inotifyInstances)

  // A timer firing here proves this thread was not parked by the subscriptions.
  const progressed = yield* Effect.promise(() => new Promise<boolean>((resolve) => setTimeout(() => resolve(true), 50)))

  const delivered = yield* Deferred.make<string>()
  const rootDelivered = yield* Deferred.make<string>()
  yield* events.subscribe(Watcher.Event.Updated).pipe(
    Stream.runForEach((event) => {
      if (event.data.file === head) return Deferred.succeed(delivered, event.data.event).pipe(Effect.asVoid)
      if (event.data.file === rootFile) return Deferred.succeed(rootDelivered, event.data.event).pipe(Effect.asVoid)
      return Effect.void
    }),
    Effect.forkScoped,
  )
  yield* Effect.yieldNow
  const branch = `refusal-${Math.random().toString(36).slice(2)}`
  yield* Effect.promise(() => $`git switch -q -c ${branch}`.cwd(directory).quiet())
  const headEvent = yield* Deferred.await(delivered).pipe(Effect.timeoutOption("3 seconds"))

  const settled = waitRoot
    ? yield* Effect.gen(function* () {
        while (true) {
          const items = yield* watcher.status
          if (items.find((item) => item.watch === "root")?.state !== "starting") return items
          yield* Effect.sleep("100 millis")
        }
      }).pipe(Effect.timeoutOption("15 seconds"))
    : undefined

  // Observed delivery, not the reported status, is what shows the root watch works.
  yield* Effect.promise(() => fs.writeFile(rootFile, "root"))
  const rootEvent = yield* Deferred.await(rootDelivered).pipe(Effect.timeoutOption("3 seconds"))

  return {
    booted,
    instances,
    progressed,
    headEvent: headEvent._tag === "Some" ? headEvent.value : undefined,
    rootEvent: rootEvent._tag === "Some" ? rootEvent.value : undefined,
    settled: settled?._tag === "Some" ? settled.value : undefined,
  }
})

const layer = AppNodeBuilder.build(LayerNode.group([Watcher.node, EventV2.node, FSUtil.node]), [
  [Config.node, Layer.succeed(Config.Service, Config.Service.of({ entries: () => Effect.succeed([]) }))],
  [
    Location.node,
    Layer.succeed(
      Location.Service,
      Location.Service.of(
        location(
          { directory: AbsolutePath.make(directory) },
          { vcs: { type: "git", store: AbsolutePath.make(path.join(directory, ".git")) } },
        ),
      ),
    ),
  ],
]).pipe(
  Layer.provide(
    ConfigProvider.layer(
      ConfigProvider.fromUnknown({
        OPENCODE_EXPERIMENTAL_FILEWATCHER: "true",
        OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER: "false",
      }),
    ),
  ),
)

result(await program.pipe(Effect.scoped, Effect.provide(layer), Effect.runPromise))
