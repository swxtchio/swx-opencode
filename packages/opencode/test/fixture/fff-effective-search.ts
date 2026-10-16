import { Account } from "../../src/account/account"
import { Auth } from "../../src/auth"
import { watcherConfigNode } from "../../src/filesystem/location-services"
import { AccountTest } from "../fake/account"
import { AuthTest } from "../fake/auth"
import { NpmTest } from "../fake/npm"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { FileSystemSearch } from "@opencode-ai/core/filesystem/search"
import { Watcher } from "@opencode-ai/core/filesystem/watcher"
import { WatcherConfig } from "@opencode-ai/core/filesystem/watcher-config"
import { Global } from "@opencode-ai/core/global"
import { Git } from "@opencode-ai/core/git"
import { Location } from "@opencode-ai/core/location"
import { Npm } from "@opencode-ai/core/npm"
import { AbsolutePath, RelativePath } from "@opencode-ai/core/schema"
import { Project } from "@opencode-ai/schema/project"
import { ConfigProvider, Effect, Layer } from "effect"
import fs from "node:fs/promises"
import path from "node:path"
import { HttpClient } from "effect/unstable/http"

const root = process.argv[2]
const global = process.argv[3]
const git = process.argv[4] === "git"
if (!root || !global) throw new Error("expected project and global config directories")
;(Global.Path as { config: string }).config = global

const unexpectedHttp = HttpClient.make((request) =>
  Effect.die(`unexpected HTTP request: ${request.method} ${request.url}`),
)
const locationLayer = Layer.succeed(
  Location.Service,
  Location.Service.of({
    directory: AbsolutePath.make(root),
    project: { id: Project.ID.global, directory: AbsolutePath.make(root) },
    vcs: git ? { type: "git", store: AbsolutePath.make(path.join(root, ".git")) } : undefined,
  }),
)
const configLayer = LayerNode.compile(
  LayerNode.group([
    watcherConfigNode,
    FileSystemSearch.node,
    Watcher.node,
    EventV2.node,
    Git.node,
    CrossSpawnSpawner.node,
  ]),
  [
    [WatcherConfig.node, watcherConfigNode],
    [Auth.node, AuthTest.empty],
    [Account.node, AccountTest.empty],
    [Npm.node, NpmTest.noop],
    [httpClient, Layer.succeed(HttpClient.HttpClient, unexpectedHttp)],
    [Location.node, locationLayer],
  ],
)
const layer = Layer.provide(
  configLayer,
  ConfigProvider.layer(
    ConfigProvider.fromUnknown({
      OPENCODE_EXPERIMENTAL_FILEWATCHER: "false",
      OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER: "false",
    }),
  ),
)

const result = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const search = yield* FileSystemSearch.Service
      const watcher = yield* Watcher.Service
      const target = RelativePath.make("src/nested/needle-target.ts")
      const files = yield* waitForFile(search, target)
      const directories = yield* search.find({ type: "directory", query: "nested", limit: 10 })
      const glob = yield* search.glob({ pattern: "src/nested/*.ts", limit: 10 })
      const matches = yield* search.grep({ pattern: "picker-content", limit: 10 })
      const watcherConfig = yield* (yield* WatcherConfig.Service).get()
      const nativeWatcher = yield* nativeWatcherReady(watcherConfig?.fff !== false)
      const watcherStatus = yield* watcher.status
      return {
        files: files.map((item) => item.path),
        directories: directories.map((item) => item.path),
        glob: glob.map((item) => item.path),
        matches: matches.map((item) => item.text),
        watcherConfig,
        watcherStatus,
        nativeWatcher,
      }
    }).pipe(Effect.provide(layer)),
  ),
)

console.log(`FFF_REPORT ${JSON.stringify(result)}`)

function waitForFile(search: FileSystemSearch.Interface, target: RelativePath) {
  return Effect.gen(function* () {
    const deadline = Date.now() + 10_000
    while (Date.now() < deadline) {
      const files = yield* search.find({ type: "file", query: "needle-target", limit: 10 })
      if (files.some((item) => item.path === target)) return files
      yield* Effect.sleep("20 millis")
    }
    return yield* Effect.fail(new Error(`FFF did not index ${target}`))
  })
}

function nativeWatcherReady(expected: boolean) {
  if (process.platform !== "linux") return Effect.succeed(undefined)
  return Effect.gen(function* () {
    const deadline = Date.now() + 10_000
    while (Date.now() < deadline) {
      const tasks = yield* Effect.promise(() => fs.readdir("/proc/self/task"))
      const names = yield* Effect.promise(() =>
        Promise.all(tasks.map((task) => fs.readFile(`/proc/self/task/${task}/comm`, "utf8").catch(() => ""))),
      )
      const running = names.some((name) => name.trim().startsWith("notify-rs"))
      if (running === expected) return running
      yield* Effect.sleep("20 millis")
    }
    return yield* Effect.fail(new Error(`FFF watcher did not reach expected state: ${expected}`))
  })
}
