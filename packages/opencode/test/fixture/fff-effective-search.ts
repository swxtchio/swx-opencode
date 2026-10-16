import { Account } from "../../src/account/account"
import { Auth } from "../../src/auth"
import { buildOpenCodeLocationServiceMap, LocationServiceMap } from "../../src/filesystem/location-services"
import { AccountTest } from "../fake/account"
import { AuthTest } from "../fake/auth"
import { NpmTest } from "../fake/npm"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { FileSystemSearch } from "@opencode-ai/core/filesystem/search"
import { Watcher } from "@opencode-ai/core/filesystem/watcher"
import { Global } from "@opencode-ai/core/global"
import { Location } from "@opencode-ai/core/location"
import { Npm } from "@opencode-ai/core/npm"
import { AbsolutePath, RelativePath } from "@opencode-ai/core/schema"
import { ConfigProvider, Effect, Layer } from "effect"
import fs from "node:fs/promises"
import path from "node:path"
import { HttpClient } from "effect/unstable/http"

const root = process.argv[2]
const global = process.argv[3]
const expectWatcher = process.argv[4] === "watch"
if (!root || !global) throw new Error("expected project and global config directories")
;(Global.Path as { config: string }).config = global

const unexpectedHttp = HttpClient.make((request) =>
  Effect.die(`unexpected HTTP request: ${request.method} ${request.url}`),
)
const replacements = [
  [Auth.node, AuthTest.empty],
  [Account.node, AccountTest.empty],
  [Npm.node, NpmTest.noop],
  [httpClient, Layer.succeed(HttpClient.HttpClient, unexpectedHttp)],
] as const
const flags = ConfigProvider.layer(
  ConfigProvider.fromUnknown({
    OPENCODE_EXPERIMENTAL_FILEWATCHER: "false",
    OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER: "false",
  }),
)
const locationServices = Layer.provide(
  LocationServiceMap.Service.get(Location.Ref.make({ directory: AbsolutePath.make(root) })),
  Layer.provide(buildOpenCodeLocationServiceMap(replacements), flags),
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
      const watcherStatus = yield* watcher.status
      const indexed = yield* waitForScanProgress(search)
      if (!indexed) return yield* Effect.fail(new Error("FFF initial scan did not reach every fixture sentinel"))
      // Only sample thread state after the production picker indexed known files at the start, middle, and end of the fixture.
      const nativeWatcher = yield* nativeWatcherReady(expectWatcher)
      return {
        files: files.map((item) => item.path),
        directories: directories.map((item) => item.path),
        glob: glob.map((item) => item.path),
        matches: matches.map((item) => item.text),
        watcherStatus,
        nativeWatcher,
      }
    }).pipe(Effect.provide(locationServices)),
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

function waitForScanProgress(search: FileSystemSearch.Interface) {
  const expected = ["src/a-first.ts", "src/nested/needle-target.ts", "z-last.ts"]
  const patterns = ["src/a-first.ts", "src/nested/*.ts", "z-last.ts"]
  return Effect.gen(function* () {
    const deadline = Date.now() + 10_000
    while (Date.now() < deadline) {
      const results = yield* Effect.forEach(patterns, (pattern) => search.glob({ pattern, limit: 10 }))
      if (expected.every((target, index) => results[index].some((item) => item.path === target))) return true
      yield* Effect.sleep("20 millis")
    }
    return yield* Effect.fail(new Error("FFF initial scan did not index every sentinel file"))
  })
}

function nativeWatcherReady(expected: boolean) {
  if (process.platform !== "linux") return Effect.succeed(undefined)
  return Effect.gen(function* () {
    const deadline = Date.now() + 10_000
    let absentSince: number | undefined
    while (Date.now() < deadline) {
      const tasks = yield* Effect.promise(() => fs.readdir("/proc/self/task"))
      const names = yield* Effect.promise(() =>
        Promise.all(tasks.map((task) => fs.readFile(`/proc/self/task/${task}/comm`, "utf8").catch(() => ""))),
      )
      const running = names.some((name) => name.trim().startsWith("notify-rs"))
      if (expected && running) return true
      if (!expected && running) return yield* Effect.fail(new Error("FFF native watcher started in no-watch mode"))
      if (!expected) {
        absentSince ??= Date.now()
        if (Date.now() - absentSince >= 500) return false
      }
      yield* Effect.sleep("20 millis")
    }
    return yield* Effect.fail(new Error(`FFF watcher did not reach expected state: ${expected}`))
  })
}
