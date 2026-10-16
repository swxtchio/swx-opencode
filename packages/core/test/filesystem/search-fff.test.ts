import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import fs from "node:fs/promises"
import path from "node:path"
import { Config } from "@opencode-ai/core/config"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { FileSystemSearch } from "@opencode-ai/core/filesystem/search"
import { WatcherConfig } from "@opencode-ai/core/filesystem/watcher-config"
import { Global } from "@opencode-ai/core/global"
import { Location } from "@opencode-ai/core/location"
import { AbsolutePath, RelativePath } from "@opencode-ai/core/schema"
import { location } from "../fixture/location"
import { tmpdir } from "../fixture/tmpdir"
import { testEffect } from "../lib/effect"

const it = testEffect(Layer.empty)
const withTmp = <A, E, R>(f: (directory: string) => Effect.Effect<A, E, R>) =>
  Effect.acquireRelease(
    Effect.promise(() => tmpdir()),
    (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
  ).pipe(Effect.flatMap((tmp) => f(tmp.path)))

describe("FileSystemSearch.fffLayer", () => {
  it.live(
    "supports native file, directory, glob, and content search",
    () =>
      withTmp((directory) =>
        Effect.gen(function* () {
          yield* Effect.promise(() => fs.mkdir(path.join(directory, "src", "nested"), { recursive: true }))
          yield* Effect.promise(() =>
            fs.writeFile(path.join(directory, "src", "nested", "needle-target.ts"), "picker-content\n"),
          )

          yield* Effect.scoped(
            Effect.gen(function* () {
              const search = yield* FileSystemSearch.Service
              const target = RelativePath.make("src/nested/needle-target.ts")
              expect((yield* waitForFile(search, target)).map((item) => item.path)).toContain(target)

              const directories = yield* search.find({ type: "directory", query: "nested", limit: 10 })
              expect(directories.some((item) => item.path.includes("nested"))).toBe(true)

              const glob = yield* search.glob({ pattern: "src/nested/*.ts", limit: 10 })
              expect(glob.map((item) => item.path)).toContain(target)

              const matches = yield* search.grep({ pattern: "picker-content", limit: 10 })
              expect(matches.some((match) => match.text.includes("picker-content"))).toBe(true)
            }).pipe(Effect.provide(searchLayer(directory))),
          )
        }),
      ),
    15_000,
  )

  it.live(
    "honors a Core WatcherConfig fff false value and keeps native search useful",
    () =>
      withTmp((directory) =>
        Effect.promise(async () => {
          await fs.mkdir(path.join(directory, "src", "nested"), { recursive: true })
          await fs.writeFile(path.join(directory, "src", "nested", "needle-target.ts"), "picker-content\n")
          await fs.writeFile(path.join(directory, "opencode.json"), JSON.stringify({ watcher: { fff: false } }))
        }).pipe(
          Effect.andThen(
            Effect.scoped(
              Effect.gen(function* () {
                const search = yield* FileSystemSearch.Service
                const target = RelativePath.make("src/nested/needle-target.ts")
                const files = yield* waitForFile(search, target)
                const directories = yield* search.find({ type: "directory", query: "nested", limit: 10 })
                const glob = yield* search.glob({ pattern: "src/nested/*.ts", limit: 10 })
                const matches = yield* search.grep({ pattern: "picker-content", limit: 10 })
                expect(files.map((item) => item.path)).toContain(target)
                expect(directories.some((item) => item.path.includes("nested"))).toBe(true)
                expect(glob.map((item) => item.path)).toContain(target)
                expect(matches.some((item) => item.text.includes("picker-content"))).toBe(true)
                if (process.platform === "linux") expect(yield* noNativeWatcherAfterScan()).toBe(false)
              }).pipe(Effect.provide(searchLayer(directory))),
            ),
          ),
        ),
      ),
    15_000,
  )
})

function searchLayer(directory: string) {
  const locationLayer = Layer.succeed(
    Location.Service,
    Location.Service.of(location({ directory: AbsolutePath.make(directory) })),
  )
  const watcherConfigLayer = AppNodeBuilder.build(WatcherConfig.node, [
    [Config.node, configLayer(directory)],
    [Location.node, locationLayer],
  ])
  return Layer.provide(FileSystemSearch.fffLayer, Layer.mergeAll(watcherConfigLayer, locationLayer))
}

function configLayer(directory: string) {
  const locationLayer = Layer.succeed(
    Location.Service,
    Location.Service.of(location({ directory: AbsolutePath.make(directory) })),
  )
  return AppNodeBuilder.build(Config.node, [
    [Location.node, locationLayer],
    [Global.node, Global.layerWith({ config: path.join(directory, ".global") })],
  ])
}

function waitForFile(
  search: FileSystemSearch.Interface,
  relativePath: RelativePath,
  started = Date.now(),
): ReturnType<FileSystemSearch.Interface["find"]> {
  return Effect.gen(function* () {
    const files = yield* search.find({ type: "file", query: "needle-target", limit: 10 })
    if (files.some((item) => item.path === relativePath)) return files
    if (Date.now() - started > 10_000) throw new Error(`FFF did not index ${relativePath}`)
    yield* Effect.sleep("100 millis")
    return yield* waitForFile(search, relativePath, started)
  })
}

function noNativeWatcherAfterScan() {
  if (process.platform !== "linux") return Effect.succeed(undefined)
  return Effect.gen(function* () {
    const deadline = Date.now() + 5_000
    let absentSince: number | undefined
    while (Date.now() < deadline) {
      const tasks = yield* Effect.promise(() => fs.readdir("/proc/self/task"))
      const names = yield* Effect.promise(() =>
        Promise.all(tasks.map((task) => fs.readFile(`/proc/self/task/${task}/comm`, "utf8").catch(() => ""))),
      )
      if (names.some((name) => name.trim().startsWith("notify-rs")))
        return yield* Effect.fail(new Error("Core watcher.fff=false started the native FFF watcher"))
      absentSince ??= Date.now()
      if (Date.now() - absentSince >= 500) return false
      yield* Effect.sleep("20 millis")
    }
    return yield* Effect.fail(new Error("Core FFF watcher absence was not stable after search indexing"))
  })
}
