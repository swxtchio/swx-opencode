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
