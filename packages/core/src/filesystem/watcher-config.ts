export * as WatcherConfig from "./watcher-config"

import { Context, Effect, Layer } from "effect"
import { Config } from "../config"
import { ConfigWatcher } from "../config/watcher"
import { makeLocationNode } from "../effect/app-node"

export interface Interface {
  readonly get: () => Effect.Effect<ConfigWatcher.Info | undefined>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/WatcherConfig") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const config = yield* Config.Service
    return Service.of({
      get: () =>
        Effect.gen(function* () {
          const entries = yield* config.entries()
          const documents = entries.filter((entry): entry is Config.Document => entry.type === "document")
          const fff = documents.flatMap((entry) =>
            entry.info.watcher?.fff === undefined ? [] : [entry.info.watcher.fff],
          )
          const ignore = documents.flatMap((entry) => entry.info.watcher?.ignore ?? [])
          return { fff: fff.at(-1), ignore }
        }),
    })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [Config.node],
})
