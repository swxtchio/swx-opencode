import { buildLocationServiceMap, LocationServiceMap } from "@opencode-ai/core/location-services"
import { WatcherConfig } from "@opencode-ai/core/filesystem/watcher-config"
import { makeLocationNode } from "@opencode-ai/core/effect/app-node"
import { Location } from "@opencode-ai/core/location"
import { Config } from "@/config/config"
import { InstanceRef } from "@/effect/instance-ref"
import { Project } from "@opencode-ai/schema/project"
import { Effect, Layer } from "effect"

export { LocationServiceMap }

export const watcherConfigNode = makeLocationNode({
  service: WatcherConfig.Service,
  layer: Layer.effect(
    WatcherConfig.Service,
    Effect.gen(function* () {
      const config = yield* Config.Service
      const location = yield* Location.Service
      const instance = (yield* InstanceRef) ?? {
        directory: location.directory,
        worktree: location.project.directory,
        project: {
          id: Project.ID.global,
          worktree: location.project.directory,
          time: { created: 0, updated: 0 },
          sandboxes: [],
        },
      }
      return WatcherConfig.Service.of({
        get: () => Effect.map(config.get().pipe(Effect.provideService(InstanceRef, instance)), (info) => info.watcher),
      })
    }),
  ),
  deps: [Config.node, Location.node],
})

export function buildOpenCodeLocationServiceMap() {
  return buildLocationServiceMap([[WatcherConfig.node, watcherConfigNode]])
}

export const locationServiceMapLayer = buildOpenCodeLocationServiceMap()
