// Boots the production Watcher layer with the gated parcel worker and drives one
// root-watch status transition, printing the observed status and logs. It runs
// in its own process because the process has a single parcel host.
import fs from "fs/promises"
import path from "path"
import { BroadcastChannel } from "worker_threads"
import { ConfigProvider, Deferred, Effect, Fiber, Layer, Logger, Scope, Stream } from "effect"
import { TestClock } from "effect/testing"
import { Config } from "@opencode-ai/core/config"
import { makeLocationNode } from "@opencode-ai/core/effect/app-node"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Git } from "@opencode-ai/core/git"
import { Watcher } from "@opencode-ai/core/filesystem/watcher"
import { Location } from "@opencode-ai/core/location"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { announce, result } from "./child-liveness"
import { GATE } from "./gated-parcel-worker"
import { location } from "./location"

announce()

const directory = await fs.realpath(process.argv[2])
const scenario = process.argv[3]
const TIMEOUT = 10_000

const channel = new BroadcastChannel(GATE)
const held = Promise.withResolvers<void>()
channel.onmessage = (event: { data: { type: string; message?: string } }) => {
  if (event.data.type === "held") held.resolve()
}

const logs: string[] = []
const loggerLayer = Logger.layer(
  [
    Logger.make((options) => {
      const message = Array.isArray(options.message) ? options.message : [options.message]
      const fields = message[1]
      if (typeof fields === "object" && fields !== null && "watch" in fields && fields.watch === "root")
        logs.push(String(message[0]))
    }),
  ],
  { mergeWithExisting: true },
)

// Real-time waits: the TestClock does not advance on its own. Deadlines only bound failure.
const rootState = (watcher: Watcher.Interface, state: Watcher.WatchState) =>
  Effect.gen(function* () {
    const deadline = Date.now() + 30_000
    while (true) {
      const root = (yield* watcher.status).find((item) => item.watch === "root")
      if (root?.state === state) return root
      if (Date.now() > deadline)
        return yield* Effect.fail(new Error(`root watch never became ${state}: ${JSON.stringify(root)}`))
      yield* Effect.promise(() => Bun.sleep(20))
    }
  })

// Waits for the gate to report a held acknowledgement. A root watch that became
// unavailable instead (the gated worker failed) ends the wait with its reason.
const awaitHeld = (watcher: Watcher.Interface) =>
  Effect.gen(function* () {
    const state = { held: false }
    void held.promise.then(() => (state.held = true))
    const deadline = Date.now() + 60_000
    while (!state.held) {
      const current = yield* root(watcher)
      if (current?.state === "unavailable")
        return yield* Effect.fail(new Error(`root watch unavailable before its acknowledgement: ${current.reason}`))
      if (Date.now() > deadline) return yield* Effect.fail(new Error("the gate never held an acknowledgement"))
      yield* Effect.promise(() => Bun.sleep(20))
    }
  })

const root = (watcher: Watcher.Interface) =>
  watcher.status.pipe(Effect.map((items) => items.find((item) => item.watch === "root")))

const delivered = (file: string) =>
  Effect.gen(function* () {
    const events = yield* EventV2.Service
    const seen = yield* Deferred.make<string>()
    const fiber = yield* events.subscribe(Watcher.Event.Updated).pipe(
      Stream.runForEach((event) =>
        event.data.file === file ? Deferred.succeed(seen, event.data.event).pipe(Effect.asVoid) : Effect.void,
      ),
      Effect.forkScoped,
    )
    yield* Effect.yieldNow
    yield* Effect.promise(() => fs.writeFile(file, "x"))
    const event = yield* Effect.raceFirst(
      Deferred.await(seen),
      Effect.promise(() => Bun.sleep(15_000)).pipe(Effect.andThen(Effect.fail(new Error(`no event for ${file}`)))),
    )
    yield* Fiber.interrupt(fiber)
    return event
  })

const scenarios: Record<string, Effect.Effect<unknown, Error, Watcher.Service | EventV2.Service | Scope.Scope>> = {
  // Acknowledged first, then the backstop expires.
  backstop: Effect.gen(function* () {
    const watcher = yield* Watcher.Service
    yield* awaitHeld(watcher)
    channel.postMessage({ type: "release" })
    yield* rootState(watcher, "active")
    const before = [...logs]
    yield* TestClock.adjust(TIMEOUT)
    return {
      before,
      after: yield* root(watcher),
      logs: [...logs],
      event: yield* delivered(path.join(directory, "a.txt")),
    }
  }),
  // The backstop expires while the acknowledgement is held; nothing writes files,
  // so only the late acknowledgement can activate the watch.
  late: Effect.gen(function* () {
    const watcher = yield* Watcher.Service
    yield* awaitHeld(watcher)
    yield* TestClock.adjust(TIMEOUT)
    const expired = yield* root(watcher)
    channel.postMessage({ type: "release" })
    yield* rootState(watcher, "active")
    return { expired, logs: [...logs] }
  }),
  // A callback error on an active watch, then a real event re-confirms it.
  reactivate: Effect.gen(function* () {
    const watcher = yield* Watcher.Service
    yield* awaitHeld(watcher)
    channel.postMessage({ type: "release" })
    yield* rootState(watcher, "active")
    channel.postMessage({ type: "error", message: "Events were dropped by the FSEvents client." })
    const errored = yield* rootState(watcher, "unconfirmed")
    const event = yield* delivered(path.join(directory, "b.txt"))
    return { errored, event, after: yield* rootState(watcher, "active"), logs: [...logs] }
  }),
}

const gatedWatcher = makeLocationNode({
  service: Watcher.Service,
  layer: Watcher.layerWith({ workerTarget: new URL("./gated-parcel-worker.ts", import.meta.url) }),
  deps: [FSUtil.node, Location.node, Config.node, Git.node, EventV2.node],
})

const layer = AppNodeBuilder.build(LayerNode.group([gatedWatcher, EventV2.node, FSUtil.node]), [
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
        OPENCODE_EXPERIMENTAL_WATCHER_SUBSCRIBE_TIMEOUT_MS: String(TIMEOUT),
      }),
    ),
  ),
)

result(
  await scenarios[scenario].pipe(
    Effect.scoped,
    Effect.provide(layer),
    Effect.provide(Layer.mergeAll(TestClock.layer(), loggerLayer)),
    Effect.runPromise,
  ),
)
