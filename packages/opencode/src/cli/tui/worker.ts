import { Server } from "@/server/server"
import { InstanceRuntime } from "@/project/instance-runtime"
import { Rpc } from "@/util/rpc"
import { upgrade } from "@/cli/upgrade"
import { Config } from "@/config/config"
import { GlobalBus } from "@/bus/global"
import { ServerAuth } from "@/server/auth"
import { writeHeapSnapshot } from "node:v8"
import { Heap } from "@/cli/heap"
import { AppRuntime } from "@/effect/app-runtime"
import { Effect, ManagedRuntime } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner"
import { disposeAllInstancesAndEmitGlobalDisposed } from "@/server/global-lifecycle"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventWriter } from "@opencode-ai/core/event/writer"

Heap.start()

const onUnhandledRejection = (_error: unknown) => {}

const onUncaughtException = (_error: Error) => {}

process.on("unhandledRejection", onUnhandledRejection)
process.on("uncaughtException", onUncaughtException)

// Subscribe to global events and forward them via RPC
GlobalBus.on("event", (event) => {
  Rpc.emit("global.event", event)
})

let server: Awaited<ReturnType<typeof Server.listen>> | undefined
const groupAcknowledgements = new Map<number, () => void>()
const groupIdentities = new Map<number, EventWriter.ProcessIdentity>()
let groupEventID = 0
let testShutdownGate: Promise<void> | undefined
const testSpawnerRuntimes = new Set<unknown>()

CrossSpawnSpawner.setProcessGroupObserver(async (event) => {
  const identity = EventWriter.readIdentity(event.pgid) ?? groupIdentities.get(event.pgid) ?? {
    pid: event.pgid,
    bootID: null,
    startTicks: null,
  }
  groupIdentities.set(event.pgid, identity)
  const eventID = groupEventID++
  const acknowledgement = Promise.withResolvers<void>()
  groupAcknowledgements.set(eventID, acknowledgement.resolve)
  Rpc.emit("process.group", { ...event, eventID, identity })
  await acknowledgement.promise
})

export const rpc = {
  ready() {
    return true
  },
  async testSpawnProcessGroup(input: { resistTerm: boolean; blockShutdown: boolean }) {
    const fixtureFile = process.env.SWX_TUI_GROUP_FIXTURE_FILE
    if (!process.env.OPENCODE_TEST_HOME || !fixtureFile || !process.env.SWX_TUI_GROUP_RECORD_FILE)
      throw new Error("test process-group fixture is not configured")
    if (input.blockShutdown) testShutdownGate = new Promise<void>(() => {})
    const spawned = Promise.withResolvers<number>()
    const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`
    const command = `${input.resistTerm ? "trap '' TERM; " : ""}exec ${quote(process.execPath)} ${quote(fixtureFile)}`
    const runtime = ManagedRuntime.make(LayerNode.compile(CrossSpawnSpawner.node))
    testSpawnerRuntimes.add(runtime)
    void runtime.runFork(
      Effect.scoped(
        Effect.gen(function* () {
          const spawner = yield* ChildProcessSpawner
          const child = yield* spawner.spawn(
            ChildProcess.make("bash", ["-lc", command], {
              cwd: process.cwd(),
              env: process.env,
              stdin: "ignore",
              stdout: "ignore",
              stderr: "ignore",
              detached: true,
              forceKillAfter: "3 seconds",
            }),
          )
          spawned.resolve(Number(child.pid))
          yield* Effect.never
        }),
      ),
    )
    return { pgid: await spawned.promise }
  },
  ackProcessGroup(input: { eventID: number }) {
    const acknowledge = groupAcknowledgements.get(input.eventID)
    if (!acknowledge) return false
    groupAcknowledgements.delete(input.eventID)
    acknowledge()
    return true
  },
  async fetch(input: { url: string; method: string; headers: Record<string, string>; body?: string }) {
    const headers = { ...input.headers }
    const auth = ServerAuth.header()
    if (auth && !headers["authorization"] && !headers["Authorization"]) {
      headers["Authorization"] = auth
    }
    const request = new Request(input.url, {
      method: input.method,
      headers,
      body: input.body,
    })
    const response = await Server.Default().app.fetch(request)
    const body = await response.text()
    return {
      status: response.status,
      headers: Object.fromEntries(response.headers.entries()),
      body,
    }
  },
  snapshot() {
    const result = writeHeapSnapshot("server.heapsnapshot")
    return result
  },
  async server(input: { port: number; hostname: string; mdns?: boolean; cors?: string[] }) {
    if (server) await server.stop(true)
    server = await Server.listen(input)
    return { url: server.url.toString() }
  },
  async checkUpgrade(input: { directory: string }) {
    await InstanceRuntime.load({ directory: input.directory })
    await upgrade().catch(() => {})
  },
  async reload() {
    await AppRuntime.runPromise(
      Effect.gen(function* () {
        const cfg = yield* Config.Service
        yield* cfg.invalidate()
        yield* disposeAllInstancesAndEmitGlobalDisposed({ swallowErrors: true })
      }),
    )
  },
  async shutdown() {
    if (testShutdownGate) await testShutdownGate
    await InstanceRuntime.disposeAllInstances()
    if (server) await server.stop(true)
    await CrossSpawnSpawner.shutdownOwnedProcessGroups()
    process.off("unhandledRejection", onUnhandledRejection)
    process.off("uncaughtException", onUncaughtException)
  },
}

Rpc.listen(rpc)
