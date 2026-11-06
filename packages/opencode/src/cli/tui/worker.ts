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
import { Cause, Effect, ManagedRuntime } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner"
import { disposeAllInstancesAndEmitGlobalDisposed } from "@/server/global-lifecycle"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { readGroupMembers } from "@opencode-ai/core/process-group"
import { ProcessGroup } from "./process-group"

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
let testShutdownGate: Promise<void> | undefined
const testSpawnerRuntimes = new Set<unknown>()

const groupDiscoveryHoldFile = process.env.OPENCODE_TEST_HOME
  ? process.env.SWX_TUI_GROUP_DISCOVERY_HOLD_FILE
  : undefined
const groupDiscoveryFailure = process.env.OPENCODE_TEST_HOME && process.env.SWX_TUI_GROUP_DISCOVERY_FAILURE === "1"
const groupTracker = ProcessGroup.makeWorkerTracker(
  (event) => Rpc.emit("process.group", event),
  5_000,
  async (pgid, identity) => {
    if (groupDiscoveryHoldFile) {
      const startedFile = `${groupDiscoveryHoldFile}.started`
      const releaseFile = `${groupDiscoveryHoldFile}.release`
      await Bun.write(startedFile, JSON.stringify({ pgid }))
      const deadline = Date.now() + 30_000
      while (Date.now() < deadline && !(await Bun.file(releaseFile).exists())) await Bun.sleep(20)
      if (!(await Bun.file(releaseFile).exists())) throw new Error("test group discovery was not released")
    }
    if (groupDiscoveryFailure) throw new Error("injected process-group discovery failure")
    return readGroupMembers(pgid, identity)
  },
)
CrossSpawnSpawner.setProcessGroupObserver(groupTracker.observe)

export const rpc = {
  ready() {
    return true
  },
  async testSpawnProcessGroup(input: {
    resistTerm: boolean
    blockShutdown: boolean
    exitMode?: "long" | "cleanup-owed" | "release"
  }) {
    // The private Worker probe exists only in isolated test homes to make group lifecycle cases deterministic.
    const recordFile = process.env.SWX_TUI_GROUP_RECORD_FILE
    if (!process.env.OPENCODE_TEST_HOME || !recordFile)
      throw new Error("test process-group fixture is not configured")
    if (input.blockShutdown) testShutdownGate = new Promise<void>(() => {})
    const spawned = Promise.withResolvers<{ pgid: number; identity: EventWriter.ProcessIdentity }>()
    const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`
    const descendantFile = `${recordFile}.descendant`
    const leaderExitFile = `${recordFile}.exit`
    const descendantCode = input.resistTerm
      ? 'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(10**6)'
      : "import time; time.sleep(10**6)"
    const launch = `nohup python3 -c ${quote(descendantCode)} >/dev/null 2>&1 & printf '%s' "$!" > ${quote(descendantFile)}`
    const exitMode = input.exitMode ?? "long"
    const finish =
      exitMode === "long"
        ? "wait"
        : `while [ ! -e ${quote(leaderExitFile)} ]; do sleep 0.02; done; exit ${exitMode === "cleanup-owed" ? "1" : "0"}`
    const command = `${input.resistTerm ? "trap '' TERM; " : ""}${launch}; ${finish}`
    const runtime = ManagedRuntime.make(LayerNode.compile(CrossSpawnSpawner.node))
    testSpawnerRuntimes.add(runtime)
    const task = Effect.scoped(
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner
        const child = yield* spawner.spawn(
          ChildProcess.make("bash", ["--noprofile", "--norc", "-c", command], {
            cwd: process.cwd(),
            env: {
              ...process.env,
              SWX_TUI_GROUP_LEADER_EXIT_FILE: leaderExitFile,
            },
            stdin: "ignore",
            stdout: "ignore",
            stderr: "ignore",
            detached: true,
            forceKillAfter: "3 seconds",
          }),
        )
        const pgid = Number(child.pid)
        const identity = EventWriter.readIdentity(pgid)
        if (!identity?.bootID || !identity.startTicks) throw new Error("Worker process group has no identity")
        const descendantPID = yield* Effect.promise(async () => {
          const deadline = Date.now() + 10_000
          while (Date.now() < deadline) {
            const value = await Bun.file(descendantFile).text().catch(() => "")
            const pid = Number(value)
            if (Number.isSafeInteger(pid) && pid > 0) return pid
            await Bun.sleep(20)
          }
          throw new Error("Worker group leader did not publish its descendant PID")
        })
        const descendant = EventWriter.readIdentity(descendantPID)
        if (!descendant?.bootID || !descendant.startTicks)
          throw new Error("Worker process-group descendant has no identity")
        yield* Effect.promise(() => Bun.write(recordFile, JSON.stringify({ leader: identity, descendant, pgid })))
        spawned.resolve({ pgid, identity })
        yield* Effect.never
      }),
    ).pipe(
      Effect.catchCause((cause) =>
        Effect.sync(() => spawned.reject(new Error(`Worker process-group spawn failed: ${Cause.pretty(cause)}`))),
      ),
    )
    void runtime.runFork(task)
    return await spawned.promise
  },
  async testCompletedProcessGroups(input: { count: number }) {
    if (!process.env.OPENCODE_TEST_HOME || !Number.isSafeInteger(input.count) || input.count < 1 || input.count > 64)
      throw new Error("completed process-group test inputs are invalid")
    const runtime = ManagedRuntime.make(LayerNode.compile(CrossSpawnSpawner.node))
    testSpawnerRuntimes.add(runtime)
    return runtime.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const spawner = yield* ChildProcessSpawner
          const sizes: number[] = []
          for (let index = 0; index < input.count; index++) {
            const child = yield* spawner.spawn(
              ChildProcess.make(process.execPath, ["-e", "process.exit(0)"], {
                stdin: "ignore",
                stdout: "ignore",
                stderr: "ignore",
                detached: true,
              }),
            )
            yield* child.exitCode.pipe(Effect.timeout("5 seconds"))
            const size = yield* Effect.promise(async () => {
              const deadline = Date.now() + 5_000
              while (groupTracker.size > 0 && Date.now() < deadline) await Bun.sleep(20)
              return groupTracker.size
            })
            if (size !== 0) throw new Error(`completed process group ${index} remained tracked`)
            sizes.push(size)
          }
          return sizes
        }),
      ),
    )
  },
  testProcessGroupTrackerSize() {
    if (!process.env.OPENCODE_TEST_HOME) throw new Error("test process-group fixture is not configured")
    return groupTracker.size
  },
  ackProcessGroup(input: { eventID: number }) {
    return groupTracker.acknowledge(input.eventID)
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
