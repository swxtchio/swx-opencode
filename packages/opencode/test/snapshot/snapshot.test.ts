import { afterEach, expect } from "bun:test"
import { $ } from "bun"
import { AppProcess } from "@opencode-ai/core/process"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"
import { Hash } from "@opencode-ai/core/util/hash"
import { randomUUID } from "crypto"
import { readFileSync } from "fs"
import fs from "fs/promises"
import os from "os"
import path from "path"
import { Clock, Duration, Effect, Exit, Fiber, Layer, Logger, Option, PlatformError, Semaphore } from "effect"
import { Config } from "../../src/config/config"
import { Snapshot } from "../../src/snapshot"
import {
  MaintenanceService,
  acquire,
  lockCommand,
  maintenanceNode,
  withSnapshotLocks,
} from "../../src/snapshot/maintenance"
import type {
  FileLockAttempt,
  LockRequest,
  LockRuntime,
  MaintenanceInterface,
  RunInput,
} from "../../src/snapshot/maintenance"
import {
  disposeAllInstances,
  provideInstance,
  testInstanceStoreLayer,
  TestInstance,
  tmpdirScoped,
} from "../fixture/fixture"
import { TestConfig } from "../fixture/config"
import { awaitWithTimeout, testEffect } from "../lib/effect"

const deferred = () => {
  let resolve = () => {}
  const promise = new Promise<void>((done) => (resolve = done))
  return { promise, resolve }
}

const it = testEffect(
  Layer.mergeAll(LayerNode.compile(LayerNode.group([Snapshot.node, FSUtil.node])), testInstanceStoreLayer),
)
const maintenanceLockIt = testEffect(
  Layer.mergeAll(LayerNode.compile(maintenanceNode), LayerNode.compile(CrossSpawnSpawner.node)),
)
const realLockData = path.join(os.tmpdir(), `opencode-snapshot-real-lock-${randomUUID()}`)
const realLockSignal = { armed: false, entered: () => {} }
const realLockStarted = new Promise<void>((resolve) => (realLockSignal.entered = resolve))
const realLockGc = {
  attempts: 0,
  active: 0,
  maxActive: 0,
  contender: deferred(),
  started: deferred(),
  gate: deferred(),
}
const resetRealLockGc = () => {
  realLockGc.attempts = 0
  realLockGc.active = 0
  realLockGc.maxActive = 0
  realLockGc.contender = deferred()
  realLockGc.started = deferred()
  realLockGc.gate = deferred()
}
const realLockConfig = TestConfig.layer({
  get: () =>
    Effect.sync(() => {
      if (realLockSignal.armed) realLockSignal.entered()
      return {}
    }),
})
const realLockCalls: RunInput[] = []
const realLockLockCalls: string[] = []
const realLockTrackWait = { file: "", entered: deferred() }
const realLockMaintenanceNode = LayerNode.make({
  service: MaintenanceService,
  layer: Layer.effect(
    MaintenanceService,
    Effect.gen(function* () {
      const fs = yield* FSUtil.Service
      const acquireFile = (request: Extract<LockRequest, { role: "box" | "repo" }>) => {
        realLockLockCalls.push(request.file)
        if (request.file === realLockTrackWait.file) realLockTrackWait.entered.resolve()
        if (path.basename(request.file) === "gc.lock") {
          realLockGc.attempts++
          if (realLockGc.attempts === 2) realLockGc.contender.resolve()
        }
        return fs.ensureDir(path.dirname(request.file)).pipe(
          Effect.mapError((cause) => (cause instanceof Error ? cause : new Error(String(cause)))),
          Effect.andThen(
            Effect.tryPromise({
              try: (signal) => acquire(request.file, signal, !request.wait),
              catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
            }),
          ),
          Effect.map((attempt) =>
            attempt.status === "acquired" ? { status: "acquired" as const, lease: attempt } : attempt,
          ),
        )
      }
      const withLocks: MaintenanceInterface["withLocks"] = (locks, self) =>
        withSnapshotLocks(
          locks.map((request) =>
            request.role === "repo" && request.waitMillis !== undefined ? { ...request, waitMillis: 25 } : request,
          ),
          self,
          acquireFile,
        )
      return MaintenanceService.of({
        withLocks,
        run: (input) =>
          Effect.promise(async () => {
            realLockCalls.push(input)
            realLockGc.active++
            realLockGc.maxActive = Math.max(realLockGc.maxActive, realLockGc.active)
            realLockGc.started.resolve()
            await realLockGc.gate.promise
            realLockGc.active--
            return { exitCode: 0, stderr: "" }
          }),
        now: Effect.sync(() => Date.now()),
        random: Effect.sync(() => 0.5),
      })
    }),
  ),
  deps: [FSUtil.node],
})
const realLockIt = testEffect(
  Layer.mergeAll(
    LayerNode.compile(LayerNode.group([Snapshot.node, FSUtil.node]), [
      [maintenanceNode, realLockMaintenanceNode],
      [Global.node, Layer.succeed(Global.Service, Global.Service.of(Global.make({ data: realLockData })))],
      [Config.node, realLockConfig],
    ]),
    LayerNode.compile(CrossSpawnSpawner.node),
    testInstanceStoreLayer,
  ),
)
// Windows forbids both * and : in directory names.
const nonWindowsIt = process.platform === "win32" ? it.live.skip : it.live

const makeMaintenanceHarness = (input?: {
  readonly run?: (command: RunInput) => Effect.Effect<{ exitCode: number; stderr: string }>
  readonly onLock?: (file: string) => void
  readonly onLocks?: (locks: readonly LockRequest[]) => void
  readonly onRelease?: (file: string) => void
  readonly beforeLock?: (file: string) => Effect.Effect<void>
  readonly afterLocalContention?: () => Effect.Effect<void>
  readonly tryLock?: (file: string) => boolean
  readonly lockRuntime?: (request: Extract<LockRequest, { role: "box" | "repo" }>) => LockRuntime | undefined
  readonly lockError?: (request: Extract<LockRequest, { role: "box" | "repo" }>) => Error | undefined
  readonly workFailure?: (locks: readonly LockRequest[]) => boolean
  readonly waitMillisOverride?: number
  readonly now?: (current: number) => Effect.Effect<number>
  readonly config?: Layer.Layer<Config.Service>
  readonly effectClock?: Layer.Layer<never>
  readonly filesystemLayer?: Layer.Layer<FSUtil.Service>
  readonly appProcessLayer?: Layer.Layer<AppProcess.Service>
}) => {
  const data = path.join(os.tmpdir(), `opencode-snapshot-maintenance-${randomUUID()}`)
  const calls: RunInput[] = []
  const heldFiles = new Set<string>()
  const lockCalls: string[] = []
  const lockReleases: string[] = []
  const clock = { now: 10_000_000 }
  const runAdvance = { millis: 0 }
  const randomValues: number[] = []
  const outcome = { exitCode: 0, stderr: "" }
  const acquireFile = (
    request: Extract<LockRequest, { role: "box" | "repo" }>,
  ): Effect.Effect<FileLockAttempt, Error> =>
    Effect.gen(function* () {
      const file = request.file
      lockCalls.push(file)
      input?.onLock?.(file)
      const lockRuntime = input?.lockRuntime?.(request)
      if (lockRuntime) {
        const attempt = yield* Effect.tryPromise({
          try: (signal) => acquire(file, signal, !request.wait, lockRuntime),
          catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
        })
        return attempt.status === "acquired" ? { status: "acquired" as const, lease: attempt } : attempt
      }
      const lockError = input?.lockError?.(request)
      if (lockError) return yield* Effect.fail(lockError)
      yield* Effect.promise(async () => {
        await fs.mkdir(path.dirname(file), { recursive: true })
        const handle = await fs.open(file, "a")
        await handle.close()
      })
      if (request.wait) {
        while (input?.tryLock?.(file) === false || heldFiles.has(file)) yield* Effect.sleep(Duration.millis(5))
      } else if (input?.tryLock?.(file) === false || heldFiles.has(file)) {
        return { status: "contended" }
      }
      heldFiles.add(file)
      if (input?.beforeLock) {
        const exit = yield* Effect.exit(input.beforeLock(file))
        if (Exit.isFailure(exit)) {
          heldFiles.delete(file)
          return yield* Effect.failCause(exit.cause)
        }
      }
      return {
        status: "acquired",
        lease: {
          release: async () => {
            heldFiles.delete(file)
            lockReleases.push(file)
            input?.onRelease?.(file)
            return { exitCode: 0, stderr: "" }
          },
        },
      }
    })
  const service = MaintenanceService.of({
    withLocks: (locks, self) => {
      input?.onLocks?.(locks)
      const requests = locks.map((request) => {
        if (request.role === "local" && input?.afterLocalContention) {
          const semaphore = request.semaphore
          const afterLocalContention = input.afterLocalContention
          return {
            ...request,
            semaphore: {
              resize: (permits: number) => semaphore.resize(permits),
              take: (permits: number) => semaphore.take(permits),
              release: (permits: number) => semaphore.release(permits),
              releaseAll: semaphore.releaseAll,
              withPermit: <A, E, R>(self: Effect.Effect<A, E, R>) => semaphore.withPermit(self),
              withPermits:
                (permits: number) =>
                <A, E, R>(self: Effect.Effect<A, E, R>) =>
                  Effect.gen(function* () {
                    const available = yield* semaphore.withPermitsIfAvailable(permits)(Effect.succeed(true))
                    if (Option.isNone(available)) yield* afterLocalContention()
                    return yield* semaphore.withPermits(permits)(self)
                  }),
              withPermitsIfAvailable:
                (permits: number) =>
                <A, E, R>(self: Effect.Effect<A, E, R>) =>
                  semaphore.withPermitsIfAvailable(permits)(self),
            } satisfies Semaphore.Semaphore,
          }
        }
        if (request.role !== "local" && request.waitMillis !== undefined && input?.waitMillisOverride !== undefined) {
          return { ...request, waitMillis: input.waitMillisOverride }
        }
        return request
      })
      return withSnapshotLocks(
        requests,
        input?.workFailure?.(locks) ? Effect.die(new Error("simulated maintenance work failure")) : self,
        acquireFile,
      )
    },
    run: (command) =>
      Effect.suspend(() => {
        calls.push(command)
        if (input?.run) {
          return input.run(command).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                clock.now += runAdvance.millis
              }),
            ),
          )
        }
        return Effect.sync(() => {
          clock.now += runAdvance.millis
          return outcome
        })
      }),
    now: Effect.suspend(() => input?.now?.(clock.now) ?? Effect.succeed(clock.now)),
    random: Effect.sync(() => randomValues.shift() ?? 0.5),
  })
  const layer = Layer.mergeAll(
    LayerNode.compile(LayerNode.group([Snapshot.node, FSUtil.node]), [
      [maintenanceNode, Layer.succeed(MaintenanceService, service)],
      [Global.node, Layer.succeed(Global.Service, Global.Service.of(Global.make({ data })))],
      ...(input?.config ? [[Config.node, input.config] as const] : []),
      ...(input?.filesystemLayer ? [[FSUtil.node, input.filesystemLayer] as const] : []),
      ...(input?.appProcessLayer ? [[AppProcess.node, input.appProcessLayer] as const] : []),
    ]),
    LayerNode.compile(CrossSpawnSpawner.node),
    testInstanceStoreLayer,
    ...(input?.effectClock ? [input.effectClock] : []),
  )
  return { data, calls, lockCalls, lockReleases, clock, randomValues, runAdvance, outcome, it: testEffect(layer) }
}

const makeManualClock = () => {
  let now = 0
  const requests: { duration: number; deadline: number }[] = []
  const requestWaiters = new Set<() => void>()
  const sleepers = new Set<{
    deadline: number
    resume: (effect: Effect.Effect<void>) => void
  }>()
  const notifyRequests = () => {
    for (const waiter of Array.from(requestWaiters)) waiter()
  }
  const waitForRequests = (condition: () => boolean) =>
    new Promise<void>((resolve) => {
      if (condition()) return resolve()
      const waiter = () => {
        if (!condition()) return
        requestWaiters.delete(waiter)
        resolve()
      }
      requestWaiters.add(waiter)
    })
  const advanceTo = (target: number) =>
    Effect.gen(function* () {
      const end = Math.max(now, target)
      while (true) {
        const next = Array.from(sleepers)
          .filter((item) => item.deadline <= end)
          .sort((a, b) => a.deadline - b.deadline)[0]
        if (!next) break
        now = next.deadline
        for (const item of Array.from(sleepers).filter((item) => item.deadline <= now)) {
          sleepers.delete(item)
          item.resume(Effect.void)
        }
        yield* Effect.yieldNow
      }
      now = end
      yield* Effect.yieldNow
    })
  const service: Clock.Clock = {
    currentTimeMillisUnsafe: () => now,
    currentTimeMillis: Effect.sync(() => now),
    currentTimeNanosUnsafe: () => BigInt(Math.floor(now * 1_000_000)),
    currentTimeNanos: Effect.sync(() => BigInt(Math.floor(now * 1_000_000))),
    sleep: (duration) => {
      const millis = Duration.toMillis(duration)
      if (millis <= 0) return Effect.void
      return Effect.callback<void>((resume) => {
        const sleeper = { deadline: now + millis, resume }
        sleepers.add(sleeper)
        requests.push({ duration: millis, deadline: sleeper.deadline })
        notifyRequests()
        return Effect.sync(() => sleepers.delete(sleeper))
      })
    },
  }
  return {
    layer: Layer.succeed(Clock.Clock, service),
    requests,
    waitForRequests,
    advanceTo,
    advanceBy: (duration: Duration.Duration) => advanceTo(now + Duration.toMillis(duration)),
    get now() {
      return now
    },
    reset: () => {
      now = 0
      requests.length = 0
      requestWaiters.clear()
      sleepers.clear()
    },
  }
}

const gcHarness = makeMaintenanceHarness()
const gcWorkFailureHarness = makeMaintenanceHarness({
  run: () => Effect.die(new Error("simulated gc runner defect")),
})
const traversalHarness = makeMaintenanceHarness()
const traversalIt = traversalHarness.it
const localTrackGate = { enabled: false, started: deferred(), release: deferred() }
const localTrackSemaphoreWait = { enabled: false, entered: deferred(), release: deferred() }
const localTrackAppProcess = Layer.effect(
  AppProcess.Service,
  Effect.gen(function* () {
    const appProcess = yield* AppProcess.Service
    return AppProcess.Service.of({
      ...appProcess,
      run: (command, options) => {
        if (
          !localTrackGate.enabled ||
          command._tag !== "StandardCommand" ||
          command.command !== "git" ||
          !command.args.includes("diff-files")
        ) {
          return appProcess.run(command, options)
        }
        return Effect.promise(async () => {
          localTrackGate.started.resolve()
          await localTrackGate.release.promise
        }).pipe(Effect.andThen(appProcess.run(command, options)))
      },
    })
  }),
).pipe(Layer.provide(LayerNode.compile(AppProcess.node)))
const localTrackHarness = makeMaintenanceHarness({
  appProcessLayer: localTrackAppProcess,
  waitMillisOverride: 25,
  afterLocalContention: () => {
    if (!localTrackSemaphoreWait.enabled) return Effect.void
    return Effect.sync(() => localTrackSemaphoreWait.entered.resolve()).pipe(
      Effect.andThen(Effect.promise(() => localTrackSemaphoreWait.release.promise)),
    )
  },
})
const scheduleClock = makeManualClock()
const scheduleEvents = {
  count: 0,
  times: [] as number[],
  now: () => 0,
  waiters: new Set<() => void>(),
  waitFor(target: number) {
    return new Promise<void>((resolve) => {
      if (this.count >= target) return resolve()
      const waiter = () => {
        if (this.count < target) return
        this.waiters.delete(waiter)
        resolve()
      }
      this.waiters.add(waiter)
    })
  },
  signal() {
    this.count++
    this.times.push(this.now())
    for (const waiter of Array.from(this.waiters)) waiter()
  },
}
const scheduleHarness = makeMaintenanceHarness({
  effectClock: scheduleClock.layer,
  config: TestConfig.layer({
    get: () =>
      Effect.sync(() => {
        scheduleEvents.signal()
        return { snapshot: false }
      }),
  }),
})
const reapContention = { busy: false, repoFile: "" }
const reapWorkFailure = { failNext: false, repoFile: "" }
const reapInfraFailure = { enabled: false, repoFile: "" }
const reapHarness = makeMaintenanceHarness({
  tryLock: (file) => !reapContention.busy || file !== reapContention.repoFile,
  lockError: (request) =>
    reapInfraFailure.enabled && request.role === "repo" && request.file === reapInfraFailure.repoFile
      ? new Error("simulated advisory lock failure")
      : undefined,
  workFailure: (locks) => {
    if (!reapWorkFailure.failNext) return false
    if (!locks.some((request) => request.role === "repo" && request.file === reapWorkFailure.repoFile)) return false
    reapWorkFailure.failNext = false
    return true
  },
})
const unstatableWorktree = { path: "" }
const unstatableFilesystem = Layer.effect(
  FSUtil.Service,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    return FSUtil.Service.of({
      ...fs,
      stat: (file) =>
        file === unstatableWorktree.path
          ? Effect.fail(
              new PlatformError.PlatformError(
                new PlatformError.SystemError({
                  _tag: "PermissionDenied",
                  module: "FileSystem",
                  method: "stat",
                  pathOrDescriptor: file,
                }),
              ),
            )
          : fs.stat(file),
    })
  }),
).pipe(Layer.provide(LayerNode.compile(FSUtil.node)))
const unstatableReapHarness = makeMaintenanceHarness({ filesystemLayer: unstatableFilesystem })
const busyTrack = { busy: false, repoFile: "", waitMillis: 0 }
const busyTrackHarness = makeMaintenanceHarness({
  tryLock: (file) => !busyTrack.busy || file !== busyTrack.repoFile,
  onLocks: (locks) => {
    for (const request of locks) {
      if (request.role === "repo" && request.waitMillis !== undefined) busyTrack.waitMillis = request.waitMillis
    }
  },
})
const cleanupWait = { busy: false, requested: 0 }
const cleanupWaitHarness = makeMaintenanceHarness({
  tryLock: (file) => path.basename(file) !== "gc.lock" || !cleanupWait.busy,
  onLocks: (locks) => {
    for (const request of locks) {
      if (request.role === "box" && request.waitMillis !== undefined) cleanupWait.requested = request.waitMillis
    }
  },
  waitMillisOverride: 25,
})
const bodyFailure = { reads: 0 }
const bodyFailureHarness = makeMaintenanceHarness({
  config: TestConfig.layer({
    get: () =>
      Effect.sync(() => bodyFailure.reads++).pipe(Effect.andThen(Effect.die(new Error("simulated config failure")))),
  }),
})
const lockFailure: { role?: LockRequest["role"] } = {}
const lockFailureHarness = makeMaintenanceHarness({
  lockRuntime: (request) => (lockFailure.role === request.role ? { platform: "linux", which: () => null } : undefined),
})
const admissionFailure = { fail: false }
const admissionFailureHarness = makeMaintenanceHarness({
  now: (current) =>
    admissionFailure.fail ? Effect.die(new Error("simulated cleanup admission work failure")) : Effect.succeed(current),
})
const trackWorkFailure = { fail: false }
const trackWorkFailureHarness = makeMaintenanceHarness({
  workFailure: (locks) => trackWorkFailure.fail && locks.some((request) => request.role === "repo"),
})
const inLockGraceRace = { gitdir: "", worktree: "", since: 0 }
const inLockGraceHarness = makeMaintenanceHarness({
  beforeLock: (file) => {
    if (!inLockGraceRace.gitdir || !file.endsWith(`${Hash.fast(inLockGraceRace.worktree)}.lock`)) return Effect.void
    return Effect.promise(() =>
      fs.writeFile(
        path.join(inLockGraceRace.gitdir, "info", "opencode-worktree.json"),
        JSON.stringify({
          version: 1,
          project: path.basename(path.dirname(inLockGraceRace.gitdir)),
          worktree: inLockGraceRace.worktree,
          missingSince: inLockGraceRace.since,
        }),
      ),
    )
  },
})
const lockOrder = { started: deferred(), gate: deferred() }
const lockOrderHarness = makeMaintenanceHarness({
  run: () =>
    Effect.promise(async () => {
      lockOrder.started.resolve()
      await lockOrder.gate.promise
      return { exitCode: 0, stderr: "" }
    }),
})
const completionProbe = {
  boxLock: "",
  completion: "",
  armed: false,
  freshen: false,
  freshText: "",
  atRelease: [] as string[],
}
const completionHarness = makeMaintenanceHarness({
  beforeLock: (file) => {
    if (!completionProbe.armed || !completionProbe.freshen || file !== completionProbe.boxLock) return Effect.void
    return Effect.promise(() => fs.writeFile(completionProbe.completion, completionProbe.freshText))
  },
  onRelease: (file) => {
    if (!completionProbe.armed || file !== completionProbe.boxLock) return
    try {
      completionProbe.atRelease.push(readFileSync(completionProbe.completion, "utf8"))
    } catch {
      completionProbe.atRelease.push("missing")
    }
  },
})
const cleanupRace = {
  lockAttempts: 0,
  processCalls: 0,
  active: 0,
  maxActive: 0,
  started: deferred(),
  contender: deferred(),
  gate: deferred(),
}
const sameRepoRace = {
  lockAttempts: 0,
  active: 0,
  maxActive: 0,
  started: deferred(),
  contender: deferred(),
  gate: deferred(),
}
const sameRepoHarness = makeMaintenanceHarness({
  onLock: (file) => {
    if (path.basename(file) !== "gc.lock") return
    sameRepoRace.lockAttempts++
    if (sameRepoRace.lockAttempts === 2) sameRepoRace.contender.resolve()
  },
  run: () =>
    Effect.promise(async () => {
      sameRepoRace.active++
      sameRepoRace.maxActive = Math.max(sameRepoRace.maxActive, sameRepoRace.active)
      sameRepoRace.started.resolve()
      await sameRepoRace.gate.promise
      sameRepoRace.active--
      return { exitCode: 0, stderr: "" }
    }),
})
const concurrencyHarness = makeMaintenanceHarness({
  onLock: (file) => {
    if (path.basename(file) !== "gc.lock") return
    cleanupRace.lockAttempts++
    if (cleanupRace.lockAttempts === 2) cleanupRace.contender.resolve()
  },
  run: () =>
    Effect.promise(async () => {
      cleanupRace.processCalls++
      const call = cleanupRace.processCalls
      cleanupRace.active++
      cleanupRace.maxActive = Math.max(cleanupRace.maxActive, cleanupRace.active)
      if (cleanupRace.processCalls === 2) cleanupRace.contender.resolve()
      cleanupRace.started.resolve()
      await cleanupRace.gate.promise
      cleanupRace.active--
      return call === 1 ? { exitCode: 1, stderr: "simulated first repo failure" } : { exitCode: 0, stderr: "" }
    }),
})
const fixtureRoots = [
  gcHarness.data,
  gcWorkFailureHarness.data,
  scheduleHarness.data,
  reapHarness.data,
  unstatableReapHarness.data,
  localTrackHarness.data,
  admissionFailureHarness.data,
  trackWorkFailureHarness.data,
  cleanupWaitHarness.data,
  completionHarness.data,
  sameRepoHarness.data,
  lockOrderHarness.data,
  concurrencyHarness.data,
  traversalHarness.data,
  realLockData,
  busyTrackHarness.data,
  bodyFailureHarness.data,
  lockFailureHarness.data,
  inLockGraceHarness.data,
]
const gcIt = gcHarness.it
const gcWorkFailureIt = gcWorkFailureHarness.it
const scheduleIt = scheduleHarness.it
const reapIt = reapHarness.it
const localTrackIt = localTrackHarness.it
const unstatableReapIt = unstatableReapHarness.it
const completionIt = completionHarness.it
const cleanupWaitIt = cleanupWaitHarness.it
const nonWindowsReapIt = process.platform === "win32" ? reapIt.live.skip : reapIt.live
const nonWindowsUnstatableReapIt = process.platform === "win32" ? unstatableReapIt.live.skip : unstatableReapIt.live
const lockOrderIt = lockOrderHarness.it
const concurrencyIt = concurrencyHarness.it
const sameRepoIt = sameRepoHarness.it
const busyTrackIt = busyTrackHarness.it
const bodyFailureIt = bodyFailureHarness.it
const lockFailureIt = lockFailureHarness.it
const admissionFailureIt = admissionFailureHarness.it
const trackWorkFailureIt = trackWorkFailureHarness.it
const inLockGraceIt = inLockGraceHarness.it

// Git always outputs /-separated paths internally. Snapshot.patch() joins them
// with path.join (which produces \ on Windows) then normalizes back to /.
// This helper does the same for expected values so assertions match cross-platform.
const fwd = (...parts: string[]) => path.join(...parts).replaceAll("\\", "/")
const SNAPSHOT_BATCH_BOUNDARY = 100
const OVER_BATCH_COUNT = SNAPSHOT_BATCH_BOUNDARY + 1
const MIXED_BATCH_GROUP_COUNT = Math.ceil(OVER_BATCH_COUNT / 4)

afterEach(async () => {
  localTrackGate.enabled = false
  localTrackGate.release.resolve()
  localTrackSemaphoreWait.enabled = false
  localTrackSemaphoreWait.release.resolve()
  busyTrack.busy = false
  busyTrack.repoFile = ""
  busyTrack.waitMillis = 0
  cleanupWait.busy = false
  cleanupWait.requested = 0
  cleanupRace.gate.resolve()
  sameRepoRace.gate.resolve()
  lockOrder.gate.resolve()
  reapContention.busy = false
  reapContention.repoFile = ""
  reapWorkFailure.failNext = false
  reapWorkFailure.repoFile = ""
  reapInfraFailure.enabled = false
  reapInfraFailure.repoFile = ""
  admissionFailure.fail = false
  trackWorkFailure.fail = false
  completionProbe.boxLock = ""
  completionProbe.completion = ""
  completionProbe.armed = false
  completionProbe.freshen = false
  completionProbe.freshText = ""
  completionProbe.atRelease.length = 0
  realLockGc.gate.resolve()
  await disposeAllInstances()
  cleanupRace.lockAttempts = 0
  cleanupRace.processCalls = 0
  cleanupRace.active = 0
  cleanupRace.maxActive = 0
  sameRepoRace.lockAttempts = 0
  sameRepoRace.active = 0
  sameRepoRace.maxActive = 0
  await Promise.all(fixtureRoots.map((dir) => fs.rm(dir, { recursive: true, force: true })))
  for (const harness of [
    gcHarness,
    gcWorkFailureHarness,
    traversalHarness,
    scheduleHarness,
    reapHarness,
    localTrackHarness,
    admissionFailureHarness,
    trackWorkFailureHarness,
    cleanupWaitHarness,
    completionHarness,
    busyTrackHarness,
    bodyFailureHarness,
    lockFailureHarness,
    inLockGraceHarness,
    lockOrderHarness,
    concurrencyHarness,
    sameRepoHarness,
  ]) {
    harness.calls.length = 0
    harness.lockCalls.length = 0
    harness.lockReleases.length = 0
    harness.randomValues.length = 0
    harness.clock.now = 10_000_000
    harness.runAdvance.millis = 0
    harness.outcome.exitCode = 0
    harness.outcome.stderr = ""
  }
  bodyFailure.reads = 0
  delete lockFailure.role
  scheduleClock.reset()
  scheduleEvents.count = 0
  scheduleEvents.times.length = 0
  scheduleEvents.now = () => 0
  scheduleEvents.waiters.clear()
  inLockGraceRace.gitdir = ""
  inLockGraceRace.worktree = ""
  inLockGraceRace.since = 0
  realLockSignal.armed = false
  realLockCalls.length = 0
  realLockLockCalls.length = 0
  realLockTrackWait.file = ""
  realLockTrackWait.entered = deferred()
  localTrackGate.started = deferred()
  localTrackGate.release = deferred()
  localTrackSemaphoreWait.entered = deferred()
  localTrackSemaphoreWait.release = deferred()
  resetRealLockGc()
  cleanupRace.started = deferred()
  cleanupRace.contender = deferred()
  cleanupRace.gate = deferred()
  sameRepoRace.started = deferred()
  sameRepoRace.contender = deferred()
  sameRepoRace.gate = deferred()
  lockOrder.started = deferred()
  lockOrder.gate = deferred()
})

const snapshotGitdir = async (data: string, worktree: string) => {
  for (const project of await fs.readdir(path.join(data, "snapshot"), { withFileTypes: true })) {
    if (!project.isDirectory()) continue
    for (const repo of await fs.readdir(path.join(data, "snapshot", project.name), { withFileTypes: true })) {
      if (!repo.isDirectory()) continue
      const gitdir = path.join(data, "snapshot", project.name, repo.name)
      const record = await fs
        .readFile(path.join(gitdir, "info", "opencode-worktree.json"), "utf8")
        .then((text) => JSON.parse(text) as { worktree?: string })
        .catch(() => undefined)
      if (record?.worktree === worktree) return gitdir
    }
  }
  throw new Error(`missing snapshot evidence for ${worktree}`)
}

const gcCompletionFile = (data: string) => path.join(data, "snapshot", "gc-completed.timestamp")

const repoLockFile = (data: string, gitdir: string) =>
  path.join(data, "snapshot", "locks", path.basename(path.dirname(gitdir)), `${path.basename(gitdir)}.lock`)

const startAdvisoryLock = (file: string) =>
  Effect.promise(async () => {
    await fs.mkdir(path.dirname(file), { recursive: true })
    const child = Bun.spawn(lockCommand(process.platform, file, Bun.which), {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
    })
    const reader = child.stdout.getReader()
    let output = ""
    try {
      while (!output.includes("\n")) {
        const next = await reader.read()
        if (next.done) {
          throw new Error(
            `advisory lock holder exited (${await child.exited}): ${await new Response(child.stderr).text()}`,
          )
        }
        output += new TextDecoder().decode(next.value)
      }
      return child
    } catch (cause) {
      child.kill()
      throw cause
    }
  })

const existsPath = (file: string) =>
  fs
    .stat(file)
    .then(() => true)
    .catch(() => false)
it.effect(
  "selects installed advisory-lock mechanisms for supported platforms",
  Effect.sync(() => {
    const which = (command: string) => {
      if (command === "flock") return "/usr/bin/flock"
      if (command === "perl") return "/usr/bin/perl"
      if (command === "powershell.exe") return "C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"
      return null
    }
    expect(lockCommand("linux", "/data/snapshot/gc.lock", which)).toEqual([
      "/usr/bin/flock",
      "-x",
      "/data/snapshot/gc.lock",
      "sh",
      "-c",
      'printf "locked\\n"; exec cat',
    ])
    expect(lockCommand("linux", "/data/snapshot/repo.lock", which, true)).toEqual([
      "/usr/bin/flock",
      "-x",
      "-n",
      "/data/snapshot/repo.lock",
      "sh",
      "-c",
      'printf "locked\\n"; exec cat',
    ])
    expect(lockCommand("darwin", "/data/snapshot/gc.lock", which)).toEqual([
      "/usr/bin/perl",
      "-MFcntl=:flock",
      "-e",
      'use strict; open my $lock, ">>", $ARGV[0] or die $!; flock($lock, LOCK_EX) or die $!; $| = 1; print "locked\\n"; <STDIN>;',
      "/data/snapshot/gc.lock",
    ])
    expect(lockCommand("darwin", "/data/snapshot/repo.lock", which, true)[3]).toContain("LOCK_EX|LOCK_NB")
    const powershell = lockCommand("win32", "C:/data/snapshot/gc.lock", which)
    expect(powershell.slice(0, 4)).toEqual([
      "C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
      "-NoProfile",
      "-NonInteractive",
      "-EncodedCommand",
    ])
    expect(Buffer.from(powershell[4]!, "base64").toString("utf16le")).toContain("$stream.Lock(0, 1)")
    expect(
      Buffer.from(lockCommand("win32", "C:/data/snapshot/repo.lock", which, true)[4]!, "base64").toString("utf16le"),
    ).toContain("exit 75")
  }),
)

maintenanceLockIt.live(
  "maps BusyBox and GNU conflicts to contention while retaining flock infrastructure errors",
  Effect.gen(function* () {
    const binaries = ["/stub/busybox/flock", "/stub/gnu/flock"]
    const args: string[][] = []
    const spawn: NonNullable<LockRuntime["spawn"]> = (command, options) => {
      args.push(command)
      const busybox = command[0] === binaries[0]
      const exitCode = busybox ? 2 : 1
      return Bun.spawn([process.execPath, "-e", "process.exit(" + exitCode + ")"], options)
    }

    for (const binary of binaries) {
      const lease = yield* Effect.promise(() =>
        acquire("/data/snapshot/repo.lock", new AbortController().signal, true, {
          platform: "linux",
          which: () => binary,
          spawn,
        }),
      )
      expect(lease.status).toBe("contended")
    }

    expect(args).toEqual(
      binaries.map((binary) => [
        binary,
        "-x",
        "-n",
        "/data/snapshot/repo.lock",
        "sh",
        "-c",
        'printf "locked\\n"; exec cat',
      ]),
    )
    expect(args.every((command) => !command.includes("--conflict-exit-code"))).toBe(true)
    const infrastructure = yield* Effect.exit(
      Effect.promise(() =>
        acquire("/data/snapshot/repo.lock", new AbortController().signal, true, {
          platform: "linux",
          which: () => "/stub/flock",
          spawn: (_command, options) =>
            Bun.spawn([process.execPath, "-e", 'process.stderr.write("permission denied"); process.exit(1)'], options),
        }),
      ),
    )
    expect(Exit.isFailure(infrastructure)).toBe(true)
  }),
)

maintenanceLockIt.live(
  "rejects lock requests that reverse the snapshot lock order",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped()
    const maintenance = yield* MaintenanceService
    const valid = [
      { role: "box", file: path.join(dir, "gc.lock") },
      { role: "repo", file: path.join(dir, "repo.lock") },
      { role: "local", semaphore: Semaphore.makeUnsafe(1) },
    ] satisfies LockRequest[]
    const invalid = [
      [valid[1]!, valid[0]!, valid[2]!],
      [valid[0]!, valid[2]!, valid[1]!],
      [valid[0]!, valid[1]!, valid[2]!, valid[1]!],
      [
        { role: "box", file: path.join(dir, "first-box.lock") },
        { role: "box", file: path.join(dir, "later-waiting-box.lock"), wait: true },
      ],
      [],
    ] satisfies LockRequest[][]

    const repoOnly = yield* maintenance.withLocks(
      [{ role: "repo", file: path.join(dir, "repo-only.lock") }],
      Effect.succeed("repo-owned work"),
    )
    expect(repoOnly).toEqual({ status: "acquired", value: "repo-owned work" })
    const tracked = yield* maintenance.withLocks(
      [
        { role: "repo", file: path.join(dir, "repo-wait.lock"), wait: true },
        { role: "local", semaphore: Semaphore.makeUnsafe(1), wait: true },
      ],
      Effect.succeed("tracking completed"),
    )
    expect(tracked).toEqual({ status: "acquired", value: "tracking completed" })

    for (const locks of invalid) {
      const result = yield* Effect.exit(maintenance.withLocks(locks, Effect.void))
      expect(Exit.isFailure(result)).toBe(true)
    }
  }),
)

maintenanceLockIt.live(
  "waits for local snapshot ownership and completes tracked work",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped()
    const maintenance = yield* MaintenanceService
    const semaphore = Semaphore.makeUnsafe(1)
    const entered = deferred()
    const release = deferred()
    const holder = yield* Effect.forkScoped(
      semaphore.withPermits(1)(
        Effect.sync(entered.resolve).pipe(Effect.andThen(Effect.promise(() => release.promise))),
      ),
    )
    yield* awaitWithTimeout(
      Effect.promise(() => entered.promise),
      "local lock holder did not start",
      "2 seconds",
    )
    const finished = { value: false }
    const tracking = yield* Effect.forkScoped(
      maintenance.withLocks(
        [
          { role: "repo", file: path.join(dir, "repo.lock"), wait: true },
          { role: "local", semaphore, wait: true },
        ],
        Effect.succeed("tracked").pipe(Effect.tap(() => Effect.sync(() => (finished.value = true)))),
      ),
    )
    yield* Effect.sleep(Duration.millis(25))
    expect(finished.value).toBe(false)
    release.resolve()
    yield* awaitWithTimeout(Fiber.join(holder), "local lock holder did not release", "2 seconds")
    const attempt = yield* awaitWithTimeout(
      Fiber.join(tracking),
      "tracking did not finish after local release",
      "2 seconds",
    )
    expect(attempt).toEqual({ status: "acquired", value: "tracked" })
    expect(finished.value).toBe(true)
  }),
)

maintenanceLockIt.live(
  "surfaces work failures from inside the acquired lock chain",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped()
    const maintenance = yield* MaintenanceService
    const failure = yield* Effect.exit(
      maintenance.withLocks(
        [
          { role: "box", file: path.join(dir, "gc.lock") },
          { role: "repo", file: path.join(dir, "repo.lock") },
        ],
        Effect.die(new Error("simulated maintenance work failure")),
      ),
    )

    expect(Exit.isFailure(failure)).toBe(true)
  }),
)

maintenanceLockIt.live(
  "keeps successful work successful when an advisory lock child exits badly on release",
  Effect.gen(function* () {
    const spawn: NonNullable<LockRuntime["spawn"]> = (_command, options) =>
      Bun.spawn(
        [
          process.execPath,
          "-e",
          'process.stdout.write("locked\\n"); process.stdin.on("end", () => process.exit(9)); process.stdin.resume()',
        ],
        options,
      )
    const getAttempt = () =>
      Effect.promise(() =>
        acquire("/data/snapshot/repo.lock", new AbortController().signal, true, {
          platform: "linux",
          which: () => "/stub/flock",
          spawn,
        }),
      )
    const released = yield* getAttempt()
    expect(released.status).toBe("acquired")
    if (released.status !== "acquired") return
    const releaseStatus = yield* awaitWithTimeout(
      Effect.promise(() => released.release()),
      "lock child did not release",
      "2 seconds",
    )
    expect(releaseStatus.exitCode).toBe(9)

    const attempt = yield* getAttempt()
    expect(attempt.status).toBe("acquired")
    if (attempt.status !== "acquired") return
    const logged: string[] = []
    const result = yield* withSnapshotLocks(
      [{ role: "box", file: "/data/snapshot/gc.lock" }],
      Effect.succeed("work finished"),
      () => Effect.succeed({ status: "acquired" as const, lease: attempt }),
    ).pipe(Effect.provide(Logger.layer([Logger.make((item) => logged.push(String(item.message)))])))

    expect(result).toEqual({ status: "acquired", value: "work finished" })
    expect(logged.some((message) => message.includes("snapshot advisory lock release failed"))).toBe(true)
  }),
)

it.live(
  "loads the snapshot maintenance module as a direct entry point",
  Effect.gen(function* () {
    const child = Bun.spawn(
      [process.execPath, "run", path.resolve(import.meta.dir, "../../src/snapshot/maintenance.ts")],
      { cwd: process.cwd(), stdout: "ignore", stderr: "pipe" },
    )
    const stderr = new Response(child.stderr).text()
    const exitCode = yield* Effect.acquireUseRelease(
      Effect.succeed(child),
      (proc) =>
        awaitWithTimeout(
          Effect.promise(() => proc.exited),
          "maintenance import timed out",
          "5 seconds",
        ),
      (proc) => Effect.sync(() => proc.kill()),
    )

    expect(exitCode).toBe(0)
    expect(yield* Effect.promise(() => stderr)).toBe("")
  }),
)

traversalIt.live(
  "does not traverse advisory-lock directories as shadow repositories",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    const root = path.join(traversalHarness.data, "snapshot")
    yield* Effect.promise(() =>
      fs.writeFile(gcCompletionFile(traversalHarness.data), String(traversalHarness.clock.now - 30_000)),
    )

    yield* snapshot.cleanup().pipe(provideInstance(dir))

    expect(yield* Effect.promise(() => existsPath(path.join(root, "locks", "locks")))).toBe(false)
  }),
)

busyTrackIt.live(
  "waits through repo-lock contention and completes tracking without loss",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    const gitdir = yield* Effect.promise(() => snapshotGitdir(busyTrackHarness.data, dir))
    const evidenceFile = path.join(gitdir, "info", "opencode-worktree.json")
    const evidence = JSON.parse(yield* Effect.promise(() => fs.readFile(evidenceFile, "utf8"))) as {
      version: 1
      project: string
      worktree: string
    }
    yield* Effect.promise(() => fs.writeFile(evidenceFile, JSON.stringify({ ...evidence, missingSince: 123 })))
    busyTrack.repoFile = repoLockFile(busyTrackHarness.data, gitdir)
    busyTrack.waitMillis = 0
    busyTrack.busy = true
    const beforeTrack = busyTrackHarness.lockCalls.length
    const finished = { value: false }
    const tracking = yield* Effect.forkScoped(
      snapshot
        .track()
        .pipe(provideInstance(dir))
        .pipe(Effect.ensuring(Effect.sync(() => (finished.value = true)))),
    )
    yield* Effect.sleep(Duration.millis(25))
    expect(busyTrackHarness.lockCalls.slice(beforeTrack)).toEqual([busyTrack.repoFile])
    expect(busyTrack.waitMillis).toBeGreaterThanOrEqual(Duration.toMillis(Duration.seconds(2)))
    yield* Effect.sleep(Duration.millis(1_200))
    expect(finished.value).toBe(false)

    busyTrack.busy = false
    const hash = yield* awaitWithTimeout(
      Fiber.join(tracking),
      "tracking did not finish after lock release",
      "2 seconds",
    )
    const after = JSON.parse(yield* Effect.promise(() => fs.readFile(evidenceFile, "utf8"))) as {
      missingSince?: number
    }

    expect(hash).toBeTruthy()
    expect(finished.value).toBe(true)
    expect(after.missingSince).toBeUndefined()
  }),
  { timeout: 10_000 },
)

localTrackIt.live(
  "keeps the repo lease while tracking waits for local snapshot work",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    const initial = yield* snapshot.track().pipe(provideInstance(dir))
    if (!initial) throw new Error("tracking did not return an initial snapshot")
    const gitdir = yield* Effect.promise(() => snapshotGitdir(localTrackHarness.data, dir))
    const evidencePath = path.join(gitdir, "info", "opencode-worktree.json")
    const evidence = JSON.parse(yield* Effect.promise(() => fs.readFile(evidencePath, "utf8"))) as Record<
      string,
      unknown
    >
    yield* Effect.promise(() => fs.writeFile(evidencePath, JSON.stringify({ ...evidence, missingSince: 123 })))
    localTrackGate.started = deferred()
    localTrackGate.release = deferred()
    localTrackGate.enabled = true
    localTrackSemaphoreWait.entered = deferred()
    localTrackSemaphoreWait.release = deferred()
    localTrackSemaphoreWait.enabled = true
    const patching = yield* Effect.forkScoped(snapshot.patch(initial).pipe(provideInstance(dir)))
    yield* awaitWithTimeout(
      Effect.promise(() => localTrackGate.started.promise),
      "patch did not enter the local snapshot section",
      "2 seconds",
    )

    const repoFile = repoLockFile(localTrackHarness.data, gitdir)
    const beforeRelease = localTrackHarness.lockReleases.length
    const beforeLocks = localTrackHarness.lockCalls.length
    const finished = { value: false }
    const tracking = yield* Effect.forkScoped(
      snapshot
        .track()
        .pipe(provideInstance(dir))
        .pipe(Effect.ensuring(Effect.sync(() => (finished.value = true)))),
    )
    yield* awaitWithTimeout(
      Effect.promise(() => localTrackSemaphoreWait.entered.promise),
      "tracking did not encounter local lock contention",
      "2 seconds",
    )
    expect(finished.value).toBe(false)
    expect(localTrackHarness.lockReleases.slice(beforeRelease)).toEqual([])
    expect(localTrackHarness.lockCalls.slice(beforeLocks)).toEqual([repoFile])

    localTrackSemaphoreWait.enabled = false
    localTrackSemaphoreWait.release.resolve()
    yield* Effect.yieldNow
    expect(finished.value).toBe(false)
    expect(localTrackHarness.lockReleases.slice(beforeRelease)).toEqual([])

    localTrackGate.enabled = false
    localTrackGate.release.resolve()
    const patch = yield* awaitWithTimeout(Fiber.join(patching), "patch did not finish after release", "3 seconds")
    const hash = yield* awaitWithTimeout(Fiber.join(tracking), "track did not finish after local release", "3 seconds")
    const after = JSON.parse(yield* Effect.promise(() => fs.readFile(evidencePath, "utf8"))) as {
      missingSince?: number
    }

    expect(patch.hash).toBe(initial)
    expect(hash).toBeTruthy()
    expect(finished.value).toBe(true)
    expect(after.missingSince).toBeUndefined()
  }),
  { timeout: 10_000 },
)

lockFailureIt.live(
  "logs and surfaces tracking failure when the advisory lock command is unavailable",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    const gitdir = yield* Effect.promise(() => snapshotGitdir(lockFailureHarness.data, dir))
    const evidenceFile = path.join(gitdir, "info", "opencode-worktree.json")
    const before = yield* Effect.promise(() => fs.readFile(evidenceFile, "utf8"))
    const logged: string[] = []
    const logger = Logger.layer([Logger.make((item) => logged.push(String(item.message)))])

    lockFailure.role = "repo"
    const tracked = yield* Effect.exit(snapshot.track().pipe(provideInstance(dir), Effect.provide(logger)))
    lockFailure.role = "box"
    const admission = yield* Effect.exit(snapshot.cleanup().pipe(provideInstance(dir), Effect.provide(logger)))
    lockFailure.role = "repo"
    const beforeRepoFailure = lockFailureHarness.lockCalls.length
    const repo = yield* Effect.exit(snapshot.cleanup().pipe(provideInstance(dir), Effect.provide(logger)))

    expect(Exit.isFailure(tracked)).toBe(true)
    expect(Exit.isSuccess(admission)).toBe(true)
    expect(Exit.isSuccess(repo)).toBe(true)
    expect(yield* Effect.promise(() => fs.readFile(evidenceFile, "utf8"))).toBe(before)
    expect(lockFailureHarness.calls).toEqual([])
    expect(lockFailureHarness.lockCalls.slice(beforeRepoFailure)).toEqual([
      path.join(lockFailureHarness.data, "snapshot", "gc.lock"),
      repoLockFile(lockFailureHarness.data, gitdir),
    ])
    expect(logged.filter((message) => message.includes("snapshot advisory lock unavailable"))).toHaveLength(3)
  }),
)

bodyFailureIt.live(
  "surfaces configuration failure before tracking starts",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    const exit = yield* Effect.exit(snapshot.track().pipe(provideInstance(dir)))

    expect(Exit.isFailure(exit)).toBe(true)
    expect(bodyFailure.reads).toBe(1)
  }),
)

trackWorkFailureIt.live(
  "surfaces work failure from the acquired repo-locked tracking pass",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    trackWorkFailure.fail = true
    const result = yield* Effect.exit(snapshot.track().pipe(provideInstance(dir)))

    expect(Exit.isFailure(result)).toBe(true)
    expect(trackWorkFailureHarness.lockReleases).toHaveLength(1)
    expect(path.basename(trackWorkFailureHarness.lockReleases[0]!)).toBe(`${Hash.fast(dir)}.lock`)
    expect(trackWorkFailureHarness.calls).toEqual([])
  }),
)

admissionFailureIt.live(
  "surfaces cleanup admission work failure after acquiring the box lock",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    const before = admissionFailureHarness.lockCalls.length
    admissionFailure.fail = true

    const result = yield* Effect.exit(snapshot.cleanup().pipe(provideInstance(dir)))

    expect(Exit.isFailure(result)).toBe(true)
    expect(admissionFailureHarness.lockCalls.slice(before)).toEqual([
      path.join(admissionFailureHarness.data, "snapshot", "gc.lock"),
    ])
    expect(admissionFailureHarness.calls).toEqual([])
  }),
)

cleanupWaitIt.live(
  "warns and skips when gc admission expires after its long wait",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    cleanupWait.busy = true
    cleanupWait.requested = 0
    const logged: string[] = []

    yield* awaitWithTimeout(
      snapshot
        .cleanup()
        .pipe(
          provideInstance(dir),
          Effect.provide(Logger.layer([Logger.make((item) => logged.push(String(item.message)))])),
        ),
      "cleanup did not surface its gc admission expiry",
      "2 seconds",
    )

    expect(cleanupWait.requested).toBeGreaterThanOrEqual(Duration.toMillis(Duration.minutes(1)))
    expect(logged.some((message) => message.includes("snapshot cleanup skipped after waiting for gc admission"))).toBe(
      true,
    )
    expect(cleanupWaitHarness.calls).toEqual([])
  }),
)

realLockIt.live(
  "snapshot cleanup skips a repo held by an independent process without waiting",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    const gitdir = yield* Effect.promise(() => snapshotGitdir(realLockData, dir))
    yield* Effect.promise(() =>
      fs.writeFile(gcCompletionFile(realLockData), String(Date.now() - Duration.toMillis(Duration.hours(2)))),
    )
    resetRealLockGc()
    realLockLockCalls.length = 0
    realLockCalls.length = 0
    const holder = yield* startAdvisoryLock(repoLockFile(realLockData, gitdir))
    realLockSignal.armed = true

    yield* Effect.acquireUseRelease(
      Effect.succeed(holder),
      () =>
        Effect.gen(function* () {
          const sweep = yield* Effect.forkScoped(snapshot.cleanup().pipe(provideInstance(dir)))
          yield* awaitWithTimeout(
            Effect.promise(() => realLockStarted),
            "sweep did not reach the repo lease",
            "2 seconds",
          )
          yield* Effect.sleep(Duration.millis(100))
          expect(realLockCalls).toEqual([])
          yield* awaitWithTimeout(Fiber.join(sweep), "cleanup waited on the contended repo lock", "2 seconds")
          expect(realLockLockCalls).toContain(repoLockFile(realLockData, gitdir))
          holder.stdin.end()
          yield* awaitWithTimeout(
            Effect.promise(() => holder.exited),
            "lock holder did not exit",
            "2 seconds",
          )
        }),
      (child) =>
        Effect.promise(async () => {
          child.stdin.end()
          await child.exited
        }),
    )
  }),
)

realLockIt.live(
  "retries track lock windows against an independent repo holder and completes tracking",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    const gitdir = yield* Effect.promise(() => snapshotGitdir(realLockData, dir))
    const evidencePath = path.join(gitdir, "info", "opencode-worktree.json")
    const evidence = JSON.parse(yield* Effect.promise(() => fs.readFile(evidencePath, "utf8"))) as Record<
      string,
      unknown
    >
    yield* Effect.promise(() => fs.writeFile(evidencePath, JSON.stringify({ ...evidence, missingSince: 123 })))
    const repoFile = repoLockFile(realLockData, gitdir)
    realLockTrackWait.file = repoFile
    realLockTrackWait.entered = deferred()
    realLockLockCalls.length = 0
    const holder = yield* startAdvisoryLock(repoFile)

    yield* Effect.acquireUseRelease(
      Effect.succeed(holder),
      () =>
        Effect.gen(function* () {
          const finished = { value: false }
          const tracking = yield* Effect.forkScoped(
            snapshot
              .track()
              .pipe(provideInstance(dir))
              .pipe(Effect.ensuring(Effect.sync(() => (finished.value = true)))),
          )
          yield* awaitWithTimeout(
            Effect.promise(() => realLockTrackWait.entered.promise),
            "tracking did not reach the independent repo lock",
            "2 seconds",
          )
          yield* Effect.sleep(Duration.millis(150))
          expect(finished.value).toBe(false)
          expect(realLockLockCalls.filter((file) => file === repoFile).length).toBeGreaterThan(1)
          holder.stdin.end()
          yield* awaitWithTimeout(
            Effect.promise(() => holder.exited),
            "repo lock holder did not exit",
            "2 seconds",
          )
          const hash = yield* awaitWithTimeout(
            Fiber.join(tracking),
            "tracking did not finish after the repo lock was released",
            "2 seconds",
          )
          expect(hash).toBeTruthy()
          expect(finished.value).toBe(true)
          expect(
            (JSON.parse(yield* Effect.promise(() => fs.readFile(evidencePath, "utf8"))) as { missingSince?: number })
              .missingSince,
          ).toBeUndefined()
        }),
      (child) =>
        Effect.promise(async () => {
          child.stdin.end()
          await child.exited
        }),
    )
  }),
  { timeout: 10_000 },
)

realLockIt.live(
  "waits for an independent box-lock holder and suppresses the second repo after global gc",
  Effect.gen(function* () {
    const first = yield* tmpdirScoped({ git: true })
    const second = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(first))
    yield* snapshot.track().pipe(provideInstance(second))
    resetRealLockGc()
    realLockLockCalls.length = 0
    realLockCalls.length = 0
    const stale = String(Date.now() - Duration.toMillis(Duration.hours(2)))
    yield* Effect.promise(() => fs.writeFile(gcCompletionFile(realLockData), stale))
    const holder = yield* startAdvisoryLock(path.join(realLockData, "snapshot", "gc.lock"))
    realLockSignal.armed = true

    yield* Effect.acquireUseRelease(
      Effect.succeed(holder),
      () =>
        Effect.gen(function* () {
          const oneFinished = { value: false }
          const twoFinished = { value: false }
          const one = yield* Effect.forkScoped(
            snapshot
              .cleanup()
              .pipe(provideInstance(first))
              .pipe(Effect.ensuring(Effect.sync(() => (oneFinished.value = true)))),
          )
          const two = yield* Effect.forkScoped(
            snapshot
              .cleanup()
              .pipe(provideInstance(second))
              .pipe(Effect.ensuring(Effect.sync(() => (twoFinished.value = true)))),
          )
          yield* awaitWithTimeout(
            Effect.promise(() => realLockGc.contender.promise),
            "both cleanups did not contend for the shared gc lock",
            "2 seconds",
          )
          yield* Effect.sleep(Duration.millis(2_100))
          expect(realLockCalls).toEqual([])
          expect(oneFinished.value || twoFinished.value).toBe(false)
          holder.stdin.end()
          yield* awaitWithTimeout(
            Effect.promise(() => holder.exited),
            "gc lock holder did not exit",
            "2 seconds",
          )
          yield* awaitWithTimeout(
            Effect.promise(() => realLockGc.started.promise),
            "first cleanup did not start after the box lock holder exited",
            "2 seconds",
          )
          yield* Effect.sleep(Duration.millis(100))
          expect(realLockCalls).toHaveLength(1)
          expect(realLockGc.maxActive).toBe(1)
          realLockGc.gate.resolve()
          yield* awaitWithTimeout(Fiber.join(one), "first cleanup stayed behind box admission", "2 seconds")
          yield* awaitWithTimeout(Fiber.join(two), "second cleanup stayed behind box admission", "2 seconds")
          expect(realLockCalls).toHaveLength(1)
          expect(realLockGc.maxActive).toBe(1)
          expect(oneFinished.value && twoFinished.value).toBe(true)
        }),
      (child) =>
        Effect.promise(async () => {
          child.stdin.end()
          await child.exited
        }),
    )
  }),
  { timeout: 10_000 },
)

maintenanceLockIt.live(
  "reports advisory lock contention and releases the lease after the body",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped()
    const maintenance = yield* MaintenanceService
    const gate = { ready: () => {}, release: () => {} }
    const started = new Promise<void>((resolve) => (gate.ready = resolve))
    const released = new Promise<void>((resolve) => (gate.release = resolve))
    const completion = { secondRuns: 0 }
    const request = [
      { role: "box", file: path.join(dir, "gc.lock") },
      { role: "repo", file: path.join(dir, "snapshot.lock") },
    ] satisfies LockRequest[]

    yield* Effect.gen(function* () {
      const first = yield* Effect.forkScoped(
        maintenance.withLocks(
          request,
          Effect.promise(async () => {
            gate.ready()
            await released
          }),
        ),
      )
      yield* awaitWithTimeout(
        Effect.promise(() => started),
        "first advisory lock was not acquired",
        "2 seconds",
      )
      const second = yield* awaitWithTimeout(
        maintenance.withLocks(
          request,
          Effect.sync(() => completion.secondRuns++),
        ),
        "contended advisory lock did not return promptly",
        "2 seconds",
      )
      expect(second.status).toBe("contended")
      expect(completion.secondRuns).toBe(0)
      gate.release()
      const result = yield* Fiber.join(first)
      expect(result.status).toBe("acquired")
      const retry = yield* maintenance.withLocks(
        request,
        Effect.sync(() => completion.secondRuns++),
      )
      expect(retry.status).toBe("acquired")
      expect(completion.secondRuns).toBe(1)
    }).pipe(Effect.ensuring(Effect.sync(() => gate.release())))
  }),
)

maintenanceLockIt.live(
  "skips immediately when another process holds an advisory lock",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped()
    const maintenance = yield* MaintenanceService
    const lock = path.join(dir, "snapshot.lock")
    const holder = yield* startAdvisoryLock(lock)

    yield* Effect.acquireUseRelease(
      Effect.succeed(holder),
      () =>
        Effect.gen(function* () {
          const result = yield* awaitWithTimeout(
            maintenance.withLocks([{ role: "box", file: lock }], Effect.succeed("unexpected")),
            "contended advisory lock did not return promptly",
            "2 seconds",
          )
          expect(result.status).toBe("contended")
        }),
      (child) =>
        Effect.promise(async () => {
          child.stdin.end()
          await child.exited
        }),
    )
  }),
)

maintenanceLockIt.live(
  "interrupts a cleanup waiting on the gc admission lock",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped()
    const maintenance = yield* MaintenanceService
    const lock = path.join(dir, "gc.lock")
    const holder = yield* startAdvisoryLock(lock)

    yield* Effect.acquireUseRelease(
      Effect.succeed(holder),
      () =>
        Effect.gen(function* () {
          const waiter = yield* Effect.forkScoped(
            maintenance.withLocks([{ role: "box", file: lock, wait: true }], Effect.succeed("ran")),
          )
          yield* Effect.sleep(Duration.millis(50))
          yield* awaitWithTimeout(Fiber.interrupt(waiter), "gc admission ignored interruption", "2 seconds")
          const interrupted = yield* awaitWithTimeout(
            Fiber.await(waiter),
            "interrupted admission fiber did not finish",
            "2 seconds",
          )
          expect(Exit.isFailure(interrupted)).toBe(true)
        }),
      (child) =>
        Effect.promise(async () => {
          child.stdin.end()
          await child.exited
        }),
    )
  }),
)

gcIt.live(
  "skips gc after a recent successful maintenance pass",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    yield* Effect.promise(() => fs.writeFile(gcCompletionFile(gcHarness.data), String(gcHarness.clock.now - 30_000)))
    const beforeCleanup = gcHarness.lockCalls.length

    yield* snapshot.cleanup().pipe(provideInstance(dir))

    expect(gcHarness.calls).toEqual([])
    expect(gcHarness.lockCalls.slice(beforeCleanup)).toEqual([path.join(gcHarness.data, "snapshot", "gc.lock")])
  }),
)

gcIt.live(
  "applies one global cooldown across worktrees and lets the same worktree run after an hour",
  Effect.gen(function* () {
    const first = yield* tmpdirScoped({ git: true })
    const second = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(first))
    yield* snapshot.track().pipe(provideInstance(second))

    yield* snapshot.cleanup().pipe(provideInstance(first))
    expect(gcHarness.calls).toHaveLength(1)
    expect(gcHarness.calls[0]?.cwd).toBe(first)
    yield* snapshot.cleanup().pipe(provideInstance(second))
    expect(gcHarness.calls).toHaveLength(1)

    gcHarness.clock.now += Duration.toMillis(Duration.hours(1))
    yield* snapshot.cleanup().pipe(provideInstance(first))

    expect(gcHarness.calls).toHaveLength(2)
    expect(gcHarness.calls[1]?.cwd).toBe(first)
    expect(yield* Effect.promise(() => fs.readFile(gcCompletionFile(gcHarness.data), "utf8"))).toBe(
      String(gcHarness.clock.now),
    )
    expect(
      (yield* Effect.promise(() => fs.readdir(path.join(gcHarness.data, "snapshot")))).filter((name) =>
        name.startsWith("gc-completed"),
      ),
    ).toEqual(["gc-completed.timestamp"])
  }),
)

gcIt.live(
  "treats a future global completion timestamp as stale",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    yield* Effect.promise(() => fs.writeFile(gcCompletionFile(gcHarness.data), String(gcHarness.clock.now + 30_000)))

    yield* snapshot.cleanup().pipe(provideInstance(dir))

    expect(gcHarness.calls).toHaveLength(1)
    expect(yield* Effect.promise(() => fs.readFile(gcCompletionFile(gcHarness.data), "utf8"))).toBe(
      String(gcHarness.clock.now),
    )
  }),
)

gcIt.live(
  "runs auto gc with reduced process priority and records only successful completion",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    const completion = gcCompletionFile(gcHarness.data)
    gcHarness.clock.now += Duration.toMillis(Duration.hours(2))
    gcHarness.outcome.exitCode = 1
    gcHarness.outcome.stderr = "simulated failure"

    const logged: string[] = []
    const failure = yield* Effect.exit(
      snapshot
        .cleanup()
        .pipe(
          provideInstance(dir),
          Effect.provide(Logger.layer([Logger.make((item) => logged.push(String(item.message)))])),
        ),
    )

    expect(Exit.isSuccess(failure)).toBe(true)
    expect(yield* Effect.promise(() => existsPath(completion))).toBe(false)
    expect(gcHarness.calls).toHaveLength(1)
    expect(logged.some((message) => message.includes("snapshot cleanup failed"))).toBe(true)
    gcHarness.outcome.exitCode = 0
    gcHarness.runAdvance.millis = 400
    yield* snapshot.cleanup().pipe(provideInstance(dir))

    expect(gcHarness.calls).toHaveLength(2)
    expect(yield* Effect.promise(() => fs.readFile(completion, "utf8"))).toBe(String(gcHarness.clock.now))
    const invocation = gcHarness.calls[1]!
    const args = [invocation.command, ...invocation.args]
    const gitIndex = args.indexOf("git")
    expect(gitIndex).toBeGreaterThanOrEqual(0)
    const gcIndex = args.indexOf("gc")
    expect(args.slice(gcIndex - 2, gcIndex)).toEqual(["-c", "gc.autoDetach=false"])
    expect(args.slice(gitIndex)).toContain("gc")
    expect(args.slice(gitIndex)).toContain("--auto")
    expect(args.slice(gitIndex)).toContain("--prune=7.days")
    if (process.platform !== "win32" && Bun.which("nice")) {
      expect(args.slice(0, gitIndex)).toEqual(expect.arrayContaining(["-n", "10"]))
    }
    if (process.platform === "linux" && Bun.which("ionice")) {
      expect(args.slice(0, gitIndex)).toEqual(expect.arrayContaining(["-c", "2", "-n", "7"]))
    }
  }),
)

gcWorkFailureIt.live(
  "catches an effect failure from per-repo gc while holding maintenance locks",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    const logged: string[] = []
    const result = yield* Effect.exit(
      snapshot
        .cleanup()
        .pipe(
          provideInstance(dir),
          Effect.provide(Logger.layer([Logger.make((item) => logged.push(String(item.message)))])),
        ),
    )

    expect(Exit.isSuccess(result)).toBe(true)
    expect(gcWorkFailureHarness.calls).toHaveLength(1)
    expect(logged.some((message) => message.includes("snapshot per-repo cleanup failed"))).toBe(true)
    expect(yield* Effect.promise(() => existsPath(gcCompletionFile(gcWorkFailureHarness.data)))).toBe(false)
  }),
)

completionIt.live(
  "rechecks and writes the global completion record under gc admission",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    completionProbe.boxLock = path.join(completionHarness.data, "snapshot", "gc.lock")
    completionProbe.completion = gcCompletionFile(completionHarness.data)
    completionProbe.armed = true
    completionProbe.freshen = true
    completionProbe.freshText = String(completionHarness.clock.now)
    yield* Effect.promise(() =>
      fs.writeFile(
        completionProbe.completion,
        String(completionHarness.clock.now - Duration.toMillis(Duration.hours(2))),
      ),
    )

    yield* snapshot.cleanup().pipe(provideInstance(dir))

    expect(completionHarness.calls).toEqual([])
    expect(completionProbe.atRelease).toEqual([String(completionHarness.clock.now)])
    completionProbe.freshen = false
    completionProbe.atRelease.length = 0
    yield* Effect.promise(() =>
      fs.writeFile(
        completionProbe.completion,
        String(completionHarness.clock.now - Duration.toMillis(Duration.hours(2))),
      ),
    )

    yield* snapshot.cleanup().pipe(provideInstance(dir))

    expect(completionHarness.calls).toHaveLength(1)
    expect(completionProbe.atRelease).toEqual([String(completionHarness.clock.now)])
  }),
)

concurrencyIt.live(
  "logs one repo gc failure and continues with the next serialized cleanup",
  Effect.gen(function* () {
    const first = yield* tmpdirScoped({ git: true })
    const second = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(first))
    yield* snapshot.track().pipe(provideInstance(second))
    const beforeConcurrentCleanup = concurrencyHarness.lockCalls.length
    concurrencyHarness.clock.now += Duration.toMillis(Duration.hours(2))
    cleanupRace.lockAttempts = 0
    cleanupRace.processCalls = 0
    cleanupRace.active = 0
    cleanupRace.maxActive = 0
    cleanupRace.started = deferred()
    cleanupRace.contender = deferred()
    cleanupRace.gate = deferred()
    const logged: string[] = []
    const logger = Logger.layer([Logger.make((item) => logged.push(String(item.message)))])

    const one = yield* Effect.forkScoped(snapshot.cleanup().pipe(provideInstance(first), Effect.provide(logger)))
    const two = yield* Effect.forkScoped(snapshot.cleanup().pipe(provideInstance(second), Effect.provide(logger)))
    yield* awaitWithTimeout(
      Effect.promise(() => cleanupRace.started.promise),
      "first snapshot gc did not start",
      "2 seconds",
    )
    yield* awaitWithTimeout(
      Effect.promise(() => cleanupRace.contender.promise),
      "competing cleanup did not contend",
      "2 seconds",
    )
    expect(concurrencyHarness.calls).toHaveLength(1)
    expect(cleanupRace.maxActive).toBe(1)
    cleanupRace.gate.resolve()
    yield* Fiber.join(one)
    yield* Fiber.join(two)

    expect(concurrencyHarness.calls).toHaveLength(2)
    expect(
      concurrencyHarness.lockCalls.slice(beforeConcurrentCleanup).filter((file) => path.basename(file) === "gc.lock")
        .length,
    ).toBeGreaterThanOrEqual(2)
    expect(new Set(concurrencyHarness.calls.map((call) => call.cwd))).toEqual(new Set([first, second]))
    expect(yield* Effect.promise(() => fs.readFile(gcCompletionFile(concurrencyHarness.data), "utf8"))).toBe(
      String(concurrencyHarness.clock.now),
    )
    expect(logged.some((message) => message.includes("snapshot cleanup failed"))).toBe(true)
  }),
)

sameRepoIt.live(
  "holds gc admission until slow gc completes and rechecks completion before running again",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    sameRepoHarness.clock.now += Duration.toMillis(Duration.hours(2))
    sameRepoHarness.runAdvance.millis = 400
    sameRepoRace.lockAttempts = 0
    sameRepoRace.active = 0
    sameRepoRace.maxActive = 0
    sameRepoRace.started = deferred()
    sameRepoRace.contender = deferred()
    sameRepoRace.gate = deferred()
    const beforeRelease = sameRepoHarness.lockReleases.length

    const one = yield* Effect.forkScoped(snapshot.cleanup().pipe(provideInstance(dir)))
    const two = yield* Effect.forkScoped(snapshot.cleanup().pipe(provideInstance(dir)))
    yield* awaitWithTimeout(
      Effect.promise(() => sameRepoRace.started.promise),
      "same-repo gc did not start",
      "2 seconds",
    )
    yield* awaitWithTimeout(
      Effect.promise(() => sameRepoRace.contender.promise),
      "second cleanup did not wait for box admission",
      "2 seconds",
    )
    yield* Effect.sleep(Duration.millis(50))
    expect(sameRepoHarness.calls).toHaveLength(1)
    expect(sameRepoRace.lockAttempts).toBe(2)
    expect(sameRepoRace.maxActive).toBe(1)
    expect(
      sameRepoHarness.lockReleases.slice(beforeRelease).filter((file) => path.basename(file) === "gc.lock"),
    ).toEqual([])
    sameRepoRace.gate.resolve()
    yield* Fiber.join(one)
    yield* Fiber.join(two)

    expect(sameRepoHarness.calls).toHaveLength(1)
    expect(
      sameRepoHarness.lockReleases.slice(beforeRelease).filter((file) => path.basename(file) === "gc.lock"),
    ).toHaveLength(2)
    yield* snapshot.cleanup().pipe(provideInstance(dir))
    expect(sameRepoHarness.calls).toHaveLength(1)
    expect(yield* Effect.promise(() => fs.readFile(gcCompletionFile(sameRepoHarness.data), "utf8"))).toBe(
      String(sameRepoHarness.clock.now),
    )
  }),
)

lockOrderIt.live(
  "tracks a second repo while gc holds the box lock for another repo",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const other = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    yield* snapshot.track().pipe(provideInstance(other))
    lockOrder.started = deferred()
    lockOrder.gate = deferred()

    const cleanup = yield* Effect.forkScoped(snapshot.cleanup().pipe(provideInstance(dir)))
    yield* awaitWithTimeout(
      Effect.promise(() => lockOrder.started.promise),
      "cleanup did not enter gc while holding box and repo locks",
      "2 seconds",
    )
    const beforeTrack = lockOrderHarness.lockCalls.length
    const hash = yield* awaitWithTimeout(
      snapshot.track().pipe(provideInstance(other)),
      "tracking waited on gc",
      "2 seconds",
    )
    expect(hash).toBeTruthy()
    const otherGitdir = yield* Effect.promise(() => snapshotGitdir(lockOrderHarness.data, other))
    expect(lockOrderHarness.lockCalls.slice(beforeTrack)).toEqual([repoLockFile(lockOrderHarness.data, otherGitdir)])
    lockOrder.gate.resolve()
    yield* Fiber.join(cleanup)
    expect(lockOrderHarness.calls).toHaveLength(1)
  }).pipe(Effect.ensuring(Effect.sync(() => lockOrder.gate.resolve()))),
)

scheduleIt.live(
  "jitters each scoped loop start and keeps the hourly repeat cadence",
  Effect.gen(function* () {
    const first = yield* tmpdirScoped({ git: true })
    const second = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    scheduleEvents.now = () => scheduleClock.now
    scheduleHarness.randomValues.push(0, 0.99)
    yield* provideInstance(first)(
      Effect.gen(function* () {
        yield* snapshot.track()
        yield* provideInstance(second)(
          Effect.gen(function* () {
            yield* snapshot.track()
            yield* Effect.yieldNow
            scheduleEvents.count = 0
            scheduleEvents.times.length = 0
            const waitForCleanup = (message: string, target: number) =>
              Effect.gen(function* () {
                if (scheduleEvents.count >= target) return
                const wait = yield* Effect.forkScoped(
                  awaitWithTimeout(
                    Effect.promise(() => scheduleEvents.waitFor(target)),
                    message,
                    "30 seconds",
                  ),
                )
                yield* Effect.yieldNow
                yield* scheduleClock.advanceBy(Duration.seconds(31))
                return yield* Fiber.join(wait)
              })
            const waitForFirstRuns = yield* Effect.forkScoped(
              awaitWithTimeout(
                Effect.promise(() =>
                  scheduleClock.waitForRequests(
                    () =>
                      scheduleClock.requests.filter(
                        (request) => request.duration === Duration.toMillis(Duration.minutes(1)),
                      ).length === 1 &&
                      scheduleClock.requests.filter(
                        (request) => request.duration === Duration.toMillis(Duration.hours(1)),
                      ).length === 1,
                  ),
                ),
                "scoped loops did not schedule distinct first cleanups",
                "30 seconds",
              ),
            )
            yield* Effect.forEach(Array.from({ length: 4 }), () => Effect.yieldNow)
            yield* scheduleClock.advanceBy(Duration.seconds(31))
            yield* Fiber.join(waitForFirstRuns)

            const minute = Duration.toMillis(Duration.minutes(1))
            const hour = Duration.toMillis(Duration.hours(1))
            const firstStarts = () => scheduleClock.requests.filter((request) => request.duration === minute)
            const secondStarts = () => scheduleClock.requests.filter((request) => request.duration === hour)
            const first = firstStarts()[0]!
            const second = secondStarts()[0]!
            expect(first.deadline).toBeLessThan(second.deadline)
            yield* scheduleClock.advanceTo(first.deadline)
            yield* waitForCleanup("the first cleanup did not run", 1)
            expect(scheduleEvents.count).toBe(1)

            yield* scheduleClock.advanceTo(second.deadline - 1)
            expect(scheduleEvents.count).toBe(1)
            yield* scheduleClock.advanceTo(second.deadline)
            yield* waitForCleanup("the second cleanup did not run", 2)
            expect(scheduleEvents.count).toBe(2)

            const repeatAt = scheduleEvents.times[0]! + hour
            expect(repeatAt).toBeGreaterThan(scheduleClock.now)
            yield* scheduleClock.advanceTo(repeatAt - 1)
            expect(scheduleEvents.count).toBe(2)
            yield* scheduleClock.advanceTo(repeatAt)
            yield* waitForCleanup("the hourly repeat did not run", 3)
            expect(scheduleEvents.count).toBe(3)
          }),
        )
      }),
    )
  }),
)

reapIt.live(
  "reaps a tracked shadow repo only after its absent worktree outlasts the grace",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    const gitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, dir))
    const completion = gcCompletionFile(reapHarness.data)
    expect(path.basename(gitdir)).toBe(Hash.fast(dir))
    yield* Effect.promise(() => fs.rm(dir, { recursive: true, force: true }))
    reapHarness.clock.now += Duration.toMillis(Duration.hours(2))
    yield* Effect.promise(() => fs.writeFile(completion, String(reapHarness.clock.now - 30_000)))

    yield* snapshot.cleanup().pipe(provideInstance(dir))
    const evidencePath = path.join(gitdir, "info", "opencode-worktree.json")
    const evidence = JSON.parse(yield* Effect.promise(() => fs.readFile(evidencePath, "utf8"))) as {
      missingSince?: number
    }
    expect(evidence.missingSince).toBe(reapHarness.clock.now)

    reapHarness.clock.now += Duration.toMillis(Duration.days(7)) - 1
    yield* Effect.promise(() => fs.writeFile(completion, String(reapHarness.clock.now - 30_000)))
    const beforeRecentCheck = reapHarness.lockCalls.length
    yield* snapshot.cleanup().pipe(provideInstance(dir))
    expect(yield* Effect.promise(() => existsPath(gitdir))).toBe(true)
    expect(
      reapHarness.lockCalls.slice(beforeRecentCheck).filter((file) => file === repoLockFile(reapHarness.data, gitdir)),
    ).toHaveLength(0)

    reapHarness.clock.now += 1
    yield* Effect.promise(() => fs.writeFile(completion, String(reapHarness.clock.now - 30_000)))
    const beforeEligibleReap = reapHarness.lockCalls.length
    yield* snapshot.cleanup().pipe(provideInstance(dir))
    expect(yield* Effect.promise(() => existsPath(gitdir))).toBe(false)
    expect(yield* Effect.promise(() => existsPath(completion))).toBe(true)
    expect(reapHarness.lockCalls.slice(beforeEligibleReap)).toEqual([
      path.join(reapHarness.data, "snapshot", "gc.lock"),
      repoLockFile(reapHarness.data, gitdir),
    ])
  }),
)

reapIt.live(
  "retains present worktrees and shadow repos without trustworthy path evidence",
  Effect.gen(function* () {
    const present = yield* tmpdirScoped({ git: true })
    const unknown = yield* tmpdirScoped({ git: true })
    const forged = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(present))
    yield* snapshot.track().pipe(provideInstance(unknown))
    yield* snapshot.track().pipe(provideInstance(forged))
    const presentGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, present))
    const unknownGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, unknown))
    const forgedGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, forged))
    const movedPresent = `${present}.moved`
    const forgedEvidencePath = path.join(forgedGitdir, "info", "opencode-worktree.json")
    const forgedEvidence = JSON.parse(yield* Effect.promise(() => fs.readFile(forgedEvidencePath, "utf8"))) as Record<
      string,
      unknown
    >
    yield* Effect.promise(() =>
      Promise.all([
        fs.rename(present, movedPresent),
        fs.rm(unknown, { recursive: true, force: true }),
        fs.rm(forged, { recursive: true, force: true }),
        fs.rm(path.join(unknownGitdir, "info", "opencode-worktree.json"), { force: true }),
        fs.writeFile(forgedEvidencePath, JSON.stringify({ ...forgedEvidence, project: "untrusted" })),
      ]),
    )
    reapHarness.clock.now += Duration.toMillis(Duration.hours(2))
    yield* snapshot.cleanup().pipe(provideInstance(present))
    yield* Effect.promise(() => fs.rename(movedPresent, present))
    reapHarness.clock.now += Duration.toMillis(Duration.days(8))

    yield* snapshot.cleanup().pipe(provideInstance(present))

    const presentEvidence = JSON.parse(
      yield* Effect.promise(() => fs.readFile(path.join(presentGitdir, "info", "opencode-worktree.json"), "utf8")),
    ) as { missingSince?: number }
    expect(yield* Effect.promise(() => existsPath(presentGitdir))).toBe(true)
    expect(presentEvidence.missingSince).toBeUndefined()
    expect(yield* Effect.promise(() => existsPath(unknownGitdir))).toBe(true)
    expect(yield* Effect.promise(() => existsPath(forgedGitdir))).toBe(true)
  }),
)

nonWindowsUnstatableReapIt(
  "retains a shadow repo when its recorded worktree cannot be stated",
  Effect.gen(function* () {
    const host = yield* tmpdirScoped({ git: true })
    const workspace = yield* tmpdirScoped()
    const worktree = path.join(workspace, "repo")
    yield* Effect.promise(() => fs.mkdir(worktree, { recursive: true }))
    yield* exec(worktree, ["git", "init"])
    yield* exec(worktree, ["git", "config", "user.email", "test@opencode.test"])
    yield* exec(worktree, ["git", "config", "user.name", "Test"])
    yield* exec(worktree, ["git", "commit", "--allow-empty", "-m", "root"])
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(host))
    yield* snapshot.track().pipe(provideInstance(worktree))
    const gitdir = yield* Effect.promise(() => snapshotGitdir(unstatableReapHarness.data, worktree))
    const evidencePath = path.join(gitdir, "info", "opencode-worktree.json")
    yield* Effect.promise(() => fs.rm(worktree, { recursive: true, force: true }))
    unstatableReapHarness.clock.now += Duration.toMillis(Duration.hours(2))
    yield* Effect.promise(() =>
      fs.writeFile(gcCompletionFile(unstatableReapHarness.data), String(unstatableReapHarness.clock.now - 30_000)),
    )
    yield* snapshot.cleanup().pipe(provideInstance(host))
    const missing = JSON.parse(yield* Effect.promise(() => fs.readFile(evidencePath, "utf8"))) as {
      missingSince?: number
    }
    expect(missing.missingSince).toBe(unstatableReapHarness.clock.now)

    unstatableReapHarness.clock.now += Duration.toMillis(Duration.days(8))
    yield* Effect.promise(() => fs.mkdir(worktree, { recursive: true }))
    unstatableWorktree.path = worktree
    yield* Effect.promise(() =>
      fs.writeFile(gcCompletionFile(unstatableReapHarness.data), String(unstatableReapHarness.clock.now - 30_000)),
    )
    yield* Effect.acquireUseRelease(
      Effect.sync(() => worktree),
      () => snapshot.cleanup().pipe(provideInstance(host)),
      () => Effect.sync(() => (unstatableWorktree.path = "")),
    )

    const retained = JSON.parse(yield* Effect.promise(() => fs.readFile(evidencePath, "utf8"))) as {
      missingSince?: number
    }
    expect(yield* Effect.promise(() => existsPath(gitdir))).toBe(true)
    expect(retained.missingSince).toBe(missing.missingSince)
    expect(unstatableReapHarness.calls).toEqual([])
  }),
)

reapIt.live(
  "skips a contended candidate repo during admitted reaping",
  Effect.gen(function* () {
    const host = yield* tmpdirScoped({ git: true })
    const candidate = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(host))
    yield* snapshot.track().pipe(provideInstance(candidate))
    const hostGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, host))
    const candidateGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, candidate))
    const evidencePath = path.join(candidateGitdir, "info", "opencode-worktree.json")
    yield* Effect.promise(() => fs.rm(candidate, { recursive: true, force: true }))
    reapHarness.clock.now += Duration.toMillis(Duration.hours(2))
    yield* Effect.promise(() =>
      fs.writeFile(gcCompletionFile(reapHarness.data), String(reapHarness.clock.now - 30_000)),
    )
    yield* snapshot.cleanup().pipe(provideInstance(host))
    const firstEvidence = JSON.parse(yield* Effect.promise(() => fs.readFile(evidencePath, "utf8"))) as {
      missingSince?: number
    }
    expect(firstEvidence.missingSince).toBe(reapHarness.clock.now)
    const missingSince = firstEvidence.missingSince
    reapHarness.clock.now += Duration.toMillis(Duration.days(8))
    yield* Effect.promise(() =>
      fs.writeFile(gcCompletionFile(reapHarness.data), String(reapHarness.clock.now - 30_000)),
    )
    reapContention.repoFile = repoLockFile(reapHarness.data, candidateGitdir)
    reapContention.busy = true
    const before = reapHarness.lockCalls.length

    yield* snapshot.cleanup().pipe(provideInstance(host))

    expect(yield* Effect.promise(() => existsPath(candidateGitdir))).toBe(true)
    expect(JSON.parse(yield* Effect.promise(() => fs.readFile(evidencePath, "utf8"))).missingSince).toBe(missingSince)
    const locks = reapHarness.lockCalls.slice(before)
    expect(locks[0]).toBe(path.join(reapHarness.data, "snapshot", "gc.lock"))
    expect(locks).toContain(reapContention.repoFile)
    expect(reapHarness.calls).toEqual([])
  }),
)

reapIt.live(
  "logs a candidate lock infrastructure failure and retains that repo",
  Effect.gen(function* () {
    const host = yield* tmpdirScoped({ git: true })
    const candidate = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(host))
    yield* snapshot.track().pipe(provideInstance(candidate))
    const hostGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, host))
    const candidateGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, candidate))
    yield* Effect.promise(() => fs.rm(candidate, { recursive: true, force: true }))
    reapHarness.clock.now += Duration.toMillis(Duration.hours(2))
    yield* Effect.promise(() =>
      fs.writeFile(gcCompletionFile(reapHarness.data), String(reapHarness.clock.now - 30_000)),
    )
    yield* snapshot.cleanup().pipe(provideInstance(host))
    reapHarness.clock.now += Duration.toMillis(Duration.days(8))
    yield* Effect.promise(() =>
      fs.writeFile(gcCompletionFile(reapHarness.data), String(reapHarness.clock.now - 30_000)),
    )
    reapInfraFailure.enabled = true
    reapInfraFailure.repoFile = repoLockFile(reapHarness.data, candidateGitdir)
    const logged: string[] = []

    yield* snapshot
      .cleanup()
      .pipe(
        provideInstance(host),
        Effect.provide(Logger.layer([Logger.make((item) => logged.push(String(item.message)))])),
      )

    expect(yield* Effect.promise(() => existsPath(candidateGitdir))).toBe(true)
    expect(logged.some((message) => message.includes("snapshot advisory lock unavailable"))).toBe(true)
    expect(reapHarness.calls).toEqual([])
  }),
)

nonWindowsReapIt(
  "logs unreadable snapshot and project scans while continuing the maintenance pass",
  Effect.gen(function* () {
    const host = yield* tmpdirScoped({ git: true })
    const unreadable = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(host))
    yield* snapshot.track().pipe(provideInstance(unreadable))
    const hostGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, host))
    const unreadableGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, unreadable))
    const root = path.join(reapHarness.data, "snapshot")
    const projectDir = path.dirname(unreadableGitdir)
    yield* Effect.promise(() => fs.rm(unreadable, { recursive: true, force: true }))
    reapHarness.clock.now += Duration.toMillis(Duration.hours(2))
    yield* Effect.promise(() =>
      Promise.all([
        fs.writeFile(path.join(root, "gc.lock"), ""),
        fs.writeFile(gcCompletionFile(reapHarness.data), String(reapHarness.clock.now - 30_000)),
      ]),
    )
    const logged: string[] = []

    yield* Effect.acquireUseRelease(
      Effect.promise(async () => {
        await fs.chmod(root, 0o300)
        return root
      }),
      () =>
        snapshot
          .cleanup()
          .pipe(provideInstance(host))
          .pipe(Effect.provide(Logger.layer([Logger.make((item) => logged.push(String(item.message)))]))),
      (dir) => Effect.promise(() => fs.chmod(dir, 0o755)),
    )
    yield* Effect.acquireUseRelease(
      Effect.promise(async () => {
        await fs.chmod(projectDir, 0)
        return projectDir
      }),
      () =>
        snapshot
          .cleanup()
          .pipe(provideInstance(host))
          .pipe(Effect.provide(Logger.layer([Logger.make((item) => logged.push(String(item.message)))]))),
      (dir) => Effect.promise(() => fs.chmod(dir, 0o755)),
    )

    expect(yield* Effect.promise(() => existsPath(unreadableGitdir))).toBe(true)
    expect(logged.some((message) => message.includes("snapshot shadow repo scan failed"))).toBe(true)
    expect(logged.some((message) => message.includes("snapshot shadow project scan failed"))).toBe(true)
    expect(reapHarness.calls).toEqual([])
  }),
)

reapIt.live(
  "logs a candidate work failure and continues reaping other repos",
  Effect.gen(function* () {
    const host = yield* tmpdirScoped({ git: true })
    const failedCandidate = yield* tmpdirScoped({ git: true })
    const nextCandidate = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(host))
    yield* snapshot.track().pipe(provideInstance(failedCandidate))
    yield* snapshot.track().pipe(provideInstance(nextCandidate))
    const hostGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, host))
    const failedGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, failedCandidate))
    const nextGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, nextCandidate))
    yield* Effect.promise(() =>
      Promise.all([
        fs.rm(failedCandidate, { recursive: true, force: true }),
        fs.rm(nextCandidate, { recursive: true, force: true }),
      ]),
    )
    reapHarness.clock.now += Duration.toMillis(Duration.hours(2))
    yield* Effect.promise(() =>
      fs.writeFile(gcCompletionFile(reapHarness.data), String(reapHarness.clock.now - 30_000)),
    )
    yield* snapshot.cleanup().pipe(provideInstance(host))
    reapHarness.clock.now += Duration.toMillis(Duration.days(8))
    yield* Effect.promise(() =>
      fs.writeFile(gcCompletionFile(reapHarness.data), String(reapHarness.clock.now - 30_000)),
    )
    reapWorkFailure.repoFile = repoLockFile(reapHarness.data, failedGitdir)
    reapWorkFailure.failNext = true
    const logged: string[] = []
    const before = reapHarness.lockCalls.length

    const result = yield* Effect.exit(
      snapshot
        .cleanup()
        .pipe(
          provideInstance(host),
          Effect.provide(Logger.layer([Logger.make((item) => logged.push(String(item.message)))])),
        ),
    )

    expect(Exit.isSuccess(result)).toBe(true)
    expect(yield* Effect.promise(() => existsPath(failedGitdir))).toBe(true)
    expect(yield* Effect.promise(() => existsPath(nextGitdir))).toBe(false)
    expect(logged.some((message) => message.includes("snapshot shadow repo reaping failed"))).toBe(true)
    expect(reapHarness.lockCalls.slice(before)[0]).toBe(path.join(reapHarness.data, "snapshot", "gc.lock"))
    expect(reapHarness.calls).toEqual([])
  }),
)

inLockGraceIt.live(
  "rechecks the worktree grace after taking the repo lease",
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped({ git: true })
    const snapshot = yield* Snapshot.Service
    yield* snapshot.track().pipe(provideInstance(dir))
    const gitdir = yield* Effect.promise(() => snapshotGitdir(inLockGraceHarness.data, dir))
    const completion = gcCompletionFile(inLockGraceHarness.data)
    yield* Effect.promise(() => fs.rm(dir, { recursive: true, force: true }))
    inLockGraceHarness.clock.now += Duration.toMillis(Duration.hours(2))
    inLockGraceRace.gitdir = gitdir
    inLockGraceRace.worktree = dir
    inLockGraceRace.since = inLockGraceHarness.clock.now
    yield* Effect.promise(() => fs.writeFile(completion, String(inLockGraceHarness.clock.now - 30_000)))

    yield* snapshot.cleanup().pipe(provideInstance(dir))

    const record = JSON.parse(
      yield* Effect.promise(() => fs.readFile(path.join(gitdir, "info", "opencode-worktree.json"), "utf8")),
    ) as { missingSince?: number }
    expect(record.missingSince).toBe(inLockGraceHarness.clock.now)
    expect(yield* Effect.promise(() => existsPath(gitdir))).toBe(true)
    expect(inLockGraceHarness.lockCalls).toContain(
      path.join(
        inLockGraceHarness.data,
        "snapshot",
        "locks",
        path.basename(path.dirname(gitdir)),
        Hash.fast(dir) + ".lock",
      ),
    )
  }),
)

reapIt.live(
  "parks repeatedly failing reaps and lets later eligible repos advance",
  Effect.gen(function* () {
    const snapshot = yield* Snapshot.Service
    const host = yield* tmpdirScoped({ git: true })
    yield* snapshot.track().pipe(provideInstance(host))
    const worktrees = yield* Effect.forEach(
      Array.from({ length: 17 }),
      () =>
        Effect.gen(function* () {
          const dir = yield* tmpdirScoped({ git: true })
          yield* snapshot.track().pipe(provideInstance(dir))
          return dir
        }),
      { concurrency: 1 },
    )
    const gitdirs = yield* Effect.promise(() =>
      Promise.all(worktrees.map((dir) => snapshotGitdir(reapHarness.data, dir))),
    )
    yield* Effect.promise(() => Promise.all(worktrees.map((dir) => fs.rm(dir, { recursive: true, force: true }))))
    reapHarness.clock.now += Duration.toMillis(Duration.hours(2))
    const root = path.join(reapHarness.data, "snapshot")
    const hostGitdir = yield* Effect.promise(() => snapshotGitdir(reapHarness.data, host))
    const completion = gcCompletionFile(reapHarness.data)
    const refreshCompletion = () => fs.writeFile(completion, String(reapHarness.clock.now - 30_000))
    yield* Effect.promise(refreshCompletion)
    const beforeFirstSweep = reapHarness.lockCalls.length

    yield* snapshot.cleanup().pipe(provideInstance(host))
    const firstPage = Array.from(
      new Set(
        reapHarness.lockCalls
          .slice(beforeFirstSweep)
          .filter(
            (file) => file.startsWith(path.join(root, "locks") + path.sep) && path.basename(file).endsWith(".lock"),
          ),
      ),
    ).slice(0, 16)
    expect(firstPage).toHaveLength(16)

    yield* Effect.promise(refreshCompletion)
    yield* snapshot.cleanup().pipe(provideInstance(host))
    const records = yield* Effect.promise(() =>
      Promise.all(gitdirs.map((gitdir) => fs.readFile(path.join(gitdir, "info", "opencode-worktree.json"), "utf8"))),
    )
    expect(records.every((record) => JSON.parse(record).missingSince !== undefined)).toBe(true)

    const firstPageGitdirs = firstPage.map((file) => {
      const [project, repo] = path.relative(path.join(root, "locks"), file).split(path.sep)
      return path.join(root, project!, repo!.slice(0, -".lock".length))
    })
    const blockedParents = Array.from(new Set(firstPageGitdirs.map((gitdir) => path.dirname(gitdir))))
    expect(blockedParents).toHaveLength(16)
    const laterGitdir = gitdirs.find((gitdir) => !firstPageGitdirs.includes(gitdir))!
    reapHarness.clock.now += Duration.toMillis(Duration.days(8))

    yield* Effect.acquireUseRelease(
      Effect.promise(async () => {
        await Promise.all(blockedParents.map((dir) => fs.chmod(dir, 0o555)))
        return blockedParents
      }),
      () =>
        Effect.gen(function* () {
          for (let attempt = 1; attempt <= 4; attempt++) {
            yield* Effect.promise(refreshCompletion)
            yield* snapshot.cleanup().pipe(provideInstance(host))
            expect(yield* Effect.promise(() => existsPath(laterGitdir))).toBe(attempt < 4)
          }
          const firstPageStillExists = yield* Effect.promise(() =>
            Promise.all(firstPageGitdirs.map((gitdir) => existsPath(gitdir))),
          )
          expect(firstPageStillExists.every(Boolean)).toBe(true)
          const parked = JSON.parse(
            yield* Effect.promise(() =>
              fs.readFile(path.join(firstPageGitdirs[0]!, "info", "opencode-worktree.json"), "utf8"),
            ),
          ) as { reapFailures?: number; worktree: string }
          expect(parked.reapFailures).toBe(3)

          const beforeParkedSweep = reapHarness.lockCalls.length
          yield* Effect.promise(refreshCompletion)
          yield* snapshot.cleanup().pipe(provideInstance(host))
          expect(reapHarness.lockCalls.slice(beforeParkedSweep)).not.toContain(
            repoLockFile(reapHarness.data, firstPageGitdirs[0]!),
          )

          reapHarness.clock.now += Duration.toMillis(Duration.days(1))
          const beforeParkedRetry = reapHarness.lockCalls.length
          yield* Effect.promise(refreshCompletion)
          yield* snapshot.cleanup().pipe(provideInstance(host))
          expect(reapHarness.lockCalls.slice(beforeParkedRetry)).toContain(
            repoLockFile(reapHarness.data, firstPageGitdirs[0]!),
          )
          const retried = JSON.parse(
            yield* Effect.promise(() =>
              fs.readFile(path.join(firstPageGitdirs[0]!, "info", "opencode-worktree.json"), "utf8"),
            ),
          ) as { reapFailures?: number }
          expect(retried.reapFailures).toBe(1)

          const parkedWorktree = parked.worktree
          yield* Effect.promise(() => fs.mkdir(parkedWorktree))
          yield* Effect.promise(refreshCompletion)
          yield* snapshot.cleanup().pipe(provideInstance(host))
          const restored = JSON.parse(
            yield* Effect.promise(() =>
              fs.readFile(path.join(firstPageGitdirs[0]!, "info", "opencode-worktree.json"), "utf8"),
            ),
          ) as { missingSince?: number; reapFailures?: number }
          expect(restored.missingSince).toBeUndefined()
          expect(restored.reapFailures).toBeUndefined()

          yield* Effect.promise(() => fs.rm(parkedWorktree, { recursive: true, force: true }))
          yield* Effect.promise(refreshCompletion)
          yield* snapshot.cleanup().pipe(provideInstance(host))
          const absentAgain = JSON.parse(
            yield* Effect.promise(() =>
              fs.readFile(path.join(firstPageGitdirs[0]!, "info", "opencode-worktree.json"), "utf8"),
            ),
          ) as { missingSince?: number; reapFailures?: number }
          expect(absentAgain.missingSince).toBe(reapHarness.clock.now)
          expect(absentAgain.reapFailures).toBeUndefined()
        }),
      (dirs) => Effect.promise(() => Promise.all(dirs.map((dir) => fs.chmod(dir, 0o755))).then(() => undefined)),
    )
  }),
  { timeout: 30_000 },
)

const exec = (cwd: string, command: string[]) =>
  Effect.promise(async () => {
    const proc = Bun.spawn(command, { cwd, stdout: "ignore", stderr: "pipe" })
    const code = await proc.exited
    if (code !== 0) throw new Error(`${command.join(" ")} failed: ${await new Response(proc.stderr).text()}`)
  })

const write = (file: string, content: string | Uint8Array) =>
  FSUtil.Service.use((fs) => fs.writeWithDirs(file, content))
const readText = (file: string) => FSUtil.Service.use((fs) => fs.readFileString(file))
const exists = (file: string) => FSUtil.Service.use((fs) => fs.existsSafe(file))
const mkdirp = (dir: string) => FSUtil.Service.use((fs) => fs.ensureDir(dir))
const rm = (file: string) =>
  FSUtil.Service.use((fs) => fs.remove(file, { recursive: true, force: true }).pipe(Effect.ignore))

const initialize = Effect.fn("SnapshotTest.initialize")(function* (dir: string) {
  const unique = Math.random().toString(36).slice(2)
  const aContent = `A${unique}`
  const bContent = `B${unique}`
  yield* write(`${dir}/a.txt`, aContent)
  yield* write(`${dir}/b.txt`, bContent)
  return { aContent, bContent }
})

type Bootstrapped = { path: string; extra: { aContent: string; bContent: string } }

const bootstrap = Effect.fn("SnapshotTest.bootstrap")(function* () {
  const tmp = yield* TestInstance
  return { path: tmp.directory, extra: yield* initialize(tmp.directory) }
})

const withTrackedSnapshot = <A, E, R>(
  fn: (input: { tmp: Bootstrapped; snapshot: Snapshot.Interface; before: string }) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    return yield* fn({ tmp, snapshot, before: before! })
  })

const bootstrapScoped = Effect.fn("SnapshotTest.bootstrapScoped")(function* () {
  const dir = yield* tmpdirScoped({ git: true }).pipe(Effect.provide(LayerNode.compile(CrossSpawnSpawner.node)))
  return { path: dir, extra: yield* initialize(dir) }
})

const scopedGitTmpdir = () =>
  tmpdirScoped({ git: true }).pipe(Effect.provide(LayerNode.compile(CrossSpawnSpawner.node)))

const cleanupWorktree = (repo: string, worktree: string, files: string[] = []) =>
  Effect.promise(async () => {
    await $`git worktree remove --force ${worktree}`.cwd(repo).quiet().nothrow()
    await fs.rm(worktree, { recursive: true, force: true }).catch(() => undefined)
    await Promise.all(files.map((file) => fs.rm(file, { recursive: true, force: true }).catch(() => undefined)))
  })

const withGitConfigGlobal = <A, E, R>(config: string, self: Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const previous = process.env.GIT_CONFIG_GLOBAL
      process.env.GIT_CONFIG_GLOBAL = config
      return previous
    }),
    () => self,
    (previous) =>
      Effect.sync(() => {
        if (previous) process.env.GIT_CONFIG_GLOBAL = previous
        else delete process.env.GIT_CONFIG_GLOBAL
      }),
  )

it.instance(
  "tracks deleted files correctly",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* rm(`${tmp.path}/a.txt`)
      expect((yield* snapshot.patch(before)).files).toContain(fwd(tmp.path, "a.txt"))
    }),
  ),
  { git: true },
)

it.instance(
  "revert should remove new files",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/new.txt`, "NEW")
      const patch = yield* snapshot.patch(before)
      yield* snapshot.revert([patch])
      expect(yield* exists(`${tmp.path}/new.txt`)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "revert in subdirectory",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* mkdirp(`${tmp.path}/sub`)
      yield* write(`${tmp.path}/sub/file.txt`, "SUB")
      const patch = yield* snapshot.patch(before)
      yield* snapshot.revert([patch])
      expect(yield* exists(`${tmp.path}/sub/file.txt`)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "multiple file operations",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* rm(`${tmp.path}/a.txt`)
      yield* write(`${tmp.path}/c.txt`, "C")
      yield* mkdirp(`${tmp.path}/dir`)
      yield* write(`${tmp.path}/dir/d.txt`, "D")
      yield* write(`${tmp.path}/b.txt`, "MODIFIED")
      const patch = yield* snapshot.patch(before)
      yield* snapshot.revert([patch])
      expect(yield* readText(`${tmp.path}/a.txt`)).toBe(tmp.extra.aContent)
      expect(yield* exists(`${tmp.path}/c.txt`)).toBe(false)
      expect(yield* readText(`${tmp.path}/b.txt`)).toBe(tmp.extra.bContent)
    }),
  ),
  { git: true },
)

it.instance(
  "empty directory handling",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* mkdirp(`${tmp.path}/empty`)
      expect((yield* snapshot.patch(before)).files.length).toBe(0)
    }),
  ),
  { git: true },
)

it.instance(
  "binary file handling",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/image.png`, new Uint8Array([0x89, 0x50, 0x4e, 0x47]))
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(fwd(tmp.path, "image.png"))
      yield* snapshot.revert([patch])
      expect(yield* exists(`${tmp.path}/image.png`)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "symlink handling",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* Effect.promise(() => fs.symlink(`${tmp.path}/a.txt`, `${tmp.path}/link.txt`, "file"))
      expect((yield* snapshot.patch(before)).files).toContain(fwd(tmp.path, "link.txt"))
    }),
  ),
  { git: true },
)

it.instance(
  "file under size limit handling",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/large.txt`, "x".repeat(1024 * 1024))
      expect((yield* snapshot.patch(before)).files).toContain(fwd(tmp.path, "large.txt"))
    }),
  ),
  { git: true },
)

it.instance(
  "large added files are skipped",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/huge.txt`, new Uint8Array(2 * 1024 * 1024 + 1))
      expect((yield* snapshot.patch(before)).files).toEqual([])
      expect(yield* snapshot.diff(before)).toBe("")
      expect(yield* snapshot.track()).toBe(before)
    }),
  ),
  { git: true },
)

it.instance(
  "nested directory revert",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* mkdirp(`${tmp.path}/level1/level2/level3`)
      yield* write(`${tmp.path}/level1/level2/level3/deep.txt`, "DEEP")
      const patch = yield* snapshot.patch(before)
      yield* snapshot.revert([patch])
      expect(yield* exists(`${tmp.path}/level1/level2/level3/deep.txt`)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "special characters in filenames",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/file with spaces.txt`, "SPACES")
      yield* write(`${tmp.path}/file-with-dashes.txt`, "DASHES")
      yield* write(`${tmp.path}/file_with_underscores.txt`, "UNDERSCORES")
      const files = (yield* snapshot.patch(before)).files
      expect(files).toContain(fwd(tmp.path, "file with spaces.txt"))
      expect(files).toContain(fwd(tmp.path, "file-with-dashes.txt"))
      expect(files).toContain(fwd(tmp.path, "file_with_underscores.txt"))
    }),
  ),
  { git: true },
)

it.instance(
  "revert with empty patches",
  Effect.gen(function* () {
    yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* snapshot.revert([])
    yield* snapshot.revert([{ hash: "dummy", files: [] }])
  }),
  { git: true },
)

it.instance(
  "patch with invalid hash",
  withTrackedSnapshot(({ tmp, snapshot }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/test.txt`, "TEST")
      const patch = yield* snapshot.patch("invalid-hash-12345")
      expect(patch.files).toEqual([])
      expect(patch.hash).toBe("invalid-hash-12345")
    }),
  ),
  { git: true },
)

it.instance(
  "revert non-existent file",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* snapshot.revert([{ hash: before, files: [`${tmp.path}/nonexistent.txt`] }])
    }),
  ),
  { git: true },
)

it.instance(
  "unicode filenames",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      const unicodeFiles = [
        { path: fwd(tmp.path, "文件.txt"), content: "chinese content" },
        { path: fwd(tmp.path, "🚀rocket.txt"), content: "emoji content" },
        { path: fwd(tmp.path, "café.txt"), content: "accented content" },
        { path: fwd(tmp.path, "файл.txt"), content: "cyrillic content" },
      ]
      yield* Effect.all(
        unicodeFiles.map((file) => write(file.path, file.content)),
        { concurrency: "unbounded" },
      )
      const patch = yield* snapshot.patch(before)
      expect(patch.files.length).toBe(4)
      for (const file of unicodeFiles) expect(patch.files).toContain(file.path)
      yield* snapshot.revert([patch])
      for (const file of unicodeFiles) expect(yield* exists(file.path)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance.skip(
  "unicode filenames modification and restore",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const chineseFile = fwd(tmp.path, "文件.txt")
    const cyrillicFile = fwd(tmp.path, "файл.txt")
    yield* write(chineseFile, "original chinese")
    yield* write(cyrillicFile, "original cyrillic")
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* write(chineseFile, "modified chinese")
    yield* write(cyrillicFile, "modified cyrillic")
    const patch = yield* snapshot.patch(before!)
    expect(patch.files).toContain(chineseFile)
    expect(patch.files).toContain(cyrillicFile)
    yield* snapshot.revert([patch])
    expect(yield* readText(chineseFile)).toBe("original chinese")
    expect(yield* readText(cyrillicFile)).toBe("original cyrillic")
  }),
  { git: true },
)

it.instance(
  "unicode filenames in subdirectories",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* mkdirp(`${tmp.path}/目录/подкаталог`)
      const deepFile = fwd(tmp.path, "目录", "подкаталог", "文件.txt")
      yield* write(deepFile, "deep unicode content")
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(deepFile)
      yield* snapshot.revert([patch])
      expect(yield* exists(deepFile)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "very long filenames",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      const longFile = fwd(tmp.path, `${"a".repeat(200)}.txt`)
      yield* write(longFile, "long filename content")
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(longFile)
      yield* snapshot.revert([patch])
      expect(yield* exists(longFile)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "hidden files",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/.hidden`, "hidden content")
      yield* write(`${tmp.path}/.gitignore`, "*.log")
      yield* write(`${tmp.path}/.config`, "config content")
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(fwd(tmp.path, ".hidden"))
      expect(patch.files).toContain(fwd(tmp.path, ".gitignore"))
      expect(patch.files).toContain(fwd(tmp.path, ".config"))
    }),
  ),
  { git: true },
)

it.instance(
  "nested symlinks",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* mkdirp(`${tmp.path}/sub/dir`)
      yield* write(`${tmp.path}/sub/dir/target.txt`, "target content")
      yield* Effect.promise(() => fs.symlink(`${tmp.path}/sub/dir/target.txt`, `${tmp.path}/sub/dir/link.txt`, "file"))
      yield* Effect.promise(() => fs.symlink(`${tmp.path}/sub`, `${tmp.path}/sub-link`, "dir"))
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(fwd(tmp.path, "sub", "dir", "link.txt"))
      expect(patch.files).toContain(fwd(tmp.path, "sub-link"))
    }),
  ),
  { git: true },
)

it.instance(
  "file permissions and ownership changes",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* Effect.promise(() => fs.chmod(`${tmp.path}/a.txt`, 0o600))
      yield* Effect.promise(() => fs.chmod(`${tmp.path}/a.txt`, 0o755))
      yield* Effect.promise(() => fs.chmod(`${tmp.path}/a.txt`, 0o644))
      expect((yield* snapshot.patch(before)).files.length).toBe(0)
    }),
  ),
  { git: true },
)

it.instance(
  "circular symlinks",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* Effect.promise(() =>
        fs.symlink(`${tmp.path}/circular`, `${tmp.path}/circular`, "dir").catch(() => undefined),
      )
      expect((yield* snapshot.patch(before)).files.length).toBeGreaterThanOrEqual(0)
    }),
  ),
  { git: true },
)

it.live(
  "source project gitignore is respected - ignored files are not snapshotted",
  Effect.gen(function* () {
    const dir = yield* scopedGitTmpdir()
    yield* write(`${dir}/.gitignore`, "*.ignored\nbuild/\nnode_modules/\n")
    yield* write(`${dir}/tracked.txt`, "tracked content")
    yield* write(`${dir}/ignored.ignored`, "ignored content")
    yield* mkdirp(`${dir}/build`)
    yield* write(`${dir}/build/output.js`, "build output")
    yield* write(`${dir}/normal.js`, "normal js")
    yield* exec(dir, ["git", "add", "."])
    yield* exec(dir, ["git", "commit", "-m", "init"])
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before = yield* snapshot.track()
      expect(before).toBeTruthy()
      yield* write(`${dir}/tracked.txt`, "modified tracked")
      yield* write(`${dir}/new.ignored`, "new ignored")
      yield* write(`${dir}/new-tracked.txt`, "new tracked")
      yield* write(`${dir}/build/new-build.js`, "new build file")
      const patch = yield* snapshot.patch(before!)
      expect(patch.files).toContain(fwd(dir, "new-tracked.txt"))
      expect(patch.files).toContain(fwd(dir, "tracked.txt"))
      expect(patch.files).not.toContain(fwd(dir, "new.ignored"))
      expect(patch.files).not.toContain(fwd(dir, "ignored.ignored"))
      expect(patch.files).not.toContain(fwd(dir, "build/output.js"))
      expect(patch.files).not.toContain(fwd(dir, "build/new-build.js"))
    }).pipe(provideInstance(dir))
  }),
)

it.live(
  "subdirectory snapshots include scoped changes only",
  Effect.gen(function* () {
    const dir = yield* scopedGitTmpdir()
    const frontend = path.join(dir, "frontend")
    yield* write(`${frontend}/tracked.txt`, "initial")
    yield* write(`${frontend}/deleted.txt`, "initial")
    yield* write(`${dir}/backend/tracked.txt`, "initial")
    yield* write(`${dir}/backend/deleted.txt`, "initial")
    yield* exec(dir, ["git", "add", "."])
    yield* exec(dir, ["git", "commit", "-m", "init"])
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before = yield* snapshot.track()
      expect(before).toBeTruthy()
      yield* write(`${frontend}/tracked.txt`, "changed")
      yield* write(`${frontend}/untracked.txt`, "new")
      yield* rm(`${frontend}/deleted.txt`)
      yield* write(`${dir}/backend/tracked.txt`, "changed")
      yield* rm(`${dir}/backend/deleted.txt`)
      const patch = yield* snapshot.patch(before!)
      const diff = yield* snapshot.diff(before!)
      expect(patch.files).toContain(fwd(frontend, "tracked.txt"))
      expect(patch.files).toContain(fwd(frontend, "untracked.txt"))
      expect(patch.files).toContain(fwd(frontend, "deleted.txt"))
      expect(patch.files).not.toContain(fwd(dir, "backend", "tracked.txt"))
      expect(patch.files).not.toContain(fwd(dir, "backend", "deleted.txt"))
      expect(diff).not.toContain("backend/tracked.txt")
      expect(diff).not.toContain("backend/deleted.txt")
    }).pipe(provideInstance(frontend))
  }),
)

nonWindowsIt(
  "subdirectory snapshots treat wildcard characters literally",
  Effect.gen(function* () {
    const dir = yield* scopedGitTmpdir()
    const subdir = path.join(dir, "src*")
    yield* write(`${subdir}/file.txt`, "initial")
    yield* write(`${subdir}/later-ignored.txt`, "initial")
    yield* write(`${dir}/srca/file.txt`, "initial")
    yield* exec(dir, ["git", "add", "."])
    yield* exec(dir, ["git", "commit", "-m", "init"])
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before = yield* snapshot.track()
      expect(before).toBeTruthy()
      yield* write(`${subdir}/file.txt`, "changed")
      yield* write(`${subdir}/later-ignored.txt`, "changed")
      yield* write(`${subdir}/.gitignore`, "later-ignored.txt\n")
      yield* write(`${dir}/srca/file.txt`, "changed")
      const patch = yield* snapshot.patch(before!)
      const diff = yield* snapshot.diff(before!)
      expect(patch.files).toContain(fwd(subdir, "file.txt"))
      expect(patch.files).toContain(fwd(subdir, ".gitignore"))
      expect(patch.files).not.toContain(fwd(subdir, "later-ignored.txt"))
      expect(patch.files).not.toContain(fwd(dir, "srca", "file.txt"))
      expect(diff).toContain("src*/later-ignored.txt")
      expect(diff).toContain("deleted file mode")
      expect(diff).not.toContain("srca/file.txt")
    }).pipe(provideInstance(subdir))
  }),
)

nonWindowsIt(
  "subdirectory snapshots treat leading colons literally",
  Effect.gen(function* () {
    const dir = yield* scopedGitTmpdir()
    const subdir = path.join(dir, ":src")
    yield* write(`${subdir}/kept.txt`, "initial")
    yield* write(`${subdir}/later-ignored.txt`, "initial")
    yield* exec(dir, ["git", "add", "."])
    yield* exec(dir, ["git", "commit", "-m", "init"])
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before = yield* snapshot.track()
      expect(before).toBeTruthy()
      yield* write(`${subdir}/kept.txt`, "changed")
      yield* write(`${subdir}/later-ignored.txt`, "changed")
      yield* write(`${subdir}/.gitignore`, "later-ignored.txt\n")
      const patch = yield* snapshot.patch(before!)
      const diff = yield* snapshot.diff(before!)
      expect(patch.files).toContain(fwd(subdir, "kept.txt"))
      expect(patch.files).toContain(fwd(subdir, ".gitignore"))
      expect(patch.files).not.toContain(fwd(subdir, "later-ignored.txt"))
      expect(diff).toContain(":src/later-ignored.txt")
      expect(diff).toContain("deleted file mode")
    }).pipe(provideInstance(subdir))
  }),
)

it.instance(
  "gitignore changes",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/.gitignore`, "*.ignored")
      yield* write(`${tmp.path}/test.ignored`, "ignored content")
      yield* write(`${tmp.path}/normal.txt`, "normal content")
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(fwd(tmp.path, ".gitignore"))
      expect(patch.files).toContain(fwd(tmp.path, "normal.txt"))
      expect(patch.files).not.toContain(fwd(tmp.path, "test.ignored"))
    }),
  ),
  { git: true },
)

it.instance(
  "files tracked in snapshot but now gitignored are filtered out",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* write(`${tmp.path}/later-ignored.txt`, "initial content")
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* write(`${tmp.path}/later-ignored.txt`, "modified content")
    yield* write(`${tmp.path}/.gitignore`, "later-ignored.txt\n")
    yield* write(`${tmp.path}/still-tracked.txt`, "new tracked file")
    const patch = yield* snapshot.patch(before!)
    expect(patch.files).not.toContain(fwd(tmp.path, "later-ignored.txt"))
    expect(patch.files).toContain(fwd(tmp.path, ".gitignore"))
    expect(patch.files).toContain(fwd(tmp.path, "still-tracked.txt"))
  }),
  { git: true },
)

it.instance(
  "gitignore updated between track calls filters from diff",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/a.txt`, "modified content")
      yield* write(`${tmp.path}/.gitignore`, "a.txt\n")
      yield* write(`${tmp.path}/b.txt`, "also modified")
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.some((x) => x.file === "a.txt")).toBe(false)
      expect(diffs.some((x) => x.file === ".gitignore")).toBe(true)
      expect(diffs.some((x) => x.file === "b.txt")).toBe(true)
    }),
  ),
  { git: true },
)

it.instance(
  "git info exclude changes",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      const file = `${tmp.path}/.git/info/exclude`
      yield* write(file, `${(yield* Effect.promise(() => Bun.file(file).text())).trimEnd()}\nignored.txt\n`)
      yield* write(`${tmp.path}/ignored.txt`, "ignored content")
      yield* write(`${tmp.path}/normal.txt`, "normal content")
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(fwd(tmp.path, "normal.txt"))
      expect(patch.files).not.toContain(fwd(tmp.path, "ignored.txt"))
      const after = yield* snapshot.track()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.some((x) => x.file === "normal.txt")).toBe(true)
      expect(diffs.some((x) => x.file === "ignored.txt")).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "git info exclude keeps global excludes",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const global = `${tmp.path}/global.ignore`
    const config = `${tmp.path}/global.gitconfig`
    yield* write(global, "global.tmp\n")
    yield* write(config, `[core]\n\texcludesFile = ${global.replaceAll("\\", "/")}\n`)
    yield* withGitConfigGlobal(
      config,
      Effect.gen(function* () {
        const snapshot = yield* Snapshot.Service
        const before = yield* snapshot.track()
        expect(before).toBeTruthy()
        const file = `${tmp.path}/.git/info/exclude`
        yield* write(file, `${(yield* Effect.promise(() => Bun.file(file).text())).trimEnd()}\ninfo.tmp\n`)
        yield* write(`${tmp.path}/global.tmp`, "global content")
        yield* write(`${tmp.path}/info.tmp`, "info content")
        yield* write(`${tmp.path}/normal.txt`, "normal content")
        const patch = yield* snapshot.patch(before!)
        expect(patch.files).toContain(fwd(tmp.path, "normal.txt"))
        expect(patch.files).not.toContain(fwd(tmp.path, "global.tmp"))
        expect(patch.files).not.toContain(fwd(tmp.path, "info.tmp"))
      }),
    )
  }),
  { git: true },
)

it.instance(
  "concurrent file operations during patch",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      const fiber = yield* Effect.gen(function* () {
        for (let i = 0; i < 10; i++) {
          yield* write(`${tmp.path}/concurrent${i}.txt`, `concurrent${i}`)
          yield* Effect.sleep("1 millis")
        }
      }).pipe(Effect.forkScoped)
      const patch = yield* snapshot.patch(before)
      yield* Fiber.join(fiber)
      expect(patch.files.length).toBeGreaterThanOrEqual(0)
    }),
  ),
  { git: true },
)

it.live(
  "snapshot state isolation between projects",
  Effect.gen(function* () {
    const tmp1 = yield* bootstrapScoped()
    const tmp2 = yield* bootstrapScoped()
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before1 = yield* snapshot.track()
      yield* write(`${tmp1.path}/project1.txt`, "project1 content")
      const patch1 = yield* snapshot.patch(before1!)
      expect(patch1.files).toContain(fwd(tmp1.path, "project1.txt"))
    }).pipe(provideInstance(tmp1.path))
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before2 = yield* snapshot.track()
      yield* write(`${tmp2.path}/project2.txt`, "project2 content")
      const patch2 = yield* snapshot.patch(before2!)
      expect(patch2.files).toContain(fwd(tmp2.path, "project2.txt"))
      expect(patch2.files).not.toContain(fwd(tmp1.path, "project1.txt"))
    }).pipe(provideInstance(tmp2.path))
  }),
)

it.live(
  "patch detects changes in secondary worktree",
  Effect.gen(function* () {
    const tmp = yield* bootstrapScoped()
    const worktreePath = `${tmp.path}-worktree`
    yield* exec(tmp.path, ["git", "worktree", "add", worktreePath, "HEAD"])
    yield* Effect.addFinalizer(() => cleanupWorktree(tmp.path, worktreePath))
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      expect(yield* snapshot.track()).toBeTruthy()
    }).pipe(provideInstance(tmp.path))
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before = yield* snapshot.track()
      expect(before).toBeTruthy()
      const worktreeFile = fwd(worktreePath, "worktree.txt")
      yield* write(worktreeFile, "worktree content")
      expect((yield* snapshot.patch(before!)).files).toContain(worktreeFile)
    }).pipe(provideInstance(worktreePath))
  }),
)

it.live(
  "revert only removes files in invoking worktree",
  Effect.gen(function* () {
    const tmp = yield* bootstrapScoped()
    const worktreePath = `${tmp.path}-worktree`
    const primaryFile = `${tmp.path}/worktree.txt`
    yield* exec(tmp.path, ["git", "worktree", "add", worktreePath, "HEAD"])
    yield* Effect.addFinalizer(() => cleanupWorktree(tmp.path, worktreePath, [primaryFile]))
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      expect(yield* snapshot.track()).toBeTruthy()
    }).pipe(provideInstance(tmp.path))
    yield* write(primaryFile, "primary content")
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before = yield* snapshot.track()
      expect(before).toBeTruthy()
      const worktreeFile = fwd(worktreePath, "worktree.txt")
      yield* write(worktreeFile, "worktree content")
      const patch = yield* snapshot.patch(before!)
      yield* snapshot.revert([patch])
      expect(yield* exists(worktreeFile)).toBe(false)
    }).pipe(provideInstance(worktreePath))
    expect(yield* readText(primaryFile)).toBe("primary content")
  }),
)

it.live(
  "diff reports worktree-only/shared edits and ignores primary-only",
  Effect.gen(function* () {
    const tmp = yield* bootstrapScoped()
    const worktreePath = `${tmp.path}-worktree`
    yield* exec(tmp.path, ["git", "worktree", "add", worktreePath, "HEAD"])
    yield* Effect.addFinalizer(() =>
      cleanupWorktree(tmp.path, worktreePath, [`${tmp.path}/shared.txt`, `${tmp.path}/primary-only.txt`]),
    )
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      expect(yield* snapshot.track()).toBeTruthy()
    }).pipe(provideInstance(tmp.path))
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before = yield* snapshot.track()
      expect(before).toBeTruthy()
      yield* write(`${worktreePath}/worktree-only.txt`, "worktree diff content")
      yield* write(`${worktreePath}/shared.txt`, "worktree edit")
      yield* write(`${tmp.path}/shared.txt`, "primary edit")
      yield* write(`${tmp.path}/primary-only.txt`, "primary change")
      const diff = yield* snapshot.diff(before!)
      expect(diff).toContain("worktree-only.txt")
      expect(diff).toContain("shared.txt")
      expect(diff).not.toContain("primary-only.txt")
    }).pipe(provideInstance(worktreePath))
  }),
)

it.instance(
  "track with no changes returns same hash",
  withTrackedSnapshot(({ snapshot, before }) =>
    Effect.gen(function* () {
      expect(yield* snapshot.track()).toBe(before)
      expect(yield* snapshot.track()).toBe(before)
    }),
  ),
  { git: true },
)

it.instance(
  "diff function with various changes",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* rm(`${tmp.path}/a.txt`)
      yield* write(`${tmp.path}/new.txt`, "new content")
      yield* write(`${tmp.path}/b.txt`, "modified content")
      const diff = yield* snapshot.diff(before)
      expect(diff).toContain("a.txt")
      expect(diff).toContain("b.txt")
      expect(diff).toContain("new.txt")
    }),
  ),
  { git: true },
)

it.instance(
  "restore function",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* rm(`${tmp.path}/a.txt`)
      yield* write(`${tmp.path}/new.txt`, "new content")
      yield* write(`${tmp.path}/b.txt`, "modified")
      yield* snapshot.restore(before)
      expect(yield* exists(`${tmp.path}/a.txt`)).toBe(true)
      expect(yield* readText(`${tmp.path}/a.txt`)).toBe(tmp.extra.aContent)
      expect(yield* exists(`${tmp.path}/new.txt`)).toBe(true)
      expect(yield* readText(`${tmp.path}/b.txt`)).toBe(tmp.extra.bContent)
    }),
  ),
  { git: true },
)

it.instance(
  "revert should not delete files that existed but were deleted in snapshot",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const snapshot1 = yield* snapshot.track()
    expect(snapshot1).toBeTruthy()
    yield* rm(`${tmp.path}/a.txt`)
    const snapshot2 = yield* snapshot.track()
    expect(snapshot2).toBeTruthy()
    yield* write(`${tmp.path}/a.txt`, "recreated content")
    const patch = yield* snapshot.patch(snapshot2!)
    expect(patch.files).toContain(fwd(tmp.path, "a.txt"))
    yield* snapshot.revert([patch])
    expect(yield* exists(`${tmp.path}/a.txt`)).toBe(false)
  }),
  { git: true },
)

it.instance(
  "revert preserves file that existed in snapshot when deleted then recreated",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* write(`${tmp.path}/existing.txt`, "original content")
    const hash = yield* snapshot.track()
    expect(hash).toBeTruthy()
    yield* rm(`${tmp.path}/existing.txt`)
    yield* write(`${tmp.path}/existing.txt`, "recreated")
    yield* write(`${tmp.path}/newfile.txt`, "new")
    const patch = yield* snapshot.patch(hash!)
    expect(patch.files).toContain(fwd(tmp.path, "existing.txt"))
    expect(patch.files).toContain(fwd(tmp.path, "newfile.txt"))
    yield* snapshot.revert([patch])
    expect(yield* exists(`${tmp.path}/newfile.txt`)).toBe(false)
    expect(yield* exists(`${tmp.path}/existing.txt`)).toBe(true)
    expect(yield* readText(`${tmp.path}/existing.txt`)).toBe("original content")
  }),
  { git: true },
)

it.instance(
  "diffFull sets status based on git change type",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* write(`${tmp.path}/grow.txt`, "one\n")
    yield* write(`${tmp.path}/trim.txt`, "line1\nline2\n")
    yield* write(`${tmp.path}/delete.txt`, "gone")
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* write(`${tmp.path}/grow.txt`, "one\ntwo\n")
    yield* write(`${tmp.path}/trim.txt`, "line1\n")
    yield* rm(`${tmp.path}/delete.txt`)
    yield* write(`${tmp.path}/added.txt`, "new")
    const after = yield* snapshot.track()
    expect(after).toBeTruthy()
    const diffs = yield* snapshot.diffFull(before!, after!)
    expect(diffs.length).toBe(4)
    expect(diffs.find((d) => d.file === "added.txt")!.status).toBe("added")
    expect(diffs.find((d) => d.file === "delete.txt")!.status).toBe("deleted")
    const grow = diffs.find((d) => d.file === "grow.txt")!
    expect(grow.status).toBe("modified")
    expect(grow.additions).toBeGreaterThan(0)
    expect(grow.deletions).toBe(0)
    const trim = diffs.find((d) => d.file === "trim.txt")!
    expect(trim.status).toBe("modified")
    expect(trim.additions).toBe(0)
    expect(trim.deletions).toBeGreaterThan(0)
  }),
  { git: true },
)

it.instance(
  "diffFull with new file additions",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/new.txt`, "new content")
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(1)
      expect(diffs[0].file).toBe("new.txt")
      expect(diffs[0].patch).toContain("+new content")
      expect(diffs[0].additions).toBe(1)
      expect(diffs[0].deletions).toBe(0)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with a large interleaved mixed diff",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const ids = Array.from({ length: MIXED_BATCH_GROUP_COUNT }, (_, i) => i.toString().padStart(3, "0"))
    const mod = ids.map((id) => fwd(tmp.path, "mix", `${id}-mod.txt`))
    const del = ids.map((id) => fwd(tmp.path, "mix", `${id}-del.txt`))
    const add = ids.map((id) => fwd(tmp.path, "mix", `${id}-add.txt`))
    const bin = ids.map((id) => fwd(tmp.path, "mix", `${id}-bin.bin`))
    yield* mkdirp(`${tmp.path}/mix`)
    yield* Effect.all(
      [
        ...mod.map((file, i) => write(file, `before-${ids[i]}-é\n🙂\nline`)),
        ...del.map((file, i) => write(file, `gone-${ids[i]}\n你好`)),
        ...bin.map((file, i) => write(file, new Uint8Array([0, i, 255, i % 251]))),
      ],
      { concurrency: "unbounded" },
    )
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* Effect.all(
      [
        ...mod.map((file, i) => write(file, `after-${ids[i]}-é\n🚀\nline`)),
        ...add.map((file, i) => write(file, `new-${ids[i]}\nこんにちは`)),
        ...bin.map((file, i) => write(file, new Uint8Array([9, i, 8, i % 251]))),
        ...del.map((file) => rm(file)),
      ],
      { concurrency: "unbounded" },
    )
    const after = yield* snapshot.track()
    expect(after).toBeTruthy()
    const diffs = yield* snapshot.diffFull(before!, after!)
    expect(diffs).toHaveLength(ids.length * 4)
    const map = new Map(diffs.map((item) => [item.file, item]))
    for (let i = 0; i < ids.length; i++) {
      const m = map.get(fwd("mix", `${ids[i]}-mod.txt`))
      expect(m).toBeDefined()
      expect(m!.patch).toContain(`-before-${ids[i]}-é`)
      expect(m!.patch).toContain(`+after-${ids[i]}-é`)
      expect(m!.status).toBe("modified")
      const d = map.get(fwd("mix", `${ids[i]}-del.txt`))
      expect(d).toBeDefined()
      expect(d!.patch).toContain(`-gone-${ids[i]}`)
      expect(d!.status).toBe("deleted")
      const a = map.get(fwd("mix", `${ids[i]}-add.txt`))
      expect(a).toBeDefined()
      expect(a!.patch).toContain(`+new-${ids[i]}`)
      expect(a!.status).toBe("added")
      const b = map.get(fwd("mix", `${ids[i]}-bin.bin`))
      expect(b).toBeDefined()
      expect(b!.patch).toBe("")
      expect(b!.additions).toBe(0)
      expect(b!.deletions).toBe(0)
      expect(b!.status).toBe("modified")
    }
  }),
  { git: true },
)

it.instance(
  "diffFull preserves git diff order across batch boundaries",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const ids = Array.from({ length: OVER_BATCH_COUNT }, (_, i) => i.toString().padStart(3, "0"))
    yield* mkdirp(`${tmp.path}/order`)
    yield* Effect.all(
      ids.map((id) => write(`${tmp.path}/order/${id}.txt`, `before-${id}`)),
      { concurrency: "unbounded" },
    )
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* Effect.all(
      ids.map((id) => write(`${tmp.path}/order/${id}.txt`, `after-${id}`)),
      { concurrency: "unbounded" },
    )
    const after = yield* snapshot.track()
    expect(after).toBeTruthy()
    expect((yield* snapshot.diffFull(before!, after!)).map((item) => item.file)).toEqual(
      ids.map((id) => `order/${id}.txt`),
    )
  }),
  { git: true },
)

it.instance(
  "diffFull with file modifications",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/b.txt`, "modified content")
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(1)
      expect(diffs[0].file).toBe("b.txt")
      expect(diffs[0].patch).toContain(`-${tmp.extra.bContent}`)
      expect(diffs[0].patch).toContain("+modified content")
      expect(diffs[0].additions).toBeGreaterThan(0)
      expect(diffs[0].deletions).toBeGreaterThan(0)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with file deletions",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* rm(`${tmp.path}/a.txt`)
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(1)
      expect(diffs[0].file).toBe("a.txt")
      expect(diffs[0].patch).toContain(`-${tmp.extra.aContent}`)
      expect(diffs[0].additions).toBe(0)
      expect(diffs[0].deletions).toBe(1)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with multiple line additions",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/multi.txt`, "line1\nline2\nline3")
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(1)
      expect(diffs[0].file).toBe("multi.txt")
      expect(diffs[0].patch).toContain("+line1")
      expect(diffs[0].patch).toContain("+line3")
      expect(diffs[0].additions).toBe(3)
      expect(diffs[0].deletions).toBe(0)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with addition and deletion",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/added.txt`, "added content")
      yield* rm(`${tmp.path}/a.txt`)
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(2)
      const added = diffs.find((d) => d.file === "added.txt")!
      expect(added.patch).toContain("+added content")
      expect(added.additions).toBe(1)
      expect(added.deletions).toBe(0)
      const removed = diffs.find((d) => d.file === "a.txt")!
      expect(removed.patch).toContain(`-${tmp.extra.aContent}`)
      expect(removed.additions).toBe(0)
      expect(removed.deletions).toBe(1)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with multiple additions and deletions",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/multi1.txt`, "line1\nline2\nline3")
      yield* write(`${tmp.path}/multi2.txt`, "single line")
      yield* rm(`${tmp.path}/a.txt`)
      yield* rm(`${tmp.path}/b.txt`)
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(4)
      expect(diffs.find((d) => d.file === "multi1.txt")!.additions).toBe(3)
      expect(diffs.find((d) => d.file === "multi1.txt")!.deletions).toBe(0)
      expect(diffs.find((d) => d.file === "multi2.txt")!.additions).toBe(1)
      expect(diffs.find((d) => d.file === "multi2.txt")!.deletions).toBe(0)
      expect(diffs.find((d) => d.file === "a.txt")!.additions).toBe(0)
      expect(diffs.find((d) => d.file === "a.txt")!.deletions).toBe(1)
      expect(diffs.find((d) => d.file === "b.txt")!.additions).toBe(0)
      expect(diffs.find((d) => d.file === "b.txt")!.deletions).toBe(1)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with no changes",
  withTrackedSnapshot(({ snapshot, before }) =>
    Effect.gen(function* () {
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      expect((yield* snapshot.diffFull(before, after!)).length).toBe(0)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with binary file changes",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/binary.bin`, new Uint8Array([0x00, 0x01, 0x02, 0x03]))
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(1)
      expect(diffs[0].file).toBe("binary.bin")
      expect(diffs[0].patch).toBe("")
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with whitespace changes",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* write(`${tmp.path}/whitespace.txt`, "line1\nline2")
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* write(`${tmp.path}/whitespace.txt`, "line1\n\nline2\n")
    const after = yield* snapshot.track()
    expect(after).toBeTruthy()
    const diffs = yield* snapshot.diffFull(before!, after!)
    expect(diffs.length).toBe(1)
    expect(diffs[0].file).toBe("whitespace.txt")
    expect(diffs[0].additions).toBeGreaterThan(0)
  }),
  { git: true },
)

it.instance(
  "revert with overlapping files across patches uses first patch hash",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* write(`${tmp.path}/shared.txt`, "v1")
    const snap1 = yield* snapshot.track()
    expect(snap1).toBeTruthy()
    yield* write(`${tmp.path}/shared.txt`, "v2")
    const snap2 = yield* snapshot.track()
    expect(snap2).toBeTruthy()
    yield* write(`${tmp.path}/shared.txt`, "v3")
    const patch1 = yield* snapshot.patch(snap1!)
    const patch2 = yield* snapshot.patch(snap2!)
    expect(patch1.files).toContain(fwd(tmp.path, "shared.txt"))
    expect(patch2.files).toContain(fwd(tmp.path, "shared.txt"))
    yield* snapshot.revert([patch1, patch2])
    expect(yield* readText(`${tmp.path}/shared.txt`)).toBe("v1")
  }),
  { git: true },
)

it.instance(
  "revert preserves patch order when the same hash appears again",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* mkdirp(`${tmp.path}/foo`)
    yield* write(`${tmp.path}/foo/bar`, "v1")
    yield* write(`${tmp.path}/a.txt`, "v1")
    const snap1 = yield* snapshot.track()
    expect(snap1).toBeTruthy()
    yield* rm(`${tmp.path}/foo`)
    yield* write(`${tmp.path}/foo`, "v2")
    yield* write(`${tmp.path}/a.txt`, "v2")
    const snap2 = yield* snapshot.track()
    expect(snap2).toBeTruthy()
    yield* rm(`${tmp.path}/foo`)
    yield* write(`${tmp.path}/a.txt`, "v3")
    yield* snapshot.revert([
      { hash: snap1!, files: [fwd(tmp.path, "a.txt")] },
      { hash: snap2!, files: [fwd(tmp.path, "foo")] },
      { hash: snap1!, files: [fwd(tmp.path, "foo", "bar")] },
    ])
    expect(yield* readText(`${tmp.path}/a.txt`)).toBe("v1")
    expect((yield* Effect.promise(() => fs.stat(`${tmp.path}/foo`))).isDirectory()).toBe(true)
    expect(yield* readText(`${tmp.path}/foo/bar`)).toBe("v1")
  }),
  { git: true },
)

it.instance(
  "revert handles large mixed batches across chunk boundaries",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const base = Array.from({ length: OVER_BATCH_COUNT }, (_, i) => fwd(tmp.path, "batch", `${i}.txt`))
    const fresh = [fwd(tmp.path, "fresh", "0.txt")]
    yield* mkdirp(`${tmp.path}/batch`)
    yield* mkdirp(`${tmp.path}/fresh`)
    yield* Effect.all(
      base.map((file, i) => write(file, `base-${i}`)),
      { concurrency: "unbounded" },
    )
    const snap = yield* snapshot.track()
    expect(snap).toBeTruthy()
    yield* Effect.all(
      [...base.map((file, i) => write(file, `next-${i}`)), ...fresh.map((file, i) => write(file, `fresh-${i}`))],
      { concurrency: "unbounded" },
    )
    const patch = yield* snapshot.patch(snap!)
    expect(patch.files.length).toBe(base.length + fresh.length)
    yield* snapshot.revert([patch])
    for (let i = 0; i < base.length; i++) expect(yield* readText(base[i])).toBe(`base-${i}`)
    for (const file of fresh) expect(yield* exists(file)).toBe(false)
  }),
  { git: true },
)
