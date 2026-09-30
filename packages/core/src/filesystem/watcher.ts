export * as Watcher from "./watcher"

import type ParcelWatcher from "@parcel/watcher"
import { makeLocationNode } from "../effect/app-node"
import { Cause, Context, Deferred, Effect, Layer } from "effect"
import { FileSystemWatcher } from "@opencode-ai/schema/filesystem-watcher"
import { existsSync, watch } from "fs"
import path from "path"
import { fileURLToPath } from "url"
import { Worker } from "worker_threads"
import { Config } from "../config"
import { EventV2 } from "../event"
import { Flag } from "../flag/flag"
import { FSUtil } from "../fs-util"
import { Git } from "../git"
import { Location } from "../location"
import { Ignore } from "./ignore"
import { ParcelBinding } from "./parcel-binding"
import type { Reply, Request, Response } from "./parcel-worker"
import { Protected } from "./protected"

declare const OPENCODE_WATCHER_WORKER_PATH: string | undefined

// Backstop only: an unanswered root subscription is reported as unconfirmed
// after this long, and still becomes active if the worker acknowledges later.
const SUBSCRIBE_TIMEOUT_MS = 10_000

export const Event = FileSystemWatcher.Event

function getBackend() {
  if (process.platform === "win32") return "windows"
  if (process.platform === "darwin") return "fs-events"
  if (process.platform === "linux") return "inotify"
}

function protecteds(dir: string) {
  return Protected.paths().filter((item) => {
    const relative = path.relative(dir, item)
    return relative !== "" && !relative.startsWith("..") && !path.isAbsolute(relative)
  })
}

export const hasNativeBinding = () => !!ParcelBinding.load()

export type WatchState = "starting" | "active" | "unavailable" | "unconfirmed"

export interface WatchStatus {
  readonly watch: "git" | "root"
  readonly directory: string
  readonly state: WatchState
  readonly reason?: string
}

export interface Interface {
  readonly status: Effect.Effect<ReadonlyArray<WatchStatus>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/FileWatcher") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const statuses = new Map<WatchStatus["watch"], WatchStatus>()
    const service = Service.of({ status: Effect.sync(() => [...statuses.values()]) })
    if (yield* Flag.OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER) return service

    const location = yield* Location.Service
    const events = yield* EventV2.Service
    const fs = yield* FSUtil.Service
    const git = yield* Git.Service
    const context = yield* Effect.context()
    const runFork = Effect.runForkWith(context)
    const publish = (file: string, event: "add" | "change" | "unlink") =>
      runFork(events.publish(Event.Updated, { file, event }))
    const report = (status: WatchStatus) => {
      statuses.set(status.watch, status)
      const fields = {
        watch: status.watch,
        directory: status.directory,
        ...(status.reason ? { reason: status.reason } : {}),
      }
      if (status.state === "active") return runFork(Effect.logInfo("watcher active", fields))
      if (status.state === "unavailable")
        return runFork(Effect.logWarning("watcher unavailable, continuing without it", fields))
      if (status.state === "unconfirmed")
        return runFork(Effect.logWarning("watcher not confirmed, continuing without it until it confirms", fields))
    }

    const config = (yield* (yield* Config.Service).entries())
      .filter((entry): entry is Config.Document => entry.type === "document")
      .flatMap((item) => item.info.watcher?.ignore ?? [])

    if (location.vcs?.type === "git") {
      const resolved = (yield* git.repo.discover(location.directory))?.gitDirectory
      const vcs = resolved ? yield* fs.realPath(resolved).pipe(Effect.catch(() => Effect.succeed(resolved))) : undefined
      if (vcs && !config.includes(".git") && !config.includes(vcs) && (!resolved || !config.includes(resolved))) {
        yield* watchHead(vcs)
      }
    }

    if (location.vcs && (yield* Flag.OPENCODE_EXPERIMENTAL_FILEWATCHER)) {
      yield* watchRoot(location.directory, [...Ignore.PATTERNS, ...config, ...protecteds(location.directory)])
    }

    return service

    // Only HEAD in the git directory matters to consumers (branch changes). A
    // non-recursive fs.watch needs one inotify instance that it requests
    // synchronously, so a refused instance fails here with its errno instead of
    // parking the thread the way parcel's native backend does.
    function watchHead(vcs: string) {
      return Effect.gen(function* () {
        const head = path.join(vcs, "HEAD")
        const present = { value: existsSync(head) }
        const watcher = yield* Effect.try({
          try: () =>
            watch(vcs, (type, name) => {
              if (name !== "HEAD") return
              if (type === "change") return publish(head, "change")
              const exists = existsSync(head)
              const event = !exists ? "unlink" : present.value ? "change" : "add"
              present.value = exists
              publish(head, event)
            }),
          catch: (error) => error,
        }).pipe(Effect.catch((error) => Effect.sync(() => void report(failure("git", vcs, error)))))
        if (!watcher) return
        watcher.unref()
        watcher.on("error", (error) => {
          watcher.close()
          report(failure("git", vcs, error))
        })
        yield* Effect.addFinalizer(() => Effect.sync(() => watcher.close()))
        report({ watch: "git", directory: vcs, state: "active" })
      })
    }

    // The root watch needs parcel's recursive ignore support, so it keeps parcel
    // but subscribes through the parcel worker (see parcel-worker.ts). Only the
    // worker's acknowledgement marks it active.
    function watchRoot(directory: string, ignore: string[]) {
      return Effect.gen(function* () {
        const backend = getBackend()
        if (!backend) {
          yield* Effect.logError("watcher backend not supported", { directory, platform: process.platform })
          return report({ watch: "root", directory, state: "unavailable", reason: "backend not supported" })
        }
        if (!ParcelBinding.load())
          return report({ watch: "root", directory, state: "unavailable", reason: "native binding unavailable" })

        const host = yield* Effect.try({ try: parcelHost, catch: (error) => error }).pipe(
          Effect.catch((error) => Effect.sync(() => void report(failure("root", directory, error)))),
        )
        if (!host) return
        const id = host.next++
        const unsubscribed = yield* Deferred.make<void>()
        statuses.set("root", { watch: "root", directory, state: "starting" })

        host.listeners.set(id, (reply) => {
          if (reply.type === "updates")
            return reply.updates.forEach((update) => publish(update.path, KINDS[update.type]))
          if (reply.type === "subscribed") return report({ watch: "root", directory, state: "active" })
          if (reply.type === "failed" || reply.type === "error")
            return report({ watch: "root", directory, state: "unavailable", reason: reply.message })
          if (reply.type === "unsubscribed") Deferred.doneUnsafe(unsubscribed, Effect.void)
        })
        host.worker.postMessage({ id, type: "subscribe", directory, ignore, backend } satisfies Request)

        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            // Always sent, so a subscription acknowledged after disposal is released.
            host.worker.postMessage({ id, type: "unsubscribe" } satisfies Request)
            // Awaited only when active: a parked worker never answers.
            if (statuses.get("root")?.state === "active")
              yield* Deferred.await(unsubscribed).pipe(Effect.timeoutOption(SUBSCRIBE_TIMEOUT_MS))
            host.listeners.delete(id)
          }),
        )
        yield* Effect.sleep(SUBSCRIBE_TIMEOUT_MS).pipe(
          Effect.andThen(
            Effect.sync(() => {
              if (statuses.get("root")?.state !== "starting") return
              report({
                watch: "root",
                directory,
                state: "unconfirmed",
                reason: `no subscription acknowledgement within ${SUBSCRIBE_TIMEOUT_MS}ms`,
              })
            }),
          ),
          Effect.forkScoped,
        )
      })
    }
  }).pipe(
    Effect.catchCause((cause) => {
      return Effect.logError("failed to init watcher service", { cause: Cause.pretty(cause) }).pipe(
        Effect.as(Service.of({ status: Effect.succeed([]) })),
      )
    }),
  ),
)

const KINDS = { create: "add", update: "change", delete: "unlink" } as const satisfies Record<
  ParcelWatcher.EventType,
  string
>

function failure(watch: WatchStatus["watch"], directory: string, error: unknown): WatchStatus {
  const code = error instanceof Error && "code" in error ? String(error.code) : undefined
  const message = error instanceof Error ? error.message : String(error)
  return {
    watch,
    directory,
    state: "unavailable",
    reason: code && !message.includes(code) ? `${code}: ${message}` : message,
  }
}

type ParcelHost = { worker: Worker; listeners: Map<number, (reply: Reply) => void>; next: number }

const parcel: { host?: ParcelHost } = {}

// The one worker that owns every parcel subscription in this process. It is
// never terminated: a worker parked in native code cannot be, so it is unref'd
// and must not hold the process open. If it dies, every subscription it held
// becomes unavailable and the next root watch starts a new worker.
function parcelHost() {
  if (parcel.host) return parcel.host
  const worker = new Worker(workerTarget())
  worker.unref()
  const host: ParcelHost = { worker, listeners: new Map(), next: 0 }
  const fail = (message: string) => {
    if (parcel.host === host) parcel.host = undefined
    host.listeners.forEach((listener) => listener({ type: "failed", message }))
  }
  worker.on("message", (response: Response) => host.listeners.get(response.id)?.(response))
  worker.on("error", (error) => fail(error.message))
  worker.on("exit", (code) => fail(`watcher worker exited (${code})`))
  parcel.host = host
  return host
}

function workerTarget() {
  if (typeof OPENCODE_WATCHER_WORKER_PATH !== "undefined") return OPENCODE_WATCHER_WORKER_PATH
  const built = new URL("./parcel-worker.js", import.meta.url)
  if (existsSync(fileURLToPath(built))) return built
  return new URL("./parcel-worker.ts", import.meta.url)
}

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [FSUtil.node, Location.node, Config.node, Git.node, EventV2.node],
})
