import { EventWriter } from "@opencode-ai/core/event/writer"
import type { ProcessGroupEvent } from "@opencode-ai/core/cross-spawn-spawner"
import { withTimeout } from "@/util/timeout"

export type Event = ProcessGroupEvent & {
  readonly eventID: number
  readonly identity: EventWriter.ProcessIdentity
}

export type WorkerTracker = {
  observe(event: ProcessGroupEvent): Promise<void>
  acknowledge(eventID: number): boolean
  readonly identityCount: number
}

export type Owner = {
  stop(): Promise<void>
  get(pgid: number): Event | undefined
  has(pgid: number): boolean
  readonly size: number
}

type OwnerClient = {
  subscribe(handler: (event: Event) => void): () => void
  acknowledge(eventID: number): Promise<unknown>
  shutdown(): Promise<unknown>
}

type OwnerWorker = {
  onerror: ((event: ErrorEvent) => unknown) | null
  terminate(): unknown
}

type Owned = Pick<Event, "pgid">

function sameIdentity(left: EventWriter.ProcessIdentity, right: EventWriter.ProcessIdentity) {
  return left.pid === right.pid && left.bootID === right.bootID && left.startTicks === right.startTicks
}

function alive(pgid: number) {
  try {
    process.kill(-pgid, 0)
    return true
  } catch (error) {
    return !(error instanceof Error && "code" in error && error.code === "ESRCH")
  }
}

function signalOwned(groups: Iterable<Owned>) {
  if (process.platform === "win32") return
  // The owner retains each recorded PGID until the Worker confirms release, including after its leader exits.
  for (const group of groups) {
    if (!alive(group.pgid)) continue
    try {
      process.kill(-group.pgid, "SIGKILL")
    } catch {}
  }
}

async function killOwned(groups: ReadonlyMap<number, Event>) {
  if (process.platform === "win32") return
  const deadline = Date.now() + 5_000
  while (groups.size > 0) {
    const active = [...groups.values()]
    signalOwned(active)
    if (!active.some((group) => groups.has(group.pgid) && alive(group.pgid))) return
    if (Date.now() >= deadline) throw new Error("Recorded TUI process groups survived the owner backstop")
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

export function makeWorkerTracker(emit: (event: Event) => void, ackTimeoutMs = 5_000): WorkerTracker {
  const identities = new Map<number, EventWriter.ProcessIdentity>()
  const pending = new Map<number, { event: Event; resolve: () => void }>()
  let eventID = 0

  return {
    async observe(event) {
      const identity = EventWriter.readIdentity(event.pgid) ?? identities.get(event.pgid) ?? {
        pid: event.pgid,
        bootID: null,
        startTicks: null,
      }
      identities.set(event.pgid, identity)
      const enriched: Event = { ...event, eventID: eventID++, identity }
      const acknowledgement = Promise.withResolvers<void>()
      pending.set(enriched.eventID, { event: enriched, resolve: acknowledgement.resolve })
      try {
        emit(enriched)
        await withTimeout(acknowledgement.promise, ackTimeoutMs)
      } catch (error) {
        pending.delete(enriched.eventID)
        if (enriched.action === "acquire") {
          const current = identities.get(enriched.pgid)
          if (current && sameIdentity(current, enriched.identity)) identities.delete(enriched.pgid)
        }
        throw error
      }
    },
    acknowledge(id) {
      const acknowledgement = pending.get(id)
      if (!acknowledgement) return false
      pending.delete(id)
      if (acknowledgement.event.action === "release") {
        const current = identities.get(acknowledgement.event.pgid)
        if (current && sameIdentity(current, acknowledgement.event.identity))
          identities.delete(acknowledgement.event.pgid)
      }
      acknowledgement.resolve()
      return true
    },
    get identityCount() {
      return identities.size
    },
  }
}

export function makeOwner(input: {
  client: OwnerClient
  worker: OwnerWorker
  shutdownTimeoutMs?: number
}): Owner {
  const groups = new Map<number, Event>()
  const unsubscribe = input.client.subscribe((event) => {
    if (event.action === "acquire") {
      groups.set(event.pgid, event)
    } else {
      const current = groups.get(event.pgid)
      if (current && sameIdentity(current.identity, event.identity)) groups.delete(event.pgid)
    }
    void input.client.acknowledge(event.eventID).catch(() => {})
  })
  const processExit = () => signalOwned(groups.values())
  process.once("exit", processExit)
  const previousErrorHandler = input.worker.onerror
  const workerError = (event: ErrorEvent) => {
    try {
      previousErrorHandler?.call(input.worker, event)
    } finally {
      signalOwned(groups.values())
    }
  }
  input.worker.onerror = workerError
  let stopped = false

  return {
    async stop() {
      if (stopped) return
      stopped = true
      const confirmed = await withTimeout(
        Promise.resolve().then(input.client.shutdown),
        input.shutdownTimeoutMs ?? 5_000,
      ).then(
        () => true,
        () => false,
      )
      if (!confirmed || groups.size > 0) await killOwned(groups).catch(() => signalOwned(groups.values()))
      try {
        input.worker.terminate()
      } finally {
        process.off("exit", processExit)
        if (input.worker.onerror === workerError) input.worker.onerror = previousErrorHandler
        unsubscribe()
      }
    },
    get: (pgid) => groups.get(pgid),
    has: (pgid) => groups.has(pgid),
    get size() {
      return groups.size
    },
  }
}

export * as ProcessGroup from "./process-group"
