import { EventWriter } from "@opencode-ai/core/event/writer"
import type { ProcessGroupEvent } from "@opencode-ai/core/cross-spawn-spawner"
import {
  hasIdentity,
  isAlive,
  isGroupMember,
  readGroupMembers,
  sameIdentity,
  signalGroup,
} from "@opencode-ai/core/process-group"
import { withTimeout } from "@/util/timeout"

export type Event = ProcessGroupEvent & {
  readonly eventID: number
  readonly members: ReadonlyArray<EventWriter.ProcessIdentity>
}

export type WorkerTracker = {
  observe(event: ProcessGroupEvent): Promise<void>
  acknowledge(eventID: number): boolean
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

function signalOwned(groups: Iterable<Event>) {
  for (const group of groups) {
    signalGroup(group.pgid, [group.identity, ...group.members], "SIGKILL")
  }
}

async function killOwned(groups: Map<number, Event>) {
  if (process.platform === "win32") return
  const deadline = Date.now() + 5_000
  while (groups.size > 0) {
    const active = [...groups.values()].flatMap((group) => {
      if (!isAlive(group.pgid)) {
        groups.delete(group.pgid)
        return []
      }
      return [group]
    })
    if (!active.length) return
    signalOwned(active)
    if (!active.some((group) => groups.has(group.pgid) && isAlive(group.pgid))) return
    if (Date.now() >= deadline) throw new Error("Recorded TUI process groups survived the owner backstop")
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

// Detached spawns wait for the parent to record ownership; this timeout fails spawn rather than expose an unowned group.
export function makeWorkerTracker(emit: (event: Event) => void, ackTimeoutMs = 5_000): WorkerTracker {
  const identities = new Map<number, EventWriter.ProcessIdentity>()
  const activeMembers = new Map<number, Map<number, EventWriter.ProcessIdentity>>()
  // A released handle can be reacquired before scope close; keep its member identities to reject a reused PGID.
  const releasedMembers = new Map<number, { identity: EventWriter.ProcessIdentity; members: ReadonlyArray<EventWriter.ProcessIdentity> }>()
  const pending = new Map<number, { resolve: () => void }>()
  let eventID = 0

  return {
    async observe(event) {
      const previous = identities.get(event.pgid)
      const released = releasedMembers.get(event.pgid)
      const identity = hasIdentity(event.identity) ? event.identity : previous ?? released?.identity ?? event.identity
      const reuseReleasedMembers = event.action === "acquire" && released && sameIdentity(released.identity, identity)
      const knownMembers = new Map<number, EventWriter.ProcessIdentity>()
      if (reuseReleasedMembers) released.members.forEach((member) => knownMembers.set(member.pid, member))
      activeMembers.get(event.pgid)?.forEach((member) => knownMembers.set(member.pid, member))
      const currentMembers = new Map(
        [...knownMembers.values()]
          .filter((member) => isGroupMember(event.pgid, member))
          .map((member) => [member.pid, member] as const),
      )
      const discoverMembers = reuseReleasedMembers || (event.action === "acquire" && event.cleanupOwed)
      if (discoverMembers) {
        const discovered = await readGroupMembers(event.pgid, identity)
        discovered.forEach((member) => currentMembers.set(member.pid, member))
      }
      const current = [...currentMembers.values()]
      if (event.action === "acquire") {
        identities.set(event.pgid, identity)
        releasedMembers.delete(event.pgid)
        activeMembers.set(event.pgid, new Map(current.map((member) => [member.pid, member])))
      } else {
        if (previous && sameIdentity(previous, identity)) identities.delete(event.pgid)
        activeMembers.delete(event.pgid)
        releasedMembers.set(event.pgid, { identity, members: current })
      }
      const enriched: Event = {
        ...event,
        eventID: eventID++,
        identity,
        members: current,
      }
      const acknowledgement = Promise.withResolvers<void>()
      pending.set(enriched.eventID, { resolve: acknowledgement.resolve })
      try {
        emit(enriched)
        await withTimeout(acknowledgement.promise, ackTimeoutMs)
      } catch (error) {
        pending.delete(enriched.eventID)
        throw error
      }
    },
    acknowledge(id) {
      const acknowledgement = pending.get(id)
      if (!acknowledgement) return false
      pending.delete(id)
      acknowledgement.resolve()
      return true
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
      const current = groups.get(event.pgid)
      const members = new Map<number, EventWriter.ProcessIdentity>()
      current?.members.forEach((member) => members.set(member.pid, member))
      event.members.forEach((member) => members.set(member.pid, member))
      groups.set(event.pgid, {
        ...event,
        identity:
          current && hasIdentity(current.identity) && !hasIdentity(event.identity) ? current.identity : event.identity,
        members: [...members.values()],
      })
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
