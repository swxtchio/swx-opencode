import { EventWriter } from "@opencode-ai/core/event/writer"
import type { ProcessGroupEvent } from "@opencode-ai/core/cross-spawn-spawner"
import { execFileSync } from "node:child_process"
import { readFileSync } from "node:fs"
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

function sameIdentity(left: EventWriter.ProcessIdentity, right: EventWriter.ProcessIdentity) {
  return left.pid === right.pid && left.bootID === right.bootID && left.startTicks === right.startTicks
}

function hasIdentity(identity: EventWriter.ProcessIdentity) {
  return Boolean(identity.bootID && identity.startTicks)
}

function sameProcess(identity: EventWriter.ProcessIdentity) {
  if (!hasIdentity(identity)) return false
  const current = EventWriter.readIdentity(identity.pid)
  return current !== undefined && current.bootID === identity.bootID && current.startTicks === identity.startTicks
}

function isGroupMember(pgid: number, identity: EventWriter.ProcessIdentity) {
  if (!sameProcess(identity)) return false
  try {
    const stat = readFileSync(`/proc/${identity.pid}/stat`, "utf8")
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ")
    const current = EventWriter.readIdentity(identity.pid)
    return (
      current !== undefined &&
      current.bootID === identity.bootID &&
      current.startTicks === identity.startTicks &&
      Number(fields[2]) === pgid
    )
  } catch {
    return false
  }
}

function readGroupMembers(pgid: number, leaderIdentity: EventWriter.ProcessIdentity) {
  if (process.platform !== "linux" || !alive(pgid)) return []
  try {
    const currentLeader = EventWriter.readIdentity(pgid)
    if (currentLeader && hasIdentity(currentLeader) && !sameIdentity(currentLeader, leaderIdentity)) return []
    return execFileSync("ps", ["-e", "-o", "pid=,pgid="], { encoding: "utf8", timeout: 250, maxBuffer: 4_194_304 })
      .split("\n")
      .flatMap((line) => {
        const [pid, group] = line.trim().split(/\s+/).map(Number)
        if (!pid || group !== pgid) return []
        const identity = EventWriter.readIdentity(pid)
        return identity && hasIdentity(identity) && isGroupMember(pgid, identity) ? [identity] : []
      })
  } catch {
    return []
  }
}

function currentMembers(group: Event) {
  const recorded = [group.identity, ...group.members].filter((identity) => isGroupMember(group.pgid, identity))
  return recorded.length ? recorded : readGroupMembers(group.pgid, group.identity)
}

function alive(pgid: number) {
  try {
    process.kill(-pgid, 0)
    return true
  } catch (error) {
    return !(error instanceof Error && "code" in error && error.code === "ESRCH")
  }
}

function ownsLiveMember(group: Event) {
  return currentMembers(group).length > 0
}

function signalOwned(groups: Iterable<Event>) {
  if (process.platform === "win32") return
  // A PGID is signalable only while one of its recorded member identities still names a current group member.
  for (const group of groups) {
    if (!ownsLiveMember(group)) continue
    if (!alive(group.pgid)) continue
    try {
      process.kill(-group.pgid, "SIGKILL")
    } catch {}
  }
}

async function killOwned(groups: Map<number, Event>) {
  if (process.platform === "win32") return
  const deadline = Date.now() + 5_000
  while (groups.size > 0) {
    const active = [...groups.values()].flatMap((group) => {
      const members = currentMembers(group)
      if (!members.length) {
        groups.delete(group.pgid)
        return []
      }
      const recorded = new Map<number, EventWriter.ProcessIdentity>()
      group.members.forEach((member) => recorded.set(member.pid, member))
      members.forEach((member) => recorded.set(member.pid, member))
      const updated = { ...group, members: [...recorded.values()] }
      groups.set(group.pgid, updated)
      return [updated]
    })
    if (!active.length) return
    signalOwned(active)
    if (!active.some((group) => groups.has(group.pgid) && ownsLiveMember(group) && alive(group.pgid))) return
    if (Date.now() >= deadline) throw new Error("Recorded TUI process groups survived the owner backstop")
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

// Detached spawns wait for the parent to record ownership; this timeout fails spawn rather than expose an unowned group.
export function makeWorkerTracker(emit: (event: Event) => void, ackTimeoutMs = 5_000): WorkerTracker {
  const identities = new Map<number, EventWriter.ProcessIdentity>()
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
      const members = reuseReleasedMembers
        ? (released?.members ?? []).filter((member) => isGroupMember(event.pgid, member))
        : event.action === "release" || (event.cleanupOwed && !sameProcess(identity))
          ? readGroupMembers(event.pgid, identity)
          : []
      if (event.action === "acquire") {
        identities.set(event.pgid, identity)
        releasedMembers.delete(event.pgid)
      } else {
        if (previous && sameIdentity(previous, identity)) identities.delete(event.pgid)
        releasedMembers.set(event.pgid, { identity, members })
      }
      const enriched: Event = {
        ...event,
        eventID: eventID++,
        identity,
        members,
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
