import { EventWriter } from "@opencode-ai/core/event/writer"
import type { ProcessGroupEvent } from "@opencode-ai/core/cross-spawn-spawner"
import {
  decideGroupOwnership,
  hasIdentity,
  isGroupMember,
  readGroupMembers,
  sameProcess,
  sameIdentity,
  signalGroup,
} from "@opencode-ai/core/process-group"
import type { IdentityReader } from "@opencode-ai/core/process-group"
import { withTimeout } from "@/util/timeout"

export type Event = ProcessGroupEvent & {
  readonly eventID: number
  readonly members: ReadonlyArray<EventWriter.ProcessIdentity>
  readonly membersPending: boolean
  readonly membersSnapshotComplete: boolean
  readonly generation: number
}

export type WorkerTracker = {
  observe(event: ProcessGroupEvent): Promise<void>
  acknowledge(eventID: number): boolean
  readonly size: number
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

function groupOwnershipInput(group: Event, readIdentity: IdentityReader) {
  return {
    pgid: group.pgid,
    identities: [group.identity, ...group.members],
    cleanupOwed: group.cleanupOwed,
    membersPending: group.membersPending,
    readIdentity,
  }
}

function signalOwned(groups: Map<number, Event>, readIdentity: IdentityReader) {
  for (const group of [...groups.values()]) {
    const decision = signalGroup(groupOwnershipInput(group, readIdentity), "SIGKILL")
    if (decision.action === "prune") groups.delete(group.pgid)
  }
}

async function killOwned(groups: Map<number, Event>, readIdentity: IdentityReader) {
  if (process.platform === "win32") return
  const deadline = Date.now() + 5_000
  while (groups.size > 0) {
    signalOwned(groups, readIdentity)
    if (!groups.size) return
    if (Date.now() >= deadline) throw new Error("Recorded TUI process groups survived the owner backstop")
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

// Detached spawns wait for the parent to record ownership; this timeout fails spawn rather than expose an unowned group.
export function makeWorkerTracker(
  emit: (event: Event) => void,
  ackTimeoutMs = 5_000,
  readMembers = readGroupMembers,
): WorkerTracker {
  const groups = new Map<number, Event>()
  const queues = new Map<number, Promise<void>>()
  const pending = new Map<number, { resolve: () => void }>()
  let eventID = 0

  const publish = async (event: Event) => {
    const acknowledgement = Promise.withResolvers<void>()
    pending.set(event.eventID, { resolve: acknowledgement.resolve })
    try {
      emit(event)
      await withTimeout(acknowledgement.promise, ackTimeoutMs)
    } catch (error) {
      pending.delete(event.eventID)
      throw error
    }
  }

  const observeOrdered = async (event: ProcessGroupEvent) => {
    const previous = groups.get(event.pgid)
    const identity = hasIdentity(event.identity) ? event.identity : previous?.identity ?? event.identity
    const members = new Map<number, EventWriter.ProcessIdentity>()
    if (previous && sameIdentity(previous.identity, identity))
      previous.members.forEach((member) => members.set(member.pid, member))
    event.members?.forEach((member) => members.set(member.pid, member))
    const currentMembers = [...members.values()].filter((member) => isGroupMember(event.pgid, member))
    const generation = event.generation ?? previous?.generation ?? 0
    const membersSnapshotComplete = event.membersSnapshotComplete ?? false
    const membersPending =
      event.membersPending ??
      (event.cleanupOwed && !membersSnapshotComplete && hasIdentity(identity) && !sameProcess(identity))
    const decision = decideGroupOwnership({
      pgid: event.pgid,
      identities: [identity, ...currentMembers],
      cleanupOwed: event.cleanupOwed,
      membersPending,
    })
    if (event.action === "acquire" && decision.action === "signal") {
      groups.set(event.pgid, {
        ...event,
        eventID: -1,
        identity,
        members: currentMembers,
        membersPending,
        membersSnapshotComplete,
        generation,
      })
    }
    if (event.action === "release") {
      const currentDecision = previous
        ? decideGroupOwnership({
            pgid: event.pgid,
            identities: [previous.identity, ...previous.members],
            cleanupOwed: previous.cleanupOwed,
            membersPending: previous.membersPending,
          })
        : decision
      if ((previous && sameIdentity(previous.identity, identity)) || currentDecision.action === "prune")
        groups.delete(event.pgid)
    }
    if (decision.action === "prune" && event.action === "acquire") groups.delete(event.pgid)
    const needsSnapshot = event.action === "acquire" && event.cleanupOwed && !membersSnapshotComplete
    const enriched: Event = {
      ...event,
      eventID: eventID++,
      identity,
      members: currentMembers,
      membersPending,
      membersSnapshotComplete,
      generation,
    }
    if (!needsSnapshot) {
      await publish(enriched)
      return
    }

    const initialAck = publish(enriched).then(
      () => ({ ok: true as const }),
      (error) => ({ ok: false as const, error }),
    )
    const snapshot = readMembers(event.pgid, identity).then(
      (found) => ({ ok: true as const, found }),
      (error) => ({ ok: false as const, error }),
    )
    const [ack, discovery] = await Promise.all([initialAck, snapshot])
    if (!discovery.ok) {
      if (!ack.ok) throw ack.error
      return
    }

    const resolvedMembers = new Map(currentMembers.map((member) => [member.pid, member] as const))
    discovery.found.members.forEach((member) => resolvedMembers.set(member.pid, member))
    const nextMembers = [...resolvedMembers.values()]
    const nextMembersPending = membersPending && !discovery.found.complete
    const nextDecision = decideGroupOwnership({
      pgid: event.pgid,
      identities: [identity, ...nextMembers],
      cleanupOwed: event.cleanupOwed,
      membersPending: nextMembersPending,
    })
    const resolvedEvent: Event = {
      ...enriched,
      action: nextDecision.action === "prune" ? "release" : "acquire",
      eventID: eventID++,
      members: nextDecision.action === "prune" ? [] : nextMembers,
      membersPending: nextMembersPending,
      membersSnapshotComplete: discovery.found.complete,
    }
    if (nextDecision.action === "signal")
      groups.set(event.pgid, { ...resolvedEvent, action: "acquire" })
    if (nextDecision.action === "prune") groups.delete(event.pgid)
    const resolvedAck = publish(resolvedEvent).then(
      () => ({ ok: true as const }),
      (error) => ({ ok: false as const, error }),
    )
    const result = await resolvedAck
    if (!ack.ok) throw ack.error
    if (!result.ok) throw result.error
  }

  return {
    observe(event) {
      const previous = queues.get(event.pgid)
      const current = previous ? previous.then(() => observeOrdered(event)) : observeOrdered(event)
      const tail = current.then(
        () => undefined,
        () => undefined,
      )
      queues.set(event.pgid, tail)
      void tail.then(() => {
        if (queues.get(event.pgid) === tail) queues.delete(event.pgid)
      })
      return current
    },
    acknowledge(id) {
      const acknowledgement = pending.get(id)
      if (!acknowledgement) return false
      pending.delete(id)
      acknowledgement.resolve()
      return true
    },
    get size() {
      return groups.size
    },
  }
}

export function makeOwner(input: {
  client: OwnerClient
  worker: OwnerWorker
  shutdownTimeoutMs?: number
  readIdentity?: IdentityReader
}): Owner {
  const groups = new Map<number, Event>()
  const readIdentity = input.readIdentity ?? EventWriter.readIdentity
  const unsubscribe = input.client.subscribe((event) => {
    const current = groups.get(event.pgid)
    const identity = current && hasIdentity(current.identity) && !hasIdentity(event.identity) ? current.identity : event.identity
    const members = new Map<number, EventWriter.ProcessIdentity>()
    current?.members.forEach((member) => members.set(member.pid, member))
    event.members.forEach((member) => members.set(member.pid, member))
    const membersSnapshotComplete = event.membersSnapshotComplete ?? current?.membersSnapshotComplete ?? false
    const next: Event = {
      ...event,
      identity,
      members: [...members.values()],
      membersPending:
        event.membersPending ??
        (event.cleanupOwed && !membersSnapshotComplete && hasIdentity(identity) && !sameProcess(identity)),
      membersSnapshotComplete,
      generation: event.generation ?? current?.generation ?? 0,
    }
    const decision = decideGroupOwnership({
      pgid: event.pgid,
      identities: [next.identity, ...next.members],
      cleanupOwed: next.cleanupOwed,
      membersPending: next.membersPending,
      readIdentity,
    })
    if (event.action === "acquire") {
      if (decision.action === "signal") groups.set(event.pgid, next)
      else groups.delete(event.pgid)
    } else {
      if (current && sameIdentity(current.identity, identity)) {
        groups.delete(event.pgid)
      } else if (
        current &&
        decideGroupOwnership({
          pgid: event.pgid,
          identities: [current.identity, ...current.members],
          cleanupOwed: current.cleanupOwed,
          membersPending: current.membersPending,
          readIdentity,
        }).action === "prune"
      ) {
        groups.delete(event.pgid)
      }
    }
    void input.client.acknowledge(event.eventID).catch(() => {})
  })
  const processExit = () => signalOwned(groups, readIdentity)
  process.once("exit", processExit)
  const previousErrorHandler = input.worker.onerror
  const workerError = (event: ErrorEvent) => {
    try {
      previousErrorHandler?.call(input.worker, event)
    } finally {
      signalOwned(groups, readIdentity)
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
      if (!confirmed || groups.size > 0) await killOwned(groups, readIdentity).catch(() => signalOwned(groups, readIdentity))
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
