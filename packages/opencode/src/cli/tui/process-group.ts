import { EventWriter } from "@opencode-ai/core/event/writer"
import type { ProcessGroupEvent } from "@opencode-ai/core/cross-spawn-spawner"
import {
  decideGroupOwnership,
  hasIdentity,
  isAlive,
  isGroupMember,
  isLiveProcess,
  readGroupMembers,
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
  readonly membersUnavailable: boolean
  readonly generation: number
  readonly ownershipSequence: number
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
    membersUnavailable: group.membersUnavailable,
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
  releasedMemberLifetimeMs = 5_000,
): WorkerTracker {
  const groups = new Map<number, Event>()
  const releasedMembers = new Map<
    number,
    {
      identity: EventWriter.ProcessIdentity
      members: ReadonlyArray<EventWriter.ProcessIdentity>
      membersUnavailable: boolean
      expiresAt: number
    }
  >()
  const releasedTimers = new Map<number, ReturnType<typeof setTimeout>>()
  const pendingSnapshots = new Map<number, number>()
  const latestSnapshots = new Map<number, number>()
  const pending = new Map<number, { resolve: () => void }>()
  let eventID = 0
  let ownershipSequence = 0

  const retireReleasedMembers = (pgid: number) => {
    const timer = releasedTimers.get(pgid)
    if (timer) clearTimeout(timer)
    releasedTimers.delete(pgid)
    releasedMembers.delete(pgid)
  }

  const pruneReleasedMembers = () =>
    releasedMembers.forEach((entry, pgid) => {
      if (Date.now() >= entry.expiresAt || !isAlive(pgid)) retireReleasedMembers(pgid)
    })

  const rememberReleasedMembers = (
    pgid: number,
    identity: EventWriter.ProcessIdentity,
    members: ReadonlyArray<EventWriter.ProcessIdentity>,
    membersUnavailable: boolean,
    decision: ReturnType<typeof decideGroupOwnership>,
  ) => {
    if (decision.action === "prune" || (!members.length && decision.evidence !== "unavailable")) {
      retireReleasedMembers(pgid)
      return
    }
    retireReleasedMembers(pgid)
    const entry = { identity, members, membersUnavailable, expiresAt: Date.now() + releasedMemberLifetimeMs }
    releasedMembers.set(pgid, entry)
    const timer = setTimeout(() => {
      if (releasedMembers.get(pgid) === entry) retireReleasedMembers(pgid)
    }, releasedMemberLifetimeMs)
    timer.unref?.()
    releasedTimers.set(pgid, timer)
  }

  const finishSnapshot = (pgid: number) => {
    const count = (pendingSnapshots.get(pgid) ?? 1) - 1
    if (count > 0) {
      pendingSnapshots.set(pgid, count)
      return
    }
    pendingSnapshots.delete(pgid)
    latestSnapshots.delete(pgid)
  }

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

  const observe = (event: ProcessGroupEvent) => {
    pruneReleasedMembers()
    const previous = groups.get(event.pgid)
    const released = releasedMembers.get(event.pgid)
    const identity = hasIdentity(event.identity)
      ? event.identity
      : previous?.identity ?? released?.identity ?? event.identity
    const reuseReleasedMembers =
      event.action === "acquire" && released !== undefined && sameIdentity(released.identity, identity)
    if (released && event.action === "acquire" && !reuseReleasedMembers) retireReleasedMembers(event.pgid)
    const sequence = ++ownershipSequence
    if (pendingSnapshots.has(event.pgid)) latestSnapshots.set(event.pgid, sequence)
    const members = new Map<number, EventWriter.ProcessIdentity>()
    if (previous && sameIdentity(previous.identity, identity))
      previous.members.forEach((member) => members.set(member.pid, member))
    if (reuseReleasedMembers) released.members.forEach((member) => members.set(member.pid, member))
    event.members?.forEach((member) => members.set(member.pid, member))
    const currentMembers = [...members.values()].filter((member) => isGroupMember(event.pgid, member))
    const generation = event.generation ?? previous?.generation ?? 0
    const membersSnapshotComplete = event.membersSnapshotComplete ?? false
    const membersUnavailable = event.membersUnavailable ?? released?.membersUnavailable ?? false
    const membersPending =
      event.membersPending ??
      (event.cleanupOwed && !membersSnapshotComplete && !membersUnavailable && hasIdentity(identity) && !isLiveProcess(identity))
    const decision = decideGroupOwnership({
      pgid: event.pgid,
      identities: [identity, ...currentMembers],
      cleanupOwed: event.cleanupOwed,
      membersPending,
      membersUnavailable,
    })
    const enriched: Event = {
      ...event,
      eventID: eventID++,
      identity,
      members: currentMembers,
      membersPending,
      membersSnapshotComplete,
      membersUnavailable,
      generation,
      ownershipSequence: sequence,
    }
    if (event.action === "acquire") {
      retireReleasedMembers(event.pgid)
      if (decision.action === "signal") groups.set(event.pgid, enriched)
      else groups.delete(event.pgid)
    }
    const releaseMatches = !previous || sameIdentity(previous.identity, identity)
    if (event.action === "release") {
      const currentDecision = previous
        ? decideGroupOwnership({
            pgid: event.pgid,
            identities: [previous.identity, ...previous.members],
            cleanupOwed: previous.cleanupOwed,
            membersPending: previous.membersPending,
            membersUnavailable: previous.membersUnavailable,
          })
        : decision
      if (releaseMatches || currentDecision.action === "prune") groups.delete(event.pgid)
      if (releaseMatches && released && sameIdentity(released.identity, identity)) retireReleasedMembers(event.pgid)
    }
    const needsSnapshot =
      (event.action === "acquire" &&
        decision.action === "signal" &&
        event.cleanupOwed &&
        !membersSnapshotComplete &&
        !membersUnavailable) ||
      (event.action === "release" &&
        releaseMatches &&
        !membersSnapshotComplete &&
        !membersUnavailable &&
        isAlive(event.pgid))
    if (needsSnapshot) {
      pendingSnapshots.set(event.pgid, (pendingSnapshots.get(event.pgid) ?? 0) + 1)
      latestSnapshots.set(event.pgid, sequence)
    }
    if (event.action === "release" && !needsSnapshot && releaseMatches) {
      rememberReleasedMembers(
        event.pgid,
        identity,
        currentMembers,
        membersUnavailable,
        decideGroupOwnership({
          pgid: event.pgid,
          identities: [identity, ...currentMembers],
          cleanupOwed: false,
          membersPending: false,
          membersUnavailable,
        }),
      )
    }
    const acknowledgement = publish(enriched)
    if (!needsSnapshot) {
      if (!pendingSnapshots.has(event.pgid)) latestSnapshots.delete(event.pgid)
      return acknowledgement
    }

    void readMembers(event.pgid, identity)
      .catch(() => ({ members: [], complete: false }))
      .then((discovery) => {
        if (latestSnapshots.get(event.pgid) !== sequence) return
        const current = groups.get(event.pgid)
        if (event.action === "acquire" && current?.ownershipSequence !== sequence) return
        if (event.action === "release" && current) return
        const resolvedMembers = new Map(currentMembers.map((member) => [member.pid, member] as const))
        discovery.members.forEach((member) => resolvedMembers.set(member.pid, member))
        const nextMembers = [...resolvedMembers.values()].filter((member) => isGroupMember(event.pgid, member))
        const nextMembersUnavailable = !discovery.complete
        if (event.action === "release") {
          const releasedDecision = decideGroupOwnership({
            pgid: event.pgid,
            identities: [identity, ...nextMembers],
            cleanupOwed: false,
            membersPending: false,
            membersUnavailable: nextMembersUnavailable,
          })
          rememberReleasedMembers(event.pgid, identity, nextMembers, nextMembersUnavailable, releasedDecision)
          return
        }
        const nextDecision = decideGroupOwnership({
          pgid: event.pgid,
          identities: [identity, ...nextMembers],
          cleanupOwed: event.cleanupOwed,
          membersPending: false,
          membersUnavailable: nextMembersUnavailable,
        })
        const resolvedEvent: Event = {
          ...enriched,
          action: nextDecision.action === "prune" ? "release" : "acquire",
          eventID: eventID++,
          members: nextDecision.action === "prune" ? [] : nextMembers,
          membersPending: false,
          membersSnapshotComplete: discovery.complete,
          membersUnavailable: nextMembersUnavailable,
        }
        if (nextDecision.action === "signal") groups.set(event.pgid, resolvedEvent)
        if (nextDecision.action === "prune") {
          groups.delete(event.pgid)
          retireReleasedMembers(event.pgid)
        }
        return publish(resolvedEvent)
      })
      .catch(() => {})
      .finally(() => finishSnapshot(event.pgid))
    return acknowledgement
  }

  return {
    observe,
    acknowledge(id) {
      const acknowledgement = pending.get(id)
      if (!acknowledgement) return false
      pending.delete(id)
      acknowledgement.resolve()
      return true
    },
    get size() {
      pruneReleasedMembers()
      return new Set([...groups.keys(), ...releasedMembers.keys(), ...pendingSnapshots.keys()]).size
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
    if (current && event.ownershipSequence < current.ownershipSequence) {
      void input.client.acknowledge(event.eventID).catch(() => {})
      return
    }
    const identity = current && hasIdentity(current.identity) && !hasIdentity(event.identity) ? current.identity : event.identity
    const members = new Map<number, EventWriter.ProcessIdentity>()
    current?.members.forEach((member) => members.set(member.pid, member))
    event.members.forEach((member) => members.set(member.pid, member))
    const membersSnapshotComplete = event.membersSnapshotComplete ?? current?.membersSnapshotComplete ?? false
    const membersUnavailable = event.membersUnavailable ?? current?.membersUnavailable ?? false
    const next: Event = {
      ...event,
      identity,
      members: [...members.values()],
      membersPending:
        event.membersPending ??
        (event.cleanupOwed && !membersSnapshotComplete && !membersUnavailable && hasIdentity(identity) && !isLiveProcess(identity)),
      membersSnapshotComplete,
      membersUnavailable,
      generation: event.generation ?? current?.generation ?? 0,
    }
    const decision = decideGroupOwnership({
      pgid: event.pgid,
      identities: [next.identity, ...next.members],
      cleanupOwed: next.cleanupOwed,
      membersPending: next.membersPending,
      membersUnavailable: next.membersUnavailable,
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
          membersUnavailable: current.membersUnavailable,
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
