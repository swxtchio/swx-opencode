export * as ProcessGroup from "./process-group"

import { readFileSync } from "node:fs"
import { readFile, readdir } from "node:fs/promises"
import { EventWriter } from "./event/writer"

export type IdentityReader = (pid: number) => EventWriter.ProcessIdentity | undefined

export function hasIdentity(identity: EventWriter.ProcessIdentity | undefined): identity is EventWriter.ProcessIdentity {
  return Boolean(identity?.bootID && identity.startTicks)
}

export function sameIdentity(left: EventWriter.ProcessIdentity, right: EventWriter.ProcessIdentity | undefined) {
  return (
    right !== undefined &&
    left.pid === right.pid &&
    left.bootID === right.bootID &&
    left.startTicks === right.startTicks
  )
}

export function sameProcess(identity: EventWriter.ProcessIdentity, readIdentity = EventWriter.readIdentity) {
  if (!hasIdentity(identity)) return false
  return sameIdentity(identity, readIdentity(identity.pid))
}

export function processGroupIDFromStat(stat: string) {
  const value = Number(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[2])
  return Number.isSafeInteger(value) && value > 0 ? value : undefined
}

export function processParentIDFromStat(stat: string) {
  const value = Number(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[1])
  return Number.isSafeInteger(value) && value > 0 ? value : undefined
}

export function isLiveProcessStat(stat: string) {
  const state = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]
  return state !== "Z" && state !== "X" && state !== "x"
}

export function processGroupID(pid: number) {
  if (process.platform !== "linux") return
  try {
    return processGroupIDFromStat(readFileSync(`/proc/${pid}/stat`, "utf8"))
  } catch {
    return
  }
}

export function processParentID(pid: number) {
  if (process.platform !== "linux") return
  try {
    return processParentIDFromStat(readFileSync(`/proc/${pid}/stat`, "utf8"))
  } catch {
    return
  }
}

export function isGroupMember(
  pgid: number,
  identity: EventWriter.ProcessIdentity | undefined,
  readIdentity = EventWriter.readIdentity,
) {
  if (!hasIdentity(identity) || !sameIdentity(identity, readIdentity(identity.pid))) return false
  if (process.platform !== "linux") return false
  try {
    const stat = readFileSync(`/proc/${identity.pid}/stat`, "utf8")
    return isLiveProcessStat(stat) && processGroupIDFromStat(stat) === pgid
  } catch {
    return false
  }
}

export function isAlive(pgid: number) {
  if (process.platform === "win32") return false
  try {
    process.kill(-pgid, 0)
    return true
  } catch (error) {
    return !(error instanceof Error && "code" in error && error.code === "ESRCH")
  }
}

export type GroupOwnershipEvidence = "confirmed" | "pending" | "unavailable" | "refuted" | "gone"
export type GroupOwnershipDecision =
  | { readonly action: "signal"; readonly evidence: "confirmed" | "pending" | "unavailable" }
  | { readonly action: "prune"; readonly evidence: "refuted" | "gone" }
export type GroupOwnershipInput = {
  readonly pgid: number
  readonly identities: Iterable<EventWriter.ProcessIdentity>
  readonly cleanupOwed?: boolean
  readonly membersPending?: boolean
  readonly readIdentity?: IdentityReader
}
export type GroupMembersSnapshot = {
  readonly members: ReadonlyArray<EventWriter.ProcessIdentity>
  readonly complete: boolean
}

export function decideGroupOwnership(input: GroupOwnershipInput): GroupOwnershipDecision {
  if (!isAlive(input.pgid)) return { action: "prune", evidence: "gone" }
  const readIdentity = input.readIdentity ?? EventWriter.readIdentity
  const confirmed = Array.from(input.identities).some(
    (identity) => hasIdentity(identity) && isGroupMember(input.pgid, identity, readIdentity),
  )
  if (confirmed) return { action: "signal", evidence: "confirmed" }
  if (input.cleanupOwed && input.membersPending) return { action: "signal", evidence: "pending" }
  if (!hasIdentity(readIdentity(process.pid))) return { action: "signal", evidence: "unavailable" }
  return { action: "prune", evidence: "refuted" }
}

export function signalGroup(input: GroupOwnershipInput, signal: NodeJS.Signals) {
  const decision = decideGroupOwnership(input)
  if (decision.action === "signal") {
    try {
      process.kill(-input.pgid, signal)
    } catch {}
  }
  return decision
}

export async function readGroupMembers(pgid: number, leaderIdentity: EventWriter.ProcessIdentity): Promise<GroupMembersSnapshot> {
  if (process.platform !== "linux") return { members: [], complete: false }
  if (!isAlive(pgid)) return { members: [], complete: true }
  const currentLeader = EventWriter.readIdentity(pgid)
  if (
    hasIdentity(currentLeader) &&
    hasIdentity(leaderIdentity) &&
    !sameIdentity(currentLeader, leaderIdentity) &&
    processGroupID(pgid) === pgid
  )
    return { members: [], complete: true }
  const entries = await readdir("/proc").catch(() => undefined)
  if (!entries) return { members: [], complete: false }
  let complete = true
  const identities = await Promise.all(
    entries.flatMap((entry) => {
      if (!/^\d+$/.test(entry)) return []
      const pid = Number(entry)
      return [
        (async () => {
          const stat = await readFile(`/proc/${pid}/stat`, "utf8").catch((error) => {
            if (error instanceof Error && "code" in error && (error.code === "EACCES" || error.code === "EPERM"))
              complete = false
            return undefined
          })
          if (!stat || !isLiveProcessStat(stat) || processGroupIDFromStat(stat) !== pgid) return
          const identity = EventWriter.readIdentity(pid)
          if (!hasIdentity(identity)) {
            const currentStat = await readFile(`/proc/${pid}/stat`, "utf8").catch(() => undefined)
            if (currentStat && isLiveProcessStat(currentStat) && processGroupIDFromStat(currentStat) === pgid)
              complete = false
            return
          }
          const currentStat = await readFile(`/proc/${pid}/stat`, "utf8").catch((error) => {
            if (error instanceof Error && "code" in error && (error.code === "EACCES" || error.code === "EPERM"))
              complete = false
            return undefined
          })
          if (!currentStat || !isLiveProcessStat(currentStat) || processGroupIDFromStat(currentStat) !== pgid) return
          const currentIdentity = EventWriter.readIdentity(pid)
          if (!sameIdentity(identity, currentIdentity)) {
            complete = false
            return
          }
          return identity
        })(),
      ]
    }),
  )
  return {
    members: identities.filter((identity): identity is EventWriter.ProcessIdentity => identity !== undefined),
    complete,
  }
}
