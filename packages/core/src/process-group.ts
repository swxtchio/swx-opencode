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
  return processGroupID(identity.pid) === pgid
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

export function canSignalGroup(
  pgid: number,
  identities: Iterable<EventWriter.ProcessIdentity>,
  readIdentity = EventWriter.readIdentity,
) {
  if (!isAlive(pgid)) return false
  const recorded = Array.from(identities)
  const readableRecorded = recorded.filter(hasIdentity)
  if (readableRecorded.length === 0) return true
  let identityUnavailable = false
  for (const identity of readableRecorded) {
    const current = readIdentity(identity.pid)
    if (!hasIdentity(current)) {
      identityUnavailable = true
      continue
    }
    if (!sameIdentity(identity, current)) {
      identityUnavailable = true
      continue
    }
    const currentGroup = processGroupID(identity.pid)
    if (currentGroup === pgid) return true
    if (currentGroup === undefined) identityUnavailable = true
  }
  // A live PGID cannot be reused while one of its process-group members remains alive.
  return identityUnavailable && !hasIdentity(readIdentity(process.pid))
}

export function signalGroup(
  pgid: number,
  identities: Iterable<EventWriter.ProcessIdentity>,
  signal: NodeJS.Signals,
  readIdentity = EventWriter.readIdentity,
) {
  if (!canSignalGroup(pgid, identities, readIdentity)) return false
  try {
    process.kill(-pgid, signal)
    return true
  } catch {
    return false
  }
}

export async function readGroupMembers(pgid: number, leaderIdentity: EventWriter.ProcessIdentity) {
  if (process.platform !== "linux" || !isAlive(pgid)) return []
  const currentLeader = EventWriter.readIdentity(pgid)
  if (
    hasIdentity(currentLeader) &&
    hasIdentity(leaderIdentity) &&
    !sameIdentity(currentLeader, leaderIdentity) &&
    processGroupID(pgid) === pgid
  )
    return []
  const entries = await readdir("/proc").catch(() => [])
  const identities = await Promise.all(
    entries.flatMap((entry) => {
      if (!/^\d+$/.test(entry)) return []
      const pid = Number(entry)
      return [
        (async () => {
          const stat = await readFile(`/proc/${pid}/stat`, "utf8").catch(() => undefined)
          if (!stat || processGroupIDFromStat(stat) !== pgid) return
          const identity = EventWriter.readIdentity(pid)
          if (!hasIdentity(identity)) return
          const currentStat = await readFile(`/proc/${pid}/stat`, "utf8").catch(() => undefined)
          if (!currentStat || processGroupIDFromStat(currentStat) !== pgid) return
          const currentIdentity = EventWriter.readIdentity(pid)
          if (!sameIdentity(identity, currentIdentity)) return
          return identity
        })(),
      ]
    }),
  )
  return identities.filter((identity): identity is EventWriter.ProcessIdentity => identity !== undefined)
}
