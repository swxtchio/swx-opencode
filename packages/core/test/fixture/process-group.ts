import { EventWriter } from "@opencode-ai/core/event/writer"
import { ProcessGroup } from "@opencode-ai/core/process-group"
import { readFileSync, readdirSync } from "node:fs"

export type ProcessIdentity = EventWriter.ProcessIdentity
export type ProcessGroupFixture = {
  readonly leader: EventWriter.ProcessIdentity
  readonly descendant: EventWriter.ProcessIdentity
  readonly pgid: number
}

export function processIdentity(value: unknown): EventWriter.ProcessIdentity | undefined {
  if (typeof value === "number" && Number.isSafeInteger(value) && value > 0) {
    const identity = EventWriter.readIdentity(value)
    if (identity?.bootID && identity.startTicks) return identity
    const started = Bun.spawnSync(["ps", "-p", String(value), "-o", "lstart="]).stdout.toString().trim()
    return started ? { pid: value, bootID: "ps", startTicks: started } : undefined
  }
  if (typeof value !== "object" || value === null || !("pid" in value) || typeof value.pid !== "number") return
  if (!("bootID" in value) || !("startTicks" in value)) return
  if (value.bootID !== null && typeof value.bootID !== "string") return
  if (value.startTicks !== null && typeof value.startTicks !== "string") return
  return { pid: value.pid, bootID: value.bootID, startTicks: value.startTicks }
}

export function processGroupFixture(value: unknown): ProcessGroupFixture | undefined {
  if (typeof value !== "object" || value === null || !("pgid" in value) || typeof value.pgid !== "number") return
  const leader = "leader" in value ? processIdentity(value.leader) : undefined
  const descendant = "descendant" in value ? processIdentity(value.descendant) : undefined
  if (!leader || !descendant) return
  return { leader, descendant, pgid: value.pgid }
}

export function sameProcess(identity: EventWriter.ProcessIdentity) {
  const current = processIdentity(identity.pid)
  return current?.bootID === identity.bootID && current.startTicks === identity.startTicks
}

export function sameIdentity(left: EventWriter.ProcessIdentity, right: EventWriter.ProcessIdentity) {
  return left.pid === right.pid && left.bootID === right.bootID && left.startTicks === right.startTicks
}

export function startIdentity(pid: number) {
  const identity = processIdentity(pid)
  if (!identity) return
  return `${identity.bootID}:${identity.startTicks}`
}

export function ownsStartIdentity(recorded: string | undefined, current: string | undefined) {
  return recorded !== undefined && current !== undefined && recorded === current
}

export function processGroupMembers(pgid: number) {
  if (process.platform === "linux") {
    return readdirSync("/proc").flatMap((entry) => {
      if (!/^\d+$/.test(entry)) return []
      const pid = Number(entry)
      return ProcessGroup.processGroupID(pid) === pgid ? [pid] : []
    })
  }
  if (process.platform === "win32") return []
  const result = Bun.spawnSync(["ps", "-e", "-o", "pid=,pgid="])
  if (result.exitCode !== 0) throw new Error(`could not inspect process group ${pgid}`)
  return result.stdout
    .toString()
    .split("\n")
    .flatMap((line) => {
      const [pid, group] = line.trim().split(/\s+/).map(Number)
      return pid && group === pgid ? [pid] : []
    })
}

export function processChildren(parentPID: number) {
  if (process.platform === "linux") {
    return readdirSync("/proc").flatMap((entry) => {
      if (!/^\d+$/.test(entry)) return []
      const pid = Number(entry)
      return ProcessGroup.processParentID(pid) === parentPID ? [pid] : []
    })
  }
  if (process.platform === "win32") return []
  const result = Bun.spawnSync(["ps", "-e", "-o", "pid=,ppid="])
  if (result.exitCode !== 0) throw new Error(`could not inspect children of process ${parentPID}`)
  return result.stdout
    .toString()
    .split("\n")
    .flatMap((line) => {
      const [pid, parent] = line.trim().split(/\s+/).map(Number)
      return pid && parent === parentPID ? [pid] : []
    })
}

export function processGroupMember(pgid: number, identity: EventWriter.ProcessIdentity | undefined) {
  return Boolean(identity && sameProcess(identity) && processGroupMembers(pgid).includes(identity.pid))
}

export function groupAlive(pgid: number) {
  return ProcessGroup.isAlive(pgid)
}
