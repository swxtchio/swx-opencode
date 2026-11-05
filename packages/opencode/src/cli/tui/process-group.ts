import { EventWriter } from "@opencode-ai/core/event/writer"

export type Event = {
  readonly eventID: number
  readonly action: "acquire" | "release"
  readonly pgid: number
  readonly cleanupOwed: boolean
  readonly identity: EventWriter.ProcessIdentity
}

type Owned = Pick<Event, "pgid">

export function sameIdentity(left: EventWriter.ProcessIdentity, right: EventWriter.ProcessIdentity) {
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

export function signalOwned(groups: Iterable<Owned>) {
  if (process.platform === "win32") return
  // The owner retains each recorded PGID until the Worker confirms release, including after its leader exits.
  for (const group of groups) {
    if (!alive(group.pgid)) continue
    try {
      process.kill(-group.pgid, "SIGKILL")
    } catch {}
  }
}

export async function killOwned(groups: ReadonlyMap<number, Event>) {
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

export * as ProcessGroup from "./process-group"
