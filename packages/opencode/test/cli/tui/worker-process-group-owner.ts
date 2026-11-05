import { EventWriter } from "@opencode-ai/core/event/writer"
import { ProcessGroup } from "../../../src/cli/tui/process-group"
import type { rpc } from "../../../src/cli/tui/worker"
import { Rpc } from "../../../src/util/rpc"
import { withTimeout } from "../../../src/util/timeout"
import path from "node:path"

type ProcessGroupFixture = {
  readonly leader: EventWriter.ProcessIdentity
  readonly descendant: EventWriter.ProcessIdentity
  readonly pgid: number
}

function processIdentity(value: unknown): EventWriter.ProcessIdentity | undefined {
  if (typeof value !== "object" || value === null || !("pid" in value) || typeof value.pid !== "number") return
  if (!("bootID" in value) || !("startTicks" in value)) return
  if (value.bootID !== null && typeof value.bootID !== "string") return
  if (value.startTicks !== null && typeof value.startTicks !== "string") return
  return { pid: value.pid, bootID: value.bootID, startTicks: value.startTicks }
}

function processGroupFixture(value: unknown): ProcessGroupFixture | undefined {
  if (typeof value !== "object" || value === null || !("pgid" in value) || typeof value.pgid !== "number") return
  const leader = "leader" in value ? processIdentity(value.leader) : undefined
  const descendant = "descendant" in value ? processIdentity(value.descendant) : undefined
  if (!leader || !descendant) return
  return { leader, descendant, pgid: value.pgid }
}

function sameProcess(identity: EventWriter.ProcessIdentity) {
  const current = EventWriter.readIdentity(identity.pid)
  return current?.bootID === identity.bootID && current.startTicks === identity.startTicks
}

function sameIdentity(left: EventWriter.ProcessIdentity, right: EventWriter.ProcessIdentity) {
  return left.pid === right.pid && left.bootID === right.bootID && left.startTicks === right.startTicks
}

function groupAlive(pgid: number) {
  try {
    process.kill(-pgid, 0)
    return true
  } catch (error) {
    return !(error instanceof Error && "code" in error && error.code === "ESRCH")
  }
}

const action = process.env.SWX_TUI_OWNER_ACTION
const recordFile = process.env.SWX_TUI_GROUP_RECORD_FILE
const resultFile = process.env.SWX_TUI_OWNER_RESULT_FILE
const ownerStartFile = process.env.SWX_TUI_OWNER_START_FILE
if (!action || !recordFile || !resultFile || !ownerStartFile)
  throw new Error("TUI process-group owner fixture inputs are missing")
const ownerIdentity = EventWriter.readIdentity(process.pid)
if (!ownerIdentity?.bootID || !ownerIdentity.startTicks) throw new Error("TUI owner has no process identity")
await Bun.write(ownerStartFile, JSON.stringify(ownerIdentity))
const requiredRecordFile: string = recordFile

const worker = new Worker(path.join(import.meta.dir, "../../../src/cli/tui/worker.ts"), {
  env: Object.fromEntries(
    Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined),
  ),
})
const client = Rpc.client<typeof rpc>(worker)
const shutdownStarted = Promise.withResolvers<void>()
const events: { action: string; cleanupOwed: boolean; groupAlive: boolean }[] = []
const owner = ProcessGroup.makeOwner({
  client: {
    subscribe: (handler) =>
      client.on<ProcessGroup.Event>("process.group", (event) => {
        events.push({ action: event.action, cleanupOwed: event.cleanupOwed, groupAlive: groupAlive(event.pgid) })
        handler(event)
      }),
    acknowledge: async (eventID) => client.call("ackProcessGroup", { eventID }),
    shutdown: async () => {
      shutdownStarted.resolve()
      return client.call("shutdown", undefined)
    },
  },
  worker,
})

async function ready() {
  const deadline = Date.now() + 20_000
  while (Date.now() < deadline) {
    const response = await Promise.race([
      client.call("ready", undefined).catch(() => false),
      Bun.sleep(250).then(() => false),
    ])
    if (response) return
    await Bun.sleep(20)
  }
  throw new Error("TUI Server Worker did not become ready")
}

async function waitFor(predicate: () => boolean, description: string) {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    if (predicate()) return
    await Bun.sleep(20)
  }
  throw new Error(`timed out waiting for ${description}`)
}

async function waitForRecord() {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    try {
      const value: unknown = await Bun.file(requiredRecordFile).json()
      const parsed = processGroupFixture(value)
      if (parsed) return parsed
    } catch {}
    await Bun.sleep(20)
  }
  throw new Error("Worker process-group identities were not recorded")
}

await ready()
const started = await withTimeout(
  client.call("testSpawnProcessGroup", {
    resistTerm: process.env.SWX_TUI_GROUP_RESIST_TERM === "1",
    blockShutdown: process.env.SWX_TUI_BLOCK_SHUTDOWN === "1",
    exitMode: process.env.SWX_TUI_GROUP_EXIT_MODE as "long" | "cleanup-owed" | "release" | undefined,
  }),
  20_000,
)
const record = await waitForRecord()
if (record.pgid !== started.pgid || !sameIdentity(record.leader, started.identity))
  throw new Error(`Worker record ${record.pgid} did not match acknowledged PGID ${started.pgid}`)
const pgid = record.pgid
if (action === "cleanup-owed" || action === "release") await Bun.write(`${recordFile}.exit`, "exit")

if (action === "exit") {
  void owner.stop()
  await shutdownStarted.promise
  await Bun.write(resultFile, JSON.stringify({ events }))
  process.exit(0)
}

if (action === "worker-terminate-exit") {
  void worker.terminate()
  await Bun.write(resultFile, JSON.stringify({ events }))
  process.exit(0)
}

if (action === "stop" || action === "stop-confirmed") {
  await owner.stop()
  await Bun.write(resultFile, JSON.stringify({ ownerSize: owner.size, events }))
  process.exit(0)
}

if (action === "cleanup-owed") {
  await waitFor(() => owner.get(pgid)?.cleanupOwed === true, "the cleanup-owed acquire event")
  await Bun.write(
    resultFile,
    JSON.stringify({ ownerSize: owner.size, cleanupOwed: owner.get(pgid)?.cleanupOwed, events }),
  )
  process.exit(0)
}

if (action === "release") {
  await waitFor(() => !owner.has(pgid), "the acknowledged release event")
  const identityCount = await client.call("testProcessGroupIdentityCount", undefined)
  await Bun.write(
    resultFile,
    JSON.stringify({ ownerSize: owner.size, identityCount, descendantAlive: sameProcess(record.descendant), events }),
  )
  process.exit(0)
}

if (action === "worker-error") {
  if (!worker.onerror) throw new Error("production owner did not install its Worker error hook")
  worker.onerror(new ErrorEvent("error", { message: "simulated Worker failure" }))
  await waitFor(() => !sameProcess(record.descendant), "the Worker-error group signal")
  await owner.stop()
  await Bun.write(
    resultFile,
    JSON.stringify({ ownerAlive: sameProcess(ownerIdentity), ownerSize: owner.size, events }),
  )
  process.exit(0)
}

throw new Error(`unknown TUI owner action: ${action}`)
