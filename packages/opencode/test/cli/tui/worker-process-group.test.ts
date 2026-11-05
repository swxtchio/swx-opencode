import { expect, test } from "bun:test"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { ProcessGroup } from "../../../src/cli/tui/process-group"
import type { rpc } from "../../../src/cli/tui/worker"
import { Rpc } from "../../../src/util/rpc"
import { withTimeout } from "../../../src/util/timeout"
import { tmpdir } from "../../fixture/fixture"

type ProcessGroupFixture = {
  readonly owner: EventWriter.ProcessIdentity
  readonly leader: EventWriter.ProcessIdentity
  readonly descendant: EventWriter.ProcessIdentity
  readonly pgid: number
}

type RpcClient = ReturnType<typeof Rpc.client<typeof rpc>>

function processIdentity(value: unknown): EventWriter.ProcessIdentity | undefined {
  if (typeof value !== "object" || value === null || !("pid" in value) || typeof value.pid !== "number") return
  if (!("bootID" in value) || !("startTicks" in value)) return
  if (value.bootID !== null && typeof value.bootID !== "string") return
  if (value.startTicks !== null && typeof value.startTicks !== "string") return
  return { pid: value.pid, bootID: value.bootID, startTicks: value.startTicks }
}

function processGroupFixture(value: unknown): ProcessGroupFixture | undefined {
  if (typeof value !== "object" || value === null || !("pgid" in value) || typeof value.pgid !== "number") return
  const owner = "owner" in value ? processIdentity(value.owner) : undefined
  const leader = "leader" in value ? processIdentity(value.leader) : undefined
  const descendant = "descendant" in value ? processIdentity(value.descendant) : undefined
  if (!owner || !leader || !descendant) return
  return { owner, leader, descendant, pgid: value.pgid }
}

function sameProcess(identity: EventWriter.ProcessIdentity) {
  const current = EventWriter.readIdentity(identity.pid)
  return current?.bootID === identity.bootID && current.startTicks === identity.startTicks
}

function groupAlive(pgid: number) {
  try {
    process.kill(-pgid, 0)
    return true
  } catch (error) {
    return !(error instanceof Error && "code" in error && error.code === "ESRCH")
  }
}

async function workerReady(client: RpcClient, failure: () => string | undefined) {
  const deadline = Date.now() + 20_000
  while (Date.now() < deadline) {
    const response = await Promise.race([
      client.call("ready", undefined).catch(() => false),
      Bun.sleep(250).then(() => false),
    ])
    if (response) return
    await Bun.sleep(20)
  }
  throw new Error(`TUI Server Worker did not become ready: ${failure() ?? "no Worker response"}`)
}

async function runWorkerProcessGroupCase(
  directory: string,
  input: { name: string; resistTerm: boolean; blockShutdown: boolean },
) {
  const recordFile = `${directory}/tui-worker-process-group-${input.name}.json`
  const fixtureFile = `${import.meta.dir}/worker-process-group-worker.ts`
  const workerFile = `${import.meta.dir}/../../../src/cli/tui/worker.ts`
  const env = Object.fromEntries(
    Object.entries({
      ...process.env,
      HOME: directory,
      OPENCODE_TEST_HOME: directory,
      OPENCODE_DB: ":memory:",
      OPENCODE_CONFIG_CONTENT: "{}",
      OPENCODE_DISABLE_PROJECT_CONFIG: "1",
      OPENCODE_DISABLE_AUTOUPDATE: "1",
      OPENCODE_DISABLE_AUTOCOMPACT: "1",
      OPENCODE_DISABLE_MODELS_FETCH: "1",
      SWX_TUI_GROUP_RECORD_FILE: recordFile,
      SWX_TUI_GROUP_FIXTURE_FILE: fixtureFile,
      SWX_TUI_GROUP_RESIST_TERM: input.resistTerm ? "1" : "0",
    }).filter((entry): entry is [string, string] => entry[1] !== undefined),
  )
  const worker = new Worker(workerFile, { env })
  const client = Rpc.client<typeof rpc>(worker)
  const ownedGroups = new Map<number, ProcessGroup.Event>()
  let workerError: string | undefined
  worker.onerror = (event) => {
    workerError = event.message
  }
  const removeGroupListener = client.on<ProcessGroup.Event>("process.group", (event) => {
    if (event.action === "acquire") {
      ownedGroups.set(event.pgid, event)
    } else {
      const current = ownedGroups.get(event.pgid)
      if (current && ProcessGroup.sameIdentity(current.identity, event.identity)) ownedGroups.delete(event.pgid)
    }
    void client.call("ackProcessGroup", { eventID: event.eventID }).catch(() => {})
  })
  let stopped = false
  const cleanup = async () => {
    if (stopped) return
    await ProcessGroup.killOwned(ownedGroups).catch(() => ProcessGroup.signalOwned(ownedGroups.values()))
    await worker.terminate()
    removeGroupListener()
  }

  try {
    await workerReady(client, () => workerError)
    const started = await withTimeout(
      client.call("testSpawnProcessGroup", { resistTerm: input.resistTerm, blockShutdown: input.blockShutdown }),
      20_000,
    )
    const fixture = await withTimeout(
      (async () => {
        const deadline = Date.now() + 10_000
        while (Date.now() < deadline) {
          try {
            const value: unknown = await Bun.file(recordFile).json()
            const parsed = processGroupFixture(value)
            if (parsed) return parsed
          } catch {}
          await Bun.sleep(20)
        }
        throw new Error("TUI Server Worker did not write its process-group fixture")
      })(),
      12_000,
    )

    expect(fixture.owner.pid).toBe(process.pid)
    expect(sameProcess(fixture.owner)).toBe(true)
    expect(fixture.pgid).toBe(started.pgid)
    expect(fixture.pgid).toBe(fixture.leader.pid)
    expect(sameProcess(fixture.leader)).toBe(true)
    expect(sameProcess(fixture.descendant)).toBe(true)
    expect(groupAlive(fixture.pgid)).toBe(true)
    expect(ProcessGroup.sameIdentity(ownedGroups.get(fixture.pgid)!.identity, fixture.leader)).toBe(true)

    if (input.blockShutdown) {
      const confirmed = await withTimeout(client.call("shutdown", undefined), 1).then(
        () => true,
        () => false,
      )
      expect(confirmed).toBe(false)
      await ProcessGroup.killOwned(ownedGroups)
    } else {
      await withTimeout(client.call("shutdown", undefined), 15_000)
      expect(ownedGroups.size).toBe(0)
    }
    await worker.terminate()
    stopped = true
    removeGroupListener()

    const groupDeadline = Date.now() + 5_000
    while (groupAlive(fixture.pgid)) {
      if (Date.now() >= groupDeadline) throw new Error(`Worker process group ${fixture.pgid} survived owner exit`)
      await Bun.sleep(20)
    }
    expect(sameProcess(fixture.leader)).toBe(false)
    expect(sameProcess(fixture.descendant)).toBe(false)
  } finally {
    await cleanup()
  }
}

test(
  "TUI Server Worker cleans its process group before ack and the parent backs up a missing reply",
  async () => {
    if (process.platform !== "linux") return
    await using directory = await tmpdir()
    await runWorkerProcessGroupCase(directory.path, { name: "cooperative", resistTerm: false, blockShutdown: false })
    await runWorkerProcessGroupCase(directory.path, { name: "resistant", resistTerm: true, blockShutdown: true })
  },
  90_000,
)
