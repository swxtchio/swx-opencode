import { expect, test } from "bun:test"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { ProcessGroup } from "../../../src/cli/tui/process-group"
import { tmpdir } from "../../fixture/fixture"
import path from "node:path"
import { withTimeout } from "../../../src/util/timeout"

type ProcessIdentity = EventWriter.ProcessIdentity
type ProcessGroupFixture = {
  readonly leader: ProcessIdentity
  readonly descendant: ProcessIdentity
  readonly pgid: number
}
type OwnerGroupEvent = { readonly action: string; readonly cleanupOwed: boolean; readonly groupAlive: boolean }

const cases = [
  { action: "exit", name: "exit-gated", resistTerm: true, blockShutdown: true, exitMode: "long", result: "gone" },
  {
    action: "worker-terminate-exit",
    name: "worker-terminate-exit",
    resistTerm: true,
    blockShutdown: true,
    exitMode: "long",
    result: "gone",
  },
  { action: "stop", name: "stop-timeout", resistTerm: true, blockShutdown: true, exitMode: "long", result: "gone" },
  {
    action: "cleanup-owed",
    name: "cleanup-owed",
    resistTerm: true,
    blockShutdown: false,
    exitMode: "cleanup-owed",
    result: "gone",
  },
  { action: "release", name: "intentional-release", resistTerm: false, blockShutdown: false, exitMode: "release", result: "released" },
  { action: "worker-error", name: "worker-error", resistTerm: true, blockShutdown: false, exitMode: "long", result: "gone" },
  { action: "stop-confirmed", name: "stop-confirmed", resistTerm: false, blockShutdown: false, exitMode: "long", result: "gone" },
] as const

function processIdentity(value: unknown): ProcessIdentity | undefined {
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

function ownerGroupEvents(value: unknown): OwnerGroupEvent[] {
  if (!Array.isArray(value)) return []
  return value.filter(
    (event): event is OwnerGroupEvent =>
      typeof event === "object" &&
      event !== null &&
      "action" in event &&
      typeof event.action === "string" &&
      "cleanupOwed" in event &&
      typeof event.cleanupOwed === "boolean" &&
      "groupAlive" in event &&
      typeof event.groupAlive === "boolean",
  )
}

function sameProcess(identity: ProcessIdentity) {
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

async function waitForRecord(file: string) {
  const deadline = Date.now() + 20_000
  while (Date.now() < deadline) {
    try {
      const value: unknown = await Bun.file(file).json()
      const parsed = processGroupFixture(value)
      if (parsed) return parsed
    } catch {}
    await Bun.sleep(20)
  }
  throw new Error(`process-group fixture did not write ${file}`)
}

async function runOwnerCase(input: (typeof cases)[number]) {
  await using directory = await tmpdir()
  const home = directory.path
  const recordFile = path.join(home, `worker-process-group-${input.name}.json`)
  const resultFile = path.join(home, `worker-process-group-${input.name}-result.json`)
  const ownerStartFile = path.join(home, `worker-process-group-${input.name}-owner.json`)
  const ownerFile = path.join(import.meta.dir, "worker-process-group-owner.ts")
  const env = Object.fromEntries(
    Object.entries({
      ...process.env,
      HOME: home,
      OPENCODE_TEST_HOME: home,
      OPENCODE_DB: ":memory:",
      OPENCODE_CONFIG_CONTENT: "{}",
      OPENCODE_DISABLE_PROJECT_CONFIG: "1",
      OPENCODE_DISABLE_AUTOUPDATE: "1",
      OPENCODE_DISABLE_AUTOCOMPACT: "1",
      OPENCODE_DISABLE_MODELS_FETCH: "1",
      SWX_TUI_GROUP_RECORD_FILE: recordFile,
      SWX_TUI_GROUP_RESIST_TERM: input.resistTerm ? "1" : "0",
      SWX_TUI_GROUP_EXIT_MODE: input.exitMode,
      SWX_TUI_BLOCK_SHUTDOWN: input.blockShutdown ? "1" : "0",
      SWX_TUI_OWNER_ACTION: input.action,
      SWX_TUI_OWNER_START_FILE: ownerStartFile,
      SWX_TUI_OWNER_RESULT_FILE: resultFile,
    }).filter((entry): entry is [string, string] => entry[1] !== undefined),
  )
  const ownerProcess = Bun.spawn([process.execPath, ownerFile], {
    cwd: process.cwd(),
    env,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    detached: true,
  })
  const output = Promise.all([new Response(ownerProcess.stdout).text(), new Response(ownerProcess.stderr).text()])
  let ownerIdentity: ProcessIdentity | undefined
  let fixture: ProcessGroupFixture | undefined

  try {
    try {
      ownerIdentity = await withTimeout(
        (async () => {
          const deadline = Date.now() + 10_000
          while (Date.now() < deadline) {
            try {
              const value: unknown = await Bun.file(ownerStartFile).json()
              const parsed = processIdentity(value)
              if (parsed) return parsed
            } catch {}
            await Bun.sleep(20)
          }
          throw new Error("TUI owner did not publish its process identity")
        })(),
        12_000,
      )
    } catch (error) {
      ownerProcess.kill("SIGKILL")
      const [stdout, stderr] = await output
      throw new Error(`${error instanceof Error ? error.message : String(error)}\n${stdout}\n${stderr}`)
    }
    const startup = await withTimeout(
      Promise.race([
        waitForRecord(recordFile).then((fixture) => ({ type: "record" as const, fixture })),
        ownerProcess.exited.then((exitCode) => ({ type: "exit" as const, exitCode })),
      ]),
      25_000,
    )
    if (startup.type === "exit") {
      const [stdout, stderr] = await output
      throw new Error(`TUI owner fixture exited ${startup.exitCode} before recording its group: ${stdout}\n${stderr}`)
    }
    fixture = startup.fixture
    expect(fixture.pgid).toBe(fixture.leader.pid)

    const ownerResultState = await withTimeout(
      Promise.race([
        (async () => {
          const deadline = Date.now() + 25_000
          while (Date.now() < deadline) {
            try {
              const result: unknown = await Bun.file(resultFile).json()
              if (typeof result === "object" && result !== null) return { type: "result" as const, result }
            } catch {}
            await Bun.sleep(20)
          }
          throw new Error("TUI owner fixture did not publish its result")
        })(),
        ownerProcess.exited.then((exitCode) => ({ type: "exit" as const, exitCode })),
      ]),
      30_000,
    )
    if (ownerResultState.type === "exit") {
      const [stdout, stderr] = await output
      throw new Error(`TUI owner fixture exited ${ownerResultState.exitCode} before publishing a result: ${stdout}\n${stderr}`)
    }
    const ownerResult = ownerResultState.result
    await withTimeout(
      (async () => {
        const deadline = Date.now() + 20_000
        while (Date.now() < deadline) {
          if (!sameProcess(ownerIdentity)) return
          await Bun.sleep(20)
        }
        throw new Error("TUI owner fixture did not exit after publishing its result")
      })(),
      25_000,
    )
    const exitCode = await Promise.race([ownerProcess.exited, Bun.sleep(1_000).then(() => undefined)])
    if (exitCode !== undefined && exitCode !== 0) {
      const [stdout, stderr] = await output
      throw new Error(`TUI owner fixture exited ${exitCode}: ${stdout}\n${stderr}`)
    }
    if (typeof ownerResult !== "object" || ownerResult === null)
      throw new Error("TUI owner fixture did not publish its result")
    const events = ownerGroupEvents("events" in ownerResult ? ownerResult.events : undefined)
    expect(events.some((event) => event.action === "acquire")).toBe(true)
    if (input.action === "exit" || input.action === "worker-terminate-exit")
      expect(events.some((event) => event.action === "acquire" && event.groupAlive)).toBe(true)

    if (input.action === "stop-confirmed" || input.action === "worker-error") {
      if (!("ownerSize" in ownerResult)) throw new Error("TUI owner fixture did not publish its ledger result")
      expect(ownerResult.ownerSize).toBe(0)
    }
    if (input.action === "cleanup-owed") {
      expect("cleanupOwed" in ownerResult && ownerResult.cleanupOwed).toBe(true)
      expect(events.some((event) => event.action === "acquire" && event.cleanupOwed && event.groupAlive)).toBe(true)
    }
    if (input.result === "released") {
      expect(
        "ownerSize" in ownerResult &&
          ownerResult.ownerSize === 0 &&
          "identityCount" in ownerResult &&
          ownerResult.identityCount === 0 &&
          "descendantAlive" in ownerResult &&
          ownerResult.descendantAlive,
      ).toBe(true)
      expect(events.some((event) => event.action === "release" && event.groupAlive)).toBe(true)
      expect(sameProcess(fixture.descendant)).toBe(true)
      return
    }
    if (input.action === "stop-confirmed") {
      const acquire = events.findIndex((event) => event.action === "acquire" && event.cleanupOwed)
      const release = events.findIndex((event, index) => index > acquire && event.action === "release")
      expect(acquire).toBeGreaterThanOrEqual(0)
      expect(events[acquire]?.groupAlive).toBe(true)
      expect(release).toBeGreaterThan(acquire)
      expect(events[release]?.groupAlive).toBe(false)
    }
    if (input.action === "worker-error") expect("ownerAlive" in ownerResult && ownerResult.ownerAlive).toBe(true)

    const deadline = Date.now() + 5_000
    while (groupAlive(fixture.pgid)) {
      if (Date.now() >= deadline) throw new Error(`${input.action} left process group ${fixture.pgid} alive`)
      await Bun.sleep(20)
    }
    expect(sameProcess(fixture.leader)).toBe(false)
    expect(sameProcess(fixture.descendant)).toBe(false)
  } finally {
    if (fixture && groupAlive(fixture.pgid)) {
      try {
        process.kill(-fixture.pgid, "SIGKILL")
      } catch {}
    }
    if (ownerIdentity && sameProcess(ownerIdentity)) {
      try {
        process.kill(ownerIdentity.pid, "SIGKILL")
      } catch {}
    } else if (!ownerIdentity) {
      ownerProcess.kill("SIGKILL")
    }
  }
}

test(
  "TUI owner uses production process-group wiring across exit, release, and Worker-failure paths",
  async () => {
    if (process.platform !== "linux") return
    const selected = cases.filter(
      (scenario) => !process.env.SWX_TUI_GROUP_CASE || process.env.SWX_TUI_GROUP_CASE === scenario.action,
    )
    await Promise.all(selected.slice(0, 3).map(runOwnerCase))
    await Promise.all(selected.slice(3, 6).map(runOwnerCase))
    await Promise.all(selected.slice(6).map(runOwnerCase))
  },
  120_000,
)

test("Worker process-group tracker forgets identities only after release acknowledgment", async () => {
  const events: ProcessGroup.Event[] = []
  const tracker = ProcessGroup.makeWorkerTracker((event) => events.push(event))
  const group = { action: "acquire" as const, pgid: process.pid, cleanupOwed: false }
  const acquire = tracker.observe(group)
  const acquired = events[0]
  expect(acquired?.action).toBe("acquire")
  expect(tracker.identityCount).toBe(1)
  expect(tracker.acknowledge(acquired!.eventID)).toBe(true)
  await acquire

  const release = tracker.observe({ ...group, action: "release" })
  const released = events[1]
  expect(released?.action).toBe("release")
  expect(tracker.identityCount).toBe(1)
  expect(tracker.acknowledge(released!.eventID)).toBe(true)
  await release
  expect(tracker.identityCount).toBe(0)
})
