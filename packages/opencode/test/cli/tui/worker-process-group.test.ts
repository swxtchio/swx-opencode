import { expect, test } from "bun:test"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { isGroupMember } from "@opencode-ai/core/process-group"
import { ProcessGroup } from "../../../src/cli/tui/process-group"
import { tmpdir } from "../../fixture/fixture"
import path from "node:path"
import { withTimeout } from "../../../src/util/timeout"
import { groupAlive, processGroupFixture, processIdentity, sameProcess } from "../../../../core/test/fixture/process-group"
import type { ProcessGroupFixture, ProcessIdentity } from "../../../../core/test/fixture/process-group"
type OwnerGroupEvent = {
  readonly action: string
  readonly cleanupOwed: boolean
  readonly groupAlive: boolean
  readonly membersPending: boolean
  readonly membersUnavailable: boolean
}

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
  {
    action: "release-ack-rejected",
    name: "release-ack-rejected",
    resistTerm: false,
    blockShutdown: false,
    exitMode: "release",
    result: "released",
  },
  {
    action: "release-ack-delayed",
    name: "release-ack-delayed",
    resistTerm: false,
    blockShutdown: false,
    exitMode: "release",
    result: "released",
  },
  { action: "worker-error", name: "worker-error", resistTerm: true, blockShutdown: false, exitMode: "long", result: "gone" },
  {
    action: "discovery-failure",
    name: "discovery-failure",
    resistTerm: true,
    blockShutdown: false,
    exitMode: "cleanup-owed",
    result: "gone",
  },
  { action: "stop-confirmed", name: "stop-confirmed", resistTerm: false, blockShutdown: false, exitMode: "long", result: "gone" },
] as const

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
      typeof event.groupAlive === "boolean" &&
      "membersPending" in event &&
      typeof event.membersPending === "boolean" &&
      "membersUnavailable" in event &&
      typeof event.membersUnavailable === "boolean",
  )
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
  const discoveryHoldFile = input.action === "stop-confirmed" ? path.join(home, "cleanup-discovery") : undefined
  const cleanupAcquireEventFile = input.action === "stop-confirmed" ? path.join(home, "cleanup-acquire.json") : undefined
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
      SWX_TUI_GROUP_DISCOVERY_FAILURE: input.action === "discovery-failure" ? "1" : undefined,
      SWX_TUI_GROUP_DISCOVERY_HOLD_FILE: discoveryHoldFile,
      SWX_TUI_GROUP_CLEANUP_ACQUIRE_EVENT_FILE: cleanupAcquireEventFile,
      SWX_TUI_BLOCK_SHUTDOWN: input.blockShutdown ? "1" : "0",
      SWX_TUI_OWNER_ACTION: input.action,
      SWX_TUI_GROUP_ACK_MODE:
        input.action === "release-ack-rejected"
          ? "rejected"
          : input.action === "release-ack-delayed"
            ? "delayed"
            : undefined,
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
      throw new Error(
        `TUI owner process identity is unavailable; cleanup skipped: ${error instanceof Error ? error.message : String(error)}`,
      )
    }
    const startup = await withTimeout(
      Promise.race([
        waitForRecord(recordFile).then((fixture) => ({ type: "record" as const, fixture })),
        ownerProcess.exited.then((exitCode) => ({ type: "exit" as const, exitCode })),
      ]),
      25_000,
    )
    const startupFixture =
      startup.type === "record"
        ? startup.fixture
        : startup.exitCode === 0
          ? await Bun.file(recordFile)
              .json()
              .then(processGroupFixture)
              .catch(() => undefined)
          : undefined
    if (!startupFixture) {
      const [stdout, stderr] = await withTimeout(output, 5_000)
      const result = await Bun.file(resultFile).text().catch(() => "<missing>")
      throw new Error(
        `TUI owner fixture for ${input.action} exited ${startup.type === "exit" ? startup.exitCode : "without an exit"} before recording its group: ${result}\n${stdout}\n${stderr}`,
      )
    }
    fixture = startupFixture
    expect(fixture.pgid).toBe(fixture.leader.pid)
    if (input.action === "stop-confirmed") {
      if (!discoveryHoldFile || !cleanupAcquireEventFile) throw new Error("cleanup discovery witness paths are missing")
      await withTimeout(
        (async () => {
          while (!(await Bun.file(cleanupAcquireEventFile).exists())) await Bun.sleep(20)
        })(),
        5_000,
      )
      await withTimeout(
        (async () => {
          while (!(await Bun.file(`${discoveryHoldFile}.started`).exists())) await Bun.sleep(20)
        })(),
        5_000,
      )
      await Bun.write(`${discoveryHoldFile}.release`, "release")
    }

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
    if (ownerResultState.type === "exit" && ownerResultState.exitCode !== 0) {
      const [stdout, stderr] = await withTimeout(output, 5_000)
      const result = await Bun.file(resultFile).text().catch(() => "<missing>")
      throw new Error(
        `TUI owner fixture for ${input.action} exited ${ownerResultState.exitCode} before publishing a result: ${result}\n${stdout}\n${stderr}`,
      )
    }
    const ownerResult =
      ownerResultState.type === "result"
        ? ownerResultState.result
        : await Bun.file(resultFile).json().catch(() => undefined)
    if (typeof ownerResult !== "object" || ownerResult === null)
      throw new Error(`TUI owner fixture for ${input.action} exited without a valid result file`)
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
      const [stdout, stderr] = await withTimeout(output, 5_000)
      throw new Error(`TUI owner fixture exited ${exitCode}: ${stdout}\n${stderr}`)
    }
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
    if (input.action === "discovery-failure") {
      expect("ownerSize" in ownerResult && ownerResult.ownerSize).toBe(0)
      expect(events.some((event) => event.action === "acquire" && event.cleanupOwed && event.membersPending)).toBe(true)
      expect(
        events.some(
          (event) => event.action === "acquire" && event.cleanupOwed && event.membersUnavailable && !event.membersPending,
        ),
      ).toBe(true)
    }
    if (input.result === "released") {
      expect(
        "ownerSize" in ownerResult &&
          ownerResult.ownerSize === 0 &&
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
      if (release <= acquire) throw new Error(`stop-confirmed omitted the later release event: ${JSON.stringify(events)}`)
      expect(events[release]?.groupAlive).toBe(false)
    }
    if (input.action === "worker-error") expect("ownerAlive" in ownerResult && ownerResult.ownerAlive).toBe(true)

    const deadline = Date.now() + 5_000
    while (sameProcess(fixture.leader) || sameProcess(fixture.descendant)) {
      if (Date.now() >= deadline) throw new Error(`${input.action} left process group ${fixture.pgid} alive`)
      await Bun.sleep(20)
    }
    expect(sameProcess(fixture.leader)).toBe(false)
    expect(sameProcess(fixture.descendant)).toBe(false)
  } finally {
    if (discoveryHoldFile) await Bun.write(`${discoveryHoldFile}.release`, "release").catch(() => {})
    if (fixture && (sameProcess(fixture.leader) || sameProcess(fixture.descendant))) {
      try {
        process.kill(-fixture.pgid, "SIGKILL")
      } catch {}
    }
    if (ownerIdentity && sameProcess(ownerIdentity)) {
      try {
        process.kill(ownerIdentity.pid, "SIGKILL")
      } catch {}
    }
    if (ownerIdentity) await withTimeout(ownerProcess.exited, 5_000)
  }
}

test(
  "TUI owner uses production process-group wiring across exit, release, and Worker-failure paths",
  async () => {
    if (process.platform !== "linux") return
    const selectedActions = process.env.SWX_TUI_GROUP_CASE?.split(",")
    const selected = cases.filter((scenario) => !selectedActions || selectedActions.includes(scenario.action))
    for (const scenario of selected) await runOwnerCase(scenario)
  },
  120_000,
)

test("real Core cleanup publication beats a delayed initial acquire acknowledgement", async () => {
  if (process.platform !== "linux") return
  await using directory = await tmpdir()
  const home = directory.path
  const recordFile = path.join(home, "pending-owner-record.json")
  const resultFile = path.join(home, "pending-owner-result.json")
  const ownerStartFile = path.join(home, "pending-owner-start.json")
  const firstEventFile = path.join(home, "pending-owner-first-event.json")
  const pendingEventFile = path.join(home, "pending-owner-cleanup-event.json")
  const initialAckStartedFile = path.join(home, "pending-owner-initial-ack-started")
  const initialAckReleaseFile = path.join(home, "pending-owner-initial-ack-release")
  const discoveryHoldFile = path.join(home, "pending-owner-discovery")
  const ownerExitFile = path.join(home, "pending-owner-exit")
  const ownerFile = path.join(import.meta.dir, "worker-process-group-owner.ts")
  const ownerProcess = Bun.spawn([process.execPath, ownerFile], {
    cwd: process.cwd(),
    env: Object.fromEntries(
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
        SWX_TUI_GROUP_RESIST_TERM: "1",
        SWX_TUI_GROUP_EXIT_MODE: "cleanup-owed",
        SWX_TUI_GROUP_ACK_MODE: "delayed-initial",
        SWX_TUI_GROUP_DISCOVERY_FAILURE: undefined,
        SWX_TUI_GROUP_FIRST_EVENT_FILE: firstEventFile,
        SWX_TUI_GROUP_PENDING_EVENT_FILE: pendingEventFile,
        SWX_TUI_GROUP_INITIAL_ACK_STARTED_FILE: initialAckStartedFile,
        SWX_TUI_GROUP_INITIAL_ACK_RELEASE_FILE: initialAckReleaseFile,
        SWX_TUI_GROUP_DISCOVERY_HOLD_FILE: discoveryHoldFile,
        SWX_TUI_OWNER_ACTION: "pending-owner-exit",
        SWX_TUI_OWNER_START_FILE: ownerStartFile,
        SWX_TUI_OWNER_RESULT_FILE: resultFile,
        SWX_TUI_OWNER_EXIT_FILE: ownerExitFile,
      }).filter((entry): entry is [string, string] => entry[1] !== undefined),
    ),
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    detached: true,
  })
  const output = Promise.all([new Response(ownerProcess.stdout).text(), new Response(ownerProcess.stderr).text()])
  const ownerIdentity = processIdentity(ownerProcess.pid)
  let pgid: number | undefined
  let descendant: ProcessIdentity | undefined

  try {
    const firstEvent = await withTimeout(
      (async () => {
        const deadline = Date.now() + 10_000
        while (Date.now() < deadline) {
          try {
            const value: unknown = await Bun.file(firstEventFile).json()
            if (typeof value === "object" && value !== null && "pgid" in value && typeof value.pgid === "number") {
              const identity = "identity" in value ? processIdentity(value.identity) : undefined
              if (identity) return { pgid: value.pgid, identity }
            }
          } catch {}
          await Bun.sleep(20)
        }
        throw new Error("Core did not publish the initial acquire event")
      })(),
      12_000,
    )
    pgid = firstEvent.pgid
    await withTimeout(
      (async () => {
        while (!(await Bun.file(initialAckStartedFile).exists())) await Bun.sleep(20)
      })(),
      5_000,
    )
    descendant = await withTimeout(
      (async () => {
        const deadline = Date.now() + 5_000
        while (Date.now() < deadline) {
          const pid = Number((await Bun.file(`${recordFile}.descendant`).text().catch(() => "")).trim())
          const identity = processIdentity(pid)
          if (identity && isGroupMember(pgid!, identity)) return identity
          await Bun.sleep(20)
        }
        throw new Error("real Core producer did not start a live descendant")
      })(),
      5_500,
    )
    await Bun.write(`${recordFile}.exit`, "exit")
    const pendingSeen = await withTimeout(
      (async () => {
        while (!(await Bun.file(pendingEventFile).exists())) await Bun.sleep(20)
        return true
      })(),
      2_000,
    ).then(
      () => true,
      () => false,
    )
    const discoveryHeld = await withTimeout(
      (async () => {
        while (!(await Bun.file(`${discoveryHoldFile}.started`).exists())) await Bun.sleep(20)
        return true
      })(),
      2_000,
    ).then(
      () => true,
      () => false,
    )
    expect(await Bun.file(initialAckReleaseFile).exists()).toBe(false)
    await Bun.write(ownerExitFile, "exit")
    const ownerExit = await withTimeout(ownerProcess.exited, 10_000)
    if (ownerExit !== 0) {
      const [stdout, stderr] = await withTimeout(output, 2_000)
      throw new Error(`pending owner exited ${ownerExit}: ${stdout}\n${stderr}`)
    }
    const result: unknown = await Bun.file(resultFile).json()
    const events = ownerGroupEvents(
      typeof result === "object" && result !== null && "events" in result ? result.events : undefined,
    )
    expect(pendingSeen).toBe(true)
    expect(discoveryHeld).toBe(true)
    expect(events.some((event) => event.cleanupOwed && event.membersPending)).toBe(true)
    await withTimeout(
      (async () => {
        while (isGroupMember(pgid!, descendant)) await Bun.sleep(20)
      })(),
      5_000,
    )
    expect(isGroupMember(pgid, descendant)).toBe(false)
  } finally {
    await Bun.write(`${recordFile}.exit`, "exit").catch(() => {})
    await Bun.write(initialAckReleaseFile, "release").catch(() => {})
    await Bun.write(`${discoveryHoldFile}.release`, "release").catch(() => {})
    await Bun.write(ownerExitFile, "exit").catch(() => {})
    if (pgid && descendant && isGroupMember(pgid, descendant)) {
      try {
        process.kill(-pgid, "SIGKILL")
      } catch {}
    }
    if (ownerIdentity && sameProcess(ownerIdentity)) {
      try {
        process.kill(ownerProcess.pid, "SIGKILL")
      } catch {}
    }
    await withTimeout(ownerProcess.exited, 5_000).catch(() => undefined)
  }
}, 30_000)

test("real cancellation acquire publishes while discovery is held before leader exit", async () => {
  if (process.platform !== "linux") return
  await using directory = await tmpdir()
  const home = directory.path
  const recordFile = path.join(home, "cancel-overlap-group.json")
  const resultFile = path.join(home, "cancel-overlap-result.json")
  const ownerStartFile = path.join(home, "cancel-overlap-owner.json")
  const discoveryHoldFile = path.join(home, "cancel-overlap-discovery")
  const cleanupAcquireEventFile = path.join(home, "cancel-overlap-live-acquire.json")
  const pendingEventFile = path.join(home, "cancel-overlap-pending-acquire.json")
  const cancelExitFile = path.join(home, "cancel-overlap-trigger-worker-error")
  const ownerFile = path.join(import.meta.dir, "worker-process-group-owner.ts")
  const ownerProcess = Bun.spawn([process.execPath, ownerFile], {
    cwd: process.cwd(),
    env: Object.fromEntries(
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
        SWX_TUI_GROUP_RESIST_TERM: "1",
        SWX_TUI_GROUP_EXIT_MODE: "cancel-overlap",
        SWX_TUI_GROUP_DISCOVERY_FAILURE: undefined,
        SWX_TUI_GROUP_DISCOVERY_HOLD_FILE: discoveryHoldFile,
        SWX_TUI_GROUP_CLEANUP_ACQUIRE_EVENT_FILE: cleanupAcquireEventFile,
        SWX_TUI_GROUP_PENDING_EVENT_FILE: pendingEventFile,
        SWX_TUI_GROUP_CANCEL_EXIT_FILE: cancelExitFile,
        SWX_TUI_OWNER_ACTION: "cancel-overlap",
        SWX_TUI_OWNER_START_FILE: ownerStartFile,
        SWX_TUI_OWNER_RESULT_FILE: resultFile,
      }).filter((entry): entry is [string, string] => entry[1] !== undefined),
    ),
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    detached: true,
  })
  const output = Promise.all([new Response(ownerProcess.stdout).text(), new Response(ownerProcess.stderr).text()])
  const ownerIdentity = processIdentity(ownerProcess.pid)
  let fixture: ProcessGroupFixture | undefined

  try {
    fixture = await waitForRecord(recordFile)
    expect(isGroupMember(fixture.pgid, fixture.leader)).toBe(true)
    expect(isGroupMember(fixture.pgid, fixture.descendant)).toBe(true)
    await withTimeout(
      (async () => {
        while (!(await Bun.file(cleanupAcquireEventFile).exists())) await Bun.sleep(20)
      })(),
      5_000,
    )
    const liveAcquire: unknown = await Bun.file(cleanupAcquireEventFile).json()
    if (
      typeof liveAcquire !== "object" ||
      liveAcquire === null ||
      !("action" in liveAcquire) ||
      !("cleanupOwed" in liveAcquire) ||
      !("membersPending" in liveAcquire)
    )
      throw new Error("cancellation did not publish its live cleanup acquire")
    expect(liveAcquire.action).toBe("acquire")
    expect(liveAcquire.cleanupOwed).toBe(true)
    expect(liveAcquire.membersPending).toBe(false)
    await withTimeout(
      (async () => {
        while (!(await Bun.file(`${discoveryHoldFile}.started`).exists())) await Bun.sleep(20)
      })(),
      5_000,
    )
    expect(isGroupMember(fixture.pgid, fixture.leader)).toBe(true)
    expect(isGroupMember(fixture.pgid, fixture.descendant)).toBe(true)

    await Bun.write(`${recordFile}.exit`, "exit")
    const pendingPublished = await withTimeout(
      (async () => {
        while (!(await Bun.file(pendingEventFile).exists())) await Bun.sleep(20)
        return true
      })(),
      5_000,
    ).then(
      () => true,
      () => false,
    )
    expect(pendingPublished).toBe(true)
    await Bun.write(cancelExitFile, "worker-error")
    const ownerExit = await withTimeout(ownerProcess.exited, 15_000)
    if (ownerExit !== 0) {
      const [stdout, stderr] = await withTimeout(output, 2_000)
      throw new Error(`cancellation-overlap owner exited ${ownerExit}: ${stdout}\n${stderr}`)
    }
    const result: unknown = await Bun.file(resultFile).json()
    if (
      typeof result !== "object" ||
      result === null ||
      !("descendantAlive" in result) ||
      typeof result.descendantAlive !== "boolean" ||
      !("events" in result)
    )
      throw new Error("cancellation-overlap owner result is missing its boolean descendant and event state")
    const events = ownerGroupEvents(result.events)
    const liveIndex = events.findIndex((event) => event.action === "acquire" && event.cleanupOwed && !event.membersPending)
    const exitedIndex = events.findIndex(
      (event, index) => index > liveIndex && event.action === "acquire" && event.cleanupOwed && event.membersPending,
    )
    expect(liveIndex).toBeGreaterThanOrEqual(0)
    expect(events[liveIndex]?.groupAlive).toBe(true)
    expect(exitedIndex).toBeGreaterThan(liveIndex)
    expect(result.descendantAlive).toBe(false)
    expect(isGroupMember(fixture.pgid, fixture.descendant)).toBe(false)
  } finally {
    await Bun.write(`${recordFile}.exit`, "exit").catch(() => {})
    await Bun.write(`${discoveryHoldFile}.release`, "release").catch(() => {})
    await Bun.write(cancelExitFile, "worker-error").catch(() => {})
    if (fixture && (isGroupMember(fixture.pgid, fixture.leader) || isGroupMember(fixture.pgid, fixture.descendant))) {
      try {
        process.kill(-fixture.pgid, "SIGKILL")
      } catch {}
    }
    if (ownerIdentity && sameProcess(ownerIdentity)) {
      try {
        process.kill(ownerProcess.pid, "SIGKILL")
      } catch {}
    }
    await withTimeout(ownerProcess.exited, 5_000).catch(() => undefined)
  }
}, 35_000)

test("owner uses the live-PGID fallback when process identity is unavailable", async () => {
  if (process.platform === "win32") return
  const child = Bun.spawn([process.execPath, "-e", 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1000)'], {
    stdin: "ignore",
    stdout: "ignore",
    stderr: "ignore",
    detached: true,
  })
  const identity = processIdentity(child.pid)
  if (!identity) throw new Error("identity fallback fixture has no process identity for cleanup")
  const unavailable = { pid: child.pid, bootID: null, startTicks: null }
  const event: ProcessGroup.Event = {
    eventID: 0,
    action: "acquire",
    pgid: child.pid,
    cleanupOwed: true,
    identity: unavailable,
    members: [],
    membersPending: false,
    membersSnapshotComplete: false,
    membersUnavailable: false,
    generation: 1,
    ownershipSequence: 1,
  }
  const worker = { onerror: null as ((event: ErrorEvent) => unknown) | null, terminate: () => {} }
  const owner = ProcessGroup.makeOwner({
    client: {
      subscribe(handler) {
        handler(event)
        return () => {}
      },
      acknowledge: async () => undefined,
      shutdown: async () => {
        throw new Error("injected unconfirmed shutdown")
      },
    },
    worker,
    readIdentity: () => undefined,
  })
  try {
    expect(owner.has(child.pid)).toBe(true)
    await owner.stop()
    await withTimeout(child.exited, 5_000)
    expect(groupAlive(child.pid)).toBe(false)
  } finally {
    if (sameProcess(identity)) {
      try {
        process.kill(-child.pid, "SIGKILL")
      } catch {}
    }
    await withTimeout(child.exited, 5_000)
    await owner.stop()
  }
})

test("Worker tracker and owner stay aligned across rejected or delayed acknowledgements and group reuse", async () => {
  if (process.platform !== "linux") return
  for (const mode of ["rejected", "delayed"] as const) {
    const child = Bun.spawn([process.execPath, "-e", "setInterval(() => {}, 1000)"], {
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
      detached: true,
    })
    const identity = EventWriter.readIdentity(child.pid)
    if (!identity?.bootID || !identity.startTicks) throw new Error("acknowledgement fixture has no process identity")
    const events: ProcessGroup.Event[] = []
    const eventsByID = new Map<number, ProcessGroup.Event>()
    let receive: ((event: ProcessGroup.Event) => void) | undefined
    const tracker = ProcessGroup.makeWorkerTracker((event) => {
      events.push(event)
      eventsByID.set(event.eventID, event)
      receive?.(event)
    }, 25)
    const worker = { onerror: null as ((event: ErrorEvent) => unknown) | null, terminate: () => {} }
    const owner = ProcessGroup.makeOwner({
      client: {
        subscribe(handler) {
          receive = handler
          return () => (receive = undefined)
        },
        async acknowledge(eventID) {
          if (!eventsByID.has(eventID)) throw new Error(`unknown process-group event ${eventID}`)
          if (mode === "rejected") throw new Error("injected acknowledgement rejection")
          await Bun.sleep(100)
          return tracker.acknowledge(eventID)
        },
        async shutdown() {
          return true
        },
      },
      worker,
    })
    const group = { pgid: child.pid, cleanupOwed: false, identity }
    try {
      await expect(tracker.observe({ action: "acquire", ...group })).rejects.toThrow()
      expect(owner.has(group.pgid)).toBe(true)
      expect(owner.get(group.pgid)?.identity).toEqual(identity)

      if (sameProcess(identity)) {
        try {
          process.kill(-identity.pid, "SIGKILL")
        } catch {}
      }
      await withTimeout(child.exited, 5_000)

      const originalKill = process.kill.bind(process)
      const forwarded: Array<NodeJS.Signals | number | string | undefined> = []
      const patchedKill: typeof process.kill = (pid, signal) => {
        if (pid === -identity.pid) {
          if (signal === 0) return true
          forwarded.push(signal)
          return true
        }
        return originalKill(pid, signal)
      }
      process.kill = patchedKill
      try {
        worker.onerror?.(new ErrorEvent("error", { message: "simulated owner-side Worker failure" }))
        expect(forwarded).toEqual([])
      } finally {
        process.kill = originalKill
      }

      await expect(tracker.observe({ action: "release", ...group })).rejects.toThrow()
      expect(owner.has(group.pgid)).toBe(false)
      await expect(tracker.observe({ ...group, action: "acquire", cleanupOwed: true })).rejects.toThrow()
      expect(owner.has(group.pgid)).toBe(false)
      await expect(tracker.observe({ ...group, action: "release", cleanupOwed: false })).rejects.toThrow()
      expect(owner.has(group.pgid)).toBe(false)
      // A pruned acquire for a gone group owes no resolved event.
      expect(events.map((event) => event.identity)).toEqual([identity, identity, identity, identity])
    } finally {
      if (sameProcess(identity)) {
        try {
          process.kill(-identity.pid, "SIGKILL")
        } catch {}
      }
      await withTimeout(child.exited, 5_000)
      await owner.stop()
    }
  }
})

test("owner does not signal a live PGID using another process group's recorded identity", async () => {
  if (process.platform !== "linux") return
  await using directory = await tmpdir()
  const readyFile = path.join(directory.path, "first-ready")
  const first = Bun.spawn(
    [
      process.execPath,
      "-e",
      'process.on("SIGUSR1", () => process.exit(42)); require("node:fs").writeFileSync(process.env.SWX_PGID_READY_FILE, "ready"); setInterval(() => {}, 1000)',
    ],
    {
      env: { ...process.env, SWX_PGID_READY_FILE: readyFile },
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
      detached: true,
    },
  )
  const second = Bun.spawn([process.execPath, "-e", "setInterval(() => {}, 1000)"], {
    stdin: "ignore",
    stdout: "ignore",
    stderr: "ignore",
    detached: true,
  })
  const firstIdentity = EventWriter.readIdentity(first.pid)
  const secondIdentity = EventWriter.readIdentity(second.pid)
  let receive: ((event: ProcessGroup.Event) => void) | undefined
  const worker = { onerror: null as ((event: ErrorEvent) => unknown) | null, terminate: () => {} }
  let owner: ProcessGroup.Owner | undefined
  try {
    if (!firstIdentity?.bootID || !firstIdentity.startTicks || !secondIdentity?.bootID || !secondIdentity.startTicks)
      throw new Error("PGID reuse fixture has no recorded process identities")
    owner = ProcessGroup.makeOwner({
      client: {
        subscribe(handler) {
          receive = handler
          return () => (receive = undefined)
        },
        async acknowledge() {
          return true
        },
        async shutdown() {
          return true
        },
      },
      worker,
    })
    await withTimeout(
      (async () => {
        const deadline = Date.now() + 5_000
        while (Date.now() < deadline) {
          if ((await Bun.file(readyFile).text().catch(() => "")).trim() === "ready") return
          await Bun.sleep(20)
        }
        throw new Error("stale-PGID first process did not become signal-ready")
      })(),
      6_000,
    )
    if (!receive) throw new Error("process-group owner did not install its event receiver")
    receive({
      eventID: 1,
      action: "acquire",
      pgid: first.pid,
      cleanupOwed: false,
      identity: secondIdentity,
      members: [],
      membersPending: false,
      membersSnapshotComplete: true,
      membersUnavailable: false,
      generation: 1,
      ownershipSequence: 1,
    })
    const originalKill = process.kill.bind(process)
    const forwarded: Array<NodeJS.Signals | number | undefined> = []
    const patchedKill: typeof process.kill = (pid, signal) => {
      if (pid === -first.pid && signal === "SIGKILL") {
        forwarded.push(signal)
        return true
      }
      return originalKill(pid, signal)
    }
    process.kill = patchedKill
    try {
      worker.onerror?.(new ErrorEvent("error", { message: "simulated stale PGID ownership" }))
      expect(forwarded).toEqual([])
    } finally {
      process.kill = originalKill
    }
    await owner.stop()
    expect(sameProcess(firstIdentity)).toBe(true)
    process.kill(firstIdentity.pid, "SIGUSR1")
    expect(await withTimeout(first.exited, 5_000)).toBe(42)
    expect(sameProcess(secondIdentity)).toBe(true)
  } finally {
    if (firstIdentity && sameProcess(firstIdentity)) {
      try {
        process.kill(-first.pid, "SIGKILL")
      } catch {}
    }
    if (secondIdentity && sameProcess(secondIdentity)) {
      try {
        process.kill(-second.pid, "SIGKILL")
      } catch {}
    }
    await withTimeout(first.exited, 5_000)
    await withTimeout(second.exited, 5_000)
    await owner?.stop()
  }
})

test("stop prunes a refuted live PGID without waiting for its deadline", async () => {
  if (process.platform !== "linux") return
  const child = Bun.spawn([process.execPath, "-e", "setInterval(() => {}, 1000)"], {
    stdin: "ignore",
    stdout: "ignore",
    stderr: "ignore",
    detached: true,
  })
  const identity = EventWriter.readIdentity(child.pid)
  if (!identity?.bootID || !identity.startTicks) throw new Error("refuted stop fixture has no process identity")
  let refuted = false
  const readIdentity = (pid: number) => {
    if (refuted && pid === identity.pid) return { ...identity, startTicks: "stale" }
    return EventWriter.readIdentity(pid)
  }
  const worker = { onerror: null as ((event: ErrorEvent) => unknown) | null, terminate: () => {} }
  const owner = ProcessGroup.makeOwner({
    client: {
      subscribe(handler) {
        handler({
          eventID: 1,
          action: "acquire",
          pgid: child.pid,
          cleanupOwed: false,
          identity,
          members: [],
          membersPending: false,
          membersSnapshotComplete: true,
          membersUnavailable: false,
          generation: 1,
          ownershipSequence: 1,
        })
        return () => {}
      },
      acknowledge: async () => undefined,
      shutdown: async () => true,
    },
    worker,
    readIdentity,
  })
  const originalKill = process.kill.bind(process)
  const forwarded: Array<NodeJS.Signals | number | string | undefined> = []
  process.kill = ((pid, signal) => {
    if (pid === -child.pid && signal === "SIGKILL") forwarded.push(signal)
    return originalKill(pid, signal)
  }) as typeof process.kill
  try {
    expect(owner.has(child.pid)).toBe(true)
    refuted = true
    await withTimeout(owner.stop(), 500)
    expect(owner.has(child.pid)).toBe(false)
    expect(forwarded).toEqual([])
    expect(sameProcess(identity)).toBe(true)
  } finally {
    process.kill = originalKill
    if (sameProcess(identity)) {
      try {
        process.kill(-child.pid, "SIGKILL")
      } catch {}
    }
    await withTimeout(child.exited, 5_000)
  }
})

test("owner prunes a refuted live leader even while cleanup discovery is pending", async () => {
  if (process.platform !== "linux") return
  const child = Bun.spawn([process.execPath, "-e", "setInterval(() => {}, 1000)"], {
    stdin: "ignore",
    stdout: "ignore",
    stderr: "ignore",
    detached: true,
  })
  const identity = EventWriter.readIdentity(child.pid)
  if (!identity?.bootID || !identity.startTicks) throw new Error("refuted pending fixture has no identity")
  const stale = { ...identity, startTicks: `${identity.startTicks}:stale` }
  const event: ProcessGroup.Event = {
    eventID: 1,
    action: "acquire",
    pgid: child.pid,
    cleanupOwed: true,
    identity: stale,
    members: [],
    membersPending: true,
    membersSnapshotComplete: false,
    membersUnavailable: false,
    generation: 1,
    ownershipSequence: 1,
  }
  const worker = { onerror: null as ((event: ErrorEvent) => unknown) | null, terminate: () => {} }
  const owner = ProcessGroup.makeOwner({
    client: {
      subscribe(handler) {
        handler(event)
        return () => {}
      },
      acknowledge: async () => undefined,
      shutdown: async () => true,
    },
    worker,
  })
  const originalKill = process.kill.bind(process)
  const forwarded: Array<NodeJS.Signals | number | string | undefined> = []
  process.kill = ((pid, signal) => {
    if (pid === -child.pid && signal === "SIGKILL") forwarded.push(signal)
    return originalKill(pid, signal)
  }) as typeof process.kill
  try {
    expect(groupAlive(child.pid)).toBe(true)
    expect(owner.has(child.pid)).toBe(false)
    await withTimeout(owner.stop(), 500)
    expect(forwarded).toEqual([])
    expect(sameProcess(identity)).toBe(true)
  } finally {
    process.kill = originalKill
    if (sameProcess(identity)) {
      try {
        process.kill(-child.pid, "SIGKILL")
      } catch {}
    }
    await withTimeout(child.exited, 5_000)
  }
})

test("release then reacquire uses recorded descendant identity after the leader exits", async () => {
  if (process.platform !== "linux") return
  for (const mode of ["rejected", "delayed"] as const) {
    await using directory = await tmpdir()
    const descendantFile = path.join(directory.path, "descendant.pid")
    const exitFile = path.join(directory.path, "leader.exit")
    const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`
    const descendantCode = 'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(10**6)'
    const command = `nohup python3 -c ${quote(descendantCode)} >/dev/null 2>&1 & printf '%s' "$!" > ${quote(descendantFile)}; while [ ! -e ${quote(exitFile)} ]; do sleep 0.01; done; exit 0`
    const child = Bun.spawn(["bash", "--noprofile", "--norc", "-c", command], {
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
      detached: true,
    })
    const leader = EventWriter.readIdentity(child.pid)
    if (!leader?.bootID || !leader.startTicks) throw new Error("released-group leader has no process identity")
    const descendant = await withTimeout(
      (async () => {
        const deadline = Date.now() + 5_000
        while (Date.now() < deadline) {
          try {
            const pid = Number((await Bun.file(descendantFile).text()).trim())
            const identity = EventWriter.readIdentity(pid)
            if (identity?.bootID && identity.startTicks && sameProcess(identity)) return identity
          } catch {}
          await Bun.sleep(20)
        }
        throw new Error("released-group descendant did not publish its identity")
      })(),
      6_000,
    )
    const eventsByID = new Map<number, ProcessGroup.Event>()
    const events: ProcessGroup.Event[] = []
    let receive: ((event: ProcessGroup.Event) => void) | undefined
    let initialAcquire = true
    const tracker = ProcessGroup.makeWorkerTracker((event) => {
      events.push(event)
      eventsByID.set(event.eventID, event)
      receive?.(event)
    }, 25)
    const worker = { onerror: null as ((event: ErrorEvent) => unknown) | null, terminate: () => {} }
    const owner = ProcessGroup.makeOwner({
      client: {
        subscribe(handler) {
          receive = handler
          return () => (receive = undefined)
        },
        async acknowledge(eventID) {
          const event = eventsByID.get(eventID)
          if (event?.action === "acquire" && !event.cleanupOwed && initialAcquire) {
            initialAcquire = false
            return tracker.acknowledge(eventID)
          }
          if (mode === "rejected") throw new Error("injected ownership acknowledgement rejection")
          await Bun.sleep(100)
          return tracker.acknowledge(eventID)
        },
        async shutdown() {
          return true
        },
      },
      worker,
    })
    const group = { pgid: child.pid, cleanupOwed: false, identity: leader }

    try {
      await tracker.observe({ action: "acquire", ...group })
      expect(owner.has(group.pgid)).toBe(true)
      expect(sameProcess(descendant)).toBe(true)
      await Bun.write(exitFile, "exit")
      await withTimeout(child.exited, 5_000)
      await withTimeout(
        (async () => {
          while (sameProcess(leader)) await Bun.sleep(20)
        })(),
        5_000,
      )

      await expect(tracker.observe({ action: "release", ...group })).rejects.toThrow()
      expect(owner.has(group.pgid)).toBe(false)
      const reacquireEventIndex = events.length
      await expect(tracker.observe({ ...group, action: "acquire", cleanupOwed: true })).rejects.toThrow()
      const reacquireEvent = events
        .slice(reacquireEventIndex)
        .find((event) => event.action === "acquire" && event.cleanupOwed)
      if (!reacquireEvent) throw new Error("reacquire did not publish its initial acquire event")
      await withTimeout(
        (async () => {
          const deadline = Date.now() + 5_000
          while (Date.now() < deadline) {
            if (
              events.some(
                (event) =>
                  event.ownershipSequence === reacquireEvent.ownershipSequence &&
                  event.action === "acquire" &&
                  !event.membersPending &&
                  event.membersSnapshotComplete,
              )
            )
              return
            await Bun.sleep(20)
          }
          throw new Error("reacquire did not publish its resolved member snapshot")
        })(),
        5_500,
      )
      const hasDescendant = () =>
        owner.get(group.pgid)?.members.some(
          (member) =>
            member.pid === descendant.pid &&
            member.bootID === descendant.bootID &&
            member.startTicks === descendant.startTicks,
        ) === true
      expect(hasDescendant()).toBe(true)

      worker.onerror?.(new ErrorEvent("error", { message: "simulated owner exit after reacquire" }))
      await withTimeout(
        (async () => {
          while (sameProcess(descendant)) await Bun.sleep(20)
        })(),
        5_000,
      )
      expect(sameProcess(descendant)).toBe(false)
      await expect(tracker.observe({ ...group, action: "release", cleanupOwed: false })).rejects.toThrow()
      expect(owner.has(group.pgid)).toBe(false)
      expect(events.map((event) => event.identity)).toEqual([leader, leader, leader, leader, leader])
    } finally {
      if (sameProcess(leader) || sameProcess(descendant)) {
        try {
          process.kill(-group.pgid, "SIGKILL")
        } catch {}
      }
      await withTimeout(child.exited, 5_000)
      await owner.stop()
    }
  }
})

test("a newer release rejects an older held snapshot without resurrecting the group", async () => {
  if (process.platform === "win32") return
  const child = Bun.spawn([process.execPath, "-e", "setInterval(() => {}, 1000)"], {
    stdin: "ignore",
    stdout: "ignore",
    stderr: "ignore",
    detached: true,
  })
  const identity = processIdentity(child.pid)
  if (!identity) throw new Error("ordering fixture has no process identity")
  const discovery = Promise.withResolvers<{ members: ProcessIdentity[]; complete: boolean }>()
  const initialEvent = Promise.withResolvers<ProcessGroup.Event>()
  const events: ProcessGroup.Event[] = []
  let receive: ((event: ProcessGroup.Event) => void) | undefined
  let tracker: ProcessGroup.WorkerTracker
  tracker = ProcessGroup.makeWorkerTracker(
    (event) => {
      events.push(event)
      receive?.(event)
      if (events.length === 1) initialEvent.resolve(event)
    },
    5_000,
    () => discovery.promise,
  )
  const worker = { onerror: null as ((event: ErrorEvent) => unknown) | null, terminate: () => {} }
  const owner = ProcessGroup.makeOwner({
    client: {
      subscribe(handler) {
        receive = handler
        return () => (receive = undefined)
      },
      async acknowledge(eventID) {
        return tracker.acknowledge(eventID)
      },
      async shutdown() {
        return true
      },
    },
    worker,
  })
  const group = {
    pgid: child.pid,
    cleanupOwed: true,
    identity,
    members: [],
    membersPending: false,
    membersSnapshotComplete: false,
    generation: 1,
  }

  try {
    const acquire = tracker.observe({ action: "acquire", ...group })
    await withTimeout(initialEvent.promise, 5_000)
    if (sameProcess(identity)) process.kill(-child.pid, "SIGKILL")
    await withTimeout(child.exited, 5_000)
    const release = tracker.observe({
      ...group,
      action: "release",
      cleanupOwed: false,
      membersPending: false,
      membersSnapshotComplete: true,
      generation: 2,
    })
    await release
    expect(events.map((event) => event.action)).toEqual(["acquire", "release"])
    expect(owner.has(child.pid)).toBe(false)
    expect(tracker.size).toBe(1)

    discovery.resolve({ members: [identity], complete: true })
    await acquire
    await withTimeout(
      (async () => {
        while (tracker.size > 0) await Bun.sleep(20)
      })(),
      5_000,
    )
    expect(events.map((event) => event.action)).toEqual(["acquire", "release"])
    expect(owner.has(child.pid)).toBe(false)
    expect(tracker.size).toBe(0)
  } finally {
    discovery.resolve({ members: [identity], complete: true })
    if (sameProcess(identity)) {
      try {
        process.kill(-child.pid, "SIGKILL")
      } catch {}
    }
    await withTimeout(child.exited, 5_000)
    await owner.stop()
  }
})

test("Core-spawned completed commands retire Worker tracker ownership", async () => {
  if (process.platform === "win32") return
  await using directory = await tmpdir()
  const home = directory.path
  const recordFile = path.join(home, "retirement-record.json")
  const resultFile = path.join(home, "retirement-result.json")
  const ownerStartFile = path.join(home, "retirement-owner.json")
  const ownerFile = path.join(import.meta.dir, "worker-process-group-owner.ts")
  const ownerProcess = Bun.spawn([process.execPath, ownerFile], {
    cwd: process.cwd(),
    env: Object.fromEntries(
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
        SWX_TUI_GROUP_DISCOVERY_FAILURE: undefined,
        SWX_TUI_GROUP_DISCOVERY_HOLD_FILE: undefined,
        SWX_TUI_OWNER_ACTION: "retire-many",
        SWX_TUI_OWNER_START_FILE: ownerStartFile,
        SWX_TUI_OWNER_RESULT_FILE: resultFile,
      }).filter((entry): entry is [string, string] => entry[1] !== undefined),
    ),
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    detached: true,
  })
  const output = Promise.all([new Response(ownerProcess.stdout).text(), new Response(ownerProcess.stderr).text()])
  const ownerIdentity = processIdentity(ownerProcess.pid)
  try {
    await withTimeout(
      (async () => {
        while (!(await Bun.file(ownerStartFile).exists())) await Bun.sleep(20)
      })(),
      10_000,
    )
    const result = await withTimeout(
      Promise.race([
        (async () => {
          const deadline = Date.now() + 25_000
          while (Date.now() < deadline) {
            try {
              const value: unknown = await Bun.file(resultFile).json()
              if (typeof value === "object" && value !== null) return { type: "result" as const, value }
            } catch {}
            await Bun.sleep(20)
          }
          throw new Error("retirement fixture did not publish its tracker sizes")
        })(),
        ownerProcess.exited.then((code) => ({ type: "exit" as const, code })),
      ]),
      30_000,
    )
    if (result.type === "exit" && result.code !== 0) {
      const [stdout, stderr] = await withTimeout(output, 5_000)
      throw new Error(`retirement owner exited ${result.code}: ${stdout}\n${stderr}`)
    }
    const resultValue =
      result.type === "result" ? result.value : await Bun.file(resultFile).json().catch(() => undefined)
    if (typeof resultValue !== "object" || resultValue === null || !("sizes" in resultValue))
      throw new Error("retirement owner returned no tracker size list")
    const sizes = Array.isArray(resultValue.sizes)
      ? resultValue.sizes.filter((size): size is number => typeof size === "number")
      : []
    expect(sizes).toEqual(Array.from({ length: 24 }, () => 0))
    expect("ownerSize" in resultValue && resultValue.ownerSize).toBe(0)
    expect(await withTimeout(ownerProcess.exited, 5_000)).toBe(0)
  } finally {
    if (ownerIdentity && sameProcess(ownerIdentity)) {
      try {
        process.kill(ownerProcess.pid, "SIGKILL")
      } catch {}
    }
    await withTimeout(ownerProcess.exited, 5_000).catch(() => undefined)
  }
}, 35_000)
