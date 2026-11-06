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
  if (!identity.bootID || !identity.startTicks) return false
  const current = EventWriter.readIdentity(identity.pid)
  return current?.bootID === identity.bootID && current.startTicks === identity.startTicks
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
    if (startup.type === "exit") {
      const [stdout, stderr] = await withTimeout(output, 5_000)
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
      const [stdout, stderr] = await withTimeout(output, 5_000)
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
      const [stdout, stderr] = await withTimeout(output, 5_000)
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
    while (sameProcess(fixture.leader) || sameProcess(fixture.descendant)) {
      if (Date.now() >= deadline) throw new Error(`${input.action} left process group ${fixture.pgid} alive`)
      await Bun.sleep(20)
    }
    expect(sameProcess(fixture.leader)).toBe(false)
    expect(sameProcess(fixture.descendant)).toBe(false)
  } finally {
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
    const selected = cases.filter(
      (scenario) => !process.env.SWX_TUI_GROUP_CASE || process.env.SWX_TUI_GROUP_CASE === scenario.action,
    )
    await Promise.all(selected.slice(0, 3).map(runOwnerCase))
    await Promise.all(selected.slice(3, 6).map(runOwnerCase))
    await Promise.all(selected.slice(6).map(runOwnerCase))
  },
  120_000,
)

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
      const forwarded: Array<NodeJS.Signals | number | undefined> = []
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
      expect(owner.get(group.pgid)?.identity).toEqual(identity)
      await expect(tracker.observe({ ...group, action: "release", cleanupOwed: false })).rejects.toThrow()
      expect(owner.has(group.pgid)).toBe(false)
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
      await expect(tracker.observe({ ...group, action: "acquire", cleanupOwed: true })).rejects.toThrow()
      expect(
        owner.get(group.pgid)?.members.some(
          (member) =>
            member.pid === descendant.pid &&
            member.bootID === descendant.bootID &&
            member.startTicks === descendant.startTicks,
        ),
      ).toBe(true)

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
      expect(events.map((event) => event.identity)).toEqual([leader, leader, leader, leader])
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
