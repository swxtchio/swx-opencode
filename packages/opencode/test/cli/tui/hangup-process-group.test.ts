import { expect, test } from "bun:test"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { tmpdir } from "../../fixture/fixture"
import { withTimeout } from "../../../src/util/timeout"
import path from "node:path"

type ProcessIdentity = EventWriter.ProcessIdentity
type ProcessGroupFixture = {
  readonly leader: ProcessIdentity
  readonly descendant: ProcessIdentity
  readonly pgid: number
}

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

async function waitForFile(file: string, parse: (value: string) => unknown) {
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    try {
      const value = await Bun.file(file).text()
      const result = parse(value)
      if (result !== undefined) return result
    } catch {}
    await Bun.sleep(20)
  }
  throw new Error(`TUI hangup fixture did not publish ${file}`)
}

test(
  "terminal SIGHUP stops a live Worker-owned group through TuiThreadCommand's run finally",
  async () => {
    if (process.platform !== "linux") return
    await using home = await tmpdir()
    const recordFile = path.join(home.path, "tui-hangup-process-group.json")
    const groupReadyFile = path.join(home.path, "tui-hangup-group-ready.json")
    const signalReadyFile = path.join(home.path, "tui-hangup-signal-ready")
    const stoppedFile = path.join(home.path, "tui-hangup-stopped")
    const exitGateFile = path.join(home.path, "tui-hangup-exit-gate")
    const entry = path.join(import.meta.dir, "../../../src/index.ts")
    const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`
    const child = Bun.spawn(["script", "-q", "-e", "-c", `${quote(process.execPath)} ${quote(entry)}`, "/dev/null"], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        HOME: home.path,
        OPENCODE_TEST_HOME: home.path,
        XDG_CONFIG_HOME: path.join(home.path, ".config"),
        XDG_DATA_HOME: path.join(home.path, ".local/share"),
        XDG_STATE_HOME: path.join(home.path, ".local/state"),
        XDG_CACHE_HOME: path.join(home.path, ".cache"),
        OPENCODE_DB: ":memory:",
        OPENCODE_CONFIG_CONTENT: "{}",
        OPENCODE_DISABLE_PROJECT_CONFIG: "1",
        OPENCODE_PURE: "1",
        OPENCODE_DISABLE_AUTOUPDATE: "1",
        OPENCODE_DISABLE_AUTOCOMPACT: "1",
        OPENCODE_DISABLE_MODELS_FETCH: "1",
        OPENCODE_AUTH_CONTENT: "{}",
        SWX_TUI_GROUP_TUI_TEST: "1",
        SWX_TUI_GROUP_RECORD_FILE: recordFile,
        SWX_TUI_GROUP_TUI_READY_FILE: groupReadyFile,
        SWX_TUI_GROUP_TUI_STOP_FILE: stoppedFile,
        SWX_TUI_GROUP_TUI_EXIT_GATE_FILE: exitGateFile,
        SWX_TUI_SIGHUP_READY_FILE: signalReadyFile,
      },
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      detached: true,
    })
    const launcher = EventWriter.readIdentity(child.pid)
    if (!launcher?.bootID || !launcher.startTicks) {
      child.kill("SIGKILL")
      throw new Error("TUI terminal launcher has no start identity")
    }
    const output = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()])
    let tui: ProcessIdentity | undefined
    let group: ProcessGroupFixture | undefined

    try {
      const groupReady = await waitForFile(groupReadyFile, (value) => JSON.parse(value))
      const signalReady = await waitForFile(signalReadyFile, (value) => {
        const pid = Number(value)
        return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined
      })
      group = await waitForFile(recordFile, (value) => processGroupFixture(JSON.parse(value)))
      if (!group) throw new Error("Worker did not record its group identities")
      tui = EventWriter.readIdentity(signalReady as number)
      if (!tui?.bootID || !tui.startTicks) throw new Error("TUI owner did not retain its start identity")
      if (typeof groupReady !== "object" || groupReady === null || !("pgid" in groupReady))
        throw new Error("TUI owner did not publish its Worker group identity")
      expect(groupReady.pgid).toBe(group.pgid)
      expect(sameProcess(group.leader)).toBe(true)
      expect(sameProcess(group.descendant)).toBe(true)
      expect(sameProcess(tui)).toBe(true)

      process.kill(tui.pid, "SIGHUP")
      await waitForFile(stoppedFile, (value) => (value.trim() === "stopped" ? value : undefined))
      const deadline = Date.now() + 10_000
      while (groupAlive(group.pgid)) {
        if (Date.now() >= deadline) throw new Error(`terminal SIGHUP left Worker group ${group.pgid} alive`)
        await Bun.sleep(20)
      }
      expect(sameProcess(group.leader)).toBe(false)
      expect(sameProcess(group.descendant)).toBe(false)
      expect(sameProcess(tui)).toBe(true)

      await Bun.write(exitGateFile, "release")
      const exitCode = await withTimeout(child.exited, 15_000)
      const [stdout, stderr] = await output
      if (exitCode !== 0) throw new Error(`terminal TUI exited ${exitCode}: ${stdout}\n${stderr}`)
    } finally {
      await Bun.write(exitGateFile, "release")
      if (group && groupAlive(group.pgid)) {
        try {
          process.kill(-group.pgid, "SIGKILL")
        } catch {}
      }
      if (tui && sameProcess(tui)) {
        try {
          process.kill(tui.pid, "SIGKILL")
        } catch {}
      }
      if (sameProcess(launcher)) {
        try {
          process.kill(-launcher.pid, "SIGKILL")
        } catch {}
      }
      await child.exited
      await output
    }
  },
  90_000,
)
