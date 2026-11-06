import { expect, test } from "bun:test"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { readFileSync } from "node:fs"
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
  if (!identity.bootID || !identity.startTicks) return false
  const current = EventWriter.readIdentity(identity.pid)
  return current?.bootID === identity.bootID && current.startTicks === identity.startTicks
}

function processGroupMember(pgid: number, identity: ProcessIdentity | undefined) {
  if (!identity || !sameProcess(identity)) return false
  try {
    const stat = readFileSync(`/proc/${identity.pid}/stat`, "utf8")
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ")
    const current = EventWriter.readIdentity(identity.pid)
    return current?.bootID === identity.bootID && current.startTicks === identity.startTicks && Number(fields[2]) === pgid
  } catch {
    return false
  }
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
    const initialized = Bun.spawnSync(["git", "init", "-q"], { cwd: home.path, timeout: 5_000 })
    if (initialized.exitCode !== 0) throw new Error(`could not initialize isolated TUI project: ${initialized.stderr}`)
    const child = Bun.spawn(["script", "-q", "-e", "-c", `${quote(process.execPath)} ${quote(entry)}`, "/dev/null"], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        GIT_DIR: path.join(home.path, ".git"),
        GIT_WORK_TREE: process.cwd(),
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
    const output = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()])
    let tui: ProcessIdentity | undefined
    let group: ProcessGroupFixture | undefined
    let failure: unknown

    try {
      if (!launcher?.bootID || !launcher.startTicks)
        throw new Error("TUI terminal launcher has no start identity; cleanup skipped")
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
      while (sameProcess(group.leader) || sameProcess(group.descendant)) {
        if (Date.now() >= deadline) throw new Error(`terminal SIGHUP left Worker group ${group.pgid} alive`)
        await Bun.sleep(20)
      }
      expect(sameProcess(group.leader)).toBe(false)
      expect(sameProcess(group.descendant)).toBe(false)
      expect(sameProcess(tui)).toBe(true)

      await Bun.write(exitGateFile, "release")
      const exitCode = await withTimeout(child.exited, 15_000)
      const [stdout, stderr] = await withTimeout(output, 5_000)
      if (exitCode !== 0) throw new Error(`terminal TUI exited ${exitCode}: ${stdout}\n${stderr}`)
    } catch (error) {
      failure = error
    } finally {
      await Bun.write(exitGateFile, "release")
      if (group && (sameProcess(group.leader) || sameProcess(group.descendant))) {
        try {
          process.kill(-group.pgid, "SIGKILL")
        } catch {}
      }
      if (processGroupMember(child.pid, launcher) || processGroupMember(child.pid, tui)) {
        try {
          process.kill(-child.pid, "SIGKILL")
        } catch {}
      } else if (groupAlive(child.pid)) {
        throw new Error(`cannot clean PTY process group ${child.pid} without a current recorded member identity`)
      }
      if (tui && sameProcess(tui)) {
        try {
          process.kill(tui.pid, "SIGKILL")
        } catch {}
      }
      const exitCode = await withTimeout(child.exited, 5_000).then(
        (value) => value,
        () => undefined,
      )
      const exited = exitCode !== undefined
      const drained = await withTimeout(output, 5_000).then(
        () => true,
        () => false,
      )
      if (!exited) throw new Error("TUI hangup fixture child survived identity-owned cleanup")
      if (!drained) throw new Error("TUI hangup fixture output did not drain after cleanup")
      if (failure) {
        const [stdout, stderr] = await withTimeout(output, 5_000)
        const files = await Promise.all(
          [recordFile, groupReadyFile, signalReadyFile, stoppedFile].map((file) => Bun.file(file).exists()),
        )
        throw new Error(
          `${failure instanceof Error ? failure.message : String(failure)}\nexit=${exitCode}\nfiles=${files.join(",")}\n${stdout}\n${stderr}`,
        )
      }
    }
  },
  90_000,
)
