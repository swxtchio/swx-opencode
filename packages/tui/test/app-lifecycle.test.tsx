import { expect, mock, test } from "bun:test"
import type { TuiPluginApi } from "@opencode-ai/plugin/tui"
import { createTestRenderer } from "@opentui/core/testing"
import { Effect } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { Global } from "@opencode-ai/core/global"
import path from "node:path"
import { tmpdir } from "./fixture/fixture"
import { createTuiResolvedConfig } from "./fixture/tui-runtime"
import { createEventSource, createFetch, directory, json } from "./fixture/tui-sdk"

type SighupFixtureMode = "terminal" | "background"
type SighupFixtureResult = {
  terminal: boolean
  sighupHandlerAdded: boolean
  rendererDestroyed: boolean
  disposed: number
  sighupListenersRestored: boolean
  terminalTitleCleared: boolean
  signalHandled?: number
}

async function runSighupFixture(mode: SighupFixtureMode): Promise<SighupFixtureResult> {
  if (process.platform !== "linux") throw new Error("SIGHUP process fixture requires Linux")
  await using temp = await tmpdir()
  const readyFile = path.join(temp.path, "ready.json")
  const resultFile = path.join(temp.path, "result.json")
  const acknowledgementFile = path.join(temp.path, "acknowledged")
  const workerFile = path.join(
    import.meta.dir,
    "fixture",
    mode === "terminal" ? "sighup-terminal-worker.ts" : "sighup-ignore-worker.ts",
  )
  const env: Record<string, string | undefined> = {
    ...process.env,
    SWX_TUI_SIGHUP_MODE: mode,
    SWX_TUI_SIGHUP_READY_FILE: readyFile,
    SWX_TUI_SIGHUP_RESULT_FILE: resultFile,
    SWX_TUI_SIGHUP_ACK_FILE: acknowledgementFile,
  }
  const options = {
    cwd: process.cwd(),
    env,
    stdin: "ignore" as const,
    stdout: "pipe" as const,
    stderr: "pipe" as const,
    detached: true,
  }
  const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`
  const command = [process.execPath, workerFile].map(quote).join(" ")
  const worker =
    mode === "terminal"
      ? Bun.spawn(["script", "-q", "-e", "-c", command, "/dev/null"], options)
      : Bun.spawn(["nohup", process.execPath, workerFile], options)
  const launcherIdentity = EventWriter.readIdentity(worker.pid)
  if (!launcherIdentity?.bootID || !launcherIdentity.startTicks)
    throw new Error("SIGHUP launcher has no recorded start identity")
  let workerIdentity: EventWriter.ProcessIdentity | undefined
  const sameProcess = (identity: EventWriter.ProcessIdentity) => {
    const current = EventWriter.readIdentity(identity.pid)
    return current?.bootID === identity.bootID && current?.startTicks === identity.startTicks
  }
  const output = Promise.all([new Response(worker.stdout).text(), new Response(worker.stderr).text()])
  let result: SighupFixtureResult | undefined
  let failure: unknown
  let diagnostics = ""

  try {
    const readyDeadline = Date.now() + 15_000
    let ready:
      | { pid: number; terminal: boolean; stdinTTY: boolean; stdoutTTY: boolean; sighupHandlerAdded: boolean }
      | undefined
    while (Date.now() < readyDeadline && !ready) {
      try {
        const value: unknown = await Bun.file(readyFile).json()
        if (
          typeof value === "object" &&
          value !== null &&
          "pid" in value &&
          typeof value.pid === "number" &&
          "terminal" in value &&
          typeof value.terminal === "boolean" &&
          "stdinTTY" in value &&
          typeof value.stdinTTY === "boolean" &&
          "stdoutTTY" in value &&
          typeof value.stdoutTTY === "boolean" &&
          "sighupHandlerAdded" in value &&
          typeof value.sighupHandlerAdded === "boolean"
        ) {
          ready = {
            pid: value.pid,
            terminal: value.terminal,
            stdinTTY: value.stdinTTY,
            stdoutTTY: value.stdoutTTY,
            sighupHandlerAdded: value.sighupHandlerAdded,
          }
        }
      } catch {}
      if (!ready && !sameProcess(launcherIdentity)) {
        const [stdout, stderr] = await output
        throw new Error(`SIGHUP launcher exited before ready:\n${stdout}\n${stderr}`)
      }
      if (!ready) await new Promise((resolve) => setTimeout(resolve, 20))
    }
    if (!ready) throw new Error("SIGHUP worker did not report a mounted TUI")

    workerIdentity = EventWriter.readIdentity(ready.pid)
    if (!workerIdentity?.bootID || !workerIdentity.startTicks || !sameProcess(workerIdentity))
      throw new Error("SIGHUP worker has no current start identity")
    if (ready.terminal !== (mode === "terminal"))
      throw new Error(
        `SIGHUP worker terminal state was wrong for ${mode} (stdin=${ready.stdinTTY}, stdout=${ready.stdoutTTY})`,
      )
    if (mode === "terminal" && !ready.sighupHandlerAdded)
      throw new Error("terminal TUI did not install its SIGHUP lifecycle handler")
    if (mode === "background" && ready.sighupHandlerAdded)
      throw new Error("background TUI installed a SIGHUP listener despite lacking a terminal")

    process.kill(workerIdentity.pid, "SIGHUP")
    if (mode === "background") {
      if (!sameProcess(workerIdentity)) throw new Error("nohup TUI exited after receiving SIGHUP")
      process.kill(workerIdentity.pid, "SIGUSR1")
      const acknowledgementDeadline = Date.now() + 10_000
      let acknowledged = false
      while (Date.now() < acknowledgementDeadline && !acknowledged) {
        try {
          acknowledged = (await Bun.file(acknowledgementFile).text()).trim() === "ready"
        } catch {}
        if (!acknowledged) {
          if (!sameProcess(workerIdentity))
            throw new Error(
              `nohup TUI stopped after receiving SIGHUP (terminal=${ready.terminal}, handlerAdded=${ready.sighupHandlerAdded})`,
            )
          await new Promise((resolve) => setTimeout(resolve, 20))
        }
      }
      if (!acknowledged) throw new Error("nohup TUI did not acknowledge surviving SIGHUP")
      if (!sameProcess(workerIdentity)) throw new Error("nohup TUI stopped before its SIGHUP acknowledgement")
      if (ready.sighupHandlerAdded) throw new Error("background TUI installed a SIGHUP handler over nohup's ignore")
      process.kill(workerIdentity.pid, "SIGUSR2")
    }

    let timer: ReturnType<typeof setTimeout> | undefined
    const exitCode = await Promise.race([
      worker.exited,
      new Promise<undefined>((resolve) => {
        timer = setTimeout(() => resolve(undefined), 15_000)
      }),
    ])
    if (timer) clearTimeout(timer)
    if (exitCode === undefined) throw new Error("SIGHUP worker did not finish its owned lifecycle")
    const [stdout, stderr] = await output
    if (exitCode !== 0) throw new Error(`SIGHUP worker exited ${exitCode}: ${stdout}\n${stderr}`)

    const value: unknown = await Bun.file(resultFile).json()
    if (
      typeof value !== "object" ||
      value === null ||
      !("rendererDestroyed" in value) ||
      typeof value.rendererDestroyed !== "boolean" ||
      !("disposed" in value) ||
      typeof value.disposed !== "number" ||
      !("sighupListenersRestored" in value) ||
      typeof value.sighupListenersRestored !== "boolean" ||
      !("terminalTitleCleared" in value) ||
      typeof value.terminalTitleCleared !== "boolean"
    )
      throw new Error("SIGHUP worker did not publish its lifecycle result")
    const signalHandled = "signalHandled" in value && typeof value.signalHandled === "number" ? value.signalHandled : undefined
    if (mode === "background" && signalHandled === undefined)
      throw new Error("nohup SIGHUP worker did not report its signal handling")
    result = {
      terminal: ready.terminal,
      sighupHandlerAdded: ready.sighupHandlerAdded,
      rendererDestroyed: value.rendererDestroyed,
      disposed: value.disposed,
      sighupListenersRestored: value.sighupListenersRestored,
      terminalTitleCleared: value.terminalTitleCleared,
      ...(signalHandled === undefined ? {} : { signalHandled }),
    }
  } catch (error) {
    failure = error
  } finally {
    if (workerIdentity && sameProcess(workerIdentity)) {
      try {
        process.kill(workerIdentity.pid, "SIGKILL")
      } catch {}
    }
    if (sameProcess(launcherIdentity)) {
      try {
        process.kill(launcherIdentity.pid, "SIGKILL")
      } catch {}
    }
    let timer: ReturnType<typeof setTimeout> | undefined
    const closed = await Promise.race([
      worker.exited,
      new Promise<undefined>((resolve) => {
        timer = setTimeout(() => resolve(undefined), 5_000)
      }),
    ])
    if (timer) clearTimeout(timer)
    const [stdout, stderr] = await output
    diagnostics = `worker exit=${closed ?? "unconfirmed"}\n${stdout}\n${stderr}`
    if (closed === undefined && failure === undefined) failure = new Error("SIGHUP worker survived fixture cleanup")
  }
  if (failure) throw new Error(`${failure instanceof Error ? failure.message : String(failure)}\n${diagnostics}`, { cause: failure })
  if (!result) throw new Error(`SIGHUP fixture returned no result\n${diagnostics}`)
  return result
}

test("terminal SIGHUP clears title and disposes scoped resources once", async () => {
  if (process.platform !== "linux") return
  expect(await runSighupFixture("terminal")).toEqual({
    terminal: true,
    sighupHandlerAdded: true,
    rendererDestroyed: true,
    disposed: 1,
    sighupListenersRestored: true,
    terminalTitleCleared: true,
  })
})

test("nohup SIGHUP stays ignored outside a terminal", async () => {
  if (process.platform !== "linux") return
  expect(await runSighupFixture("background")).toEqual({
    terminal: false,
    sighupHandlerAdded: false,
    rendererDestroyed: false,
    disposed: 0,
    sighupListenersRestored: true,
    terminalTitleCleared: false,
    signalHandled: 0,
  })
})

test("app.exit prints the session epilogue after scoped cleanup", async () => {
  const setup = await createTestRenderer({ width: 80, height: 24, useThread: false })
  const core = await import("@opentui/core")
  mock.module("@opentui/core", () => ({ ...core, createCliRenderer: async () => setup.renderer }))
  const events = createEventSource()
  const calls = createFetch((url) => {
    if (url.pathname === "/session")
      return json([
        {
          id: "dummy",
          title: "Demo session",
          slug: "dummy",
          projectID: "project",
          directory,
          version: "0.0.0-test",
          time: { created: 0, updated: 0 },
        },
      ])
  })
  const originalWrite = process.stdout.write.bind(process.stdout)
  let stdout = ""
  let api: TuiPluginApi | undefined
  let started!: () => void
  const ready = new Promise<void>((resolve) => {
    started = resolve
  })

  process.stdout.write = ((chunk: string | Uint8Array) => {
    stdout += String(chunk)
    return true
  }) as typeof process.stdout.write

  try {
    const { run } = await import("../src/app")
    const task = Effect.runPromise(
      run({
        url: "http://test",
        directory,
        config: createTuiResolvedConfig({ plugin_enabled: {} }),
        fetch: calls.fetch,
        events: events.source,
        args: { continue: true },
        pluginHost: {
          async start(input) {
            api = input.api
            started()
          },
          async dispose() {},
        },
      }).pipe(Effect.provide(AppNodeBuilder.build(Global.node))),
    )

    await ready
    await setup.renderOnce()
    await setup.renderOnce()
    api?.keymap.dispatchCommand("app.exit")
    await task

    expect(stdout).toContain("Demo session")
    expect(stdout).toContain("opencode -s dummy")
  } finally {
    process.stdout.write = originalWrite
    if (!setup.renderer.isDestroyed) setup.renderer.destroy()
    mock.restore()
  }
})

test("fatal startup errors set a nonzero exit after scoped cleanup", async () => {
  const setup = await createTestRenderer({ width: 80, height: 24, useThread: false })
  const core = await import("@opentui/core")
  await mock.module("@opentui/core", () => ({ ...core, createCliRenderer: async () => setup.renderer }))
  const events = createEventSource()
  const calls = createFetch((url) => {
    if (url.pathname === "/config")
      return json(
        {
          name: "ConfigRemoteAuthError",
          data: {
            url: "https://example.com",
            remote: "https://config.example.com/opencode.json",
          },
        },
        { status: 400 },
      )
    return undefined
  })
  let disposes = 0
  const originalWrite = process.stderr.write.bind(process.stderr)
  const originalExitCode = process.exitCode ?? 0
  let stderr = ""

  process.stderr.write = ((chunk: string | Uint8Array) => {
    stderr += String(chunk)
    return true
  }) as typeof process.stderr.write

  try {
    const { run } = await import("../src/app")
    const task = Effect.runPromise(
      run({
        url: "http://test",
        directory,
        config: createTuiResolvedConfig({ plugin_enabled: {} }),
        fetch: calls.fetch,
        events: events.source,
        args: {},
        pluginHost: {
          async start() {},
          async dispose() {
            disposes++
          },
        },
      }).pipe(Effect.provide(AppNodeBuilder.build(Global.node))),
    )

    await task
    expect(stderr).toContain("Run `opencode auth login https://example.com` to re-authenticate.")
    expect(stderr).not.toContain("Unexpected server error")
    expect(process.exitCode).toBe(1)
    expect(setup.renderer.isDestroyed).toBe(true)
    expect(disposes).toBe(1)
  } finally {
    process.stderr.write = originalWrite
    process.exitCode = originalExitCode
    if (!setup.renderer.isDestroyed) setup.renderer.destroy()
    mock.restore()
  }
})
