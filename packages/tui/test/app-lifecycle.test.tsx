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
import { processGroupMember, sameProcess } from "../../core/test/fixture/process-group"

type SighupFixtureMode = "terminal" | "nohup" | "non-tty"
type SighupFixtureResult =
  | {
      type: "terminal"
      terminal: true
      sighupHandlerAdded: true
      rendererDestroyed: boolean
      disposed: number
      sighupListenersRestored: boolean
      terminalTitleCleared: boolean
    }
  | {
      type: "nohup"
      terminal: false
      sighupHandlerAdded: false
      sighupListenersRestored: boolean
      signalHandled: number
    }
  | {
      type: "default"
      terminal: false
      sighupHandlerAdded: false
      exitCode: number
    }

type PipeDrain = { type: "drained"; stdout: string; stderr: string } | { type: "timeout" }

async function drainPipeOutput(output: Promise<[string, string]>, timeoutMs: number): Promise<PipeDrain> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const drained = await Promise.race([
    output.then(([stdout, stderr]) => ({ type: "drained" as const, stdout, stderr })),
    new Promise<{ type: "timeout" }>((resolve) => {
      timer = setTimeout(() => resolve({ type: "timeout" }), timeoutMs)
    }),
  ])
  if (timer) clearTimeout(timer)
  return drained
}

async function bounded<T>(promise: Promise<T>, timeoutMs: number): Promise<T | undefined> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const value = await Promise.race([
    promise,
    new Promise<undefined>((resolve) => {
      timer = setTimeout(() => resolve(undefined), timeoutMs)
    }),
  ])
  if (timer) clearTimeout(timer)
  return value
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
      : mode === "nohup"
        ? Bun.spawn(["nohup", process.execPath, workerFile], options)
        : Bun.spawn(
            [
              "python3",
              "-c",
              "import os, signal, sys; signal.signal(signal.SIGHUP, signal.SIG_DFL); os.execv(sys.argv[1], [sys.argv[1], sys.argv[2]])",
              process.execPath,
              workerFile,
            ],
            options,
          )
  const launcherIdentity = EventWriter.readIdentity(worker.pid)
  if (!launcherIdentity?.bootID || !launcherIdentity.startTicks)
    throw new Error("SIGHUP launcher has no recorded start identity")
  let workerIdentity: EventWriter.ProcessIdentity | undefined
  const output = Promise.all([new Response(worker.stdout).text(), new Response(worker.stderr).text()])
  const drainOutput = () => drainPipeOutput(output, 5_000)
  const cleanupOwned = () => {
    if (workerIdentity && sameProcess(workerIdentity)) {
      try {
        process.kill(workerIdentity.pid, "SIGKILL")
      } catch {}
    }
    if (processGroupMember(launcherIdentity.pid, launcherIdentity)) {
      try {
        process.kill(-launcherIdentity.pid, "SIGKILL")
      } catch {}
      return
    }
    if (sameProcess(launcherIdentity)) {
      try {
        process.kill(launcherIdentity.pid, "SIGKILL")
      } catch {}
    }
  }
  let result: SighupFixtureResult | undefined
  let failure: unknown
  let diagnostics = ""
  let outputDrain: Awaited<ReturnType<typeof drainOutput>> | undefined

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
      if (!ready && !sameProcess(launcherIdentity)) throw new Error("SIGHUP launcher exited before ready")
      if (!ready) await new Promise((resolve) => setTimeout(resolve, 20))
    }
    if (!ready) throw new Error("SIGHUP fixture did not report its readiness")

    workerIdentity = EventWriter.readIdentity(ready.pid)
    if (!workerIdentity?.bootID || !workerIdentity.startTicks || !sameProcess(workerIdentity))
      throw new Error("SIGHUP worker has no current start identity")
    if (ready.terminal !== (mode === "terminal"))
      throw new Error(
        `SIGHUP worker terminal state was wrong for ${mode} (stdin=${ready.stdinTTY}, stdout=${ready.stdoutTTY})`,
      )
    if (mode === "terminal" && !ready.sighupHandlerAdded)
      throw new Error("terminal TUI did not install its SIGHUP lifecycle handler")
    if (mode !== "terminal" && ready.sighupHandlerAdded)
      throw new Error(`${mode} fixture installed a SIGHUP listener despite lacking a terminal`)

    process.kill(workerIdentity.pid, "SIGHUP")
    if (mode === "nohup") {
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

    if (mode === "non-tty") {
      let timer: ReturnType<typeof setTimeout> | undefined
      const exitCode = await Promise.race([
        worker.exited,
        new Promise<undefined>((resolve) => {
          timer = setTimeout(() => resolve(undefined), 5_000)
        }),
      ])
      if (timer) clearTimeout(timer)
      if (exitCode === undefined) throw new Error("non-TTY TUI did not take the default SIGHUP action")
      if (exitCode !== 129) throw new Error(`non-TTY TUI exited ${exitCode}, not from default SIGHUP disposition`)
      if (sameProcess(workerIdentity)) throw new Error("non-TTY TUI survived SIGHUP without a handler")
      if (await Bun.file(resultFile).exists())
        throw new Error("non-TTY TUI performed graceful lifecycle cleanup instead of the default SIGHUP action")
      result = {
        type: "default",
        terminal: false,
        sighupHandlerAdded: false,
        exitCode,
      }
    }

    if (mode !== "non-tty") {
      let timer: ReturnType<typeof setTimeout> | undefined
      const exitCode = await Promise.race([
        worker.exited,
        new Promise<undefined>((resolve) => {
          timer = setTimeout(() => resolve(undefined), 15_000)
        }),
      ])
      if (timer) clearTimeout(timer)
      if (exitCode === undefined) throw new Error("SIGHUP worker did not finish its owned lifecycle")
      cleanupOwned()
      outputDrain = await drainOutput()
      if (outputDrain.type === "timeout")
        throw new Error("SIGHUP launcher exited while a writer still held stdout/stderr open after 5 seconds")
      const { stdout, stderr } = outputDrain
      if (exitCode !== 0) throw new Error(`SIGHUP worker exited ${exitCode}: ${stdout}\n${stderr}`)

      const value: unknown = await Bun.file(resultFile).json()
      if (
        typeof value !== "object" ||
        value === null ||
        !("sighupListenersRestored" in value) ||
        typeof value.sighupListenersRestored !== "boolean"
      )
        throw new Error("SIGHUP fixture did not publish its observed listener state")
      if (mode === "terminal") {
        if (
          !("rendererDestroyed" in value) ||
          typeof value.rendererDestroyed !== "boolean" ||
          !("disposed" in value) ||
          typeof value.disposed !== "number" ||
          !("terminalTitleCleared" in value) ||
          typeof value.terminalTitleCleared !== "boolean"
        )
          throw new Error("terminal TUI did not publish its observed lifecycle result")
        result = {
          type: "terminal",
          terminal: true,
          sighupHandlerAdded: true,
          rendererDestroyed: value.rendererDestroyed,
          disposed: value.disposed,
          sighupListenersRestored: value.sighupListenersRestored,
          terminalTitleCleared: value.terminalTitleCleared,
        }
      } else {
        if (!("signalHandled" in value) || typeof value.signalHandled !== "number")
          throw new Error("nohup SIGHUP fixture did not report its observed signal handling")
        result = {
          type: "nohup",
          terminal: false,
          sighupHandlerAdded: false,
          sighupListenersRestored: value.sighupListenersRestored,
          signalHandled: value.signalHandled,
        }
      }
    }
  } catch (error) {
    failure = error
  } finally {
    cleanupOwned()
    let timer: ReturnType<typeof setTimeout> | undefined
    const closed = await Promise.race([
      worker.exited,
      new Promise<undefined>((resolve) => {
        timer = setTimeout(() => resolve(undefined), 5_000)
      }),
    ])
    if (timer) clearTimeout(timer)
    outputDrain ??= await drainOutput()
    diagnostics =
      outputDrain.type === "drained"
        ? `worker exit=${closed ?? "unconfirmed"}\n${outputDrain.stdout}\n${outputDrain.stderr}`
        : `worker exit=${closed ?? "unconfirmed"}\nstdout/stderr did not drain within 5 seconds`
    if (closed === undefined && failure === undefined) failure = new Error("SIGHUP worker survived fixture cleanup")
    if (outputDrain.type === "timeout" && failure === undefined)
      failure = new Error("SIGHUP launcher exited while a writer still held stdout/stderr open after 5 seconds")
  }
  if (failure) throw new Error(`${failure instanceof Error ? failure.message : String(failure)}\n${diagnostics}`, { cause: failure })
  if (!result) throw new Error(`SIGHUP fixture returned no result\n${diagnostics}`)
  return result
}

test("terminal SIGHUP clears title and disposes scoped resources once", async () => {
  if (process.platform !== "linux") return
  expect(await runSighupFixture("terminal")).toEqual({
    type: "terminal",
    terminal: true,
    sighupHandlerAdded: true,
    rendererDestroyed: true,
    disposed: 1,
    sighupListenersRestored: true,
    terminalTitleCleared: true,
  })
})

test("nohup SIGHUP listener policy preserves process survival without a terminal", async () => {
  if (process.platform !== "linux") return
  expect(await runSighupFixture("nohup")).toEqual({
    type: "nohup",
    terminal: false,
    sighupHandlerAdded: false,
    sighupListenersRestored: true,
    signalHandled: 0,
  })
})

test("non-TTY listener policy leaves SIGHUP at the OS default", async () => {
  if (process.platform !== "linux") return
  expect(await runSighupFixture("non-tty")).toEqual({
    type: "default",
    terminal: false,
    sighupHandlerAdded: false,
    exitCode: 129,
  })
})

test("SIGHUP output drain is bounded when a writer outlives its launcher", async () => {
  if (process.platform !== "linux") return
  await using temp = await tmpdir()
  const writerPIDFile = path.join(temp.path, "writer.pid")
  const exitFile = path.join(temp.path, "launcher.exit")
  const program = [
    "import os,subprocess,time",
    "writer=subprocess.Popen(['sleep','30'])",
    `open(${JSON.stringify(writerPIDFile)},"w").write(str(writer.pid))`,
    `while not os.path.exists(${JSON.stringify(exitFile)}): time.sleep(0.01)`,
  ].join("\n")
  const launcher = Bun.spawn(["python3", "-c", program], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    detached: true,
  })
  const identity = EventWriter.readIdentity(launcher.pid)
  if (!identity?.bootID || !identity.startTicks) throw new Error("pipe fixture launcher has no process identity")
  let writerIdentity: EventWriter.ProcessIdentity | undefined
  const output = Promise.all([new Response(launcher.stdout).text(), new Response(launcher.stderr).text()])
  try {
    const writerPID = await bounded(
      (async () => {
        while (!(await Bun.file(writerPIDFile).exists())) await Bun.sleep(20)
        return Number((await Bun.file(writerPIDFile).text()).trim())
      })(),
      5_000,
    )
    if (!writerPID) throw new Error("pipe fixture did not start its inherited writer")
    writerIdentity = EventWriter.readIdentity(writerPID)
    if (!writerIdentity?.bootID || !writerIdentity.startTicks)
      throw new Error("pipe fixture writer has no process identity")
    await Bun.write(exitFile, "exit")
    expect(await bounded(launcher.exited, 5_000)).toBe(0)
    expect(await drainPipeOutput(output, 100)).toEqual({ type: "timeout" })
  } finally {
    if (writerIdentity && sameProcess(writerIdentity)) {
      try {
        process.kill(writerIdentity.pid, "SIGKILL")
      } catch {}
    }
    if (processGroupMember(launcher.pid, identity)) {
      try {
        process.kill(-launcher.pid, "SIGKILL")
      } catch {}
    }
    await bounded(launcher.exited, 5_000)
    expect((await drainPipeOutput(output, 5_000)).type).toBe("drained")
  }
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
