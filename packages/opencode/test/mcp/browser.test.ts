import { expect, mock, test } from "bun:test"
import { spawn, type ChildProcess } from "node:child_process"
import { once } from "node:events"
import { createInterface } from "node:readline"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Cause, Effect, Exit } from "effect"

type LateLauncher = {
  subprocess: ChildProcess
  childReady: Promise<void>
  exitListenerReady: Promise<void>
  exitReleased: Promise<void>
  closeOutput: () => void
}

let captureLateLauncher:
  | ((value: LateLauncher) => void)
  | undefined

await mock.module("@opencode-ai/core/open", () => ({
  openUrl: async (url: string) => {
    if (new URL(url).pathname === "/later-failure") {
      if (!captureLateLauncher) throw new Error("No test is waiting for the delayed launcher")
      const subprocess = spawn(
        process.execPath,
        [
          "-e",
          `process.stdin.once("data", () => process.stdout.write("released\\n", () => process.exit(29))); process.stdout.write("ready\\n")`,
        ],
        { stdio: ["pipe", "pipe", "ignore"] },
      )
      const output = createInterface({ input: subprocess.stdout, crlfDelay: Infinity })
      const lines = output[Symbol.asyncIterator]()
      const readLine = async (expected: string) => {
        const line = await lines.next()
        if (line.done || line.value !== expected) throw new Error(`Expected child acknowledgement ${expected}`)
      }
      const childReady = readLine("ready")
      const exitReleased = childReady.then(() => readLine("released"))
      const exitListenerReady = new Promise<void>((resolve, reject) => {
        subprocess.on("newListener", (event) => {
          if (event !== "exit") return
          queueMicrotask(() => {
            if (subprocess.listenerCount("exit") === 0)
              return reject(new Error("Production browser exit listener was not installed"))
            resolve()
            subprocess.stdin?.end("exit\n")
          })
        })
      })
      captureLateLauncher({ subprocess, childReady, exitListenerReady, exitReleased, closeOutput: () => output.close() })
      await within(childReady, "browser child did not acknowledge readiness")
      return subprocess
    }
    const code = new URL(url).pathname === "/successful" ? 0 : 23
    const subprocess = spawn(process.execPath, ["-e", `process.exit(${code})`], { stdio: "ignore" })
    await once(subprocess, "close")
    return subprocess
  },
}))

const { McpBrowser } = await import("../../src/mcp/browser")

test("reports a browser launcher that exited before openUrl returned", async () => {
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const browser = yield* McpBrowser.Service
      return yield* Effect.exit(browser.open("https://example.com/authorize"))
    }).pipe(Effect.provide(LayerNode.compile(McpBrowser.node))),
  )

  const error = Exit.isFailure(result) ? Cause.prettyErrors(result.cause)[0] : undefined
  expect(error?.message).toBe("Browser open failed with exit code 23")
})

test("accepts a browser launcher that exited successfully before openUrl returned", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const browser = yield* McpBrowser.Service
      yield* browser.open("https://example.com/successful")
    }).pipe(Effect.provide(LayerNode.compile(McpBrowser.node))),
  )
})

test("reports a browser launcher that exits after openUrl returns", async () => {
  const launched = new Promise<LateLauncher>((resolve) => (captureLateLauncher = resolve))
  const opening = Effect.runPromise(
    Effect.gen(function* () {
      const browser = yield* McpBrowser.Service
      return yield* Effect.flip(browser.open("https://example.com/later-failure"))
    }).pipe(Effect.provide(LayerNode.compile(McpBrowser.node))),
  ).then(
    (error) => ({ _tag: "browser-error" as const, error }),
    (cause) => ({ _tag: "unexpected-result" as const, cause }),
  )
  const { subprocess, childReady, exitListenerReady, exitReleased, closeOutput } = await within(
    launched,
    "browser launcher was not created",
  )
  const closed = once(subprocess, "close")

  try {
    await within(childReady, "browser child did not acknowledge readiness")
    await within(exitListenerReady, "browser exit listener was not registered")
    await within(exitReleased, "browser child did not acknowledge exit release")
    const result = await within(opening, "browser open did not settle after the child exited")
    expect(result).toMatchObject({
      _tag: "browser-error",
      error: { message: "Browser open failed with exit code 29" },
    })
    const [code, signal] = await within(closed, "browser launcher did not close")
    expect([code, signal]).toEqual([29, null])
  } finally {
    if (subprocess.exitCode === null && subprocess.signalCode === null) subprocess.stdin?.end("cleanup\n")
    await within(closed, "browser launcher cleanup did not complete")
    closeOutput()
    captureLateLauncher = undefined
  }
})

function within<T>(promise: Promise<T>, message: string) {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), 5_000)
    promise.then(
      (value) => {
        clearTimeout(timer)
        resolve(value)
      },
      (error) => {
        clearTimeout(timer)
        reject(error)
      },
    )
  })
}
