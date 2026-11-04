import { expect, mock, test } from "bun:test"
import { spawn, type ChildProcess } from "node:child_process"
import { once } from "node:events"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Effect } from "effect"

let captureLateLauncher:
  | ((value: { subprocess: ChildProcess; exitListenerReady: Promise<void> }) => void)
  | undefined

await mock.module("@opencode-ai/core/open", () => ({
  openUrl: async (url: string) => {
    if (new URL(url).pathname === "/later-failure") {
      const subprocess = spawn(
        process.execPath,
        ["-e", `process.stdin.once("data", () => process.exit(29))`],
        { stdio: ["pipe", "ignore", "ignore"] },
      )
      const exitListenerReady = new Promise<void>((resolve) => {
        subprocess.on("newListener", (event) => {
          if (event === "exit") resolve()
        })
      })
      if (!captureLateLauncher) throw new Error("No test is waiting for the delayed launcher")
      captureLateLauncher({ subprocess, exitListenerReady })
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
  const error = await Effect.runPromise(
    Effect.gen(function* () {
      const browser = yield* McpBrowser.Service
      return yield* Effect.flip(browser.open("https://example.com/authorize"))
    }).pipe(Effect.provide(LayerNode.compile(McpBrowser.node))),
  )

  expect(error.message).toBe("Browser open failed with exit code 23")
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
  const launched = new Promise<{ subprocess: ChildProcess; exitListenerReady: Promise<void> }>((resolve) => {
    captureLateLauncher = resolve
  })
  const opening = openBrowserFailure("https://example.com/later-failure")
  const { subprocess, exitListenerReady } = await within(launched, "browser launcher was not created")
  const closed = once(subprocess, "close")

  try {
    await within(exitListenerReady, "browser exit listener was not registered")
    subprocess.stdin?.end("exit")
    const error = await within(opening, "browser open did not settle after the child exited")
    expect(error.message).toBe("Browser open failed with exit code 29")
    const [code, signal] = await within(closed, "browser launcher did not close")
    expect([code, signal]).toEqual([29, null])
  } finally {
    subprocess.stdin?.end("cleanup")
    await within(closed, "browser launcher cleanup did not complete")
    captureLateLauncher = undefined
  }
})

function openBrowserFailure(url: string) {
  return Effect.runPromise(
    Effect.gen(function* () {
      const browser = yield* McpBrowser.Service
      return yield* Effect.flip(browser.open(url))
    }).pipe(Effect.provide(LayerNode.compile(McpBrowser.node))),
  )
}

function within<T>(promise: Promise<T>, message: string) {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), 1_000)
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
