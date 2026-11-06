/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { testRender } from "@opentui/solid"
import path from "node:path"
import { onMount } from "solid-js"
import { SDKProvider, useSDK } from "../../../src/context/sdk"
import { TestTuiContexts } from "../../fixture/tui-environment"
import { directory } from "../../fixture/tui-sdk"

const testName = "SDKProvider completes a held external SDK request after Bun's idle timeout"

async function waitWithin<T>(promise: Promise<T>, timeoutMs: number, description: string) {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${description} exceeded ${timeoutMs}ms`)), timeoutMs)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

function sameProcess(identity: EventWriter.ProcessIdentity) {
  if (!identity.bootID || !identity.startTicks) return false
  const current = EventWriter.readIdentity(identity.pid)
  return current?.bootID === identity.bootID && current.startTicks === identity.startTicks
}

async function cleanupChild(input: {
  pid: number
  exited: Promise<number>
  identity: EventWriter.ProcessIdentity | undefined
  output: Promise<[string, string]>
}) {
  if (input.identity?.bootID && input.identity.startTicks && sameProcess(input.identity)) {
    try {
      process.kill(-input.pid, "SIGKILL")
    } catch {}
  }
  const exited = await waitWithin(input.exited, 5_000, "nested TUI SDK child exit").then(
    () => true,
    () => false,
  )
  const drained = await waitWithin(input.output, 5_000, "nested TUI SDK output drain").then(
    () => true,
    () => false,
  )
  if (!exited) throw new Error("nested TUI SDK child survived identity-owned cleanup")
  if (!drained) throw new Error("nested TUI SDK child output did not drain after cleanup")
}

test(
  testName,
  async () => {
    if (process.env.SWX_TUI_SDK_IDLE_CHILD !== "1") {
      if (process.platform !== "linux") return
      const file = path.join(import.meta.dir, "sdk-transport.test.tsx")
      const child = Bun.spawn([process.execPath, "test", "--timeout", "35_000", file, "-t", testName], {
        cwd: process.cwd(),
        env: {
          ...process.env,
          BUN_CONFIG_HTTP_IDLE_TIMEOUT: "2",
          SWX_TUI_SDK_IDLE_CHILD: "1",
        },
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
        detached: true,
      })
      const identity = EventWriter.readIdentity(child.pid)
      const output = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()])
      let timer: ReturnType<typeof setTimeout> | undefined
      try {
        if (!identity?.bootID || !identity.startTicks)
          throw new Error("nested TUI SDK test has no recorded process identity; cleanup skipped")
        const outcome = await Promise.race([
          child.exited.then((exitCode) => ({ type: "exit" as const, exitCode })),
          new Promise<{ type: "timeout" }>((resolve) => {
            timer = setTimeout(() => resolve({ type: "timeout" }), 40_000)
          }),
        ])
        if (timer) clearTimeout(timer)
        if (outcome.type === "timeout") throw new Error("nested TUI SDK behavior test timed out")
        const [stdout, stderr] = await waitWithin(output, 5_000, "nested TUI SDK output drain")
        if (outcome.exitCode !== 0) throw new Error(`${stdout}\n${stderr}`)
        expect(outcome.exitCode).toBe(0)
      } finally {
        if (timer) clearTimeout(timer)
        await cleanupChild({ pid: child.pid, exited: child.exited, identity, output })
      }
      return
    }

    const eventStarted = Promise.withResolvers<void>()
    const controlStarted = Promise.withResolvers<void>()
    const controlCancelled = Promise.withResolvers<void>()
    const healthStarted = Promise.withResolvers<void>()
    const healthCancelled = Promise.withResolvers<void>()
    const releaseHealth = Promise.withResolvers<void>()
    const cancelHealthStarted = Promise.withResolvers<void>()
    const cancelHealthCancelled = Promise.withResolvers<void>()
    const encoder = new TextEncoder()
    let healthWasCancelled = false
    let cancelHealthWasCancelled = false
    let healthRequests = 0
    const events = {
      async subscribe() {
        eventStarted.resolve()
        return () => {}
      },
    }
    const server = Bun.serve({
      port: 0,
      idleTimeout: 30,
      fetch: (request) => {
        const pathname = new URL(request.url).pathname
        if (pathname === "/global/health") {
          healthRequests += 1
          if (healthRequests === 1) {
            healthStarted.resolve()
            return new Response(
              new ReadableStream<Uint8Array>({
                start(controller) {
                  controller.enqueue(encoder.encode('{"healthy":true,"version":"'))
                },
                pull(controller) {
                  return releaseHealth.promise.then(() => {
                    if (healthWasCancelled) return
                    controller.enqueue(encoder.encode('test"}'))
                    controller.close()
                  })
                },
                cancel() {
                  healthWasCancelled = true
                  healthCancelled.resolve()
                },
              }),
              { headers: { "content-type": "application/json" } },
            )
          }
          cancelHealthStarted.resolve()
          return new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(encoder.encode("cancel-ready:"))
              },
              pull: () => new Promise<void>(() => {}),
              cancel() {
                cancelHealthWasCancelled = true
                cancelHealthCancelled.resolve()
              },
            }),
            { headers: { "content-type": "text/plain" } },
          )
        }
        if (pathname === "/control") {
          controlStarted.resolve()
          return new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(encoder.encode("control-ready:"))
              },
              pull: () => new Promise<void>(() => {}),
              cancel() {
                controlCancelled.resolve()
              },
            }),
            { headers: { "content-type": "text/plain" } },
          )
        }
        return new Response(`unexpected path: ${pathname}`, { status: 404 })
      },
    })

    let app: Awaited<ReturnType<typeof testRender>> | undefined
    let resolveMounted: (() => void) | undefined
    const mounted = new Promise<void>((resolve) => {
      resolveMounted = resolve
    })
    let client!: ReturnType<typeof useSDK>
    function Probe() {
      client = useSDK()
      onMount(() => {
        resolveMounted?.()
      })
      return <box />
    }

    try {
      app = await testRender(() => (
        <TestTuiContexts>
          <SDKProvider url={server.url.toString()} directory={directory} events={events}>
            <Probe />
          </SDKProvider>
        </TestTuiContexts>
      ))
      await mounted
      await eventStarted.promise

      // Stagger the default fetch control so the TUI requests have a clear idle-expiry lead.
      await Bun.sleep(4_000)
      const healthRequest = client.client.global.health()
      await healthStarted.promise
      const rawFetch = globalThis.fetch
      const controlRequest = rawFetch(new URL("/control", server.url))
        .then(async (response) => ({ type: "completed" as const, body: await response.text() }))
        .catch((error: unknown) => ({
          type: "failed" as const,
          name: error instanceof Error ? error.name : "unknown",
        }))
      await controlStarted.promise
      const first = await Promise.race([
        controlCancelled.promise.then(() => ({ type: "control-expired" as const })),
        healthCancelled.promise.then(() => ({ type: "health-cancelled" as const })),
        Bun.sleep(20_000).then(() => ({ type: "bounded-wait-expired" as const })),
      ])
      expect(first.type).toBe("control-expired")
      if (first.type !== "control-expired") throw new Error(`unexpected long-call outcome: ${first.type}`)
      const control = await controlRequest
      expect(control.type).toBe("failed")
      if (control.type !== "failed") throw new Error("unmodified control fetch unexpectedly completed")
      expect(control.name).toBe("TimeoutError")
      await Bun.sleep(500)
      expect(healthWasCancelled).toBe(false)

      releaseHealth.resolve()
      const health = await Promise.race([
        healthRequest,
        Bun.sleep(5_000).then(() => Promise.reject(new Error("ordinary TUI SDK call did not finish after release"))),
      ])
      expect(health.response.ok).toBe(true)
      expect(health.data?.healthy).toBe(true)

      const cancelRequest = client.client.global.health().catch(() => undefined)
      await cancelHealthStarted.promise
      app.renderer.destroy()
      await cancelHealthCancelled.promise
      await cancelRequest
      expect(cancelHealthWasCancelled).toBe(true)
    } finally {
      releaseHealth.resolve()
      if (app && !app.renderer.isDestroyed) app.renderer.destroy()
      await server.stop(true)
    }
  },
  60_000,
)

test("SDKProvider keeps ordinary external transport and caller cancellation portable", async () => {
  const mounted = Promise.withResolvers<void>()
  const heldStarted = Promise.withResolvers<void>()
  const heldCancelled = Promise.withResolvers<void>()
  const encoder = new TextEncoder()
  let healthRequests = 0
  const server = Bun.serve({
    port: 0,
    idleTimeout: 30,
    fetch: () => {
      healthRequests += 1
      if (healthRequests === 1) return Response.json({ healthy: true, version: "portable" })
      heldStarted.resolve()
      return new Response(
        new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(encoder.encode("partial"))
          },
          pull: () => new Promise<void>(() => {}),
          cancel() {
            heldCancelled.resolve()
          },
        }),
        { headers: { "content-type": "application/json" } },
      )
    },
  })

  const events = {
    async subscribe() {
      return () => {}
    },
  }
  let app: Awaited<ReturnType<typeof testRender>> | undefined
  let client!: ReturnType<typeof useSDK>
  function Probe() {
    client = useSDK()
    onMount(() => mounted.resolve())
    return <box />
  }

  const abort = new AbortController()
  try {
    app = await testRender(() => (
      <TestTuiContexts>
        <SDKProvider url={server.url.toString()} directory={directory} events={events}>
          <Probe />
        </SDKProvider>
      </TestTuiContexts>
    ))
    await mounted.promise
    const health = await client.client.global.health()
    expect(health.data?.healthy).toBe(true)

    const cancellation = client.client.global.health({ signal: abort.signal }).then(
      () => "completed",
      (error: unknown) => (error instanceof Error ? error.name : "unknown"),
    )
    await heldStarted.promise
    abort.abort()
    await heldCancelled.promise
    await cancellation
    expect(healthRequests).toBe(2)
  } finally {
    abort.abort()
    if (app && !app.renderer.isDestroyed) app.renderer.destroy()
    await server.stop(true)
  }
}, 15_000)

test("nested SDK fixture bounds identity-owned cleanup when a child holds its output pipes", async () => {
  if (process.platform !== "linux") return
  const child = Bun.spawn([process.execPath, "-e", "process.stdout.write('held-ready\\n'); setInterval(() => {}, 1000)"], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    detached: true,
  })
  const identity = EventWriter.readIdentity(child.pid)
  const ready = Promise.withResolvers<void>()
  const stdout = (async () => {
    const reader = child.stdout.getReader()
    const decoder = new TextDecoder()
    let output = ""
    while (true) {
      const result = await reader.read()
      if (result.done) return output
      output += decoder.decode(result.value)
      if (output.includes("held-ready")) ready.resolve()
    }
  })()
  const output = Promise.all([stdout, new Response(child.stderr).text()])

  try {
    if (!identity?.bootID || !identity.startTicks) throw new Error("held-pipe child has no recorded process identity")
    await waitWithin(ready.promise, 5_000, "held-pipe child readiness")
    expect(sameProcess(identity)).toBe(true)
    const exitOutcome = await waitWithin(child.exited, 100, "held-pipe child exit").then(
      () => "exited",
      () => "timed-out",
    )
    expect(exitOutcome).toBe("timed-out")
    await cleanupChild({ pid: child.pid, exited: child.exited, identity, output })
    expect(sameProcess(identity)).toBe(false)
  } finally {
    if (identity?.bootID && identity.startTicks && sameProcess(identity)) {
      try {
        process.kill(-identity.pid, "SIGKILL")
      } catch {}
    }
    await waitWithin(child.exited, 5_000, "held-pipe child exit").catch(() => undefined)
    await waitWithin(output, 5_000, "held-pipe output drain").catch(() => undefined)
  }
}, 20_000)
