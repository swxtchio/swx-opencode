/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { testRender } from "@opentui/solid"
import path from "node:path"
import { onCleanup, onMount } from "solid-js"
import { SDKProvider, useSDK } from "../../../src/context/sdk"
import { TestTuiContexts } from "../../fixture/tui-environment"
import { directory } from "../../fixture/tui-sdk"

const testName = "SDKProvider completes a held external SDK request after Bun's idle timeout"

test(
  testName,
  async () => {
    if (process.env.SWX_TUI_SDK_IDLE_CHILD !== "1") {
      const file = path.join(import.meta.dir, "sdk-transport.test.tsx")
      const child = Bun.spawn([process.execPath, "test", "--timeout", "45_000", file, "-t", testName], {
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
      if (!identity?.bootID || !identity.startTicks) {
        child.kill("SIGKILL")
        throw new Error("nested TUI SDK test has no recorded process identity")
      }
      const output = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()])
      let timer: ReturnType<typeof setTimeout> | undefined
      try {
        const outcome = await Promise.race([
          child.exited.then((exitCode) => ({ type: "exit" as const, exitCode })),
          new Promise<{ type: "timeout" }>((resolve) => {
            timer = setTimeout(() => resolve({ type: "timeout" }), 50_000)
          }),
        ])
        if (timer) clearTimeout(timer)
        const [stdout, stderr] = await output
        if (outcome.type === "timeout")
          throw new Error(`nested TUI SDK behavior test timed out\n${stdout}\n${stderr}`)
        if (outcome.exitCode !== 0) throw new Error(`${stdout}\n${stderr}`)
        expect(outcome.exitCode).toBe(0)
      } finally {
        if (timer) clearTimeout(timer)
        const current = EventWriter.readIdentity(identity.pid)
        if (current?.bootID === identity.bootID && current.startTicks === identity.startTicks) {
          try {
            process.kill(-identity.pid, "SIGKILL")
          } catch {}
        }
        await child.exited
        await output
      }
      return
    }

    const eventStarted = Promise.withResolvers<void>()
    const eventCancelled = Promise.withResolvers<void>()
    const eventReceived = Promise.withResolvers<void>()
    const releaseEvent = Promise.withResolvers<void>()
    const controlStarted = Promise.withResolvers<void>()
    const controlCancelled = Promise.withResolvers<void>()
    const encoder = new TextEncoder()
    let eventWasCancelled = false
    const server = Bun.serve({
      port: 0,
      idleTimeout: 30,
      fetch: (request) => {
        const pathname = new URL(request.url).pathname
        if (pathname === "/global/event") {
          eventStarted.resolve()
          return new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(encoder.encode(": ready\n\n"))
              },
              pull(controller) {
                return releaseEvent.promise.then(() => {
                  if (eventWasCancelled) return
                  controller.enqueue(
                    encoder.encode(
                      `data: ${JSON.stringify({
                        directory,
                        payload: { id: "evt_long_call", type: "server.connected", properties: {} },
                      })}\n\n`,
                    ),
                  )
                  controller.close()
                })
              },
              cancel() {
                eventWasCancelled = true
                eventCancelled.resolve()
              },
            }),
            { headers: { "content-type": "text/event-stream" } },
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
        const unsubscribe = client.event.on("event", (event) => {
          if (event.payload.type === "server.connected") eventReceived.resolve()
        })
        onCleanup(unsubscribe)
        resolveMounted?.()
      })
      return <box />
    }

    try {
      app = await testRender(() => (
        <TestTuiContexts>
          <SDKProvider url={server.url.toString()} directory={directory}>
            <Probe />
          </SDKProvider>
        </TestTuiContexts>
      ))
      await mounted
      await eventStarted.promise

      // Stagger the default fetch control so the TUI requests have a clear idle-expiry lead.
      await Bun.sleep(4_000)
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
        eventCancelled.promise.then(() => ({ type: "event-cancelled" as const })),
        Bun.sleep(20_000).then(() => ({ type: "bounded-wait-expired" as const })),
      ])
      expect(first.type).toBe("control-expired")
      if (first.type !== "control-expired") throw new Error(`unexpected long-call outcome: ${first.type}`)
      const control = await controlRequest
      expect(control.type).toBe("failed")
      if (control.type !== "failed") throw new Error("unmodified control fetch unexpectedly completed")
      expect(control.name).toBe("TimeoutError")
      expect(eventWasCancelled).toBe(false)

      releaseEvent.resolve()
      await Promise.race([
        eventReceived.promise,
        Bun.sleep(5_000).then(() => Promise.reject(new Error("TUI SDK did not receive the held event response"))),
      ])
      expect(eventWasCancelled).toBe(false)

      app.renderer.destroy()
    } finally {
      releaseEvent.resolve()
      if (app && !app.renderer.isDestroyed) app.renderer.destroy()
      await server.stop(true)
    }
  },
  60_000,
)
