/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import { testRender } from "@opentui/solid"
import { onMount } from "solid-js"
import { SDKProvider, useSDK } from "../../../src/context/sdk"
import { TestTuiContexts } from "../../fixture/tui-environment"
import { directory } from "../../fixture/tui-sdk"

test("SDKProvider uses the long-call fetch policy for its real event client", async () => {
  const requestStarted = Promise.withResolvers<void>()
  const bodyReleased = Promise.withResolvers<void>()
  const server = Bun.serve({
    port: 0,
    fetch: (request) => {
      const pathname = new URL(request.url).pathname
      if (pathname !== "/global/event") return new Response(`unexpected path: ${pathname}`, { status: 404 })
      requestStarted.resolve()
      return new Response(
        new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(": ready\n\n"))
          },
          pull: () => new Promise<void>(() => {}),
          cancel() {
            bodyReleased.resolve()
          },
        }),
        { headers: { "content-type": "text/event-stream" } },
      )
    },
  })
  const originalFetch = globalThis.fetch
  let timeoutDisabled = false
  globalThis.fetch = ((input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    timeoutDisabled = init !== undefined && "timeout" in init && init.timeout === false
    return originalFetch(input, init)
  }) as typeof globalThis.fetch

  let app: Awaited<ReturnType<typeof testRender>> | undefined
  let mounted!: () => void
  const ready = new Promise<void>((resolve) => {
    mounted = resolve
  })
  let client!: ReturnType<typeof useSDK>
  function Probe() {
    client = useSDK()
    onMount(mounted)
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
    await ready
    let requestTimer: ReturnType<typeof setTimeout> | undefined
    try {
      await Promise.race([
        requestStarted.promise,
        new Promise<never>((_, reject) => {
          requestTimer = setTimeout(() => reject(new Error("SDK event request did not reach the local server")), 5_000)
        }),
      ])
    } finally {
      if (requestTimer) clearTimeout(requestTimer)
    }
    expect(client.url).toBe(server.url.toString())
    expect(timeoutDisabled).toBe(true)

    app.renderer.destroy()
    let releaseTimer: ReturnType<typeof setTimeout> | undefined
    try {
      await Promise.race([
        bodyReleased.promise,
        new Promise<never>((_, reject) => {
          releaseTimer = setTimeout(() => reject(new Error("aborted SDK event body was not released")), 5_000)
        }),
      ])
    } finally {
      if (releaseTimer) clearTimeout(releaseTimer)
    }
  } finally {
    if (app && !app.renderer.isDestroyed) app.renderer.destroy()
    globalThis.fetch = originalFetch
    await server.stop(true)
  }
})
