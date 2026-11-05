import { expect, mock, test } from "bun:test"
import path from "node:path"

const testName = "runMini interactive attached runtime uses its no-timeout SDK for successful and cancellable calls"

if (process.env.SWX_MINI_INTERACTIVE_CHILD !== "1") {
  test.skip(testName, () => {})
} else {
  const requestFile = process.env.SWX_MINI_INTERACTIVE_REQUEST_FILE
  const controlURL = process.env.SWX_MINI_INTERACTIVE_CONTROL_URL
  const controlFile = process.env.SWX_MINI_INTERACTIVE_CONTROL_FILE
  const resultFile = process.env.SWX_MINI_INTERACTIVE_RESULT_FILE
  const attach = process.env.SWX_MINI_INTERACTIVE_ATTACH_URL
  const session = process.env.SWX_MINI_INTERACTIVE_SESSION
  if (!requestFile || !controlURL || !controlFile || !resultFile || !attach || !session)
    throw new Error("interactive Mini attach worker inputs are missing")

  type HealthResult = {
    readonly response: Response
    readonly data: { readonly healthy: true; readonly version: string }
  }
  type SDK = {
    readonly global: {
      health(options?: { signal?: AbortSignal }): Promise<HealthResult>
    }
  }

  const rawFetch = globalThis.fetch
  let healthRequests: { timeoutDisabled: boolean; hasSignal: boolean }[] = []
  let expectedSignal: AbortSignal | undefined
  globalThis.fetch = (async (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    const request = input instanceof Request ? input : undefined
    const url = new URL(request?.url ?? String(input))
    if (url.pathname === "/global/health") {
      healthRequests = [
        ...healthRequests,
        {
          timeoutDisabled: init !== undefined && "timeout" in init && init.timeout === false,
          hasSignal:
            expectedSignal !== undefined && (init?.signal === expectedSignal || request?.signal === expectedSignal),
        },
      ]
      await Bun.write(requestFile, JSON.stringify(healthRequests))
    }
    return rawFetch(input, init)
  }) as typeof globalThis.fetch

  const runtime = path.resolve(import.meta.dir, "../../../src/cli/cmd/run/runtime.ts")
  void mock.module(runtime, () => ({
    runInteractiveMode: async (input: { sdk: SDK }) => {
      const healthRequest = input.sdk.global.health()
      const requestDeadline = Date.now() + 10_000
      while (Date.now() < requestDeadline) {
        if (healthRequests.length > 0) break
        await Bun.sleep(20)
      }
      if (healthRequests.length === 0) throw new Error("interactive runtime did not issue an ordinary SDK request")

      const control = await rawFetch(controlURL)
        .then(async (response) => ({ type: "completed" as const, body: await response.text() }))
        .catch((error: unknown) => ({
          type: "failed" as const,
          name: error instanceof Error ? error.name : "unknown",
        }))
      await Bun.write(controlFile, JSON.stringify(control))
      if (control.type !== "failed" || control.name !== "TimeoutError")
        throw new Error(`interactive control fetch did not expire: ${JSON.stringify(control)}`)

      const first = await healthRequest
      const abort = new AbortController()
      expectedSignal = abort.signal
      const cancelledRequest = input.sdk.global.health({ signal: abort.signal }).then(
        () => ({ type: "completed" as const }),
        (error: unknown) => ({ type: "failed" as const, name: error instanceof Error ? error.name : "unknown" }),
      )
      const cancellationDeadline = Date.now() + 10_000
      while (Date.now() < cancellationDeadline && healthRequests.length < 2) await Bun.sleep(20)
      if (healthRequests.length < 2) throw new Error("interactive runtime did not issue the cancellable SDK request")
      abort.abort()
      const cancellation = await cancelledRequest
      await Bun.write(
        resultFile,
        JSON.stringify({
          status: first.response.status,
          healthy: first.data.healthy,
          timeoutDisabled: healthRequests[0]?.timeoutDisabled,
          cancelTimeoutDisabled: healthRequests[1]?.timeoutDisabled,
          cancelHasSignal: healthRequests[1]?.hasSignal,
          cancellation: cancellation.type === "failed" ? cancellation.name : cancellation.type,
        }),
      )
    },
  }))

  const { runMini } = await import("../../../src/cli/cmd/run")
  test(testName, async () => {
    await runMini({
      attach,
      session,
      prompt: "interactive attached transport probe",
      model: "test/test-model",
    })
    const result: unknown = await Bun.file(resultFile).json()
    expect(result).toMatchObject({
      status: 200,
      healthy: true,
      timeoutDisabled: true,
      cancelTimeoutDisabled: true,
      cancelHasSignal: true,
      cancellation: "AbortError",
    })
  })
}
