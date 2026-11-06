import { expect } from "bun:test"
import { RequestExecutor } from "@opencode-ai/llm/route"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { Effect, Fiber, Exit, Option } from "effect"
import { FetchHttpClient, HttpClientRequest } from "effect/unstable/http"
import { createServer } from "node:http"
import path from "node:path"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { LayerNodePlatform } from "@opencode-ai/core/effect/app-node-platform"
import { testEffect } from "../lib/effect"

const it = testEffect(LayerNode.compile(LayerNodePlatform.requestExecutor))
const nativeIdleTestName = "native provider requests survive a short Bun idle timeout while its control expires"

it.live(
  nativeIdleTestName,
  Effect.gen(function* () {
    if (process.env.SWX_CORE_IDLE_TEST_CHILD !== "1" && process.platform !== "linux") return
    if (process.env.SWX_CORE_IDLE_TEST_CHILD !== "1") {
      const child = Bun.spawn(
        [process.execPath, "test", "--timeout", "35_000", path.join(import.meta.dir, "app-node-platform.test.ts"), "-t", nativeIdleTestName],
        {
          cwd: process.cwd(),
          env: {
            ...process.env,
            BUN_CONFIG_HTTP_IDLE_TIMEOUT: "2",
            SWX_CORE_IDLE_TEST_CHILD: "1",
          },
          stdin: "ignore",
          stdout: "pipe",
          stderr: "pipe",
          detached: true,
        },
      )
      const identity = EventWriter.readIdentity(child.pid)
      const output = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()])
      try {
        if (!identity?.bootID || !identity.startTicks)
          throw new Error("native idle-timeout test child has no recorded process identity; cleanup skipped")
        const exitCode = yield* Effect.promise(() => child.exited).pipe(Effect.timeout("30 seconds"))
        const [stdout, stderr] = yield* Effect.promise(() => output).pipe(Effect.timeout("5 seconds"))
        expect(exitCode).toBe(0)
        if (exitCode !== 0) throw new Error(`${stdout}\n${stderr}`)
      } finally {
        if (identity?.bootID && identity.startTicks) {
          const current = EventWriter.readIdentity(identity.pid)
          if (current?.bootID === identity.bootID && current.startTicks === identity.startTicks) {
            try {
              process.kill(-identity.pid, "SIGKILL")
            } catch {}
          }
        }
        const stopped = yield* Effect.promise(() => child.exited).pipe(Effect.timeoutOption("5 seconds"))
        const drained = yield* Effect.promise(() => output).pipe(Effect.timeoutOption("5 seconds"))
        if (Option.isNone(stopped)) throw new Error("native idle-timeout test child survived cleanup")
        if (Option.isNone(drained)) throw new Error("native idle-timeout test child output did not drain after cleanup")
      }
      return
    }
    if (process.env.BUN_CONFIG_HTTP_IDLE_TIMEOUT !== "2")
      throw new Error("native fetch witness must inject Bun's short HTTP idle-timeout value before process start")

    const providerStarted = Promise.withResolvers<void>()
    const controlStarted = Promise.withResolvers<void>()
    const providerInit = Promise.withResolvers<unknown>()
    const release = Promise.withResolvers<void>()
    let released = false
    const listening = Promise.withResolvers<void>()
    const server = createServer((request, response) => {
      if (request.url !== "/provider" && request.url !== "/control") {
        response.writeHead(404).end()
        return
      }
      response.writeHead(200, { "content-type": "text/plain" })
      response.write("ready:")
      if (request.url === "/provider") providerStarted.resolve()
      else controlStarted.resolve()
      void release.promise.then(() => {
        if (response.destroyed) return
        response.end(request.url === "/provider" ? "native provider completed" : "default fetch completed")
      })
    })
    server.on("listening", () => listening.resolve())
    server.listen(0, "127.0.0.1")
    yield* Effect.promise(() => listening.promise).pipe(Effect.timeout("5 seconds"))
    const address = server.address()
    if (!address || typeof address === "string") throw new Error("native HTTP fixture did not acquire a TCP port")
    const url = `http://127.0.0.1:${address.port}`
    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        released = true
        release.resolve()
        if (!server.listening) return
        await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())))
      }),
    )

    const executor = yield* RequestExecutor.Service
    const configuredTimeout = { timeout: 1 } as RequestInit & { timeout: number }
    const observeFetch = ((input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
      const requestURL = input instanceof Request ? input.url : String(input)
      if (new URL(requestURL).pathname === "/provider")
        providerInit.resolve(init && "timeout" in init ? init.timeout : undefined)
      return fetch(input, init)
    }) as typeof globalThis.fetch
    const provider = yield* Effect.gen(function* () {
      const response = yield* executor.execute(HttpClientRequest.get(`${url}/provider`))
      return yield* response.text
    }).pipe(
      Effect.timeout("45 seconds"),
      Effect.provideService(FetchHttpClient.Fetch, observeFetch),
      Effect.provideService(FetchHttpClient.RequestInit, configuredTimeout),
      Effect.forkChild,
    )
    const controlStartedAt = Date.now()
    const control = fetch(`${url}/control`)
      .then(async (response) => ({
        type: "completed" as const,
        body: await response.text(),
        elapsed: Date.now() - controlStartedAt,
      }))
      .catch((error: unknown) => ({ type: "failed" as const, error, elapsed: Date.now() - controlStartedAt }))

    yield* Effect.promise(() => Promise.all([providerStarted.promise, controlStarted.promise])).pipe(
      Effect.timeout("15 seconds"),
    )
    expect(yield* Effect.promise(() => providerInit.promise).pipe(Effect.timeout("5 seconds"))).toBe(false)
    const controlOutcome = yield* Effect.promise(() => control).pipe(Effect.timeout("7 minutes"))
    expect(controlOutcome.type).toBe("failed")
    if (controlOutcome.type !== "failed") throw new Error("default Bun fetch unexpectedly completed the held control")
    expect(controlOutcome.error).toBeInstanceOf(Error)
    if (controlOutcome.error instanceof Error) expect(controlOutcome.error.name).toBe("TimeoutError")
    expect(controlOutcome.elapsed).toBeGreaterThanOrEqual(4_000)
    expect(controlOutcome.elapsed).toBeLessThan(15_000)
    expect(released).toBe(false)
    expect(server.listening).toBe(true)

    yield* Effect.sleep("2 seconds")
    expect(released).toBe(false)
    released = true
    release.resolve()
    const response = yield* Fiber.join(provider).pipe(Effect.timeout("10 seconds"))
    expect(response).toBe("ready:native provider completed")
  }),
  45_000,
)

it.live(
  "caller expiry aborts a native request and releases its response body",
  Effect.gen(function* () {
    const started = Promise.withResolvers<void>()
    const released = Promise.withResolvers<void>()
    const server = Bun.serve({
      port: 0,
      fetch: () =>
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode("partial"))
              started.resolve()
            },
            pull: () => new Promise<void>(() => {}),
            cancel() {
              released.resolve()
            },
          }),
          { headers: { "content-type": "text/plain" } },
        ),
    })
    yield* Effect.addFinalizer(() => Effect.promise(() => server.stop(true)).pipe(Effect.asVoid))

    const executor = yield* RequestExecutor.Service
    const request = Effect.gen(function* () {
      const response = yield* executor.execute(HttpClientRequest.get(new URL("/cancel", server.url)))
      return yield* response.text
    }).pipe(Effect.timeout("500 millis"), Effect.exit)
    const fiber = yield* request.pipe(Effect.forkChild)
    yield* Effect.promise(() => started.promise).pipe(Effect.timeout("5 seconds"))
    const result = yield* Fiber.join(fiber).pipe(Effect.timeout("5 seconds"))
    expect(Exit.isFailure(result)).toBe(true)
    yield* Effect.promise(() => released.promise).pipe(Effect.timeout("5 seconds"))
  }),
  12_000,
)
