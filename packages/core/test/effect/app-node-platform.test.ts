import { expect } from "bun:test"
import { RequestExecutor } from "@opencode-ai/llm/route"
import { Effect, Fiber, Exit } from "effect"
import { FetchHttpClient, HttpClientRequest } from "effect/unstable/http"
import { createServer } from "node:http"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { LayerNodePlatform } from "@opencode-ai/core/effect/app-node-platform"
import { testEffect } from "../lib/effect"

const it = testEffect(LayerNode.compile(LayerNodePlatform.requestExecutor))

it.live(
  "native provider requests override fetch options while preserving the response body",
  Effect.gen(function* () {
    if (process.platform === "win32") return

    const providerStarted = Promise.withResolvers<void>()
    const providerInit = Promise.withResolvers<unknown>()
    const release = Promise.withResolvers<void>()
    let released = false
    const listening = Promise.withResolvers<void>()
    const server = createServer((request, response) => {
      if (request.url !== "/provider") {
        response.writeHead(404).end()
        return
      }
      providerStarted.resolve()
      response.writeHead(200, { "content-type": "text/plain" })
      response.write("ready:")
      void release.promise.then(() => {
        if (response.destroyed) return
        response.end("native provider completed")
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
      Effect.provideService(FetchHttpClient.Fetch, observeFetch),
      Effect.provideService(FetchHttpClient.RequestInit, configuredTimeout),
      Effect.forkChild,
    )

    yield* Effect.promise(() => providerStarted.promise).pipe(Effect.timeout("5 seconds"))
    expect(yield* Effect.promise(() => providerInit.promise).pipe(Effect.timeout("5 seconds"))).toBe(false)
    expect(released).toBe(false)
    expect(server.listening).toBe(true)

    released = true
    release.resolve()
    const response = yield* Fiber.join(provider).pipe(Effect.timeout("10 seconds"))
    expect(response).toBe("ready:native provider completed")
  }),
  20_000,
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
