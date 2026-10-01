// Subprocess integration tests for `opencode serve`. Spawns the real CLI in
// headless mode and exercises it over HTTP — this is the only test tier that
// catches bugs spanning argv → server boot → routing → instance loading.
//
// `serve` is long-lived: the harness returns a handle (url/port/kill/exited)
// and kills the process when the test scope closes. The OS-assigned port is
// parsed off the "listening on http://..." line.
import { describe, expect } from "bun:test"
import { Duration, Effect, Fiber, Schedule } from "effect"
import { HttpClient, HttpClientRequest } from "effect/unstable/http"
import { createServer } from "node:net"
import { cliIt, deadline } from "../../lib/cli-process"

describe("opencode serve (subprocess)", () => {
  // Smoke test: server starts, binds a port, and /global/health responds.
  // If this fails, all other serve tests likely will too — debug here first.
  cliIt.live(
    "starts, binds a port, and serves /global/health",
    ({ opencode }) =>
      Effect.gen(function* () {
        const server = yield* opencode.serve()
        expect(server.port).toBeGreaterThan(0)
        expect(server.url).toMatch(/^http:\/\//)

        const client = yield* HttpClient.HttpClient
        const res = yield* client.get(`${server.url}/global/health`)
        expect(res.status).toBe(200)
        // GlobalHealth schema is { success: true, ... } | { success: false, error }.
        // We don't lock in further shape here — any 200 with parseable JSON is
        // enough proof the routing + auth-bypass + instance loading is alive.
        const body = yield* res.json
        expect(body).toBeDefined()
      }),
    60_000,
  )

  // swx-abbe#441: a request that reached a fresh server as its port opened was
  // read and never answered, because the HTTP server listened before it
  // attached its request handler. The fixture's listening line comes after
  // that window, so this test picks the port itself and sends its first
  // request, the authenticated POST /session Abbe's adapter sends, as soon
  // as the port accepts a connection.
  cliIt.live(
    "answers the first request sent as the port starts accepting",
    ({ opencode, home }) =>
      Effect.gen(function* () {
        const port = yield* Effect.promise(freePort)
        const password = "first-request"
        const client = yield* HttpClient.HttpClient
        const sent = { at: 0 }
        const first = yield* Effect.suspend(() => {
          sent.at = Date.now()
          return client.execute(
            HttpClientRequest.post(`http://127.0.0.1:${port}/session`).pipe(
              HttpClientRequest.setUrlParam("directory", home),
              HttpClientRequest.basicAuth("opencode", password),
              HttpClientRequest.bodyJsonUnsafe({ title: "first request" }),
            ),
          )
        }).pipe(
          // Only a refused connection is retried; a request the server
          // accepted but never answers runs into the timeout below.
          Effect.retry({
            while: (error) => error.reason._tag === "TransportError",
            schedule: Schedule.spaced("5 millis"),
          }),
          Effect.flatMap((res) => Effect.map(res.json, (body) => ({ status: res.status, body }))),
          Effect.timeout(Duration.millis(deadline(20_000))),
          Effect.forkScoped,
        )

        yield* opencode.serve({
          port,
          extraArgs: ["--pure", "--print-logs"],
          env: { OPENCODE_SERVER_PASSWORD: password },
        })
        const listeningAt = Date.now()
        const result = yield* Fiber.join(first)

        // The request must have gone out before the listening line, or this
        // run did not exercise the window at all.
        expect(sent.at).toBeLessThan(listeningAt)
        expect(result.status).toBe(200)
        expect(result.body).toMatchObject({ id: expect.stringMatching(/^ses_/) })
      }),
    60_000,
  )

  // The scope-close finalizer must actually terminate the child. Without this
  // test a regression in the kill path (e.g. a future refactor that forgets
  // to wire the finalizer) would leak processes on every test run.
  cliIt.live(
    "kills the subprocess on scope close",
    ({ opencode }) =>
      Effect.gen(function* () {
        // Inner scope so we can observe `.exited` resolving after it closes.
        const exitedPromise = yield* Effect.scoped(
          Effect.gen(function* () {
            const server = yield* opencode.serve()
            // Capture the Promise, not the resolved value — scope closes after
            // this gen returns, at which point the finalizer kills the child.
            return server.exited
          }),
        )
        // After scope close: finalizer fired, process must have exited.
        const code = yield* Effect.promise(() => exitedPromise)
        // Bun reports the exit code; SIGTERM-killed processes return non-null
        // (typically 143 on POSIX). We just require resolution within a sane
        // window — anything else means the kill didn't take.
        expect(typeof code === "number" || code === null).toBe(true)
      }),
    60_000,
  )
})

function freePort() {
  return new Promise<number>((resolve, reject) => {
    const server = createServer()
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      const port = typeof address === "object" && address ? address.port : 0
      server.close(() => resolve(port))
    })
  })
}
