import { describe, expect } from "bun:test"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { Deferred, Effect, Layer } from "effect"
import { FetchHttpClient, HttpClient } from "effect/unstable/http"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Agent } from "../../src/agent/agent"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import { Permission } from "../../src/permission"
import { Truncate } from "@/tool/truncate"
import { MAX_REDIRECTS, WebFetchTool } from "../../src/tool/webfetch"
import { SessionID, MessageID } from "../../src/session/schema"
import { Tool } from "@/tool/tool"
import { testEffect } from "../lib/effect"

const noopBootstrap = Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void }))
const it = testEffect(
  LayerNode.compile(
    LayerNode.group([
      httpClient,
      Truncate.node,
      Agent.node,
      Permission.node,
      EventV2Bridge.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
    ]),
    [
      [httpClient, FetchHttpClient.layer as Layer.Layer<HttpClient.HttpClient>],
      [InstanceStore.bootstrapNode, noopBootstrap],
    ],
  ),
)

const ctx = {
  sessionID: SessionID.make("ses_test"),
  messageID: MessageID.make("msg_message"),
  callID: "call_webfetch_test",
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
}
const initialPermissionPatterns: string[] = []
const redirectChecks: string[] = []

const withFetch = <A, E, R>(
  fetch: (req: Request) => Response | Promise<Response>,
  fn: (url: URL) => Effect.Effect<A, E, R>,
) =>
  Effect.acquireUseRelease(
    Effect.sync(() => Bun.serve({ port: 0, fetch })),
    (server) => fn(server.url),
    (server) => Effect.sync(() => server.stop(true)),
  )

const exec = Effect.fn("WebFetchToolTest.exec")(function* (
  args: Tool.InferParameters<typeof WebFetchTool>,
  ruleset: PermissionV1.Ruleset = [{ permission: "webfetch", pattern: "*", action: "allow" }],
) {
  const permission = yield* Permission.Service
  const info = yield* WebFetchTool
  const tool = yield* info.init()
  return yield* tool.execute(args, {
    ...ctx,
    ask: (request) => {
      initialPermissionPatterns.push(...request.patterns)
      return permission
        .ask({
          ...request,
          sessionID: ctx.sessionID,
          tool: { messageID: ctx.messageID, callID: ctx.callID },
          ruleset,
        })
        .pipe(Effect.orDie)
    },
    assertNotDenied: (request) =>
      Effect.sync(() => redirectChecks.push(...request.patterns)).pipe(
        Effect.andThen(
          permission.assertNotDenied({
            ...request,
            sessionID: ctx.sessionID,
            tool: { messageID: ctx.messageID, callID: ctx.callID },
            ruleset,
          }),
        ),
      ),
  })
})

const expectNoPermissionAsk = <A, E, R>(self: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const permission = yield* Permission.Service
    const events = yield* EventV2Bridge.Service
    const asked = yield* Deferred.make<PermissionV1.Request>()
    const unsubscribe = yield* events.listen((event) =>
      event.type === Permission.Event.Asked.type
        ? Deferred.succeed(asked, event.data as PermissionV1.Request).pipe(Effect.asVoid)
        : Effect.void,
    )
    yield* Effect.addFinalizer(() => unsubscribe)

    const outcome = yield* Effect.race(
      self.pipe(Effect.map((value) => ({ type: "completed" as const, value }))),
      Deferred.await(asked).pipe(Effect.map((request) => ({ type: "asked" as const, request }))),
    )
    if (outcome.type === "asked") return yield* Effect.fail(new Error("Webfetch opened a redirect permission request"))
    expect(yield* permission.list()).toEqual([])
    return outcome.value
  }).pipe(
    Effect.timeoutOrElse({
      duration: "4 seconds",
      orElse: () => Effect.fail(new Error("Webfetch neither completed nor opened a permission request")),
    }),
  )

describe("tool.webfetch", () => {
  it.instance("denies canonical mixed-case and percent-encoded hosts before transport", () => {
    const received: string[] = []
    return withFetch(
      (request) => {
        received.push(new URL(request.url).href)
        return new Response("must not be requested", { headers: { "content-type": "text/plain" } })
      },
      (url) =>
        Effect.gen(function* () {
          const rawInputs = [
            `http://LOCALHOST:${url.port}/mixed-case`,
            `http://%6cocalhost:${url.port}/encoded-host`,
          ]
          const results: Array<{
            url: string
            outcome: string
            patterns: string[]
            requests: string[]
          }> = []

          yield* Effect.forEach(
            rawInputs,
            (raw) =>
              Effect.gen(function* () {
                initialPermissionPatterns.length = 0
                received.length = 0
                const canonical = new URL(raw).href
                const outcome = yield* Effect.exit(
                  exec(
                    { url: raw, format: "text" },
                    [
                      { permission: "webfetch", pattern: "*", action: "allow" },
                      { permission: "webfetch", pattern: canonical, action: "deny" },
                    ],
                  ),
                )

                results.push({
                  url: canonical,
                  outcome: outcome._tag,
                  patterns: [...initialPermissionPatterns],
                  requests: [...received],
                })
                expect(yield* (yield* Permission.Service).list()).toEqual([])
              }),
            { concurrency: 1 },
          )
          expect(results).toEqual(
            rawInputs.map((raw) => ({
              url: new URL(raw).href,
              outcome: "Failure",
              patterns: [new URL(raw).href],
              requests: [],
            })),
          )
        }),
    )
  }, 15_000)

  it.instance("transports an uppercase-scheme URL in its canonical form under an allowing rule", () => {
    const received: string[] = []
    return withFetch(
      (request) => {
        received.push(new URL(request.url).href)
        return new Response("canonical transport", { headers: { "content-type": "text/plain" } })
      },
      (url) =>
        Effect.gen(function* () {
          initialPermissionPatterns.length = 0
          const raw = `HTTP://LOCALHOST:${url.port}/allowed`
          const canonical = new URL(raw).href
          const outcome = yield* expectNoPermissionAsk(
            Effect.exit(
              exec({ url: raw, format: "text" }, [
                { permission: "webfetch", pattern: "*", action: "deny" },
                { permission: "webfetch", pattern: canonical, action: "allow" },
              ]),
            ),
          )

          expect(outcome._tag).toBe("Success")
          if (outcome._tag === "Success") expect(outcome.value.output).toBe("canonical transport")
          expect(initialPermissionPatterns).toEqual([canonical])
          expect(received).toEqual([canonical])
          expect(yield* (yield* Permission.Service).list()).toEqual([])
        }),
    )
  }, 15_000)

  it.instance("rejects malformed and unsupported input URLs before permission or transport", () => {
    const received: string[] = []
    return withFetch(
      (request) => {
        received.push(new URL(request.url).href)
        return new Response("unexpected", { headers: { "content-type": "text/plain" } })
      },
      (url) =>
        Effect.gen(function* () {
          const inputs = ["http://", "file:///etc/passwd"]
          const results: Array<{ url: string; outcome: string; patterns: string[]; requests: string[] }> = []
          for (const input of inputs) {
            initialPermissionPatterns.length = 0
            received.length = 0
            const outcome = yield* Effect.exit(exec({ url: input, format: "text" }))
            results.push({
              url: input,
              outcome: outcome._tag,
              patterns: [...initialPermissionPatterns],
              requests: [...received],
            })
          }
          expect(results).toEqual(
            inputs.map((input) => ({ url: input, outcome: "Failure", patterns: [], requests: [] })),
          )
        }),
    )
  }, 15_000)

  it.instance("checks supported relative and absolute redirects before transport", () => {
    const received: string[] = []
    return withFetch(
      (request) => {
        const pathname = new URL(request.url).pathname
        received.push(pathname)
        if (pathname.startsWith("/status/")) {
          return new Response("", {
            status: Number(pathname.slice("/status/".length)),
            headers: { location: "/target" },
          })
        }
        if (pathname === "/absolute") {
          return new Response("", { status: 302, headers: { location: new URL("/target", request.url).href } })
        }
        return new Response("redirect target", { headers: { "content-type": "text/plain" } })
      },
      (url) =>
        Effect.gen(function* () {
          for (const status of [301, 302, 303, 307, 308]) {
            received.length = 0
            redirectChecks.length = 0
            const start = new URL(`/status/${status}`, url).href
            const target = new URL("/target", url).href
            const outcome = yield* Effect.exit(exec({ url: start, format: "text" }))

            expect(outcome._tag).toBe("Success")
            if (outcome._tag === "Success") expect(outcome.value.output).toBe("redirect target")
            expect(received).toEqual([`/status/${status}`, "/target"])
            expect(redirectChecks).toEqual([target])
          }

          received.length = 0
          redirectChecks.length = 0
          const start = new URL("/absolute", url).href
          const target = new URL("/target", url).href
          const outcome = yield* Effect.exit(exec({ url: start, format: "text" }))

          expect(outcome._tag).toBe("Success")
          if (outcome._tag === "Success") expect(outcome.value.output).toBe("redirect target")
          expect(received).toEqual(["/absolute", "/target"])
          expect(redirectChecks).toEqual([target])
          expect(yield* (yield* Permission.Service).list()).toEqual([])
        }),
    )
  }, 15_000)

  it.instance("continues an ask-only redirect without creating a permission request", () => {
    const received: string[] = []
    return withFetch(
      (request) => {
        const pathname = new URL(request.url).pathname
        received.push(pathname)
        return pathname === "/start"
          ? new Response("", { status: 302, headers: { location: "/target" } })
          : new Response("ask-only target", { headers: { "content-type": "text/plain" } })
      },
      (url) =>
        Effect.gen(function* () {
          received.length = 0
          redirectChecks.length = 0
          const start = new URL("/start", url).href
          const target = new URL("/target", url).href
          const result = yield* expectNoPermissionAsk(
            exec({ url: start, format: "text" }, [
              { permission: "webfetch", pattern: "*", action: "allow" },
              { permission: "webfetch", pattern: target, action: "ask" },
            ]),
          )

          expect(result.output).toBe("ask-only target")
          expect(received).toEqual(["/start", "/target"])
          expect(redirectChecks).toEqual([target])
        }),
    )
  }, 15_000)

  it.instance("checks a later absolute redirect against deny rules before the server receives it", () => {
    const received: string[] = []
    return withFetch(
      (request) => {
        const pathname = new URL(request.url).pathname
        received.push(pathname)
        if (pathname === "/start") return new Response("", { status: 302, headers: { location: "/middle" } })
        if (pathname === "/middle") {
          return new Response("", { status: 307, headers: { location: new URL("/denied", request.url).href } })
        }
        return new Response("should not be requested", { headers: { "content-type": "text/plain" } })
      },
      (url) =>
        Effect.gen(function* () {
          received.length = 0
          redirectChecks.length = 0
          const start = new URL("/start", url).href
          const denied = new URL("/denied", url).href
          const outcome = yield* expectNoPermissionAsk(
            Effect.exit(
              exec({ url: start, format: "text" }, [
                { permission: "webfetch", pattern: "*", action: "allow" },
                { permission: "webfetch", pattern: denied, action: "deny" },
              ]),
            ),
          )

          expect(outcome._tag).toBe("Failure")
          expect(received).toEqual(["/start", "/middle"])
          expect(redirectChecks).toEqual([new URL("/middle", url).href, denied])
        }),
    )
  }, 15_000)

  it.instance("rejects unsupported redirects and bounds redirect chains", () => {
    const received: string[] = []
    let loopRequests = 0
    return withFetch(
      (request) => {
        const pathname = new URL(request.url).pathname
        received.push(pathname)
        if (pathname === "/unsupported") {
          return new Response("", { status: 301, headers: { location: "file:///etc/passwd" } })
        }
        if (pathname === "/loop") {
          loopRequests++
          if (loopRequests <= MAX_REDIRECTS + 1) {
            return new Response("", { status: 302, headers: { location: "/loop" } })
          }
          return new Response("end", { status: 404 })
        }
        return new Response("unexpected", { headers: { "content-type": "text/plain" } })
      },
      (url) =>
        Effect.gen(function* () {
          received.length = 0
          redirectChecks.length = 0
          const unsupported = new URL("/unsupported", url).href
          const unsupportedResult = yield* expectNoPermissionAsk(Effect.exit(exec({ url: unsupported, format: "text" })))

          expect(unsupportedResult._tag).toBe("Failure")
          expect(received).toEqual(["/unsupported"])
          expect(redirectChecks).toEqual([])

          received.length = 0
          redirectChecks.length = 0
          loopRequests = 0
          const loop = new URL("/loop", url).href
          const loopResult = yield* expectNoPermissionAsk(Effect.exit(exec({ url: loop, format: "text" })))

          expect(loopResult._tag).toBe("Failure")
          expect(received).toHaveLength(MAX_REDIRECTS + 1)
          expect(redirectChecks).toHaveLength(MAX_REDIRECTS)
          expect(yield* (yield* Permission.Service).list()).toEqual([])
        }),
    )
  }, 15_000)

  it.instance("rechecks every redirect after a Cloudflare challenge retry", () => {
    const received: Array<{ path: string; agent: string | null }> = []
    return withFetch(
      (request) => {
        const pathname = new URL(request.url).pathname
        const agent = request.headers.get("user-agent")
        received.push({ path: pathname, agent })
        if (pathname === "/challenge" && agent !== "opencode") {
          return new Response("challenge", { status: 403, headers: { "cf-mitigated": "challenge" } })
        }
        if (pathname === "/challenge") {
          return new Response("", { status: 302, headers: { location: new URL("/target", request.url).href } })
        }
        return new Response("retried target", { headers: { "content-type": "text/plain" } })
      },
      (url) =>
        Effect.gen(function* () {
          received.length = 0
          redirectChecks.length = 0
          const start = new URL("/challenge", url).href
          const target = new URL("/target", url).href
          const result = yield* expectNoPermissionAsk(
            exec({ url: start, format: "text" }, [
              { permission: "webfetch", pattern: "*", action: "allow" },
              { permission: "webfetch", pattern: target, action: "ask" },
            ]),
          )

          expect(result.output).toBe("retried target")
          expect(received.map((request) => request.path)).toEqual(["/challenge", "/challenge", "/target"])
          expect(received[1]?.agent).toBe("opencode")
          expect(received[2]?.agent).toBe("opencode")
          expect(redirectChecks).toEqual([target])
        }),
    )
  }, 15_000)

  it.instance("returns image responses as file attachments", () =>
    Effect.gen(function* () {
      const bytes = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10])
      yield* withFetch(
        () => new Response(bytes, { status: 200, headers: { "content-type": "IMAGE/PNG; charset=binary" } }),
        (url) =>
          Effect.gen(function* () {
            const result = yield* exec({ url: new URL("/image.png", url).toString(), format: "markdown" })
            expect(result.output).toBe("Image fetched successfully")
            expect(result.attachments).toBeDefined()
            expect(result.attachments?.length).toBe(1)
            expect(result.attachments?.[0].type).toBe("file")
            expect(result.attachments?.[0].mime).toBe("image/png")
            expect(result.attachments?.[0].url.startsWith("data:image/png;base64,")).toBe(true)
            expect(result.attachments?.[0]).not.toHaveProperty("id")
            expect(result.attachments?.[0]).not.toHaveProperty("sessionID")
            expect(result.attachments?.[0]).not.toHaveProperty("messageID")
          }),
      )
    }),
  )

  it.instance("keeps svg as text output", () =>
    withFetch(
      () =>
        new Response('<svg xmlns="http://www.w3.org/2000/svg"><text>hello</text></svg>', {
          status: 200,
          headers: { "content-type": "image/svg+xml; charset=UTF-8" },
        }),
      (url) =>
        Effect.gen(function* () {
          const result = yield* exec({ url: new URL("/image.svg", url).toString(), format: "html" })
          expect(result.output).toContain("<svg")
          expect(result.attachments).toBeUndefined()
        }),
    ),
  )

  it.instance("keeps text responses as text output", () =>
    withFetch(
      () =>
        new Response("hello from webfetch", {
          status: 200,
          headers: { "content-type": "text/plain; charset=utf-8" },
        }),
      (url) =>
        Effect.gen(function* () {
          const result = yield* exec({ url: new URL("/file.txt", url).toString(), format: "text" })
          expect(result.output).toBe("hello from webfetch")
          expect(result.attachments).toBeUndefined()
        }),
    ),
  )

  it.instance("extracts text from html without scripts or styles", () =>
    withFetch(
      () =>
        new Response(
          "<html><head><style>.hidden{}</style><script>alert('x')</script></head><body>Hello <b>world</b></body></html>",
          {
            status: 200,
            headers: { "content-type": "text/html; charset=utf-8" },
          },
        ),
      (url) =>
        Effect.gen(function* () {
          const result = yield* exec({ url: new URL("/page.html", url).toString(), format: "text" })
          expect(result.output).toBe("Hello world")
          expect(result.attachments).toBeUndefined()
        }),
    ),
  )
})
