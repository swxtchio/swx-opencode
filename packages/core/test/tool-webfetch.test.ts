import { describe, expect, test } from "bun:test"
import { Deferred, Duration, Effect, Fiber, Layer, Schema } from "effect"
import * as TestClock from "effect/testing/TestClock"
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http"
import { AgentV2 } from "@opencode-ai/core/agent"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { LayerNodePlatform } from "@opencode-ai/core/effect/app-node-platform"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { Location } from "@opencode-ai/core/location"
import { PermissionV2 } from "@opencode-ai/core/permission"
import { PermissionSaved } from "@opencode-ai/core/permission/saved"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { ToolRegistry } from "@opencode-ai/core/tool/registry"
import { WebFetchTool } from "@opencode-ai/core/tool/webfetch"
import { ToolOutputStore } from "@opencode-ai/core/tool-output-store"
import { testEffect } from "./lib/effect"
import { toolIdentity, executeTool, settleTool, toolDefinitions } from "./lib/tool"
import { location } from "./fixture/location"

const sessionID = SessionV2.ID.make("ses_webfetch_test")
const requests: Array<{ readonly url: string; readonly headers: Record<string, string> }> = []
const assertions: PermissionV2.AssertInput[] = []
const checks: PermissionV2.AssertInput[] = []
const denied = new Set<string>()
let respond = (_request: HttpClientRequest.HttpClientRequest) =>
  Effect.succeed(new Response("hello", { headers: { "content-type": "text/plain" } }))

const http = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make((request) =>
    Effect.sync(() => requests.push({ url: request.url, headers: request.headers })).pipe(
      Effect.andThen(respond(request)),
      Effect.map((response) => HttpClientResponse.fromWeb(request, response)),
    ),
  ),
)
const permission = Layer.succeed(
  PermissionV2.Service,
  PermissionV2.Service.of({
    assert: (input) => Effect.sync(() => assertions.push(input)),
    assertNotDenied: (input) =>
      Effect.sync(() => checks.push(input)).pipe(
        Effect.andThen(
          input.resources.some((resource) => denied.has(resource))
            ? Effect.fail(new PermissionV2.BlockedError({ rules: [] }))
            : Effect.void,
        ),
      ),
    ask: () => Effect.die("unused"),
    reply: () => Effect.die("unused"),
    get: () => Effect.die("unused"),
    forSession: () => Effect.die("unused"),
    list: () => Effect.die("unused"),
  }),
)
const toolLayer = (replacements: LayerNode.Replacements = []) =>
  AppNodeBuilder.build(LayerNode.group([ToolRegistry.node, ToolRegistry.toolsNode, WebFetchTool.node]), [
    [PermissionV2.node, permission],
    [ToolOutputStore.node, ToolOutputStore.nodeWithoutConfig],
    ...replacements,
  ])
const it = testEffect(toolLayer([[LayerNodePlatform.httpClient, http]]))
const live = testEffect(toolLayer())
const currentLocation = Layer.succeed(
  Location.Service,
  Location.Service.of(location({ directory: AbsolutePath.make("/project") })),
)
const policy = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      EventV2.node,
      SessionStore.node,
      PermissionSaved.node,
      AgentV2.node,
      PermissionV2.node,
      ToolRegistry.node,
      ToolRegistry.toolsNode,
      WebFetchTool.node,
      ToolOutputStore.node,
    ]),
    [
      [Location.node, currentLocation],
      [ToolOutputStore.node, ToolOutputStore.nodeWithoutConfig],
    ],
  ),
)

const reset = () => {
  requests.length = 0
  assertions.length = 0
  checks.length = 0
  denied.clear()
  respond = () => Effect.succeed(new Response("hello", { headers: { "content-type": "text/plain" } }))
}

const call = (input: typeof WebFetchTool.Input.Type, id = "call-webfetch", agent = toolIdentity.agent) => ({
  sessionID,
  ...toolIdentity,
  agent,
  call: { type: "tool-call" as const, id, name: "webfetch", input },
})

const setupPolicy = () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: "test",
        directory: "/project",
        title: "test",
        version: "test",
        agent: "test",
      })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
  })

const setPermissions = (agent: string, rules: PermissionV2.Ruleset) =>
  Effect.gen(function* () {
    const agents = yield* AgentV2.Service
    yield* agents.transform((editor) =>
      editor.update(AgentV2.ID.make(agent), (value) => {
        value.permissions = [...rules]
      }),
    )
  })

const expectNoPermissionV2Ask = <A, E, R>(self: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const permission = yield* PermissionV2.Service
    const events = yield* EventV2.Service
    const asked = yield* Deferred.make<PermissionV2.Request>()
    const unsubscribe = yield* events.listen((event) =>
      event.type === PermissionV2.Event.Asked.type
        ? Deferred.succeed(asked, event.data as PermissionV2.Request).pipe(Effect.asVoid)
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

const unboundedResponse = (
  id: string,
  status: number,
  location: string | undefined,
  releases: Map<string, Deferred.Deferred<void>>,
  released: string[],
  headers: Record<string, string> = {},
) => {
  const signal = Deferred.makeUnsafe<void>()
  releases.set(id, signal)
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array([1]))
      },
      // Keep the body open after its first chunk so only reader cancellation ends it.
      pull: () => new Promise<void>(() => {}),
      cancel() {
        released.push(id)
        Deferred.doneUnsafe(signal, Effect.void)
      },
    }),
    { status, headers: { ...headers, ...(location ? { location } : {}) } },
  )
}

const waitForBodyRelease = (releases: Map<string, Deferred.Deferred<void>>, id: string) =>
  Effect.gen(function* () {
    const signal = releases.get(id)
    if (!signal) return yield* Effect.fail(new Error(`missing response body signal: ${id}`))
    yield* Deferred.await(signal).pipe(
      Effect.timeoutOrElse({
        duration: "2 seconds",
        orElse: () => Effect.fail(new Error(`response body was not released: ${id}`)),
      }),
    )
  })

describe("WebFetchTool real permission policy", () => {
  policy.live("cancels discarded streaming responses across redirects and error exits", () =>
    Effect.gen(function* () {
      yield* setupPolicy()
      const registry = yield* ToolRegistry.Service
      const releases = new Map<string, Deferred.Deferred<void>>()
      const released: string[] = []
      const requests: string[] = []
      // Retain raw Fetch responses so garbage collection cannot make missing cleanup look successful.
      const heldResponses: Response[] = []
      const previousBodiesUsedBeforeFetch: boolean[] = []
      const nativeFetch = globalThis.fetch
      const trackedFetch = Object.assign(
        (...args: Parameters<typeof globalThis.fetch>) => {
          if (heldResponses.length > 0) previousBodiesUsedBeforeFetch.push(heldResponses.at(-1)?.bodyUsed ?? false)
          return nativeFetch(...args).then((response) => {
            heldResponses.push(response)
            return response
          })
        },
        { preconnect: nativeFetch.preconnect },
      )
      let limitRequests = 0
      yield* setPermissions("build", [{ action: "webfetch", resource: "*", effect: "allow" }])

      return yield* Effect.acquireUseRelease(
        Effect.sync(() =>
          Bun.serve({
            port: 0,
            // Let the release assertion, rather than Bun's idle timeout, own the response lifetime.
            idleTimeout: 60,
            fetch: (request) => {
              const pathname = new URL(request.url).pathname
              requests.push(pathname)
              if (pathname === "/follow") return unboundedResponse("follow", 302, "/target", releases, released)
              if (pathname === "/deny") return unboundedResponse("deny-hop", 302, "/blocked", releases, released)
              if (pathname === "/limit") {
                limitRequests++
                if (limitRequests <= WebFetchTool.MAX_REDIRECTS + 1) {
                  return unboundedResponse(`limit-${limitRequests}`, 302, "/limit", releases, released)
                }
                return unboundedResponse("limit-extra", 404, undefined, releases, released)
              }
              if (pathname === "/unsupported") {
                return unboundedResponse("unsupported", 301, "file:///etc/passwd", releases, released)
              }
              if (pathname === "/not-found") return unboundedResponse("not-found", 404, undefined, releases, released)
              if (pathname === "/unsupported-content") {
                return unboundedResponse("unsupported-content", 200, undefined, releases, released, {
                  "content-type": "application/pdf",
                })
              }
              if (pathname === "/challenge" && request.headers.get("user-agent") !== "opencode") {
                return unboundedResponse("cloudflare", 403, undefined, releases, released, {
                  "cf-mitigated": "challenge",
                })
              }
              return new Response(pathname === "/challenge" ? "retried" : "target", {
                headers: { "content-type": "text/plain" },
              })
            },
          }),
        ),
        (server) =>
          Effect.gen(function* () {
            const execute = (pathname: string, id: string) =>
              executeTool(registry, call({ url: new URL(pathname, server.url).href, format: "text" }, id)).pipe(
                Effect.provideService(FetchHttpClient.Fetch, trackedFetch),
              )
            const follow = yield* execute("/follow", "call-body-follow")
            expect(follow).toEqual({ type: "text", value: "target" })
            expect(heldResponses).toHaveLength(requests.length)
            expect(heldResponses[0]?.bodyUsed).toBe(true)
            yield* waitForBodyRelease(releases, "follow")

            const deniedUrl = new URL("/blocked", server.url).href
            yield* setPermissions("build", [
              { action: "webfetch", resource: "*", effect: "allow" },
              { action: "webfetch", resource: deniedUrl, effect: "deny" },
            ])
            const denied = yield* execute("/deny", "call-body-denied")
            expect(denied).toMatchObject({ type: "error" })
            yield* waitForBodyRelease(releases, "deny-hop")
            expect(requests).not.toContain("/blocked")

            yield* setPermissions("build", [{ action: "webfetch", resource: "*", effect: "allow" }])
            const limit = yield* execute("/limit", "call-body-limit")
            expect(limit).toMatchObject({ type: "error" })
            yield* Effect.forEach(
              Array.from({ length: WebFetchTool.MAX_REDIRECTS + 1 }, (_, index) => `limit-${index + 1}`),
              (id) => waitForBodyRelease(releases, id),
              { concurrency: 1 },
            )
            expect(limitRequests).toBe(WebFetchTool.MAX_REDIRECTS + 1)

            const unsupported = yield* execute("/unsupported", "call-body-unsupported")
            expect(unsupported).toMatchObject({ type: "error" })
            yield* waitForBodyRelease(releases, "unsupported")

            const notFound = yield* execute("/not-found", "call-body-not-found")
            expect(notFound).toMatchObject({ type: "error" })
            yield* waitForBodyRelease(releases, "not-found")

            const unsupportedContent = yield* execute("/unsupported-content", "call-body-unsupported-content")
            expect(unsupportedContent).toMatchObject({ type: "error" })
            yield* waitForBodyRelease(releases, "unsupported-content")

            const challenge = yield* execute("/challenge", "call-body-challenge")
            expect(challenge).toEqual({ type: "text", value: "retried" })
            yield* waitForBodyRelease(releases, "cloudflare")
            expect(heldResponses).toHaveLength(requests.length)
            expect(heldResponses.every((response) => response.bodyUsed)).toBe(true)
            expect(previousBodiesUsedBeforeFetch).toHaveLength(requests.length - 1)
            expect(previousBodiesUsedBeforeFetch.every(Boolean)).toBe(true)
          }),
        (server) => Effect.promise(() => server.stop(true)),
      )
    }),
    15_000,
  )

  policy.live("uses parsed URL hrefs as V2 permission resources", () =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const received: string[] = []
        const server = Bun.serve({
          port: 0,
          fetch: (request) => {
            received.push(new URL(request.url).href)
            return new Response("fetched", { headers: { "content-type": "text/plain" } })
          },
        })
        return { server, received }
      }),
      ({ server, received }) =>
        Effect.gen(function* () {
          yield* setupPolicy()
          const registry = yield* ToolRegistry.Service
          const permissions = yield* PermissionV2.Service
          const reviewer = AgentV2.ID.make("reviewer")
          yield* setPermissions("test", [{ action: "webfetch", resource: "*", effect: "allow" }])

          const rawInputs = [
            `http://LOCALHOST:${server.port}/mixed-case`,
            `http://%6cocalhost:${server.port}/encoded-host`,
          ]
          const results: Array<{ url: string; result: string; requests: string[] }> = []
          yield* Effect.forEach(
            rawInputs,
            (url) =>
              Effect.gen(function* () {
                const canonical = new URL(url).href
                yield* setPermissions("reviewer", [
                  { action: "webfetch", resource: "*", effect: "allow" },
                  { action: "webfetch", resource: canonical, effect: "deny" },
                ])
                received.length = 0

                const result = yield* executeTool(registry, call({ url, format: "text" }, `call-denied-${results.length}`, reviewer))
                results.push({ url: canonical, result: result.type, requests: [...received] })
                expect(yield* permissions.list()).toEqual([])
              }),
            { concurrency: 1 },
          )
          const raw = `HTTP://LOCALHOST:${server.port}/allowed`
          const canonical = new URL(raw).href
          yield* setPermissions("test", [{ action: "webfetch", resource: "*", effect: "deny" }])
          yield* setPermissions("reviewer", [
            { action: "webfetch", resource: "*", effect: "deny" },
            { action: "webfetch", resource: canonical, effect: "allow" },
          ])
          received.length = 0

          const allowed = yield* executeTool(registry, call({ url: raw, format: "text" }, "call-uppercase", reviewer))
          const allowedRequests = [...received]
          expect(yield* permissions.list()).toEqual([])
          expect(results.map((result) => [result.url, result.result, result.requests])).toEqual(
            rawInputs.map((url) => [new URL(url).href, "error", []]),
          )
          expect(allowed).toEqual({ type: "text", value: "fetched" })
          expect(allowedRequests).toEqual([canonical])
        }),
      ({ server }) => Effect.promise(() => server.stop(true)),
    ),
  )

  policy.live("rejects malformed and unsupported inputs without transport or pending permission asks", () =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const received: string[] = []
        const server = Bun.serve({
          port: 0,
          fetch: (request) => {
            received.push(new URL(request.url).href)
            return new Response("unexpected", { headers: { "content-type": "text/plain" } })
          },
        })
        return { server, received }
      }),
      ({ server, received }) =>
        Effect.gen(function* () {
          yield* setupPolicy()
          const registry = yield* ToolRegistry.Service
          const permissions = yield* PermissionV2.Service
          const reviewer = AgentV2.ID.make("reviewer")
          yield* setPermissions("test", [{ action: "webfetch", resource: "*", effect: "allow" }])
          yield* setPermissions("reviewer", [{ action: "webfetch", resource: "*", effect: "allow" }])

          for (const [index, url] of ["http://", "file:///etc/passwd"].entries()) {
            received.length = 0
            expect(yield* executeTool(registry, call({ url, format: "text" }, `call-invalid-${index}`, reviewer))).toEqual({
              type: "error",
              value: `Unable to fetch ${url}`,
            })
            expect(received).toEqual([])
            expect(yield* permissions.list()).toEqual([])
          }
        }),
      ({ server }) => Effect.promise(() => server.stop(true)),
    ),
  )

  policy.live("supports every redirect status and absolute Location values", () =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const received: string[] = []
        const server = Bun.serve({
          port: 0,
          fetch: (request) => {
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
        })
        return { server, received }
      }),
      ({ server, received }) =>
        Effect.gen(function* () {
          yield* setupPolicy()
          const registry = yield* ToolRegistry.Service
          const reviewer = AgentV2.ID.make("reviewer")
          yield* setPermissions("test", [{ action: "webfetch", resource: "*", effect: "deny" }])
          yield* setPermissions("reviewer", [{ action: "webfetch", resource: "*", effect: "allow" }])

          for (const status of [301, 302, 303, 307, 308]) {
            received.length = 0
            const start = new URL(`/status/${status}`, server.url).href
            const result = yield* expectNoPermissionV2Ask(
              executeTool(registry, call({ url: start, format: "text" }, `call-status-${status}`, reviewer)),
            )
            expect(result).toEqual({ type: "text", value: "redirect target" })
            expect(received).toEqual([`/status/${status}`, "/target"])
          }

          received.length = 0
          const result = yield* expectNoPermissionV2Ask(
            executeTool(
              registry,
              call({ url: new URL("/absolute", server.url).href, format: "text" }, "call-absolute", reviewer),
            ),
          )
          expect(result).toEqual({ type: "text", value: "redirect target" })
          expect(received).toEqual(["/absolute", "/target"])
        }),
      ({ server }) => Effect.promise(() => server.stop(true)),
    ),
  )

  policy.live("continues an ask-only redirect under the explicit V2 agent", () =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const received: string[] = []
        const server = Bun.serve({
          port: 0,
          fetch: (request) => {
            const pathname = new URL(request.url).pathname
            received.push(pathname)
            return pathname === "/start"
              ? new Response("", { status: 302, headers: { location: "/target" } })
              : new Response("ask-only target", { headers: { "content-type": "text/plain" } })
          },
        })
        return { server, received }
      }),
      ({ server, received }) =>
        Effect.gen(function* () {
          yield* setupPolicy()
          const registry = yield* ToolRegistry.Service
          const reviewer = AgentV2.ID.make("reviewer")
          yield* setPermissions("test", [{ action: "webfetch", resource: "*", effect: "deny" }])
          const start = new URL("/start", server.url).href
          const target = new URL("/target", server.url).href
          yield* setPermissions("reviewer", [
            { action: "webfetch", resource: "*", effect: "allow" },
            { action: "webfetch", resource: target, effect: "ask" },
          ])

          const result = yield* expectNoPermissionV2Ask(
            executeTool(registry, call({ url: start, format: "text" }, "call-ask-redirect", reviewer)),
          )
          expect(result).toEqual({ type: "text", value: "ask-only target" })
          expect(received).toEqual(["/start", "/target"])
        }),
      ({ server }) => Effect.promise(() => server.stop(true)),
    ),
  )

  policy.live("denies a later redirect hop before its live HTTP request", () =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const received: string[] = []
        const server = Bun.serve({
          port: 0,
          fetch: (request) => {
            const pathname = new URL(request.url).pathname
            received.push(pathname)
            if (pathname === "/start") return new Response("", { status: 302, headers: { location: "/middle" } })
            if (pathname === "/middle") {
              return new Response("", { status: 307, headers: { location: new URL("/denied", request.url).href } })
            }
            return new Response("should not be requested", { headers: { "content-type": "text/plain" } })
          },
        })
        return { server, received }
      }),
      ({ server, received }) =>
        Effect.gen(function* () {
          yield* setupPolicy()
          const registry = yield* ToolRegistry.Service
          const reviewer = AgentV2.ID.make("reviewer")
          yield* setPermissions("test", [{ action: "webfetch", resource: "*", effect: "allow" }])
          const start = new URL("/start", server.url).href
          const denied = new URL("/denied", server.url).href
          yield* setPermissions("reviewer", [
            { action: "webfetch", resource: "*", effect: "allow" },
            { action: "webfetch", resource: denied, effect: "deny" },
          ])

          const outcome = yield* expectNoPermissionV2Ask(
            Effect.exit(executeTool(registry, call({ url: start, format: "text" }, "call-later-deny", reviewer))),
          )
          expect(outcome._tag).toBe("Success")
          if (outcome._tag === "Success") expect(outcome.value).toMatchObject({ type: "error" })
          expect(received).toEqual(["/start", "/middle"])
        }),
      ({ server }) => Effect.promise(() => server.stop(true)),
    ),
  )

  policy.live("keeps challenge retries on the manual redirect policy", () =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const received: Array<{ path: string; agent: string | null }> = []
        const server = Bun.serve({
          port: 0,
          fetch: (request) => {
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
        })
        return { server, received }
      }),
      ({ server, received }) =>
        Effect.gen(function* () {
          yield* setupPolicy()
          const registry = yield* ToolRegistry.Service
          const reviewer = AgentV2.ID.make("reviewer")
          yield* setPermissions("test", [{ action: "webfetch", resource: "*", effect: "deny" }])
          const start = new URL("/challenge", server.url).href
          const target = new URL("/target", server.url).href
          yield* setPermissions("reviewer", [
            { action: "webfetch", resource: "*", effect: "allow" },
            { action: "webfetch", resource: target, effect: "ask" },
          ])

          const result = yield* expectNoPermissionV2Ask(
            executeTool(registry, call({ url: start, format: "text" }, "call-cloudflare-redirect", reviewer)),
          )
          expect(result).toEqual({ type: "text", value: "retried target" })
          expect(received.map((request) => request.path)).toEqual(["/challenge", "/challenge", "/target"])
          expect(received[1]?.agent).toBe("opencode")
          expect(received[2]?.agent).toBe("opencode")
        }),
      ({ server }) => Effect.promise(() => server.stop(true)),
    ),
  )
})

describe("WebFetchTool helpers", () => {
  test("defaults format and rejects invalid timeout controls", () => {
    const decode = Schema.decodeUnknownSync(WebFetchTool.Input)
    expect(decode({ url: "https://example.com" })).toEqual({ url: "https://example.com", format: "markdown" })
    expect(() => decode({ url: "https://example.com", timeout: 0 })).toThrow()
    expect(() => decode({ url: "https://example.com", timeout: WebFetchTool.MAX_TIMEOUT_SECONDS + 1 })).toThrow()
  })

  test("ports HTML text and markdown conversions without active content", () => {
    const html = "<h1>Hello</h1><script>bad()</script><p>world <strong>wide</strong></p><style>.bad {}</style>"
    expect(WebFetchTool.extractTextFromHTML(html)).toBe("Helloworld wide")
    expect(WebFetchTool.convertHTMLToMarkdown(html)).toBe("# Hello\n\nworld **wide**")
  })
})

describe("WebFetchTool registration", () => {
  it.effect("registers and fetches an ordinary hostname HTTP URL without rewriting it", () =>
    Effect.gen(function* () {
      reset()
      const registry = yield* ToolRegistry.Service
      const url = "http://example.com/public"

      expect((yield* toolDefinitions(registry)).map((tool) => tool.name)).toEqual(["webfetch"])
      expect(yield* settleTool(registry, call({ url, format: "text", timeout: 4 }))).toEqual({
        result: { type: "text", value: "hello" },
        output: {
          structured: { url, contentType: "text/plain", format: "text", output: "hello" },
          content: [{ type: "text", text: "hello" }],
        },
      })
      expect(assertions).toMatchObject([
        { sessionID, action: "webfetch", resources: [url], save: ["*"], metadata: { url, format: "text", timeout: 4 } },
      ])
      expect(requests).toMatchObject([{ url, headers: { accept: expect.stringContaining("text/plain;q=1.0") } }])
    }),
  )

  it.effect("accepts localhost URLs with the same requested-URL permission check", () =>
    Effect.gen(function* () {
      reset()
      const registry = yield* ToolRegistry.Service
      const url = "http://localhost/private"

      expect(yield* executeTool(registry, call({ url, format: "text" }))).toEqual({
        type: "text",
        value: "hello",
      })
      expect(assertions).toMatchObject([
        { sessionID, action: "webfetch", resources: [url], save: ["*"], metadata: { url, format: "text" } },
      ])
      expect(requests.map((request) => request.url)).toEqual([url])
    }),
  )

  live.effect("checks a relative redirect before the server receives it", () => {
    const received: string[] = []
    return Effect.acquireUseRelease(
      Effect.sync(() =>
        Bun.serve({
          port: 0,
          fetch: (request) => {
            const pathname = new URL(request.url).pathname
            received.push(pathname)
            return pathname === "/redirect"
              ? new Response("", { status: 302, headers: { location: "/target" } })
              : new Response("redirected", { headers: { "content-type": "text/plain" } })
          },
        }),
      ),
      (server) =>
        Effect.gen(function* () {
          reset()
          const registry = yield* ToolRegistry.Service
          const url = new URL("/redirect", server.url).toString()
          const target = new URL("/target", server.url).toString()

          expect(yield* executeTool(registry, call({ url, format: "text" }))).toEqual({
            type: "text",
            value: "redirected",
          })
          expect(assertions).toMatchObject([
            { sessionID, action: "webfetch", resources: [url], save: ["*"], metadata: { url, format: "text" } },
          ])
          expect(checks.map((check) => check.resources)).toEqual([[target]])
          expect(received).toEqual(["/redirect", "/target"])
        }),
      (server) => Effect.promise(() => server.stop(true)),
    )
  })

  it.effect("stops at a redirect target denied by the permission check", () =>
    Effect.gen(function* () {
      reset()
      const registry = yield* ToolRegistry.Service
      const url = "https://example.com/start"
      const target = "http://169.254.169.254/latest/meta-data"
      denied.add(target)
      respond = () => Effect.succeed(new Response("", { status: 302, headers: { location: target } }))

      expect(yield* executeTool(registry, call({ url, format: "text" }))).toEqual({
        type: "error",
        value: `Unable to fetch ${url}`,
      })
      expect(checks.map((check) => check.resources)).toEqual([[target]])
      expect(requests.map((request) => request.url)).toEqual([url])
    }),
  )

  it.effect("rejects unsupported redirect targets and bounds redirect chains", () =>
    Effect.gen(function* () {
      reset()
      const registry = yield* ToolRegistry.Service
      const url = "https://example.com/a"
      respond = () => Effect.succeed(new Response("", { status: 301, headers: { location: "file:///etc/passwd" } }))
      expect(yield* executeTool(registry, call({ url, format: "text" }))).toEqual({
        type: "error",
        value: `Unable to fetch ${url}`,
      })
      expect(checks).toEqual([])
      expect(requests.map((request) => request.url)).toEqual([url])

      reset()
      let loopRequests = 0
      respond = (request) => {
        loopRequests++
        if (loopRequests <= WebFetchTool.MAX_REDIRECTS + 1) {
          return Effect.succeed(new Response("", { status: 307, headers: { location: `${request.url}x` } }))
        }
        return Effect.succeed(new Response("", { status: 404 }))
      }
      expect(yield* executeTool(registry, call({ url, format: "text" }))).toEqual({
        type: "error",
        value: `Unable to fetch ${url}`,
      })
      expect(requests).toHaveLength(WebFetchTool.MAX_REDIRECTS + 1)
      expect(checks).toHaveLength(WebFetchTool.MAX_REDIRECTS)
    }),
  )

  it.effect("rejects non-HTTP schemes before permission or transport", () =>
    Effect.gen(function* () {
      const registry = yield* ToolRegistry.Service
      const urls = ["file:///etc/passwd", "http://"]
      const outcomes: Array<{ url: string; type: string; value: string; assertions: number; requests: string[] }> = []
      for (const url of urls) {
        reset()
        const result = yield* executeTool(registry, call({ url, format: "text" }))
        outcomes.push({
          url,
          type: result.type,
          value: result.value,
          assertions: assertions.length,
          requests: requests.map((request) => request.url),
        })
      }
      expect(outcomes).toEqual(
        urls.map((url) => ({
          url,
          type: "error",
          value: `Unable to fetch ${url}`,
          assertions: 0,
          requests: [],
        })),
      )
    }),
  )

  it.effect("converts HTML to requested markdown and text", () =>
    Effect.gen(function* () {
      reset()
      respond = () =>
        Effect.succeed(
          new Response("<h1>Hello</h1><p>world</p><script>bad()</script>", {
            headers: { "content-type": "text/html; charset=utf-8" },
          }),
        )
      const registry = yield* ToolRegistry.Service

      expect(yield* executeTool(registry, call({ url: "https://1.1.1.1", format: "markdown" }))).toEqual({
        type: "text",
        value: "# Hello\n\nworld",
      })
      expect(yield* executeTool(registry, call({ url: "https://1.1.1.1", format: "text" }))).toEqual({
        type: "text",
        value: "Helloworld",
      })
    }),
  )

  it.effect("returns an error result when HTML-to-Markdown conversion throws", () =>
    Effect.gen(function* () {
      reset()
      respond = () =>
        Effect.succeed(
          new Response("<div>".repeat(10_000) + "content" + "</div>".repeat(10_000), {
            headers: { "content-type": "text/html" },
          }),
        )
      const registry = yield* ToolRegistry.Service
      const url = "https://1.1.1.1/deep-html"

      expect(yield* executeTool(registry, call({ url, format: "markdown" }))).toEqual({
        type: "error",
        value: `Unable to fetch ${url}`,
      })
    }),
  )

  it.effect("rejects declared and streamed oversized bodies", () =>
    Effect.gen(function* () {
      reset()
      const registry = yield* ToolRegistry.Service
      respond = () =>
        Effect.succeed(
          new Response("small", {
            headers: { "content-type": "text/plain", "content-length": String(WebFetchTool.MAX_RESPONSE_BYTES + 1) },
          }),
        )
      expect(yield* executeTool(registry, call({ url: "https://1.1.1.1/declared", format: "text" }))).toEqual({
        type: "error",
        value: "Unable to fetch https://1.1.1.1/declared",
      })

      respond = () =>
        Effect.succeed(
          new Response("x".repeat(WebFetchTool.MAX_RESPONSE_BYTES + 1), { headers: { "content-type": "text/plain" } }),
        )
      expect(yield* executeTool(registry, call({ url: "https://1.1.1.1/streamed", format: "text" }))).toEqual({
        type: "error",
        value: "Unable to fetch https://1.1.1.1/streamed",
      })
    }),
  )

  it.effect("keeps images and files unsupported until typed settlement can carry attachments", () =>
    Effect.gen(function* () {
      reset()
      const registry = yield* ToolRegistry.Service
      respond = () => Effect.succeed(new Response("png", { headers: { "content-type": "image/png" } }))
      expect(yield* executeTool(registry, call({ url: "https://1.1.1.1/image", format: "html" }))).toEqual({
        type: "error",
        value: "Unable to fetch https://1.1.1.1/image",
      })

      respond = () => Effect.succeed(new Response("pdf", { headers: { "content-type": "application/pdf" } }))
      expect(yield* executeTool(registry, call({ url: "https://1.1.1.1/file", format: "html" }))).toEqual({
        type: "error",
        value: "Unable to fetch https://1.1.1.1/file",
      })
    }),
  )

  it.effect("retries Cloudflare challenges with an honest user agent", () =>
    Effect.gen(function* () {
      reset()
      let count = 0
      const url = "https://1.1.1.1/start"
      const target = "https://1.1.1.1/after-challenge"
      respond = () => {
        count++
        if (count === 1) return Effect.succeed(new Response("challenge", { status: 403, headers: { "cf-mitigated": "challenge" } }))
        if (count === 2) return Effect.succeed(new Response("", { status: 302, headers: { location: target } }))
        return Effect.succeed(new Response("ok", { headers: { "content-type": "text/plain" } }))
      }
      const registry = yield* ToolRegistry.Service

      expect(yield* executeTool(registry, call({ url, format: "text" }))).toEqual({
        type: "text",
        value: "ok",
      })
      expect(requests.map((request) => request.url)).toEqual([url, url, target])
      expect(checks.map((check) => check.resources)).toEqual([[target]])
      expect(requests[0]?.headers["user-agent"]).toContain("Mozilla/5.0")
      expect(requests[1]?.headers["user-agent"]).toBe("opencode")
      expect(requests[2]?.headers["user-agent"]).toBe("opencode")
    }),
  )

  it.effect("times out stalled requests", () =>
    Effect.gen(function* () {
      reset()
      respond = () => Effect.never
      const registry = yield* ToolRegistry.Service
      const fiber = yield* executeTool(
        registry,
        call({ url: "https://1.1.1.1/slow", format: "text", timeout: 1 }),
      ).pipe(Effect.forkChild)
      yield* TestClock.adjust(Duration.seconds(1))

      expect(yield* Fiber.join(fiber)).toEqual({ type: "error", value: "Unable to fetch https://1.1.1.1/slow" })
    }),
  )
})
