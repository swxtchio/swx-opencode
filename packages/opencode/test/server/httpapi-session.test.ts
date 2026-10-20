import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { afterEach, describe, expect, test } from "bun:test"
import { NodeHttpServer, NodeServices } from "@effect/platform-node"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { mkdir, open, stat } from "node:fs/promises"
import path from "node:path"
import { setTimeout } from "node:timers/promises"
import { pathToFileURL } from "node:url"
import { Cause, Config, Deferred, Effect, Exit, Fiber, Layer, Option, Queue, Schema, Stream, Tracer } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse, HttpRouter, HttpServer } from "effect/unstable/http"
import { Sse } from "effect/unstable/encoding"
import { layerWebSocketConstructorGlobal } from "effect/unstable/socket/Socket"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Flag } from "@opencode-ai/core/flag/flag"
import { Global } from "@opencode-ai/core/global"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { registerAdapter } from "../../src/control-plane/adapters"
import type { WorkspaceAdapter } from "../../src/control-plane/types"
import { Workspace } from "../../src/control-plane/workspace"

import { InstanceBootstrap as InstanceBootstrapService } from "../../src/project/bootstrap-service"
import { InstanceStore } from "../../src/project/instance-store"
import { Project } from "../../src/project/project"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import * as HttpSessionError from "../../src/server/routes/instance/httpapi/handlers/session-errors"
import { ExperimentalPaths } from "../../src/server/routes/instance/httpapi/groups/experimental"
import { InstancePaths } from "../../src/server/routes/instance/httpapi/groups/instance"
import { SessionPaths } from "../../src/server/routes/instance/httpapi/groups/session"
import { SessionQueuePaths } from "../../src/server/routes/instance/httpapi/groups/session-queue"
import { SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { EventPaths } from "../../src/server/routes/instance/httpapi/groups/event"
import { WorkspacePaths } from "../../src/server/routes/instance/httpapi/groups/workspace"
import type { SessionQueue } from "../../src/session/queue"
import { Session } from "@/session/session"
import { MessageID, PartID, SessionID, type SessionID as SessionIDType } from "../../src/session/schema"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { EventTable } from "@opencode-ai/core/event/sql"
import { EventV2Bridge } from "@/event-v2-bridge"
import { SessionInputTable, SessionMessageTable, SessionTable } from "@opencode-ai/core/session/sql"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { ProviderTurnInterruptedMessage, ProviderTurnInterruptedOrigin } from "@opencode-ai/core/session/error"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import * as DateTime from "effect/DateTime"
import * as TestClock from "effect/testing/TestClock"
import { eq } from "drizzle-orm"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, provideInstanceEffect, TestInstance, tmpdirScoped } from "../fixture/fixture"
import { reply, TestLLMServer } from "../lib/llm-server"
import { testProviderConfig } from "../lib/test-provider"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"
import { spanHold } from "../fixture/span-hold"
import { applyRetentionFixture } from "../fixture/session-retention"

const originalWorkspaces = Flag.OPENCODE_EXPERIMENTAL_WORKSPACES
const noopBootstrapLayer = Layer.succeed(
  InstanceBootstrapService.Service,
  InstanceBootstrapService.Service.of({ run: Effect.void }),
)
const appLayer = AppNodeBuilder.build(
  LayerNode.group([InstanceStore.node, Project.node, Session.node, Workspace.node, Database.node, Ripgrep.node]),
  [[InstanceStore.bootstrapNode, noopBootstrapLayer]],
)
const servedRoutes: Layer.Layer<never, Config.ConfigError, HttpServer.HttpServer> = HttpRouter.serve(
  HttpApiApp.routes,
  {
    disableListenLog: true,
    disableLogger: true,
  },
)
const httpApiLayer = servedRoutes.pipe(
  Layer.provide(layerWebSocketConstructorGlobal),
  Layer.provideMerge(NodeHttpServer.layerTest),
  Layer.provideMerge(NodeServices.layer),
)
const it = testEffect(Layer.mergeAll(appLayer, httpApiLayer))
// Its own served routes, so request fibers carry the hold tracer whatever the shared server was built with.
const racingSpans = spanHold()
const racingRoutes: Layer.Layer<never, Config.ConfigError, HttpServer.HttpServer> = HttpRouter.serve(
  HttpApiApp.routes,
  {
    disableListenLog: true,
    disableLogger: true,
  },
)
const itRacing = testEffect(
  Layer.mergeAll(
    appLayer,
    AppNodeBuilder.build(EventV2Bridge.node),
    racingRoutes.pipe(
      Layer.provide(layerWebSocketConstructorGlobal),
      Layer.provideMerge(NodeHttpServer.layerTest),
      Layer.provideMerge(NodeServices.layer),
    ),
  ).pipe(Layer.provide(Layer.succeed(Tracer.Tracer, racingSpans.tracer))),
)
// Its own served routes on the test clock, so a review hold's lease can lapse
// within a test; the test's own waits run on the live clock.
const clockedRoutes: Layer.Layer<never, Config.ConfigError, HttpServer.HttpServer> = HttpRouter.serve(
  HttpApiApp.routes,
  {
    disableListenLog: true,
    disableLogger: true,
  },
)
const itClocked = testEffect(
  Layer.mergeAll(
    appLayer,
    clockedRoutes.pipe(
      Layer.provide(layerWebSocketConstructorGlobal),
      Layer.provideMerge(NodeHttpServer.layerTest),
      Layer.provideMerge(NodeServices.layer),
    ),
  ).pipe(Layer.provideMerge(TestClock.layer())),
)

function pathFor(path: string, params: Record<string, string>) {
  return Object.entries(params).reduce((result, [key, value]) => result.replace(`:${key}`, value), path)
}

function createSession(input?: Session.CreateInput) {
  return Session.use.create(input)
}

function createTextMessage(sessionID: SessionIDType, text: string) {
  return Effect.gen(function* () {
    const svc = yield* Session.Service
    const info = yield* svc.updateMessage({
      id: MessageID.ascending(),
      role: "user",
      sessionID,
      agent: "build",
      model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
      time: { created: Date.now() },
    })
    const part = yield* svc.updatePart({
      id: PartID.ascending(),
      sessionID,
      messageID: info.id,
      type: "text",
      text,
    })
    return { info, part }
  })
}

const localAdapter = (directory: string): WorkspaceAdapter => ({
  name: "Local Test",
  description: "Create a local test workspace",
  configure: (info) => ({ ...info, name: "local-test", directory }),
  create: async () => {
    await mkdir(directory, { recursive: true })
  },
  async remove() {},
  target: () => ({ type: "local" as const, directory }),
})

const createLocalWorkspace = (input: { projectID: Project.Info["id"]; type: string; directory: string }) =>
  Effect.acquireRelease(
    Effect.gen(function* () {
      registerAdapter(input.projectID, input.type, localAdapter(input.directory))
      return yield* Workspace.Service.use((svc) =>
        svc.create({
          type: input.type,
          branch: null,
          extra: null,
          projectID: input.projectID,
        }),
      )
    }),
    (info) => Workspace.use.remove(info.id).pipe(Effect.ignore),
  )

const insertLegacyAssistantMessage = (sessionID: SessionIDType, seq = 1, time = seq) =>
  Effect.gen(function* () {
    const message = SessionMessage.Assistant.make({
      id: SessionMessage.ID.create(),
      type: "assistant",
      agent: "build",
      model: {
        id: ModelV2.ID.make("model"),
        providerID: ProviderV2.ID.make("provider"),
        variant: ModelV2.VariantID.make("default"),
      },
      time: { created: DateTime.makeUnsafe(time) },
      content: [],
    })
    const { db } = yield* Database.Service
    yield* db
      .insert(SessionMessageTable)
      .values([
        {
          id: message.id,
          session_id: sessionID,
          type: message.type,
          seq,
          time_created: time,
          data: {
            time: { created: time },
            agent: message.agent,
            model: message.model,
            content: message.content,
          } as NonNullable<(typeof SessionMessageTable.$inferInsert)["data"]>,
        },
      ])
      .run()
      .pipe(Effect.orDie)
    return message
  })

const insertCorruptV2Message = (sessionID: SessionIDType, time = 1) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .insert(SessionMessageTable)
      .values([
        {
          id: SessionMessage.ID.create(),
          session_id: sessionID,
          type: "assistant",
          seq: time,
          time_created: time,
          data: {} as NonNullable<(typeof SessionMessageTable.$inferInsert)["data"]>,
        },
      ])
      .run()
      .pipe(Effect.orDie)
  })

const setLegacySummaryDiff = (sessionID: SessionIDType) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .update(SessionTable)
      .set({
        summary_additions: 1,
        summary_deletions: 0,
        summary_files: 1,
        summary_diffs: [{ additions: 1, deletions: 0 }],
      })
      .where(eq(SessionTable.id, sessionID))
      .run()
      .pipe(Effect.orDie)
  })

const getWorkspaceID = (sessionID: SessionIDType) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    return yield* db
      .select({ workspaceID: SessionTable.workspace_id })
      .from(SessionTable)
      .where(eq(SessionTable.id, sessionID))
      .get()
      .pipe(Effect.orDie)
  })

const clearSessionPath = (sessionID: SessionIDType) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db.update(SessionTable).set({ path: null }).where(eq(SessionTable.id, sessionID)).run().pipe(Effect.orDie)
  })

function request(path: string, init?: RequestInit) {
  const url = new URL(path, "http://localhost")
  return HttpClientRequest.fromWeb(new Request(url, init)).pipe(
    HttpClientRequest.setUrl(url.pathname),
    HttpClient.execute,
  )
}

function json<T>(response: HttpClientResponse.HttpClientResponse) {
  if (response.status !== 200) return response.text.pipe(Effect.flatMap((text) => Effect.die(new Error(text))))
  return response.json.pipe(Effect.map((value) => value as T))
}

function responseJson(response: HttpClientResponse.HttpClientResponse) {
  return response.json
}

function requestJson<T>(path: string, init?: RequestInit) {
  return request(path, init).pipe(Effect.flatMap(json<T>))
}

function createNativeSession(directory: string, id?: SessionV2.ID) {
  return requestJson<{ data: SessionV2.Info }>("/api/session", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ id, model: { id: "test-model", providerID: "test" }, location: { directory } }),
  }).pipe(Effect.map(({ data }) => data))
}

const writeNativeTestProvider = (directory: string, url: string) =>
  Effect.promise(() =>
    Bun.write(
      path.join(directory, "opencode.json"),
      JSON.stringify({
        model: "test/test-model",
        providers: {
          test: {
            name: "Test",
            api: { type: "aisdk", package: "@ai-sdk/openai-compatible", url, settings: {} },
            request: { body: { apiKey: "test-key" } },
            models: {
              "test-model": {
                name: "Test Model",
                api: { id: "test-model" },
                capabilities: { tools: true, input: ["text"], output: ["text"] },
                limit: { context: 100_000, output: 10_000 },
                cost: { input: 0, output: 0 },
              },
            },
          },
        },
        permissions: [{ action: "read", resource: "*", effect: "allow" }],
      }),
    ),
  ).pipe(Effect.asVoid)

const awaitNativeModel = (directory: string) =>
  pollWithTimeout(
    requestJson<{ data: Array<{ id: string; providerID: string }> }>("/api/model", {
      headers: { "x-opencode-directory": directory },
    }).pipe(Effect.map(({ data }) => data.find((model) => model.providerID === "test" && model.id === "test-model"))),
    "the configured V2 model did not become available",
    "10 seconds",
  )

const decodeNativeSessionEvents = <E>(bytes: Stream.Stream<Uint8Array, E>) =>
  bytes.pipe(
    Stream.decodeText(),
    Stream.pipeThroughChannel(Sse.decode()),
    Stream.catchTag("Retry", () => Stream.empty),
    Stream.filter((event) => event.data.length > 0),
    Stream.map((event) =>
      Schema.decodeUnknownSync(SessionEvent.Durable)(
        Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(event.data),
      ),
    ),
  )

const openNativeSessionEvents = (sessionID: SessionV2.ID, after: number, headers?: RequestInit["headers"]) =>
  Effect.gen(function* () {
    const response = yield* request(`/api/session/${sessionID}/event?after=${after}`, { headers })
    if (response.status !== 200)
      return yield* Effect.fail(new Error(`Session event stream returned ${response.status}`))
    const events = yield* Queue.unbounded<SessionEvent.DurableEvent>()
    const fiber = yield* decodeNativeSessionEvents(response.stream).pipe(
      Stream.runForEach((event) => Queue.offer(events, event).pipe(Effect.asVoid)),
      Effect.forkScoped,
    )
    return { events, fiber, response }
  })

test("decodes native SSE frames independent of transport chunking", async () => {
  const sessionID = SessionV2.ID.make("ses_sse_decoder")
  const event = (seq: number) =>
    SessionEvent.ContextUpdated.make({
      id: EventV2.ID.create(),
      type: SessionEvent.ContextUpdated.type,
      durable: { aggregateID: sessionID, seq, version: 1 },
      data: {
        sessionID,
        timestamp: DateTime.makeUnsafe(seq),
        messageID: SessionMessage.ID.create(),
        text: "Résumé 🐈",
      },
    })
  const first = event(1)
  const second = event(2)
  const encoder = new TextEncoder()
  const frame = (value: SessionEvent.DurableEvent) =>
    `data: ${JSON.stringify(Schema.encodeUnknownSync(SessionEvent.Durable)(value))}\r\n\r\n`
  const firstFrame = frame(first)
  const secondFrame = frame(second)
  const summary = (events: ReadonlyArray<SessionEvent.DurableEvent>) =>
    events.map((value) => {
      if (value.type !== SessionEvent.ContextUpdated.type) throw new Error(`Unexpected event type: ${value.type}`)
      return { id: value.id, type: value.type, seq: value.durable?.seq, text: value.data.text }
    })
  const collect = (chunks: ReadonlyArray<Uint8Array>) =>
    Effect.runPromise(
      decodeNativeSessionEvents(Stream.fromIterable(chunks)).pipe(
        Stream.runCollect,
        Effect.map((events) => Array.from(events)),
      ),
    )
  const firstExpected = summary([first])

  const firstBytes = encoder.encode(firstFrame)
  const splitAfterTwo = await collect([firstBytes.slice(0, 2), firstBytes.slice(2)])
  expect(summary(splitAfterTwo)).toEqual(firstExpected)

  const bytewiseEvents = await collect(
    Array.from({ length: firstBytes.length }, (_, index) => firstBytes.slice(index, index + 1)),
  )
  expect(summary(bytewiseEvents)).toEqual(firstExpected)

  expect(summary(await collect([encoder.encode(firstFrame + secondFrame)]))).toEqual(summary([first, second]))
})

afterEach(async () => {
  Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = originalWorkspaces
  await disposeAllInstances()
  await resetDatabase()
})

describe("session HttpApi", () => {
  it.effect("maps busy sessions to public session busy errors", () =>
    Effect.gen(function* () {
      const sessionID = SessionID.descending()
      const exit = yield* HttpSessionError.mapBusy(Effect.fail(new Session.BusyError({ sessionID }))).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        expect(Cause.squash(exit.cause)).toMatchObject({
          _tag: "SessionBusyError",
          sessionID,
          message: `Session is busy: ${sessionID}`,
        })
      }
    }),
  )

  it.instance(
    "returns declared not found errors for read routes",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory }
        const missingSession = SessionID.descending()
        const missingSessionBody = {
          name: "NotFoundError",
          data: { message: `Session not found: ${missingSession}` },
        }

        const get = yield* request(pathFor(SessionPaths.get, { sessionID: missingSession }), { headers })
        expect(get.status).toBe(404)
        expect(yield* responseJson(get)).toEqual(missingSessionBody)

        const children = yield* request(pathFor(SessionPaths.children, { sessionID: missingSession }), { headers })
        expect(children.status).toBe(404)
        expect(yield* responseJson(children)).toEqual(missingSessionBody)

        const todo = yield* request(pathFor(SessionPaths.todo, { sessionID: missingSession }), { headers })
        expect(todo.status).toBe(404)
        expect(yield* responseJson(todo)).toEqual(missingSessionBody)

        const messages = yield* request(pathFor(SessionPaths.messages, { sessionID: missingSession }), { headers })
        expect(messages.status).toBe(404)
        expect(yield* responseJson(messages)).toEqual(missingSessionBody)

        const remove = yield* request(pathFor(SessionPaths.remove, { sessionID: missingSession }), {
          headers,
          method: "DELETE",
        })
        expect(remove.status).toBe(404)
        expect(yield* responseJson(remove)).toEqual(missingSessionBody)

        const prompt = yield* request(pathFor(SessionPaths.prompt, { sessionID: missingSession }), {
          headers: { ...headers, "content-type": "application/json" },
          method: "POST",
          body: JSON.stringify({ agent: "build", noReply: true, parts: [{ type: "text", text: "hello" }] }),
        })
        expect(prompt.status).toBe(404)
        expect(yield* responseJson(prompt)).toEqual(missingSessionBody)

        const abort = yield* request(pathFor(SessionPaths.abort, { sessionID: missingSession }), {
          headers,
          method: "POST",
        })
        expect(abort.status).toBe(200)
        expect(yield* responseJson(abort)).toBe(true)

        const session = yield* createSession({ title: "missing message" })
        const missingMessage = MessageID.ascending()
        const message = yield* request(
          pathFor(SessionPaths.message, { sessionID: session.id, messageID: missingMessage }),
          { headers },
        )
        expect(message.status).toBe(404)
        expect(yield* responseJson(message)).toEqual({
          name: "NotFoundError",
          data: { message: `Message not found: ${missingMessage}` },
        })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "serves read routes",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory }
        const parent = yield* createSession({ title: "parent" })
        const child = yield* createSession({ title: "child", parentID: parent.id })
        const message = yield* createTextMessage(parent.id, "hello")
        yield* createTextMessage(parent.id, "world")

        const listed = yield* requestJson<Session.Info[]>(`${SessionPaths.list}?roots=true`, { headers })
        expect(listed.map((item) => item.id)).toContain(parent.id)
        expect(Object.hasOwn(listed[0]!, "parentID")).toBe(false)

        expect(yield* requestJson<Record<string, unknown>>(SessionPaths.status, { headers })).toEqual({})

        expect(
          yield* requestJson<Session.Info>(pathFor(SessionPaths.get, { sessionID: parent.id }), { headers }),
        ).toMatchObject({ id: parent.id, title: "parent" })

        expect(
          (yield* requestJson<Session.Info[]>(pathFor(SessionPaths.children, { sessionID: parent.id }), {
            headers,
          })).map((item) => item.id),
        ).toEqual([child.id])

        expect(
          yield* requestJson<unknown[]>(pathFor(SessionPaths.todo, { sessionID: parent.id }), { headers }),
        ).toEqual([])

        expect(
          yield* requestJson<unknown[]>(pathFor(SessionPaths.diff, { sessionID: parent.id }), { headers }),
        ).toEqual([])

        const messages = yield* request(`${pathFor(SessionPaths.messages, { sessionID: parent.id })}?limit=1`, {
          headers,
        })
        const messagePage = yield* json<SessionV1.WithParts[]>(messages)
        const nextCursor = messages.headers["x-next-cursor"]
        expect(nextCursor).toBeTruthy()
        expect(messagePage[0]?.parts[0]).toMatchObject({ type: "text" })

        expect(
          (yield* request(`${pathFor(SessionPaths.messages, { sessionID: parent.id })}?before=${nextCursor}`, {
            headers,
          })).status,
        ).toBe(400)
        expect(
          (yield* request(`${pathFor(SessionPaths.messages, { sessionID: parent.id })}?limit=1&before=invalid`, {
            headers,
          })).status,
        ).toBe(400)

        expect(
          yield* requestJson<SessionV1.WithParts>(
            pathFor(SessionPaths.message, { sessionID: parent.id, messageID: message.info.id }),
            { headers },
          ),
        ).toMatchObject({ info: { id: message.info.id } })

        yield* insertLegacyAssistantMessage(parent.id)

        expect(
          (yield* requestJson<{ data: SessionMessage.Message[] }>(`/api/session/${parent.id}/message`, {
            headers,
          })).data,
        ).toMatchObject([{ type: "assistant" }])
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

it.live(
  "enhances a draft through the configured provider without creating a session prompt",
  () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.text("clear rewritten draft")
      const base = testProviderConfig(llm.url)
      const config = {
        ...base,
        provider: {
          ...base.provider,
          test: {
            ...base.provider.test,
            options: { ...base.provider.test.options, setCacheKey: true },
          },
        },
      }
      const directory = yield* tmpdirScoped({ git: true, config })
      const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
      const response = yield* request(`${InstancePaths.promptEnhance}?directory=${encodeURIComponent(directory)}`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          model: "test/test-model",
          draft: "rough draft",
          context: "visible session context",
        }),
      })

      expect(response.status).toBe(200)
      expect(yield* json<string>(response)).toBe("clear rewritten draft")
      const calls = yield* llm.inputs
      expect(calls).toHaveLength(1)
      expect(calls[0]?.model).toBe("test-model")
      expect(JSON.stringify(calls[0])).toContain("visible session context")
      expect(JSON.stringify(calls[0])).toContain("rough draft")
      expect(JSON.stringify(calls[0])).toContain("preserving its intent")
      expect(JSON.stringify(calls[0])).toContain("promptCacheKey")
    }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
  30_000,
)

it.live(
  "reports an unavailable prompt enhancement model as a model resolution failure",
  () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
      const response = yield* request(`${InstancePaths.promptEnhance}?directory=${encodeURIComponent(directory)}`, {
        method: "POST",
        headers: { "x-opencode-directory": directory, "content-type": "application/json" },
        body: JSON.stringify({ model: "test/missing-model", draft: "rough draft", context: "" }),
      })

      expect(response.status).toBe(404)
      const body = yield* response.text
      expect(body).toContain("test/missing-model")
      expect(body).toContain("Model not found")
      expect(yield* llm.calls).toBe(0)
    }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
  30_000,
)

it.live(
  "reports enhancement provider failures with the provider error detail",
  () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.error(401, { error: "credentials rejected" })
      const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
      const response = yield* request(`${InstancePaths.promptEnhance}?directory=${encodeURIComponent(directory)}`, {
        method: "POST",
        headers: { "x-opencode-directory": directory, "content-type": "application/json" },
        body: JSON.stringify({ model: "test/test-model", draft: "rough draft", context: "" }),
      })

      expect(response.status).toBe(502)
      const body = yield* response.text
      expect(body).toContain("credentials rejected")
      expect(yield* llm.calls).toBe(1)
    }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
  30_000,
)

it.live(
  "uses the persisted session directory for prompt requests",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.text("ok", { usage: { input: 1, output: 1 } })

        const config = testProviderConfig(llm.url)
        const sessionDirectory = yield* tmpdirScoped({ git: true, config })
        const requestDirectory = yield* tmpdirScoped({ git: true, config })
        const session = yield* createSession({ title: "directory regression" }).pipe(
          provideInstanceEffect(sessionDirectory),
        )

        const response = yield* request(
          `${pathFor(SessionPaths.prompt, { sessionID: session.id })}?directory=${encodeURIComponent(requestDirectory)}`,
          {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              agent: "build",
              model: { providerID: "test", modelID: "test-model" },
              parts: [{ type: "text", text: "which directory?" }],
            }),
          },
        )

        expect(response.status).toBe(200)
        yield* responseJson(response)

        const messages = yield* Session.use
          .messages({ sessionID: session.id })
          .pipe(provideInstanceEffect(sessionDirectory), Effect.orDie)
        const assistant = messages.find((message) => message.info.role === "assistant")
        expect(assistant?.info.role === "assistant" ? assistant.info.path : undefined).toEqual({
          cwd: sessionDirectory,
          root: sessionDirectory,
        })
      }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
    30_000,
  )

  it.live(
    "summarize keeps its initiating compaction root when queued behind a shell",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({
          git: true,
          config: () => ({ ...testProviderConfig(llm.url), shell: "/bin/sh" }),
        })
        const session = yield* createSession({ title: "queued summarize root" }).pipe(provideInstanceEffect(directory))
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const admitNoReply = (text: string) =>
          request(pathFor(SessionPaths.promptAsync, { sessionID: session.id }), {
            method: "POST",
            headers,
            body: JSON.stringify({
              agent: "build",
              model: { providerID: "test", modelID: "test-model" },
              noReply: true,
              parts: [{ type: "text", text }],
            }),
          }).pipe(
            Effect.flatMap((response) => {
              expect(response.status).toBe(204)
              return pollWithTimeout(
                requestJson<SessionV1.WithParts[]>(pathFor(SessionPaths.messages, { sessionID: session.id }), {
                  headers,
                }).pipe(
                  Effect.map((messages) =>
                    messages.find(
                      (message) =>
                        message.info.role === "user" &&
                        message.parts.some((part) => part.type === "text" && part.text === text),
                    ),
                  ),
                ),
                `HTTP prompt_async did not persist ${text}`,
                "10 seconds",
              )
            }),
          )

        const startedFile = path.join(directory, ".http-summarize-started")
        const shell = yield* request(pathFor(SessionPaths.shell, { sessionID: session.id }), {
          method: "POST",
          headers,
          body: JSON.stringify({
            agent: "build",
            model: { providerID: "test", modelID: "test-model" },
            command: `printf started > "${startedFile}"; sleep 15`,
          }),
        }).pipe(Effect.forkChild)
        yield* pollWithTimeout(
          Effect.promise(async () => (await Bun.file(startedFile).exists()) || undefined),
          "HTTP shell did not start",
          "10 seconds",
        )

        yield* llm.text("latest persisted input handled")
        const summarize = yield* request(pathFor(SessionPaths.summarize, { sessionID: session.id }), {
          method: "POST",
          headers,
          body: JSON.stringify({ providerID: "test", modelID: "test-model", auto: false }),
        }).pipe(Effect.forkChild)
        const compactionRoot = yield* pollWithTimeout(
          requestJson<SessionV1.WithParts[]>(pathFor(SessionPaths.messages, { sessionID: session.id }), {
            headers,
          }).pipe(
            Effect.map((messages) =>
              messages.find(
                (message) => message.info.role === "user" && message.parts.some((part) => part.type === "compaction"),
              ),
            ),
          ),
          "HTTP summarize did not persist its compaction input",
          "10 seconds",
        )
        if (compactionRoot.info.role !== "user") throw new Error("expected the initiating compaction message")
        expect(summarize.pollUnsafe()).toBeUndefined()

        const later = yield* admitNoReply("later noReply summarize input")
        if (later.info.role !== "user") throw new Error("expected the later user input")
        expect(later.info.noReply).toBe(true)
        expect(yield* llm.inputs).toHaveLength(0)

        const summarizeResponse = yield* awaitWithTimeout(
          Fiber.join(summarize),
          "HTTP summarize did not finish after the shell released",
          "30 seconds",
        )
        expect(summarizeResponse.status).toBe(200)
        expect(yield* json<boolean>(summarizeResponse)).toBe(true)
        const shellResponse = yield* awaitWithTimeout(Fiber.join(shell), "HTTP shell did not finish", "10 seconds")
        expect(shellResponse.status).toBe(200)

        const messages = yield* requestJson<SessionV1.WithParts[]>(
          pathFor(SessionPaths.messages, { sessionID: session.id }),
          { headers },
        )
        const summary = messages.findLast(
          (message) => message.info.role === "assistant" && message.info.parentID === compactionRoot.info.id,
        )
        expect(summary?.info.role).toBe("assistant")
        expect(summary?.info.role === "assistant" ? summary.info.summary : undefined).toBe(true)
        expect(
          messages.some((message) => message.info.role === "assistant" && message.info.parentID === later.info.id),
        ).toBe(false)
        expect(JSON.stringify(yield* llm.inputs)).not.toContain("later noReply summarize input")
        expect(yield* llm.calls).toBe(1)
      }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
    60_000,
  )

  it.instance(
    "returns v2 public request errors for cursor and workspace query failures",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory }
        const session = yield* createSession({ title: "v2 cursor" })
        const firstMessage = yield* insertLegacyAssistantMessage(session.id, 1, 2)
        const secondMessage = yield* insertLegacyAssistantMessage(session.id, 2, 1)

        const sessionPage = yield* request(
          `/api/session?${new URLSearchParams({
            limit: "1",
            order: "asc",
            directory: test.directory,
            search: "v2",
          })}`,
          { headers },
        )
        const sessionCursor = (yield* json<{ data: Session.Info[]; cursor: { next?: string } }>(sessionPage)).cursor
          .next
        expect(sessionCursor).toBeTruthy()
        expect(JSON.parse(Buffer.from(sessionCursor!, "base64url").toString("utf8"))).toMatchObject({
          order: "asc",
          directory: test.directory,
          search: "v2",
          anchor: { id: session.id, direction: "next" },
        })

        const sessionNextPage = yield* request(`/api/session?cursor=${sessionCursor}`, { headers })
        expect(sessionNextPage.status).toBe(200)

        const invalidSessionCursor = yield* request(`/api/session?cursor=invalid`, { headers })
        expect(invalidSessionCursor.status).toBe(400)
        expect(yield* responseJson(invalidSessionCursor)).toMatchObject({
          _tag: "InvalidCursorError",
          message: "Invalid cursor",
        })

        const invalidWorkspace = yield* request(`/api/session?workspace=bad`, { headers })
        expect(invalidWorkspace.status).toBe(400)
        expect(yield* responseJson(invalidWorkspace)).toMatchObject({
          _tag: "InvalidRequestError",
          kind: "Query",
        })

        const messagePage = yield* request(`/api/session/${session.id}/message?limit=1`, { headers })
        const messageBody = yield* json<{ data: SessionMessage.Message[]; cursor: { next?: string } }>(messagePage)
        const messageCursor = messageBody.cursor.next
        expect(messageCursor).toBeTruthy()
        expect(messageBody.data.map((message) => message.id)).toEqual([secondMessage.id])
        expect(JSON.parse(Buffer.from(messageCursor!, "base64url").toString("utf8"))).toEqual({
          id: secondMessage.id,
          order: "desc",
          direction: "next",
        })

        const nextMessagePage = yield* request(`/api/session/${session.id}/message?cursor=${messageCursor}`, {
          headers,
        })
        expect(
          (yield* json<{ data: SessionMessage.Message[] }>(nextMessagePage)).data.map((message) => message.id),
        ).toEqual([firstMessage.id])

        const legacyMessageCursor = Buffer.from(
          JSON.stringify({ id: secondMessage.id, time: 1, order: "desc", direction: "next" }),
        ).toString("base64url")
        const legacyMessagePage = yield* request(`/api/session/${session.id}/message?cursor=${legacyMessageCursor}`, {
          headers,
        })
        expect(
          (yield* json<{ data: SessionMessage.Message[] }>(legacyMessagePage)).data.map((message) => message.id),
        ).toEqual([firstMessage.id])

        const messageCursorWithOrder = yield* request(
          `/api/session/${session.id}/message?cursor=${messageCursor}&order=asc`,
          { headers },
        )
        expect(messageCursorWithOrder.status).toBe(400)
        expect(yield* responseJson(messageCursorWithOrder)).toMatchObject({
          _tag: "InvalidCursorError",
          message: "Cursor cannot be combined with order",
        })

        const invalidMessageCursor = yield* request(`/api/session/${session.id}/message?cursor=invalid`, { headers })
        expect(invalidMessageCursor.status).toBe(400)
        expect(yield* responseJson(invalidMessageCursor)).toMatchObject({
          _tag: "InvalidCursorError",
          message: "Invalid cursor",
        })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "returns v2 public not found errors for missing sessions",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory }
        const missing = SessionID.descending()
        const expected = {
          _tag: "SessionNotFoundError",
          sessionID: missing,
          message: `Session not found: ${missing}`,
        }

        const messages = yield* request(`/api/session/${missing}/message`, { headers })
        expect(messages.status).toBe(404)
        expect(yield* responseJson(messages)).toEqual(expected)

        const context = yield* request(`/api/session/${missing}/context`, { headers })
        expect(context.status).toBe(404)
        expect(yield* responseJson(context)).toEqual(expected)

        const compact = yield* request(`/api/session/${missing}/compact`, { method: "POST", headers })
        expect(compact.status).toBe(404)
        expect(yield* responseJson(compact)).toEqual(expected)

        const wait = yield* request(`/api/session/${missing}/wait`, { method: "POST", headers })
        expect(wait.status).toBe(404)
        expect(yield* responseJson(wait)).toEqual(expected)

        const prompt = yield* request(`/api/session/${missing}/prompt`, {
          method: "POST",
          headers: { ...headers, "content-type": "application/json" },
          body: JSON.stringify({ prompt: { text: "hello" } }),
        })
        expect(prompt.status).toBe(404)
        expect(yield* responseJson(prompt)).toEqual(expected)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "durably records one v2 prompt for exact message-ID retries",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory }
        const session = yield* createSession({ title: "v2 prompt recording" })

        const recordPrompt = () =>
          request(`/api/session/${session.id}/prompt`, {
            method: "POST",
            headers: { ...headers, "content-type": "application/json" },
            body: JSON.stringify({ id: "msg_http_prompt", prompt: { text: "hello" }, resume: false }),
          })
        const first = yield* recordPrompt()
        const retried = yield* recordPrompt()
        type PromptBody = { id: string; prompt: { text: string }; delivery: string; promotedSeq?: number }
        const firstBody = yield* json<{ data: PromptBody }>(first)
        const retriedBody = yield* json<{ data: PromptBody }>(retried)
        expect(first.status).toBe(200)
        expect(retried.status).toBe(200)
        expect(retriedBody).toEqual(firstBody)
        expect(firstBody).toMatchObject({
          data: { id: "msg_http_prompt", prompt: { text: "hello" }, delivery: "steer" },
        })

        const messages = yield* requestJson<{ data: PromptBody[] }>(`/api/session/${session.id}/message`, {
          headers,
        })
        expect(messages.data).toHaveLength(0)
        const admitted = yield* Database.Service.use(({ db }) =>
          db
            .select()
            .from(SessionInputTable)
            .where(eq(SessionInputTable.id, SessionMessage.ID.make("msg_http_prompt")))
            .get()
            .pipe(Effect.orDie),
        )
        expect(admitted).toMatchObject({
          id: "msg_http_prompt",
          session_id: session.id,
          delivery: "steer",
          promoted_seq: null,
        })
        const conflict = yield* request(`/api/session/${session.id}/prompt`, {
          method: "POST",
          headers: { ...headers, "content-type": "application/json" },
          body: JSON.stringify({ id: "msg_http_prompt", prompt: { text: "goodbye" } }),
        })
        expect(conflict.status).toBe(409)
        expect(yield* responseJson(conflict)).toEqual({
          _tag: "ConflictError",
          message: "Prompt message ID conflicts with an existing durable record: msg_http_prompt",
          resource: "msg_http_prompt",
        })

        const wakeID = SessionMessage.ID.make("msg_http_wake")
        const wake = yield* request(`/api/session/${session.id}/prompt`, {
          method: "POST",
          headers: { ...headers, "content-type": "application/json" },
          body: JSON.stringify({ id: wakeID, prompt: { text: "hello again" } }),
        })
        expect(wake.status).toBe(200)
        const message = yield* pollWithTimeout(
          requestJson<{ data: SessionMessage.Message[] }>(`/api/session/${session.id}/message`, { headers }).pipe(
            Effect.map(({ data }) => data.find((message) => message.id === wakeID)),
          ),
          "V2 prompt was not promoted after wake",
          "10 seconds",
        )
        expect(message).toMatchObject({ id: wakeID, type: "user" })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "returns completed native wait results with explicit and persisted caller directories",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({
          git: true,
          init: (directory) => writeNativeTestProvider(directory, llm.url),
        })
        const legacy = yield* createSession({ title: "v2 compact unavailable" })
        const compact = yield* request(`/api/session/${legacy.id}/compact`, {
          method: "POST",
          headers: { "x-opencode-directory": directory },
        })
        expect(compact.status).toBe(503)
        expect(yield* responseJson(compact)).toEqual({
          _tag: "ServiceUnavailableError",
          message: "Session compact is not available yet",
          service: "session.compact",
        })

        for (const [index, includeDirectory] of [true, false].entries()) {
          const id = SessionV2.ID.make(includeDirectory ? "ses_wait_explicit" : "ses_wait_persisted")
          const session = yield* createNativeSession(directory, id)
          yield* awaitNativeModel(directory)
          const directoryHeader: Record<string, string> = includeDirectory ? { "x-opencode-directory": directory } : {}
          const idleResponse = yield* request(`/api/session/${session.id}/wait`, {
            method: "POST",
            headers: directoryHeader,
          })
          expect(idleResponse.status).toBe(200)
          expect(yield* json<{ data: SessionV2.WaitResult }>(idleResponse)).toEqual({ data: { type: "idle" } })
          yield* llm.text(`Answer for ${id}`)
          const headers = { ...directoryHeader, "content-type": "application/json" }
          const admission = yield* requestJson<{ data: { id: string; admittedSeq: number } }>(
            `/api/session/${session.id}/prompt`,
            {
              method: "POST",
              headers,
              body: JSON.stringify({ prompt: { text: `Prompt for ${id}` } }),
            },
          )
          yield* awaitWithTimeout(llm.wait(index + 1), `provider did not start for ${id}`, "10 seconds")

          const wait = yield* request(`/api/session/${session.id}/wait`, {
            method: "POST",
            headers: directoryHeader,
          })
          expect(wait.status).toBe(200)
          const outcome = yield* json<{ data: SessionV2.WaitResult }>(wait)
          expect(outcome.data).toMatchObject({ type: "completed", admittedSeq: admission.data.admittedSeq })
          if (outcome.data.type !== "completed") throw new Error(`Expected completed wait for ${id}`)
          const completed = outcome.data

          const messages = yield* requestJson<{ data: SessionMessage.Message[] }>(
            `/api/session/${session.id}/message`,
            { headers: directoryHeader },
          )
          const user = messages.data.find((message) => message.id === admission.data.id)
          const assistant = messages.data.find((message) => message.id === completed.assistantMessageID)
          if (assistant === undefined) throw new Error(`Wait assistant was not projected for ${id}`)
          const single = yield* requestJson<{ data: SessionMessage.Message }>(
            `/api/session/${session.id}/message/${completed.assistantMessageID}`,
            { headers: directoryHeader },
          )
          const history = yield* requestJson<{ data: SessionEvent.DurableEvent[]; hasMore: boolean }>(
            `/api/session/${session.id}/history?limit=100`,
            { headers: directoryHeader },
          )

          expect(user).toMatchObject({ id: admission.data.id, type: "user", text: `Prompt for ${id}` })
          expect(assistant).toMatchObject({ id: outcome.data.assistantMessageID, type: "assistant" })
          expect(single.data).toEqual(assistant)
          expect(history.hasMore).toBe(false)
          expect(history.data.map((event) => event.type)).toContain(SessionEvent.Step.Ended.type)
          const providerCalls = yield* llm.calls
          expect(providerCalls).toBe(index + 1)
        }
        expect(yield* llm.calls).toBe(2)
      }).pipe(Effect.provide(TestLLMServer.layer)),
    { git: true, config: { formatter: false, lsp: false } },
    30_000,
  )

  it.instance(
    "logs producer wait registration and resolution for a held HTTP wait",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({
          git: true,
          init: (directory) => writeNativeTestProvider(directory, llm.url),
        })
        const session = yield* createNativeSession(directory, SessionV2.ID.make("ses_wait_registration_http"))
        yield* awaitNativeModel(directory)
        let releaseFinal = () => {}
        const finalGate = new Promise<void>((resolve) => (releaseFinal = resolve))
        yield* Effect.addFinalizer(() => Effect.sync(releaseFinal))
        yield* llm.hold("Wait registration was observed", finalGate)

        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const admission = yield* requestJson<{ data: { id: string; admittedSeq: number } }>(
          `/api/session/${session.id}/prompt`,
          { method: "POST", headers, body: JSON.stringify({ prompt: { text: "Hold a wait registration" } }) },
        )
        yield* awaitWithTimeout(llm.wait(1), "wait-registration provider request did not start", "10 seconds")
        const logFile = path.join(Global.Path.log, "opencode.log")
        let logOffset = yield* Effect.promise(() => stat(logFile).then((info) => info.size, () => 0))
        let logText = ""
        const waitResponse = yield* Deferred.make<HttpClientResponse.HttpClientResponse>()
        const waiter = yield* request(`/api/session/${session.id}/wait`, { method: "POST", headers }).pipe(
          Effect.tap((response) => Deferred.succeed(waitResponse, response)),
          Effect.forkScoped,
        )
        const waitLog = (event: string, waitID?: string) =>
          Effect.promise(async () => {
            const deadline = performance.now() + 10_000
            while (performance.now() < deadline) {
              const handle = await open(logFile, "r").catch(() => undefined)
              if (handle) {
                try {
                  const info = await handle.stat()
                  if (info.size > logOffset) {
                    const buffer = Buffer.alloc(info.size - logOffset)
                    const read = await handle.read(buffer, 0, buffer.length, logOffset)
                    logOffset += read.bytesRead
                    logText += buffer.toString("utf8", 0, read.bytesRead)
                  }
                } finally {
                  await handle.close()
                }
              }
              const match = logText.split(/\r?\n/).find(
                (line) =>
                  line.includes(`event=${event}`) &&
                  line.includes(`session_id=${session.id}`) &&
                  line.includes(`admitted_seq=${admission.data.admittedSeq}`) &&
                  line.includes(`message_id=${admission.data.id}`) &&
                  (waitID === undefined || line.includes(`wait_id=${waitID}`)),
              )
              if (match) return match
              await setTimeout(25)
            }
            throw new Error(`timed out waiting for producer wait log ${event}`)
          })
        const registration = yield* waitLog("session.wait.registered").pipe(
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Effect.sync(releaseFinal) : Effect.void)),
        )
        const waitID = /(?:^|\s)wait_id=([^\s]+)/.exec(registration)?.[1]
        if (!waitID) throw new Error(`wait registration had no server-generated wait ID: ${registration}`)
        expect(yield* Deferred.isDone(waitResponse)).toBe(false)

        releaseFinal()
        const response = yield* awaitWithTimeout(
          Deferred.await(waitResponse),
          "HTTP wait did not return after releasing the provider",
          "10 seconds",
        )
        expect(response.status).toBe(200)
        const outcome = yield* json<{ data: SessionV2.WaitResult }>(response)
        expect(outcome.data).toMatchObject({ type: "completed", admittedSeq: admission.data.admittedSeq })
        yield* Fiber.join(waiter)
        const resolution = yield* waitLog("session.wait.resolved", waitID)
        expect(resolution).toContain("outcome=completed")
      }).pipe(Effect.provide(TestLLMServer.layer)),
    { git: true, config: { formatter: false, lsp: false } },
    30_000,
  )

  it.instance(
    "returns pending for native admit-only work",
    () =>
      Effect.gen(function* () {
        const directory = yield* tmpdirScoped({ git: true })
        const sessionID = SessionV2.ID.make("ses_wait_pending")
        const session = yield* createNativeSession(directory, sessionID)
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const admission = yield* requestJson<{ data: { id: string; admittedSeq: number } }>(
          `/api/session/${session.id}/prompt`,
          {
            method: "POST",
            headers,
            body: JSON.stringify({ prompt: { text: "Admit without resuming" }, resume: false }),
          },
        )

        const pending = yield* request(`/api/session/${session.id}/wait`, { method: "POST", headers })
        expect(pending.status).toBe(200)
        expect(yield* json<{ data: SessionV2.WaitResult }>(pending)).toMatchObject({
          data: { type: "pending", admittedSeq: admission.data.admittedSeq, messageID: admission.data.id },
        })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "resumes terminal tool and assistant events after an HTTP disconnect",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({
          git: true,
          init: (directory) =>
            writeNativeTestProvider(directory, llm.url).pipe(
              Effect.andThen(
                Effect.promise(() => Bun.write(path.join(directory, "cursor.txt"), "terminal tool payload")).pipe(
                  Effect.asVoid,
                ),
              ),
            ),
        })
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        yield* awaitNativeModel(directory)
        let releaseFinal = () => {}
        const finalGate = new Promise<void>((resolve) => {
          releaseFinal = resolve
        })
        yield* Effect.addFinalizer(() => Effect.sync(releaseFinal))
        const session = yield* createNativeSession(directory, SessionV2.ID.make("ses_wait_cursor_http"))
        const { db } = yield* Database.Service
        const initialCursor = yield* EventV2.latestSequence(db, session.id)
        yield* llm.tool("read", { path: "cursor.txt" })
        yield* llm.hold("Final answer", finalGate)
        const admission = yield* requestJson<{ data: { id: string; admittedSeq: number } }>(
          `/api/session/${session.id}/prompt`,
          {
            method: "POST",
            headers,
            body: JSON.stringify({ prompt: { text: "Run and reconnect" } }),
          },
        )
        yield* awaitWithTimeout(llm.wait(1), "tool provider request never started", "10 seconds")
        const live = yield* openNativeSessionEvents(session.id, initialCursor, headers)
        const waitResponse = yield* Deferred.make<HttpClientResponse.HttpClientResponse>()
        const waiter = yield* request(`/api/session/${session.id}/wait`, { method: "POST", headers }).pipe(
          Effect.tap((response) => Deferred.succeed(waitResponse, response)),
          Effect.forkScoped,
        )
        yield* Effect.yieldNow
        expect(yield* Deferred.isDone(waitResponse)).toBe(false)

        const consumed: SessionEvent.DurableEvent[] = []
        let consumedTool: SessionEvent.Tool.Success | undefined
        while (consumedTool === undefined) {
          const event = yield* Queue.take(live.events).pipe(
            Effect.timeoutOrElse({
              duration: "10 seconds",
              orElse: () => Effect.fail(new Error("timed out waiting for the terminal tool event")),
            }),
          )
          consumed.push(event)
          if (event.type === SessionEvent.Tool.Success.type) consumedTool = event
        }
        if (consumedTool.durable === undefined) throw new Error("Consumed terminal tool event had no cursor")
        const toolCursor = consumedTool.durable.seq
        yield* awaitWithTimeout(llm.wait(2), "final provider request never started", "10 seconds")
        expect(yield* Deferred.isDone(waitResponse)).toBe(false)
        yield* Fiber.interrupt(live.fiber)
        yield* Effect.sync(releaseFinal)

        const completedResponse = yield* awaitWithTimeout(
          Deferred.await(waitResponse),
          "HTTP wait did not return after assistant publication",
          "10 seconds",
        )
        expect(completedResponse.status).toBe(200)
        const completed = yield* json<{ data: SessionV2.WaitResult }>(completedResponse)
        expect(completed.data).toMatchObject({ type: "completed", admittedSeq: admission.data.admittedSeq })
        if (completed.data.type !== "completed") throw new Error("Expected completed HTTP wait")
        yield* Fiber.join(waiter)

        const allHistory = yield* requestJson<{ data: SessionEvent.DurableEvent[]; hasMore: boolean }>(
          `/api/session/${session.id}/history?limit=100`,
          { headers },
        )
        const resumedHistory = yield* requestJson<{ data: SessionEvent.DurableEvent[]; hasMore: boolean }>(
          `/api/session/${session.id}/history?after=${toolCursor}&limit=100`,
          { headers },
        )
        const resumed = yield* openNativeSessionEvents(session.id, toolCursor, headers)
        const resumedEvents: SessionEvent.DurableEvent[] = []
        for (let index = 0; index < resumedHistory.data.length; index++)
          resumedEvents.push(
            yield* Queue.take(resumed.events).pipe(
              Effect.timeoutOrElse({
                duration: "10 seconds",
                orElse: () => Effect.fail(new Error("timed out resuming native Session events")),
              }),
            ),
          )
        yield* Fiber.interrupt(resumed.fiber)

        expect(allHistory.hasMore).toBe(false)
        expect(allHistory.data.find((event) => event.id === consumedTool.id)?.durable?.seq).toBe(toolCursor)
        const consumedPrefix = allHistory.data.filter(
          (event) => event.durable !== undefined && event.durable.seq <= toolCursor,
        )
        expect(consumed.map((event) => [event.id, event.durable?.seq])).toEqual(
          consumedPrefix.map((event) => [event.id, event.durable?.seq]),
        )
        expect(resumedHistory.hasMore).toBe(false)
        expect(resumedEvents.map((event) => [event.id, event.durable?.seq])).toEqual(
          resumedHistory.data.map((event) => [event.id, event.durable?.seq]),
        )
        expect(new Set(resumedEvents.map((event) => event.id)).size).toBe(resumedEvents.length)
        expect(resumedEvents.map((event) => event.id)).not.toContain(consumedTool.id)
        expect(
          resumedEvents.some((event) => event.type === SessionEvent.Step.Ended.type && event.data.finish === "stop"),
        ).toBe(true)
        expect(consumed.map((event) => event.id)).toContain(consumedTool.id)
        const allObserved = [...consumed, ...resumedEvents]
        expect(allObserved.map((event) => event.id)).toEqual(allHistory.data.map((event) => event.id))
        expect(new Set(allObserved.map((event) => event.id)).size).toBe(allObserved.length)
        const providerCalls = yield* llm.calls
        expect(providerCalls).toBe(2)
      }).pipe(Effect.provide(TestLLMServer.layer)),
    { git: true, config: { formatter: false, lsp: false } },
    60_000,
  )

  it.instance(
    "returns a failed terminal-tool outcome through native HTTP wait",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({
          git: true,
          init: (directory) => writeNativeTestProvider(directory, llm.url),
        })
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        yield* awaitNativeModel(directory)

        const failedSession = yield* createNativeSession(directory, SessionV2.ID.make("ses_wait_failed_http"))
        yield* llm.tool("missing", {})
        yield* llm.text("Handled the missing tool")
        const failedAdmission = yield* requestJson<{ data: { admittedSeq: number } }>(
          `/api/session/${failedSession.id}/prompt`,
          {
            method: "POST",
            headers,
            body: JSON.stringify({ prompt: { text: "Fail the provider turn" } }),
          },
        )
        yield* awaitWithTimeout(llm.wait(1), "failed tool provider request never started", "10 seconds")
        yield* awaitWithTimeout(llm.wait(2), "failure handling continuation never started", "10 seconds")
        const failedResponse = yield* request(`/api/session/${failedSession.id}/wait`, {
          method: "POST",
          headers,
        })
        expect(failedResponse.status).toBe(200)
        const failed = yield* json<{ data: SessionV2.WaitResult }>(failedResponse)
        expect(failed.data).toMatchObject({ type: "failed", admittedSeq: failedAdmission.data.admittedSeq })
        const failedMessages = yield* requestJson<{ data: SessionMessage.Message[] }>(
          `/api/session/${failedSession.id}/message`,
          { headers },
        )
        const failedTool = failedMessages.data
          .filter((message): message is SessionMessage.Assistant => message.type === "assistant")
          .flatMap((message) => message.content)
          .find((content) => content.type === "tool" && content.state.status === "error")
        expect(failedTool).toBeDefined()
        const failedHistory = yield* requestJson<{ data: SessionEvent.DurableEvent[] }>(
          `/api/session/${failedSession.id}/history?limit=100`,
          { headers },
        )
        expect(failedHistory.data.map((event) => event.type)).toContain(SessionEvent.Tool.Failed.type)
        const providerCalls = yield* llm.calls
        expect(providerCalls).toBe(2)
      }).pipe(Effect.provide(TestLLMServer.layer)),
    { git: true, config: { formatter: false, lsp: false } },
    30_000,
  )

  it.instance(
    "returns an interrupted native wait after publishing terminal tool cleanup",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({
          git: true,
          init: (directory) => writeNativeTestProvider(directory, llm.url),
        })
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        yield* awaitNativeModel(directory)
        const interruptedSession = yield* createNativeSession(directory, SessionV2.ID.make("ses_wait_interrupted_http"))
        yield* llm.toolHang("read", { path: "cursor.txt" })
        const interruptedAdmission = yield* requestJson<{ data: { admittedSeq: number } }>(
          `/api/session/${interruptedSession.id}/prompt`,
          {
            method: "POST",
            headers,
            body: JSON.stringify({ prompt: { text: "Interrupt the provider turn" } }),
          },
        )
        yield* awaitWithTimeout(llm.wait(1), "held provider request never started", "10 seconds")
        const partial = yield* pollWithTimeout(
          requestJson<{ data: SessionMessage.Message[] }>(`/api/session/${interruptedSession.id}/message`, {
            headers,
          }).pipe(
            Effect.map(({ data }) =>
              data.find(
                (message) => message.type === "assistant" && message.content.some((content) => content.type === "tool"),
              ),
            ),
          ),
          "held provider turn did not publish its tool projection",
          "10 seconds",
        )
        expect(partial?.type).toBe("assistant")
        if (partial?.type !== "assistant") throw new Error("Expected a projected assistant while the provider is held")
        expect(partial.time.completed).toBeUndefined()
        expect(partial.content.some((content) => content.type === "tool" && content.state.status === "pending")).toBe(
          true,
        )
        const waitResponse = yield* Deferred.make<HttpClientResponse.HttpClientResponse>()
        const waiting = yield* request(`/api/session/${interruptedSession.id}/wait`, {
          method: "POST",
          headers,
        }).pipe(
          Effect.tap((response) => Deferred.succeed(waitResponse, response)),
          Effect.forkChild,
        )
        yield* Effect.yieldNow
        expect(yield* Deferred.isDone(waitResponse)).toBe(false)
        const interruptExit = yield* request(`/api/session/${interruptedSession.id}/interrupt`, {
          method: "POST",
          headers,
        }).pipe(Effect.exit)
        if (Exit.isFailure(interruptExit)) return yield* Effect.failCause(interruptExit.cause)
        const interrupt = interruptExit.value
        expect(interrupt.status).toBe(204)
        const interruptedResponse = yield* awaitWithTimeout(
          Deferred.await(waitResponse),
          "wait did not settle after interruption",
          "10 seconds",
        )
        expect(interruptedResponse.status).toBe(200)
        const interrupted = yield* json<{ data: SessionV2.WaitResult }>(interruptedResponse)
        expect(interrupted.data).toMatchObject({
          type: "interrupted",
          admittedSeq: interruptedAdmission.data.admittedSeq,
        })
        const interruptedMessages = yield* requestJson<{ data: SessionMessage.Message[] }>(
          `/api/session/${interruptedSession.id}/message`,
          { headers },
        )
        const interruptedAssistant = interruptedMessages.data.find((message) => message.type === "assistant")
        expect(interruptedAssistant).toMatchObject({
          type: "assistant",
          finish: "error",
          error: { message: ProviderTurnInterruptedMessage, origin: ProviderTurnInterruptedOrigin },
        })
        const interruptedHistory = yield* requestJson<{ data: SessionEvent.DurableEvent[] }>(
          `/api/session/${interruptedSession.id}/history?limit=100`,
          { headers },
        )
        expect(interruptedHistory.data.map((event) => event.type)).toContain(SessionEvent.Tool.Failed.type)
        const providerCalls = yield* llm.calls
        expect(providerCalls).toBe(1)
        yield* Fiber.join(waiting)
      }).pipe(Effect.provide(TestLLMServer.layer)),
    { git: true, config: { formatter: false, lsp: false } },
    30_000,
  )

  it.instance(
    "refuses V2 history and event readers for a redacted aggregate before returning content",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const session = yield* createSession({ title: "v2 retention sentinel" })
        const producerMessage = yield* createTextMessage(session.id, "v2 body sentinel")
        const { db } = yield* Database.Service
        const currentSequence = yield* EventV2.latestSequence(db, session.id)
        yield* applyRetentionFixture(session.id)
        const headers = { "x-opencode-directory": test.directory }

        const wait = yield* request(`/api/session/${session.id}/wait`, { method: "POST", headers })
        expect(wait.status).toBe(503)
        expect(yield* responseJson(wait)).toMatchObject({
          _tag: "ServiceUnavailableError",
          message: "Session wait outcome is not observable",
          service: "session.wait",
        })

        const history = yield* request(`/api/session/${session.id}/history?after=0&limit=10`, { headers })
        expect(history.status).not.toBe(200)
        expect(yield* history.text).not.toContain("v2 body sentinel")

        const currentHistory = yield* request(`/api/session/${session.id}/history?after=${currentSequence}&limit=10`, {
          headers,
        })
        expect(currentHistory.status).not.toBe(200)
        expect(yield* currentHistory.text).not.toContain("v2 body sentinel")

        const eventsV2 = yield* request(`/api/session/${session.id}/event?after=${currentSequence}`, { headers })
        expect(eventsV2.status).not.toBe(200)
        expect(yield* eventsV2.text).not.toContain("v2 body sentinel")

        const messagesV2 = yield* request(`/api/session/${session.id}/message`, { headers })
        expect(messagesV2.status).not.toBe(200)
        expect(yield* messagesV2.text).not.toContain("v2 body sentinel")

        const context = yield* request(`/api/session/${session.id}/context`, { headers })
        expect(context.status).not.toBe(200)
        expect(yield* context.text).not.toContain("v2 body sentinel")

        const messagesV1 = yield* request(pathFor(SessionPaths.messages, { sessionID: session.id }), { headers })
        expect(messagesV1.status).not.toBe(200)
        expect(yield* messagesV1.text).not.toContain("v2 body sentinel")

        const messageV1 = yield* request(
          pathFor(SessionPaths.message, { sessionID: session.id, messageID: producerMessage.info.id }),
          { headers },
        )
        expect(messageV1.status).not.toBe(200)
        expect(yield* messageV1.text).not.toContain("v2 body sentinel")

        const todo = yield* request(pathFor(SessionPaths.todo, { sessionID: session.id }), { headers })
        expect(todo.status).not.toBe(200)

        const queue = yield* request(pathFor(SessionQueuePaths.list, { sessionID: session.id }), { headers })
        expect(queue.status).not.toBe(200)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "returns safe v2 unknown errors for corrupt projected messages",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const session = yield* createSession({ title: "v2 corrupt message" })
        yield* insertCorruptV2Message(session.id)

        const messages = yield* request(`/api/session/${session.id}/message`, {
          headers: { "x-opencode-directory": test.directory },
        })
        const messagesBody = yield* responseJson(messages)
        expect(messages.status).toBe(500)
        expect(messagesBody).toMatchObject({
          _tag: "UnknownError",
          message: "Unexpected server error. Check server logs for details.",
        })
        expect((messagesBody as { ref?: unknown }).ref).toMatch(/^err_[0-9a-f-]{8}$/)
        expect(JSON.stringify(messagesBody)).not.toContain("assistant")

        const context = yield* request(`/api/session/${session.id}/context`, {
          headers: { "x-opencode-directory": test.directory },
        })
        const contextBody = yield* responseJson(context)
        expect(context.status).toBe(500)
        expect(contextBody).toMatchObject({
          _tag: "UnknownError",
          message: "Unexpected server error. Check server logs for details.",
        })
        expect((contextBody as { ref?: unknown }).ref).toMatch(/^err_[0-9a-f-]{8}$/)
        expect(JSON.stringify(contextBody)).not.toContain("assistant")
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "serves sessions with migrated summary diffs missing file details",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const session = yield* createSession({ title: "legacy diff" })
        yield* setLegacySummaryDiff(session.id)

        const response = yield* request(pathFor(SessionPaths.get, { sessionID: session.id }), {
          headers: { "x-opencode-directory": test.directory },
        })

        expect(response.status).toBe(200)
        expect((yield* json<Session.Info>(response)).summary?.diffs).toEqual([{ additions: 1, deletions: 0 }])
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "serves lifecycle mutation routes",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory, "content-type": "application/json" }

        const createdEmpty = yield* requestJson<Session.Info>(SessionPaths.create, {
          method: "POST",
          headers,
        })
        expect(createdEmpty.id).toBeTruthy()

        const created = yield* requestJson<Session.Info>(SessionPaths.create, {
          method: "POST",
          headers,
          body: JSON.stringify({ title: "created" }),
        })
        expect(created.title).toBe("created")

        const updated = yield* requestJson<Session.Info>(pathFor(SessionPaths.update, { sessionID: created.id }), {
          method: "PATCH",
          headers,
          body: JSON.stringify({ title: "updated", time: { archived: 1 } }),
        })
        expect(updated).toMatchObject({ id: created.id, title: "updated", time: { archived: 1 } })

        const forked = yield* requestJson<Session.Info>(pathFor(SessionPaths.fork, { sessionID: created.id }), {
          method: "POST",
          headers,
        })
        expect(forked.id).not.toBe(created.id)

        const forkedWithoutContentType = yield* requestJson<Session.Info>(
          pathFor(SessionPaths.fork, { sessionID: created.id }),
          {
            method: "POST",
            headers: { "x-opencode-directory": test.directory },
          },
        )
        expect(forkedWithoutContentType.id).not.toBe(created.id)

        const invalidFork = yield* request(pathFor(SessionPaths.fork, { sessionID: created.id }), {
          method: "POST",
          headers,
          body: "{",
        })
        expect(invalidFork.status).toBe(400)

        const forkedWhitespace = yield* requestJson<Session.Info>(
          pathFor(SessionPaths.fork, { sessionID: created.id }),
          {
            method: "POST",
            headers,
            body: "  \n",
          },
        )
        expect(forkedWhitespace.id).not.toBe(created.id)

        expect(
          yield* requestJson<boolean>(pathFor(SessionPaths.abort, { sessionID: created.id }), {
            method: "POST",
            headers,
          }),
        ).toBe(true)

        expect(
          yield* requestJson<boolean>(pathFor(SessionPaths.remove, { sessionID: created.id }), {
            method: "DELETE",
            headers,
          }),
        ).toBe(true)
      }),
    { git: true, config: { formatter: false, lsp: false, share: "disabled" } },
  )

  it.instance(
    "persists selected workspace id when creating a session",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const project = yield* Project.use.fromDirectory(test.directory)
        const workspace = yield* createLocalWorkspace({
          projectID: project.project.id,
          type: "session-create-workspace",
          directory: path.join(test.directory, ".workspace-local"),
        })

        const created = yield* requestJson<Session.Info>(`${SessionPaths.create}?workspace=${workspace.id}`, {
          method: "POST",
          headers: { "x-opencode-directory": test.directory, "content-type": "application/json" },
          body: JSON.stringify({ title: "workspace session" }),
        })
        const messages = yield* request(
          `${pathFor(SessionPaths.messages, { sessionID: created.id })}?workspace=${workspace.id}`,
          {
            headers: { "x-opencode-directory": test.directory },
          },
        )

        expect(created).toMatchObject({ id: created.id, workspaceID: workspace.id })
        expect(messages.status).toBe(200)
        expect(yield* getWorkspaceID(created.id)).toEqual({ workspaceID: workspace.id })
      }),
    { git: true, config: { formatter: false, lsp: false, share: "disabled" } },
  )

  it.instance(
    "validates archived timestamp values",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory, "content-type": "application/json" }
        const session = yield* createSession({ title: "archived" })
        const body = JSON.stringify({ time: { archived: -1 } })

        const response = yield* request(pathFor(SessionPaths.update, { sessionID: session.id }), {
          method: "PATCH",
          headers,
          body,
        })
        expect(response.status).toBe(200)
        expect((yield* json<Session.Info>(response)).time.archived).toBe(-1)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "uses project-scoped path and directory precedence",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const currentDir = path.join(test.directory, "packages", "opencode", "src")
        yield* Effect.promise(() => mkdir(currentDir, { recursive: true }))

        const store = yield* InstanceStore.Service
        const { pathSession, pathlessSession } = yield* store.provide(
          { directory: currentDir },
          Effect.gen(function* () {
            return {
              pathSession: yield* createSession(),
              pathlessSession: yield* createSession(),
            }
          }).pipe(Effect.provideService(TestInstance, { directory: currentDir })),
        )
        yield* clearSessionPath(pathlessSession.id)

        const query = new URLSearchParams({
          scope: "project",
          path: "packages/opencode/src",
          directory: currentDir,
        })
        const headers = { "x-opencode-directory": test.directory }
        const sessions = (yield* json<Session.Info[]>(
          yield* request(`${SessionPaths.list}?${query}`, { headers }),
        )).map((item) => item.id)

        expect(sessions).toContain(pathSession.id)
        expect(sessions).not.toContain(pathlessSession.id)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "lists sessions created through an equivalent directory hint",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const hint = test.directory + path.sep
        const headers = { "x-opencode-directory": hint, "content-type": "application/json" }
        const created = yield* requestJson<Session.Info>(SessionPaths.create, {
          method: "POST",
          headers,
          body: JSON.stringify({ title: "hinted" }),
        })

        const query = new URLSearchParams({ directory: hint, roots: "true" })
        const listed = yield* requestJson<Session.Info[]>(`${SessionPaths.list}?${query}`, { headers })
        expect(listed.map((item) => item.id)).toContain(created.id)

        const globalQuery = new URLSearchParams({ directory: hint })
        const global = yield* requestJson<Session.Info[]>(`${ExperimentalPaths.session}?${globalQuery}`, { headers })
        expect(global.map((item) => item.id)).toContain(created.id)
      }),
    { git: true, config: { formatter: false, lsp: false, share: "disabled" } },
  )

  it.instance(
    "lists Windows sessions for equivalent directory spellings",
    () =>
      Effect.gen(function* () {
        if (process.platform !== "win32") return
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory, "content-type": "application/json" }
        const created = yield* requestJson<Session.Info>(SessionPaths.create, {
          method: "POST",
          headers,
          body: JSON.stringify({ title: "windows spelling" }),
        })

        const forwardSlashes = test.directory.replaceAll("\\", "/")
        const lowercaseDrive = test.directory.replace(/^[A-Z]:/, (drive) => drive.toLowerCase())
        const trailingSeparator = `${test.directory}\\`
        for (const spelling of [forwardSlashes, lowercaseDrive, trailingSeparator]) {
          const query = new URLSearchParams({ directory: spelling, roots: "true" })
          const listed = yield* requestJson<Session.Info[]>(`${SessionPaths.list}?${query}`, { headers })
          expect({ spelling, ids: listed.map((item) => item.id) }).toEqual({ spelling, ids: [created.id] })
        }
      }),
    { git: true, config: { formatter: false, lsp: false, share: "disabled" } },
    { timeout: 15000 },
  )

  it.instance(
    "lists Windows sessions created through the global worktree sentinel",
    () =>
      Effect.gen(function* () {
        if (process.platform !== "win32") return
        const globalWorktreeSentinel = "/"
        const headers = { "x-opencode-directory": globalWorktreeSentinel, "content-type": "application/json" }
        const driveRootSession = yield* requestJson<Session.Info>(SessionPaths.create, {
          method: "POST",
          headers,
          body: JSON.stringify({ title: "created at drive root" }),
        })
        expect(driveRootSession.directory).toMatch(/^[A-Za-z]:\\$/)

        const query = new URLSearchParams({ directory: globalWorktreeSentinel, roots: "true" })
        const listed = yield* requestJson<Session.Info[]>(`${SessionPaths.list}?${query}`, { headers })
        expect(listed.map((item) => item.id)).toContain(driveRootSession.id)
      }),
    { git: true, config: { formatter: false, lsp: false, share: "disabled" } },
    { timeout: 15000 },
  )

  it.instance(
    "serves paginated message link headers",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory }
        const session = yield* createSession({ title: "messages" })
        yield* createTextMessage(session.id, "first")
        yield* createTextMessage(session.id, "second")
        const route = `${pathFor(SessionPaths.messages, { sessionID: session.id })}?limit=1`

        const response = yield* request(route, { headers })

        expect(response.headers["x-next-cursor"]).toBeTruthy()
        expect(response.headers["link"]).toContain("limit=1")
        expect(response.headers["access-control-expose-headers"]?.toLowerCase()).toContain("x-next-cursor")
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "serves message mutation routes",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory, "content-type": "application/json" }
        const session = yield* createSession({ title: "messages" })
        const first = yield* createTextMessage(session.id, "first")
        const second = yield* createTextMessage(session.id, "second")

        const updated = yield* requestJson<SessionV1.Part>(
          pathFor(SessionPaths.updatePart, {
            sessionID: session.id,
            messageID: first.info.id,
            partID: first.part.id,
          }),
          {
            method: "PATCH",
            headers,
            body: JSON.stringify({ ...first.part, text: "updated" }),
          },
        )
        expect(updated).toMatchObject({ id: first.part.id, type: "text", text: "updated" })

        expect(
          yield* requestJson<boolean>(
            pathFor(SessionPaths.deletePart, {
              sessionID: session.id,
              messageID: first.info.id,
              partID: first.part.id,
            }),
            { method: "DELETE", headers },
          ),
        ).toBe(true)

        expect(
          yield* requestJson<boolean>(
            pathFor(SessionPaths.deleteMessage, { sessionID: session.id, messageID: second.info.id }),
            { method: "DELETE", headers },
          ),
        ).toBe(true)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "rejects part updates whose path and body ids disagree",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory, "content-type": "application/json" }
        const session = yield* createSession({ title: "part mismatch" })
        const message = yield* createTextMessage(session.id, "first")
        const response = yield* request(
          pathFor(SessionPaths.updatePart, {
            sessionID: session.id,
            messageID: message.info.id,
            partID: message.part.id,
          }),
          {
            method: "PATCH",
            headers,
            body: JSON.stringify({ ...message.part, id: PartID.ascending() }),
          },
        )

        expect(response.status).toBe(400)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "serves remaining non-LLM session mutation routes",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory, "content-type": "application/json" }
        const session = yield* createSession({ title: "remaining" })

        expect(
          yield* requestJson<Session.Info>(pathFor(SessionPaths.revert, { sessionID: session.id }), {
            method: "POST",
            headers,
            body: JSON.stringify({ messageID: MessageID.ascending() }),
          }),
        ).toMatchObject({ id: session.id })

        expect(
          yield* requestJson<Session.Info>(pathFor(SessionPaths.unrevert, { sessionID: session.id }), {
            method: "POST",
            headers,
          }),
        ).toMatchObject({ id: session.id })

        const permissionID = String(PermissionV1.ID.ascending())
        const permission = yield* request(
          pathFor(SessionPaths.permissions, {
            sessionID: session.id,
            permissionID,
          }),
          {
            method: "POST",
            headers,
            body: JSON.stringify({ response: "once" }),
          },
        )
        expect(permission.status).toBe(404)
        expect(yield* responseJson(permission)).toEqual({
          _tag: "PermissionNotFoundError",
          requestID: permissionID,
          message: `Permission request not found: ${permissionID}`,
        })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  // Prompt queue (swxtchio/swx-opencode#68).

  it.live(
    "serves the V1 prompt queue: list, withdraw, restore and send now",
    () => {
      // On every exit, before the fake LLM server shuts down, abort the session
      // and release its held reply, so a failing assertion ends the drain and
      // the queued command request instead of leaving them retrying.
      const gate = Deferred.makeUnsafe<void>()
      const stop: { abort?: Effect.Effect<unknown, unknown, HttpClient.HttpClient> } = {}
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        yield* llm.text("steered done")
        yield* llm.text("command done")
        yield* llm.text("idle done")
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "queue routes" }).pipe(provideInstanceEffect(directory))
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const params = { sessionID: session.id }
        const model = { providerID: "test", modelID: "test-model" }
        stop.abort = request(pathFor(SessionPaths.abort, params), { method: "POST", headers })
        const post = (path: string, body: unknown) =>
          request(pathFor(path, params), { method: "POST", headers, body: JSON.stringify(body) })
        const listed = () => requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers })
        const item = (method: string, itemID: string, body?: unknown) =>
          request(pathFor(SessionQueuePaths.withdraw, { ...params, itemID }), {
            method,
            headers,
            body: body === undefined ? undefined : JSON.stringify(body),
          })

        expect(
          (yield* post(SessionPaths.promptAsync, { agent: "build", model, parts: [{ type: "text", text: "start" }] }))
            .status,
        ).toBe(204)
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        expect(
          (yield* post(SessionPaths.promptAsync, {
            agent: "build",
            model,
            delivery: "queue",
            parts: [{ type: "text", text: "held prompt" }],
          })).status,
        ).toBe(204)
        yield* pollWithTimeout(
          listed().pipe(Effect.map((items) => (items.length === 1 ? true : undefined))),
          "held prompt never queued",
        )
        const command = yield* post(SessionPaths.command, {
          command: "init",
          arguments: "",
          model: "test/test-model",
          delivery: "queue",
        }).pipe(Effect.forkChild)
        const items = yield* pollWithTimeout(
          listed().pipe(Effect.map((items) => (items.length === 2 ? items : undefined))),
          "command never queued",
        )
        // Admission seqs follow the requests: start (1, already delivered), held (2), command (3).
        expect(items.map((entry) => [entry.delivery, entry.seq, entry.sessionID])).toEqual([
          ["queue", 2, session.id],
          ["queue", 3, session.id],
        ])
        expect(items[0]!.input.parts).toEqual([{ type: "text", text: "held prompt" }])
        const held = items[0]!

        const withdrawn = yield* item("DELETE", held.id)
        expect(withdrawn.status).toBe(200)
        expect(yield* json<SessionQueue.Item>(withdrawn)).toEqual(held)
        expect((yield* listed()).map((entry) => entry.id)).toEqual([items[1]!.id])
        const again = yield* item("DELETE", held.id)
        expect(again.status).toBe(404)
        expect(yield* responseJson(again)).toMatchObject({
          _tag: "QueueItemNotPending",
          sessionID: session.id,
          itemID: held.id,
        })

        const restored = yield* post(SessionQueuePaths.restore, { id: held.id })
        expect(restored.status).toBe(200)
        expect(yield* json<SessionQueue.Item>(restored)).toEqual(held)
        const restoredAgain = yield* post(SessionQueuePaths.restore, { id: held.id })
        expect(restoredAgain.status).toBe(404)
        expect(yield* responseJson(restoredAgain)).toMatchObject({
          _tag: "QueueItemNotWithdrawn",
          sessionID: session.id,
          itemID: held.id,
        })

        const steered = yield* item("PATCH", held.id, { delivery: "steer" })
        expect(steered.status).toBe(200)
        expect(yield* json<SessionQueue.Item>(steered)).toEqual({ ...held, delivery: "steer" })

        yield* Deferred.succeed(gate, void 0)
        const finished = yield* awaitWithTimeout(Fiber.join(command), "queued command never finished", "10 seconds")
        expect(finished.status).toBe(200)
        const inputs = yield* llm.inputs
        expect(inputs).toHaveLength(3)
        const lastUser = (input: Record<string, unknown> | undefined) =>
          Array.isArray(input?.messages) ? withoutQueueNote(input.messages.at(-1)) : undefined
        const userText = (input: Record<string, unknown> | undefined) =>
          JSON.stringify(
            Array.isArray(input?.messages) ? input.messages.filter((message) => message?.role === "user") : [],
          )
        // The init command's template, which the queued command turn delivers.
        const initTemplate = "Create or update `AGENTS.md` for this repository."
        // Sent now, the held prompt steers the next step, ahead of the queued command.
        expect(lastUser(inputs[1])).toEqual({ role: "user", content: "held prompt" })
        expect(userText(inputs[1])).not.toContain(initTemplate)
        expect(JSON.stringify(lastUser(inputs[2]))).toContain(initTemplate)
        expect(yield* listed()).toEqual([])
        expect((yield* item("DELETE", held.id)).status).toBe(404)

        // A queued prompt on an idle session runs at once through the sync route.
        const idle = yield* post(SessionPaths.prompt, {
          agent: "build",
          model,
          delivery: "queue",
          parts: [{ type: "text", text: "queued while idle" }],
        })
        expect(idle.status).toBe(200)
        expect(yield* responseJson(idle)).toMatchObject({ info: { role: "assistant" } })
        expect(lastUser((yield* llm.inputs)[3])).toEqual({ role: "user", content: "queued while idle" })
      }).pipe(
        Effect.ensuring(
          Effect.suspend(() => stop.abort ?? Effect.void).pipe(
            Effect.ignore,
            Effect.andThen(Deferred.succeed(gate, void 0)),
          ),
        ),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )

  it.live(
    "a queued json_schema prompt is listed and promoted from its stored row into a structured turn, and a text-format steer is promoted from its row",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        yield* llm.push(reply().tool("StructuredOutput", { answer: "42" }))
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "queue format" }).pipe(provideInstanceEffect(directory))
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const params = { sessionID: session.id }
        const model = { providerID: "test", modelID: "test-model" }
        const post = (path: string, body: unknown) =>
          request(pathFor(path, params), { method: "POST", headers, body: JSON.stringify(body) })
        const schema = { type: "object", properties: { answer: { type: "string" } } }

        expect(
          (yield* post(SessionPaths.promptAsync, { agent: "build", model, parts: [{ type: "text", text: "start" }] }))
            .status,
        ).toBe(204)
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        expect(
          (yield* post(SessionPaths.promptAsync, {
            agent: "build",
            model,
            delivery: "queue",
            format: { type: "json_schema", schema },
            parts: [{ type: "text", text: "held with a format" }],
          })).status,
        ).toBe(204)
        const [held] = yield* pollWithTimeout(
          requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers }).pipe(
            Effect.map((items) => (items.length === 1 ? items : undefined)),
          ),
          "formatted prompt never queued",
        )
        // The listed input carries the format as the route decoded it, default included.
        expect(held!.input.format).toEqual({ type: "json_schema", schema, retryCount: 2 })
        yield* Deferred.succeed(gate, void 0)
        yield* pollWithTimeout(
          requestJson<Record<string, { type: string }>>(SessionPaths.status, { headers }).pipe(
            Effect.map((status) => (status[session.id] === undefined ? true : undefined)),
          ),
          "session never went idle",
        )
        // Promoted from its row at the would-idle point, it ran as a structured turn.
        const afterHeld = yield* Session.use
          .messages({ sessionID: session.id })
          .pipe(provideInstanceEffect(directory), Effect.orDie)
        const heldMessage = afterHeld.find((message) =>
          message.parts.some((part) => part.type === "text" && part.text === "held with a format"),
        )?.info
        expect(heldMessage?.role === "user" ? heldMessage.format : undefined).toMatchObject({
          type: "json_schema",
          schema,
          retryCount: 2,
        })
        const structured = afterHeld.find(
          (message) => message.info.role === "assistant" && message.info.parentID === heldMessage?.id,
        )?.info
        expect(structured?.role === "assistant" ? structured.structured : undefined).toEqual({ answer: "42" })
        expect(yield* llm.calls).toBe(2)

        // On an idle session the steer becomes a message at once, through the stored row.
        yield* llm.text("text done")
        const textReply = yield* post(SessionPaths.prompt, {
          agent: "build",
          model,
          format: { type: "text" },
          parts: [{ type: "text", text: "plain text please" }],
        })
        expect(textReply.status).toBe(200)
        expect(yield* responseJson(textReply)).toMatchObject({ info: { role: "assistant" } })
        const messages = yield* Session.use
          .messages({ sessionID: session.id })
          .pipe(provideInstanceEffect(directory), Effect.orDie)
        const stored = messages.find((message) =>
          message.parts.some((part) => part.type === "text" && part.text === "plain text please"),
        )?.info
        expect(stored?.role === "user" ? stored.format?.type : undefined).toBe("text")
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, void 0)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )

  it.live(
    "a sync prompt or command whose queued prompt is withdrawn answers 409 PromptWithdrawn",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "queue withdrawn caller" }).pipe(provideInstanceEffect(directory))
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const params = { sessionID: session.id }
        const model = { providerID: "test", modelID: "test-model" }
        const post = (path: string, body: unknown) =>
          request(pathFor(path, params), { method: "POST", headers, body: JSON.stringify(body) })

        yield* post(SessionPaths.promptAsync, { agent: "build", model, parts: [{ type: "text", text: "start" }] })
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        const prompt = yield* post(SessionPaths.prompt, {
          agent: "build",
          model,
          delivery: "queue",
          parts: [{ type: "text", text: "withdrawn while waiting" }],
        }).pipe(Effect.forkChild)
        const command = yield* post(SessionPaths.command, {
          command: "init",
          arguments: "",
          model: "test/test-model",
          delivery: "queue",
        }).pipe(Effect.forkChild)
        const items = yield* pollWithTimeout(
          requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers }).pipe(
            Effect.map((items) => (items.length === 2 ? items : undefined)),
          ),
          "prompt and command never queued",
        )
        for (const item of items)
          expect(
            (yield* request(pathFor(SessionQueuePaths.withdraw, { ...params, itemID: item.id }), {
              method: "DELETE",
              headers,
            })).status,
          ).toBe(200)
        yield* Deferred.succeed(gate, void 0)

        const promptItem = items.find((item) =>
          item.input.parts.some((part) => part.type === "text" && part.text === "withdrawn while waiting"),
        )!
        const commandItem = items.find((item) => item.id !== promptItem.id)!
        for (const [caller, item] of [
          [prompt, promptItem],
          [command, commandItem],
        ] as const) {
          const response = yield* awaitWithTimeout(Fiber.join(caller), "withdrawn caller never answered", "10 seconds")
          expect(response.status).toBe(409)
          expect(yield* responseJson(response)).toMatchObject({
            _tag: "PromptWithdrawn",
            sessionID: session.id,
            itemID: item.id,
          })
        }
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, void 0)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )

  it.live(
    "a recalled prompt resent with edits is a new request, and only the edits reach the model",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        yield* llm.text("lint done")
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "queue recall" }).pipe(provideInstanceEffect(directory))
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const params = { sessionID: session.id }
        const model = { providerID: "test", modelID: "test-model" }
        const post = (path: string, body: unknown) =>
          request(pathFor(path, params), { method: "POST", headers, body: JSON.stringify(body) })
        // As the TUI sends: a steer naming its own request.
        const send = (requestID: string, text: string) =>
          post(SessionPaths.prompt, { requestID, agent: "build", model, parts: [{ type: "text", text }] })
        const withdraw = (itemID: string) =>
          request(pathFor(SessionQueuePaths.withdraw, { ...params, itemID }), { method: "DELETE", headers })

        yield* post(SessionPaths.promptAsync, { agent: "build", model, parts: [{ type: "text", text: "start" }] })
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        // Held, the steer stays pending as one a busy session cannot take yet.
        expect((yield* post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
        const first = yield* send("recall-first", "check the build").pipe(Effect.forkChild)
        const [pending] = yield* pollWithTimeout(
          requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers }).pipe(
            Effect.map((items) => (items.length === 1 ? items : undefined)),
          ),
          "the sent prompt was never listed",
        )
        expect(pending!.requestID).toBe("recall-first")

        // UP takes it back: the withdrawn item carries the prompt as sent.
        const recalled = yield* withdraw(pending!.id)
        expect(recalled.status).toBe(200)
        expect((yield* json<SessionQueue.Item>(recalled)).input.parts).toEqual([
          { type: "text", text: "check the build" },
        ])
        // Its request says it was withdrawn, so a sender reading its receipt is not left waiting.
        const receiptOf = (requestID: string) =>
          requestJson<SessionQueue.Receipt>(pathFor(SessionQueuePaths.receipt, { ...params, requestID }), { headers })
        expect(yield* receiptOf("recall-first")).toEqual({ state: "withdrawn", itemID: pending!.id })
        // A second recall from a stale list gets nothing.
        const stale = yield* withdraw(pending!.id)
        expect(stale.status).toBe(404)
        expect(yield* responseJson(stale)).toMatchObject({ _tag: "QueueItemNotPending", itemID: pending!.id })

        // Sent again unchanged, the withdrawn request is told so and admits nothing.
        const retried = yield* awaitWithTimeout(
          send("recall-first", "check the build"),
          "the retry of a withdrawn request never answered",
          "10 seconds",
        )
        expect(retried.status).toBe(409)
        expect(yield* responseJson(retried)).toMatchObject({ _tag: "PromptWithdrawn" })
        expect(yield* requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers })).toEqual(
          [],
        )

        const resent = yield* send("recall-resend", "check the lint").pipe(Effect.forkChild)
        yield* pollWithTimeout(
          requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers }).pipe(
            Effect.map((items) => (items.some((item) => item.requestID === "recall-resend") ? true : undefined)),
          ),
          "the resend was never listed",
        )
        expect((yield* post(SessionQueuePaths.release, { holder: "view" })).status).toBe(204)
        yield* Deferred.succeed(gate, undefined)
        // The first send's caller learns at the run's next boundary that it was withdrawn.
        const answered = yield* awaitWithTimeout(Fiber.join(first), "withdrawn send never answered", "10 seconds")
        expect(answered.status).toBe(409)
        expect(yield* responseJson(answered)).toMatchObject({ _tag: "PromptWithdrawn", itemID: pending!.id })
        expect((yield* awaitWithTimeout(Fiber.join(resent), "resend never answered", "10 seconds")).status).toBe(200)

        expect((yield* receiptOf("recall-resend")).state).toBe("delivered")
        // The edited resend is a new request; the original stays withdrawn.
        expect(yield* receiptOf("recall-first")).toEqual({ state: "withdrawn", itemID: pending!.id })
        const sent = JSON.stringify(yield* llm.inputs)
        expect(sent).toContain("check the lint")
        expect(sent).not.toContain("check the build")
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )

  it.live(
    "a withdrawn prompt_async prompt ends without a session error",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        yield* llm.text("marker done")
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "queue async withdraw" }).pipe(provideInstanceEffect(directory))
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const params = { sessionID: session.id }
        const model = { providerID: "test", modelID: "test-model" }
        const post = (path: string, body: unknown) =>
          request(pathFor(path, params), { method: "POST", headers, body: JSON.stringify(body) })

        const events = yield* request(`${EventPaths.event}?directory=${encodeURIComponent(directory)}`)
        const seen: { type: string; sessionID?: string }[] = []
        const idles = yield* Queue.unbounded<void>()
        let buffered = ""
        yield* events.stream.pipe(
          Stream.decodeText,
          Stream.runForEach((chunk) =>
            Effect.gen(function* () {
              buffered += chunk
              while (buffered.includes("\n\n")) {
                const end = buffered.indexOf("\n\n")
                const data = buffered
                  .slice(0, end)
                  .split("\n")
                  .filter((line) => line.startsWith("data: "))
                  .map((line) => line.slice("data: ".length))
                  .join("\n")
                buffered = buffered.slice(end + 2)
                if (!data) continue
                const event = JSON.parse(data) as { type: string; properties?: { sessionID?: string } }
                if (event.properties?.sessionID !== session.id) continue
                seen.push({ type: event.type, sessionID: event.properties.sessionID })
                if (event.type === "session.idle") yield* Queue.offer(idles, undefined)
              }
            }),
          ),
          Effect.forkScoped,
        )
        const idle = awaitWithTimeout(Queue.take(idles), "session never went idle", "10 seconds")

        yield* post(SessionPaths.promptAsync, { agent: "build", model, parts: [{ type: "text", text: "start" }] })
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        expect(
          (yield* post(SessionPaths.promptAsync, {
            agent: "build",
            model,
            delivery: "queue",
            parts: [{ type: "text", text: "withdrawn async" }],
          })).status,
        ).toBe(204)
        const [held] = yield* pollWithTimeout(
          requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers }).pipe(
            Effect.map((items) => (items.length === 1 ? items : undefined)),
          ),
          "prompt never queued",
        )
        expect(
          (yield* request(pathFor(SessionQueuePaths.withdraw, { ...params, itemID: held!.id }), {
            method: "DELETE",
            headers,
          })).status,
        ).toBe(200)
        yield* Deferred.succeed(gate, void 0)
        yield* idle
        // A later run on the session bounds the window in which the withdrawn
        // prompt's caller could still report an error.
        yield* post(SessionPaths.promptAsync, { agent: "build", model, parts: [{ type: "text", text: "marker" }] })
        yield* idle
        expect(yield* llm.calls).toBe(2)
        expect(seen.filter((event) => event.type === "session.error")).toEqual([])
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, void 0)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )

  it.live(
    "restore and send now wake an idle session whose prompts an abort parked",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hang
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "queue wake" }).pipe(provideInstanceEffect(directory))
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const params = { sessionID: session.id }
        const model = { providerID: "test", modelID: "test-model" }
        const post = (path: string, body?: unknown) =>
          request(pathFor(path, params), {
            method: "POST",
            headers,
            body: body === undefined ? undefined : JSON.stringify(body),
          })
        const listed = () => requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers })
        const queued = (text: string) =>
          post(SessionPaths.promptAsync, { agent: "build", model, delivery: "queue", parts: [{ type: "text", text }] })
        const idle = () =>
          pollWithTimeout(
            requestJson<Record<string, { type: string }>>(SessionPaths.status, { headers }).pipe(
              Effect.map((status) => (status[session.id] === undefined ? true : undefined)),
            ),
            "session never went idle",
          )
        const answered = (text: string) =>
          pollWithTimeout(
            llm.inputs.pipe(
              Effect.map((inputs) =>
                inputs.some((input) => {
                  const messages = Array.isArray(input.messages) ? input.messages : []
                  return (
                    JSON.stringify(withoutQueueNote(messages.at(-1))) ===
                    JSON.stringify({ role: "user", content: text })
                  )
                })
                  ? true
                  : undefined,
              ),
            ),
            `"${text}" never reached the model`,
            "10 seconds",
          )

        yield* post(SessionPaths.promptAsync, { agent: "build", model, parts: [{ type: "text", text: "start" }] })
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        yield* queued("restored after abort")
        yield* queued("parked after abort")
        const [restoredItem] = yield* pollWithTimeout(
          listed().pipe(Effect.map((items) => (items.length === 2 ? items : undefined))),
          "prompts never queued",
        )
        expect(
          (yield* request(pathFor(SessionQueuePaths.withdraw, { ...params, itemID: restoredItem!.id }), {
            method: "DELETE",
            headers,
          })).status,
        ).toBe(200)
        yield* post(SessionPaths.abort)
        yield* idle()
        expect(yield* llm.calls).toBe(1)

        yield* llm.text("restored done")
        yield* llm.text("parked done")
        expect((yield* post(SessionQueuePaths.restore, { id: restoredItem!.id })).status).toBe(200)
        yield* answered("restored after abort")
        yield* answered("parked after abort")
        // The wake delivers the parked prompts in admission order and never retries the aborted turn.
        const lastUsers = (yield* llm.inputs).map((input) =>
          Array.isArray(input.messages) ? withoutQueueNote(input.messages.at(-1)) : undefined,
        )
        expect(lastUsers).toEqual([
          { role: "user", content: "start" },
          { role: "user", content: "restored after abort" },
          { role: "user", content: "parked after abort" },
        ])
        yield* idle()
        expect(yield* listed()).toEqual([])

        yield* llm.hang
        yield* post(SessionPaths.promptAsync, { agent: "build", model, parts: [{ type: "text", text: "again" }] })
        yield* awaitWithTimeout(llm.wait(4), "fourth provider call never started", "10 seconds")
        yield* queued("steered after abort")
        const [parked] = yield* pollWithTimeout(
          listed().pipe(Effect.map((items) => (items.length === 1 ? items : undefined))),
          "prompt never queued",
        )
        yield* post(SessionPaths.abort)
        yield* idle()
        yield* llm.text("steered done")
        const steered = yield* request(pathFor(SessionQueuePaths.update, { ...params, itemID: parked!.id }), {
          method: "PATCH",
          headers,
          body: JSON.stringify({ delivery: "steer" }),
        })
        expect(steered.status).toBe(200)
        yield* answered("steered after abort")
      }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
    30_000,
  )

  // Review view (swxtchio/swx-opencode#212).
  const reviewRoutes = (directory: string, sessionID: SessionIDType, llm: TestLLMServer["Service"]) => {
    const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
    const params = { sessionID }
    const post = (path: string, body?: unknown) =>
      request(pathFor(path, params), {
        method: "POST",
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
      })
    // Without the note a prompt that waited is delivered with, which the
    // queue freshness tests check.
    const lastUsers = llm.inputs.pipe(
      Effect.map((inputs) =>
        inputs.map((input) => (Array.isArray(input.messages) ? withoutQueueNote(input.messages.at(-1)) : undefined)),
      ),
    )
    return {
      headers,
      post,
      // The synchronous route, answered with the turn's final message.
      sync: (text: string, delivery?: SessionQueue.Delivery) =>
        post(SessionPaths.prompt, {
          agent: "build",
          model: { providerID: "test", modelID: "test-model" },
          ...(delivery ? { delivery } : {}),
          parts: [{ type: "text", text }],
        }),
      replyText: (response: HttpClientResponse.HttpClientResponse) =>
        json<SessionV1.WithParts>(response).pipe(
          Effect.map((message) => message.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")),
        ),
      prompt: (text: string, delivery?: SessionQueue.Delivery) =>
        post(SessionPaths.promptAsync, {
          agent: "build",
          model: { providerID: "test", modelID: "test-model" },
          ...(delivery ? { delivery } : {}),
          parts: [{ type: "text", text }],
        }),
      listed: () => requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers }),
      texts: (items: readonly SessionQueue.Item[]) =>
        items.map((item) => item.input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")),
      idle: () =>
        pollWithTimeout(
          requestJson<Record<string, { type: string }>>(SessionPaths.status, { headers }).pipe(
            Effect.map((status) => (status[sessionID] === undefined ? true : undefined)),
          ),
          "session never went idle",
        ),
      lastUsers,
      delivered: (count: number) =>
        pollWithTimeout(
          lastUsers.pipe(Effect.map((users) => (users.length >= count ? users : undefined))),
          `the model never received ${count} turns`,
          "10 seconds",
        ),
    }
  }

  const withoutQueueNote = (message: unknown) => {
    if (!message || typeof message !== "object" || !("content" in message) || !Array.isArray(message.content))
      return message
    const kept = message.content.filter(
      (part: { type?: string; text?: string }) => !(part.type === "text" && part.text?.startsWith("[Queued prompt")),
    )
    return { ...message, content: kept.length === 1 && kept[0].type === "text" ? kept[0].text : kept }
  }

  it.live(
    "a review hold keeps admitted steers and queued prompts pending until its release delivers them",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "review hold" }).pipe(provideInstanceEffect(directory))
        const api = reviewRoutes(directory, session.id, llm)

        yield* api.prompt("start")
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        const opened = yield* api.post(SessionQueuePaths.hold, { holder: "view" })
        expect(opened.status).toBe(200)
        expect(yield* json<SessionQueue.Item[]>(opened)).toEqual([])

        // Unheld, the steer would be promoted at admission into the running turn's history.
        yield* api.prompt("steer while held")
        yield* api.prompt("queued while held", "queue")
        const pending = yield* pollWithTimeout(
          api.listed().pipe(Effect.map((items) => (items.length === 2 ? items : undefined))),
          "prompts never queued",
        )
        expect(pending.map((item) => item.delivery)).toEqual(["steer", "queue"])

        // The turn ends where it would promote both, and goes idle with both pending.
        yield* Deferred.succeed(gate, undefined)
        yield* api.idle()
        expect(yield* llm.calls).toBe(1)
        expect(api.texts(yield* api.listed())).toEqual(["steer while held", "queued while held"])
        const messages = yield* requestJson<SessionV1.WithParts[]>(
          pathFor(SessionPaths.messages, { sessionID: session.id }),
          {
            headers: api.headers,
          },
        )
        expect(JSON.stringify(messages)).not.toContain("while held")

        // A sync prompt on a new session waits out the hold rather than failing.
        const fresh = yield* createSession({ title: "review hold, new session" }).pipe(provideInstanceEffect(directory))
        const freshApi = reviewRoutes(directory, fresh.id, llm)
        expect((yield* freshApi.post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
        const first = yield* freshApi
          .post(SessionPaths.prompt, {
            agent: "build",
            model: { providerID: "test", modelID: "test-model" },
            parts: [{ type: "text", text: "first prompt while held" }],
          })
          .pipe(Effect.forkChild)
        yield* pollWithTimeout(
          freshApi.listed().pipe(Effect.map((items) => (items.length === 1 ? true : undefined))),
          "first prompt never queued",
        )

        yield* llm.text("steer done")
        yield* llm.text("queue done")
        const released = yield* api.post(SessionQueuePaths.release, { holder: "view" })
        expect(released.status).toBe(204)
        expect(yield* api.delivered(3)).toEqual([
          { role: "user", content: "start" },
          { role: "user", content: "steer while held" },
          { role: "user", content: "queued while held" },
        ])
        yield* api.idle()
        expect(yield* api.listed()).toEqual([])

        yield* llm.text("first done")
        expect((yield* freshApi.post(SessionQueuePaths.release, { holder: "view" })).status).toBe(204)
        const answered = yield* awaitWithTimeout(Fiber.join(first), "held first prompt never answered", "10 seconds")
        expect(answered.status).toBe(200)
        expect(yield* responseJson(answered)).toMatchObject({ info: { role: "assistant" } })
        expect((yield* api.lastUsers).at(-1)).toEqual({ role: "user", content: "first prompt while held" })
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )

  // swxtchio/swx-opencode#215: the turn before the hold is not a held prompt's answer.
  it.live(
    "a sync prompt held on an existing session waits for its own turn's reply",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "review hold, existing session" }).pipe(
          provideInstanceEffect(directory),
        )
        const api = reviewRoutes(directory, session.id, llm)

        yield* llm.text("prior done")
        const prior = yield* api.sync("before the hold")
        expect(prior.status).toBe(200)
        expect(yield* api.replyText(prior)).toBe("prior done")

        for (const [text, delivery, done] of [
          ["steer while held", undefined, "steer done"],
          ["queued while held", "queue", "queue done"],
        ] as const) {
          expect((yield* api.post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
          const calls = yield* llm.calls
          const held = yield* api.sync(text, delivery).pipe(Effect.forkChild)
          yield* pollWithTimeout(
            api.listed().pipe(Effect.map((items) => (api.texts(items).includes(text) ? true : undefined))),
            `${text} never queued`,
          )
          // Unfixed, the caller's joined run ended at once and answered it with "prior done".
          expect(Option.isNone(yield* Fiber.await(held).pipe(Effect.timeoutOption("1 second")))).toBe(true)
          expect(api.texts(yield* api.listed())).toEqual([text])
          expect(yield* llm.calls).toBe(calls)
          const messages = yield* requestJson<SessionV1.WithParts[]>(
            pathFor(SessionPaths.messages, { sessionID: session.id }),
            { headers: api.headers },
          )
          expect(JSON.stringify(messages)).not.toContain(text)

          yield* llm.text(done)
          expect((yield* api.post(SessionQueuePaths.release, { holder: "view" })).status).toBe(204)
          const answered = yield* awaitWithTimeout(Fiber.join(held), `${text} never answered`, "10 seconds")
          expect(answered.status).toBe(200)
          expect(yield* api.replyText(answered)).toBe(done)
          expect((yield* api.lastUsers).at(-1)).toEqual({ role: "user", content: text })
          yield* api.idle()
          expect(yield* api.listed()).toEqual([])
        }
        expect(yield* llm.calls).toBe(3)
      }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
    30_000,
  )

  // A lapse ends the hold through the caller's own wait and the hold's lapse
  // watch, not the release route (#215).
  itClocked.live(
    "a sync prompt held on an existing session gets its own turn's reply when the hold's lease lapses",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "review hold lapses" }).pipe(provideInstanceEffect(directory))
        const api = reviewRoutes(directory, session.id, llm)
        yield* llm.text("prior done")
        expect(yield* api.replyText(yield* api.sync("before the hold"))).toBe("prior done")

        for (const [text, delivery, done] of [
          ["steer until the lapse", undefined, "steer done"],
          ["queued until the lapse", "queue", "queue done"],
        ] as const) {
          expect((yield* api.post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
          const calls = yield* llm.calls
          const held = yield* api.sync(text, delivery).pipe(Effect.forkChild)
          yield* TestClock.withLive(
            pollWithTimeout(
              api.listed().pipe(Effect.map((items) => (api.texts(items).includes(text) ? true : undefined))),
              `${text} never queued`,
            ),
          )
          yield* TestClock.adjust("5 minutes")
          expect(
            Option.isNone(yield* TestClock.withLive(Fiber.await(held).pipe(Effect.timeoutOption("1 second")))),
          ).toBe(true)
          expect(api.texts(yield* api.listed())).toEqual([text])
          expect(yield* llm.calls).toBe(calls)
          const messages = yield* requestJson<SessionV1.WithParts[]>(
            pathFor(SessionPaths.messages, { sessionID: session.id }),
            { headers: api.headers },
          )
          expect(JSON.stringify(messages)).not.toContain(text)

          yield* llm.text(done)
          yield* TestClock.adjust("1 minute")
          const answered = yield* TestClock.withLive(
            awaitWithTimeout(Fiber.join(held), `${text} never answered after the lapse`, "10 seconds"),
          )
          expect(answered.status).toBe(200)
          expect(yield* api.replyText(answered)).toBe(done)
          expect((yield* api.lastUsers).at(-1)).toEqual({ role: "user", content: text })
          yield* TestClock.withLive(api.idle())
          expect(yield* api.listed()).toEqual([])
        }
        expect(yield* llm.calls).toBe(3)
      }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
    30_000,
  )

  // The caller saw the hold live and the session idle after its run, then the
  // release ended the hold before the caller decided what its prompt got (#215).
  itRacing.live(
    "a held sync prompt whose hold ends during its caller's checks still gets its own turn's reply",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "review hold ends mid-check" }).pipe(
          provideInstanceEffect(directory),
        )
        const api = reviewRoutes(directory, session.id, llm)
        yield* llm.text("prior done")
        expect(yield* api.replyText(yield* api.sync("before the hold"))).toBe("prior done")

        // The release's own drain is held before its promotion commits, then
        // after it commits while that turn is still with the provider.
        for (const [text, delivery, done, committed] of [
          ["steered before commit", undefined, "steer done", false],
          ["queued after commit", "queue", "queue done", true],
        ] as const) {
          expect((yield* api.post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
          const checked = yield* racingSpans.arm({ name: "SessionQueue.awaitingDrain", parent: "SessionPrompt.loop" })
          const held = yield* api.sync(text, delivery).pipe(Effect.forkChild)
          const gates: Effect.Effect<void>[] = [checked.disarm]
          yield* Effect.gen(function* () {
            yield* awaitWithTimeout(checked.reached, `${text}: caller never checked for pending work`, "10 seconds")
            const idle = yield* racingSpans.arm({ name: "SessionRunState.assertNotBusy", parent: "SessionPrompt.loop" })
            gates.push(idle.disarm)
            yield* checked.release
            yield* awaitWithTimeout(idle.reached, `${text}: caller never checked for a run`, "10 seconds")

            const calls = yield* llm.calls
            const promoting = committed
              ? undefined
              : yield* racingSpans.arm({ name: "SessionPrompt.prepareUserMessage", parent: "SessionQueue.promote" })
            if (promoting) gates.push(promoting.disarm)
            yield* committed ? llm.hold(done, Effect.runPromise(Deferred.await(gate))) : llm.text(done)
            expect((yield* api.post(SessionQueuePaths.release, { holder: "view" })).status).toBe(204)
            if (promoting) {
              yield* awaitWithTimeout(promoting.reached, `${text}: release never promoted`, "10 seconds")
              expect(api.texts(yield* api.listed())).toEqual([text])
            }
            if (!promoting) {
              yield* awaitWithTimeout(llm.wait(calls + 1), `${text}: delivered turn never started`, "10 seconds")
              expect(yield* api.listed()).toEqual([])
            }

            yield* idle.release
            // Unfixed, the caller answered here with "prior done".
            expect(Option.isNone(yield* Fiber.await(held).pipe(Effect.timeoutOption("1 second")))).toBe(true)
            yield* promoting ? promoting.release : Deferred.succeed(gate, undefined)
            const answered = yield* awaitWithTimeout(Fiber.join(held), `${text} never answered`, "10 seconds")
            expect(answered.status).toBe(200)
            expect(yield* api.replyText(answered)).toBe(done)
            expect((yield* api.lastUsers).at(-1)).toEqual({ role: "user", content: text })
            yield* api.idle()
          }).pipe(Effect.ensuring(Effect.all(gates).pipe(Effect.andThen(Fiber.interrupt(held)))))
        }
        expect(yield* llm.calls).toBe(3)
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )

  it.live(
    "deleted, reordered and consolidated pending prompts are delivered as the review left them",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "review edits" }).pipe(provideInstanceEffect(directory))
        const api = reviewRoutes(directory, session.id, llm)

        yield* api.prompt("start")
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        expect((yield* api.post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
        const admitted = [
          "drop this one",
          "run lint now",
          "fix auth.ts: handle a null token",
          "run lint now",
          "fix auth.ts: add a regression test",
          "write the changelog",
        ]
        yield* Effect.forEach(admitted, (text) => api.prompt(text, "queue"), { discard: true })
        // The second "run lint now" is taken into the first as it is admitted.
        const pending = yield* pollWithTimeout(
          api.listed().pipe(Effect.map((items) => (items.length === admitted.length - 1 ? items : undefined))),
          "prompts never queued",
        )
        expect(api.texts(pending)).toEqual([
          "drop this one",
          "run lint now",
          "fix auth.ts: handle a null token",
          "fix auth.ts: add a regression test",
          "write the changelog",
        ])
        const [doomed] = pending

        const removed = yield* api.post(SessionQueuePaths.remove, { id: doomed!.id })
        expect(removed.status).toBe(200)
        expect(yield* json<SessionQueue.Item>(removed)).toEqual(doomed!)
        const again = yield* api.post(SessionQueuePaths.remove, { id: doomed!.id })
        expect(again.status).toBe(404)
        expect(yield* responseJson(again)).toMatchObject({ _tag: "QueueItemNotPending", itemID: doomed!.id })

        const consolidated = yield* api.post(SessionQueuePaths.consolidate)
        expect(consolidated.status).toBe(200)
        const merged = yield* json<SessionQueue.Item[]>(consolidated)
        expect(api.texts(merged)).toEqual([
          "run lint now",
          "fix auth.ts:\n1. handle a null token\n2. add a regression test",
          "write the changelog",
        ])

        const [lint, fix, changelog] = merged
        const stale = yield* api.post(SessionQueuePaths.reorder, { items: [doomed!.id, lint!.id] })
        expect(stale.status).toBe(404)
        const reordered = yield* api.post(SessionQueuePaths.reorder, { items: [changelog!.id, fix!.id, lint!.id] })
        expect(reordered.status).toBe(200)
        expect(api.texts(yield* json<SessionQueue.Item[]>(reordered))).toEqual([
          "write the changelog",
          "fix auth.ts:\n1. handle a null token\n2. add a regression test",
          "run lint now",
        ])

        yield* Deferred.succeed(gate, undefined)
        yield* api.idle()
        yield* llm.text("changelog done")
        yield* llm.text("fix done")
        yield* llm.text("lint done")
        expect((yield* api.post(SessionQueuePaths.release, { holder: "view" })).status).toBe(204)
        expect(yield* api.delivered(4)).toEqual([
          { role: "user", content: "start" },
          { role: "user", content: "write the changelog" },
          { role: "user", content: "fix auth.ts:\n1. handle a null token\n2. add a regression test" },
          { role: "user", content: "run lint now" },
        ])
        yield* api.idle()
        expect(JSON.stringify(yield* llm.inputs)).not.toContain("drop this one")
        expect(yield* api.listed()).toEqual([])

        // Deleting a new session's first prompt answers its sync caller as withdrawn.
        const fresh = yield* createSession({ title: "review edits, new session" }).pipe(
          provideInstanceEffect(directory),
        )
        const freshApi = reviewRoutes(directory, fresh.id, llm)
        expect((yield* freshApi.post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
        const first = yield* freshApi
          .post(SessionPaths.prompt, {
            agent: "build",
            model: { providerID: "test", modelID: "test-model" },
            parts: [{ type: "text", text: "deleted before it started" }],
          })
          .pipe(Effect.forkChild)
        const [firstItem] = yield* pollWithTimeout(
          freshApi.listed().pipe(Effect.map((items) => (items.length === 1 ? items : undefined))),
          "first prompt never queued",
        )
        expect((yield* freshApi.post(SessionQueuePaths.remove, { id: firstItem!.id })).status).toBe(200)
        expect((yield* freshApi.post(SessionQueuePaths.release, { holder: "view" })).status).toBe(204)
        const withdrawn = yield* awaitWithTimeout(
          Fiber.join(first),
          "deleted first prompt never answered",
          "10 seconds",
        )
        expect(withdrawn.status).toBe(409)
        expect(yield* responseJson(withdrawn)).toMatchObject({ _tag: "PromptWithdrawn", itemID: firstItem!.id })
        expect(yield* llm.calls).toBe(4)
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )

  it.live(
    "publishes session.queue.updated on the event stream with the full pending list",
    () => {
      // Released on every exit before the fake LLM server shuts down, so a
      // failing assertion cannot hold its reply open.
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "queue events" }).pipe(provideInstanceEffect(directory))
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const params = { sessionID: session.id }
        const model = { providerID: "test", modelID: "test-model" }

        const events = yield* request(`${EventPaths.event}?directory=${encodeURIComponent(directory)}`)
        const chunks = yield* Queue.unbounded<string>()
        yield* events.stream.pipe(
          Stream.decodeText,
          Stream.runForEach((chunk) => Queue.offer(chunks, chunk)),
          Effect.forkScoped,
        )
        let buffered = ""
        const nextQueueUpdate = Effect.gen(function* () {
          while (true) {
            const end = buffered.indexOf("\n\n")
            if (end === -1) {
              buffered += yield* Queue.take(chunks)
              continue
            }
            const frame = buffered.slice(0, end)
            buffered = buffered.slice(end + 2)
            const data = frame
              .split("\n")
              .filter((line) => line.startsWith("data: "))
              .map((line) => line.slice("data: ".length))
              .join("\n")
            if (!data) continue
            const event = JSON.parse(data) as {
              type: string
              properties: { sessionID?: string; items?: SessionQueue.Item[] }
            }
            if (event.type === "session.queue.updated" && event.properties.sessionID === session.id)
              return event.properties.items
          }
        }).pipe(
          Effect.timeoutOrElse({
            duration: "5 seconds",
            orElse: () => Effect.fail(new Error("no session.queue.updated event")),
          }),
        )

        const start = yield* request(pathFor(SessionPaths.promptAsync, params), {
          method: "POST",
          headers,
          body: JSON.stringify({ agent: "build", model, parts: [{ type: "text", text: "start" }] }),
        })
        expect(start.status).toBe(204)
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        // The start prompt's own admission, preparation and promotion publish
        // first; skip to the held one.
        yield* nextQueueUpdate
        yield* nextQueueUpdate
        yield* nextQueueUpdate
        yield* request(pathFor(SessionPaths.promptAsync, params), {
          method: "POST",
          headers,
          body: JSON.stringify({ agent: "build", model, delivery: "queue", parts: [{ type: "text", text: "held" }] }),
        })
        // Listed on arrival, and again once its sender has prepared it (#249).
        const admittedList = yield* nextQueueUpdate
        expect(yield* nextQueueUpdate).toEqual(admittedList)
        const listed = yield* requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers })
        expect(admittedList).toEqual(listed)
        expect(listed.map((entry) => entry.input.parts)).toEqual([[{ type: "text", text: "held" }]])

        const withdrawn = yield* request(pathFor(SessionQueuePaths.withdraw, { ...params, itemID: listed[0]!.id }), {
          method: "DELETE",
          headers,
        })
        expect(withdrawn.status).toBe(200)
        expect(yield* nextQueueUpdate).toEqual([])

        const held = listed[0]!
        const restored = yield* request(pathFor(SessionQueuePaths.restore, params), {
          method: "POST",
          headers,
          body: JSON.stringify({ id: held.id }),
        })
        expect(restored.status).toBe(200)
        expect(yield* nextQueueUpdate).toEqual([held])
        const steered = yield* request(pathFor(SessionQueuePaths.update, { ...params, itemID: held.id }), {
          method: "PATCH",
          headers,
          body: JSON.stringify({ delivery: "steer" }),
        })
        expect(steered.status).toBe(200)
        expect(yield* nextQueueUpdate).toEqual([{ ...held, delivery: "steer" }])
        // Promotion at the next step empties the list.
        yield* Deferred.succeed(gate, void 0)
        expect(yield* nextQueueUpdate).toEqual([])
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, void 0)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )
})

describe("session HttpApi pending prompt selection", () => {
  // Two models, each with two efforts, so a test can tell which selection a
  // provider request was made under.
  const selectableConfig = (url: string) => {
    const base = testProviderConfig(url)
    const model = (id: string) => ({
      ...base.provider.test.models["test-model"],
      id,
      name: id,
      variants: { low: { reasoningEffort: "low" }, high: { reasoningEffort: "high" } },
    })
    return {
      ...base,
      provider: {
        test: { ...base.provider.test, models: { "model-a": model("model-a"), "model-b": model("model-b") } },
      },
    }
  }

  it.live(
    "pending prompts run at the session's selection when delivered, not the one they were sent with",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        const directory = yield* tmpdirScoped({ git: true, config: selectableConfig(llm.url) })
        const session = yield* createSession({ title: "current selection" }).pipe(provideInstanceEffect(directory))
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const params = { sessionID: session.id }
        const post = (path: string, body?: unknown) =>
          request(pathFor(path, params), {
            method: "POST",
            headers,
            body: body === undefined ? undefined : JSON.stringify(body),
          })
        const underA = (text: string, delivery?: SessionQueue.Delivery) =>
          post(SessionPaths.promptAsync, {
            agent: "build",
            model: { providerID: "test", modelID: "model-a" },
            variant: "low",
            ...(delivery ? { delivery } : {}),
            parts: [{ type: "text", text }],
          })
        const listed = () => requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers })

        yield* underA("start")
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        expect((yield* post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
        // Held, the steer stays pending as a queued prompt does.
        yield* underA("steer under a")
        yield* underA("queued under a", "queue")
        yield* underA("run lint", "queue")
        yield* underA("run lint --fix", "queue")
        yield* pollWithTimeout(
          listed().pipe(Effect.map((items) => (items.length === 4 ? true : undefined))),
          "prompts never queued",
        )
        expect((yield* post(SessionQueuePaths.consolidate)).status).toBe(200)
        // A row a stopped process left behind, admitted under A.
        const { db } = yield* Database.Service
        yield* db
          .insert(SessionPromptQueueTable)
          .values({
            id: SessionPromptQueue.ItemID.create(),
            session_id: session.id,
            seq: 1000,
            delivery: "queue",
            input: {
              agent: "build",
              model: { providerID: "test", modelID: "model-a" },
              variant: "low",
              parts: [{ type: "text", text: "persisted under a" }],
            },
            time_created: Date.now(),
          })
          .run()

        const selected = yield* request(pathFor(SessionPaths.update, params), {
          method: "PATCH",
          headers,
          body: JSON.stringify({ model: { providerID: "test", id: "model-b", variant: "high" } }),
        })
        expect(selected.status).toBe(200)
        expect((yield* json<{ model?: unknown }>(selected)).model).toEqual({
          providerID: "test",
          id: "model-b",
          variant: "high",
        })

        yield* Deferred.succeed(gate, undefined)
        for (const text of ["steer done", "queued done", "lint done", "persisted done"]) yield* llm.text(text)
        expect((yield* post(SessionQueuePaths.release, { holder: "view" })).status).toBe(204)
        const sent = yield* pollWithTimeout(
          llm.inputs.pipe(Effect.map((inputs) => (inputs.length === 5 ? inputs : undefined))),
          "the model never received every pending prompt",
          "10 seconds",
        )
        const turns = sent.map((input) => {
          const messages = input.messages as { role: string; content: unknown }[]
          return { model: input.model, effort: input.reasoning_effort, last: JSON.stringify(messages.at(-1)) }
        })
        expect(turns.map((turn) => [turn.model, turn.effort])).toEqual([
          ["model-a", "low"],
          ["model-b", "high"],
          ["model-b", "high"],
          ["model-b", "high"],
          ["model-b", "high"],
        ])
        expect(turns[1]!.last).toContain("steer under a")
        expect(turns[2]!.last).toContain("queued under a")
        expect(turns[3]!.last).toContain("--fix")
        expect(turns[4]!.last).toContain("persisted under a")
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )

  it.live(
    "a prompt's own model and effort become the selection its delivery and later prompts run at",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        const directory = yield* tmpdirScoped({ git: true, config: selectableConfig(llm.url) })
        const session = yield* createSession({ title: "selection by prompt" }).pipe(provideInstanceEffect(directory))
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const send = (text: string, selection: object) =>
          request(pathFor(SessionPaths.promptAsync, { sessionID: session.id }), {
            method: "POST",
            headers,
            body: JSON.stringify({ agent: "build", delivery: "queue", ...selection, parts: [{ type: "text", text }] }),
          })

        yield* send("start", { model: { providerID: "test", modelID: "model-a" }, variant: "low" })
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        yield* send("queued under b", { model: { providerID: "test", modelID: "model-b" }, variant: "high" })
        // Names no model, as a firstmate send to an unmanaged session does.
        yield* send("queued with no selection", {})

        yield* llm.text("b done")
        yield* llm.text("unnamed done")
        yield* Deferred.succeed(gate, undefined)
        const sent = yield* pollWithTimeout(
          llm.inputs.pipe(Effect.map((inputs) => (inputs.length === 3 ? inputs : undefined))),
          "the model never received every queued prompt",
          "10 seconds",
        )
        expect(sent.map((input) => [input.model, input.reasoning_effort])).toEqual([
          ["model-a", "low"],
          ["model-b", "high"],
          ["model-b", "high"],
        ])
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )
})

describe("session HttpApi summary records what it sent", () => {
  // swxtchio/swx-opencode#227: a summary keeps the selection it always had and
  // records the variant and effort its request actually sent. model-a's own
  // options send a high effort; its variants send low or high.
  const effortConfig = (url: string) => {
    const base = testProviderConfig(url)
    return {
      ...base,
      provider: {
        test: {
          ...base.provider.test,
          models: {
            "model-a": {
              ...base.provider.test.models["test-model"],
              id: "model-a",
              name: "model-a",
              options: { reasoningEffort: "high" },
              variants: { low: { reasoningEffort: "low" }, high: { reasoningEffort: "high" } },
            },
          },
        },
      },
    }
  }
  const routes = (directory: string, sessionID: SessionIDType) => {
    const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
    const params = { sessionID }
    return {
      prompt: (text: string, variant?: string) =>
        request(pathFor(SessionPaths.prompt, params), {
          method: "POST",
          headers,
          body: JSON.stringify({
            agent: "build",
            model: { providerID: "test", modelID: "model-a" },
            ...(variant ? { variant } : {}),
            parts: [{ type: "text", text }],
          }),
        }),
      summarize: () =>
        request(pathFor(SessionPaths.summarize, params), {
          method: "POST",
          headers,
          body: JSON.stringify({ providerID: "test", modelID: "model-a", auto: false }),
        }),
      summaries: () =>
        requestJson<SessionV1.WithParts[]>(pathFor(SessionPaths.messages, params), { headers }).pipe(
          Effect.map((messages) =>
            messages.flatMap((message) =>
              message.info.role === "assistant" && message.info.summary
                ? [
                    {
                      modelID: String(message.info.modelID),
                      variant: message.info.variant,
                      effort: message.info.effort,
                    },
                  ]
                : [],
            ),
          ),
        ),
    }
  }

  it.live(
    "an explicit summary runs as it always has and records the effort its request sent",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({ git: true, config: effortConfig(llm.url) })
        const api = routes(
          directory,
          (yield* createSession({ title: "explicit summary" }).pipe(provideInstanceEffect(directory))).id,
        )
        yield* llm.text("start done")
        expect((yield* api.prompt("start")).status).toBe(200)
        yield* llm.text("first summary")
        expect((yield* api.summarize()).status).toBe(200)
        // A selected variant is the prompts' selection; an explicit summary names no variant, as before.
        yield* llm.text("low done")
        expect((yield* api.prompt("at low effort", "low")).status).toBe(200)
        yield* llm.text("second summary")
        expect((yield* api.summarize()).status).toBe(200)

        const sent = yield* llm.inputs
        expect(sent.map((input) => input.reasoning_effort)).toEqual(["high", "high", "low", "high"])
        expect(yield* api.summaries()).toEqual([
          { modelID: "model-a", variant: undefined, effort: "high" },
          { modelID: "model-a", variant: undefined, effort: "high" },
        ])
      }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
    30_000,
  )

  it.live(
    "a summary records the effort a plugin's chat.params left its request with",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const plugin = path.join(yield* tmpdirScoped(), "summary-effort.ts")
        yield* Effect.promise(() =>
          Bun.write(
            plugin,
            [
              "export default async () => ({",
              '  "chat.params": async (input, output) => {',
              '    if (input.agent === "compaction") output.options.reasoningEffort = "low"',
              "  },",
              "})",
              "",
            ].join("\n"),
          ),
        )
        const directory = yield* tmpdirScoped({
          git: true,
          config: { ...effortConfig(llm.url), plugin: [pathToFileURL(plugin).href] },
        })
        const api = routes(
          directory,
          (yield* createSession({ title: "plugin-adjusted summary" }).pipe(provideInstanceEffect(directory))).id,
        )
        yield* llm.text("start done")
        expect((yield* api.prompt("start")).status).toBe(200)
        yield* llm.text("summary")
        expect((yield* api.summarize()).status).toBe(200)

        expect((yield* llm.inputs).map((input) => input.reasoning_effort)).toEqual(["high", "low"])
        expect(yield* api.summaries()).toEqual([{ modelID: "model-a", variant: undefined, effort: "low" }])
      }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
    30_000,
  )

  it.live(
    "a Gemini summary records the thinking level its request sent by default",
    () =>
      Effect.gen(function* () {
        // A Gemini endpoint that answers every request with one finished reply.
        const bodies: { generationConfig?: { thinkingConfig?: { thinkingLevel?: string } } }[] = []
        const server = yield* Effect.acquireRelease(
          Effect.sync(() =>
            Bun.serve({
              port: 0,
              async fetch(request) {
                bodies.push(await request.json())
                const chunk = {
                  candidates: [
                    { content: { parts: [{ text: "done" }], role: "model" }, finishReason: "STOP", index: 0 },
                  ],
                  usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 5, totalTokenCount: 15 },
                }
                return new Response(`data: ${JSON.stringify(chunk)}\n\n`, {
                  headers: { "content-type": "text/event-stream" },
                })
              },
            }),
          ),
          (server) => Effect.promise(() => server.stop(true)),
        )
        const directory = yield* tmpdirScoped({
          git: true,
          config: {
            formatter: false,
            lsp: false,
            provider: {
              gemini: {
                name: "Gemini",
                id: "gemini",
                env: [],
                npm: "@ai-sdk/google",
                options: { apiKey: "test-key", baseURL: server.url.href.replace(/\/$/, "") },
                models: {
                  "gemini-3-pro-preview": {
                    id: "gemini-3-pro-preview",
                    name: "Gemini 3 Pro",
                    attachment: false,
                    reasoning: true,
                    temperature: false,
                    tool_call: true,
                    release_date: "2025-01-01",
                    limit: { context: 100_000, output: 10_000 },
                    cost: { input: 0, output: 0 },
                    options: {},
                  },
                },
              },
            },
          },
        })
        const sessionID = (yield* createSession({ title: "gemini summary" }).pipe(provideInstanceEffect(directory))).id
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const model = { providerID: "gemini", modelID: "gemini-3-pro-preview" }
        const prompted = yield* request(pathFor(SessionPaths.prompt, { sessionID }), {
          method: "POST",
          headers,
          body: JSON.stringify({ agent: "build", model, parts: [{ type: "text", text: "start" }] }),
        })
        expect(prompted.status).toBe(200)
        const summarized = yield* request(pathFor(SessionPaths.summarize, { sessionID }), {
          method: "POST",
          headers,
          body: JSON.stringify({ ...model, auto: false }),
        })
        expect(summarized.status).toBe(200)

        expect(bodies.map((body) => body.generationConfig?.thinkingConfig?.thinkingLevel)).toEqual(["high", "high"])
        const messages = yield* requestJson<SessionV1.WithParts[]>(pathFor(SessionPaths.messages, { sessionID }), {
          headers,
        })
        expect(
          messages.flatMap((message) =>
            message.info.role === "assistant" && message.info.summary ? [message.info.effort] : [],
          ),
        ).toEqual(["high"])
      }).pipe(Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
    30_000,
  )

  it.live(
    "a summary interrupted after its request was sent still records what it sent",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({ git: true, config: effortConfig(llm.url) })
        const sessionID = (yield* createSession({ title: "interrupted summary" }).pipe(
          provideInstanceEffect(directory),
        )).id
        const api = routes(directory, sessionID)
        yield* llm.text("start done")
        expect((yield* api.prompt("start")).status).toBe(200)
        yield* llm.hold("summary", Effect.runPromise(Deferred.await(gate)))
        const summarizing = yield* api.summarize().pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(2), "the summary never reached the provider", "10 seconds")
        expect((yield* llm.inputs)[1]?.reasoning_effort).toBe("high")
        const aborted = yield* request(pathFor(SessionPaths.abort, { sessionID }), {
          method: "POST",
          headers: { "x-opencode-directory": directory, "content-type": "application/json" },
          body: "{}",
        })
        expect(aborted.status).toBe(200)
        yield* awaitWithTimeout(Fiber.await(summarizing), "the summary request never returned", "10 seconds")
        const stopped = yield* pollWithTimeout(
          requestJson<SessionV1.WithParts[]>(pathFor(SessionPaths.messages, { sessionID }), {
            headers: { "x-opencode-directory": directory },
          }).pipe(
            Effect.map((messages) =>
              messages.find(
                (message) =>
                  message.info.role === "assistant" &&
                  message.info.summary &&
                  message.info.time.completed !== undefined,
              ),
            ),
          ),
          "the interrupted summary never ended",
          "10 seconds",
        )
        expect(stopped.info.role === "assistant" ? stopped.info.effort : undefined).toBe("high")
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )

  it.live(
    "an automatic terminal summary resumes compacted work and keeps the last prompt's variant",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const directory = yield* tmpdirScoped({ git: true, config: effortConfig(llm.url) })
        const api = routes(
          directory,
          (yield* createSession({ title: "automatic summary" }).pipe(provideInstanceEffect(directory))).id,
        )
        // A terminal soft-threshold crossing compacts at idle and resumes before later prompts.
        yield* llm.text("start done", { usage: { input: 95_000, output: 10 } })
        expect((yield* api.prompt("start", "low")).status).toBe(200)
        yield* llm.text("automatic summary")
        yield* llm.text("answered")
        yield* llm.text("next prompt answered")
        expect((yield* api.prompt("after the context filled", "low")).status).toBe(200)

        const sent = yield* llm.inputs
        expect(sent.map((input) => input.reasoning_effort)).toEqual(["low", "low", "low", "low"])
        expect(JSON.stringify((sent[1]!.messages as unknown[]).at(-1))).toContain("Here is the conversation so far")
        expect(JSON.stringify(sent[2].messages)).toContain("Continue if you have next steps")
        expect(JSON.stringify(sent[3].messages)).toContain("after the context filled")
        expect(JSON.stringify(sent[3].messages)).not.toContain("next prompt answered")
        expect(yield* api.summaries()).toEqual([{ modelID: "model-a", variant: "low", effort: "low" }])
      }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
    30_000,
  )
})

describe("session HttpApi queue freshness", () => {
  const iso = (time: number) => new Date(time).toISOString().replace(/\.\d{3}Z$/, "Z")
  const envelope = (msgid: string, text: string) =>
    `[fm-from-peer]\x1f peer-a\x1f /home/peer\x1f ${msgid}\x1f [fm-level:request]\x1f ${text}`

  it.live(
    "a prompt that waited is delivered with when it was queued, while its stored message stays as sent",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "queue freshness" }).pipe(provideInstanceEffect(directory))
        const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
        const params = { sessionID: session.id }
        const post = (path: string, body?: unknown) =>
          request(pathFor(path, params), {
            method: "POST",
            headers,
            body: body === undefined ? undefined : JSON.stringify(body),
          })
        const send = (text: string, selection: object = {}) =>
          post(SessionPaths.promptAsync, {
            agent: "build",
            delivery: "queue",
            ...selection,
            parts: [{ type: "text", text }],
          })
        const listed = () => requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers })
        const text = (item: SessionQueue.Item) =>
          item.input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")
        const peerOne = envelope("p1760000000-1-abcd", "please check the build")
        const peerTwo = envelope("p1760000001-2-beef", "please check the deploy")

        yield* send("start")
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        expect((yield* post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
        yield* send("same request")
        // A copy that names the model it was sent under is still the same request.
        yield* send("same request", { model: { providerID: "test", modelID: "test-model" } })
        for (const prompt of ["run lint", "run lint --fix", peerOne, peerTwo]) yield* send(prompt)
        const admitted = yield* pollWithTimeout(
          listed().pipe(Effect.map((items) => (items.length === 5 ? items : undefined))),
          "prompts never queued",
        )
        // Pending, each is listed exactly as sent, as an exact-text delivery check expects; the
        // second copy of "same request" took the first in as it was admitted, keeping its time.
        expect(admitted.map(text)).toEqual(["same request", "run lint", "run lint --fix", peerOne, peerTwo])
        const [same, lint, fix] = admitted
        const first = same!.origins[0]!.time
        expect(same!.origins).toEqual([{ time: first, repeats: [{ time: same!.time.created }] }])
        expect(first).toBeLessThanOrEqual(same!.time.created)
        expect(admitted.slice(1).map((item) => item.origins)).toEqual(
          admitted.slice(1).map((item) => [{ time: item.time.created }]),
        )
        // A row a stopped process left two hours ago, stored before origins were kept.
        const old = Date.now() - 2 * 60 * 60 * 1000
        const { db } = yield* Database.Service
        yield* db
          .insert(SessionPromptQueueTable)
          .values({
            id: SessionPromptQueue.ItemID.create(),
            session_id: session.id,
            seq: 1000,
            delivery: "queue",
            input: { agent: "build", parts: [{ type: "text", text: "left by a stopped process" }] },
            time_created: old,
          })
          .run()

        const consolidated = yield* json<SessionQueue.Item[]>(yield* post(SessionQueuePaths.consolidate))
        expect(consolidated.map(text)).toEqual([
          "same request",
          "run lint:\n1. run lint\n2. --fix",
          peerOne,
          peerTwo,
          "left by a stopped process",
        ])
        expect(consolidated[0]).toEqual(same)
        expect(consolidated[1]!.origins).toEqual([{ time: lint!.time.created }, { time: fix!.time.created }])
        expect(consolidated[4]!.origins).toEqual([{ time: old }])

        yield* Deferred.succeed(gate, undefined)
        for (const reply of ["same done", "lint done", "old done", "peers done"]) yield* llm.text(reply)
        expect((yield* post(SessionQueuePaths.release, { holder: "view" })).status).toBe(204)
        const sent = yield* pollWithTimeout(
          llm.inputs.pipe(Effect.map((inputs) => (inputs.length === 5 ? inputs : undefined))),
          "the model never received every queued prompt",
          "10 seconds",
        )
        const lastUser = sent.map((input) => JSON.stringify((input.messages as unknown[]).at(-1)))
        // Delivered by its own admission, the first prompt is timestamped too.
        expect(lastUser[0]).toMatch(
          /\[Queued prompt: queued at \S+; delivered at \S+, \d+s after it was first queued\.]/,
        )
        expect(lastUser[1]).toContain(
          `[Queued prompt: queued at ${iso(first)}, and again at ${iso(same!.time.created)}; delivered at `,
        )
        expect(lastUser[2]).toContain(
          `[Queued prompt merged from 2 queued instructions: step 1 at ${iso(lint!.time.created)}; step 2 at ${iso(fix!.time.created)}; delivered at `,
        )
        // Native human input remains individual and precedes the complete peer batch.
        expect(lastUser[3]).toContain(`[Queued prompt: queued at ${iso(old)}; delivered at `)
        expect(lastUser[3]).toMatch(/, 2h(\d+m)?(\d+s)? after it was first queued\.]/)
        expect(lastUser[4]).toContain(JSON.stringify(peerOne).slice(1, -1))
        expect(lastUser[4]).toContain(JSON.stringify(peerTwo).slice(1, -1))
        expect(lastUser[4]).toContain(
          `[Queued prompt merged from 2 queued instructions: step 1 at ${iso(admitted[3]!.time.created)}; step 2 at ${iso(admitted[4]!.time.created)}; delivered at `,
        )
        expect(sent.every((input) => input.model === "test-model")).toBe(true)
        expect(yield* llm.pending).toBe(0)

        // Freshness notes belong to model requests. Native text stays as sent;
        // grouped peers retain their exact originals in the established constituent metadata.
        const messages = yield* requestJson<SessionV1.WithParts[]>(pathFor(SessionPaths.messages, params), { headers })
        const stored = messages.filter((message) => message.info.role === "user")
        expect(stored).toHaveLength(5)
        stored.forEach((message) =>
          message.parts.forEach((part) => {
            if (part.type === "text") expect(part.text).not.toContain("[Queued prompt")
          }),
        )
        const isPeerAggregate = (part: SessionV1.Part) =>
          part.type === "text" &&
          !part.ignored &&
          !!part.metadata?.sessionQueue &&
          Schema.decodeUnknownSync(SessionPromptQueue.Group)(part.metadata.sessionQueue).kind === "peer"
        const peerMessage = stored.find((message) => message.parts.some(isPeerAggregate))!
        const aggregate = peerMessage.parts.find(isPeerAggregate)
        if (aggregate?.type !== "text") throw new Error("Expected the actual peer aggregate")
        const group = Schema.decodeUnknownSync(SessionPromptQueue.Group)(aggregate.metadata?.sessionQueue)
        expect(group.kind).toBe("peer")
        expect(group.constituents.map((item) => item.input.parts)).toEqual([
          [{ type: "text", text: peerOne }],
          [{ type: "text", text: peerTwo }],
        ])
        expect(group.constituents.map((item) => item.time.created)).toEqual(
          admitted.slice(3).map((item) => item.time.created),
        )
        const witnesses = peerMessage.parts.flatMap((part) =>
          part.type === "text" && part.ignored && part.metadata?.sessionQueue
            ? Schema.decodeUnknownSync(SessionPromptQueue.Group)(part.metadata.sessionQueue).constituents
            : [],
        )
        expect(witnesses.toSorted((a, b) => a.seq - b.seq)).toEqual([...group.constituents])
        expect(
          stored
            .filter((message) => message.info.id !== peerMessage.info.id)
            .map((message) => message.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")),
        ).toEqual(["start", "same request", "run lint:\n1. run lint\n2. --fix", "left by a stopped process"])
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )
})

describe("session HttpApi request receipts", () => {
  const receiptRoutes = (directory: string, sessionID: SessionIDType) => {
    const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
    const params = { sessionID }
    const post = (path: string, body?: unknown) =>
      request(pathFor(path, params), {
        method: "POST",
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
      })
    return {
      headers,
      params,
      post,
      send: (text: string, requestID?: string) =>
        post(SessionPaths.promptAsync, {
          agent: "build",
          delivery: "queue",
          ...(requestID ? { requestID } : {}),
          parts: [{ type: "text", text }],
        }),
      listed: () => requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers }),
      receipt: (requestID: string) =>
        request(pathFor(SessionQueuePaths.receipt, { ...params, requestID }), { headers }),
      users: () =>
        requestJson<SessionV1.WithParts[]>(pathFor(SessionPaths.messages, params), { headers }).pipe(
          Effect.map((messages) =>
            messages
              .filter((message) => message.info.role === "user")
              .map((message) => ({
                id: message.info.id,
                text: message.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join(""),
              })),
          ),
        ),
    }
  }

  it.live(
    "a request's receipt follows it from pending to delivered, and its retry admits nothing",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "request receipts" }).pipe(provideInstanceEffect(directory))
        const routes = receiptRoutes(directory, session.id)
        const sent = "please check the build"

        expect((yield* routes.send("start")).status).toBe(204)
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        expect((yield* routes.send(sent, "p1760000000-1-abcd")).status).toBe(204)
        const [item] = yield* pollWithTimeout(
          routes.listed().pipe(Effect.map((items) => (items.length === 1 ? items : undefined))),
          "the request never queued",
        )
        expect(item!.requestID).toBe("p1760000000-1-abcd")
        expect(yield* json(yield* routes.receipt("p1760000000-1-abcd"))).toEqual({ state: "pending", itemID: item!.id })

        // The same prompt again is the same request: nothing more is queued.
        expect((yield* routes.send(sent, "p1760000000-1-abcd")).status).toBe(204)
        // The same request with another prompt is refused before anything runs.
        const conflict = yield* routes.send("please check the deploy", "p1760000000-1-abcd")
        expect(conflict.status).toBe(409)
        expect(yield* conflict.json).toMatchObject({ _tag: "PromptRequestConflict", requestID: "p1760000000-1-abcd" })
        const unknown = yield* routes.receipt("p1760000009-9-0000")
        expect(unknown.status).toBe(404)
        expect(yield* unknown.json).toMatchObject({ name: "NotFoundError" })
        expect((yield* routes.receipt("not a request id")).status).toBe(400)
        expect((yield* routes.listed()).map((queued) => queued.id)).toEqual([item!.id])

        yield* llm.text("build checked")
        yield* Deferred.succeed(gate, undefined)
        const delivered = yield* pollWithTimeout(
          routes.receipt("p1760000000-1-abcd").pipe(
            Effect.flatMap(json<SessionQueue.Receipt>),
            Effect.map((receipt) => (receipt.state === "delivered" ? receipt : undefined)),
          ),
          "the request was never delivered",
          "10 seconds",
        )
        const users = yield* routes.users()
        expect(users.filter((user) => user.text === sent)).toEqual([{ id: delivered.messageID, text: sent }])

        // A retry after delivery admits nothing either.
        expect((yield* routes.send(sent, "p1760000000-1-abcd")).status).toBe(204)
        expect(yield* routes.listed()).toEqual([])
        expect((yield* routes.users()).filter((user) => user.text === sent)).toHaveLength(1)
        expect(yield* json(yield* routes.receipt("p1760000000-1-abcd"))).toEqual(delivered)
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )

  it.live(
    "a deleted request says so, and a survivor without a request carries the one merged into it",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "request receipts" }).pipe(provideInstanceEffect(directory))
        const routes = receiptRoutes(directory, session.id)

        yield* routes.send("start")
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        expect((yield* routes.post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
        yield* routes.send("run lint")
        yield* routes.send("run lint --fix", "tui-fix")
        yield* routes.send("drop this", "tui-drop")
        const queued = yield* pollWithTimeout(
          routes.listed().pipe(Effect.map((items) => (items.length === 3 ? items : undefined))),
          "prompts never queued",
        )
        expect((yield* routes.post(SessionQueuePaths.remove, { id: queued[2]!.id })).status).toBe(200)
        expect(yield* json(yield* routes.receipt("tui-drop"))).toEqual({ state: "deleted" })

        const [merged] = yield* json<SessionQueue.Item[]>(yield* routes.post(SessionQueuePaths.consolidate))
        expect(merged!.id).toBe(queued[0]!.id)
        expect(merged!.requestID).toBe("tui-fix")
        expect(merged!.origins).toEqual([
          { time: queued[0]!.time.created },
          { time: queued[1]!.time.created, requestID: "tui-fix" },
        ])
        expect(yield* json(yield* routes.receipt("tui-fix"))).toEqual({ state: "pending", itemID: merged!.id })

        yield* llm.text("lint done")
        yield* Deferred.succeed(gate, undefined)
        expect((yield* routes.post(SessionQueuePaths.release, { holder: "view" })).status).toBe(204)
        const delivered = yield* pollWithTimeout(
          routes.receipt("tui-fix").pipe(
            Effect.flatMap(json<SessionQueue.Receipt>),
            Effect.map((receipt) => (receipt.state === "delivered" ? receipt : undefined)),
          ),
          "the merged prompt was never delivered",
          "10 seconds",
        )
        expect((yield* routes.users()).find((user) => user.id === delivered.messageID)?.text).toBe(
          "run lint:\n1. run lint\n2. --fix",
        )
        expect(yield* json(yield* routes.receipt("tui-drop"))).toEqual({ state: "deleted" })
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    30_000,
  )
})

describe("session HttpApi queue replacement at admission", () => {
  const iso = (time: number) => new Date(time).toISOString().replace(/\.\d{3}Z$/, "Z")
  const firstmate = (level: string, text: string) => `[fm-from-firstmate]\x1f[fm-level:${level}]\x1f ${text}`
  const peer = (name: string, msgid: string, level: string, text: string) =>
    `[fm-from-peer]\x1f ${name}\x1f /home/${name}\x1f ${msgid}\x1f [fm-level:${level}]\x1f ${text}`
  const routesFor = (directory: string, sessionID: SessionIDType) => {
    const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
    const params = { sessionID }
    const body = (text: string, requestID?: string) => ({
      agent: "build",
      ...(requestID ? { requestID } : {}),
      parts: [{ type: "text", text }],
    })
    return {
      send: (text: string, requestID?: string, delivery?: SessionQueue.Delivery) =>
        request(pathFor(SessionPaths.promptAsync, params), {
          method: "POST",
          headers,
          body: JSON.stringify({ ...body(text, requestID), ...(delivery ? { delivery } : {}) }),
        }),
      sync: (text: string, requestID: string) =>
        request(pathFor(SessionPaths.prompt, params), {
          method: "POST",
          headers,
          body: JSON.stringify(body(text, requestID)),
        }),
      listed: () => requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers }),
      receipt: (requestID: string) =>
        requestJson<SessionQueue.Receipt>(pathFor(SessionQueuePaths.receipt, { ...params, requestID }), { headers }),
    }
  }
  const text = (item: SessionQueue.Item) =>
    item.input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")

  it.live(
    "notices a newer one positively supersedes retire, repeated peer requests merge, and every distinct item stays with its age",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "queue replacement" }).pipe(provideInstanceEffect(directory))
        const routes = routesFor(directory, session.id)
        const ask = "please review the hostname brief"
        const notices = {
          waiting1: firstmate("nudge:wait.hostname", "Waiting on the hostname brief review since 10:00"),
          review1: firstmate("nudge:review.r1", "Reviewer r1 completed round 1"),
          idle1: firstmate("nudge:idle.datadog", "Datadog task idle for 10m"),
          clerk1: firstmate("nudge:state.clerk", "Clerk draft: convergence approved"),
          ask1: peer("peer-a", "p1760000000-1-aaaa", "request", ask),
          waiting2: firstmate("nudge:wait.hostname", "Waiting on the hostname brief review since 10:00, now 40m"),
          idle2: firstmate("nudge:idle.datadog", "Datadog task idle for 20m"),
          review2: firstmate("nudge:review.r1", "Reviewer r1 completed round 2"),
          ask2: peer("peer-a", "p1760000000-2-bbbb", "request", ask),
          clerk2: firstmate(
            "nudge:state.clerk",
            "Clerk draft: exchange missing; earlier convergence notices are stale",
          ),
          storageA: firstmate("nudge:wait.storage-a", "Storage A held on dependency X"),
          storageB: firstmate("nudge:wait.storage-b", "Storage B held on dependency Y"),
          otherPeer: peer("peer-b", "p1760000000-3-cccc", "request", ask),
          reply: peer("peer-a", "p1760000000-4-dddd", "info", "re:p1760000000-1-aaaa thanks"),
          emulator: firstmate("request", "Emulator removal is active"),
          quote: firstmate("request", "Plan the quote recovery"),
          unnamed: firstmate("nudge:idle.datadog", "Datadog task idle for 30m, sent without a request"),
          // Another sender's notice under the same key, and the same reply sent again.
          peerWaiting: peer("peer-b", "p1760000000-7-abab", "nudge:wait.hostname", "peer-b waits on the brief too"),
          reply2: peer("peer-a", "p1760000000-8-efef", "info", "re:p1760000000-1-aaaa thanks"),
        }
        const requestIDs: Record<string, string | undefined> = {
          ask1: "p1760000000-1-aaaa",
          ask2: "p1760000000-2-bbbb",
          otherPeer: "p1760000000-3-cccc",
          reply: "p1760000000-4-dddd",
          unnamed: undefined,
          peerWaiting: "p1760000000-7-abab",
          reply2: "p1760000000-8-efef",
        }
        const named = (key: string) => (key in requestIDs ? requestIDs[key] : `fm-${key}`)

        yield* routes.send("start")
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        for (const [key, message] of Object.entries(notices))
          expect((yield* routes.send(message, named(key))).status).toBe(204)
        const survivors = [
          notices.waiting2,
          notices.review2,
          notices.idle2,
          notices.clerk2,
          notices.ask2,
          notices.storageA,
          notices.storageB,
          notices.otherPeer,
          notices.reply,
          notices.emulator,
          notices.quote,
          notices.unnamed,
          notices.peerWaiting,
          notices.reply2,
        ]
        const pending = yield* pollWithTimeout(
          routes.listed().pipe(Effect.map((items) => (items.length === survivors.length ? items : undefined))),
          "the notices never settled",
        )
        // The newest of each kind holds the place of the oldest it replaced.
        expect(pending.map(text)).toEqual(survivors)

        for (const [old, by] of [
          ["waiting1", "waiting2"],
          ["review1", "review2"],
          ["idle1", "idle2"],
          ["clerk1", "clerk2"],
        ])
          expect(yield* routes.receipt(named(old)!)).toEqual({ state: "superseded", by: named(by)! })
        const merged = pending.find((item) => item.requestID === "p1760000000-2-bbbb")!
        expect(merged.origins).toHaveLength(1)
        expect(merged.origins[0]).toMatchObject({
          requestID: "p1760000000-1-aaaa",
          repeats: [{ requestID: "p1760000000-2-bbbb" }],
        })
        expect(yield* routes.receipt("p1760000000-1-aaaa")).toEqual({ state: "merged", into: "p1760000000-2-bbbb" })
        for (const key of ["storageA", "storageB", "otherPeer", "reply", "emulator", "quote"])
          expect(yield* routes.receipt(named(key)!)).toMatchObject({ state: "pending" })

        yield* Deferred.succeed(gate, undefined)
        const inputs = yield* pollWithTimeout(
          llm.inputs.pipe(Effect.map((inputs) => (inputs.length === 4 ? inputs : undefined))),
          "the survivors were never all delivered",
          "20 seconds",
        )
        const last = inputs.map((input) => JSON.stringify((input.messages as unknown[]).at(-1)))
        survivors.forEach((survivor) =>
          expect(last.slice(1).some((message) => message.includes(JSON.stringify(survivor).slice(1, -1)))).toBe(true),
        )
        expect(last[1]).toContain(JSON.stringify(notices.ask1).slice(1, -1))
        expect(last[1]).toContain("informational entries create no reply obligation")
        const sent = JSON.stringify(inputs)
        for (const retired of ["convergence approved", "completed round 1", "idle for 10m", 'since 10:00"'])
          expect(sent).not.toContain(retired)
        // The merged peer request says when each send was queued, and names both requests to answer.
        const askNote = last.find(
          (message) => message.includes("p1760000000-2-bbbb") && message.includes("[Queued prompt"),
        )!
        expect(askNote).toContain(
          `step 1 at ${iso(merged.origins[0]!.time)}, and again at ${iso(merged.origins[0]!.repeats![0]!.time)}`,
        )
        expect(askNote).not.toContain("each owed its own reply")
        expect(askNote).toContain("Reply only to each response-owed original msgid")
        const delivered = yield* routes.receipt("p1760000000-2-bbbb")
        expect(delivered.state).toBe("delivered")
        expect(yield* routes.receipt("p1760000000-1-aaaa")).toEqual({
          state: "merged",
          into: "p1760000000-2-bbbb",
          messageID: (delivered as Extract<SessionQueue.Receipt, { state: "delivered" }>).messageID,
        })
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    60_000,
  )

  it.live(
    "a sync caller of a merged request gets its survivor's reply, and commands merge only when each names a request",
    () => {
      const gate = Deferred.makeUnsafe<void>()
      return Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
        const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const session = yield* createSession({ title: "queue replacement" }).pipe(provideInstanceEffect(directory))
        const routes = routesFor(directory, session.id)
        const ask = "please rerun the deploy check"

        yield* routes.send("start")
        yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
        const waiting = yield* routes
          .sync(peer("peer-a", "p1760000000-5-eeee", "request", ask), "p1760000000-5-eeee")
          .pipe(Effect.forkChild)
        yield* pollWithTimeout(
          routes.listed().pipe(Effect.map((items) => (items.length === 1 ? items : undefined))),
          "the sync request never queued",
        )
        yield* routes.send(peer("peer-a", "p1760000000-6-ffff", "request", ask), "p1760000000-6-ffff")
        yield* routes.send("run lint", "tui-lint", "queue")
        yield* routes.send("run lint --fix", "tui-fix", "queue")
        yield* routes.send("deploy app", undefined, "queue")
        yield* routes.send("deploy app now", "tui-deploy", "queue")
        const pending = yield* pollWithTimeout(
          routes.listed().pipe(Effect.map((items) => (items.length === 4 ? items : undefined))),
          "the prompts never settled",
        )
        expect(pending.map(text)).toEqual([
          peer("peer-a", "p1760000000-6-ffff", "request", ask),
          "run lint:\n1. run lint\n2. --fix",
          "deploy app",
          "deploy app now",
        ])
        expect(yield* routes.receipt("tui-fix")).toEqual({ state: "merged", into: "tui-lint" })
        expect(yield* routes.receipt("p1760000000-5-eeee")).toEqual({ state: "merged", into: "p1760000000-6-ffff" })

        // Held human/native commands keep their individual precedence; the peer batch follows them.
        yield* llm.text("lint checked")
        yield* llm.text("app checked")
        yield* llm.text("app now checked")
        yield* llm.text("deploy check rerun")
        yield* Deferred.succeed(gate, undefined)
        const reply = yield* Fiber.join(waiting).pipe(Effect.flatMap(json<SessionV1.WithParts>))
        expect(reply.info.role).toBe("assistant")
        expect(reply.parts.flatMap((part) => (part.type === "text" ? [part.text] : []))).toEqual(["deploy check rerun"])
        const delivered = yield* routes.receipt("p1760000000-6-ffff")
        expect(delivered.state).toBe("delivered")
        if (delivered.state === "delivered" && reply.info.role === "assistant")
          expect(reply.info.parentID).toBe(delivered.messageID)
      }).pipe(
        Effect.ensuring(Deferred.succeed(gate, undefined)),
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
      )
    },
    60_000,
  )
})

describe("session HttpApi writes racing removal", () => {
  const options = { git: true, config: { formatter: false, lsp: false, share: "disabled" as const } }

  const aggregate = (sessionID: SessionIDType) =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      return {
        seq: yield* EventV2.latestSequence(db, sessionID),
        events: (yield* db.select().from(EventTable).where(eq(EventTable.aggregate_id, sessionID)).all()).length,
      }
    })

  const send = (directory: string, path: string, method: string, body?: unknown) =>
    request(path, {
      method,
      headers: { "x-opencode-directory": directory, "content-type": "application/json" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    })

  // A bound on reaching the hold, well clear of the real work some routes do first (revert snapshots the
  // worktree); reaching it is the signal, so this only decides how long a broken route takes to fail.
  const ready = "30 seconds"
  const racing = 60_000

  // Lets a held request continue and stops waiting for it on every exit, so a failed race never leaves a server
  // request parked behind the hold.
  const released = (release: Effect.Effect<void>, response: Fiber.Fiber<unknown, unknown>) =>
    release.pipe(Effect.andThen(Fiber.interrupt(response)), Effect.asVoid)

  // Holds the request right after `hold` (a Session read under a named span) and removes the Session there. A
  // request that lost the race must answer the route's declared not found, never success or a 500, and leave no
  // aggregate behind.
  const expectRemovedDuring = (input: {
    readonly sessionID: SessionIDType
    readonly hold: { readonly name: string; readonly parent: string }
    readonly request: Effect.Effect<HttpClientResponse.HttpClientResponse, unknown, HttpClient.HttpClient>
  }) =>
    Effect.gen(function* () {
      const hold = yield* racingSpans.arm(input.hold)
      const response = yield* input.request.pipe(Effect.forkChild)

      yield* Effect.gen(function* () {
        yield* awaitWithTimeout(hold.reached, `request never read the Session under ${input.hold.parent}`, ready)
        yield* Session.use.remove(input.sessionID)
        yield* hold.release
        const answered = yield* Fiber.join(response)

        expect(answered.status).toBe(404)
        expect(yield* responseJson(answered)).toMatchObject({ name: "NotFoundError" })
        expect(yield* aggregate(input.sessionID)).toEqual({ seq: -1, events: 0 })
      }).pipe(Effect.ensuring(released(hold.disarm, response)))
    })

  // Session.patch reads the Session and then publishes; these hold a setter between the two.
  const insidePatch = (setter: string) => ({ name: "Session.get", parent: setter })
  // These handlers check the Session and then write without reading it again.
  const afterCheck = { name: "Session.get", parent: "SessionHttpApi.requireSession" }

  itRacing.instance(
    "answers not found when the title write commits after the Session was removed",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "removed during patch" })
        yield* expectRemovedDuring({
          sessionID: created.id,
          hold: insidePatch("Session.setTitle"),
          request: send(test.directory, pathFor(SessionPaths.update, { sessionID: created.id }), "PATCH", {
            title: "late",
          }),
        })
      }),
    options,
    racing,
  )

  itRacing.instance(
    "answers not found when the Session is removed between two of the update's writes",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const events = yield* EventV2Bridge.Service
        const created = yield* createSession({ title: "removed between writes" })
        const titled = yield* Deferred.make<void>()
        const removed = yield* Deferred.make<void>()
        // Durable listeners run in the publishing request after its commit, so this holds the request after setTitle.
        const unsubscribe = yield* events.listen((event) =>
          event.type === Session.Event.Updated.type &&
          (event.data as { sessionID?: string }).sessionID === created.id &&
          (event.data as { info?: { title?: string } }).info?.title === "titled"
            ? Deferred.succeed(titled, undefined).pipe(Effect.andThen(Deferred.await(removed)))
            : Effect.void,
        )
        yield* Effect.addFinalizer(() => unsubscribe)
        const response = yield* send(test.directory, pathFor(SessionPaths.update, { sessionID: created.id }), "PATCH", {
          title: "titled",
          metadata: { late: true },
        }).pipe(Effect.forkChild)

        yield* Effect.gen(function* () {
          yield* awaitWithTimeout(Deferred.await(titled), "update never committed its title", ready)
          yield* Session.use.remove(created.id)
          yield* Deferred.succeed(removed, undefined)
          const answered = yield* Fiber.join(response)

          expect(answered.status).toBe(404)
          expect(yield* responseJson(answered)).toMatchObject({ name: "NotFoundError" })
          expect(yield* aggregate(created.id)).toEqual({ seq: -1, events: 0 })
        }).pipe(Effect.ensuring(released(Deferred.succeed(removed, undefined).pipe(Effect.asVoid), response)))
      }),
    options,
    racing,
  )

  itRacing.instance(
    "answers not found when a message delete commits after the Session was removed",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "message delete race" })
        const { info } = yield* createTextMessage(created.id, "doomed")
        yield* expectRemovedDuring({
          sessionID: created.id,
          hold: afterCheck,
          request: send(
            test.directory,
            pathFor(SessionPaths.deleteMessage, { sessionID: created.id, messageID: info.id }),
            "DELETE",
          ),
        })
      }),
    options,
    racing,
  )

  itRacing.instance(
    "answers not found when a part delete commits after the Session was removed",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "part delete race" })
        const { info, part } = yield* createTextMessage(created.id, "doomed")
        yield* expectRemovedDuring({
          sessionID: created.id,
          hold: afterCheck,
          request: send(
            test.directory,
            pathFor(SessionPaths.deletePart, { sessionID: created.id, messageID: info.id, partID: part.id }),
            "DELETE",
          ),
        })
      }),
    options,
    racing,
  )

  itRacing.instance(
    "answers not found when a part update commits after the Session was removed",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "part update race" })
        const { info, part } = yield* createTextMessage(created.id, "before")
        yield* expectRemovedDuring({
          sessionID: created.id,
          hold: afterCheck,
          request: send(
            test.directory,
            pathFor(SessionPaths.updatePart, { sessionID: created.id, messageID: info.id, partID: part.id }),
            "PATCH",
            { ...part, text: "after" },
          ),
        })
      }),
    options,
    racing,
  )

  itRacing.instance(
    "answers not found when a revert records after the Session was removed",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "revert race" })
        const { info } = yield* createTextMessage(created.id, "revert to here")
        yield* expectRemovedDuring({
          sessionID: created.id,
          hold: insidePatch("Session.setRevert"),
          request: send(test.directory, pathFor(SessionPaths.revert, { sessionID: created.id }), "POST", {
            messageID: info.id,
          }),
        })
      }),
    options,
    racing,
  )

  itRacing.instance(
    "answers not found when an unrevert clears after the Session was removed",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "unrevert race" })
        const { info } = yield* createTextMessage(created.id, "reverted")
        yield* Session.use.setRevert({ sessionID: created.id, revert: { messageID: info.id }, summary: undefined })
        yield* expectRemovedDuring({
          sessionID: created.id,
          hold: insidePatch("Session.clearRevert"),
          request: send(test.directory, pathFor(SessionPaths.unrevert, { sessionID: created.id }), "POST"),
        })
      }),
    options,
    racing,
  )

  itRacing.instance(
    "answers not found when an unshare records after the Session was removed",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "unshare race" })
        yield* expectRemovedDuring({
          sessionID: created.id,
          hold: insidePatch("Session.setShare"),
          request: send(test.directory, pathFor(SessionPaths.share, { sessionID: created.id }), "DELETE"),
        })
      }),
    options,
    racing,
  )

  itRacing.instance(
    "answers not found when a summarize records its compaction after the Session was removed",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "summarize race" })
        yield* createTextMessage(created.id, "summarize me")
        yield* expectRemovedDuring({
          sessionID: created.id,
          // The last Session read before the compaction message is written.
          hold: { name: "Session.messages", parent: "SessionHttpApi.summarize" },
          request: send(test.directory, pathFor(SessionPaths.summarize, { sessionID: created.id }), "POST", {
            providerID: "test",
            modelID: "test",
          }),
        })
      }),
    options,
    racing,
  )

  itRacing.instance(
    "answers not found when a warp records its workspace after the Session was removed",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "warp race" })
        yield* expectRemovedDuring({
          sessionID: created.id,
          hold: insidePatch("Session.setWorkspace"),
          request: send(test.directory, WorkspacePaths.warp, "POST", {
            id: null,
            sessionID: created.id,
            copyChanges: false,
          }),
        })
      }),
    options,
    racing,
  )

  it.effect("leaves other defects as defects", () =>
    Effect.gen(function* () {
      const defect = new Error("unrelated failure")
      const exit = yield* HttpSessionError.mapSessionWriteNotFound(Effect.die(defect)).pipe(Effect.exit)

      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(defect)
    }),
  )

  itRacing.instance(
    "still updates a Session that is not removed",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "kept" })
        const before = yield* aggregate(created.id)

        const answered = yield* send(test.directory, pathFor(SessionPaths.update, { sessionID: created.id }), "PATCH", {
          title: "renamed",
          metadata: { kept: true },
        })

        expect(answered.status).toBe(200)
        expect(yield* responseJson(answered)).toMatchObject({
          id: created.id,
          title: "renamed",
          metadata: { kept: true },
        })
        expect(yield* aggregate(created.id)).toEqual({ seq: before.seq + 2, events: before.events + 2 })
      }),
    options,
    racing,
  )

  itRacing.instance(
    "still edits and deletes parts and messages of a Session that is not removed",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "kept messages" })
        const { info, part } = yield* createTextMessage(created.id, "before")
        const before = yield* aggregate(created.id)
        const partPath = pathFor(SessionPaths.updatePart, {
          sessionID: created.id,
          messageID: info.id,
          partID: part.id,
        })

        expect((yield* send(test.directory, partPath, "PATCH", { ...part, text: "after" })).status).toBe(200)
        expect(
          (yield* send(
            test.directory,
            pathFor(SessionPaths.deletePart, { sessionID: created.id, messageID: info.id, partID: part.id }),
            "DELETE",
          )).status,
        ).toBe(200)
        expect(
          (yield* send(
            test.directory,
            pathFor(SessionPaths.deleteMessage, { sessionID: created.id, messageID: info.id }),
            "DELETE",
          )).status,
        ).toBe(200)
        expect(yield* aggregate(created.id)).toEqual({ seq: before.seq + 3, events: before.events + 3 })
      }),
    options,
    racing,
  )
})

describe("session HttpApi submission-time admission", () => {
  const options = { git: true, config: { formatter: false, lsp: false, share: "disabled" as const } }

  // swxtchio/swx-opencode#249: admission is the first thing a prompt does. Even
  // held at its first config read it is listed, and it holds up neither a
  // later send's admission nor its own withdrawal.
  itRacing.instance(
    "a sent prompt is pending before any other work on it, and its preparation holds up nothing",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "admitted before config" })
        const headers = { "x-opencode-directory": test.directory, "content-type": "application/json" }
        const params = { sessionID: created.id }
        const post = (path: string, body: unknown) =>
          request(pathFor(path, params), { method: "POST", headers, body: JSON.stringify(body) })
        const listed = () => requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers })
        const send = (requestID: string, text: string) =>
          post(SessionPaths.prompt, { requestID, agent: "build", parts: [{ type: "text", text }] })
        // Held for review, so nothing is delivered and no provider is called.
        expect((yield* post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
        const hold = yield* racingSpans.arm({ name: "Config.get", parent: "SessionPrompt.prompt" })
        const first = yield* send("first", "check the build").pipe(Effect.forkChild)
        const second = yield* Effect.gen(function* () {
          yield* awaitWithTimeout(hold.reached, "the prompt never read its config", "30 seconds")
          const items = yield* listed()
          expect(items.map((item) => item.requestID)).toEqual(["first"])
          expect(
            yield* requestJson<SessionQueue.Receipt>(
              pathFor(SessionQueuePaths.receipt, { ...params, requestID: "first" }),
              { headers },
            ),
          ).toEqual({ state: "pending", itemID: items[0].id })
          const second = yield* send("second", "check the lint").pipe(Effect.forkChild)
          yield* awaitWithTimeout(
            Effect.gen(function* () {
              while ((yield* listed()).length < 2) yield* Effect.sleep("10 millis")
            }),
            "a later send was not admitted while the first prepared",
          )
          const withdrawn = yield* request(pathFor(SessionQueuePaths.withdraw, { ...params, itemID: items[0].id }), {
            method: "DELETE",
            headers,
          }).pipe(Effect.timeoutOption("5 seconds"))
          expect(Option.map(withdrawn, (response) => response.status)).toEqual(Option.some(200))
          expect((yield* listed()).map((item) => item.requestID)).toEqual(["second"])
          return second
        }).pipe(Effect.ensuring(hold.disarm))
        yield* Fiber.interrupt(second)
        yield* Fiber.interrupt(first)
      }),
    options,
    60_000,
  )

  // An exact retry that arrives while its original still prepares waits for
  // the original's classified delivery rather than refusing the provisional one.
  itRacing.instance(
    "an exact retry of a prompt still being prepared follows it",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "retry while preparing" })
        const headers = { "x-opencode-directory": test.directory, "content-type": "application/json" }
        const params = { sessionID: created.id }
        const post = (path: string, body: unknown) =>
          request(pathFor(path, params), { method: "POST", headers, body: JSON.stringify(body) })
        // Only the configured marker queues it; until its config is read it is a steer.
        const body = {
          messageID: MessageID.ascending(),
          requestID: "held-original",
          agent: "build",
          parts: [{ type: "text", text: "CUSTOM-HOLD: wait for the boundary" }],
        }
        expect((yield* post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
        const hold = yield* racingSpans.arm({ name: "Config.get", parent: "SessionPrompt.prompt" })
        const original = yield* post(SessionPaths.prompt, body).pipe(Effect.forkChild)
        const retry = yield* Effect.gen(function* () {
          yield* awaitWithTimeout(hold.reached, "the original never read its config", "30 seconds")
          const waiting = yield* racingSpans.observe({
            name: "SessionQueue.awaitPrepared",
            parent: "SessionPrompt.prompt",
          })
          const retry = yield* post(SessionPaths.prompt, body).pipe(Effect.forkChild)
          // The retry waits on the original instead of refusing its provisional delivery.
          yield* awaitWithTimeout(waiting, "the retry never waited on its original", "30 seconds")
          expect(retry.pollUnsafe()).toBeUndefined()
          return retry
        }).pipe(Effect.ensuring(hold.disarm))
        const items = yield* awaitWithTimeout(
          Effect.gen(function* () {
            while (true) {
              const listed = yield* requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), {
                headers,
              })
              if (listed[0]?.delivery === "queue") return listed
              yield* Effect.sleep("10 millis")
            }
          }),
          "the original was not prepared",
        )
        expect(items).toHaveLength(1)
        const withdrawn = yield* request(pathFor(SessionQueuePaths.withdraw, { ...params, itemID: items[0].id }), {
          method: "DELETE",
          headers,
        })
        expect(withdrawn.status).toBe(200)
        // A new session's first prompt waits for the review to end.
        expect((yield* post(SessionQueuePaths.release, { holder: "view" })).status).toBe(204)
        // Both callers followed the one prompt, so both are told it was withdrawn.
        for (const fiber of [original, retry])
          expect((yield* awaitWithTimeout(Fiber.join(fiber), "a caller never answered", "30 seconds")).status).toBe(409)
      }),
    {
      ...options,
      config: { ...options.config, machine_message_markers: { hold: [{ type: "prefix", value: "CUSTOM-HOLD:" }] } },
    },
    60_000,
  )

  // A retry waiting on an original whose preparation then fails is sent
  // anew, not told the original was withdrawn.
  itRacing.instance(
    "an exact retry of a prompt whose preparation fails is admitted again",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "x-opencode-directory": test.directory, "content-type": "application/json" }
        // Matched by its transport identity, or by its request's receipt.
        const identities: { messageID?: MessageID; requestID?: string }[] = [
          { messageID: MessageID.ascending() },
          { requestID: "failed-original" },
        ]
        for (const identity of identities) {
          const created = yield* createSession({ title: "retry after failed preparation" })
          const params = { sessionID: created.id }
          const listed = () => requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers })
          // As below: with only an absent provider configured, a variant has no model to select it on.
          const body = {
            ...identity,
            agent: "build",
            variant: "high",
            parts: [{ type: "text", text: "check the build" }],
          }
          const send = () =>
            request(pathFor(SessionPaths.prompt, params), { method: "POST", headers, body: JSON.stringify(body) })
          const first = yield* racingSpans.arm({ name: "Session.get", parent: "SessionPrompt.prompt" })
          const original = yield* send().pipe(Effect.forkChild)
          const retry = yield* Effect.gen(function* () {
            yield* awaitWithTimeout(first.reached, "the original never read its Session", "30 seconds")
            const admitted = yield* listed()
            expect(admitted).toHaveLength(1)
            const waiting = yield* racingSpans.observe({
              name: "SessionQueue.awaitPrepared",
              parent: "SessionPrompt.prompt",
            })
            const retry = yield* send().pipe(Effect.forkChild)
            yield* awaitWithTimeout(waiting, "the retry never waited on its original", "30 seconds")
            // Held where it is admitted again, once the original has failed.
            const again = yield* racingSpans.arm({ name: "Session.get", parent: "SessionPrompt.prompt" })
            yield* first.disarm
            yield* Effect.gen(function* () {
              expect(
                (yield* awaitWithTimeout(Fiber.join(original), "the original never answered", "30 seconds")).status,
              ).toBe(500)
              yield* awaitWithTimeout(again.reached, "the retry was not admitted again", "30 seconds")
              const readmitted = yield* listed()
              expect(readmitted).toHaveLength(1)
              expect(readmitted[0].id).not.toBe(admitted[0].id)
              expect(readmitted[0].input.parts).toEqual(admitted[0].input.parts)
              // A new receipt names the new item.
              if (identity.requestID)
                expect(
                  yield* requestJson<SessionQueue.Receipt>(
                    pathFor(SessionQueuePaths.receipt, { ...params, requestID: identity.requestID }),
                    { headers },
                  ),
                ).toEqual({ state: "pending", itemID: readmitted[0].id })
            }).pipe(Effect.ensuring(again.disarm))
            return retry
          }).pipe(Effect.ensuring(first.disarm))
          expect((yield* awaitWithTimeout(Fiber.join(retry), "the retry never answered", "30 seconds")).status).toBe(
            500,
          )
          expect(yield* listed()).toEqual([])
        }
      }),
    { ...options, config: { ...options.config, provider: { "absent-provider": {} } } },
    60_000,
  )

  // A send whose preparation fails takes its admission back, so nothing is
  // left pending and an exact retry is admitted again rather than following it.
  itRacing.instance(
    "a send whose preparation fails leaves nothing pending, and its exact retry is admitted again",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "unprepared" })
        const headers = { "x-opencode-directory": test.directory, "content-type": "application/json" }
        const params = { sessionID: created.id }
        const post = (path: string, body: unknown) =>
          request(pathFor(path, params), { method: "POST", headers, body: JSON.stringify(body) })
        const listed = () => requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers })
        // A variant with no model selects it on the session's model, and with
        // only an absent provider configured there is none, so preparing fails.
        const send = () =>
          post(SessionPaths.prompt, {
            requestID: "unprepared",
            agent: "build",
            variant: "high",
            parts: [{ type: "text", text: "check the build" }],
          }).pipe(Effect.timeoutOption("20 seconds"))
        expect((yield* post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
        const hold = yield* racingSpans.arm({ name: "Session.get", parent: "SessionPrompt.prompt" })
        const sent = yield* send().pipe(Effect.forkChild)
        yield* Effect.gen(function* () {
          yield* awaitWithTimeout(hold.reached, "the prompt never read its Session", "30 seconds")
          expect((yield* listed()).map((item) => item.requestID)).toEqual(["unprepared"])
        }).pipe(Effect.ensuring(hold.disarm))
        const failed = yield* Fiber.join(sent)
        expect(Option.map(failed, (response) => response.status)).toEqual(Option.some(500))
        expect(yield* listed()).toEqual([])
        const receipt = yield* request(pathFor(SessionQueuePaths.receipt, { ...params, requestID: "unprepared" }), {
          headers,
        })
        expect(receipt.status).toBe(404)
        // Admitted and failed again, where following a stale admission would
        // have waited on a prompt the held session never delivers.
        expect(Option.map(yield* send(), (response) => response.status)).toEqual(Option.some(500))
        expect(yield* listed()).toEqual([])
      }),
    { ...options, config: { ...options.config, provider: { "absent-provider": {} } } },
    60_000,
  )

  // swxtchio/swx-opencode#249: the prompt is pending before the session work
  // its delivery waits on (revert cleanup and model selection) has run.
  itRacing.instance(
    "a sent prompt is pending before the session work its delivery waits on",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const created = yield* createSession({ title: "admitted first" })
        const headers = { "x-opencode-directory": test.directory, "content-type": "application/json" }
        const params = { sessionID: created.id }
        const post = (path: string, body: unknown) =>
          request(pathFor(path, params), { method: "POST", headers, body: JSON.stringify(body) })
        // Held for review, so nothing is delivered and no provider is called.
        expect((yield* post(SessionQueuePaths.hold, { holder: "view" })).status).toBe(200)
        const hold = yield* racingSpans.arm({ name: "Session.get", parent: "SessionPrompt.prompt" })
        const sent = yield* post(SessionPaths.prompt, {
          requestID: "admitted-first",
          agent: "build",
          model: { providerID: "test", modelID: "test-model" },
          parts: [{ type: "text", text: "check the build" }],
        }).pipe(Effect.forkChild)
        yield* Effect.gen(function* () {
          yield* awaitWithTimeout(hold.reached, "the prompt never read its Session", "30 seconds")
          const items = yield* requestJson<SessionQueue.Item[]>(pathFor(SessionQueuePaths.list, params), { headers })
          expect(items.map((item) => item.requestID)).toEqual(["admitted-first"])
          expect(
            yield* requestJson<SessionQueue.Receipt>(
              pathFor(SessionQueuePaths.receipt, { ...params, requestID: "admitted-first" }),
              { headers },
            ),
          ).toEqual({ state: "pending", itemID: items[0]!.id })
        }).pipe(Effect.ensuring(hold.disarm.pipe(Effect.andThen(Fiber.interrupt(sent)))))
      }),
    options,
    60_000,
  )
})

it.live(
  "historical executable prompt and command IDs are refused over HTTP before side effects",
  () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const directory = yield* tmpdirScoped({ git: true })
      const marker = path.join(directory, "command-executions.txt")
      yield* Effect.promise(() => Bun.write(marker, "retained original\n"))
      yield* Effect.promise(() =>
        Bun.write(
          path.join(directory, "opencode.json"),
          JSON.stringify({
            ...testProviderConfig(llm.url),
            command: {
              historical: { template: `!\`printf replay >> '${marker}'; printf historical\`` },
              init: { template: `!\`printf init-replay >> '${marker}'; printf initialize\`` },
            },
          }),
        ),
      )
      const session = yield* createSession({ title: "Historical HTTP original" }).pipe(provideInstanceEffect(directory))
      const original = yield* createTextMessage(session.id, "historical").pipe(provideInstanceEffect(directory))
      const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
      const params = { sessionID: session.id }
      const before = yield* Session.Service.use((svc) => svc.messages({ sessionID: session.id })).pipe(
        provideInstanceEffect(directory),
      )
      for (const [route, body] of [
        [
          SessionPaths.prompt,
          {
            messageID: original.info.id,
            agent: "build",
            model: { providerID: "test", modelID: "test" },
            parts: [{ type: "text", text: "historical" }],
          },
        ],
        [
          SessionPaths.prompt,
          {
            messageID: original.info.id,
            agent: "build",
            model: { providerID: "test", modelID: "test" },
            system: "excluded controls",
            parts: [{ type: "text", text: "changed" }],
          },
        ],
        [
          SessionPaths.prompt,
          {
            messageID: original.info.id,
            noReply: true,
            agent: "build",
            model: { providerID: "test", modelID: "test" },
            parts: [{ type: "text", text: "historical" }],
          },
        ],
        [SessionPaths.command, { messageID: original.info.id, command: "historical", arguments: "" }],
        [SessionPaths.init, { messageID: original.info.id, providerID: "test", modelID: "test" }],
      ] as const) {
        const response = yield* request(pathFor(route, params), { method: "POST", headers, body: JSON.stringify(body) })
        expect(response.status).toBe(400)
        expect(yield* Effect.promise(() => Bun.file(marker).text())).toBe("retained original\n")
        expect(
          yield* Session.Service.use((svc) => svc.messages({ sessionID: session.id })).pipe(
            provideInstanceEffect(directory),
          ),
        ).toEqual(before)
      }
      expect(yield* llm.calls).toBe(0)
    }).pipe(Effect.provide(TestLLMServer.layer), Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node))),
  30000,
)
