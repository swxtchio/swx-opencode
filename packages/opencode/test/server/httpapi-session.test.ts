import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { afterEach, describe, expect, test } from "bun:test"
import { NodeHttpServer, NodeServices } from "@effect/platform-node"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { mkdir } from "node:fs/promises"
import path from "node:path"
import { Cause, Config, Deferred, Effect, Exit, Fiber, Layer, Queue, Schema, Stream, Tracer } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse, HttpRouter, HttpServer } from "effect/unstable/http"
import { Sse } from "effect/unstable/encoding"
import { layerWebSocketConstructorGlobal } from "effect/unstable/socket/Socket"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Flag } from "@opencode-ai/core/flag/flag"
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
import { SessionPaths } from "../../src/server/routes/instance/httpapi/groups/session"
import { SessionQueuePaths } from "../../src/server/routes/instance/httpapi/groups/session-queue"
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
          Array.isArray(input?.messages) ? input.messages.at(-1) : undefined
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
                  return JSON.stringify(messages.at(-1)) === JSON.stringify({ role: "user", content: text })
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
          Array.isArray(input.messages) ? input.messages.at(-1) : undefined,
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
        // The start prompt's own admission and promotion publish first; skip to the held one.
        yield* nextQueueUpdate
        yield* nextQueueUpdate
        yield* request(pathFor(SessionPaths.promptAsync, params), {
          method: "POST",
          headers,
          body: JSON.stringify({ agent: "build", model, delivery: "queue", parts: [{ type: "text", text: "held" }] }),
        })
        const admittedList = yield* nextQueueUpdate
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
