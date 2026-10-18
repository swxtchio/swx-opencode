import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { NodeHttpServer, NodeServices } from "@effect/platform-node"
import { Flag } from "@opencode-ai/core/flag/flag"
import { describe, expect } from "bun:test"
import { Config, ConfigProvider, Context, Duration, Effect, FileSystem, Layer, Path, Schedule } from "effect"
import { HttpClient, HttpClientRequest, HttpRouter, HttpServer } from "effect/unstable/http"
import * as Socket from "effect/unstable/socket/Socket"
import { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { ControlPaths } from "../../src/server/routes/instance/httpapi/groups/control"
import { InstancePaths } from "../../src/server/routes/instance/httpapi/groups/instance"
import { SessionPaths } from "../../src/server/routes/instance/httpapi/groups/session"
import { ProjectV2 } from "@opencode-ai/core/project"
import { QuestionID } from "../../src/question/schema"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { HEADER as FenceHeader } from "../../src/server/shared/fence"
import { resetDatabase } from "../fixture/db"
import { tmpdirScoped } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { AskEmit } from "../../src/ask/emit"
import { ServerAuth } from "../../src/server/auth"
import { writeReceiver } from "../lib/ask-receiver"
import { deadline } from "../lib/deadline"
import { TestLLMServer } from "../lib/llm-server"
import { testProviderConfig } from "../lib/test-provider"

// Flip the experimental workspaces flag so EventV2.run actually writes to
// EventSequenceTable (the source of truth the fence middleware reads). Reset
// the database around the test so per-instance state does not leak between
// runs. resetDatabase() already calls disposeAllInstances(), so we don't
// repeat it.
const testStateLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const originalWorkspaces = Flag.OPENCODE_EXPERIMENTAL_WORKSPACES
    Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
    yield* Effect.promise(() => resetDatabase())
    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = originalWorkspaces
        await resetDatabase()
      }),
    )
  }),
)

// Mount the production HttpApi route tree on a real Node HTTP server bound to
// 127.0.0.1:0 and a fetch-based HttpClient that prepends the server URL. This
// keeps the test wired directly through the same route layer production uses.
const servedRoutes: Layer.Layer<never, Config.ConfigError, HttpServer.HttpServer> = HttpRouter.serve(
  HttpApiApp.routes,
  { disableListenLog: true, disableLogger: true },
)

const httpApiServerLayer = servedRoutes.pipe(
  Layer.provide(Socket.layerWebSocketConstructorGlobal),
  Layer.provideMerge(NodeHttpServer.layerTest),
  Layer.provideMerge(NodeServices.layer),
)

const it = testEffect(Layer.mergeAll(testStateLayer, httpApiServerLayer))
const handlerContext = Context.empty() as Context.Context<unknown>

const directoryHeader = (dir: string) => HttpClientRequest.setHeader("x-opencode-directory", dir)

describe("instance HttpApi", () => {
  it.live("serves the OpenAPI document", () =>
    Effect.gen(function* () {
      const response = yield* HttpClient.get("/doc")

      expect(response.status).toBe(200)
      expect(response.headers["content-type"]).toContain("application/json")
      expect(yield* response.json).toMatchObject({
        openapi: expect.any(String),
        info: expect.any(Object),
        paths: expect.objectContaining({
          "/global/health": expect.any(Object),
          "/session": expect.any(Object),
        }),
      })
    }),
  )

  it.live("emits a sync fence header for fixed-workspace mutations", () =>
    Effect.gen(function* () {
      const originalWorkspaceID = Flag.OPENCODE_WORKSPACE_ID
      Flag.OPENCODE_WORKSPACE_ID = WorkspaceV2.ID.ascending()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          Flag.OPENCODE_WORKSPACE_ID = originalWorkspaceID
        }),
      )

      const dir = yield* tmpdirScoped({ git: true })
      const response = yield* HttpClientRequest.post(SessionPaths.create).pipe(
        directoryHeader(dir),
        HttpClientRequest.bodyJson({ title: "fenced" }),
        Effect.flatMap(HttpClient.execute),
      )

      expect(response.status).toBe(200)
      expect(JSON.parse(response.headers[FenceHeader] ?? "{}")).not.toEqual({})
    }),
  )

  it.live("does not emit sync fence headers for fixed-workspace reads or no-op mutations", () =>
    Effect.gen(function* () {
      const originalWorkspaceID = Flag.OPENCODE_WORKSPACE_ID
      Flag.OPENCODE_WORKSPACE_ID = WorkspaceV2.ID.ascending()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          Flag.OPENCODE_WORKSPACE_ID = originalWorkspaceID
        }),
      )

      const dir = yield* tmpdirScoped({ git: true })
      const read = yield* HttpClientRequest.get(InstancePaths.path).pipe(directoryHeader(dir), HttpClient.execute)
      const log = yield* HttpClientRequest.post(ControlPaths.log).pipe(
        directoryHeader(dir),
        HttpClientRequest.bodyJson({ service: "fence-test", level: "info", message: "noop" }),
        Effect.flatMap(HttpClient.execute),
      )

      expect(read.status).toBe(200)
      expect(read.headers[FenceHeader]).toBeUndefined()
      expect(log.status).toBe(200)
      expect(log.headers[FenceHeader]).toBeUndefined()
    }),
  )

  it.live("rejects malformed permission and question request ids", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const request = (path: string, init?: RequestInit) =>
        Effect.promise(() =>
          HttpApiApp.webHandler().handler(
            new Request(`http://localhost${path}`, {
              ...init,
              headers: { "x-opencode-directory": dir, "content-type": "application/json", ...init?.headers },
            }),
            handlerContext,
          ),
        )
      const [permission, questionReply, questionReject] = yield* Effect.all(
        [
          request("/permission/invalid-permission-id/reply", {
            method: "POST",
            body: JSON.stringify({ reply: "once" }),
          }),
          request("/question/invalid-question-id/reply", {
            method: "POST",
            body: JSON.stringify({ answers: [["Yes"]] }),
          }),
          request("/question/invalid-question-id/reject", { method: "POST" }),
        ],
        { concurrency: "unbounded" },
      )

      expect(permission.status).toBe(400)
      expect(questionReply.status).toBe(400)
      expect(questionReject.status).toBe(400)
    }),
  )

  it.live("returns typed not found bodies for missing permission and question requests", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const request = (path: string, init?: RequestInit) =>
        Effect.promise(() =>
          HttpApiApp.webHandler().handler(
            new Request(`http://localhost${path}`, {
              ...init,
              headers: { "x-opencode-directory": dir, "content-type": "application/json", ...init?.headers },
            }),
            handlerContext,
          ),
        )
      const permissionID = PermissionV1.ID.ascending()
      const questionReplyID = QuestionID.ascending()
      const questionRejectID = QuestionID.ascending()
      const [permission, questionReply, questionReject] = yield* Effect.all(
        [
          request(`/permission/${permissionID}/reply`, {
            method: "POST",
            body: JSON.stringify({ reply: "once" }),
          }),
          request(`/question/${questionReplyID}/reply`, {
            method: "POST",
            body: JSON.stringify({ answers: [["Yes"]] }),
          }),
          request(`/question/${questionRejectID}/reject`, { method: "POST" }),
        ],
        { concurrency: "unbounded" },
      )

      expect(permission.status).toBe(404)
      expect(yield* Effect.promise(() => permission.json())).toEqual({
        _tag: "PermissionNotFoundError",
        requestID: permissionID,
        message: `Permission request not found: ${permissionID}`,
      })
      expect(questionReply.status).toBe(404)
      expect(yield* Effect.promise(() => questionReply.json())).toEqual({
        _tag: "QuestionNotFoundError",
        requestID: questionReplyID,
        message: `Question request not found: ${questionReplyID}`,
      })
      expect(questionReject.status).toBe(404)
      expect(yield* Effect.promise(() => questionReject.json())).toEqual({
        _tag: "QuestionNotFoundError",
        requestID: questionRejectID,
        message: `Question request not found: ${questionRejectID}`,
      })
    }),
  )

  it.live("returns typed not found bodies for missing projects", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const projectID = ProjectV2.ID.make("project_missing")
      const response = yield* Effect.promise(() =>
        HttpApiApp.webHandler().handler(
          new Request(`http://localhost/project/${projectID}`, {
            method: "PATCH",
            headers: { "x-opencode-directory": dir, "content-type": "application/json" },
            body: JSON.stringify({ name: "Missing" }),
          }),
          handlerContext,
        ),
      )

      expect(response.status).toBe(404)
      expect(yield* Effect.promise(() => response.json())).toEqual({
        _tag: "ProjectNotFoundError",
        projectID,
        message: `Project not found: ${projectID}`,
      })
    }),
  )

  it.live("serves path and VCS read endpoints", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      yield* fs.writeFileString(path.join(dir, "changed.txt"), "hello")

      const [paths, vcs, diff] = yield* Effect.all(
        [
          HttpClientRequest.get(InstancePaths.path).pipe(directoryHeader(dir), HttpClient.execute),
          HttpClientRequest.get(InstancePaths.vcs).pipe(directoryHeader(dir), HttpClient.execute),
          HttpClientRequest.get(InstancePaths.vcsDiff).pipe(
            HttpClientRequest.setUrlParam("mode", "git"),
            directoryHeader(dir),
            HttpClient.execute,
          ),
        ],
        { concurrency: "unbounded" },
      )

      expect(paths.status).toBe(200)
      expect(yield* paths.json).toMatchObject({ directory: dir, worktree: dir })

      expect(vcs.status).toBe(200)
      expect(yield* vcs.json).toMatchObject({ branch: expect.any(String) })

      expect(diff.status).toBe(200)
      expect(yield* diff.json).toContainEqual(
        expect.objectContaining({ file: "changed.txt", additions: 1, status: "added" }),
      )
    }),
  )

  it.live(
    "answers an emitted question only when authorized and in its own directory, and tells unknown from duplicate",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const supervisor = yield* supervised("secret")
        const dir = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        const other = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
        yield* llm.tool("question", {
          questions: [{ question: "Proceed?", header: "Go", options: [{ label: "Yes", description: "Go on" }] }],
        })
        yield* llm.text("done")

        const sessionID = yield* supervisor.start(dir)
        const [emitted] = yield* supervisor.receiver.read(1)
        const listed = supervisor
          .call("GET", "/question", dir)
          .pipe(Effect.map((result) => (result.body as Array<{ id: string }>).map((item) => item.id)))
        const answer = (body: unknown, directory = dir, id: string = emitted.id, authorized = true) =>
          supervisor.call("POST", `/question/${id}/answer`, directory, body, authorized)

        expect((yield* answer({ answers: [["Yes"]] }, dir, emitted.id, false)).status).toBe(401)
        expect(yield* listed).toEqual([emitted.id])

        const elsewhere = yield* answer({ answers: [["Yes"]] }, other)
        expect(elsewhere.status).toBe(404)
        expect(elsewhere.body).toMatchObject({ _tag: "QuestionNotFoundError", requestID: emitted.id })
        expect(yield* listed).toEqual([emitted.id])

        const unknownID = QuestionID.ascending()
        const unknown = yield* answer({ answers: [["Yes"]] }, dir, unknownID)
        expect(unknown.status).toBe(404)
        expect(unknown.body).toMatchObject({ _tag: "QuestionNotFoundError", requestID: unknownID })
        expect((yield* answer({ answers: [["Yes"]] }, dir, "not-a-question")).status).toBe(400)
        expect(yield* listed).toEqual([emitted.id])

        const settled = yield* answer({ answers: [["Yes"]] })
        expect(settled.status).toBe(200)
        expect(settled.body).toEqual({
          requestID: emitted.id,
          sessionID,
          disposition: "settled",
          decision: { type: "answered", answers: [["Yes"]] },
          recovered: false,
        })
        const duplicate = yield* answer({ reject: true })
        expect(duplicate.status).toBe(200)
        expect(duplicate.body).toMatchObject({
          disposition: "duplicate",
          decision: { type: "answered", answers: [["Yes"]] },
        })
        const human = yield* supervisor.call("POST", `/question/${emitted.id}/reply`, dir, { answers: [["No"]] })
        expect(human.status).toBe(404)
        expect(human.body).toMatchObject({
          requestID: emitted.id,
          message: `Question request already settled: ${emitted.id}`,
        })

        const part = yield* supervisor.toolPart(dir, sessionID, emitted.origin.callID, "completed")
        expect(part.state.metadata.answers).toEqual([["Yes"]])
        expect(yield* listed).toEqual([])
      }).pipe(Effect.provide(TestLLMServer.layer)),
    60_000,
  )

  it.live("answers an emitted permission request through its route with the permission's own reply", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const supervisor = yield* supervised("secret")
      const dir = yield* tmpdirScoped({
        git: true,
        config: { ...testProviderConfig(llm.url), permission: { bash: "ask" } },
      })
      const other = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
      yield* llm.tool("bash", { command: "echo supervised", description: "Echo" })
      yield* llm.text("done")

      const sessionID = yield* supervisor.start(dir)
      const [emitted] = yield* supervisor.receiver.read(1)
      expect(emitted.kind).toBe("permission")
      expect(emitted.permission).toMatchObject({ permission: "bash", patterns: ["echo supervised"] })
      const answer = (body: unknown, directory = dir) =>
        supervisor.call("POST", `/permission/${emitted.id}/answer`, directory, body)

      expect((yield* answer({ reply: "once" }, other)).status).toBe(404)
      const unknownID = PermissionV1.ID.ascending()
      const unknown = yield* supervisor.call("POST", `/permission/${unknownID}/answer`, dir, { reply: "once" })
      expect(unknown.body).toMatchObject({ _tag: "PermissionNotFoundError", requestID: unknownID })

      expect((yield* answer({ reply: "once" })).body).toMatchObject({
        disposition: "settled",
        decision: { type: "replied", reply: "once" },
        recovered: false,
      })
      expect((yield* answer({ reply: "reject" })).body).toMatchObject({
        disposition: "duplicate",
        decision: { type: "replied", reply: "once" },
      })
      const part = yield* supervisor.toolPart(dir, sessionID, emitted.origin.callID, "completed")
      expect(part.state.output).toContain("supervised")
    }).pipe(Effect.provide(TestLLMServer.layer)),
    60_000,
  )
})

// A production route tree with configured authorization, emitting asks to a receiver in this process.
const supervised = Effect.fn("supervised")(function* (password: string) {
  const home = yield* tmpdirScoped()
  const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
  AskEmit.configure({ command: receiver.path })
  yield* Effect.addFinalizer(() => Effect.sync(() => AskEmit.configure(undefined)))
  const handler = HttpRouter.toWebHandler(
    HttpApiApp.routes.pipe(
      Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENCODE_SERVER_PASSWORD: password }))),
    ),
    { disableLogger: true },
  )
  yield* Effect.addFinalizer(() => Effect.promise(() => handler.dispose()))
  const authorization = ServerAuth.header({ username: "opencode", password }) ?? ""
  const call = (method: string, path: string, directory: string, body?: unknown, authorized = true) =>
    Effect.promise(async () => {
      const response = await handler.handler(
        new Request(`http://localhost${path}`, {
          method,
          headers: {
            "x-opencode-directory": directory,
            "content-type": "application/json",
            ...(authorized ? { authorization } : {}),
          },
          body: body === undefined ? undefined : JSON.stringify(body),
        }),
        HttpApiApp.context,
      )
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return { status: response.status, body: (await response.json().catch(() => undefined)) as any }
    })
  const start = Effect.fn("supervised.start")(function* (directory: string) {
    const session = yield* call("POST", SessionPaths.create, directory, { title: "supervised" })
    const prompt = yield* call("POST", `/session/${session.body.id}/prompt_async`, directory, {
      model: { providerID: "test", modelID: "test-model" },
      parts: [{ type: "text", text: "ask" }],
    })
    expect(prompt.status).toBeLessThan(300)
    return session.body.id as string
  })
  const toolPart = (directory: string, sessionID: string, callID: string, status: string) =>
    call("GET", `/session/${sessionID}/message`, directory).pipe(
      Effect.map((result) =>
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (result.body as Array<{ parts: any[] }>)
          .flatMap((message) => message.parts)
          .find((part) => part.callID === callID),
      ),
      Effect.filterOrFail(
        (part) => part?.state?.status === status,
        () => new Error(`tool call ${callID} is not ${status}`),
      ),
      Effect.retry(Schedule.spaced("50 millis")),
      Effect.timeout(Duration.millis(deadline(20_000))),
    )
  return { receiver, call, start, toolPart }
})
