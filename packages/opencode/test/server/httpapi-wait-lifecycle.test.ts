import { afterEach, expect } from "bun:test"
import { NodeHttpServer, NodeServices } from "@effect/platform-node"
import { Config, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse, HttpRouter, HttpServer } from "effect/unstable/http"
import { layerWebSocketConstructorGlobal } from "effect/unstable/socket/Socket"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Global } from "@opencode-ai/core/global"
import { Database } from "@opencode-ai/core/database/database"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { SessionV2 } from "@opencode-ai/core/session"
import { InstanceBootstrap } from "../../src/project/bootstrap-service"
import { InstanceStore } from "../../src/project/instance-store"
import { Project } from "../../src/project/project"
import { Workspace } from "../../src/control-plane/workspace"
import { Session } from "../../src/session/session"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, tmpdirScoped } from "../fixture/fixture"
import { TestLLMServer } from "../lib/llm-server"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"
import { open, stat } from "node:fs/promises"
import path from "node:path"
import { setTimeout } from "node:timers/promises"

const appLayer = AppNodeBuilder.build(
  LayerNode.group([InstanceStore.node, Project.node, Session.node, Workspace.node, Database.node, Ripgrep.node]),
  [[InstanceStore.bootstrapNode, Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void }))]],
)
const servedRoutes: Layer.Layer<never, Config.ConfigError, HttpServer.HttpServer> = HttpRouter.serve(HttpApiApp.routes, {
  disableListenLog: true,
  disableLogger: true,
})
const it = testEffect(Layer.mergeAll(appLayer, servedRoutes.pipe(
  Layer.provide(layerWebSocketConstructorGlobal),
  Layer.provideMerge(NodeHttpServer.layerTest),
  Layer.provideMerge(NodeServices.layer),
)))

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

function request(path: string, init?: RequestInit) {
  const url = new URL(path, "http://localhost")
  return HttpClientRequest.fromWeb(new Request(url, init)).pipe(HttpClientRequest.setUrl(url.pathname), HttpClient.execute)
}

function json(response: HttpClientResponse.HttpClientResponse) {
  if (response.status !== 200) return response.text.pipe(Effect.flatMap((text) => Effect.die(new Error(text))))
  return response.json
}

function requestJson(path: string, init?: RequestInit) {
  return request(path, init).pipe(Effect.flatMap(json))
}

it.instance(
  "logs producer wait registration and resolution for a held HTTP wait",
  () => Effect.gen(function* () {
    const llm = yield* TestLLMServer
    const directory = yield* tmpdirScoped({
      git: true,
      init: (directory) => Effect.promise(() => Bun.write(path.join(directory, "opencode.json"), JSON.stringify({
        model: "test/test-model",
        providers: {
          test: {
            name: "Test",
            api: { type: "aisdk", package: "@ai-sdk/openai-compatible", url: llm.url, settings: {} },
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
      }))).pipe(Effect.asVoid),
    })
    const session = Schema.decodeUnknownSync(Schema.Struct({ data: Schema.Struct({ id: Schema.String }) }))(
      yield* requestJson("/api/session", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: SessionV2.ID.make("ses_wait_registration_http"), model: { id: "test-model", providerID: "test" }, location: { directory } }),
      }),
    ).data
    yield* pollWithTimeout(requestJson("/api/model", { headers: { "x-opencode-directory": directory } }).pipe(
      Effect.map(Schema.decodeUnknownSync(Schema.Struct({ data: Schema.Array(Schema.Struct({ id: Schema.String, providerID: Schema.String })) }))),
      Effect.map(({ data }) => data.find((model) => model.providerID === "test" && model.id === "test-model")),
    ), "the configured V2 model did not become available", "10 seconds")
    let releaseFinal = () => {}
    const finalGate = new Promise<void>((resolve) => (releaseFinal = resolve))
    yield* Effect.addFinalizer(() => Effect.sync(releaseFinal))
    yield* llm.hold("Wait registration was observed", finalGate)

    const headers = { "x-opencode-directory": directory, "content-type": "application/json" }
    const admission = Schema.decodeUnknownSync(Schema.Struct({ data: Schema.Struct({ id: Schema.String, admittedSeq: Schema.Number }) }))(
      yield* requestJson(`/api/session/${session.id}/prompt`, {
        method: "POST", headers, body: JSON.stringify({ prompt: { text: "Hold a wait registration" } }),
      }),
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
    const waitLog = (event: string, waitID?: string) => Effect.promise(async () => {
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
        const match = logText.split(/\r?\n/).find((line) =>
          line.includes(`event=${event}`) && line.includes(`session_id=${session.id}`) &&
          line.includes(`admitted_seq=${admission.data.admittedSeq}`) && line.includes(`message_id=${admission.data.id}`) &&
          (waitID === undefined || line.includes(`wait_id=${waitID}`)),
        )
        if (match) return match
        await setTimeout(25)
      }
      throw new Error(`timed out waiting for producer wait log ${event}`)
    })
    const registration = yield* waitLog("session.wait.registered").pipe(
      Effect.onExit((exit) => Exit.isFailure(exit) ? Effect.sync(releaseFinal) : Effect.void),
    )
    const waitID = /(?:^|\s)wait_id=([^\s]+)/.exec(registration)?.[1]
    if (!waitID) throw new Error(`wait registration had no server-generated wait ID: ${registration}`)
    expect(yield* Deferred.isDone(waitResponse)).toBe(false)
    releaseFinal()
    const response = yield* awaitWithTimeout(Deferred.await(waitResponse), "HTTP wait did not return after releasing the provider", "10 seconds")
    expect(response.status).toBe(200)
    const outcome = Schema.decodeUnknownSync(Schema.Struct({ data: SessionV2.WaitResult }))(yield* json(response))
    expect(outcome.data).toMatchObject({ type: "completed", admittedSeq: admission.data.admittedSeq })
    yield* Fiber.join(waiter)
    const resolution = yield* waitLog("session.wait.resolved", waitID)
    expect(resolution).toContain("outcome=completed")
  }).pipe(Effect.provide(TestLLMServer.layer)),
  { git: true, config: { formatter: false, lsp: false } },
  30_000,
)
