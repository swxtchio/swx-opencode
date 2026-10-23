import fs from "fs/promises"
import path from "path"
import { describe, expect } from "bun:test"
import { DateTime, Effect, Equal, Hash, Layer, Schema, Stream } from "effect"
import { LLMClient, LLMError, TransportReason, type LLMClientShape, type LLMRequest } from "@opencode-ai/llm"
import { Tool } from "@opencode-ai/core/tool/tool"
import { define } from "@opencode-ai/plugin/v2/effect"
import { AgentV2 } from "@opencode-ai/core/agent"
import { Catalog } from "@opencode-ai/core/catalog"
import { Config } from "@opencode-ai/core/config"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNodePlatform } from "@opencode-ai/core/effect/app-node-platform"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { LocationServiceMap } from "@opencode-ai/core/location-services"
import { Location } from "@opencode-ai/core/location"
import { PluginV2 } from "@opencode-ai/core/plugin"
import { PluginInternal } from "@opencode-ai/core/plugin/internal"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionExecutionLocal } from "@opencode-ai/core/session/execution/local"
import { Prompt } from "@opencode-ai/core/session/prompt"
import { SessionRunnerModel } from "@opencode-ai/core/session/runner/model"
import { tmpdir } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"
import { toolDefinitions } from "./lib/tool"
import { FSUtil } from "../src/fs-util"
import { Credential } from "../src/credential"
import { Database } from "../src/database/database"
import { EventV2 } from "../src/event"
import { Global } from "../src/global"
import { ModelsDev } from "../src/models-dev"
import { Npm } from "../src/npm"
import { Project } from "../src/project"
import { Reference } from "../src/reference"
import { ToolRegistry } from "../src/tool/registry"
import { ApplicationTools } from "../src/tool/application-tools"

// The OpenCode jail's opencode.json: no `model`, one config-declared provider. The image names it with
// OPENCODE_CONFIG and the entrypoint renders the proxy values into OPENCODE_CONFIG_CONTENT.
const jailConfig = (options: { baseURL: string; apiKey: string }) => ({
  $schema: "https://opencode.ai/config.json",
  autoupdate: false,
  plugin: [],
  provider: {
    "swxtch-egress": {
      npm: "@ai-sdk/openai-compatible",
      name: "swXtch egress proxy",
      options,
      models: { "accounts/fireworks/models/deepseek-v4p1-flash": { name: "DeepSeek V4.1 Flash" } },
    },
  },
})

// The jail's runtime: its config sources, sessions executed locally, and every internal plugin booting
// against the recorded models.dev corpus, so credential-free catalog models compete with the egress
// provider exactly as they do in the jail.
const jailServices = Effect.fn(function* (root: string, replacements: LayerNode.Replacements = []) {
  const file = path.join(root, "jail", "opencode.json")
  const global = path.join(root, "global")
  const directory = path.join(root, "agent")
  yield* Effect.promise(async () => {
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.mkdir(global, { recursive: true })
    await fs.mkdir(directory, { recursive: true })
    await fs.writeFile(
      file,
      JSON.stringify(
        jailConfig({ baseURL: "{env:OPENCODE_PROXY_BASE_URL}", apiKey: "{env:OPENCODE_PROXY_CAPABILITY}" }),
      ),
    )
  })
  const corpus = yield* Effect.promise(() =>
    Bun.file(path.join(import.meta.dir, "..", "..", "opencode", "test", "tool", "fixtures", "models-api.json")).json(),
  )
  return {
    location: Location.Ref.make({ directory: AbsolutePath.make(directory) }),
    layer: AppNodeBuilder.build(
      LayerNode.group([ApplicationTools.node, Database.node, EventV2.node, LocationServiceMap.node, SessionV2.node]),
      [
        [SessionExecution.node, SessionExecutionLocal.node],
        [Global.node, Global.layerWith({ config: global })],
        [
          ModelsDev.node,
          Layer.succeed(
            ModelsDev.Service,
            ModelsDev.Service.of({ get: () => Effect.succeed(corpus), refresh: () => Effect.void }),
          ),
        ],
        [
          Config.node,
          Config.nodeWith({
            file,
            content: JSON.stringify(jailConfig({ baseURL: "https://egress.test/v1", apiKey: "capability" })),
          }),
        ],
        ...replacements,
      ],
    ),
  }
})

const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([ApplicationTools.node, Database.node, EventV2.node, LocationServiceMap.node])),
)

describe("LocationServiceMap", () => {
  it.live("reuses cached services for constructed and decoded location refs", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.scoped(
          Effect.gen(function* () {
            const locations = yield* LocationServiceMap.Service
            const directory = AbsolutePath.make(dir.path)
            const constructed = Location.Ref.make({ directory })
            const decoded = Schema.decodeUnknownSync(Location.Ref)({ directory })

            expect(constructed).toEqual({ directory, workspaceID: undefined })
            expect(decoded).toEqual(constructed)
            expect(Equal.equals(constructed, decoded)).toBe(true)
            expect(Hash.hash(constructed)).toBe(Hash.hash(decoded))
            expect(yield* locations.contextEffect(constructed)).toBe(yield* locations.contextEffect(decoded))
          }),
        ),
      ),
    ),
  )

  it.live("isolates location state while sharing location policy with catalog", () =>
    Effect.acquireRelease(
      Effect.promise(() => Promise.all([tmpdir(), tmpdir()])),
      (dirs) => Effect.promise(() => Promise.all(dirs.map((dir) => dir[Symbol.asyncDispose]())).then(() => undefined)),
    ).pipe(
      Effect.flatMap(([blocked, allowed]) =>
        Effect.gen(function* () {
          yield* (yield* ApplicationTools.Service).register({
            application_context: Tool.make({
              description: "Read application context",
              input: Schema.Struct({}),
              output: Schema.Struct({ ok: Schema.Boolean }),
              execute: () => Effect.succeed({ ok: true }),
            }),
          })
          yield* Effect.promise(() =>
            fs.writeFile(
              path.join(blocked.path, "opencode.json"),
              JSON.stringify({
                experimental: { policies: [{ effect: "deny", action: "provider.use", resource: "test" }] },
              }),
            ),
          )

          const update = (directory: string) =>
            Effect.gen(function* () {
              yield* Reference.Service
              const catalog = yield* Catalog.Service
              yield* catalog.transform((editor) => editor.provider.update(ProviderV2.ID.make("test"), () => {}))
              return {
                providers: yield* catalog.provider.all(),
                tools: yield* toolDefinitions(yield* ToolRegistry.Service),
              }
            }).pipe(
              Effect.scoped,
              Effect.provide(
                LocationServiceMap.Service.get(Location.Ref.make({ directory: AbsolutePath.make(directory) })),
              ),
            )

          const blockedState = yield* update(blocked.path)
          expect(blockedState.providers.some((provider) => provider.id === ProviderV2.ID.make("test"))).toBe(false)
          expect(blockedState.tools.map((tool) => tool.name).sort()).toEqual([
            "application_context",
            "apply_patch",
            "bash",
            "edit",
            "glob",
            "grep",
            "question",
            "read",
            "skill",
            "todowrite",
            "webfetch",
            "websearch",
            "write",
          ])
          const allowedState = yield* update(allowed.path)
          expect(allowedState.providers.some((provider) => provider.id === ProviderV2.ID.make("test"))).toBe(true)
          expect(allowedState.tools.map((tool) => tool.name).sort()).toEqual([
            "application_context",
            "apply_patch",
            "bash",
            "edit",
            "glob",
            "grep",
            "question",
            "read",
            "skill",
            "todowrite",
            "webfetch",
            "websearch",
            "write",
          ])
        }),
      ),
    ),
  )

  it.live("rejects an unavailable selected model during location model resolution", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const location = Location.Ref.make({ directory: AbsolutePath.make(dir.path) })
          yield* Effect.promise(() =>
            fs.writeFile(
              path.join(dir.path, "opencode.json"),
              JSON.stringify({
                providers: {
                  unavailable: {
                    name: "Unavailable",
                    api: { type: "native", settings: {} },
                    models: { chat: { disabled: true } },
                  },
                },
              }),
            ),
          )
          const failure = yield* SessionRunnerModel.Service.use((models) =>
            models.resolve(
              SessionV2.Info.make({
                id: SessionV2.ID.make("ses_unavailable_model"),
                projectID: ProjectV2.ID.global,
                title: "test",
                model: {
                  id: ModelV2.ID.make("chat"),
                  providerID: ProviderV2.ID.make("unavailable"),
                },
                cost: 0,
                tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
                time: { created: DateTime.makeUnsafe(0), updated: DateTime.makeUnsafe(0) },
                location,
              }),
            ),
          ).pipe(Effect.provide(LocationServiceMap.Service.get(location)), Effect.flip)

          expect(failure).toMatchObject({
            _tag: "SessionRunnerModel.ModelUnavailableError",
            providerID: "unavailable",
            modelID: "chat",
          })
        }),
      ),
    ),
  )

  it.live("resolves a location-only V2 session in the OpenCode jail to its config-declared egress model", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const jail = yield* jailServices(dir.path)
          const { available, resolved } = yield* Effect.gen(function* () {
            const catalog = yield* Catalog.Service
            const models = yield* SessionRunnerModel.Service
            const plugins = yield* PluginInternal.Service
            yield* plugins.ready
            return {
              available: yield* catalog.model.available(),
              resolved: yield* models.resolve(
                SessionV2.Info.make({
                  id: SessionV2.ID.make("ses_jail_location_only"),
                  projectID: ProjectV2.ID.global,
                  title: "test",
                  cost: 0,
                  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
                  time: { created: DateTime.makeUnsafe(0), updated: DateTime.makeUnsafe(0) },
                  location: jail.location,
                }),
              ),
            }
          }).pipe(Effect.provide(LocationServiceMap.Service.get(jail.location)), Effect.provide(jail.layer))

          // Credential-free catalog models are available alongside the egress one.
          expect(available.some((model) => model.providerID !== "swxtch-egress")).toBe(true)
          expect(resolved).toMatchObject({
            provider: "swxtch-egress",
            id: "accounts/fireworks/models/deepseek-v4p1-flash",
          })
          expect(resolved.route.endpoint).toMatchObject({ baseURL: "https://egress.test/v1" })
        }),
      ),
    ),
  )

  it.live("runs a location-only V2 turn admitted while its location boots on the jail's egress model", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const requests: LLMRequest[] = []
          // Records where the turn is sent, then fails it: the provider's answer is not under test.
          const client = Layer.succeed(
            LLMClient.Service,
            LLMClient.Service.of({
              prepare: () => Effect.die("unused"),
              stream: ((request: LLMRequest) => {
                requests.push(request)
                return Stream.fail(
                  new LLMError({
                    module: "test",
                    method: "stream",
                    reason: new TransportReason({ message: "recorded" }),
                  }),
                )
              }) as unknown as LLMClientShape["stream"],
              generate: () => Effect.die("unused"),
            }),
          )
          const jail = yield* jailServices(dir.path, [[LayerNodePlatform.llmClient, client]])

          // As the jail's adapter does: create with only a location, prompt, and drain at once, so the
          // location's services boot under the turn.
          yield* Effect.gen(function* () {
            const sessions = yield* SessionV2.Service
            const session = yield* sessions.create({ location: jail.location })
            yield* sessions.prompt({ sessionID: session.id, prompt: Prompt.make({ text: "Hello" }), resume: false })
            yield* sessions.resume(session.id).pipe(Effect.exit)
          }).pipe(Effect.provide(jail.layer))

          expect(requests).toHaveLength(1)
          expect(requests[0]?.model).toMatchObject({
            provider: "swxtch-egress",
            id: "accounts/fireworks/models/deepseek-v4p1-flash",
          })
          expect(requests[0]?.model.route.endpoint).toMatchObject({ baseURL: "https://egress.test/v1" })
        }),
      ),
    ),
  )

  it.live("installs public plugins into a location", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const plugins = yield* PluginV2.Service
          const reviewer = define({
            id: "reviewer",
            effect: (ctx) =>
              ctx.agent
                .transform((agent) => {
                  agent.update("reviewer", (item) => {
                    item.description = "Reviews code"
                    item.mode = "subagent"
                  })
                })
                .pipe(Effect.asVoid),
          })
          yield* plugins.add(PluginV2.ID.make(reviewer.id), reviewer.effect)

          expect(yield* (yield* AgentV2.Service).get(AgentV2.ID.make("reviewer"))).toMatchObject({
            description: "Reviews code",
            mode: "subagent",
          })
        }).pipe(
          Effect.scoped,
          Effect.provide(LocationServiceMap.Service.get(Location.Ref.make({ directory: AbsolutePath.make(dir.path) }))),
        ),
      ),
    ),
  )
})
