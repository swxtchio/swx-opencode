import fs from "fs/promises"
import path from "path"
import { describe, expect } from "bun:test"
import { Cause, DateTime, Deferred, Effect, Equal, Exit, Fiber, Hash, Layer, Schema, Stream } from "effect"
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
import { PermissionV2 } from "@opencode-ai/core/permission"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionMessage } from "@opencode-ai/core/session/message"
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
import { ProjectTools } from "../src/tool/project"

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

async function stageProjectTool(directory: string, marker: string, toolName: string) {
  const opencode = path.join(directory, ".opencode")
  const tools = path.join(opencode, "tools")
  const pluginPackage = path.join(opencode, "node_modules", "@opencode-ai", "plugin")
  await fs.mkdir(tools, { recursive: true })
  await fs.mkdir(path.dirname(pluginPackage), { recursive: true })
  await fs.symlink(path.resolve(import.meta.dir, "../../plugin"), pluginPackage, "dir")
  await stageProjectSdk(opencode)
  await Bun.write(
    path.join(tools, `${toolName}.js`),
    [
      'import { tool } from "@opencode-ai/plugin"',
      "export default tool({",
      `  description: 'Tool for ${marker}',`,
      "  args: {},",
      `  async execute(_args, context) { return { output: JSON.stringify({ marker: '${marker}', directory: context.directory, worktree: context.worktree }) } },`,
      "})",
      "",
    ].join("\n"),
  )
  await Bun.write(
    path.join(tools, `${toolName}_broken.js`),
    "import { missing } from '@project-tool-dependency-that-is-not-installed'; export default missing",
  )
}

async function stageProjectSdk(opencode: string) {
  const plugin = (await Bun.file(path.resolve(import.meta.dir, "../../plugin/package.json")).json()) as {
    version: string
  }
  const name = "location-layer-project-tools"
  const dependencies = { "@opencode-ai/plugin": plugin.version }
  await Bun.write(
    path.join(opencode, "package.json"),
    JSON.stringify({ name, private: true, dependencies }),
  )
  await Bun.write(
    path.join(opencode, "package-lock.json"),
    JSON.stringify({
      name,
      lockfileVersion: 3,
      packages: {
        "": { name, dependencies },
        "node_modules/@opencode-ai/plugin": { version: plugin.version },
      },
    }),
  )
}

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

          // A first prompt also sends a title request, which falls back to the turn's model without a small model.
          const isTitle = (request: LLMRequest) =>
            request.messages.some((message) =>
              message.content.some(
                (content) =>
                  content.type === "text" && content.text.startsWith("Generate a title for this conversation"),
              ),
            )
          expect(requests.filter((request) => !isTitle(request))).toHaveLength(1)
          expect(requests.filter(isTitle)).toHaveLength(1)
          for (const request of requests) {
            expect(request.model).toMatchObject({
              provider: "swxtch-egress",
              id: "accounts/fireworks/models/deepseek-v4p1-flash",
            })
            expect(request.model.route.endpoint).toMatchObject({ baseURL: "https://egress.test/v1" })
          }
        }),
      ),
    ),
  )

  it.live("isolates project tools per Location and rediscovers new modules after Location recreation", () =>
    Effect.acquireRelease(
      Effect.promise(() => Promise.all([tmpdir(), tmpdir(), tmpdir()])),
      (dirs) => Effect.promise(() => Promise.all(dirs.map((dir) => dir[Symbol.asyncDispose]())).then(() => undefined)),
    ).pipe(
      Effect.flatMap(([firstDir, secondDir, emptyDir]) =>
        Effect.promise(async () => {
          const name = `location_probe_${path.basename(firstDir.path).replaceAll("-", "_")}`
          await stageProjectTool(firstDir.path, "first", name)
          await stageProjectTool(secondDir.path, "second", name)
          return { name, lateName: `${name}_late` }
        }).pipe(
          Effect.flatMap(({ name, lateName }) =>
            Effect.gen(function* () {
              const locations = yield* LocationServiceMap.Service
              const materialize = (directory: string, callID: string, toolName = name) =>
                Effect.gen(function* () {
                  const ref = Location.Ref.make({ directory: AbsolutePath.make(directory) })
                  return yield* Effect.gen(function* () {
                    const projectTools = yield* ProjectTools.Service
                    yield* projectTools.ready
                    const location = yield* Location.Service
                    const registry = yield* ToolRegistry.Service
                    const materialized = yield* registry.materialize()
                    const definition = materialized.definitions.find((item) => item.name === toolName)
                    const settlement = definition
                      ? yield* materialized.settle({
                          sessionID: SessionV2.ID.make(`ses_${callID}`),
                          agent: AgentV2.ID.make("build"),
                          assistantMessageID: SessionMessage.ID.make(`msg_${callID}`),
                          call: { type: "tool-call", id: callID, name: toolName, input: {} },
                        })
                      : undefined
                    return {
                      location,
                      names: materialized.definitions.map((definition) => definition.name),
                      settlement,
                    }
                  }).pipe(Effect.provide(locations.get(ref)))
                })

              const first = yield* materialize(firstDir.path, "location_first")
              const second = yield* materialize(secondDir.path, "location_second")
              const absent = yield* materialize(emptyDir.path, "location_empty")
              expect(first.names).toContain(name)
              expect(second.names).toContain(name)
              expect(first.names).toContain("read")
              expect(first.names).not.toContain(`${name}_broken`)
              expect(absent.names).not.toContain(name)
              expect(absent.names).toContain("read")
              expect(JSON.parse((first.settlement?.output?.structured as { output: string }).output)).toEqual({
                marker: "first",
                directory: first.location.directory,
                worktree: first.location.project.directory,
              })
              expect(JSON.parse((second.settlement?.output?.structured as { output: string }).output)).toEqual({
                marker: "second",
                directory: second.location.directory,
                worktree: second.location.project.directory,
              })

              yield* Effect.promise(() =>
                Bun.write(
                  path.join(firstDir.path, ".opencode", "tools", `${lateName}.js`),
                  [
                    'import { tool } from "@opencode-ai/plugin"',
                    `export default tool({ description: 'Discovered after recreation', args: {}, execute: async () => '${lateName}' })`,
                    "",
                  ].join("\n"),
                ),
              )
              expect((yield* materialize(firstDir.path, "location_first_before_recreate")).names).not.toContain(
                lateName,
              )
              const ref = Location.Ref.make({ directory: AbsolutePath.make(firstDir.path) })
              yield* locations.invalidate(ref)
              const recreated = yield* materialize(firstDir.path, "location_first_after_recreate", lateName)
              expect(recreated.names).toContain(lateName)
              const late = recreated.settlement
              expect(late?.result).toMatchObject({ type: "text", value: lateName })
            }),
          ),
        ),
      ),
    ),
  )

  it.live("cancels an outstanding project-tool permission ask with its tool settlement", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((dir) =>
        Effect.gen(function* () {
          const jail = yield* jailServices(dir.path)
          const opencode = path.join(dir.path, "agent", ".opencode")
          const tools = path.join(opencode, "tools")
          const aborted = path.join(tools, "aborted")
          const pluginPackage = path.join(opencode, "node_modules", "@opencode-ai", "plugin")
          yield* Effect.promise(async () => {
            await fs.mkdir(tools, { recursive: true })
            await fs.mkdir(path.dirname(pluginPackage), { recursive: true })
            await fs.symlink(path.resolve(import.meta.dir, "../../plugin"), pluginPackage, "dir")
            await stageProjectSdk(opencode)
            await Bun.write(
              path.join(tools, "ask_permission.js"),
              [
                'import { writeFile } from "node:fs/promises"',
                'import { tool } from "@opencode-ai/plugin"',
                "export default tool({",
                "  description: 'Wait for project permission before resuming',",
                "  args: {},",
                "  async execute(_args, context) {",
                "    context.abort.addEventListener('abort', () => { void writeFile(new URL('./aborted', import.meta.url), 'aborted') })",
                "    await context.ask({ permission: 'project.test', patterns: ['workspace'], always: [], metadata: {} })",
                "    await writeFile(new URL('./continued', import.meta.url), 'resumed')",
                "    return 'resumed'",
                "  },",
                "})",
                "",
              ].join("\n"),
            )
          })
          yield* Effect.gen(function* () {
            const agentID = AgentV2.ID.make("build")
            const session = yield* (yield* SessionV2.Service).create({
              location: jail.location,
              agent: agentID,
            })
            const locations = yield* LocationServiceMap.Service
            const events = yield* EventV2.Service
            const asked = yield* Deferred.make<PermissionV2.Request>()
            const unsubscribe = yield* events.listen((event) =>
              event.type === PermissionV2.Event.Asked.type
                ? Deferred.succeed(asked, event.data as PermissionV2.Request).pipe(Effect.asVoid)
                : Effect.void,
            )
            yield* Effect.addFinalizer(() => unsubscribe)
            yield* Effect.gen(function* () {
              const agents = yield* AgentV2.Service
              yield* agents.transform((editor) =>
                editor.update(agentID, (agent) => {
                  agent.permissions = [{ action: "project.test", resource: "workspace", effect: "ask" }]
                }),
              )
              const projectTools = yield* ProjectTools.Service
              yield* projectTools.ready
              const registry = yield* ToolRegistry.Service
              const materialized = yield* registry.materialize()
              const fiber = yield* materialized
                .settle({
                  sessionID: session.id,
                  agent: agentID,
                  assistantMessageID: SessionMessage.ID.make("msg_permission_cancel"),
                  call: { type: "tool-call", id: "call-permission-cancel", name: "ask_permission", input: {} },
                })
                .pipe(Effect.forkChild)
              const request = yield* Deferred.await(asked)
              const permissions = yield* PermissionV2.Service
              expect(yield* permissions.list()).toEqual([request])
              yield* Fiber.interrupt(fiber)
              const settlement = yield* Fiber.await(fiber)
              expect(Exit.isFailure(settlement) && Cause.hasInterrupts(settlement.cause)).toBe(true)
              let signalAborted = yield* Effect.promise(() => Bun.file(aborted).exists())
              for (let attempt = 0; attempt < 100 && !signalAborted; attempt++) {
                yield* Effect.sleep("5 millis")
                signalAborted = yield* Effect.promise(() => Bun.file(aborted).exists())
              }
              expect(signalAborted).toBe(true)
              let pending = yield* permissions.list()
              for (let attempt = 0; attempt < 100 && pending.length > 0; attempt++) {
                yield* Effect.sleep("5 millis")
                pending = yield* permissions.list()
              }
              expect(pending).toEqual([])
              expect(
                Exit.isFailure(yield* permissions.reply({ requestID: request.id, reply: "once" }).pipe(Effect.exit)),
              ).toBe(true)
              expect(yield* Effect.promise(() => Bun.file(path.join(tools, "continued")).exists())).toBe(false)
            }).pipe(Effect.provide(locations.get(jail.location)))
          }).pipe(Effect.provide(jail.layer))
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
