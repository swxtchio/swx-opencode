import { describe, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Tool } from "@opencode-ai/core/tool/tool"
import { AgentV2 } from "@opencode-ai/core/agent"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ApplicationTools } from "@opencode-ai/core/tool/application-tools"
import { Config } from "@opencode-ai/core/config"
import { Global } from "@opencode-ai/core/global"
import { Location } from "@opencode-ai/core/location"
import { Npm } from "@opencode-ai/core/npm"
import { PermissionV2 } from "@opencode-ai/core/permission"
import { ProjectTools } from "@opencode-ai/core/tool/project"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionV2 } from "@opencode-ai/core/session"
import { ToolOutputStore } from "@opencode-ai/core/tool-output-store"
import { ToolRegistry } from "@opencode-ai/core/tool/registry"
import { executeTool, settleTool, toolDefinitions } from "./lib/tool"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, SchemaGetter, SchemaIssue, Scope } from "effect"
import { testEffect } from "./lib/effect"
import { location } from "./fixture/location"
import { tmpdir } from "./fixture/tmpdir"

const bounds: ToolOutputStore.BoundInput[] = []
const retentionFailure = new ToolOutputStore.StorageError({ operation: "write", cause: new Error("disk full") })
const outputStore = Layer.mock(ToolOutputStore.Service, {
  bound: (input) => {
    if (input.toolCallID === "call-retention-failure") return Effect.fail(retentionFailure)
    return Effect.sync(() => bounds.push(input)).pipe(
      Effect.as(
        input.toolCallID === "call-bounded"
          ? {
              output: { structured: {}, content: [{ type: "text" as const, text: "bounded reference" }] },
              outputPaths: ["/managed/generic"],
            }
          : { output: input.output, outputPaths: [] },
      ),
    )
  },
})
const registryLayer = AppNodeBuilder.build(ToolRegistry.node, [[ToolOutputStore.node, outputStore]])
const it = testEffect(registryLayer)
const integrated = testEffect(
  AppNodeBuilder.build(LayerNode.group([ApplicationTools.node, ToolRegistry.node]), [
    [ToolOutputStore.node, outputStore],
  ]),
)
const projectIt = testEffect(Layer.empty)
const identity = {
  agent: AgentV2.ID.make("build"),
  assistantMessageID: SessionMessage.ID.make("msg_registry"),
}
const sessionID = SessionV2.ID.make("ses_registry")
const call = (name: string, id = `call-${name}`): ToolRegistry.ExecuteInput => ({
  sessionID,
  ...identity,
  call: { type: "tool-call", id, name, input: { text: name } },
})

const make = (permission?: string) => {
  const tool = Tool.make({
    description: "Echo text",
    input: Schema.Struct({ text: Schema.String }),
    output: Schema.Struct({ text: Schema.String }),
    execute: ({ text }) => Effect.succeed({ text }),
    toModelOutput: ({ output }) => [{ type: "text", text: output.text }],
  })
  return permission ? Tool.withPermission(tool, permission) : tool
}

const projectToolLayer = (
  directory: string,
  requests: PermissionV2.AssertInput[],
  install: Npm.Interface["install"] = () => Effect.void,
) => {
  const projectDirectory = AbsolutePath.make(directory)
  return AppNodeBuilder.build(LayerNode.group([ToolRegistry.node, ToolRegistry.toolsNode, ProjectTools.node]), [
    [
      Location.node,
      Layer.succeed(
        Location.Service,
        Location.Service.of(location(Location.Ref.make({ directory: projectDirectory }), { projectDirectory })),
      ),
    ],
    [Global.node, Global.layerWith({ config: path.join(directory, "global") })],
    [Config.node, Config.nodeWith({})],
    [
      Npm.node,
      Layer.succeed(
        Npm.Service,
        Npm.Service.of({
          add: () => Effect.die("unused"),
          install,
          which: () => Effect.succeed(undefined),
        }),
      ),
    ],
    [
      PermissionV2.node,
      Layer.succeed(
        PermissionV2.Service,
        PermissionV2.Service.of({
          assert: (input) =>
            Effect.sync(() => {
              requests.push(input)
            }),
          ask: () => Effect.die("unused"),
          reply: () => Effect.die("unused"),
          get: () => Effect.succeed(undefined),
          forSession: () => Effect.succeed([]),
          list: () => Effect.succeed([]),
        }),
      ),
    ],
    [ToolOutputStore.node, outputStore],
  ])
}

describe("ToolRegistry", () => {
  projectIt.live("continues Location discovery after its first ready waiter is interrupted", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((tmp) => {
        const permissionRequests: PermissionV2.AssertInput[] = []
        const toolDirectory = path.join(tmp.path, ".opencode", "tools")
        const pluginPackage = path.join(tmp.path, ".opencode", "node_modules", "@opencode-ai", "plugin")
        const marker = path.join(toolDirectory, "import-started")
        const completedMarker = path.join(toolDirectory, "import-completed")
        return Effect.promise(async () => {
          await fs.mkdir(toolDirectory, { recursive: true })
          await fs.mkdir(path.dirname(pluginPackage), { recursive: true })
          await fs.symlink(path.resolve(import.meta.dir, "../../plugin"), pluginPackage, "dir")
          await Bun.write(
            path.join(toolDirectory, "delayed.js"),
            [
              'import { writeFile } from "node:fs/promises"',
              'import { fileURLToPath } from "node:url"',
              'import { tool } from "@opencode-ai/plugin"',
              'await writeFile(fileURLToPath(new URL("./import-started", import.meta.url)), "started")',
              "await new Promise((resolve) => setTimeout(resolve, 500))",
              'await writeFile(fileURLToPath(new URL("./import-completed", import.meta.url)), "completed")',
              "export default tool({ description: 'Survives an interrupted discovery waiter', args: {}, execute: async () => 'survived' })",
              "",
            ].join("\n"),
          )
        }).pipe(
          Effect.andThen(
            Effect.gen(function* () {
              const projectTools = yield* ProjectTools.Service
              const first = yield* projectTools.ready.pipe(Effect.forkChild)
              const started = yield* Effect.promise(async () => {
                for (let attempt = 0; attempt < 100; attempt++) {
                  if (await Bun.file(marker).exists()) return true
                  await Bun.sleep(5)
                }
                return false
              })
              expect(started).toBe(true)
              yield* Fiber.interrupt(first)
              expect(Exit.isFailure(yield* Fiber.await(first))).toBe(true)
              expect(yield* Effect.promise(() => Bun.file(completedMarker).exists())).toBe(false)

              yield* projectTools.ready
              expect(yield* Effect.promise(() => Bun.file(completedMarker).exists())).toBe(true)
              const registry = yield* ToolRegistry.Service
              const materialized = yield* registry.materialize()
              expect(materialized.definitions.map((definition) => definition.name)).toContain("delayed")
              expect(
                yield* materialized.settle({
                  sessionID,
                  ...identity,
                  call: { type: "tool-call", id: "call-delayed-after-interrupt", name: "delayed", input: {} },
                }),
              ).toMatchObject({ result: { type: "text", value: "survived" } })
              expect(permissionRequests).toEqual([])
            }).pipe(Effect.provide(projectToolLayer(tmp.path, permissionRequests))),
          ),
        )
      }),
    ),
  )

  projectIt.live("prepares the plugin SDK before importing a fresh project's bare module", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((tmp) => {
        const permissionRequests: PermissionV2.AssertInput[] = []
        const installRequests: { dir: string; packages: Npm.AddRequest[] }[] = []
        const opencode = path.join(tmp.path, ".opencode")
        const tools = path.join(opencode, "tools")
        const install: Npm.Interface["install"] = (dir, input) =>
          Effect.promise(async () => {
            installRequests.push({ dir, packages: input?.add ?? [] })
            const pluginPackage = path.join(dir, "node_modules", "@opencode-ai", "plugin")
            await fs.mkdir(path.dirname(pluginPackage), { recursive: true })
            const source = path.resolve(import.meta.dir, "../../plugin")
            await fs.symlink(source, pluginPackage, "dir")
          })
        return Effect.promise(async () => {
          await fs.mkdir(tools, { recursive: true })
          await Bun.write(
            path.join(tools, "fresh.js"),
            [
              'import { tool } from "@opencode-ai/plugin"',
              "export default tool({",
              "  description: 'Tool loaded after SDK preparation',",
              "  args: { value: tool.schema.string() },",
              "  execute: async ({ value }) => `installed:${value}`,",
              "})",
              "",
            ].join("\n"),
          )
        }).pipe(
          Effect.andThen(
            Effect.gen(function* () {
              const projectTools = yield* ProjectTools.Service
              yield* projectTools.ready
              const registry = yield* ToolRegistry.Service
              const materialized = yield* registry.materialize()
              expect(installRequests).toEqual([
                {
                  dir: opencode,
                  packages: [{ name: "@opencode-ai/plugin", version: undefined }],
                },
              ])
              expect(materialized.definitions.map((definition) => definition.name)).toContain("fresh")
              expect(
                yield* materialized.settle({
                  sessionID,
                  ...identity,
                  call: { type: "tool-call", id: "call-fresh-project-tool", name: "fresh", input: { value: "ready" } },
                }),
              ).toMatchObject({ result: { type: "text", value: "installed:ready" } })
              expect(permissionRequests).toEqual([])
            }).pipe(Effect.provide(projectToolLayer(tmp.path, permissionRequests, install))),
          ),
        )
      }),
    ),
  )

  projectIt.live("prepares declared local-file dependencies when the plugin SDK is already installed", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((tmp) => {
        const permissionRequests: PermissionV2.AssertInput[] = []
        const installRequests: { dir: string; packages: Npm.AddRequest[] }[] = []
        const opencode = path.join(tmp.path, ".opencode")
        const tools = path.join(opencode, "tools")
        const localDependencyName = "project-tool-local"
        const install: Npm.Interface["install"] = (dir, input) =>
          Effect.promise(async () => {
            installRequests.push({ dir, packages: input?.add ?? [] })
            const manifest = (await Bun.file(path.join(dir, "package.json")).json()) as {
              dependencies: Record<string, string>
            }
            if (manifest.dependencies[localDependencyName] !== "file:./local-dependency") return
            const packagePath = path.join(dir, "node_modules", localDependencyName)
            await fs.mkdir(path.dirname(packagePath), { recursive: true })
            await fs.symlink(path.join(dir, "local-dependency"), packagePath, "dir")
          })
        return Effect.promise(async () => {
          const plugin = (await Bun.file(path.resolve(import.meta.dir, "../../plugin/package.json")).json()) as {
            version: string
          }
          const dependencies = {
            "@opencode-ai/plugin": plugin.version,
            [localDependencyName]: "file:./local-dependency",
          }
          const pluginPackage = path.join(opencode, "node_modules", "@opencode-ai", "plugin")
          await fs.mkdir(tools, { recursive: true })
          await fs.mkdir(path.dirname(pluginPackage), { recursive: true })
          await fs.symlink(path.resolve(import.meta.dir, "../../plugin"), pluginPackage, "dir")
          await fs.mkdir(path.join(opencode, "local-dependency"), { recursive: true })
          await Bun.write(
            path.join(opencode, "package.json"),
            JSON.stringify({ name: "project-tool-dependencies", private: true, dependencies }),
          )
          await Bun.write(
            path.join(opencode, "package-lock.json"),
            JSON.stringify({
              name: "project-tool-dependencies",
              lockfileVersion: 3,
              packages: {
                "": {
                  name: "project-tool-dependencies",
                  dependencies: { "@opencode-ai/plugin": plugin.version },
                },
                "node_modules/@opencode-ai/plugin": { version: plugin.version },
              },
            }),
          )
          await Bun.write(
            path.join(opencode, "local-dependency", "package.json"),
            JSON.stringify({ name: localDependencyName, type: "module", exports: "./index.js" }),
          )
          await Bun.write(path.join(opencode, "local-dependency", "index.js"), 'export const value = "local-file"\n')
          await Bun.write(
            path.join(tools, "dirty_dependency.js"),
            [
              'import { tool } from "@opencode-ai/plugin"',
              `import { value } from "${localDependencyName}"`,
              "export default tool({",
              "  description: 'Uses a dependency declared with a local file spec',",
              "  args: { input: tool.schema.string() },",
              "  execute: async ({ input }) => `${value}:${input}`,",
              "})",
              "",
            ].join("\n"),
          )
        }).pipe(
          Effect.andThen(
            Effect.gen(function* () {
              const projectTools = yield* ProjectTools.Service
              yield* projectTools.ready
              expect(installRequests).toHaveLength(1)
              expect(installRequests[0]?.dir).toBe(opencode)
              expect(installRequests[0]?.packages.map((pkg) => pkg.name)).toContain("@opencode-ai/plugin")
              const registry = yield* ToolRegistry.Service
              const materialized = yield* registry.materialize()
              expect(materialized.definitions.map((definition) => definition.name)).toContain("dirty_dependency")
              expect(
                yield* materialized.settle({
                  sessionID,
                  ...identity,
                  call: {
                    type: "tool-call",
                    id: "call-local-file-dependency",
                    name: "dirty_dependency",
                    input: { input: "ready" },
                  },
                }),
              ).toMatchObject({ result: { type: "text", value: "local-file:ready" } })
              expect(permissionRequests).toEqual([])
            }).pipe(Effect.provide(projectToolLayer(tmp.path, permissionRequests, install))),
          ),
        )
      }),
    ),
  )

  void projectIt.effect("discovers plugin modules and settles their validated output through canonical tools", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((tmp) => {
        const permissionRequests: PermissionV2.AssertInput[] = []
        return Effect.promise(async () => {
          const opencode = path.join(tmp.path, ".opencode")
          const tools = path.join(opencode, "tools")
          const singular = path.join(opencode, "tool")
          const pluginPackage = path.join(opencode, "node_modules", "@opencode-ai", "plugin")
          await fs.mkdir(tools, { recursive: true })
          await fs.mkdir(singular, { recursive: true })
          await fs.mkdir(path.dirname(pluginPackage), { recursive: true })
          await fs.symlink(path.resolve(import.meta.dir, "../../plugin"), pluginPackage, "dir")
          await Bun.write(
            path.join(tools, "broken.js"),
            "import { missing } from '@project-tool-dependency-that-is-not-installed'; export default missing",
          )
          await Bun.write(
            path.join(tools, "healthy.js"),
            [
              'import { tool } from "@opencode-ai/plugin"',
              "export const named = tool({ description: 'Named local tool', args: {}, execute: async () => 'named' })",
              "export default tool({",
              "  description: 'Echo one validated string',",
              "  args: { text: tool.schema.string().describe('Text to echo') },",
              "  async execute({ text }, context) {",
              "    await context.ask({ permission: 'project.test', patterns: ['workspace'], always: ['workspace'], metadata: { source: 'project-tool' } })",
              "    context.metadata({ title: 'Context title', metadata: { contextValue: 'kept' } })",
              "    return { output: JSON.stringify({ text, sessionID: context.sessionID, messageID: context.messageID, agent: context.agent, directory: context.directory, worktree: context.worktree, signalAborted: context.abort.aborted }), metadata: { resultValue: 'kept' }, attachments: [{ type: 'file', mime: 'text/plain', url: 'data:text/plain;base64,b2s=', filename: 'result.txt' }] }",
              "  },",
              "})",
              "",
            ].join("\n"),
          )
          await Bun.write(
            path.join(singular, "legacy.ts"),
            [
              "export default {",
              "  description: 'Legacy JSON Schema tool',",
              "  args: { query: { type: 'string', description: 'Search query' } },",
              "  execute: async ({ query }) => `legacy:${query}`,",
              "}",
              "",
            ].join("\n"),
          )
        }).pipe(
          Effect.andThen(
            Effect.gen(function* () {
              const projectDirectory = AbsolutePath.make(tmp.path)
              const toolsService = yield* ProjectTools.Service
              yield* toolsService.ready
              const registry = yield* ToolRegistry.Service
              const materialized = yield* registry.materialize()
              const names = materialized.definitions.map((definition) => definition.name)
              expect(names).toContain("healthy")
              expect(names).toContain("healthy_named")
              expect(names).toContain("legacy")
              expect(names).not.toContain("broken")
              const healthy = materialized.definitions.find((definition) => definition.name === "healthy")
              expect(healthy?.description).toBe("Echo one validated string")
              expect(healthy?.inputSchema).toMatchObject({
                type: "object",
                properties: { text: { type: "string", description: "Text to echo" } },
                required: ["text"],
              })
              expect(
                materialized.definitions.find((definition) => definition.name === "legacy")?.inputSchema,
              ).toMatchObject({
                type: "object",
                properties: { query: { type: "string", description: "Search query" } },
                required: ["query"],
              })
              expect(
                yield* materialized.settle({
                  sessionID,
                  ...identity,
                  call: { type: "tool-call", id: "call-project-tool", name: "healthy", input: { text: 12 } },
                }),
              ).toMatchObject({ result: { type: "error", value: expect.stringContaining("Invalid tool input:") } })

              const result = yield* materialized.settle({
                sessionID,
                ...identity,
                call: { type: "tool-call", id: "call-project-tool-valid", name: "healthy", input: { text: "hello" } },
              })
              expect(result.output?.structured).toEqual({
                title: "Context title",
                output: JSON.stringify({
                  text: "hello",
                  sessionID,
                  messageID: identity.assistantMessageID,
                  agent: identity.agent,
                  directory: projectDirectory,
                  worktree: projectDirectory,
                  signalAborted: false,
                }),
                metadata: { resultValue: "kept", contextValue: "kept" },
                attachments: [
                  { type: "file", mime: "text/plain", url: "data:text/plain;base64,b2s=", filename: "result.txt" },
                ],
              })
              expect(result.result.type).toBe("content")
              expect(
                yield* materialized.settle({
                  sessionID,
                  ...identity,
                  call: { type: "tool-call", id: "call-project-tool-named", name: "healthy_named", input: {} },
                }),
              ).toMatchObject({
                result: { type: "text", value: "named" },
                output: { structured: "named", content: [{ type: "text", text: "named" }] },
              })
              expect(
                yield* materialized.settle({
                  sessionID,
                  ...identity,
                  call: { type: "tool-call", id: "call-project-tool-legacy", name: "legacy", input: { query: "find" } },
                }),
              ).toMatchObject({
                result: { type: "text", value: "legacy:find" },
                output: { structured: "legacy:find", content: [{ type: "text", text: "legacy:find" }] },
              })
              expect(permissionRequests).toHaveLength(1)
              expect(permissionRequests).toMatchObject([
                {
                  sessionID,
                  agent: identity.agent,
                  action: "project.test",
                  resources: ["workspace"],
                  save: ["workspace"],
                  source: {
                    type: "tool",
                    messageID: identity.assistantMessageID,
                    callID: "call-project-tool-valid",
                  },
                },
              ])
            }).pipe(Effect.provide(projectToolLayer(tmp.path, permissionRequests))),
          ),
        )
      }),
    ),
  )

  it.effect("filters disabled tools with edit aliases and ordered wildcard precedence", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      yield* service.register({
        question: make(),
        bash: make(),
        edit: make("edit"),
        write: make("edit"),
        apply_patch: make("edit"),
      })
      const names = (rules: Parameters<ToolRegistry.Interface["materialize"]>[0]) =>
        toolDefinitions(service, rules).pipe(Effect.map((definitions) => definitions.map((tool) => tool.name)))

      expect(yield* names([{ action: "question", resource: "*", effect: "deny" }])).toEqual([
        "bash",
        "edit",
        "write",
        "apply_patch",
      ])
      expect(
        yield* names([
          { action: "*", resource: "*", effect: "deny" },
          { action: "question", resource: "private", effect: "allow" },
        ]),
      ).toEqual(["question"])
      expect(
        yield* names([
          { action: "question", resource: "private", effect: "allow" },
          { action: "*", resource: "*", effect: "deny" },
        ]),
      ).toEqual([])
      expect(yield* names([{ action: "edit", resource: "*", effect: "deny" }])).toEqual(["question", "bash"])
    }),
  )

  it.effect("keeps permission decoration isolated between registrations", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      const shared = make()
      yield* service.register({ first: shared })
      yield* service.register({ second: Tool.withPermission(shared, "edit") })
      Tool.withPermission(shared, "question")

      expect(
        (yield* toolDefinitions(service, [{ action: "edit", resource: "*", effect: "deny" }])).map(
          (definition) => definition.name,
        ),
      ).toEqual(["first"])
    }),
  )

  it.effect("reuses model definitions across provider turns", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      yield* service.register({ echo: make() })
      const first = yield* toolDefinitions(service)
      const second = yield* toolDefinitions(service)

      expect(second[0]).toBe(first[0])
    }),
  )

  it.effect("removes a scoped registration", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      const scope = yield* Scope.make()
      yield* service.register({ echo: make() }).pipe(Scope.provide(scope))
      expect((yield* toolDefinitions(service)).map((tool) => tool.name)).toEqual(["echo"])
      yield* Scope.close(scope, Exit.void)
      expect(yield* toolDefinitions(service)).toEqual([])
    }),
  )

  it.effect("preserves an interrupted registration until its scope closes", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      const scope = yield* Scope.make()
      const registered = yield* Deferred.make<void>()
      const fiber = yield* service
        .register({ echo: make() })
        .pipe(
          Effect.andThen(Deferred.succeed(registered, undefined)),
          Effect.andThen(Effect.never),
          Scope.provide(scope),
          Effect.forkChild,
        )
      yield* Deferred.await(registered)
      yield* Fiber.interrupt(fiber)

      expect((yield* toolDefinitions(service)).map((tool) => tool.name)).toEqual(["echo"])
      yield* Scope.close(scope, Exit.void)
      expect(yield* toolDefinitions(service)).toEqual([])
    }),
  )

  it.effect("returns model errors without swallowing interruption or defects", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      yield* service.register({
        failed: Tool.make({
          description: "Failed",
          input: Schema.Struct({}),
          output: Schema.Struct({ ok: Schema.Boolean }),
          execute: () => Effect.fail(new Tool.Failure({ message: "Denied" })),
        }),
      })
      expect(
        yield* executeTool(service, {
          sessionID,
          ...identity,
          call: { type: "tool-call", id: "failed", name: "failed", input: {} },
        }),
      ).toEqual({ type: "error", value: "Denied" })
      expect(
        yield* executeTool(service, {
          sessionID,
          ...identity,
          call: { type: "tool-call", id: "missing", name: "missing", input: {} },
        }),
      ).toEqual({ type: "error", value: "Unknown tool: missing" })

      yield* service.register({
        defect: Tool.make({
          description: "Defect",
          input: Schema.Struct({}),
          output: Schema.Struct({}),
          execute: () => Effect.die("unexpected executor defect"),
        }),
      })
      expect(
        yield* service.materialize().pipe(
          Effect.flatMap((materialized) =>
            materialized.settle({
              sessionID,
              ...identity,
              call: { type: "tool-call", id: "defect", name: "defect", input: {} },
            }),
          ),
          Effect.catchDefect(Effect.succeed),
        ),
      ).toBe("unexpected executor defect")
    }),
  )

  it.effect("propagates retention failures through settlement", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      yield* service.register({ echo: make() })
      const materialized = yield* service.materialize()
      const exit = yield* materialized.settle(call("echo", "call-retention-failure")).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toBe(retentionFailure)
      expect(retentionFailure.message).toBe("Failed to write tool output: disk full")
    }),
  )

  it.effect("exposes settlement only through materialization", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      expect("definitions" in service).toBe(false)
      expect("execute" in service).toBe(false)
      expect("settle" in service).toBe(false)
      expect(typeof service.materialize).toBe("function")
    }),
  )

  it.effect("passes complete invocation identity to the canonical handler", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      const contexts: Tool.Context[] = []
      yield* service.register({
        context: Tool.make({
          description: "Context",
          input: Schema.Struct({}),
          output: Schema.Struct({ ok: Schema.Boolean }),
          execute: (_, context) => Effect.sync(() => contexts.push(context)).pipe(Effect.as({ ok: true })),
        }),
      })
      yield* executeTool(service, {
        sessionID,
        ...identity,
        call: { type: "tool-call", id: "call-context", name: "context", input: {} },
      })
      expect(contexts).toEqual([{ sessionID, ...identity, toolCallID: "call-context" }])
    }),
  )

  it.effect("encodes output and applies generic settlement bounding", () =>
    Effect.gen(function* () {
      bounds.length = 0
      const service = yield* ToolRegistry.Service
      yield* service.register({ bounded: make() })
      expect(
        yield* settleTool(service, {
          sessionID,
          ...identity,
          call: { type: "tool-call", id: "call-bounded", name: "bounded", input: { text: "complete" } },
        }),
      ).toEqual({
        result: { type: "text", value: "bounded reference" },
        output: { structured: {}, content: [{ type: "text", text: "bounded reference" }] },
        outputPaths: ["/managed/generic"],
      })
      expect(bounds).toHaveLength(1)
    }),
  )

  it.effect("enforces transformed codecs at execution and projection boundaries", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      const executed: string[] = []
      const Transformed = Schema.Boolean.pipe(
        Schema.decodeTo(Schema.String, {
          decode: SchemaGetter.transform((value) => (value ? "yes" : "no")),
          encode: SchemaGetter.transform((value) => value === "yes"),
        }),
      )
      yield* service.register({
        transformed: Tool.make({
          description: "Transform values",
          input: Schema.Struct({ value: Transformed }),
          output: Schema.Struct({ value: Transformed }),
          execute: ({ value }) => Effect.sync(() => executed.push(value)).pipe(Effect.as({ value })),
          toModelOutput: ({ output }) => [{ type: "text", text: String(output.value) }],
        }),
      })

      expect(
        yield* executeTool(service, {
          sessionID,
          ...identity,
          call: { type: "tool-call", id: "transformed", name: "transformed", input: { value: true } },
        }),
      ).toEqual({ type: "text", value: "true" })
      expect(executed).toEqual(["yes"])
      expect(
        yield* executeTool(service, {
          sessionID,
          ...identity,
          call: { type: "tool-call", id: "invalid-input", name: "transformed", input: { value: "yes" } },
        }),
      ).toMatchObject({ type: "error", value: expect.stringContaining("Invalid tool input") })
      expect(executed).toEqual(["yes"])

      yield* service.register({
        invalid_output: Tool.make({
          description: "Return invalid output",
          input: Schema.Struct({}),
          output: Schema.Struct({
            value: Schema.Boolean.pipe(
              Schema.decodeTo(Schema.String, {
                decode: SchemaGetter.transform((value) => String(value)),
                encode: SchemaGetter.transformOrFail((value) =>
                  value === "valid"
                    ? Effect.succeed(true)
                    : Effect.fail(new SchemaIssue.InvalidValue(Option.some(value), { message: "invalid output" })),
                ),
              }),
            ),
          }),
          execute: () => Effect.succeed({ value: "invalid" }),
        }),
      })
      expect(
        yield* executeTool(service, {
          sessionID,
          ...identity,
          call: { type: "tool-call", id: "invalid-output", name: "invalid_output", input: {} },
        }),
      ).toMatchObject({ type: "error", value: expect.stringContaining("invalid value for its output schema") })
    }),
  )

  it.effect("executes the unchanged registration advertised for a provider turn", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      yield* service.register({ echo: make() })
      const materialized = yield* service.materialize()

      expect((yield* materialized.settle(call("echo"))).result).toEqual({ type: "text", value: "echo" })
    }),
  )

  it.effect("rejects a call when its advertised registration was removed", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      const scope = yield* Scope.make()
      yield* service.register({ echo: make() }).pipe(Scope.provide(scope))
      const materialized = yield* service.materialize()
      yield* Scope.close(scope, Exit.void)

      expect((yield* materialized.settle(call("echo"))).result).toEqual({
        type: "error",
        value: "Stale tool call: echo",
      })
    }),
  )

  it.effect("rejects only the replaced name from a multi-tool provider turn", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      yield* service.register({ first: make(), second: make() })
      const materialized = yield* service.materialize()
      yield* service.register({ first: make() })

      expect((yield* materialized.settle(call("first"))).result).toEqual({
        type: "error",
        value: "Stale tool call: first",
      })
      expect((yield* materialized.settle(call("second"))).result).toEqual({ type: "text", value: "second" })
    }),
  )

  it.effect("treats revealing a previous overlay as stale", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      yield* service.register({ echo: make() })
      const overlay = yield* Scope.make()
      yield* service.register({ echo: make() }).pipe(Scope.provide(overlay))
      const materialized = yield* service.materialize()
      yield* Scope.close(overlay, Exit.void)

      expect((yield* materialized.settle(call("echo"))).result).toEqual({
        type: "error",
        value: "Stale tool call: echo",
      })
    }),
  )

  integrated.effect("rejects an application call after a Location override is registered", () =>
    Effect.gen(function* () {
      const applications = yield* ApplicationTools.Service
      const service = yield* ToolRegistry.Service
      yield* applications.register({ echo: make() })
      const materialized = yield* service.materialize()
      yield* service.register({ echo: make() })

      expect((yield* materialized.settle(call("echo"))).result).toEqual({
        type: "error",
        value: "Stale tool call: echo",
      })
    }),
  )

  integrated.effect("rejects a Location call after removal reveals an application registration", () =>
    Effect.gen(function* () {
      const applications = yield* ApplicationTools.Service
      const service = yield* ToolRegistry.Service
      yield* applications.register({ echo: make() })
      const scope = yield* Scope.make()
      yield* service.register({ echo: make() }).pipe(Scope.provide(scope))
      const materialized = yield* service.materialize()
      yield* Scope.close(scope, Exit.void)

      expect((yield* materialized.settle(call("echo"))).result).toEqual({
        type: "error",
        value: "Stale tool call: echo",
      })
    }),
  )

  it.effect("keeps captured execution running after registration mutation", () =>
    Effect.gen(function* () {
      const service = yield* ToolRegistry.Service
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* service
        .register({
          echo: Tool.make({
            description: "Echo text",
            input: Schema.Struct({ text: Schema.String }),
            output: Schema.Struct({ text: Schema.String }),
            execute: ({ text }) =>
              Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.as({ text })),
            toModelOutput: ({ output }) => [{ type: "text", text: output.text }],
          }),
        })
        .pipe(Scope.provide(scope))
      const materialized = yield* service.materialize()
      const settlement = yield* materialized.settle(call("echo")).pipe(Effect.forkChild)
      yield* Deferred.await(started)
      yield* Scope.close(scope, Exit.void)
      yield* service.register({ echo: make() })
      yield* Deferred.succeed(release, undefined)

      expect(yield* Fiber.join(settlement)).toMatchObject({ result: { type: "text", value: "echo" } })
    }),
  )
})
