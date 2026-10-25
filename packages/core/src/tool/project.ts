export * as ProjectTools from "./project"

import { Context, Effect, JsonSchema, Layer, Schema, Scope } from "effect"
import path from "path"
import { pathToFileURL } from "url"
import { z } from "zod"
import type { ToolDefinition } from "@opencode-ai/llm"
import { Config } from "../config"
import { makeLocationNode } from "../effect/app-node"
import { Location } from "../location"
import { PermissionV2 } from "../permission"
import { Glob } from "../util/glob"
import { Tool } from "./tool"
import { ToolRegistry } from "./registry"
import { Tools } from "./tools"

export interface Interface {
  readonly ready: Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/ProjectTools") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const config = yield* Config.Service
    const location = yield* Location.Service
    const permissions = yield* PermissionV2.Service
    const tools = yield* Tools.Service
    const scope = yield* Scope.Scope
    const ready = yield* Effect.cached(
      Effect.gen(function* () {
        const entries = yield* config.entries()
        const directories = entries.filter((entry): entry is Config.Directory => entry.type === "directory")
        for (const directory of directories) {
          const files = yield* Effect.try({
            try: () =>
              Glob.scanSync("{tool,tools}/*.{js,ts}", {
                cwd: directory.path,
                absolute: true,
                dot: true,
                symlink: true,
              }).sort(),
            catch: errorMessage,
          }).pipe(
            Effect.tapError((error) =>
              Effect.logError("failed to scan project tool directory", { path: directory.path, error }),
            ),
            Effect.catch(() => Effect.succeed([])),
          )
          for (const file of files) {
            const namespace = path.basename(file, path.extname(file))
            const loaded = yield* Effect.tryPromise({
              try: async () => {
                const module = await import(pathToFileURL(file).href)
                return Object.fromEntries(
                  Object.entries(module).flatMap(([name, value]) =>
                    isPluginTool(value)
                      ? [
                          [
                            name === "default" ? namespace : `${namespace}_${name}`,
                            fromPlugin(
                              value,
                              { directory: location.directory, worktree: location.project.directory },
                              permissions,
                            ),
                          ],
                        ]
                      : [],
                  ),
                ) as Record<string, Tool.AnyTool>
              },
              catch: errorMessage,
            }).pipe(
              Effect.tapError((error) => Effect.logError("failed to load project tool module", { path: file, error })),
              Effect.catch(() => Effect.succeed(undefined)),
            )
            if (!loaded || Object.keys(loaded).length === 0) continue
            yield* tools.register(loaded).pipe(
              Effect.tapError((error) =>
                Effect.logError("failed to register project tool module", { path: file, error }),
              ),
              Effect.catch(() => Effect.void),
              Scope.provide(scope),
            )
          }
        }
      }).pipe(Scope.provide(scope)),
    )
    return Service.of({ ready })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [Config.node, Location.node, PermissionV2.node, ToolRegistry.toolsNode],
})

function fromPlugin(
  def: ProjectToolModule,
  project: { readonly directory: string; readonly worktree: string },
  permissions: PermissionV2.Interface,
): Tool.AnyTool {
  const args = def.args ?? {}
  const entries = Object.entries(args)
  const allZod = entries.every((entry) => isZodType(entry[1]))
  const zodParams = allZod ? z.object(args) : undefined
  return Tool.make({
    description: def.description,
    input: zodParams
      ? Schema.declare<unknown>((input): input is unknown => zodParams.safeParse(input).success)
      : Schema.Unknown,
    inputJsonSchema: zodParams ? zodJsonSchema(zodParams) : legacyJsonSchema(entries),
    output: Schema.Unknown,
    execute: (input, context) =>
      Effect.gen(function* () {
        const runtime = yield* Effect.context()
        const controller = new AbortController()
        let title: string | undefined
        let metadata: Record<string, unknown> | undefined
        const pluginContext: ProjectToolContext = {
          sessionID: context.sessionID,
          messageID: context.assistantMessageID,
          agent: context.agent,
          directory: project.directory,
          worktree: project.worktree,
          abort: controller.signal,
          metadata: (value) => {
            title = value.title ?? title
            if (value.metadata !== undefined) metadata = { ...metadata, ...value.metadata }
          },
          ask: (value) =>
            Effect.runPromiseWith(runtime)(
              permissions.assert({
                sessionID: context.sessionID,
                agent: context.agent,
                action: value.permission,
                resources: value.patterns,
                save: value.always,
                metadata: value.metadata,
                source: {
                  type: "tool",
                  messageID: context.assistantMessageID,
                  callID: context.toolCallID,
                },
              }),
            ),
        }
        const result = yield* Effect.acquireUseRelease(
          Effect.succeed(controller),
          () => Effect.promise(() => def.execute(input, pluginContext)),
          (current) => Effect.sync(() => current.abort()),
        )
        if (title === undefined && metadata === undefined) return result
        if (typeof result === "string") {
          return {
            ...(title === undefined ? {} : { title }),
            output: result,
            ...(metadata === undefined ? {} : { metadata }),
          }
        }
        return {
          ...result,
          ...(result.title !== undefined || title === undefined ? {} : { title }),
          ...(metadata === undefined ? {} : { metadata: { ...metadata, ...result.metadata } }),
        }
      }),
    toModelOutput: ({ output }) => {
      const text =
        typeof output === "string"
          ? output
          : isRecord(output) && typeof output.output === "string"
            ? output.output
            : undefined
      const content: Tool.Content[] = text === undefined ? [] : [{ type: "text", text }]
      if (!isRecord(output) || !Array.isArray(output.attachments)) return content
      for (const attachment of output.attachments) {
        if (
          !isRecord(attachment) ||
          attachment.type !== "file" ||
          typeof attachment.mime !== "string" ||
          typeof attachment.url !== "string"
        )
          continue
        const data = /^data:[^;,]+;base64,(.*)$/.exec(attachment.url)?.[1]
        if (data === undefined) continue
        content.push({
          type: "file",
          data,
          mime: attachment.mime,
          ...(typeof attachment.filename === "string" ? { name: attachment.filename } : {}),
        })
      }
      return content
    },
  })
}

type ProjectToolModule = {
  readonly args?: Readonly<Record<string, unknown>>
  readonly description: ToolDefinition["description"]
  readonly execute: (args: unknown, context: ProjectToolContext) => Promise<ProjectToolResult>
}

type ProjectToolContext = {
  readonly sessionID: string
  readonly messageID: string
  readonly agent: string
  readonly directory: string
  readonly worktree: string
  readonly abort: AbortSignal
  readonly metadata: (input: { readonly title?: string; readonly metadata?: Record<string, unknown> }) => void
  readonly ask: (input: {
    readonly permission: string
    readonly patterns: ReadonlyArray<string>
    readonly always: ReadonlyArray<string>
    readonly metadata: Record<string, unknown>
  }) => Promise<void>
}

type ProjectToolResult =
  | string
  | {
      readonly title?: string
      readonly output: string
      readonly metadata?: Record<string, unknown>
      readonly attachments?: ReadonlyArray<{
        readonly type: "file"
        readonly mime: string
        readonly url: string
        readonly filename?: string
      }>
    }

function isPluginTool(value: unknown): value is ProjectToolModule {
  return typeof value === "object" && value !== null && "args" in value && "description" in value && "execute" in value
}

function isZodType(value: unknown): value is z.ZodType {
  return typeof value === "object" && value !== null && "_zod" in value
}

function isJsonSchemaDefinition(value: unknown): value is JsonSchema.JsonSchema | boolean {
  return typeof value === "boolean" || (typeof value === "object" && value !== null && !Array.isArray(value))
}

function legacyJsonSchema(entries: [string, unknown][]): JsonSchema.JsonSchema {
  const properties = Object.fromEntries(
    entries.filter((entry): entry is [string, JsonSchema.JsonSchema | boolean] => isJsonSchemaDefinition(entry[1])),
  )
  return {
    type: "object",
    properties,
    required: Object.keys(properties),
  }
}

function zodJsonSchema(schema: z.ZodType): JsonSchema.JsonSchema {
  const result = normalizeZodJsonSchema(z.toJSONSchema(schema, { io: "input", metadata: zodMetadataRegistry(schema) }))
  if (!isRecord(result)) throw new Error("project tool Zod schema produced a non-object JSON Schema")
  const { $defs, ...rest } = result
  return $defs && isRecord($defs) ? { ...rest, definitions: $defs } : rest
}

function zodMetadataRegistry(schema: z.ZodType) {
  const registry = z.registry<Record<string, unknown>>()
  const seen = new WeakSet<object>()
  const collect = (value: unknown) => {
    if (typeof value !== "object" || value === null) return
    if (seen.has(value)) return
    seen.add(value)
    if (isZodType(value)) {
      const metadata = typeof value.meta === "function" ? value.meta() : undefined
      const description = typeof value.description === "string" ? value.description : undefined
      const merged = {
        ...(metadata && typeof metadata === "object" ? metadata : {}),
        ...(description ? { description } : {}),
      }
      if (Object.keys(merged).length) registry.add(value, merged)
      collect(value._zod.def)
      return
    }
    for (const item of Object.values(value)) collect(item)
  }
  collect(schema)
  return registry
}

function normalizeZodJsonSchema(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((item) => normalizeZodJsonSchema(item))
  if (!isRecord(value)) return value
  return Object.fromEntries(
    Object.entries(value)
      .filter(
        ([key, item]) => !((key === "exclusiveMaximum" || key === "exclusiveMinimum") && typeof item === "boolean"),
      )
      .map(([key, item]) => [key, normalizeZodJsonSchema(item)]),
  )
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}
