import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { EventRetentionTable, EventTable } from "@opencode-ai/core/event/sql"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Global } from "@opencode-ai/core/global"
import { and, asc, eq } from "drizzle-orm"
import { EventV2Bridge } from "@/event-v2-bridge"
import { afterAll, expect, test } from "bun:test"
import { Database as Sqlite } from "bun:sqlite"
import { randomUUID } from "node:crypto"
import { lstat, mkdir, readFile, readlink, readdir, realpath, rm, symlink, writeFile } from "node:fs/promises"
import { createRequire } from "node:module"
import { pathToFileURL } from "node:url"
import { Npm } from "@opencode-ai/core/npm"
import { EffectFlock } from "@opencode-ai/core/util/effect-flock"
import { filesystem } from "@opencode-ai/core/effect/app-node-platform"
import { makeGlobalNode } from "@opencode-ai/core/effect/app-node"
import {
  Cause,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Logger,
  Option,
  Schema,
  Scope,
  Stream,
} from "effect"
import { HttpClient } from "effect/unstable/http"
import os from "os"
import path from "path"
import { fileURLToPath } from "url"
import { NamedError } from "@opencode-ai/core/util/error"
import { Agent as AgentSvc } from "../../src/agent/agent"
import { BackgroundJob } from "@/background/job"
import { Command } from "../../src/command"
import { Config } from "@/config/config"
import { LSP } from "@/lsp/lsp"
import { MCP } from "../../src/mcp"
import { Permission } from "../../src/permission"
import { Plugin } from "../../src/plugin"
import { Provider as ProviderSvc } from "@/provider/provider"
import { Env } from "../../src/env"
import { Git } from "../../src/git"
import { Image } from "../../src/image/image"

import { Question } from "../../src/question"
import { Todo } from "../../src/session/todo"
import { Session } from "@/session/session"
import { MessageTable, PartTable, SessionMessageTable } from "@opencode-ai/core/session/sql"
import { SessionPromptQueueSequenceTable, SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import { LLM } from "../../src/session/llm"
import { LLMEvent } from "@opencode-ai/llm"
import { MessageV2 } from "../../src/session/message-v2"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { SessionCompaction } from "../../src/session/compaction"
import { SessionSummary } from "../../src/session/summary"
import { Instruction } from "../../src/session/instruction"
import { SessionProcessor } from "../../src/session/processor"
import { SessionPrompt } from "../../src/session/prompt"
import { MachineMessage } from "../../src/session/machine-message"
import { SessionRevert } from "../../src/session/revert"
import { SessionRunState } from "../../src/session/run-state"
import { SessionQueue } from "../../src/session/queue"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionStatus } from "../../src/session/status"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { Skill } from "../../src/skill"
import { SystemPrompt } from "../../src/session/system"
import { Shell } from "@opencode-ai/core/shell"
import { Snapshot } from "../../src/snapshot"
import { ToolRegistry } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { ChildProcessSpawner } from "effect/unstable/process"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { Format } from "../../src/format"
import { provideInstanceEffect, TestInstance, tmpdirScoped } from "../fixture/fixture"
import { installWorkspacePluginSdk, type UnrelatedPackage } from "../fixture/plugin-sdk"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"
import { raw, reply, TestLLMServer } from "../lib/llm-server"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { sqliteLockMessage } from "@/util/sqlite-error"
import { newStatementAttempts, patchBunQuery } from "@opencode-ai/core/database/sqlite-attempts"
import { InstanceRef } from "@/effect/instance-ref"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { LocationServiceMap, locationServiceMapLayer } from "@opencode-ai/core/location-services"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { runImport } from "../../src/cli/cmd/import"
import { ShareNext } from "../../src/share/share-next"

const promptTests = <R, E>(layer: Layer.Layer<R, E>) => testEffect(layer, { git: true })

const summary = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({
    summarize: () => Effect.void,
    diff: () => Effect.succeed([]),
    computeDiff: () => Effect.succeed([]),
  }),
)

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

function withSh<A, E, R>(fx: () => Effect.Effect<A, E, R>) {
  return Effect.acquireUseRelease(
    Effect.sync(() => {
      const prev = process.env.SHELL
      process.env.SHELL = "/bin/sh"
      Shell.preferred.reset()
      return prev
    }),
    () => fx(),
    (prev) =>
      Effect.sync(() => {
        if (prev === undefined) delete process.env.SHELL
        else process.env.SHELL = prev
        Shell.preferred.reset()
      }),
  )
}

function toolPart(parts: SessionV1.Part[]) {
  return parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
}

type CompletedToolPart = SessionV1.ToolPart & { state: SessionV1.ToolStateCompleted }
type ErrorToolPart = SessionV1.ToolPart & { state: SessionV1.ToolStateError }

function completedTool(parts: SessionV1.Part[]) {
  const part = toolPart(parts)
  expect(part?.state.status).toBe("completed")
  return part?.state.status === "completed" ? (part as CompletedToolPart) : undefined
}

function errorTool(parts: SessionV1.Part[]) {
  const part = toolPart(parts)
  expect(part?.state.status).toBe("error")
  return part?.state.status === "error" ? (part as ErrorToolPart) : undefined
}

// MCP resource reads for the queue promotion tests: HELD_RESOURCE returns text,
// after waiting on `heldResource` when a test arms it; any other URI is missing,
// which makes createUserMessage fail.
const HELD_RESOURCE = "held://resource"
const heldResource = {
  gate: undefined as undefined | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> },
}

function makeMcp(instructions: MCP.ServerInstructions[] = []) {
  return Layer.succeed(
    MCP.Service,
    MCP.Service.of({
      status: () => Effect.succeed({}),
      clients: () => Effect.succeed({}),
      instructions: () => Effect.succeed(instructions),
      tools: () => Effect.succeed({}),
      prompts: () => Effect.succeed({}),
      resources: () => Effect.succeed({}),
      resourceTemplates: () => Effect.succeed({}),
      add: () => Effect.succeed({ status: { status: "disabled" as const } }),
      connect: () => Effect.void,
      disconnect: () => Effect.void,
      getPrompt: () => Effect.succeed(undefined),
      readResource: (_clientName: string, uri: string) =>
        uri !== HELD_RESOURCE
          ? Effect.succeed(undefined)
          : Effect.gen(function* () {
              const gate = heldResource.gate
              heldResource.gate = undefined
              if (gate) {
                yield* Deferred.succeed(gate.entered, undefined)
                yield* Deferred.await(gate.release)
              }
              return { contents: [{ uri, text: "held resource text" }] }
            }),
      startAuth: () => Effect.die("unexpected MCP auth in prompt-effect tests"),
      authenticate: () => Effect.die("unexpected MCP auth in prompt-effect tests"),
      finishAuth: () => Effect.die("unexpected MCP auth in prompt-effect tests"),
      removeAuth: () => Effect.void,
      supportsOAuth: () => Effect.succeed(false),
      hasStoredTokens: () => Effect.succeed(false),
      getAuthStatus: () => Effect.succeed("not_authenticated" as const),
    }),
  )
}

const lsp = Layer.succeed(
  LSP.Service,
  LSP.Service.of({
    init: () => Effect.void,
    status: () => Effect.succeed([]),
    hasClients: () => Effect.succeed(false),
    touchFile: () => Effect.void,
    diagnostics: () => Effect.succeed({}),
    hover: () => Effect.succeed(undefined),
    definition: () => Effect.succeed([]),
    references: () => Effect.succeed([]),
    implementation: () => Effect.succeed([]),
    documentSymbol: () => Effect.succeed([]),
    workspaceSymbol: () => Effect.succeed([]),
    prepareCallHierarchy: () => Effect.succeed([]),
    incomingCalls: () => Effect.succeed([]),
    outgoingCalls: () => Effect.succeed([]),
  }),
)

const processorCreateStarted: Array<() => void> = []
const processorImplementation = SessionProcessor.node.implementation
if (!processorImplementation) throw new Error("SessionProcessor node has no implementation")
const processorDependencies = [
  Session.node,
  Config.node,
  Snapshot.node,
  AgentSvc.node,
  LLM.node,
  Permission.node,
  Plugin.node,
  SessionSummary.node,
  SessionStatus.node,
  Image.node,
  EventV2Bridge.node,
  Database.node,
  ProviderSvc.node,
] as const
function processorWithCreate(create: SessionProcessor.Interface["create"]) {
  return LayerNode.make({
    service: SessionProcessor.Service,
    layer: Layer.effect(
      SessionProcessor.Service,
      Effect.gen(function* () {
        const real = yield* SessionProcessor.Service
        return SessionProcessor.Service.of({ ...real, create })
      }),
    ).pipe(
      Layer.provide(
        processorImplementation as Layer.Layer<
          SessionProcessor.Service,
          never,
          | Session.Service
          | Config.Service
          | Snapshot.Service
          | AgentSvc.Service
          | LLM.Service
          | Permission.Service
          | Plugin.Service
          | SessionSummary.Service
          | SessionStatus.Service
          | Image.Service
          | EventV2Bridge.Service
          | Database.Service
          | ProviderSvc.Service
        >,
      ),
    ),
    deps: processorDependencies,
  })
}

const abruptAssistantLLMCalls = { value: 0 }
const abruptAssistantLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () => {
      abruptAssistantLLMCalls.value++
      if (abruptAssistantLLMCalls.value > 1)
        return Stream.fromIterable([
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.reasoningStart({
            id: "completed_reasoning",
            providerMetadata: { openai: { itemID: "completed_reasoning" } },
          }),
          LLMEvent.reasoningDelta({
            id: "completed_reasoning",
            text: "completed later reasoning",
            providerMetadata: { openai: { itemID: "completed_reasoning" } },
          }),
          LLMEvent.reasoningEnd({ id: "completed_reasoning" }),
          LLMEvent.textStart({ id: "completed_answer" }),
          LLMEvent.textDelta({ id: "completed_answer", text: "later turn completed" }),
          LLMEvent.textEnd({ id: "completed_answer" }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ])
      return Stream.concat(
        Stream.fromIterable([
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.reasoningStart({
            id: "reasoning_before_process_exit",
            providerMetadata: { openai: { itemID: "reasoning_before_process_exit" } },
          }),
          LLMEvent.reasoningDelta({
            id: "reasoning_before_process_exit",
            text: "reasoning before process exit",
            providerMetadata: { openai: { itemID: "reasoning_before_process_exit" } },
          }),
          LLMEvent.toolCall({
            id: "call_before_process_exit",
            name: "read",
            input: { filePath: "/tmp/unfinished.ts" },
          }),
        ]),
        Stream.never,
      )
    },
  }),
)

const blockingProcessor = processorWithCreate(() =>
  Effect.sync(() => processorCreateStarted.shift()?.()).pipe(Effect.andThen(Effect.never)),
)

const runtimeFlags = RuntimeFlags.layer({ experimentalEventSystem: true })
const sdkReadiness = {
  gate: undefined as undefined | { dir: string; entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> },
  unrelated: undefined as undefined | { dir: string; pkg: UnrelatedPackage },
  activity: undefined as
    | undefined
    | {
        dir: string
        requests: number
        active: number
        maxActive: number
        completed: number
        firstInstallSucceeded: boolean
        secondRequested: Deferred.Deferred<void>
      },
}

// Provider tests resolve the actual workspace export through a serialized install fixture.
// Config still owns and joins its dependency fiber; readiness requires a usable SDK export.
const npmImplementation = Npm.node.implementation
if (!npmImplementation) throw new Error("Npm implementation is missing")
const promptSdk = makeGlobalNode({
  service: Npm.Service,
  layer: Layer.effect(
    Npm.Service,
    Effect.gen(function* () {
      const real = yield* Npm.Service
      const flock = yield* EffectFlock.Service
      return Npm.Service.of({
        ...real,
        install: Effect.fn("test.workspacePluginSdk")(function* (dir, input) {
          if (input?.add.some((item) => item.name !== "@opencode-ai/plugin")) return yield* real.install(dir, input)
          const ctx = yield* InstanceRef
          if (!ctx) return yield* Effect.die(new Error("SDK fixture has no instance owner"))
          const relative = path.relative(ctx.directory, dir)
          if (dir !== Global.Path.config && (path.isAbsolute(relative) || relative.startsWith("..")))
            return yield* Effect.die(new Error(`SDK fixture refused ancestor configuration: ${dir}`))
          const activity = sdkReadiness.activity?.dir === dir ? sdkReadiness.activity : undefined
          const request = activity ? ++activity.requests : 0
          if (activity) {
            if (request === 2) yield* Deferred.succeed(activity.secondRequested, undefined)
          }

          yield* flock.withLock(
            Effect.gen(function* () {
              if (activity) {
                activity.active++
                activity.maxActive = Math.max(activity.maxActive, activity.active)
              }
              yield* Effect.gen(function* () {
                const gate = sdkReadiness.gate?.dir === dir ? sdkReadiness.gate : undefined
                if (gate) {
                  sdkReadiness.gate = undefined
                  yield* Deferred.succeed(gate.entered, undefined)
                  yield* Deferred.await(gate.release)
                }

                const unrelated = sdkReadiness.unrelated?.dir === dir ? sdkReadiness.unrelated.pkg : undefined
                if (unrelated) sdkReadiness.unrelated = undefined
                const workspace = path.resolve(import.meta.dir, "../../../plugin")
                yield* Effect.promise(() =>
                  installWorkspacePluginSdk({
                    sandboxRoot: os.tmpdir(),
                    ownerRoot: dir === Global.Path.config ? Global.Path.config : ctx.directory,
                    consumerDirectory: dir,
                    workspace,
                    unrelated,
                  }),
                )

                const resolved = createRequire(path.join(dir, "sdk-fixture.cjs")).resolve("@opencode-ai/plugin")
                const target = yield* Effect.promise(() => realpath(path.join(workspace, "src/index.ts")))
                if ((yield* Effect.promise(() => realpath(resolved))) !== target)
                  return yield* Effect.die(new Error("SDK fixture resolved a different plugin package"))
                const sdk = yield* Effect.promise(() => import(pathToFileURL(resolved).href))
                if (typeof sdk.tool !== "function")
                  return yield* Effect.die(new Error("workspace plugin SDK is not ready"))
                if (activity) {
                  activity.completed++
                  if (request === 1) activity.firstInstallSucceeded = true
                }
              }).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    if (activity) activity.active--
                  }),
                ),
              )
            }),
            `npm-install:${dir}`,
          )
        }),
      })
    }),
  ).pipe(
    Layer.provide(
      npmImplementation as Layer.Layer<
        Npm.Service,
        never,
        FSUtil.Service | Global.Service | FileSystem.FileSystem | EffectFlock.Service
      >,
    ),
  ),
  deps: [FSUtil.node, Global.node, filesystem, EffectFlock.node],
})

const testLLMServerNode = LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })
const processorCreateDie = processorWithCreate(() => Effect.die(new Error("processor creation defect")))

const userShellChunks: { text: string; time: number }[] = []
const userShellRunning: Effect.Effect<boolean, unknown>[] = []
const crossSpawnImplementation = CrossSpawnSpawner.node.implementation
if (!crossSpawnImplementation) throw new Error("CrossSpawnSpawner node has no implementation")
const userShellSpawner = makeGlobalNode({
  service: ChildProcessSpawner.ChildProcessSpawner,
  layer: Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
      return ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          const handle = yield* spawner.spawn(command)
          userShellRunning.push(handle.isRunning)
          return ChildProcessSpawner.makeHandle({
            ...handle,
            all: handle.all.pipe(
              Stream.tap((bytes) =>
                Effect.sync(() => userShellChunks.push({ text: Buffer.from(bytes).toString("utf-8"), time: Date.now() })),
              ),
            ),
          })
        }),
      )
    }),
  ).pipe(Layer.provide([crossSpawnImplementation])),
  deps: [CrossSpawnSpawner.node.dependencies[0], CrossSpawnSpawner.node.dependencies[1]],
}) as typeof CrossSpawnSpawner.node

const promptRoot = LayerNode.group([
  Npm.node,
  SessionPrompt.node,
  Session.node,
  SessionProjector.node,
  MessageV2.node,
  Snapshot.node,
  LLM.node,
  Env.node,
  AgentSvc.node,
  Command.node,
  Permission.node,
  Plugin.node,
  Config.node,
  ProviderSvc.node,
  LSP.node,
  MCP.node,
  FSUtil.node,
  BackgroundJob.node,
  SessionStatus.node,
  SessionRunState.node,
  SessionQueue.node,
  Database.node,
  EventV2Bridge.node,
  Question.node,
  Todo.node,
  ToolRegistry.node,
  Skill.node,
  Git.node,
  Ripgrep.node,
  Format.node,
  Truncate.node,
  SessionProcessor.node,
  Image.node,
  SessionCompaction.node,
  SessionRevert.node,
  Instruction.node,
  SystemPrompt.node,
  CrossSpawnSpawner.node,
  RuntimeFlags.node,
])

type PromptTestOptions = {
  mcpInstructions?: MCP.ServerInstructions[]
  processor?: "blocking"
  status?: Layer.Layer<SessionStatus.Service>
  compaction?: Layer.Layer<SessionCompaction.Service>
  plugin?: Layer.Layer<Plugin.Service>
  llm?: Layer.Layer<LLM.Service>
  spawner?: typeof CrossSpawnSpawner.node
  eventBridge?: typeof EventV2Bridge.node
}

function makePrompt(input?: PromptTestOptions) {
  const replacements = [
    [Npm.node, promptSdk],
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp(input?.mcpInstructions)],
    [RuntimeFlags.node, runtimeFlags],
    ...(input?.status ? [[SessionStatus.node, input.status] as const] : []),
    ...(input?.compaction ? [[SessionCompaction.node, input.compaction] as const] : []),
    ...(input?.plugin ? [[Plugin.node, input.plugin] as const] : []),
    ...(input?.llm ? [[LLM.node, input.llm] as const] : []),
    ...(input?.spawner ? [[CrossSpawnSpawner.node, input.spawner] as const] : []),
    ...(input?.eventBridge ? [[EventV2Bridge.node, input.eventBridge] as const] : []),
  ] as const
  if (input?.processor === "blocking") {
    return LayerNode.compile(promptRoot, [...replacements, [SessionProcessor.node, blockingProcessor]])
  }
  return LayerNode.compile(promptRoot, replacements)
}

function makeHttp(input?: PromptTestOptions) {
  const root = LayerNode.group([promptRoot, testLLMServerNode])
  const replacements = [
    [Npm.node, promptSdk],
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp(input?.mcpInstructions)],
    [RuntimeFlags.node, runtimeFlags],
    ...(input?.status ? [[SessionStatus.node, input.status] as const] : []),
    ...(input?.compaction ? [[SessionCompaction.node, input.compaction] as const] : []),
    ...(input?.plugin ? [[Plugin.node, input.plugin] as const] : []),
    ...(input?.llm ? [[LLM.node, input.llm] as const] : []),
    ...(input?.spawner ? [[CrossSpawnSpawner.node, input.spawner] as const] : []),
    ...(input?.eventBridge ? [[EventV2Bridge.node, input.eventBridge] as const] : []),
  ] as const
  if (input?.processor === "blocking") {
    return LayerNode.compile(root, [...replacements, [SessionProcessor.node, blockingProcessor]])
  }
  return LayerNode.compile(root, replacements)
}

function makeHttpWithDatabase(database: Layer.Layer<Database.Service>) {
  return LayerNode.compile(LayerNode.group([promptRoot, testLLMServerNode]), [
    [Npm.node, promptSdk],
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp()],
    [RuntimeFlags.node, runtimeFlags],
    [Database.node, database],
  ])
}

function makeHttpNoLLMServer(input?: PromptTestOptions) {
  return makePrompt(input)
}

// Production-boundary gates for the V1 lost-wakeup regression. `finishingRead`
// holds the next Session.findMessage call, which is the finishing run's
// lastAssistant read; `nextEnsureRunning` resolves once the next caller of
// SessionRunState.ensureRunning has joined or started a run, and records which.
// With `hold`, that caller also waits after its run ends, as a slow one would.
const gates = {
  commandBefore: undefined as undefined | Effect.Effect<void, unknown>,
  messagesTransform: undefined as undefined | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> },
  queueSelection: undefined as undefined | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> },
  finishingRead: undefined as undefined | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> },
  nextEnsureRunning: undefined as
    | undefined
    | { reached: Deferred.Deferred<void>; startedRun: boolean; hold?: Deferred.Deferred<void> },
  // Holds a compaction between its summary and its continue message.
  compactionContinue: undefined as undefined | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> },
  compactionWriteFailure: undefined as undefined | { triggered: boolean },
  structuredUpdateFailure: undefined as undefined | { triggered: boolean },
  presetErrorUpdateFailure: undefined as
    | undefined
    | { name: "ContentFilterError" | "StructuredOutputError"; triggered: boolean },
  outcomeFailure: undefined as undefined | { triggered: boolean },
  noReplyAdmission: undefined as
    | undefined
    | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void>; skip?: number },
  noReplyWrite: undefined as undefined | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> },
  userShellPartUpdate: undefined as
    | undefined
    | {
        entered: Deferred.Deferred<void>
        release: Deferred.Deferred<void>
        written: Deferred.Deferred<void>
        held: boolean
        contains?: string
      },
}

const gatedSession = LayerNode.make({
  service: Session.Service,
  layer: Layer.effect(
    Session.Service,
    Effect.gen(function* () {
      const real = yield* Session.Service
      return Session.Service.of({
        ...real,
        updateMessage: <T extends SessionV1.Info>(msg: T) =>
          Effect.gen(function* () {
            const structuredFailure = gates.structuredUpdateFailure
            if (structuredFailure && msg.role === "assistant" && msg.structured !== undefined) {
              structuredFailure.triggered = true
              gates.structuredUpdateFailure = undefined
              return yield* Effect.die(new Error("injected structured output persistence failure"))
            }
            const presetErrorFailure = gates.presetErrorUpdateFailure
            if (presetErrorFailure && msg.role === "assistant" && msg.error?.name === presetErrorFailure.name) {
              presetErrorFailure.triggered = true
              gates.presetErrorUpdateFailure = undefined
              return yield* Effect.die(new Error(`injected ${presetErrorFailure.name} persistence failure`))
            }
            const gate = msg.role === "user" && msg.noReply === true ? gates.noReplyWrite : undefined
            if (gate) {
              gates.noReplyWrite = undefined
              yield* Deferred.succeed(gate.entered, undefined)
              yield* Deferred.await(gate.release)
            }
            return yield* real.updateMessage(msg)
          }),
        findMessage: (sessionID, predicate) =>
          Effect.gen(function* () {
            const gate = gates.finishingRead
            gates.finishingRead = undefined
            if (gate) {
              yield* Deferred.succeed(gate.entered, undefined)
              yield* Deferred.await(gate.release)
            }
            return yield* real.findMessage(sessionID, predicate)
          }),
      })
    }),
  ).pipe(
    Layer.provide(
      Session.node.implementation as Layer.Layer<
        Session.Service,
        never,
        BackgroundJob.Service | RuntimeFlags.Service | Database.Service | EventV2Bridge.Service
      >,
    ),
  ),
  deps: [BackgroundJob.node, RuntimeFlags.node, Database.node, EventV2Bridge.node],
})

const gatedEventV2Bridge = LayerNode.make({
  service: EventV2Bridge.Service,
  layer: Layer.effect(
    EventV2Bridge.Service,
    Effect.gen(function* () {
      const real = yield* EventV2Bridge.Service
      const publish: EventV2.Interface["publish"] = (definition, data, options) =>
        Effect.gen(function* () {
          const shellGate = gates.userShellPartUpdate
          if (shellGate && !shellGate.held && definition.type === SessionV1.Event.PartUpdated.type) {
            const update = data as typeof SessionV1.Event.PartUpdated.data.Type
            const output = update.part.type === "tool" && update.part.state.status === "running"
              ? update.part.state.metadata?.output
              : undefined
            if (output && (!shellGate.contains || output.includes(shellGate.contains))) {
              shellGate.held = true
              yield* Deferred.succeed(shellGate.entered, undefined)
              yield* Deferred.await(shellGate.release)
              const result = yield* real.publish(definition, data, options)
              yield* Deferred.succeed(shellGate.written, undefined)
              return result
            }
          }
          if (definition.type === SessionV1.Event.MessageUpdated.type) {
            const update = data as typeof SessionV1.Event.MessageUpdated.data.Type
            const failure = gates.compactionWriteFailure
            if (failure && update.info.role === "user" && !failure.triggered) {
              failure.triggered = true
              gates.compactionWriteFailure = undefined
              return yield* Effect.die(new Error("injected compaction write failure"))
            }
          }
          return yield* real.publish(definition, data, options)
        })
      return EventV2Bridge.Service.of({ ...real, publish })
    }),
  ).pipe(
    Layer.provide(EventV2Bridge.node.implementation as Layer.Layer<EventV2Bridge.Service, never, EventV2.Service>),
  ),
  deps: [EventV2.node],
})

const instructionClearFailure = { armed: false, triggered: false }
const instructionImplementation = Instruction.node.implementation
if (!instructionImplementation) throw new Error("Instruction node has no implementation")
const gatedInstruction = LayerNode.make({
  service: Instruction.Service,
  layer: Layer.effect(
    Instruction.Service,
    Effect.gen(function* () {
      const real = yield* Instruction.Service
      return Instruction.Service.of({
        ...real,
        clear: (messageID) =>
          Effect.gen(function* () {
            if (instructionClearFailure.armed && !instructionClearFailure.triggered) {
              instructionClearFailure.triggered = true
              instructionClearFailure.armed = false
              return yield* Effect.die(new Error("injected instruction.clear failure"))
            }
            return yield* real.clear(messageID)
          }),
      })
    }),
  ).pipe(
    Layer.provide(
      instructionImplementation as Layer.Layer<
        Instruction.Service,
        never,
        Config.Service | FSUtil.Service | Global.Service | RuntimeFlags.Service | HttpClient.HttpClient
      >,
    ),
  ),
  deps: [Config.node, FSUtil.node, Global.node, RuntimeFlags.node, httpClient],
})

const gatedRunState = LayerNode.make({
  service: SessionRunState.Service,
  layer: Layer.effect(
    SessionRunState.Service,
    Effect.gen(function* () {
      const real = yield* SessionRunState.Service
      return SessionRunState.Service.of({
        ...real,
        assertNotBusy: (sessionID) =>
          Effect.gen(function* () {
            const result = yield* real.assertNotBusy(sessionID).pipe(Effect.exit)
            const gate = gates.noReplyAdmission
            if (gate?.skip) gate.skip--
            else if (gate) {
              gates.noReplyAdmission = undefined
              yield* Deferred.succeed(gate.entered, undefined)
              yield* Deferred.await(gate.release)
            }
            if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause)
          }),
        ensureRunning: (sessionID, onInterrupt, work) =>
          Effect.gen(function* () {
            const marked = gates.nextEnsureRunning
            gates.nextEnsureRunning = undefined
            if (!marked) return yield* real.ensureRunning(sessionID, onInterrupt, work)
            // Deferred resumption evaluates the waiting fiber synchronously, so
            // join (or start) the run first and only then tell the test.
            const call = yield* real
              .ensureRunning(
                sessionID,
                onInterrupt,
                Effect.sync(() => void (marked.startedRun = true)).pipe(Effect.andThen(work)),
              )
              .pipe(Effect.forkChild({ startImmediately: true }))
            yield* Deferred.succeed(marked.reached, undefined)
            const result = yield* Fiber.join(call)
            if (marked.hold) yield* Deferred.await(marked.hold)
            return result
          }),
      })
    }),
  ).pipe(
    Layer.provide(
      SessionRunState.node.implementation as Layer.Layer<
        SessionRunState.Service,
        never,
        BackgroundJob.Service | SessionStatus.Service
      >,
    ),
  ),
  deps: [BackgroundJob.node, SessionStatus.node],
})

const gatedPlugin = LayerNode.make({
  service: Plugin.Service,
  layer: Layer.effect(
    Plugin.Service,
    Effect.gen(function* () {
      const real = yield* Plugin.Service
      const trigger = ((name, input, output) =>
        Effect.gen(function* () {
          const command = name === "command.execute.before" ? gates.commandBefore : undefined
          if (command) {
            gates.commandBefore = undefined
            yield* command
          }
          const selection = name === "chat.message" ? gates.queueSelection : undefined
          if (selection) {
            gates.queueSelection = undefined
            yield* Deferred.succeed(selection.entered, undefined)
            yield* Deferred.await(selection.release)
          }
          const transform = name === "experimental.chat.messages.transform" ? gates.messagesTransform : undefined
          if (transform)
            yield* Effect.fn("test.experimental.chat.messages.transform")(function* () {
              yield* Deferred.succeed(transform.entered, undefined)
              yield* Deferred.await(transform.release)
            })()
          const failure = gates.outcomeFailure
          if (name === "experimental.chat.messages.transform" && failure && !failure.triggered) {
            failure.triggered = true
            gates.outcomeFailure = undefined
            return yield* Effect.die(new Error("injected prompt outcome failure"))
          }
          const gate = name === "experimental.compaction.autocontinue" ? gates.compactionContinue : undefined
          if (gate) {
            gates.compactionContinue = undefined
            yield* Deferred.succeed(gate.entered, undefined)
            yield* Deferred.await(gate.release)
          }
          return yield* real.trigger(name, input, output)
        })) as typeof real.trigger
      return Plugin.Service.of({ ...real, trigger })
    }),
  ).pipe(
    Layer.provide(
      Plugin.node.implementation as Layer.Layer<
        Plugin.Service,
        never,
        EventV2Bridge.Service | Config.Service | RuntimeFlags.Service
      >,
    ),
  ),
  deps: [EventV2Bridge.node, Config.node, RuntimeFlags.node],
})

const it = promptTests(makeHttp())
const abruptPrompt = promptTests(makeHttp({ llm: abruptAssistantLLM }))
const gated = promptTests(
  LayerNode.compile(LayerNode.group([promptRoot, testLLMServerNode]), [
    [Npm.node, promptSdk],
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp()],
    [RuntimeFlags.node, runtimeFlags],
    [EventV2Bridge.node, gatedEventV2Bridge],
    [Session.node, gatedSession],
    [SessionRunState.node, gatedRunState],
    [Plugin.node, gatedPlugin],
  ]),
)
const instructionClearFailureTest = promptTests(
  LayerNode.compile(LayerNode.group([promptRoot, testLLMServerNode]), [
    [Npm.node, promptSdk],
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp()],
    [RuntimeFlags.node, runtimeFlags],
    [Instruction.node, gatedInstruction],
  ]),
)
const noLLMServer = promptTests(makeHttpNoLLMServer())
const userShellPrompt = promptTests(makeHttpNoLLMServer({ spawner: userShellSpawner }))
const userShellOrderingPrompt = promptTests(
  makeHttpNoLLMServer({ spawner: userShellSpawner, eventBridge: gatedEventV2Bridge }),
)
const raceNoLLMServer = promptTests(makeHttpNoLLMServer({ processor: "blocking" }))
const processorDies = promptTests(
  LayerNode.compile(promptRoot, [
    [Npm.node, promptSdk],
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp()],
    [RuntimeFlags.node, runtimeFlags],
    [SessionProcessor.node, processorCreateDie],
  ]),
)
const withMcpInstructions = promptTests(
  makeHttp({
    mcpInstructions: [
      {
        name: "guide-server",
        instructions: "Use lookup before mutate.",
        tools: ["guide-server_lookup"],
      },
    ],
  }),
)
const unix = process.platform !== "win32" ? it.instance : it.instance.skip
const unixNoLLMServer = process.platform !== "win32" ? noLLMServer.instance : noLLMServer.instance.skip
const unixUserShell = process.platform !== "win32" ? userShellPrompt.instance : userShellPrompt.instance.skip
const unixUserShellOrdering =
  process.platform !== "win32" ? userShellOrderingPrompt.instance : userShellOrderingPrompt.instance.skip
const nonOwnerCancelDatabasePath = path.join(os.tmpdir(), `opencode-non-owner-cancel-${randomUUID()}.db`)
const nonOwnerCancel = promptTests(makeHttpWithDatabase(Database.layerFromPath(nonOwnerCancelDatabasePath)))
const nonOwnerRunState = LayerNode.compile(
  LayerNode.group([SessionRunState.node, SessionStatus.node, EventV2Bridge.node, Database.node]),
  [[Database.node, Database.layerFromPath(nonOwnerCancelDatabasePath)]],
)

afterAll(async () => {
  await Promise.all(
    [nonOwnerCancelDatabasePath, consumeLockDbPath].flatMap((file) =>
      [file, `${file}-wal`, `${file}-shm`].map((each) => rm(each, { force: true })),
    ),
  )
})

// Config that registers a custom "test" provider with a "test-model" model
// so provider model lookup succeeds inside the loop.
const cfg = {
  provider: {
    test: {
      name: "Test",
      id: "test",
      env: [],
      npm: "@ai-sdk/openai-compatible",
      models: {
        "test-model": {
          id: "test-model",
          name: "Test Model",
          attachment: false,
          reasoning: false,
          temperature: false,
          tool_call: true,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: {
        apiKey: "test-key",
        baseURL: "http://localhost:1/v1",
      },
    },
  },
}

function providerCfg(url: string) {
  return {
    ...cfg,
    provider: {
      ...cfg.provider,
      test: {
        ...cfg.provider.test,
        options: {
          ...cfg.provider.test.options,
          baseURL: url,
        },
      },
    },
  }
}

const writeText = Effect.fn("test.writeText")(function* (file: string, text: string) {
  const fs = yield* FSUtil.Service
  yield* fs.writeWithDirs(file, text)
})

const writeConfig = Effect.fn("test.writeConfig")(function* (dir: string, config: Partial<ConfigV1.Info>) {
  yield* writeText(
    path.join(dir, "opencode.json"),
    JSON.stringify({ $schema: "https://opencode.ai/config.json", ...config }),
  )
})

async function createPluginSourceSurrogate(root: string) {
  const directory = path.join(root, `sdk-source-${randomUUID()}`)
  const scope = path.join(directory, "@opencode-ai")
  const packageDirectory = path.join(scope, "plugin")
  await mkdir(path.join(packageDirectory, "src"), { recursive: true })
  await writeFile(
    path.join(packageDirectory, "package.json"),
    JSON.stringify({ name: "@opencode-ai/plugin", version: "1.18.32", type: "module", exports: "./src/index.ts" }),
  )
  await writeFile(path.join(packageDirectory, "src/index.ts"), "export const marker = 'protected source'\n")
  await writeFile(path.join(packageDirectory, "preserved.txt"), "protected bytes\n")
  await symlink("src/index.ts", path.join(packageDirectory, "source-link.ts"))
  await Promise.all(
    Array.from({ length: 41 }, (_, index) =>
      writeFile(path.join(packageDirectory, `protected-${index}.txt`), `protected ${index}\n`),
    ),
  )
  return { directory, scope, packageDirectory }
}

async function snapshotTree(root: string, directory = root): Promise<string[]> {
  return (
    await Promise.all(
      (await readdir(directory, { withFileTypes: true })).map(async (entry) => {
        const file = path.join(directory, entry.name)
        const relative = path.relative(root, file)
        if (entry.isSymbolicLink()) return [`link:${relative}:${await readlink(file)}`]
        if (entry.isDirectory()) return [`directory:${relative}`, ...(await snapshotTree(root, file))]
        return [`file:${relative}:${Buffer.from(await readFile(file)).toString("base64")}`]
      }),
    )
  )
    .flat()
    .sort()
}

const useServerConfig = Effect.fn("test.useServerConfig")(function* (config: (url: string) => Partial<ConfigV1.Info>) {
  const { directory: dir } = yield* TestInstance
  const llm = yield* TestLLMServer
  yield* writeConfig(dir, config(llm.url))
  return { dir, llm }
})

function alive(pid: number) {
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

// Wait for a session's runner to enter a busy state. SessionStatus is flipped
// inside Runner.startShell's serialized transition, so cancel can't no-op once
// we observe it.
const waitForBusy = (sessionID: SessionID, duration: Duration.Input = "2 seconds") =>
  pollWithTimeout(
    Effect.gen(function* () {
      const status = yield* SessionStatus.Service
      const s = yield* status.get(sessionID)
      return s.type === "busy" ? (true as const) : undefined
    }),
    `session ${sessionID} never became busy`,
    duration,
  )

function lastUserContent(input: Record<string, unknown>) {
  if (!Array.isArray(input.messages)) throw new Error("expected provider message array")
  const message = input.messages.findLast(
    (item) => typeof item === "object" && item !== null && "role" in item && item.role === "user",
  )
  return JSON.stringify(message)
}

function lastProviderMessage(input: Record<string, unknown>) {
  if (!Array.isArray(input.messages)) throw new Error("expected provider message array")
  return input.messages.at(-1)
}

const hasBash = Effect.sync(() => Bun.which("bash") !== null)

const deferredAsPromise = <A>(deferred: Deferred.Deferred<A>): PromiseLike<A> => ({
  then: (onfulfilled, onrejected) => {
    Effect.runFork(
      Deferred.await(deferred).pipe(
        Effect.match({
          onFailure: (error) => {
            onrejected?.(error)
          },
          onSuccess: (value) => {
            onfulfilled?.(value)
          },
        }),
      ),
    )
    return deferredAsPromise(deferred) as PromiseLike<never>
  },
})

function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

function failingChatTransformPlugin() {
  let failNext = false
  const layer = Layer.succeed(
    Plugin.Service,
    Plugin.Service.of({
      init: () => Effect.void,
      list: () => Effect.succeed([]),
      trigger: (name, _input, output) =>
        Effect.gen(function* () {
          if (name !== "experimental.chat.messages.transform" || !failNext) return output
          failNext = false
          return yield* Effect.die(new Error("prompt preparation failed before provider start"))
        }),
    } satisfies Plugin.Interface),
  )
  return { layer, fail: () => (failNext = true) }
}

const preProviderFailurePlugin = failingChatTransformPlugin()
const preProviderFailurePrompt = promptTests(makeHttp({ plugin: preProviderFailurePlugin.layer }))

function busyStatusGate(skipBusy = 0) {
  const entered = defer<void>()
  const release = defer<void>()
  const statuses = new Map<SessionID, SessionStatus.Info>()
  let block = true
  let remainingSkips = skipBusy
  const status = Layer.succeed(
    SessionStatus.Service,
    SessionStatus.Service.of({
      get: (sessionID) => Effect.succeed(statuses.get(sessionID) ?? { type: "idle" as const }),
      list: () => Effect.succeed(new Map(statuses)),
      set: (sessionID, value) =>
        Effect.gen(function* () {
          if (value.type === "idle") {
            statuses.delete(sessionID)
            return
          }
          statuses.set(sessionID, value)
          if (value.type !== "busy" || !block) return
          if (remainingSkips > 0) {
            remainingSkips--
            return
          }
          block = false
          entered.resolve()
          yield* Effect.promise(() => release.promise)
        }),
    }),
  )
  return { entered, release, status }
}

const firstLoad = busyStatusGate()
const firstLoadPrompt = promptTests(makeHttp({ status: firstLoad.status }))
const rootlessLoopGate = busyStatusGate()
const rootlessLoopPrompt = promptTests(makeHttp({ status: rootlessLoopGate.status }))
const shellQueuedLoopGate = busyStatusGate(1)
const shellQueuedLoopPrompt = promptTests(makeHttp({ status: shellQueuedLoopGate.status }))

const directProjectionEntered = defer<void>()
const directProjectionRelease = defer<void>()
let gateDirectProjection = false
const directProjectionCompaction = Layer.succeed(
  SessionCompaction.Service,
  SessionCompaction.Service.of({
    isOverflow: () => {
      if (!gateDirectProjection) return Effect.succeed(false)
      gateDirectProjection = false
      directProjectionEntered.resolve()
      return Effect.promise(() => directProjectionRelease.promise).pipe(Effect.as(false))
    },
    prune: () => Effect.void,
    process: () => Effect.succeed("stop" as const),
    create: () => Effect.succeed(MessageID.ascending()),
  }),
)
const directProjectionPrompt = promptTests(makeHttp({ compaction: directProjectionCompaction }))

const succeedVoid = (deferred: Deferred.Deferred<void>) => {
  Effect.runSync(Deferred.succeed(deferred, void 0).pipe(Effect.ignore))
}

const user = Effect.fn("test.user")(function* (sessionID: SessionID, text: string) {
  const session = yield* Session.Service
  const msg = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: msg.id,
    sessionID,
    type: "text",
    text,
  })
  return msg
})

const seed = Effect.fn("test.seed")(function* (sessionID: SessionID, opts?: { finish?: string }) {
  const session = yield* Session.Service
  const msg = yield* user(sessionID, "hello")
  const assistant: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    parentID: msg.id,
    sessionID,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    time: { created: Date.now() },
    ...(opts?.finish ? { finish: opts.finish } : {}),
  }
  yield* session.updateMessage(assistant)
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: assistant.id,
    sessionID,
    type: "text",
    text: "hi there",
  })
  return { user: msg, assistant }
})

const addSubtask = (sessionID: SessionID, messageID: MessageID, model = ref) =>
  Effect.gen(function* () {
    const session = yield* Session.Service
    yield* session.updatePart({
      id: PartID.ascending(),
      messageID,
      sessionID,
      type: "subtask",
      prompt: "look into the cache key path",
      description: "inspect bug",
      agent: "general",
      model,
    })
  })

const boot = Effect.fn("test.boot")(function* (input?: { title?: string }) {
  const config = yield* Config.Service
  const prompt = yield* SessionPrompt.Service
  const run = yield* SessionRunState.Service
  const sessions = yield* Session.Service
  yield* config.get()
  const chat = yield* sessions.create(input ?? { title: "Pinned" })
  return { prompt, run, sessions, chat }
})

const seedUser = Effect.fn("test.seedUser")(function* (input: Omit<SessionPrompt.PromptInput, "noReply">) {
  const prompt = yield* SessionPrompt.Service
  const sessions = yield* Session.Service
  const message = yield* prompt.prompt({ ...input, noReply: true })
  if (message.info.role !== "user") throw new Error("expected a user message")
  const info = { ...message.info, noReply: false }
  yield* sessions.updateMessage(info)
  return { info, parts: message.parts }
})

// These prefix-only defaults are separate from the tokenized current-producer captures below.
test("holds each filed default marker only with its required framing", () => {
  expect(MachineMessage.classify("[fm-from-peer]\x1f peer-name\x1f /peer/home\x1f p1-2-abcd\x1f peer request")).toBe(
    "hold",
  )
  expect(MachineMessage.classify("[fm-from-firstmate]\x1f request from firstmate")).toBe("hold")
  expect(MachineMessage.classify("\x1f daemon request")).toBe("hold")
  expect(MachineMessage.classify("WATCHER FIRED [turn-ended]")).toBe("hold")
  expect(MachineMessage.classify("OBSERVER: follow this direction")).toBe("hold")

  for (const message of [
    "[fm-from-peer] peer request",
    "[fm-from-firstmate] request from firstmate",
    "human text [fm-from-peer]\x1f peer request",
    "human text [fm-from-firstmate]\x1f request from firstmate",
    " \x1f daemon request",
    "human text WATCHER FIRED [turn-ended]",
    "WATCHER FIRED turn-ended",
    "observer: follow this direction",
    "human text OBSERVER: follow this direction",
  ])
    expect(MachineMessage.classify(message)).toBeUndefined()
})

// The inline capture is re-tokenized by fm_message_level_token/heartbeat_build_typed_message at b6efb8eb;
// the summary uses heartbeat_build_bounded_long_summary with the tracked complete-solutions duty.
// The exact-budget file is a typed-builder boundary case; the legacy capture is only for old fallback coverage.
// The 701-character payload and its summary are paired outputs from the same typed-builder input.
test("classifies producer-emitted heartbeat fixtures and exact budget boundaries", async () => {
  const heartbeat = await Bun.file(path.join(import.meta.dir, "fixtures", "fleet-heartbeat.txt")).text()
  const summary = await Bun.file(path.join(import.meta.dir, "fixtures", "fleet-heartbeat-summary.txt")).text()
  const boundary = await Bun.file(path.join(import.meta.dir, "fixtures", "fleet-heartbeat-inline-boundary.txt")).text()
  const overInline = await Bun.file(path.join(import.meta.dir, "fixtures", "fleet-heartbeat-overinline.txt")).text()
  const overInlineSummary = await Bun.file(
    path.join(import.meta.dir, "fixtures", "fleet-heartbeat-overinline-summary.txt"),
  ).text()
  const legacyHeartbeat = await Bun.file(path.join(import.meta.dir, "fixtures", "legacy-heartbeat-20260927.txt")).text()
  const heartbeatToken = "[fm-level:nudge:heartbeat]\x1f "
  const summaryToken = "[fm-level:nudge:heartbeat.duty.complete-solutions]\x1f "
  const boundaryToken = "[fm-level:nudge:heartbeat.duty.complete-solutions]\x1f "
  const receiptPattern = / \[fm-heartbeat-receipt:[a-zA-Z0-9._-]+\]$/
  const heartbeatMarker = { type: "fleet-heartbeat" } as const
  expect(heartbeat.startsWith(heartbeatToken)).toBe(true)
  expect(summary.startsWith(`${summaryToken}Heartbeat summary: `)).toBe(true)
  expect(boundary.startsWith(boundaryToken)).toBe(true)
  expect(overInline.startsWith(`${summaryToken}2026-09-27T`)).toBe(true)
  expect(overInlineSummary.startsWith(`${summaryToken}Heartbeat summary: `)).toBe(true)
  expect(MachineMessage.classify(heartbeat)).toBe("hold")
  expect(MachineMessage.classify(summary)).toBe("hold")
  expect(MachineMessage.classify(legacyHeartbeat)).toBe("hold")
  expect(MachineMessage.classify(boundary)).toBe("hold")

  const boundaryBody = boundary.slice(boundaryToken.length)
  expect(Array.from(boundaryBody)).toHaveLength(700)
  const overInlineBody = overInline.slice(summaryToken.length)
  expect(Array.from(overInlineBody)).toHaveLength(701)
  expect(MachineMessage.classify(boundary, { critical: [heartbeatMarker] })).toBe("critical")
  expect(MachineMessage.classify(overInlineBody)).toBeUndefined()
  expect(MachineMessage.classify(overInline, { critical: [heartbeatMarker] })).toBe("hold")
  expect(MachineMessage.classify(summary, { critical: [heartbeatMarker] })).toBe("critical")
  expect(MachineMessage.classify(overInlineSummary, { critical: [heartbeatMarker] })).toBe("critical")

  const boundaryReceipt = boundaryBody.match(receiptPattern)?.[0]
  const summaryReceipt = summary.match(receiptPattern)?.[0]
  const overInlineReceipt = overInline.match(receiptPattern)?.[0]
  const overInlineSummaryReceipt = overInlineSummary.match(receiptPattern)?.[0]
  const summaryBoundary = summary.indexOf("… Full message:")
  const overInlineSummaryBoundary = overInlineSummary.indexOf("… Full message:")
  if (
    !boundaryReceipt ||
    !summaryReceipt ||
    !overInlineReceipt ||
    overInlineSummaryReceipt !== overInlineReceipt ||
    summaryBoundary === -1 ||
    overInlineSummaryBoundary === -1
  )
    throw new Error("producer heartbeat fixture lost its receipt or summary boundary")
  const shortSummary = `${summary.slice(0, summaryBoundary - 1)}${summary.slice(summaryBoundary)}`
  const oversizedSummary = `${summary.slice(0, summaryBoundary)}x${summary.slice(summaryBoundary)}`
  const arbitrarySummary = `${summaryToken}Heartbeat summary: ordinary user text${summary.slice(summaryBoundary)}`
  for (const message of [
    legacyHeartbeat.replace(receiptPattern, ""),
    `${legacyHeartbeat} trailing text`,
    `${summary.slice(0, -summaryReceipt.length)}`,
    shortSummary,
    oversizedSummary,
    arbitrarySummary,
    `human note ${legacyHeartbeat}`,
  ])
    expect(MachineMessage.classify(message, { critical: [heartbeatMarker] })).not.toBe("critical")
  expect(MachineMessage.classify(legacyHeartbeat.replace(receiptPattern, ""))).toBeUndefined()
  expect(MachineMessage.classify(legacyHeartbeat.replace(receiptPattern, "") + " trailing text")).toBeUndefined()
})

test("classifies level-token framing across supported identity prefixes", () => {
  const levelToken = (level: string, key?: string) => `[fm-level:${level}${key ? `:${key}` : ""}]\x1f `
  const peerEnvelope = "[fm-from-peer]\x1f peer-name\x1f /peer/home\x1f p1-2-abcd\x1f "
  const peerRequest = `${peerEnvelope}${levelToken("request")}review the branch`
  const peerInfo = `${peerEnvelope}${levelToken("info")}peer status`
  const peerCritical = `${peerEnvelope}${levelToken("critical")}disk alert`
  const firstmateRequest = `[fm-from-firstmate]\x1f${levelToken("request")}inspect the issue`
  const firstmateCritical = `[fm-from-firstmate]\x1f${levelToken("critical")}disk alert`
  const daemonCritical = `\x1f${levelToken("critical")}disk alert`
  const observerDirective = `OBSERVER: ${levelToken("directive")}look up the open issue before editing`
  const watcherRequest = `${levelToken("request")}WATCHER FIRED [failure-1] - handle the retained watcher episode`
  const watcherWake = `${levelToken("nudge", "watcher.wake")}WATCHER FIRED [wake-1] - drain queued wakes`
  const turnendNudge = `${levelToken("nudge", "turnend.blind")}TURN WOULD END BLIND - supervision is off`
  const malformedPeerCritical = `${"[fm-from-peer]\x1f peer-name\x1f /peer/home\x1f not-a-msgid\x1f "}${levelToken("critical")}disk alert`

  for (const message of [
    peerRequest,
    peerInfo,
    firstmateRequest,
    observerDirective,
    watcherRequest,
    watcherWake,
    turnendNudge,
  ])
    expect(MachineMessage.classify(message)).toBe("hold")
  expect(MachineMessage.classify(peerCritical)).toBe("critical")
  expect(MachineMessage.classify(firstmateCritical)).toBe("critical")
  expect(MachineMessage.classify(daemonCritical)).toBe("critical")

  for (const message of [
    "[fm-level:unknown]\x1f WATCHER FIRED [unknown]",
    "[fm-level:nudge:BadKey]\x1f WATCHER FIRED [bad key]",
    "[fm-level:request\x1f WATCHER FIRED [unterminated]",
    "human note [fm-level:critical]\x1f disk alert",
  ])
    expect(MachineMessage.classify(message)).toBeUndefined()
  expect(MachineMessage.classify(malformedPeerCritical)).toBe("hold")

  const customHold = { type: "prefix", value: "CUSTOM-HOLD:" } as const
  const customCritical = { type: "prefix", value: "CUSTOM-CRITICAL:" } as const
  expect(MachineMessage.classify("CUSTOM-HOLD: deploy update", { hold: [customHold] })).toBe("hold")
  expect(MachineMessage.classify("CUSTOM-CRITICAL: alert", { critical: [customCritical] })).toBe("critical")
})

it.instance("imports successive message batches with one persisted session admission order", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const fs = yield* FSUtil.Service
    const sessions = yield* Session.Service
    const ctx = yield* InstanceRef
    if (!ctx) throw new Error("expected an instance context")
    const localImportShare = Layer.mock(ShareNext.Service, { url: () => Effect.succeed("") })

    const session = yield* sessions.create({ title: "Imported admission order" })
    const firstID = MessageID.make("msg_import_first")
    const secondID = MessageID.make("msg_import_second")
    const file = path.join(ctx.directory, "session-import.json")
    const first = {
      info: {
        id: firstID,
        sessionID: session.id,
        role: "user" as const,
        time: { created: 200 },
        agent: "build",
        model: { providerID: ref.providerID, modelID: ref.modelID },
      },
      parts: [],
    }
    const second = {
      info: {
        id: secondID,
        sessionID: session.id,
        role: "user" as const,
        time: { created: 100 },
        agent: "build",
        model: { providerID: ref.providerID, modelID: ref.modelID },
      },
      parts: [],
    }

    yield* fs.writeJson(file, { info: session, messages: [first] })
    yield* runImport(file, ctx).pipe(Effect.provide(localImportShare))
    yield* fs.writeJson(file, { info: session, messages: [second] })
    yield* runImport(file, ctx).pipe(Effect.provide(localImportShare))

    const imported = yield* db
      .select({ id: MessageTable.id, admission_seq: MessageTable.admission_seq })
      .from(MessageTable)
      .where(eq(MessageTable.session_id, session.id))
      .orderBy(MessageTable.admission_seq)
      .all()
      .pipe(Effect.orDie)
    expect(imported.map((message) => message.id)).toEqual([firstID, secondID])
    expect(imported[1]?.admission_seq).toBeGreaterThan(imported[0]?.admission_seq ?? 0)

    yield* db.insert(EventRetentionTable).values({
      aggregate_id: session.id,
      state: "redacting",
      progress_table: "event",
      progress_id: "evt_import_retention",
      evidence: {},
      time_started: 1,
      time_updated: 1,
    })
    const blockedID = MessageID.make("msg_import_blocked")
    yield* fs.writeJson(file, {
      info: session,
      messages: [
        {
          info: {
            id: blockedID,
            sessionID: session.id,
            role: "user" as const,
            time: { created: 300 },
            agent: "build",
            model: { providerID: ref.providerID, modelID: ref.modelID },
          },
          parts: [
            {
              id: PartID.make("prt_import_blocked"),
              sessionID: session.id,
              messageID: blockedID,
              type: "text" as const,
              text: "blocked import sentinel",
            },
          ],
        },
      ],
    })
    const blockedImport = yield* runImport(file, ctx).pipe(Effect.provide(localImportShare), Effect.exit)
    expect(Exit.isFailure(blockedImport)).toBe(true)
    expect(
      yield* db
        .select({ id: MessageTable.id })
        .from(MessageTable)
        .where(eq(MessageTable.session_id, session.id))
        .all(),
    ).toHaveLength(2)
    expect(
      yield* db
        .select({ id: PartTable.id })
        .from(PartTable)
        .where(eq(PartTable.session_id, session.id))
        .all(),
    ).toHaveLength(0)
  }),
)

it.instance(
  "keeps noReply input out of automatic turns and preserves real prompts",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const active = yield* sessions.create({ title: "No reply during active turn" })
      const response = yield* Deferred.make<void>()
      yield* llm.push(reply().wait(deferredAsPromise(response)).text("task finished").stop().item())

      const run = yield* prompt
        .prompt({
          sessionID: active.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "active task" }],
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "active task did not reach the provider", "30 seconds")

      const reminder = yield* prompt.prompt({
        sessionID: active.id,
        agent: "build",
        model: ref,
        noReply: true,
        parts: [{ type: "text", text: "synthetic bookkeeping reminder" }],
      })
      expect(reminder.info.role).toBe("user")
      if (reminder.info.role !== "user") throw new Error("expected the bookkeeping user message")
      expect(reminder.info.noReply).toBe(true)

      yield* Deferred.succeed(response, void 0)
      yield* awaitWithTimeout(Fiber.await(run), "active task did not finish", "30 seconds")
      expect(yield* llm.calls).toBe(1)

      const rootless = yield* sessions.create({ title: "No reply rootless turn" })
      const rootlessReminder = yield* prompt.prompt({
        sessionID: rootless.id,
        agent: "build",
        model: ref,
        noReply: true,
        parts: [{ type: "text", text: "rootless bookkeeping reminder" }],
      })
      if (rootlessReminder.info.role !== "user") throw new Error("expected an admit-only user message")
      expect(rootlessReminder.info.noReply).toBe(true)
      expect(yield* llm.calls).toBe(1)
      const loop = yield* prompt.loop({ sessionID: rootless.id }).pipe(Effect.forkChild)
      expect(
        Exit.isSuccess(
          yield* awaitWithTimeout(Fiber.await(loop), "explicit rootless loop did not finish", "30 seconds"),
        ),
      ).toBe(true)
      expect(yield* llm.calls).toBe(1)

      const realID = MessageID.make("msg_real_prompt_after_no_reply")
      yield* llm.text("real prompt handled")
      const real = yield* prompt
        .prompt({
          sessionID: rootless.id,
          messageID: realID,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "real prompt after bookkeeping" }],
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(2), "real prompt after bookkeeping did not reach the provider", "30 seconds")
      const realRequest = (yield* llm.inputs)[1]
      if (!realRequest) throw new Error("expected the real prompt provider request")
      expect(lastUserContent(realRequest)).toContain("real prompt after bookkeeping")
      expect(
        Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(real), "real prompt did not finish", "30 seconds")),
      ).toBe(true)
      expect(yield* llm.calls).toBe(2)
    }),
  60_000,
)

it.instance(
  "completed prompt releases its run control before the next prompt",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Run control cleanup" })
      const firstID = MessageID.make("msg_control_first")
      const secondID = MessageID.make("msg_control_second")
      yield* llm.push(
        reply().text("first prompt handled").stop().item(),
        reply().text("second prompt handled").stop().item(),
      )

      const first = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID: firstID,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "first prompt" }],
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "first prompt did not reach the provider", "30 seconds")
      expect(
        Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(first), "first prompt did not finish", "30 seconds")),
      ).toBe(true)

      const second = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID: secondID,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "second prompt" }],
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(2), "second prompt did not reach the provider", "30 seconds")
      const request = (yield* llm.inputs)[1]
      if (!request) throw new Error("expected the second provider request")
      expect(lastUserContent(request)).toContain("second prompt")
      expect(
        Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(second), "second prompt did not finish", "30 seconds")),
      ).toBe(true)
      expect(yield* llm.calls).toBe(2)
    }),
  60_000,
)

firstLoadPrompt.instance(
  "keeps marked input out of the initiating prompt before the first history load",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Pinned" })
      const taskID = MessageID.make("msg_first_load_task")
      const heldID = MessageID.make("msg_first_load_held")

      yield* Effect.gen(function* () {
        yield* llm.push(reply().text("task finished").stop().item(), reply().text("held handled").stop().item())
        const task = yield* prompt
          .prompt({
            sessionID: session.id,
            messageID: taskID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "original task" }],
          })
          .pipe(Effect.forkChild)
        yield* awaitWithTimeout(
          Effect.promise(() => firstLoad.entered.promise),
          "first load did not gate",
          "10 seconds",
        )

        const held = yield* prompt
          .prompt({
            sessionID: session.id,
            messageID: heldID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "[fm-from-peer]\x1f committed before the first load" }],
          })
          .pipe(Effect.forkChild)
        const queued = yield* pollWithTimeout(
          queue.list(session.id).pipe(Effect.map((items) => items.find((item) => item.input.messageID === heldID))),
          "marked first-load input was not queued",
          "10 seconds",
        )
        expect(queued.delivery).toBe("queue")

        yield* Effect.sync(() => firstLoad.release.resolve())
        yield* awaitWithTimeout(llm.wait(1), "original task did not reach the provider", "10 seconds")
        const taskRequest = (yield* llm.inputs)[0]
        if (!taskRequest) throw new Error("expected the original task request")
        expect(JSON.stringify(taskRequest.messages)).toContain("original task")
        expect(JSON.stringify(taskRequest.messages)).not.toContain("committed before the first load")

        yield* awaitWithTimeout(llm.wait(2), "held input did not follow the original task", "10 seconds")
        const heldRequest = (yield* llm.inputs)[1]
        if (!heldRequest) throw new Error("expected the held follow-up request")
        expect(JSON.stringify(heldRequest.messages)).toContain("committed before the first load")

        expect(
          Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(task), "task prompt did not finish", "10 seconds")),
        ).toBe(true)
        expect(
          Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(held), "held prompt did not finish", "10 seconds")),
        ).toBe(true)
        expect(yield* llm.calls).toBe(2)
      }).pipe(Effect.ensuring(Effect.sync(() => firstLoad.release.resolve())))
    }),
  60_000,
)

rootlessLoopPrompt.instance(
  "anchors rootless loop initialization with persisted admission order",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Rootless admission order" })
      const earlier = yield* seedUser({
        sessionID: session.id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "earlier admitted task" }],
      })
      const root = yield* seedUser({
        sessionID: session.id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "latest admitted task" }],
      })
      yield* sessions.updateMessage({ ...earlier.info, time: { created: 300 } })
      yield* sessions.updateMessage({ ...root.info, time: { created: 100 } })
      const admission = yield* MessageV2.admission(session.id)
      expect(admission.order.get(earlier.info.id)).toBeLessThan(admission.order.get(root.info.id) ?? Infinity)

      const markedGate = yield* Deferred.make<void>()
      const markedText = "[fm-from-firstmate]\x1f queued during rootless initialization"
      yield* llm.push(
        reply().text("rootless task finished").stop().item(),
        reply().wait(deferredAsPromise(markedGate)).text("marked message handled").stop().item(),
      )
      const run = yield* prompt.loop({ sessionID: session.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(
        Effect.promise(() => rootlessLoopGate.entered.promise),
        "rootless loop did not reach its first load boundary",
        "10 seconds",
      )

      const markedID = MessageID.make("msg_rootless_marked")
      const marked = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID: markedID,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: markedText }],
        })
        .pipe(Effect.forkChild)
      const pending = yield* pollWithTimeout(
        queue.list(session.id).pipe(Effect.map((items) => items.find((item) => item.input.messageID === markedID))),
        "marked rootless input was not admitted",
        "10 seconds",
      )
      expect(pending.delivery).toBe("queue")

      yield* Effect.sync(() => rootlessLoopGate.release.resolve())
      yield* awaitWithTimeout(llm.wait(1), "rootless loop did not reach the provider", "10 seconds")
      const taskRequest = (yield* llm.inputs)[0]
      if (!taskRequest) throw new Error("expected the rootless task request")
      expect(JSON.stringify(taskRequest.messages)).toContain("latest admitted task")
      expect(JSON.stringify(taskRequest.messages)).not.toContain("queued during rootless initialization")
      expect(
        (yield* sessions.messages({ sessionID: session.id })).some(
          (message) => message.info.role === "assistant" && message.info.parentID === root.info.id,
        ),
      ).toBe(true)

      yield* awaitWithTimeout(llm.wait(2), "marked message did not run after the rootless task", "10 seconds")
      const markedRequest = (yield* llm.inputs)[1]
      if (!markedRequest) throw new Error("expected the marked follow-up request")
      expect(JSON.stringify(markedRequest.messages)).toContain("queued during rootless initialization")
      yield* Deferred.succeed(markedGate, void 0)
      yield* awaitWithTimeout(Fiber.await(run), "rootless loop did not finish", "10 seconds")
      const markedExit = yield* awaitWithTimeout(Fiber.await(marked), "marked prompt did not finish", "10 seconds")
      expect(Exit.isSuccess(markedExit)).toBe(true)
      expect(yield* llm.calls).toBe(2)
    }).pipe(Effect.ensuring(Effect.sync(() => rootlessLoopGate.release.resolve()))),
  60_000,
)

preProviderFailurePrompt.instance("allows retry after preparation fails before provider start", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({ title: "Retry before provider start" })
    const messageID = MessageID.make("msg_retry_before_provider_start")

    preProviderFailurePlugin.fail()
    const failed = yield* prompt
      .prompt({
        sessionID: session.id,
        messageID,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "retry this admitted input" }],
      })
      .pipe(Effect.forkChild)
    const failedExit = yield* awaitWithTimeout(
      Fiber.await(failed),
      "pre-provider prompt failure did not finish",
      "30 seconds",
    )
    expect(Exit.isFailure(failedExit)).toBe(true)
    expect(yield* llm.calls).toBe(0)
    const failedMessages = yield* sessions.messages({ sessionID: session.id })
    const failedAssistant = failedMessages.findLast((message) => message.info.role === "assistant")
    expect(failedAssistant?.info.role).toBe("assistant")
    if (failedAssistant?.info.role === "assistant") expect(failedAssistant.info.error?.name).toBe("UnknownError")

    yield* llm.text("retried input handled")
    const retry = yield* prompt.loop({ sessionID: session.id, messageID }).pipe(Effect.forkChild)
    yield* awaitWithTimeout(llm.wait(1), "failed input did not reach its retry", "30 seconds")
    const request = (yield* llm.inputs)[0]
    if (!request) throw new Error("expected the retry provider request")
    expect(lastUserContent(request)).toContain("retry this admitted input")
    expect(
      Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(retry), "provider retry did not finish", "30 seconds")),
    ).toBe(true)
    expect(yield* llm.calls).toBe(1)
  }),
  60_000,
)

it.instance(
  "orders marked watcher and turn-end prompts by durable queue admission",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Pinned" })
      const taskGate = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() => Deferred.succeed(taskGate, void 0).pipe(Effect.ignore))
      const firstText =
        "[fm-level:nudge:watcher.wake]\x1f WATCHER FIRED [wake-1] - drain queued wakes with bin/fm-wake-drain.sh, handle the reported wake, and continue normal supervision\n\nsignal: waiting: peer reply"
      const secondText =
        "[fm-level:nudge:turnend.blind]\x1f TURN WOULD END BLIND - supervision is off. Resume supervision according to the session-start operating block before ending the turn.\n\nwatcher: arm exited"

      yield* llm.push(
        reply().wait(deferredAsPromise(taskGate)).text("task finished").stop().item(),
        reply().text("first marked handled").stop().item(),
        reply().text("second marked handled").stop().item(),
      )
      const task = yield* prompt
        .prompt({ sessionID: session.id, agent: "build", model: ref, parts: [{ type: "text", text: "original task" }] })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "provider did not receive the active task", "10 seconds")

      const first = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: firstText }],
        })
        .pipe(Effect.forkChild)
      yield* pollWithTimeout(
        queue.list(session.id).pipe(Effect.map((items) => (items.length === 1 ? items : undefined))),
        "first queued prompt was not admitted",
        "10 seconds",
      )
      const second = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: secondText }],
        })
        .pipe(Effect.forkChild)
      const pending = yield* pollWithTimeout(
        queue.list(session.id).pipe(Effect.map((items) => (items.length === 2 ? items : undefined))),
        "both queued prompts were not admitted",
        "10 seconds",
      )
      const pendingTexts = pending.map((item) =>
        item.input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join(" "),
      )
      expect(pending.map((item) => item.delivery)).toEqual(["queue", "queue"])
      expect(pendingTexts).toEqual([firstText, secondText])
      const firstPending = pending[0]
      const secondPending = pending[1]
      if (!firstPending || !secondPending) throw new Error("expected both pending marked prompts")
      expect(firstPending.seq).toBeLessThan(secondPending.seq)

      const active = (yield* llm.inputs)[0]
      if (!active) throw new Error("expected the active task request")
      expect(JSON.stringify(active.messages)).not.toContain(firstText)
      expect(JSON.stringify(active.messages)).not.toContain(secondText)

      yield* Deferred.succeed(taskGate, void 0)
      yield* awaitWithTimeout(llm.wait(2), "first marked prompt was not promoted", "10 seconds")
      const firstTurn = (yield* llm.inputs)[1]
      if (!firstTurn) throw new Error("expected the first queued provider request")
      expect(lastUserContent(firstTurn)).toContain("WATCHER FIRED [wake-1]")

      yield* awaitWithTimeout(llm.wait(3), "second marked prompt was not promoted", "10 seconds")
      const secondTurn = (yield* llm.inputs)[2]
      if (!secondTurn) throw new Error("expected the second queued provider request")
      expect(lastUserContent(secondTurn)).toContain("TURN WOULD END BLIND")

      expect(Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(task), "task did not finish", "10 seconds"))).toBe(true)
      expect(
        Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(first), "first marked prompt did not finish", "10 seconds")),
      ).toBe(true)
      expect(
        Exit.isSuccess(
          yield* awaitWithTimeout(Fiber.await(second), "second marked prompt did not finish", "10 seconds"),
        ),
      ).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(3)
    }),
  60_000,
)

it.instance(
  "holds a TUI-marked message after synthetic editor context",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Pinned" })
      const taskGate = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() => Deferred.succeed(taskGate, void 0).pipe(Effect.ignore))
      yield* llm.push(
        reply().wait(deferredAsPromise(taskGate)).text("task finished").stop().item(),
        reply().text("TUI machine message handled").stop().item(),
      )

      const task = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "original task" }],
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "provider did not receive the active task", "10 seconds")
      const markedID = MessageID.ascending()
      const markedText = "[fm-from-firstmate]\x1f TUI marked message"
      const marked = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID: markedID,
          agent: "build",
          model: ref,
          parts: [
            { type: "text", text: "Selected editor context from TUI", synthetic: true },
            { type: "text", text: markedText },
          ],
        })
        .pipe(Effect.forkChild)
      const pending = yield* pollWithTimeout(
        queue.list(session.id).pipe(Effect.map((items) => items.find((item) => item.input.messageID === markedID))),
        "TUI marked text after synthetic editor context was not held",
        "10 seconds",
      )
      expect(pending.delivery).toBe("queue")

      const active = (yield* llm.inputs)[0]
      if (!active) throw new Error("expected the active task request")
      expect(JSON.stringify(active.messages)).not.toContain("TUI marked message")

      yield* Deferred.succeed(taskGate, void 0)
      yield* awaitWithTimeout(llm.wait(2), "TUI held message did not reach its own turn", "10 seconds")
      const next = (yield* llm.inputs)[1]
      if (!next) throw new Error("expected the TUI message provider request")
      expect(lastUserContent(next)).toContain("TUI marked message")

      const taskExit = yield* awaitWithTimeout(Fiber.await(task), "task did not finish", "10 seconds")
      const markedExit = yield* awaitWithTimeout(
        Fiber.await(marked),
        "TUI prompt did not receive its reply",
        "10 seconds",
      )
      const messages = yield* sessions.messages({ sessionID: session.id })
      const markedUser = messages.find(
        (message) => message.info.role === "user" && message.parts.some((part) => part.type === "text" && part.text === markedText),
      )
      if (!markedUser) throw new Error("expected the promoted TUI user message")
      expect(Exit.isSuccess(taskExit)).toBe(true)
      expect(Exit.isSuccess(markedExit)).toBe(true)
      expect(
        messages.some((message) => message.info.role === "assistant" && message.info.parentID === markedUser.info.id),
      ).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(2)
    }),
  60_000,
)

it.instance(
  "steers live critical-level prompts during a continuation while holding ordinary machine mail",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const toolGate = yield* Deferred.make<void>()
      const stopGate = yield* Deferred.make<void>()
      const heldGate = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        yield* llm.push(
          reply().wait(deferredAsPromise(toolGate)).tool("first", { value: "continue" }).item(),
          reply().wait(deferredAsPromise(stopGate)).text("critical alert handled").stop().item(),
          reply().wait(deferredAsPromise(heldGate)).text("held machine message handled").stop().item(),
        )
        yield* seedUser({
          sessionID: session.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "original task" }],
        })
        const run = yield* prompt.loop({ sessionID: session.id }).pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(1), "provider did not receive the original task", "10 seconds")
        yield* waitForBusy(session.id)

        const heldID = MessageID.ascending()
        const held = yield* prompt
          .prompt({
            sessionID: session.id,
            messageID: heldID,
            agent: "build",
            model: ref,
            delivery: "steer",
            origin: "tui-composer",
            parts: [{ type: "text", text: "OBSERVER: [fm-level:directive]\x1f queued-mail" }],
          })
          .pipe(Effect.forkChild)
        const queuedHeld = yield* pollWithTimeout(
          queue.list(session.id).pipe(Effect.map((items) => items.find((item) => item.input.messageID === heldID))),
          "ordinary machine mail was not queued",
          "10 seconds",
        )
        expect(queuedHeld.delivery).toBe("queue")

        const criticalID = MessageID.ascending()
        const critical = yield* prompt
          .prompt({
            sessionID: session.id,
            messageID: criticalID,
            agent: "build",
            model: ref,
            delivery: "queue",
            origin: "tui-composer",
            parts: [
              {
                type: "text",
                text: "[fm-from-peer]\x1f peer-name\x1f /peer/home\x1f p1-2-abcd\x1f [fm-level:critical]\x1f disk alert",
              },
            ],
          })
          .pipe(Effect.forkChild)
        yield* pollWithTimeout(
          sessions
            .messages({ sessionID: session.id })
            .pipe(
              Effect.map((messages) =>
                messages.some((message) => message.info.role === "user" && message.info.id === criticalID)
                  ? true
                  : undefined,
              ),
            ),
          "live critical-level prompt was not admitted",
          "10 seconds",
        )

        yield* Deferred.succeed(toolGate, void 0)
        yield* awaitWithTimeout(llm.wait(2), "next provider step did not start", "10 seconds")
        const next = (yield* llm.inputs).at(1)
        if (!next) throw new Error("expected the next provider request")
        expect(JSON.stringify(next.messages)).toContain("disk alert")
        expect(JSON.stringify(next.messages)).not.toContain("queued-mail")

        yield* Deferred.succeed(stopGate, void 0)
        yield* awaitWithTimeout(llm.wait(3), "held prompt did not start after the current turn stopped", "10 seconds")
        const heldTurn = (yield* llm.inputs).at(2)
        if (!heldTurn) throw new Error("expected the held provider request")
        expect(JSON.stringify(heldTurn.messages)).toContain("queued-mail")
        const messages = yield* sessions.messages({ sessionID: session.id })
        const heldUser = messages.find(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text.includes("queued-mail")),
        )
        if (!heldUser) throw new Error("expected the promoted held user message")
        expect(
          messages.some((message) => message.info.role === "assistant" && message.info.parentID === heldUser.info.id),
        ).toBe(true)

        yield* Deferred.succeed(heldGate, void 0)
        const runExit = yield* awaitWithTimeout(Fiber.await(run), "session run did not finish", "10 seconds")
        const criticalExit = yield* awaitWithTimeout(
          Fiber.await(critical),
          "critical prompt caller did not finish",
          "10 seconds",
        )
        const heldExit = yield* awaitWithTimeout(Fiber.await(held), "held prompt caller did not finish", "10 seconds")
        expect(Exit.isSuccess(runExit)).toBe(true)
        expect(Exit.isSuccess(criticalExit)).toBe(true)
        expect(Exit.isSuccess(heldExit)).toBe(true)

        expect(
          messages.some((message) => message.info.role === "assistant" && message.info.parentID === criticalID),
        ).toBe(true)
        expect(yield* llm.calls).toBe(3)
      }).pipe(
        Effect.ensuring(
          Effect.all(
            [toolGate, stopGate, heldGate].map((gate) => Deferred.succeed(gate, void 0).pipe(Effect.ignore)),
            {
              discard: true,
            },
          ),
        ),
      )
    }),
  60_000,
)

it.instance(
  "routes configured machine_message_markers through prompt admission",
  () =>
    Effect.gen(function* () {
      const markers = {
        hold: [{ type: "prefix" as const, value: "CUSTOM-HOLD:" }],
        critical: [{ type: "prefix" as const, value: "CUSTOM-CRITICAL:" }],
      }
      const { llm } = yield* useServerConfig((url) => ({ ...providerCfg(url), machine_message_markers: markers }))
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({
        title: "Configured machine markers",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const toolGate = yield* Deferred.make<void>()
      const stopGate = yield* Deferred.make<void>()
      const heldGate = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() =>
        Effect.all(
          [toolGate, stopGate, heldGate].map((gate) => Deferred.succeed(gate, void 0).pipe(Effect.ignore)),
          { discard: true },
        ),
      )

      yield* llm.push(
        reply().wait(deferredAsPromise(toolGate)).tool("first", { value: "continue" }).item(),
        reply().wait(deferredAsPromise(stopGate)).text("configured critical handled").stop().item(),
        reply().wait(deferredAsPromise(heldGate)).text("configured held handled").stop().item(),
      )
      const task = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "original task" }],
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "provider did not receive the configured-marker task", "30 seconds")

      const heldID = MessageID.ascending()
      const heldText = "CUSTOM-HOLD: wait until the task reaches its boundary"
      const held = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID: heldID,
          agent: "build",
          model: ref,
          delivery: "steer",
          origin: "tui-composer",
          parts: [{ type: "text", text: heldText }],
        })
        .pipe(Effect.forkChild)
      // Listed on arrival (#249), it takes the configured classification once prepared.
      const queued = yield* pollWithTimeout(
        queue
          .list(session.id)
          .pipe(
            Effect.map((items) => items.find((item) => item.input.messageID === heldID && item.delivery === "queue")),
          ),
        "configured hold marker did not queue the prompt",
        "30 seconds",
      )
      expect(queued.delivery).toBe("queue")

      const criticalID = MessageID.ascending()
      const criticalText = "CUSTOM-CRITICAL: interrupt at the next eligible step"
      const critical = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID: criticalID,
          agent: "build",
          model: ref,
          delivery: "queue",
          origin: "tui-composer",
          parts: [{ type: "text", text: criticalText }],
        })
        .pipe(Effect.forkChild)
      // A promotion writes the message before it marks the row promoted, so wait until the queue
      // itself stops listing the critical prompt instead of reading the queue once the message lands.
      const admitted = yield* pollWithTimeout(
        Effect.gen(function* () {
          const pending = yield* queue.list(session.id)
          if (pending.some((item) => item.input.messageID === criticalID)) return undefined
          const messages = yield* sessions.messages({ sessionID: session.id })
          return messages.some((message) => message.info.role === "user" && message.info.id === criticalID)
            ? { pending, messages }
            : undefined
        }),
        "configured critical marker did not promote the prompt",
        "30 seconds",
      )
      expect(admitted.pending.map((item) => [item.input.messageID, item.delivery])).toEqual([[heldID, "queue"]])
      expect(
        admitted.messages.some(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === heldText),
        ),
      ).toBe(false)

      yield* Deferred.succeed(toolGate, void 0)
      yield* awaitWithTimeout(llm.wait(2), "configured critical prompt did not reach the next step", "30 seconds")
      const criticalTurn = (yield* llm.inputs)[1]
      if (!criticalTurn) throw new Error("expected the configured critical provider request")
      expect(JSON.stringify(criticalTurn.messages)).toContain(criticalText)
      expect(JSON.stringify(criticalTurn.messages)).not.toContain(heldText)

      yield* Deferred.succeed(stopGate, void 0)
      yield* awaitWithTimeout(llm.wait(3), "configured hold prompt did not reach its own turn", "30 seconds")
      const heldTurn = (yield* llm.inputs)[2]
      if (!heldTurn) throw new Error("expected the configured held provider request")
      expect(JSON.stringify(heldTurn.messages)).toContain(heldText)

      yield* Deferred.succeed(heldGate, void 0)
      expect(Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(task), "task did not finish", "30 seconds"))).toBe(true)
      expect(
        Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(critical), "critical prompt did not finish", "30 seconds")),
      ).toBe(true)
      expect(
        Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(held), "held prompt did not finish", "30 seconds")),
      ).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(3)
    }),
  60_000,
)

// swxtchio/swx-opencode#249: a prompt is admitted before its config is read,
// so its first identity check cannot hold it to a delivery; it is held to its
// classified delivery once it has one, as the original was.
it.instance(
  "a retry admitted before classification follows its original, and one under another delivery conflicts",
  () =>
    Effect.gen(function* () {
      const markers = { hold: [{ type: "prefix" as const, value: "CUSTOM-HOLD:" }] }
      const { llm } = yield* useServerConfig((url) => ({ ...providerCfg(url), machine_message_markers: markers }))
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Classified retry" })
      yield* queue.hold(session.id, "review")
      const send = (messageID: MessageID, delivery: SessionQueue.Delivery, text: string) =>
        prompt
          .prompt({
            sessionID: session.id,
            messageID,
            agent: "build",
            model: ref,
            delivery,
            parts: [{ type: "text", text }],
          })
          .pipe(Effect.forkChild)
      const listed = (messageID: MessageID, delivery: SessionQueue.Delivery) =>
        pollWithTimeout(
          queue
            .list(session.id)
            .pipe(
              Effect.map((items) =>
                items.find((item) => item.input.messageID === messageID && item.delivery === delivery),
              ),
            ),
          `${messageID} was not queued as ${delivery}`,
          "30 seconds",
        )

      // Only the configured marker makes this one queued; its exact retry presents steer again.
      const heldID = MessageID.ascending()
      const held = yield* send(heldID, "steer", "CUSTOM-HOLD: wait for the boundary")
      yield* listed(heldID, "queue")
      const retried = yield* send(heldID, "steer", "CUSTOM-HOLD: wait for the boundary")

      // The same ID with the same words under another delivery is another prompt.
      const plainID = MessageID.ascending()
      const plain = yield* send(plainID, "queue", "check the build")
      yield* listed(plainID, "queue")
      const conflict = yield* awaitWithTimeout(
        Fiber.await(yield* send(plainID, "steer", "check the build")),
        "a retry under another delivery was not refused",
        "30 seconds",
      )
      expect(Exit.isFailure(conflict) && String(Cause.squash(conflict.cause))).toContain(
        "Conflicting prompt message ID",
      )

      yield* llm.push(reply().text("held handled").stop().item(), reply().text("plain handled").stop().item())
      expect(yield* queue.release(session.id, "review")).toBe(true)
      for (const [fiber, what] of [
        [held, "the held prompt"],
        [retried, "its retry"],
        [plain, "the plain prompt"],
      ] as const)
        expect(
          Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(fiber), `${what} did not finish`, "30 seconds")),
        ).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
    }),
  60_000,
)

it.instance(
  "answers unmarked steers together in admission order before held mail",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const toolFile = path.join(dir, "steer-step.txt")
      yield* writeText(toolFile, "steer continuation")
      const session = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const toolGate = yield* Deferred.make<void>()
      const firstSteerGate = yield* Deferred.make<void>()
      const firstID = MessageID.make("msg_z_steer_first")
      const secondID = MessageID.make("msg_a_steer_second")
      const heldID = MessageID.make("msg_steer_held")

      yield* Effect.gen(function* () {
        yield* llm.push(
          reply().wait(deferredAsPromise(toolGate)).tool("glob", { pattern: "steer-step.txt" }).item(),
          reply().wait(deferredAsPromise(firstSteerGate)).text("captains handled together").stop().item(),
          reply().text("held message handled").stop().item(),
        )
        yield* seedUser({
          sessionID: session.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "original task" }],
        })
        const run = yield* prompt.loop({ sessionID: session.id }).pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(1), "original provider request did not start", "10 seconds")

        const first = yield* prompt
          .prompt({
            sessionID: session.id,
            messageID: firstID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "captain one" }],
          })
          .pipe(Effect.forkChild)
        const second = yield* prompt
          .prompt({
            sessionID: session.id,
            messageID: secondID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "captain two" }],
          })
          .pipe(Effect.forkChild)
        const held = yield* prompt
          .prompt({
            sessionID: session.id,
            messageID: heldID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "[fm-from-firstmate]\x1f held mail" }],
          })
          .pipe(Effect.forkChild)
        yield* pollWithTimeout(
          sessions
            .messages({ sessionID: session.id })
            .pipe(
              Effect.map((messages) =>
                messages.some(
                  (message) =>
                    message.info.role === "user" &&
                    message.parts.some((part) => part.type === "text" && part.text === "captain one"),
                ) &&
                messages.some(
                  (message) =>
                    message.info.role === "user" &&
                    message.parts.some((part) => part.type === "text" && part.text === "captain two"),
                )
                  ? true
                  : undefined,
              ),
            ),
          "direct steering prompts were not admitted",
          "10 seconds",
        )
        const queuedHeld = yield* pollWithTimeout(
          queue.list(session.id).pipe(Effect.map((items) => items.find((item) => item.input.messageID === heldID))),
          "held machine mail was not kept in the queue",
          "10 seconds",
        )
        expect(queuedHeld.delivery).toBe("queue")
        const firstSaved = (yield* sessions.messages({ sessionID: session.id })).find(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === "captain one"),
        )
        const secondSaved = (yield* sessions.messages({ sessionID: session.id })).find(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === "captain two"),
        )
        if (!firstSaved || firstSaved.info.role !== "user" || !secondSaved || secondSaved.info.role !== "user")
          throw new Error("expected both direct messages to be persisted")
        yield* sessions.updateMessage({ ...firstSaved.info, time: { created: 300 } })
        yield* sessions.updateMessage({ ...secondSaved.info, time: { created: 100 } })
        const original = (yield* llm.inputs)[0]
        if (!original) throw new Error("expected the original provider request")
        expect(JSON.stringify(original.messages)).not.toContain("captain one")
        expect(JSON.stringify(original.messages)).not.toContain("captain two")
        expect(JSON.stringify(original.messages)).not.toContain("held mail")
        const order = (yield* MessageV2.admission(session.id)).order
        expect(order.get(firstSaved.info.id)).toBeLessThan(order.get(secondSaved.info.id) ?? Infinity)

        yield* Deferred.succeed(toolGate, void 0)
        yield* awaitWithTimeout(
          llm.wait(2),
          "the first direct steer missed the next continuation boundary",
          "10 seconds",
        )
        const continuation = (yield* llm.inputs)[1]
        if (!continuation) throw new Error("expected the direct steering continuation request")
        const continuationText = JSON.stringify(continuation.messages)
        expect(continuationText).toContain("captain one")
        expect(continuationText).toContain("captain two")
        expect(continuationText).toContain("original task")
        expect(continuationText.indexOf("original task")).toBeLessThan(continuationText.indexOf("captain one"))
        expect(continuationText.indexOf("captain one")).toBeLessThan(continuationText.indexOf("captain two"))
        expect(continuationText.indexOf(toolFile)).toBeLessThan(continuationText.indexOf("captain one"))
        expect(continuationText).not.toContain("held mail")
        expect(continuationText).toContain(toolFile)

        yield* Deferred.succeed(firstSteerGate, void 0)
        yield* awaitWithTimeout(llm.wait(3), "held mail did not follow the direct steers", "10 seconds")
        const heldTurn = (yield* llm.inputs)[2]
        if (!heldTurn) throw new Error("expected the held mail request")
        const heldText = JSON.stringify(heldTurn.messages)
        expect(heldText).toContain("held mail")
        expect(heldText.indexOf("captains handled together")).toBeLessThan(heldText.indexOf("held mail"))

        expect(
          Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(run), "session run did not finish", "10 seconds")),
        ).toBe(true)
        const firstExit = yield* awaitWithTimeout(
          Fiber.await(first),
          "first direct prompt did not finish",
          "10 seconds",
        )
        const secondExit = yield* awaitWithTimeout(
          Fiber.await(second),
          "second direct prompt did not finish",
          "10 seconds",
        )
        expect(Exit.isSuccess(firstExit)).toBe(true)
        expect(Exit.isSuccess(secondExit)).toBe(true)
        if (Exit.isSuccess(firstExit) && Exit.isSuccess(secondExit))
          expect(firstExit.value.info.id).toBe(secondExit.value.info.id)
        expect(
          Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(held), "held prompt did not finish", "10 seconds")),
        ).toBe(true)
        const messages = yield* sessions.messages({ sessionID: session.id })
        expect(
          messages.some(
            (message) => message.info.role === "assistant" && message.info.parentID === secondSaved.info.id,
          ),
        ).toBe(true)
        const promotedHeld = messages.findLast(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === "[fm-from-firstmate]\x1f held mail"),
        )
        expect(promotedHeld?.info.role).toBe("user")
        if (promotedHeld?.info.role !== "user") throw new Error("expected held mail to be promoted")
        expect(
          messages.some(
            (message) => message.info.role === "assistant" && message.info.parentID === promotedHeld.info.id,
          ),
        ).toBe(true)
        expect(yield* llm.calls).toBe(3)
      }).pipe(
        Effect.ensuring(
          Effect.all(
            [toolGate, firstSteerGate].map((gate) => Deferred.succeed(gate, void 0).pipe(Effect.ignore)),
            { discard: true },
          ),
        ),
      )
    }),
  60_000,
)

it.instance("steers an active held turn at its next provider boundary", () =>
  Effect.gen(function* () {
    const { dir, llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const toolFile = path.join(dir, "held-steer-step.txt")
    yield* writeText(toolFile, "held steer continuation")
    const session = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    const stopGate = yield* Deferred.make<void>()
    const heldToolGate = yield* Deferred.make<void>()
    const heldID = MessageID.make("msg_active_held")
    const directID = MessageID.make("msg_active_held_direct")

    yield* Effect.gen(function* () {
      yield* llm.push(
        reply().wait(deferredAsPromise(stopGate)).text("task finished").stop().item(),
        reply().wait(deferredAsPromise(heldToolGate)).tool("glob", { pattern: "held-steer-step.txt" }).item(),
        reply().text("captain intervention handled").stop().item(),
      )
      yield* seedUser({
        sessionID: session.id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "original task" }],
      })
      const run = yield* prompt.loop({ sessionID: session.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "original task provider request did not start", "10 seconds")

      const held = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID: heldID,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "[fm-from-firstmate]\x1f held task" }],
        })
        .pipe(Effect.forkChild)
      const pendingHeld = yield* pollWithTimeout(
        queue.list(session.id).pipe(Effect.map((items) => items.find((item) => item.input.messageID === heldID))),
        "held task was not queued",
        "10 seconds",
      )
      expect(pendingHeld.delivery).toBe("queue")

      yield* Deferred.succeed(stopGate, void 0)
      yield* awaitWithTimeout(llm.wait(2), "held task provider request did not start", "10 seconds")
      const active = (yield* llm.inputs)[1]
      if (!active) throw new Error("expected the held task provider request")
      expect(JSON.stringify(active.messages)).toContain("held task")

      const direct = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID: directID,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "captain typed during held work" }],
        })
        .pipe(Effect.forkChild)
      yield* pollWithTimeout(
        sessions
          .messages({ sessionID: session.id })
          .pipe(Effect.map((messages) => messages.some((message) => message.info.id === directID) || undefined)),
        "direct input was not admitted during held work",
        "10 seconds",
      )

      yield* Deferred.succeed(heldToolGate, void 0)
      yield* awaitWithTimeout(llm.wait(3), "direct input missed the held continuation boundary", "10 seconds")
      const steered = (yield* llm.inputs)[2]
      if (!steered) throw new Error("expected the direct input provider request")
      const steeredText = JSON.stringify(steered.messages)
      expect(steeredText).toContain(toolFile)
      expect(steeredText).toContain("held task")
      expect(steeredText.indexOf("held task")).toBeLessThan(steeredText.indexOf("captain typed during held work"))

      expect(
        Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(run), "session run did not finish", "10 seconds")),
      ).toBe(true)
      expect(
        Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(held), "held prompt did not finish", "10 seconds")),
      ).toBe(true)
      expect(
        Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(direct), "direct prompt did not finish", "10 seconds")),
      ).toBe(true)
      const messages = yield* sessions.messages({ sessionID: session.id })
      const heldUser = messages.find(
        (message) =>
          message.info.role === "user" &&
          message.parts.some((part) => part.type === "text" && part.text.includes("held task")),
      )
      const directUser = messages.find(
        (message) =>
          message.info.role === "user" &&
          message.parts.some((part) => part.type === "text" && part.text.includes("captain typed during held work")),
      )
      if (!heldUser || !directUser) throw new Error("expected both held and direct user messages")
      expect(
        messages.some((message) => message.info.role === "assistant" && message.info.parentID === heldUser.info.id),
      ).toBe(true)
      expect(
        messages.some((message) => message.info.role === "assistant" && message.info.parentID === directUser.info.id),
      ).toBe(true)
      expect(yield* llm.calls).toBe(3)
    }).pipe(
      Effect.ensuring(
        Effect.all(
          [stopGate, heldToolGate].map((gate) => Deferred.succeed(gate, void 0).pipe(Effect.ignore)),
          {
            discard: true,
          },
        ),
      ),
    )
  }),
)

directProjectionPrompt.instance(
  "keeps a mid-run direct root after the prior task through its tool continuation",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const toolFile = path.join(dir, "direct-step.txt")
      yield* writeText(toolFile, "direct tool step")
      const session = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const directID = MessageID.make("msg_direct_projection")
      const directGate = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        gateDirectProjection = true
        yield* llm.push(
          reply().tool("glob", { pattern: "direct-step.txt" }).item(),
          reply().text("task finished").stop().item(),
          reply().tool("glob", { pattern: "direct-step.txt" }).item(),
          reply().wait(deferredAsPromise(directGate)).text("captain direct handled").stop().item(),
        )
        const root = yield* seedUser({
          sessionID: session.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "original task" }],
        })
        if (root.info.role !== "user") throw new Error("expected the original user message")
        const run = yield* prompt.loop({ sessionID: session.id }).pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(1), "original tool step did not start", "10 seconds")
        yield* awaitWithTimeout(
          Effect.promise(() => directProjectionEntered.promise),
          "continuation history load did not reach its gate",
          "10 seconds",
        )

        yield* seedUser({
          sessionID: session.id,
          messageID: directID,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "captain direct arrived before task completion" }],
        })
        yield* Effect.sync(() => directProjectionRelease.resolve())
        yield* awaitWithTimeout(llm.wait(2), "original task continuation did not start", "10 seconds")
        const taskContinuation = (yield* llm.inputs)[1]
        if (!taskContinuation) throw new Error("expected the original task continuation")
        expect(JSON.stringify(taskContinuation.messages)).not.toContain("captain direct arrived before task completion")

        const current = yield* MessageV2.admission(session.id)
        const messages = yield* sessions.messages({ sessionID: session.id })
        const finalTaskID =
          messages.findLast((message) => message.info.role === "assistant" && message.info.parentID === root.info.id)
            ?.info.id ?? (yield* Effect.die(new Error("expected the task's terminal assistant")))
        expect(current.order.get(directID)).toBeLessThan(current.order.get(finalTaskID) ?? Infinity)

        yield* awaitWithTimeout(llm.wait(3), "pending direct turn did not start", "10 seconds")
        const directTurn = (yield* llm.inputs)[2]
        if (!directTurn) throw new Error("expected the pending direct turn")
        const firstDirect = JSON.stringify(directTurn.messages)
        expect(firstDirect.indexOf("captain direct arrived before task completion")).toBeLessThan(
          firstDirect.indexOf("task finished"),
        )

        yield* awaitWithTimeout(llm.wait(4), "direct tool continuation did not start", "10 seconds")
        const directContinuation = (yield* llm.inputs)[3]
        if (!directContinuation) throw new Error("expected the direct tool continuation")
        const secondDirect = JSON.stringify(directContinuation.messages)
        expect(secondDirect.indexOf("captain direct arrived before task completion")).toBeLessThan(
          secondDirect.indexOf("task finished"),
        )
        expect(secondDirect).toContain(toolFile)

        yield* Deferred.succeed(directGate, void 0)
        const runExit = yield* awaitWithTimeout(Fiber.await(run), "session run did not finish", "10 seconds")
        if (Exit.isFailure(runExit)) throw new Error(Cause.pretty(runExit.cause))
        expect(yield* llm.calls).toBe(4)
      }).pipe(Effect.ensuring(Effect.sync(() => directProjectionRelease.resolve())))
    }),
  60_000,
)

it.instance("starts a marked prompt normally when no run is active", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({ title: "Pinned" })
    yield* llm.text("idle machine message handled")

    const run = yield* prompt
      .prompt({
        sessionID: session.id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "[fm-from-firstmate]\x1f idle request" }],
      })
      .pipe(Effect.forkChild)
    yield* awaitWithTimeout(llm.wait(1), "idle marked prompt did not start a provider turn", "5 seconds")
    const request = (yield* llm.inputs).at(0)
    if (!request) throw new Error("expected the idle provider request")
    expect(lastUserContent(request)).toContain("idle request")

    const exit = yield* awaitWithTimeout(Fiber.await(run), "idle marked prompt did not finish", "5 seconds")
    expect(Exit.isSuccess(exit)).toBe(true)
    expect(yield* llm.calls).toBe(1)
  }),
)

it.instance(
  "does not re-enter a rejected tool loop without held work",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig((url) => {
        const config = providerCfg(url)
        return {
          ...config,
          permission: { bash: "ask" },
          experimental: { continue_loop_on_deny: false },
        }
      })
      const prompt = yield* SessionPrompt.Service
      const permission = yield* Permission.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })

      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "attempt a denied command" }],
      })
      yield* llm.push(
        reply().tool("bash", { command: "echo denied" }).stop().item(),
        reply().text("unexpected extra provider step").stop().item(),
      )

      const run = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "provider did not request the denied tool", "10 seconds")
      const request = yield* pollWithTimeout(
        permission.list().pipe(Effect.map((pending) => pending[0])),
        "denied tool did not ask for permission",
        "10 seconds",
      )
      yield* permission.reply({ requestID: request.id, reply: "reject" })
      const exit = yield* awaitWithTimeout(Fiber.await(run), "rejected tool run did not finish", "10 seconds")
      expect(Exit.isSuccess(exit)).toBe(true)
      expect(yield* llm.calls).toBe(1)
    }),
  60_000,
)

// Loop semantics

noLLMServer.instance(
  "loop exits immediately when last assistant has stop finish",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* seed(chat.id, { finish: "stop" })

      const result = yield* prompt.loop({ sessionID: chat.id })
      expect(result.info.role).toBe("assistant")
      if (result.info.role === "assistant") expect(result.info.finish).toBe("stop")
    }),
  { config: cfg },
)

noLLMServer.instance(
  "loop exits for a completed parent turn with nonmonotonic message IDs",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const userID = MessageID.make("msg_z_user")
      const assistantID = MessageID.make("msg_a_assistant")
      yield* sessions.updateMessage({
        id: userID,
        role: "user",
        sessionID: chat.id,
        agent: "build",
        model: ref,
        time: { created: 100 },
      })
      yield* sessions.updateMessage({
        id: assistantID,
        role: "assistant",
        parentID: userID,
        sessionID: chat.id,
        mode: "build",
        agent: "build",
        cost: 0,
        path: { cwd: "/tmp", root: "/tmp" },
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        modelID: ref.modelID,
        providerID: ref.providerID,
        time: { created: 200, completed: 201 },
        finish: "stop",
      })

      const result = yield* prompt.loop({ sessionID: chat.id })

      expect(result.info.id).toBe(assistantID)
    }),
  { config: cfg },
)

it.instance("loop exits without an LLM request for interrupted orphan tool calls", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    const seeded = yield* seed(chat.id, { finish: "stop" })
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: seeded.assistant.id,
      sessionID: chat.id,
      type: "tool",
      callID: "interrupted-call",
      tool: "edit",
      state: {
        status: "error",
        input: {},
        error: "Tool execution aborted",
        metadata: { interrupted: true },
        time: { start: 1, end: 2 },
      },
    })

    const result = yield* prompt.loop({ sessionID: chat.id })
    expect(result.info.id).toBe(seeded.assistant.id)
    expect(yield* llm.hits).toHaveLength(0)
  }),
)

it.instance("loop calls LLM and returns assistant message", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* seedUser({
      sessionID: chat.id,
      agent: "build",
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.text("world")

    const result = yield* prompt.loop({ sessionID: chat.id })
    expect(result.info.role).toBe("assistant")
    const parts = result.parts.filter((p) => p.type === "text")
    expect(parts.some((p) => p.type === "text" && p.text === "world")).toBe(true)
    expect(yield* llm.hits).toHaveLength(1)
  }),
)

processorDies.instance(
  "loop terminalizes the assistant when processor setup dies",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const events = yield* EventV2Bridge.Service
      const status = yield* SessionStatus.Service
      const chat = yield* sessions.create({
        title: "Processor setup failure",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const errors: string[] = []
      const off = yield* events.listen((event) => {
        if (event.type !== Session.Event.Error.type) return Effect.void
        const data = event.data as typeof Session.Event.Error.data.Type
        if (data.sessionID !== chat.id || !data.error) return Effect.void
        errors.push(data.error.name)
        return Effect.void
      })

      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: said("hello"),
      })
      const exit = yield* Effect.exit(prompt.loop({ sessionID: chat.id }))
      const messages = yield* sessions.messages({ sessionID: chat.id, limit: 10 })
      const assistant = messages.findLast((message) => message.info.role === "assistant")
      const state = yield* status.get(chat.id)
      yield* off

      expect(Exit.isFailure(exit)).toBe(true)
      expect(state).toMatchObject({ type: "idle" })
      expect(assistant?.info.role).toBe("assistant")
      if (assistant?.info.role === "assistant") {
        expect(assistant.info.time.completed).toBeDefined()
        const error = assistant.info.error
        expect(error?.name).toBe("UnknownError")
        if (error?.name === "UnknownError") expect(errors).toContain(error.name)
      }
    }),
  { config: cfg },
  20_000,
)

gated.instance(
  "outcome block die terminalizes the persisted assistant",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const events = yield* EventV2Bridge.Service
      const status = yield* SessionStatus.Service
      const chat = yield* sessions.create({
        title: "Outcome block failure",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const fault = { triggered: false }
      const errors: string[] = []
      gates.outcomeFailure = fault
      yield* Effect.addFinalizer(() => Effect.sync(() => (gates.outcomeFailure = undefined)))
      const off = yield* events.listen((event) => {
        if (event.type !== Session.Event.Error.type) return Effect.void
        const data = event.data as typeof Session.Event.Error.data.Type
        if (data.sessionID !== chat.id || !data.error) return Effect.void
        errors.push(data.error.name)
        return Effect.void
      })

      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: said("hello"),
      })
      const exit = yield* Effect.exit(prompt.loop({ sessionID: chat.id }))
      const messages = yield* sessions.messages({ sessionID: chat.id, limit: 10 })
      const assistant = messages.findLast((message) => message.info.role === "assistant")
      const state = yield* status.get(chat.id)
      yield* off

      expect(fault.triggered).toBe(true)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(state).toMatchObject({ type: "idle" })
      expect(assistant?.info.role).toBe("assistant")
      if (assistant?.info.role === "assistant") {
        expect(assistant.info.time.completed).toBeDefined()
        const error = assistant.info.error
        expect(error?.name).toBe("UnknownError")
        if (error?.name === "UnknownError") {
          expect(error.data.message).toContain("injected prompt outcome failure")
          expect(errors).toContain(error.name)
        }
      }
    }),
  { config: cfg },
  20_000,
)

gated.instance(
  "failed compaction write preserves the completed answer and tool context",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig((url) => ({ ...providerCfg(url), compaction: { auto: true } }))
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const events = yield* EventV2Bridge.Service
      const provider = yield* ProviderSvc.Service
      const instance = yield* TestInstance
      const chat = yield* sessions.create({ title: "Compaction write failure" })
      const model = yield* provider.getModel(ref.providerID, ref.modelID)
      const probe = path.join(instance.directory, "probe.txt")
      yield* writeText(probe, "probe")
      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: said("Use the tool, answer, and then compact"),
      })
      yield* llm.tool("glob", { pattern: "**/*.txt" })
      yield* llm.push(
        raw({
          chunks: [
            {
              id: "chatcmpl-test",
              object: "chat.completion.chunk",
              created: 1790550000,
              model: "test-model",
              choices: [{ index: 0, delta: { role: "assistant", content: "partial response" }, finish_reason: null }],
            },
            { error: "Your input exceeds the context window of this model" },
          ],
        }),
      )
      const fault = { triggered: false }
      gates.compactionWriteFailure = fault
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (gates.compactionWriteFailure === fault) gates.compactionWriteFailure = undefined
        }),
      )
      const eventMessages: string[] = []
      const off = yield* events.listen((event) => {
        if (event.type !== Session.Event.Error.type) return Effect.void
        const data = event.data as typeof Session.Event.Error.data.Type
        if (data.sessionID !== chat.id || !data.error) return Effect.void
        if (data.error.name === "UnknownError") eventMessages.push(data.error.data.message)
        return Effect.void
      })

      const exit = yield* Effect.exit(prompt.loop({ sessionID: chat.id }))
      yield* off
      const messages = yield* sessions.messages({ sessionID: chat.id })
      const assistant = messages.findLast((message) => message.info.role === "assistant")
      const tool = messages
        .flatMap((message) => message.parts)
        .find((part): part is SessionV1.ToolPart => part.type === "tool" && part.tool === "glob")
      const modelMessages = yield* MessageV2.toModelMessagesEffect(messages, model)
      const modelContext = JSON.stringify(modelMessages)

      expect(fault.triggered).toBe(true)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(assistant?.info.role).toBe("assistant")
      if (assistant?.info.role === "assistant") {
        expect(assistant.info.time.completed).toBeDefined()
        expect(assistant.info.error).toBeUndefined()
        expect(assistant.parts.some((part) => part.type === "text" && part.text.includes("partial response"))).toBe(
          true,
        )
      }
      expect(tool?.state.status).toBe("completed")
      expect(tool?.state.status === "completed" ? tool.state.output : "").toContain("probe.txt")
      expect(modelContext).toContain("partial response")
      expect(modelContext).toContain("probe.txt")
      expect(eventMessages).toEqual(["injected compaction write failure"])
    }),
  { config: { ...cfg, compaction: { auto: true } } },
  20_000,
)

gated.instance(
  "structured output persistence failure preserves the completed assistant",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const events = yield* EventV2Bridge.Service
      const provider = yield* ProviderSvc.Service
      const chat = yield* sessions.create({ title: "Structured output write failure" })
      const model = yield* provider.getModel(ref.providerID, ref.modelID)
      const format = Schema.decodeUnknownSync(SessionV1.Format)({
        type: "json_schema",
        schema: { type: "object", properties: { answer: { type: "string" } }, required: ["answer"] },
      })
      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        format,
        parts: said("Return the structured answer"),
      })
      yield* llm.push(reply().tool("StructuredOutput", { answer: "42" }))
      const fault = { triggered: false }
      gates.structuredUpdateFailure = fault
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (gates.structuredUpdateFailure === fault) gates.structuredUpdateFailure = undefined
        }),
      )
      const eventMessages: string[] = []
      const off = yield* events.listen((event) => {
        if (event.type !== Session.Event.Error.type) return Effect.void
        const data = event.data as typeof Session.Event.Error.data.Type
        if (data.sessionID !== chat.id || !data.error) return Effect.void
        if (data.error.name === "UnknownError") eventMessages.push(data.error.data.message)
        return Effect.void
      })

      const exit = yield* Effect.exit(prompt.loop({ sessionID: chat.id }))
      yield* off
      const messages = yield* sessions.messages({ sessionID: chat.id })
      const assistant = messages.findLast((message) => message.info.role === "assistant")
      const modelMessages = yield* MessageV2.toModelMessagesEffect(messages, model)

      expect(fault.triggered).toBe(true)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(assistant?.info.role).toBe("assistant")
      if (assistant?.info.role === "assistant") {
        expect(assistant.info.time.completed).toBeDefined()
        expect(assistant.info.error).toBeUndefined()
        expect(assistant.parts.some((part) => part.type === "tool" && part.tool === "StructuredOutput")).toBe(true)
      }
      expect(JSON.stringify(modelMessages)).toContain("StructuredOutput")
      expect(eventMessages).toEqual(["injected structured output persistence failure"])
    }),
  { config: cfg },
  20_000,
)

gated.instance(
  "content-filter persistence failure publishes the follow-up error and preserves the provider error",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const events = yield* EventV2Bridge.Service
      const chat = yield* sessions.create({ title: "Content-filter persistence failure" })
      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        parts: said("Return a response that the provider filters"),
      })
      yield* llm.push(reply().text("partial response").contentFilter())
      const fault = { name: "ContentFilterError" as const, triggered: false }
      gates.presetErrorUpdateFailure = fault
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (gates.presetErrorUpdateFailure === fault) gates.presetErrorUpdateFailure = undefined
        }),
      )
      const eventMessages: string[] = []
      const off = yield* events.listen((event) => {
        if (event.type !== Session.Event.Error.type) return Effect.void
        const data = event.data as typeof Session.Event.Error.data.Type
        if (data.sessionID !== chat.id || !data.error) return Effect.void
        if (data.error.name === "UnknownError") eventMessages.push(data.error.data.message)
        return Effect.void
      })

      const exit = yield* Effect.exit(prompt.loop({ sessionID: chat.id }))
      yield* off
      const messages = yield* sessions.messages({ sessionID: chat.id })
      const assistant = messages.findLast((message) => message.info.role === "assistant")

      expect(fault.triggered).toBe(true)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(assistant?.info.role).toBe("assistant")
      if (assistant?.info.role === "assistant") {
        expect(assistant.info.finish).toBe("content-filter")
        expect(assistant.info.time.completed).toBeDefined()
        expect(assistant.info.error).toMatchObject({
          name: "ContentFilterError",
          data: { message: "The response was blocked by the provider's content filter" },
        })
      }
      expect(eventMessages).toEqual(["injected ContentFilterError persistence failure"])
    }),
  { config: cfg },
  20_000,
)

gated.instance(
  "missing structured-output persistence failure publishes the follow-up error and preserves the provider error",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const events = yield* EventV2Bridge.Service
      const chat = yield* sessions.create({ title: "Missing structured-output persistence failure" })
      const format = Schema.decodeUnknownSync(SessionV1.Format)({
        type: "json_schema",
        schema: { type: "object", properties: { answer: { type: "string" } }, required: ["answer"] },
      })
      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        format,
        parts: said("Return a structured answer"),
      })
      yield* llm.push(reply().text("plain text instead of structured output"))
      const fault = { name: "StructuredOutputError" as const, triggered: false }
      gates.presetErrorUpdateFailure = fault
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (gates.presetErrorUpdateFailure === fault) gates.presetErrorUpdateFailure = undefined
        }),
      )
      const eventMessages: string[] = []
      const off = yield* events.listen((event) => {
        if (event.type !== Session.Event.Error.type) return Effect.void
        const data = event.data as typeof Session.Event.Error.data.Type
        if (data.sessionID !== chat.id || !data.error) return Effect.void
        if (data.error.name === "UnknownError") eventMessages.push(data.error.data.message)
        return Effect.void
      })

      const exit = yield* Effect.exit(prompt.loop({ sessionID: chat.id }))
      yield* off
      const messages = yield* sessions.messages({ sessionID: chat.id })
      const assistant = messages.findLast((message) => message.info.role === "assistant")

      expect(fault.triggered).toBe(true)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(assistant?.info.role).toBe("assistant")
      if (assistant?.info.role === "assistant") {
        expect(assistant.info.time.completed).toBeDefined()
        expect(assistant.info.error).toMatchObject({
          name: "StructuredOutputError",
          data: { message: "Model did not produce structured output", retries: 0 },
        })
        expect(assistant.info.structured).toBeUndefined()
      }
      expect(eventMessages).toEqual(["injected StructuredOutputError persistence failure"])
    }),
  { config: cfg },
  20_000,
)

instructionClearFailureTest.instance(
  "instruction.clear failure preserves the completed assistant",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const events = yield* EventV2Bridge.Service
      const provider = yield* ProviderSvc.Service
      const chat = yield* sessions.create({ title: "Instruction clear failure" })
      const model = yield* provider.getModel(ref.providerID, ref.modelID)
      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: said("Preserve this answer after instruction cleanup"),
      })
      yield* llm.text("answer before instruction clear")
      instructionClearFailure.armed = true
      instructionClearFailure.triggered = false
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          instructionClearFailure.armed = false
          instructionClearFailure.triggered = false
        }),
      )
      const eventMessages: string[] = []
      const off = yield* events.listen((event) => {
        if (event.type !== Session.Event.Error.type) return Effect.void
        const data = event.data as typeof Session.Event.Error.data.Type
        if (data.sessionID !== chat.id || !data.error) return Effect.void
        if (data.error.name === "UnknownError") eventMessages.push(data.error.data.message)
        return Effect.void
      })

      const exit = yield* Effect.exit(prompt.loop({ sessionID: chat.id }))
      yield* off
      const messages = yield* sessions.messages({ sessionID: chat.id })
      const assistant = messages.findLast((message) => message.info.role === "assistant")
      const modelMessages = yield* MessageV2.toModelMessagesEffect(messages, model)

      expect(instructionClearFailure.triggered).toBe(true)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(assistant?.info.role).toBe("assistant")
      if (assistant?.info.role === "assistant") {
        expect(assistant.info.time.completed).toBeDefined()
        expect(assistant.info.error).toBeUndefined()
        expect(
          assistant.parts.some((part) => part.type === "text" && part.text.includes("answer before instruction clear")),
        ).toBe(true)
      }
      expect(JSON.stringify(modelMessages)).toContain("answer before instruction clear")
      expect(eventMessages).toEqual(["injected instruction.clear failure"])
    }),
  { config: cfg },
  20_000,
)

withMcpInstructions.instance(
  "loop includes MCP instructions in model system context",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.hang
      yield* user(chat.id, "hello")

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "timed out waiting for MCP instruction request", "10 seconds")

      const hits = yield* llm.hits
      const body = JSON.stringify(hits[0]?.body)
      expect(body).toContain('<server name=\\"guide-server\\">')
      expect(body).toContain("Use lookup before mutate.")
      yield* Fiber.interrupt(fiber)
    }),
  15_000,
)

it.instance("legacy prompt emits message events without session.next events", () =>
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Pinned",
      agent: "plan",
      model: { providerID: ProviderV2.ID.make("old"), id: ModelV2.ID.make("old-model") },
    })
    const seen: string[] = []
    const off = yield* events.listen((event) => {
      seen.push(event.type)
      return Effect.void
    })

    const first = yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      model: ref,
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    const second = yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "again" }],
    })
    yield* off

    expect(first.info.role).toBe("user")
    expect(second.info.role).toBe("user")
    if (first.info.role === "user" && second.info.role === "user") {
      expect(first.info.model).toEqual(ref)
      expect(second.info.model).toEqual(ref)
    }
    expect(yield* sessions.get(chat.id)).toMatchObject({
      agent: "build",
      model: { providerID: ref.providerID, id: ref.modelID },
    })
    expect(seen).toContain(Session.Event.Updated.type)
    expect(seen).toContain(MessageV2.Event.Updated.type)
    expect(seen).toContain(MessageV2.Event.PartUpdated.type)
    expect(seen.filter((type) => type.startsWith("session.next."))).toEqual([])
  }),
)

it.instance(
  "delivers queued machine mail after a content-filter terminal",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const events = yield* EventV2Bridge.Service
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const terminal = yield* Deferred.make<void>()
      const errors: NonNullable<SessionV1.Assistant["error"]>[] = []
      const expected = {
        name: "ContentFilterError",
        data: { message: "The response was blocked by the provider's content filter" },
      } satisfies NonNullable<SessionV1.Assistant["error"]>
      const off = yield* events.listen((event) => {
        if (event.type !== Session.Event.Error.type) return Effect.void
        const data = event.data as typeof Session.Event.Error.data.Type
        if (data.sessionID === chat.id && data.error) errors.push(data.error)
        return Effect.void
      })

      yield* Effect.gen(function* () {
        yield* llm.push(
          reply().wait(deferredAsPromise(terminal)).text("partial response").contentFilter().item(),
          reply().text("held response").stop().item(),
        )
        const root = yield* seedUser({
          sessionID: chat.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "hello" }],
        })
        const run = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(1), "provider did not receive the original task", "10 seconds")

        const heldID = MessageID.ascending()
        const held = yield* prompt
          .prompt({
            sessionID: chat.id,
            messageID: heldID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "[fm-from-firstmate]\x1f queued after filter" }],
          })
          .pipe(Effect.forkChild)
        const pending = yield* pollWithTimeout(
          queue.list(chat.id).pipe(Effect.map((items) => items.find((item) => item.input.messageID === heldID))),
          "machine mail was not queued before the filter terminal",
          "10 seconds",
        )
        expect(pending.delivery).toBe("queue")
        const beforeFilter = (yield* llm.inputs)[0]
        expect(JSON.stringify(beforeFilter?.messages)).not.toContain("queued after filter")

        yield* Deferred.succeed(terminal, void 0)
        yield* awaitWithTimeout(llm.wait(2), "queued machine mail did not start after content filter", "10 seconds")
        const heldRequest = (yield* llm.inputs)[1]
        if (!heldRequest) throw new Error("expected the queued machine request")
        expect(lastProviderMessage(heldRequest)).toMatchObject({ role: "user" })
        expect(JSON.stringify(lastProviderMessage(heldRequest))).toContain("queued after filter")
        const exit = yield* awaitWithTimeout(Fiber.await(run), "session run did not finish", "10 seconds")
        yield* awaitWithTimeout(
          Fiber.await(held),
          "queued machine mail did not return after the run stopped",
          "10 seconds",
        )
        const messages = yield* sessions.messages({ sessionID: chat.id })
        const filtered = messages.find(
          (message): message is SessionV1.WithParts & { info: SessionV1.Assistant } =>
            message.info.role === "assistant" && message.info.parentID === root.info.id,
        )
        yield* off

        expect(Exit.isSuccess(exit)).toBe(true)
        expect(yield* llm.calls).toBe(2)
        expect(yield* queue.list(chat.id)).toEqual([])
        expect(
          messages.some((message) =>
            message.parts.some((part) => part.type === "text" && part.text.includes("queued after filter")),
          ),
        ).toBe(true)
        expect(filtered?.info.role).toBe("assistant")
        if (filtered?.info.role === "assistant") {
          expect(filtered.info.finish).toBe("content-filter")
          expect(filtered.info.error).toEqual(expected)
          expect(errors).toContainEqual(expected)
        }
        expect(filtered?.parts).toEqual(
          expect.arrayContaining([expect.objectContaining({ type: "text", text: "partial response" })]),
        )
      }).pipe(Effect.ensuring(Deferred.succeed(terminal, void 0).pipe(Effect.ignore)))
    }),
  60_000,
)

it.instance(
  "delivers queued machine mail after a length terminal",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const terminal = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        yield* llm.push(
          reply().wait(deferredAsPromise(terminal)).text("partial response").finish("length").item(),
          reply().text("held response").stop().item(),
        )
        const root = yield* seedUser({
          sessionID: chat.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "task cut off by provider" }],
        })
        const run = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(1), "provider did not receive the original task", "10 seconds")
        const heldID = MessageID.ascending()
        const held = yield* prompt
          .prompt({
            sessionID: chat.id,
            messageID: heldID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "[fm-from-firstmate]\x1f after length finish" }],
          })
          .pipe(Effect.forkChild)
        const pending = yield* pollWithTimeout(
          queue.list(chat.id).pipe(Effect.map((items) => items.find((item) => item.input.messageID === heldID))),
          "machine mail was not queued before the length terminal",
          "10 seconds",
        )
        expect(pending.delivery).toBe("queue")
        expect(JSON.stringify((yield* llm.inputs)[0]?.messages)).not.toContain("after length finish")

        yield* Deferred.succeed(terminal, void 0)
        yield* awaitWithTimeout(llm.wait(2), "queued turn did not start after length finish", "10 seconds")
        const request = (yield* llm.inputs)[1]
        if (!request) throw new Error("expected the held provider request")
        expect(JSON.stringify(lastProviderMessage(request))).toContain("after length finish")
        const exit = yield* awaitWithTimeout(Fiber.await(run), "session run did not finish", "10 seconds")
        yield* awaitWithTimeout(Fiber.await(held), "queued machine mail did not return", "10 seconds")
        const messages = yield* sessions.messages({ sessionID: chat.id })
        const terminalAssistant = messages.find(
          (message): message is SessionV1.WithParts & { info: SessionV1.Assistant } =>
            message.info.role === "assistant" && message.info.parentID === root.info.id,
        )
        expect(Exit.isSuccess(exit)).toBe(true)
        expect(terminalAssistant?.info.role).toBe("assistant")
        if (terminalAssistant?.info.role === "assistant") expect(terminalAssistant.info.finish).toBe("length")
        expect(yield* queue.list(chat.id)).toEqual([])
        expect(yield* llm.calls).toBe(2)
      }).pipe(Effect.ensuring(Deferred.succeed(terminal, void 0).pipe(Effect.ignore)))
    }),
  60_000,
)

it.instance(
  "delivers queued machine mail after a structured-output break",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const terminal = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        yield* llm.push(
          reply().wait(deferredAsPromise(terminal)).text("not structured output").stop().item(),
          reply().text("held response").stop().item(),
        )
        const root = yield* seedUser({
          sessionID: chat.id,
          agent: "build",
          model: ref,
          format: new SessionV1.OutputFormatJsonSchema({
            type: "json_schema",
            schema: { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"] },
            retryCount: 2,
          }),
          parts: [{ type: "text", text: "return the requested object" }],
        })
        const run = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(1), "provider did not receive the structured-output task", "10 seconds")
        const heldID = MessageID.ascending()
        const heldText = "[fm-from-peer]\x1f after structured-output break"
        const held = yield* prompt
          .prompt({
            sessionID: chat.id,
            messageID: heldID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: heldText }],
          })
          .pipe(Effect.forkChild)
        const pending = yield* pollWithTimeout(
          queue.list(chat.id).pipe(Effect.map((items) => items.find((item) => item.input.messageID === heldID))),
          "machine mail was not queued before the structured-output break",
          "10 seconds",
        )
        expect(pending.delivery).toBe("queue")

        yield* Deferred.succeed(terminal, void 0)
        yield* awaitWithTimeout(
          llm.wait(2),
          "queued machine mail was not delivered after structured-output break",
          "10 seconds",
        )
        const heldRequest = (yield* llm.inputs)[1]
        if (!heldRequest) throw new Error("expected the queued machine request")
        expect(lastUserContent(heldRequest)).toContain("after structured-output break")
        const exit = yield* awaitWithTimeout(Fiber.await(run), "session run did not finish", "10 seconds")
        const heldExit = yield* awaitWithTimeout(Fiber.await(held), "queued machine mail did not return", "10 seconds")
        const messages = yield* sessions.messages({ sessionID: chat.id })
        const failed = messages.find(
          (message): message is SessionV1.WithParts & { info: SessionV1.Assistant } =>
            message.info.role === "assistant" && message.info.parentID === root.info.id,
        )
        expect(Exit.isSuccess(exit)).toBe(true)
        expect(Exit.isSuccess(heldExit)).toBe(true)
        expect(failed?.info.role).toBe("assistant")
        if (failed?.info.role === "assistant") expect(failed.info.error?.name).toBe("StructuredOutputError")
        expect(yield* queue.list(chat.id)).toEqual([])
        expect(yield* llm.calls).toBe(2)
      }).pipe(Effect.ensuring(Deferred.succeed(terminal, void 0).pipe(Effect.ignore)))
    }),
  60_000,
)

it.instance(
  "delivers queued machine mail after a failed compaction summary",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const compaction = yield* SessionCompaction.Service
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const toolFile = path.join(dir, "compaction-stop.txt")
      yield* writeText(toolFile, "queue delivery after a failed summary")
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const toolGate = yield* Deferred.make<void>()
      const compactionGate = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() =>
        Effect.all(
          [toolGate, compactionGate].map((gate) => Deferred.succeed(gate, void 0).pipe(Effect.ignore)),
          { discard: true },
        ),
      )

      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "task before tool continuation" }],
      })
      yield* llm.push(reply().wait(deferredAsPromise(toolGate)).tool("glob", { pattern: "compaction-stop.txt" }).item())
      yield* llm.error(400, { error: { message: "summary rejected" } }, deferredAsPromise(compactionGate))
      yield* llm.text("held machine mail handled")

      const run = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "task tool call did not reach the provider", "10 seconds")
      const held = yield* prompt
        .prompt({
          sessionID: chat.id,
          messageID: MessageID.ascending(),
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "[fm-from-firstmate]\x1f after compaction" }],
        })
        .pipe(Effect.forkChild)
      yield* pollWithTimeout(
        queue.list(chat.id).pipe(Effect.map((items) => items.find((item) => item.delivery === "queue"))),
        "machine mail was not queued during compaction",
        "10 seconds",
      )

      const compactionID = yield* compaction.create({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        auto: true,
      })

      yield* Deferred.succeed(toolGate, void 0)
      yield* awaitWithTimeout(llm.wait(2), "compaction summary did not reach the provider", "10 seconds")
      yield* Deferred.succeed(compactionGate, void 0)
      yield* awaitWithTimeout(
        llm.wait(3),
        "queued machine mail was not delivered after compaction stopped",
        "10 seconds",
      )
      const heldRequest = (yield* llm.inputs)[2]
      if (!heldRequest) throw new Error("expected the queued machine request")
      expect(lastUserContent(heldRequest)).toContain("after compaction")

      const runExit = yield* awaitWithTimeout(Fiber.await(run), "session run did not finish", "10 seconds")
      const heldExit = yield* awaitWithTimeout(
        Fiber.await(held),
        "held prompt did not receive its own reply",
        "10 seconds",
      )
      const messages = yield* sessions.messages({ sessionID: chat.id })
      const failedSummary = messages.find(
        (message) => message.info.role === "assistant" && message.info.parentID === compactionID,
      )
      const heldUser = messages.find(
        (message) =>
          message.info.role === "user" &&
          message.parts.some((part) => part.type === "text" && part.text.includes("after compaction")),
      )
      if (!Exit.isSuccess(heldExit) || !heldUser) throw new Error("expected the queued prompt's assistant reply")
      expect(Exit.isSuccess(runExit)).toBe(true)
      expect(failedSummary?.info.role).toBe("assistant")
      if (failedSummary?.info.role === "assistant") expect(failedSummary.info.error).toBeDefined()
      expect(heldExit.value.info.role).toBe("assistant")
      if (heldExit.value.info.role === "assistant") expect(heldExit.value.info.parentID).toBe(heldUser.info.id)
      expect(
        heldExit.value.parts.some((part) => part.type === "text" && part.text === "held machine mail handled"),
      ).toBe(true)
      expect(yield* queue.list(chat.id)).toEqual([])
      expect(yield* llm.calls).toBe(3)
    }),
  60_000,
)

it.instance("loop stops provider overflow instead of auto-compacting when disabled", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig((url) => ({
      ...providerCfg(url),
      compaction: { auto: false },
    }))
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })

    yield* llm.error(413, { error: { message: "request entity too large" } })
    yield* seedUser({
      sessionID: chat.id,
      agent: "build",
      parts: [{ type: "text", text: "hello" }],
    })

    const result = yield* prompt.loop({ sessionID: chat.id })
    const messages = yield* sessions.messages({ sessionID: chat.id })

    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.info.error?.name).toBe("ContextOverflowError")
      expect(result.info.finish).toBe("error")
    }
    expect(messages.some((message) => message.parts.some((part) => part.type === "compaction"))).toBe(false)
  }),
)

it.instance("automatically compacts and continues after an unparseable mid-stream error chunk", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig((url) => ({
      ...providerCfg(url),
      compaction: { auto: true },
    }))
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })

    yield* llm.push(
      raw({
        chunks: [
          {
            id: "chatcmpl-test",
            object: "chat.completion.chunk",
            created: 1790550000,
            model: "test-model",
            choices: [{ index: 0, delta: { role: "assistant", content: "partial response" }, finish_reason: null }],
          },
          { error: "Your input exceeds the context window of this model" },
        ],
      }),
      reply().text("Compaction summary from the mock.").stop(),
      reply().text("Continued after automatic compaction.").stop(),
    )

    const result = yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      parts: [{ type: "text", text: "Continue after handling a mocked overflow." }],
    })
    const messages = yield* sessions.messages({ sessionID: chat.id })

    expect(yield* llm.hits).toHaveLength(3)
    expect(messages.some((message) => message.parts.some((part) => part.type === "compaction"))).toBe(true)
    expect(
      messages.some((message) =>
        message.parts.some((part) => part.type === "text" && part.text === "partial response"),
      ),
    ).toBe(true)
    expect(result.info.role).toBe("assistant")
    expect(result.parts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: "text", text: "Continued after automatic compaction." }),
      ]),
    )
    if (result.info.role === "assistant") expect(result.info.error).toBeUndefined()
  }),
)

noLLMServer.instance.skip(
  "prompt emits v2 prompted and synthetic events (v2 projector disabled)",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
        parts: [
          { type: "text", text: "hello v2" },
          {
            type: "file",
            mime: "text/plain",
            filename: "note.txt",
            url: "data:text/plain;base64,bm90ZSBjb250ZW50",
          },
        ],
      })

      const messages = yield* SessionV2.Service.use((session) => session.messages({ sessionID: chat.id })).pipe(
        Effect.provide(
          LayerNode.compile(SessionV2.node, [
            [SessionExecution.node, SessionExecution.noopLayer],
            [LocationServiceMap.node, locationServiceMapLayer],
          ]),
        ),
      )
      const { db } = yield* Database.Service
      const row = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, chat.id))
        .get()
        .pipe(Effect.orDie)
      expect(messages.find((message) => message.type === "user")).toMatchObject({ type: "user", text: "hello v2" })
      expect(typeof row?.data.time.created).toBe("number")
      expect(messages).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ type: "synthetic", text: expect.stringContaining("Called the Read tool") }),
          expect.objectContaining({ type: "synthetic", text: "note content" }),
        ]),
      )
    }),
  { config: cfg },
)

it.instance("static loop returns assistant text through local provider", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Prompt provider",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })

    yield* seedUser({
      sessionID: session.id,
      agent: "build",
      parts: [{ type: "text", text: "hello" }],
    })

    yield* llm.text("world")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(result.info.role).toBe("assistant")
    expect(result.parts.some((part) => part.type === "text" && part.text === "world")).toBe(true)
    expect(yield* llm.hits).toHaveLength(1)
    expect(yield* llm.pending).toBe(0)
  }),
)

it.instance("static loop consumes queued replies across turns", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Prompt provider turns",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })

    yield* seedUser({
      sessionID: session.id,
      agent: "build",
      parts: [{ type: "text", text: "hello one" }],
    })

    yield* llm.text("world one")

    const first = yield* prompt.loop({ sessionID: session.id })
    expect(first.info.role).toBe("assistant")
    expect(first.parts.some((part) => part.type === "text" && part.text === "world one")).toBe(true)

    yield* seedUser({
      sessionID: session.id,
      agent: "build",
      parts: [{ type: "text", text: "hello two" }],
    })

    yield* llm.text("world two")

    const second = yield* prompt.loop({ sessionID: session.id })
    expect(second.info.role).toBe("assistant")
    expect(second.parts.some((part) => part.type === "text" && part.text === "world two")).toBe(true)

    expect(yield* llm.hits).toHaveLength(2)
    expect(yield* llm.pending).toBe(0)
  }),
)

it.instance("loop continues when finish is tool-calls", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* seedUser({
      sessionID: session.id,
      agent: "build",
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.tool("first", { value: "first" })
    yield* llm.text("second")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(yield* llm.calls).toBe(2)
    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)
      expect(result.info.finish).toBe("stop")
    }
  }),
)

it.instance("loop continues when finish is unknown", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* seedUser({
      sessionID: session.id,
      agent: "build",
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.push(reply())
    yield* llm.text("second")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(yield* llm.calls).toBe(2)
    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)
      expect(result.info.finish).toBe("stop")
    }
  }),
)

it.instance("glob tool keeps instance context during prompt runs", () =>
  Effect.gen(function* () {
    const { dir, llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Glob context",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    const file = path.join(dir, "probe.txt")
    yield* writeText(file, "probe")

    yield* seedUser({
      sessionID: session.id,
      agent: "build",
      parts: [{ type: "text", text: "find text files" }],
    })
    yield* llm.tool("glob", { pattern: "**/*.txt" })
    yield* llm.text("done")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(result.info.role).toBe("assistant")

    const msgs = yield* MessageV2.filterCompactedEffect(session.id)
    const tool = msgs
      .flatMap((msg) => msg.parts)
      .find(
        (part): part is CompletedToolPart =>
          part.type === "tool" && part.tool === "glob" && part.state.status === "completed",
      )
    if (!tool) return

    expect(tool.state.output).toContain(file)
    expect(tool.state.output).not.toContain("No context found for instance")
    expect(result.parts.some((part) => part.type === "text" && part.text === "done")).toBe(true)
  }),
)

it.instance("loop continues when finish is stop but assistant has tool parts", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* seedUser({
      sessionID: session.id,
      agent: "build",
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.push(reply().tool("first", { value: "first" }).stop())
    yield* llm.text("second")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(yield* llm.calls).toBe(2)
    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)
      expect(result.info.finish).toBe("stop")
    }
  }),
)

it.instance("failed subtask preserves metadata on error tool state", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig((url) => ({
      ...providerCfg(url),
      agent: {
        general: {
          model: "test/missing-model",
        },
      },
    }))
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.tool("task", {
      description: "inspect bug",
      prompt: "look into the cache key path",
      subagent_type: "general",
    })
    yield* llm.text("done")
    const msg = yield* user(chat.id, "hello")
    yield* addSubtask(chat.id, msg.id)

    const result = yield* prompt.loop({ sessionID: chat.id })
    expect(result.info.role).toBe("assistant")
    expect(yield* llm.calls).toBe(2)

    const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
    const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
    expect(taskMsg?.info.role).toBe("assistant")
    if (!taskMsg || taskMsg.info.role !== "assistant") return

    const tool = errorTool(taskMsg.parts)
    if (!tool) return

    expect(tool.state.error).toContain("Tool execution failed")
    expect(tool.state.metadata).toBeDefined()
    expect(tool.state.metadata?.sessionId).toBeDefined()
    expect(tool.state.metadata?.model).toEqual({
      providerID: ProviderV2.ID.make("test"),
      modelID: ModelV2.ID.make("missing-model"),
    })
  }),
)

it.instance("subtask child inherits parent session external_directory allow", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Parent",
      permission: [{ permission: "external_directory", pattern: "/tmp/allowed/*", action: "allow" }],
    })
    yield* llm.text("done")
    const msg = yield* user(chat.id, "hello")
    yield* addSubtask(chat.id, msg.id)

    yield* prompt.loop({ sessionID: chat.id })

    const kids = yield* sessions.children(chat.id)
    expect(kids).toHaveLength(1)
    const child = kids[0]!
    const rules = child.permission ?? []
    expect(rules).toEqual(
      expect.arrayContaining([{ permission: "external_directory", pattern: "/tmp/allowed/*", action: "allow" }]),
    )
    expect(Permission.evaluate("external_directory", "/tmp/allowed/file", rules).action).toBe("allow")
    expect(Permission.evaluate("task", "anything", rules).action).toBe("deny")
  }),
)

noLLMServer.instance("prompt tools replace previous prompt tool rules", () =>
  Effect.gen(function* () {
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({ title: "Prompt tools" })

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      tools: { bash: false },
      parts: [{ type: "text", text: "first" }],
    })
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      tools: { read: true },
      parts: [{ type: "text", text: "second" }],
    })

    const reloaded = yield* sessions.get(session.id)
    expect(reloaded.permission).toEqual([{ permission: "read", pattern: "*", action: "allow" }])
    expect(Permission.evaluate("bash", "anything", reloaded.permission ?? []).action).toBe("ask")
  }),
)

it.instance(
  "running subtask preserves metadata after tool-call transition",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* llm.hang
      const msg = yield* user(chat.id, "hello")
      yield* addSubtask(chat.id, msg.id)

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)

      const tool = yield* pollWithTimeout(
        Effect.gen(function* () {
          const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
          const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
          const tool = taskMsg?.parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
          if (tool?.state.status === "running" && tool.state.metadata?.sessionId) return tool
        }),
        "timed out waiting for running subtask metadata",
      )

      if (tool.state.status !== "running") return
      expect(typeof tool.state.metadata?.sessionId).toBe("string")
      expect(tool.state.title).toBeDefined()
      expect(tool.state.metadata?.model).toBeDefined()

      yield* prompt.cancel(chat.id)
      yield* Fiber.await(fiber)
    }),
  5_000,
)

it.instance(
  "running task tool preserves metadata after tool-call transition",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.tool("task", {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      })
      yield* llm.hang
      yield* user(chat.id, "hello")

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)

      const tool = yield* pollWithTimeout(
        Effect.gen(function* () {
          const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
          const assistant = msgs.findLast((item) => item.info.role === "assistant" && item.info.agent === "build")
          const tool = assistant?.parts.find(
            (part): part is SessionV1.ToolPart => part.type === "tool" && part.tool === "task",
          )
          if (tool?.state.status === "running" && tool.state.metadata?.sessionId) return tool
        }),
        "timed out waiting for running task metadata",
      )

      if (tool.state.status !== "running") return
      expect(typeof tool.state.metadata?.sessionId).toBe("string")
      expect(tool.state.title).toBe("inspect bug")
      expect(tool.state.metadata?.model).toBeDefined()

      yield* prompt.cancel(chat.id)
      yield* Fiber.await(fiber)
    }),
  10_000,
)

it.instance(
  "loop sets status to busy then idle",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const status = yield* SessionStatus.Service

      yield* llm.hang

      const chat = yield* sessions.create({})
      yield* user(chat.id, "hi")

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)
      expect((yield* status.get(chat.id)).type).toBe("busy")
      yield* prompt.cancel(chat.id)
      yield* Fiber.await(fiber)
      expect((yield* status.get(chat.id)).type).toBe("idle")
    }),
  3_000,
)

// Cancel semantics

it.instance("cancel interrupts loop and resolves with an assistant message", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* seed(chat.id)

    yield* llm.hang

    yield* user(chat.id, "more")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)
    yield* prompt.cancel(chat.id)
    const exit = yield* Fiber.await(fiber)
    expect(Exit.isSuccess(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      expect(exit.value.info.role).toBe("assistant")
    }
  }),
)

it.instance(
  "delivers queued machine mail in its own turn after an earlier run is cancelled",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const originalResponse = yield* Deferred.make<void>()
      yield* Effect.gen(function* () {
        yield* llm.push(
          reply().wait(deferredAsPromise(originalResponse)).text("task finished").stop().item(),
          reply().text("captain follow-up handled").stop().item(),
          reply().text("machine mail handled").stop().item(),
        )
        const rootID = MessageID.make("msg_cancelled_run_root")
        const run = yield* prompt
          .prompt({
            sessionID: chat.id,
            messageID: rootID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "original task" }],
          })
          .pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(1), "provider did not receive the original task", "10 seconds")
        const machineID = MessageID.make("msg_cancelled_machine_mail")
        const machineText = "[fm-from-peer]\x1f peer alert"
        const machine = yield* prompt
          .prompt({
            sessionID: chat.id,
            messageID: machineID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: machineText }],
          })
          .pipe(Effect.forkChild)
        const queuedMachine = yield* pollWithTimeout(
          queue.list(chat.id).pipe(Effect.map((items) => items.find((item) => item.input.messageID === machineID))),
          "machine mail was not kept in the queue",
          "10 seconds",
        )
        expect(queuedMachine.delivery).toBe("queue")
        expect(yield* llm.calls).toBe(1)

        const cancellation = yield* prompt.cancel(chat.id).pipe(Effect.forkChild)
        const cancelExit = yield* awaitWithTimeout(
          Fiber.await(cancellation),
          "cancellation did not finish",
          "2 seconds",
        )
        expect(Exit.isSuccess(cancelExit)).toBe(true)
        const exit = yield* awaitWithTimeout(Fiber.await(run), "cancelled session run did not finish", "2 seconds")
        expect(Exit.isSuccess(exit)).toBe(true)
        const machineExit = yield* awaitWithTimeout(
          Fiber.await(machine),
          "queued machine caller did not settle with the cancelled run",
          "2 seconds",
        )
        expect(Exit.isSuccess(machineExit)).toBe(true)
        expect(yield* llm.calls).toBe(1)

        expect((yield* queue.list(chat.id)).some((item) => item.id === queuedMachine.id)).toBe(true)
        const followUpID = MessageID.make("msg_after_cancel_followup")
        const followUp = yield* prompt
          .prompt({
            sessionID: chat.id,
            messageID: followUpID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "captain follow-up" }],
          })
          .pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(2), "the captain follow-up did not reach the provider", "10 seconds")
        const followUpRequest = (yield* llm.inputs)[1]
        if (!followUpRequest) throw new Error("expected the captain follow-up request")
        expect(JSON.stringify(followUpRequest.messages)).toContain("captain follow-up")
        expect(JSON.stringify(followUpRequest.messages)).not.toContain(machineText)

        yield* awaitWithTimeout(llm.wait(3), "machine mail did not get its own turn", "10 seconds")
        const machineRequest = (yield* llm.inputs)[2]
        if (!machineRequest) throw new Error("expected the machine mail request")
        expect(JSON.stringify(machineRequest.messages)).toContain("peer alert")
        expect(
          Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(followUp), "follow-up did not finish", "10 seconds")),
        ).toBe(true)
        const messages = yield* sessions.messages({ sessionID: chat.id })
        const deliveredMachine = messages.findLast(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === machineText),
        )
        expect(deliveredMachine?.info.role).toBe("user")
        if (deliveredMachine?.info.role !== "user") throw new Error("expected the machine message to be promoted")
        expect(
          messages.some(
            (message) => message.info.role === "assistant" && message.info.parentID === deliveredMachine.info.id,
          ),
        ).toBe(true)
        expect(yield* llm.calls).toBe(3)
      }).pipe(Effect.ensuring(Deferred.succeed(originalResponse, void 0).pipe(Effect.ignore)))
    }),
  60_000,
)

it.instance(
  "cancels a held provider request while the drain is active",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const runState = yield* SessionRunState.Service
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const heldResponse = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        yield* llm.push(
          reply().text("task finished").stop().item(),
          reply().wait(deferredAsPromise(heldResponse)).text("held response").stop().item(),
        )
        yield* seedUser({
          sessionID: chat.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "original task" }],
        })
        const run = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(1), "provider did not receive the original task", "10 seconds")
        yield* seedUser({
          sessionID: chat.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "[fm-from-firstmate]\x1f held while draining" }],
        })
        yield* awaitWithTimeout(llm.wait(2), "held provider request did not start", "10 seconds")

        const cancellation = yield* runState.cancel(chat.id).pipe(Effect.forkChild)
        const cancelExit = yield* awaitWithTimeout(
          Fiber.await(cancellation),
          "cancel stayed blocked on the held provider request",
          "2 seconds",
        )
        expect(Exit.isSuccess(cancelExit)).toBe(true)
        const runExit = yield* awaitWithTimeout(Fiber.await(run), "cancelled drain did not finish", "2 seconds")
        expect(Exit.isSuccess(runExit)).toBe(true)
        expect(yield* llm.calls).toBe(2)
      }).pipe(
        Effect.ensuring(
          Effect.gen(function* () {
            yield* Deferred.succeed(heldResponse, void 0).pipe(Effect.ignore)
          }),
        ),
      )
    }),
  60_000,
)

it.instance(
  "cancels a held drain during a subtask tool step through runner cancellation",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const runState = yield* SessionRunState.Service
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const registry = yield* ToolRegistry.Service
      const { task } = yield* registry.named()
      const original = task.execute
      const ready = yield* Deferred.make<void>()
      const stopped = yield* Deferred.make<void>()
      const releaseTask = defer<void>()
      task.execute = (_args, ctx) =>
        Effect.callback<{
          title: string
          output: string
          metadata: {
            parentSessionId: SessionID
            sessionId: SessionID
            model: { modelID: ModelV2.ID; providerID: ProviderV2.ID }
          }
        }>((resume) => {
          let finished = false
          const complete = () => {
            if (finished) return
            finished = true
            resume(
              Effect.succeed({
                title: "held task",
                output: "task finished",
                metadata: {
                  parentSessionId: ctx.sessionID,
                  sessionId: ctx.sessionID,
                  model: { modelID: ref.modelID, providerID: ref.providerID },
                },
              }),
            )
          }
          releaseTask.promise.then(complete)
          succeedVoid(ready)
          return Effect.sync(() => {
            finished = true
            succeedVoid(stopped)
          })
        })
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          task.execute = original
          releaseTask.resolve()
        }),
      )

      const session = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const taskGate = yield* Deferred.make<void>()
      yield* llm.push(reply().wait(deferredAsPromise(taskGate)).text("task finished").stop().item())
      yield* seedUser({
        sessionID: session.id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "original task" }],
      })
      const run = yield* prompt.loop({ sessionID: session.id }).pipe(Effect.forkChild)
      let cancellation: Fiber.Fiber<void> | undefined
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          releaseTask.resolve()
          yield* Fiber.await(run).pipe(Effect.exit, Effect.asVoid)
          if (cancellation) yield* Fiber.await(cancellation).pipe(Effect.exit, Effect.asVoid)
        }),
      )
      yield* awaitWithTimeout(llm.wait(1), "provider did not receive the original task", "10 seconds")

      const heldID = MessageID.make("msg_held_subtask")
      yield* seedUser({
        sessionID: session.id,
        messageID: heldID,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "[fm-from-peer]\x1f held task" }],
      })
      yield* addSubtask(session.id, heldID)
      yield* Deferred.succeed(taskGate, void 0)
      yield* awaitWithTimeout(Deferred.await(ready), "held subtask did not start", "10 seconds")

      cancellation = yield* runState.cancel(session.id).pipe(Effect.forkChild)
      const cancelExit = yield* awaitWithTimeout(
        Fiber.await(cancellation),
        "runner cancellation stayed blocked on the held tool step",
        "2 seconds",
      )
      expect(Exit.isSuccess(cancelExit)).toBe(true)
      expect(
        Exit.isSuccess(yield* awaitWithTimeout(Fiber.await(run), "held tool-step drain did not finish", "2 seconds")),
      ).toBe(true)
      yield* awaitWithTimeout(Deferred.await(stopped), "held subtask fiber did not stop", "2 seconds")
      expect(yield* llm.calls).toBe(1)
    }),
  60_000,
)

it.instance("cancel records MessageAbortedError on interrupted process", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.hang
    yield* user(chat.id, "hello")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)
    yield* prompt.cancel(chat.id)
    const exit = yield* Fiber.await(fiber)
    expect(Exit.isSuccess(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      const info = exit.value.info
      if (info.role === "assistant") {
        expect(info.error?.name).toBe("MessageAbortedError")
      }
    }
  }),
)

raceNoLLMServer.instance(
  "finalizes assistant when cancelled before processor creation completes",
  () =>
    Effect.gen(function* () {
      processorCreateStarted.length = 0
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          processorCreateStarted.length = 0
        }),
      )

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Processor creation race" })

      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        parts: [{ type: "text", text: "first" }],
      })

      const firstCreate = defer<void>()
      processorCreateStarted.push(firstCreate.resolve)
      const first = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.promise(() => firstCreate.promise)

      yield* prompt.cancel(chat.id)
      const firstExit = yield* Fiber.await(first)
      expect(Exit.isSuccess(firstExit)).toBe(true)

      let messages = yield* sessions.messages({ sessionID: chat.id })
      const firstInterrupted = messages.at(-1)
      expect(firstInterrupted?.info.role).toBe("assistant")
      expect(firstInterrupted?.parts).toHaveLength(0)
      if (firstInterrupted?.info.role === "assistant") {
        expect(firstInterrupted.info.finish).toBeUndefined()
        expect(firstInterrupted.info.time.completed).toBeNumber()
        expect(firstInterrupted.info.error?.name).toBe("MessageAbortedError")
      }

      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        parts: [{ type: "text", text: "second" }],
      })

      const secondCreate = defer<void>()
      processorCreateStarted.push(secondCreate.resolve)
      const second = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.promise(() => secondCreate.promise)

      yield* prompt.cancel(chat.id)
      const secondExit = yield* Fiber.await(second)
      expect(Exit.isSuccess(secondExit)).toBe(true)

      messages = yield* sessions.messages({ sessionID: chat.id })
      const poisonMessages = messages.filter(
        (message) =>
          message.info.role === "assistant" &&
          message.parts.length === 0 &&
          !message.info.finish &&
          !message.info.time.completed &&
          !message.info.error,
      )
      expect(poisonMessages).toHaveLength(0)

      const interruptedMessages = messages.filter(
        (message) =>
          message.info.role === "assistant" &&
          message.parts.length === 0 &&
          message.info.time.completed &&
          message.info.error?.name === "MessageAbortedError",
      )
      expect(interruptedMessages).toHaveLength(2)

      const lastUser = messages.at(-2)
      const lastAssistant = messages.at(-1)
      expect(lastUser?.info.role).toBe("user")
      expect(lastAssistant?.info.role).toBe("assistant")
      if (lastUser?.info.role === "user" && lastAssistant?.info.role === "assistant") {
        expect(lastAssistant.info.parentID).toBe(lastUser?.info.id)
      }
    }),
  { config: cfg },
  3_000,
)

noLLMServer.instance(
  "cancel finalizes subtask tool state",
  () =>
    Effect.gen(function* () {
      const ready = yield* Deferred.make<void>()
      const aborted = yield* Deferred.make<void>()
      const registry = yield* ToolRegistry.Service
      const { task } = yield* registry.named()
      const original = task.execute
      task.execute = (_args, ctx) =>
        Effect.callback<never>((_resume) => {
          ctx.abort.addEventListener("abort", () => succeedVoid(aborted), { once: true })
          if (ctx.abort.aborted) succeedVoid(aborted)
          succeedVoid(ready)
          return Effect.sync(() => succeedVoid(aborted))
        })
      yield* Effect.addFinalizer(() => Effect.sync(() => void (task.execute = original)))

      const { prompt, chat } = yield* boot()
      const msg = yield* user(chat.id, "hello")
      yield* addSubtask(chat.id, msg.id)

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(ready), "timed out waiting for task tool to start", "10 seconds")
      yield* prompt.cancel(chat.id)

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)
      yield* awaitWithTimeout(Deferred.await(aborted), "timed out waiting for task tool abort", "10 seconds")

      const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
      const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
      expect(taskMsg?.info.role).toBe("assistant")
      if (!taskMsg || taskMsg.info.role !== "assistant") return

      const tool = toolPart(taskMsg.parts)
      expect(tool?.type).toBe("tool")
      if (!tool) return

      expect(tool.state.status).not.toBe("running")
      expect(taskMsg.info.time.completed).toBeDefined()
      expect(taskMsg.info.finish).toBeDefined()
    }),
  { config: cfg },
  30_000,
)

it.instance(
  "cancel propagates from slash command subtask to child session",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const status = yield* SessionStatus.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* llm.hang
      const msg = yield* user(chat.id, "hello")
      yield* addSubtask(chat.id, msg.id)

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)

      const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
      const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
      const tool = taskMsg ? toolPart(taskMsg.parts) : undefined
      const sessionID = tool?.state.status === "running" ? tool.state.metadata?.sessionId : undefined
      expect(typeof sessionID).toBe("string")
      if (typeof sessionID !== "string") throw new Error("missing child session id")
      const childID = SessionID.make(sessionID)
      expect((yield* status.get(childID)).type).toBe("busy")

      yield* prompt.cancel(chat.id)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)

      expect((yield* status.get(chat.id)).type).toBe("idle")
      expect((yield* status.get(childID)).type).toBe("idle")
    }),
  10_000,
)

it.instance(
  "cancel with queued callers resolves all cleanly",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* llm.hang
      yield* user(chat.id, "hello")

      const a = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)
      const b = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      yield* prompt.cancel(chat.id)
      const [exitA, exitB] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])
      expect(Exit.isSuccess(exitA)).toBe(true)
      expect(Exit.isSuccess(exitB)).toBe(true)
      if (Exit.isSuccess(exitA) && Exit.isSuccess(exitB)) {
        expect(exitA.value.info.id).toBe(exitB.value.info.id)
      }
    }),
  { git: true },
  10_000,
)

// Queue semantics

noLLMServer.instance("concurrent loop callers get same result", () =>
  Effect.gen(function* () {
    const { prompt, run, chat } = yield* boot()
    yield* seed(chat.id, { finish: "stop" })

    const [a, b] = yield* Effect.all([prompt.loop({ sessionID: chat.id }), prompt.loop({ sessionID: chat.id })], {
      concurrency: "unbounded",
    })

    expect(a.info.id).toBe(b.info.id)
    expect(a.info.role).toBe("assistant")
    yield* run.assertNotBusy(chat.id)
  }),
)

it.instance("concurrent loop callers all receive same error result", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })

    yield* llm.fail("boom")
    yield* user(chat.id, "hello")

    const [a, b] = yield* Effect.all([prompt.loop({ sessionID: chat.id }), prompt.loop({ sessionID: chat.id })], {
      concurrency: "unbounded",
    })
    expect(a.info.id).toBe(b.info.id)
    expect(a.info.role).toBe("assistant")
  }),
)

it.instance("prompt submitted during an active run is included in the next LLM input", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const gate = yield* Deferred.make<void>()
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })

    yield* llm.hold("first", deferredAsPromise(gate))
    yield* llm.text("second")

    const a = yield* prompt
      .prompt({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "first" }],
      })
      .pipe(Effect.forkChild)

    yield* llm.wait(1)
    yield* waitForBusy(chat.id)

    const id = MessageID.ascending()
    const b = yield* prompt
      .prompt({
        sessionID: chat.id,
        messageID: id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "second" }],
      })
      .pipe(Effect.forkChild)

    yield* pollWithTimeout(
      sessions
        .messages({ sessionID: chat.id })
        .pipe(
          Effect.map((msgs) => (msgs.some((msg) => msg.info.role === "user" && msg.info.id === id) ? true : undefined)),
        ),
      "timed out waiting for second prompt to save",
    )

    yield* Deferred.succeed(gate, void 0)

    const [ea, eb] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])
    expect(Exit.isSuccess(ea)).toBe(true)
    expect(Exit.isSuccess(eb)).toBe(true)
    expect(yield* llm.calls).toBe(2)

    const msgs = yield* sessions.messages({ sessionID: chat.id })
    const assistants = msgs.filter((msg) => msg.info.role === "assistant")
    expect(assistants).toHaveLength(2)
    const last = assistants.at(-1)
    if (!last || last.info.role !== "assistant") throw new Error("expected second assistant")
    expect(last.info.parentID).toBe(id)
    expect(last.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)

    const inputs = yield* llm.inputs
    expect(inputs).toHaveLength(2)
    expect(modelMessages(inputs.at(-1)).at(-1)).toEqual({ role: "user", content: "second" })
  }),
)

gated.instance(
  "prompt admitted after the finishing run's last history read is still answered",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const gate = yield* Deferred.make<void>()
      const finishingRead = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      const joined = { reached: yield* Deferred.make<void>(), startedRun: false }
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          gates.finishingRead = undefined
          gates.nextEnsureRunning = undefined
        }),
      )

      yield* llm.hold("first", deferredAsPromise(gate))
      yield* llm.text("second")

      const a = yield* prompt
        .prompt({ sessionID: chat.id, agent: "build", model: ref, parts: [{ type: "text", text: "first" }] })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")

      // The first run's last history read happens after this release; its
      // following lastAssistant read is held open by the gated Session layer.
      gates.finishingRead = finishingRead
      yield* Deferred.succeed(gate, void 0)
      yield* awaitWithTimeout(Deferred.await(finishingRead.entered), "finishing lastAssistant read never started")

      gates.nextEnsureRunning = joined
      const b = yield* prompt
        .prompt({ sessionID: chat.id, agent: "build", model: ref, parts: [{ type: "text", text: "second" }] })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(joined.reached), "second prompt never reached ensureRunning")
      yield* Deferred.succeed(finishingRead.release, void 0)

      const [ea, eb] = yield* awaitWithTimeout(
        Effect.all([Fiber.await(a), Fiber.await(b)]),
        "prompts never finished",
        "10 seconds",
      )
      expect(Exit.isSuccess(ea)).toBe(true)
      expect(Exit.isSuccess(eb)).toBe(true)
      // The second prompt must have joined the finishing run; a run of its own
      // would mean this setup never exercised the finishing window.
      expect(joined.startedRun).toBe(false)
      yield* awaitWithTimeout(llm.wait(2), "second prompt was never answered")
      expect(modelMessages((yield* llm.inputs).at(1)).at(-1)).toEqual({ role: "user", content: "second" })
      // A synchronous caller gets the reply to its own prompt, not the run it joined.
      const asked = (yield* sessions.messages({ sessionID: chat.id })).find((msg) =>
        msg.parts.some((part) => part.type === "text" && part.text === "second"),
      )
      const answer = Exit.isSuccess(eb) ? eb.value : undefined
      expect(answer?.info.role === "assistant" ? answer.info.parentID : undefined).toBe(asked?.info.id)
      expect(answer?.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)
    }),
  10_000,
)

// Prompt queue (swxtchio/swx-opencode#68). Expected model inputs come from the
// prompts each test sends, never from the queue under test.

const said = (value: string) => [{ type: "text" as const, text: value }]

// Without the note a prompt that waited in the queue is delivered with (#212),
// which test/server/httpapi-session.test.ts checks.
const modelMessages = (input: Record<string, unknown> | undefined) => {
  const messages = input?.messages
  if (!Array.isArray(messages)) throw new Error("expected LLM messages")
  return messages.map((message) => {
    if (message?.role !== "user" || !Array.isArray(message.content)) return message
    const kept = message.content.filter(
      (part: { type?: string; text?: string }) => !(part.type === "text" && part.text?.startsWith("[Queued prompt")),
    )
    if (kept.length === message.content.length) return message
    return { ...message, content: kept.length === 1 && kept[0].type === "text" ? kept[0].text : kept }
  })
}

const lastUser = (input: Record<string, unknown> | undefined) => modelMessages(input).at(-1)

function visibleUserText(message: SessionV1.WithParts) {
  return message.parts
    .filter((part): part is SessionV1.TextPart => part.type === "text" && !part.synthetic && !part.ignored)
    .map((part) => part.text)
    .join("")
}

function lastUserText(input: Record<string, unknown>) {
  const messages = input.messages
  if (!Array.isArray(messages)) throw new Error("expected LLM messages")
  const message = messages.findLast(
    (item: unknown) => typeof item === "object" && item !== null && "role" in item && item.role === "user",
  )
  if (!message || typeof message !== "object" || !("content" in message)) return ""
  const queueNote = (text: string) => text.replace(/^\[Queued prompt[^\]]*\]\s*/, "")
  if (typeof message.content === "string") return queueNote(message.content)
  const content = message.content
  if (!Array.isArray(content)) return ""
  return content.flatMap((part: unknown) =>
    typeof part === "object" && part !== null && "text" in part && typeof part.text === "string"
      ? [queueNote(part.text)]
      : [],
  ).join("")
}

const mentions = (input: Record<string, unknown> | undefined, value: string) =>
  JSON.stringify(modelMessages(input)).includes(value)

const queued = (sessionID: SessionID, count: number) =>
  pollWithTimeout(
    Effect.gen(function* () {
      const queue = yield* SessionQueue.Service
      const items = yield* queue.list(sessionID)
      return items.length === count ? items : undefined
    }),
    `queue never held ${count} item(s)`,
  )

// Waits until a prompt was admitted, whether it is still queued or already in
// the message history, so a regression shows up in the model inputs instead.
const admitted = (sessionID: SessionID, text: string) =>
  pollWithTimeout(
    Effect.gen(function* () {
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      if (
        (yield* queue.list(sessionID)).some((item) =>
          item.input.parts.some((part) => part.type === "text" && part.text === text),
        )
      )
        return true
      const msgs = yield* sessions.messages({ sessionID })
      return msgs.some((msg) => msg.parts.some((part) => part.type === "text" && part.text === text)) ? true : undefined
    }),
    `prompt "${text}" was never admitted`,
  )

// Joins the prompts of one test, failing on its own if a drain never ends.
const finish = <A, E>(...fibers: Fiber.Fiber<A, E>[]) =>
  awaitWithTimeout(Effect.all(fibers.map((fiber) => Fiber.join(fiber))), "prompts never finished", "10 seconds")

const startHeld = Effect.fn("test.startHeld")(function* (input?: {
  tool?: boolean | { name: string; args: Record<string, unknown> }
  agent?: string
  format?: SessionPrompt.PromptInput["format"]
  diagnostic?: (value: {
    sessionID: SessionID
    calls: number
    runner?: { state: string; frame?: string; fiberID?: number }
  }) => void
  config?: (url: string) => Partial<ConfigV1.Info>
}) {
  const { llm } = yield* useServerConfig(input?.config ?? providerCfg)
  const prompt = yield* SessionPrompt.Service
  const sessions = yield* Session.Service
  const queue = yield* SessionQueue.Service
  const runState = yield* SessionRunState.Service
  const chat = yield* sessions.create({
    title: "Pinned",
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  })
  const gate = yield* Deferred.make<void>()
  yield* input?.tool
    ? llm.push(
        reply()
          .wait(deferredAsPromise(gate))
          .tool(
            input.tool === true ? "first" : input.tool.name,
            input.tool === true ? { value: "first" } : input.tool.args,
          ),
      )
    : llm.hold("task done", deferredAsPromise(gate))
  const task = yield* prompt
    .prompt({
      sessionID: chat.id,
      agent: input?.agent ?? "build",
      model: ref,
      format: input?.format,
      parts: said("start the task"),
    })
    .pipe(Effect.forkChild)
  // A prompt setup failure must retain its cause rather than masquerade as provider readiness.
  yield* awaitWithTimeout(
    Effect.raceFirst(
      llm.wait(1),
      Fiber.join(task).pipe(Effect.andThen(Effect.fail(new Error("prompt finished before first provider call")))),
    ),
    "first provider call never started",
    "10 seconds",
  ).pipe(
    Effect.onError((cause) =>
      Effect.gen(function* () {
        const pending = yield* queue.list(chat.id).pipe(Effect.timeoutOption("100 millis"), Effect.exit)
        const history = yield* sessions.messages({ sessionID: chat.id }).pipe(
          Effect.map((messages) =>
            messages.map((message) => ({ id: message.info.id, role: message.info.role, parts: message.parts.length })),
          ),
          Effect.timeoutOption("100 millis"),
          Effect.exit,
        )
        const calls = yield* llm.calls
        const promptExit = task.pollUnsafe()
        const active = yield* runState.current(chat.id)
        const running =
          active?._tag === "Running"
            ? active.run.fiber
            : active?._tag === "Shell" || active?._tag === "ShellThenRun"
              ? active.shell.fiber
              : undefined
        const runner = active && { state: active._tag, frame: running?.currentStackFrame?.name, fiberID: running?.id }
        input?.diagnostic?.({ sessionID: chat.id, calls, runner })
        yield* Effect.sync(() =>
          console.error("first provider boundary failed", {
            cause: Cause.pretty(cause),
            error: Cause.squash(cause),
            promptExit: promptExit && (Exit.isFailure(promptExit) ? Cause.pretty(promptExit.cause) : "success"),
            promptFrame: task.currentStackFrame,
            runner,
            runnerExit: running?.pollUnsafe(),
            calls,
            pending:
              Exit.isSuccess(pending) && Option.isSome(pending.value)
                ? pending.value.value.map((item) => ({ id: item.id, delivery: item.delivery }))
                : pending,
            history,
          }),
        )
      }),
    ),
  )
  const send = (text: string, extra?: Partial<SessionPrompt.PromptInput>) =>
    prompt
      .prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said(text), ...extra })
      .pipe(Effect.forkChild)
  const release = Deferred.succeed(gate, void 0)
  return { llm, prompt, sessions, queue, chat, task, send, release }
})

it.instance("prompt enhancement uses the configured provider without writing to a session", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    yield* llm.text("clear rewritten draft")
    const result = yield* prompt.enhance({
      model: "test/test-model",
      draft: "rough draft",
      context: "visible session context",
    })
    const calls = yield* llm.inputs
    expect(result).toBe("clear rewritten draft")
    expect(calls).toHaveLength(1)
    expect(calls[0]?.model).toBe("test-model")
    expect(JSON.stringify(calls[0])).toContain("visible session context")
    expect(JSON.stringify(calls[0])).toContain("rough draft")
    expect(JSON.stringify(calls[0])).toContain("preserving its intent")
    expect(yield* sessions.messages({ sessionID: session.id })).toEqual([])
  }),
  20_000,
)

it.instance("prompt enhancement reports an unavailable configured model before making a provider request", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const result = yield* Effect.exit(
      prompt.enhance({ model: "test/missing-model", draft: "draft", context: "" }),
    )
    expect(Exit.isFailure(result)).toBe(true)
    expect(yield* llm.calls).toBe(0)
  }),
)

it.instance("prompt enhancement reports provider failures", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    yield* llm.error(503, { error: "provider unavailable" })
    const result = yield* Effect.exit(
      prompt.enhance({ model: "test/test-model", draft: "draft", context: "visible context" }),
    )
    expect(Exit.isFailure(result)).toBe(true)
    expect(yield* llm.calls).toBeGreaterThan(0)
  }),
)

it.instance("direct TUI composer prompts persist their transport origin on text parts", () =>
  Effect.gen(function* () {
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const message = yield* prompt.prompt({
      sessionID: session.id,
      origin: "tui-composer",
      noReply: true,
      parts: [{ type: "text", text: "direct composer prompt" }],
    })

    expect(
      message.parts.some(
        (part) =>
          part.type === "text" && part.metadata?.[SessionPromptQueue.TuiComposerOriginMetadataKey] === true,
      ),
    ).toBe(true)
  }),
)

it.instance("busy TUI composer prompts are answered once as one ordered user message", () =>
  Effect.gen(function* () {
    const held = yield* startHeld()
    const first = yield* held.send("A", {
      requestID: "composer-a",
      delivery: "queue",
      origin: "tui-composer",
    })
    const second = yield* held.send("B", {
      requestID: "composer-b",
      delivery: "queue",
      origin: "tui-composer",
    })
    yield* queued(held.chat.id, 2)
    yield* held.llm.text("one answer for the batch")
    yield* held.release
    yield* finish(held.task, first, second)

    const calls = yield* held.llm.inputs
    const batchedCalls = calls.filter((call) => lastUserText(call).includes("A\nB"))
    expect(calls).toHaveLength(2)
    expect(batchedCalls).toHaveLength(1)
    expect(lastUserText(batchedCalls[0]!)).toBe("A\nB")
    const messages = yield* held.sessions.messages({ sessionID: held.chat.id })
    const delivered = messages.filter((message) => message.info.role === "user" && visibleUserText(message) === "A\nB")
    expect(delivered).toHaveLength(1)
    for (const requestID of ["composer-a", "composer-b"]) {
      const receipt = yield* held.queue.receipt(held.chat.id, requestID)
      expect(Option.isSome(receipt) ? receipt.value : undefined).toEqual({
        state: "delivered",
        messageID: delivered[0]!.info.id,
      })
    }
    expect(yield* held.queue.list(held.chat.id)).toEqual([])
  }),
  { timeout: 30000 },
)

it.instance("an individual queued prompt retains its agent and session model variant", () =>
  Effect.gen(function* () {
    const held = yield* startHeld({
      config: (url) => {
        const base = providerCfg(url)
        return {
          ...base,
          provider: {
            ...base.provider,
            test: {
              ...base.provider.test,
              models: {
                ...base.provider.test.models,
                "test-model": {
                  ...base.provider.test.models["test-model"],
                  variants: { low: {}, high: {} },
                },
              },
            },
          },
        } as Partial<ConfigV1.Info>
      },
    })
    const queuedPrompt = yield* held.send("individual recipient selection", {
      delivery: "queue",
      agent: "plan",
      variant: "high",
    })
    yield* queued(held.chat.id, 1)
    yield* held.llm.text("queued prompt answered")
    yield* held.release
    yield* finish(held.task, queuedPrompt)

    const delivered = (yield* held.sessions.messages({ sessionID: held.chat.id })).find(
      (message) => message.info.role === "user" && visibleUserText(message) === "individual recipient selection",
    )
    expect(delivered?.info.role).toBe("user")
    if (delivered?.info.role !== "user") throw new Error("individual prompt did not reach history")
    expect(delivered.info.agent).toBe("plan")
    expect(delivered.info.model.providerID).toBe(ref.providerID)
    expect(delivered.info.model.modelID).toBe(ref.modelID)
    expect(delivered.info.model.variant).toBe("high")
  }),
  { timeout: 30000 },
)

it.instance(
  "identical busy TUI composer prompts survive admission and answer once as A newline A",
  () =>
    Effect.gen(function* () {
      const held = yield* startHeld()
      const first = yield* held.send("A", {
        requestID: "composer-duplicate-a",
        delivery: "queue",
        origin: "tui-composer",
      })
      const second = yield* held.send("A", {
        requestID: "composer-duplicate-b",
        delivery: "queue",
        origin: "tui-composer",
      })
      const pending = yield* queued(held.chat.id, 2)
      expect(pending.map((item) => item.input.parts[0]?.type === "text" && item.input.parts[0].text)).toEqual([
        "A",
        "A",
      ])
      yield* held.llm.text("one answer for the duplicate batch")
      yield* held.release
      yield* finish(held.task, first, second)

      const calls = yield* held.llm.inputs
      const batch = calls.filter((call) => lastUserText(call).includes("A\nA"))
      expect(calls).toHaveLength(2)
      expect(batch).toHaveLength(1)
      expect(lastUserText(batch[0]!)).toBe("A\nA")
      const delivered = (yield* held.sessions.messages({ sessionID: held.chat.id })).filter(
        (message) => message.info.role === "user" && visibleUserText(message) === "A\nA",
      )
      expect(delivered).toHaveLength(1)
      for (const requestID of ["composer-duplicate-a", "composer-duplicate-b"]) {
        const receipt = yield* held.queue.receipt(held.chat.id, requestID)
        expect(Option.isSome(receipt) ? receipt.value : undefined).toEqual({
          state: "delivered",
          messageID: delivered[0]!.info.id,
        })
      }
      expect(yield* held.queue.list(held.chat.id)).toEqual([])
    }),
  { timeout: 30000 },
)

function registerEnvironmentTest(name: string, enabled: boolean, register: () => void) {
  if (enabled) return register()
  test.skip(name, () => {})
}

registerEnvironmentTest(
  "dangling-assistant-process-worker persists unfinished reasoning and tool parts before process exit",
  process.env.OPENCODE_DANGLING_ASSISTANT_OUTPUT !== undefined,
  () =>
    abruptPrompt.instance(
      "dangling-assistant-process-worker persists unfinished reasoning and tool parts before process exit",
      () =>
        Effect.gen(function* () {
          const output = process.env.OPENCODE_DANGLING_ASSISTANT_OUTPUT
          if (!output) return

          const directory = yield* TestInstance
          const prompt = yield* SessionPrompt.Service
          const sessions = yield* Session.Service
          const chat = yield* sessions.create({
            title: "Dangling assistant process",
            permission: [{ permission: "*", pattern: "*", action: "allow" }],
          })
          abruptAssistantLLMCalls.value = 0
          yield* prompt
            .prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("start unfinished work") })
            .pipe(Effect.forkChild)

          const unfinished = yield* pollWithTimeout(
            Effect.gen(function* () {
              const history = yield* sessions.messages({ sessionID: chat.id })
              const assistant = history.findLast(
                (message) => message.info.role === "assistant" && message.info.time.completed === undefined,
              )
              if (!assistant || assistant.info.role !== "assistant" || assistant.parts.length < 3) return undefined
              return assistant
            }),
            "the production turn did not persist any assistant parts",
            "15 seconds",
          )
          const hasOpenReasoning = unfinished.parts.some(
            (part) => part.type === "reasoning" && part.time.end === undefined && part.metadata !== undefined,
          )
          const runningTool = unfinished.parts.find(
            (part): part is SessionV1.ToolPart => part.type === "tool" && part.state.status === "running",
          )
          if (!hasOpenReasoning || !runningTool)
            throw new Error(`production parts were not both open: ${JSON.stringify(unfinished.parts)}`)
          const messages = yield* sessions.messages({ sessionID: chat.id })

          const session = yield* sessions.get(chat.id)
          yield* Effect.promise(() =>
            Bun.write(output, JSON.stringify({ session, messages, directory: directory.directory })),
          )
          process.exit(0)
        }),
      { git: true, config: cfg },
      30_000,
    ),
)

registerEnvironmentTest(
  "dangling-assistant-shell-worker persists a running shell tool before process exit",
  process.env.OPENCODE_DANGLING_SHELL_OUTPUT !== undefined,
  () =>
    it.instance(
      "dangling-assistant-shell-worker persists a running shell tool before process exit",
      () =>
        Effect.gen(function* () {
          const output = process.env.OPENCODE_DANGLING_SHELL_OUTPUT
          const sourcePath = process.env.OPENCODE_DANGLING_SOURCE
          const command = process.env.OPENCODE_DANGLING_COMMAND
          if (!output || !sourcePath || !command) return

          const prompt = yield* SessionPrompt.Service
          const sessions = yield* Session.Service
          const status = yield* SessionStatus.Service
          const events = yield* EventV2Bridge.Service
          const source = Schema.decodeUnknownSync(Schema.Struct({ messages: Schema.Array(SessionV1.WithParts) }))(
            Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(
              yield* Effect.promise(() => Bun.file(sourcePath).text()),
            ),
          )
          const chat = yield* sessions.create({
            title: "Dangling assistant shell",
            permission: [{ permission: "*", pattern: "*", action: "allow" }],
          })
          const shellOwnerEvent = yield* Deferred.make<SessionStatus.Info>()
          const unsubscribe = yield* events.listen((event) => {
            if (event.type !== SessionStatus.Event.Status.type) return Effect.void
            return Effect.gen(function* () {
              const data = Schema.decodeUnknownSync(SessionStatus.Event.Status.data)(event.data)
              if (
                data.sessionID === chat.id &&
                data.status.type === "busy" &&
                data.status.activeAssistantMessageID !== undefined &&
                data.status.activeAssistantMessageID !== null
              )
                yield* Deferred.succeed(shellOwnerEvent, data.status)
            })
          })
          yield* Effect.forEach(
            source.messages,
            (message) =>
              Effect.gen(function* () {
                const info = Schema.decodeUnknownSync(SessionV1.Info)({ ...message.info, sessionID: chat.id })
                // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- the legacy writer requires mutable V1 records after schema validation
                const mutableInfo = info as SessionV1.Info
                yield* sessions.updateMessage(mutableInfo)
                yield* Effect.forEach(
                  message.parts,
                  (part) =>
                    Effect.gen(function* () {
                      const validated = Schema.decodeUnknownSync(SessionV1.Part)({ ...part, sessionID: chat.id })
                      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- the legacy writer requires mutable V1 parts after schema validation
                      const mutable = validated as SessionV1.Part
                      yield* sessions.updatePart(mutable)
                    }),
                  { discard: true },
                )
              }),
            { discard: true },
          )
          yield* prompt.shell({ sessionID: chat.id, agent: "build", model: ref, command }).pipe(Effect.forkChild)
          const owner = yield* pollWithTimeout(
            Effect.gen(function* () {
              const history = yield* sessions.messages({ sessionID: chat.id })
              const assistant = history.findLast(
                (message) =>
                  message.info.role === "assistant" &&
                  message.parts.some(
                    (part) =>
                      part.type === "tool" &&
                      part.tool === "bash" &&
                      part.state.status === "running" &&
                      part.state.metadata?.output?.includes("started"),
                  ),
              )
              if (!assistant || assistant.info.role !== "assistant") return
              const currentStatus = yield* status.get(chat.id)
              return currentStatus.type === "busy" && currentStatus.activeAssistantMessageID === assistant.info.id
                ? { messages: history, status: currentStatus, assistantID: assistant.info.id }
                : undefined
            }),
            "the production shell owner did not publish its running assistant identity",
            "15 seconds",
          )
          const statusEvent = yield* awaitWithTimeout(
            Deferred.await(shellOwnerEvent),
            "the running shell owner event did not include its assistant ID",
            "15 seconds",
          )
          yield* unsubscribe
          const directory = yield* TestInstance
          const session = yield* sessions.get(chat.id)
          yield* Effect.promise(() =>
            Bun.write(output, JSON.stringify({ ...owner, session, statusEvent, directory: directory.directory })),
          )
          process.exit(0)
        }),
      { git: true, config: cfg },
      30_000,
    ),
)

registerEnvironmentTest(
  "dangling-assistant-task-process-worker persists a running task with retrying child status before process exit",
  process.env.OPENCODE_DANGLING_TASK_OUTPUT !== undefined,
  () =>
    it.instance(
      "dangling-assistant-task-process-worker persists a running task with retrying child status before process exit",
      () =>
        Effect.gen(function* () {
          const output = process.env.OPENCODE_DANGLING_TASK_OUTPUT
          if (!output) return

          const { dir, llm } = yield* useServerConfig(providerCfg)
          const childFile = path.join(dir, "task-child.txt")
          yield* writeText(childFile, "task child read result")
          const prompt = yield* SessionPrompt.Service
          const sessions = yield* Session.Service
          const status = yield* SessionStatus.Service
          const chat = yield* sessions.create({
            title: "Dangling assistant task",
            permission: [{ permission: "*", pattern: "*", action: "allow" }],
          })
          yield* llm.tool("task", {
            description: "Inspect task ownership",
            prompt: "inspect a task while the model retries",
            subagent_type: "general",
          })
          yield* llm.tool("read", { filePath: childFile })
          yield* llm.error(429, {
            error: { message: "Provider is rate limited", type: "rate_limit_error", code: "rate_limit_exceeded" },
          })
          yield* llm.hang
          yield* user(chat.id, "start a task")
          yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)

          const task = yield* pollWithTimeout(
            Effect.gen(function* () {
              const messages = yield* sessions.messages({ sessionID: chat.id })
              const assistant = messages.findLast((message) => message.info.role === "assistant")
              if (!assistant || assistant.info.role !== "assistant") return
              const task = assistant.parts.find(
                (part): part is SessionV1.ToolPart => part.type === "tool" && part.tool === "task",
              )
              if (task?.state.status === "running" && typeof task.state.metadata?.sessionId === "string")
                return { assistant, task, childSessionID: task.state.metadata.sessionId }
            }),
            "the production Task tool did not enter its running state",
            "15 seconds",
          )
          const childSessionID = SessionID.make(task.childSessionID)
          const childStatus = yield* pollWithTimeout(
            Effect.gen(function* () {
              const value = yield* status.get(childSessionID)
              return value.type === "retry" ? value : undefined
            }),
            "the production Task child did not enter retry status",
            "15 seconds",
          )
          const childMessages = yield* sessions.messages({ sessionID: childSessionID })
          yield* prompt
            .prompt({
              sessionID: chat.id,
              agent: "build",
              model: ref,
              parts: said("steer the active Task turn"),
            })
            .pipe(Effect.forkChild)
          const parentMessages = yield* pollWithTimeout(
            Effect.gen(function* () {
              const messages = yield* sessions.messages({ sessionID: chat.id })
              const last = messages.at(-1)
              const parentStatus = yield* status.get(chat.id)
              const hasSteer =
                last?.info.role === "user" &&
                last.parts.some((part) => part.type === "text" && part.text === "steer the active Task turn")
              return hasSteer &&
                parentStatus.type === "busy" &&
                parentStatus.activeAssistantMessageID === task.assistant.info.id
                ? messages
                : undefined
            }),
            "the steered Task turn lost its active assistant identity",
            "15 seconds",
          )
          const parentStatus = yield* status.get(chat.id)
          const session = yield* sessions.get(chat.id)
          yield* Effect.promise(() =>
            Bun.write(
              output,
              JSON.stringify({
                session,
                messages: parentMessages,
                taskAssistantID: task.assistant.info.id,
                childSessionID,
                childMessages,
                childStatus,
                parentStatus,
                directory: dir,
              }),
            ),
          )
          process.exit(0)
        }),
      { git: true, config: cfg },
      30_000,
    ),
)

registerEnvironmentTest(
  "dangling-assistant-subtask-process-worker persists a direct subtask owner before process exit",
  process.env.OPENCODE_DANGLING_SUBTASK_OUTPUT !== undefined,
  () =>
    it.instance(
      "dangling-assistant-subtask-process-worker persists a direct subtask owner before process exit",
      () =>
        Effect.gen(function* () {
          const output = process.env.OPENCODE_DANGLING_SUBTASK_OUTPUT
          if (!output) return

          const { dir, llm } = yield* useServerConfig(providerCfg)
          const childFile = path.join(dir, "direct-subtask-child.txt")
          yield* writeText(childFile, "direct subtask child result")
          const prompt = yield* SessionPrompt.Service
          const sessions = yield* Session.Service
          const status = yield* SessionStatus.Service
          const chat = yield* sessions.create({
            title: "Direct subtask owner",
            permission: [{ permission: "*", pattern: "*", action: "allow" }],
          })
          yield* llm.tool("read", { filePath: childFile })
          yield* llm.error(429, {
            error: { message: "Provider is rate limited", type: "rate_limit_error", code: "rate_limit_exceeded" },
          })
          yield* llm.hang
          const parentUser = yield* user(chat.id, "start a direct subtask")
          yield* sessions.updatePart({
            id: PartID.ascending(),
            messageID: parentUser.id,
            sessionID: chat.id,
            type: "subtask",
            prompt: "read direct-subtask-child.txt, then report the result",
            description: "Inspect direct subtask ownership",
            agent: "general",
            model: ref,
          })
          yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
          const active = yield* pollWithTimeout(
            Effect.gen(function* () {
              const messages = yield* sessions.messages({ sessionID: chat.id })
              const assistant = messages.findLast(
                (message) => message.info.role === "assistant" && message.info.agent === "general",
              )
              if (!assistant || assistant.info.role !== "assistant") return
              const task = assistant.parts.find(
                (part): part is SessionV1.ToolPart => part.type === "tool" && part.tool === "task",
              )
              const parentStatus = yield* status.get(chat.id)
              if (
                task?.state.status !== "running" ||
                typeof task.state.metadata?.sessionId !== "string" ||
                parentStatus.type !== "busy" ||
                parentStatus.activeAssistantMessageID !== assistant.info.id
              )
                return
              return {
                messages,
                assistantID: assistant.info.id,
                childSessionID: SessionID.make(task.state.metadata.sessionId),
              }
            }),
            "the direct subtask producer did not publish its active assistant owner",
            "15 seconds",
          )
          const childStatus = yield* pollWithTimeout(
            Effect.gen(function* () {
              const value = yield* status.get(active.childSessionID)
              return value.type === "retry" ? value : undefined
            }),
            "the direct subtask child did not enter retry status",
            "15 seconds",
          )
          const childMessages = yield* sessions.messages({ sessionID: active.childSessionID })
          yield* prompt
            .prompt({
              sessionID: chat.id,
              agent: "build",
              model: ref,
              parts: said("steer the active direct subtask"),
            })
            .pipe(Effect.forkChild)
          const parentMessages = yield* pollWithTimeout(
            Effect.gen(function* () {
              const messages = yield* sessions.messages({ sessionID: chat.id })
              const last = messages.at(-1)
              const parentStatus = yield* status.get(chat.id)
              const hasSteer =
                last?.info.role === "user" &&
                last.parts.some((part) => part.type === "text" && part.text === "steer the active direct subtask")
              return hasSteer &&
                parentStatus.type === "busy" &&
                parentStatus.activeAssistantMessageID === active.assistantID
                ? messages
                : undefined
            }),
            "the direct subtask lost ownership after a steer",
            "15 seconds",
          )
          const parentStatus = yield* status.get(chat.id)
          const session = yield* sessions.get(chat.id)
          const directory = yield* TestInstance
          yield* Effect.promise(() =>
            Bun.write(
              output,
              JSON.stringify({
                session,
                messages: parentMessages,
                assistantID: active.assistantID,
                childSessionID: active.childSessionID,
                childMessages,
                childStatus,
                parentStatus,
                directory: directory.directory,
              }),
            ),
          )
          process.exit(0)
        }),
      { git: true, config: cfg },
      30_000,
    ),
)

registerEnvironmentTest(
  "dangling-assistant-production-integration emits real active and completed history for the TUI regression",
  process.env.OPENCODE_DANGLING_ASSISTANT_SNAPSHOT !== undefined,
  () =>
    abruptPrompt.instance(
      "dangling-assistant-production-integration emits real active and completed history for the TUI regression",
      () =>
        Effect.gen(function* () {
          const output = process.env.OPENCODE_DANGLING_ASSISTANT_SNAPSHOT
          if (!output) return

          const instance = yield* TestInstance
          const dir = instance.directory
          const workerOutput = path.join(dir, "unfinished-assistant.json")
          const workerDatabase = `${workerOutput}.db`
          const runChild = (name: string, extra: Record<string, string>, database = workerDatabase) => {
            const workerEnv = Object.fromEntries(
              Object.entries(process.env).flatMap(([key, value]) => (value === undefined ? [] : [[key, value]])),
            )
            Object.assign(workerEnv, { OPENCODE_DB: database }, extra)
            return Bun.spawn([process.execPath, "test", "test/session/prompt.test.ts", `--test-name-pattern=${name}`], {
              cwd: path.join(import.meta.dir, "../.."),
              env: workerEnv,
              stdout: "ignore",
              stderr: "ignore",
            })
          }
          const worker = runChild("dangling-assistant-process-worker", {
            OPENCODE_DANGLING_ASSISTANT_OUTPUT: workerOutput,
          })
          yield* Effect.addFinalizer(() => Effect.sync(() => worker.kill()))
          const workerCode = yield* awaitWithTimeout(
            Effect.promise(() => worker.exited),
            "the production worker did not exit after persisting its unfinished turn",
            "30 seconds",
          )
          expect(workerCode).toBe(0)

          const serialized = Schema.decodeUnknownSync(
            Schema.Struct({
              session: SessionV1.SessionInfo,
              messages: Schema.Array(SessionV1.WithParts),
              directory: Schema.String,
            }),
          )(
            Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(
              yield* Effect.promise(() => Bun.file(workerOutput).text()),
            ),
          )
          yield* Effect.addFinalizer(() =>
            Effect.promise(() => rm(serialized.directory, { recursive: true, force: true })),
          )
          const source = serialized.messages.findLast(
            (message) => message.info.role === "assistant" && message.info.time.completed === undefined,
          )
          if (!source || source.info.role !== "assistant")
            throw new Error("producer did not preserve its unfinished assistant")
          expect(source.info.error).toBeUndefined()
          expect(source.parts.some((part) => part.type === "reasoning" && part.time.end === undefined)).toBe(true)
          expect(source.parts.some((part) => part.type === "reasoning" && part.metadata !== undefined)).toBe(true)

          const shellScript = path.join(dir, "hold-shell.mjs")
          const shellRelease = path.join(dir, "release-shell")
          const shellPID = path.join(dir, "shell.pid")
          yield* Effect.promise(() =>
            Bun.write(
              shellScript,
              [
                'import { existsSync, watch, writeFileSync } from "node:fs"',
                'import { basename, dirname } from "node:path"',
                "const [release, pid] = process.argv.slice(2)",
                "writeFileSync(pid, String(process.pid))",
                'process.stdout.write("started\\n")',
                "if (!existsSync(release)) await new Promise((resolve, reject) => {",
                "  const watcher = watch(dirname(release), (_event, name) => {",
                "    if (name?.toString() !== basename(release) || !existsSync(release)) return",
                "    watcher.close()",
                "    resolve()",
                "  })",
                '  watcher.on("error", reject)',
                "})",
                'process.stdout.write("finished\\n")',
              ].join("\n"),
            ),
          )
          const shellCommand = `"${process.execPath}" "${shellScript}" "${shellRelease}" "${shellPID}"`
          const shellOutput = path.join(dir, "running-shell.json")
          const shellWorker = runChild(
            "dangling-assistant-shell-worker",
            {
              OPENCODE_DANGLING_SHELL_OUTPUT: shellOutput,
              OPENCODE_DANGLING_SOURCE: workerOutput,
              OPENCODE_DANGLING_COMMAND: shellCommand,
            },
            `${shellOutput}.db`,
          )
          yield* Effect.addFinalizer(() => Effect.sync(() => shellWorker.kill()))
          const shellCode = yield* awaitWithTimeout(
            Effect.promise(() => shellWorker.exited),
            "the running shell worker did not exit after persisting its tool",
            "30 seconds",
          )
          expect(shellCode).toBe(0)
          const runningShell = Schema.decodeUnknownSync(
            Schema.Struct({
              session: SessionV1.SessionInfo,
              messages: Schema.Array(SessionV1.WithParts),
              assistantID: SessionV1.MessageID,
              status: SessionStatus.Info,
              statusEvent: SessionStatus.Info,
              directory: Schema.String,
            }),
          )(
            Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(
              yield* Effect.promise(() => Bun.file(shellOutput).text()),
            ),
          )
          yield* Effect.addFinalizer(() =>
            Effect.promise(async () => {
              const pid = Number(
                await Bun.file(shellPID)
                  .text()
                  .catch(() => ""),
              )
              if (Number.isSafeInteger(pid) && pid > 0 && alive(pid)) {
                if (process.platform === "win32") process.kill(pid)
                else process.kill(pid, "SIGKILL")
              }
              await rm(runningShell.directory, { recursive: true, force: true })
            }),
          )
          if (runningShell.status.type !== "busy") throw new Error("expected the running shell owner status")
          expect(runningShell.status.activeAssistantMessageID).toBe(runningShell.assistantID)
          if (runningShell.statusEvent.type !== "busy") throw new Error("expected the running shell owner event")
          expect(runningShell.statusEvent.activeAssistantMessageID).toBe(runningShell.assistantID)
          yield* Effect.promise(() => Bun.write(shellRelease, "release"))
          const pid = Number(yield* Effect.promise(() => Bun.file(shellPID).text()))
          if (alive(pid)) {
            if (process.platform === "win32") process.kill(pid)
            else process.kill(pid, "SIGKILL")
          }

          const taskOutput = path.join(dir, "running-task.json")
          const taskWorker = runChild(
            "dangling-assistant-task-process-worker",
            { OPENCODE_DANGLING_TASK_OUTPUT: taskOutput },
            `${taskOutput}.db`,
          )
          yield* Effect.addFinalizer(() => Effect.sync(() => taskWorker.kill()))
          const taskCode = yield* awaitWithTimeout(
            Effect.promise(() => taskWorker.exited),
            "the Task worker did not exit after persisting its running Task",
            "30 seconds",
          )
          expect(taskCode).toBe(0)
          const taskSnapshot = Schema.decodeUnknownSync(
            Schema.Struct({
              session: SessionV1.SessionInfo,
              messages: Schema.Array(SessionV1.WithParts),
              taskAssistantID: SessionV1.MessageID,
              childSessionID: SessionID,
              childMessages: Schema.Array(SessionV1.WithParts),
              childStatus: SessionStatus.Info,
              parentStatus: SessionStatus.Info,
              directory: Schema.String,
            }),
          )(
            Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(
              yield* Effect.promise(() => Bun.file(taskOutput).text()),
            ),
          )
          yield* Effect.addFinalizer(() =>
            Effect.promise(() => rm(taskSnapshot.directory, { recursive: true, force: true })),
          )
          const taskAssistant = taskSnapshot.messages.find(
            (message) => message.info.id === taskSnapshot.taskAssistantID,
          )
          if (!taskAssistant || taskAssistant.info.role !== "assistant")
            throw new Error("Task producer did not preserve its assistant row")
          const taskPart = taskAssistant.parts.find(
            (part): part is SessionV1.ToolPart => part.type === "tool" && part.tool === "task",
          )
          if (!taskPart || taskPart.state.status !== "running")
            throw new Error("Task producer did not preserve its running Task part")
          const childAssistant = taskSnapshot.childMessages.findLast((message) => message.info.role === "assistant")
          if (!childAssistant || childAssistant.info.role !== "assistant")
            throw new Error("Task producer did not preserve its child assistant")
          if (taskSnapshot.childStatus.type !== "retry") throw new Error("expected the Task child retry status")
          expect(taskSnapshot.childStatus.activeAssistantMessageID).toBe(childAssistant.info.id)
          if (taskSnapshot.parentStatus.type !== "busy") throw new Error("expected the Task parent busy status")
          expect(taskSnapshot.parentStatus.activeAssistantMessageID).toBe(taskSnapshot.taskAssistantID)

          const subtaskOutput = path.join(dir, "running-subtask.json")
          const subtaskWorker = runChild(
            "dangling-assistant-subtask-process-worker",
            { OPENCODE_DANGLING_SUBTASK_OUTPUT: subtaskOutput },
            `${subtaskOutput}.db`,
          )
          yield* Effect.addFinalizer(() => Effect.sync(() => subtaskWorker.kill()))
          const subtaskCode = yield* awaitWithTimeout(
            Effect.promise(() => subtaskWorker.exited),
            "the direct subtask worker did not exit after persisting its running Task",
            "30 seconds",
          )
          expect(subtaskCode).toBe(0)
          const subtaskSnapshot = Schema.decodeUnknownSync(
            Schema.Struct({
              session: SessionV1.SessionInfo,
              messages: Schema.Array(SessionV1.WithParts),
              assistantID: SessionV1.MessageID,
              childSessionID: SessionID,
              childMessages: Schema.Array(SessionV1.WithParts),
              childStatus: SessionStatus.Info,
              parentStatus: SessionStatus.Info,
              directory: Schema.String,
            }),
          )(
            Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(
              yield* Effect.promise(() => Bun.file(subtaskOutput).text()),
            ),
          )
          yield* Effect.addFinalizer(() =>
            Effect.promise(() => rm(subtaskSnapshot.directory, { recursive: true, force: true })),
          )
          const subtaskAssistant = subtaskSnapshot.messages.find(
            (message) => message.info.id === subtaskSnapshot.assistantID,
          )
          if (
            !subtaskAssistant ||
            subtaskAssistant.info.role !== "assistant" ||
            subtaskAssistant.info.agent !== "general"
          )
            throw new Error("direct subtask producer did not preserve its assistant owner")
          const subtaskPart = subtaskAssistant.parts.find(
            (part): part is SessionV1.ToolPart => part.type === "tool" && part.tool === "task",
          )
          if (!subtaskPart || subtaskPart.state.status !== "running")
            throw new Error("direct subtask producer did not preserve its running Task part")
          if (subtaskSnapshot.childStatus.type !== "retry")
            throw new Error("expected the direct subtask child retry status")
          expect(subtaskSnapshot.childStatus.activeAssistantMessageID).toBe(
            subtaskSnapshot.childMessages.findLast((message) => message.info.role === "assistant")?.info.id,
          )
          if (subtaskSnapshot.parentStatus.type !== "busy")
            throw new Error("expected the direct subtask parent busy status")
          expect(subtaskSnapshot.parentStatus.activeAssistantMessageID).toBe(subtaskSnapshot.assistantID)
          expect(subtaskSnapshot.messages.at(-1)?.info.role).toBe("user")

          const sessions = yield* Session.Service
          const prompt = yield* SessionPrompt.Service
          const status = yield* SessionStatus.Service
          const events = yield* EventV2Bridge.Service
          const preAssistantShellAssistant = runningShell.messages.findLast(
            (message) => message.info.role === "assistant" && message.info.id !== source.info.id,
          )
          if (!preAssistantShellAssistant || preAssistantShellAssistant.info.role !== "assistant")
            throw new Error("the pre-assistant fixture has no running shell assistant")
          const preAssistantShellParentID = preAssistantShellAssistant.info.parentID
          const preAssistantShellUser = runningShell.messages.find(
            (message) => message.info.id === preAssistantShellParentID,
          )
          if (!preAssistantShellUser || preAssistantShellUser.info.role !== "user")
            throw new Error("the pre-assistant shell row has no user parent")
          const preAssistantRows = [
            ...serialized.messages,
            preAssistantShellUser,
            preAssistantShellAssistant,
            ...taskSnapshot.messages,
          ]
          const preAssistantMessageIDs = new Map(
            preAssistantRows.map((message) => [message.info.id, MessageID.ascending()] as const),
          )
          if (preAssistantMessageIDs.size !== preAssistantRows.length)
            throw new Error("pre-assistant producer fixtures reuse a message ID")
          const preAssistantOldAssistantID = preAssistantMessageIDs.get(source.info.id)
          const preAssistantShellAssistantID = preAssistantMessageIDs.get(preAssistantShellAssistant.info.id)
          const preAssistantTaskAssistantID = preAssistantMessageIDs.get(taskSnapshot.taskAssistantID)
          if (!preAssistantOldAssistantID || !preAssistantShellAssistantID || !preAssistantTaskAssistantID)
            throw new Error("pre-assistant producer rows were not rekeyed")
          const preAssistantChat = yield* sessions.create({
            title: "Pre-assistant owner signal",
            permission: [{ permission: "*", pattern: "*", action: "allow" }],
          })
          yield* Effect.forEach(
            preAssistantRows,
            (message) =>
              Effect.gen(function* () {
                const id = preAssistantMessageIDs.get(message.info.id)
                if (!id) throw new Error(`missing rekeyed pre-assistant message ID: ${message.info.id}`)
                const info = Schema.decodeUnknownSync(SessionV1.Info)(
                  message.info.role === "assistant"
                    ? {
                        ...message.info,
                        id,
                        parentID: preAssistantMessageIDs.get(message.info.parentID) ?? message.info.parentID,
                        sessionID: preAssistantChat.id,
                      }
                    : { ...message.info, id, sessionID: preAssistantChat.id },
                )
                // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- the legacy writer requires mutable V1 records after schema validation
                const mutableInfo = info as SessionV1.Info
                yield* sessions.updateMessage(mutableInfo)
                yield* Effect.forEach(
                  message.parts,
                  (part) => {
                    const validated = Schema.decodeUnknownSync(SessionV1.Part)({
                      ...part,
                      id: PartID.ascending(),
                      messageID: id,
                      sessionID: preAssistantChat.id,
                    })
                    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- the legacy writer requires mutable V1 parts after schema validation
                    const mutable = validated as SessionV1.Part
                    return sessions.updatePart(mutable)
                  },
                  { discard: true },
                )
              }),
            { discard: true },
          )
          const busyWithoutOwner = yield* Deferred.make<SessionStatus.Info>()
          const unsubscribe = yield* events.listen((event) => {
            if (event.type !== SessionStatus.Event.Status.type) return Effect.void
            return Effect.gen(function* () {
              const data = Schema.decodeUnknownSync(SessionStatus.Event.Status.data)(event.data)
              if (
                data.sessionID === preAssistantChat.id &&
                data.status.type === "busy" &&
                data.status.activeAssistantMessageID === null
              )
                yield* Deferred.succeed(busyWithoutOwner, data.status)
            })
          })
          const preAssistantPrompt = yield* prompt
            .prompt({
              sessionID: preAssistantChat.id,
              agent: "build",
              model: {
                providerID: ProviderV2.ID.make("missing-owner-provider"),
                modelID: ModelV2.ID.make("missing-owner-model"),
              },
              parts: said("admit a new turn before its assistant exists"),
            })
            .pipe(Effect.forkChild)
          const preAssistantStatus = yield* awaitWithTimeout(
            Deferred.await(busyWithoutOwner),
            "the owner did not publish an explicit no-assistant status",
            "15 seconds",
          )
          const preAssistantMessages = yield* pollWithTimeout(
            Effect.gen(function* () {
              const messages = yield* sessions.messages({ sessionID: preAssistantChat.id })
              const latest = messages.at(-1)
              if (
                latest?.info.role === "user" &&
                latest.parts.some(
                  (part) => part.type === "text" && part.text === "admit a new turn before its assistant exists",
                )
              )
                return messages
            }),
            "the pre-assistant user input was not persisted",
            "15 seconds",
          )
          const preAssistantAssistant = preAssistantMessages.findLast((message) => message.info.role === "assistant")
          if (!preAssistantAssistant || preAssistantAssistant.info.role !== "assistant")
            throw new Error("the pre-assistant fixture has no prior assistant")
          expect(preAssistantAssistant.info.id).toBe(preAssistantTaskAssistantID)
          const failedPreAssistantTurn = yield* awaitWithTimeout(
            Fiber.await(preAssistantPrompt),
            "the missing-agent pre-assistant turn did not terminate",
            "15 seconds",
          )
          yield* unsubscribe
          expect(Exit.isFailure(failedPreAssistantTurn)).toBe(true)
          if (preAssistantStatus.type !== "busy") throw new Error("expected the explicit no-owner busy status")
          expect(preAssistantStatus.activeAssistantMessageID).toBeNull()
          const admittedPreAssistantUser = preAssistantMessages.at(-1)
          if (!admittedPreAssistantUser || admittedPreAssistantUser.info.role !== "user")
            throw new Error("the pre-assistant owner fixture did not end with its admitted user")
          expect(
            preAssistantMessages.some(
              (message) =>
                message.info.role === "assistant" && message.info.parentID === admittedPreAssistantUser.info.id,
            ),
          ).toBe(false)
          const preAssistantSession = yield* sessions.get(preAssistantChat.id)

          const chat = yield* sessions.create({
            title: "Dangling assistant recovery",
            permission: [{ permission: "*", pattern: "*", action: "allow" }],
          })
          yield* Effect.forEach(
            runningShell.messages,
            (message) =>
              Effect.gen(function* () {
                const info = Schema.decodeUnknownSync(SessionV1.Info)({ ...message.info, sessionID: chat.id })
                // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- the legacy writer requires mutable V1 records after schema validation
                const mutableInfo = info as SessionV1.Info
                yield* sessions.updateMessage(mutableInfo)
                yield* Effect.forEach(
                  message.parts,
                  (part) => {
                    const validated = Schema.decodeUnknownSync(SessionV1.Part)({ ...part, sessionID: chat.id })
                    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- the legacy writer requires mutable V1 parts after schema validation
                    const mutable = validated as SessionV1.Part
                    return sessions.updatePart(mutable)
                  },
                  { discard: true },
                )
              }),
            { discard: true },
          )

          abruptAssistantLLMCalls.value = 0
          const task = yield* prompt
            .prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("start a later turn") })
            .pipe(Effect.forkChild)
          yield* pollWithTimeout(
            Effect.gen(function* () {
              const history = yield* sessions.messages({ sessionID: chat.id })
              const current = history.findLast(
                (message) => message.info.role === "assistant" && message.info.id !== source.info.id,
              )
              if (!current || current.info.role !== "assistant" || current.info.time.completed !== undefined)
                return undefined
              const hasReasoning = current.parts.some(
                (part) => part.type === "reasoning" && part.time.end === undefined && part.metadata !== undefined,
              )
              const activeRead = current.parts.find(
                (part): part is SessionV1.ToolPart => part.type === "tool" && part.tool === "read",
              )
              return hasReasoning && activeRead?.state.status === "running" ? true : undefined
            }),
            "the later production turn did not persist its live reasoning and running tool",
            "15 seconds",
          )
          const activeMessages = yield* pollWithTimeout(
            Effect.gen(function* () {
              const history = yield* sessions.messages({ sessionID: chat.id })
              const current = history.findLast(
                (message) => message.info.role === "assistant" && message.info.id !== source.info.id,
              )
              if (!current || current.info.role !== "assistant" || current.info.time.completed !== undefined)
                return undefined
              const activeRead = current.parts.find(
                (part): part is SessionV1.ToolPart => part.type === "tool" && part.tool === "read",
              )
              if (!activeRead || activeRead.state.status !== "running") return undefined
              return history
            }),
            "the later production tool did not enter its running state",
            "15 seconds",
          )
          const activeAssistant = activeMessages.findLast(
            (message) => message.info.role === "assistant" && message.info.id !== source.info.id,
          )
          if (!activeAssistant || activeAssistant.info.role !== "assistant")
            throw new Error("expected the later production assistant to be active")
          const activeStatus = yield* status.get(chat.id)
          if (activeStatus.type !== "busy") throw new Error("expected the later production status to be busy")
          expect(activeStatus.activeAssistantMessageID).toBe(activeAssistant.info.id)
          const activeStatusMap = (yield* status.list()).get(chat.id)
          if (activeStatusMap?.type !== "busy") throw new Error("expected the active status-map entry")
          expect(activeStatusMap.activeAssistantMessageID).toBe(activeAssistant.info.id)

          yield* awaitWithTimeout(prompt.cancel(chat.id), "the later production turn did not cancel", "15 seconds")
          yield* awaitWithTimeout(Fiber.await(task), "the cancelled production turn did not stop", "15 seconds")
          const finalTask = yield* prompt
            .prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("complete a later turn") })
            .pipe(Effect.forkChild)
          yield* awaitWithTimeout(Fiber.join(finalTask), "the final production turn did not finish", "15 seconds")
          const completedMessages = yield* sessions.messages({ sessionID: chat.id })
          const completedAssistant = completedMessages.findLast(
            (message) => message.info.role === "assistant" && message.info.id !== source.info.id,
          )
          if (!completedAssistant || completedAssistant.info.role !== "assistant")
            throw new Error("expected the later production assistant to complete")
          expect(completedAssistant.info.time.completed).toBeDefined()
          expect(completedAssistant.info.error).toBeUndefined()
          const completedStatus = yield* status.get(chat.id)
          expect(completedStatus.type).toBe("idle")

          const unchangedOld = completedMessages.find((message) => message.info.id === source.info.id)
          if (unchangedOld?.info.role !== "assistant") throw new Error("the original producer row disappeared")
          expect(unchangedOld.info.time.completed).toBeUndefined()
          expect(unchangedOld.info.error).toBeUndefined()
          const finalSession = yield* sessions.get(chat.id)
          yield* Effect.promise(() =>
            Bun.write(
              output,
              JSON.stringify({
                session: finalSession,
                oldAssistantID: source.info.id,
                shellAssistantID: runningShell.assistantID,
                shellProducer: {
                  session: runningShell.session,
                  messages: runningShell.messages,
                  assistantID: runningShell.assistantID,
                  status: runningShell.status,
                  statusEvent: runningShell.statusEvent,
                },
                taskAssistantID: taskSnapshot.taskAssistantID,
                taskProducerSessionID: taskSnapshot.session.id,
                taskProducerSession: taskSnapshot.session,
                taskProducerMessages: taskSnapshot.messages,
                taskChildSessionID: taskSnapshot.childSessionID,
                taskChildMessages: taskSnapshot.childMessages,
                taskChildStatus: taskSnapshot.childStatus,
                taskParentStatus: taskSnapshot.parentStatus,
                directSubtask: subtaskSnapshot,
                preAssistant: {
                  session: preAssistantSession,
                  messages: preAssistantMessages,
                  oldAssistantID: preAssistantOldAssistantID,
                  shellAssistantID: preAssistantShellAssistantID,
                  taskAssistantID: preAssistantTaskAssistantID,
                  status: preAssistantStatus,
                },
                currentAssistantID: activeAssistant.info.id,
                completedAssistantID: completedAssistant.info.id,
                active: { messages: activeMessages, status: activeStatus },
                completed: { messages: completedMessages, status: completedStatus },
              }),
            ),
          )
        }),
      { git: true, config: cfg },
      120_000,
    ),
)

nonOwnerCancel.instance(
  "a cancel without a local runner does not signal idle for a live owner turn",
  () =>
    Effect.gen(function* () {
      const events = yield* EventV2Bridge.Service
      const status = yield* SessionStatus.Service
      const runState = yield* SessionRunState.Service
      const ownerStatusEvents: { sessionID: string; status: string }[] = []
      const ownerIdleEvents: string[] = []
      const unsubscribeOwner = yield* events.listen((event) => {
        if (event.type === SessionStatus.Event.Status.type) {
          return Effect.sync(() => {
            const data = Schema.decodeUnknownSync(SessionStatus.Event.Status.data)(event.data)
            ownerStatusEvents.push({ sessionID: data.sessionID, status: data.status.type })
          })
        }
        if (event.type === SessionStatus.Event.Idle.type) {
          return Effect.sync(() => {
            const data = Schema.decodeUnknownSync(SessionStatus.Event.Idle.data)(event.data)
            ownerIdleEvents.push(data.sessionID)
          })
        }
        return Effect.void
      })

      const { llm, sessions, chat, task, release } = yield* startHeld()
      yield* pollWithTimeout(
        Effect.gen(function* () {
          const current = (yield* status.list()).get(chat.id)
          return current?.type === "busy" ? true : undefined
        }),
        "owner turn never reported busy",
      )
      const unfinished = yield* pollWithTimeout(
        Effect.gen(function* () {
          const messages = yield* sessions.messages({ sessionID: chat.id })
          const assistant = messages.findLast((message) => message.info.role === "assistant")
          if (!assistant || assistant.info.role !== "assistant" || assistant.info.time.completed !== undefined)
            return undefined
          return assistant
        }),
        "owner assistant was not persisted before cancellation",
      )
      if (unfinished.info.role !== "assistant") throw new Error("expected the persisted unfinished assistant")
      expect(unfinished.info.time.completed).toBeUndefined()

      const nonOwnerDirectory = yield* tmpdirScoped({ git: true })
      const nonOwnerStatusEvents: { sessionID: string; status: string }[] = []
      const nonOwnerIdleEvents: string[] = []
      const nonOwner = yield* Effect.gen(function* () {
        const events = yield* EventV2Bridge.Service
        const status = yield* SessionStatus.Service
        const runState = yield* SessionRunState.Service
        const unsubscribe = yield* events.listen((event) => {
          if (event.type === SessionStatus.Event.Status.type) {
            return Effect.sync(() => {
              const data = Schema.decodeUnknownSync(SessionStatus.Event.Status.data)(event.data)
              nonOwnerStatusEvents.push({ sessionID: data.sessionID, status: data.status.type })
            })
          }
          if (event.type === SessionStatus.Event.Idle.type) {
            return Effect.sync(() => {
              const data = Schema.decodeUnknownSync(SessionStatus.Event.Idle.data)(event.data)
              nonOwnerIdleEvents.push(data.sessionID)
            })
          }
          return Effect.void
        })
        yield* runState.cancel(chat.id)
        const statuses = yield* status.list()
        yield* unsubscribe
        return statuses
      }).pipe(Effect.provide(nonOwnerRunState), provideInstanceEffect(nonOwnerDirectory))

      expect(nonOwner.size).toBe(0)
      expect(nonOwnerStatusEvents).toEqual([])
      expect(nonOwnerIdleEvents).toEqual([])
      expect((yield* status.list()).get(chat.id)?.type).toBe("busy")
      const afterNonOwnerCancel = (yield* sessions.messages({ sessionID: chat.id })).findLast(
        (message) => message.info.id === unfinished.info.id,
      )
      if (afterNonOwnerCancel?.info.role !== "assistant") throw new Error("owner assistant disappeared after cancel")
      expect(afterNonOwnerCancel.info.time.completed).toBeUndefined()
      expect(afterNonOwnerCancel.info.error).toBeUndefined()

      yield* release
      const [completed] = yield* finish(task)
      if (!completed || completed.info.role !== "assistant") throw new Error("owner turn did not return its assistant")
      expect(completed.info.time.completed).toBeDefined()
      expect((yield* status.list()).has(chat.id)).toBe(false)
      const persisted = (yield* sessions.messages({ sessionID: chat.id })).findLast(
        (message) => message.info.id === unfinished.info.id,
      )
      if (persisted?.info.role !== "assistant") throw new Error("completed owner assistant was not persisted")
      expect(persisted.info.time.completed).toBeDefined()
      expect(ownerStatusEvents.some((event) => event.sessionID === chat.id && event.status === "busy")).toBe(true)
      expect(ownerStatusEvents.some((event) => event.sessionID === chat.id && event.status === "idle")).toBe(true)
      expect(ownerIdleEvents).toContain(chat.id)

      const locallyCancelled = yield* sessions.create({ title: "Local cancel idle signal" })
      const localStarted = yield* Deferred.make<void>()
      const localHold = yield* Deferred.make<void>()
      const statusIdle = yield* Deferred.make<void>()
      const deprecatedIdle = yield* Deferred.make<void>()
      const localStatusEvents: string[] = []
      const localIdleEvents: string[] = []
      const unsubscribeLocal = yield* events.listen((event) => {
        if (event.type === SessionStatus.Event.Status.type) {
          return Effect.gen(function* () {
            const data = Schema.decodeUnknownSync(SessionStatus.Event.Status.data)(event.data)
            if (data.sessionID !== locallyCancelled.id) return
            localStatusEvents.push(data.status.type)
            if (data.status.type === "idle") yield* Deferred.succeed(statusIdle, undefined)
          })
        }
        if (event.type === SessionStatus.Event.Idle.type) {
          return Effect.gen(function* () {
            const data = Schema.decodeUnknownSync(SessionStatus.Event.Idle.data)(event.data)
            if (data.sessionID !== locallyCancelled.id) return
            localIdleEvents.push(data.sessionID)
            yield* Deferred.succeed(deprecatedIdle, undefined)
          })
        }
        return Effect.void
      })
      const localTurn = yield* runState
        .startShell(
          locallyCancelled.id,
          Effect.succeed(completed),
          Effect.gen(function* () {
            yield* Deferred.succeed(localStarted, undefined)
            yield* Deferred.await(localHold)
            return completed
          }),
        )
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(localStarted), "local runner never started")
      yield* pollWithTimeout(
        Effect.gen(function* () {
          return (yield* status.list()).get(locallyCancelled.id)?.type === "busy" ? true : undefined
        }),
        "local runner never reported busy",
      )
      yield* runState.cancel(locallyCancelled.id)
      yield* awaitWithTimeout(Deferred.await(statusIdle), "local cancel did not publish session.status idle")
      yield* awaitWithTimeout(Deferred.await(deprecatedIdle), "local cancel did not publish session.idle")
      const localExit = yield* awaitWithTimeout(Fiber.await(localTurn), "local runner did not settle after cancel")
      expect(Exit.isSuccess(localExit)).toBe(true)
      expect((yield* status.list()).has(locallyCancelled.id)).toBe(false)
      expect(localStatusEvents).toContain("busy")
      expect(localStatusEvents).toContain("idle")
      expect(localIdleEvents).toContain(locallyCancelled.id)
      yield* unsubscribeLocal
      yield* unsubscribeOwner
      expect(yield* llm.calls).toBe(1)
    }),
  30_000,
)

it.instance(
  "queued prompt waits for the task to finish and runs as its own turn",
  () =>
    Effect.gen(function* () {
      const { llm, queue, chat, task, send, release } = yield* startHeld({ tool: true })
      yield* llm.text("task done")
      yield* llm.text("queued done")

      const held = yield* send("after the task", { delivery: "queue" })
      yield* admitted(chat.id, "after the task")
      yield* release
      yield* finish(task, held)

      const inputs = yield* llm.inputs
      expect(inputs).toHaveLength(3)
      expect(mentions(inputs[1], "after the task")).toBe(false)
      expect(lastUser(inputs[2])).toEqual({ role: "user", content: "after the task" })
      expect(yield* queue.list(chat.id)).toEqual([])
    }),
  15_000,
)

it.instance(
  "three queued prompts run as three more turns in admission order while the session stays busy",
  () =>
    Effect.gen(function* () {
      const events = yield* EventV2Bridge.Service
      const { llm, chat, task, send, release } = yield* startHeld()
      yield* llm.text("one done")
      yield* llm.text("two done")
      yield* llm.text("three done")
      const idleAt: number[] = []
      const unsubscribe = yield* events.listen((event) =>
        event.type === SessionStatus.Event.Idle.type && (event.data as { sessionID: string }).sessionID === chat.id
          ? llm.calls.pipe(Effect.map((calls) => void idleAt.push(calls)))
          : Effect.void,
      )
      yield* Effect.addFinalizer(() => unsubscribe)

      const first = yield* send("queued one", { delivery: "queue" })
      yield* queued(chat.id, 1)
      const second = yield* send("queued two", { delivery: "queue" })
      yield* queued(chat.id, 2)
      const third = yield* send("queued three", { delivery: "queue" })
      yield* queued(chat.id, 3)
      yield* release
      yield* finish(task, first, second, third)

      const inputs = yield* llm.inputs
      expect(inputs).toHaveLength(4)
      expect(inputs.slice(1).map(lastUser)).toEqual([
        { role: "user", content: "queued one" },
        { role: "user", content: "queued two" },
        { role: "user", content: "queued three" },
      ])
      // One drain: busy through every queued turn, idle once after the last.
      expect(idleAt).toEqual([4])
    }),
  15_000,
)

it.instance(
  "a later steer reaches the next step before an earlier queued prompt",
  () =>
    Effect.gen(function* () {
      const { llm, chat, task, send, release } = yield* startHeld()
      yield* llm.text("steer done")
      yield* llm.text("queued done")

      const held = yield* send("queued earlier", { delivery: "queue" })
      yield* admitted(chat.id, "queued earlier")
      const steer = yield* send("steer later")
      yield* admitted(chat.id, "steer later")
      yield* release
      yield* finish(task, held, steer)

      const inputs = yield* llm.inputs
      expect(inputs).toHaveLength(3)
      expect(lastUser(inputs[1])).toEqual({ role: "user", content: "steer later" })
      expect(mentions(inputs[1], "queued earlier")).toBe(false)
      expect(lastUser(inputs[2])).toEqual({ role: "user", content: "queued earlier" })
    }),
  15_000,
)

it.instance(
  "a queued prompt's turn starts with a fresh step allowance",
  () =>
    Effect.gen(function* () {
      const { llm, chat, task, send, release } = yield* startHeld({
        config: (url) => ({ ...providerCfg(url), agent: { build: { steps: 2 } } }),
      })
      yield* llm.text("queued done")

      const held = yield* send("queued after a step", { delivery: "queue" })
      yield* admitted(chat.id, "queued after a step")
      yield* release
      yield* finish(task, held)

      const inputs = yield* llm.inputs
      expect(lastUser(inputs[1])).toEqual({ role: "user", content: "queued after a step" })
      // The task used one of its two steps; the queued turn must not start on the last one.
      expect(mentions(inputs[1], "MAXIMUM STEPS REACHED")).toBe(false)
    }),
  15_000,
)

it.instance(
  "a withdrawn prompt never reaches the model and a delivered one can no longer be withdrawn",
  () =>
    Effect.gen(function* () {
      const { llm, queue, chat, task, send, release } = yield* startHeld()
      yield* llm.text("kept done")

      const withdrawn = yield* send("withdrawn prompt", { delivery: "queue" })
      const [admitted] = yield* queued(chat.id, 1)
      const kept = yield* send("kept prompt", { delivery: "queue" })
      const [, keptItem] = yield* queued(chat.id, 2)

      expect(Option.getOrUndefined(yield* queue.withdraw(chat.id, admitted!.id))).toEqual(admitted)
      expect(Option.isNone(yield* queue.withdraw(chat.id, admitted!.id))).toBe(true)
      yield* release
      yield* finish(task, kept)
      // Its own caller learns the prompt was withdrawn rather than getting someone else's reply.
      const gone = yield* awaitWithTimeout(Fiber.await(withdrawn), "withdrawn prompt never returned")
      expect(Exit.isFailure(gone) ? Cause.squash(gone.cause) : undefined).toBeInstanceOf(SessionQueue.WithdrawnError)

      const inputs = yield* llm.inputs
      expect(inputs).toHaveLength(2)
      expect(inputs.some((input) => mentions(input, "withdrawn prompt"))).toBe(false)
      expect(lastUser(inputs[1])).toEqual({ role: "user", content: "kept prompt" })
      expect(Option.isNone(yield* queue.withdraw(chat.id, keptItem!.id))).toBe(true)
    }),
  15_000,
)

it.instance(
  "a restored prompt keeps its identity and admission order",
  () =>
    Effect.gen(function* () {
      const { llm, queue, chat, task, send, release } = yield* startHeld()
      yield* llm.text("first done")
      yield* llm.text("second done")

      const first = yield* send("restored first", { delivery: "queue" })
      const [admitted] = yield* queued(chat.id, 1)
      const second = yield* send("queued second", { delivery: "queue" })
      yield* queued(chat.id, 2)
      yield* queue.withdraw(chat.id, admitted!.id)
      yield* queued(chat.id, 1)

      expect(Option.getOrUndefined(yield* queue.restore(chat.id, admitted!.id))).toEqual(admitted)
      expect((yield* queue.list(chat.id)).map((item) => [item.id, item.seq])).toEqual([
        [admitted!.id, admitted!.seq],
        [expect.any(String), admitted!.seq + 1],
      ])
      yield* release
      yield* finish(task, first, second)

      const inputs = yield* llm.inputs
      expect(inputs.slice(1).map(lastUser)).toEqual([
        { role: "user", content: "restored first" },
        { role: "user", content: "queued second" },
      ])
    }),
  15_000,
)

it.instance(
  "an abort parks pending prompts until the next admission delivers them in order",
  () =>
    Effect.gen(function* () {
      const { llm, prompt, queue, chat, task, send } = yield* startHeld()
      const first = yield* send("parked one", { delivery: "queue" })
      yield* queued(chat.id, 1)
      const second = yield* send("parked two", { delivery: "queue" })
      yield* queued(chat.id, 2)

      yield* prompt.cancel(chat.id)
      yield* finish(task, first, second)
      expect(yield* llm.calls).toBe(1)
      expect((yield* queue.list(chat.id)).map((item) => item.input.parts)).toEqual([
        said("parked one"),
        said("parked two"),
      ])

      yield* llm.text("wake done")
      yield* llm.text("one done")
      yield* llm.text("two done")
      yield* prompt.prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("wake up") })

      const inputs = yield* llm.inputs
      expect(inputs.slice(1).map(lastUser)).toEqual([
        { role: "user", content: "wake up" },
        { role: "user", content: "parked one" },
        { role: "user", content: "parked two" },
      ])
      expect(yield* queue.list(chat.id)).toEqual([])
    }),
  15_000,
)

it.instance(
  "a provider error parks pending prompts until the next admission",
  () =>
    Effect.gen(function* () {
      const { llm, prompt, sessions, queue, chat, task, send, release } = yield* startHeld({ tool: true })
      yield* llm.error(400, { error: { message: "rejected by the provider" } })

      const parked = "[fm-from-peer]\x1f parked by the error"
      const held = yield* send(parked)
      yield* queued(chat.id, 1)
      yield* release
      yield* finish(task, held)
      const stopped = (yield* sessions.messages({ sessionID: chat.id })).at(-1)?.info
      expect(stopped?.role === "assistant" ? stopped.error?.name : undefined).toBe("APIError")
      expect(yield* llm.calls).toBe(2)
      expect((yield* queue.list(chat.id)).map((item) => item.input.parts)).toEqual([said(parked)])

      yield* llm.text("wake done")
      yield* llm.text("parked done")
      yield* prompt.prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("wake up") })
      const inputs = yield* llm.inputs
      expect(lastUser(inputs[2])).toEqual({ role: "user", content: "wake up" })
      expect(JSON.stringify(lastUser(inputs[3]))).toContain("parked by the error")
    }),
  15_000,
)

// Consume recovery (swxtchio/swx-opencode#167). The run's consume after a tool step meets a hold taken at the loop top:
// a write lock from a second connection on a file database, as another writer's long transaction would hold it, or the
// session permit, as a stalled session writer would. Expected model inputs and records come from what each test sends
// and what the bash tool writes, never from the queue under test.

const consumeLockDbPath = path.join(os.tmpdir(), `opencode-consume-lock-${randomUUID()}.db`)
const consumeLock = promptTests(makeHttpWithDatabase(Database.layerFromPath(consumeLockDbPath)))

// The production consume reports each attempt that could not begin with one of these logs, which the tests capture.
const consumeLockedLog = "queue consume met a held write lock"
const consumeBackstopLog = "queue consume waited past its acquisition backstop"

// Takes the hold on the session just before its consume; the listener runs inline with the loop's status publish.
type ConsumeHold = (sessionID: SessionID) => Effect.Effect<void>

const startConsumeLocked = Effect.fn("test.startConsumeLocked")(function* (hold: ConsumeHold) {
  const { dir, llm } = yield* useServerConfig(providerCfg)
  const prompt = yield* SessionPrompt.Service
  const sessions = yield* Session.Service
  const events = yield* EventV2Bridge.Service
  const chat = yield* sessions.create({
    title: "Consume under a held lock",
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  })
  const marker = path.join(dir, "tool-runs.txt")
  const gate = yield* Deferred.make<void>()
  yield* llm.push(
    reply()
      .wait(deferredAsPromise(gate))
      .tool("bash", { command: `echo ran >> "${marker}"; echo tool-output-ok`, description: "Mark the tool run" }),
  )

  // The loop publishes its busy status just before consume, and listeners run inline with the publish, so a hold
  // taken here once the tool step is persisted is held when that loop's consume begins.
  const taken = { done: false }
  const held = yield* Deferred.make<void>()
  const off = yield* events.listen((event) =>
    Effect.gen(function* () {
      if (taken.done || event.type !== SessionStatus.Event.Status.type) return
      const data = Schema.decodeUnknownSync(SessionStatus.Event.Status.data)(event.data)
      if (data.sessionID !== chat.id || data.status.type !== "busy" || data.status.activeAssistantMessageID !== null)
        return
      const history = yield* sessions.messages({ sessionID: chat.id }).pipe(Effect.orDie)
      if (!history.some((message) => message.info.role === "assistant" && message.info.finish === "tool-calls")) return
      taken.done = true
      yield* hold(chat.id)
      yield* Deferred.succeed(held, undefined)
    }),
  )
  // Reads the records through its own connection, since the run's connection may be waiting.
  const reader = new Sqlite(consumeLockDbPath, { readonly: true })
  yield* Effect.addFinalizer(() => off.pipe(Effect.andThen(Effect.sync(() => reader.close()))))

  const send = (text: string, extra?: Partial<SessionPrompt.PromptInput>) =>
    prompt
      .prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said(text), ...extra })
      .pipe(Effect.forkChild)
  const task = yield* send("run the tool")
  yield* awaitWithTimeout(llm.wait(1), "the tool step's provider call never started", "10 seconds")
  const continuation = yield* send("continue after the tool", { delivery: "queue" })
  yield* admitted(chat.id, "continue after the tool")
  yield* Deferred.succeed(gate, undefined)
  yield* awaitWithTimeout(Deferred.await(held), "the loop never reached consume after the tool step", "10 seconds")

  const records = () => {
    const queue = reader
      .query("select time_promoted, message_id, time_withdrawn from session_prompt_queue where session_id = ?")
      .all(chat.id)
    const tools = reader
      .query<
        { id: PartID; status: SessionV1.ToolPart["state"]["status"] },
        [string]
      >("select id, json_extract(data, '$.state.status') as status from part " + "where session_id = ? and json_extract(data, '$.tool') = 'bash'")
      .all(chat.id)
    return { queue, tools }
  }
  const toolRuns = Effect.promise(() => Bun.file(marker).text())
  return { llm, prompt, sessions, chat, task, continuation, records, toolRuns }
})

// A second connection's write lock, held until released.
const writeLockHold = Effect.fn("test.writeLockHold")(function* () {
  const lock: { holder?: Sqlite } = {}
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      if (lock.holder?.inTransaction) lock.holder.run("ROLLBACK")
      lock.holder?.close()
    }),
  )
  const take: ConsumeHold = () =>
    Effect.sync(() => {
      lock.holder = new Sqlite(consumeLockDbPath)
      lock.holder.run("BEGIN IMMEDIATE")
    })
  return { take, release: Effect.sync(() => lock.holder!.run("COMMIT")) }
})

// `onLocked` runs inline with a locked attempt's log, before the consume makes its next attempt.
const captureConsumeLogs = Effect.fn("test.captureConsumeLogs")(function* (onLocked?: () => void) {
  const locked: unknown[] = []
  const backstop: unknown[] = []
  const capture = Logger.make((options) => {
    const [text, fields] = Array.isArray(options.message) ? options.message : [options.message]
    if (text === consumeLockedLog) {
      locked.push(fields)
      onLocked?.()
    }
    if (text === consumeBackstopLog) backstop.push(fields)
  })
  const loggers = yield* Effect.service(Logger.CurrentLoggers)
  return { locked, backstop, loggers: new Set([...loggers, capture]) }
})

const consumeRefusals = (exits: ReadonlyArray<Exit.Exit<SessionV1.WithParts, unknown>>) =>
  exits.map((exit) => (Exit.isFailure(exit) ? Cause.squash(exit.cause) : exit.value))

// After a refusal, the lock released: a later prompt delivers the kept continuation once, without running the tool again.
const wakeDeliversContinuation = Effect.fn("test.wakeDeliversContinuation")(function* (
  started: Effect.Success<ReturnType<typeof startConsumeLocked>>,
) {
  yield* started.llm.text("wake done")
  yield* started.llm.text("continuation done")
  yield* started.prompt.prompt({ sessionID: started.chat.id, agent: "build", model: ref, parts: said("wake up") })
  const inputs = yield* started.llm.inputs
  expect(inputs).toHaveLength(3)
  expect(lastUser(inputs[1])).toEqual({ role: "user", content: "wake up" })
  expect(lastUser(inputs[2])).toEqual({ role: "user", content: "continue after the tool" })
  expect(yield* started.toolRuns).toBe("ran\n")
  const history = yield* started.sessions.messages({ sessionID: started.chat.id })
  const users = history.filter((message) =>
    message.parts.some((part) => part.type === "text" && part.text === "continue after the tool"),
  )
  expect(users).toHaveLength(1)
  expect(
    history.filter((message) => message.info.role === "assistant" && message.info.parentID === users[0]!.info.id),
  ).toHaveLength(1)
})

consumeLock.instance(
  "a consume that meets a held write lock recovers on release, and the turn and its queued continuation run once",
  () =>
    Effect.gen(function* () {
      const capture = yield* captureConsumeLogs()
      yield* Effect.gen(function* () {
        const lock = yield* writeLockHold()
        const { llm, sessions, chat, task, continuation, records, toolRuns } = yield* startConsumeLocked(lock.take)
        yield* llm.text("task done")
        yield* llm.text("continuation done")
        const before = records()
        expect(before.queue).toEqual([{ time_promoted: null, message_id: null, time_withdrawn: null }])
        expect(before.tools).toEqual([{ id: expect.any(String), status: "completed" }])

        // Release on the logged failed attempt, or once the run ended without one, which is the unrecovered outcome.
        yield* awaitWithTimeout(
          Effect.race(
            pollWithTimeout(
              Effect.sync(() => (capture.locked.length > 0 ? true : undefined)),
              "consume never reported the held lock",
              "30 seconds",
            ),
            Fiber.await(task),
          ),
          "neither a failed consume nor the run's end was observed",
          "40 seconds",
        )
        const failed = records()
        yield* lock.release
        const [taskReply, continuationReply] = yield* finish(task, continuation)

        expect(capture.locked).toEqual([{ "session.id": chat.id, attempts: 1 }])
        expect(capture.backstop).toEqual([])
        // The failed attempt changed nothing: the continuation is still pending and the tool result kept.
        expect(failed).toEqual(before)
        const inputs = yield* llm.inputs
        expect(inputs).toHaveLength(3)
        expect(mentions(inputs[1], "tool-output-ok")).toBe(true)
        expect(mentions(inputs[1], "continue after the tool")).toBe(false)
        expect(lastUser(inputs[2])).toEqual({ role: "user", content: "continue after the tool" })
        expect(yield* toolRuns).toBe("ran\n")

        const history = yield* sessions.messages({ sessionID: chat.id })
        const users = history.filter((message) =>
          message.parts.some((part) => part.type === "text" && part.text === "continue after the tool"),
        )
        expect(users.map((message) => message.info.role)).toEqual(["user"])
        const answers = history.filter(
          (message) => message.info.role === "assistant" && message.info.parentID === users[0]!.info.id,
        )
        expect(answers).toHaveLength(1)
        const answer = answers[0]!
        if (answer.info.role !== "assistant") throw new Error("the continuation's answer is not an assistant message")
        expect(answer.info.time.completed).toBeDefined()
        expect(answer.info.error).toBeUndefined()
        expect(answer.parts.some((part) => part.type === "text" && part.text === "continuation done")).toBe(true)
        expect(continuationReply.info.id).toBe(answer.info.id)
        expect(taskReply.parts.some((part) => part.type === "text" && part.text === "task done")).toBe(true)
        const tools = history.flatMap((message) =>
          message.parts.filter((part): part is SessionV1.ToolPart => part.type === "tool"),
        )
        expect(tools.map((part) => ({ id: part.id, status: part.state.status }))).toEqual(before.tools)
        const remaining = yield* Effect.gen(function* () {
          const { db } = yield* Database.Service
          return yield* db
            .select()
            .from(SessionPromptQueueTable)
            .where(eq(SessionPromptQueueTable.session_id, chat.id))
            .all()
        })
        expect(remaining).toEqual([])
      }).pipe(Effect.provideService(Logger.CurrentLoggers, capture.loggers))
    }),
  60_000,
)

consumeLock.instance(
  "a consume whose lock outlasts its recovery bound stops the run with a named error and keeps the continuation",
  () =>
    Effect.gen(function* () {
      const capture = yield* captureConsumeLogs()
      yield* Effect.gen(function* () {
        const status = yield* SessionStatus.Service
        const runState = yield* SessionRunState.Service
        const lock = yield* writeLockHold()
        const started = yield* startConsumeLocked(lock.take)
        const { llm, chat, task, continuation, records } = started
        const before = records()
        const exits = yield* awaitWithTimeout(
          Effect.all([Fiber.await(task), Fiber.await(continuation)]),
          "the run never stopped under the held lock",
          "120 seconds",
        )

        consumeRefusals(exits).forEach((refusal) => {
          expect(refusal).toBeInstanceOf(SessionQueue.ConsumeLockedError)
          expect(refusal).toMatchObject({ sessionID: chat.id, attempts: 5 })
        })
        expect(capture.locked).toEqual([1, 2, 3, 4, 5].map((attempts) => ({ "session.id": chat.id, attempts })))
        // The refusal kept the continuation pending and the tool result, and left nothing running to retry it.
        expect(records()).toEqual(before)
        expect(yield* llm.calls).toBe(1)
        expect((yield* status.list()).has(chat.id)).toBe(false)
        expect(Exit.isSuccess(yield* runState.assertNotBusy(chat.id).pipe(Effect.exit))).toBe(true)

        yield* lock.release
        yield* wakeDeliversContinuation(started)
      }).pipe(Effect.provideService(Logger.CurrentLoggers, capture.loggers))
    }),
  180_000,
)

consumeLock.instance(
  "a consume that cannot get the session permit is bounded by its backstop and stops the run with a named error",
  () =>
    Effect.gen(function* () {
      const capture = yield* captureConsumeLogs()
      yield* Effect.gen(function* () {
        const queue = yield* SessionQueue.Service
        const status = yield* SessionStatus.Service
        const runState = yield* SessionRunState.Service
        // A session writer that never finishes holds the permit every consume attempt waits for.
        const scope = yield* Scope.Scope
        const permitRelease = yield* Deferred.make<void>()
        const take: ConsumeHold = (sessionID) =>
          Effect.gen(function* () {
            const taken = yield* Deferred.make<void>()
            yield* queue
              .exclusive(
                sessionID,
                Deferred.succeed(taken, undefined).pipe(Effect.andThen(Deferred.await(permitRelease))),
              )
              .pipe(Effect.forkIn(scope))
            yield* Deferred.await(taken)
          })
        const started = yield* startConsumeLocked(take)
        const { llm, chat, task, continuation, records } = started
        const before = records()
        const exits = yield* awaitWithTimeout(
          Effect.all([Fiber.await(task), Fiber.await(continuation)]),
          "the run never stopped while the session permit was held",
          "160 seconds",
        ).pipe(Effect.onError(() => Deferred.succeed(permitRelease, undefined)))

        consumeRefusals(exits).forEach((refusal) => {
          expect(refusal).toBeInstanceOf(SessionQueue.ConsumeLockedError)
          expect(refusal).toMatchObject({ sessionID: chat.id, attempts: 5 })
        })
        expect(capture.backstop).toEqual([1, 2, 3, 4, 5].map((attempts) => ({ "session.id": chat.id, attempts })))
        expect(capture.locked).toEqual([])
        expect(records()).toEqual(before)
        expect(yield* llm.calls).toBe(1)
        expect((yield* status.list()).has(chat.id)).toBe(false)
        expect(Exit.isSuccess(yield* runState.assertNotBusy(chat.id).pipe(Effect.exit))).toBe(true)

        yield* Deferred.succeed(permitRelease, undefined)
        yield* wakeDeliversContinuation(started)
      }).pipe(Effect.provideService(Logger.CurrentLoggers, capture.loggers))
    }),
  220_000,
)

consumeLock.instance(
  "a consume whose later attempts wait on a connection another transaction keeps refuses before it ends, and its abandoned attempts delete nothing",
  () =>
    Effect.gen(function* () {
      const queue = yield* SessionQueue.Service
      const status = yield* SessionStatus.Service
      const runState = yield* SessionRunState.Service
      const { db } = yield* Database.Service
      const services = yield* Effect.context<never>()
      // The consume's own BEGIN IMMEDIATE attempts and its deletes, counted at the native statement.
      const begins = newStatementAttempts()
      yield* patchBunQuery((sql) => sql === "begin immediate", begins)
      const deletes = newStatementAttempts()
      yield* patchBunQuery(
        (sql) => /^delete from "session_prompt_queue" where .*"time_promoted" is not null/.test(sql),
        deletes,
      )

      // An unrelated deferred transaction that keeps the shared connection until released. It starts inside the
      // first failed attempt's log, before the next attempt is made, so it takes the connection ahead of it.
      const owned = yield* Deferred.make<void>()
      const holderRelease = yield* Deferred.make<void>()
      const holdConnection = () =>
        Effect.runForkWith(services)(
          db.transaction(() => Deferred.succeed(owned, undefined).pipe(Effect.andThen(Deferred.await(holderRelease)))),
        )
      const capture = yield* captureConsumeLogs(() => {
        if (!Deferred.isDoneUnsafe(owned)) holdConnection()
      })
      yield* Effect.gen(function* () {
        const lock = yield* writeLockHold()
        const started = yield* startConsumeLocked(lock.take)
        const { llm, chat, task, continuation, records } = started
        const before = records()
        const deletesBefore = deletes.count

        // The first attempt meets the native lock; then the transaction owns the connection and the lock is released.
        yield* awaitWithTimeout(Deferred.await(owned), "the transaction never took the shared connection", "30 seconds")
        expect(begins.busy.length).toBeGreaterThan(0)
        expect(capture.locked).toEqual([{ "session.id": chat.id, attempts: 1 }])
        yield* lock.release
        const exits = yield* awaitWithTimeout(
          Effect.all([Fiber.await(task), Fiber.await(continuation)]),
          "the run never stopped while another transaction kept the connection",
          "140 seconds",
        ).pipe(Effect.onError(() => Deferred.succeed(holderRelease, undefined)))

        // Refused, attributably and with the runner stopped, while the transaction still holds the connection.
        consumeRefusals(exits).forEach((refusal) => {
          expect(refusal).toBeInstanceOf(SessionQueue.ConsumeLockedError)
          expect(refusal).toMatchObject({ sessionID: chat.id, attempts: 5 })
        })
        expect(capture.backstop).toEqual([2, 3, 4, 5].map((attempts) => ({ "session.id": chat.id, attempts })))
        expect((yield* status.list()).has(chat.id)).toBe(false)
        expect(Exit.isSuccess(yield* runState.assertNotBusy(chat.id).pipe(Effect.exit))).toBe(true)
        expect(Deferred.isDoneUnsafe(holderRelease)).toBe(false)
        const beginsAtRefusal = begins.count

        // Released, the abandoned attempt that kept the session permit gets the connection and begins its
        // transaction. Once the permit is free again it has ended, and nothing was deleted.
        yield* Deferred.succeed(holderRelease, undefined)
        yield* awaitWithTimeout(
          queue.exclusive(chat.id, Effect.void),
          "the abandoned attempt never released the session permit",
          "15 seconds",
        )
        expect(begins.count).toBeGreaterThan(beginsAtRefusal)
        expect(deletes.count).toBe(deletesBefore)
        expect(records()).toEqual(before)
        expect(yield* llm.calls).toBe(1)

        yield* wakeDeliversContinuation(started)
      }).pipe(Effect.provideService(Logger.CurrentLoggers, capture.loggers))
    }),
  240_000,
)

// Makes the consume's delete, which runs only after its transaction began, fail with SQLITE_BUSY while armed, so the
// driver's own statement retry gives up inside a transaction body that already ran.
const busyConsumeDelete = Effect.fn("test.busyConsumeDelete")(function* () {
  const armed = { on: false, executions: 0 }
  const prototype = Sqlite.prototype
  const descriptor =
    Object.getOwnPropertyDescriptor(prototype, "query") ??
    (yield* Effect.die(new Error("bun:sqlite query method was not found")))
  const query: typeof prototype.query = descriptor.value
  const patched = new WeakSet<object>()
  Object.defineProperty(prototype, "query", {
    ...descriptor,
    value: function (this: Sqlite, sql: string) {
      const statement = query.call(this, sql)
      if (!/^delete from "session_prompt_queue" where .*"time_promoted" is not null/.test(sql) || patched.has(statement))
        return statement
      patched.add(statement)
      const all = statement.all
      Object.defineProperty(statement, "all", {
        configurable: true,
        writable: true,
        value: (...params: unknown[]) => {
          if (!armed.on) return all.apply(statement, params as never)
          armed.executions++
          throw Object.assign(new Error("database is locked"), { code: "SQLITE_BUSY", errno: 5 })
        },
      })
      return statement
    },
  })
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      armed.on = false
      Object.defineProperty(prototype, "query", descriptor)
    }),
  )
  return armed
})

consumeLock.instance(
  "a consume whose write lock fails after its delete began dies with that error instead of replaying the body",
  () =>
    Effect.gen(function* () {
      const capture = yield* captureConsumeLogs()
      yield* Effect.gen(function* () {
        const busy = yield* busyConsumeDelete()
        const { llm, chat, task, continuation, records } = yield* startConsumeLocked(() =>
          Effect.sync(() => void (busy.on = true)),
        )
        const before = records()
        const exits = yield* awaitWithTimeout(
          Effect.all([Fiber.await(task), Fiber.await(continuation)]),
          "the run never stopped on the failing delete",
          "45 seconds",
        )
        busy.on = false

        consumeRefusals(exits).forEach((refusal) => {
          expect(refusal).not.toBeInstanceOf(SessionQueue.ConsumeLockedError)
          expect(sqliteLockMessage(refusal)).toBe("Database is locked (SQLITE_BUSY)")
        })
        // The delete's own statement retries ran inside its attempt, and no further attempt was made.
        expect(busy.executions).toBeGreaterThan(0)
        expect(capture.locked).toEqual([])
        expect(capture.backstop).toEqual([])
        expect(records()).toEqual(before)
        expect(yield* llm.calls).toBe(1)
      }).pipe(Effect.provideService(Logger.CurrentLoggers, capture.loggers))
    }),
  60_000,
)

const resourcePart = (uri: string) => ({
  type: "file" as const,
  mime: "text/plain",
  filename: "resource.txt",
  url: uri,
  source: { type: "resource" as const, clientName: "test", uri, text: { value: "", start: 0, end: 0 } },
})

const itemTexts = (items: ReadonlyArray<SessionQueue.Item>) =>
  items.map((item) => item.input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join(" "))

it.instance(
  "a steer after a compaction whose summary turn failed is delivered as its own turn",
  () =>
    Effect.gen(function* () {
      const compaction = yield* SessionCompaction.Service
      const { llm, prompt, chat, task, release } = yield* startHeld()
      yield* compaction.create({ sessionID: chat.id, agent: "build", model: ref, auto: false })
      yield* llm.error(400, { error: { message: "summary rejected" } })
      yield* release
      yield* finish(task)
      expect(yield* llm.calls).toBe(2)

      yield* llm.text("steer done")
      yield* awaitWithTimeout(
        prompt.prompt({
          sessionID: chat.id,
          agent: "build",
          model: ref,
          parts: said("steer after the failed summary"),
        }),
        "steer prompt never returned",
        "10 seconds",
      )
      yield* awaitWithTimeout(llm.wait(3), "steer never reached the model", "10 seconds")
      expect(lastUser((yield* llm.inputs)[2])).toEqual({ role: "user", content: "steer after the failed summary" })
    }),
  15_000,
)

it.instance(
  "a steer after a compaction whose summary turn was aborted is delivered as its own turn",
  () =>
    Effect.gen(function* () {
      const compaction = yield* SessionCompaction.Service
      const { llm, prompt, chat, task, release } = yield* startHeld()
      yield* compaction.create({ sessionID: chat.id, agent: "build", model: ref, auto: false })
      yield* llm.hang
      yield* release
      yield* awaitWithTimeout(llm.wait(2), "summary turn never started", "10 seconds")
      yield* prompt.cancel(chat.id)
      yield* finish(task)

      yield* llm.text("steer done")
      yield* awaitWithTimeout(
        prompt.prompt({
          sessionID: chat.id,
          agent: "build",
          model: ref,
          parts: said("steer after the aborted summary"),
        }),
        "steer prompt never returned",
        "10 seconds",
      )
      yield* awaitWithTimeout(llm.wait(3), "steer never reached the model", "10 seconds")
      expect(lastUser((yield* llm.inputs)[2])).toEqual({ role: "user", content: "steer after the aborted summary" })
    }),
  15_000,
)

it.instance(
  "a structured result settles the run: its caller keeps it and a queued prompt still runs",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const gate = yield* Deferred.make<void>()
      yield* llm.push(reply().wait(deferredAsPromise(gate)).tool("StructuredOutput", { answer: "42" }))
      yield* llm.text("queued done")
      const events = yield* EventV2Bridge.Service
      const idleAt: number[] = []
      const unsubscribe = yield* events.listen((event) =>
        event.type === SessionStatus.Event.Idle.type && (event.data as { sessionID: string }).sessionID === chat.id
          ? llm.calls.pipe(Effect.map((calls) => void idleAt.push(calls)))
          : Effect.void,
      )
      yield* Effect.addFinalizer(() => unsubscribe)
      const format = Schema.decodeUnknownSync(SessionV1.Format)({
        type: "json_schema",
        schema: { type: "object", properties: { answer: { type: "string" } }, required: ["answer"] },
      })

      const structured = yield* prompt
        .prompt({ sessionID: chat.id, agent: "build", model: ref, format, parts: said("answer in structure") })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "structured call never started", "10 seconds")
      const held = yield* prompt
        .prompt({ sessionID: chat.id, agent: "build", model: ref, delivery: "queue", parts: said("queued after it") })
        .pipe(Effect.forkChild)
      yield* queued(chat.id, 1)
      yield* Deferred.succeed(gate, void 0)

      const [result, queuedReply] = yield* finish(structured, held)
      expect(result?.info.role === "assistant" ? result.info.structured : undefined).toEqual({ answer: "42" })
      expect(queuedReply?.info.role).toBe("assistant")
      expect(queuedReply?.parts.some((part) => part.type === "text" && part.text === "queued done")).toBe(true)
      yield* awaitWithTimeout(llm.wait(2), "queued prompt never ran after the structured result", "10 seconds")
      expect(lastUser((yield* llm.inputs)[1])).toEqual({ role: "user", content: "queued after it" })
      // One drain carries the queued turn: the session never goes idle between them.
      expect(idleAt).toEqual([2])
    }),
  15_000,
)

it.instance(
  "queue lists are published in the order their changes happened",
  () =>
    Effect.gen(function* () {
      const events = yield* EventV2Bridge.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const firstHeld = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const early = yield* Deferred.make<void>()
      const seen: string[][] = []
      let calls = 0
      // The first listener call pauses between the list snapshot and its delivery.
      const unsubscribe = yield* events.listen((event) => {
        if (event.type !== SessionQueue.Event.Updated.type) return Effect.void
        const data = event.data as { sessionID: string; items: ReadonlyArray<SessionQueue.Item> }
        if (data.sessionID !== chat.id) return Effect.void
        calls += 1
        const call = calls
        return Effect.gen(function* () {
          if (call === 1) {
            yield* Deferred.succeed(firstHeld, undefined)
            yield* Deferred.await(release)
          }
          if (call === 2) yield* Deferred.succeed(early, undefined)
          seen.push(itemTexts(data.items))
        })
      })
      yield* Effect.addFinalizer(() => unsubscribe)
      const admit = (text: string) =>
        queue.admit({ sessionID: chat.id, agent: "build", model: ref, delivery: "queue", parts: said(text) })

      const first = yield* admit("first").pipe(Effect.forkChild({ startImmediately: true }))
      yield* awaitWithTimeout(Deferred.await(firstHeld), "first admission never published")
      const second = yield* admit("second").pipe(Effect.forkChild({ startImmediately: true }))
      // Only a second admission racing past the paused first one can publish now.
      yield* Deferred.await(early).pipe(Effect.timeoutOption("1 second"))
      yield* Deferred.succeed(release, undefined)
      yield* finish(first, second)

      expect(seen).toEqual([["first"], ["first", "second"]])
    }),
  15_000,
)

it.instance(
  "a prompt stays listed while it becomes a message, and an interrupted promotion leaves it pending",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const gate = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      heldResource.gate = gate
      yield* Effect.addFinalizer(() => Effect.sync(() => void (heldResource.gate = undefined)))

      const slow = yield* prompt
        .prompt({
          sessionID: chat.id,
          agent: "build",
          model: ref,
          parts: [...said("with a slow resource"), resourcePart(HELD_RESOURCE)],
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(gate.entered), "promotion never read the resource")
      const listed = yield* queue.list(chat.id)
      expect(itemTexts(listed)).toEqual(["with a slow resource"])

      yield* Fiber.interrupt(slow)
      expect((yield* queue.list(chat.id)).map((item) => item.id)).toEqual(listed.map((item) => item.id))

      yield* llm.text("done")
      yield* prompt.prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("wake up") })
      const inputs = yield* llm.inputs
      expect(inputs).toHaveLength(1)
      expect(lastUser(inputs[0])).toEqual({ role: "user", content: "wake up" })
      expect(JSON.stringify(modelMessages(inputs[0])).split("with a slow resource").length - 1).toBe(1)
      expect(mentions(inputs[0], "held resource text")).toBe(true)
      expect(yield* queue.list(chat.id)).toEqual([])
    }),
  15_000,
)

it.instance(
  "an older steer that cannot become a message does not fail a new prompt",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const events = yield* EventV2Bridge.Service
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const errors: unknown[] = []
      const unsubscribe = yield* events.listen((event) =>
        event.type === Session.Event.Error.type && (event.data as { sessionID?: string }).sessionID === chat.id
          ? Effect.sync(() => void errors.push(event.data))
          : Effect.void,
      )
      yield* Effect.addFinalizer(() => unsubscribe)
      // A steer left pending (as one held behind a compaction is) whose resource is missing.
      yield* queue.admit({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: [...said("older and broken"), resourcePart("missing://resource")],
      })
      yield* llm.text("newer done")

      const result = yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: said("newer works"),
      })

      expect(result.info.role).toBe("assistant")
      const inputs = yield* llm.inputs
      expect(inputs).toHaveLength(1)
      expect(lastUser(inputs[0])).toEqual({ role: "user", content: "newer works" })
      expect(mentions(inputs[0], "older and broken")).toBe(false)
      expect(errors).toHaveLength(1)
      expect(yield* queue.list(chat.id)).toEqual([])
    }),
  15_000,
)

it.instance(
  "a withdraw wins over a promotion still preparing the same prompt",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const gate = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      heldResource.gate = gate
      yield* Effect.addFinalizer(() => Effect.sync(() => void (heldResource.gate = undefined)))

      const slow = yield* prompt
        .prompt({
          sessionID: chat.id,
          agent: "build",
          model: ref,
          parts: [...said("withdrawn while preparing"), resourcePart(HELD_RESOURCE)],
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(gate.entered), "promotion never read the resource")
      const [item] = yield* queue.list(chat.id)
      // The session lock is free while the message is prepared, so the withdraw lands now.
      const withdrawn = yield* awaitWithTimeout(queue.withdraw(chat.id, item!.id), "withdraw waited for the promotion")
      expect(Option.getOrUndefined(withdrawn)?.id).toBe(item!.id)
      yield* Deferred.succeed(gate.release, undefined)

      const exit = yield* awaitWithTimeout(Fiber.await(slow), "withdrawn prompt never returned")
      expect(Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined).toBeInstanceOf(SessionQueue.WithdrawnError)
      const messages = yield* sessions.messages({ sessionID: chat.id })
      expect(
        messages.some((msg) =>
          msg.parts.some((part) => part.type === "text" && part.text === "withdrawn while preparing"),
        ),
      ).toBe(false)
      expect(yield* llm.calls).toBe(0)
      expect(yield* queue.list(chat.id)).toEqual([])
    }),
  15_000,
)

gated.instance(
  "a prompt admitted while a stopping turn is still in flight is not parked by that stop",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const gate = yield* Deferred.make<void>()
      yield* llm.push(reply().wait(deferredAsPromise(gate)).contentFilter())
      yield* llm.text("after the stop")
      yield* Effect.addFinalizer(() => Effect.sync(() => void (gates.nextEnsureRunning = undefined)))

      const task = yield* prompt
        .prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("start the task") })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
      // Admitted after the stopping turn's last history read, it joins that turn's
      // run before the stop lands: the stop never saw it.
      const joined = { reached: yield* Deferred.make<void>(), startedRun: false }
      gates.nextEnsureRunning = joined
      const late = yield* prompt
        .prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("admitted before the stop landed") })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(joined.reached), "late prompt never reached the running turn")
      expect(joined.startedRun).toBe(false)
      yield* Deferred.succeed(gate, void 0)

      const [, answer] = yield* finish(task, late)
      expect(yield* llm.calls).toBe(2)
      expect(lastUser((yield* llm.inputs)[1])).toEqual({ role: "user", content: "admitted before the stop landed" })
      expect(answer?.parts.some((part) => part.type === "text" && part.text === "after the stop")).toBe(true)
    }),
  15_000,
)

gated.instance(
  "a steer admitted while a compaction writes its continue message lands after that message",
  () =>
    Effect.gen(function* () {
      const compaction = yield* SessionCompaction.Service
      const { llm, chat, task, send, release } = yield* startHeld()
      yield* compaction.create({ sessionID: chat.id, agent: "build", model: ref, auto: true })
      yield* llm.text("summary of the task")
      yield* llm.text("steer done")
      const hook = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      gates.compactionContinue = hook
      yield* Effect.addFinalizer(() => Effect.sync(() => void (gates.compactionContinue = undefined)))

      yield* release
      yield* awaitWithTimeout(Deferred.await(hook.entered), "compaction never reached its continue message")
      const steer = yield* send("steer during the continue")
      yield* admitted(chat.id, "steer during the continue")
      yield* Deferred.succeed(hook.release, undefined)
      yield* finish(task, steer)

      const inputs = yield* llm.inputs
      expect(inputs).toHaveLength(3)
      expect(mentions(inputs[1], "steer during the continue")).toBe(false)
      expect(mentions(inputs[2], "Continue if you have next steps")).toBe(true)
      expect(lastUser(inputs[2])).toEqual({ role: "user", content: "steer during the continue" })
    }),
  15_000,
)

// Reply routing: every caller shape, asserting the assistant each caller got
// back. A turn's reply is its final message, which the prompt that began it and
// every steer that joined it share; a queued prompt's turn is its own.

// The user message a prompt became, once it has; a steer sent during a step is
// then known to be part of that step's history.
const asked = (sessionID: SessionID, text: string) =>
  pollWithTimeout(
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      return (yield* sessions.messages({ sessionID })).find(
        (msg) => msg.info.role === "user" && msg.parts.some((part) => part.type === "text" && part.text === text),
      )?.info.id
    }),
    `prompt "${text}" never became a message`,
  )

const answered = (
  reply: SessionV1.WithParts | undefined,
): { parentID: string; texts: string[]; error?: string } | undefined =>
  reply?.info.role === "assistant"
    ? {
        parentID: reply.info.parentID,
        texts: reply.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])),
        error: reply.info.error?.name,
      }
    : undefined

it.instance(
  "a plain text turn steered mid-turn: its caller and both steers get the turn's final reply, not the step they overtook",
  () =>
    Effect.gen(function* () {
      const { llm, chat, task, send, release } = yield* startHeld()
      yield* llm.text("steered reply")
      yield* llm.text("queued reply")

      const held = yield* send("queued behind the turn", { delivery: "queue" })
      yield* queued(chat.id, 1)
      const first = yield* send("first steer")
      yield* asked(chat.id, "first steer")
      const second = yield* send("second steer")
      yield* asked(chat.id, "second steer")
      yield* release
      const [original, older, newer, queuedReply] = yield* finish(task, first, second, held)

      expect(yield* llm.calls).toBe(3)
      const turn = { parentID: yield* asked(chat.id, "second steer"), texts: ["steered reply"], error: undefined }
      expect([original, older, newer].map(answered)).toEqual([turn, turn, turn])
      expect(answered(queuedReply)).toEqual({
        parentID: yield* asked(chat.id, "queued behind the turn"),
        texts: ["queued reply"],
        error: undefined,
      })
    }),
  15_000,
)

it.instance(
  "a plain text turn steered mid-turn whose continuation errors: every caller gets the errored reply",
  () =>
    Effect.gen(function* () {
      const { llm, chat, task, send, release } = yield* startHeld()
      yield* llm.error(400, { error: { message: "rejected by the provider" } })

      const first = yield* send("first steer")
      yield* asked(chat.id, "first steer")
      const second = yield* send("second steer")
      yield* asked(chat.id, "second steer")
      yield* release
      const replies = yield* finish(task, first, second)

      expect(yield* llm.calls).toBe(2)
      const turn = { parentID: yield* asked(chat.id, "second steer"), texts: [], error: "APIError" }
      expect(replies.map(answered)).toEqual([turn, turn, turn])
    }),
  15_000,
)

it.instance(
  "a multi-step tool turn steered mid-turn: its caller and the steer get the turn's final reply",
  () =>
    Effect.gen(function* () {
      const { llm, chat, task, send, release } = yield* startHeld({ tool: true })
      const step = yield* Deferred.make<void>()
      yield* llm.hold("task done", deferredAsPromise(step))
      yield* llm.text("steered reply")
      yield* llm.text("queued reply")

      const held = yield* send("queued behind the turn", { delivery: "queue" })
      yield* queued(chat.id, 1)
      yield* release
      yield* awaitWithTimeout(llm.wait(2), "the tool turn never took its second step", "10 seconds")
      const steer = yield* send("steer mid-turn")
      yield* asked(chat.id, "steer mid-turn")
      yield* Deferred.succeed(step, void 0)
      const [original, steered, queuedReply] = yield* finish(task, steer, held)

      expect(yield* llm.calls).toBe(4)
      const turn = { parentID: yield* asked(chat.id, "steer mid-turn"), texts: ["steered reply"], error: undefined }
      expect([original, steered].map(answered)).toEqual([turn, turn])
      expect(answered(queuedReply)).toEqual({
        parentID: yield* asked(chat.id, "queued behind the turn"),
        texts: ["queued reply"],
        error: undefined,
      })
    }),
  15_000,
)

it.instance(
  "a queued caller joining an active run gets its own turn's final reply, and the task's caller keeps its own",
  () =>
    Effect.gen(function* () {
      const { llm, chat, task, send, release } = yield* startHeld()
      const step = yield* Deferred.make<void>()
      yield* llm.hold("queued step", deferredAsPromise(step))
      yield* llm.text("steered reply")

      const held = yield* send("queued behind the task", { delivery: "queue" })
      yield* queued(chat.id, 1)
      yield* release
      yield* awaitWithTimeout(llm.wait(2), "the queued turn never started", "10 seconds")
      const steer = yield* send("steer into the queued turn")
      yield* asked(chat.id, "steer into the queued turn")
      yield* Deferred.succeed(step, void 0)
      const [original, queuedReply, steered] = yield* finish(task, held, steer)

      expect(yield* llm.calls).toBe(3)
      expect(answered(original)).toEqual({
        parentID: yield* asked(chat.id, "start the task"),
        texts: ["task done"],
        error: undefined,
      })
      const turn = {
        parentID: yield* asked(chat.id, "steer into the queued turn"),
        texts: ["steered reply"],
        error: undefined,
      }
      expect([queuedReply, steered].map(answered)).toEqual([turn, turn])
    }),
  15_000,
)

gated.instance(
  "a turn that stops answers its callers then, so one slow to collect never gets a later turn's reply",
  () =>
    Effect.gen(function* () {
      const { llm, chat, task, send, release } = yield* startHeld()
      const stop = yield* Deferred.make<void>()
      yield* llm.push(reply().wait(deferredAsPromise(stop)).contentFilter())
      yield* llm.text("after the stop")
      yield* Effect.addFinalizer(() => Effect.sync(() => void (gates.nextEnsureRunning = undefined)))

      // A steer the stopping turn carries; its caller is slow to collect once its run ends.
      const slow = { reached: yield* Deferred.make<void>(), startedRun: false, hold: yield* Deferred.make<void>() }
      gates.nextEnsureRunning = slow
      const steer = yield* send("steer into the stop")
      yield* awaitWithTimeout(Deferred.await(slow.reached), "the steer never joined the running turn")
      yield* release
      yield* awaitWithTimeout(llm.wait(2), "the steered step never started", "10 seconds")
      // Sent after the stopping step's history read, so it gets a turn of its own.
      const late = yield* send("sent before the stop")
      yield* asked(chat.id, "sent before the stop")
      yield* Deferred.succeed(stop, void 0)
      const [original, lateReply] = yield* finish(task, late)
      yield* Deferred.succeed(slow.hold, void 0)
      const [steered] = yield* finish(steer)

      expect(yield* llm.calls).toBe(3)
      const stopped = { parentID: yield* asked(chat.id, "steer into the stop"), texts: [], error: "ContentFilterError" }
      expect([original, steered].map(answered)).toEqual([stopped, stopped])
      expect(answered(lateReply)).toEqual({
        parentID: yield* asked(chat.id, "sent before the stop"),
        texts: ["after the stop"],
        error: undefined,
      })
    }),
  15_000,
)

gated.instance(
  "a cancelled turn answers its callers as it stops, so one slow to collect never gets the next turn's reply",
  () =>
    Effect.gen(function* () {
      const { llm, prompt, chat, task, send, release } = yield* startHeld()
      yield* llm.hold("never finished", new Promise(() => {}))
      yield* Effect.addFinalizer(() => Effect.sync(() => void (gates.nextEnsureRunning = undefined)))

      const slow = { reached: yield* Deferred.make<void>(), startedRun: false, hold: yield* Deferred.make<void>() }
      gates.nextEnsureRunning = slow
      const steer = yield* send("steer into the cancelled turn")
      yield* awaitWithTimeout(Deferred.await(slow.reached), "the steer never joined the running turn")
      yield* release
      yield* awaitWithTimeout(llm.wait(2), "the steered step never started", "10 seconds")
      yield* prompt.cancel(chat.id)
      const [original] = yield* finish(task)

      yield* llm.text("wake reply")
      const woken = yield* prompt.prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("wake up") })
      yield* Deferred.succeed(slow.hold, void 0)
      const [steered] = yield* finish(steer)

      const cancelled = {
        parentID: yield* asked(chat.id, "steer into the cancelled turn"),
        texts: [],
        error: "MessageAbortedError",
      }
      expect([original, steered].map(answered)).toEqual([cancelled, cancelled])
      expect(answered(woken)).toEqual({
        parentID: yield* asked(chat.id, "wake up"),
        texts: ["wake reply"],
        error: undefined,
      })
    }),
  15_000,
)

it.instance(
  "a withdraw that beats a new session's first queued prompt ends its caller with WithdrawnError",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const gate = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      heldResource.gate = gate
      yield* Effect.addFinalizer(() => Effect.sync(() => void (heldResource.gate = undefined)))

      const first = yield* prompt
        .prompt({
          sessionID: chat.id,
          agent: "build",
          model: ref,
          delivery: "queue",
          parts: [...said("first and withdrawn"), resourcePart(HELD_RESOURCE)],
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(gate.entered), "first prompt was never prepared")
      const [item] = yield* queue.list(chat.id)
      expect(Option.isSome(yield* queue.withdraw(chat.id, item!.id))).toBe(true)
      yield* Deferred.succeed(gate.release, undefined)

      const exit = yield* awaitWithTimeout(Fiber.await(first), "withdrawn first prompt never returned")
      expect(Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined).toBeInstanceOf(SessionQueue.WithdrawnError)
      expect(yield* sessions.messages({ sessionID: chat.id })).toEqual([])
      expect(yield* llm.calls).toBe(0)
    }),
  15_000,
)

it.instance(
  "an interrupted caller leaves no reply tracked for its prompt",
  () =>
    Effect.gen(function* () {
      const { llm, queue, chat, task, send, release } = yield* startHeld()
      yield* llm.text("delivered anyway")
      const abandoned = yield* send("queued then abandoned", { delivery: "queue" })
      const [item] = yield* queued(chat.id, 1)
      yield* Fiber.interrupt(abandoned)

      yield* release
      yield* finish(task)
      // The run still delivers the prompt; only its caller is gone.
      expect(lastUser((yield* llm.inputs)[1])).toEqual({ role: "user", content: "queued then abandoned" })
      expect(yield* queue.reply(item!.id)).toBeUndefined()
    }),
  15_000,
)

it.instance(
  "a run interrupted without a prompt cancel, as instance disposal does, also parks pending prompts",
  () =>
    Effect.gen(function* () {
      const run = yield* SessionRunState.Service
      const { llm, queue, chat, task, send } = yield* startHeld()
      const held = yield* send("parked by the interrupt", { delivery: "queue" })
      yield* queued(chat.id, 1)

      yield* run.cancel(chat.id)
      yield* finish(task, held)
      expect(yield* llm.calls).toBe(1)
      expect((yield* queue.list(chat.id)).map((item) => item.input.parts)).toEqual([said("parked by the interrupt")])
    }),
  15_000,
)

it.instance(
  "a steer sent while a compaction task is pending reaches the first call after it, not the compaction",
  () =>
    Effect.gen(function* () {
      const compaction = yield* SessionCompaction.Service
      const { llm, chat, task, send, release } = yield* startHeld()
      yield* llm.text("summary of the task")
      yield* llm.text("steer done")

      yield* compaction.create({ sessionID: chat.id, agent: "build", model: ref, auto: false })
      const steer = yield* send("steer during compaction")
      yield* admitted(chat.id, "steer during compaction")
      yield* release
      yield* finish(task, steer)

      const inputs = yield* llm.inputs
      expect(inputs).toHaveLength(3)
      expect(mentions(inputs[1], "steer during compaction")).toBe(false)
      expect(lastUser(inputs[2])).toEqual({ role: "user", content: "steer during compaction" })
    }),
  15_000,
)

it.instance(
  "a prompt still pending when a cancel parks its idle session is not run by its caller's drain",
  () =>
    Effect.gen(function* () {
      const { llm, prompt, chat, task, send } = yield* startHeld()
      const queue = yield* SessionQueue.Service
      const held = yield* send("held over the cancel", { delivery: "queue", requestID: "held-over" })
      const [pending] = yield* queued(chat.id, 1)
      yield* prompt.cancel(chat.id)
      yield* finish(task, held)
      expect(yield* llm.calls).toBe(1)

      // Its retry waits on the same item, after the cancel and with nothing
      // admitted since, so the session is idle and still parked.
      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: said("held over the cancel"),
        delivery: "queue",
        requestID: "held-over",
      })
      expect(yield* queue.list(chat.id)).toEqual([pending])
      expect(yield* llm.calls).toBe(1)
    }),
  15_000,
)

it.instance(
  "queued prompts survive a rebuild of the session services over the same database",
  () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const { llm, prompt, chat, task, send } = yield* startHeld()
      const held = yield* send("survives the rebuild", { delivery: "queue" })
      const [pending] = yield* queued(chat.id, 1)
      yield* prompt.cancel(chat.id)
      yield* finish(task, held)

      yield* llm.text("wake done")
      yield* llm.text("held done")
      const rebuilt = LayerNode.compile(promptRoot, [
        [Npm.node, promptSdk],
    [SessionSummary.node, summary],
        [LSP.node, lsp],
        [MCP.node, makeMcp()],
        [RuntimeFlags.node, runtimeFlags],
        [Database.node, Layer.succeed(Database.Service, database)],
      ])
      yield* Effect.gen(function* () {
        const queue = yield* SessionQueue.Service
        const again = yield* SessionPrompt.Service
        expect(yield* queue.list(chat.id)).toEqual([pending])
        yield* again.prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("wake up") })
        expect(yield* queue.list(chat.id)).toEqual([])
      }).pipe(Effect.provide(rebuilt))

      const inputs = yield* llm.inputs
      expect(inputs.slice(1).map(lastUser)).toEqual([
        { role: "user", content: "wake up" },
        { role: "user", content: "survives the rebuild" },
      ])
    }),
  15_000,
)

it.instance(
  "queued prompts disappear with their session",
  () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const { prompt, sessions, chat, task, send } = yield* startHeld()
      const held = yield* send("deleted with the session", { delivery: "queue" })
      yield* queued(chat.id, 1)
      yield* prompt.cancel(chat.id)
      yield* finish(task, held)

      yield* sessions.remove(chat.id)
      const rows = yield* db
        .select({ id: SessionPromptQueueTable.id })
        .from(SessionPromptQueueTable)
        .where(eq(SessionPromptQueueTable.session_id, chat.id))
        .all()
      const sequences = yield* db
        .select({ seq: SessionPromptQueueSequenceTable.seq })
        .from(SessionPromptQueueSequenceTable)
        .where(eq(SessionPromptQueueSequenceTable.session_id, chat.id))
        .all()
      expect(rows).toEqual([])
      expect(sequences).toEqual([])
    }),
  15_000,
)

it.instance(
  "a queued first prompt on a new idle session runs in the first provider call",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* llm.text("ran at once")

      const result = yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        delivery: "queue",
        parts: said("queued on an idle session"),
      })

      expect(result.info.role).toBe("assistant")
      expect(yield* llm.calls).toBe(1)
      expect(lastUser((yield* llm.inputs)[0])).toEqual({ role: "user", content: "queued on an idle session" })
    }),
  15_000,
)

it.instance(
  "noReply writes its message directly and starts no drain",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const run = yield* SessionRunState.Service
      const chat = yield* sessions.create({ title: "Pinned" })

      const message = yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        noReply: true,
        parts: said("context only"),
      })

      expect(message.info.role).toBe("user")
      expect((yield* sessions.messages({ sessionID: chat.id })).map((msg) => msg.info.id)).toEqual([message.info.id])
      expect(yield* queue.list(chat.id)).toEqual([])
      yield* run.assertNotBusy(chat.id)
      expect(yield* llm.calls).toBe(0)
    }),
  15_000,
)

it.instance(
  "writes an idle marked noReply directly without queueing or draining",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const run = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Idle noReply" })
      const markedText = "[fm-from-firstmate]\x1f idle machine mail"
      expect(MachineMessage.classify(markedText)).toBe("hold")

      const message = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        model: ref,
        noReply: true,
        parts: said(markedText),
      })
      if (message.info.role !== "user") throw new Error("expected the idle noReply user")
      expect(message.info.noReply).toBe(true)
      const messages = yield* sessions.messages({ sessionID: session.id })
      expect(messages).toHaveLength(1)
      expect(messages[0]?.info.id).toBe(message.info.id)
      expect(messages[0]?.parts.some((part) => part.type === "text" && part.text === markedText)).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
      yield* run.assertNotBusy(session.id)
      expect(yield* llm.calls).toBe(0)
    }),
  15_000,
)

it.instance(
  "holds marked noReply input out of an active run until the terminal boundary",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      yield* writeText(path.join(dir, "noReply-tool.txt"), "active run continuation")
      const session = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const toolGate = yield* Deferred.make<void>()
      const stopGate = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() =>
        Effect.all(
          [toolGate, stopGate].map((gate) => Deferred.succeed(gate, void 0).pipe(Effect.ignore)),
          { discard: true },
        ),
      )

      yield* llm.push(
        reply().wait(deferredAsPromise(toolGate)).tool("glob", { pattern: "noReply-tool.txt" }).item(),
        reply().wait(deferredAsPromise(stopGate)).text("active task finished").stop().item(),
        reply().text("held noReply handled").stop().item(),
      )
      const task = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "build",
          model: ref,
          parts: said("original task"),
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "provider did not receive the active task", "10 seconds")

      const messageID = MessageID.ascending()
      const markedText = "[fm-from-firstmate]\x1f noReply machine mail"
      const staged = yield* prompt.prompt({
        sessionID: session.id,
        messageID,
        agent: "build",
        model: ref,
        noReply: true,
        parts: said(markedText),
      })
      if (staged.info.role !== "user") throw new Error("expected the staged noReply user")
      expect(staged.info.noReply).toBe(true)
      const pending = yield* pollWithTimeout(
        queue.list(session.id).pipe(Effect.map((items) => items.find((item) => item.input.messageID === messageID))),
        "marked noReply input was not held in the queue",
        "10 seconds",
      )
      expect(pending.delivery).toBe("queue")
      expect(
        (yield* sessions.messages({ sessionID: session.id })).some(
          (message) => message.info.role === "user" && message.info.id === messageID,
        ),
      ).toBe(false)
      expect(yield* llm.calls).toBe(1)

      yield* Deferred.succeed(toolGate, void 0)
      yield* awaitWithTimeout(llm.wait(2), "active task continuation did not start", "10 seconds")
      const continuation = (yield* llm.inputs)[1]
      if (!continuation) throw new Error("expected the active task continuation")
      expect(JSON.stringify(continuation.messages)).not.toContain("noReply machine mail")

      yield* Deferred.succeed(stopGate, void 0)
      yield* awaitWithTimeout(llm.wait(3), "held noReply message did not start after the run", "10 seconds")
      const heldTurn = (yield* llm.inputs)[2]
      if (!heldTurn) throw new Error("expected the held noReply provider request")
      expect(JSON.stringify(heldTurn.messages)).toContain("noReply machine mail")

      const runExit = yield* awaitWithTimeout(Fiber.await(task), "active task did not finish", "10 seconds")
      expect(Exit.isSuccess(runExit)).toBe(true)
      const messages = yield* sessions.messages({ sessionID: session.id })
      const promoted = messages.find(
        (message) =>
          message.info.role === "user" &&
          message.parts.some((part) => part.type === "text" && part.text === markedText),
      )
      if (!promoted) throw new Error("expected the promoted noReply message")
      expect(
        messages.some((message) => message.info.role === "assistant" && message.info.parentID === promoted.info.id),
      ).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(3)
    }),
  60_000,
)

gated.instance(
  "drains marked noReply admitted after the active run has finished",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const run = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({
        title: "Held noReply admission race",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const runResponse = yield* Deferred.make<void>()
      const admissionEntered = yield* Deferred.make<void>()
      const admissionRelease = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() =>
        Effect.all(
          [runResponse, admissionRelease].map((gate) => Deferred.succeed(gate, undefined).pipe(Effect.ignore)),
          { discard: true },
        ).pipe(Effect.andThen(Effect.sync(() => void (gates.noReplyAdmission = undefined)))),
      )
      yield* llm.push(
        reply().wait(deferredAsPromise(runResponse)).text("active task finished").stop().item(),
        reply().text("held noReply handled").stop().item(),
      )

      const task = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "build",
          model: ref,
          parts: said("active task before held input"),
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "active task did not reach the provider", "10 seconds")

      gates.noReplyAdmission = { entered: admissionEntered, release: admissionRelease }
      const messageID = MessageID.ascending()
      const markedText = "[fm-from-firstmate]\x1f noReply machine mail after run"
      const staged = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID,
          agent: "build",
          model: ref,
          noReply: true,
          parts: said(markedText),
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(
        Deferred.await(admissionEntered),
        "marked noReply admission did not reach the gate after its busy check",
        "10 seconds",
      )
      expect(Exit.isFailure(yield* run.assertNotBusy(session.id).pipe(Effect.exit))).toBe(true)

      yield* Deferred.succeed(runResponse, undefined)
      const taskExit = yield* awaitWithTimeout(Fiber.await(task), "active task did not finish", "10 seconds")
      expect(Exit.isSuccess(taskExit)).toBe(true)
      expect(Exit.isSuccess(yield* run.assertNotBusy(session.id).pipe(Effect.exit))).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(
        (yield* sessions.messages({ sessionID: session.id })).some((message) =>
          message.parts.some((part) => part.type === "text" && part.text === markedText),
        ),
      ).toBe(false)

      yield* Deferred.succeed(admissionRelease, undefined)
      const stagedMessage = yield* awaitWithTimeout(Fiber.join(staged), "held noReply did not return", "10 seconds")
      if (stagedMessage.info.role !== "user") throw new Error("expected the staged noReply user")
      expect(stagedMessage.info.noReply).toBe(true)
      yield* awaitWithTimeout(llm.wait(2), "held noReply admission was stranded after the active run", "10 seconds")
      const input = (yield* llm.inputs)[1]
      if (!input) throw new Error("expected the held noReply provider request")
      expect(JSON.stringify(input.messages)).toContain("noReply machine mail after run")

      const delivered = yield* pollWithTimeout(
        Effect.gen(function* () {
          const messages = yield* sessions.messages({ sessionID: session.id })
          const user = messages.find(
            (message) =>
              message.info.role === "user" && message.parts.some((part) => part.type === "text" && part.text === markedText),
          )
          if (!user) return undefined
          return messages.some((message) => message.info.role === "assistant" && message.info.parentID === user.info.id)
            ? true
            : undefined
        }),
        "held noReply was not delivered after the original run",
        "10 seconds",
      )
      expect(delivered).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(2)
    }),
  30_000,
)

gated.instance(
  "rechecks an idle marked noReply before writing during a tool-call run",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const run = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({
        title: "Inverse noReply admission race",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const toolPath = path.join(dir, "round17-inverse.txt")
      yield* writeText(toolPath, "active tool-call run")
      const admissionEntered = yield* Deferred.make<void>()
      const admissionRelease = yield* Deferred.make<void>()
      const toolResponse = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() =>
        Effect.all(
          [admissionRelease, toolResponse].map((gate) => Deferred.succeed(gate, undefined).pipe(Effect.ignore)),
          {
            discard: true,
          },
        ).pipe(
          Effect.andThen(
            Effect.sync(() => {
              gates.noReplyAdmission = undefined
            }),
          ),
        ),
      )
      yield* llm.push(
        reply().wait(deferredAsPromise(toolResponse)).tool("glob", { pattern: "round17-inverse.txt" }).item(),
        reply().text("active task finished").stop().item(),
        reply().text("held noReply handled").stop().item(),
      )

      gates.noReplyAdmission = { entered: admissionEntered, release: admissionRelease }
      const messageID = MessageID.ascending()
      const markedText = "[fm-from-firstmate]\x1f round17 inverse machine mail"
      const staged = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID,
          agent: "build",
          model: ref,
          noReply: true,
          parts: said(markedText),
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(
        Deferred.await(admissionEntered),
        "marked noReply did not sample the idle run state",
        "10 seconds",
      )
      expect(Exit.isSuccess(yield* run.assertNotBusy(session.id).pipe(Effect.exit))).toBe(true)

      const task = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "build",
          model: ref,
          parts: said("ordinary task starts a tool-call run"),
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "ordinary task did not start its provider run", "10 seconds")
      expect(Exit.isFailure(yield* run.assertNotBusy(session.id).pipe(Effect.exit))).toBe(true)

      yield* Deferred.succeed(admissionRelease, undefined)
      const stagedMessage = yield* awaitWithTimeout(
        Fiber.join(staged),
        "marked noReply did not finish its idle-to-busy admission",
        "10 seconds",
      )
      if (stagedMessage.info.role !== "user") throw new Error("expected the staged noReply user")
      expect(stagedMessage.info.noReply).toBe(true)
      const pending = (yield* queue.list(session.id)).find((item) => item.input.messageID === messageID)
      if (!pending) throw new Error("marked noReply was written directly after the run became active")
      expect(pending.delivery).toBe("queue")
      expect(
        (yield* sessions.messages({ sessionID: session.id })).some((message) => message.info.id === messageID),
      ).toBe(false)
      expect(yield* llm.calls).toBe(1)

      yield* Deferred.succeed(toolResponse, undefined)
      yield* awaitWithTimeout(llm.wait(2), "active task tool continuation did not start", "10 seconds")
      const continuation = (yield* llm.inputs)[1]
      if (!continuation) throw new Error("expected the active task continuation request")
      expect(JSON.stringify(continuation.messages)).not.toContain("round17 inverse machine mail")

      yield* awaitWithTimeout(llm.wait(3), "held noReply did not start after the active run", "10 seconds")
      const held = (yield* llm.inputs)[2]
      if (!held) throw new Error("expected the held noReply request")
      expect(JSON.stringify(held.messages)).toContain("round17 inverse machine mail")
      const taskExit = yield* awaitWithTimeout(Fiber.await(task), "active task did not finish", "10 seconds")
      expect(Exit.isSuccess(taskExit)).toBe(true)
      const messages = yield* sessions.messages({ sessionID: session.id })
      const promoted = messages.find(
        (message) =>
          message.info.role === "user" &&
          message.parts.some((part) => part.type === "text" && part.text === markedText),
      )
      if (!promoted) throw new Error("expected the queued marked noReply message after the run")
      expect(
        messages.some((message) => message.info.role === "assistant" && message.info.parentID === promoted.info.id),
      ).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(3)
    }),
  60_000,
)

gated.instance(
  "keeps a rechecked marked noReply out of root-pinned tool continuations",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const run = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({
        title: "Root-pinned noReply continuation",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const toolPath = path.join(dir, "round18-root.txt")
      yield* writeText(toolPath, "root-pinned tool continuation")
      const root = yield* seedUser({
        sessionID: session.id,
        agent: "build",
        model: ref,
        parts: said("root-pinned task"),
      })
      const writeEntered = yield* Deferred.make<void>()
      const writeRelease = yield* Deferred.make<void>()
      const toolResponse = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() =>
        Effect.all(
          [writeRelease, toolResponse].map((gate) => Deferred.succeed(gate, undefined).pipe(Effect.ignore)),
          {
            discard: true,
          },
        ).pipe(
          Effect.andThen(
            Effect.sync(() => {
              gates.noReplyWrite = undefined
            }),
          ),
        ),
      )
      yield* llm.push(
        reply().wait(deferredAsPromise(toolResponse)).tool("glob", { pattern: "round18-root.txt" }).item(),
        reply().text("root-pinned task finished").stop().item(),
        reply().text("held root-pinned noReply handled").stop().item(),
      )

      gates.noReplyWrite = { entered: writeEntered, release: writeRelease }
      const messageID = MessageID.ascending()
      const markedText = "[fm-from-firstmate]\x1f root-pinned machine mail"
      const noReply = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID,
          agent: "build",
          model: ref,
          noReply: true,
          parts: said(markedText),
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(
        Deferred.await(writeEntered),
        "marked noReply did not reach the write after its idle recheck",
        "10 seconds",
      )
      expect(Exit.isSuccess(yield* run.assertNotBusy(session.id).pipe(Effect.exit))).toBe(true)

      const active = yield* prompt
        .loop({ sessionID: session.id, messageID: root.info.id })
        .pipe(Effect.forkChild)
      yield* pollWithTimeout(
        run.assertNotBusy(session.id).pipe(
          Effect.exit,
          Effect.map((exit) => (Exit.isFailure(exit) ? true : undefined)),
        ),
        "root-pinned run did not start during the post-recheck write",
        "10 seconds",
      )

      yield* Deferred.succeed(writeRelease, undefined)
      const direct = yield* awaitWithTimeout(
        Fiber.join(noReply),
        "rechecked marked noReply did not finish its direct write",
        "10 seconds",
      )
      if (direct.info.role !== "user") throw new Error("expected the direct noReply user")
      expect(direct.info.noReply).toBe(true)
      expect(
        (yield* sessions.messages({ sessionID: session.id })).some((message) => message.info.id === messageID),
      ).toBe(false)
      const pending = (yield* queue.list(session.id)).find((item) => item.input.messageID === messageID)
      if (!pending) throw new Error("marked noReply was not queued after the run started during its write")
      expect(pending.delivery).toBe("queue")

      yield* awaitWithTimeout(llm.wait(1), "root-pinned provider request did not start", "10 seconds")
      const first = (yield* llm.inputs)[0]
      if (!first) throw new Error("expected the root-pinned first request")
      expect(JSON.stringify(first.messages)).not.toContain("root-pinned machine mail")

      yield* Deferred.succeed(toolResponse, undefined)
      yield* awaitWithTimeout(llm.wait(2), "root-pinned tool continuation did not start", "10 seconds")
      const continuation = (yield* llm.inputs)[1]
      if (!continuation) throw new Error("expected the root-pinned continuation request")
      expect(JSON.stringify(continuation.messages)).not.toContain("root-pinned machine mail")

      yield* awaitWithTimeout(llm.wait(3), "rechecked marked noReply did not get its later turn", "10 seconds")
      const held = (yield* llm.inputs)[2]
      if (!held) throw new Error("expected the queued marked noReply turn")
      expect(JSON.stringify(held.messages)).toContain("root-pinned machine mail")
      const exit = yield* awaitWithTimeout(Fiber.await(active), "root-pinned run did not finish", "10 seconds")
      expect(Exit.isSuccess(exit)).toBe(true)
      const messages = yield* sessions.messages({ sessionID: session.id })
      const promoted = messages.find(
        (message) =>
          message.info.role === "user" &&
          message.parts.some((part) => part.type === "text" && part.text === markedText),
      )
      if (!promoted || promoted.info.role !== "user") throw new Error("expected the queued noReply row to remain in history")
      expect(promoted.info.noReply).toBeUndefined()
      expect(
        messages.some((message) => message.info.role === "assistant" && message.info.parentID === promoted.info.id),
      ).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(3)
    }),
  60_000,
)

it.instance(
  "does not rewrite an exact marked noReply messageID retry with direct custody",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Exact marked retry" })
      const messageID = MessageID.ascending()
      const markedText = "[fm-from-firstmate]\x1f exact retry machine mail"
      const input = {
        sessionID: session.id,
        messageID,
        agent: "build",
        model: ref,
        noReply: true,
        parts: said(markedText),
      }

      const original = yield* prompt.prompt(input)
      const retry = yield* prompt.prompt(input)
      expect(retry).toEqual(original)
      const messages = yield* sessions.messages({ sessionID: session.id })
      expect(messages).toHaveLength(1)
      expect(messages[0]?.parts.filter((part) => part.type === "text" && part.text === markedText)).toHaveLength(1)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(0)
    }),
  30_000,
)

it.instance(
  "refuses an idle changed marked noReply messageID against its direct original",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Idle changed marked retry" })
      const messageID = MessageID.ascending()
      const originalText = "[fm-from-firstmate]\x1f idle original machine mail"
      const changedText = "[fm-from-firstmate]\x1f idle changed machine mail"
      const original = yield* prompt.prompt({
        sessionID: session.id,
        messageID,
        agent: "build",
        model: ref,
        noReply: true,
        parts: said(originalText),
      })
      if (original.info.role !== "user") throw new Error("expected the original marked message")

      const retry = yield* prompt
        .prompt({
          sessionID: session.id,
          messageID,
          agent: "build",
          model: ref,
          noReply: true,
          parts: said(changedText),
        })
        .pipe(Effect.exit)
      expect(Exit.isFailure(retry)).toBe(true)
      if (!Exit.isFailure(retry)) throw new Error("changed bare identity was accepted")
      expect(Cause.pretty(retry.cause)).toContain("Conflicting direct noReply original")
      expect(yield* llm.calls).toBe(0)
      expect(yield* queue.list(session.id)).toEqual([])
      const messages = yield* sessions.messages({ sessionID: session.id })
      expect(messages).toHaveLength(1)
      expect(messages[0]?.info.id).toBe(messageID)
      expect(messages[0]?.parts.some((part) => part.type === "text" && part.text === originalText)).toBe(true)
      expect(messages[0]?.parts.some((part) => part.type === "text" && part.text === changedText)).toBe(false)
    }),
  30_000,
)

it.instance(
  "refuses repeated changed marked same-ID retries while preserving a busy multi-step run",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({
        title: "Repeated busy same-ID retry",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* writeText(path.join(dir, "round20-repeat-id.txt"), "root tool")
      const messageID = MessageID.ascending()
      const originalText = "[fm-from-firstmate]\x1f busy original machine mail"
      const changedText = "[fm-from-firstmate]\x1f repeated busy machine mail"
      const original = yield* prompt.prompt({
        sessionID: session.id,
        messageID,
        agent: "build",
        model: ref,
        noReply: true,
        parts: said(originalText),
      })
      if (original.info.role !== "user") throw new Error("expected the original marked message")
      const root = yield* seedUser({
        sessionID: session.id,
        agent: "build",
        model: ref,
        parts: said("repeated retry active root"),
      })
      const toolResponse = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() => Deferred.succeed(toolResponse, undefined).pipe(Effect.ignore))
      yield* llm.push(
        reply().wait(deferredAsPromise(toolResponse)).tool("glob", { pattern: "round20-repeat-id.txt" }).item(),
        reply().text("root task finished").stop().item(),
        reply().text("one held retry handled").stop().item(),
        reply().text("duplicate retry handled").stop().item(),
      )
      const active = yield* prompt.loop({ sessionID: session.id, messageID: root.info.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "active root did not reach the provider", "10 seconds")

      const retryInput = {
        sessionID: session.id,
        messageID,
        agent: "build",
        model: ref,
        noReply: true,
        parts: said(changedText),
      }
      for (const attempt of [1, 2, 3]) {
        const exit = yield* prompt.prompt(retryInput).pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
        if (!Exit.isFailure(exit)) throw new Error(`ambiguous retry ${attempt} was accepted`)
        expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionQueue.HistoricalIdentityError)
        expect(yield* queue.list(session.id)).toEqual([])
      }
      expect(yield* llm.calls).toBe(1)

      yield* Deferred.succeed(toolResponse, undefined)
      yield* awaitWithTimeout(llm.wait(2), "active root continuation did not start", "10 seconds")
      const continuation = (yield* llm.inputs)[1]
      if (!continuation) throw new Error("expected the active root continuation")
      expect(JSON.stringify(continuation.messages)).not.toContain("repeated busy machine mail")
      const exit = yield* awaitWithTimeout(Fiber.await(active), "active root did not finish", "10 seconds")
      expect(Exit.isSuccess(exit)).toBe(true)

      const messages = yield* sessions.messages({ sessionID: session.id })
      const originalRow = messages.find((message) => message.info.id === messageID)
      if (!originalRow || originalRow.info.role !== "user") throw new Error("expected the old row to remain")
      expect(originalRow.parts.some((part) => part.type === "text" && part.text === originalText)).toBe(true)
      expect(originalRow.parts.some((part) => part.type === "text" && part.text === changedText)).toBe(false)
      expect(
        messages.some((message) => message.parts.some((part) => part.type === "text" && part.text === changedText)),
      ).toBe(false)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(2)
    }),
  60_000,
)

it.instance(
  "preserves an initially-busy exact marked same-ID retry with direct custody",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({
        title: "Busy exact same-ID retry",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* writeText(path.join(dir, "round20-exact-id.txt"), "root tool")
      const messageID = MessageID.ascending()
      const markedText = "[fm-from-firstmate]\x1f exact busy machine mail"
      const input = {
        sessionID: session.id,
        messageID,
        agent: "build",
        model: ref,
        noReply: true,
        parts: said(markedText),
      }
      const original = yield* prompt.prompt(input)
      if (original.info.role !== "user") throw new Error("expected the original marked message")
      const root = yield* seedUser({
        sessionID: session.id,
        agent: "build",
        model: ref,
        parts: said("busy exact retry root"),
      })
      const toolResponse = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() => Deferred.succeed(toolResponse, undefined).pipe(Effect.ignore))
      yield* llm.push(
        reply().wait(deferredAsPromise(toolResponse)).tool("glob", { pattern: "round20-exact-id.txt" }).item(),
        reply().text("root task finished").stop().item(),
        reply().text("unexpected duplicate exact retry").stop().item(),
      )
      const active = yield* prompt.loop({ sessionID: session.id, messageID: root.info.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "active root did not reach the provider", "10 seconds")

      const retry = yield* prompt.prompt(input)
      expect(retry).toEqual(original)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(1)
      yield* Deferred.succeed(toolResponse, undefined)
      yield* awaitWithTimeout(llm.wait(2), "active root continuation did not start", "10 seconds")
      const continuation = (yield* llm.inputs)[1]
      if (!continuation) throw new Error("expected the active root continuation")
      expect(JSON.stringify(continuation.messages)).toContain("exact busy machine mail")
      const exit = yield* awaitWithTimeout(Fiber.await(active), "active root did not finish", "10 seconds")
      expect(Exit.isSuccess(exit)).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(2)
      const messages = yield* sessions.messages({ sessionID: session.id })
      expect(messages.filter((message) => message.info.role === "user" && message.info.id === messageID)).toHaveLength(
        1,
      )
      expect(
        messages.filter((message) => message.info.role === "assistant" && message.info.parentID === messageID),
      ).toHaveLength(0)
    }),
  60_000,
)

gated.instance(
  "represented changed marked identity is refused before idle-write race gates",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Ambiguous identity before write race" })
      const messageID = MessageID.ascending()
      const originalText = "[fm-from-firstmate]\x1f original marker without custody"
      const input = {
        sessionID: session.id,
        messageID,
        agent: "build",
        model: ref,
        noReply: true,
        parts: said(originalText),
      }
      yield* prompt.prompt(input)
      const before = yield* sessions.messages({ sessionID: session.id })
      const admission = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      const write = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      gates.noReplyAdmission = admission
      gates.noReplyWrite = write
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          gates.noReplyAdmission = undefined
          gates.noReplyWrite = undefined
        }).pipe(
          Effect.andThen(Deferred.succeed(admission.release, undefined)),
          Effect.andThen(Deferred.succeed(write.release, undefined)),
          Effect.asVoid,
        ),
      )
      const exit = yield* prompt
        .prompt({ ...input, parts: said("[fm-from-firstmate]\x1f conflicting retry") })
        .pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) throw new Error("ambiguous changed identity was written")
      expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionQueue.HistoricalIdentityError)
      expect(yield* Deferred.isDone(admission.entered)).toBe(false)
      expect(yield* Deferred.isDone(write.entered)).toBe(false)
      expect(yield* sessions.messages({ sessionID: session.id })).toEqual(before)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(0)
    }),
  30000,
)

it.instance(
  "keeps an idle marked noReply out of a gate-free root-pinned tool continuation",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({
        title: "Gate-free root-pinned noReply continuation",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const toolPath = path.join(dir, "round18-gate-free-root.txt")
      yield* writeText(toolPath, "root-pinned tool continuation")
      const root = yield* seedUser({
        sessionID: session.id,
        agent: "build",
        model: ref,
        parts: said("gate-free root-pinned task"),
      })
      const messageID = MessageID.ascending()
      const markedText = "[fm-from-firstmate]\x1f gate-free rootful machine mail"
      const markerContent = "gate-free rootful machine mail"
      const direct = yield* prompt.prompt({
        sessionID: session.id,
        messageID,
        agent: "build",
        model: ref,
        noReply: true,
        parts: said(markedText),
      })
      if (direct.info.role !== "user") throw new Error("expected the direct idle noReply user")
      expect(direct.info.noReply).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(0)
      const config = yield* Config.Service
      const markers = (yield* config.get()).machine_message_markers
      expect(MachineMessage.classify(markedText, markers)).toBe("hold")
      const history = yield* MessageV2.snapshot(session.id)
      expect(history.admissionOrder.get(root.info.id)).toBeLessThan(history.admissionOrder.get(messageID) ?? Infinity)
      const persisted = history.messages.find((message) => message.info.id === messageID)
      if (!persisted || persisted.info.role !== "user") throw new Error("expected the marked noReply history row")
      expect(persisted.info.noReply).toBe(true)
      expect(
        MachineMessage.classify(
          persisted.parts
            .flatMap((part) => (part.type === "text" && part.synthetic !== true ? [part.text] : []))
            .join(""),
          markers,
        ),
      ).toBe("hold")

      yield* llm.push(
        reply().tool("glob", { pattern: "round18-gate-free-root.txt" }).item(),
        reply().text("root-pinned task finished").stop().item(),
      )
      const run = yield* prompt.loop({ sessionID: session.id, messageID: root.info.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "gate-free root-pinned provider request did not start", "10 seconds")
      const first = (yield* llm.inputs)[0]
      if (!first) throw new Error("expected the gate-free root-pinned first request")
      expect(JSON.stringify(first.messages)).not.toContain(markerContent)

      yield* awaitWithTimeout(llm.wait(2), "gate-free root-pinned continuation did not start", "10 seconds")
      const continuation = (yield* llm.inputs)[1]
      if (!continuation) throw new Error("expected the gate-free root-pinned continuation")
      expect(JSON.stringify(continuation.messages)).not.toContain(markerContent)
      const exit = yield* awaitWithTimeout(Fiber.await(run), "gate-free root-pinned run did not finish", "10 seconds")
      expect(Exit.isSuccess(exit)).toBe(true)

      const messages = yield* sessions.messages({ sessionID: session.id })
      const retained = messages.find((message) => message.info.role === "user" && message.info.id === messageID)
      if (!retained || retained.info.role !== "user")
        throw new Error("expected the idle noReply row to remain persisted")
      expect(retained.info.noReply).toBe(true)
      expect(messages.some((message) => message.info.role === "assistant" && message.info.parentID === messageID)).toBe(
        false,
      )
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* llm.calls).toBe(2)
    }),
  60_000,
)

it.instance("keeps message history and admission order coherent across revert cleanup", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const revert = yield* SessionRevert.Service
    const session = yield* sessions.create({ title: "Revert history read race" })
    const kept = yield* seedUser({
      sessionID: session.id,
      agent: "build",
      model: ref,
      parts: said("kept message"),
    })
    const removed = yield* seedUser({
      sessionID: session.id,
      agent: "build",
      model: ref,
      parts: said("reverted message"),
    })
    yield* sessions.setRevert({
      sessionID: session.id,
      revert: { messageID: removed.info.id },
      summary: { additions: 0, deletions: 0, files: 0 },
    })

    const info = yield* sessions.get(session.id)
    const snapshot = yield* MessageV2.snapshot(session.id, () => revert.cleanup(info))
    expect(
      MessageV2.latest(snapshot.messages, { admissionOrder: snapshot.admissionOrder, excludeNoReply: true }).user?.id,
    ).toBe(removed.info.id)

    const afterCleanup = yield* MessageV2.snapshot(session.id)
    expect(afterCleanup.messages.map((message) => message.info.id)).toEqual([kept.info.id])
    expect(
      MessageV2.latest(afterCleanup.messages, {
        admissionOrder: afterCleanup.admissionOrder,
        excludeNoReply: true,
      }).user?.id,
    ).toBe(kept.info.id)

      yield* llm.text("remaining message processed")
      const result = yield* prompt.loop({ sessionID: session.id })
      expect(result.info.role).toBe("assistant")
      expect(result.parts.some((part) => part.type === "text" && part.text === "remaining message processed")).toBe(
        true,
      )
    }),
  30_000,
)

it.instance(
  "a queued prompt with an older supplied messageID is stored after the reply it waited for",
  () =>
    Effect.gen(function* () {
      const stale = MessageID.ascending()
      const { llm, sessions, chat, task, send, release } = yield* startHeld()
      yield* llm.text("queued done")

      const held = yield* send("supplied an old id", { delivery: "queue", messageID: stale })
      yield* admitted(chat.id, "supplied an old id")
      yield* release
      yield* finish(task, held)

      const msgs = yield* sessions.messages({ sessionID: chat.id })
      const reply = msgs.find((msg) => msg.info.role === "assistant")
      const promoted = msgs.find((msg) =>
        msg.parts.some((part) => part.type === "text" && part.text === "supplied an old id"),
      )
      if (!reply || !promoted) throw new Error("expected the first reply and the promoted prompt")
      expect(promoted.info.id).not.toBe(stale)
      expect(promoted.info.id > reply.info.id).toBe(true)
      const answer = msgs.at(-1)?.info
      expect(answer?.role === "assistant" ? answer.parentID : undefined).toBe(promoted.info.id)
    }),
  15_000,
)

it.instance("assertNotBusy fails with BusyError when loop running", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const run = yield* SessionRunState.Service
    const sessions = yield* Session.Service
    yield* llm.hang

    const chat = yield* sessions.create({})
    yield* user(chat.id, "hi")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)

    const exit = yield* run.assertNotBusy(chat.id).pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.BusyError)
      expect(Cause.squash(exit.cause)).toMatchObject({ _tag: "SessionBusyError", sessionID: chat.id })
    }

    yield* prompt.cancel(chat.id)
    yield* Fiber.await(fiber)
  }),
)

noLLMServer.instance("assertNotBusy succeeds when idle", () =>
  Effect.gen(function* () {
    const run = yield* SessionRunState.Service
    const sessions = yield* Session.Service

    const chat = yield* sessions.create({})
    const exit = yield* run.assertNotBusy(chat.id).pipe(Effect.exit)
    expect(Exit.isSuccess(exit)).toBe(true)
  }),
)

// Shell semantics

const shellWriteIntervalMs = 500
const shellPreviewByteLimit = 32 * 1024

function userShellEvents(sessionID: SessionID) {
  return Effect.gen(function* () {
    const database = yield* Database.Service
    const rows = yield* database.db
      .select({ data: EventTable.data })
      .from(EventTable)
      .where(
        and(
          eq(EventTable.aggregate_id, sessionID),
          eq(
            EventTable.type,
            EventV2.versionedType(SessionV1.Event.PartUpdated.type, SessionV1.Event.PartUpdated.durable!.version),
          ),
        ),
      )
      .orderBy(asc(EventTable.seq))
      .all()
      .pipe(Effect.orDie)
    return rows.flatMap((row) => {
      const data = Schema.decodeUnknownOption(SessionV1.Event.PartUpdated.data)(row.data)
      if (Option.isNone(data) || data.value.part.type !== "tool") return []
      return [{ part: data.value.part, time: data.value.time, bytes: Buffer.byteLength(JSON.stringify(row.data), "utf8") }]
    })
  })
}

function userShellOutput(chunks: { text: string; time: number }[]) {
  return chunks.map((chunk) => chunk.text).join("")
}

function expectUserShellSettlement(sessionID: SessionID, expected: string) {
  return Effect.gen(function* () {
    const sessions = yield* Session.Service
    const part = (yield* userShellEvents(sessionID)).at(-1)?.part
    expect(part?.state.status).toBe("completed")
    if (!part || part.state.status !== "completed") return
    expect(part.state.output).toBe(expected)
    expect(part.state.metadata.output).toBe(expected)
    const stored = yield* sessions.getPart({ sessionID, messageID: part.messageID, partID: part.id })
    expect(stored?.type).toBe("tool")
    if (stored?.type !== "tool" || stored.state.status !== "completed") return
    expect(stored.state.output).toBe(expected)
    expect(stored.state.metadata.output).toBe(expected)
  })
}

function holdUserShellPartUpdate(contains?: string) {
  return Effect.gen(function* () {
    const gate = {
      entered: yield* Deferred.make<void>(),
      release: yield* Deferred.make<void>(),
      written: yield* Deferred.make<void>(),
      held: false,
      ...(contains ? { contains } : {}),
    }
    gates.userShellPartUpdate = gate
    yield* Effect.addFinalizer(() =>
      Deferred.succeed(gate.release, undefined).pipe(
        Effect.andThen(
          Effect.sync(() => {
            if (gates.userShellPartUpdate === gate) gates.userShellPartUpdate = undefined
          }),
        ),
      ),
    )
    return gate
  })
}

it.instance("shell rejects with BusyError when loop running", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.hang
    yield* user(chat.id, "hi")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)

    const exit = yield* prompt.shell({ sessionID: chat.id, agent: "build", command: "echo hi" }).pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.BusyError)
      expect(Cause.squash(exit.cause)).toMatchObject({ _tag: "SessionBusyError", sessionID: chat.id })
    }

    yield* prompt.cancel(chat.id)
    yield* Fiber.await(fiber)
  }),
)

unixNoLLMServer(
  "shell captures stdout and stderr in completed tool output",
  () =>
    Effect.gen(function* () {
      const { prompt, run, chat } = yield* boot()
      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "printf out && printf err >&2",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.output).toContain("out")
      expect(tool.state.output).toContain("err")
      expect(tool.state.metadata.output).toContain("out")
      expect(tool.state.metadata.output).toContain("err")
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell completes a fast command on the preferred shell",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const { prompt, run, chat } = yield* boot()
      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "pwd",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.input.command).toBe("pwd")
      expect(tool.state.output).toContain(dir)
      expect(tool.state.metadata.output).toContain(dir)
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell uses configured shell over env shell",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        if (!(yield* hasBash)) return

        const { prompt, chat } = yield* boot()
        const result = yield* prompt.shell({
          sessionID: chat.id,
          agent: "build",
          command: "[[ 1 -eq 1 ]] && printf configured",
        })

        const tool = completedTool(result.parts)
        if (!tool) return
        expect(tool.state.output).toContain("configured")
      }),
    ),
  { config: { ...cfg, shell: "bash" } },
  30_000,
)

unixNoLLMServer(
  "shell commands can change directory after startup",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { directory: dir } = yield* TestInstance
        const { prompt, run, chat } = yield* boot()
        const parent = path.dirname(dir)
        const result = yield* prompt.shell({
          sessionID: chat.id,
          agent: "build",
          command: "cd .. && pwd",
        })

        expect(result.info.role).toBe("assistant")
        const tool = completedTool(result.parts)
        if (!tool) return

        expect(tool.state.output).toContain(parent)
        expect(tool.state.metadata.output).toContain(parent)
        yield* run.assertNotBusy(chat.id)
      }),
    ),
  { config: cfg },
)

unixNoLLMServer(
  "shell lists files from the project directory",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const { prompt, run, chat } = yield* boot()
      yield* writeText(path.join(dir, "README.md"), "# e2e\n")

      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "command ls",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.input.command).toBe("command ls")
      expect(tool.state.output).toContain("README.md")
      expect(tool.state.metadata.output).toContain("README.md")
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell captures stderr from a failing command",
  () =>
    Effect.gen(function* () {
      const { prompt, run, chat } = yield* boot()
      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "command -v __nonexistent_cmd_e2e__ || echo 'not found' >&2; exit 1",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.output).toContain("not found")
      expect(tool.state.metadata.output).toContain("not found")
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell updates running metadata before process exit",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, chat } = yield* boot()

        const fiber = yield* prompt
          .shell({ sessionID: chat.id, agent: "build", command: "printf first && sleep 1 && printf second" })
          .pipe(Effect.forkChild)

        yield* pollWithTimeout(
          Effect.gen(function* () {
            const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
            const taskMsg = msgs.find((item) => item.info.role === "assistant")
            const tool = taskMsg ? toolPart(taskMsg.parts) : undefined
            if (tool?.state.status === "running" && tool.state.metadata?.output.includes("first")) return true
            return undefined
          }),
          "timed out waiting for running shell metadata",
        )

        const exit = yield* Fiber.await(fiber)
        expect(Exit.isSuccess(exit)).toBe(true)
      }),
    ),
  { config: cfg },
  30_000,
)

unixUserShell(
  "shell persists a coalesced quiet tail while running and keeps settlement final",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        userShellChunks.length = 0
        userShellRunning.length = 0
        const { prompt, sessions, chat } = yield* boot()
        const command = `${process.execPath} -e 'for(let i=1;i<=8;i++){process.stdout.write("burst-"+i+"\\n");await new Promise(r=>setTimeout(r,20))}await new Promise(r=>setTimeout(r,1500))'`
        const fiber = yield* prompt.shell({ sessionID: chat.id, agent: "build", command }).pipe(Effect.forkChild)
        yield* waitForBusy(chat.id)
        yield* pollWithTimeout(
          Effect.sync(() => (userShellOutput(userShellChunks).includes("burst-8\n") ? true : undefined)),
          "user shell did not consume the quiet-tail marker",
        )

        const expected = Array.from({ length: 8 }, (_, index) => `burst-${index + 1}\n`).join("")
        const chunks = [...userShellChunks]
        expect(chunks.length).toBeGreaterThan(1)
        expect(userShellOutput(chunks)).toBe(expected)
        const span = chunks.at(-1)!.time - chunks[0].time
        expect(span).toBeLessThan(shellWriteIntervalMs)
        expect(chunks.length).toBeGreaterThan(Math.ceil(span / shellWriteIntervalMs) + 1)

        const visible = yield* pollWithTimeout(
          Effect.gen(function* () {
            const writes = yield* userShellEvents(chat.id)
            const write = writes.findLast(
              (item) => item.part.state.status === "running" && item.part.state.metadata?.output === expected,
            )
            return write ? { write, observedAt: Date.now() } : undefined
          }),
          "quiet user-shell output did not reach the committed running part",
          "2 seconds",
        )
        expect(visible.write.part.state.status).toBe("running")
        expect(visible.observedAt - chunks.at(-1)!.time).toBeLessThanOrEqual(shellWriteIntervalMs + 750)
        const status = yield* SessionStatus.Service
        expect((yield* status.get(chat.id)).type).toBe("busy")

        const exit = yield* Fiber.await(fiber)
        expect(Exit.isSuccess(exit)).toBe(true)
        if (!Exit.isSuccess(exit)) return
        const part = toolPart(exit.value.parts)
        expect(part?.state.status).toBe("completed")
        if (!part || part.state.status !== "completed") return
        expect(part.state.output).toBe(expected)
        expect(part.state.metadata.output).toBe(expected)

        const settled = yield* sessions.getPart({ sessionID: chat.id, messageID: part.messageID, partID: part.id })
        expect(settled?.type).toBe("tool")
        if (settled?.type !== "tool" || settled.state.status !== "completed") return
        expect(settled.state.output).toBe(expected)
        expect(settled.state.metadata.output).toBe(expected)

        const committed = (yield* userShellEvents(chat.id)).filter((item) => item.part.id === part.id)
        const running = committed.filter(
          (item) => item.part.state.status === "running" && item.part.state.metadata?.output,
        )
        expect(running.length).toBeLessThanOrEqual(Math.ceil(span / shellWriteIntervalMs) + 1)
        expect(
          running.some((item) => item.part.state.status === "running" && item.part.state.metadata?.output === expected),
        ).toBe(true)
        expect(committed.at(-1)?.part.state.status).toBe("completed")
        expect(userShellRunning).toHaveLength(1)
        expect(yield* userShellRunning[0]).toBe(false)

        yield* Effect.sleep(shellWriteIntervalMs + 250)
        const afterThrottle = (yield* userShellEvents(chat.id)).filter((item) => item.part.id === part.id)
        expect(afterThrottle).toHaveLength(committed.length)
        expect(afterThrottle.at(-1)?.part.state.status).toBe("completed")
        const stored = yield* sessions.getPart({ sessionID: chat.id, messageID: part.messageID, partID: part.id })
        expect(stored).toMatchObject({ state: { status: "completed", output: expected, metadata: { output: expected } } })
      }),
    ),
  { config: cfg },
  30_000,
)

unixUserShell(
  "shell settles before its pending running snapshot and does not write after settlement",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        userShellChunks.length = 0
        userShellRunning.length = 0
        const { prompt, sessions, chat } = yield* boot()
        const expected = "fast-settlement-tail\n"
        const result = yield* prompt.shell({
          sessionID: chat.id,
          agent: "build",
          command: "printf 'fast-settlement-tail\\n'; sleep 0.1",
        })
        expect(userShellOutput(userShellChunks)).toBe(expected)
        const part = toolPart(result.parts)
        expect(part?.state.status).toBe("completed")
        if (!part || part.state.status !== "completed") return
        const committed = (yield* userShellEvents(chat.id)).filter((item) => item.part.id === part.id)
        const snapshots = committed.filter(
          (item) => item.part.state.status === "running" && item.part.state.metadata?.output,
        )
        const settlement = committed.filter((item) => item.part.state.status === "completed")
        expect(snapshots).toHaveLength(0)
        expect(settlement).toHaveLength(1)
        expect(settlement[0].time - userShellChunks.at(-1)!.time).toBeLessThan(shellWriteIntervalMs)
        const settled = settlement[0]?.part
        expect(settled?.state.status).toBe("completed")
        if (settled?.state.status === "completed") expect(settled.state.output).toBe(expected)

        yield* Effect.sleep(shellWriteIntervalMs + 250)
        const afterThrottle = (yield* userShellEvents(chat.id)).filter((item) => item.part.id === part.id)
        expect(afterThrottle).toHaveLength(committed.length)
        expect(afterThrottle.at(-1)?.part.state.status).toBe("completed")
        const stored = yield* sessions.getPart({ sessionID: chat.id, messageID: part.messageID, partID: part.id })
        expect(stored).toMatchObject({ state: { status: "completed", output: expected, metadata: { output: expected } } })
      }),
    ),
  { config: cfg },
  30_000,
)

unixUserShellOrdering(
  "shell serializes scheduled writes so a delayed older snapshot cannot follow a newer one",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        userShellChunks.length = 0
        userShellRunning.length = 0
        const gate = yield* holdUserShellPartUpdate()
        const { prompt, sessions, chat } = yield* boot()
        const lines = Array.from({ length: 60 }, (_, index) => `line-${String(index).padStart(2, "0")}:${"x".repeat(128)}\n`)
        const expected = lines.join("")
        const command = `${process.execPath} -e 'for(let i=0;i<60;i++){process.stdout.write("line-"+String(i).padStart(2,"0")+":"+"x".repeat(128)+"\\n");await new Promise(r=>setTimeout(r,50))}'`
        const fiber = yield* prompt.shell({ sessionID: chat.id, agent: "build", command }).pipe(Effect.forkChild)
        yield* waitForBusy(chat.id)
        yield* awaitWithTimeout(
          Deferred.await(gate.entered),
          "first running snapshot did not enter the held commit",
          "5 seconds",
        )
        const beforeRelease = yield* userShellEvents(chat.id)
        const initial = beforeRelease.find((item) => item.part.state.status === "running")
        expect(initial).toBeDefined()
        if (!initial) return

        yield* Effect.sleep(shellWriteIntervalMs + 250)
        expect(userShellOutput(userShellChunks)).toContain("line-10:")
        const whileHeld = (yield* userShellEvents(chat.id)).filter((item) => item.part.id === initial.part.id)
        const outstanding = whileHeld.filter(
          (item) => item.part.state.status === "running" && item.part.state.metadata?.output,
        )
        yield* Deferred.succeed(gate.release, undefined)
        expect(outstanding).toHaveLength(0)

        const exit = yield* Fiber.await(fiber)
        expect(Exit.isSuccess(exit)).toBe(true)
        if (!Exit.isSuccess(exit)) return
        const part = toolPart(exit.value.parts)
        expect(part?.state.status).toBe("completed")
        if (!part || part.state.status !== "completed") return
        expect(userShellOutput(userShellChunks)).toBe(expected)
        expect(part.state.output).toBe(expected)
        expect(part.state.metadata.output).toBe(expected)
        const committed = (yield* userShellEvents(chat.id)).filter((item) => item.part.id === part.id)
        const snapshots = committed.filter(
          (item) => item.part.state.status === "running" && item.part.state.metadata?.output,
        )
        expect(snapshots.length).toBeGreaterThanOrEqual(2)
        snapshots.slice(1).forEach((item, index) => expect(item.time).toBeGreaterThanOrEqual(snapshots[index].time))
        const stored = yield* sessions.getPart({ sessionID: chat.id, messageID: part.messageID, partID: part.id })
        expect(stored).toMatchObject({ state: { status: "completed", output: expected, metadata: { output: expected } } })
      }),
    ),
  { config: cfg },
  30_000,
)

unixUserShell(
  "shell bounds durable running writes and bytes for a sustained stream",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        userShellChunks.length = 0
        userShellRunning.length = 0
        const { prompt, sessions, chat } = yield* boot()
        const lines = Array.from({ length: 128 }, (_, index) => `${String(index).padStart(3, "0")}:${"x🌱".repeat(817)}\n`)
        const expected = lines.join("")
        const command = `${process.execPath} -e 'for(let i=0;i<128;i++){process.stdout.write(String(i).padStart(3,"0")+":"+"x🌱".repeat(817)+"\\n");await new Promise(r=>setTimeout(r,20))}'`
        const result = yield* prompt.shell({ sessionID: chat.id, agent: "build", command })
        const chunks = [...userShellChunks]
        expect(userShellOutput(chunks)).toBe(expected)
        const span = chunks.at(-1)!.time - chunks[0].time
        const allowedRunningWrites = Math.ceil(span / shellWriteIntervalMs) + 1
        expect(chunks.length).toBeGreaterThan(allowedRunningWrites)

        const part = toolPart(result.parts)
        expect(part?.state.status).toBe("completed")
        if (!part || part.state.status !== "completed") return
        expect(part.state.output).toBe(expected)
        expect(part.state.metadata.output).toBe(expected)

        const committed = (yield* userShellEvents(chat.id)).filter((item) => item.part.id === part.id)
        const initialization = committed.filter(
          (item) => item.part.state.status === "running" && !item.part.state.metadata?.output,
        )
        const snapshots = committed.filter(
          (item) => item.part.state.status === "running" && item.part.state.metadata?.output,
        )
        const settlement = committed.filter((item) => item.part.state.status === "completed")
        expect(initialization).toHaveLength(1)
        expect(snapshots.length).toBeGreaterThan(0)
        expect(snapshots.length).toBeLessThanOrEqual(allowedRunningWrites)
        expect(settlement).toHaveLength(1)

        const persistedBytes = committed.reduce((total, item) => total + item.bytes, 0)
        expect(settlement[0].bytes).toBeGreaterThanOrEqual(Buffer.byteLength(expected, "utf8"))
        expect(persistedBytes).toBeLessThan(settlement[0].bytes * 2)
        snapshots.forEach((item, index) => {
          if (index > 0) expect(item.time).toBeGreaterThanOrEqual(snapshots[index - 1].time)
          const consumed = chunks.filter((chunk) => chunk.time <= item.time).map((chunk) => chunk.text).join("")
          const output = String(item.part.state.status === "running" ? item.part.state.metadata?.output ?? "" : "")
          expect(Buffer.byteLength(output, "utf8")).toBeLessThanOrEqual(shellPreviewByteLimit)
          expect(Buffer.from(output, "utf8").toString("utf8")).toBe(output)
          expect(consumed.endsWith(output.slice(-128))).toBe(true)
          expect(item.bytes).toBeLessThanOrEqual(shellPreviewByteLimit + 1024)
        })

        const stored = yield* sessions.getPart({ sessionID: chat.id, messageID: part.messageID, partID: part.id })
        expect(stored).toMatchObject({ state: { status: "completed", output: expected, metadata: { output: expected } } })
        expect(userShellRunning).toHaveLength(1)
        expect(yield* userShellRunning[0]).toBe(false)
      }),
    ),
  { config: cfg },
  30_000,
)

unixUserShell(
  "shell settles complete output after nonzero exit and execution failure",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const payload = " ".repeat(40_960)
        userShellChunks.length = 0
        userShellRunning.length = 0
        const { prompt, chat } = yield* boot()
        const nonzeroOutput = `${payload}nonzero-tail\n`
        const nonzero = yield* prompt.shell({
          sessionID: chat.id,
          agent: "build",
          command: "printf '%40960s' ''; printf 'nonzero-tail\\n'; exit 7",
        })
        expect(userShellOutput(userShellChunks)).toBe(nonzeroOutput)
        const completed = completedTool(nonzero.parts)
        expect(completed?.state.output).toBe(nonzeroOutput)
        expect(completed?.state.metadata.output).toBe(nonzeroOutput)
        yield* expectUserShellSettlement(chat.id, nonzeroOutput)
        expect(userShellRunning).toHaveLength(1)
        expect(yield* userShellRunning[0]).toBe(false)

        userShellChunks.length = 0
        userShellRunning.length = 0
        const { chat: failedChat, prompt: failedPrompt } = yield* boot()
        const failedOutput = `${payload}failure-tail\n`
        const failure = yield* failedPrompt
          .shell({
            sessionID: failedChat.id,
            agent: "build",
            command: "printf '%40960s' ''; printf 'failure-tail\\n'; kill -TERM $$",
          })
          .pipe(Effect.exit)
        expect(Exit.isFailure(failure)).toBe(true)
        expect(userShellOutput(userShellChunks)).toBe(failedOutput)
        yield* expectUserShellSettlement(failedChat.id, failedOutput)
        expect(userShellRunning).toHaveLength(1)
        expect(yield* userShellRunning[0]).toBe(false)
      }),
    ),
  { config: cfg },
  30_000,
)

unixUserShell(
  "shell settles complete output after an Effect timeout",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        userShellChunks.length = 0
        userShellRunning.length = 0
        const { prompt, chat } = yield* boot()
        const timeoutCommand = `${process.execPath} -e 'for(;;){process.stdout.write("t".repeat(4096));await new Promise(r=>setTimeout(r,100))}'`
        const timeoutFiber = yield* prompt
          .shell({
            sessionID: chat.id,
            agent: "build",
            command: timeoutCommand,
          })
          .pipe(Effect.timeout("4 seconds"), Effect.forkChild)
        yield* waitForBusy(chat.id)
        const timeoutExit = yield* Fiber.await(timeoutFiber)
        expect(Exit.isFailure(timeoutExit)).toBe(true)
        const timeoutOutput = userShellOutput(userShellChunks)
        expect(timeoutOutput.length).toBeGreaterThan(shellPreviewByteLimit)
        expect(timeoutOutput).toBe("t".repeat(timeoutOutput.length))
        expect(userShellOutput(userShellChunks)).toBe(timeoutOutput)
        const timeoutAnnotation = `${timeoutOutput}\n\n<metadata>\nUser aborted the command\n</metadata>`
        yield* expectUserShellSettlement(chat.id, timeoutAnnotation)
        const status = yield* SessionStatus.Service
        expect((yield* status.get(chat.id)).type).toBe("idle")
        expect(userShellRunning).toHaveLength(1)
        expect(yield* userShellRunning[0]).toBe(false)
      }),
    ),
  { config: cfg },
  30_000,
)

unixUserShellOrdering(
  "shell waits for a held running commit before normal exit and explicit cancellation settle",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const payload = " ".repeat(40_960)
        userShellChunks.length = 0
        userShellRunning.length = 0
        const normalGate = yield* holdUserShellPartUpdate("normal-tail")
        const { prompt, chat } = yield* boot()
        const normalOutput = `normal-start\n${payload}normal-tail\n`
        const normalFiber = yield* prompt
          .shell({
            sessionID: chat.id,
            agent: "build",
            command: "printf 'normal-start\\n'; sleep 0.1; printf '%40960s' ''; printf 'normal-tail\\n'; sleep 1",
          })
          .pipe(Effect.forkChild)
        yield* waitForBusy(chat.id)
        yield* awaitWithTimeout(Deferred.await(normalGate.entered), "normal output commit did not enter the hold", "5 seconds")
        yield* pollWithTimeout(
          Effect.gen(function* () {
            return userShellRunning.length === 1 && !(yield* userShellRunning[0]) ? true : undefined
          }),
          "normal shell process did not exit while its running commit was held",
          "5 seconds",
        )
        expect(userShellOutput(userShellChunks)).toBe(normalOutput)
        expect(userShellChunks.length).toBeGreaterThan(1)
        yield* Deferred.succeed(normalGate.release, undefined)
        yield* awaitWithTimeout(Deferred.await(normalGate.written), "held normal running commit did not finish", "5 seconds")
        const normalExit = yield* Fiber.await(normalFiber)
        expect(Exit.isSuccess(normalExit)).toBe(true)
        yield* expectUserShellSettlement(chat.id, normalOutput)
        expect(userShellRunning).toHaveLength(1)
        expect(yield* userShellRunning[0]).toBe(false)

        gates.userShellPartUpdate = undefined
        userShellChunks.length = 0
        userShellRunning.length = 0
        const cancelGate = yield* holdUserShellPartUpdate("interrupt-tail")
        const { prompt: cancelPrompt, chat: cancelChat, run } = yield* boot()
        const cancelOutput = `interrupt-start\n${payload}interrupt-tail\n`
        const cancelShell = yield* cancelPrompt
          .shell({
            sessionID: cancelChat.id,
            agent: "build",
            command:
              "printf 'interrupt-start\\n'; sleep 0.1; printf '%40960s' ''; printf 'interrupt-tail\\n'; while :; do sleep 1; done",
          })
          .pipe(Effect.forkChild)
        yield* waitForBusy(cancelChat.id)
        yield* awaitWithTimeout(Deferred.await(cancelGate.entered), "cancel output commit did not enter the hold", "5 seconds")
        const beforeDelay = yield* userShellEvents(cancelChat.id)
        expect(
          beforeDelay.some(
            (item) =>
              item.part.state.status === "running" &&
              String(item.part.state.metadata?.output ?? "").includes("interrupt-tail"),
          ),
        ).toBe(false)
        yield* Effect.sleep("750 millis")
        const afterDelay = yield* userShellEvents(cancelChat.id)
        expect(
          afterDelay.some(
            (item) =>
              item.part.state.status === "running" &&
              String(item.part.state.metadata?.output ?? "").includes("interrupt-tail"),
          ),
        ).toBe(false)
        const cancelFiber = yield* cancelPrompt.cancel(cancelChat.id).pipe(Effect.forkChild({ startImmediately: true }))
        yield* pollWithTimeout(
          Effect.gen(function* () {
            const state = yield* run.current(cancelChat.id)
            return state?._tag === "Idle" ? true : undefined
          }),
          "explicit cancellation did not reach the shell runner",
          "5 seconds",
        )
        yield* Deferred.succeed(cancelGate.release, undefined)
        yield* awaitWithTimeout(Deferred.await(cancelGate.written), "held cancellation running commit did not finish", "5 seconds")
        const cancelExit = yield* Fiber.await(cancelFiber)
        expect(Exit.isSuccess(cancelExit)).toBe(true)
        const shellExit = yield* Fiber.await(cancelShell)
        expect(Exit.isSuccess(shellExit)).toBe(true)
        expect(userShellOutput(userShellChunks)).toBe(cancelOutput)
        expect(userShellChunks.length).toBeGreaterThan(1)
        const cancelAnnotation = `${cancelOutput}\n\n<metadata>\nUser aborted the command\n</metadata>`
        yield* expectUserShellSettlement(cancelChat.id, cancelAnnotation)
        expect(userShellRunning).toHaveLength(1)
        expect(yield* userShellRunning[0]).toBe(false)
        gates.userShellPartUpdate = undefined
      }),
    ),
  { config: cfg },
  30_000,
)

it.instance(
  "loop waits while shell runs and starts after shell exits",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.text("after-shell")

      const sh = yield* prompt
        .shell({ sessionID: chat.id, agent: "build", command: "sleep 0.2" })
        .pipe(Effect.forkChild)
      yield* waitForBusy(chat.id)

      const loop = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      expect(yield* llm.calls).toBe(0)

      yield* Fiber.await(sh)
      const exit = yield* Fiber.await(loop)

      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) {
        expect(exit.value.info.role).toBe("assistant")
        expect(exit.value.parts.some((part) => part.type === "text" && part.text === "after-shell")).toBe(true)
      }
      expect(yield* llm.calls).toBe(1)
    }),
  { git: true },
  10_000,
)

it.instance(
  "shell completion resumes queued loop callers",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.text("done")

      const sh = yield* prompt
        .shell({ sessionID: chat.id, agent: "build", command: "sleep 0.2" })
        .pipe(Effect.forkChild)
      yield* waitForBusy(chat.id)

      const a = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      const b = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      expect(yield* llm.calls).toBe(0)

      yield* Fiber.await(sh)
      const [ea, eb] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])

      expect(Exit.isSuccess(ea)).toBe(true)
      expect(Exit.isSuccess(eb)).toBe(true)
      if (Exit.isSuccess(ea) && Exit.isSuccess(eb)) {
        expect(ea.value.info.id).toBe(eb.value.info.id)
        expect(ea.value.info.role).toBe("assistant")
      }
      expect(yield* llm.calls).toBe(1)
    }),
  { git: true },
  10_000,
)

unix(
  "command ! expansion uses configured shell over env shell",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        if (!(yield* hasBash)) return
        const { llm } = yield* useServerConfig((url) => ({
          ...providerCfg(url),
          shell: "bash",
          command: {
            probe: {
              template: "Probe: !`[[ 1 -eq 1 ]] && printf configured`",
            },
          },
        }))

        const { prompt, chat } = yield* boot()
        yield* llm.text("done")

        const result = yield* prompt.command({
          sessionID: chat.id,
          command: "probe",
          arguments: "",
        })

        expect(result.info.role).toBe("assistant")
        const inputs = yield* llm.inputs
        expect(JSON.stringify(inputs.at(-1)?.messages)).toContain("configured")
      }),
    ),
  30_000,
)

unixNoLLMServer(
  "cancel interrupts shell and resolves cleanly",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, run, chat } = yield* boot()
        const { directory: dir } = yield* TestInstance
        const afs = yield* FSUtil.Service
        const ready = path.join(dir, ".shell-ready")

        const sh = yield* prompt
          .shell({ sessionID: chat.id, agent: "build", command: ": > '.shell-ready'; sleep 30" })
          .pipe(Effect.forkChild)
        yield* pollWithTimeout(
          afs.existsSafe(ready).pipe(Effect.map((exists) => (exists ? (true as const) : undefined))),
          "shell never created readiness marker",
        )

        yield* prompt.cancel(chat.id)

        const status = yield* SessionStatus.Service
        expect((yield* status.get(chat.id)).type).toBe("idle")
        const busy = yield* run.assertNotBusy(chat.id).pipe(Effect.exit)
        expect(Exit.isSuccess(busy)).toBe(true)

        const exit = yield* Fiber.await(sh)
        expect(Exit.isSuccess(exit)).toBe(true)
        if (Exit.isSuccess(exit)) {
          expect(exit.value.info.role).toBe("assistant")
          const tool = completedTool(exit.value.parts)
          if (tool) {
            expect(tool.state.output).toContain("User aborted the command")
          }
        }
      }),
    ),
  { git: true, config: cfg },
  30_000,
)

unixNoLLMServer(
  "cancel persists aborted shell result when shell ignores TERM",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, chat } = yield* boot()
        const { directory: dir } = yield* TestInstance
        const afs = yield* FSUtil.Service
        const ready = path.join(dir, ".trap-ready")

        const sh = yield* prompt
          .shell({
            sessionID: chat.id,
            agent: "build",
            // Touch marker AFTER trap installs so the test waits for the actual
            // ignore-TERM state before cancelling; otherwise SIGTERM can arrive
            // before `trap` runs and the escalation path is never exercised.
            command: `trap '' TERM; touch "${ready}"; sleep 30`,
          })
          .pipe(Effect.forkChild)

        yield* Effect.gen(function* () {
          while (!(yield* afs.existsSafe(ready))) {
            yield* Effect.sleep(Duration.millis(10))
          }
        }).pipe(Effect.timeout(Duration.seconds(5)))

        yield* prompt.cancel(chat.id)

        const exit = yield* Fiber.await(sh)
        expect(Exit.isSuccess(exit)).toBe(true)
        if (Exit.isSuccess(exit)) {
          expect(exit.value.info.role).toBe("assistant")
          const tool = completedTool(exit.value.parts)
          if (tool) {
            expect(tool.state.output).toContain("User aborted the command")
          }
        }
      }),
    ),
  { git: true, config: cfg },
  30_000,
)

unix(
  "cancel finalizes interrupted bash tool output through normal truncation",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Interrupted bash truncation",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })

      yield* seedUser({
        sessionID: chat.id,
        agent: "build",
        parts: [{ type: "text", text: "run bash" }],
      })

      yield* llm.tool("bash", {
        command:
          'i=0; while [ "$i" -lt 4000 ]; do printf "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx %05d\\n" "$i"; i=$((i + 1)); done; printf truncation-ready; sleep 30',
        timeout: 30_000,
        workdir: path.resolve(dir),
      })

      const run = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)
      yield* pollWithTimeout(
        Effect.gen(function* () {
          const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
          const assistant = msgs.findLast((item) => item.info.role === "assistant")
          const tool = assistant ? toolPart(assistant.parts) : undefined
          if (tool?.state.status === "running" && tool.state.metadata?.output.includes("truncation-ready")) return true
        }),
        "timed out waiting for truncated shell output",
      )
      yield* prompt.cancel(chat.id)

      const exit = yield* Fiber.await(run)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isFailure(exit)) return

      const tool = completedTool(exit.value.parts)
      if (!tool) return

      expect(tool.state.metadata.truncated).toBe(true)
      expect(typeof tool.state.metadata.outputPath).toBe("string")
      expect(tool.state.output).toMatch(/\.\.\.output truncated\.\.\./)
      expect(tool.state.output).toMatch(/Full output saved to:\s+\S+/)
      expect(tool.state.output).not.toContain("Tool execution aborted")
    }),
  { git: true },
  30_000,
)

unixNoLLMServer(
  "cancel interrupts loop queued behind shell",
  () =>
    Effect.gen(function* () {
      const { prompt, chat } = yield* boot()

      const sh = yield* prompt.shell({ sessionID: chat.id, agent: "build", command: "sleep 30" }).pipe(Effect.forkChild)
      yield* waitForBusy(chat.id)

      const loop = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      yield* prompt.cancel(chat.id)

      const exit = yield* Fiber.await(loop)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) {
        const tool = completedTool(exit.value.parts)
        expect(tool?.state.output).toContain("User aborted the command")
      }

      yield* Fiber.await(sh)
    }),
  { git: true, config: cfg },
  30_000,
)

shellQueuedLoopPrompt.instance(
  "shell-queued rootless compaction keeps its initiating root ahead of a later noReply input",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { directory: dir } = yield* TestInstance
        const { llm } = yield* useServerConfig(providerCfg)
        const db = (yield* Database.Service).db
        const prompt = yield* SessionPrompt.Service
        const compaction = yield* SessionCompaction.Service
        const sessions = yield* Session.Service
        const session = yield* sessions.create({ title: "Rootless compaction behind shell" })
        const releaseFile = path.join(dir, ".rootless-shell-release")
        yield* Effect.addFinalizer(() => writeText(releaseFile, "release").pipe(Effect.ignore))
        yield* Effect.addFinalizer(() => Effect.sync(() => shellQueuedLoopGate.release.resolve()))

        const history = yield* seedUser({
          sessionID: session.id,
          messageID: MessageID.make("msg_z_rootless_history"),
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "rootless compaction history" }],
        })
        if (history.info.role !== "user") throw new Error("expected the history message")
        yield* sessions.updateMessage({ ...history.info, time: { created: 300 } })

        const shell = yield* prompt
          .shell({
            sessionID: session.id,
            agent: "build",
            command: `while [ ! -f "${releaseFile}" ]; do sleep 0.01; done; printf 'round18-shell-persisted'`,
          })
          .pipe(Effect.forkChild)
        const persistedShell = yield* pollWithTimeout(
          Effect.gen(function* () {
            const messages = yield* sessions.messages({ sessionID: session.id })
            const user = messages.find(
              (message) =>
                message.info.role === "user" &&
                message.parts.some(
                  (part) =>
                    part.type === "text" &&
                    part.synthetic === true &&
                    part.text === "The following tool was executed by the user",
                ),
            )
            const tool = messages.find(
              (message) =>
                message.info.role === "assistant" &&
                message.parts.some((part) => part.type === "tool" && part.state.status === "running"),
            )
            if (!user || user.info.role !== "user" || !tool || tool.info.role !== "assistant") return undefined
            return { user, tool }
          }),
          "shell user/part and running tool were not persisted before compaction.create",
          "10 seconds",
        )
        const shellCreated = Date.now() + 60_000
        yield* sessions.updateMessage({
          ...persistedShell.user.info,
          time: { ...persistedShell.user.info.time, created: shellCreated },
        })
        yield* db
          .update(MessageTable)
          .set({ time_created: shellCreated })
          .where(eq(MessageTable.id, persistedShell.user.info.id))
          .run()
          .pipe(Effect.orDie)

        const rootMessageID = yield* compaction.create({
          sessionID: session.id,
          agent: "build",
          model: ref,
          auto: false,
        })
        const historySnapshot = yield* MessageV2.snapshot(session.id)
        const admission = historySnapshot.admissionOrder
        const shellUserOrder = admission.get(persistedShell.user.info.id)
        const shellToolOrder = admission.get(persistedShell.tool.info.id)
        const compactionOrder = admission.get(rootMessageID)
        if (shellUserOrder === undefined || shellToolOrder === undefined || compactionOrder === undefined)
          throw new Error("expected persisted admission order for shell and compaction messages")
        expect(shellUserOrder).toBeLessThan(compactionOrder)
        expect(shellToolOrder).toBeLessThan(compactionOrder)
        expect(
          MessageV2.latest(historySnapshot.messages, {
            admissionOrder: historySnapshot.admissionOrder,
            excludeNoReply: true,
          }).user?.id,
        ).toBe(rootMessageID)

        const loop = yield* prompt.loop({ sessionID: session.id }).pipe(Effect.forkChild)
        yield* llm.text("initiating compaction completed")
        yield* writeText(releaseFile, "release")
        yield* awaitWithTimeout(
          Effect.promise(() => shellQueuedLoopGate.entered.promise),
          "shell-queued rootless loop did not reach its first provider boundary",
          "10 seconds",
        )

        const lateMessageID = MessageID.make("msg_a_rootless_late_no_reply")
        const late = yield* prompt.prompt({
          sessionID: session.id,
          messageID: lateMessageID,
          agent: "build",
          model: ref,
          noReply: true,
          parts: [{ type: "text", text: "later noReply input" }],
        })
        if (late.info.role !== "user") throw new Error("expected the later user input")
        expect(late.info.noReply).toBe(true)
        yield* sessions.updateMessage({ ...late.info, time: { created: 100 } })
        const order = (yield* MessageV2.admission(session.id)).order
        expect(order.get(rootMessageID)).toBeLessThan(order.get(late.info.id) ?? Infinity)
        expect(yield* llm.inputs).toHaveLength(0)

        yield* Effect.sync(() => shellQueuedLoopGate.release.resolve())
        yield* awaitWithTimeout(llm.wait(1), "queued compaction did not reach the provider", "10 seconds")
        const request = (yield* llm.inputs)[0]
        if (!request) throw new Error("expected the queued compaction provider request")
        const providerHistory = JSON.stringify(request.messages)
        const positions = [
          providerHistory.indexOf("rootless compaction history"),
          providerHistory.indexOf("The following tool was executed by the user"),
          providerHistory.indexOf("round18-shell-persisted"),
        ]
        if (positions.some((position) => position < 0))
          throw new Error(`provider history sections were missing or reordered: ${positions.join(",")}`)
        expect(positions).toEqual([...positions].sort((a, b) => a - b))
        expect(providerHistory).not.toContain("later noReply input")

        const loopExit = yield* awaitWithTimeout(
          Fiber.await(loop),
          "queued compaction loop did not finish",
          "10 seconds",
        )
        expect(Exit.isSuccess(loopExit)).toBe(true)
        const shellExit = yield* awaitWithTimeout(Fiber.await(shell), "shell did not finish", "10 seconds")
        expect(Exit.isSuccess(shellExit)).toBe(true)
        const messages = yield* sessions.messages({ sessionID: session.id })
        const summary = messages.findLast(
          (message) => message.info.role === "assistant" && message.info.parentID === rootMessageID,
        )
        expect(summary?.info.role).toBe("assistant")
        expect(summary?.info.role === "assistant" ? summary.info.summary : undefined).toBe(true)
        expect(
          messages.some((message) => message.info.role === "assistant" && message.info.parentID === late.info.id),
        ).toBe(false)
        expect(yield* llm.calls).toBe(1)
      }),
    ),
  { git: true, config: cfg },
  60_000,
)

unix(
  "cancelling a queued loop leaves the next prompt runnable",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { llm } = yield* useServerConfig(providerCfg)
        const { directory: dir } = yield* TestInstance
        const prompt = yield* SessionPrompt.Service
        const sessions = yield* Session.Service
        const session = yield* sessions.create({ title: "Queued loop cancellation" })
        const root = yield* seedUser({
          sessionID: session.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "queued task" }],
        })
        // Each run appends its own pid, so the file names the real child and counts replays.
        const runs = path.join(dir, "shell-runs")
        const readRuns = Effect.promise(() =>
          Bun.file(runs)
            .text()
            .catch(() => ""),
        ).pipe(Effect.map((text) => text.split("\n").filter(Boolean).map(Number)))
        // Detached, so a cancel that never settles fails its own bound instead of stalling teardown.
        const shell = yield* prompt
          .shell({
            sessionID: session.id,
            agent: "build",
            command: `echo $$ >> '${runs}'; printf shell-ready; sleep 30`,
          })
          .pipe(Effect.forkDetach)
        yield* Effect.addFinalizer(() =>
          readRuns.pipe(Effect.map((pids) => pids.filter(alive).forEach((pid) => process.kill(-pid, "SIGKILL")))),
        )
        yield* pollWithTimeout(
          readRuns.pipe(Effect.map((pids) => (pids.length > 0 ? true : undefined))),
          "shell did not start",
          "30 seconds",
        )

        const queued = yield* prompt.loop({ sessionID: session.id, messageID: root.info.id }).pipe(Effect.forkDetach)
        yield* Effect.yieldNow
        yield* awaitWithTimeout(prompt.cancel(session.id), "cancel did not return", "15 seconds")
        const queuedExit = yield* awaitWithTimeout(Fiber.await(queued), "queued loop did not cancel", "30 seconds")
        const shellExit = yield* awaitWithTimeout(Fiber.await(shell), "shell did not cancel", "30 seconds")
        expect(Exit.isSuccess(queuedExit)).toBe(true)
        expect(Exit.isSuccess(shellExit)).toBe(true)
        expect(yield* llm.calls).toBe(0)
        const pids = yield* readRuns
        expect(pids).toHaveLength(1)
        expect(alive(pids[0])).toBe(false)
        const tool = (yield* sessions.messages({ sessionID: session.id }))
          .flatMap((message) => message.parts)
          .find((part) => part.type === "tool")
        expect(tool?.type === "tool" ? tool.state.status : undefined).toBe("completed")
        expect(tool?.type === "tool" && tool.state.status === "completed" ? tool.state.output : "").toContain(
          "User aborted the command",
        )

        const messageID = MessageID.make("msg_after_queued_cancel")
        yield* llm.text("fresh task finished")
        const next = yield* prompt
          .prompt({
            sessionID: session.id,
            messageID,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "fresh task after cancellation" }],
          })
          .pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(1), "the next prompt was poisoned by the cancelled queue", "30 seconds")
        const request = (yield* llm.inputs)[0]
        if (!request) throw new Error("expected the next provider request")
        expect(JSON.stringify(request.messages)).toContain("fresh task after cancellation")
        const nextExit = yield* awaitWithTimeout(Fiber.await(next), "the next prompt did not finish", "30 seconds")
        expect(Exit.isSuccess(nextExit)).toBe(true)
        expect(yield* llm.calls).toBe(1)
        expect(yield* readRuns).toEqual(pids)
      }),
    ),
  { git: true, config: cfg },
  60_000,
)

unixNoLLMServer(
  "shell rejects when another shell is already running",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, chat } = yield* boot()

        const a = yield* prompt
          .shell({ sessionID: chat.id, agent: "build", command: "sleep 30" })
          .pipe(Effect.forkChild)
        yield* waitForBusy(chat.id)

        const exit = yield* prompt.shell({ sessionID: chat.id, agent: "build", command: "echo hi" }).pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.BusyError)
        }

        yield* prompt.cancel(chat.id)
        yield* Fiber.await(a)
      }),
    ),
  { git: true, config: cfg },
  30_000,
)

// Abort signal propagation tests for inline tool execution

function hangUntilAborted(tool: { execute: (...args: any[]) => any }) {
  return Effect.gen(function* () {
    const ready = yield* Deferred.make<void>()
    const aborted = yield* Deferred.make<void>()
    const original = tool.execute
    tool.execute = (_args: any, ctx: any) => {
      ctx.abort.addEventListener("abort", () => succeedVoid(aborted), { once: true })
      if (ctx.abort.aborted) succeedVoid(aborted)
      succeedVoid(ready)
      return Effect.callback<never>(() => Effect.sync(() => succeedVoid(aborted)))
    }
    const restore = Effect.addFinalizer(() => Effect.sync(() => void (tool.execute = original)))
    return { ready, aborted, restore }
  })
}

noLLMServer.instance(
  "interrupt propagates abort signal to read tool via file part (text/plain)",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const registry = yield* ToolRegistry.Service
      const { read } = yield* registry.named()
      const { ready, restore } = yield* hangUntilAborted(read)
      yield* restore

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Abort Test" })

      const testFile = path.join(dir, "test.txt")
      yield* writeText(testFile, "hello world")

      const fiber = yield* prompt
        .prompt({
          sessionID: chat.id,
          agent: "build",
          parts: [
            { type: "text", text: "read this" },
            { type: "file", url: `file://${testFile}`, filename: "test.txt", mime: "text/plain" },
          ],
        })
        .pipe(Effect.forkChild)

      yield* awaitWithTimeout(Deferred.await(ready), "timed out waiting for read tool to start", "10 seconds")
      yield* prompt.cancel(chat.id)
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  { config: cfg },
  30_000,
)

noLLMServer.instance(
  "interrupt propagates abort signal to read tool via file part (directory)",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const registry = yield* ToolRegistry.Service
      const { read } = yield* registry.named()
      const { ready, restore } = yield* hangUntilAborted(read)
      yield* restore

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Abort Test" })

      const fiber = yield* prompt
        .prompt({
          sessionID: chat.id,
          agent: "build",
          parts: [
            { type: "text", text: "read this" },
            { type: "file", url: `file://${dir}`, filename: "dir", mime: "application/x-directory" },
          ],
        })
        .pipe(Effect.forkChild)

      yield* awaitWithTimeout(Deferred.await(ready), "timed out waiting for read tool to start", "10 seconds")
      yield* prompt.cancel(chat.id)
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  { config: cfg },
  30_000,
)

// Missing file handling

noLLMServer.instance(
  "does not fail the prompt when a file part is missing",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})

      const missing = path.join(dir, "does-not-exist.ts")
      const msg = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        parts: [
          { type: "text", text: "please review @does-not-exist.ts" },
          {
            type: "file",
            mime: "text/plain",
            url: `file://${missing}`,
            filename: "does-not-exist.ts",
          },
        ],
      })

      if (msg.info.role !== "user") throw new Error("expected user message")
      const hasFailure = msg.parts.some(
        (part) => part.type === "text" && part.synthetic && part.text.includes("Read tool failed to read"),
      )
      expect(hasFailure).toBe(true)

      yield* sessions.remove(session.id)
    }),
  { config: cfg },
)

noLLMServer.instance(
  "keeps stored part order stable when file resolution is async",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})

      const missing = path.join(dir, "still-missing.ts")
      const msg = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        parts: [
          {
            type: "file",
            mime: "text/plain",
            url: `file://${missing}`,
            filename: "still-missing.ts",
          },
          { type: "text", text: "after-file" },
        ],
      })

      if (msg.info.role !== "user") throw new Error("expected user message")

      const stored = yield* MessageV2.get({
        sessionID: session.id,
        messageID: msg.info.id,
      })
      const text = stored.parts.filter((part) => part.type === "text").map((part) => part.text)

      expect(text[0]?.startsWith("Called the Read tool with the following input:")).toBe(true)
      expect(text[1]?.includes("Read tool failed to read")).toBe(true)
      expect(text[2]).toBe("after-file")

      yield* sessions.remove(session.id)
    }),
  { config: cfg },
)

// Special characters in filenames

noLLMServer.instance(
  "handles filenames with # character",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      yield* writeText(path.join(dir, "file#name.txt"), "special content\n")

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const parts = yield* prompt.resolvePromptParts("Read @file#name.txt")
      const fileParts = parts.filter((part) => part.type === "file")

      expect(fileParts.length).toBe(1)
      expect(fileParts[0].filename).toBe("file#name.txt")
      expect(fileParts[0].url).toContain("%23")

      const decodedPath = fileURLToPath(fileParts[0].url)
      expect(decodedPath).toBe(path.join(dir, "file#name.txt"))

      const message = yield* prompt.prompt({
        sessionID: session.id,
        parts,
        noReply: true,
      })
      const stored = yield* MessageV2.get({ sessionID: session.id, messageID: message.info.id })
      const textParts = stored.parts.filter((part) => part.type === "text")
      const hasContent = textParts.some((part) => part.text.includes("special content"))
      expect(hasContent).toBe(true)

      yield* sessions.remove(session.id)
    }),
  { git: true, config: cfg },
)

// Regression: empty assistant turn loop

it.instance("does not loop empty assistant turns for a simple reply", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({ title: "Prompt regression" })

    yield* llm.text("packages/opencode/src/session/processor.ts")

    const result = yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      parts: [{ type: "text", text: "Where is SessionProcessor?" }],
    })

    expect(result.info.role).toBe("assistant")
    expect(result.parts.some((part) => part.type === "text" && part.text.includes("processor.ts"))).toBe(true)

    const msgs = yield* sessions.messages({ sessionID: session.id })
    expect(msgs.filter((msg) => msg.info.role === "assistant")).toHaveLength(1)
    expect(yield* llm.calls).toBe(1)
  }),
)

it.instance("records aborted errors when prompt is cancelled mid-stream", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({ title: "Prompt cancel regression" })

    yield* llm.hang

    const fiber = yield* prompt
      .prompt({
        sessionID: session.id,
        agent: "build",
        parts: [{ type: "text", text: "Cancel me" }],
      })
      .pipe(Effect.forkChild)

    yield* llm.wait(1)
    yield* waitForBusy(session.id)
    yield* prompt.cancel(session.id)

    const exit = yield* Fiber.await(fiber)
    expect(Exit.isSuccess(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      expect(exit.value.info.role).toBe("assistant")
      if (exit.value.info.role === "assistant") {
        expect(exit.value.info.error?.name).toBe("MessageAbortedError")
      }
    }

    const msgs = yield* sessions.messages({ sessionID: session.id })
    const last = msgs.findLast((msg) => msg.info.role === "assistant")
    expect(last?.info.role).toBe("assistant")
    if (last?.info.role === "assistant") {
      expect(last.info.error?.name).toBe("MessageAbortedError")
    }
  }),
)

// Agent variant

noLLMServer.instance(
  "applies agent variant only when using agent model",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})

      const other = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        model: { providerID: ProviderV2.ID.make("opencode"), modelID: ModelV2.ID.make("kimi-k2.5-free") },
        noReply: true,
        parts: [{ type: "text", text: "hello" }],
      })
      if (other.info.role !== "user") throw new Error("expected user message")
      expect(other.info.model.variant).toBeUndefined()

      const match = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        parts: [{ type: "text", text: "hello again" }],
      })
      if (match.info.role !== "user") throw new Error("expected user message")
      expect(match.info.model).toEqual({
        providerID: ProviderV2.ID.make("test"),
        modelID: ModelV2.ID.make("test-model"),
        variant: "xhigh",
      })
      expect(match.info.model.variant).toBe("xhigh")

      const override = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        variant: "high",
        parts: [{ type: "text", text: "hello third" }],
      })
      if (override.info.role !== "user") throw new Error("expected user message")
      expect(override.info.model.variant).toBe("high")

      yield* sessions.remove(session.id)
    }),
  {
    config: {
      ...cfg,
      provider: {
        ...cfg.provider,
        test: {
          ...cfg.provider.test,
          models: {
            "test-model": {
              ...cfg.provider.test.models["test-model"],
              variants: { xhigh: {}, high: {} },
            },
          },
        },
      },
      agent: {
        build: {
          model: "test/test-model",
          variant: "xhigh",
        },
      },
    },
  },
)

// Agent / command resolution errors

noLLMServer.instance(
  "unknown agent throws typed error",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const exit = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "nonexistent-agent-xyz",
          noReply: true,
          parts: [{ type: "text", text: "hello" }],
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const err = Cause.squash(exit.cause)
        expect(err).not.toBeInstanceOf(TypeError)
        expect(NamedError.Unknown.isInstance(err)).toBe(true)
        if (NamedError.Unknown.isInstance(err)) {
          expect(err.data.message).toContain('Agent not found: "nonexistent-agent-xyz"')
        }
      }
    }),
  30_000,
)

noLLMServer.instance(
  "unknown agent error includes available agent names",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const exit = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "nonexistent-agent-xyz",
          noReply: true,
          parts: [{ type: "text", text: "hello" }],
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const err = Cause.squash(exit.cause)
        expect(NamedError.Unknown.isInstance(err)).toBe(true)
        if (NamedError.Unknown.isInstance(err)) {
          expect(err.data.message).toContain("build")
        }
      }
    }),
  30_000,
)

noLLMServer.instance(
  "unknown command throws typed error with available names",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const exit = yield* prompt
        .command({
          sessionID: session.id,
          command: "nonexistent-command-xyz",
          arguments: "",
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const err = Cause.squash(exit.cause)
        expect(err).not.toBeInstanceOf(TypeError)
        expect(NamedError.Unknown.isInstance(err)).toBe(true)
        if (NamedError.Unknown.isInstance(err)) {
          expect(err.data.message).toContain('Command not found: "nonexistent-command-xyz"')
          expect(err.data.message).toContain("init")
        }
      }
    }),
  30_000,
)

// GOAL: #18. An unknown effort used to reach request.ts, where
// `input.model.variants[variant]` is undefined and merges as nothing - so
// `--effort hgih` changed the request in no way and reported nothing. The
// issue's own words: a silently ignored effort level is worse than a rejected
// one.
it.instance("rejects an unknown effort and lists the ones the model has", () =>
  Effect.gen(function* () {
    yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Effort",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })

    // Effect.exit + Cause.squash, matching compaction.test.ts. Effect.either
    // does not exist in this Effect version (4.0.0-beta.83).
    const exit = yield* Effect.exit(
      prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
        variant: "hgih",
        parts: [{ type: "text", text: "hello" }],
      }),
    )

    expect(Exit.isFailure(exit)).toBe(true)
    if (!Exit.isFailure(exit)) throw new Error("unreachable")
    const error = Cause.squash(exit.cause)
    // The text lives in `data.message`, not `.message` - matching the
    // "Agent not found" assertion earlier in this file. Reading `.message`
    // gives the tag, "UnknownError", which passes no useful assertion.
    expect(NamedError.Unknown.isInstance(error)).toBe(true)
    if (!NamedError.Unknown.isInstance(error)) throw new Error("unreachable")
    expect(error.data.message).toContain('Unknown effort "hgih"')
    expect(error.data.message).toContain("test/test-model")
    // The test model declares no variants, so an effort could never have done
    // anything for it - which is worth saying rather than ignoring.
    expect(error.data.message).toContain("declares none")
  }),
)

// GOAL: and the sentinel still works. "default" means "no variant" elsewhere
// in this file, so validating it would break the selector's own reset path.
it.instance("accepts the default sentinel without checking it", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Default effort",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* seedUser({
      sessionID: chat.id,
      agent: "build",
      variant: "default",
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.text("ok")
    const result = yield* prompt.loop({ sessionID: chat.id })
    expect(result.info.role).toBe("assistant")
  }),
)

// GOAL: the other branch - #18 asks for the available levels, not a bare
// rejection, so a model that DOES declare variants must list them.
it.instance("lists the available efforts when the model declares some", () =>
  Effect.gen(function* () {
    yield* useServerConfig((url) => {
      const base = providerCfg(url)
      return {
        ...base,
        provider: {
          ...base.provider,
          test: {
            ...base.provider.test,
            models: {
              "test-model": {
                ...base.provider.test.models["test-model"],
                variants: { low: { reasoningEffort: "low" }, high: { reasoningEffort: "high" } },
              },
            },
          },
        },
      } as Partial<ConfigV1.Info>
    })
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Effort list",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })

    const exit = yield* Effect.exit(
      prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
        variant: "nope",
        parts: [{ type: "text", text: "hello" }],
      }),
    )

    expect(Exit.isFailure(exit)).toBe(true)
    if (!Exit.isFailure(exit)) throw new Error("unreachable")
    const error = Cause.squash(exit.cause)
    expect(NamedError.Unknown.isInstance(error)).toBe(true)
    if (!NamedError.Unknown.isInstance(error)) throw new Error("unreachable")
    expect(error.data.message).toContain("Available:")
    expect(error.data.message).toContain("high")
    expect(error.data.message).toContain("low")
  }),
)

it.instance(
  "captain peer batches reach the actual provider before coalesced automation using recipient model and effort",
  () =>
    Effect.gen(function* () {
      const held = yield* startHeld({
        config: (url) => {
          const base = providerCfg(url)
          return {
            ...base,
            agent: { build: { model: "test/test-model", variant: "low" } },
            provider: {
              ...base.provider,
              test: {
                ...base.provider.test,
                models: {
                  ...base.provider.test.models,
                  "test-model": {
                    ...base.provider.test.models["test-model"],
                    variants: { low: { reasoningEffort: "low" } },
                  },
                  "selected-model": {
                    ...base.provider.test.models["test-model"],
                    id: "selected-model",
                    name: "Selected model",
                    variants: { high: { reasoningEffort: "high" } },
                  },
                },
              },
            },
          }
        },
      })
      const automation =
        "[fm-level:nudge:watcher.wake]\x1f WATCHER FIRED [wake-1] - handle wake.\n\ncheck: /scratch/task.check.sh: still actionable"
      yield* held.queue.admit({ sessionID: held.chat.id, delivery: "queue", model: ref, parts: said(automation) })
      const peers = Array.from(
        { length: 11 },
        (_, index) =>
          `[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791809000-${index + 1}-abcd\x1f [fm-level:request]\x1f provider peer ${index + 1}\ncomplete tail ${index + 1}`,
      )
      yield* Effect.forEach(peers, (value, index) =>
        held.queue.admit({
          sessionID: held.chat.id,
          delivery: "queue",
          model: ref,
          parts: [
            ...said(value),
            ...(index === 0
              ? [
                  {
                    type: "file" as const,
                    mime: "text/plain",
                    filename: "peer-evidence.txt",
                    url: "data:text/plain;base64,Y29tcGxldGUgZXZpZGVuY2U=",
                  },
                ]
              : []),
          ],
        }),
      )
      yield* held.sessions.setAgentModel({
        sessionID: held.chat.id,
        agent: "build",
        model: { providerID: ref.providerID, id: ModelV2.ID.make("selected-model"), variant: "high" },
        time: Date.now(),
      })
      yield* held.llm.text("first ten handled")
      yield* held.llm.text("overflow handled")
      yield* held.llm.text("automation handled")
      yield* held.release
      yield* finish(held.task)
      const inputs = yield* held.llm.inputs
      expect(inputs).toHaveLength(4)
      peers
        .slice(0, 10)
        .forEach((value) =>
          expect(lastUserContent(inputs[1])).toContain(value.replaceAll("\x1f", "\\u001f").replaceAll("\n", "\\n")),
        )
      expect(lastUserContent(inputs[1])).toContain("complete evidence")
      expect(lastUserContent(inputs[1])).not.toContain("provider peer 11")
      expect(lastUserContent(inputs[2])).toContain("provider peer 11")
      expect(lastUserContent(inputs[3])).toContain("still actionable")
      expect(inputs[1]?.model).toBe("selected-model")
      expect(inputs[1]?.reasoning_effort).toBe("high")
      expect(yield* held.queue.list(held.chat.id)).toEqual([])
    }),
)

it.instance("held provider setup exposes the actual prompt failure before any model call", () =>
  Effect.gen(function* () {
    const exit = yield* startHeld({ agent: "missing-provider-boundary-agent" }).pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (!Exit.isFailure(exit)) throw new Error("expected actual preparation failure")
    const error = Cause.squash(exit.cause)
    expect(NamedError.Unknown.isInstance(error)).toBe(true)
    if (!NamedError.Unknown.isInstance(error)) throw new Error("expected unknown-agent cause")
    expect(error.data.message).toContain('Agent not found: "missing-provider-boundary-agent"')
    const llm = yield* TestLLMServer
    expect(yield* llm.calls).toBe(0)
  }),
)

gated.instance(
  "first-provider diagnostic identifies the held actual runner under its unchanged bound",
  () =>
    Effect.gen(function* () {
      const gate = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      const observed: {
        sessionID?: SessionID
        calls?: number
        runner?: { state: string; frame?: string; fiberID?: number }
      } = {}
      gates.messagesTransform = gate
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(gate.release, undefined).pipe(
          Effect.andThen(
            Effect.sync(() => {
              gates.messagesTransform = undefined
            }),
          ),
          Effect.asVoid,
        ),
      )
      const started = yield* startHeld({ diagnostic: (value) => Object.assign(observed, value) }).pipe(
        Effect.exit,
        Effect.forkChild,
      )
      yield* awaitWithTimeout(
        Deferred.await(gate.entered),
        "valid runner never reached its real transform hook",
        "10 seconds",
      )
      const exit = yield* Fiber.join(started)
      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) throw new Error("held runner unexpectedly reached the provider")
      expect(Cause.squash(exit.cause)).toMatchObject({ message: "first provider call never started" })
      expect(observed.calls).toBe(0)
      expect(observed.runner?.state).toBe("Running")
      expect(observed.runner?.frame).toBe("test.experimental.chat.messages.transform")
      expect(observed.runner?.fiberID).toBeGreaterThan(0)
      if (!observed.sessionID) throw new Error("expected inspected session owner")
      const prompt = yield* SessionPrompt.Service
      yield* prompt.cancel(observed.sessionID)
    }),
  20000,
)

it.instance("captain critical peer remains an immediate steer during active provider work", () =>
  Effect.gen(function* () {
    const held = yield* startHeld({ tool: true })
    const critical =
      "[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791809000-1-abcd\x1f [fm-level:critical]\x1f urgent peer direction"
    const sent = yield* held.send(critical)
    yield* pollWithTimeout(
      held.sessions
        .messages({ sessionID: held.chat.id })
        .pipe(
          Effect.map((messages) =>
            messages.some((message) =>
              message.parts.some((part) => part.type === "text" && part.text.includes(critical)),
            )
              ? true
              : undefined,
          ),
        ),
      "critical peer was not immediately promoted",
    )
    expect(yield* held.queue.list(held.chat.id)).toEqual([])
    yield* held.llm.text("critical handled")
    yield* held.release
    yield* finish(held.task, sent)
    expect(JSON.stringify((yield* held.llm.inputs)[1]?.messages)).toContain("urgent peer direction")
  }),
)

gated.instance("captain grouped preparation cannot restore model selection changed before commit", () =>
  Effect.gen(function* () {
    const held = yield* startHeld({
      config: (url) => {
        const base = providerCfg(url)
        return {
          ...base,
          provider: {
            ...base.provider,
            test: {
              ...base.provider.test,
              models: {
                ...base.provider.test.models,
                "picked-later": {
                  ...base.provider.test.models["test-model"],
                  id: "picked-later",
                  name: "Picked later",
                },
              },
            },
          },
        }
      },
    })
    yield* held.queue.admit({
      sessionID: held.chat.id,
      delivery: "queue",
      model: ref,
      parts: said(
        "[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791809000-1-abcd\x1f [fm-level:request]\x1f select at committed promotion",
      ),
    })
    const gate = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
    gates.queueSelection = gate
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        gates.queueSelection = undefined
      }),
    )
    yield* held.llm.text("peer handled on later selection")
    yield* held.release
    yield* awaitWithTimeout(Deferred.await(gate.entered), "group never paused during preparation")
    yield* held.sessions.setAgentModel({
      sessionID: held.chat.id,
      agent: "build",
      model: { providerID: ref.providerID, id: ModelV2.ID.make("picked-later") },
      time: Date.now(),
    })
    yield* Deferred.succeed(gate.release, undefined)
    yield* finish(held.task)
    expect((yield* held.llm.inputs)[1]?.model).toBe("picked-later")
    expect((yield* held.sessions.get(held.chat.id)).model?.id).toBe(ModelV2.ID.make("picked-later"))
  }),
)

it.instance(
  "exact peer retry callers survive one cancelled lifetime and receive their own turn once",
  () =>
    Effect.gen(function* () {
      const held = yield* startHeld()
      yield* held.llm.text("peer original answer")
      const content =
        "[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791809000-999-abcd\x1f [fm-level:request]\x1f Exact retry caller obligation"
      const messageID = MessageID.ascending()
      const manual = yield* held.queue.admit({
        sessionID: held.chat.id,
        messageID,
        delivery: "queue",
        agent: "build",
        model: ref,
        parts: said(content),
      })
      const admittedAll = yield* Deferred.make<void>()
      const events = yield* EventV2Bridge.Service
      let admissions = 0
      const unsubscribe = yield* events.listen((event) =>
        event.type === SessionQueue.Event.Updated.type
          ? Effect.sync(() => {
              admissions++
            }).pipe(
              Effect.andThen(
                Effect.suspend(() =>
                  admissions === 3 ? Deferred.succeed(admittedAll, undefined).pipe(Effect.asVoid) : Effect.void,
                ),
              ),
            )
          : Effect.void,
      )
      const first = yield* held.send(content, { messageID, delivery: "queue" })
      const second = yield* held.send(content, { messageID, delivery: "queue" })
      const cancelled = yield* held.send(content, { messageID, delivery: "queue" })
      yield* Deferred.await(admittedAll)
      yield* unsubscribe
      yield* Fiber.interrupt(cancelled)
      yield* held.release
      const [root, one, two] = yield* finish(held.task, first, second)
      expect(answered(root)?.texts).toEqual(["task done"])
      expect(answered(one)?.texts).toEqual(["peer original answer"])
      expect(answered(two)).toEqual(answered(one))
      const recorded = yield* held.prompt.prompt({
        sessionID: held.chat.id,
        messageID,
        delivery: "queue",
        agent: "build",
        model: ref,
        parts: said(content),
      })
      expect(answered(recorded)).toEqual(answered(one))
      expect(yield* held.llm.calls).toBe(2)
      expect(yield* held.queue.list(held.chat.id)).toEqual([])
      yield* held.queue.forget(manual.id)
      expect(yield* held.queue.reply(manual.id)).toBeUndefined()
    }),
  20000,
)

it.instance(
  "ordinary peers reach each safe active tool continuation in capped batches without advancing holds",
  () =>
    Effect.gen(function* () {
      const directory = yield* TestInstance
      const file = path.join(directory.directory, "peer-continuation.txt")
      yield* writeText(file, "real tool result retained")
      const held = yield* startHeld({ tool: { name: "read", args: { filePath: file } } })
      const lastGate = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() => Deferred.succeed(lastGate, undefined).pipe(Effect.asVoid))
      yield* held.llm.tool("read", { filePath: file })
      yield* held.llm.hold("active peers handled", deferredAsPromise(lastGate))
      yield* held.llm.text("held human handled")
      yield* held.llm.text("automation handled")
      const automationText =
        "[fm-level:nudge:watcher.wake]\x1f WATCHER FIRED [active-1] - handle wake.\n\ncheck: /scratch/active.check.sh: historical notice"
      const automation = yield* held.send(automationText)
      const human = yield* held.send("held human must wait", { delivery: "queue" })
      const inputs = Array.from({ length: 11 }, (_, index) => ({
        messageID: MessageID.ascending(),
        text: `[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791900000-${index + 1}-abcd\x1f [fm-level:request]\x1f Active peer ${index + 1}\nFull original tail ${index + 1}`,
      }))
      const peers = yield* Effect.forEach(inputs, (input) => held.send(input.text, { messageID: input.messageID }))
      const pending = yield* queued(held.chat.id, 13)
      expect(pending.every((item) => item.delivery === "queue")).toBe(true)
      const originals = inputs.map((input) => pending.find((item) => item.input.messageID === input.messageID)!)
      expect(yield* held.llm.calls).toBe(1)
      expect(
        (yield* held.sessions.messages({ sessionID: held.chat.id })).filter((message) => message.info.role === "user"),
      ).toHaveLength(1)
      yield* held.release
      yield* awaitWithTimeout(held.llm.wait(3), "active overflow never reached the continuing provider", "10 seconds")
      const requests = yield* held.llm.inputs
      if (!requests[1] || !requests[2]) throw new Error("expected both peer provider continuations")
      inputs
        .slice(0, 10)
        .forEach((input) =>
          expect(lastUserContent(requests[1])).toContain(
            input.text.replaceAll("\x1f", "\\u001f").replaceAll("\n", "\\n"),
          ),
        )
      expect(lastUserContent(requests[1])).not.toContain("Active peer 11")
      expect(lastUserContent(requests[2])).toContain("Active peer 11")
      expect(JSON.stringify(requests[1]?.messages)).toContain("real tool result retained")
      expect(JSON.stringify(requests[2]?.messages)).toContain("real tool result retained")
      expect(JSON.stringify(requests[2]?.messages)).not.toContain(automationText)
      expect(JSON.stringify(requests[2]?.messages)).not.toContain("held human must wait")
      expect((yield* held.queue.list(held.chat.id)).map((item) => item.input.parts[0])).toEqual([
        said(automationText)[0],
        said("held human must wait")[0],
      ])
      const messages = yield* held.sessions.messages({ sessionID: held.chat.id })
      const groups = messages.flatMap((message) =>
        message.parts.flatMap((part) =>
          part.type === "text" && !part.ignored && part.metadata?.sessionQueue
            ? [Schema.decodeUnknownSync(SessionPromptQueue.Group)(part.metadata.sessionQueue)]
            : [],
        ),
      ).filter((group) => group.kind === "peer")
      expect(groups.map((group) => group.constituents.map((item) => item.id))).toEqual([
        originals.slice(0, 10).map((item) => item.id),
        [originals[10].id],
      ])
      const retry = yield* held.queue.admit({
        sessionID: held.chat.id,
        messageID: MessageID.ascending(),
        delivery: "queue",
        agent: "build",
        model: ref,
        parts: said(inputs[0].text),
      })
      expect(retry.id).toBe(originals[0].id)
      yield* Deferred.succeed(lastGate, undefined)
      const replies = yield* finish(held.task, ...peers, human, automation)
      expect(replies.slice(0, 12).map((reply) => answered(reply)?.texts)).toEqual(
        Array.from({ length: 12 }, () => ["active peers handled"]),
      )
      expect(answered(replies[12])?.texts).toEqual(["held human handled"])
      expect(answered(replies[13])?.texts).toEqual(["automation handled"])
      expect(answered(yield* held.queue.reply(retry.id))?.texts).toEqual(["active peers handled"])
      expect(yield* held.llm.calls).toBe(5)
      expect(yield* held.queue.list(held.chat.id)).toEqual([])
    }),
  30000,
)

it.instance(
  "ordinary peer arriving during a long tool waits for its completion before model visibility",
  () =>
    Effect.gen(function* () {
      const directory = yield* TestInstance
      const file = path.join(directory.directory, "long-peer-tool.txt")
      yield* writeText(file, "completed long tool evidence")
      const held = yield* startHeld({ tool: { name: "read", args: { filePath: file } } })
      const registry = yield* ToolRegistry.Service
      const named = yield* registry.named()
      const original = named.read.execute
      const toolEntered = yield* Deferred.make<void>()
      const toolRelease = yield* Deferred.make<void>()
      const providerRelease = yield* Deferred.make<void>()
      const executed: string[] = []
      const interrupted: boolean[] = []
      named.read.execute = (args, ctx) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(toolEntered, undefined)
          yield* Deferred.await(toolRelease)
          expect(ctx.abort.aborted).toBe(false)
          executed.push(args.filePath)
          return yield* original(args, ctx)
        }).pipe(
          Effect.onInterrupt(() =>
            Effect.sync(() => {
              interrupted.push(true)
            }),
          ),
        )
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          named.read.execute = original
        }).pipe(
          Effect.andThen(Deferred.succeed(toolRelease, undefined)),
          Effect.andThen(Deferred.succeed(providerRelease, undefined)),
          Effect.asVoid,
        ),
      )
      yield* held.llm.tool("read", { filePath: file })
      yield* held.llm.hold("long-tool peers answered", deferredAsPromise(providerRelease))
      yield* held.llm.text("long-tool human answered")
      yield* held.llm.text("long-tool automation answered")
      const firstText =
        "[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791900001-1-abcd\x1f [fm-level:request]\x1f Peer before tool\nOriginal first obligation"
      const first = yield* held.send(firstText)
      yield* queued(held.chat.id, 1)
      yield* held.release
      yield* awaitWithTimeout(Deferred.await(toolEntered), "real long read tool did not start", "10 seconds")
      const secondText =
        "[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791900001-2-abcd\x1f [fm-level:info]\x1f Peer during tool\nOriginal second tail"
      const automationText =
        "[fm-level:nudge:watcher.wake]\x1f WATCHER FIRED [wake-2] - handle wake.\n\ncheck: /scratch/long.check.sh: historical observation"
      const second = yield* held.send(secondText)
      const human = yield* held.send("long-tool held human", { delivery: "queue" })
      const automation = yield* held.send(automationText)
      yield* queued(held.chat.id, 3)
      expect(yield* held.llm.calls).toBe(2)
      expect(executed).toEqual([])
      expect(interrupted).toEqual([])
      const during = yield* held.sessions.messages({ sessionID: held.chat.id })
      expect(
        during
          .flatMap((message) => message.parts)
          .some((part) => part.type === "tool" && part.state.status === "running"),
      ).toBe(true)
      expect(
        during
          .flatMap((message) => message.parts)
          .some((part) => part.type === "text" && part.text.includes(secondText)),
      ).toBe(false)
      yield* Deferred.succeed(toolRelease, undefined)
      yield* awaitWithTimeout(held.llm.wait(3), "long-tool peer never reached the next provider request", "10 seconds")
      const requests = yield* held.llm.inputs
      if (!requests[1] || !requests[2]) throw new Error("expected actual tool continuations")
      expect(lastUserContent(requests[1])).toContain("Peer before tool")
      expect(lastUserContent(requests[1])).not.toContain("Peer during tool")
      expect(lastUserContent(requests[2])).toContain("Peer during tool")
      expect(lastUserContent(requests[2])).toContain("Original second tail")
      expect(JSON.stringify(requests[2].messages)).toContain("completed long tool evidence")
      expect(JSON.stringify(requests[2].messages)).not.toContain("long-tool held human")
      expect(JSON.stringify(requests[2].messages)).not.toContain("historical observation")
      expect(executed).toEqual([file])
      expect(interrupted).toEqual([])
      const after = yield* held.sessions.messages({ sessionID: held.chat.id })
      expect(
        after
          .flatMap((message) => message.parts)
          .filter((part) => part.type === "tool" && part.state.status === "completed"),
      ).toHaveLength(2)
      yield* Deferred.succeed(providerRelease, undefined)
      const replies = yield* finish(held.task, first, second, human, automation)
      expect(replies.slice(0, 3).map((reply) => answered(reply)?.texts)).toEqual(
        Array.from({ length: 3 }, () => ["long-tool peers answered"]),
      )
      expect(answered(replies[3])?.texts).toEqual(["long-tool human answered"])
      expect(answered(replies[4])?.texts).toEqual(["long-tool automation answered"])
      expect(yield* held.llm.calls).toBe(5)
    }),
  30000,
)

it.instance(
  "historical native stable-ID admission refuses uncertainty with zero provider execution",
  () =>
    Effect.gen(function* () {
      const config = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      for (const system of [undefined, "excluded original execution controls"]) {
        const session = yield* sessions.create({ title: "Historical stable original" })
        const input = {
          sessionID: session.id,
          messageID: MessageID.ascending(),
          agent: "build",
          model: ref,
          system,
          parts: said("historical native request"),
        }
        // Pre-upgrade bare projection: do not create current-build noReply custody.
        yield* sessions.updateMessage({
          id: input.messageID,
          sessionID: session.id,
          role: "user",
          agent: input.agent,
          model: input.model,
          system: input.system,
          time: { created: Date.now() },
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          messageID: input.messageID,
          sessionID: session.id,
          type: "text",
          text: "historical native request",
        })
        const before = yield* sessions.messages({ sessionID: session.id })
        for (const retry of [
          input,
          { ...input, delivery: "queue" as const },
          { ...input, parts: said("changed historical intent") },
          { ...input, system: "conflicting execution controls" },
          { ...input, noReply: true },
        ]) {
          const exit = yield* prompt.prompt(retry).pipe(Effect.exit)
          expect(Exit.isFailure(exit)).toBe(true)
          if (!Exit.isFailure(exit)) throw new Error("historical identity was silently replayed")
          expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionQueue.HistoricalIdentityError)
          expect(yield* queue.list(session.id)).toEqual([])
          expect(yield* sessions.messages({ sessionID: session.id })).toEqual(before)
        }
      }
      expect(yield* config.llm.calls).toBe(0)
    }),
  30000,
)

unix(
  "historical command stable-ID refusal occurs before shell expansion and provider execution",
  () =>
    Effect.gen(function* () {
      const directory = yield* TestInstance
      const marker = path.join(directory.directory, "old-command-executions.txt")
      yield* writeText(marker, "original execution retained\n")
      const config = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        command: {
          historical: {
            template: `!\`printf 'second execution\\n' >> '${marker}'; printf 'historical command intent'\``,
          },
        },
      }))
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "Historical command original" })
      const messageID = MessageID.ascending()
      yield* seedUser({
        sessionID: session.id,
        messageID,
        agent: "build",
        model: ref,
        parts: said("historical command intent"),
      })
      const before = yield* sessions.messages({ sessionID: session.id })
      for (const args of ["", "changed arguments"]) {
        const exit = yield* prompt
          .command({
            sessionID: session.id,
            messageID,
            command: "historical",
            arguments: args,
            agent: "build",
            model: "test/test-model",
          })
          .pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
        if (!Exit.isFailure(exit)) throw new Error("historical command was replayed")
        expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionQueue.HistoricalIdentityError)
        expect(yield* Effect.promise(() => Bun.file(marker).text())).toBe("original execution retained\n")
        expect(yield* sessions.messages({ sessionID: session.id })).toEqual(before)
        expect(yield* queue.list(session.id)).toEqual([])
      }
      expect(yield* config.llm.calls).toBe(0)
    }),
  30000,
)

it.instance(
  "round4 represented noReply reconciles full input and delivery without rewriting native history",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Represented noReply" })
      const input = {
        sessionID: session.id,
        messageID: MessageID.ascending(),
        agent: "build",
        model: ref,
        parts: said("native original"),
      }
      yield* llm.text("native answer")
      const answered = yield* prompt.prompt(input)
      const before = yield* sessions.messages({ sessionID: session.id })
      const exact = yield* prompt.prompt({ ...input, noReply: true })
      expect(exact.info.id).toBe(input.messageID)
      expect(yield* sessions.messages({ sessionID: session.id })).toEqual(before)
      for (const changed of [
        { parts: said("conflicting native text") },
        { delivery: "queue" as const },
        { system: "different system" },
        { tools: { read: false } },
        { variant: "different" },
      ]) {
        const exit = yield* Effect.exit(prompt.prompt({ ...input, ...changed, noReply: true }))
        expect(Exit.isFailure(exit)).toBe(true)
        expect(yield* sessions.messages({ sessionID: session.id })).toEqual(before)
      }
      yield* sessions.setRevert({
        sessionID: session.id,
        revert: { messageID: input.messageID },
        summary: session.summary,
      })
      const reverted = yield* sessions.get(session.id)
      const retried = yield* prompt.prompt(input)
      expect(retried.info.id).toBe(answered.info.id)
      expect((yield* sessions.get(session.id)).revert).toEqual(reverted.revert)
      expect(yield* sessions.messages({ sessionID: session.id })).toEqual(before)
      expect(yield* llm.calls).toBe(1)
    }),
  30000,
)

it.instance(
  "round4 unmarked direct noReply records an exact original and refuses conflicting reuse",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Direct noReply custody" })
      const input = {
        sessionID: session.id,
        messageID: MessageID.ascending(),
        agent: "build",
        model: ref,
        noReply: true,
        parts: said("direct original"),
      }
      const first = yield* prompt.prompt(input)
      expect(yield* prompt.prompt(input)).toEqual(first)
      const before = yield* sessions.messages({ sessionID: session.id })
      for (const changed of [{ parts: said("changed") }, { noReply: false }, { delivery: "queue" as const }]) {
        expect(Exit.isFailure(yield* Effect.exit(prompt.prompt({ ...input, ...changed })))).toBe(true)
        expect(yield* sessions.messages({ sessionID: session.id })).toEqual(before)
      }
      expect(first.parts.filter((part) => part.type === "text" && part.metadata?.sessionQueueNoReply)).toHaveLength(1)
      expect(yield* llm.calls).toBe(0)
    }),
  30000,
)

for (const custody of ["pending", "retained", "consumed"] as const)
  unix(
    `round4 represented command ${custody} retry refuses before executable expansion`,
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const marker = path.join(test.directory, "command-once.txt")
        const command = { probe: { template: `!\`printf 'execution\\n' >> '${marker}'; printf 'command intent'\`` } }
        const prompt = yield* SessionPrompt.Service
        const queue = yield* SessionQueue.Service
        const sessions = yield* Session.Service
        const held =
          custody !== "consumed" ? yield* startHeld({ config: (url) => ({ ...providerCfg(url), command }) }) : undefined
        const configured = held ? undefined : yield* useServerConfig((url) => ({ ...providerCfg(url), command }))
        const llm = held?.llm ?? configured!.llm
        const session = held?.chat ?? (yield* sessions.create({ title: "Consumed command" }))
        const input = {
          sessionID: session.id,
          messageID: MessageID.ascending(),
          command: "probe",
          arguments: "",
          delivery: custody === "consumed" ? ("steer" as const) : ("queue" as const),
        }
        if (!held) yield* llm.text("command complete")
        const original = yield* prompt.command(input).pipe(Effect.forkChild)
        if (!held) yield* awaitWithTimeout(Fiber.join(original), "original command did not complete", "10 seconds")
        if (held) {
          yield* queued(session.id, 1)
          if (custody === "retained")
            yield* queue.promote({
              sessionID: session.id,
              delivery: "queue",
              prepare: (input) => Effect.succeed(input),
              write: (input) =>
                Effect.gen(function* () {
                  yield* sessions.updateMessage({
                    id: input.messageID,
                    sessionID: session.id,
                    role: "user",
                    agent: input.agent ?? "build",
                    model: input.model ?? ref,
                    time: { created: Date.now() },
                  })
                  yield* Effect.forEach(
                    input.parts,
                    (part) =>
                      part.type === "text"
                        ? sessions.updatePart({
                            ...part,
                            id: part.id ?? PartID.ascending(),
                            sessionID: session.id,
                            messageID: input.messageID,
                          })
                        : Effect.void,
                    { discard: true },
                  )
                }),
              rejected: (cause) => Effect.failCause(cause),
            })
        }
        // HTTP request receipt can precede the runner's initial snapshot write; freeze only after that part exists.
        if (held)
          yield* pollWithTimeout(
            sessions.messages({ sessionID: session.id }).pipe(
              Effect.map((messages) =>
                messages.some((message) =>
                  message.info.role === "assistant" && message.parts.some((part) => part.type === "step-start"),
                ) ? true : undefined,
              ),
            ),
            "held provider did not persist its initial step",
            "10 seconds",
          )
        const before = yield* sessions.messages({ sessionID: session.id })
        const pending = yield* queue.list(session.id)
        const calls = yield* llm.calls
        for (const changed of [
          {},
          { arguments: "different arguments" },
          { command: "other-command" },
          { delivery: "steer" as const },
        ]) {
          const exit = yield* Effect.exit(prompt.command({ ...input, ...changed }))
          expect(Exit.isFailure(exit)).toBe(true)
          if (Exit.isFailure(exit))
            expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionQueue.HistoricalIdentityError)
          expect(yield* Effect.promise(() => Bun.file(marker).text())).toBe("execution\n")
          expect(yield* llm.calls).toBe(calls)
          expect(yield* sessions.messages({ sessionID: session.id })).toEqual(before)
          expect(yield* queue.list(session.id)).toEqual(pending)
        }
        if (held) {
          yield* llm.text("command complete")
          yield* held.release
          yield* finish(held.task, original)
        }
      }),
    30000,
  )

unix(
  "round4 shell represented and bare identities refuse before command execution",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const test = yield* TestInstance
      const marker = path.join(test.directory, "shell-once.txt")
      const represented = yield* sessions.create({ title: "Represented shell ID" })
      const messageID = MessageID.ascending()
      yield* llm.text("original native answer")
      yield* prompt.prompt({
        sessionID: represented.id,
        messageID,
        agent: "build",
        model: ref,
        parts: said("normal original"),
      })
      const fresh = yield* sessions.create({ title: "Bare shell ID" })
      const bareID = MessageID.ascending()
      const shell = {
        sessionID: fresh.id,
        messageID: bareID,
        agent: "build",
        model: ref,
        command: `printf 'execution\\n' >> '${marker}'`,
      }
      yield* prompt.shell(shell)
      for (const input of [
        shell,
        { ...shell, command: `printf changed >> '${marker}'` },
        { ...shell, sessionID: represented.id, messageID },
      ]) {
        const before = yield* sessions.messages({ sessionID: input.sessionID })
        const exit = yield* Effect.exit(prompt.shell(input))
        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionQueue.HistoricalIdentityError)
        expect(yield* sessions.messages({ sessionID: input.sessionID })).toEqual(before)
        expect(yield* Effect.promise(() => Bun.file(marker).text())).toBe("execution\n")
      }
      expect(yield* llm.calls).toBe(1)
    }),
  30000,
)

it.instance(
  "round4 peer continuation defers while the root structured output contract is unanswered",
  () =>
    Effect.gen(function* () {
      const held = yield* startHeld({
        tool: { name: "glob", args: { pattern: "*.txt" } },
        format: Schema.decodeUnknownSync(SessionV1.Format)({
          type: "json_schema",
          schema: { type: "object", properties: { answer: { type: "string" } }, required: ["answer"] },
        }),
      })
      const text = "[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791854000-401-abcd\x1f [fm-level:request]\x1f Structured root peer\nFull peer tail"
      const peer = yield* held.send(text, { delivery: "queue" })
      yield* queued(held.chat.id, 1)
      yield* held.llm.push(
        reply().tool("StructuredOutput", { answer: "root contract preserved" }).item(),
        reply().text("peer answered later").stop().item(),
      )
      yield* held.release
      const root = yield* awaitWithTimeout(Fiber.join(held.task), "structured root did not settle", "10 seconds")
      yield* awaitWithTimeout(Fiber.join(peer), "deferred peer did not settle", "10 seconds")
      const requests = yield* held.llm.inputs
      expect(JSON.stringify(requests[1]?.messages)).not.toContain(JSON.stringify(text).slice(1, -1))
      expect(JSON.stringify(requests[1]?.tools)).toContain("StructuredOutput")
      expect(requests[1]?.tool_choice).toBe("required")
      expect(root.info.role === "assistant" && root.info.structured).toEqual({ answer: "root contract preserved" })
      expect(root.info.role === "assistant" && root.info.error).toBeUndefined()
      expect(JSON.stringify(requests[2]?.messages)).toContain(JSON.stringify(text).slice(1, -1))
      expect(yield* held.llm.calls).toBe(3)
    }),
  30000,
)

it.instance(
  "round4 held actual SDK dependency remains a no-provider failure under unchanged readiness bound",
  () =>
    Effect.gen(function* () {
      const instance = yield* TestInstance
      yield* writeText(
        path.join(instance.directory, ".opencode/tool/sdk-boundary.ts"),
        'import { tool } from "@opencode-ai/plugin"; export default tool({ description: "local SDK boundary", args: {}, async execute() { return "unused" } })',
      )
      const gate = {
        dir: path.join(instance.directory, ".opencode"),
        entered: yield* Deferred.make<void>(),
        release: yield* Deferred.make<void>(),
      }
      sdkReadiness.gate = gate
      const observations: Array<{ sessionID: SessionID; calls: number; runner?: { state: string; frame?: string } }> = []
      const attempt = yield* startHeld({ diagnostic: (value) => observations.push(value) }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(gate.entered), "actual SDK dependency fixture did not enter its owner")
      const cfg = yield* Config.Service
      const dirs = yield* cfg.directories()
      expect(
        dirs.every((dir) => dir === Global.Path.config || dir === path.join(instance.directory, ".opencode")),
      ).toBe(true)
      const exit = yield* awaitWithTimeout(
        Fiber.await(attempt),
        "unchanged first-provider backstop did not fire",
        "15 seconds",
      )
      yield* Deferred.succeed(gate.release, undefined)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.pretty(exit.cause)).toContain("first provider call never started")
      expect(observations).toHaveLength(1)
      expect(observations[0]?.calls).toBe(0)
      expect(observations[0]?.runner?.state).toBe("Running")
      expect(observations[0]?.runner?.frame).toBe("Config.waitForDependencies")
    }),
  30000,
)

it.instance(
  "round4 critical noReply peer preserves original transport identity and exact aliases",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Direct critical peer identity" })
      const text =
        "[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791854000-402-abcd\x1f [fm-level:critical]\x1f Critical excluded peer\nComplete tail"
      const input = { sessionID: session.id, agent: "build", model: ref, noReply: true, parts: said(text) }
      const original = yield* prompt.prompt(input)
      const exact = yield* prompt.prompt(input)
      expect(exact.info.id).toBe(original.info.id)
      const alias = { ...input, messageID: MessageID.ascending() }
      expect((yield* prompt.prompt(alias)).info.id).toBe(original.info.id)
      const before = yield* sessions.messages({ sessionID: session.id })
      expect((yield* prompt.prompt(alias)).info.id).toBe(original.info.id)
      expect(Exit.isFailure(yield* Effect.exit(prompt.prompt({ ...alias, parts: said(text + " changed") })))).toBe(true)
      expect(yield* sessions.messages({ sessionID: session.id })).toEqual(before)
      expect(before).toHaveLength(1)
      expect(yield* llm.calls).toBe(0)
    }),
  30000,
)

unix(
  "round5 overlapping command expansion executes once and binds before a retry enters",
  () =>
    Effect.gen(function* () {
      const instance = yield* TestInstance
      const marker = path.join(instance.directory, "inflight-command.txt")
      const release = path.join(instance.directory, "release-command")
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        command: {
          probe: {
            template: `!\`printf 'execution\\n' >> '${marker}'; while [ ! -f '${release}' ]; do sleep 0.01; done; printf 'prepared once'\``,
          },
        },
      }))
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Held executable preparation" })
      const input = { sessionID: session.id, messageID: MessageID.ascending(), command: "probe", arguments: "" }
      yield* llm.text("executed once")
      const first = yield* prompt.command(input).pipe(Effect.forkChild)
      yield* Effect.addFinalizer(() => writeText(release, "released").pipe(Effect.orDie))
      yield* pollWithTimeout(
        Effect.promise(async () =>
          (await Bun.file(marker).exists()) ? await Bun.file(marker).text() : undefined,
        ).pipe(Effect.map((text) => (text === "execution\n" ? true : undefined))),
        "first executable expansion did not publish its marker",
        "10 seconds",
      )
      const entered = yield* Deferred.make<void>()
      const second = yield* Deferred.succeed(entered, undefined).pipe(
        Effect.andThen(prompt.command(input)),
        Effect.forkChild,
      )
      yield* Deferred.await(entered)
      expect(yield* llm.calls).toBe(0)
      yield* writeText(release, "released")
      const result = yield* awaitWithTimeout(Fiber.join(first), "first command did not finish", "10 seconds")
      const retry = yield* awaitWithTimeout(Fiber.await(second), "second command did not refuse", "10 seconds")
      expect(Exit.isFailure(retry)).toBe(true)
      expect(result.info.role).toBe("assistant")
      expect(yield* Effect.promise(() => Bun.file(marker).text())).toBe("execution\n")
      expect(yield* llm.calls).toBe(1)
      expect(
        (yield* sessions.messages({ sessionID: session.id })).filter((message) => message.info.role === "user"),
      ).toHaveLength(1)
    }),
  30000,
)

unix(
  "round5 foreign projected identity appearing during expansion refuses admission without a DB IO lock",
  () =>
    Effect.gen(function* () {
      const instance = yield* TestInstance
      const marker = path.join(instance.directory, "foreign-command.txt")
      const release = path.join(instance.directory, "release-foreign-command")
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        command: {
          probe: {
            template: `!\`printf 'execution\\n' >> '${marker}'; while [ ! -f '${release}' ]; do sleep 0.01; done; printf 'prepared once'\``,
          },
        },
      }))
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ title: "Foreign identity during IO" })
      const input = { sessionID: session.id, messageID: MessageID.ascending(), command: "probe", arguments: "" }
      const first = yield* prompt.command(input).pipe(Effect.forkChild)
      yield* Effect.addFinalizer(() => writeText(release, "released").pipe(Effect.orDie))
      yield* pollWithTimeout(
        Effect.promise(async () => ((await Bun.file(marker).exists()) ? true : undefined)),
        "command did not enter expansion",
        "10 seconds",
      )
      const bare = yield* awaitWithTimeout(
        sessions.updateMessage({
          id: input.messageID,
          sessionID: session.id,
          role: "user",
          agent: "build",
          model: ref,
          time: { created: 456 },
        }),
        "expansion held a DB transaction",
        "2 seconds",
      )
      const before = yield* sessions.messages({ sessionID: session.id })
      yield* writeText(release, "released")
      const result = yield* awaitWithTimeout(Fiber.await(first), "foreign identity did not refuse", "10 seconds")
      expect(Exit.isFailure(result)).toBe(true)
      expect(yield* sessions.messages({ sessionID: session.id })).toEqual(before)
      expect((yield* sessions.messages({ sessionID: session.id }))[0]?.info.id).toBe(bare.id)
      expect(yield* llm.calls).toBe(0)
    }),
  30000,
)

it.instance(
  "round5 SDK fixture replaces an unrelated installed package with the exact workspace source",
  () =>
    Effect.gen(function* () {
      const instance = yield* TestInstance
      const dir = path.join(instance.directory, ".opencode")
      const parent = path.join(dir, "node_modules/@opencode-ai")
      const entry = path.join(parent, "plugin")
      const tempRoot = yield* Effect.promise(() => realpath(os.tmpdir()))
      expect(process.env["OPENCODE_CONFIG"]).toBeUndefined()
      expect(process.env["OPENCODE_CONFIG_CONTENT"]).toBeUndefined()
      expect(process.env["OPENCODE_CONFIG_DIR"]).toBeUndefined()
      expect(process.env["OPENCODE_DISABLE_PROJECT_CONFIG"]).toBeUndefined()
      expect(
        [
          Global.Path.data,
          Global.Path.cache,
          Global.Path.config,
          Global.Path.state,
          Global.Path.tmp,
          Global.Path.home,
          process.env["OPENCODE_TEST_MANAGED_CONFIG_DIR"]!,
          process.env["XDG_DATA_HOME"]!,
          process.env["XDG_CACHE_HOME"]!,
          process.env["XDG_CONFIG_HOME"]!,
          process.env["XDG_STATE_HOME"]!,
          instance.directory,
        ].every((candidate) => FSUtil.contains(tempRoot, path.resolve(candidate))),
      ).toBe(true)

      const source = yield* Effect.promise(() => createPluginSourceSurrogate(tempRoot))
      const before = yield* Effect.promise(() => snapshotTree(source.directory))
      yield* Effect.addFinalizer(() => Effect.promise(() => rm(source.directory, { recursive: true, force: true })))
      yield* Effect.promise(async () => {
        await mkdir(path.dirname(parent), { recursive: true })
        await symlink(source.scope, parent, "junction")
      })
      expect((yield* Effect.promise(() => lstat(parent))).isSymbolicLink()).toBe(true)
      expect(yield* Effect.promise(() => realpath(entry))).toBe(source.packageDirectory)

      const gate = { dir, entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      const activity = {
        dir,
        requests: 0,
        active: 0,
        maxActive: 0,
        completed: 0,
        firstInstallSucceeded: false,
        secondRequested: yield* Deferred.make<void>(),
      }
      let unrelatedInstalled:
        | undefined
        | { target: string; sameStagedDirectory: boolean; manifest: string; module: string }
      sdkReadiness.gate = gate
      sdkReadiness.activity = activity
      sdkReadiness.unrelated = {
        dir,
        pkg: {
          manifest: {
            name: "@opencode-ai/plugin",
            version: "1.19.0",
            type: "module",
            exports: "./index.js",
          },
          module: "export const tool = () => 'wrong SDK'\n",
          afterInstalled: async ({ entry, device, inode }) => {
            const installed = await lstat(entry)
            unrelatedInstalled = {
              target: await realpath(entry),
              sameStagedDirectory: installed.dev === device && installed.ino === inode,
              manifest: await readFile(path.join(entry, "package.json"), "utf8"),
              module: await readFile(path.join(entry, "index.js"), "utf8"),
            }
          },
        },
      }
      const npm = yield* Npm.Service
      const config = yield* Config.Service
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(gate.release, undefined).pipe(Effect.asVoid, Effect.ignore)
          yield* config.waitForDependencies().pipe(Effect.ignore)
          if (sdkReadiness.gate === gate) sdkReadiness.gate = undefined
          if (sdkReadiness.activity === activity) sdkReadiness.activity = undefined
          if (sdkReadiness.unrelated?.dir === dir) sdkReadiness.unrelated = undefined
        }),
      )

      yield* config.get()
      yield* awaitWithTimeout(Deferred.await(gate.entered), "Config did not start the scoped SDK install")
      expect(activity.requests).toBe(1)
      const direct = yield* npm.install(dir, { add: [{ name: "@opencode-ai/plugin" }] }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(
        Deferred.await(activity.secondRequested),
        "direct SDK install did not overlap Config dependency setup",
      )
      yield* Deferred.succeed(gate.release, undefined)
      yield* awaitWithTimeout(config.waitForDependencies(), "Config SDK dependencies did not settle", "10 seconds")
      const directExit = yield* awaitWithTimeout(Fiber.await(direct), "direct SDK install did not settle", "10 seconds")
      expect(Exit.isSuccess(directExit)).toBe(true)
      expect(activity.firstInstallSucceeded).toBe(true)
      expect(activity.completed).toBeGreaterThanOrEqual(2)
      expect(unrelatedInstalled).toBeDefined()
      if (!unrelatedInstalled) throw new Error("Config install never placed the unrelated package at its entry")
      expect(FSUtil.contains(instance.directory, unrelatedInstalled.target)).toBe(true)
      expect(unrelatedInstalled.sameStagedDirectory).toBe(true)
      expect(unrelatedInstalled.manifest).toContain('"version":"1.19.0"')
      expect(unrelatedInstalled.manifest).toContain('"exports":"./index.js"')
      expect(unrelatedInstalled.module).toContain("wrong SDK")

      const directories = yield* config.directories()
      const resolvedDirectories = yield* Effect.promise(() =>
        Promise.all(directories.map((directory) => realpath(directory))),
      )
      expect(resolvedDirectories.every((directory) => FSUtil.contains(tempRoot, directory))).toBe(true)
      expect(activity.requests).toBeGreaterThanOrEqual(2)
      expect(activity.maxActive).toBe(1)
      const actual = yield* Effect.promise(() =>
        realpath(createRequire(path.join(dir, "sdk-fixture.cjs")).resolve("@opencode-ai/plugin")),
      )
      const expected = yield* Effect.promise(() =>
        realpath(path.resolve(import.meta.dir, "../../../plugin/src/index.ts")),
      )
      expect(actual).toBe(expected)
      expect(yield* Effect.promise(() => realpath(entry))).toBe(
        yield* Effect.promise(() => realpath(path.resolve(import.meta.dir, "../../../plugin"))),
      )
      const sdk = yield* Effect.promise(() => import(pathToFileURL(actual).href))
      expect(typeof sdk.tool).toBe("function")
      const definition = { description: "workspace SDK fixture", args: {}, execute: async () => "usable tool" }
      expect(sdk.tool(definition)).toEqual(definition)
      expect(sdk.tool.schema.string().safeParse("usable").success).toBe(true)
      expect(yield* Effect.promise(() => snapshotTree(source.directory))).toEqual(before)
    }),
  30000,
)

it.instance(
  "round5 SDK fixture failure preserves a linked source manifest and removes staged files",
  () =>
    Effect.gen(function* () {
      const instance = yield* TestInstance
      const dir = path.join(instance.directory, ".opencode")
      const parent = path.join(dir, "node_modules/@opencode-ai")
      const entry = path.join(parent, "plugin")
      const tempRoot = yield* Effect.promise(() => realpath(os.tmpdir()))
      const source = yield* Effect.promise(() => createPluginSourceSurrogate(tempRoot))
      const before = yield* Effect.promise(() => snapshotTree(source.directory))
      yield* Effect.addFinalizer(() => Effect.promise(() => rm(source.directory, { recursive: true, force: true })))
      const originalManifest = yield* Effect.promise(() =>
        readFile(path.join(source.packageDirectory, "package.json"), "utf8"),
      )
      expect(originalManifest).toContain('"exports":"./src/index.ts"')
      yield* Effect.promise(async () => {
        await mkdir(parent, { recursive: true })
        await symlink(source.packageDirectory, entry, "junction")
      })
      expect((yield* Effect.promise(() => lstat(entry))).isSymbolicLink()).toBe(true)
      expect(yield* Effect.promise(() => realpath(entry))).toBe(source.packageDirectory)

      let setupStarted = false
      let stagedManifest = ""
      let stagedModuleExists = true
      const result = yield* awaitWithTimeout(
        Effect.promise(() =>
          installWorkspacePluginSdk({
            sandboxRoot: tempRoot,
            ownerRoot: instance.directory,
            consumerDirectory: dir,
            workspace: path.resolve(import.meta.dir, "../../../plugin"),
            unrelated: {
              manifest: {
                name: "@opencode-ai/plugin",
                version: "1.19.0",
                type: "module",
                exports: "./index.js",
              },
              module: "export const tool = () => 'wrong SDK'\n",
              afterManifest: async ({ staging }) => {
                setupStarted = true
                stagedManifest = await readFile(path.join(staging, "package.json"), "utf8")
                stagedModuleExists = await Bun.file(path.join(staging, "index.js")).exists()
                throw new Error("injected SDK fixture setup failure")
              },
            },
          }),
        ).pipe(Effect.exit),
        "SDK fixture failure injection did not settle",
        "5 seconds",
      )
      expect(Exit.isFailure(result)).toBe(true)
      if (Exit.isFailure(result)) expect(Cause.pretty(result.cause)).toContain("injected SDK fixture setup failure")
      expect(setupStarted).toBe(true)
      expect(stagedManifest).toContain('"exports":"./index.js"')
      expect(stagedModuleExists).toBe(false)
      const resolved = createRequire(path.join(dir, "sdk-fixture.cjs")).resolve("@opencode-ai/plugin")
      expect(yield* Effect.promise(() => realpath(resolved))).toBe(path.join(source.packageDirectory, "src/index.ts"))
      expect(yield* Effect.promise(() => readlink(entry))).toBe(source.packageDirectory)
      expect(yield* Effect.promise(() => snapshotTree(source.directory))).toEqual(before)
      expect(
        (yield* Effect.promise(() => readdir(instance.directory))).some((name) =>
          name.startsWith(".plugin-sdk-fixture-"),
        ),
      ).toBe(false)
    }),
  30000,
)

unix(
  "round5 SDK fixture subprocess death reclaims enclosing scratch and preserves linked source",
  () =>
    Effect.gen(function* () {
      const instance = yield* TestInstance
      const tempRoot = yield* Effect.promise(() => realpath(os.tmpdir()))
      const source = yield* Effect.promise(() => createPluginSourceSurrogate(tempRoot))
      const before = yield* Effect.promise(() => snapshotTree(source.directory))
      yield* Effect.addFinalizer(() => Effect.promise(() => rm(source.directory, { recursive: true, force: true })))

      const fixtureDirectory = yield* Effect.scoped(
        Effect.gen(function* () {
          const fixtureRoot = yield* tmpdirScoped()
          const dir = path.join(fixtureRoot, ".opencode")
          const parent = path.join(dir, "node_modules/@opencode-ai")
          const marker = path.join(fixtureRoot, "sdk-fixture-progress.json")
          const workspace = path.join(fixtureRoot, "workspace-plugin")
          yield* Effect.promise(async () => {
            await mkdir(path.dirname(parent), { recursive: true })
            await mkdir(workspace)
            await symlink(source.scope, parent, "junction")
          })

          const helper = pathToFileURL(path.resolve(import.meta.dir, "../fixture/plugin-sdk.ts")).href
          const script = `
            import { installWorkspacePluginSdk } from ${JSON.stringify(helper)}
            import { lstat, realpath } from "node:fs/promises"
            import path from "node:path"
            await installWorkspacePluginSdk({
              sandboxRoot: ${JSON.stringify(tempRoot)},
              ownerRoot: ${JSON.stringify(fixtureRoot)},
              consumerDirectory: ${JSON.stringify(dir)},
              workspace: ${JSON.stringify(workspace)},
              unrelated: {
                manifest: { name: "@opencode-ai/plugin", version: "1.19.0", type: "module", exports: "./index.js" },
                module: "export const tool = () => 'wrong SDK'\\n",
                afterManifest: async ({ entry, staging }) => {
                  await Bun.write(${JSON.stringify(marker)}, JSON.stringify({
                    entry,
                    entryExists: await lstat(entry).then(() => true, () => false),
                    parentTarget: await realpath(path.dirname(entry)),
                    staging,
                    stagingTarget: await realpath(staging),
                  }))
                  await new Promise(() => {})
                },
              },
            })
          `
          const child = Bun.spawn([process.execPath, "-e", script], {
            cwd: fixtureRoot,
            env: { ...process.env },
            stdin: "ignore",
            stdout: "ignore",
            stderr: "pipe",
          })
          yield* Effect.addFinalizer(() =>
            Effect.promise(async () => {
              if (child.exitCode === null) child.kill("SIGKILL")
              await child.exited
            }),
          )
          yield* pollWithTimeout(
            Effect.promise(async () => ((await Bun.file(marker).exists()) ? true : undefined)),
            "SDK fixture child did not publish setup progress",
            "5 seconds",
          )
          const progress = JSON.parse(yield* Effect.promise(() => readFile(marker, "utf8"))) as {
            entry: string
            entryExists: boolean
            parentTarget: string
            staging: string
            stagingTarget: string
          }
          expect(FSUtil.contains(fixtureRoot, progress.staging)).toBe(true)
          expect(FSUtil.contains(fixtureRoot, progress.entry)).toBe(true)
          child.kill("SIGKILL")
          const exitCode = yield* awaitWithTimeout(
            Effect.promise(() => child.exited),
            "SDK fixture child did not exit",
            "5 seconds",
          )
          expect(exitCode).not.toBe(0)
          return { fixtureRoot, progress }
        }),
      )

      expect(yield* Effect.promise(() => snapshotTree(source.directory))).toEqual(before)
      expect(FSUtil.contains(fixtureDirectory.fixtureRoot, fixtureDirectory.progress.stagingTarget)).toBe(true)
      expect(FSUtil.contains(fixtureDirectory.fixtureRoot, fixtureDirectory.progress.parentTarget)).toBe(true)
      expect(fixtureDirectory.progress.entryExists).toBe(false)
      expect(yield* Effect.promise(() => readdir(tempRoot))).not.toContain(path.basename(fixtureDirectory.fixtureRoot))
    }),
  30000,
)

unix(
  "round6 held executable IO allows critical steer direct prompt and active cancellation",
  () =>
    Effect.gen(function* () {
      const instance = yield* TestInstance
      const marker = path.join(instance.directory, "round6-control-entered")
      const release = path.join(instance.directory, "round6-control-release")
      const held = yield* startHeld({
        config: (url) => ({
          ...providerCfg(url),
          command: {
            probe: {
              template: `!\`printf 'execution\\n' >> '${marker}'; while [ ! -f '${release}' ]; do sleep 0.01; done; printf 'held command intent'\``,
            },
          },
        }),
      })
      const expanding = yield* held.prompt
        .command({
          sessionID: held.chat.id,
          messageID: MessageID.ascending(),
          command: "probe",
          arguments: "",
          delivery: "queue",
        })
        .pipe(Effect.forkChild)
      yield* Effect.addFinalizer(() => writeText(release, "released").pipe(Effect.orDie))
      yield* pollWithTimeout(
        Effect.promise(async () => ((await Bun.file(marker).exists()) ? true : undefined)),
        "command never entered external preparation",
      )
      const critical =
        "[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791854000-998-abcd\x1f [fm-level:critical]\x1f Critical full envelope while expansion is held"
      const criticalID = MessageID.ascending()
      const sent = yield* held.send(critical, { messageID: criticalID })
      const visible = yield* pollWithTimeout(
        held.sessions
          .messages({ sessionID: held.chat.id })
          .pipe(
            Effect.map((messages) =>
              messages.some((message) =>
                message.parts.some((part) => part.type === "text" && part.text.includes(critical)),
              )
                ? true
                : undefined,
            ),
          ),
        "critical steer blocked by expansion",
        "2 seconds",
      ).pipe(Effect.exit)
      const criticalProjection = (yield* held.sessions.messages({ sessionID: held.chat.id })).find((message) =>
        message.parts.some((part) => part.type === "text" && part.text.includes(critical)),
      )
      const directID = MessageID.ascending()
      const direct = yield* awaitWithTimeout(
        held.prompt.prompt({
          sessionID: held.chat.id,
          messageID: directID,
          agent: "build",
          model: ref,
          noReply: true,
          parts: said("independent direct original"),
        }),
        "direct prompt blocked by expansion",
        "2 seconds",
      ).pipe(Effect.exit)
      const cancellation = yield* awaitWithTimeout(
        held.prompt.cancel(held.chat.id),
        "active cancel blocked by expansion",
        "2 seconds",
      ).pipe(Effect.exit)
      const terminal = yield* awaitWithTimeout(
        Fiber.await(held.task),
        "active provider remained running after cancellation",
        "2 seconds",
      ).pipe(Effect.exit)
      if (Exit.isFailure(visible) || Exit.isFailure(direct) || Exit.isFailure(cancellation) || Exit.isFailure(terminal))
        yield* Effect.sync(() =>
          console.error("held executable progress failures", {
            critical: Exit.isFailure(visible) ? Cause.pretty(visible.cause) : "Success",
            direct: Exit.isFailure(direct) ? Cause.pretty(direct.cause) : "Success",
            cancel: Exit.isFailure(cancellation) ? Cause.pretty(cancellation.cause) : "Success",
            providerTerminal: Exit.isFailure(terminal) ? Cause.pretty(terminal.cause) : "Success",
          }),
        )
      const callsBeforeRelease = yield* held.llm.calls
      const stillHeld = !(yield* Effect.promise(() => Bun.file(release).exists()))
      yield* held.llm.text("critical answered")
      yield* held.llm.text("command answered")
      yield* writeText(release, "released")
      yield* held.release
      yield* finish(held.task, sent, expanding)
      expect(Exit.isSuccess(visible)).toBe(true)
      const witness = criticalProjection?.parts.flatMap((part) =>
        part.type === "text" && !part.ignored && part.metadata?.sessionQueue ? [part.metadata.sessionQueue] : [],
      )[0]
      const original = Schema.decodeUnknownSync(SessionPromptQueue.Group)(witness).constituents[0]
      expect(original?.delivery).toBe("steer")
      expect(original?.input.messageID).toBe(criticalID)
      expect(original?.input.parts).toEqual(said(critical))
      expect(Exit.isSuccess(direct)).toBe(true)
      if (Exit.isSuccess(direct)) expect(direct.value.info.id).toBe(directID)
      expect(Exit.isSuccess(cancellation)).toBe(true)
      expect(Exit.isSuccess(terminal)).toBe(true)
      expect(callsBeforeRelease).toBe(1)
      expect(stillHeld).toBe(true)
      expect(yield* Effect.promise(() => Bun.file(marker).text())).toBe("execution\n")
    }),
  30000,
)

unix(
  "round6 ordinary peer provider continuation completes while command expansion remains held",
  () =>
    Effect.gen(function* () {
      const instance = yield* TestInstance
      const file = path.join(instance.directory, "round6-real-read.txt")
      const marker = path.join(instance.directory, "round6-continuation-entered")
      const release = path.join(instance.directory, "round6-continuation-release")
      yield* writeText(file, "retained real read result")
      const held = yield* startHeld({
        tool: { name: "read", args: { filePath: file } },
        config: (url) => ({
          ...providerCfg(url),
          command: {
            probe: {
              template: `!\`printf 'execution\\n' >> '${marker}'; while [ ! -f '${release}' ]; do sleep 0.01; done; printf 'deferred command intent'\``,
            },
          },
        }),
      })
      const expanding = yield* held.prompt
        .command({
          sessionID: held.chat.id,
          messageID: MessageID.ascending(),
          command: "probe",
          arguments: "",
          delivery: "queue",
        })
        .pipe(Effect.forkChild)
      yield* Effect.addFinalizer(() => writeText(release, "released").pipe(Effect.orDie))
      yield* pollWithTimeout(
        Effect.promise(async () => ((await Bun.file(marker).exists()) ? true : undefined)),
        "command preparation did not enter",
      )
      const input = {
        messageID: MessageID.ascending(),
        text: "[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791854000-999-abcd\x1f [fm-level:request]\x1f Ordinary peer during expansion\nComplete original tail",
      }
      const sent = yield* held.send(input.text, { messageID: input.messageID })
      const original = (yield* awaitWithTimeout(
        queued(held.chat.id, 1),
        "ordinary peer admission blocked by expansion",
        "2 seconds",
      ))[0]
      if (!original) throw new Error("missing admitted peer")
      expect(original.input.messageID).toBe(input.messageID)
      // Retain a caller in this scope so reply custody remains readable after child callers finish.
      const custody = yield* held.queue.admit({
        ...original.input,
        sessionID: held.chat.id,
        delivery: original.delivery,
      })
      expect(custody.id).toBe(original.id)
      yield* held.llm.text("peer continuation complete")
      yield* held.release
      yield* awaitWithTimeout(held.llm.wait(2), "continuation blocked by command expansion", "2 seconds")
      const replies = yield* finish(held.task, sent)
      expect(replies[0]?.info.id).toBe(replies[1]?.info.id)
      expect((yield* held.queue.reply(original.id))?.info.id).toBe(replies[1]?.info.id)
      const requests = yield* held.llm.inputs
      expect(JSON.stringify(requests[1]?.messages)).toContain("retained real read result")
      expect(JSON.stringify(requests[1]?.messages)).toContain("Complete original tail")
      expect(yield* held.queue.list(held.chat.id)).toEqual([])
      expect(yield* held.llm.calls).toBe(2)
      expect(yield* Effect.promise(() => Bun.file(release).exists())).toBe(false)
      yield* held.llm.text("command complete")
      yield* writeText(release, "released")
      yield* finish(expanding)
      expect(yield* held.llm.calls).toBe(3)
      expect(yield* Effect.promise(() => Bun.file(marker).text())).toBe("execution\n")
    }),
  30000,
)

gated.instance(
  "round6 command pre-execution hook can await a distinct same-session direct prompt",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        command: { probe: { template: "plain command intent" } },
      }))
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const directID = MessageID.ascending()
      const observed: SessionV1.WithParts[] = []
      const entered = yield* Deferred.make<void>()
      const completed = yield* Deferred.make<void>()
      const providerRelease = yield* Deferred.make<void>()
      gates.commandBefore = Deferred.succeed(entered, undefined).pipe(
        Effect.andThen(
          prompt.prompt({
            sessionID: session.id,
            messageID: directID,
            model: ref,
            agent: "build",
            noReply: true,
            parts: said("direct input from awaited hook"),
          }),
        ),
        Effect.tap((message) =>
          Effect.sync(() => {
            observed.push(message)
          }),
        ),
        Effect.andThen(Deferred.succeed(completed, undefined)),
        Effect.asVoid,
      )
      yield* llm.hold("command complete", deferredAsPromise(providerRelease))
      const task = yield* prompt
        .command({ sessionID: session.id, messageID: MessageID.ascending(), command: "probe", arguments: "" })
        .pipe(Effect.forkChild)
      yield* Effect.addFinalizer(() => Deferred.succeed(providerRelease, undefined).pipe(Effect.asVoid))
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          gates.commandBefore = undefined
        }),
      )
      yield* awaitWithTimeout(Deferred.await(entered), "command setup never reached pre-execution hook", "10 seconds")
      yield* awaitWithTimeout(Deferred.await(completed), "awaited hook direct input did not complete", "2 seconds")
      yield* awaitWithTimeout(
        Effect.raceFirst(
          llm.wait(1),
          Fiber.join(task).pipe(Effect.andThen(Effect.fail(new Error("command ended before provider request")))),
        ),
        "command provider never started",
        "10 seconds",
      )
      // The actual hook is complete. Holding provider completion past its bound must not fail that phase.
      yield* Effect.sleep("2100 millis")
      const held = task.pollUnsafe()
      if (held)
        yield* Effect.sync(() =>
          console.error("command ended after hook completion", {
            observed: observed.length,
            cause: Exit.isFailure(held) ? Cause.pretty(held.cause) : "Success",
          }),
        )
      yield* Deferred.succeed(providerRelease, undefined)
      const result = yield* Fiber.join(task)
      expect(held).toBeUndefined()
      expect(observed).toHaveLength(1)
      expect(observed[0]?.info.id).toBe(directID)
      expect(result.info.role).toBe("assistant")
      expect(yield* llm.calls).toBe(1)
      expect(
        (yield* sessions.messages({ sessionID: session.id })).filter((message) => message.info.role === "user"),
      ).toHaveLength(2)
    }),
  30000,
)

unix(
  "round7 interrupted real expansion keeps its identity until external termination",
  () =>
    Effect.gen(function* () {
      const instance = yield* TestInstance
      const marker = path.join(instance.directory, "round7-native-entered")
      const ended = path.join(instance.directory, "round7-native-ended")
      const release = path.join(instance.directory, "round7-native-release")
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        command: {
          probe: {
            template: `!\`printf 'execution\\n' >> '${marker}'; while [ ! -f '${release}' ]; do sleep 0.01; done; printf 'terminated\\n' >> '${ended}'; printf 'cancelled intent'\``,
          },
        },
      }))
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const input = { sessionID: session.id, messageID: MessageID.ascending(), command: "probe", arguments: "" }
      const lines = () =>
        Effect.promise(async () =>
          (await Bun.file(marker).exists()) ? (await Bun.file(marker).text()).trim().split("\n").length : 0,
        )
      yield* llm.text("unexpected retry answer")
      const first = yield* prompt.command(input).pipe(Effect.forkChild)
      yield* Effect.addFinalizer(() => writeText(release, "released").pipe(Effect.orDie))
      yield* pollWithTimeout(
        lines().pipe(Effect.map((count) => (count === 1 ? true : undefined))),
        "native expansion did not enter",
      )
      const stopping = yield* Fiber.interrupt(first).pipe(Effect.forkChild({ startImmediately: true }))
      const second = yield* prompt.command(input).pipe(Effect.forkChild)
      yield* Effect.addFinalizer(() => writeText(release, "released").pipe(Effect.orDie))
      const outcome = yield* Effect.raceFirst(
        Fiber.await(second).pipe(
          Effect.map((exit) =>
            Exit.isFailure(exit) && Cause.squash(exit.cause) instanceof SessionQueue.HistoricalIdentityError
              ? "refused"
              : "unexpected terminal",
          ),
        ),
        pollWithTimeout(
          lines().pipe(Effect.map((count) => (count === 2 ? true : undefined))),
          "retry did not expand",
          "2 seconds",
        ).pipe(Effect.as("expanded")),
      )
      const beforeRelease = {
        outcome,
        markers: yield* lines(),
        released: yield* Effect.promise(() => Bun.file(release).exists()),
        firstExit: first.pollUnsafe()?._tag,
        calls: yield* llm.calls,
      }
      const direct = yield* awaitWithTimeout(
        prompt.prompt({
          sessionID: session.id,
          messageID: MessageID.ascending(),
          noReply: true,
          agent: "build",
          model: ref,
          parts: said("distinct input during interrupted expansion"),
        }),
        "unrelated input blocked by interrupted preparation",
        "2 seconds",
      )
      yield* writeText(release, "released")
      const firstExit = yield* awaitWithTimeout(
        Fiber.await(first),
        "interrupted expansion did not settle after native termination",
        "10 seconds",
      )
      yield* Fiber.join(stopping)
      yield* awaitWithTimeout(Fiber.await(second), "retry did not settle", "10 seconds")
      expect(beforeRelease).toEqual({ outcome: "refused", markers: 1, released: false, firstExit: undefined, calls: 0 })
      expect(direct.info.role).toBe("user")
      expect(Exit.isFailure(firstExit) && Cause.hasInterrupts(firstExit.cause)).toBe(true)
      expect(yield* Effect.promise(() => Bun.file(ended).text())).toBe("terminated\n")
      expect(yield* lines()).toBe(1)
      expect(yield* llm.calls).toBe(0)
      expect(
        (yield* sessions.messages({ sessionID: session.id })).filter((message) => message.info.role === "user"),
      ).toHaveLength(1)
    }),
  30000,
)

unix(
  "round7 failed parallel expansion keeps custody until its surviving native child exits",
  () =>
    Effect.gen(function* () {
      const instance = yield* TestInstance
      const marker = path.join(instance.directory, "round7-parallel-entered")
      const ended = path.join(instance.directory, "round7-parallel-ended")
      const release = path.join(instance.directory, "round7-parallel-release")
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        command: {
          probe: {
            template: `!\`printf 'execution\\n' >> '${marker}'; while [ ! -f '${release}' ]; do sleep 0.01; done; printf 'terminated\\n' >> '${ended}'\` !\`printf 'invalid\0argument'\``,
          },
        },
      }))
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const input = { sessionID: session.id, messageID: MessageID.ascending(), command: "probe", arguments: "" }
      const first = yield* prompt.command(input).pipe(Effect.forkChild)
      yield* Effect.addFinalizer(() => writeText(release, "released").pipe(Effect.orDie))
      yield* pollWithTimeout(
        Effect.promise(async () => ((await Bun.file(marker).exists()) ? true : undefined)),
        "parallel surviving child did not enter",
      )
      const second = yield* prompt.command(input).pipe(Effect.forkChild)
      yield* Effect.addFinalizer(() => writeText(release, "released").pipe(Effect.orDie))
      const retry = yield* awaitWithTimeout(
        Fiber.await(second),
        "parallel failure released or stalled identity before surviving IO ended",
        "2 seconds",
      )
      const pending = first.pollUnsafe()
      const before = yield* Effect.promise(() => Bun.file(marker).text())
      const stillRunning = !(yield* Effect.promise(() => Bun.file(ended).exists()))
      yield* writeText(release, "released")
      const failed = yield* Fiber.await(first)
      expect(Exit.isFailure(retry) && Cause.squash(retry.cause) instanceof SessionQueue.HistoricalIdentityError).toBe(
        true,
      )
      expect(pending).toBeUndefined()
      expect(before).toBe("execution\n")
      expect(stillRunning).toBe(true)
      expect(Exit.isFailure(failed)).toBe(true)
      if (Exit.isFailure(failed)) expect(Cause.pretty(failed.cause)).toContain("null bytes")
      expect(yield* Effect.promise(() => Bun.file(ended).text())).toBe("terminated\n")
      expect(yield* sessions.messages({ sessionID: session.id })).toEqual([])
      expect(yield* llm.calls).toBe(0)
    }),
  30000,
)

gated.instance(
  "round7 interrupted promise hook keeps custody until its returned work settles",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        command: { probe: { template: "promise hook intent" } },
      }))
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const entered = defer<void>()
      const release = defer<void>()
      const markers: string[] = []
      const hook = Effect.promise(() => {
        markers.push("entered")
        entered.resolve()
        return release.promise.then(() => {
          markers.push("terminated")
        })
      })
      gates.commandBefore = hook
      const input = { sessionID: session.id, messageID: MessageID.ascending(), command: "probe", arguments: "" }
      yield* llm.text("unexpected retry answer")
      const first = yield* prompt.command(input).pipe(Effect.forkChild)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          release.resolve()
          gates.commandBefore = undefined
        }),
      )
      yield* awaitWithTimeout(
        Effect.promise(() => entered.promise),
        "command did not enter its native promise hook",
        "10 seconds",
      )
      gates.commandBefore = hook
      const stopping = yield* Fiber.interrupt(first).pipe(Effect.forkChild({ startImmediately: true }))
      const second = yield* prompt.command(input).pipe(Effect.forkChild)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          release.resolve()
          gates.commandBefore = undefined
        }),
      )
      const outcome = yield* Effect.raceFirst(
        Fiber.await(second).pipe(
          Effect.map((exit) =>
            Exit.isFailure(exit) && Cause.squash(exit.cause) instanceof SessionQueue.HistoricalIdentityError
              ? "refused"
              : "unexpected terminal",
          ),
        ),
        pollWithTimeout(
          Effect.sync(() => (markers.filter((marker) => marker === "entered").length === 2 ? true : undefined)),
          "retry did not enter native hook",
          "2 seconds",
        ).pipe(Effect.as("entered twice")),
      )
      const before = { outcome, markers: [...markers], firstExit: first.pollUnsafe()?._tag, calls: yield* llm.calls }
      release.resolve()
      yield* Fiber.join(stopping)
      const firstExit = yield* Fiber.await(first)
      yield* Fiber.await(second)
      gates.commandBefore = undefined
      expect(before).toEqual({ outcome: "refused", markers: ["entered"], firstExit: undefined, calls: 0 })
      expect(markers).toEqual(["entered", "terminated"])
      expect(Exit.isFailure(firstExit) && Cause.hasInterrupts(firstExit.cause)).toBe(true)
      expect(yield* sessions.messages({ sessionID: session.id })).toEqual([])
      expect(yield* llm.calls).toBe(0)
    }),
  30000,
)
