import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { eq } from "drizzle-orm"
import { EventV2Bridge } from "@/event-v2-bridge"
import { expect } from "bun:test"
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Layer, Option, Schema } from "effect"
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
import { SessionMessageTable } from "@opencode-ai/core/session/sql"
import { SessionPromptQueueSequenceTable, SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import { LLM } from "../../src/session/llm"
import { MessageV2 } from "../../src/session/message-v2"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { SessionCompaction } from "../../src/session/compaction"
import { SessionSummary } from "../../src/session/summary"
import { Instruction } from "../../src/session/instruction"
import { SessionProcessor } from "../../src/session/processor"
import { SessionPrompt } from "../../src/session/prompt"
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
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { Format } from "../../src/format"
import { TestInstance } from "../fixture/fixture"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"
import { reply, TestLLMServer } from "../lib/llm-server"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { LocationServiceMap, locationServiceMapLayer } from "@opencode-ai/core/location-services"

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
const blockingProcessor = Layer.succeed(
  SessionProcessor.Service,
  SessionProcessor.Service.of({
    create: () => Effect.sync(() => processorCreateStarted.shift()?.()).pipe(Effect.andThen(Effect.never)),
  }),
)

const runtimeFlags = RuntimeFlags.layer({ experimentalEventSystem: true })

const testLLMServerNode = LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })

const promptRoot = LayerNode.group([
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

function makePrompt(input?: { mcpInstructions?: MCP.ServerInstructions[]; processor?: "blocking" }) {
  const replacements = [
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp(input?.mcpInstructions)],
    [RuntimeFlags.node, runtimeFlags],
  ] as const
  if (input?.processor === "blocking") {
    return LayerNode.compile(promptRoot, [...replacements, [SessionProcessor.node, blockingProcessor]])
  }
  return LayerNode.compile(promptRoot, replacements)
}

function makeHttp(input?: { mcpInstructions?: MCP.ServerInstructions[]; processor?: "blocking" }) {
  const root = LayerNode.group([promptRoot, testLLMServerNode])
  const replacements = [
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp(input?.mcpInstructions)],
    [RuntimeFlags.node, runtimeFlags],
  ] as const
  if (input?.processor === "blocking") {
    return LayerNode.compile(root, [...replacements, [SessionProcessor.node, blockingProcessor]])
  }
  return LayerNode.compile(root, replacements)
}

function makeHttpNoLLMServer(input?: { mcpInstructions?: MCP.ServerInstructions[]; processor?: "blocking" }) {
  return makePrompt(input)
}

// Production-boundary gates for the V1 lost-wakeup regression. `finishingRead`
// holds the next Session.findMessage call, which is the finishing run's
// lastAssistant read; `nextEnsureRunning` resolves once the next caller of
// SessionRunState.ensureRunning has joined or started a run, and records which.
const gates = {
  finishingRead: undefined as undefined | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> },
  nextEnsureRunning: undefined as undefined | { reached: Deferred.Deferred<void>; startedRun: boolean },
  // Holds a compaction between its summary and its continue message.
  compactionContinue: undefined as undefined | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> },
}

const gatedSession = LayerNode.make({
  service: Session.Service,
  layer: Layer.effect(
    Session.Service,
    Effect.gen(function* () {
      const real = yield* Session.Service
      return Session.Service.of({
        ...real,
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

const gatedRunState = LayerNode.make({
  service: SessionRunState.Service,
  layer: Layer.effect(
    SessionRunState.Service,
    Effect.gen(function* () {
      const real = yield* SessionRunState.Service
      return SessionRunState.Service.of({
        ...real,
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
            return yield* Fiber.join(call)
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

const it = testEffect(makeHttp())
const gated = testEffect(
  LayerNode.compile(LayerNode.group([promptRoot, testLLMServerNode]), [
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp()],
    [RuntimeFlags.node, runtimeFlags],
    [Session.node, gatedSession],
    [SessionRunState.node, gatedRunState],
    [Plugin.node, gatedPlugin],
  ]),
)
const noLLMServer = testEffect(makeHttpNoLLMServer())
const raceNoLLMServer = testEffect(makeHttpNoLLMServer({ processor: "blocking" }))
const withMcpInstructions = testEffect(
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

const useServerConfig = Effect.fn("test.useServerConfig")(function* (config: (url: string) => Partial<ConfigV1.Info>) {
  const { directory: dir } = yield* TestInstance
  const llm = yield* TestLLMServer
  yield* writeConfig(dir, config(llm.url))
  return { dir, llm }
})

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
    yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
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

it.instance("loop surfaces content-filter finishes as session errors", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const events = yield* EventV2Bridge.Service
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
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

    yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.push(reply().text("partial response").contentFilter())

    const result = yield* prompt.loop({ sessionID: chat.id })
    const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: result.info.id })
    yield* off

    expect(yield* llm.hits).toHaveLength(1)
    expect(result.info.role).toBe("assistant")
    expect(stored.info.role).toBe("assistant")
    if (result.info.role === "assistant" && stored.info.role === "assistant") {
      expect(result.info.finish).toBe("content-filter")
      expect(result.info.error).toEqual(expected)
      expect(stored.info.error).toEqual(result.info.error)
      expect(errors).toContainEqual(expected)
    }
    expect(result.parts).toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "text", text: "partial response" })]),
    )
  }),
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
    yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
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

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
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

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello one" }],
    })

    yield* llm.text("world one")

    const first = yield* prompt.loop({ sessionID: session.id })
    expect(first.info.role).toBe("assistant")
    expect(first.parts.some((part) => part.type === "text" && part.text === "world one")).toBe(true)

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
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
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
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
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
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

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
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
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
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

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
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

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
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
    const messages = inputs.at(-1)?.messages
    if (!Array.isArray(messages)) throw new Error("expected LLM messages")
    expect(messages.at(-1)).toEqual({ role: "user", content: "second" })
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
      const messages = (yield* llm.inputs).at(1)?.messages
      if (!Array.isArray(messages)) throw new Error("expected LLM messages")
      expect(messages.at(-1)).toEqual({ role: "user", content: "second" })
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

const modelMessages = (input: Record<string, unknown> | undefined) => {
  const messages = input?.messages
  if (!Array.isArray(messages)) throw new Error("expected LLM messages")
  return messages
}

const lastUser = (input: Record<string, unknown> | undefined) => modelMessages(input).at(-1)

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
  tool?: boolean
  config?: (url: string) => Partial<ConfigV1.Info>
}) {
  const { llm } = yield* useServerConfig(input?.config ?? providerCfg)
  const prompt = yield* SessionPrompt.Service
  const sessions = yield* Session.Service
  const queue = yield* SessionQueue.Service
  const chat = yield* sessions.create({
    title: "Pinned",
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  })
  const gate = yield* Deferred.make<void>()
  yield* input?.tool
    ? llm.push(reply().wait(deferredAsPromise(gate)).tool("first", { value: "first" }))
    : llm.hold("task done", deferredAsPromise(gate))
  const task = yield* prompt
    .prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("start the task") })
    .pipe(Effect.forkChild)
  yield* awaitWithTimeout(llm.wait(1), "first provider call never started", "10 seconds")
  const send = (text: string, extra?: Partial<SessionPrompt.PromptInput>) =>
    prompt
      .prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said(text), ...extra })
      .pipe(Effect.forkChild)
  const release = Deferred.succeed(gate, void 0)
  return { llm, prompt, sessions, queue, chat, task, send, release }
})

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

      const held = yield* send("parked by the error", { delivery: "queue" })
      yield* queued(chat.id, 1)
      yield* release
      yield* finish(task, held)
      const stopped = (yield* sessions.messages({ sessionID: chat.id })).at(-1)?.info
      expect(stopped?.role === "assistant" ? stopped.error?.name : undefined).toBe("APIError")
      expect(yield* llm.calls).toBe(2)
      expect((yield* queue.list(chat.id)).map((item) => item.input.parts)).toEqual([said("parked by the error")])

      yield* llm.text("wake done")
      yield* llm.text("parked done")
      yield* prompt.prompt({ sessionID: chat.id, agent: "build", model: ref, parts: said("wake up") })
      expect((yield* llm.inputs).slice(2).map(lastUser)).toEqual([
        { role: "user", content: "wake up" },
        { role: "user", content: "parked by the error" },
      ])
    }),
  15_000,
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

it.instance(
  "a tool-step turn steered by later prompts: its caller and the steers share the turn's final reply, and each queued caller gets its own turn's",
  () =>
    Effect.gen(function* () {
      const { llm, sessions, chat, task, send, release } = yield* startHeld({ tool: true })
      yield* llm.text("steer answer")
      yield* llm.text("queued answer")
      yield* llm.text("last answer")

      const held = yield* send("queued behind the turn", { delivery: "queue" })
      yield* admitted(chat.id, "queued behind the turn")
      const last = yield* send("queued last", { delivery: "queue" })
      yield* admitted(chat.id, "queued last")
      const first = yield* send("first steer")
      yield* admitted(chat.id, "first steer")
      const second = yield* send("second steer")
      yield* admitted(chat.id, "second steer")
      yield* release
      const [original, firstReply, secondReply, queuedReply, lastReply] = yield* finish(task, first, second, held, last)

      const inputs = yield* llm.inputs
      expect(inputs).toHaveLength(4)
      expect(lastUser(inputs[1])).toEqual({ role: "user", content: "second steer" })
      expect(lastUser(inputs[2])).toEqual({ role: "user", content: "queued behind the turn" })
      const messages = yield* sessions.messages({ sessionID: chat.id })
      const asked = (text: string) =>
        messages.find(
          (msg) => msg.info.role === "user" && msg.parts.some((part) => part.type === "text" && part.text === text),
        )?.info.id
      const texts = (reply: SessionV1.WithParts | undefined) =>
        reply?.parts.flatMap((part) => (part.type === "text" ? [part.text] : []))
      // The task's caller does not stop at its tool-call step: the turn it began
      // ends with the reply to the steers, which it shares with them.
      for (const reply of [original, firstReply, secondReply]) {
        expect(reply?.info.role === "assistant" ? [reply.info.finish, reply.info.parentID] : undefined).toEqual([
          "stop",
          asked("second steer"),
        ])
        expect(texts(reply)).toEqual(["steer answer"])
      }
      expect(queuedReply?.info.role === "assistant" ? queuedReply.info.parentID : undefined).toBe(
        asked("queued behind the turn"),
      )
      // The drain's final message answers the last queued caller, not this one.
      expect(texts(queuedReply)).toEqual(["queued answer"])
      expect(texts(lastReply)).toEqual(["last answer"])
    }),
  15_000,
)

it.instance(
  "two steers promoted into one run get an assistant reply, never their own user message",
  () =>
    Effect.gen(function* () {
      const { llm, chat, task, send, release } = yield* startHeld()
      yield* llm.text("steered reply")

      const first = yield* send("first steer")
      yield* admitted(chat.id, "first steer")
      const second = yield* send("second steer")
      yield* admitted(chat.id, "second steer")
      yield* release
      const [, older, newer] = yield* finish(task, first, second)

      const inputs = yield* llm.inputs
      expect(inputs).toHaveLength(2)
      expect(mentions(inputs[1], "first steer")).toBe(true)
      expect(lastUser(inputs[1])).toEqual({ role: "user", content: "second steer" })
      for (const answer of [older, newer]) {
        expect(answer?.info.role).toBe("assistant")
        expect(answer?.parts.some((part) => part.type === "text" && part.text === "steered reply")).toBe(true)
      }
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
      expect(yield* queue.delivered(item!.id)).toBeUndefined()
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
          .shell({ sessionID: chat.id, agent: "build", command: "printf first && sleep 0.2 && printf second" })
          .pipe(Effect.forkChild)

        yield* pollWithTimeout(
          Effect.gen(function* () {
            const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
            const taskMsg = msgs.find((item) => item.info.role === "assistant")
            const tool = taskMsg ? toolPart(taskMsg.parts) : undefined
            if (tool?.state.status === "running" && tool.state.metadata?.output.includes("first")) return true
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

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
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
    yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
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
