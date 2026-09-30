import { SessionV1 } from "@opencode-ai/core/v1/session"
import { eq } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { EventV2Bridge } from "@/event-v2-bridge"
import { afterAll, expect } from "bun:test"
import { Database as Sqlite } from "bun:sqlite"
import { tool } from "ai"
import { Cause, Effect, Exit, Fiber, Layer, Option, Stream } from "effect"
import { isSqlError } from "effect/unstable/sql/SqlError"
import { rm } from "node:fs/promises"
import path from "path"
import z from "zod"
import type { Agent } from "../../src/agent/agent"
import { Provider } from "@/provider/provider"
import { BackgroundJob } from "@/background/job"

import { Session } from "@/session/session"
import { LLM } from "../../src/session/llm"
import { MessageV2 } from "../../src/session/message-v2"
import { SessionProcessor } from "../../src/session/processor"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionStatus } from "../../src/session/status"
import { SessionSummary } from "../../src/session/summary"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { provideTmpdirInstance, provideTmpdirServer } from "../fixture/fixture"
import { produceSqliteBusyError } from "../fixture/sqlite-lock"
import { testEffect } from "../lib/effect"
import { raw, reply, TestLLMServer } from "../lib/llm-server"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { LLMEvent, Usage } from "@opencode-ai/llm"
import { aggregateSessionStats, displayStats } from "@/cli/cmd/stats"
import { readExport } from "@/cli/cmd/db-export-usage"
import { servedAcrossSession, servedModelLabel } from "@/cli/cmd/run/variant.shared"
import type { SessionMessages } from "@/cli/cmd/run/session.shared"
import type { RunProvider } from "@/cli/cmd/run/types"

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

function routedConfig() {
  return {
    provider: {
      test: {
        name: "Test",
        id: "test",
        env: [],
        npm: "@ai-sdk/openai-compatible",
        models: {
          "request-model": { cost: { input: 10, output: 20 } },
          "served-a": { cost: { input: 1, output: 2 } },
          "served-b": { cost: { input: 3, output: 4 } },
          "unpriced-model": { name: "Unpriced Model" },
          "free-model": { cost: { input: 0, output: 0 } },
        },
        options: { apiKey: "test-key", baseURL: "http://localhost:1/v1" },
      },
    },
  }
}

function routerLabelConfig() {
  const base = cfg.provider.test.models["test-model"]
  return {
    provider: {
      llmrouter: {
        ...cfg.provider.test,
        id: "llmrouter",
        name: "LLMRouter",
        models: {
          auto: { ...base, id: "auto", name: "Auto" },
          "luna-max": { ...base, id: "luna-max", name: "luna-max" },
          "glm-5.3-flash": { ...base, id: "glm-5.3-flash", name: "glm-5.3-flash" },
          "sol-high": { ...base, id: "sol-high", name: "sol-high" },
        },
      },
    },
  }
}

function agent(): Agent.Info {
  return {
    name: "build",
    mode: "primary",
    options: {},
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  }
}

function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const waitFor = <A>(check: Effect.Effect<A | undefined>, message: string) =>
  Effect.gen(function* () {
    const stop = Date.now() + 500
    while (Date.now() < stop) {
      const value = yield* check
      if (value !== undefined) return value
      yield* Effect.sleep("10 millis")
    }
    return yield* Effect.fail(new Error(message))
  })

const user = Effect.fn("TestSession.user")(function* (sessionID: SessionID, text: string) {
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

const assistant = Effect.fn("TestSession.assistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  root: string,
) {
  const session = yield* Session.Service
  const msg: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    sessionID,
    mode: "build",
    agent: "build",
    path: { cwd: root, root },
    cost: 0,
    tokens: {
      total: 0,
      input: 0,
      output: 0,
      reasoning: 0,
      cache: { read: 0, write: 0 },
    },
    modelID: ref.modelID,
    providerID: ref.providerID,
    parentID,
    time: { created: Date.now() },
    finish: "end_turn",
  }
  yield* session.updateMessage(msg)
  return msg
})

const root = LayerNode.group([
  SessionProcessor.node,
  Session.node,
  SessionProjector.node,
  Provider.node,
  Database.node,
  EventV2Bridge.node,
  SessionStatus.node,
  CrossSpawnSpawner.node,
])
const replacements = [
  [SessionSummary.node, summary],
  [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true })],
] as const

const cleanupFault = { failed: false }
const cleanupPartFailure = { failed: false }
const cleanupFaultEvent = LayerNode.make({
  service: EventV2Bridge.Service,
  layer: Layer.effect(
    EventV2Bridge.Service,
    Effect.gen(function* () {
      const real = yield* EventV2Bridge.Service
      const publish: EventV2.Interface["publish"] = (definition, data, options) =>
        Effect.gen(function* () {
          if (definition.type === SessionV1.Event.MessageUpdated.type) {
            const update = data as typeof SessionV1.Event.MessageUpdated.data.Type
            if (update.info.role === "assistant" && update.info.time.completed !== undefined && !cleanupFault.failed) {
              cleanupFault.failed = true
              return yield* Effect.die(new Error("one-shot cleanup persistence failure"))
            }
          }
          return yield* real.publish(definition, data, options)
        })
      return EventV2Bridge.Service.of({
        ...real,
        publish,
      })
    }),
  ).pipe(
    Layer.provide(EventV2Bridge.node.implementation as Layer.Layer<EventV2Bridge.Service, never, EventV2.Service>),
  ),
  deps: [EventV2.node],
})
const cleanupPartFaultEvent = LayerNode.make({
  service: EventV2Bridge.Service,
  layer: Layer.effect(
    EventV2Bridge.Service,
    Effect.gen(function* () {
      const real = yield* EventV2Bridge.Service
      const publish: EventV2.Interface["publish"] = (definition, data, options) =>
        Effect.gen(function* () {
          if (definition.type === SessionV1.Event.PartUpdated.type) {
            const update = data as typeof SessionV1.Event.PartUpdated.data.Type
            if (
              update.part.type === "text" &&
              "time" in update.part &&
              update.part.time?.end !== undefined &&
              !cleanupPartFailure.failed
            ) {
              cleanupPartFailure.failed = true
              return yield* Effect.die(new Error("one-shot text finalization failure"))
            }
          }
          return yield* real.publish(definition, data, options)
        })
      return EventV2Bridge.Service.of({
        ...real,
        publish,
      })
    }),
  ).pipe(
    Layer.provide(EventV2Bridge.node.implementation as Layer.Layer<EventV2Bridge.Service, never, EventV2.Service>),
  ),
  deps: [EventV2.node],
})
type LockTerminalFault = {
  armed: boolean
  remaining: number
  error: unknown
  failed: number
  messageID: MessageID
}
const lockTerminalFaults = new Map<SessionID, LockTerminalFault>()
const trip = <A, E, R>(fault: LockTerminalFault, self: Effect.Effect<A, E, R>) =>
  Effect.suspend(() => {
    if (!fault.armed || fault.remaining === 0) return self
    fault.remaining--
    fault.failed++
    return Effect.die(fault.error)
  })
const lockTerminalSession = LayerNode.make({
  service: Session.Service,
  layer: Layer.effect(
    Session.Service,
    Effect.gen(function* () {
      const real = yield* Session.Service
      return Session.Service.of({
        ...real,
        updatePart: <T extends SessionV1.Part>(part: T) => {
          const fault = lockTerminalFaults.get(part.sessionID)
          if (!fault || part.messageID !== fault.messageID) return real.updatePart(part)
          if (
            part.type === "reasoning" &&
            part.time.end !== undefined &&
            !fault.armed &&
            fault.remaining > 0
          ) {
            fault.armed = true
          }
          return trip(fault, real.updatePart(part))
        },
        updateMessage: <T extends SessionV1.Info>(msg: T) => {
          const fault = lockTerminalFaults.get(msg.sessionID)
          return fault?.messageID === msg.id ? trip(fault, real.updateMessage(msg)) : real.updateMessage(msg)
        },
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
const cleanupFaultEnv = LayerNode.compile(
  LayerNode.group([root, LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })]),
  [...replacements, [EventV2Bridge.node, cleanupFaultEvent]],
)
const itCleanupFault = testEffect(cleanupFaultEnv)

const env = LayerNode.compile(
  LayerNode.group([root, LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })]),
  replacements,
)

const it = testEffect(env)

const routedEvents: LLMEvent[] = []
const routedLLM = Layer.succeed(LLM.Service, LLM.Service.of({ stream: () => Stream.fromIterable(routedEvents) }))
const routedExportDbPath = path.join(import.meta.dir, `.opencode-served-cost-${crypto.randomUUID()}.db`)
const sqliteLockDbPath = path.join(import.meta.dir, `.opencode-sqlite-lock-${crypto.randomUUID()}.db`)
const routerLabelEvents: LLMEvent[] = []
const routerLabelLLM = Layer.succeed(LLM.Service, LLM.Service.of({ stream: () => Stream.fromIterable(routerLabelEvents) }))
const routerLabelDbPath = path.join(import.meta.dir, `.opencode-router-label-${crypto.randomUUID()}.db`)
const sqliteTerminalDbPaths = ([2, 3] as const).map((failures) => ({
  failures,
  path: path.join(import.meta.dir, `.opencode-sqlite-terminal-${failures}-${crypto.randomUUID()}.db`),
}))
const routedEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, routedLLM],
  [Database.node, Database.layerFromPath(routedExportDbPath)],
])
const itRouted = testEffect(routedEnv)
const routerLabelEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, routerLabelLLM],
  [Database.node, Database.layerFromPath(routerLabelDbPath)],
])
const itRouterLabel = testEffect(routerLabelEnv)

afterAll(async () => {
  await Promise.all(
    [
      routedExportDbPath,
      `${routedExportDbPath}-wal`,
      `${routedExportDbPath}-shm`,
      sqliteLockDbPath,
      `${sqliteLockDbPath}-wal`,
      `${sqliteLockDbPath}-shm`,
      ...sqliteTerminalDbPaths.flatMap((item) => [item.path, `${item.path}-wal`, `${item.path}-shm`]),
      routerLabelDbPath,
      `${routerLabelDbPath}-wal`,
      `${routerLabelDbPath}-shm`,
    ].map((file) => rm(file, { force: true })),
  )
})

const providerErrorLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-1", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-1", name: "lookup" }),
        LLMEvent.toolCall({ id: "call-1", name: "lookup", input: {}, providerExecuted: true }),
        LLMEvent.toolResult({
          id: "call-1",
          name: "lookup",
          result: { type: "error", value: "provider boom" },
          providerExecuted: true,
        }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ),
  }),
)
const providerErrorEnv = LayerNode.compile(root, [...replacements, [LLM.node, providerErrorLLM]])
const itProviderError = testEffect(providerErrorEnv)

const fragmentFailureLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.reasoningStart({ id: "reasoning-1" }),
        LLMEvent.reasoningDelta({ id: "reasoning-1", text: "thinking" }),
        LLMEvent.textStart({ id: "text-1" }),
        LLMEvent.textDelta({ id: "text-1", text: "partial" }),
        LLMEvent.providerError({ message: "provider boom" }),
      ),
  }),
)
const fragmentFailureEnv = LayerNode.compile(root, [...replacements, [LLM.node, fragmentFailureLLM]])
const itFragmentFailure = testEffect(fragmentFailureEnv)

const cleanupPartFailureLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.reasoningStart({ id: "reasoning-1" }),
        LLMEvent.reasoningDelta({ id: "reasoning-1", text: "thinking" }),
        LLMEvent.textStart({ id: "text-1" }),
        LLMEvent.textDelta({ id: "text-1", text: "partial" }),
        LLMEvent.toolInputStart({ id: "call-1", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-1", name: "lookup" }),
        LLMEvent.toolCall({ id: "call-1", name: "lookup", input: { query: "weather" } }),
        LLMEvent.providerError({ message: "original provider failure" }),
      ),
  }),
)
const cleanupPartFailureEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, cleanupPartFailureLLM],
  [EventV2Bridge.node, cleanupPartFaultEvent],
])
const itCleanupPartFailure = testEffect(cleanupPartFailureEnv)

const overflowCleanupFailureLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.textStart({ id: "text-1" }),
        LLMEvent.textDelta({ id: "text-1", text: "overflow response" }),
        LLMEvent.stepFinish({ index: 0, reason: "stop", usage: new Usage({ inputTokens: 100, outputTokens: 0 }) }),
        LLMEvent.finish({ reason: "stop" }),
      ),
  }),
)
const overflowCleanupFailureEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, overflowCleanupFailureLLM],
  [EventV2Bridge.node, cleanupPartFaultEvent],
])
const itOverflowCleanupFailure = testEffect(overflowCleanupFailureEnv)

const lockTerminalLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.reasoningStart({ id: "reasoning-1" }),
        LLMEvent.reasoningDelta({ id: "reasoning-1", text: "thinking" }),
        LLMEvent.reasoningEnd({ id: "reasoning-1" }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ),
  }),
)
const sqliteLockFailure = defer<unknown>()
const sqliteLockLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.unwrap(
        Effect.promise(() => sqliteLockFailure.promise).pipe(Effect.map((error) => Stream.die(error))),
      ),
  }),
)
const sqliteLockEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, sqliteLockLLM],
  [Database.node, Database.layerFromPath(sqliteLockDbPath)],
])
const itSqliteLock = testEffect(sqliteLockEnv)

const boot = Effect.fn("test.boot")(function* () {
  const processors = yield* SessionProcessor.Service
  const session = yield* Session.Service
  const provider = yield* Provider.Service
  return { processors, session, provider }
})

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

it.live("session.processor effect tests capture llm input cleanly", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.text("hello")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "hi")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const input = {
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "hi" }],
          tools: {},
        } satisfies LLM.StreamInput

        const value = yield* handle.process(input)
        const parts = yield* MessageV2.parts(msg.id)
        const calls = yield* llm.calls

        expect(value).toBe("continue")
        expect(calls).toBe(1)
        expect(parts.some((part) => part.type === "text" && part.text === "hello")).toBe(true)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests preserve text start time", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const gate = defer<void>()
        const { processors, session, provider } = yield* boot()

        yield* llm.push(
          raw({
            head: [
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: { role: "assistant" } }],
              },
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: { content: "hello" } }],
              },
            ],
            wait: gate.promise,
            tail: [
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: {}, finish_reason: "stop" }],
              },
            ],
          }),
        )

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "hi")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "hi" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* waitFor(
          MessageV2.parts(msg.id).pipe(
            Effect.map((parts) => parts.find((part): part is SessionV1.TextPart => part.type === "text")),
            Effect.provideService(Database.Service, database),
          ),
          "timed out waiting for text part",
        )
        yield* Effect.sleep("20 millis")
        gate.resolve()

        const exit = yield* Fiber.await(run)
        const text = (yield* MessageV2.parts(msg.id)).find((part): part is SessionV1.TextPart => part.type === "text")

        expect(Exit.isSuccess(exit)).toBe(true)
        expect(text?.text).toBe("hello")
        expect(text?.time?.start).toBeDefined()
        expect(text?.time?.end).toBeDefined()
        if (!text?.time?.start || !text.time.end) return
        expect(text.time.start).toBeLessThan(text.time.end)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests stop after token overflow requests compaction", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.text("after", { usage: { input: 100, output: 0 } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "compact")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const base = yield* provider.getModel(ref.providerID, ref.modelID)
        const mdl = { ...base, limit: { context: 20, output: 10 } }
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "compact" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("compact")
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(parts.some((part) => part.type === "step-finish")).toBe(true)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests capture reasoning from http mock", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.push(reply().reason("think").text("done").stop())

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "reason")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "reason" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)
        const reasoning = parts.find((part): part is SessionV1.ReasoningPart => part.type === "reasoning")
        const text = parts.find((part): part is SessionV1.TextPart => part.type === "text")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(1)
        expect(reasoning?.text).toBe("think")
        expect(text?.text).toBe("done")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests reset reasoning state across retries", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.push(reply().reason("one").reset(), reply().reason("two").stop())

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "reason")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "reason" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)
        const reasoning = parts.filter((part): part is SessionV1.ReasoningPart => part.type === "reasoning")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(reasoning.some((part) => part.text === "two")).toBe(true)
        expect(reasoning.some((part) => part.text === "onetwo")).toBe(false)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests do not retry unknown json errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(400, { error: { message: "no_kv_space" } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "json" }],
          tools: {},
        })

        expect(value).toBe("stop")
        expect(yield* llm.calls).toBe(1)
        expect(handle.message.error?.name).toBe("APIError")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests retry recognized structured json errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(429, { type: "error", error: { type: "too_many_requests" } })
        yield* llm.text("after")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry json" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests retry OpenAI-compatible midstream server errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.push(raw({ chunks: [{ error: { type: "server_error", code: "server_error", message: "xxx" } }] }))
        yield* llm.text("after")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry midstream server error")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry midstream server error" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests retry network_error finish reasons", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.push(
          raw({
            chunks: [
              {
                id: "chatcmpl-network-error",
                object: "chat.completion.chunk",
                choices: [{ index: 0, delta: { role: "assistant", content: "" }, finish_reason: "network_error" }],
              },
            ],
          }),
        )
        yield* llm.text("after retry")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry network error")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry network error" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after retry")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests publish retry status updates", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        yield* llm.error(503, { error: "boom" })
        yield* llm.text("")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const states: number[] = []
        const off = yield* events.listen((evt) => {
          if (evt.type !== SessionStatus.Event.Status.type) return Effect.void
          const data = evt.data as typeof SessionStatus.Event.Status.data.Type
          if (data.sessionID === chat.id && data.status.type === "retry") states.push(data.status.attempt)
          return Effect.void
        })
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry" }],
          tools: {},
        })

        yield* off

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(states).toStrictEqual([1])
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests compact on structured context overflow", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(400, { type: "error", error: { code: "context_length_exceeded" } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "compact json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "compact json" }],
          tools: {},
        })

        expect(value).toBe("compact")
        expect(yield* llm.calls).toBe(1)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests complete AI SDK tool calls when native flag is off", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.tool("lookup", { query: "weather" })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "tool")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "tool" }],
          tools: {
            lookup: tool({
              description: "Look up information",
              inputSchema: z.object({ query: z.string() }),
              execute: async (input) => ({
                title: "Weather lookup",
                output: `result:${input.query}`,
                metadata: { source: "test" },
              }),
            }),
          },
        })

        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(1)
        expect(call?.callID).toBe("call_1")
        expect(call?.tool).toBe("lookup")
        expect(call?.state.status).toBe("completed")
        if (call?.state.status !== "completed") return
        expect(call.state.input).toEqual({ query: "weather" })
        expect(call.state.output).toBe("result:weather")
        expect(call.state.title).toBe("Weather lookup")
        expect(call.state.metadata).toEqual({ source: "test" })
        expect(call.state.time.start).toBeDefined()
        expect(call.state.time.end).toBeDefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests mark pending tools as aborted on cleanup", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.toolHang("bash", { cmd: "pwd" })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "tool abort")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "tool abort" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* waitFor(
          MessageV2.parts(msg.id).pipe(
            Effect.map((parts) => parts.find((part): part is SessionV1.ToolPart => part.type === "tool")),
            Effect.provideService(Database.Service, database),
          ),
          "timed out waiting for tool part",
        )
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")

        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true)
        }
        expect(yield* llm.calls).toBe(1)
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") {
          expect(call.state.error).toBe("Tool execution aborted")
          expect(call.state.metadata?.interrupted).toBe(true)
          expect(call.state.time.end).toBeDefined()
        }
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itCleanupFault.live(
  "session.processor effect tests finalize the assistant when cleanup persistence dies",
  () =>
    provideTmpdirServer(
      ({ dir, llm }) =>
        Effect.gen(function* () {
          cleanupFault.failed = false
          const { processors, session, provider } = yield* boot()
          const events = yield* EventV2Bridge.Service
          const status = yield* SessionStatus.Service
          const eventErrors: string[] = []

          yield* llm.text("cleanup persistence")

          const chat = yield* session.create({})
          const parent = yield* user(chat.id, "cleanup failure")
          const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
          const model = yield* provider.getModel(ref.providerID, ref.modelID)
          const off = yield* events.listen((event) => {
            if (event.type !== Session.Event.Error.type) return Effect.void
            const data = event.data as typeof Session.Event.Error.data.Type
            if (data.sessionID !== chat.id || !data.error) return Effect.void
            eventErrors.push(data.error.name)
            return Effect.void
          })
          const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
          const result = yield* handle.process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "cleanup failure" }],
            tools: {},
          })
          const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
          const sessionStatus = yield* status.get(chat.id)
          yield* off

          expect(cleanupFault.failed).toBe(true)
          expect(result).toBe("stop")
          expect(sessionStatus).toMatchObject({ type: "idle" })
          expect(yield* llm.calls).toBe(1)
          expect(stored.info.role).toBe("assistant")
          if (stored.info.role === "assistant") {
            expect(stored.info.time.completed).toBeDefined()
            const error = stored.info.error
            expect(error?.name).toBe("UnknownError")
            if (error?.name === "UnknownError") {
              expect(error.data.message).toContain("one-shot cleanup persistence failure")
              expect(eventErrors).toContain(error.name)
            }
          }
        }),
      { config: (url) => providerCfg(url) },
    ),
  20_000,
)

itCleanupPartFailure.live(
  "session.processor effect tests preserve the provider error and finish remaining cleanup parts",
  () =>
    provideTmpdirInstance(
      (dir) =>
        Effect.gen(function* () {
          cleanupPartFailure.failed = false
          yield* Effect.addFinalizer(() => Effect.sync(() => void (cleanupPartFailure.failed = true)))
          const { processors, session, provider } = yield* boot()
          const events = yield* EventV2Bridge.Service
          const chat = yield* session.create({})
          const parent = yield* user(chat.id, "cleanup part failure")
          const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
          const model = yield* provider.getModel(ref.providerID, ref.modelID)
          const eventErrors: string[] = []
          const off = yield* events.listen((event) => {
            if (event.type !== Session.Event.Error.type) return Effect.void
            const data = event.data as typeof Session.Event.Error.data.Type
            if (data.sessionID !== chat.id || !data.error) return Effect.void
            if (data.error.name === "UnknownError") eventErrors.push(data.error.data.message)
            return Effect.void
          })
          const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
          const result = yield* handle.process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "cleanup part failure" }],
            tools: {},
          })
          const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
          const parts = yield* MessageV2.parts(msg.id)
          const reasoning = parts.find((part): part is SessionV1.ReasoningPart => part.type === "reasoning")
          const toolCall = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
          yield* off

          expect(cleanupPartFailure.failed).toBe(true)
          expect(result).toBe("stop")
          expect(stored.info.role).toBe("assistant")
          if (stored.info.role === "assistant") {
            expect(stored.info.time.completed).toBeDefined()
            expect(stored.info.error?.name).toBe("UnknownError")
            if (stored.info.error?.name === "UnknownError") {
              expect(stored.info.error.data.message).toContain("original provider failure")
              expect(stored.info.error.data.message).not.toContain("text finalization failure")
            }
          }
          expect(reasoning?.time.end).toBeDefined()
          expect(toolCall?.state.status).toBe("error")
          if (toolCall?.state.status === "error") {
            expect(toolCall.state.error).toBe("Tool execution aborted")
            expect(toolCall.state.time.end).toBeDefined()
          }
          expect(eventErrors).toHaveLength(1)
          expect(eventErrors[0]).toContain("original provider failure")
        }),
      { config: cfg },
    ),
  20_000,
)

itOverflowCleanupFailure.live(
  "session.processor stops instead of compacting when overflow cleanup fails",
  () =>
    provideTmpdirInstance(
      (dir) =>
        Effect.gen(function* () {
          cleanupPartFailure.failed = false
          yield* Effect.addFinalizer(() => Effect.sync(() => void (cleanupPartFailure.failed = true)))
          const { processors, session, provider } = yield* boot()
          const events = yield* EventV2Bridge.Service
          const statuses = yield* SessionStatus.Service
          const chat = yield* session.create({})
          const parent = yield* user(chat.id, "overflow cleanup failure")
          const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
          const base = yield* provider.getModel(ref.providerID, ref.modelID)
          const model = { ...base, limit: { context: 20, output: 10 } }
          const eventErrors: string[] = []
          const off = yield* events.listen((event) => {
            if (event.type !== Session.Event.Error.type) return Effect.void
            const data = event.data as typeof Session.Event.Error.data.Type
            if (data.sessionID !== chat.id || !data.error) return Effect.void
            if (data.error.name === "UnknownError") eventErrors.push(data.error.data.message)
            return Effect.void
          })
          const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
          const result = yield* handle.process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "overflow cleanup failure" }],
            tools: {},
          })
          const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
          yield* off

          expect(cleanupPartFailure.failed).toBe(true)
          expect(result).toBe("stop")
          expect((yield* statuses.get(chat.id)).type).toBe("idle")
          expect(stored.info.role).toBe("assistant")
          if (stored.info.role === "assistant") {
            expect(stored.info.time.completed).toBeDefined()
            expect(stored.info.error?.name).toBe("UnknownError")
            if (stored.info.error?.name === "UnknownError") {
              expect(stored.info.error.data.message).toContain("one-shot text finalization failure")
            }
          }
          expect(eventErrors).toHaveLength(1)
          expect(eventErrors[0]).toContain("one-shot text finalization failure")
        }),
      { config: cfg },
    ),
  20_000,
)

for (const { failures, path: databasePath } of sqliteTerminalDbPaths) {
  const itLockTerminal = testEffect(
    LayerNode.compile(root, [
      ...replacements,
      [Database.node, Database.layerFromPath(databasePath)],
      [Session.node, lockTerminalSession],
      [LLM.node, lockTerminalLLM],
    ]),
  )
  itLockTerminal.live(
    `processor terminalizes the persisted turn after ${failures} produced SQLite lock failures`,
    () =>
      provideTmpdirInstance(
        (dir) =>
          Effect.gen(function* () {
            const { processors, session, provider } = yield* boot()
            const database = yield* Database.Service
            const produced = yield* produceSqliteBusyError(database.db, databasePath)
            expect(produced).toBeInstanceOf(EffectDrizzleQueryError)
            if (!(produced instanceof EffectDrizzleQueryError) || !Cause.isCause(produced.cause)) return
            const error = Option.getOrUndefined(Cause.findErrorOption(produced.cause))
            expect(isSqlError(error)).toBe(true)
            if (!isSqlError(error)) return
            expect(error.reason._tag).toBe("LockTimeoutError")
            expect(error.message).toContain("database is locked")

            const events = yield* EventV2Bridge.Service
            const statuses = yield* SessionStatus.Service
            const chat = yield* session.create({})
            const parent = yield* user(chat.id, "terminalize produced lock failure")
            const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
            const model = yield* provider.getModel(ref.providerID, ref.modelID)
            const fault = { armed: false, remaining: failures, error, failed: 0, messageID: msg.id }
            lockTerminalFaults.set(chat.id, fault)
            yield* Effect.addFinalizer(() => Effect.sync(() => void lockTerminalFaults.delete(chat.id)))
            const eventErrors: string[] = []
            const off = yield* events.listen((event) => {
              if (event.type !== Session.Event.Error.type) return Effect.void
              const data = event.data as typeof Session.Event.Error.data.Type
              if (data.sessionID !== chat.id || !data.error) return Effect.void
              eventErrors.push(data.error.name === "UnknownError" ? data.error.data.message : data.error.name)
              return Effect.void
            })
            const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
            const exit = yield* Effect.exit(
              handle.process({
                user: {
                  id: parent.id,
                  sessionID: chat.id,
                  role: "user",
                  time: parent.time,
                  agent: parent.agent,
                  model: { providerID: ref.providerID, modelID: ref.modelID },
                } satisfies SessionV1.User,
                sessionID: chat.id,
                model,
                agent: agent(),
                system: [],
                messages: [{ role: "user", content: "terminalize produced lock failure" }],
                tools: {},
              }),
            )
            const injected = fault.failed
            lockTerminalFaults.delete(chat.id)
            yield* off
            const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })

            expect(injected).toBe(failures)
            expect(Exit.isSuccess(exit)).toBe(true)
            if (Exit.isSuccess(exit)) expect(exit.value).toBe("stop")
            expect((yield* statuses.get(chat.id)).type).toBe("idle")
            expect(stored.info.role).toBe("assistant")
            if (stored.info.role === "assistant") {
              expect(stored.info.time.completed).toBeDefined()
              expect(stored.info.error?.name).toBe("UnknownError")
              if (stored.info.error?.name === "UnknownError") {
                expect(stored.info.error.data.message.toLowerCase()).toContain("database is locked")
              }
            }
            expect(eventErrors.some((message) => message.toLowerCase().includes("database is locked"))).toBe(true)
          }),
        { config: cfg },
      ),
    20_000,
  )
}

it.live("session.processor effect tests record aborted errors and idle state", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const seen = defer<void>()
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service
        const sts = yield* SessionStatus.Service

        yield* llm.hang

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "abort")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const errs: string[] = []
        const off = yield* events.listen((evt) => {
          if (evt.type !== Session.Event.Error.type) return Effect.void
          const data = evt.data as typeof Session.Event.Error.data.Type
          if (data.sessionID !== chat.id || !data.error) return Effect.void
          errs.push(data.error.name)
          seen.resolve()
          return Effect.void
        })
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "abort" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        yield* Effect.promise(() => seen.promise)
        const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
        const state = yield* sts.get(chat.id)
        yield* off

        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true)
        }
        expect(handle.message.error?.name).toBe("MessageAbortedError")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") {
          expect(stored.info.error?.name).toBe("MessageAbortedError")
        }
        expect(state).toMatchObject({ type: "idle" })
        expect(errs).toContain("MessageAbortedError")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests mark interruptions aborted without manual abort", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const sts = yield* SessionStatus.Service

        yield* llm.hang

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "interrupt")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "interrupt" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
        const state = yield* sts.get(chat.id)

        expect(Exit.isFailure(exit)).toBe(true)
        expect(handle.message.error?.name).toBe("MessageAbortedError")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") {
          expect(stored.info.error?.name).toBe("MessageAbortedError")
        }
        expect(state).toMatchObject({ type: "idle" })
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itProviderError.live("session.processor effect tests fail provider-executed error results", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "provider tool error")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const seen: string[] = []
        const off = yield* events.listen((event) => {
          seen.push(event.type)
          return Effect.void
        })
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "provider tool error" }],
          tools: {},
        })
        yield* off

        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") expect(call.state.error).toBe("provider boom")
        expect(seen).toContain(MessageV2.Event.PartUpdated.type)
        expect(seen).toContain(MessageV2.Event.Updated.type)
        expect(seen.filter((type) => type.startsWith("session.next."))).toEqual([])
      }),
    { config: cfg },
  ),
)

itFragmentFailure.live("session.processor effect tests retain partial legacy parts without v2 events", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "provider failure")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const seen: string[] = []
        const off = yield* events.listen((event) => {
          seen.push(event.type)
          return Effect.void
        })
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        expect(
          yield* handle.process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "provider failure" }],
            tools: {},
          }),
        ).toBe("stop")
        yield* off

        const parts = yield* MessageV2.parts(msg.id)
        expect(parts).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ type: "text", text: "partial" }),
            expect.objectContaining({ type: "reasoning", text: "thinking" }),
          ]),
        )
        expect(seen).toContain(MessageV2.Event.PartUpdated.type)
        expect(seen).toContain(Session.Event.Error.type)
        expect(seen.filter((type) => type.startsWith("session.next."))).toEqual([])
      }),
    { config: cfg },
  ),
)

itSqliteLock.live(
  "session.processor persists and publishes safe SQLite lock diagnostics",
  () =>
    provideTmpdirInstance(
      (dir) =>
        Effect.gen(function* () {
          const { processors, session, provider } = yield* boot()
          const events = yield* EventV2Bridge.Service
          const chat = yield* session.create({})
          const parent = yield* user(chat.id, "sqlite lock persistence")
          const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
          const model = yield* provider.getModel(ref.providerID, ref.modelID)
          const database = yield* Database.Service
          const error = yield* produceSqliteBusyError(database.db, sqliteLockDbPath)
          expect(error).toBeInstanceOf(EffectDrizzleQueryError)
          if (!(error instanceof EffectDrizzleQueryError)) return
          expect(error.query).toBe("Database is locked (SQLITE_BUSY)")
          expect(error.params).toEqual([])
          const cause = error.cause
          const failure = Cause.isCause(cause) ? Option.getOrUndefined(Cause.findErrorOption(cause)) : undefined
          expect(isSqlError(failure)).toBe(true)
          if (!isSqlError(failure)) return
          expect(failure.reason._tag).toBe("LockTimeoutError")
          expect(failure.reason.cause).toMatchObject({ code: "SQLITE_BUSY" })
          sqliteLockFailure.resolve(error)
          const eventMessages: string[] = []
          const off = yield* events.listen((event) => {
            if (event.type !== Session.Event.Error.type) return Effect.void
            const data = event.data as typeof Session.Event.Error.data.Type
            if (data.sessionID !== chat.id || !data.error) return Effect.void
            eventMessages.push(data.error.name === "UnknownError" ? data.error.data.message : data.error.name)
            return Effect.void
          })
          const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
          const result = yield* handle.process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "sqlite lock persistence" }],
            tools: {},
          })
          yield* off
          const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
          const lockMessage = "Database is locked (SQLITE_BUSY)"

          expect(result).toBe("stop")
          expect(handle.message.error).toMatchObject({ name: "UnknownError", data: { message: lockMessage } })
          expect(stored.info.role).toBe("assistant")
          if (stored.info.role === "assistant") {
            expect(stored.info.error).toMatchObject({ name: "UnknownError", data: { message: lockMessage } })
          }
          expect(eventMessages).toContain(lockMessage)
          expect(JSON.stringify({ assistant: handle.message.error, stored, eventMessages })).not.toContain(
            "secret_lock_fixture",
          )
          expect(JSON.stringify({ assistant: handle.message.error, stored, eventMessages })).not.toContain(
            "secret_parameter",
          )
        }),
      { config: cfg },
    ),
  15_000,
)

itRouterLabel.live(
  "feeds repeated provider step records into the router session label",
  provideTmpdirInstance((dir) =>
    Effect.gen(function* () {
      const { processors, session, provider } = yield* boot()
      const chat = yield* session.create({})
      const parent = yield* user(chat.id, "router label steps")
      const requestModelRef = { providerID: ProviderV2.ID.make("llmrouter"), modelID: ModelV2.ID.make("auto") }
      parent.model = requestModelRef
      yield* session.updateMessage(parent)

      const requestModel = yield* provider.getModel(requestModelRef.providerID, requestModelRef.modelID)
      const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
      msg.providerID = requestModelRef.providerID
      msg.modelID = requestModelRef.modelID
      yield* session.updateMessage(msg)

      routerLabelEvents.splice(
        0,
        routerLabelEvents.length,
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.stepFinish({
          index: 0,
          reason: "stop",
          usage: new Usage({ inputTokens: 10, outputTokens: 2 }),
          responseModelID: "luna-max",
        }),
        LLMEvent.stepStart({ index: 1 }),
        LLMEvent.stepFinish({
          index: 1,
          reason: "stop",
          usage: new Usage({ inputTokens: 10, outputTokens: 2 }),
          responseModelID: "luna-max",
        }),
        LLMEvent.stepStart({ index: 2 }),
        LLMEvent.stepFinish({
          index: 2,
          reason: "stop",
          usage: new Usage({ inputTokens: 10, outputTokens: 2 }),
          responseModelID: "glm-5.3-flash",
        }),
        LLMEvent.finish({ reason: "stop" }),
      )

      const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: requestModel })
      yield* handle.process({
        user: parent,
        sessionID: chat.id,
        model: requestModel,
        agent: agent(),
        system: [],
        messages: [{ role: "user", content: "router label steps" }],
        tools: {},
      })

      const messages = (yield* session.messages({ sessionID: chat.id })) as unknown as SessionMessages
      const processed = messages.find((item) => item.info.id === msg.id)
      if (!processed || processed.info.role !== "assistant") throw new Error("processor did not persist assistant message")
      const stepModelIDs = processed.parts.flatMap((part) =>
        part.type === "step-finish" && part.responseModelID !== undefined ? [part.responseModelID] : [],
      )
      expect(stepModelIDs).toEqual(["luna-max", "luna-max", "glm-5.3-flash"])

      const providers = [
        {
          id: "llmrouter",
          name: "LLMRouter",
          source: "api",
          env: [],
          options: {},
          models: {
            auto: { name: "Auto" },
            "luna-max": { name: "luna-max" },
            "glm-5.3-flash": { name: "glm-5.3-flash" },
            "sol-high": { name: "sol-high" },
          },
        },
      ] as unknown as RunProvider[]
      expect(
        servedModelLabel(
          providers,
          "llmrouter",
          "auto",
          processed.info.responseModelIDs,
          servedAcrossSession(messages, processed),
        ),
      ).toBe("Auto (luna-max:2/67%, glm-5.3-flash:1/33%, sol-high:0/0%)")
    }),
    { config: routerLabelConfig() },
  ),
)

itRouted.live(
  "prices and attributes persisted usage by the reported serving model",
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "routed usage")
        const requestModelRef = { providerID: ref.providerID, modelID: ModelV2.ID.make("request-model") }
        parent.model = requestModelRef
        yield* session.updateMessage(parent)

        const requestModel = yield* provider.getModel(requestModelRef.providerID, requestModelRef.modelID)
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        msg.modelID = requestModelRef.modelID
        yield* session.updateMessage(msg)

        const steps: Array<{
          responseModelID?: string
          input: number
          output: number
          reasoning?: number
          cacheRead?: number
          cacheWrite?: number
        }> = [
          {
            responseModelID: "served-a",
            input: 1_000_000,
            output: 1_000_000,
            reasoning: 150,
            cacheRead: 200,
            cacheWrite: 100,
          },
          { responseModelID: "served-b", input: 2_000_000, output: 1_000_000 },
          { responseModelID: "missing-model", input: 1_000_000, output: 1_000_000 },
          { responseModelID: "unpriced-model", input: 1_000_000, output: 1_000_000 },
          { input: 1_000_000, output: 1_000_000 },
          { responseModelID: "free-model", input: 1_000_000, output: 1_000_000 },
        ]
        routedEvents.splice(
          0,
          routedEvents.length,
          ...steps.flatMap((step, index) => [
            LLMEvent.stepStart({ index }),
            LLMEvent.stepFinish({
              index,
              reason: "stop",
              usage: new Usage({
                inputTokens: step.input,
                outputTokens: step.output,
                reasoningTokens: step.reasoning ?? 0,
                cacheReadInputTokens: step.cacheRead ?? 0,
                cacheWriteInputTokens: step.cacheWrite ?? 0,
                totalTokens: step.input + step.output,
              }),
              responseModelID: step.responseModelID,
            }),
          ]),
          LLMEvent.finish({ reason: "stop" }),
        )

        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: requestModel })
        expect(
          yield* handle.process({
            user: parent,
            sessionID: chat.id,
            model: requestModel,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "routed usage" }],
            tools: {},
          }),
        ).toBe("continue")

        const messages = yield* session.messages({ sessionID: chat.id })
        const processed = messages.find((message) => message.info.id === msg.id)
        expect(processed?.info.role).toBe("assistant")
        if (!processed || processed.info.role !== "assistant") return
        const stepParts = processed.parts.filter(
          (part): part is SessionV1.StepFinishPart => part.type === "step-finish",
        )
        expect(stepParts).toHaveLength(6)
        expect(stepParts[0]?.cost).toBeCloseTo(2.9997, 10)
        expect(stepParts.slice(1).map((part) => part.cost)).toEqual([10, 30, 30, 30, 0])
        expect(stepParts.map((part) => part.responseModelID)).toEqual([
          "served-a",
          "served-b",
          "missing-model",
          "unpriced-model",
          undefined,
          "free-model",
        ])
        expect(processed.info.cost).toBeCloseTo(102.9997, 10)
        expect(processed.info.responseModelIDs).toEqual([
          "served-a",
          "served-b",
          "missing-model",
          "unpriced-model",
          "free-model",
        ])

        const historical = yield* assistant(chat.id, parent.id, path.resolve(dir))
        historical.modelID = requestModelRef.modelID
        historical.responseModelIDs = ["old-served-a", "old-served-b"]
        historical.cost = 7
        historical.tokens = {
          total: 1_000,
          input: 700,
          output: 300,
          reasoning: 0,
          cache: { read: 0, write: 0 },
        }
        yield* session.updateMessage(historical)

        const database = yield* Database.Service
        const projected = yield* session.get(chat.id)
        expect(projected.cost).toBeCloseTo(102.9997, 10)
        yield* database.db
          .update(SessionTable)
          .set({
            cost: (projected.cost ?? 0) + historical.cost + 5,
            tokens_input: (projected.tokens?.input ?? 0) + historical.tokens.input + 5,
            tokens_output: (projected.tokens?.output ?? 0) + historical.tokens.output + 4,
            tokens_reasoning: (projected.tokens?.reasoning ?? 0) + historical.tokens.reasoning + 3,
            tokens_cache_read: (projected.tokens?.cache.read ?? 0) + historical.tokens.cache.read + 2,
            tokens_cache_write: (projected.tokens?.cache.write ?? 0) + historical.tokens.cache.write + 1,
          })
          .where(eq(SessionTable.id, chat.id))
          .run()
          .pipe(Effect.orDie)

        const staleRollup = yield* session.get(chat.id)
        expect(staleRollup.cost).toBeCloseTo(114.9997, 10)
        expect(staleRollup.tokens?.input).toBe(7_000_405)
        expect(staleRollup.tokens?.output).toBe(6_000_154)
        expect(staleRollup.tokens?.reasoning).toBe(153)
        expect(staleRollup.tokens?.cache).toEqual({ read: 202, write: 101 })
        const exportDatabase = new Sqlite(routedExportDbPath, { readonly: true })
        const exportSnapshot = (() => {
          try {
            return readExport(exportDatabase)
          } finally {
            exportDatabase.close()
          }
        })()
        expect(exportSnapshot.records).toHaveLength(1)
        expect(exportSnapshot.reportedCostTotal).toBeCloseTo(109.9997, 10)
        expect(exportSnapshot.reportedCostTotal).toBeCloseTo(
          exportSnapshot.records.reduce((total, item) => total + Number(item["reportedCost"]), 0),
          10,
        )
        expect(exportSnapshot.orphanMessages).toBe(0)
        expect(exportSnapshot.check.ok).toBe(true)
        expect(exportSnapshot.check.lines.join("\n")).toContain("1 of 1 disagree with exported usage")
        expect(exportSnapshot.check.lines.join("\n")).toContain("1 of 1 disagree with exported costs")
        const record = exportSnapshot.records[0]
        expect(record).toBeDefined()
        if (!record) return
        expect(record).toMatchObject({
          modelID: "request-model",
          tokens: {
            input: 7_000_400,
            output: 6_000_150,
            reasoning: 150,
            cacheRead: 200,
            cacheWrite: 100,
          },
          servedModelIDs: [
            "free-model",
            "missing-model",
            "old-served-a",
            "old-served-b",
            "served-a",
            "served-b",
            "unpriced-model",
          ],
        })
        expect(Number(record["reportedCost"])).toBeCloseTo(109.9997, 10)
        const servedUsage = record["servedModelUsage"]
        expect(Array.isArray(servedUsage)).toBe(true)
        if (!Array.isArray(servedUsage)) return
        expect(servedUsage).toEqual([
          {
            modelID: "free-model",
            tokens: { input: 1_000_000, output: 1_000_000, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
            reportedCost: 0,
          },
          {
            modelID: "missing-model",
            tokens: { input: 1_000_000, output: 1_000_000, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
            reportedCost: 30,
          },
          {
            modelID: "request-model",
            tokens: { input: 1_000_700, output: 1_000_300, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
            reportedCost: 37,
          },
          {
            modelID: "served-a",
            tokens: { input: 999_700, output: 999_850, reasoning: 150, cacheRead: 200, cacheWrite: 100 },
            reportedCost: 2.9997,
          },
          {
            modelID: "served-b",
            tokens: { input: 2_000_000, output: 1_000_000, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
            reportedCost: 10,
          },
          {
            modelID: "unpriced-model",
            tokens: { input: 1_000_000, output: 1_000_000, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
            reportedCost: 30,
          },
        ])
        expect(servedUsage.reduce((total, item) => total + item.reportedCost, 0)).toBeCloseTo(
          Number(record["reportedCost"]),
          10,
        )

        yield* database.db
          .insert(PartTable)
          .values({
            id: PartID.ascending(),
            message_id: processed.info.id,
            session_id: chat.id,
            time_created: Date.now(),
            data: {
              type: "step-finish",
              reason: "stop",
              responseModelID: "malformed-model",
              tokens: {},
            } as never,
          })
          .run()
          .pipe(Effect.orDie)
        const stats = yield* aggregateSessionStats()
        expect(stats.totalCost).toBeCloseTo(109.9997, 10)
        expect(stats.costPerDay * stats.days).toBeCloseTo(stats.totalCost, 10)
        expect(stats.totalTokens.input).toBe(7_000_400)
        expect(stats.totalTokens.output).toBe(6_000_150)
        expect(stats.totalTokens.reasoning).toBe(150)
        expect(stats.totalTokens.cache).toEqual({ read: 200, write: 100 })
        expect(stats.tokensPerSession).toBe(13_001_000)
        expect(stats.medianTokensPerSession).toBe(13_001_000)
        expect(Object.keys(stats.modelUsage).sort()).toEqual([
          "test/free-model",
          "test/malformed-model",
          "test/missing-model",
          "test/request-model",
          "test/served-a",
          "test/served-b",
          "test/unpriced-model",
        ])
        expect(stats.modelUsage["test/served-a"]?.cost).toBeCloseTo(2.9997, 10)
        expect(stats.modelUsage["test/served-a"]).toMatchObject({
          tokens: { input: 999_700, output: 1_000_000, cache: { read: 200, write: 100 } },
        })
        expect(stats.modelUsage["test/served-b"]).toMatchObject({
          cost: 10,
          tokens: { input: 2_000_000, output: 1_000_000 },
        })
        expect(stats.modelUsage["test/missing-model"]?.cost).toBe(30)
        expect(stats.modelUsage["test/unpriced-model"]?.cost).toBe(30)
        expect(stats.modelUsage["test/free-model"]).toMatchObject({
          cost: 0,
          tokens: { input: 1_000_000, output: 1_000_000 },
        })
        expect(stats.modelUsage["test/malformed-model"]).toMatchObject({
          cost: 0,
          tokens: { input: 0, output: 0, cache: { read: 0, write: 0 } },
        })
        expect(stats.modelUsage["test/request-model"]).toMatchObject({
          cost: 37,
          messages: 2,
          tokens: { input: 1_000_700, output: 1_000_300 },
        })
        const modeledCost = Object.values(stats.modelUsage).reduce((total, item) => total + item.cost, 0)
        const modeledInput = Object.values(stats.modelUsage).reduce((total, item) => total + item.tokens.input, 0)
        const modeledOutput = Object.values(stats.modelUsage).reduce((total, item) => total + item.tokens.output, 0)
        const modeledCacheRead = Object.values(stats.modelUsage).reduce(
          (total, item) => total + item.tokens.cache.read,
          0,
        )
        const modeledCacheWrite = Object.values(stats.modelUsage).reduce(
          (total, item) => total + item.tokens.cache.write,
          0,
        )
        const totalShare = Object.values(stats.modelUsage).reduce(
          (total, item) => total + item.cost / stats.totalCost,
          0,
        )
        expect(modeledCost).toBeCloseTo(stats.totalCost, 10)
        expect(modeledInput).toBe(stats.totalTokens.input)
        expect(modeledOutput).toBe(stats.totalTokens.output + stats.totalTokens.reasoning)
        expect(modeledCacheRead).toBe(stats.totalTokens.cache.read)
        expect(modeledCacheWrite).toBe(stats.totalTokens.cache.write)
        expect(totalShare).toBeCloseTo(1, 12)

        const printed: string[] = []
        const output: typeof console.log = (...values) => printed.push(values.map(String).join(" "))
        displayStats(stats, undefined, 1, output)
        expect(printed.join("\n")).toContain("$110.00")
        expect(printed.join("\n")).not.toContain("$115.00")
        expect(printed.join("\n")).toContain("33.6%")
        expect(printed.join("\n")).not.toContain("test/served-a")
        expect(printed.find((line) => line.startsWith("│Cache Read"))).toContain("200")
        expect(printed.find((line) => line.startsWith("│Cache Write"))).toContain("100")

        const zeroCostStats = {
          ...stats,
          totalCost: 0,
          modelUsage: {
            "test/free-model": {
              messages: 1,
              tokens: { input: 0, output: 0, cache: { read: 0, write: 0 } },
              cost: 0,
            },
          },
        }
        const zeroCostOutput: string[] = []
        displayStats(zeroCostStats, undefined, 1, (...values) => zeroCostOutput.push(values.map(String).join(" ")))
        expect(zeroCostOutput.join("\n")).toContain("0.0%")
      }),
    { config: routedConfig() },
  ),
)
