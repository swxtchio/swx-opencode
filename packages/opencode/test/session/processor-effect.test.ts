import { SessionV1 } from "@opencode-ai/core/v1/session"
import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { eq } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { EventV2Bridge } from "@/event-v2-bridge"
import { afterAll, expect } from "bun:test"
import { Database as Sqlite } from "bun:sqlite"
import { tool } from "ai"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Logger, Option, Scheduler, Schema, Stream } from "effect"
import { isSqlError } from "effect/unstable/sql/SqlError"
import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "path"
import z from "zod"
import type { Agent } from "../../src/agent/agent"
import { Config } from "@/config/config"
import { Provider } from "@/provider/provider"
import { BackgroundJob } from "@/background/job"

import { Session } from "@/session/session"
import { LLM } from "../../src/session/llm"
import { MessageV2 } from "../../src/session/message-v2"
import { SessionProcessor } from "../../src/session/processor"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionStatus } from "../../src/session/status"
import { SessionRunState } from "../../src/session/run-state"
import { SessionSummary } from "../../src/session/summary"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { provideTmpdirInstance, provideTmpdirServer } from "../fixture/fixture"
import { produceSqliteBusyError } from "../fixture/sqlite-lock"
import { sqliteLockMessage } from "@/util/sqlite-error"
import { awaitWithTimeout, testEffect } from "../lib/effect"
import { SessionTools } from "@/session/tools"
import { Plugin } from "@/plugin"
import { Permission } from "@/permission"
import { MCP } from "@/mcp"
import { Truncate } from "@/tool/truncate"
import { ToolRegistry } from "@/tool/registry"
import type { Tool } from "@/tool/tool"
import { LLMNativeRuntime } from "@/session/llm/native-runtime"
import type { LLMClientShape } from "@opencode-ai/llm/route"
import { existsSync } from "node:fs"
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

const preludeInterruptGate = { entered: undefined as Deferred.Deferred<void> | undefined }
type PreludeConfigInfo = Effect.Success<ReturnType<Config.Interface["get"]>>
const preludeConfigInfo = Schema.decodeUnknownSync(ConfigV1.Info)(cfg) as unknown as PreludeConfigInfo
const preludeConfigService = Config.Service.of({
  get: () =>
    Effect.gen(function* () {
      const entered = preludeInterruptGate.entered
      if (!entered) return preludeConfigInfo
      preludeInterruptGate.entered = undefined
      yield* Deferred.succeed(entered, undefined)
      return yield* Effect.never
    }),
  getGlobal: () => Effect.succeed(preludeConfigInfo),
  getConsoleState: () => Effect.succeed({ consoleManagedProviders: [], switchableOrgCount: 0 }),
  update: () => Effect.void,
  updateGlobal: () => Effect.succeed({ info: preludeConfigInfo, changed: false }),
  invalidate: () => Effect.void,
  directories: () => Effect.succeed([]),
  waitForDependencies: () => Effect.void,
})
const preludeConfigNode = LayerNode.make({
  service: Config.Service,
  layer: Layer.succeed(Config.Service, preludeConfigService),
  deps: [],
})

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

const waitFor = <A>(check: Effect.Effect<A | undefined>, message: string, timeout = 500) =>
  Effect.gen(function* () {
    const stop = Date.now() + timeout
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
const preludeInterruptStreamCalls = { count: 0 }
const preludeInterruptLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () => {
      preludeInterruptStreamCalls.count++
      return Stream.empty
    },
  }),
)
const preludeInterruptEnv = LayerNode.compile(root, [
  ...replacements,
  [Config.node, preludeConfigNode],
  [LLM.node, preludeInterruptLLM],
])
const itPreludeInterrupt = testEffect(preludeInterruptEnv)

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
const heldLockDbPath = path.join(import.meta.dir, `.opencode-sqlite-held-lock-${crypto.randomUUID()}.db`)
const streamLockDbPath = path.join(import.meta.dir, `.opencode-sqlite-stream-lock-${crypto.randomUUID()}.db`)
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
      heldLockDbPath,
      `${heldLockDbPath}-wal`,
      `${heldLockDbPath}-shm`,
      streamLockDbPath,
      `${streamLockDbPath}-wal`,
      `${streamLockDbPath}-shm`,
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
type HeldLock = {
  messageID: MessageID
  // The write the second connection takes the lock just before: the reasoning-end part or the step-finish message.
  trigger: "reasoning-end" | "step-finish"
  holder?: Sqlite
  streaming: ReturnType<typeof defer<void>>
  resume: ReturnType<typeof defer<void>>
  // The first lock failure the held lock produced in the turn's own writes.
  produced?: unknown
  // While set, writes of the turn's message fail with this instead of reaching the database.
  replay?: unknown
  replayed: number
  skipped?: Deferred.Deferred<void>
}
const heldLocks = new Map<SessionID, HeldLock>()
const heldLockWrite = <A, E, R>(lock: HeldLock | undefined, write: Effect.Effect<A, E, R>) =>
  Effect.suspend(() => {
    if (!lock) return write
    if (lock.replay !== undefined) {
      lock.replayed++
      return Effect.die(lock.replay)
    }
    return write.pipe(Effect.tapCause((cause) => Effect.sync(() => void (lock.produced ??= Cause.squash(cause)))))
  })
const heldLockSession = LayerNode.make({
  service: Session.Service,
  layer: Layer.effect(
    Session.Service,
    Effect.gen(function* () {
      const real = yield* Session.Service
      const lockFor = (sessionID: SessionID, messageID: MessageID) => {
        const lock = heldLocks.get(sessionID)
        return lock?.messageID === messageID ? lock : undefined
      }
      return Session.Service.of({
        ...real,
        updatePart: <T extends SessionV1.Part>(part: T) =>
          Effect.suspend(() => {
            const lock = lockFor(part.sessionID, part.messageID)
            // A second connection takes the write lock just before the turn's reasoning-end write.
            if (
              lock?.trigger === "reasoning-end" &&
              !lock.holder &&
              part.type === "reasoning" &&
              part.time.end !== undefined
            ) {
              lock.holder = new Sqlite(heldLockDbPath)
              lock.holder.run("BEGIN IMMEDIATE")
            }
            return heldLockWrite(lock, real.updatePart(part))
          }),
        updateMessage: <T extends SessionV1.Info>(msg: T) =>
          Effect.suspend(() => {
            const lock = lockFor(msg.sessionID, msg.id)
            if (lock?.trigger === "step-finish" && !lock.holder && msg.role === "assistant" && msg.finish) {
              lock.holder = new Sqlite(heldLockDbPath)
              lock.holder.run("BEGIN IMMEDIATE")
            }
            return heldLockWrite(lock, real.updateMessage(msg))
          }),
        updateExistingMessage: (msg: SessionV1.Info) =>
          Effect.suspend(() => {
            const lock = lockFor(msg.sessionID, msg.id)
            return heldLockWrite(lock, real.updateExistingMessage(msg)).pipe(
              Effect.tap((written) =>
                !written && lock?.skipped ? Deferred.succeed(lock.skipped, undefined) : Effect.void,
              ),
            )
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
const heldLockLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: (input) =>
      Stream.concat(
        Stream.make(
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.reasoningStart({ id: "reasoning-1" }),
          LLMEvent.reasoningDelta({ id: "reasoning-1", text: "thinking" }),
        ),
        Stream.unwrap(
          Effect.promise(async () => {
            const lock = heldLocks.get(input.sessionID as SessionID)
            lock?.streaming.resolve()
            await lock?.resume.promise
            return Stream.make(
              LLMEvent.reasoningEnd({ id: "reasoning-1" }),
              LLMEvent.stepFinish({ index: 0, reason: "stop" }),
              LLMEvent.finish({ reason: "stop" }),
            )
          }),
        ),
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
// The provider stream dies with the session's lock failure after its handled events all succeed.
const streamLockFailures = new Map<SessionID, unknown>()
// A named callable inside the provider stream, so the halt line must name it rather than the processor around it.
const failProviderStream = Effect.fn("test.failProviderStream")(function* (sessionID: SessionID) {
  return yield* Effect.die(streamLockFailures.get(sessionID))
})
const itStreamLock = testEffect(
  LayerNode.compile(root, [
    ...replacements,
    [
      LLM.node,
      Layer.succeed(
        LLM.Service,
        LLM.Service.of({
          stream: (input) =>
            Stream.concat(
              Stream.make(
                LLMEvent.stepStart({ index: 0 }),
                LLMEvent.reasoningStart({ id: "reasoning-1" }),
                LLMEvent.reasoningEnd({ id: "reasoning-1" }),
              ),
              Stream.unwrap(failProviderStream(input.sessionID as SessionID)),
            ),
        }),
      ),
    ],
    [Database.node, Database.layerFromPath(streamLockDbPath)],
  ]),
)

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

itCleanupFault.live(
  "session.processor retains the stalled Foundry error across retries and a one-shot persistence failure",
  () =>
    provideTmpdirServer(
      ({ dir }) =>
        Effect.gen(function* () {
          cleanupFault.failed = false
          const events = yield* EventV2Bridge.Service
          const { processors, session, provider } = yield* boot()
          const status = yield* SessionStatus.Service
          const providerID = ProviderV2.ID.make("swx-azure")
          const modelID = ModelV2.ID.make("gpt-6.1-sol")
          const attempts = { requests: 0, aborted: 0, cancelled: 0, keepalives: 0 }
          const eventErrors: Array<{ name: string; message?: string; code?: string }> = []
          const off = yield* events.listen((event) => {
            if (event.type !== Session.Event.Error.type) return Effect.void
            const data = event.data as typeof Session.Event.Error.data.Type
            if (data.error) {
              const api = SessionV1.APIError.isInstance(data.error)
              eventErrors.push({
                name: data.error.name,
                message: api ? data.error.data.message : undefined,
                code: api ? data.error.data.metadata?.code : undefined,
              })
            }
            return Effect.void
          })
          yield* Effect.addFinalizer(() => off)
          yield* Effect.addFinalizer(() => Effect.sync(() => (cleanupFault.failed = false)))

          const configured = yield* provider.getProvider(providerID)
          configured.options.fetch = async (_input: RequestInfo | URL, init?: RequestInit) => {
            attempts.requests++
            const release = defer<void>()
            init?.signal?.addEventListener(
              "abort",
              () => {
                attempts.aborted++
                release.resolve()
              },
              { once: true },
            )
            const encoder = new TextEncoder()
            const event = (value: unknown) => `data: ${JSON.stringify(value)}\n\n`
            const initial = [
              {
                type: "response.created",
                response: {
                  id: `resp-process-stall-${attempts.requests}`,
                  created_at: Math.floor(Date.now() / 1000),
                  model: "gpt-6.1-sol",
                  service_tier: null,
                },
              },
              {
                type: "response.output_item.added",
                output_index: 0,
                item: { type: "reasoning", id: `rs-process-stall-${attempts.requests}`, encrypted_content: null },
              },
              {
                type: "response.reasoning_summary_part.added",
                item_id: `rs-process-stall-${attempts.requests}`,
                summary_index: 0,
              },
              {
                type: "response.reasoning_summary_text.delta",
                item_id: `rs-process-stall-${attempts.requests}`,
                summary_index: 0,
                delta: "provider reasoning before the stall",
              },
            ]
              .map(event)
              .join("")
            return new Response(
              new ReadableStream<Uint8Array>({
                start(controller) {
                  controller.enqueue(encoder.encode(initial))
                },
                async pull(controller) {
                  const action = await Promise.race([
                    release.promise.then(() => "release" as const),
                    new Promise<"keepalive">((resolve) => setTimeout(() => resolve("keepalive"), 20)),
                  ])
                  if (action === "release") {
                    controller.close()
                    return
                  }
                  attempts.keepalives++
                  controller.enqueue(encoder.encode(": keep-alive\n\n"))
                },
                cancel() {
                  attempts.cancelled++
                  release.resolve()
                },
              }),
              { status: 200, headers: { "content-type": "text/event-stream; charset=utf-8" } },
            )
          }

          const chat = yield* session.create({ title: "Stalled Foundry provider attempt" })
          const parent = yield* session.updateMessage({
            id: MessageID.ascending(),
            role: "user",
            sessionID: chat.id,
            agent: "build",
            model: { providerID, modelID },
            time: { created: Date.now() },
          })
          yield* session.updatePart({
            id: PartID.ascending(),
            messageID: parent.id,
            sessionID: chat.id,
            type: "text",
            text: "Write a short answer after reasoning.",
          })
          const root = path.resolve(dir)
          const assistantMessage: SessionV1.Assistant = {
            id: MessageID.ascending(),
            role: "assistant",
            sessionID: chat.id,
            mode: "build",
            agent: "build",
            path: { cwd: root, root },
            cost: 0,
            tokens: { total: 0, input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
            modelID,
            providerID,
            parentID: parent.id,
            time: { created: Date.now() },
            finish: "end_turn",
          }
          yield* session.updateMessage(assistantMessage)
          const model = yield* provider.getModel(providerID, modelID)
          const handle = yield* processors.create({ assistantMessage, sessionID: chat.id, model })
          const input = {
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID, modelID, variant: "high" },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "Write a short answer after reasoning." }],
            tools: {},
          } satisfies LLM.StreamInput

          const result = yield* awaitWithTimeout(
            handle.process(input),
            "processor did not terminalize the stalled Foundry attempts",
            "90 seconds",
          )
          const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: assistantMessage.id })
          const finalStatus = yield* status.get(chat.id)
          yield* off

          expect(cleanupFault.failed).toBe(true)
          expect(result).toBe("stop")
          expect(attempts.requests).toBe(6)
          expect(attempts.aborted).toBe(attempts.requests)
          expect(attempts.cancelled).toBe(attempts.requests)
          expect(attempts.keepalives).toBeGreaterThan(0)
          expect(finalStatus).toMatchObject({ type: "idle" })
          expect(eventErrors[0]).toMatchObject({
            name: "APIError",
            message: "SSE generation progress timed out",
            code: "ProviderResponseStreamError",
          })
          expect(eventErrors.filter((error) => error.code === "ProviderResponseStreamError")).toHaveLength(1)
          expect(stored.info.role).toBe("assistant")
          if (stored.info.role === "assistant") {
            expect(stored.info.time.completed).toBeDefined()
            expect(stored.info.error?.name).toBe("APIError")
            if (SessionV1.APIError.isInstance(stored.info.error)) {
              expect(stored.info.error.data.message).toBe("SSE generation progress timed out")
              expect(stored.info.error.data.metadata?.code).toBe("ProviderResponseStreamError")
            }
          }
        }),
      {
        config: (url) => ({
          model: "swx-azure/gpt-6.1-sol",
          enabled_providers: ["swx-azure"],
          provider: {
            "swx-azure": {
              name: "swXtch Foundry",
              npm: "@ai-sdk/openai",
              options: { apiKey: "test-key", baseURL: url, chunkTimeout: 100, headerTimeout: false },
              models: {
                "gpt-6.1-sol": {
                  id: "gpt-6.1-sol",
                  name: "GPT-6.1 Sol",
                  reasoning: true,
                  tool_call: true,
                  provider: { npm: "@ai-sdk/openai" },
                  options: { reasoningEffort: "high", reasoningSummary: "auto" },
                },
              },
            },
          },
        }),
      },
    ),
  100_000,
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
          expect(eventErrors).toHaveLength(2)
          expect(eventErrors[0]).toContain("original provider failure")
          expect(eventErrors[1]).toContain("one-shot text finalization failure")
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

const itHeldLock = testEffect(
  LayerNode.compile(LayerNode.group([root, SessionRunState.node]), [
    ...replacements,
    [Database.node, Database.layerFromPath(heldLockDbPath)],
    [Session.node, heldLockSession],
    [LLM.node, heldLockLLM],
  ]),
)

// Runs a turn whose reasoning-end write takes a real write lock that stays held after the processor exits.
const failTurnUnderHeldLock = Effect.fn("test.failTurnUnderHeldLock")(function* (
  dir: string,
  trigger: HeldLock["trigger"] = "reasoning-end",
) {
  const { processors, session, provider } = yield* boot()
  const events = yield* EventV2Bridge.Service
  const statuses = yield* SessionStatus.Service
  const chat = yield* session.create({})
  const parent = yield* user(chat.id, "hold the write lock")
  const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
  const model = yield* provider.getModel(ref.providerID, ref.modelID)
  const lock: HeldLock = { messageID: msg.id, trigger, streaming: defer<void>(), resume: defer<void>(), replayed: 0 }
  heldLocks.set(chat.id, lock)
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      heldLocks.delete(chat.id)
      if (lock.holder?.inTransaction) lock.holder.run("ROLLBACK")
      lock.holder?.close()
    }),
  )
  const terminal = yield* Deferred.make<void>()
  const off = yield* events.listen((event) => {
    if (event.type !== SessionV1.Event.MessageUpdated.type) return Effect.void
    const info = (event.data as typeof SessionV1.Event.MessageUpdated.data.Type).info
    if (info.id !== msg.id || info.role !== "assistant" || info.time.completed === undefined) return Effect.void
    return Deferred.succeed(terminal, undefined).pipe(Effect.asVoid)
  })
  yield* Effect.addFinalizer(() => off)

  const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
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
      model,
      agent: agent(),
      system: [],
      messages: [{ role: "user", content: "hold the write lock" }],
      tools: {},
    })
    .pipe(Effect.exit, Effect.forkChild)

  // Mid-stream, the owner reports the turn as active and it is not terminal.
  yield* Effect.promise(() => lock.streaming.promise)
  expect((yield* statuses.get(chat.id)).type).toBe("busy")
  const streaming = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
  expect(streaming.info.role === "assistant" && streaming.info.time.completed).toBeUndefined()
  lock.resume.resolve()
  const exit = yield* Fiber.join(run)

  // The turn failed with the lock still held, so none of its terminalizing writes landed.
  expect(lock.holder?.inTransaction).toBe(true)
  const locked = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
  expect(locked.info.role === "assistant" && locked.info.time.completed).toBeUndefined()
  return { chat, msg, lock, exit, terminal }
})

itHeldLock.live(
  "processor persists a failed turn once a lock held across every terminalizing write is released",
  () =>
    provideTmpdirInstance(
      (dir) =>
        Effect.gen(function* () {
          const statuses = yield* SessionStatus.Service
          const runs = yield* SessionRunState.Service
          const { chat, msg, lock, exit, terminal } = yield* failTurnUnderHeldLock(dir)

          // Cancelling the now-idle session does not stand in for terminalizing it.
          yield* runs.cancel(chat.id)
          expect((yield* statuses.get(chat.id)).type).toBe("idle")
          const cancelled = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
          expect(cancelled.info.role === "assistant" && cancelled.info.time.completed).toBeUndefined()

          lock.holder?.run("ROLLBACK")
          yield* Deferred.await(terminal).pipe(
            Effect.timeoutOrElse({
              duration: "10 seconds",
              orElse: () => Effect.die(new Error("the failed turn was not persisted after the lock was released")),
            }),
          )
          const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
          expect(stored.info.role).toBe("assistant")
          if (stored.info.role === "assistant") {
            expect(stored.info.time.completed).toBeDefined()
            expect(stored.info.error?.name).toBe("UnknownError")
            if (stored.info.error?.name === "UnknownError") {
              expect(stored.info.error.data.message.toLowerCase()).toContain("database is locked")
            }
          }
          expect(Exit.isSuccess(exit)).toBe(true)
          if (Exit.isSuccess(exit)) expect(exit.value).toBe("stop")
        }),
      { config: cfg },
    ),
  60_000,
)

itHeldLock.live(
  "processor keeps reporting a terminal write that a held lock keeps refusing",
  () =>
    provideTmpdirInstance(
      (dir) =>
        Effect.gen(function* () {
          const progress: unknown[] = []
          const capture = Logger.make((options) => {
            const [text, fields] = Array.isArray(options.message) ? options.message : [options.message]
            if (text === "still waiting for SQLite to persist failed assistant") progress.push(fields)
          })
          const loggers = yield* Effect.service(Logger.CurrentLoggers)
          const { chat, msg, lock, terminal } = yield* failTurnUnderHeldLock(dir).pipe(
            Effect.provideService(Logger.CurrentLoggers, new Set([...loggers, capture])),
          )

          // The retry keeps reporting while the lock refuses it, naming only the session, message and attempts.
          yield* waitFor(
            Effect.sync(() => (progress.length >= 2 ? true : undefined)),
            "the refused terminal write reported no progress",
            120_000,
          )
          expect(progress.slice(0, 2)).toEqual([
            { "session.id": chat.id, messageID: msg.id, attempts: 4 },
            { "session.id": chat.id, messageID: msg.id, attempts: 8 },
          ])
          // Reporting progress is not persisting: the row stays non-terminal until the write commits.
          expect(yield* Deferred.isDone(terminal)).toBe(false)
          const waiting = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
          expect(waiting.info.role === "assistant" && waiting.info.time.completed).toBeUndefined()

          lock.holder?.run("ROLLBACK")
          yield* Deferred.await(terminal).pipe(
            Effect.timeoutOrElse({
              duration: "10 seconds",
              orElse: () => Effect.die(new Error("the failed turn was not persisted after the lock was released")),
            }),
          )
          const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
          expect(stored.info.role === "assistant" && stored.info.time.completed).toBeDefined()
        }),
      { config: cfg },
    ),
  180_000,
)

itHeldLock.live(
  "processor does not recreate a failed turn removed before the lock-blocked terminal write lands",
  () =>
    provideTmpdirInstance(
      (dir) =>
        Effect.gen(function* () {
          const session = yield* Session.Service
          const { chat, msg, lock, terminal } = yield* failTurnUnderHeldLock(dir)
          expect(lock.produced && sqliteLockMessage(lock.produced)).toBe("Database is locked (SQLITE_BUSY)")

          // Keep the retry off the database while the lock is released and the assistant is removed.
          const skipped = yield* Deferred.make<void>()
          lock.skipped = skipped
          lock.replay = lock.produced
          yield* waitFor(
            Effect.sync(() => (lock.replayed > 0 ? true : undefined)),
            "the terminal write was not retried",
            5_000,
          )
          lock.holder?.run("ROLLBACK")
          yield* session.removeMessage({ sessionID: chat.id, messageID: msg.id })
          expect(Exit.isFailure(yield* Effect.exit(MessageV2.get({ sessionID: chat.id, messageID: msg.id })))).toBe(
            true,
          )
          lock.replay = undefined

          const outcome = yield* Effect.raceFirst(
            Deferred.await(terminal).pipe(Effect.as("recreated" as const)),
            Deferred.await(skipped).pipe(Effect.as("skipped" as const)),
          ).pipe(
            Effect.timeoutOrElse({
              duration: "10 seconds",
              orElse: () => Effect.die(new Error("the terminal write was not retried after the removal")),
            }),
          )
          expect(outcome).toBe("skipped")
          expect(Exit.isFailure(yield* Effect.exit(MessageV2.get({ sessionID: chat.id, messageID: msg.id })))).toBe(
            true,
          )
        }),
      { config: cfg },
    ),
  60_000,
)

// Captures halt's error line, which carries the failed operation's recorded origin.
const haltLines = () => {
  const lines: Record<string, unknown>[] = []
  const logger = Logger.make((options) => {
    const [text, fields] = Array.isArray(options.message) ? options.message : [options.message]
    if (options.logLevel === "Error" && text === "process" && typeof fields === "object" && fields !== null)
      lines.push(fields)
  })
  return { lines, logger }
}

for (const scenario of [
  {
    trigger: "reasoning-end",
    event: SessionV1.Event.PartUpdated.type,
    callSite: ["Session.updatePart", "SessionProcessor.finishReasoning"],
  },
  { trigger: "step-finish", event: SessionV1.Event.MessageUpdated.type, callSite: ["Session.updateMessage"] },
] as const) {
  itHeldLock.live(
    `processor halt names the ${scenario.trigger} write that met the held lock`,
    () =>
      provideTmpdirInstance(
        (dir) =>
          Effect.gen(function* () {
            const halt = haltLines()
            const loggers = yield* Effect.service(Logger.CurrentLoggers)
            const { lock, exit, terminal } = yield* failTurnUnderHeldLock(dir, scenario.trigger).pipe(
              Effect.provideService(Logger.CurrentLoggers, new Set([...loggers, halt.logger])),
            )
            expect(lock.produced && sqliteLockMessage(lock.produced)).toBe("Database is locked (SQLITE_BUSY)")

            expect(halt.lines).toHaveLength(1)
            expect(String(halt.lines[0].error).toLowerCase()).toContain("database is locked (sqlite_busy)")
            const failure = halt.lines[0].failure as Record<string, unknown> | undefined
            expect(failure).toMatchObject({ event: scenario.event, stage: "handler", case: scenario.trigger })
            // The span chain runs from the failed commit outward, through the helper the case called.
            const frames = String(failure?.callSite).split(" < ")
            expect(frames[0]).toBe(scenario.callSite[0])
            const positions = scenario.callSite.map((name) => frames.indexOf(name))
            expect(positions.every((position) => position >= 0)).toBe(true)
            expect(positions).toEqual(positions.toSorted((a, b) => a - b))

            // The diagnostics leave the stop, its idle state and the eventual terminal write as they were.
            expect(Exit.isSuccess(exit) && exit.value).toBe("stop")
            lock.holder?.run("ROLLBACK")
            yield* Deferred.await(terminal).pipe(
              Effect.timeoutOrElse({
                duration: "10 seconds",
                orElse: () => Effect.die(new Error("the failed turn was not persisted after the lock was released")),
              }),
            )
          }),
        { config: cfg },
      ),
    80_000,
  )
}

itStreamLock.live(
  "processor halt attributes a provider stream lock failure to the stream, not the last handled case",
  () =>
    provideTmpdirInstance(
      (dir) =>
        Effect.gen(function* () {
          const { processors, session, provider } = yield* boot()
          const database = yield* Database.Service
          const chat = yield* session.create({})
          const parent = yield* user(chat.id, "stream lock origin")
          const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
          const model = yield* provider.getModel(ref.providerID, ref.modelID)
          const error = yield* produceSqliteBusyError(database.db, streamLockDbPath)
          streamLockFailures.set(chat.id, error)
          yield* Effect.addFinalizer(() => Effect.sync(() => streamLockFailures.delete(chat.id)))

          const halt = haltLines()
          const loggers = yield* Effect.service(Logger.CurrentLoggers)
          const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
          const result = yield* handle
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
              model,
              agent: agent(),
              system: [],
              messages: [{ role: "user", content: "stream lock origin" }],
              tools: {},
            })
            .pipe(Effect.provideService(Logger.CurrentLoggers, new Set([...loggers, halt.logger])))

          expect(result).toBe("stop")
          expect(halt.lines).toHaveLength(1)
          expect(String(halt.lines[0].error).toLowerCase()).toContain("database is locked (sqlite_busy)")
          const failure = halt.lines[0].failure as Record<string, unknown> | undefined
          expect(failure).toMatchObject({ stage: "stream" })
          // The call site is the failing callable inside the stream, not the processor that observed the failure.
          expect(String(failure?.callSite).split(" < ")[0]).toBe("test.failProviderStream")
          expect(failure).not.toHaveProperty("event")
          expect(failure).not.toHaveProperty("case")
          // The handled reasoning case wrote its part before the stream failed.
          const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
          expect(stored.parts.some((part) => part.type === "reasoning")).toBe(true)
        }),
      { config: cfg },
    ),
  30_000,
)

itPreludeInterrupt.live("session.processor classifies a prelude interrupt as an aborted assistant", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        preludeInterruptStreamCalls.count = 0
        preludeInterruptGate.entered = undefined
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service
        const statuses = yield* SessionStatus.Service
        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "interrupt in process prelude")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const model = yield* provider.getModel(ref.providerID, ref.modelID)
        const errors: string[] = []
        const off = yield* events.listen((event) => {
          if (event.type !== Session.Event.Error.type) return Effect.void
          const data = event.data as typeof Session.Event.Error.data.Type
          if (data.sessionID === chat.id && data.error) errors.push(data.error.name)
          return Effect.void
        })
        yield* Effect.addFinalizer(() => off)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
        const entered = yield* Deferred.make<void>()
        preludeInterruptGate.entered = entered
        yield* Effect.addFinalizer(() => Effect.sync(() => void (preludeInterruptGate.entered = undefined)))
        const fiber = yield* handle.process({
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
          messages: [{ role: "user", content: "interrupt in process prelude" }],
          tools: {},
        }).pipe(Effect.forkChild)

        // Config.get is the prelude immediately before the stream's onInterrupt handler is installed.
        yield* Deferred.await(entered).pipe(Effect.timeout("5 seconds"))
        yield* Fiber.interrupt(fiber)
        const exit = yield* Fiber.await(fiber)
        const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
        yield* off

        expect(Exit.isFailure(exit)).toBe(true)
        expect(preludeInterruptStreamCalls.count).toBe(0)
        expect((yield* statuses.get(chat.id)).type).toBe("idle")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") {
          expect(stored.info.time.completed).toBeDefined()
          expect(stored.info.error?.name).toBe("MessageAbortedError")
        }
        expect(errors).toEqual(["MessageAbortedError"])
      }),
    { config: cfg },
  ),
  20_000,
)

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

// ---------------------------------------------------------------------------
// Tool execution custody (#234): a locally executed call is owned and durably
// admitted before its plugin hook or body runs, and cancellation settles every
// tool part the attempt created. Effects are private marker files.
// ---------------------------------------------------------------------------

const custodyDbPath = path.join(import.meta.dir, `.opencode-tool-custody-${crypto.randomUUID()}.db`)
const nativeCustodyDbPath = path.join(import.meta.dir, `.opencode-native-custody-${crypto.randomUUID()}.db`)

afterAll(async () => {
  await Promise.all(
    [custodyDbPath, nativeCustodyDbPath]
      .flatMap((file) => [file, `${file}-wal`, `${file}-shm`])
      .map((file) => rm(file, { force: true })),
  )
})

const itCustody = testEffect(
  LayerNode.compile(
    LayerNode.group([root, LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })]),
    [...replacements, [Database.node, Database.layerFromPath(custodyDbPath)]],
  ),
)

type StoredToolPart = {
  id: string
  sessionID: string
  messageID: string
  type: string
  tool: string
  callID: string
  state: SessionV1.ToolPart["state"]
}

// Reads through a connection of its own, as a process reopening the store would, so nothing the processor holds in
// memory can stand in for what was committed.
function storedRows(dbPath: string, sql: string, id: string) {
  const db = new Sqlite(dbPath, { readonly: true })
  try {
    db.exec("PRAGMA busy_timeout = 5000")
    return db.query<{ id: string; session_id: string; message_id?: string; data: string }, [string]>(sql).all(id)
  } finally {
    db.close()
  }
}

function storedToolPart(dbPath: string, messageID: string, callID: string) {
  return storedRows(
    dbPath,
    "select id, session_id, message_id, data from part where message_id = ? order by id",
    messageID,
  )
    .map(
      (row) =>
        ({
          ...JSON.parse(row.data),
          id: row.id,
          sessionID: row.session_id,
          messageID: row.message_id,
        }) as StoredToolPart,
    )
    .find((part) => part.type === "tool" && part.callID === callID)
}

function storedAssistant(dbPath: string, messageID: string) {
  const row = storedRows(dbPath, "select id, session_id, data from message where id = ?", messageID)[0]
  return row ? (JSON.parse(row.data) as Pick<SessionV1.Assistant, "time" | "error">) : undefined
}

type CustodyEntry = { callID: string; stored: StoredToolPart | undefined }

// Resolves the production tool wrappers for one `probe` tool against a real processor handle. The plugin hook and the
// tool body each record the stored part they started under, then the body runs the case's private effect.
const custodyTurn = Effect.fn("test.custodyTurn")(function* (input: {
  dir: string
  dbPath: string
  body: (args: { value: number }, ctx: Tool.Context) => Effect.Effect<void>
  // Runs when the runtime dispatches a call, before the production wrapper receives it.
  dispatch?: (args: { value: number }) => Effect.Effect<void>
}) {
  const { processors, session, provider } = yield* boot()
  const chat = yield* session.create({})
  const parent = yield* user(chat.id, "probe")
  const msg = yield* assistant(chat.id, parent.id, path.resolve(input.dir))
  const model = yield* provider.getModel(ref.providerID, ref.modelID)
  const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
  const plugin: CustodyEntry[] = []
  const body: CustodyEntry[] = []
  const executions: Promise<"fulfilled" | "rejected">[] = []
  const dispatched = Deferred.makeUnsafe<string>()
  const read = (callID: string | undefined) => storedToolPart(input.dbPath, msg.id, callID ?? "")

  const tools = yield* SessionTools.resolve({
    agent: agent(),
    model,
    session: chat,
    processor: handle,
    bypassAgentCheck: false,
    messages: [],
    promptOps: {} as never,
  }).pipe(
    Effect.provideService(
      Plugin.Service,
      Plugin.Service.of({
        init: () => Effect.void,
        list: () => Effect.succeed([]),
        trigger: (name, hook, output) =>
          Effect.sync(() => {
            if (name !== "tool.execute.before") return output
            const callID = (hook as { callID: string }).callID
            plugin.push({ callID, stored: read(callID) })
            return output
          }),
      }),
    ),
    Effect.provideService(
      Permission.Service,
      Permission.Service.of({
        ask: () => Effect.void,
        reply: () => Effect.die(new Error("unexpected permission reply")),
        list: () => Effect.succeed([]),
        grant: () => Effect.die(new Error("unexpected permission grant")),
      }),
    ),
    Effect.provideService(
      MCP.Service,
      MCP.Service.of({
        tools: () => Effect.succeed({}),
        clients: () => Effect.succeed({}),
      } as Partial<MCP.Interface> as MCP.Interface),
    ),
    Effect.provideService(
      Truncate.Service,
      Truncate.Service.of({
        cleanup: () => Effect.void,
        write: () => Effect.succeed("output.txt"),
        output: (text: string) => Effect.succeed({ content: text, truncated: false }),
        limits: () => Effect.succeed({ maxLines: 2000, maxBytes: 50 * 1024 }),
      }),
    ),
    Effect.provideService(
      ToolRegistry.Service,
      ToolRegistry.Service.of({
        ids: () => Effect.succeed(["probe"]),
        all: () => Effect.succeed([]),
        named: () => Effect.die("unused"),
        tools: () =>
          Effect.succeed([
            {
              id: "probe",
              description: "Writes a private effect marker",
              parameters: Schema.Struct({ value: Schema.Number }),
              jsonSchema: { type: "object", properties: { value: { type: "number" } }, required: ["value"] },
              execute: (args: { value: number }, ctx: Tool.Context) =>
                Effect.gen(function* () {
                  body.push({ callID: ctx.callID ?? "", stored: read(ctx.callID) })
                  yield* input.body(args, ctx)
                  return { title: "probe", metadata: {}, output: `probe:${args.value}` }
                }),
            } satisfies Tool.Def,
          ]),
      }),
    ),
    Effect.provide(RuntimeFlags.layer()),
  )
  const execute = tools.probe?.execute
  if (!execute) throw new Error("probe tool is missing execute")
  // Observes the runtime entering the production wrapper, independently of anything the wrapper then runs.
  tools.probe = {
    ...tools.probe,
    execute: (args, options) => {
      const run = (input.dispatch ? Effect.runPromise(input.dispatch(args)) : Promise.resolve()).then(
        () => execute(args, options) as PromiseLike<unknown>,
      )
      executions.push(
        run.then(
          () => "fulfilled" as const,
          () => "rejected" as const,
        ),
      )
      Deferred.doneUnsafe(dispatched, Exit.succeed(options.toolCallId))
      return run
    },
  }

  const process = handle.process({
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
    messages: [{ role: "user", content: "probe" }],
    tools,
  })
  return { chat, msg, tools, plugin, body, executions, dispatched, process }
})

// Holds the post-commit publication of the first tool part update that matches: the part is committed, but its writer
// has not yet returned from writing it.
type PublishedToolPart = Extract<typeof SessionV1.Event.PartUpdated.data.Type.part, { type: "tool" }>
const holdPartPublication = Effect.fn("test.holdPartPublication")(function* (
  matches: (part: PublishedToolPart) => boolean,
) {
  const events = yield* EventV2Bridge.Service
  const committed = Deferred.makeUnsafe<string>()
  const release = Deferred.makeUnsafe<void>()
  let held = false
  const off = yield* events.listen((event) => {
    if (held || event.type !== SessionV1.Event.PartUpdated.type) return Effect.void
    const part = (event.data as typeof SessionV1.Event.PartUpdated.data.Type).part
    if (part.type !== "tool" || !matches(part)) return Effect.void
    held = true
    return Deferred.succeed(committed, part.id).pipe(Effect.andThen(Deferred.await(release)))
  })
  // A failed assertion must not leave the writer held.
  yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.andThen(off)))
  return { committed, release, off }
})

// Holds the post-commit publication of a call's pending part: the part is committed, but the processor has not yet
// returned from writing it, which is the window the AI SDK's independent dispatch can run in.
const holdPendingCommit = (callID: string) =>
  holdPartPublication((part) => part.callID === callID && part.state.status === "pending")

const writeMarker = (file: string, text: string) => Effect.promise(() => Bun.write(file, text)).pipe(Effect.asVoid)

itCustody.live("admits an AI SDK-dispatched tool durably before its plugin hook or body runs", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const marker = path.join(dir, "probe-effect")
        const bodyEntered = Deferred.makeUnsafe<void>()
        const effectGate = Deferred.makeUnsafe<void>()
        const effectWritten = Deferred.makeUnsafe<void>()
        const settleGate = Deferred.makeUnsafe<void>()
        yield* llm.tool("probe", { value: 1 })
        const hold = yield* holdPendingCommit("call_1")
        const turn = yield* custodyTurn({
          dir,
          dbPath: custodyDbPath,
          body: (args) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(bodyEntered, undefined)
              yield* Deferred.await(effectGate)
              yield* writeMarker(marker, `effect:${args.value}`)
              yield* Deferred.succeed(effectWritten, undefined)
              yield* Deferred.await(settleGate)
            }),
        })
        const run = yield* turn.process.pipe(Effect.forkChild)

        const pendingID = yield* awaitWithTimeout(Deferred.await(hold.committed), "pending part was never committed")
        expect(yield* awaitWithTimeout(Deferred.await(turn.dispatched), "AI SDK never dispatched the call")).toBe(
          "call_1",
        )
        yield* Deferred.succeed(hold.release, undefined)
        yield* awaitWithTimeout(Deferred.await(bodyEntered), "admitted tool body never ran")

        const admitted = {
          id: pendingID,
          sessionID: turn.chat.id,
          messageID: turn.msg.id,
          callID: "call_1",
          tool: "probe",
          state: { status: "running", input: { value: 1 } },
        }
        expect(turn.plugin).toMatchObject([{ callID: "call_1", stored: admitted }])
        expect(turn.body).toMatchObject([{ callID: "call_1", stored: admitted }])

        // The admission is the same stored record before and after the effect, read fresh each time: it records that
        // execution was allowed, never whether the effect happened.
        const beforeEffect = storedToolPart(custodyDbPath, turn.msg.id, "call_1")
        expect(beforeEffect).toMatchObject(admitted)
        expect(existsSync(marker)).toBe(false)
        yield* Deferred.succeed(effectGate, undefined)
        yield* awaitWithTimeout(Deferred.await(effectWritten), "probe effect was never written")
        expect(existsSync(marker)).toBe(true)
        expect(storedToolPart(custodyDbPath, turn.msg.id, "call_1")).toEqual(beforeEffect!)
        // Built locally: a shared build would hand back the processor's own Database.
        const reopened = yield* MessageV2.parts(turn.msg.id).pipe(
          Effect.provide(Database.layerFromPath(custodyDbPath), { local: true }),
        )
        expect(reopened.find((part) => part.type === "tool" && part.callID === "call_1")).toMatchObject({
          id: pendingID,
          state: beforeEffect!.state,
        })
        expect(storedToolPart(custodyDbPath, turn.msg.id, "call_1")).toEqual(beforeEffect!)

        yield* Deferred.succeed(settleGate, undefined)
        expect(yield* Fiber.join(run)).toBe("continue")
        yield* hold.off
        expect(storedToolPart(custodyDbPath, turn.msg.id, "call_1")).toMatchObject({
          id: pendingID,
          state: { status: "completed", input: { value: 1 }, output: "probe:1" },
        })
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itCustody.live("settles an AI SDK-dispatched tool cancelled before its processor owns it, without running it", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const marker = path.join(dir, "probe-effect")
        yield* llm.tool("probe", { value: 2 })
        const hold = yield* holdPendingCommit("call_1")
        const turn = yield* custodyTurn({
          dir,
          dbPath: custodyDbPath,
          body: (args) => writeMarker(marker, `effect:${args.value}`),
        })
        const run = yield* turn.process.pipe(Effect.forkChild)

        const pendingID = yield* awaitWithTimeout(Deferred.await(hold.committed), "pending part was never committed")
        yield* awaitWithTimeout(Deferred.await(turn.dispatched), "AI SDK never dispatched the call")
        yield* Fiber.interrupt(run)
        yield* hold.off

        expect(yield* Effect.promise(() => turn.executions[0]!)).toBe("rejected")
        expect(turn.plugin).toEqual([])
        expect(turn.body).toEqual([])
        expect(existsSync(marker)).toBe(false)
        expect(storedToolPart(custodyDbPath, turn.msg.id, "call_1")).toMatchObject({
          id: pendingID,
          callID: "call_1",
          state: { status: "error", error: "Tool execution aborted", metadata: { interrupted: true } },
        })
        const message = storedAssistant(custodyDbPath, turn.msg.id)
        expect(message?.time.completed).toBeDefined()
        expect(message?.error?.name).toBe("MessageAbortedError")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itCustody.live("settles an admitted tool on cancellation and refuses the effect it gates on metadata", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const marker = path.join(dir, "probe-effect")
        const bodyEntered = Deferred.makeUnsafe<void>()
        const proceed = Deferred.makeUnsafe<void>()
        yield* llm.tool("probe", { value: 3 })
        const turn = yield* custodyTurn({
          dir,
          dbPath: custodyDbPath,
          // Like shell, which spawns only after its metadata update.
          body: (args, ctx) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(bodyEntered, undefined)
              yield* Deferred.await(proceed)
              yield* ctx.metadata({ metadata: { phase: "spawning" } })
              yield* writeMarker(marker, `effect:${args.value}`)
            }),
        })
        const run = yield* turn.process.pipe(Effect.forkChild)

        yield* awaitWithTimeout(Deferred.await(bodyEntered), "admitted tool body never ran")
        yield* Fiber.interrupt(run)
        const interrupted = storedToolPart(custodyDbPath, turn.msg.id, "call_1")
        expect(interrupted).toMatchObject({
          callID: "call_1",
          state: { status: "error", error: "Tool execution aborted", metadata: { interrupted: true } },
        })

        yield* Deferred.succeed(proceed, undefined)
        expect(yield* Effect.promise(() => turn.executions[0]!)).toBe("rejected")
        expect(existsSync(marker)).toBe(false)
        expect(storedToolPart(custodyDbPath, turn.msg.id, "call_1")).toEqual(interrupted!)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itCustody.live("keeps progress and effect evidence when a tool is cancelled after its effect", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const marker = path.join(dir, "probe-effect")
        const effectWritten = Deferred.makeUnsafe<void>()
        const finish = Deferred.makeUnsafe<void>()
        yield* llm.tool("probe", { value: 4 })
        const turn = yield* custodyTurn({
          dir,
          dbPath: custodyDbPath,
          body: (args, ctx) =>
            Effect.gen(function* () {
              yield* ctx.metadata({ metadata: { progress: "effect written" } })
              yield* writeMarker(marker, `effect:${args.value}`)
              yield* Deferred.succeed(effectWritten, undefined)
              yield* Deferred.await(finish)
            }),
        })
        const run = yield* turn.process.pipe(Effect.forkChild)

        yield* awaitWithTimeout(Deferred.await(effectWritten), "probe effect was never written")
        yield* Fiber.interrupt(run)
        yield* Deferred.succeed(finish, undefined)
        yield* Effect.promise(() => turn.executions[0]!)

        // The interrupted disposition records the cancellation; the effect happened anyway.
        expect(existsSync(marker)).toBe(true)
        expect(storedToolPart(custodyDbPath, turn.msg.id, "call_1")).toMatchObject({
          callID: "call_1",
          state: {
            status: "error",
            error: "Tool execution aborted",
            input: { value: 4 },
            metadata: { progress: "effect written", interrupted: true },
          },
        })
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itCustody.live("refuses a tool dispatched after its turn ended, though the same tool ran within the turn", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        yield* llm.tool("probe", { value: 5 })
        const turn = yield* custodyTurn({
          dir,
          dbPath: custodyDbPath,
          body: (args, ctx) => writeMarker(path.join(dir, `probe-effect-${ctx.callID}`), `effect:${args.value}`),
        })

        expect(yield* turn.process).toBe("continue")
        expect(existsSync(path.join(dir, "probe-effect-call_1"))).toBe(true)
        expect(storedToolPart(custodyDbPath, turn.msg.id, "call_1")).toMatchObject({
          state: { status: "completed", output: "probe:5" },
        })

        const late = yield* awaitWithTimeout(
          Effect.promise(() =>
            Promise.resolve(
              turn.tools.probe!.execute!(
                { value: 6 },
                { toolCallId: "call_late", messages: [], abortSignal: new AbortController().signal },
              ) as PromiseLike<unknown>,
            ).then(
              () => "fulfilled",
              () => "rejected",
            ),
          ),
          "a call dispatched after its turn ended was left waiting for admission",
        )
        expect(late).toBe("rejected")
        expect(turn.plugin.map((entry) => entry.callID)).toEqual(["call_1"])
        expect(turn.body.map((entry) => entry.callID)).toEqual(["call_1"])
        expect(existsSync(path.join(dir, "probe-effect-call_late"))).toBe(false)
        expect(storedToolPart(custodyDbPath, turn.msg.id, "call_late")).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itCustody.live("settles a cancelled turn without waiting on a running tool's held publication", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        yield* llm.tool("probe", { value: 10 })
        const hold = yield* holdPartPublication(
          (part) =>
            part.callID === "call_1" && part.state.status === "running" && part.state.metadata?.phase === "held",
        )
        const turn = yield* custodyTurn({
          dir,
          dbPath: custodyDbPath,
          body: (_args, ctx) => ctx.metadata({ metadata: { phase: "held" } }),
        })
        const run = yield* turn.process.pipe(Effect.forkChild)

        // The executing tool's progress is committed, and its own publication of it is held.
        const partID = yield* awaitWithTimeout(Deferred.await(hold.committed), "the tool's progress never committed")
        yield* awaitWithTimeout(Fiber.interrupt(run), "cancellation waited on the executing tool's publication")
        const settled = storedToolPart(custodyDbPath, turn.msg.id, "call_1")
        expect(settled).toMatchObject({
          id: partID,
          state: {
            status: "error",
            error: "Tool execution aborted",
            input: { value: 10 },
            metadata: { phase: "held", interrupted: true },
          },
        })

        yield* Deferred.succeed(hold.release, undefined)
        yield* awaitWithTimeout(
          Effect.promise(() => turn.executions[0]!),
          "the released tool never finished",
        )
        expect(storedToolPart(custodyDbPath, turn.msg.id, "call_1")).toEqual(settled!)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itCustody.live("preserves an outcome committed before cancellation interrupts its settlement", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        yield* llm.tool("probe", { value: 11 })
        const hold = yield* holdPartPublication((part) => part.callID === "call_1" && part.state.status === "completed")
        const turn = yield* custodyTurn({ dir, dbPath: custodyDbPath, body: () => Effect.void })
        const run = yield* turn.process.pipe(Effect.forkChild)

        // The outcome is committed, but the consumer has not returned from writing it, so the call is still unsettled.
        yield* awaitWithTimeout(Deferred.await(hold.committed), "the tool's outcome never committed")
        const completed = storedToolPart(custodyDbPath, turn.msg.id, "call_1")
        expect(completed).toMatchObject({ state: { status: "completed", input: { value: 11 }, output: "probe:11" } })
        yield* awaitWithTimeout(Fiber.interrupt(run), "the interrupted turn never finished settling")
        expect(storedToolPart(custodyDbPath, turn.msg.id, "call_1")).toEqual(completed!)
        expect(storedAssistant(custodyDbPath, turn.msg.id)?.error?.name).toBe("MessageAbortedError")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

// A worker process runs an admitted tool, records what is stored, then loses its owner abruptly, before or after the
// tool's private effect. The test process reopens the store afterwards.
const ownerLoss = {
  db: process.env.OPENCODE_CUSTODY_OWNER_LOSS_DB,
  phase: process.env.OPENCODE_CUSTODY_OWNER_LOSS_PHASE,
  output: process.env.OPENCODE_CUSTODY_OWNER_LOSS_OUTPUT ?? "",
  marker: process.env.OPENCODE_CUSTODY_OWNER_LOSS_MARKER ?? "",
}
const ownerLossWorker = "custody-owner-loss-worker kills its process while an admitted tool runs"

if (ownerLoss.db) {
  const db = ownerLoss.db
  testEffect(
    LayerNode.compile(
      LayerNode.group([root, LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })]),
      [...replacements, [Database.node, Database.layerFromPath(db)]],
    ),
  ).live(
    ownerLossWorker,
    () =>
      provideTmpdirServer(
        ({ dir, llm }) =>
          Effect.gen(function* () {
            yield* llm.tool("probe", { value: 1 })
            const owner = { sessionID: "", messageID: "", plugin: [] as CustodyEntry[], body: [] as CustodyEntry[] }
            const turn = yield* custodyTurn({
              dir,
              dbPath: db,
              body: (args) =>
                Effect.gen(function* () {
                  if (ownerLoss.phase === "after") yield* writeMarker(ownerLoss.marker, `effect:${args.value}`)
                  const record = { ...owner, dir, stored: storedToolPart(db, owner.messageID, "call_1") }
                  yield* Effect.promise(() => Bun.write(ownerLoss.output, JSON.stringify(record)))
                  process.kill(process.pid, "SIGKILL")
                  yield* Effect.never
                }),
            })
            Object.assign(owner, {
              sessionID: turn.chat.id,
              messageID: turn.msg.id,
              plugin: turn.plugin,
              body: turn.body,
            })
            yield* turn.process
          }),
        { config: (url) => providerCfg(url) },
      ),
    30_000,
  )
}

it.live(
  "keeps an admitted tool's record unchanged when its owner is lost before or after its effect",
  () =>
    Effect.gen(function* () {
      const dir = yield* Effect.acquireRelease(
        Effect.promise(() => mkdtemp(path.join(os.tmpdir(), "opencode-owner-loss-"))),
        (dir) => Effect.promise(() => rm(dir, { recursive: true, force: true })),
      )
      for (const phase of ["before", "after"] as const) {
        const db = path.join(dir, `${phase}.db`)
        const output = path.join(dir, `${phase}.json`)
        const marker = path.join(dir, `${phase}-effect`)
        const worker = Bun.spawn(
          [process.execPath, "test", "test/session/processor-effect.test.ts", `--test-name-pattern=${ownerLossWorker}`],
          {
            cwd: path.join(import.meta.dir, "../.."),
            env: {
              ...process.env,
              OPENCODE_CUSTODY_OWNER_LOSS_DB: db,
              OPENCODE_CUSTODY_OWNER_LOSS_PHASE: phase,
              OPENCODE_CUSTODY_OWNER_LOSS_OUTPUT: output,
              OPENCODE_CUSTODY_OWNER_LOSS_MARKER: marker,
            },
            stdout: "ignore",
            stderr: "ignore",
          },
        )
        yield* Effect.addFinalizer(() => Effect.sync(() => worker.kill()))
        yield* awaitWithTimeout(
          Effect.promise(() => worker.exited),
          `the ${phase}-effect worker never lost its owner`,
          "60 seconds",
        )
        expect(worker.signalCode).toBe("SIGKILL")
        const record = JSON.parse(yield* Effect.promise(() => Bun.file(output).text())) as {
          dir: string
          sessionID: string
          messageID: string
          stored: StoredToolPart
          plugin: CustodyEntry[]
          body: CustodyEntry[]
        }
        yield* Effect.addFinalizer(() => Effect.promise(() => rm(record.dir, { recursive: true, force: true })))

        // The owner admitted the call before its plugin hook and body ran, and was lost while it still held it.
        const admitted = {
          sessionID: record.sessionID,
          messageID: record.messageID,
          callID: "call_1",
          tool: "probe",
          state: { status: "running", input: { value: 1 } },
        }
        expect(record.stored).toMatchObject(admitted)
        expect(record.plugin).toEqual([{ callID: "call_1", stored: record.stored }])
        expect(record.body).toEqual([{ callID: "call_1", stored: record.stored }])
        expect(existsSync(marker)).toBe(phase === "after")

        // Reopened by a process that never owned it, the admission is the same record whether or not the effect
        // happened, and no outcome has been manufactured for the call or its turn.
        expect(storedToolPart(db, record.messageID, "call_1")).toEqual(record.stored)
        // Built locally: a shared build would hand back a Database this process already holds.
        const reopened = yield* MessageV2.parts(MessageID.make(record.messageID)).pipe(
          Effect.provide(Database.layerFromPath(db), { local: true }),
        )
        expect(reopened.find((part) => part.type === "tool" && part.callID === "call_1")).toMatchObject({
          id: record.stored.id,
          state: record.stored.state,
        })
        expect(storedToolPart(db, record.messageID, "call_1")).toEqual(record.stored)
        const message = storedAssistant(db, record.messageID)
        expect(message?.time.completed).toBeUndefined()
        expect(message?.error).toBeUndefined()
      }
    }),
  120_000,
)

// Forwards Session writes, letting a case act on a write before it reaches the store.
const custodyWrites: {
  before?: (part: SessionV1.Part) => Effect.Effect<void> | undefined
  read?: (partID: string) => Effect.Effect<void> | undefined
} = {}
const custodyWriteSession = LayerNode.make({
  service: Session.Service,
  layer: Layer.effect(
    Session.Service,
    Effect.gen(function* () {
      const real = yield* Session.Service
      return Session.Service.of({
        ...real,
        updatePart: <T extends SessionV1.Part>(part: T) => {
          const before = custodyWrites.before?.(part)
          return before ? before.pipe(Effect.andThen(real.updatePart(part))) : real.updatePart(part)
        },
        getPart: (input) => {
          const read = custodyWrites.read?.(input.partID)
          return read ? read.pipe(Effect.andThen(real.getPart(input))) : real.getPart(input)
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
const custodyWriteDbPath = path.join(import.meta.dir, `.opencode-tool-custody-writes-${crypto.randomUUID()}.db`)
afterAll(async () => {
  await Promise.all(
    [custodyWriteDbPath, `${custodyWriteDbPath}-wal`, `${custodyWriteDbPath}-shm`].map((file) =>
      rm(file, { force: true }),
    ),
  )
})
const itCustodyWrites = testEffect(
  LayerNode.compile(
    LayerNode.group([root, LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })]),
    [...replacements, [Session.node, custodyWriteSession], [Database.node, Database.layerFromPath(custodyWriteDbPath)]],
  ),
)
const resetCustodyWrites = Effect.addFinalizer(() =>
  Effect.sync(() => {
    custodyWrites.before = undefined
    custodyWrites.read = undefined
  }),
)

itCustodyWrites.live("settles a turn interrupted between owning a call and writing its pending part", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        yield* resetCustodyWrites
        yield* llm.tool("probe", { value: 12 })
        // Once the consumer has built the call's pending write, its next operation yields to the scheduler, and the
        // consumer is interrupted at that yield: before the write, or anything composed around it, can run.
        const yielded = Deferred.makeUnsafe<void>()
        const base = new Scheduler.MixedScheduler()
        let armed: Fiber.Fiber<unknown, unknown> | undefined
        let interrupt: Fiber.Fiber<unknown, unknown> | undefined
        const scheduler: Scheduler.Scheduler = {
          executionMode: base.executionMode,
          shouldYield: (fiber) => {
            if (fiber !== armed) return base.shouldYield(fiber)
            armed = undefined
            interrupt = fiber
            return true
          },
          makeDispatcher: () => {
            const dispatcher = base.makeDispatcher()
            return {
              scheduleTask: (task, priority) => {
                const fiber = interrupt
                interrupt = undefined
                if (fiber)
                  dispatcher.scheduleTask(() => {
                    fiber.interruptUnsafe()
                    Deferred.doneUnsafe(yielded, Exit.void)
                  }, priority)
                dispatcher.scheduleTask(task, priority)
              },
              flush: () => dispatcher.flush(),
            }
          },
        }
        custodyWrites.before = (part) => {
          if (part.type === "tool" && part.callID === "call_1" && part.state.status === "pending")
            armed = Fiber.getCurrent()
          return undefined
        }
        const turn = yield* custodyTurn({
          dir,
          dbPath: custodyWriteDbPath,
          body: (args) => writeMarker(path.join(dir, "probe-effect"), `effect:${args.value}`),
        })
        // Detached, so a turn that never settles fails this case's bound instead of holding the test's scope open.
        const run = yield* turn.process.pipe(Effect.provideService(Scheduler.Scheduler, scheduler), Effect.forkDetach)

        yield* awaitWithTimeout(Deferred.await(yielded), "the consumer was never interrupted at its pending write")
        yield* awaitWithTimeout(Fiber.await(run), "the interrupted turn never finished settling")
        expect(storedToolPart(custodyWriteDbPath, turn.msg.id, "call_1")?.state.status ?? "absent").not.toBe("pending")
        const message = storedAssistant(custodyWriteDbPath, turn.msg.id)
        expect(message?.time.completed).toBeDefined()
        expect(message?.error?.name).toBe("MessageAbortedError")
        yield* awaitWithTimeout(
          Effect.promise(() => Promise.all(turn.executions)),
          "a call dispatched by the interrupted turn was left waiting for admission",
        )
        expect(turn.plugin).toEqual([])
        expect(turn.body).toEqual([])
        expect(existsSync(path.join(dir, "probe-effect"))).toBe(false)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itCustodyWrites.live(
  "never admits a call a failed stream attempt dispatched against the retry that reuses its call ID",
  () =>
    provideTmpdirServer(
      ({ dir, llm }) =>
        Effect.gen(function* () {
          yield* resetCustodyWrites
          yield* llm.tool("probe", { value: 1 })
          yield* llm.tool("probe", { value: 2 })
          const marker = (value: number) => path.join(dir, `probe-effect-${value}`)
          const turn = yield* custodyTurn({
            dir,
            dbPath: custodyWriteDbPath,
            body: (args) => writeMarker(marker(args.value), `effect:${args.value}`),
          })
          // The first attempt's step start is held until the AI SDK has dispatched that attempt's call_1, then fails
          // retryably, so the attempt ends before its consumer registers the call. The retry dispatches call_1 again.
          let failed = false
          custodyWrites.before = (part) => {
            if (failed || part.type !== "step-start") return undefined
            failed = true
            return awaitWithTimeout(
              Deferred.await(turn.dispatched),
              "the first attempt never dispatched its call",
            ).pipe(
              Effect.andThen(
                Effect.die(Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET", syscall: "read" })),
              ),
              Effect.orDie,
            )
          }

          expect(yield* turn.process).toBe("continue")
          expect(yield* llm.calls).toBe(2)
          expect(
            yield* awaitWithTimeout(
              Effect.promise(() => Promise.all(turn.executions)),
              "the first attempt's call was left waiting for admission",
            ),
          ).toEqual(["rejected", "fulfilled"])
          const admitted = {
            callID: "call_1",
            stored: { callID: "call_1", state: { status: "running", input: { value: 2 } } },
          }
          expect(turn.plugin).toMatchObject([admitted])
          expect(turn.body).toMatchObject([admitted])
          expect(existsSync(marker(1))).toBe(false)
          expect(existsSync(marker(2))).toBe(true)
          expect(storedToolPart(custodyWriteDbPath, turn.msg.id, "call_1")).toMatchObject({
            state: { status: "completed", input: { value: 2 }, output: "probe:2" },
          })
        }),
      { config: (url) => providerCfg(url) },
    ),
  30_000,
)

// Runs a turn whose first stream attempt fails retryably while the call_1 it admitted with { value: 1 } is still
// executing, so the retry dispatches call_1 again with { value: 2 }. Each attempt's consumer is held at its first read of call_1's part after
// registering the call, outside any transaction, until that attempt's own execution has entered its body: the first
// attempt then fails, and the retry's consumer resumes only once the retry's execution has been admitted.
const retriedCall = Effect.fn("test.retriedCall")(function* (input: {
  dir: string
  body: (args: { value: number }, ctx: Tool.Context) => Effect.Effect<void>
  dispatch?: (args: { value: number }) => Effect.Effect<void>
}) {
  yield* resetCustodyWrites
  const entered = [Deferred.makeUnsafe<void>(), Deferred.makeUnsafe<void>()]
  const takenOver = Deferred.makeUnsafe<void>()
  const turn = yield* custodyTurn({
    dir: input.dir,
    dbPath: custodyWriteDbPath,
    body: (args, ctx) =>
      Deferred.succeed(entered[args.value - 1], undefined).pipe(Effect.andThen(input.body(args, ctx))),
    dispatch: input.dispatch,
  })
  let attempt = 0
  let consumer: Fiber.Fiber<unknown, unknown> | undefined
  let partID: string | undefined
  let reads = 0
  custodyWrites.before = (part) => {
    if (part.type === "step-start") {
      attempt++
      consumer = Fiber.getCurrent()
      reads = 0
    }
    if (part.type === "tool" && part.callID === "call_1" && part.state.status === "pending") partID = part.id
    return undefined
  }
  custodyWrites.read = (read) => {
    if (read !== partID || Fiber.getCurrent() !== consumer) return undefined
    reads++
    // The first attempt registers call_1 with its pending write; the retry, with the read that takes the call over.
    if (reads !== attempt) return undefined
    if (attempt === 1)
      return awaitWithTimeout(Deferred.await(entered[0]), "the first attempt's execution never entered its body").pipe(
        Effect.andThen(
          Effect.die(Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET", syscall: "read" })),
        ),
        Effect.orDie,
      )
    Deferred.doneUnsafe(takenOver, Exit.void)
    return awaitWithTimeout(
      Deferred.await(entered[1]),
      "the retry's execution never entered its body",
      "5 seconds",
    ).pipe(Effect.orDie)
  }
  // Detached, so a turn that never settles fails this case's bounds instead of holding the test's scope open.
  const run = yield* turn.process.pipe(Effect.forkDetach)
  return { ...turn, run, entered, takenOver }
})

itCustodyWrites.live(
  "admits the retry of an already-admitted call with the retry's own input",
  () =>
    provideTmpdirServer(
      ({ dir, llm }) =>
        Effect.gen(function* () {
          const release = Deferred.makeUnsafe<void>()
          const marker = (value: number) => path.join(dir, `probe-effect-${value}`)
          yield* llm.tool("probe", { value: 1 })
          yield* llm.tool("probe", { value: 2 })
          const turn = yield* retriedCall({
            dir,
            body: (args) =>
              (args.value === 1 ? Deferred.await(release) : Effect.void).pipe(
                Effect.andThen(writeMarker(marker(args.value), `effect:${args.value}`)),
              ),
          })
          yield* awaitWithTimeout(Deferred.await(turn.entered[1]), "the retry never executed call_1", "10 seconds")
          const admitted = (value: number) => ({
            callID: "call_1",
            stored: { callID: "call_1", state: { status: "running", input: { value } } },
          })
          expect(turn.plugin).toMatchObject([admitted(1), admitted(2)])
          expect(turn.body).toMatchObject([admitted(1), admitted(2)])
          expect(turn.body[1].stored?.id).toBe(turn.body[0].stored?.id)
          expect(yield* awaitWithTimeout(Fiber.join(turn.run), "the retried turn never finished", "5 seconds")).toBe(
            "continue",
          )
          yield* Deferred.succeed(release, undefined)
          expect(
            yield* awaitWithTimeout(
              Effect.promise(() => Promise.all(turn.executions)),
              "the superseded execution never finished",
            ),
          ).toEqual(["fulfilled", "fulfilled"])
          expect(existsSync(marker(1))).toBe(true)
          expect(storedToolPart(custodyWriteDbPath, turn.msg.id, "call_1")).toMatchObject({
            state: { status: "completed", input: { value: 2 }, output: "probe:2" },
          })
        }),
      { config: (url) => providerCfg(url) },
    ),
  30_000,
)

itCustodyWrites.live(
  "never lets an execution a retry superseded write metadata into or complete the retried call",
  () =>
    provideTmpdirServer(
      ({ dir, llm }) =>
        Effect.gen(function* () {
          const release = [Deferred.makeUnsafe<void>(), Deferred.makeUnsafe<void>()]
          const metadata: Exit.Exit<void>[] = []
          const marker = (value: number) => path.join(dir, `probe-effect-${value}`)
          yield* llm.tool("probe", { value: 1 })
          yield* llm.tool("probe", { value: 2 })
          const turn = yield* retriedCall({
            dir,
            body: (args, ctx) =>
              Effect.gen(function* () {
                yield* Deferred.await(release[args.value - 1])
                if (args.value === 1)
                  metadata.push(yield* Effect.exit(ctx.metadata({ title: "obsolete", metadata: { value: 1 } })))
                yield* writeMarker(marker(args.value), `effect:${args.value}`)
              }),
          })
          yield* awaitWithTimeout(Deferred.await(turn.entered[1]), "the retry never executed call_1", "10 seconds")
          // The superseded execution finishes while the retry's is still running; its wrapper, whose stream attempt
          // has ended, then tries to record its own outcome.
          yield* Deferred.succeed(release[0], undefined)
          expect(
            yield* awaitWithTimeout(
              Effect.promise(() => turn.executions[0]),
              "the superseded execution never finished",
            ),
          ).toBe("fulfilled")
          expect(metadata).toHaveLength(1)
          expect(Exit.isFailure(metadata[0])).toBe(true)
          const retried = storedToolPart(custodyWriteDbPath, turn.msg.id, "call_1")
          expect(retried?.state).toMatchObject({ status: "running", input: { value: 2 } })
          expect(retried?.state).not.toHaveProperty("title")
          expect(retried?.state).not.toHaveProperty("metadata")

          yield* Deferred.succeed(release[1], undefined)
          expect(yield* awaitWithTimeout(Fiber.join(turn.run), "the retried turn never finished", "5 seconds")).toBe(
            "continue",
          )
          expect(existsSync(marker(1))).toBe(true)
          expect(existsSync(marker(2))).toBe(true)
          expect(storedToolPart(custodyWriteDbPath, turn.msg.id, "call_1")).toMatchObject({
            id: retried?.id,
            state: { status: "completed", input: { value: 2 }, output: "probe:2" },
          })
        }),
      { config: (url) => providerCfg(url) },
    ),
  30_000,
)

itCustodyWrites.live(
  "never lets an execution write a call a retry has taken over, even before the retry admits its own",
  () =>
    provideTmpdirServer(
      ({ dir, llm }) =>
        Effect.gen(function* () {
          const release = Deferred.makeUnsafe<void>()
          const redispatch = Deferred.makeUnsafe<void>()
          const metadata: Exit.Exit<void>[] = []
          const marker = (value: number) => path.join(dir, `probe-effect-${value}`)
          yield* llm.tool("probe", { value: 1 })
          yield* llm.tool("probe", { value: 2 })
          const turn = yield* retriedCall({
            dir,
            body: (args, ctx) =>
              Effect.gen(function* () {
                if (args.value === 1) {
                  yield* Deferred.await(release)
                  metadata.push(yield* Effect.exit(ctx.metadata({ title: "obsolete", metadata: { value: 1 } })))
                }
                yield* writeMarker(marker(args.value), `effect:${args.value}`)
              }),
            // The retry's dispatch of call_1 is held from its admission until the superseded execution has finished.
            dispatch: (args) => (args.value === 2 ? Deferred.await(redispatch) : Effect.void),
          })
          yield* awaitWithTimeout(Deferred.await(turn.takenOver), "the retry never took call_1 over", "10 seconds")
          yield* Deferred.succeed(release, undefined)
          expect(
            yield* awaitWithTimeout(
              Effect.promise(() => turn.executions[0]),
              "the superseded execution never finished",
            ),
          ).toBe("fulfilled")
          expect(metadata).toHaveLength(1)
          expect(Exit.isFailure(metadata[0])).toBe(true)
          // The call keeps the admission record of the execution that last ran it until the retry admits its own.
          const taken = storedToolPart(custodyWriteDbPath, turn.msg.id, "call_1")
          expect(taken?.state).toMatchObject({ status: "running", input: { value: 1 } })
          expect(taken?.state).not.toHaveProperty("title")

          yield* Deferred.succeed(redispatch, undefined)
          expect(yield* awaitWithTimeout(Fiber.join(turn.run), "the retried turn never finished", "5 seconds")).toBe(
            "continue",
          )
          expect(turn.body).toMatchObject([
            { callID: "call_1", stored: { state: { status: "running", input: { value: 1 } } } },
            { callID: "call_1", stored: { state: { status: "running", input: { value: 2 } } } },
          ])
          expect(storedToolPart(custodyWriteDbPath, turn.msg.id, "call_1")).toMatchObject({
            id: taken?.id,
            state: { status: "completed", input: { value: 2 }, output: "probe:2" },
          })
        }),
      { config: (url) => providerCfg(url) },
    ),
  30_000,
)

// Runs a turn whose first stream attempt fails retryably while the call_1 it admitted with { value: 1 } is still
// executing. Once that attempt has ended, its execution completes call_1 itself: the completion commits, but its
// publication is held, so its writer has not yet settled the call. Only then does the retry start; it takes call_1 over
// and rewrites it running with { value: 2 }, and only after that is its dispatch of call_1 admitted. The case releases
// the held publication once the retry's execution has entered its body.
const committedBeforeTakeover = Effect.fn("test.committedBeforeTakeover")(function* (input: {
  dir: string
  body: (ctx: Tool.Context) => Effect.Effect<void>
}) {
  yield* resetCustodyWrites
  const entered = [Deferred.makeUnsafe<void>(), Deferred.makeUnsafe<void>()]
  const rewritten = Deferred.makeUnsafe<void>()
  const publication = yield* holdPartPublication(
    (part) => part.callID === "call_1" && part.state.status === "completed" && part.state.output === "probe:1",
  )
  const turn = yield* custodyTurn({
    dir: input.dir,
    dbPath: custodyWriteDbPath,
    body: (args, ctx) =>
      Deferred.succeed(entered[args.value - 1], undefined).pipe(
        Effect.andThen(
          args.value === 1
            ? awaitWithTimeout(
                Effect.callback<void>((resume) => {
                  if (ctx.abort.aborted) return resume(Effect.void)
                  ctx.abort.addEventListener("abort", () => resume(Effect.void), { once: true })
                }),
                "the first attempt never ended",
              ).pipe(Effect.orDie)
            : input.body(ctx),
        ),
      ),
    dispatch: (args) =>
      args.value === 2
        ? awaitWithTimeout(Deferred.await(rewritten), "the retry never rewrote call_1", "10 seconds").pipe(Effect.orDie)
        : Effect.void,
  })
  let attempt = 0
  let consumer: Fiber.Fiber<unknown, unknown> | undefined
  let partID: string | undefined
  let reads = 0
  custodyWrites.before = (part) => {
    if (part.type === "step-start") {
      attempt++
      consumer = Fiber.getCurrent()
      if (attempt === 2)
        return awaitWithTimeout(
          Deferred.await(publication.committed),
          "the first attempt's execution never committed its outcome",
          "5 seconds",
        ).pipe(Effect.asVoid, Effect.orDie)
    }
    if (part.type !== "tool" || part.callID !== "call_1") return undefined
    if (part.state.status === "pending") partID = part.id
    if (attempt === 2 && part.state.status === "running" && part.state.input.value === 2)
      Deferred.doneUnsafe(rewritten, Exit.void)
    return undefined
  }
  custodyWrites.read = (read) => {
    if (attempt !== 1 || read !== partID || Fiber.getCurrent() !== consumer || ++reads !== 1) return undefined
    return awaitWithTimeout(Deferred.await(entered[0]), "the first attempt's execution never entered its body").pipe(
      Effect.andThen(Effect.die(Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET", syscall: "read" }))),
      Effect.orDie,
    )
  }
  // Detached, so a turn that never settles fails this case's bounds instead of holding the test's scope open.
  const run = yield* turn.process.pipe(Effect.forkDetach)
  yield* awaitWithTimeout(Deferred.await(entered[1]), "the retry never executed call_1", "10 seconds")
  yield* Deferred.succeed(publication.release, undefined)
  expect(
    yield* awaitWithTimeout(
      Effect.promise(() => turn.executions[0]),
      "the first attempt's execution never finished",
    ),
  ).toBe("fulfilled")
  return { ...turn, run }
})

for (const gated of [false, true])
  itCustodyWrites.live(
    `keeps a retry's custody when an earlier execution's outcome settles after the retry took the call over${gated ? ", for an effect gated on metadata" : ""}`,
    () =>
      provideTmpdirServer(
        ({ dir, llm }) =>
          Effect.gen(function* () {
            const proceed = Deferred.makeUnsafe<void>()
            const marker = path.join(dir, "probe-effect-2")
            yield* llm.tool("probe", { value: 1 })
            yield* llm.tool("probe", { value: 2 })
            const turn = yield* committedBeforeTakeover({
              dir,
              body: (ctx) =>
                Deferred.await(proceed).pipe(
                  Effect.andThen(gated ? ctx.metadata({ metadata: { value: 2 } }) : Effect.void),
                  Effect.andThen(writeMarker(marker, "effect:2")),
                ),
            })
            yield* Deferred.succeed(proceed, undefined)
            expect(yield* awaitWithTimeout(Fiber.join(turn.run), "the retried turn never finished", "5 seconds")).toBe(
              "continue",
            )
            expect(
              yield* awaitWithTimeout(
                Effect.promise(() => turn.executions[1]),
                "the retry never finished",
              ),
            ).toBe("fulfilled")
            expect(existsSync(marker)).toBe(true)
            expect(storedToolPart(custodyWriteDbPath, turn.msg.id, "call_1")).toMatchObject({
              state: { status: "completed", input: { value: 2 }, output: "probe:2" },
            })
          }),
        { config: (url) => providerCfg(url) },
      ),
    30_000,
  )

itCustodyWrites.live(
  "settles a cancelled retry whose call an earlier execution's outcome settled after the takeover",
  () =>
    provideTmpdirServer(
      ({ dir, llm }) =>
        Effect.gen(function* () {
          yield* llm.tool("probe", { value: 1 })
          yield* llm.tool("probe", { value: 2 })
          const turn = yield* committedBeforeTakeover({ dir, body: () => Effect.never })
          yield* awaitWithTimeout(Fiber.interrupt(turn.run), "the cancelled retry never finished settling")
          expect(storedToolPart(custodyWriteDbPath, turn.msg.id, "call_1")).toMatchObject({
            state: { status: "error", input: { value: 2 }, error: "Tool execution aborted" },
          })
          const message = storedAssistant(custodyWriteDbPath, turn.msg.id)
          expect(message?.time.completed).toBeDefined()
          expect(message?.error?.name).toBe("MessageAbortedError")
        }),
      { config: (url) => providerCfg(url) },
    ),
  30_000,
)

// The native runtime consumes a tool call, which writes it running, before it dispatches the call; the dispatched
// wrapper's admission is therefore the call's second running write.
const admissionFault = { callID: "", running: 0 }
const admissionFaultSession = LayerNode.make({
  service: Session.Service,
  layer: Layer.effect(
    Session.Service,
    Effect.gen(function* () {
      const real = yield* Session.Service
      return Session.Service.of({
        ...real,
        updatePart: <T extends SessionV1.Part>(part: T) => {
          if (part.type !== "tool" || part.callID !== admissionFault.callID || part.state.status !== "running")
            return real.updatePart(part)
          admissionFault.running++
          if (admissionFault.running === 2) return Effect.die(new Error("admission write refused"))
          return real.updatePart(part)
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
const nativeCall = { id: "", value: 0 }
const nativeCustodyLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: (input) => {
      const native = LLMNativeRuntime.stream({
        model: {
          ...input.model,
          providerID: ProviderV2.ID.make("openai"),
          api: { ...input.model.api, npm: "@ai-sdk/openai" },
        },
        provider: {
          id: ProviderV2.ID.make("openai"),
          name: "OpenAI",
          source: "config",
          env: [],
          options: { apiKey: "test-key" },
          models: {},
        },
        auth: undefined,
        llmClient: {
          prepare: () => Effect.die("unused"),
          stream: () =>
            Stream.fromIterable([
              LLMEvent.stepStart({ index: 0 }),
              LLMEvent.toolCall({ id: nativeCall.id, name: "probe", input: { value: nativeCall.value } }),
              LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
              LLMEvent.finish({ reason: "tool-calls" }),
            ]),
          generate: () => Effect.die("unused"),
        } as unknown as LLMClientShape,
        messages: [],
        tools: input.tools,
        headers: {},
        abort: new AbortController().signal,
      })
      if (native.type === "unsupported") return Stream.die(new Error(native.reason))
      return native.stream
    },
  }),
)
const itNativeCustody = testEffect(
  LayerNode.compile(root, [
    ...replacements,
    [LLM.node, nativeCustodyLLM],
    [Session.node, admissionFaultSession],
    [Database.node, Database.layerFromPath(nativeCustodyDbPath)],
  ]),
)

itNativeCustody.live("runs nothing when the native runtime dispatches a call whose admission write fails", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const run = Effect.fn("test.nativeProbe")(function* (callID: string, value: number) {
          nativeCall.id = callID
          nativeCall.value = value
          const turn = yield* custodyTurn({
            dir,
            dbPath: nativeCustodyDbPath,
            body: (args) => writeMarker(path.join(dir, `probe-effect-${callID}`), `effect:${args.value}`),
          })
          yield* turn.process
          return { ...turn, execution: yield* Effect.promise(() => turn.executions[0]!) }
        })

        admissionFault.callID = "call-refused"
        admissionFault.running = 0
        const refused = yield* run("call-refused", 7)
        expect(admissionFault.running).toBe(2)
        expect(refused.execution).toBe("rejected")
        expect(refused.plugin).toEqual([])
        expect(refused.body).toEqual([])
        expect(existsSync(path.join(dir, "probe-effect-call-refused"))).toBe(false)
        const stored = storedToolPart(nativeCustodyDbPath, refused.msg.id, "call-refused")
        expect(stored?.state.status).toBe("error")
        if (stored?.state.status === "error") expect(stored.state.error).toContain("admission write refused")

        // The same tool runs once its admission commits.
        const admitted = yield* run("call-admitted", 8)
        expect(admitted.execution).toBe("fulfilled")
        expect(admitted.plugin).toMatchObject([
          { callID: "call-admitted", stored: { state: { status: "running", input: { value: 8 } } } },
        ])
        expect(existsSync(path.join(dir, "probe-effect-call-admitted"))).toBe(true)
        expect(storedToolPart(nativeCustodyDbPath, admitted.msg.id, "call-admitted")).toMatchObject({
          state: { status: "completed", output: "probe:8" },
        })
      }),
    { config: cfg },
  ),
)
