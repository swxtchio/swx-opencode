import { describe, expect } from "bun:test"
import {
  LLMClient,
  LLMError,
  LLMEvent,
  Model,
  TransportReason,
  InvalidRequestReason,
  type LLMClientShape,
  type LLMRequest,
} from "@opencode-ai/llm"
import * as OpenAIChat from "@opencode-ai/llm/protocols/openai-chat"
import { Database } from "@opencode-ai/core/database/database"
import { makeLocationNode } from "@opencode-ai/core/effect/app-node"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNodePlatform } from "@opencode-ai/core/effect/app-node-platform"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { PermissionV2 } from "@opencode-ai/core/permission"
import { EventTable } from "@opencode-ai/core/event/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Project, ProjectV2 } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { QuestionV2 } from "@opencode-ai/core/question"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { Snapshot } from "@opencode-ai/core/snapshot"
import {
  ContextSnapshotDecodeError,
  ProviderTurnInterruptedMessage,
  ProviderTurnInterruptedOrigin,
} from "@opencode-ai/core/session/error"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionInput } from "@opencode-ai/core/session/input"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { Prompt } from "@opencode-ai/core/session/prompt"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionRunCoordinator } from "@opencode-ai/core/session/run-coordinator"
import { SessionRunner } from "@opencode-ai/core/session/runner"
import * as SessionRunnerLLM from "@opencode-ai/core/session/runner/llm"
import { SessionRunnerModel } from "@opencode-ai/core/session/runner/model"
import { ToolRegistry } from "@opencode-ai/core/tool/registry"
import { ApplicationTools } from "@opencode-ai/core/tool/application-tools"
import { AgentV2 } from "@opencode-ai/core/agent"
import { Config } from "@opencode-ai/core/config"
import { ConfigCompaction } from "@opencode-ai/core/config/compaction"
import { Tool } from "@opencode-ai/core/tool/tool"
import {
  SessionContextEpochTable,
  SessionInputTable,
  SessionMessageTable,
  SessionTable,
} from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SystemContext } from "@opencode-ai/core/system-context"
import { SystemContextRegistry } from "@opencode-ai/core/system-context/registry"
import { SkillGuidance } from "@opencode-ai/core/skill/guidance"
import { ReferenceGuidance } from "@opencode-ai/core/reference/guidance"
import { ModelV2 } from "@opencode-ai/core/model"
import { Location } from "@opencode-ai/core/location"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Cause, DateTime, Deferred, Effect, Exit, Fiber, Layer, Queue, Schema, Stream } from "effect"
import { asc, eq } from "drizzle-orm"
import { testEffect } from "./lib/effect"

const requests: LLMRequest[] = []
let response: LLMEvent[] = []
let responses: LLMEvent[][] | undefined
let responseStream: Stream.Stream<LLMEvent, LLMError> | undefined
let streamGate: Deferred.Deferred<void> | undefined
let streamStarted: Deferred.Deferred<void> | undefined
const streamStartAcks: Deferred.Deferred<void>[] = []
let streamFailure: LLMError | undefined
let staleActiveSnapshot: (() => Effect.Effect<ReadonlySet<SessionV2.ID>>) | undefined
let activeReadAck: Deferred.Deferred<void> | undefined
let joinAck: Deferred.Deferred<void> | undefined
let waitForInactiveAfterJoin = false
let joinedFailureCount = 0
let joinedSuccessCount = 0
let toolExecutionGate: Deferred.Deferred<void> | undefined
let toolExecutionsStarted: Deferred.Deferred<void> | undefined
let toolExecutionsReady = 5
let activeToolExecutions = 0
let maxActiveToolExecutions = 0
const client = Layer.succeed(
  LLMClient.Service,
  LLMClient.Service.of({
    prepare: () => Effect.die("unused"),
    stream: ((request: LLMRequest) => {
      requests.push(request)
      const requestStarted = streamStartAcks.shift()
      const signalRequestStart = requestStarted ? Deferred.succeed(requestStarted, undefined) : Effect.void
      if (responseStream) {
        const stream = responseStream
        responseStream = undefined
        return requestStarted ? Stream.unwrap(signalRequestStart.pipe(Effect.as(stream))) : stream
      }
      const events = streamFailure
        ? Stream.fail(streamFailure)
        : Stream.fromIterable(responses === undefined ? response : (responses.shift() ?? []))
      if (!streamGate) return requestStarted ? Stream.unwrap(signalRequestStart.pipe(Effect.as(events))) : events
      return Stream.unwrap(
        (streamStarted ? Deferred.succeed(streamStarted, undefined) : Effect.void).pipe(
          Effect.andThen(signalRequestStart),
          Effect.andThen(Deferred.await(streamGate)),
          Effect.as(events),
        ),
      )
    }) as unknown as LLMClientShape["stream"],
    generate: () => Effect.die("unused"),
  }),
)
const projects = Layer.succeed(
  ProjectV2.Service,
  ProjectV2.Service.of({
    resolve: (directory) => Effect.succeed({ id: ProjectV2.ID.global, directory }),
    directories: () => Effect.succeed([]),
    commit: () => Effect.void,
  }),
)
const model = Model.make({ id: "fake-model", provider: "fake", route: OpenAIChat.route })
const replacementModel = Model.make({ id: "replacement", provider: "fake", route: OpenAIChat.route })
const compactModel = Model.make({
  id: "compact",
  provider: "fake",
  route: OpenAIChat.route.with({ limits: { context: 4_000, output: 50 } }),
})
const recoveryModel = Model.make({
  id: "recovery",
  provider: "fake",
  route: OpenAIChat.route.with({ limits: { context: 20_000, output: 1_000 } }),
})
const authorizations: Tool.Context[] = []
const executions: string[] = []
const permission = Layer.succeed(
  PermissionV2.Service,
  PermissionV2.Service.of({
    assert: () => Effect.die("unused"),
    ask: () => Effect.die("unused"),
    reply: () => Effect.die("unused"),
    get: () => Effect.die("unused"),
    forSession: () => Effect.die("unused"),
    list: () => Effect.die("unused"),
  }),
)
const echo = Layer.effectDiscard(
  ToolRegistry.Service.use((registry) =>
    registry.register({
      echo: Tool.make({
        description: "Echo text",
        input: Schema.Struct({ text: Schema.String }),
        output: Schema.Struct({ text: Schema.String }),
        toModelOutput: ({ output }) => [{ type: "text", text: output.text }],
        execute: ({ text }, context) =>
          Effect.gen(function* () {
            authorizations.push(context)
            executions.push(text)
            activeToolExecutions++
            maxActiveToolExecutions = Math.max(maxActiveToolExecutions, activeToolExecutions)
            if (activeToolExecutions === toolExecutionsReady && toolExecutionsStarted) {
              yield* Deferred.succeed(toolExecutionsStarted, undefined)
            }
            if (toolExecutionGate) yield* Deferred.await(toolExecutionGate)
            return { text }
          }).pipe(Effect.ensuring(Effect.sync(() => activeToolExecutions--))),
      }),
      defect: Tool.make({
        description: "Fail unexpectedly",
        input: Schema.Struct({}),
        output: Schema.Struct({}),
        execute: () => Effect.die("unexpected tool defect"),
      }),
    }),
  ),
)
const echoNode = makeLocationNode({ name: "test/session-runner-tools", layer: echo, deps: [ToolRegistry.node] })
let modelResolveHook = Effect.void
let currentModel = model
const models = SessionRunnerModel.layerWith((session) =>
  modelResolveHook.pipe(Effect.as(session.model?.id === "replacement" ? replacementModel : currentModel)),
)
const systemContextKey = SystemContext.Key.make("test/context")
let systemBaseline = "Initial context"
let systemRemoved = false
let systemUnavailable = false
let systemLoadHook = Effect.void
const skillBaselines = new Map<AgentV2.ID, string>()
const systemContext = Layer.effectDiscard(
  SystemContextRegistry.Service.pipe(
    Effect.flatMap((registry) =>
      registry.register({
        key: systemContextKey,
        load: Effect.sync(() =>
          SystemContext.combine(
            systemRemoved
              ? []
              : [
                  SystemContext.make({
                    key: systemContextKey,
                    codec: Schema.toCodecJson(Schema.String),
                    load: systemLoadHook.pipe(
                      Effect.andThen(
                        Effect.sync(() => (systemUnavailable ? SystemContext.unavailable : systemBaseline)),
                      ),
                    ),
                    baseline: String,
                    update: (_previous, current) => current,
                    removed: () => "System context source removed: test/context",
                  }),
                ],
          ),
        ),
      }),
    ),
  ),
).pipe(Layer.provideMerge(AppNodeBuilder.build(SystemContextRegistry.node)))
const skillGuidance = Layer.mock(SkillGuidance.Service, {
  load: (agent) =>
    Effect.succeed(
      skillBaselines.has(agent.id)
        ? SystemContext.make({
            key: SystemContext.Key.make("test/skill-guidance"),
            codec: Schema.toCodecJson(Schema.String),
            load: Effect.succeed(skillBaselines.get(agent.id)!),
            baseline: String,
            update: (_previous, current) => current,
            removed: () => "Skill guidance removed",
          })
        : SystemContext.empty,
    ),
})
const referenceGuidance = Layer.mock(ReferenceGuidance.Service, { load: () => Effect.succeed(SystemContext.empty) })
const config = Layer.succeed(
  Config.Service,
  Config.Service.of({
    entries: () =>
      Effect.succeed([
        new Config.Document({
          type: "document",
          info: new Config.Info({
            compaction: new ConfigCompaction.Info({
              buffer: 3_000,
              keep: new ConfigCompaction.Keep({ tokens: 1_000 }),
            }),
          }),
        }),
      ]),
  }),
)
const runnerLayer = AppNodeBuilder.build(SessionRunnerLLM.node, [
  [Snapshot.node, Snapshot.noopLayer],
  [LayerNodePlatform.llmClient, client],
  [SessionRunnerModel.node, models],
  [SystemContextRegistry.node, systemContext],
  [Location.node, Location.boundNode({ directory: AbsolutePath.make("/project") })],
  [SkillGuidance.node, skillGuidance],
  [ReferenceGuidance.node, referenceGuidance],
  [PermissionV2.node, permission],
  [Config.node, config],
])
const execution = Layer.effect(
  SessionExecution.Service,
  Effect.gen(function* () {
    const sessionRunner = yield* SessionRunner.Service
    const coordinator = yield* SessionRunCoordinator.make<SessionV2.ID, SessionRunner.RunError>({
      drain: (sessionID, force) => sessionRunner.run({ sessionID, force }),
    })
    const active = Effect.gen(function* () {
      const override = staleActiveSnapshot
      if (override) {
        staleActiveSnapshot = undefined
        return yield* override()
      }
      const snapshot = yield* coordinator.active
      const ack = activeReadAck
      if (ack) {
        activeReadAck = undefined
        yield* Deferred.succeed(ack, undefined)
      }
      return snapshot
    })
    return SessionExecution.Service.of({
      active,
      join: (sessionID) =>
        Effect.suspend(() => {
          const joined = coordinator.join(sessionID).pipe(
            Effect.tap((exit) =>
              Effect.sync(() => {
                if (exit === undefined) return
                if (Exit.isFailure(exit)) joinedFailureCount++
                else joinedSuccessCount++
              }),
            ),
          )
          const ack = joinAck
          if (!ack) return joined
          joinAck = undefined
          const acknowledged = Deferred.succeed(ack, undefined).pipe(Effect.andThen(joined))
          if (!waitForInactiveAfterJoin) return acknowledged
          return acknowledged.pipe(
            Effect.flatMap((exit) => {
              if (exit === undefined) return Effect.succeed(undefined)
              return Effect.gen(function* () {
                // This test stabilizes the post-join snapshot against coordinator cleanup ordering.
                for (let attempt = 0; attempt < 1_000; attempt++) {
                  if (!(yield* coordinator.active).has(sessionID)) return exit
                  yield* Effect.yieldNow
                }
                return yield* Effect.die("coordinator stayed active after the joined drain settled")
              })
            }),
          )
        }),
      resume: coordinator.run,
      wake: coordinator.wake,
      interrupt: coordinator.interrupt,
    })
  }),
).pipe(Layer.provide(runnerLayer))
const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      EventV2.node,
      QuestionV2.node,
      SessionProjector.node,
      SessionStore.node,
      ApplicationTools.node,
      AgentV2.node,
      ToolRegistry.node,
      ToolRegistry.toolsNode,
      echoNode,
      SessionRunnerModel.node,
      SystemContextRegistry.node,
      SkillGuidance.node,
      ReferenceGuidance.node,
      Config.node,
      Snapshot.node,
      SessionRunnerLLM.node,
      SessionExecution.node,
      SessionV2.node,
    ]),
    [
      [LayerNodePlatform.llmClient, client],
      [PermissionV2.node, permission],
      [SessionRunnerModel.node, models],
      [SystemContextRegistry.node, systemContext],
      [Location.node, Location.boundNode({ directory: AbsolutePath.make("/project") })],
      [SkillGuidance.node, skillGuidance],
      [ReferenceGuidance.node, referenceGuidance],
      [Snapshot.node, Snapshot.noopLayer],
      [SessionExecution.node, execution],
      [Config.node, config],
      [ProjectV2.node, projects],
    ],
  ),
)
const sessionID = SessionV2.ID.make("ses_runner_test")
const otherSessionID = SessionV2.ID.make("ses_runner_other")

const insertSession = (id: SessionV2.ID) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .insert(SessionTable)
      .values({
        id,
        project_id: Project.ID.global,
        slug: id,
        directory: "/project",
        title: "test",
        version: "test",
      })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
  })

const awaitStreamStart = (started: Deferred.Deferred<void>) =>
  Effect.gen(function* () {
    let done = false
    for (let attempt = 0; attempt < 1_000 && !done; attempt++) {
      done = yield* Deferred.isDone(started)
      if (!done) yield* Effect.yieldNow
    }
    expect(done).toBe(true)
  })

const awaitSignal = (signal: Deferred.Deferred<void>, message: string) =>
  Effect.gen(function* () {
    let done = false
    for (let attempt = 0; attempt < 1_000 && !done; attempt++) {
      done = yield* Deferred.isDone(signal)
      if (!done) yield* Effect.yieldNow
    }
    if (!done) throw new Error(message)
  })

const setup = Effect.gen(function* () {
  const { db } = yield* Database.Service
  requests.length = 0
  response = []
  systemBaseline = "Initial context"
  systemRemoved = false
  systemUnavailable = false
  systemLoadHook = Effect.void
  modelResolveHook = Effect.void
  currentModel = model
  skillBaselines.clear()
  responses = undefined
  streamFailure = undefined
  responseStream = undefined
  streamGate = undefined
  streamStarted = undefined
  streamStartAcks.length = 0
  staleActiveSnapshot = undefined
  activeReadAck = undefined
  joinAck = undefined
  waitForInactiveAfterJoin = false
  joinedFailureCount = 0
  joinedSuccessCount = 0
  toolExecutionGate = undefined
  toolExecutionsStarted = undefined
  toolExecutionsReady = 5
  activeToolExecutions = 0
  maxActiveToolExecutions = 0
  yield* db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
    .onConflictDoNothing()
    .run()
    .pipe(Effect.orDie)
  yield* insertSession(sessionID)
})

const setupNative = Effect.gen(function* () {
  yield* setup
  const { db } = yield* Database.Service
  yield* db.delete(SessionTable).where(eq(SessionTable.id, sessionID)).run().pipe(Effect.orDie)
  const session = yield* SessionV2.Service
  return yield* session.create({
    id: sessionID,
    location: Location.Ref.make({ directory: AbsolutePath.make("/project") }),
  })
})

const providerUnavailable = () =>
  new LLMError({
    module: "test",
    method: "stream",
    reason: new TransportReason({ message: "Provider unavailable" }),
  })

const setupOverflowRecovery = Effect.gen(function* () {
  yield* setup
  const session = yield* SessionV2.Service
  response = fragmentFixture("text", "text-earlier", ["Earlier answer"]).completeEvents
  yield* session.prompt({
    sessionID,
    prompt: Prompt.make({ text: "Earlier question ".repeat(700) }),
    resume: false,
  })
  yield* session.resume(sessionID)
  currentModel = recoveryModel
  requests.length = 0
  return session
})

const messageTexts = (request: LLMRequest, role: "user" | "system") =>
  request.messages.flatMap((message) =>
    message.role === role ? message.content.flatMap((content) => (content.type === "text" ? [content.text] : [])) : [],
  )
const userTexts = (request: LLMRequest) => messageTexts(request, "user")
const systemTexts = (request: LLMRequest) => messageTexts(request, "system")

const replaySessionProjection = (id: SessionV2.ID) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    const recorded = yield* db
      .select()
      .from(EventTable)
      .where(eq(EventTable.aggregate_id, id))
      .orderBy(asc(EventTable.seq))
      .all()
      .pipe(Effect.orDie)

    yield* events.remove(id)
    yield* db.delete(SessionInputTable).where(eq(SessionInputTable.session_id, id)).run().pipe(Effect.orDie)
    yield* db.delete(SessionMessageTable).where(eq(SessionMessageTable.session_id, id)).run().pipe(Effect.orDie)
    yield* events.replayAll(
      recorded.map((event) => ({
        id: event.id,
        aggregateID: event.aggregate_id,
        seq: event.seq,
        type: event.type,
        data: event.data,
      })),
    )
  })

type FragmentKind = "text" | "reasoning" | "tool input"

type FragmentFixture = {
  readonly delta: EventV2.Definition
  readonly completeEvents: LLMEvent[]
  readonly partialEvents: LLMEvent[]
  readonly expectedAssistant: unknown
  readonly expectedContent: unknown
}

const fragmentKinds: readonly FragmentKind[] = ["text", "reasoning", "tool input"]

const fragmentID = (kind: FragmentKind, suffix: string) => `${kind === "tool input" ? "call" : kind}-${suffix}`

const fragmentFixture = (kind: FragmentKind, id: string, chunks: readonly string[]): FragmentFixture => {
  const text = chunks.join("")
  switch (kind) {
    case "text": {
      const partialEvents = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.textStart({ id }),
        ...chunks.map((text) => LLMEvent.textDelta({ id, text })),
      ]
      const expectedContent = { type: "text", id, text }
      return {
        delta: SessionEvent.Text.Delta,
        partialEvents,
        completeEvents: [
          ...partialEvents,
          LLMEvent.textEnd({ id }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
        expectedAssistant: { type: "assistant", finish: "stop", content: [expectedContent] },
        expectedContent,
      }
    }
    case "reasoning": {
      const partialEvents = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.reasoningStart({ id }),
        ...chunks.map((text) => LLMEvent.reasoningDelta({ id, text })),
      ]
      const expectedContent = { type: "reasoning", id, text }
      return {
        delta: SessionEvent.Reasoning.Delta,
        partialEvents,
        completeEvents: [
          ...partialEvents,
          LLMEvent.reasoningEnd({ id }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
        expectedAssistant: { type: "assistant", finish: "stop", content: [expectedContent] },
        expectedContent,
      }
    }
    case "tool input": {
      const partialEvents = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id, name: "echo" }),
        ...chunks.map((text) => LLMEvent.toolInputDelta({ id, name: "echo", text })),
      ]
      const expectedContent = { type: "tool", id, state: { status: "pending", input: text } }
      return {
        delta: SessionEvent.Tool.Input.Delta,
        partialEvents,
        completeEvents: [...partialEvents, LLMEvent.toolInputEnd({ id, name: "echo" })],
        expectedAssistant: { type: "assistant", content: [expectedContent] },
        expectedContent,
      }
    }
  }
}

const echoTurn = (id: string, text: string) => [
  LLMEvent.stepStart({ index: 0 }),
  LLMEvent.toolCall({ id, name: "echo", input: { text } }),
  LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
  LLMEvent.finish({ reason: "tool-calls" }),
]

const textTurn = (id: string, text: string) => fragmentFixture("text", id, [text]).completeEvents

const publishHostedToolCall = (input: {
  events: EventV2.Interface
  sessionID: SessionV2.ID
  assistantMessageID: SessionMessage.ID
  callID: string
}) =>
  Effect.gen(function* () {
    const timestamp = yield* DateTime.now
    yield* input.events.publish(SessionEvent.Step.Started, {
      sessionID: input.sessionID,
      assistantMessageID: input.assistantMessageID,
      timestamp,
      agent: "build",
      model: { id: ModelV2.ID.make("fake-model"), providerID: ProviderV2.ID.make("fake") },
    })
    yield* input.events.publish(SessionEvent.Tool.Input.Started, {
      sessionID: input.sessionID,
      timestamp,
      assistantMessageID: input.assistantMessageID,
      callID: input.callID,
      name: "web_search",
    })
    yield* input.events.publish(SessionEvent.Tool.Input.Ended, {
      sessionID: input.sessionID,
      timestamp,
      assistantMessageID: input.assistantMessageID,
      callID: input.callID,
      text: '{"query":"Effect"}',
    })
    yield* input.events.publish(SessionEvent.Tool.Called, {
      sessionID: input.sessionID,
      timestamp,
      assistantMessageID: input.assistantMessageID,
      callID: input.callID,
      tool: "web_search",
      input: { query: "Effect" },
      provider: { executed: true },
    })
  })

describe("SessionV2.wait", () => {
  it.effect("rechecks execution after observing an initially inactive snapshot", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const execution = yield* SessionExecution.Service
      const providerGate = yield* Deferred.make<void>()
      const providerStarted = yield* Deferred.make<void>()
      const activeRechecked = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          staleActiveSnapshot = undefined
          activeReadAck = undefined
          streamGate = undefined
          streamStarted = undefined
        }).pipe(Effect.andThen(Deferred.succeed(providerGate, undefined)), Effect.asVoid),
      )
      let resumeFiber: Fiber.Fiber<void, SessionRunner.RunError> | undefined
      streamGate = providerGate
      streamStarted = providerStarted
      response = textTurn("text-stale-inactive-snapshot", "Done")
      const admitted = yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Start after the stale active snapshot" }),
        resume: false,
      })
      activeReadAck = activeRechecked
      staleActiveSnapshot = () =>
        Effect.gen(function* () {
          resumeFiber = yield* execution.resume(sessionID).pipe(Effect.forkChild)
          yield* awaitStreamStart(providerStarted)
          return new Set<SessionV2.ID>()
        })
      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* awaitSignal(activeRechecked, "wait did not refresh execution state after reading projection")
      expect(yield* Deferred.isDone(outcome)).toBe(false)

      yield* Deferred.succeed(providerGate, undefined)
      const result = yield* Deferred.await(outcome)
      expect(result).toMatchObject({
        _tag: "Success",
        value: { type: "completed", admittedSeq: admitted.admittedSeq },
      })
      expect(requests).toHaveLength(1)
      yield* Fiber.join(waiter)
      if (resumeFiber) yield* Fiber.join(resumeFiber)
    }),
  )

  it.effect("retains a failed tool outcome when a steer is promoted before the continuation", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const registry = yield* ToolRegistry.Service
      const toolGate = yield* Deferred.make<void>()
      const toolStarted = yield* Deferred.make<void>()
      const joined = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          joinAck = undefined
        }).pipe(Effect.andThen(Deferred.succeed(toolGate, undefined)), Effect.asVoid),
      )
      yield* registry.register({
        failAfterGate: Tool.make({
          description: "Fail after the test admits a steer",
          input: Schema.Struct({}),
          output: Schema.Struct({}),
          execute: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(toolStarted, undefined)
              yield* Deferred.await(toolGate)
              return yield* Effect.die("terminal tool failure")
            }),
        }),
      })
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-wait-terminal-error", name: "failAfterGate", input: {} }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        textTurn("text-wait-after-terminal-error", "The model answered after the tool error"),
      ]
      const first = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Run the failing tool" }) })
      yield* Deferred.await(toolStarted)
      joinAck = joined
      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* awaitSignal(joined, "wait did not join the active tool turn")
      const steered = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Continue after the tool" }) })
      yield* Deferred.succeed(toolGate, undefined)

      const result = yield* Deferred.await(outcome)
      expect(result).toMatchObject({
        _tag: "Success",
        value: { type: "failed", admittedSeq: steered.admittedSeq },
      })
      expect(steered.admittedSeq).toBeGreaterThan(first.admittedSeq)
      const messages = yield* session.messages({ sessionID, order: "asc" })
      expect(
        messages.some(
          (message) =>
            message.type === "assistant" &&
            message.content.some((content) => content.type === "tool" && content.state.status === "error"),
        ),
      ).toBe(true)
      expect(
        messages.some(
          (message) =>
            message.type === "assistant" &&
            message.content.some(
              (content) => content.type === "text" && content.text === "The model answered after the tool error",
            ),
        ),
      ).toBe(true)
      expect(requests).toHaveLength(2)
      yield* Fiber.join(waiter)
    }),
  )

  it.effect("fails when a later assistant reuses a provider-local tool ID", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const registry = yield* ToolRegistry.Service
      const toolGate = yield* Deferred.make<void>()
      const toolStarted = yield* Deferred.make<void>()
      const joined = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      let attempts = 0
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          joinAck = undefined
        }).pipe(Effect.andThen(Deferred.succeed(toolGate, undefined)), Effect.asVoid),
      )
      yield* registry.register({
        retryOnce: Tool.make({
          description: "Fail once, then recover",
          input: Schema.Struct({}),
          output: Schema.Struct({}),
          execute: () =>
            Effect.gen(function* () {
              attempts++
              if (attempts > 1) return {}
              yield* Deferred.succeed(toolStarted, undefined)
              yield* Deferred.await(toolGate)
              return yield* Effect.die("transient tool failure")
            }),
        }),
      })
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-wait-retry", name: "retryOnce", input: {} }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-wait-retry", name: "retryOnce", input: {} }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        textTurn("text-wait-after-retry", "Recovered"),
      ]
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Retry the tool call" }) })
      yield* Deferred.await(toolStarted)
      joinAck = joined
      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* awaitSignal(joined, "wait did not join the retrying tool turn")
      const steered = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Keep going" }) })
      yield* Deferred.succeed(toolGate, undefined)

      const result = yield* Deferred.await(outcome)
      expect(result).toMatchObject({
        _tag: "Success",
        value: { type: "failed", admittedSeq: steered.admittedSeq },
      })
      expect(attempts).toBe(2)
      const tools = (yield* session.messages({ sessionID, order: "asc" })).flatMap((message) =>
        message.type === "assistant"
          ? message.content.filter((content): content is SessionMessage.AssistantTool => content.type === "tool")
          : [],
      )
      expect(tools.filter((tool) => tool.id === "call-wait-retry").map((tool) => tool.state.status)).toEqual([
        "error",
        "completed",
      ])
      expect(requests).toHaveLength(3)
      yield* Fiber.join(waiter)
    }),
  )

  it.effect("completes a tool call retried under the same assistant identity", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      const admission = yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Retry the same hosted tool call" }),
        resume: false,
      })
      yield* SessionInput.promoteSteers(db, events, sessionID, Number.MAX_SAFE_INTEGER)
      const assistantMessageID = SessionMessage.ID.create()
      const callID = "call-same-assistant-retry"
      yield* publishHostedToolCall({ events, sessionID, assistantMessageID, callID })
      const failureTime = yield* DateTime.now
      yield* events.publish(SessionEvent.Tool.Failed, {
        sessionID,
        timestamp: failureTime,
        assistantMessageID,
        callID,
        error: { type: "unknown", message: "Transient hosted-tool failure" },
        provider: { executed: true },
      })
      const retryTime = yield* DateTime.now
      yield* events.publish(SessionEvent.Tool.Called, {
        sessionID,
        timestamp: retryTime,
        assistantMessageID,
        callID,
        tool: "web_search",
        input: { query: "Effect" },
        provider: { executed: true },
      })
      yield* events.publish(SessionEvent.Tool.Success, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID,
        callID,
        structured: {},
        content: [],
        provider: { executed: true },
      })
      yield* events.publish(SessionEvent.Step.Ended, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID,
        finish: "stop",
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      })

      expect(yield* session.wait(sessionID)).toMatchObject({
        type: "completed",
        admittedSeq: admission.admittedSeq,
        assistantMessageID,
      })
      expect(yield* session.message({ sessionID, messageID: assistantMessageID })).toMatchObject({
        type: "assistant",
        finish: "stop",
        content: [{ type: "tool", id: callID, state: { status: "completed" } }],
      })
    }),
  )

  it.effect("does not let a reused tool ID hide a running call in another assistant", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Keep the earlier hosted call unsettled" }),
        resume: false,
      })
      yield* SessionInput.promoteSteers(db, events, sessionID, Number.MAX_SAFE_INTEGER)
      const callID = "call-reused-running"
      const firstAssistantMessageID = SessionMessage.ID.create()
      yield* publishHostedToolCall({ events, sessionID, assistantMessageID: firstAssistantMessageID, callID })
      yield* events.publish(SessionEvent.Step.Ended, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID: firstAssistantMessageID,
        finish: "tool-calls",
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      })
      const secondAssistantMessageID = SessionMessage.ID.create()
      yield* publishHostedToolCall({ events, sessionID, assistantMessageID: secondAssistantMessageID, callID })
      yield* events.publish(SessionEvent.Tool.Success, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID: secondAssistantMessageID,
        callID,
        structured: {},
        content: [],
        provider: { executed: true },
      })
      yield* events.publish(SessionEvent.Step.Ended, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID: secondAssistantMessageID,
        finish: "stop",
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      })

      const messages = yield* session.messages({ sessionID, order: "asc" })
      const tools = messages.flatMap((message) =>
        message.type === "assistant"
          ? message.content.filter((content): content is SessionMessage.AssistantTool => content.type === "tool")
          : [],
      )
      expect(tools.filter((tool) => tool.id === callID).map((tool) => tool.state.status)).toEqual([
        "running",
        "completed",
      ])
      expect(yield* session.wait(sessionID).pipe(Effect.flip)).toMatchObject({
        _tag: "Session.OperationUnavailableError",
      })
    }),
  )

  it.effect("reports a provider-error assistant when a steer is promoted afterward", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const providerGate = yield* Deferred.make<void>()
      const providerStarted = yield* Deferred.make<void>()
      const joined = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          joinAck = undefined
          responseStream = undefined
        }).pipe(Effect.andThen(Deferred.succeed(providerGate, undefined)), Effect.asVoid),
      )
      streamStartAcks.push(providerStarted)
      responseStream = Stream.concat(
        Stream.make(LLMEvent.stepStart({ index: 0 })),
        Stream.fromEffect(Deferred.await(providerGate)).pipe(
          Stream.flatMap(() => Stream.make(LLMEvent.providerError({ message: ProviderTurnInterruptedMessage }))),
        ),
      )
      responses = [textTurn("text-wait-after-provider-error", "Recovered text does not erase the failure")]
      const first = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start provider turn" }) })
      yield* awaitStreamStart(providerStarted)
      joinAck = joined
      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* awaitSignal(joined, "wait did not join the provider turn")
      const steered = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Steer after the failure" }) })
      yield* Deferred.succeed(providerGate, undefined)

      const result = yield* Deferred.await(outcome)
      expect(result).toMatchObject({
        _tag: "Success",
        value: { type: "failed", admittedSeq: steered.admittedSeq },
      })
      expect(steered.admittedSeq).toBeGreaterThan(first.admittedSeq)
      expect(requests).toHaveLength(2)
      const providerAssistant = (yield* session.messages({ sessionID })).find(
        (message): message is SessionMessage.Assistant => message.type === "assistant",
      )
      expect(providerAssistant?.error?.origin).toBeUndefined()
      yield* Fiber.join(waiter)
    }),
  )

  it.effect("keeps a provider error in the active work group when the steer arrives afterward", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const streamTail = yield* Deferred.make<void>()
      const providerErrorSeen = yield* Deferred.make<void>()
      const providerStarted = yield* Deferred.make<void>()
      const joined = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          joinAck = undefined
          waitForInactiveAfterJoin = false
          responseStream = undefined
        }).pipe(Effect.andThen(Deferred.succeed(streamTail, undefined)), Effect.asVoid),
      )
      streamStartAcks.push(providerStarted)
      responseStream = Stream.concat(
        Stream.make(
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.providerError({ message: "Provider failed before the steer" }),
        ),
        Stream.fromEffect(
          Deferred.succeed(providerErrorSeen, undefined).pipe(Effect.andThen(Deferred.await(streamTail))),
        ).pipe(Stream.flatMap(() => Stream.fromIterable([] as LLMEvent[]))),
      )
      responses = [textTurn("text-wait-after-provider-error-steer", "Continuation answered")]
      const first = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start provider failure" }) })
      yield* awaitStreamStart(providerStarted)
      yield* Deferred.await(providerErrorSeen)
      waitForInactiveAfterJoin = true
      joinAck = joined
      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* awaitSignal(joined, "wait did not join the provider-error work group")
      const steered = yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Steer after provider failure" }),
      })
      yield* Deferred.succeed(streamTail, undefined)

      const result = yield* Deferred.await(outcome)
      expect(result).toMatchObject({
        _tag: "Success",
        value: { type: "failed", admittedSeq: steered.admittedSeq },
      })
      expect(steered.admittedSeq).toBeGreaterThan(first.admittedSeq)
      expect(requests).toHaveLength(2)
      if (!Exit.isSuccess(result)) throw new Error("Expected the in-flight provider error result")
      expect(yield* session.wait(sessionID)).toEqual(result.value)
      const history = yield* session.history({ sessionID, limit: 100 })
      const failedEvent = history.events.find((event) => event.type === SessionEvent.Step.Failed.type)
      const steeredEvent = history.events.find(
        (event) => event.type === SessionEvent.PromptAdmitted.type && event.data.messageID === steered.id,
      )
      if (!failedEvent?.durable || !steeredEvent?.durable)
        throw new Error("Expected durable provider failure and steer")
      expect(failedEvent.durable.seq).toBeLessThan(steeredEvent.durable.seq)
      yield* Fiber.join(waiter)
    }),
  )

  it.effect("settles a prompt admitted after a failed drain on its own outcome", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      responses = [
        [LLMEvent.stepStart({ index: 0 }), LLMEvent.providerError({ message: "Provider failed the first prompt" })],
        textTurn("text-wait-after-failed-drain", "Recovered"),
      ]
      const first = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail this prompt" }) })
      const failed = yield* session.wait(sessionID)
      const failedAssistant = (yield* session.messages({ sessionID })).find(
        (message): message is SessionMessage.Assistant => message.type === "assistant",
      )
      expect(failed).toEqual({
        type: "failed",
        admittedSeq: first.admittedSeq,
        assistantMessageID: failedAssistant?.id,
      })
      expect(Array.from(yield* session.active)).toEqual([])

      const second = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Prompt after the failure" }) })
      const result = yield* session.wait(sessionID)
      const recovered = (yield* session.messages({ sessionID })).find(
        (message): message is SessionMessage.Assistant =>
          message.type === "assistant" &&
          message.content.some((content) => content.type === "text" && content.text === "Recovered"),
      )
      if (!recovered) throw new Error("Expected the recovered assistant")
      expect(recovered.finish).toBe("stop")
      expect(result).toEqual({ type: "completed", admittedSeq: second.admittedSeq, assistantMessageID: recovered.id })
      expect(second.admittedSeq).toBeGreaterThan(first.admittedSeq)
      expect(requests).toHaveLength(2)
    }),
  )

  it.effect("settles a prompt sent after an interruption on its own outcome", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const providerStarted = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          responseStream = undefined
        }),
      )
      streamStartAcks.push(providerStarted)
      responseStream = Stream.concat(
        Stream.make(
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.textStart({ id: "text-wait-interrupted-before-prompt" }),
          LLMEvent.textDelta({ id: "text-wait-interrupted-before-prompt", text: "Interrupted mid-turn" }),
        ),
        Stream.never,
      )
      responses = [textTurn("text-wait-after-interruption", "Answered after the interruption")]
      const first = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Interrupt this prompt" }) })
      yield* awaitStreamStart(providerStarted)
      yield* session.interrupt(sessionID)
      const interrupted = yield* session.wait(sessionID)
      const interruptedAssistant = (yield* session.messages({ sessionID })).find(
        (message): message is SessionMessage.Assistant => message.type === "assistant",
      )
      expect(interruptedAssistant?.error?.origin).toBe(ProviderTurnInterruptedOrigin)
      expect(interrupted).toEqual({
        type: "interrupted",
        admittedSeq: first.admittedSeq,
        assistantMessageID: interruptedAssistant?.id,
      })

      const second = yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Prompt after the interruption" }),
      })
      const result = yield* session.wait(sessionID)
      const answered = (yield* session.messages({ sessionID })).find(
        (message): message is SessionMessage.Assistant =>
          message.type === "assistant" &&
          message.content.some(
            (content) => content.type === "text" && content.text === "Answered after the interruption",
          ),
      )
      if (!answered) throw new Error("Expected the assistant that answered after the interruption")
      expect(answered.finish).toBe("stop")
      expect(result).toEqual({ type: "completed", admittedSeq: second.admittedSeq, assistantMessageID: answered.id })
      expect(requests).toHaveLength(2)
    }),
  )

  it.effect("keeps an earlier drain's failure out of a later drain's continuation steer", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const gate = yield* Deferred.make<void>()
      const providerStarted = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          responseStream = undefined
        }).pipe(Effect.andThen(Deferred.succeed(gate, undefined)), Effect.asVoid),
      )
      responses = [
        [LLMEvent.stepStart({ index: 0 }), LLMEvent.providerError({ message: "Provider failed the first drain" })],
        textTurn("text-wait-steer-in-later-drain", "Steer answered"),
      ]
      const first = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail the first drain" }) })
      expect(yield* session.wait(sessionID)).toMatchObject({ type: "failed", admittedSeq: first.admittedSeq })
      expect(Array.from(yield* session.active)).toEqual([])

      streamStartAcks.push(providerStarted)
      responseStream = Stream.unwrap(
        Deferred.await(gate).pipe(Effect.as(Stream.fromIterable(textTurn("text-wait-later-drain", "Second answered")))),
      )
      const second = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start the later drain" }) })
      yield* awaitStreamStart(providerStarted)
      const steered = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Steer the later drain" }) })
      yield* Deferred.succeed(gate, undefined)
      const result = yield* session.wait(sessionID)

      const history = yield* session.history({ sessionID, limit: 100 })
      const steerPromotion = history.events.find(
        (event): event is SessionEvent.Prompted =>
          event.type === SessionEvent.Prompted.type && event.data.messageID === steered.id,
      )
      expect(steerPromotion?.data.continuation).toBe(true)
      const answered = (yield* session.messages({ sessionID })).find(
        (message): message is SessionMessage.Assistant =>
          message.type === "assistant" &&
          message.content.some((content) => content.type === "text" && content.text === "Steer answered"),
      )
      if (!answered) throw new Error("Expected the assistant that answered the steer")
      expect(answered.finish).toBe("stop")
      expect(result).toEqual({ type: "completed", admittedSeq: steered.admittedSeq, assistantMessageID: answered.id })
      expect(steered.admittedSeq).toBeGreaterThan(second.admittedSeq)
      expect(requests).toHaveLength(3)
    }),
  )

  it.effect("attributes a joined failure to an admission completed during the drain", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const gate = yield* Deferred.make<void>()
      const started = yield* Deferred.make<void>()
      const joined = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      let resolutions = 0
      waitForInactiveAfterJoin = true
      modelResolveHook = Effect.sync(() => resolutions++).pipe(
        Effect.andThen(
          Effect.suspend(() =>
            resolutions === 2 ? Effect.die(new Error("Continuation model resolution failed")) : Effect.void,
          ),
        ),
      )
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          joinAck = undefined
          streamGate = undefined
          streamStarted = undefined
          modelResolveHook = Effect.void
          waitForInactiveAfterJoin = false
        }).pipe(Effect.andThen(Deferred.succeed(gate, undefined)), Effect.asVoid),
      )
      streamGate = gate
      streamStarted = started
      response = textTurn("text-wait-watermark", "First turn held while a steer is admitted")
      const first = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start first turn" }) })
      yield* awaitStreamStart(started)
      joinAck = joined
      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* awaitSignal(joined, "wait did not join the running drain")
      const second = yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Fail continuation setup" }),
        resume: false,
      })
      yield* Deferred.succeed(gate, undefined)

      const result = yield* Deferred.await(outcome)
      expect(result).toMatchObject({
        _tag: "Success",
        value: { type: "failed", admittedSeq: second.admittedSeq },
      })
      expect(second.admittedSeq).toBeGreaterThan(first.admittedSeq)
      expect(resolutions).toBe(2)
      expect(requests).toHaveLength(1)
      yield* Fiber.join(waiter)
    }),
  )

  it.effect("retains a failed drain outcome through its coalesced no-op successor", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const gate = yield* Deferred.make<void>()
      const started = yield* Deferred.make<void>()
      const joined = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      let resolutions = 0
      modelResolveHook = Effect.sync(() => resolutions++).pipe(
        Effect.andThen(
          Effect.suspend(() =>
            resolutions === 2 ? Effect.die(new Error("Continuation model resolution failed")) : Effect.void,
          ),
        ),
      )
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          joinAck = undefined
          streamGate = undefined
          streamStarted = undefined
          modelResolveHook = Effect.void
        }).pipe(Effect.andThen(Deferred.succeed(gate, undefined)), Effect.asVoid),
      )
      streamGate = gate
      streamStarted = started
      response = textTurn("text-wait-coalesced-failure", "First turn held while a steer is admitted")
      const first = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start first turn" }) })
      yield* awaitStreamStart(started)
      joinAck = joined
      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* awaitSignal(joined, "wait did not join the running drain")
      const second = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail the steered turn" }) })
      yield* Deferred.succeed(gate, undefined)

      const result = yield* Deferred.await(outcome)
      expect(result).toMatchObject({
        _tag: "Success",
        value: { type: "failed", admittedSeq: second.admittedSeq },
      })
      expect(second.admittedSeq).toBeGreaterThan(first.admittedSeq)
      expect(resolutions).toBe(2)
      expect(requests).toHaveLength(1)
      expect(joinedFailureCount).toBeGreaterThan(0)
      expect(joinedSuccessCount).toBeGreaterThan(0)
      yield* Fiber.join(waiter)
    }),
  )

  it.effect("reports a provider failure from a forced run without admitted input", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      response = [LLMEvent.stepStart({ index: 0 }), LLMEvent.providerError({ message: "review forced failure" })]

      yield* session.resume(sessionID)

      expect(yield* session.wait(sessionID)).toMatchObject({ type: "failed" })
      expect(yield* session.messages({ sessionID })).toMatchObject([
        { type: "assistant", finish: "error", error: { message: "review forced failure" } },
      ])
      expect(requests).toHaveLength(1)
    }),
  )

  it.effect("does not report idle after incomplete forced assistant output", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.textStart({ id: "text-forced-partial" }),
        LLMEvent.textDelta({ id: "text-forced-partial", text: "Partial" }),
      ]

      yield* session.resume(sessionID)

      expect(Array.from(yield* session.active)).toEqual([])
      expect(yield* session.wait(sessionID).pipe(Effect.flip)).toMatchObject({
        _tag: "Session.OperationUnavailableError",
      })
    }),
  )

  it.effect("does not report idle while forced tool input remains unsettled", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-forced-pending", name: "echo" }),
      ]

      yield* session.resume(sessionID)

      expect(Array.from(yield* session.active)).toEqual([])
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "assistant", content: [{ type: "tool", state: { status: "pending" } }] },
      ])
      expect(yield* session.wait(sessionID).pipe(Effect.flip)).toMatchObject({
        _tag: "Session.OperationUnavailableError",
      })
    }),
  )

  it.effect("reports idle for a native Session with no admitted work", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const result = yield* session.wait(sessionID)

      expect(result).toEqual({ type: "idle" })
    }),
  )

  it.effect("returns completion after assistant and terminal tool projections agree", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      responses = [echoTurn("call-wait-completed", "settled"), textTurn("text-wait-completed", "Done")]
      const admitted = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Run the tool" }) })

      const result = yield* session.wait(sessionID)

      expect(result).toMatchObject({ type: "completed", admittedSeq: admitted.admittedSeq })
      if (result.type !== "completed") throw new Error("Expected completed Session wait result")
      const messages = yield* session.messages({ sessionID, order: "asc" })
      const assistant = yield* session.message({ sessionID, messageID: result.assistantMessageID })
      const tool = messages
        .filter((message): message is SessionMessage.Assistant => message.type === "assistant")
        .flatMap((message) => message.content)
        .find((content): content is SessionMessage.AssistantTool => content.type === "tool")
      const history = yield* session.history({ sessionID, limit: 100 })

      expect(assistant).toMatchObject({
        id: result.assistantMessageID,
        type: "assistant",
        content: [{ type: "text", text: "Done" }],
      })
      expect(tool?.state.status).toBe("completed")
      expect(history.hasMore).toBe(false)
      expect(history.events.map((event) => event.type)).toContain(SessionEvent.Tool.Success.type)
      expect(history.events.map((event) => event.type)).toContain(SessionEvent.Step.Ended.type)
      expect(requests).toHaveLength(2)
    }),
  )

  it.effect("does not settle while a local tool remains running", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const gate = yield* Deferred.make<void>()
      const started = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      toolExecutionGate = gate
      toolExecutionsStarted = started
      toolExecutionsReady = 1
      responses = [echoTurn("call-wait-held", "held"), textTurn("text-wait-held", "Done")]
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Wait for the held tool" }) })

      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* Deferred.await(started)
      yield* Effect.yieldNow
      expect(yield* Deferred.isDone(outcome)).toBe(false)

      yield* Deferred.succeed(gate, undefined)
      const result = yield* Deferred.await(outcome)
      expect(Exit.isSuccess(result)).toBe(true)
      if (Exit.isSuccess(result)) expect(result.value.type).toBe("completed")
      yield* Fiber.join(waiter)
    }).pipe(Effect.ensuring(toolExecutionGate ? Deferred.succeed(toolExecutionGate, undefined) : Effect.void)),
  )

  it.effect("covers a prompt admitted while the provider turn is running", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const gate = yield* Deferred.make<void>()
      const started = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      streamGate = gate
      streamStarted = started
      responses = [textTurn("text-wait-first", "First answer"), textTurn("text-wait-second", "Second answer")]
      const first = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First prompt" }) })
      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* Deferred.await(started)
      const second = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second prompt" }) })
      yield* Deferred.succeed(gate, undefined)

      const result = yield* Deferred.await(outcome)
      expect(Exit.isSuccess(result)).toBe(true)
      if (Exit.isSuccess(result))
        expect(result.value).toMatchObject({ type: "completed", admittedSeq: second.admittedSeq })
      expect(second.admittedSeq).toBeGreaterThan(first.admittedSeq)
      const messages = yield* session.messages({ sessionID })
      expect(messages.filter((message) => message.type === "user").map((message) => message.text)).toEqual([
        "Second prompt",
        "First prompt",
      ])
      expect(
        messages
          .filter((message): message is SessionMessage.Assistant => message.type === "assistant")
          .flatMap((message) => message.content)
          .filter((content): content is SessionMessage.AssistantText => content.type === "text")
          .map((content) => content.text),
      ).toEqual(["Second answer", "First answer"])
      expect(requests).toHaveLength(2)
      yield* Fiber.join(waiter)
    }).pipe(Effect.ensuring(streamGate ? Deferred.succeed(streamGate, undefined) : Effect.void)),
  )

  it.effect("reports provider-error events as failed instead of idle", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      response = [LLMEvent.stepStart({ index: 0 }), LLMEvent.providerError({ message: "Provider unavailable" })]
      const admitted = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail through an event" }) })

      const result = yield* session.wait(sessionID)

      expect(result).toMatchObject({ type: "failed", admittedSeq: admitted.admittedSeq })
      const assistant = (yield* session.messages({ sessionID })).find(
        (message): message is SessionMessage.Assistant => message.type === "assistant",
      )
      expect(assistant).toMatchObject({ finish: "error", error: { type: "unknown", message: "Provider unavailable" } })
      expect((yield* session.history({ sessionID, limit: 100 })).events.map((event) => event.type)).toContain(
        SessionEvent.Step.Failed.type,
      )
      expect(requests).toHaveLength(1)
    }),
  )

  it.effect("reports thrown provider failures as failed instead of idle", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      streamFailure = providerUnavailable()
      const admitted = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail through the stream" }) })

      const result = yield* session.wait(sessionID)

      expect(result).toMatchObject({ type: "failed", admittedSeq: admitted.admittedSeq })
      const assistant = (yield* session.messages({ sessionID })).find(
        (message): message is SessionMessage.Assistant => message.type === "assistant",
      )
      expect(assistant).toMatchObject({ finish: "error", error: { type: "unknown", message: "Provider unavailable" } })
      expect(requests).toHaveLength(1)
    }),
  )

  it.effect("reports a joined runner failure when no assistant turn was published", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const systemStarted = yield* Deferred.make<void>()
      const systemGate = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(systemGate, undefined)
          systemUnavailable = false
          systemLoadHook = Effect.void
        }),
      )
      systemUnavailable = false
      systemLoadHook = Deferred.succeed(systemStarted, undefined).pipe(Effect.andThen(Deferred.await(systemGate)))
      const admitted = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail before provider start" }) })
      yield* Deferred.await(systemStarted)
      expect(Array.from(yield* session.active)).toEqual([sessionID])
      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* Effect.yieldNow
      expect(yield* Deferred.isDone(outcome)).toBe(false)

      systemUnavailable = true
      yield* Deferred.succeed(systemGate, undefined)
      const result = yield* Deferred.await(outcome)
      expect(result).toMatchObject({
        _tag: "Success",
        value: { type: "failed", admittedSeq: admitted.admittedSeq },
      })
      expect(requests).toHaveLength(0)
      expect((yield* session.messages({ sessionID })).some((message) => message.type === "assistant")).toBe(false)
      const { db } = yield* Database.Service
      expect(yield* SessionInput.hasPending(db, sessionID, "steer")).toBe(true)
      yield* Fiber.join(waiter)
    }),
  )

  it.effect("does not report completion when a terminal tool failed", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-wait-missing", name: "missing", input: {} }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        textTurn("text-wait-tool-failed", "Handled the tool error"),
      ]
      const admitted = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Call an unknown tool" }) })

      const result = yield* session.wait(sessionID)

      expect(result).toMatchObject({ type: "failed", admittedSeq: admitted.admittedSeq })
      const toolMessage = (yield* session.messages({ sessionID })).find(
        (message): message is SessionMessage.Assistant =>
          message.type === "assistant" &&
          message.content.some((content) => content.type === "tool" && content.state.status === "error"),
      )
      expect(toolMessage).toBeDefined()
      expect((yield* session.history({ sessionID, limit: 100 })).events.map((event) => event.type)).toContain(
        SessionEvent.Tool.Failed.type,
      )
      expect(requests).toHaveLength(2)
    }),
  )

  it.effect("does not complete a successful tool turn without its required provider continuation", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      let resolutions = 0
      modelResolveHook = Effect.sync(() => resolutions++).pipe(
        Effect.andThen(
          Effect.suspend(() =>
            resolutions === 2 ? Effect.die(new Error("Continuation model resolution failed")) : Effect.void,
          ),
        ),
      )
      responses = [echoTurn("call-wait-continuation", "settled tool")]
      const admitted = yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Tool work still needs a provider continuation" }),
        resume: false,
      })

      const exit = yield* session.resume(sessionID).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      expect(requests).toHaveLength(1)
      expect(Array.from(yield* session.active)).toEqual([])
      const assistant = (yield* session.messages({ sessionID })).find(
        (message): message is SessionMessage.Assistant => message.type === "assistant",
      )
      if (assistant === undefined) throw new Error("Expected the completed tool turn to remain projected")
      expect(assistant.finish).toBe("tool-calls")
      expect(assistant.time.completed).toBeDefined()
      expect(assistant.content.some((content) => content.type === "tool" && content.state.status === "completed")).toBe(
        true,
      )
      expect(yield* session.wait(sessionID).pipe(Effect.flip)).toMatchObject({
        _tag: "Session.OperationUnavailableError",
      })
      expect(admitted.admittedSeq).toBe(1)
    }),
  )

  it.effect("does not carry a prior completion across a later admission", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      response = textTurn("text-wait-before-new-prompt", "First answer")
      const first = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First prompt" }) })
      const completed = yield* session.wait(sessionID)
      expect(completed).toMatchObject({ type: "completed", admittedSeq: first.admittedSeq })

      const second = yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Second prompt" }),
        resume: false,
      })
      const pending = yield* session.wait(sessionID)

      expect(second.admittedSeq).toBeGreaterThan(first.admittedSeq)
      expect(pending).toEqual({ type: "pending", admittedSeq: second.admittedSeq, messageID: second.id })
      expect(requests).toHaveLength(1)
    }),
  )

  it.effect("reports an interrupted drain and its terminal tool cleanup", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const gate = yield* Deferred.make<void>()
      const started = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      toolExecutionGate = gate
      toolExecutionsStarted = started
      toolExecutionsReady = 1
      response = echoTurn("call-wait-interrupted", "blocked")
      const admitted = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Interrupt this turn" }) })
      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* Deferred.await(started)
      yield* session.interrupt(sessionID)

      const messages = yield* session.messages({ sessionID })
      const assistant = messages.find(
        (message): message is SessionMessage.Assistant =>
          message.type === "assistant" && message.content.some((content) => content.type === "tool"),
      )
      const tool = assistant?.content.find((content) => content.type === "tool")
      expect(tool).toMatchObject({
        state: { status: "error", error: { message: "Tool execution interrupted" } },
      })
      const result = yield* Deferred.await(outcome)
      expect(result).toMatchObject({
        _tag: "Success",
        value: { type: "interrupted", admittedSeq: admitted.admittedSeq },
      })
      expect(assistant).toMatchObject({
        finish: "error",
        error: { message: ProviderTurnInterruptedMessage, origin: ProviderTurnInterruptedOrigin },
      })
      expect((yield* session.history({ sessionID, limit: 100 })).events.map((event) => event.type)).toContain(
        SessionEvent.Tool.Failed.type,
      )
      expect(requests).toHaveLength(1)
      yield* Fiber.join(waiter)
    }).pipe(Effect.ensuring(toolExecutionGate ? Deferred.succeed(toolExecutionGate, undefined) : Effect.void)),
  )

  it.effect("keeps a completed-step interruption visible to later waiters", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const tail = yield* Deferred.make<void>()
      const providerStarted = yield* Deferred.make<void>()
      const joined = yield* Deferred.make<void>()
      const outcome = yield* Deferred.make<Exit.Exit<SessionV2.WaitResult, SessionV2.Error>>()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          joinAck = undefined
          responseStream = undefined
        }).pipe(Effect.andThen(Deferred.succeed(tail, undefined)), Effect.asVoid),
      )
      responseStream = Stream.concat(
        Stream.make(
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.textStart({ id: "text-interrupt-after-step-finish" }),
          LLMEvent.textDelta({ id: "text-interrupt-after-step-finish", text: "The provider finished its step" }),
          LLMEvent.textEnd({ id: "text-interrupt-after-step-finish" }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        ),
        Stream.fromEffect(Deferred.await(tail)).pipe(
          Stream.flatMap(() => Stream.make(LLMEvent.finish({ reason: "stop" }))),
        ),
      )
      streamStartAcks.push(providerStarted)
      const admitted = yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Interrupt after the provider step finishes" }),
        resume: false,
      })
      const runner = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* awaitStreamStart(providerStarted)
      joinAck = joined
      const waiter = yield* session.wait(sessionID).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
        Effect.forkChild,
      )
      yield* awaitSignal(joined, "wait did not join the interrupted drain")
      yield* session.interrupt(sessionID)

      const first = yield* Deferred.await(outcome)
      expect(first).toMatchObject({
        _tag: "Success",
        value: { type: "interrupted", admittedSeq: admitted.admittedSeq },
      })
      const later = yield* session.wait(sessionID)
      expect(later).toMatchObject({ type: "interrupted", admittedSeq: admitted.admittedSeq })
      const assistant = (yield* session.messages({ sessionID })).find(
        (message): message is SessionMessage.Assistant => message.type === "assistant",
      )
      expect(assistant).toMatchObject({
        finish: "error",
        error: { message: ProviderTurnInterruptedMessage, origin: ProviderTurnInterruptedOrigin },
      })
      const history = yield* session.history({ sessionID, limit: 100 })
      const types = history.events.map((event) => event.type)
      expect(types).toContain(SessionEvent.Step.Failed.type)
      expect(types).not.toContain(SessionEvent.Step.Ended.type)
      const failedEvent = history.events.find(
        (event): event is SessionEvent.Step.Failed => event.type === SessionEvent.Step.Failed.type,
      )
      expect(failedEvent?.data.error.origin).toBe(ProviderTurnInterruptedOrigin)
      yield* Fiber.join(waiter)
      yield* Fiber.await(runner)
    }),
  )

  it.effect("keeps admit-only work pending and refuses an unobservable promoted turn", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const admitted = yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Admit without running" }),
        resume: false,
      })

      expect(yield* session.wait(sessionID)).toEqual({
        type: "pending",
        admittedSeq: admitted.admittedSeq,
        messageID: admitted.id,
      })
      expect(Array.from(yield* session.active)).toEqual([])

      const { db } = yield* Database.Service
      const events = yield* EventV2.Service
      yield* SessionInput.promoteSteers(db, events, sessionID, Number.MAX_SAFE_INTEGER)
      const unavailable = yield* session.wait(sessionID).pipe(Effect.flip)

      expect(unavailable._tag).toBe("Session.OperationUnavailableError")
    }),
  )
})

describe("SessionV2.events cursor", () => {
  it.effect("replays terminal events after disconnect without redelivering the consumed tool", () =>
    Effect.gen(function* () {
      yield* setupNative
      const session = yield* SessionV2.Service
      const { db } = yield* Database.Service
      const cursor = yield* EventV2.latestSequence(db, sessionID)
      const queue = yield* Queue.unbounded<SessionEvent.DurableEvent>()
      const subscriber = yield* session.events({ sessionID, after: cursor }).pipe(
        Stream.runForEach((event) => Queue.offer(queue, event).pipe(Effect.asVoid)),
        Effect.forkScoped,
      )
      const toolGate = yield* Deferred.make<void>()
      const toolStarted = yield* Deferred.make<void>()
      const finalProviderGate = yield* Deferred.make<void>()
      const finalProviderStarted = yield* Deferred.make<void>()
      toolExecutionGate = toolGate
      toolExecutionsStarted = toolStarted
      toolExecutionsReady = 1
      responses = [echoTurn("call-cursor", "tool result")]
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Produce cursor events" }) })
      yield* Deferred.await(toolStarted)
      responseStream = Stream.unwrap(
        Deferred.succeed(finalProviderStarted, undefined).pipe(
          Effect.andThen(
            Deferred.await(finalProviderGate).pipe(Effect.as(Stream.fromIterable(textTurn("text-cursor", "Complete")))),
          ),
        ),
      )
      yield* Deferred.succeed(toolGate, undefined)

      const consumed: SessionEvent.DurableEvent[] = []
      let tool: SessionEvent.Tool.Success | undefined
      while (tool === undefined) {
        const event = yield* Queue.take(queue).pipe(
          Effect.timeoutOrElse({
            duration: "10 seconds",
            orElse: () => Effect.fail(new Error("timed out waiting for the terminal tool event")),
          }),
        )
        consumed.push(event)
        if (event.type === SessionEvent.Tool.Success.type) tool = event
      }
      yield* Deferred.await(finalProviderStarted)
      yield* Fiber.interrupt(subscriber)
      yield* Deferred.succeed(finalProviderGate, undefined)
      yield* session.resume(sessionID)

      if (tool.durable === undefined) throw new Error("Consumed terminal tool event had no cursor")
      const toolCursor = tool.durable.seq
      const allHistory = yield* session.history({ sessionID, limit: 100 })
      const afterHistory = yield* session.history({ sessionID, after: toolCursor, limit: 100 })
      const resumed = Array.from(
        yield* session
          .events({ sessionID, after: toolCursor })
          .pipe(Stream.take(afterHistory.events.length), Stream.runCollect),
      )
      const ids = resumed.map((event) => event.id)
      const sequences = resumed.map((event) => event.durable?.seq)
      const consumedPrefix = allHistory.events.filter(
        (event) => event.durable !== undefined && event.durable.seq <= toolCursor,
      )
      const observed = [...consumed, ...resumed]

      expect(consumed.map((event) => event.id)).toContain(tool.id)
      expect(consumed.map((event) => [event.id, event.durable?.seq])).toEqual(
        consumedPrefix.map((event) => [event.id, event.durable?.seq]),
      )
      expect(allHistory.events.find((event) => event.id === tool.id)?.durable?.seq).toBe(toolCursor)
      expect(afterHistory.hasMore).toBe(false)
      expect(ids).toEqual(afterHistory.events.map((event) => event.id))
      expect(sequences).toEqual(afterHistory.events.map((event) => event.durable?.seq))
      expect(new Set(ids).size).toBe(ids.length)
      expect(ids).not.toContain(tool.id)
      expect(observed.map((event) => event.id)).toEqual(allHistory.events.map((event) => event.id))
      expect(new Set(observed.map((event) => event.id)).size).toBe(observed.length)
      expect(resumed.some((event) => event.type === SessionEvent.Step.Ended.type && event.data.finish === "stop")).toBe(
        true,
      )
    }).pipe(Effect.ensuring(toolExecutionGate ? Deferred.succeed(toolExecutionGate, undefined) : Effect.void)),
  )
})

const verifyEphemeralDeltas = (kind: FragmentKind) =>
  Effect.gen(function* () {
    yield* setup
    const session = yield* SessionV2.Service
    const prompt = `Stream ${kind}`
    const chunks = Array.from({ length: 32 }, (_, index) => `${index},`)
    const fixture = fragmentFixture(kind, fragmentID(kind, "many"), chunks)
    const expectedContext = [{ type: "user", text: prompt }, fixture.expectedAssistant]
    yield* session.prompt({ sessionID, prompt: Prompt.make({ text: prompt }), resume: false })
    const events = yield* EventV2.Service
    const live = yield* events.subscribe(fixture.delta).pipe(Stream.take(32), Stream.runCollect, Effect.forkScoped)
    yield* Effect.yieldNow
    response = fixture.completeEvents

    yield* session.resume(sessionID)

    const { db } = yield* Database.Service
    const deltas = yield* db
      .select({ type: EventTable.type })
      .from(EventTable)
      .where(eq(EventTable.type, EventV2.versionedType(fixture.delta.type, 1)))
      .all()
      .pipe(Effect.orDie)
    expect(Array.from(yield* Fiber.join(live))).toHaveLength(32)
    expect(deltas).toHaveLength(0)
    expect(yield* session.context(sessionID)).toMatchObject(expectedContext)

    yield* replaySessionProjection(sessionID)

    expect(yield* session.context(sessionID)).toMatchObject(expectedContext)
  })

const verifyPartialFlushOnFailure = (kind: FragmentKind) =>
  Effect.gen(function* () {
    yield* setup
    const session = yield* SessionV2.Service
    const prompt = `Fail after ${kind}`
    const fixture = fragmentFixture(kind, fragmentID(kind, "partial"), ["Partial"])
    const failure = providerUnavailable()
    yield* session.prompt({ sessionID, prompt: Prompt.make({ text: prompt }), resume: false })
    responseStream = Stream.concat(Stream.fromIterable(fixture.partialEvents), Stream.fail(failure))

    expect(yield* session.resume(sessionID).pipe(Effect.flip)).toBe(failure)
    expect(yield* session.context(sessionID)).toMatchObject([
      { type: "user", text: prompt },
      {
        type: "assistant",
        finish: "error",
        error: { type: "unknown", message: "Provider unavailable" },
        content: [fixture.expectedContent],
      },
    ])
  })

const verifyPartialFlushOnInterruption = (kind: FragmentKind) =>
  Effect.gen(function* () {
    yield* setup
    const session = yield* SessionV2.Service
    const prompt = `Interrupt after ${kind}`
    const fixture = fragmentFixture(kind, fragmentID(kind, "interrupted"), ["Partial"])
    const streamed = yield* Deferred.make<void>()
    yield* session.prompt({ sessionID, prompt: Prompt.make({ text: prompt }), resume: false })
    responseStream = Stream.concat(
      Stream.fromIterable(fixture.partialEvents),
      Stream.fromEffect(Deferred.succeed(streamed, undefined)).pipe(Stream.flatMap(() => Stream.never)),
    )

    const runner = yield* SessionRunner.Service
    const fiber = yield* runner.run({ sessionID, force: true }).pipe(Effect.forkChild)
    yield* Deferred.await(streamed)
    yield* Fiber.interrupt(fiber)
    expect(yield* session.context(sessionID)).toMatchObject([
      { type: "user", text: prompt },
      {
        type: "assistant",
        finish: "error",
        error: { type: "unknown", message: "Provider turn interrupted" },
        content: [
          kind === "tool input"
            ? { type: "tool", id: fragmentID(kind, "interrupted"), state: { status: "error" } }
            : fixture.expectedContent,
        ],
      },
    ])
  })

describe("SessionRunnerLLM", () => {
  it.effect("advertises and executes a globally attached application tool", () =>
    Effect.gen(function* () {
      yield* setup
      const applicationTools = yield* ApplicationTools.Service
      const session = yield* SessionV2.Service
      const contexts: Tool.Context[] = []
      yield* applicationTools.register({
        application_context: Tool.make({
          description: "Read application context",
          input: Schema.Struct({ query: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          execute: ({ query }, context) =>
            Effect.sync(() => {
              contexts.push(context)
              return { answer: query.toUpperCase() }
            }),
        }),
      })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Use application context" }), resume: false })
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-application", name: "application_context", input: { query: "hello" } }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [],
      ]

      yield* session.resume(sessionID)

      expect(requests[0]?.tools.map((tool) => tool.name)).toContain("application_context")
      expect(contexts).toEqual([
        {
          sessionID,
          agent: AgentV2.ID.make("build"),
          assistantMessageID: expect.stringMatching(/^msg_/),
          toolCallID: "call-application",
        },
      ])
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Use application context" },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "call-application",
              state: { status: "completed", structured: { answer: "HELLO" } },
            },
          ],
        },
      ])
    }),
  )

  it.effect(
    "starts a real runner turn after default prompt recording",
    () =>
      Effect.gen(function* () {
        yield* setup
        const session = yield* SessionV2.Service
        requests.length = 0
        responses = undefined
        streamGate = undefined
        streamStarted = undefined
        response = []
        const started = yield* Deferred.make<void>()
        streamStartAcks.push(started)

        const message = yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Run automatically" }) })
        yield* awaitStreamStart(started)

        expect(requests).toHaveLength(1)
        expect(yield* session.messages({ sessionID })).toMatchObject([
          { id: message.id, type: "user", text: "Run automatically" },
        ])
      }),
    { timeout: 10_000 },
  )

  it.effect("streams one request with registry definitions from chronological V2 user history", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second" }), resume: false })

      requests.length = 0
      responses = undefined
      streamGate = undefined
      streamStarted = undefined
      response = []
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(1)
      expect(requests[0]?.model).toBe(model)
      expect(requests[0]?.tools.map((tool) => tool.name)).toEqual(["echo", "defect"])
      expect(requests[0]?.messages.map((message) => ({ role: message.role, content: message.content }))).toEqual([
        { role: "user", content: [{ type: "text", text: "First" }] },
        { role: "user", content: [{ type: "text", text: "Second" }] },
      ])
      expect(yield* session.messages({ sessionID })).toHaveLength(2)
    }),
  )

  it.effect("retries the first provider turn after system context becomes available", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const { db } = yield* Database.Service
      const messageID = SessionMessage.ID.create()
      systemUnavailable = true
      yield* session.prompt({ id: messageID, sessionID, prompt: Prompt.make({ text: "First" }), resume: false })
      requests.length = 0

      const exit = yield* session.resume(sessionID).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(SystemContext.InitializationBlocked)
      expect(requests).toHaveLength(0)
      expect(yield* SessionInput.hasPending(db, sessionID, "steer")).toBe(true)
      expect(
        yield* db
          .select()
          .from(SessionContextEpochTable)
          .where(eq(SessionContextEpochTable.session_id, sessionID))
          .get(),
      ).toBeUndefined()

      systemUnavailable = false
      const started = yield* Deferred.make<void>()
      streamStartAcks.push(started)
      yield* session.prompt({ id: messageID, sessionID, prompt: Prompt.make({ text: "First" }) })
      yield* awaitStreamStart(started)

      expect(requests).toHaveLength(1)
      expect(requests[0]?.messages.map((message) => message.role)).toEqual(["user"])
    }),
    { timeout: 10_000 },
  )

  it.effect("interrupts a source Location runner after a Session moves", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })
      requests.length = 0
      response = []
      yield* session.resume(sessionID)

      yield* events.publish(SessionEvent.Moved, {
        sessionID,
        timestamp: DateTime.makeUnsafe(1),
        location: Location.Ref.make({ directory: AbsolutePath.make("/moved") }),
      })

      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second" }), resume: false })
      const exit = yield* session.resume(sessionID).pipe(Effect.exit)

      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
      expect(requests).toHaveLength(1)
      expect(yield* SessionInput.hasPending(db, sessionID, "steer")).toBe(true)
    }),
  )

  it.effect("fails gracefully when a stored context snapshot cannot be decoded", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const { db } = yield* Database.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })
      response = []
      yield* session.resume(sessionID)
      yield* db
        .update(SessionContextEpochTable)
        .set({ snapshot: { invalid: { value: "bad" } } })
        .where(eq(SessionContextEpochTable.session_id, sessionID))
        .run()
        .pipe(Effect.orDie)
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second" }), resume: false })
      requests.length = 0

      const exit = yield* session.resume(sessionID).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(ContextSnapshotDecodeError)
      expect(requests).toHaveLength(0)
    }),
  )

  it.effect("reuses one durable baseline after the context producer changes", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = []
      yield* session.resume(sessionID)
      systemBaseline = "Changed context"
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second" }), resume: false })
      yield* session.resume(sessionID)

      expect(requests.map((request) => request.system.map((part) => part.text))).toEqual([
        ["Initial context"],
        ["Initial context"],
      ])
      expect(requests[1]?.messages.map((message) => message.role)).toEqual(["user", "user", "system"])
      expect(requests[1]?.messages.at(-1)?.content).toEqual([{ type: "text", text: "Changed context" }])
      expect(yield* session.messages({ sessionID })).toHaveLength(3)
      const { db } = yield* Database.Service
      expect(
        yield* db
          .select({ id: EventTable.id })
          .from(EventTable)
          .where(eq(EventTable.type, "session.next.context.updated.1"))
          .all()
          .pipe(Effect.orDie),
      ).toHaveLength(1)
      yield* replaySessionProjection(sessionID)
      expect(yield* session.messages({ sessionID })).toHaveLength(3)
    }),
  )

  it.effect("includes the effective default agent system before durable context", () =>
    Effect.gen(function* () {
      yield* setup
      const agent = yield* AgentV2.Service
      yield* agent.transform((editor) =>
        editor.update(AgentV2.ID.make("build"), (agent) => {
          agent.system = "Build agent instructions"
          agent.mode = "primary"
        }),
      )
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = fragmentFixture("text", "text-build", ["Done"]).completeEvents
      yield* session.resume(sessionID)

      expect(requests.at(-1)?.system.map((part) => part.text)).toEqual(["Build agent instructions", "Initial context"])
    }),
  )

  it.effect("uses the configured default agent system for omitted-agent sessions", () =>
    Effect.gen(function* () {
      yield* setup
      const agent = yield* AgentV2.Service
      yield* agent.transform((editor) => {
        editor.update(AgentV2.ID.make("build"), (agent) => {
          agent.system = "Build agent instructions"
          agent.mode = "primary"
        })
        editor.update(AgentV2.ID.make("reviewer"), (agent) => {
          agent.system = "Reviewer instructions"
          agent.mode = "primary"
        })
        editor.default(AgentV2.ID.make("reviewer"))
      })
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = fragmentFixture("text", "text-reviewer", ["Done"]).completeEvents
      yield* session.resume(sessionID)

      expect(requests.at(-1)?.system.map((part) => part.text)).toEqual(["Reviewer instructions", "Initial context"])
      expect((yield* session.messages({ sessionID }))[0]).toMatchObject({ type: "assistant", agent: "reviewer" })
    }),
  )

  it.effect("uses an explicitly selected non-build agent system", () =>
    Effect.gen(function* () {
      yield* setup
      const { db } = yield* Database.Service
      const agent = yield* AgentV2.Service
      yield* agent.transform((editor) =>
        editor.update(AgentV2.ID.make("reviewer"), (agent) => {
          agent.system = "Reviewer instructions"
          agent.mode = "primary"
        }),
      )
      yield* db
        .update(SessionTable)
        .set({ agent: "reviewer" })
        .where(eq(SessionTable.id, sessionID))
        .run()
        .pipe(Effect.orDie)
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = fragmentFixture("text", "text-selected", ["Done"]).completeEvents
      yield* session.resume(sessionID)

      expect(requests.at(-1)?.system.map((part) => part.text)).toEqual(["Reviewer instructions", "Initial context"])
      expect((yield* session.messages({ sessionID }))[0]).toMatchObject({ type: "assistant", agent: "reviewer" })
    }),
  )

  it.effect("updates selected-agent skill guidance after an agent switch", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      skillBaselines.set(AgentV2.ID.make("build"), "Build skills")
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = []
      yield* session.resume(sessionID)
      skillBaselines.set(AgentV2.ID.make("reviewer"), "Reviewer skills")
      yield* events.publish(SessionEvent.AgentSwitched, {
        sessionID,
        messageID: SessionMessage.ID.create(),
        timestamp: DateTime.makeUnsafe(1),
        agent: "reviewer",
      })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second" }), resume: false })
      yield* session.resume(sessionID)

      expect(requests.map((request) => request.system.map((part) => part.text))).toEqual([
        ["Initial context\n\nBuild skills"],
        ["Initial context\n\nBuild skills"],
      ])
      expect(systemTexts(requests[1]!)).toContainEqual(expect.stringContaining("Reviewer skills"))
    }),
  )

  it.effect("keeps the sampled agent when selection changes during observation", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      skillBaselines.set(AgentV2.ID.make("build"), "Build skills")
      skillBaselines.set(AgentV2.ID.make("reviewer"), "Reviewer skills")
      let switched = false
      systemLoadHook = Effect.suspend(() => {
        if (switched) return Effect.void
        switched = true
        return events
          .publish(SessionEvent.AgentSwitched, {
            sessionID,
            messageID: SessionMessage.ID.create(),
            timestamp: DateTime.makeUnsafe(1),
            agent: "reviewer",
          })
          .pipe(Effect.asVoid)
      })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = []
      yield* session.resume(sessionID)

      expect(requests.map((request) => request.system.map((part) => part.text))).toEqual([
        ["Initial context\n\nBuild skills"],
      ])
    }),
  )

  it.effect("keeps the sampled model when selection changes during model resolution", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      let switched = false
      modelResolveHook = Effect.suspend(() => {
        if (switched) return Effect.void
        switched = true
        return events
          .publish(SessionEvent.ModelSwitched, {
            sessionID,
            messageID: SessionMessage.ID.create(),
            timestamp: DateTime.makeUnsafe(1),
            model: { id: ModelV2.ID.make("replacement"), providerID: ProviderV2.ID.make("fake") },
          })
          .pipe(Effect.asVoid)
      })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = []
      yield* session.resume(sessionID)
      expect(requests.map((request) => request.model)).toEqual([model])
      expect(requests.map((request) => request.system.map((part) => part.text))).toEqual([["Initial context"]])
    }),
  )

  it.effect("admits removed context as a chronological System message", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = []
      yield* session.resume(sessionID)
      systemRemoved = true
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second" }), resume: false })
      yield* session.resume(sessionID)

      expect(requests[1]?.messages.map((message) => message.role)).toEqual(["user", "user", "system"])
      expect(requests[1]?.messages.at(-1)?.content).toEqual([
        { type: "text", text: "System context source removed: test/context" },
      ])
      expect(yield* session.messages({ sessionID })).toHaveLength(3)
    }),
  )

  it.effect("keeps the baseline and chronological System updates after a model switch", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = []
      yield* session.resume(sessionID)
      systemBaseline = "Changed context"
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second" }), resume: false })
      yield* session.resume(sessionID)
      yield* events.publish(SessionEvent.ModelSwitched, {
        sessionID,
        messageID: SessionMessage.ID.create(),
        timestamp: DateTime.makeUnsafe(1),
        model: { id: ModelV2.ID.make("replacement"), providerID: ProviderV2.ID.make("fake") },
      })
      systemBaseline = "Replacement context"
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Third" }), resume: false })
      yield* session.resume(sessionID)

      expect(requests.map((request) => request.system.map((part) => part.text))).toEqual([
        ["Initial context"],
        ["Initial context"],
        ["Initial context"],
      ])
      expect(requests[1]?.messages.map((message) => message.role)).toEqual(["user", "user", "system"])
      expect(requests[2]?.messages.filter((message) => message.role === "system")).toHaveLength(2)
      expect((yield* session.context(sessionID)).map((message) => message.type)).toEqual([
        "user",
        "user",
        "system",
        "model-switched",
        "user",
        "system",
      ])
      yield* replaySessionProjection(sessionID)
      expect(yield* session.messages({ sessionID })).toHaveLength(6)
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fourth" }), resume: false })
      yield* session.resume(sessionID)
    }),
  )

  it.effect("preserves the baseline while context is temporarily unavailable", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = []
      yield* session.resume(sessionID)
      yield* events.publish(SessionEvent.ModelSwitched, {
        sessionID,
        messageID: SessionMessage.ID.create(),
        timestamp: DateTime.makeUnsafe(1),
        model: { id: ModelV2.ID.make("replacement"), providerID: ProviderV2.ID.make("fake") },
      })
      systemUnavailable = true
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second" }), resume: false })
      yield* session.resume(sessionID)
      systemUnavailable = false
      systemBaseline = "Replacement context"
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Third" }), resume: false })
      yield* session.resume(sessionID)

      expect(requests.map((request) => request.system.map((part) => part.text))).toEqual([
        ["Initial context"],
        ["Initial context"],
        ["Initial context"],
      ])
    }),
  )

  it.effect("rebuilds the baseline directly after completed compaction", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = []
      yield* session.resume(sessionID)
      const compactionID = SessionMessage.ID.create()
      yield* events.publish(SessionEvent.Compaction.Started, {
        sessionID,
        messageID: compactionID,
        timestamp: DateTime.makeUnsafe(1),
        reason: "manual",
      })
      yield* events.publish(SessionEvent.Compaction.Ended, {
        sessionID,
        messageID: compactionID,
        timestamp: DateTime.makeUnsafe(2),
        reason: "manual",
        text: "summary",
        recent: "",
      })
      systemBaseline = "Replacement context"
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second" }), resume: false })
      yield* session.resume(sessionID)

      expect(requests.map((request) => request.system.map((part) => part.text))).toEqual([
        ["Initial context"],
        ["Replacement context"],
      ])
      yield* replaySessionProjection(sessionID)
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Third" }), resume: false })
      yield* session.resume(sessionID)
    }),
  )

  it.effect("automatically compacts into a completed summary and retained recent turn", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      response = fragmentFixture("text", "text-first", ["Earlier answer"]).completeEvents
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Earlier question ".repeat(180) }),
        resume: false,
      })
      yield* session.resume(sessionID)

      currentModel = compactModel
      requests.length = 0
      responses = [
        fragmentFixture("text", "text-summary", ["## Objective\n- Preserve the task"]).completeEvents,
        fragmentFixture("text", "text-final", ["Continued"]).completeEvents,
      ]
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Recent exact request ".repeat(180) }),
        resume: false,
      })
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      expect(requests.map((request) => request.http?.headers)).toEqual([
        {
          "x-session-affinity": sessionID,
          "X-Session-Id": sessionID,
        },
        {
          "x-session-affinity": sessionID,
          "X-Session-Id": sessionID,
        },
      ])
      expect(userTexts(requests[0])[0]).toContain("## Objective")
      expect(userTexts(requests[1])).toHaveLength(1)
      expect(userTexts(requests[1])[0]).toContain("<summary>\n## Objective\n- Preserve the task\n</summary>")
      expect(userTexts(requests[1])[0]).toContain(`[User]: ${"Recent exact request ".repeat(180)}`)

      const context = yield* (yield* SessionStore.Service).context(sessionID)
      expect(context.map((message) => message.type)).toEqual(["compaction", "assistant"])
      expect(context[0]).toMatchObject({
        type: "compaction",
        summary: "## Objective\n- Preserve the task",
      })

      requests.length = 0
      executions.length = 0
      responses = [
        fragmentFixture("text", "text-summary-2", ["## Objective\n- Preserve the updated task"]).completeEvents,
        fragmentFixture("text", "text-final-2", ["Continued again"]).completeEvents,
      ]
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Newest exact request ".repeat(180) }),
        resume: false,
      })
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      expect(userTexts(requests[0])[0]).toContain(
        "<prior-summary>\n## Objective\n- Preserve the task\n</prior-summary>",
      )
      expect(userTexts(requests[0])[0]).toContain("Recent exact request")
      expect((yield* (yield* SessionStore.Service).context(sessionID))[0]).toMatchObject({
        type: "compaction",
        summary: "## Objective\n- Preserve the updated task",
      })
    }),
  )

  it.effect("retains only complete serialized messages during compaction", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const earlier = `EARLIER_BOUNDARY ${"a".repeat(3_000)} EARLIER_END`
      const recent = `RECENT_BOUNDARY ${"b".repeat(3_000)} RECENT_END`
      response = fragmentFixture("text", "text-earlier", ["Earlier answer"]).completeEvents
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: earlier }), resume: false })
      yield* session.resume(sessionID)

      currentModel = compactModel
      requests.length = 0
      responses = [
        fragmentFixture("text", "text-summary", ["## Objective\n- Preserve the task"]).completeEvents,
        fragmentFixture("text", "text-final", ["Continued"]).completeEvents,
      ]
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: recent }), resume: false })
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      const summary = userTexts(requests[0])[0]
      const continuation = userTexts(requests[1])[0]
      expect(summary.match(/EARLIER_BOUNDARY/g)).toHaveLength(1)
      expect(summary).toContain(`EARLIER_BOUNDARY ${"a".repeat(3_000)} EARLIER_END`)
      expect(summary).not.toContain("RECENT_BOUNDARY")
      expect(continuation).not.toContain("EARLIER_BOUNDARY")
      expect(continuation).not.toContain("EARLIER_END")
      expect(continuation).toContain("<recent-context>\n[Assistant]: Earlier answer")
      expect(continuation).toContain(`RECENT_BOUNDARY ${"b".repeat(3_000)} RECENT_END`)
    }),
  )

  it.effect("summarizes an oversized newest message without retaining a fragment", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      response = fragmentFixture("text", "text-earlier", ["Earlier answer"]).completeEvents
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Earlier question" }), resume: false })
      yield* session.resume(sessionID)

      const oversized = `OVERSIZED_BOUNDARY ${"x".repeat(4_500)} OVERSIZED_END`
      currentModel = compactModel
      requests.length = 0
      responses = [
        fragmentFixture("text", "text-summary", ["## Objective\n- Preserve the task"]).completeEvents,
        fragmentFixture("text", "text-final", ["Continued"]).completeEvents,
      ]
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: oversized }), resume: false })
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      const summary = userTexts(requests[0])[0]
      const continuation = userTexts(requests[1])[0]
      expect(summary.match(/OVERSIZED_BOUNDARY/g)).toHaveLength(1)
      expect(summary).toContain(oversized)
      expect(continuation).not.toContain("OVERSIZED_BOUNDARY")
      expect(continuation).not.toContain("OVERSIZED_END")
      expect(continuation).toContain("<recent-context>\n\n</recent-context>")
    }),
  )

  it.effect("forces one compaction and retries after provider context overflow", () =>
    Effect.gen(function* () {
      const session = yield* setupOverflowRecovery
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.providerError({ message: "prompt too long", classification: "context-overflow" }),
        ],
        fragmentFixture("text", "text-summary", ["## Objective\n- Recover overflow"]).completeEvents,
        fragmentFixture("text", "text-final", ["Recovered"]).completeEvents,
      ]
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Continue" }), resume: false })
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(3)
      expect(userTexts(requests[1])[0]).toContain("## Objective")
      expect(userTexts(requests[2])[0]).toContain("<summary>\n## Objective\n- Recover overflow\n</summary>")
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "compaction", summary: "## Objective\n- Recover overflow" },
        { type: "assistant", finish: "stop" },
      ])
      yield* replaySessionProjection(sessionID)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "compaction" },
        { type: "assistant", finish: "stop" },
      ])
    }),
  )

  it.effect("persists a second context overflow after one recovery", () =>
    Effect.gen(function* () {
      const session = yield* setupOverflowRecovery
      const overflow = () => [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.providerError({ message: "prompt too long", classification: "context-overflow" }),
      ]
      responses = [
        overflow(),
        fragmentFixture("text", "text-summary", ["## Objective\n- Recover once"]).completeEvents,
        overflow(),
      ]
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Continue" }), resume: false })
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(3)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "compaction" },
        { type: "assistant", finish: "error", error: { message: "prompt too long" } },
      ])
    }),
  )

  it.effect("recovers once from a raw context overflow failure", () =>
    Effect.gen(function* () {
      const session = yield* setupOverflowRecovery
      responseStream = Stream.fail(
        new LLMError({
          module: "test",
          method: "stream",
          reason: new InvalidRequestReason({
            message: "prompt too long",
            classification: "context-overflow",
          }),
        }),
      )
      responses = [
        fragmentFixture("text", "text-summary", ["## Objective\n- Recover raw overflow"]).completeEvents,
        fragmentFixture("text", "text-final", ["Recovered"]).completeEvents,
      ]
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Continue" }), resume: false })
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(3)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "compaction", summary: "## Objective\n- Recover raw overflow" },
        { type: "assistant", finish: "stop" },
      ])
    }),
  )

  it.effect("publishes the original overflow when recovery summarization fails", () =>
    Effect.gen(function* () {
      const session = yield* setupOverflowRecovery
      responses = [
        [LLMEvent.providerError({ message: "prompt too long", classification: "context-overflow" })],
        [LLMEvent.providerError({ message: "summary unavailable" })],
      ]
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Continue" }), resume: false })
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      const context = yield* session.context(sessionID)
      expect(context.some((message) => message.type === "compaction")).toBe(false)
      expect(context.slice(-2)).toMatchObject([
        { type: "user", text: "Continue" },
        { type: "assistant", finish: "error", error: { message: "prompt too long" } },
      ])
    }),
  )

  it.effect("interrupts overflow recovery while the summary provider is running", () =>
    Effect.gen(function* () {
      const session = yield* setupOverflowRecovery
      responses = [
        [LLMEvent.providerError({ message: "prompt too long", classification: "context-overflow" })],
        fragmentFixture("text", "text-summary", ["## Objective\n- Interrupted"]).completeEvents,
      ]
      const firstGate = yield* Deferred.make<void>()
      const summaryGate = yield* Deferred.make<void>()
      streamGate = firstGate
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Continue" }), resume: false })
      const run = yield* session.resume(sessionID).pipe(Effect.forkChild)
      while (requests.length < 1) yield* Effect.yieldNow
      streamGate = summaryGate
      yield* Deferred.succeed(firstGate, undefined)
      while (requests.length < 2) yield* Effect.yieldNow

      yield* session.interrupt(sessionID)
      expect(yield* Fiber.await(run)).toMatchObject({ _tag: "Failure" })
      streamGate = undefined
      expect(requests).toHaveLength(2)
      expect((yield* session.context(sessionID)).some((message) => message.type === "compaction")).toBe(false)
    }),
  )

  it.effect("preserves effective System updates while compaction rebaseline is blocked", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First" }), resume: false })

      requests.length = 0
      response = []
      yield* session.resume(sessionID)
      systemBaseline = "Changed context"
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second" }), resume: false })
      yield* session.resume(sessionID)
      const compactionID = SessionMessage.ID.create()
      yield* events.publish(SessionEvent.Compaction.Started, {
        sessionID,
        messageID: compactionID,
        timestamp: DateTime.makeUnsafe(1),
        reason: "manual",
      })
      yield* events.publish(SessionEvent.Compaction.Ended, {
        sessionID,
        messageID: compactionID,
        timestamp: DateTime.makeUnsafe(2),
        reason: "manual",
        text: "summary",
        recent: "",
      })
      systemUnavailable = true
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Third" }), resume: false })
      yield* session.resume(sessionID)

      expect(requests.at(-1)?.system.map((part) => part.text)).toEqual(["Initial context"])
      expect(systemTexts(requests.at(-1)!)).toContain("Changed context")
    }),
  )

  it.effect("projects reasoning and tool events without executing or continuing tools", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Use tools" }), resume: false })

      requests.length = 0
      responses = undefined
      streamGate = undefined
      streamStarted = undefined
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.reasoningStart({ id: "reasoning-1" }),
        LLMEvent.reasoningDelta({ id: "reasoning-1", text: "Think" }),
        LLMEvent.reasoningEnd({ id: "reasoning-1" }),
        LLMEvent.toolInputStart({ id: "call-error", name: "write" }),
        LLMEvent.toolInputDelta({ id: "call-error", name: "write", text: '{"path":"README.md"}' }),
        LLMEvent.toolInputEnd({ id: "call-error", name: "write" }),
        LLMEvent.toolCall({ id: "call-error", name: "write", input: { path: "README.md" }, providerExecuted: true }),
        LLMEvent.toolError({ id: "call-error", name: "write", message: "Denied" }),
        LLMEvent.toolResult({ id: "call-error", name: "write", result: { type: "error", value: "Denied" } }),
        LLMEvent.toolCall({
          id: "call-provider",
          name: "web_search",
          input: { query: "hello" },
          providerExecuted: true,
          providerMetadata: { fake: { source: "provider" } },
        }),
        LLMEvent.toolResult({
          id: "call-provider",
          name: "web_search",
          result: {
            type: "content",
            value: [
              { type: "text", text: "Hello" },
              { type: "file", uri: "data:image/png;base64,aGVsbG8=", mime: "image/png", name: "hello.png" },
            ],
          },
          providerExecuted: true,
          providerMetadata: { fake: { source: "provider" } },
        }),
        LLMEvent.stepFinish({
          index: 0,
          reason: "tool-calls",
          usage: {
            inputTokens: 10,
            nonCachedInputTokens: 8,
            outputTokens: 4,
            reasoningTokens: 1,
            cacheReadInputTokens: 2,
          },
        }),
        LLMEvent.finish({ reason: "tool-calls" }),
      ]

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(1)
      expect(requests[0]?.tools.map((tool) => tool.name)).toEqual(["echo", "defect"])
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Use tools" },
        {
          type: "assistant",
          finish: "tool-calls",
          tokens: { input: 8, output: 3, reasoning: 1, cache: { read: 2, write: 0 } },
          content: [
            { type: "reasoning", id: "reasoning-1", text: "Think" },
            {
              type: "tool",
              id: "call-error",
              name: "write",
              state: {
                status: "error",
                input: { path: "README.md" },
                error: { type: "unknown", message: "Denied" },
              },
            },
            {
              type: "tool",
              id: "call-provider",
              name: "web_search",
              provider: { executed: true, metadata: { fake: { source: "provider" } } },
              state: {
                status: "completed",
                input: { query: "hello" },
                structured: {},
                content: [
                  { type: "text", text: "Hello" },
                  { type: "file", mime: "image/png", uri: "data:image/png;base64,aGVsbG8=", name: "hello.png" },
                ],
              },
            },
          ],
        },
      ])
    }),
  )

  it.effect("continues with reloaded history after durably settling one local tool call", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Echo this" }), resume: false })

      requests.length = 0
      authorizations.length = 0
      executions.length = 0
      streamGate = undefined
      streamStarted = undefined
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-echo", name: "echo", input: { text: "hello" } }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.textStart({ id: "text-final" }),
          LLMEvent.textDelta({ id: "text-final", text: "Done" }),
          LLMEvent.textEnd({ id: "text-final" }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      expect(requests[1]?.messages.map((message) => message.role)).toEqual(["user", "assistant", "tool"])
      expect(authorizations).toMatchObject([{ sessionID, toolCallID: "call-echo" }])
      expect(executions).toEqual(["hello"])
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Echo this" },
        {
          type: "assistant",
          finish: "tool-calls",
          content: [
            {
              type: "tool",
              id: "call-echo",
              name: "echo",
              state: {
                status: "completed",
                input: { text: "hello" },
                structured: { text: "hello" },
                content: [{ type: "text", text: "hello" }],
              },
            },
          ],
        },
        { type: "assistant", finish: "stop", content: [{ type: "text", id: "text-final", text: "Done" }] },
      ])
    }),
  )

  it.effect("reloads a model switch before a tool-driven continuation turn", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Echo this" }), resume: false })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-echo", name: "echo", input: { text: "hello" } }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]
      toolExecutionGate = yield* Deferred.make<void>()
      toolExecutionsStarted = yield* Deferred.make<void>()
      toolExecutionsReady = 1
      const run = yield* Effect.forkChild(session.resume(sessionID))
      yield* Deferred.await(toolExecutionsStarted)
      yield* events.publish(SessionEvent.ModelSwitched, {
        sessionID,
        messageID: SessionMessage.ID.create(),
        timestamp: DateTime.makeUnsafe(1),
        model: { id: ModelV2.ID.make("replacement"), providerID: ProviderV2.ID.make("fake") },
      })
      systemBaseline = "Replacement context"
      yield* Deferred.succeed(toolExecutionGate, undefined)
      yield* Fiber.join(run)

      expect(requests.map((request) => request.model)).toEqual([model, replacementModel])
      expect(requests.map((request) => request.system.map((part) => part.text))).toEqual([
        ["Initial context"],
        ["Initial context"],
      ])
      expect(systemTexts(requests[1]!)).toContain("Replacement context")
    }),
  )

  it.effect("restores durable reasoning provider metadata in a second-turn request", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Think first" }), resume: false })

      requests.length = 0
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.reasoningStart({ id: "reasoning-anthropic" }),
        LLMEvent.reasoningDelta({ id: "reasoning-anthropic", text: "Signed thought" }),
        LLMEvent.reasoningEnd({ id: "reasoning-anthropic", providerMetadata: { anthropic: { signature: "sig_1" } } }),
        LLMEvent.reasoningStart({
          id: "reasoning-openai",
          providerMetadata: { openai: { itemId: "rs_1", reasoningEncryptedContent: null } },
        }),
        LLMEvent.reasoningDelta({ id: "reasoning-openai", text: "Encrypted thought" }),
        LLMEvent.reasoningEnd({
          id: "reasoning-openai",
          providerMetadata: { openai: { itemId: "rs_1", reasoningEncryptedContent: "encrypted-state" } },
        }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ]
      yield* session.resume(sessionID)
      yield* replaySessionProjection(sessionID)

      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Think first" },
        {
          type: "assistant",
          content: [
            { type: "reasoning", text: "Signed thought", providerMetadata: { anthropic: { signature: "sig_1" } } },
            {
              type: "reasoning",
              text: "Encrypted thought",
              providerMetadata: { openai: { itemId: "rs_1", reasoningEncryptedContent: "encrypted-state" } },
            },
          ],
        },
      ])

      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Continue" }), resume: false })
      response = []
      yield* session.resume(sessionID)

      expect(requests[1]?.messages[1]?.content).toEqual([
        { type: "reasoning", text: "Signed thought", providerMetadata: { anthropic: { signature: "sig_1" } } },
        {
          type: "reasoning",
          text: "Encrypted thought",
          providerMetadata: { openai: { itemId: "rs_1", reasoningEncryptedContent: "encrypted-state" } },
        },
      ])
    }),
  )

  it.effect("replays durable provider-executed tool results inline in a second-turn request", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Search first" }), resume: false })

      requests.length = 0
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolCall({
          id: "hosted-search",
          name: "web_search",
          input: { query: "Effect" },
          providerExecuted: true,
          providerMetadata: { openai: { itemId: "hosted-search" } },
        }),
        LLMEvent.toolResult({
          id: "hosted-search",
          name: "web_search",
          result: { type: "json", value: [{ title: "Effect" }] },
          providerExecuted: true,
          providerMetadata: { anthropic: { blockType: "web_search_tool_result" } },
        }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ]
      yield* session.resume(sessionID)
      yield* replaySessionProjection(sessionID)

      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Continue" }), resume: false })
      response = []
      yield* session.resume(sessionID)

      expect(requests[1]?.messages.map((message) => message.role)).toEqual(["user", "assistant", "user"])
      expect(requests[1]?.messages[1]?.content).toMatchObject([
        {
          type: "tool-call",
          id: "hosted-search",
          name: "web_search",
          input: { query: "Effect" },
          providerExecuted: true,
          providerMetadata: { openai: { itemId: "hosted-search" } },
        },
        {
          type: "tool-result",
          id: "hosted-search",
          name: "web_search",
          result: { type: "json", value: [{ title: "Effect" }] },
          providerExecuted: true,
          providerMetadata: { anthropic: { blockType: "web_search_tool_result" } },
        },
      ])
    }),
  )

  it.effect("starts recorded local tools eagerly and awaits settlement before continuing", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Echo five times" }), resume: false })

      requests.length = 0
      executions.length = 0
      toolExecutionGate = yield* Deferred.make<void>()
      toolExecutionsStarted = yield* Deferred.make<void>()
      const providerGate = yield* Deferred.make<void>()
      response = []
      responses = undefined
      const initial = Stream.fromIterable([
        LLMEvent.stepStart({ index: 0 }),
        ...Array.from({ length: 5 }, (_, index) =>
          LLMEvent.toolCall({ id: `call-echo-${index}`, name: "echo", input: { text: `${index}` } }),
        ),
      ])
      const final = Stream.fromIterable([
        LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
        LLMEvent.finish({ reason: "tool-calls" }),
      ])
      streamGate = undefined
      responseStream = Stream.concat(
        initial,
        Stream.fromEffect(Deferred.await(providerGate)).pipe(Stream.flatMap(() => final)),
      )

      const run = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(toolExecutionsStarted)

      expect(executions).toHaveLength(5)
      expect(maxActiveToolExecutions).toBe(5)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Echo five times" },
        {
          type: "assistant",
          content: Array.from({ length: 5 }, (_, index) => ({
            type: "tool",
            id: `call-echo-${index}`,
            state: { status: "running", input: { text: `${index}` } },
          })),
        },
      ])

      yield* Deferred.succeed(providerGate, undefined)
      yield* Effect.yieldNow
      expect(requests).toHaveLength(1)

      yield* Deferred.succeed(toolExecutionGate, undefined)
      yield* Fiber.join(run)
      toolExecutionGate = undefined
      toolExecutionsStarted = undefined

      expect(executions).toHaveLength(5)
      expect(maxActiveToolExecutions).toBe(5)
      expect(requests).toHaveLength(2)
    }),
  )

  it.effect("settles repeated provider-local tool call IDs against their owning assistant messages", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Echo twice" }), resume: false })

      requests.length = 0
      executions.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "tool_0", name: "echo", input: { text: "first" } }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "tool_0", name: "echo", input: { text: "second" } }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [],
      ]

      yield* session.resume(sessionID)

      expect(executions).toEqual(["first", "second"])
      expect(requests).toHaveLength(3)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Echo twice" },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "tool_0",
              state: { status: "completed", structured: { text: "first" }, content: [{ type: "text", text: "first" }] },
            },
          ],
        },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "tool_0",
              state: {
                status: "completed",
                structured: { text: "second" },
                content: [{ type: "text", text: "second" }],
              },
            },
          ],
        },
      ])

      yield* replaySessionProjection(sessionID)

      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Echo twice" },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "tool_0",
              state: { status: "completed", structured: { text: "first" }, content: [{ type: "text", text: "first" }] },
            },
          ],
        },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "tool_0",
              state: {
                status: "completed",
                structured: { text: "second" },
                content: [{ type: "text", text: "second" }],
              },
            },
          ],
        },
      ])
    }),
  )

  it.effect("joins concurrent resume calls into one active provider run", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Run once" }), resume: false })

      requests.length = 0
      responses = undefined
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.textStart({ id: "text-once" }),
        LLMEvent.textDelta({ id: "text-once", text: "Once" }),
        LLMEvent.textEnd({ id: "text-once" }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ]
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const first = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(streamStarted)
      const second = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Effect.yieldNow

      expect(requests).toHaveLength(1)
      yield* Deferred.succeed(streamGate, undefined)
      yield* Fiber.join(first)
      yield* Fiber.join(second)
      streamGate = undefined
      streamStarted = undefined

      expect(requests).toHaveLength(1)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Run once" },
        { type: "assistant", finish: "stop", content: [{ type: "text", id: "text-once", text: "Once" }] },
      ])
    }),
  )

  it.effect("steers an active provider turn with newly recorded prompts", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start working" }), resume: false })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const first = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(streamStarted)
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Change direction" }) })
      yield* Deferred.succeed(streamGate, undefined)
      yield* Fiber.join(first)
      streamGate = undefined
      streamStarted = undefined
      yield* Effect.yieldNow

      expect(requests).toHaveLength(2)
      expect(userTexts(requests[0]!)).toEqual(["Start working"])
      expect(userTexts(requests[1]!)).toEqual(["Start working", "Change direction"])
      expect((yield* session.context(sessionID)).map((message) => message.type)).toEqual([
        "user",
        "assistant",
        "user",
        "assistant",
      ])
    }),
  )

  it.effect("promotes queued input after continuation ends", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start working" }), resume: false })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-echo", name: "echo", input: { text: "hello" } }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const first = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(streamStarted)
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Wait until continuation ends" }),
        delivery: "queue",
      })
      yield* Deferred.succeed(streamGate, undefined)
      yield* Fiber.join(first)
      streamGate = undefined
      streamStarted = undefined

      expect(requests).toHaveLength(3)
      expect(userTexts(requests[0]!)).toEqual(["Start working"])
      expect(userTexts(requests[1]!)).toEqual(["Start working"])
      expect(userTexts(requests[2]!)).toEqual(["Start working", "Wait until continuation ends"])
    }),
  )

  it.effect("preserves durable queued input for a later wake after interruption", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const { db } = yield* Database.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Interrupt current work" }), resume: false })

      requests.length = 0
      responses = [
        [],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const run = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(streamStarted)
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Run after interrupt" }),
        delivery: "queue",
      })
      yield* session.interrupt(sessionID)
      expect(yield* Fiber.await(run)).toMatchObject({ _tag: "Failure" })
      expect(requests).toHaveLength(1)
      expect(yield* SessionInput.hasPending(db, sessionID, "queue")).toBe(true)
      const resumed = yield* session.resume(sessionID).pipe(Effect.forkChild)
      while (requests.length < 2) yield* Effect.yieldNow
      yield* Deferred.succeed(streamGate, undefined)
      yield* Fiber.join(resumed)
      streamGate = undefined
      streamStarted = undefined

      expect(requests).toHaveLength(2)
      expect(userTexts(requests[0]!)).toEqual(["Interrupt current work"])
      expect(userTexts(requests[1]!)).toEqual(["Interrupt current work", "Run after interrupt"])
    }),
  )

  it.effect("preserves durable steering input for a later resume after interruption", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const { db } = yield* Database.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Interrupt current work" }), resume: false })

      requests.length = 0
      responses = [
        [],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const run = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(streamStarted)
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Steer after interrupt" }),
      })
      yield* session.interrupt(sessionID)
      expect(yield* Fiber.await(run)).toMatchObject({ _tag: "Failure" })
      expect(requests).toHaveLength(1)
      expect(yield* SessionInput.hasPending(db, sessionID, "steer")).toBe(true)

      const resumed = yield* session.resume(sessionID).pipe(Effect.forkChild)
      while (requests.length < 2) yield* Effect.yieldNow
      yield* Deferred.succeed(streamGate, undefined)
      yield* Fiber.join(resumed)
      streamGate = undefined
      streamStarted = undefined

      expect(requests).toHaveLength(2)
      expect(userTexts(requests[0]!)).toEqual(["Interrupt current work"])
      expect(userTexts(requests[1]!)).toEqual(["Interrupt current work", "Steer after interrupt"])
    }),
  )

  it.effect("promotes queued inputs one at a time in FIFO order", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start working" }), resume: false })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const first = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(streamStarted)
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Queue first" }), delivery: "queue" })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Queue second" }), delivery: "queue" })
      yield* Deferred.succeed(streamGate, undefined)
      yield* Fiber.join(first)
      streamGate = undefined
      streamStarted = undefined

      expect(requests).toHaveLength(3)
      expect(userTexts(requests[0]!)).toEqual(["Start working"])
      expect(userTexts(requests[1]!)).toEqual(["Start working", "Queue first"])
      expect(userTexts(requests[2]!)).toEqual(["Start working", "Queue first", "Queue second"])
    }),
  )

  it.effect("promotes queued input after steering continuation ends", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start steering" }), resume: false })
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Queue for later" }),
        delivery: "queue",
        resume: false,
      })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      expect(userTexts(requests[0]!)).toEqual(["Start steering"])
      expect(userTexts(requests[1]!)).toEqual(["Start steering", "Queue for later"])
    }),
  )

  it.effect("promotes steers before the next queued input", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start working" }), resume: false })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]
      const firstGate = yield* Deferred.make<void>()
      const secondGate = yield* Deferred.make<void>()
      streamGate = firstGate

      const first = yield* session.resume(sessionID).pipe(Effect.forkChild)
      while (requests.length < 1) yield* Effect.yieldNow
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Queue first" }), delivery: "queue" })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Queue second" }), delivery: "queue" })
      streamGate = secondGate
      yield* Deferred.succeed(firstGate, undefined)
      while (requests.length < 2) yield* Effect.yieldNow
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Steer before next queued input" }) })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Also steer before next queued input" }) })
      yield* Deferred.succeed(secondGate, undefined)
      yield* Fiber.join(first)
      streamGate = undefined

      expect(requests).toHaveLength(4)
      expect(userTexts(requests[0]!)).toEqual(["Start working"])
      expect(userTexts(requests[1]!)).toEqual(["Start working", "Queue first"])
      expect(userTexts(requests[2]!)).toEqual([
        "Start working",
        "Queue first",
        "Steer before next queued input",
        "Also steer before next queued input",
      ])
      expect(userTexts(requests[3]!)).toEqual([
        "Start working",
        "Queue first",
        "Steer before next queued input",
        "Also steer before next queued input",
        "Queue second",
      ])
    }),
  )

  it.effect("coalesces multiple active steering prompts into one continuation turn", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start working" }), resume: false })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const first = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(streamStarted)
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "First steer" }) })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Second steer" }) })
      yield* Deferred.succeed(streamGate, undefined)
      yield* Fiber.join(first)
      streamGate = undefined
      streamStarted = undefined
      yield* Effect.yieldNow

      expect(requests).toHaveLength(2)
      expect(userTexts(requests[1]!)).toEqual(["Start working", "First steer", "Second steer"])
      yield* (yield* SessionExecution.Service).wake(sessionID)
      yield* Effect.yieldNow
      expect(requests).toHaveLength(2)
    }),
  )

  it.effect("runs steering input accepted while the active provider turn fails", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start working" }), resume: false })

      requests.length = 0
      responses = undefined
      response = []
      streamFailure = providerUnavailable()
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const first = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(streamStarted)
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Recover with this" }) })
      yield* Deferred.succeed(streamGate, undefined)
      expect(yield* Fiber.join(first).pipe(Effect.flip)).toBe(streamFailure)

      streamFailure = undefined
      streamGate = undefined
      streamStarted = undefined
      yield* Effect.yieldNow

      expect(requests).toHaveLength(2)
      expect(userTexts(requests[1]!)).toEqual(["Start working", "Recover with this"])
    }),
  )

  it.effect("durably fails local tools left running by a prior process before continuing", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Recover interrupted tool" }), resume: false })
      yield* SessionInput.promoteSteers((yield* Database.Service).db, events, sessionID, Number.MAX_SAFE_INTEGER)
      const assistantMessageID = SessionMessage.ID.create()
      yield* events.publish(SessionEvent.Step.Started, {
        sessionID,
        assistantMessageID,
        timestamp: yield* DateTime.now,
        agent: "build",
        model: { id: ModelV2.ID.make("fake-model"), providerID: ProviderV2.ID.make("fake") },
      })
      yield* events.publish(SessionEvent.Tool.Input.Started, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID,
        callID: "call-interrupted",
        name: "echo",
      })
      yield* events.publish(SessionEvent.Tool.Input.Ended, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID,
        callID: "call-interrupted",
        text: '{"text":"stale"}',
      })
      yield* events.publish(SessionEvent.Tool.Called, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID,
        callID: "call-interrupted",
        tool: "echo",
        input: { text: "stale" },
        provider: { executed: false },
      })
      requests.length = 0
      response = []
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(1)
      expect(requests[0]?.messages.map((message) => message.role)).toEqual(["user", "assistant", "tool"])
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Recover interrupted tool" },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "call-interrupted",
              state: { status: "error", error: { type: "unknown", message: "Tool execution interrupted" } },
            },
          ],
        },
      ])
    }),
  )

  it.effect("durably fails hosted tools left running by a prior process before continuing inline", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Recover interrupted hosted tool" }),
        resume: false,
      })
      yield* SessionInput.promoteSteers((yield* Database.Service).db, events, sessionID, Number.MAX_SAFE_INTEGER)
      const assistantMessageID = SessionMessage.ID.create()
      yield* events.publish(SessionEvent.Step.Started, {
        sessionID,
        assistantMessageID,
        timestamp: yield* DateTime.now,
        agent: "build",
        model: { id: ModelV2.ID.make("fake-model"), providerID: ProviderV2.ID.make("fake") },
      })
      yield* events.publish(SessionEvent.Tool.Input.Started, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID,
        callID: "call-hosted-interrupted",
        name: "web_search",
      })
      yield* events.publish(SessionEvent.Tool.Input.Ended, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID,
        callID: "call-hosted-interrupted",
        text: '{"query":"stale"}',
      })
      yield* events.publish(SessionEvent.Tool.Called, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID,
        callID: "call-hosted-interrupted",
        tool: "web_search",
        input: { query: "stale" },
        provider: { executed: true, metadata: { openai: { itemId: "call-hosted-interrupted" } } },
      })
      requests.length = 0
      response = []
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(1)
      expect(requests[0]?.messages.map((message) => message.role)).toEqual(["user", "assistant"])
      expect(requests[0]?.messages[1]?.content).toMatchObject([
        {
          type: "tool-call",
          id: "call-hosted-interrupted",
          providerExecuted: true,
          providerMetadata: { openai: { itemId: "call-hosted-interrupted" } },
        },
        { type: "tool-result", id: "call-hosted-interrupted", providerExecuted: true, result: { type: "error" } },
      ])
    }),
  )

  it.effect("durably fails pending tool input left by a prior process before continuing", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Recover interrupted tool input" }),
        resume: false,
      })
      yield* SessionInput.promoteSteers((yield* Database.Service).db, events, sessionID, Number.MAX_SAFE_INTEGER)
      const assistantMessageID = SessionMessage.ID.create()
      yield* events.publish(SessionEvent.Step.Started, {
        sessionID,
        assistantMessageID,
        timestamp: yield* DateTime.now,
        agent: "build",
        model: { id: ModelV2.ID.make("fake-model"), providerID: ProviderV2.ID.make("fake") },
      })
      yield* events.publish(SessionEvent.Tool.Input.Started, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID,
        callID: "call-pending-interrupted",
        name: "echo",
      })
      requests.length = 0
      response = []
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(1)
      expect(requests[0]?.messages.map((message) => message.role)).toEqual(["user", "assistant", "tool"])
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Recover interrupted tool input" },
        { type: "assistant", content: [{ type: "tool", id: "call-pending-interrupted", state: { status: "error" } }] },
      ])
    }),
  )

  it.effect("promotes the first queued input when woken while idle", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Wait in queue" }),
        delivery: "queue",
        resume: false,
      })

      requests.length = 0
      yield* (yield* SessionExecution.Service).wake(sessionID)
      yield* Effect.yieldNow

      expect(requests).toHaveLength(1)
      expect(userTexts(requests[0]!)).toEqual(["Wait in queue"])
    }),
  )

  it.effect("retries inbox input after prompt projection rolls back", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      const defect = new Error("fail after prompt promotion")
      let fail = true
      yield* events.project(SessionEvent.Prompted, () => (fail ? Effect.die(defect) : Effect.void))
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Recover promoted input" }), resume: false })

      expect(yield* session.resume(sessionID).pipe(Effect.catchDefect(Effect.succeed))).toBe(defect)
      fail = false
      requests.length = 0
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ]

      yield* (yield* SessionExecution.Service).wake(sessionID)
      while (requests.length === 0) yield* Effect.yieldNow

      expect(userTexts(requests[0]!)).toEqual(["Recover promoted input"])
    }),
  )

  it.effect("does not strand a committed promotion when a post-commit listener defects", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      yield* events.listen((event) =>
        event.type === SessionEvent.Prompted.type ? Effect.die("fail after prompt promotion commits") : Effect.void,
      )
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Run committed promotion" }),
        resume: false,
      })

      requests.length = 0
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(1)
      expect(userTexts(requests[0]!)).toEqual(["Run committed promotion"])
    }),
  )

  it.effect(
    "runs different sessions concurrently",
    () =>
      Effect.gen(function* () {
        yield* setup
        yield* insertSession(otherSessionID)
        const session = yield* SessionV2.Service
        yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Run first" }), resume: false })
        yield* session.prompt({ sessionID: otherSessionID, prompt: Prompt.make({ text: "Run second" }), resume: false })

        requests.length = 0
        responses = undefined
        response = []
        streamGate = yield* Deferred.make<void>()
        streamStarted = undefined
        const firstStarted = yield* Deferred.make<void>()
        const secondStarted = yield* Deferred.make<void>()
        streamStartAcks.push(firstStarted, secondStarted)

        const first = yield* session.resume(sessionID).pipe(Effect.forkChild)
        yield* awaitStreamStart(firstStarted)
        const second = yield* session.resume(otherSessionID).pipe(Effect.forkChild)
        yield* awaitStreamStart(secondStarted)

        expect(requests).toHaveLength(2)
        expect(requests.map((request) => request.providerOptions?.openai?.promptCacheKey)).toEqual([
          sessionID,
          otherSessionID,
        ])
        yield* Deferred.succeed(streamGate, undefined)
        yield* Fiber.join(first)
        yield* Fiber.join(second)
        streamGate = undefined
        streamStarted = undefined
      }),
    { timeout: 10_000 },
  )

  it.effect("adds session correlation headers to model requests", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Run correlated request" }), resume: false })

      requests.length = 0
      yield* session.resume(sessionID)

      expect(requests[0]?.http?.headers).toEqual({
        "x-session-affinity": sessionID,
        "X-Session-Id": sessionID,
      })
    }),
  )

  it.effect("adds the parent session header to child model requests", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const parentID = SessionV2.ID.make("ses_runner_parent")
      const { db } = yield* Database.Service
      yield* db
        .update(SessionTable)
        .set({ parent_id: parentID })
        .where(eq(SessionTable.id, sessionID))
        .run()
        .pipe(Effect.orDie)
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Run child request" }), resume: false })

      requests.length = 0
      yield* session.resume(sessionID)

      expect(requests[0]?.http?.headers?.["x-parent-session-id"]).toBe(parentID)
    }),
  )

  it.effect("bounds 64-character session prompt cache keys", () =>
    Effect.gen(function* () {
      yield* setup
      const longSessionID = SessionV2.ID.make(`ses_${"a".repeat(64)}`)
      const otherLongSessionID = SessionV2.ID.make(`ses_${"b".repeat(64)}`)
      yield* insertSession(longSessionID)
      yield* insertSession(otherLongSessionID)
      const session = yield* SessionV2.Service
      yield* session.prompt({
        sessionID: longSessionID,
        prompt: Prompt.make({ text: "Run long session" }),
        resume: false,
      })
      yield* session.prompt({
        sessionID: otherLongSessionID,
        prompt: Prompt.make({ text: "Run other long session" }),
        resume: false,
      })

      requests.length = 0
      yield* session.resume(longSessionID)
      yield* session.resume(otherLongSessionID)

      const keys = requests.map((request) => request.providerOptions?.openai?.promptCacheKey)
      expect(keys).toEqual([longSessionID.slice(4), otherLongSessionID.slice(4)])
      expect(keys.every((key) => typeof key === "string" && key.length === 64)).toBe(true)
      expect(keys[0]).not.toBe(keys[1])
    }),
  )

  it.effect("fans out one failed run and allows a later retry", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Retry after failure" }), resume: false })

      requests.length = 0
      responses = undefined
      response = []
      streamFailure = providerUnavailable()
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const first = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(streamStarted)
      const second = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Effect.yieldNow

      expect(requests).toHaveLength(1)
      yield* Deferred.succeed(streamGate, undefined)
      const [firstExit, secondExit] = yield* Effect.all([Fiber.await(first), Fiber.await(second)])
      expect(secondExit).toEqual(firstExit)

      streamFailure = undefined
      streamGate = undefined
      streamStarted = undefined
      yield* session.resume(sessionID)
      expect(requests).toHaveLength(2)
    }),
  )

  it.effect("durably settles local tool failures before continuing", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Call missing" }), resume: false })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-missing", name: "missing", input: {} }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.textStart({ id: "text-after-error" }),
          LLMEvent.textDelta({ id: "text-after-error", text: "Recovered" }),
          LLMEvent.textEnd({ id: "text-after-error" }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]
      streamGate = undefined
      streamStarted = undefined

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Call missing" },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "call-missing",
              state: { status: "error", error: { message: "Unknown tool: missing" } },
            },
          ],
        },
        { type: "assistant", finish: "stop", content: [{ type: "text", id: "text-after-error", text: "Recovered" }] },
      ])
    }),
  )

  it.effect("returns unexpected local tool defects to the model and continues", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Call defect" }), resume: false })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-defect", name: "defect", input: {} }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.textStart({ id: "text-after-defect" }),
          LLMEvent.textDelta({ id: "text-after-defect", text: "Recovered" }),
          LLMEvent.textEnd({ id: "text-after-defect" }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      expect(requests[1]?.messages.map((message) => message.role)).toEqual(["user", "assistant", "tool"])
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Call defect" },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "call-defect",
              state: {
                status: "error",
                error: { type: "unknown", message: "Tool execution failed: unexpected tool defect" },
              },
            },
          ],
        },
        { type: "assistant", finish: "stop", content: [{ type: "text", text: "Recovered" }] },
      ])
    }),
  )

  it.effect("returns policy-blocked tools to the model and continues", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const registry = yield* ToolRegistry.Service
      yield* registry.register({
        blocked: Tool.make({
          description: "Fail because policy blocked execution",
          input: Schema.Struct({}),
          output: Schema.Struct({}),
          execute: () =>
            Effect.fail(new PermissionV2.BlockedError({ rules: [] })).pipe(
              Effect.mapError(() => new Tool.Failure({ message: "Permission blocked" })),
            ),
        }),
      })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Call blocked" }), resume: false })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-blocked", name: "blocked", input: {} }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Call blocked" },
        {
          type: "assistant",
          content: [
            { type: "tool", id: "call-blocked", state: { status: "error", error: { message: "Permission blocked" } } },
          ],
        },
        { type: "assistant", finish: "stop" },
      ])
    }),
  )

  it.effect("interrupts runner continuation when permission approval is declined", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const registry = yield* ToolRegistry.Service
      yield* registry.register({
        declined: Tool.make({
          description: "Fail because the user declined approval",
          input: Schema.Struct({}),
          output: Schema.Struct({}),
          execute: () => Effect.die(new PermissionV2.DeclinedError()),
        }),
      })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Call declined" }), resume: false })

      requests.length = 0
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolCall({ id: "call-declined", name: "declined", input: {} }),
        LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
        LLMEvent.finish({ reason: "tool-calls" }),
      ]

      const exit = yield* session.resume(sessionID).pipe(Effect.exit)

      expect(exit._tag).toBe("Failure")
      if (exit._tag === "Failure") expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true)
      expect(requests).toHaveLength(1)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Call declined" },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "call-declined",
              state: { status: "error", error: { message: "Tool execution interrupted" } },
            },
          ],
        },
      ])
    }),
  )

  it.effect("returns permission corrections to the model and continues", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const registry = yield* ToolRegistry.Service
      yield* registry.register({
        corrected: Tool.make({
          description: "Fail with user correction feedback",
          input: Schema.Struct({}),
          output: Schema.Struct({}),
          execute: () =>
            Effect.fail(new PermissionV2.CorrectedError({ feedback: "Use another tool" })).pipe(
              Effect.mapError(() => new Tool.Failure({ message: "Use another tool" })),
            ),
        }),
      })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Call corrected" }), resume: false })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-corrected", name: "corrected", input: {} }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Call corrected" },
        {
          type: "assistant",
          content: [
            { type: "tool", id: "call-corrected", state: { status: "error", error: { message: "Use another tool" } } },
          ],
        },
        { type: "assistant", finish: "stop" },
      ])
    }),
  )

  it.effect("interrupts runner continuation when a question is dismissed", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const registry = yield* ToolRegistry.Service
      const questions = yield* QuestionV2.Service
      yield* registry.register({
        question: Tool.make({
          description: "Ask the user",
          input: Schema.Struct({}),
          output: Schema.Struct({}),
          execute: (_, context) =>
            questions.ask({ sessionID: context.sessionID, questions: [] }).pipe(Effect.as({}), Effect.orDie),
        }),
      })
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Ask then stop" }), resume: false })

      requests.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-question", name: "question", input: {} }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [],
      ]

      const run = yield* session.resume(sessionID).pipe(Effect.exit, Effect.forkChild)
      let pending = yield* questions.list()
      while (pending.length === 0) {
        yield* Effect.yieldNow
        pending = yield* questions.list()
      }
      yield* questions.reject(pending[0]!.id)
      const exit = yield* Fiber.join(run)

      expect(exit._tag).toBe("Failure")
      if (exit._tag === "Failure") expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true)
      expect(requests).toHaveLength(1)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Ask then stop" },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "call-question",
              state: { status: "error", error: { type: "unknown", message: "Tool execution interrupted" } },
            },
          ],
        },
      ])
    }),
  )

  it.effect("awaits started local tools before surfacing provider stream failure", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Settle before failing" }), resume: false })
      const failure = providerUnavailable()
      toolExecutionGate = yield* Deferred.make<void>()
      responseStream = Stream.concat(
        Stream.fromIterable([
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-before-failure", name: "echo", input: { text: "settle" } }),
        ]),
        Stream.fail(failure),
      )

      const run = yield* session.resume(sessionID).pipe(Effect.forkChild)
      while (executions.length === 0) yield* Effect.yieldNow
      yield* Effect.yieldNow
      yield* Deferred.succeed(toolExecutionGate, undefined)
      expect(yield* Fiber.join(run).pipe(Effect.flip)).toBe(failure)
      toolExecutionGate = undefined

      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Settle before failing" },
        {
          type: "assistant",
          content: [
            { type: "tool", id: "call-before-failure", state: { status: "completed", structured: { text: "settle" } } },
          ],
        },
      ])
    }),
  )

  it.effect("durably fails blocked local tools when a provider turn is interrupted", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Interrupt blocked tool" }), resume: false })
      executions.length = 0
      toolExecutionGate = yield* Deferred.make<void>()
      responseStream = Stream.concat(
        Stream.fromIterable([
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-before-interrupt", name: "echo", input: { text: "blocked" } }),
        ]),
        Stream.never,
      )

      const run = yield* session.resume(sessionID).pipe(Effect.forkChild)
      while (executions.length === 0) yield* Effect.yieldNow
      yield* session.interrupt(sessionID)
      toolExecutionGate = undefined

      expect(yield* Fiber.await(run)).toMatchObject({ _tag: "Failure" })
      yield* session.interrupt(sessionID)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Interrupt blocked tool" },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "call-before-interrupt",
              state: { status: "error", error: { type: "unknown", message: "Tool execution interrupted" } },
            },
          ],
        },
      ])

      yield* replaySessionProjection(sessionID)

      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Interrupt blocked tool" },
        { type: "assistant", content: [{ type: "tool", id: "call-before-interrupt", state: { status: "error" } }] },
      ])
      requests.length = 0
      responseStream = undefined
      response = []
      yield* session.resume(sessionID)
      expect(requests[0]?.messages.map((message) => message.role)).toEqual(["user", "assistant", "tool"])
    }),
  )

  it.effect("interrupts a blocked provider turn without local tool execution", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Interrupt provider" }), resume: false })
      requests.length = 0
      response = []
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const run = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(streamStarted)
      yield* session.interrupt(sessionID)
      const exit = yield* Fiber.await(run)
      streamGate = undefined
      streamStarted = undefined

      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBeTrue()
      expect(requests).toHaveLength(1)
      yield* session.interrupt(sessionID)
    }),
  )

  it.effect("durably fails blocked local tools when interrupted while awaiting settlement", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Interrupt tool settlement" }), resume: false })
      executions.length = 0
      toolExecutionGate = yield* Deferred.make<void>()
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolCall({ id: "call-await-interrupt", name: "echo", input: { text: "blocked" } }),
        LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
        LLMEvent.finish({ reason: "tool-calls" }),
      ]

      const runner = yield* SessionRunner.Service
      const run = yield* runner.run({ sessionID, force: true }).pipe(Effect.forkChild)
      while (executions.length === 0) yield* Effect.yieldNow
      yield* Fiber.interrupt(run)
      toolExecutionGate = undefined

      expect(yield* Fiber.await(run)).toMatchObject({ _tag: "Failure" })
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Interrupt tool settlement" },
        {
          type: "assistant",
          content: [
            {
              type: "tool",
              id: "call-await-interrupt",
              state: { status: "error", error: { type: "unknown", message: "Tool execution interrupted" } },
            },
          ],
        },
      ])
    }),
  )

  it.effect("forces a text response on an agent's configured final step", () =>
    Effect.gen(function* () {
      yield* setup
      const agents = yield* AgentV2.Service
      yield* agents.transform((editor) =>
        editor.update(AgentV2.ID.make("build"), (agent) => {
          agent.steps = 2
        }),
      )
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Finish at the limit" }), resume: false })

      requests.length = 0
      executions.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-terminal", name: "echo", input: { text: "done" } }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-forbidden", name: "echo", input: { text: "forbidden" } }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
      ]

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(2)
      expect(requests[0]?.toolChoice).toBeUndefined()
      expect(requests[1]?.toolChoice).toMatchObject({ type: "none" })
      expect(requests[1]?.tools).toEqual([])
      expect(requests[1]?.messages.at(-1)).toMatchObject({
        role: "assistant",
        content: [{ type: "text", text: expect.stringContaining("MAXIMUM STEPS REACHED") }],
      })
      expect(executions).toEqual(["done"])
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Finish at the limit" },
        { type: "assistant", content: [{ type: "tool", id: "call-terminal", state: { status: "completed" } }] },
        { type: "assistant", content: [{ type: "tool", id: "call-forbidden", state: { status: "error" } }] },
      ])
    }),
  )

  it.effect("resets the configured step allowance when steering input promotes", () =>
    Effect.gen(function* () {
      yield* setup
      const agents = yield* AgentV2.Service
      yield* agents.transform((editor) =>
        editor.update(AgentV2.ID.make("build"), (agent) => {
          agent.steps = 2
        }),
      )
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Start work" }), resume: false })

      requests.length = 0
      executions.length = 0
      responses = [
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-before-steer", name: "echo", input: { text: "before" } }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "call-after-steer", name: "echo", input: { text: "after" } }),
          LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ],
        [
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ],
      ]
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const run = yield* session.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(streamStarted)
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Change direction" }) })
      yield* Deferred.succeed(streamGate, undefined)
      yield* Fiber.join(run)
      streamGate = undefined
      streamStarted = undefined

      expect(requests).toHaveLength(3)
      expect(requests[1]?.toolChoice).toBeUndefined()
      expect(requests[1]?.tools).not.toEqual([])
      expect(requests[2]?.toolChoice).toMatchObject({ type: "none" })
      expect(executions).toEqual(["before", "after"])
    }),
  )

  it.effect("projects provider errors as terminal assistant step failures", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail durably" }), resume: false })

      requests.length = 0
      responses = undefined
      streamGate = undefined
      streamStarted = undefined
      response = [LLMEvent.stepStart({ index: 0 }), LLMEvent.providerError({ message: "Provider unavailable" })]

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(1)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Fail durably" },
        { type: "assistant", finish: "error", error: { type: "unknown", message: "Provider unavailable" } },
      ])
    }),
  )

  it.effect("projects provider errors emitted before assistant step start", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail before step" }), resume: false })

      requests.length = 0
      response = [LLMEvent.providerError({ message: "Provider unavailable" })]

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(1)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Fail before step" },
        { type: "assistant", finish: "error", error: { type: "unknown", message: "Provider unavailable" } },
      ])
    }),
  )

  it.effect("does not recover context overflow after durable assistant output", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail after output" }), resume: false })

      requests.length = 0
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.textStart({ id: "text-partial" }),
        LLMEvent.textDelta({ id: "text-partial", text: "Partial" }),
        LLMEvent.textEnd({ id: "text-partial" }),
        LLMEvent.providerError({ message: "prompt too long", classification: "context-overflow" }),
      ]
      yield* session.resume(sessionID)

      expect(requests).toHaveLength(1)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Fail after output" },
        {
          type: "assistant",
          finish: "error",
          error: { message: "prompt too long" },
          content: [{ type: "text", text: "Partial" }],
        },
      ])
    }),
  )

  it.effect("projects raw provider stream failures as terminal assistant step failures", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail raw stream durably" }), resume: false })
      const failure = providerUnavailable()
      responseStream = Stream.fail(failure)

      expect(yield* session.resume(sessionID).pipe(Effect.flip)).toBe(failure)
      yield* replaySessionProjection(sessionID)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Fail raw stream durably" },
        { type: "assistant", finish: "error", error: { type: "unknown", message: "Provider unavailable" } },
      ])
    }),
  )

  it.effect("does not continue automatically after a provider error follows a local tool call", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Do not continue failed provider" }),
        resume: false,
      })

      requests.length = 0
      const executionCount = executions.length
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolCall({ id: "call-before-provider-error", name: "echo", input: { text: "settled" } }),
        LLMEvent.providerError({ message: "Provider unavailable" }),
      ]

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(1)
      expect(executions.slice(executionCount)).toEqual(["settled"])
    }),
  )

  it.effect("durably fails a hosted tool when its provider errors before returning a result", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail hosted tool durably" }), resume: false })

      requests.length = 0
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolCall({
          id: "call-hosted-provider-error",
          name: "web_search",
          input: { query: "effect" },
          providerExecuted: true,
        }),
        LLMEvent.providerError({ message: "Provider unavailable" }),
      ]

      yield* session.resume(sessionID)

      expect(requests).toHaveLength(1)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Fail hosted tool durably" },
        {
          type: "assistant",
          content: [{ type: "tool", id: "call-hosted-provider-error", state: { status: "error" } }],
        },
      ])
    }),
  )

  it.effect("durably fails a hosted tool left unresolved at normal provider EOF", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Fail hosted tool at EOF" }), resume: false })
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolCall({
          id: "call-hosted-eof",
          name: "web_search",
          input: { query: "effect" },
          providerExecuted: true,
        }),
      ]

      yield* session.resume(sessionID)
      yield* replaySessionProjection(sessionID)

      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Fail hosted tool at EOF" },
        { type: "assistant", content: [{ type: "tool", id: "call-hosted-eof", state: { status: "error" } }] },
      ])
    }),
  )

  it.effect("durably fails a hosted tool left unresolved by a raw provider stream failure", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({
        sessionID,
        prompt: Prompt.make({ text: "Fail hosted tool on raw failure" }),
        resume: false,
      })
      const failure = providerUnavailable()
      responseStream = Stream.concat(
        Stream.fromIterable([
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({
            id: "call-hosted-raw-failure",
            name: "web_search",
            input: { query: "effect" },
            providerExecuted: true,
          }),
        ]),
        Stream.fail(failure),
      )

      expect(yield* session.resume(sessionID).pipe(Effect.flip)).toBe(failure)
      yield* replaySessionProjection(sessionID)
      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Fail hosted tool on raw failure" },
        {
          type: "assistant",
          finish: "error",
          error: { type: "unknown", message: "Provider unavailable" },
          content: [{ type: "tool", id: "call-hosted-raw-failure", state: { status: "error" } }],
        },
      ])
    }),
  )

  it.effect("keeps interleaved assistant text blocks separate", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Two blocks" }), resume: false })

      responses = undefined
      streamGate = undefined
      streamStarted = undefined
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.textStart({ id: "text-1" }),
        LLMEvent.textStart({ id: "text-2" }),
        LLMEvent.textDelta({ id: "text-1", text: "First" }),
        LLMEvent.textDelta({ id: "text-2", text: "Second" }),
        LLMEvent.textEnd({ id: "text-1" }),
        LLMEvent.textEnd({ id: "text-2" }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ]

      yield* session.resume(sessionID)

      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Two blocks" },
        {
          type: "assistant",
          content: [
            { type: "text", id: "text-1", text: "First" },
            { type: "text", id: "text-2", text: "Second" },
          ],
        },
      ])
    }),
  )

  for (const kind of fragmentKinds) {
    it.effect(`broadcasts provider ${kind} deltas without storing projection rewrites`, () =>
      verifyEphemeralDeltas(kind),
    )

    it.effect(`durably closes partial ${kind} when the provider stream fails`, () => verifyPartialFlushOnFailure(kind))

    it.effect(`durably closes partial ${kind} when the provider stream is interrupted`, () =>
      verifyPartialFlushOnInterruption(kind),
    )
  }

  it.effect("rejects duplicate streamed text starts", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      responses = undefined
      streamGate = undefined
      streamStarted = undefined
      response = [LLMEvent.textStart({ id: "text-1" }), LLMEvent.textStart({ id: "text-1" })]

      expect(yield* session.resume(sessionID).pipe(Effect.catchDefect(Effect.succeed))).toBe(
        "Duplicate text start: text-1",
      )
    }),
  )

  it.effect("transitions streamed raw tool input to parsed called input", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      yield* session.prompt({ sessionID, prompt: Prompt.make({ text: "Call provider tool" }), resume: false })

      responses = undefined
      streamGate = undefined
      streamStarted = undefined
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-parsed", name: "web_search" }),
        LLMEvent.toolInputDelta({ id: "call-parsed", name: "web_search", text: '{"query":"hello"}' }),
        LLMEvent.toolInputEnd({ id: "call-parsed", name: "web_search" }),
        LLMEvent.toolCall({ id: "call-parsed", name: "web_search", input: { query: "hello" }, providerExecuted: true }),
      ]

      yield* session.resume(sessionID)

      expect(yield* session.context(sessionID)).toMatchObject([
        { type: "user", text: "Call provider tool" },
        {
          type: "assistant",
          content: [{ type: "tool", id: "call-parsed", state: { status: "error", input: { query: "hello" } } }],
        },
      ])
    }),
  )

  it.effect("rejects malformed streamed tool input ordering", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      responses = undefined
      streamGate = undefined
      streamStarted = undefined
      response = [LLMEvent.toolInputDelta({ id: "call-1", name: "read", text: "{}" })]

      expect(yield* session.resume(sessionID).pipe(Effect.catchDefect(Effect.succeed))).toBe(
        "Tool input delta before start: call-1",
      )
    }),
  )

  it.effect("fails a held provider turn whose Session is removed before it records output", () =>
    Effect.gen(function* () {
      yield* setup
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      const removedID = SessionV2.ID.make("ses_runner_removed")
      yield* insertSession(removedID)
      yield* session.prompt({ sessionID: removedID, prompt: Prompt.make({ text: "Work" }), resume: false })
      requests.length = 0
      response = [
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.textStart({ id: "text-late" }),
        LLMEvent.textDelta({ id: "text-late", text: "Late" }),
        LLMEvent.textEnd({ id: "text-late" }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ]
      streamGate = yield* Deferred.make<void>()
      streamStarted = yield* Deferred.make<void>()

      const turn = yield* session.resume(removedID).pipe(Effect.exit, Effect.forkChild)
      yield* Deferred.await(streamStarted)
      // The two durable steps of V1 Session.remove, which lives in the opencode package: project the deletion, then
      // drop the aggregate.
      yield* events.publish(SessionV1.Event.Deleted, {
        sessionID: removedID,
        info: {
          id: removedID,
          slug: removedID,
          projectID: Project.ID.global,
          directory: "/project",
          title: "test",
          version: "test",
          time: { created: 0, updated: 0 },
        },
      })
      yield* events.remove(removedID)
      yield* Deferred.succeed(streamGate, undefined)
      const exit = yield* Fiber.join(turn)
      streamGate = undefined
      streamStarted = undefined

      expect(requests).toHaveLength(1)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionProjector.SessionNotProjected)
      expect(yield* EventV2.latestSequence(db, removedID)).toBe(-1)
      expect(yield* db.select().from(EventTable).where(eq(EventTable.aggregate_id, removedID)).all()).toEqual([])
      expect(
        yield* db.select().from(SessionMessageTable).where(eq(SessionMessageTable.session_id, removedID)).all(),
      ).toEqual([])
    }),
  )
})
