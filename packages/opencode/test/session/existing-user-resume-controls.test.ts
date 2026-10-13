import { expect } from "bun:test"
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { BackgroundJob } from "@/background/job"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { SessionSummary } from "@/session/summary"
import { SessionQueue } from "@/session/queue"
import { SessionProcessor } from "@/session/processor"
import { MessageV2 } from "@/session/message-v2"
import { Provider } from "@/provider/provider"
import { Agent } from "@/agent/agent"
import { MessageID, PartID } from "@/session/schema"
import { TestInstance } from "../fixture/fixture"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"
import { reply, TestLLMServer } from "../lib/llm-server"

// These controls describe the pinned producer, not a proposed resume implementation.
class OwnershipProbe extends Context.Service<
  OwnershipProbe,
  {
    ensures: number
    starts: number
    preflight?: { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> }
  }
>()("test/ExistingUserOwnershipProbe") {}

const probeNode = LayerNode.make({
  service: OwnershipProbe,
  layer: Layer.effect(
    OwnershipProbe,
    Effect.sync(() => ({ ensures: 0, starts: 0 })),
  ),
  deps: [],
})
const implementation = SessionRunState.node.implementation
if (!implementation) throw new Error("SessionRunState implementation is missing")
const observedRunState = LayerNode.make({
  service: SessionRunState.Service,
  layer: Layer.effect(
    SessionRunState.Service,
    Effect.gen(function* () {
      const real = yield* SessionRunState.Service
      const probe = yield* OwnershipProbe
      return SessionRunState.Service.of({
        ...real,
        assertNotBusy: (sessionID) =>
          Effect.gen(function* () {
            yield* real.assertNotBusy(sessionID)
            const gate = probe.preflight
            probe.preflight = undefined
            if (!gate) return
            yield* Deferred.succeed(gate.entered, undefined)
            yield* Deferred.await(gate.release)
          }),
        ensureRunning: (sessionID, onInterrupt, work) =>
          Effect.gen(function* () {
            probe.ensures++
            return yield* real.ensureRunning(
              sessionID,
              onInterrupt,
              Effect.sync(() => probe.starts++).pipe(Effect.andThen(work)),
            )
          }),
      })
    }),
  ).pipe(
    Layer.provide(
      implementation as Layer.Layer<SessionRunState.Service, never, BackgroundJob.Service | SessionStatus.Service>,
    ),
  ),
  deps: [BackgroundJob.node, SessionStatus.node, probeNode],
})
const it = testEffect(
  LayerNode.compile(
    LayerNode.group([
      SessionPrompt.node,
      Session.node,
      SessionProjector.node,
      SessionQueue.node,
      SessionRunState.node,
      Database.node,
      FSUtil.node,
      Provider.node,
      Agent.node,
      SessionProcessor.node,
      probeNode,
      LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] }),
    ]),
    [
      [SessionRunState.node, observedRunState],
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true })],
      [
        SessionSummary.node,
        Layer.mock(SessionSummary.Service, {
          summarize: () => Effect.void,
          diff: () => Effect.succeed([]),
          computeDiff: () => Effect.succeed([]),
        }),
      ],
    ],
  ),
)
const model = { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") }
const setup = Effect.fn("control.setup")(function* () {
  const test = yield* TestInstance
  const llm = yield* TestLLMServer
  const fs = yield* FSUtil.Service
  yield* fs.writeWithDirs(
    `${test.directory}/opencode.json`,
    JSON.stringify({
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
              tool_call: true,
              limit: { context: 100000, output: 10000 },
            },
          },
          options: { apiKey: "synthetic-test-key", baseURL: llm.url },
        },
      },
    }),
  )
  return llm
})

// A real durable native admission/promotion, then a synthetic crash-shaped assistant.
// No prompt route or tool is replayed to construct the retained witness.
const retained = Effect.fn("control.retained")(function* (text: string, existing?: Session.Info) {
  const sessions = yield* Session.Service
  const queue = yield* SessionQueue.Service
  const test = yield* TestInstance
  const session =
    existing ??
    (yield* sessions.create({ title: "Pinned", permission: [{ permission: "*", pattern: "*", action: "allow" }] }))
  const messageID = MessageID.ascending()
  const original = yield* queue.admit({
    sessionID: session.id,
    messageID,
    delivery: "queue",
    agent: "build",
    model,
    parts: [{ type: "text", text }],
  })
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
          agent: "build",
          model,
          time: { created: original.time.created },
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
  yield* queue.consume(session.id)
  const history = yield* sessions.messages({ sessionID: session.id })
  const user = history.find((message) => message.info.id === messageID)
  if (!user || user.info.role !== "user") throw new Error("original admission was not projected")
  const assistant = yield* sessions.updateMessage({
    id: MessageID.ascending(),
    sessionID: session.id,
    parentID: user.info.id,
    role: "assistant",
    agent: "build",
    mode: "build",
    modelID: model.modelID,
    providerID: model.providerID,
    path: { cwd: test.directory, root: test.directory },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: original.time.created },
  })
  return { session, user: { ...user, info: user.info }, assistant }
})

const join = <A, E>(task: Fiber.Fiber<A, E>) =>
  awaitWithTimeout(Fiber.join(task), "original execution owner did not settle", "10 seconds")

it.instance(
  "resume control: retained user bypasses message-history-empty return and creates another assistant",
  () =>
    Effect.gen(function* () {
      const llm = yield* setup()
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const state = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const probe = yield* OwnershipProbe
      const input = yield* retained("original retained request")
      expect(yield* queue.list(input.session.id)).toEqual([])
      expect(yield* queue.empty(input.session.id)).toBe(false)
      yield* state.assertNotBusy(input.session.id)
      yield* llm.text("continued original request")
      const result = yield* prompt.loop({ sessionID: input.session.id, messageID: input.user.info.id })
      expect(probe.ensures).toBe(1)
      expect(probe.starts).toBe(1)
      expect(yield* llm.calls).toBe(1)
      expect(result.info.id).not.toBe(input.assistant.id)
      expect(result.info.role === "assistant" && result.info.parentID).toBe(input.user.info.id)
      const history = yield* sessions.messages({ sessionID: input.session.id })
      expect(history.find((message) => message.info.id === input.assistant.id)).toEqual({
        info: input.assistant,
        parts: [],
      })
      expect(history.filter((message) => message.info.role === "user")).toEqual([input.user])
      expect(history.filter((message) => message.info.role === "assistant")).toHaveLength(2)
    }),
  20_000,
)

it.instance(
  "resume control: truly empty history returns before the execution owner and has no assistant",
  () =>
    Effect.gen(function* () {
      const llm = yield* setup()
      const sessions = yield* Session.Service
      const prompt = yield* SessionPrompt.Service
      const queue = yield* SessionQueue.Service
      const probe = yield* OwnershipProbe
      const session = yield* sessions.create({ title: "Pinned" })
      expect(yield* queue.empty(session.id)).toBe(true)
      const result = yield* Effect.exit(prompt.loop({ sessionID: session.id }))
      expect(Exit.isFailure(result)).toBe(true)
      if (Exit.isFailure(result)) expect(Cause.pretty(result.cause)).toContain("Impossible")
      expect(probe.ensures).toBe(0)
      expect(probe.starts).toBe(0)
      expect(yield* llm.calls).toBe(0)
    }),
  20_000,
)

it.instance(
  "resume control: duplicate while running joins one owner; unchanged terminal root adds no call",
  () =>
    Effect.gen(function* () {
      const llm = yield* setup()
      const prompt = yield* SessionPrompt.Service
      const probe = yield* OwnershipProbe
      const input = yield* retained("duplicate retained request")
      const release = Promise.withResolvers<void>()
      yield* llm.push(reply().wait(release.promise).text("one result").stop().item())
      yield* Effect.gen(function* () {
        const first = yield* prompt
          .loop({ sessionID: input.session.id, messageID: input.user.info.id })
          .pipe(Effect.forkChild)
        yield* awaitWithTimeout(llm.wait(1), "original provider request was not reached", "10 seconds")
        const duplicate = yield* prompt
          .loop({ sessionID: input.session.id, messageID: input.user.info.id })
          .pipe(Effect.forkChild)
        yield* pollWithTimeout(
          Effect.sync(() => (probe.ensures === 2 ? true : undefined)),
          "duplicate never reached the actual runner",
        )
        expect(probe.starts).toBe(1)
        yield* Effect.sync(() => release.resolve())
        const one = yield* join(first)
        const two = yield* join(duplicate)
        expect(one.info.id).toBe(two.info.id)
        const terminal = yield* prompt.loop({ sessionID: input.session.id, messageID: input.user.info.id })
        expect(terminal.info.id).toBe(one.info.id)
        expect(yield* llm.calls).toBe(1)
      }).pipe(Effect.ensuring(Effect.sync(() => release.resolve())))
    }),
  20_000,
)

it.instance(
  "resume control: busy transition after idle check accepts foreign root by joining unrelated owner",
  () =>
    Effect.gen(function* () {
      const llm = yield* setup()
      const prompt = yield* SessionPrompt.Service
      const state = yield* SessionRunState.Service
      const probe = yield* OwnershipProbe
      const input = yield* retained("local retained request")
      const foreign = yield* retained("foreign retained request")
      const gate = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      const running = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
      probe.preflight = gate
      const candidate = yield* prompt
        .loop({ sessionID: input.session.id, messageID: foreign.user.info.id })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(gate.entered), "idle preflight was not reached")
      const other = yield* state
        .ensureRunning(
          input.session.id,
          Effect.succeed({ info: input.assistant, parts: [] }),
          Deferred.succeed(running.entered, undefined).pipe(
            Effect.andThen(Deferred.await(running.release)),
            Effect.as({ info: input.assistant, parts: [] }),
          ),
        )
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(running.entered), "competing owner did not claim runner")
      yield* Deferred.succeed(gate.release, undefined)
      yield* pollWithTimeout(
        Effect.sync(() => (probe.ensures === 2 ? true : undefined)),
        "candidate never reached busy runner",
      )
      expect(probe.starts).toBe(1)
      expect((yield* state.current(input.session.id))?._tag).toBe("Running")
      yield* Deferred.succeed(running.release, undefined)
      const result = yield* join(candidate)
      yield* join(other)
      expect(result.info.id).toBe(input.assistant.id)
      expect(yield* llm.calls).toBe(0)
    }),
  20_000,
)

it.instance(
  "resume control: idle foreign root fails before provider work, but owner is already started",
  () =>
    Effect.gen(function* () {
      const llm = yield* setup()
      const prompt = yield* SessionPrompt.Service
      const probe = yield* OwnershipProbe
      const input = yield* retained("local original")
      const foreign = yield* retained("other session original")
      const result = yield* Effect.exit(prompt.loop({ sessionID: input.session.id, messageID: foreign.user.info.id }))
      expect(Exit.isFailure(result)).toBe(true)
      if (Exit.isFailure(result)) expect(Cause.pretty(result.cause)).toContain("Run root message not found")
      expect(probe.starts).toBe(1)
      expect(yield* llm.calls).toBe(0)
    }),
  20_000,
)

it.instance(
  "resume control: stale same-session root starts old work and then drains the newer root",
  () =>
    Effect.gen(function* () {
      const llm = yield* setup()
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const input = yield* retained("stale original user")
      const latest = yield* retained("later admitted user", input.session)
      yield* llm.push(reply().text("old result").stop().item(), reply().text("later result").stop().item())
      const result = yield* prompt.loop({ sessionID: input.session.id, messageID: input.user.info.id })
      expect(yield* llm.calls).toBe(2)
      const requests = yield* llm.inputs
      expect(JSON.stringify(requests[0]?.messages)).toContain("stale original user")
      expect(JSON.stringify(requests[0]?.messages)).not.toContain("later admitted user")
      expect(result.info.role === "assistant" && result.info.parentID).toBe(latest.user.info.id)
      expect(
        (yield* sessions.messages({ sessionID: input.session.id })).filter((message) => message.info.role === "user"),
      ).toHaveLength(2)
    }),
  20_000,
)

it.instance(
  "resume control: retained running reads have no processor adoption and become synthetic interrupted context",
  () =>
    Effect.gen(function* () {
      const llm = yield* setup()
      const sessions = yield* Session.Service
      const provider = yield* Provider.Service
      const processor = yield* SessionProcessor.Service
      const input = yield* retained("four unresolved read obligations")
      const originals = yield* Effect.forEach(["blind", "exchange", "final", "report"], (name) =>
        sessions.updatePart({
          id: PartID.ascending(),
          sessionID: input.session.id,
          messageID: input.assistant.id,
          type: "tool",
          tool: "read",
          callID: `historical_${name}`,
          state: {
            status: "running",
            input: { filePath: `/synthetic/retained/${name}.md` },
            time: { start: input.assistant.time.created },
          },
        }),
      )
      const resolved = yield* provider.getModel(model.providerID, model.modelID)
      const handle = yield* processor.create({
        assistantMessage: input.assistant,
        sessionID: input.session.id,
        model: resolved,
      })
      expect(yield* handle.updateToolCall(originals[0]!.callID, (part) => part)).toBeUndefined()
      yield* handle.completeToolCall(originals[0]!.callID, {
        title: "unattributable result",
        metadata: {},
        output: "must not settle a historical record",
      })
      const before = yield* sessions.messages({ sessionID: input.session.id })
      const converted = yield* MessageV2.toModelMessagesEffect(before, resolved)
      expect(JSON.stringify(converted).match(/Tool execution was interrupted/g)).toHaveLength(4)
      const after = yield* sessions.messages({ sessionID: input.session.id })
      expect(after).toEqual(before)
      expect(after.find((message) => message.info.id === input.assistant.id)?.parts).toEqual(originals)
      expect(yield* llm.calls).toBe(0)
    }),
  20_000,
)

it.instance(
  "resume control: actual processor in original runner can progress the exact empty assistant without admission",
  () =>
    Effect.gen(function* () {
      const llm = yield* setup()
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const provider = yield* Provider.Service
      const agents = yield* Agent.Service
      const processor = yield* SessionProcessor.Service
      const state = yield* SessionRunState.Service
      const probe = yield* OwnershipProbe
      const input = yield* retained("exact existing-user processor feasibility")
      yield* llm.text("same assistant made actual progress")
      const work = Effect.gen(function* () {
        const resolved = yield* provider.getModel(model.providerID, model.modelID)
        const agent = yield* agents.get("build")
        const history = yield* sessions.messages({ sessionID: input.session.id })
        const messages = yield* MessageV2.toModelMessagesEffect(history, resolved)
        const handle = yield* processor.create({
          assistantMessage: input.assistant,
          sessionID: input.session.id,
          model: resolved,
        })
        yield* handle.process({
          user: input.user.info,
          agent,
          permission: input.session.permission,
          sessionID: input.session.id,
          system: [],
          messages,
          tools: {},
          model: resolved,
        })
        return { info: input.assistant, parts: [] }
      })
      yield* state.ensureRunning(
        input.session.id,
        Effect.succeed({ info: input.assistant, parts: [] }),
        work.pipe(Effect.orDie),
      )
      expect(probe.starts).toBe(1)
      expect(yield* llm.calls).toBe(1)
      expect(yield* queue.list(input.session.id)).toEqual([])
      const history = yield* sessions.messages({ sessionID: input.session.id })
      expect(history.filter((message) => message.info.role === "user")).toEqual([input.user])
      const assistants = history.filter((message) => message.info.role === "assistant")
      expect(assistants).toHaveLength(1)
      expect(assistants[0]?.info.id).toBe(input.assistant.id)
      expect(assistants[0]?.info.time.created).toBe(input.assistant.time.created)
      if (assistants[0]?.info.role !== "assistant") throw new Error("expected the retained assistant")
      expect(assistants[0].info.time.completed).toBeDefined()
      expect(
        assistants[0]?.parts.some(
          (part) => part.type === "text" && part.text === "same assistant made actual progress",
        ),
      ).toBe(true)
    }),
  20_000,
)
