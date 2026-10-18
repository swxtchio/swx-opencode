// Question lifecycle under `--questions-command` (swxtchio/swx-opencode#286): the question tool's ask is recorded and
// emitted, stays pending until a decision, and is settled once by whichever resolver answers first.
import { describe, expect } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionAskTable } from "@opencode-ai/core/session/ask.sql"
import { eq } from "drizzle-orm"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Layer } from "effect"
import { AskEmit } from "../../src/ask/emit"
import { AskLedger } from "../../src/ask/ledger"
import { Agent } from "../../src/agent/agent"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import { Question } from "../../src/question"
import { QuestionID } from "../../src/question/schema"
import { MessageID } from "../../src/session/schema"
import { Session } from "../../src/session/session"
import { QuestionTool } from "../../src/tool/question"
import { Truncate } from "../../src/tool/truncate"
import { provideInstance, tmpdirScoped } from "../fixture/fixture"
import { writeReceiver } from "../lib/ask-receiver"
import { deadline } from "../lib/deadline"
import { testEffect } from "../lib/effect"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Question.node,
      AskLedger.node,
      Session.node,
      Agent.node,
      Truncate.node,
      EventV2Bridge.node,
      SessionProjector.node,
      Database.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
    ]),
    [
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalWorkspaces: false })],
      [
        InstanceBootstrap.node,
        Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void })),
      ],
    ],
  ),
)

const prompts = [
  {
    question: "Which colors?",
    header: "Colors",
    multiple: true,
    options: [
      { label: "Red", description: "Warm" },
      { label: "Blue", description: "Cool" },
    ],
  },
  { question: "Any note?", header: "Note", options: [{ label: "None", description: "No note" }] },
]

// Emission on, with a receiver that records each ask; a real Session for the asks to belong to.
const supervised = Effect.fn("supervised")(function* (receiverBody = "cat >> '$OUT'") {
  const home = yield* tmpdirScoped()
  const receiver = yield* writeReceiver(home, receiverBody)
  AskEmit.configure({ command: receiver.path, server: "http://127.0.0.1:4096" })
  yield* Effect.addFinalizer(() => Effect.sync(() => AskEmit.configure(undefined)))
  const session = yield* (yield* Session.Service).create({ title: "supervised" })
  return { receiver, sessionID: session.id }
})

// Runs the question tool as a model's call would, in the background.
const askWithTool = Effect.fn("askWithTool")(function* (sessionID: Session.Info["id"], callID: string) {
  const tool = yield* (yield* QuestionTool).init()
  return yield* tool
    .execute(
      { questions: prompts },
      {
        sessionID,
        messageID: MessageID.make("msg_supervised"),
        callID,
        agent: "build",
        abort: AbortSignal.any([]),
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      },
    )
    .pipe(Effect.forkScoped)
})

// Beyond the receiver's read deadline, so a red run fails on that deadline rather than on Bun's default.
const timeout = deadline(30_000)

// Holds every event of one type inside its publication until released, signalling when the first arrives.
const hold = Effect.fn("hold")(function* (type: string) {
  const events = yield* EventV2Bridge.Service
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const unsubscribe = yield* events.listen((event) =>
    event.type === type
      ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
      : Effect.void,
  )
  yield* Effect.addFinalizer(() => unsubscribe)
  return { entered, release }
})

// Waits on a fiber or a signal with a bound, so a decision that never arrives fails here and names what is missing.
const within = <A, E, R>(self: Effect.Effect<A, E, R>, missing = "the waiting call never received its decision") =>
  self.pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(deadline(10_000)),
      orElse: () => Effect.die(new Error(missing)),
    }),
  )

const failure = <A, E, R>(self: Effect.Effect<A, E, R>) =>
  self.pipe(
    Effect.exit,
    Effect.map((exit) => (Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined)),
  )

describe("question lifecycle under --questions-command", () => {
  it.live(
    "emits the question tool's ask, keeps it pending, and applies the receiver's answer",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const { receiver, sessionID } = yield* supervised()
          const fiber = yield* askWithTool(sessionID, "call_emit")
          const [emitted] = yield* receiver.read(1)

          expect(emitted).toMatchObject({
            type: "opencode.ask",
            version: 1,
            kind: "question",
            sessionID,
            directory: dir,
            server: "http://127.0.0.1:4096",
            origin: { messageID: "msg_supervised", callID: "call_emit", tool: "question" },
            answer: { method: "POST", path: `/question/${emitted.id}/answer`, query: { directory: dir } },
          })
          expect(emitted.question.questions).toMatchObject(prompts)
          expect((yield* question.list()).map((item) => item.id)).toEqual([emitted.id])
          expect(fiber.pollUnsafe()).toBeUndefined()

          const answers = [
            emitted.question.questions[0].options.map((option: { label: string }) => option.label),
            ["Ship it on Friday"],
          ]
          const resolution = yield* question.reply({ requestID: emitted.id, answers })
          expect(resolution).toMatchObject({ disposition: "settled", decision: { type: "answered", answers } })
          const result = yield* within(Fiber.join(fiber))
          expect(result.metadata.answers).toEqual(answers)
          expect(result.output).toContain(`"Which colors?"="Red, Blue", "Any note?"="Ship it on Friday"`)
          expect(yield* question.list()).toEqual([])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "the first decision wins whichever resolver sends it, and a later one is reported as a duplicate",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const { receiver, sessionID } = yield* supervised()

          const answered = yield* askWithTool(sessionID, "call_answer_first")
          const [first] = yield* receiver.read(1)
          yield* question.reply({ requestID: first.id, answers: [["Red"], ["None"]] })
          const late = yield* question.reject(first.id)
          expect(late).toMatchObject({
            disposition: "duplicate",
            decision: { type: "answered", answers: [["Red"], ["None"]] },
          })
          expect((yield* within(Fiber.join(answered))).metadata.answers).toEqual([["Red"], ["None"]])

          const rejected = yield* askWithTool(sessionID, "call_reject_first")
          const [, second] = yield* receiver.read(2)
          yield* question.reject(second.id)
          const lateAnswer = yield* question.reply({ requestID: second.id, answers: [["Blue"], ["None"]] })
          expect(lateAnswer).toMatchObject({ disposition: "duplicate", decision: { type: "rejected" } })
          expect(yield* failure(within(Fiber.join(rejected)))).toBeInstanceOf(Question.RejectedError)
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "concurrent competing answers settle the ask exactly once",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const { receiver, sessionID } = yield* supervised()
          const fiber = yield* askWithTool(sessionID, "call_race")
          const [emitted] = yield* receiver.read(1)

          const results = yield* Effect.all(
            [
              question.reply({ requestID: emitted.id, answers: [["Red"], ["human"]] }),
              question.reply({ requestID: emitted.id, answers: [["Blue"], ["machine"]] }),
              question.reject(emitted.id),
            ],
            { concurrency: "unbounded" },
          )
          const winners = results.filter((result) => result.disposition === "settled")
          expect(winners).toHaveLength(1)
          const winner = winners[0]!.decision
          expect(results.every((result) => JSON.stringify(result.decision) === JSON.stringify(winner))).toBe(true)
          const outcome = yield* within(Fiber.join(fiber)).pipe(Effect.exit)
          if (winner.type === "answered")
            expect(Exit.isSuccess(outcome) && outcome.value.metadata.answers).toEqual(winner.answers)
          if (winner.type === "rejected") expect(Exit.isFailure(outcome)).toBe(true)
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "a settlement that lost the race inside the ledger's transaction reports the winner and changes nothing",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const ledger = yield* AskLedger.Service
          const { receiver, sessionID } = yield* supervised()
          const fiber = yield* askWithTool(sessionID, "call_transaction")
          const [emitted] = yield* receiver.read(1)

          // Both resolvers passed decide's read of a pending ask; the transaction decides between them.
          const first = yield* ledger.settle({
            id: emitted.id,
            decision: { type: "answered", answers: [["Red"], ["first"]] },
            delivered: true,
            recovered: false,
          })
          const second = yield* ledger.settle({
            id: emitted.id,
            decision: { type: "rejected" },
            delivered: true,
            recovered: false,
          })
          expect(first.won).toBe(true)
          expect(second.won).toBe(false)
          expect(second.row.decision).toEqual({ type: "answered", answers: [["Red"], ["first"]] })
          const stored = yield* ledger.find(emitted.id, "question")
          expect(stored?.decision).toEqual({ type: "answered", answers: [["Red"], ["first"]] })
          yield* Fiber.interrupt(fiber)
          expect(yield* question.list()).toEqual([])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "an unknown request or another directory's answer fails and leaves the pending ask untouched",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        const other = yield* tmpdirScoped({ git: true })
        const { emitted, fiber } = yield* Effect.gen(function* () {
          const { receiver, sessionID } = yield* supervised()
          const fiber = yield* askWithTool(sessionID, "call_scope")
          const [emitted] = yield* receiver.read(1)
          const unknown = yield* failure(
            Question.Service.use((svc) => svc.reply({ requestID: QuestionID.ascending(), answers: [["Red"]] })),
          )
          expect(unknown).toBeInstanceOf(Question.NotFoundError)
          return { emitted, fiber }
        }).pipe(provideInstance(dir))

        const elsewhere = yield* failure(
          Question.Service.use((svc) => svc.reply({ requestID: emitted.id, answers: [["Red"], ["None"]] })),
        ).pipe(provideInstance(other))
        expect(elsewhere).toBeInstanceOf(Question.NotFoundError)

        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          expect((yield* question.list()).map((item) => item.id)).toEqual([emitted.id])
          expect(fiber.pollUnsafe()).toBeUndefined()
          yield* question.reply({ requestID: emitted.id, answers: [["Red"], ["None"]] })
          expect((yield* within(Fiber.join(fiber))).metadata.answers).toEqual([["Red"], ["None"]])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "an aborted ask is cancelled, so no later answer is applied to it",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const { receiver, sessionID } = yield* supervised()
          const fiber = yield* askWithTool(sessionID, "call_abort")
          const [emitted] = yield* receiver.read(1)
          yield* Fiber.interrupt(fiber)

          expect(yield* question.list()).toEqual([])
          const late = yield* question.reply({ requestID: emitted.id, answers: [["Red"], ["None"]] })
          expect(late).toMatchObject({ disposition: "duplicate", decision: { type: "cancelled" } })
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "an ask interrupted while its event is published is cancelled at once",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const ledger = yield* AskLedger.Service
          const { receiver, sessionID } = yield* supervised()
          const held = yield* hold(Question.Event.Asked.type)
          const fiber = yield* askWithTool(sessionID, "call_publishing")
          yield* within(Deferred.await(held.entered), "the ask event was never published")
          const [emitted] = yield* receiver.read(1)
          yield* Fiber.interrupt(fiber)

          expect(yield* ledger.find(emitted.id, "question")).toMatchObject({ decision: { type: "cancelled" } })
          expect(yield* question.list()).toEqual([])
          const late = yield* question.reply({ requestID: emitted.id, answers: [["Red"], ["None"]] })
          expect(late).toMatchObject({ disposition: "duplicate", decision: { type: "cancelled" } })
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "an ask whose event cannot be published fails and is cancelled",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const ledger = yield* AskLedger.Service
          const events = yield* EventV2Bridge.Service
          const { receiver, sessionID } = yield* supervised()
          const unsubscribe = yield* events.listen((event) =>
            event.type === Question.Event.Asked.type ? Effect.die(new Error("publication refused")) : Effect.void,
          )
          const fiber = yield* askWithTool(sessionID, "call_unpublished")
          const [emitted] = yield* receiver.read(1)
          expect(yield* failure(within(Fiber.join(fiber)))).toBeDefined()
          yield* unsubscribe

          expect(yield* ledger.find(emitted.id, "question")).toMatchObject({ decision: { type: "cancelled" } })
          expect(yield* question.list()).toEqual([])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "an answer interrupted while it settles the ask still reaches the waiting call",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const ledger = yield* AskLedger.Service
          const db = (yield* Database.Service).db
          const { receiver, sessionID } = yield* supervised()
          const fiber = yield* askWithTool(sessionID, "call_settling")
          const [emitted] = yield* receiver.read(1)
          // Another writer holds the database, so the answer, started at once, runs until its settlement waits for
          // it; the interruption is requested there.
          const locked = yield* Deferred.make<void>()
          const unlock = yield* Deferred.make<void>()
          const writer = yield* db
            .transaction(() => Deferred.succeed(locked, undefined).pipe(Effect.andThen(Deferred.await(unlock))), {
              behavior: "immediate",
            })
            .pipe(Effect.forkScoped)
          yield* within(Deferred.await(locked), "the other writer never took the database")
          const replying = yield* question
            .reply({ requestID: emitted.id, answers: [["Blue"], ["None"]] })
            .pipe(Effect.forkScoped({ startImmediately: true }))
          const interrupting = yield* Fiber.interrupt(replying).pipe(Effect.forkScoped)
          yield* Deferred.succeed(unlock, undefined)
          yield* within(Fiber.join(writer))
          yield* within(Fiber.join(interrupting))

          expect(yield* ledger.find(emitted.id, "question")).toMatchObject({
            decision: { type: "answered", answers: [["Blue"], ["None"]] },
          })
          const result = yield* within(Fiber.join(fiber))
          expect(result.metadata.answers).toEqual([["Blue"], ["None"]])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "an answer whose event cannot be published still reaches the waiting call",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const events = yield* EventV2Bridge.Service
          const { receiver, sessionID } = yield* supervised()
          const fiber = yield* askWithTool(sessionID, "call_unannounced")
          const [emitted] = yield* receiver.read(1)
          const unsubscribe = yield* events.listen((event) =>
            event.type === Question.Event.Replied.type ? Effect.die(new Error("publication refused")) : Effect.void,
          )
          expect(yield* failure(question.reply({ requestID: emitted.id, answers: [["Blue"], ["None"]] }))).toBeDefined()
          yield* unsubscribe

          const result = yield* within(Fiber.join(fiber))
          expect(result.metadata.answers).toEqual([["Blue"], ["None"]])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "a pending ask this process no longer waits on is ended by the next answer",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const ledger = yield* AskLedger.Service
          const { receiver, sessionID } = yield* supervised()
          const id = QuestionID.ascending()
          const tool = { messageID: MessageID.make("msg_supervised"), callID: "call_orphan" }
          yield* ledger.admit({
            id,
            sessionID,
            kind: "question",
            tool: "question",
            request: { id, sessionID, questions: prompts, tool },
          })
          yield* receiver.read(1)

          const late = yield* question.reply({ requestID: id, answers: [["Red"], ["None"]] })
          expect(late).toMatchObject({ disposition: "duplicate", decision: { type: "cancelled" } })
          expect(yield* ledger.find(id, "question")).toMatchObject({ decision: { type: "cancelled" } })
          expect(yield* question.list()).toEqual([])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "an answer naming another kind's ask is unknown and leaves that ask pending",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const ledger = yield* AskLedger.Service
          const { receiver, sessionID } = yield* supervised()
          const id = QuestionID.ascending()
          yield* ledger.admit({
            id,
            sessionID,
            kind: "permission",
            request: {
              id: PermissionV1.ID.ascending(),
              sessionID,
              permission: "bash",
              patterns: ["make"],
              metadata: {},
              always: [],
              tool: { messageID: MessageID.make("msg_supervised"), callID: "call_kind" },
            },
          })
          yield* receiver.read(1)

          const unknown = yield* failure(question.reply({ requestID: id, answers: [["Red"], ["None"]] }))
          expect(unknown).toBeInstanceOf(Question.NotFoundError)
          expect(yield* ledger.find(id, "permission")).toMatchObject({ time_settled: null })
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "a supervised question without a tool call is refused and never recorded",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const db = (yield* Database.Service).db
          const { sessionID } = yield* supervised()
          const refused = yield* failure(question.ask({ sessionID, questions: prompts }))
          expect(refused).toBeInstanceOf(Question.RejectedError)
          expect(
            yield* db.select().from(SessionAskTable).where(eq(SessionAskTable.session_id, sessionID)).all(),
          ).toEqual([])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "an answer whose state cannot be read fails without settling the ask",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const db = (yield* Database.Service).db
          const { receiver, sessionID } = yield* supervised()
          const fiber = yield* askWithTool(sessionID, "call_store")
          const [emitted] = yield* receiver.read(1)

          yield* db.run("ALTER TABLE session_ask RENAME TO session_ask_unavailable")
          const unavailable = yield* failure(question.reply({ requestID: emitted.id, answers: [["Red"], ["None"]] }))
          yield* db.run("ALTER TABLE session_ask_unavailable RENAME TO session_ask")
          expect(unavailable).toBeInstanceOf(AskLedger.UnavailableError)
          expect(unavailable).toMatchObject({ requestID: emitted.id, reason: "store-unavailable" })
          expect(fiber.pollUnsafe()).toBeUndefined()

          const settled = yield* question.reply({ requestID: emitted.id, answers: [["Blue"], ["None"]] })
          expect(settled.disposition).toBe("settled")
          expect((yield* within(Fiber.join(fiber))).metadata.answers).toEqual([["Blue"], ["None"]])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "without emission an ask writes no ledger row and a second answer is not found",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const question = yield* Question.Service
          const db = (yield* Database.Service).db
          const session = yield* (yield* Session.Service).create({ title: "stock" })
          const fiber = yield* askWithTool(session.id, "call_stock")
          const pending = yield* question.list().pipe(
            Effect.filterOrFail(
              (items) => items.length === 1,
              () => new Error("question not pending"),
            ),
            Effect.retry({ times: 100 }),
          )
          yield* question.reply({ requestID: pending[0]!.id, answers: [["Red"], ["None"]] })
          yield* within(Fiber.join(fiber))

          expect(yield* failure(question.reply({ requestID: pending[0]!.id, answers: [["Blue"]] }))).toBeInstanceOf(
            Question.NotFoundError,
          )
          const rows = yield* db.select().from(SessionAskTable).where(eq(SessionAskTable.session_id, session.id)).all()
          expect(rows).toEqual([])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )
})
