// Question lifecycle under `--questions emit` (swxtchio/swx-opencode#286): the question tool's ask is recorded and
// emitted, stays pending until a decision, and is settled once by whichever resolver answers first.
import { describe, expect } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionAskTable } from "@opencode-ai/core/session/ask.sql"
import { eq } from "drizzle-orm"
import { Cause, Effect, Exit, Fiber, Layer } from "effect"
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

const failure = <A, E, R>(self: Effect.Effect<A, E, R>) =>
  self.pipe(
    Effect.exit,
    Effect.map((exit) => (Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined)),
  )

describe("question lifecycle under --questions emit", () => {
  it.live("emits the question tool's ask, keeps it pending, and applies the receiver's answer", () =>
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
        const result = yield* Fiber.join(fiber)
        expect(result.metadata.answers).toEqual(answers)
        expect(result.output).toContain(`"Which colors?"="Red, Blue", "Any note?"="Ship it on Friday"`)
        expect(yield* question.list()).toEqual([])
      }).pipe(provideInstance(dir))
    }),
  )

  it.live("the first decision wins whichever resolver sends it, and a later one is reported as a duplicate", () =>
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
        expect((yield* Fiber.join(answered)).metadata.answers).toEqual([["Red"], ["None"]])

        const rejected = yield* askWithTool(sessionID, "call_reject_first")
        const [, second] = yield* receiver.read(2)
        yield* question.reject(second.id)
        const lateAnswer = yield* question.reply({ requestID: second.id, answers: [["Blue"], ["None"]] })
        expect(lateAnswer).toMatchObject({ disposition: "duplicate", decision: { type: "rejected" } })
        expect(yield* failure(Fiber.join(rejected))).toBeInstanceOf(Question.RejectedError)
      }).pipe(provideInstance(dir))
    }),
  )

  it.live("concurrent competing answers settle the ask exactly once", () =>
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
        const outcome = yield* Fiber.join(fiber).pipe(Effect.exit)
        if (winner.type === "answered")
          expect(Exit.isSuccess(outcome) && outcome.value.metadata.answers).toEqual(winner.answers)
        if (winner.type === "rejected") expect(Exit.isFailure(outcome)).toBe(true)
      }).pipe(provideInstance(dir))
    }),
  )

  it.live("a settlement that lost the race inside the ledger's transaction reports the winner and changes nothing", () =>
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
          recovered: false,
        })
        const second = yield* ledger.settle({ id: emitted.id, decision: { type: "rejected" }, recovered: false })
        expect(first.won).toBe(true)
        expect(second.won).toBe(false)
        expect(second.row.decision).toEqual({ type: "answered", answers: [["Red"], ["first"]] })
        const stored = yield* ledger.find(emitted.id)
        expect(stored?.decision).toEqual({ type: "answered", answers: [["Red"], ["first"]] })
        yield* Fiber.interrupt(fiber)
        expect(yield* question.list()).toEqual([])
      }).pipe(provideInstance(dir))
    }),
  )

  it.live("an unknown request or another directory's answer fails and leaves the pending ask untouched", () =>
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
        expect((yield* Fiber.join(fiber)).metadata.answers).toEqual([["Red"], ["None"]])
      }).pipe(provideInstance(dir))
    }),
  )

  it.live("an aborted ask is cancelled, so no later answer is applied to it", () =>
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
  )

  it.live("an answer whose state cannot be read fails without settling the ask", () =>
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
        expect((yield* Fiber.join(fiber)).metadata.answers).toEqual([["Blue"], ["None"]])
      }).pipe(provideInstance(dir))
    }),
  )

  it.live("without emission an ask writes no ledger row and a second answer is not found", () =>
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
        yield* Fiber.join(fiber)

        expect(yield* failure(question.reply({ requestID: pending[0]!.id, answers: [["Blue"]] }))).toBeInstanceOf(
          Question.NotFoundError,
        )
        const rows = yield* db.select().from(SessionAskTable).where(eq(SessionAskTable.session_id, session.id)).all()
        expect(rows).toEqual([])
      }).pipe(provideInstance(dir))
    }),
  )
})
