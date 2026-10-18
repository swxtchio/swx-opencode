import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Deferred, Effect, Layer, Schema, Context } from "effect"
import { InstanceState } from "@/effect/instance-state"
import { SessionID } from "@/session/schema"
import { QuestionID } from "./schema"
import { EventV2Bridge } from "@/event-v2-bridge"
import { QuestionV1 } from "@opencode-ai/schema/question-v1"
import type { SessionAsk } from "@opencode-ai/schema/session-ask"
import { AskEmit } from "@/ask/emit"
import { AskLedger } from "@/ask/ledger"

export const Option = QuestionV1.Option
export type Option = typeof Option.Type
export const Info = QuestionV1.Info
export type Info = typeof Info.Type
export const Prompt = QuestionV1.Prompt
export type Prompt = typeof Prompt.Type
export const Tool = QuestionV1.Tool
export type Tool = typeof Tool.Type
export const Request = QuestionV1.Request
export type Request = typeof Request.Type
export const Answer = QuestionV1.Answer
export type Answer = typeof Answer.Type
export const Reply = QuestionV1.Reply
export type Reply = typeof Reply.Type
export const Replied = QuestionV1.Replied
export const Rejected = QuestionV1.Rejected
export const Event = QuestionV1.Event

export class RejectedError extends Schema.TaggedErrorClass<RejectedError>()("QuestionRejectedError", {}) {
  override get message() {
    return "The user dismissed this question"
  }
}

export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()("Question.NotFoundError", {
  requestID: QuestionID,
}) {}

interface PendingEntry {
  info: Request
  deferred: Deferred.Deferred<ReadonlyArray<Answer>, RejectedError>
}

interface State {
  pending: Map<QuestionID, PendingEntry>
}

// Service

export interface Interface {
  readonly ask: (input: {
    sessionID: SessionID
    questions: ReadonlyArray<Info>
    tool?: Tool
    // The producing tool's name, reported to a supervisor and used to recover the call after a restart.
    producer?: string
  }) => Effect.Effect<ReadonlyArray<Answer>, RejectedError>
  readonly reply: (input: {
    requestID: QuestionID
    answers: ReadonlyArray<Answer>
  }) => Effect.Effect<AskLedger.Resolution, NotFoundError | AskLedger.UnavailableError>
  readonly reject: (
    requestID: QuestionID,
  ) => Effect.Effect<AskLedger.Resolution, NotFoundError | AskLedger.UnavailableError>
  readonly list: () => Effect.Effect<ReadonlyArray<Request>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Question") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const ledger = yield* AskLedger.Service
    const state = yield* InstanceState.make<State>(
      Effect.fn("Question.state")(function* () {
        const state = {
          pending: new Map<QuestionID, PendingEntry>(),
        }

        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            for (const item of state.pending.values()) {
              yield* ledger.cancel(item.info.id)
              yield* Deferred.fail(item.deferred, new RejectedError())
            }
            state.pending.clear()
          }),
        )

        return state
      }),
    )

    const ask = Effect.fn("Question.ask")(function* (input: {
      sessionID: SessionID
      questions: ReadonlyArray<Info>
      tool?: Tool
      producer?: string
    }) {
      const pending = (yield* InstanceState.get(state)).pending
      const id = QuestionID.ascending()
      yield* Effect.logInfo("asking", { id, questions: input.questions.length })

      const deferred = yield* Deferred.make<ReadonlyArray<Answer>, RejectedError>()
      const info: Request = {
        id,
        sessionID: input.sessionID,
        questions: input.questions,
        tool: input.tool,
      }
      // A supervised ask is recovered through its tool call after a restart, so one without a call is refused.
      if (AskEmit.current() && !input.tool) {
        yield* Effect.logError("supervised question refused: it has no tool call", { id, producer: input.producer })
        return yield* new RejectedError()
      }
      // Waiting before the ask is recorded or published, so no resolver can find it without its waiter.
      pending.set(id, { info, deferred })
      return yield* Effect.gen(function* () {
        yield* ledger.admit({ id, sessionID: input.sessionID, kind: "question", tool: input.producer, request: info })
        yield* events.publish(Event.Asked, info)
        return yield* Deferred.await(deferred)
      }).pipe(
        // However the ask ends, aborted or failed at any step included, a still-pending ledger row is cancelled
        // before the waiter is dropped, so no later answer finds the row without its waiter.
        Effect.onExit(() => ledger.cancel(id)),
        Effect.ensuring(
          Effect.sync(() => {
            pending.delete(id)
          }),
        ),
      )
    })

    const settle = Effect.fn("Question.settle")(function* (
      requestID: QuestionID,
      decision: typeof SessionAsk.Answered.Type | typeof SessionAsk.Rejected.Type,
    ) {
      const pending = (yield* InstanceState.get(state)).pending
      const complete = (existing: PendingEntry) =>
        decision.type === "answered"
          ? Deferred.succeed(existing.deferred, decision.answers)
          : Deferred.fail(existing.deferred, new RejectedError())
      const deliver = Effect.fn("Question.deliver")(function* (existing: PendingEntry) {
        pending.delete(requestID)
        yield* Effect.logInfo(decision.type, { requestID })
        yield* publish(existing.info.sessionID, requestID, decision)
        yield* complete(existing)
      })
      const unknown = Effect.fn("Question.unknown")(function* () {
        yield* Effect.logWarning("answer for unknown request", { requestID })
        return yield* new NotFoundError({ requestID })
      })

      if (!AskEmit.current()) {
        const existing = pending.get(requestID)
        if (!existing) return yield* unknown()
        yield* deliver(existing)
        return {
          requestID,
          sessionID: existing.info.sessionID,
          disposition: "settled",
          decision,
          recovered: false,
        } satisfies AskLedger.Resolution
      }

      // Settling and handing the answer to the waiter are one step, so a cancelled caller cannot leave a settled ask
      // whose waiter never receives it. A resolution handed back for recovery is not settled yet.
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const outcome = yield* ledger.decide({
            id: requestID,
            kind: "question",
            decision,
            live: () => pending.has(requestID),
          })
          if (outcome.type === "unknown") return yield* unknown()
          if (outcome.type !== "won") return outcome.resolution
          const existing = pending.get(requestID)
          // The waiter receives the answer even when its event cannot be published.
          if (existing) yield* deliver(existing).pipe(Effect.onError(() => complete(existing)))
          return outcome.resolution
        }),
      )
    })

    const publish = (
      sessionID: SessionID,
      requestID: QuestionID,
      decision: typeof SessionAsk.Answered.Type | typeof SessionAsk.Rejected.Type,
    ) =>
      decision.type === "answered"
        ? events.publish(Event.Replied, { sessionID, requestID, answers: decision.answers.map((a) => [...a]) })
        : events.publish(Event.Rejected, { sessionID, requestID })

    const reply = Effect.fn("Question.reply")(function* (input: {
      requestID: QuestionID
      answers: ReadonlyArray<Answer>
    }) {
      return yield* settle(input.requestID, { type: "answered", answers: input.answers.map((a) => [...a]) })
    })

    const reject = Effect.fn("Question.reject")(function* (requestID: QuestionID) {
      return yield* settle(requestID, { type: "rejected" })
    })

    const list = Effect.fn("Question.list")(function* () {
      const pending = (yield* InstanceState.get(state)).pending
      const live = Array.from(pending.values(), (x) => x.info)
      if (!AskEmit.current()) return live
      // Under emission the ledger is the registry, so an ask a restarted server still owes an answer stays listed.
      return yield* ledger.pending("question").pipe(
        Effect.map((rows) => rows.map((row) => row.request as Request)),
        Effect.catchTag("SessionAsk.UnavailableError", (error) =>
          Effect.logWarning("ask ledger unreadable; listing live questions", { error: error.message }).pipe(
            Effect.as(live),
          ),
        ),
      )
    })

    return Service.of({ ask, reply, reject, list })
  }),
)

export const node = LayerNode.make({ service: Service, layer: layer, deps: [EventV2Bridge.node, AskLedger.node] })

export * as Question from "."
