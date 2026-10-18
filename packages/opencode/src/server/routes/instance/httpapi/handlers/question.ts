import { AskRecovery } from "@/ask/recovery"
import type { AskLedger } from "@/ask/ledger"
import { Question } from "@/question"
import { QuestionID } from "@/question/schema"
import type { SessionAsk } from "@opencode-ai/schema/session-ask"
import { Effect } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import { AskNotAppliedError, AskUnavailableError, QuestionNotFoundError } from "../errors"

export const questionHandlers = HttpApiBuilder.group(InstanceHttpApi, "question", (handlers) =>
  Effect.gen(function* () {
    const svc = yield* Question.Service
    const recovery = yield* AskRecovery.Service

    // Every route settles through the service, and recovery settles and applies a decision the service handed back.
    const settle = Effect.fn("QuestionHttpApi.settle")(function* (
      resolution: Effect.Effect<
        AskLedger.Resolution,
        Question.NotFoundError | AskLedger.UnavailableError | AskLedger.NotAppliedError
      >,
    ) {
      return yield* resolution.pipe(
        Effect.flatMap(recovery.apply),
        Effect.catchTag("Question.NotFoundError", (error) =>
          Effect.fail(
            new QuestionNotFoundError({
              requestID: String(error.requestID),
              message: `Question request not found: ${error.requestID}`,
            }),
          ),
        ),
        Effect.catchTag("SessionAsk.UnavailableError", (error) =>
          Effect.fail(
            new AskUnavailableError({ requestID: error.requestID, reason: error.reason, message: error.message }),
          ),
        ),
        Effect.catchTag("SessionAsk.NotAppliedError", (error) =>
          Effect.fail(
            new AskNotAppliedError({ requestID: error.requestID, reason: error.reason, message: error.message }),
          ),
        ),
      )
    })

    // The human routes keep their contract: true when this call settled the request, not found otherwise.
    const settledHere = (resolution: AskLedger.Resolution) =>
      resolution.disposition === "settled"
        ? Effect.succeed(true)
        : Effect.fail(
            new QuestionNotFoundError({
              requestID: resolution.requestID,
              message: `Question request already settled: ${resolution.requestID}`,
            }),
          )

    const list = Effect.fn("QuestionHttpApi.list")(function* () {
      return yield* svc.list()
    })

    const reply = Effect.fn("QuestionHttpApi.reply")(function* (ctx: {
      params: { requestID: QuestionID }
      payload: Question.Reply
    }) {
      return yield* settledHere(
        yield* settle(svc.reply({ requestID: ctx.params.requestID, answers: ctx.payload.answers })),
      )
    })

    const reject = Effect.fn("QuestionHttpApi.reject")(function* (ctx: { params: { requestID: QuestionID } }) {
      return yield* settledHere(yield* settle(svc.reject(ctx.params.requestID)))
    })

    const answer = Effect.fn("QuestionHttpApi.answer")(function* (ctx: {
      params: { requestID: QuestionID }
      payload: SessionAsk.QuestionAnswer
    }) {
      const settled = yield* settle(
        "reject" in ctx.payload
          ? svc.reject(ctx.params.requestID)
          : svc.reply({ requestID: ctx.params.requestID, answers: ctx.payload.answers }),
      )
      return {
        requestID: settled.requestID,
        sessionID: settled.sessionID,
        disposition: settled.disposition,
        decision: settled.decision,
        recovered: settled.recovered,
      } satisfies SessionAsk.Result
    })

    return handlers.handle("list", list).handle("reply", reply).handle("reject", reject).handle("answer", answer)
  }),
)
