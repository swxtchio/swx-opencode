export * as AskRecovery from "./recovery"

import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { NamedError } from "@opencode-ai/core/util/error"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import type { SessionAsk } from "@opencode-ai/schema/session-ask"
import { Cause, Context, Effect, Layer, Scope } from "effect"
import { Config } from "@/config/config"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Permission } from "@/permission"
import { Provider } from "@/provider/provider"
import { Question } from "@/question"
import { SessionPrompt } from "@/session/prompt"
import { Session } from "@/session/session"
import { MessageID, type SessionID } from "@/session/schema"
import { answered } from "@/tool/question"
import { approve, declined, planPath } from "@/tool/plan"
import { AskLedger } from "./ledger"

// An ask answered after the process that waited on it is gone (swxtchio/swx-opencode#286). In one transaction this
// settles a still-pending ask and writes the outcome its producer would have produced into the stored call, so an
// accepted answer is either applied or not settled at all. An ask that was settled before its process died is
// handed here too, and gets its recorded decision if the call never received it. Where the live tool would have let
// the turn go on, the Session is woken to continue. Only the asking call is recovered: nothing replays provider
// work, and a permission-guarded call that never ran is not run here.
export interface Interface {
  // Applies a resolution the ledger handed back for recovery and returns the final one; any other is returned as is.
  readonly apply: (resolution: AskLedger.Resolution) => Effect.Effect<AskLedger.Resolution, AskLedger.UnavailableError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/AskRecovery") {}

type Outcome =
  | { status: "completed"; title: string; output: string; metadata: Record<string, unknown> }
  | { status: "error"; error: string }

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const database = yield* Database.Service
    const ledger = yield* AskLedger.Service
    const session = yield* Session.Service
    const provider = yield* Provider.Service
    const prompt = yield* SessionPrompt.Service
    const permission = yield* Permission.Service
    const config = yield* Config.Service
    const events = yield* EventV2Bridge.Service
    const scope = yield* Scope.Scope

    const apply = Effect.fn("AskRecovery.apply")(function* (resolution: AskLedger.Resolution) {
      const row = resolution.recover
      const { recover: _, ...settled } = resolution
      if (!row) return settled
      const stopOnDeny = (yield* config.get()).experimental?.continue_loop_on_deny !== true
      const result = yield* database.db
        .transaction(
          () =>
            Effect.gen(function* () {
              const claimed =
                row.time_settled === null
                  ? yield* ledger.settle({ id: row.id, decision: resolution.decision, recovered: true })
                  : { won: false, row }
              const current = claimed.row
              const final: AskLedger.Resolution = claimed.won
                ? settled
                : {
                    ...settled,
                    disposition: "duplicate",
                    decision: current.decision ?? { type: "cancelled" },
                    recovered: current.recovered,
                  }
              const decision = current.decision
              const idle = { final, row: current, won: claimed.won, written: false, wake: false }
              if (!decision || decision.type === "cancelled") return idle
              const target = yield* locate(current)
              if (!target) return idle
              const continues = continuesAfter(current, decision, stopOnDeny)
              if (target.newest && unsettled(target.part)) {
                yield* write(target.part, yield* produce(current, decision))
                yield* ledger.applied(current.id)
                return { ...idle, final: { ...final, recovered: true }, written: true, wake: continues }
              }
              // An earlier recovery wrote the call, but the wake that continues it may have been lost with its process.
              if (current.time_applied !== null && !target.continued) return { ...idle, wake: continues }
              if (target.newest)
                yield* Effect.logWarning("recovered ask's tool call already has an outcome", { id: row.id })
              else yield* Effect.logWarning("recovered ask's Session went on without it", { id: row.id })
              return idle
            }),
          { behavior: "immediate" },
        )
        .pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : Effect.logWarning("ask recovery failed", { id: row.id, cause: Cause.pretty(cause) }).pipe(
                  Effect.andThen(
                    Effect.fail(new AskLedger.UnavailableError({ requestID: row.id, reason: "store-unavailable" })),
                  ),
                ),
          ),
        )
      yield* Effect.logInfo("recovered ask", {
        id: row.id,
        won: result.won,
        written: result.written,
        wake: result.wake,
      })
      if (result.won) yield* publish(result.row)
      const decision = result.row.decision
      // An `always` grant outlives the call, so the model's retry is not asked again.
      if ((result.won || result.written) && decision?.type === "replied" && decision.reply === "always")
        yield* permission.grant(result.row.request as PermissionV1.Request)
      if (result.wake) yield* wake(result.row.session_id)
      return result.final
    })

    // The asking call and where its Session stands: whether its message is still the newest, which an accepted plan
    // changes, and whether the model has answered since, which only a continued turn does.
    const locate = Effect.fn("AskRecovery.locate")(function* (row: AskLedger.Row) {
      const origin = row.request.tool
      if (!origin) {
        yield* Effect.logWarning("recovered ask has no tool call", { id: row.id })
        return
      }
      const messages = yield* session.messages({ sessionID: row.session_id })
      const index = messages.findIndex((message) => message.info.id === MessageID.make(origin.messageID))
      const part = messages[index]?.parts.find(
        (part): part is SessionV1.ToolPart => part.type === "tool" && part.callID === origin.callID,
      )
      if (!part || (row.kind === "question" && part.tool !== row.tool)) {
        yield* Effect.logWarning("recovered ask's tool call is missing", { id: row.id, callID: origin.callID })
        return
      }
      return {
        part,
        newest: index === messages.length - 1,
        continued: messages.slice(index + 1).some((message) => message.info.role === "assistant"),
      }
    })

    // The producer's own result for the settled decision. An accepted plan's switch to the build agent is itself an
    // effect, written in the caller's transaction with the call.
    const produce = Effect.fn("AskRecovery.produce")(function* (row: AskLedger.Row, decision: SessionAsk.Decision) {
      const refused = (error: string) => ({ status: "error", error }) satisfies Outcome
      if (decision.type === "rejected") return refused(new Question.RejectedError().message)
      if (decision.type === "replied") {
        if (decision.reply === "reject")
          return refused(
            decision.message
              ? new PermissionV1.CorrectedError({ feedback: decision.message }).message
              : new PermissionV1.RejectedError().message,
          )
        return refused(
          `Permission was granted (${decision.reply}), but the server restarted while this call waited for it, so the ` +
            "call did not run. Call the tool again to perform it.",
        )
      }
      if (decision.type !== "answered") return refused("Tool execution aborted")
      if (row.tool === "plan_exit") {
        if (declined(decision.answers)) return refused(new Question.RejectedError().message)
        const plan = yield* planPath(session, row.session_id)
        return {
          status: "completed",
          ...(yield* approve({ session, provider }, row.session_id, plan)),
        } satisfies Outcome
      }
      const request = row.request as Question.Request
      return { status: "completed", ...answered(request.questions, decision.answers) } satisfies Outcome
    })

    const write = Effect.fn("AskRecovery.write")(function* (part: SessionV1.ToolPart, outcome: Outcome) {
      const start = "time" in part.state ? part.state.time.start : Date.now()
      const metadata = "metadata" in part.state ? (part.state.metadata ?? {}) : {}
      yield* session.updatePart({
        ...part,
        state:
          outcome.status === "completed"
            ? {
                status: "completed",
                input: part.state.input,
                output: outcome.output,
                metadata: outcome.metadata,
                title: outcome.title,
                time: { start, end: Date.now() },
              }
            : {
                status: "error",
                input: part.state.input,
                error: outcome.error,
                metadata,
                time: { start, end: Date.now() },
              },
      } satisfies SessionV1.ToolPart)
    })

    // The event a live settlement would have published.
    const publish = (row: AskLedger.Row) => {
      const decision = row.decision
      const sessionID = row.session_id
      const question = (row.request as Question.Request).id
      if (decision?.type === "answered")
        return events.publish(Question.Event.Replied, {
          sessionID,
          requestID: question,
          answers: decision.answers.map((answer) => [...answer]),
        })
      if (decision?.type === "rejected")
        return events.publish(Question.Event.Rejected, { sessionID, requestID: question })
      if (decision?.type === "replied")
        return events.publish(Permission.Event.Replied, {
          sessionID,
          requestID: (row.request as PermissionV1.Request).id,
          reply: decision.reply,
        })
      return Effect.void
    }

    // Wakes the Session the way the queue's send-now does: the drain runs in the background, a failure is a
    // Session error. A Session already running joins that run.
    const wake = (sessionID: SessionID) =>
      prompt.loop({ sessionID }).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            yield* Effect.logError("recovered ask wake failed", { sessionID, cause })
            yield* events.publish(Session.Event.Error, {
              sessionID,
              error: new NamedError.Unknown({ message: Cause.pretty(cause) }).toObject(),
            })
          }),
        ),
        Effect.forkIn(scope, { startImmediately: true }),
        Effect.asVoid,
      )

    return Service.of({ apply })
  }),
)

// Whether the live tool would have let the turn go on with this decision: a refusal ends it unless the operator
// configured otherwise, and feedback on a refused permission always reaches the model.
function continuesAfter(row: AskLedger.Row, decision: SessionAsk.Decision, stopOnDeny: boolean) {
  if (decision.type === "cancelled") return false
  if (decision.type === "rejected") return !stopOnDeny
  if (decision.type === "replied") return decision.reply !== "reject" || decision.message !== undefined || !stopOnDeny
  if (row.tool === "plan_exit" && declined(decision.answers)) return !stopOnDeny
  return true
}

// A call still waiting when its process died is pending or running; one an in-process shutdown cleaned up carries
// the interrupted disposition. A call with any other outcome has its own record and is never overwritten.
function unsettled(part: SessionV1.ToolPart) {
  if (part.state.status === "pending" || part.state.status === "running") return true
  return part.state.status === "error" && part.state.metadata?.interrupted === true
}

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [
    Database.node,
    AskLedger.node,
    Session.node,
    Provider.node,
    SessionPrompt.node,
    Permission.node,
    Config.node,
    EventV2Bridge.node,
  ],
})
