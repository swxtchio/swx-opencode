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
// accepted answer is either applied or not settled at all: a producer effect that fails settles nothing. A call that
// already ended without the answer (its ask was aborted) settles the ask as cancelled. For one that is gone or that its
// Session went on without, the answer fails with `call-gone` and nothing is settled, so the ask stays pending. Where the live tool would have let the turn go on, the Session
// is woken to continue, and an ask recovery applied earlier is handed here again so a wake its process lost is
// repeated. Only the asking call is recovered: nothing replays provider work, and a permission-guarded call that
// never ran is not run here. A decision a live waiter received is never handed here.
export interface Interface {
  // Applies a resolution the ledger handed back for recovery and returns the final one; any other is returned as is.
  readonly apply: (
    resolution: AskLedger.Resolution,
  ) => Effect.Effect<AskLedger.Resolution, AskLedger.UnavailableError | AskLedger.NotAppliedError>
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
              // Read again inside the transaction, which no other writer can enter.
              const current = (yield* ledger.find(row.id, row.kind)) ?? row
              const target = yield* locate(current)
              if (current.time_settled !== null) {
                const final: AskLedger.Resolution = {
                  ...settled,
                  disposition: "duplicate",
                  decision: current.decision ?? { type: "cancelled" },
                  recovered: current.recovered,
                }
                const idle = { final, row: current, won: false, wake: false, gone: false }
                if (!current.recovered || !current.decision || !target || target.continued) return idle
                return { ...idle, wake: continuesAfter(current, current.decision, stopOnDeny) }
              }
              const record = (decision: SessionAsk.Decision, recovered: boolean) =>
                ledger.settle({ id: current.id, decision, delivered: false, recovered })
              if (target && !unsettled(target.part)) {
                yield* Effect.logWarning("recovered ask's tool call ended without it; cancelling", { id: row.id })
                const ended = yield* record({ type: "cancelled" }, false)
                return {
                  final: { ...settled, disposition: "duplicate" as const, decision: { type: "cancelled" as const } },
                  row: ended.row,
                  won: false,
                  wake: false,
                  gone: false,
                }
              }
              // Nothing can apply the answer, so nothing is settled: the ask stays pending and the answer is refused.
              if (!target?.newest) {
                yield* Effect.logWarning("recovered ask's call is gone or its Session went on; answer not applied", {
                  id: row.id,
                })
                return { final: settled, row: current, won: false, wake: false, gone: true }
              }
              const outcome = yield* produce(current, resolution.decision).pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("recovered ask's answer could not be applied", {
                    id: row.id,
                    cause: Cause.pretty(cause),
                  }).pipe(
                    Effect.andThen(
                      Effect.fail(new AskLedger.NotAppliedError({ requestID: row.id, reason: "apply-failed" })),
                    ),
                  ),
                ),
              )
              yield* write(target.part, outcome)
              const applied = yield* record(resolution.decision, true)
              return {
                final: { ...settled, recovered: true },
                row: applied.row,
                won: true,
                wake: continuesAfter(current, resolution.decision, stopOnDeny),
                gone: false,
              }
            }),
          { behavior: "immediate" },
        )
        .pipe(AskLedger.settling(row.id))
      yield* Effect.logInfo("recovered ask", {
        id: row.id,
        won: result.won,
        recovered: result.final.recovered,
        wake: result.wake,
      })
      if (result.gone) return yield* new AskLedger.NotAppliedError({ requestID: row.id, reason: "call-gone" })
      if (result.won) yield* publish(result.row)
      const decision = result.row.decision
      // An `always` grant outlives the call, so the model's retry is not asked again.
      if (result.won && decision?.type === "replied" && decision.reply === "always")
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
          ...(yield* approve({ session, provider, database }, row.session_id, plan)),
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

// A call still waiting when its process died is pending or running. Any other outcome, an interrupted one included,
// means the call stopped waiting and has its own record, which is never overwritten.
function unsettled(part: SessionV1.ToolPart) {
  return part.state.status === "pending" || part.state.status === "running"
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
