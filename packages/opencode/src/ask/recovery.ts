export * as AskRecovery from "./recovery"

import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { NamedError } from "@opencode-ai/core/util/error"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import { Cause, Context, Effect, Layer, Scope } from "effect"
import { Config } from "@/config/config"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Provider } from "@/provider/provider"
import { Question } from "@/question"
import { SessionPrompt } from "@/session/prompt"
import { Session } from "@/session/session"
import { MessageID, type SessionID } from "@/session/schema"
import { answered } from "@/tool/question"
import { approve, declined, planPath } from "@/tool/plan"
import type { AskLedger } from "./ledger"

// An ask answered after the process that waited on it is gone (swxtchio/swx-opencode#286). The answer was settled
// in the ledger exactly once; this writes the outcome the owning tool would have produced into its stored call and,
// where the live tool would have let the turn go on, wakes the Session so it continues. Only the asking call is
// recovered: nothing replays provider work, and a permission-guarded call that never ran is not run here.
export interface Interface {
  readonly apply: (resolution: AskLedger.Resolution) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/AskRecovery") {}

type Outcome =
  | { status: "completed"; title: string; output: string; metadata: Record<string, unknown> }
  | { status: "error"; error: string }

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const database = yield* Database.Service
    const session = yield* Session.Service
    const provider = yield* Provider.Service
    const prompt = yield* SessionPrompt.Service
    const config = yield* Config.Service
    const events = yield* EventV2Bridge.Service
    const scope = yield* Scope.Scope

    const apply = Effect.fn("AskRecovery.apply")(function* (resolution: AskLedger.Resolution) {
      const row = resolution.recover
      if (!row) return
      const origin = row.request.tool
      if (!origin) {
        yield* Effect.logWarning("recovered ask has no tool call", { id: row.id })
        return
      }
      const sessionID = row.session_id
      const found = yield* findPart(sessionID, origin.messageID, origin.callID)
      const part = found.part
      if (!part || (row.kind === "question" && part.tool !== row.tool)) {
        yield* Effect.logWarning("recovered ask's tool call is missing", { id: row.id, callID: origin.callID })
        return
      }
      if (!found.newest) {
        yield* Effect.logWarning("recovered ask's Session went on without it", { id: row.id })
        return
      }
      // Checked before the outcome, which for an accepted plan is itself an effect.
      if (!unsettled(part)) {
        yield* Effect.logWarning("recovered ask's tool call already has an outcome", { id: row.id })
        return
      }
      const result = yield* outcome(row, resolution.decision, sessionID)
      const written = yield* database.db
        .transaction(
          () =>
            Effect.gen(function* () {
              const current = yield* session.getPart({ sessionID, messageID: part.messageID, partID: part.id })
              if (!current || current.type !== "tool" || !unsettled(current)) return false
              const start = "time" in current.state ? current.state.time.start : Date.now()
              const metadata = "metadata" in current.state ? (current.state.metadata ?? {}) : {}
              yield* session.updatePart({
                ...current,
                state:
                  result.outcome.status === "completed"
                    ? {
                        status: "completed",
                        input: current.state.input,
                        output: result.outcome.output,
                        metadata: result.outcome.metadata,
                        title: result.outcome.title,
                        time: { start, end: Date.now() },
                      }
                    : {
                        status: "error",
                        input: current.state.input,
                        error: result.outcome.error,
                        metadata,
                        time: { start, end: Date.now() },
                      },
              } satisfies SessionV1.ToolPart)
              return true
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
      yield* Effect.logInfo("recovered ask", { id: row.id, written, continues: result.continues })
      if (written && result.continues) yield* wake(sessionID)
    })

    // The producer's own result for the settled decision, and whether its turn would go on.
    const outcome = Effect.fn("AskRecovery.outcome")(function* (
      row: AskLedger.Row,
      decision: AskLedger.Resolution["decision"],
      sessionID: SessionID,
    ) {
      const stopOnDeny = (yield* config.get()).experimental?.continue_loop_on_deny !== true
      const refused = (error: string, continues: boolean) => ({
        outcome: { status: "error", error } satisfies Outcome,
        continues,
      })
      if (decision.type === "cancelled") return refused("Tool execution aborted", false)
      if (decision.type === "rejected") return refused(new Question.RejectedError().message, !stopOnDeny)
      if (decision.type === "replied") {
        if (decision.reply === "reject")
          return decision.message
            ? refused(new PermissionV1.CorrectedError({ feedback: decision.message }).message, true)
            : refused(new PermissionV1.RejectedError().message, !stopOnDeny)
        return refused(
          `Permission was granted (${decision.reply}), but the server restarted while this call waited for it, so the ` +
            "call did not run. Call the tool again to perform it.",
          true,
        )
      }
      if (row.tool === "plan_exit") {
        if (declined(decision.answers)) return refused(new Question.RejectedError().message, !stopOnDeny)
        const plan = yield* planPath(session, sessionID).pipe(Effect.orDie)
        const approved = yield* approve({ session, provider }, sessionID, plan).pipe(Effect.orDie)
        return { outcome: { status: "completed", ...approved } satisfies Outcome, continues: true }
      }
      const request = row.request as Question.Request
      return {
        outcome: { status: "completed", ...answered(request.questions, decision.answers) } satisfies Outcome,
        continues: true,
      }
    })

    // The asking call, and whether its message is still the Session's newest: a Session that went on without the
    // answer has no turn left to continue.
    const findPart = Effect.fn("AskRecovery.findPart")(function* (
      sessionID: SessionID,
      messageID: string,
      callID: string,
    ) {
      const messages = yield* session.messages({ sessionID }).pipe(Effect.orElseSucceed(() => []))
      const index = messages.findIndex((message) => message.info.id === MessageID.make(messageID))
      const part = messages[index]?.parts.find(
        (part): part is SessionV1.ToolPart => part.type === "tool" && part.callID === callID,
      )
      return { part, newest: index === messages.length - 1 }
    })

    // Wakes the Session the way the queue's send-now does: the drain runs in the background, a failure is a
    // Session error.
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

// A call still waiting when its process died is pending or running; one an in-process shutdown cleaned up carries
// the interrupted disposition. A call with any other outcome has its own record and is never overwritten.
function unsettled(part: SessionV1.ToolPart) {
  if (part.state.status === "pending" || part.state.status === "running") return true
  return part.state.status === "error" && part.state.metadata?.interrupted === true
}

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [Database.node, Session.node, Provider.node, SessionPrompt.node, Config.node, EventV2Bridge.node],
})
