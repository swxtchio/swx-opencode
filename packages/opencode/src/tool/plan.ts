import path from "path"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Effect, Schema } from "effect"
import * as Tool from "./tool"
import { Question } from "../question"
import { Session } from "@/session/session"
import { Provider } from "@/provider/provider"
import { InstanceState } from "@/effect/instance-state"
import { MessageID, PartID, type SessionID } from "../session/schema"
import EXIT_DESCRIPTION from "./plan-exit.txt"

export const Parameters = Schema.Struct({})

export const PlanExitTool = Tool.define(
  "plan_exit",
  Effect.gen(function* () {
    const session = yield* Session.Service
    const question = yield* Question.Service
    const provider = yield* Provider.Service

    return {
      description: EXIT_DESCRIPTION,
      parameters: Parameters,
      execute: (_params: {}, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const plan = yield* planPath(session, ctx.sessionID)
          const answers = yield* question.ask({
            sessionID: ctx.sessionID,
            questions: [
              {
                question: `Plan at ${plan} is complete. Would you like to switch to the build agent and start implementing?`,
                header: "Build Agent",
                custom: false,
                options: [
                  { label: "Yes", description: "Switch to build agent and start implementing the plan" },
                  { label: "No", description: "Stay with plan agent to continue refining the plan" },
                ],
              },
            ],
            tool: ctx.callID ? { messageID: ctx.messageID, callID: ctx.callID } : undefined,
            producer: "plan_exit",
          })

          if (declined(answers)) yield* new Question.RejectedError()
          return yield* approve({ session, provider }, ctx.sessionID, plan)
        }).pipe(Effect.orDie),
    }
  }),
)

export function declined(answers: ReadonlyArray<Question.Answer>) {
  return answers[0]?.[0] === "No"
}

export const planPath = Effect.fn("PlanExitTool.planPath")(function* (
  session: Session.Interface,
  sessionID: SessionID,
) {
  const instance = yield* InstanceState.context
  const info = yield* session.get(sessionID)
  return path.relative(instance.worktree, Session.plan(info, instance))
})

// Accepting the plan: the Session continues as the build agent, told to execute the plan. A call recovered after a
// restart applies the same effect, once, through its settled ask.
export const approve = Effect.fn("PlanExitTool.approve")(function* (
  services: { session: Session.Interface; provider: Provider.Interface },
  sessionID: SessionID,
  plan: string,
) {
  const messages = yield* services.session.messages({ sessionID }).pipe(Effect.orDie)
  const lastUser = messages.findLast((item) => item.info.role === "user" && item.info.model)
  const model =
    lastUser?.info.role === "user" && lastUser.info.model
      ? lastUser.info.model
      : yield* services.provider.defaultModel()

  const msg: SessionV1.User = {
    id: MessageID.ascending(),
    sessionID,
    role: "user",
    time: { created: Date.now() },
    agent: "build",
    model,
  }
  yield* services.session.updateMessage(msg)
  yield* services.session.updatePart({
    id: PartID.ascending(),
    messageID: msg.id,
    sessionID,
    type: "text",
    text: `The plan at ${plan} has been approved, you can now edit files. Execute the plan`,
    synthetic: true,
  } satisfies SessionV1.TextPart)

  return {
    title: "Switching to build agent",
    output: "User approved switching to build agent. Wait for further instructions.",
    metadata: {},
  }
})
