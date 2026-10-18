export * as SessionAsk from "./session-ask"

import { Schema } from "effect"
import { SessionID } from "./session-id"
import { QuestionV1 } from "./v1/question"
import { PermissionV1 } from "./v1/permission"

// Fork-owned supervisor delivery of model-raised asks (swxtchio/swx-opencode#286).
// An ask is a question tool request, a plan acceptance (a question the
// plan_exit tool raises) or a permission request. With `--questions-command` each
// one is recorded durably, emitted to the operator's command, and settled once
// by whichever resolver answers it first.

export const Kind = Schema.Literals(["question", "permission"]).annotate({ identifier: "SessionAskKind" })
export type Kind = typeof Kind.Type

export const Answered = Schema.Struct({
  type: Schema.Literal("answered"),
  answers: Schema.Array(QuestionV1.Answer),
}).annotate({ identifier: "SessionAskAnswered" })
export const Rejected = Schema.Struct({ type: Schema.Literal("rejected") }).annotate({
  identifier: "SessionAskRejected",
})
export const Replied = Schema.Struct({
  type: Schema.Literal("replied"),
  reply: PermissionV1.Reply,
  message: Schema.optional(Schema.String),
}).annotate({ identifier: "SessionAskReplied" })
// The owning tool stopped waiting (the turn was aborted or its instance was
// disposed) before anyone answered, so no later answer can reach it.
export const Cancelled = Schema.Struct({ type: Schema.Literal("cancelled") }).annotate({
  identifier: "SessionAskCancelled",
})
// The asking process is gone and its call provably cannot take a decision: it never had a tool part of its own, its
// message was deleted, or its Session went on. The ask is closed with nothing applied.
export const Undelivered = Schema.Struct({ type: Schema.Literal("undelivered") }).annotate({
  identifier: "SessionAskUndelivered",
})
export const Decision = Schema.Union([Answered, Rejected, Replied, Cancelled, Undelivered]).annotate({
  identifier: "SessionAskDecision",
})
export type Decision = typeof Decision.Type

// "settled": this answer settled the request. "duplicate": an earlier resolver
// already had, and `decision` is that winner's; this answer changed nothing.
export const Disposition = Schema.Literals(["settled", "duplicate"]).annotate({ identifier: "SessionAskDisposition" })
export type Disposition = typeof Disposition.Type

export const QuestionAnswer = Schema.Union([
  Schema.Struct({
    answers: Schema.Array(QuestionV1.Answer).annotate({
      description: "Answers in order of questions; each is the selected labels or custom text",
    }),
  }),
  Schema.Struct({ reject: Schema.Literal(true) }),
]).annotate({ identifier: "SessionAskQuestionAnswer" })
export type QuestionAnswer = typeof QuestionAnswer.Type

export const Result = Schema.Struct({
  requestID: Schema.String,
  sessionID: SessionID,
  disposition: Disposition,
  decision: Decision,
  // True when the owning process was gone and the answer was applied to the
  // stored tool call and its Session instead of a live waiting tool.
  recovered: Schema.Boolean,
}).annotate({ identifier: "SessionAskResult" })
export type Result = typeof Result.Type

// What the operator's command receives on stdin, one JSON document per ask.
export const Emitted = Schema.Struct({
  type: Schema.Literal("opencode.ask"),
  version: Schema.Literal(1),
  kind: Kind,
  id: Schema.String,
  sessionID: SessionID,
  parentSessionID: Schema.optional(SessionID),
  directory: Schema.String,
  workspaceID: Schema.optional(Schema.String),
  server: Schema.optional(Schema.String),
  origin: Schema.Struct({
    messageID: Schema.optional(Schema.String),
    callID: Schema.optional(Schema.String),
    tool: Schema.optional(Schema.String),
  }),
  question: Schema.optional(QuestionV1.Request),
  permission: Schema.optional(PermissionV1.Request),
  answer: Schema.Struct({
    method: Schema.Literal("POST"),
    path: Schema.String,
    query: Schema.Struct({ directory: Schema.String, workspace: Schema.optional(Schema.String) }),
  }),
}).annotate({ identifier: "SessionAskEmitted" })
export type Emitted = typeof Emitted.Type
