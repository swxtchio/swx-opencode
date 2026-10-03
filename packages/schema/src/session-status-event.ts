export * as SessionStatusEvent from "./session-status-event"

import { Schema } from "effect"
import { optional } from "./schema"
import { Event } from "./event"
import { NonNegativeInt } from "./schema"
import { SessionID } from "./session-id"

// Omission is the older-server contract; null explicitly means no locally owned assistant.
const ActiveAssistantMessageID = Schema.optional(Schema.NullOr(Schema.String.check(Schema.isStartsWith("msg"))))
const RetryFields = {
  attempt: NonNegativeInt,
  message: Schema.String,
  action: optional(
    Schema.Struct({
      reason: Schema.String,
      provider: Schema.String,
      title: Schema.String,
      message: Schema.String,
      label: Schema.String,
      link: optional(Schema.String),
    }),
  ),
  next: NonNegativeInt,
}

export const Info = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("idle"),
  }),
  Schema.Struct({
    type: Schema.Literal("retry"),
    ...RetryFields,
    activeAssistantMessageID: ActiveAssistantMessageID,
  }),
  Schema.Struct({
    type: Schema.Literal("busy"),
    activeAssistantMessageID: ActiveAssistantMessageID,
  }),
]).annotate({ identifier: "SessionStatus" })
export type Info = Schema.Schema.Type<typeof Info>

export const Status = Event.define({
  type: "session.status",
  schema: {
    sessionID: SessionID,
    status: Info,
  },
})

// deprecated
export const Idle = Event.define({
  type: "session.idle",
  schema: {
    sessionID: SessionID,
  },
})

export const Definitions = Event.inventory(Status, Idle)
