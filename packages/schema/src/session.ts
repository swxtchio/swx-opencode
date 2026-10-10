export * as Session from "./session"

import { Schema } from "effect"
import { Agent } from "./agent"
import { Location } from "./location"
import { Model } from "./model"
import { Project } from "./project"
import { DateTimeUtcFromMillis, NonNegativeInt, optional, RelativePath } from "./schema"
import { SessionEvent } from "./session-event"
import { SessionID } from "./session-id"
import { SessionMessage } from "./session-message"
import { Revert } from "./revert"

export const ID = SessionID
export type ID = SessionID

export const Event = SessionEvent

export interface Info extends Schema.Schema.Type<typeof Info> {}
export const Info = Schema.Struct({
  id: ID,
  parentID: ID.pipe(optional),
  projectID: Project.ID,
  agent: Agent.ID.pipe(optional),
  model: Model.Ref.pipe(optional),
  cost: Schema.Finite,
  tokens: Schema.Struct({
    input: Schema.Finite,
    output: Schema.Finite,
    reasoning: Schema.Finite,
    cache: Schema.Struct({
      read: Schema.Finite,
      write: Schema.Finite,
    }),
  }),
  time: Schema.Struct({
    created: DateTimeUtcFromMillis,
    updated: DateTimeUtcFromMillis,
    archived: DateTimeUtcFromMillis.pipe(optional),
  }),
  title: Schema.String,
  location: Location.Ref,
  subpath: RelativePath.pipe(optional),
  revert: Revert.State.pipe(optional),
}).annotate({ identifier: "SessionV2.Info" })

export type WaitResult = typeof WaitResult.Type
export const WaitResult = Schema.Union([
  Schema.Struct({ type: Schema.Literal("idle") }),
  Schema.Struct({ type: Schema.Literal("pending"), admittedSeq: NonNegativeInt, messageID: SessionMessage.ID }),
  Schema.Struct({
    type: Schema.Literal("completed"),
    admittedSeq: NonNegativeInt,
    assistantMessageID: SessionMessage.ID,
  }),
  Schema.Struct({
    type: Schema.Literal("failed"),
    admittedSeq: NonNegativeInt.pipe(optional),
    assistantMessageID: SessionMessage.ID.pipe(optional),
  }),
  Schema.Struct({
    type: Schema.Literal("interrupted"),
    admittedSeq: NonNegativeInt.pipe(optional),
    assistantMessageID: SessionMessage.ID.pipe(optional),
  }),
])
  .pipe(Schema.toTaggedUnion("type"))
  .annotate({ identifier: "Session.WaitResult" })

export const ListAnchor = Schema.Struct({
  id: ID,
  time: Schema.Finite,
  direction: Schema.Literals(["previous", "next"]),
}).annotate({ identifier: "Session.ListAnchor" })
export interface ListAnchor extends Schema.Schema.Type<typeof ListAnchor> {}
