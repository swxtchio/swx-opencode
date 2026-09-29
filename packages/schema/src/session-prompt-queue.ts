export * as SessionPromptQueue from "./session-prompt-queue"

import { Schema, Struct } from "effect"
import { define, inventory } from "./event"
import { ascending } from "./identifier"
import { Model } from "./model"
import { Provider } from "./provider"
import { NonNegativeInt, statics } from "./schema"
import { SessionDelivery } from "./session-delivery"
import { SessionID } from "./session-id"
import { AgentPartInput, FilePartInput, Format, MessageID, SubtaskPartInput, TextPartInput } from "./v1/session"

// Fork-owned V1 prompt queue (swxtchio/swx-opencode#68). Its names stay clear
// of V2's session_input and upstream v2's session_inbox, which a later upstream
// merge brings in.

export const ItemID = Schema.String.check(Schema.isStartsWith("que_")).pipe(
  Schema.brand("SessionPromptQueue.ItemID"),
  statics((schema) => ({ create: () => schema.make("que_" + ascending()) })),
)
export type ItemID = typeof ItemID.Type

export const Delivery = SessionDelivery.Delivery
export type Delivery = SessionDelivery.Delivery

// The V1 prompt payload. SessionPrompt.PromptInput is this plus the sessionID.
export const Input = Schema.Struct({
  messageID: Schema.optional(MessageID),
  model: Schema.optional(
    Schema.Struct({
      providerID: Provider.ID,
      modelID: Model.ID,
    }),
  ),
  agent: Schema.optional(Schema.String),
  noReply: Schema.optional(Schema.Boolean),
  tools: Schema.optional(Schema.Record(Schema.String, Schema.Boolean)).annotate({
    description:
      "@deprecated tools and permissions have been merged, you can set permissions on the session itself now",
  }),
  format: Schema.optional(Format),
  system: Schema.optional(Schema.String),
  variant: Schema.optional(Schema.String),
  delivery: Schema.optional(Delivery).annotate({
    description:
      "steer (the default) reaches the next step of an active run; queue waits until the run would otherwise go idle and then runs as its own turn",
  }),
  parts: Schema.Array(
    Schema.Union([TextPartInput, FilePartInput, AgentPartInput, SubtaskPartInput]).annotate({
      discriminator: "type",
    }),
  ),
})
export type Input = Schema.Schema.Type<typeof Input>

// What a pending item delivers. noReply prompts are never queued, and the
// item's own delivery supersedes the one it was admitted with.
export const QueuedInput = Schema.Struct(Struct.omit(Input.fields, ["noReply", "delivery"])).annotate({
  identifier: "SessionPromptQueueInput",
})
export type QueuedInput = Schema.Schema.Type<typeof QueuedInput>
// The stored form; `format` holds class instances only once decoded.
export type QueuedInputEncoded = typeof QueuedInput.Encoded

export const Item = Schema.Struct({
  id: ItemID,
  sessionID: SessionID,
  seq: NonNegativeInt.annotate({ description: "Admission order within the session; never reused" }),
  delivery: Delivery,
  input: QueuedInput,
  time: Schema.Struct({
    created: NonNegativeInt,
  }),
}).annotate({ identifier: "SessionPromptQueueItem" })
export type Item = Schema.Schema.Type<typeof Item>

const Updated = define({
  type: "session.queue.updated",
  schema: {
    sessionID: SessionID,
    items: Schema.Array(Item),
  },
})
export const Event = { Updated, Definitions: inventory(Updated) }
