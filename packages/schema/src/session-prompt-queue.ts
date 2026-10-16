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

// A sender's name for one logical request, unique within its session (#212).
// A peer send uses its msgid. Its receipt says what became of the request.
export const RequestID = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._:-]{1,128}$/)).annotate({
  identifier: "SessionPromptQueueRequestID",
})
export type RequestID = typeof RequestID.Type

export const TuiPromptAppendMetadataKey = "tuiPromptAppend"

// The V1 prompt payload. SessionPrompt.PromptInput is this plus the sessionID.
export const Input = Schema.Struct({
  requestID: Schema.optional(RequestID).annotate({
    description:
      "The sender's name for this request, unique within the session. Sending it again with an identical prompt is a retry of the same request, and with a different prompt a conflict; GET /session/:id/queue/receipt/:requestID says what became of it",
  }),
  messageID: Schema.optional(MessageID),
  model: Schema.optional(
    Schema.Struct({
      providerID: Provider.ID,
      modelID: Model.ID,
    }),
  ),
  agent: Schema.optional(Schema.String),
  origin: Schema.optional(Schema.Literal("tui-composer")).annotate({
    description: "The prompt came through the interactive TUI composer; machine-message markers still classify it.",
  }),
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
      "steer (the default) reaches the next step of an active run; queue waits until the run would otherwise go idle; validated compatible peer envelopes also reach a safe provider-continuation boundary",
  }),
  parts: Schema.Array(
    Schema.Union([TextPartInput, FilePartInput, AgentPartInput, SubtaskPartInput]).annotate({
      discriminator: "type",
    }),
  ),
})
export type Input = Schema.Schema.Type<typeof Input>

// What a pending item delivers. noReply controls the admission call; a held
// noReply can be queued and becomes a normal turn when promoted. The item's own
// delivery supersedes the one it was admitted with.
export const QueuedInput = Schema.Struct(Struct.omit(Input.fields, ["noReply", "delivery", "requestID"])).annotate({
  identifier: "SessionPromptQueueInput",
})
export type QueuedInput = Schema.Schema.Type<typeof QueuedInput>
// The stored form; `format` holds class instances only once decoded.
export type QueuedInputEncoded = typeof QueuedInput.Encoded

// When an instruction was first queued, and when copies of it were queued
// after it and collapsed into it (#212), each with the request that sent it.
export const Repeat = Schema.Struct({
  time: NonNegativeInt,
  requestID: Schema.optional(RequestID),
}).annotate({ identifier: "SessionPromptQueueRepeat" })
export type Repeat = Schema.Schema.Type<typeof Repeat>

export const Origin = Schema.Struct({
  time: NonNegativeInt,
  requestID: Schema.optional(RequestID),
  repeats: Schema.optional(Schema.Array(Repeat)),
}).annotate({ identifier: "SessionPromptQueueOrigin" })
export type Origin = Schema.Schema.Type<typeof Origin>

export const Item = Schema.Struct({
  id: ItemID,
  sessionID: SessionID,
  seq: NonNegativeInt.annotate({
    description:
      "Delivery order within the session: admission order until a reorder trades it between pending items; never reused",
  }),
  delivery: Delivery,
  requestID: Schema.optional(RequestID).annotate({
    description: "The request this item delivers; a request merged into it names it as its survivor",
  }),
  input: QueuedInput,
  time: Schema.Struct({
    created: NonNegativeInt.annotate({
      description: "When this item's own copy was last admitted; origins say when its instructions were first queued",
    }),
  }),
  origins: Schema.Array(Origin).annotate({
    description:
      "One per instruction the item carries, in order: a merged command has one per step; repeats are copies of it collapsed into it",
  }),
}).annotate({ identifier: "SessionPromptQueueItem" })
export type Item = Schema.Schema.Type<typeof Item>

// Stored on the delivered text part, so consume does not erase exact original inputs.
// This is constituent introspection, not an admission or acknowledgment receipt.
// Keep v1 fields independent of the pending editor Item's future additions (for example origins).
const Constituent = Schema.Struct({
  id: ItemID,
  sessionID: SessionID,
  seq: NonNegativeInt,
  delivery: Delivery,
  input: QueuedInput,
  time: Schema.Struct({ created: NonNegativeInt }),
})
export const Group = Schema.Struct({
  version: Schema.Literal(1),
  sessionID: SessionID,
  kind: Schema.String,
  constituents: Schema.Array(Constituent),
})
export type Group = Schema.Schema.Type<typeof Group>

// What became of a request (#212). Merged: its instruction is delivered as part
// of the survivor `into`, once that has a message. Superseded: a newer request
// of the same kind, `by`, retired it undelivered. Withdrawn: taken out of the
// queue undelivered, for example to be edited and sent again as a new request
// (#249); restoring the item makes it pending again. Deleted: removed from the
// queue undelivered, by its reviewer or because it could not become a message.
export const Receipt = Schema.Union([
  Schema.Struct({ state: Schema.Literal("pending"), itemID: ItemID }),
  Schema.Struct({ state: Schema.Literal("withdrawn"), itemID: ItemID }),
  Schema.Struct({ state: Schema.Literal("delivered"), messageID: MessageID }),
  Schema.Struct({ state: Schema.Literal("merged"), into: RequestID, messageID: Schema.optional(MessageID) }),
  Schema.Struct({ state: Schema.Literal("superseded"), by: RequestID }),
  Schema.Struct({ state: Schema.Literal("deleted") }),
]).annotate({ identifier: "SessionPromptQueueReceipt", discriminator: "state" })
export type Receipt = Schema.Schema.Type<typeof Receipt>

const Updated = define({
  type: "session.queue.updated",
  schema: {
    sessionID: SessionID,
    items: Schema.Array(Item),
  },
})
export const Event = { Updated, Definitions: inventory(Updated) }
