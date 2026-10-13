import { sqliteTable, text, integer, uniqueIndex, index, primaryKey } from "drizzle-orm/sqlite-core"
import type { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import type { SessionSchema } from "./schema"
import type { MessageID } from "../v1/session"
import { SessionTable } from "./sql"

// Fork-owned V1 prompt queue (swxtchio/swx-opencode#68). A row is pending until
// it is promoted into a V1 user message (time_promoted), withdrawn for editing
// (time_withdrawn), or deleted from the review view (#212). seq orders delivery
// within a category, and a reorder trades it between pending rows. message_id
// is reserved before the message is written, so a promotion a stopped process
// left half done is finished rather than repeated. A promoted row stays until
// the next drain's history read has seen its message; a withdrawn row stays so
// it can be restored. `origins` keeps when each instruction the row carries was
// first queued, and when its identical copies were, through consolidation
// (#212); a row stored before it existed has one origin, time_created, which is
// when the row's own copy was last admitted. request_id is the sender's request
// the row delivers, whose receipt is kept in session_prompt_receipt.
// time_preparing is set while the sender that admitted the row still prepares
// the session for it (#249); such a row is listed and can be withdrawn, but is
// never promoted or merged until its sender clears it.
export const SessionPromptQueueTable = sqliteTable(
  "session_prompt_queue",
  {
    id: text().$type<SessionPromptQueue.ItemID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    seq: integer().notNull(),
    delivery: text().$type<SessionPromptQueue.Delivery>().notNull(),
    request_id: text().$type<SessionPromptQueue.RequestID>(),
    input: text({ mode: "json" }).notNull().$type<SessionPromptQueue.QueuedInputEncoded>(),
    message_id: text().$type<MessageID>(),
    time_created: integer().notNull(),
    origins: text({ mode: "json" }).$type<SessionPromptQueue.Origin[]>(),
    time_promoted: integer(),
    time_withdrawn: integer(),
    time_preparing: integer(),
  },
  (table) => [
    uniqueIndex("session_prompt_queue_session_seq_idx").on(table.session_id, table.seq),
    index("session_prompt_queue_session_delivery_seq_idx").on(table.session_id, table.delivery, table.seq),
  ],
)

// The last admission seq handed out per session, so a seq is never reused after
// its row is promoted or withdrawn.
export const SessionPromptQueueSequenceTable = sqliteTable("session_prompt_queue_sequence", {
  session_id: text()
    .$type<SessionSchema.ID>()
    .primaryKey()
    .references(() => SessionTable.id, { onDelete: "cascade" }),
  seq: integer().notNull(),
  aliases_indexed: integer({ mode: "boolean" }).notNull().default(false),
})

// Pending identity custody moves to the existing ignored text-part witnesses at promotion/consume.
export const SessionPromptQueueAliasTable = sqliteTable(
  "session_prompt_queue_alias",
  {
    id: text().notNull(),
    item_id: text()
      .$type<SessionPromptQueue.ItemID>()
      .notNull()
      .references(() => SessionPromptQueueTable.id, { onDelete: "cascade" }),
  },
  (table) => [
    primaryKey({ columns: [table.id, table.item_id] }),
    index("session_prompt_queue_alias_item_idx").on(table.item_id),
  ],
)

// The queue times of a message promoted from the queue (#212), kept as long as
// the message, so the model request can say when its prompt was queued and
// delivered while the stored message stays exactly what the sender sent.
export const SessionPromptProvenanceTable = sqliteTable(
  "session_prompt_provenance",
  {
    message_id: text().$type<MessageID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    origins: text({ mode: "json" }).notNull().$type<SessionPromptQueue.Origin[]>(),
  },
  (table) => [index("session_prompt_provenance_session_idx").on(table.session_id)],
)

// What became of each request a sender named (#212), kept as long as the session
// so the sender can confirm it by receipt rather than by finding its text. A
// pending or merged request follows the item that carries it (item_id); a
// request merged into a survivor names the survivor's request (into_request_id)
// and gets its message once the survivor has one. fingerprint is a digest of the
// prompt, so a retry is told from a reuse; no prompt text is kept here.
export const SessionPromptReceiptTable = sqliteTable(
  "session_prompt_receipt",
  {
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    request_id: text().$type<SessionPromptQueue.RequestID>().notNull(),
    state: text().$type<SessionPromptQueue.Receipt["state"]>().notNull(),
    item_id: text().$type<SessionPromptQueue.ItemID>(),
    message_id: text().$type<MessageID>(),
    into_request_id: text().$type<SessionPromptQueue.RequestID>(),
    by_request_id: text().$type<SessionPromptQueue.RequestID>(),
    fingerprint: text().notNull(),
    time_created: integer().notNull(),
    time_updated: integer().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.session_id, table.request_id] }),
    index("session_prompt_receipt_session_item_idx").on(table.session_id, table.item_id),
  ],
)
