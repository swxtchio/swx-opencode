import { sqliteTable, text, integer, uniqueIndex, index } from "drizzle-orm/sqlite-core"
import type { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import type { SessionSchema } from "./schema"
import type { MessageID } from "../v1/session"
import { SessionTable } from "./sql"

// Fork-owned V1 prompt queue (swxtchio/swx-opencode#68). A row is pending until
// it is promoted into a V1 user message (time_promoted, message_id) or withdrawn
// for editing (time_withdrawn). A promoted row stays until the next drain's
// history read has seen its message; a withdrawn row stays so it can be restored.
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
    input: text({ mode: "json" }).notNull().$type<SessionPromptQueue.QueuedInput>(),
    message_id: text().$type<MessageID>(),
    time_created: integer().notNull(),
    time_promoted: integer(),
    time_withdrawn: integer(),
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
})
