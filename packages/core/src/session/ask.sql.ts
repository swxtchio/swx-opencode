import { sqliteTable, text, integer, index } from "drizzle-orm/sqlite-core"
import type { SessionAsk } from "@opencode-ai/schema/session-ask"
import type { QuestionV1 } from "@opencode-ai/schema/question-v1"
import type { PermissionV1 } from "@opencode-ai/schema/permission-v1"
import type { SessionSchema } from "./schema"
import { SessionTable } from "./sql"

// Fork-owned supervisor ask ledger (swxtchio/swx-opencode#286). Written only
// under `--questions emit`: one row per question or permission request the
// process emitted, pending until time_settled. decision is the first
// resolver's, and a later answer reports it instead of acting. writer_id is the
// event_writer_process that held the waiting tool, so an answer after that
// process is gone can recover the stored tool call, while an answer reaching a
// different live process is refused. directory scopes the answer route.
export const SessionAskTable = sqliteTable(
  "session_ask",
  {
    id: text().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    kind: text().$type<SessionAsk.Kind>().notNull(),
    directory: text().notNull(),
    tool: text(),
    request: text({ mode: "json" }).notNull().$type<typeof QuestionV1.Request.Type | PermissionV1.Request>(),
    writer_id: text().notNull(),
    decision: text({ mode: "json" }).$type<SessionAsk.Decision>(),
    recovered: integer({ mode: "boolean" }).notNull().default(false),
    time_created: integer().notNull(),
    time_settled: integer(),
  },
  (table) => [
    index("session_ask_session_idx").on(table.session_id),
    index("session_ask_directory_settled_idx").on(table.directory, table.time_settled),
  ],
)
