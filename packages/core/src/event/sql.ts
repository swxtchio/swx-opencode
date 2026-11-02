import { sql } from "drizzle-orm"
import { sqliteTable, text, integer, index, uniqueIndex } from "drizzle-orm/sqlite-core"
import { Durable } from "@opencode-ai/schema/durable-event-manifest"
import type { EventV2 } from "../event"

const durableTypeList = [...Durable.keys()].map((type) => `'${type.replaceAll("'", "''")}'`).join(", ")

export const EventSequenceTable = sqliteTable("event_sequence", {
  aggregate_id: text().notNull().primaryKey(),
  seq: integer().notNull(),
  owner_id: text(),
})

export const EventTable = sqliteTable(
  "event",
  {
    id: text().$type<EventV2.ID>().primaryKey(),
    aggregate_id: text()
      .notNull()
      .references(() => EventSequenceTable.aggregate_id, { onDelete: "cascade" }),
    seq: integer().notNull(),
    type: text().notNull(),
    data: text({ mode: "json" }).$type<Record<string, unknown>>().notNull(),
  },
  (table) => [
    uniqueIndex("event_aggregate_seq_idx").on(table.aggregate_id, table.seq),
    index("event_aggregate_type_seq_idx").on(table.aggregate_id, table.type, table.seq),
    index("event_aggregate_unknown_type_idx")
      .on(table.aggregate_id)
      .where(sql`${table.type} NOT IN (${sql.raw(durableTypeList)})`),
  ],
)

export const EventRetentionTable = sqliteTable("event_retention", {
  aggregate_id: text().notNull().primaryKey(),
  state: text().$type<"scanning" | "redacting" | "complete">().notNull(),
  progress_table: text(),
  progress_id: text(),
  evidence: text({ mode: "json" }).$type<Record<string, unknown>>().notNull(),
  time_started: integer().notNull(),
  time_updated: integer().notNull(),
})
