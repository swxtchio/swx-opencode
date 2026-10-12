import { sqliteTable, text, integer, primaryKey } from "drizzle-orm/sqlite-core"

// Fork-owned cross-process writer record (swxtchio/swx-firstmate#2052). Every OS process that
// opens the database through the production Database layer registers its kernel identity here,
// so another process can tell a registered holder from an unknown one and whether it is still
// the same live process.
export const EventWriterProcessTable = sqliteTable("event_writer_process", {
  writer_id: text().primaryKey(),
  pid: integer().notNull(),
  // Null when the platform does not expose a kernel boot id or process start time; such a
  // process can never be proved gone, so retention treats its writes as held.
  boot_id: text(),
  start_ticks: text(),
  time_registered: integer().notNull(),
})

// One row per durable-event aggregate and the process that appended to it, written in the same
// transaction as the event. time_last_append is the newest append time from that process.
export const EventAggregateWriterTable = sqliteTable(
  "event_aggregate_writer",
  {
    aggregate_id: text().notNull(),
    writer_id: text().notNull(),
    time_last_append: integer().notNull(),
  },
  (table) => [primaryKey({ columns: [table.aggregate_id, table.writer_id] })],
)
