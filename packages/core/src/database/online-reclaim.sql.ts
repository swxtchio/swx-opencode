import { index, integer, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core"

// Only cross-Session Part references live here. Triggers maintain this proof table;
// existing Parts are imported in bounded, resumable batches before online pruning.
export const OnlineReclaimCrossSessionPartTable = sqliteTable(
  "online_reclaim_cross_session_part",
  {
    part_id: text().primaryKey(),
    message_id: text().notNull(),
    parent_session_id: text().notNull(),
    child_session_id: text().notNull(),
  },
  (table) => [
    index("online_reclaim_cross_part_parent_idx").on(table.parent_session_id),
    index("online_reclaim_cross_part_child_idx").on(table.child_session_id),
  ],
)

export const OnlineReclaimBackfillTable = sqliteTable("online_reclaim_backfill", {
  name: text().primaryKey(),
  cursor: integer().notNull(),
  high_water: integer().notNull(),
  complete: integer().notNull(),
})

export const OnlineReclaimStableBackfillTable = sqliteTable("online_reclaim_stable_backfill", {
  name: text().primaryKey(),
  cursor: text().notNull(),
  cursor_secondary: text().notNull(),
  cursor_started: integer().notNull(),
  high_water: text().notNull(),
  high_water_secondary: text().notNull(),
  high_water_present: integer().notNull(),
  complete: integer().notNull(),
})

export const OnlineReclaimWriterLifetimeTable = sqliteTable(
  "online_reclaim_writer_lifetime",
  {
    pid: integer().notNull(),
    boot_id: text().notNull(),
    start_ticks: text().notNull(),
  },
  (table) => [primaryKey({ columns: [table.pid, table.boot_id, table.start_ticks] })],
)

// Keeps alias lookup scoped by Session without joining across the Session's entire queue history.
export const OnlineReclaimQueueAliasScopeTable = sqliteTable(
  "online_reclaim_queue_alias_scope",
  {
    alias_rowid: integer().primaryKey(),
    owner_session_id: text().notNull(),
  },
  (table) => [index("online_reclaim_queue_alias_session_rowid_idx").on(table.owner_session_id, table.alias_rowid)],
)
