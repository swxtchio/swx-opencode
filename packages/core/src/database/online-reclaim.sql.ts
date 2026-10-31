import { index, integer, sqliteTable, text } from "drizzle-orm/sqlite-core"

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
