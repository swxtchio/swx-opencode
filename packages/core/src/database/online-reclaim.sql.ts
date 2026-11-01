import { index, integer, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { sql } from "drizzle-orm"

// Legacy cross-Session Part map retained for older binaries. New pruning uses the
// owner-scoped maps below and fills them in bounded online-apply transactions.
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

// Stable alias identity: the queue alias source table has no INTEGER PRIMARY KEY.
export const OnlineReclaimQueueAliasOwnerTable = sqliteTable(
  "online_reclaim_queue_alias_owner",
  {
    item_id: text().notNull(),
    alias_id: text().notNull(),
    owner_session_id: text().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.item_id, table.alias_id] }),
    index("online_reclaim_queue_alias_owner_session_idx").on(table.owner_session_id, table.item_id, table.alias_id),
  ],
)

// Store Parts by owner Session and cross-Session role, avoiding a full-table index on `part.session_id`.
export const OnlineReclaimCrossPartScopeTable = sqliteTable(
  "online_reclaim_cross_part_scope",
  {
    session_id: text().notNull(),
    part_id: text().notNull(),
    message_id: text().notNull(),
    parent_session_id: text().notNull(),
    child_session_id: text().notNull(),
    role: text().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.session_id, table.part_id] }),
    index("online_reclaim_cross_part_scope_owner_idx")
      .on(table.session_id, table.part_id)
      .where(sql`${table.role} IN ('owned', 'child')`),
    index("online_reclaim_cross_part_scope_cross_idx")
      .on(table.session_id, table.part_id)
      .where(sql`${table.role} IN ('parent', 'child')`),
  ],
)

// Keyed by Message and child Session for the locked pre-cascade reference check.
export const OnlineReclaimCrossPartMessageTable = sqliteTable(
  "online_reclaim_cross_part_message",
  {
    message_id: text().notNull(),
    child_session_id: text().notNull(),
    part_id: text().notNull(),
    parent_session_id: text().notNull(),
  },
  (table) => [primaryKey({ columns: [table.message_id, table.child_session_id, table.part_id] })],
)

// Legacy rowid map retained for databases opened by older binaries. New pruning uses the stable owner map above.
export const OnlineReclaimQueueAliasScopeTable = sqliteTable(
  "online_reclaim_queue_alias_scope",
  {
    alias_rowid: integer().primaryKey(),
    owner_session_id: text().notNull(),
  },
  (table) => [index("online_reclaim_queue_alias_session_rowid_idx").on(table.owner_session_id, table.alias_rowid)],
)
