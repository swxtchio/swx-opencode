export * as OnlineReclaimIndexes from "./online-reclaim-indexes"

// These indexes belong only to trigger-maintained proof tables, which are empty when installed.
// Existing source-table indexes are supplied by their owning database migrations.
export const definitions = [
  {
    table: "online_reclaim_cross_session_part",
    name: "online_reclaim_cross_part_parent_idx",
    statement: `CREATE INDEX IF NOT EXISTS "online_reclaim_cross_part_parent_idx" ON "online_reclaim_cross_session_part" ("parent_session_id");`,
  },
  {
    table: "online_reclaim_cross_session_part",
    name: "online_reclaim_cross_part_child_idx",
    statement: `CREATE INDEX IF NOT EXISTS "online_reclaim_cross_part_child_idx" ON "online_reclaim_cross_session_part" ("child_session_id");`,
  },
  {
    table: "online_reclaim_queue_alias_scope",
    name: "online_reclaim_queue_alias_session_rowid_idx",
    statement: `CREATE INDEX IF NOT EXISTS "online_reclaim_queue_alias_session_rowid_idx" ON "online_reclaim_queue_alias_scope" ("owner_session_id","alias_rowid");`,
  },
  {
    table: "online_reclaim_queue_alias_owner",
    name: "online_reclaim_queue_alias_owner_session_idx",
    statement: `CREATE INDEX IF NOT EXISTS "online_reclaim_queue_alias_owner_session_idx" ON "online_reclaim_queue_alias_owner" ("owner_session_id","item_id","alias_id");`,
  },
  {
    table: "online_reclaim_cross_part_scope",
    name: "online_reclaim_cross_part_scope_owner_idx",
    statement: `CREATE INDEX IF NOT EXISTS "online_reclaim_cross_part_scope_owner_idx" ON "online_reclaim_cross_part_scope" ("session_id","part_id") WHERE "role" IN ('owned', 'child');`,
  },
  {
    table: "online_reclaim_cross_part_scope",
    name: "online_reclaim_cross_part_scope_cross_idx",
    statement: `CREATE INDEX IF NOT EXISTS "online_reclaim_cross_part_scope_cross_idx" ON "online_reclaim_cross_part_scope" ("session_id","part_id") WHERE "role" IN ('parent', 'child');`,
  },
] satisfies readonly { table: string; name: string; statement: string }[]
