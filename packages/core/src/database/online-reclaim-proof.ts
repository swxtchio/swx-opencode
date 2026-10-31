export * as OnlineReclaimProof from "./online-reclaim-proof"

import { sql } from "drizzle-orm"
import { Effect } from "effect"
import type { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { OnlineReclaimIndexes } from "./online-reclaim-indexes"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase

export const structureStatements = [
  `CREATE TABLE IF NOT EXISTS "online_reclaim_cross_session_part" (
    "part_id" TEXT PRIMARY KEY NOT NULL,
    "message_id" TEXT NOT NULL,
    "parent_session_id" TEXT NOT NULL,
    "child_session_id" TEXT NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS "online_reclaim_backfill" (
    "name" TEXT PRIMARY KEY NOT NULL,
    "cursor" INTEGER NOT NULL,
    "high_water" INTEGER NOT NULL,
    "complete" INTEGER NOT NULL
  )`,
] as const

export const triggerStatements = [
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_part_cross_insert"
  AFTER INSERT ON "part"
  BEGIN
    INSERT INTO "online_reclaim_cross_session_part" ("part_id", "message_id", "parent_session_id", "child_session_id")
    SELECT NEW."id", NEW."message_id", message."session_id", NEW."session_id"
    FROM "message" message
    WHERE message."id" = NEW."message_id" AND message."session_id" <> NEW."session_id"
    ON CONFLICT("part_id") DO UPDATE SET
      "message_id" = excluded."message_id",
      "parent_session_id" = excluded."parent_session_id",
      "child_session_id" = excluded."child_session_id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_part_cross_update"
  AFTER UPDATE OF "message_id", "session_id" ON "part"
  BEGIN
    DELETE FROM "online_reclaim_cross_session_part" WHERE "part_id" = OLD."id";
    INSERT INTO "online_reclaim_cross_session_part" ("part_id", "message_id", "parent_session_id", "child_session_id")
    SELECT NEW."id", NEW."message_id", message."session_id", NEW."session_id"
    FROM "message" message
    WHERE message."id" = NEW."message_id" AND message."session_id" <> NEW."session_id"
    ON CONFLICT("part_id") DO UPDATE SET
      "message_id" = excluded."message_id",
      "parent_session_id" = excluded."parent_session_id",
      "child_session_id" = excluded."child_session_id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_part_cross_delete"
  AFTER DELETE ON "part"
  BEGIN
    DELETE FROM "online_reclaim_cross_session_part" WHERE "part_id" = OLD."id";
  END`,
] as const

export function install(db: Database, initiallyComplete: boolean) {
  return db.transaction((tx) =>
    Effect.gen(function* () {
      const part = yield* tx.get<{ name: string }>(sql`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${"part"}
      `)
      const message = yield* tx.get<{ name: string }>(sql`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${"message"}
      `)
      if (!part || !message) return

      const existing = yield* tx.get<{ name: string }>(sql`
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name = ${"online_reclaim_cross_session_part"}
      `)
      for (const statement of structureStatements) yield* tx.run(statement)
      if (!existing) {
        for (const index of OnlineReclaimIndexes.definitions.filter(
          (item) => item.table === "online_reclaim_cross_session_part",
        ))
          yield* tx.run(index.statement)
      }
      for (const statement of triggerStatements) yield* tx.run(statement)
      yield* tx.run(sql`
        INSERT OR IGNORE INTO online_reclaim_backfill (name, cursor, high_water, complete)
        VALUES ('cross-session-parts', 0, ${initiallyComplete ? 0 : -1}, ${initiallyComplete ? 1 : 0})
      `)
      if (!existing && !initiallyComplete)
        yield* tx.run(sql`
          UPDATE online_reclaim_backfill
          SET cursor = 0, high_water = -1, complete = 0
          WHERE name = 'cross-session-parts'
        `)
    }),
  )
}
