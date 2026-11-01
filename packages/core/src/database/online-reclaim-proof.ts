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
  `CREATE TABLE IF NOT EXISTS "online_reclaim_queue_alias_scope" (
    "alias_rowid" INTEGER PRIMARY KEY NOT NULL,
    "owner_session_id" TEXT NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS "online_reclaim_backfill" (
    "name" TEXT PRIMARY KEY NOT NULL,
    "cursor" INTEGER NOT NULL,
    "high_water" INTEGER NOT NULL,
    "complete" INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS "online_reclaim_stable_backfill" (
    "name" TEXT PRIMARY KEY NOT NULL,
    "cursor" TEXT NOT NULL,
    "cursor_secondary" TEXT NOT NULL,
    "cursor_started" INTEGER NOT NULL,
    "high_water" TEXT NOT NULL,
    "high_water_secondary" TEXT NOT NULL,
    "high_water_present" INTEGER NOT NULL,
    "complete" INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS "online_reclaim_writer_lifetime" (
    "pid" INTEGER NOT NULL,
    "boot_id" TEXT NOT NULL,
    "start_ticks" TEXT NOT NULL,
    PRIMARY KEY ("pid", "boot_id", "start_ticks")
  )`,
  `CREATE TABLE IF NOT EXISTS "online_reclaim_queue_alias_owner" (
    "item_id" TEXT NOT NULL,
    "alias_id" TEXT NOT NULL,
    "owner_session_id" TEXT NOT NULL,
    PRIMARY KEY ("item_id", "alias_id")
  )`,
  `CREATE TABLE IF NOT EXISTS "online_reclaim_cross_part_scope" (
    "session_id" TEXT NOT NULL,
    "part_id" TEXT NOT NULL,
    "message_id" TEXT NOT NULL,
    "parent_session_id" TEXT NOT NULL,
    "child_session_id" TEXT NOT NULL,
    "role" TEXT NOT NULL,
    PRIMARY KEY ("session_id", "part_id")
  )`,
  `CREATE TABLE IF NOT EXISTS "online_reclaim_cross_part_message" (
    "message_id" TEXT NOT NULL,
    "child_session_id" TEXT NOT NULL,
    "part_id" TEXT NOT NULL,
    "parent_session_id" TEXT NOT NULL,
    PRIMARY KEY ("message_id", "child_session_id", "part_id")
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
    INSERT INTO "online_reclaim_cross_part_scope" (
      "session_id", "part_id", "message_id", "parent_session_id", "child_session_id", "role"
    )
    SELECT NEW."session_id", NEW."id", NEW."message_id", message."session_id", NEW."session_id",
      CASE WHEN message."session_id" = NEW."session_id" THEN 'owned' ELSE 'child' END
    FROM "message" message
    WHERE message."id" = NEW."message_id"
    ON CONFLICT("session_id", "part_id") DO UPDATE SET
      "message_id" = excluded."message_id",
      "parent_session_id" = excluded."parent_session_id",
      "child_session_id" = excluded."child_session_id",
      "role" = excluded."role";
    INSERT INTO "online_reclaim_cross_part_scope" (
      "session_id", "part_id", "message_id", "parent_session_id", "child_session_id", "role"
    )
    SELECT message."session_id", NEW."id", NEW."message_id", message."session_id", NEW."session_id", 'parent'
    FROM "message" message
    WHERE message."id" = NEW."message_id" AND message."session_id" <> NEW."session_id"
    ON CONFLICT("session_id", "part_id") DO UPDATE SET
      "message_id" = excluded."message_id",
      "parent_session_id" = excluded."parent_session_id",
      "child_session_id" = excluded."child_session_id",
      "role" = excluded."role";
    INSERT INTO "online_reclaim_cross_part_message" (
      "message_id", "child_session_id", "part_id", "parent_session_id"
    )
    SELECT NEW."message_id", NEW."session_id", NEW."id", message."session_id"
    FROM "message" message
    WHERE message."id" = NEW."message_id" AND message."session_id" <> NEW."session_id"
    ON CONFLICT("message_id", "child_session_id", "part_id") DO UPDATE SET
      "parent_session_id" = excluded."parent_session_id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_part_cross_update"
  AFTER UPDATE OF "id", "message_id", "session_id" ON "part"
  BEGIN
    DELETE FROM "online_reclaim_cross_session_part" WHERE "part_id" = OLD."id";
    DELETE FROM "online_reclaim_cross_part_scope" WHERE "part_id" = OLD."id";
    DELETE FROM "online_reclaim_cross_part_message" WHERE "part_id" = OLD."id";
    INSERT INTO "online_reclaim_cross_session_part" ("part_id", "message_id", "parent_session_id", "child_session_id")
    SELECT NEW."id", NEW."message_id", message."session_id", NEW."session_id"
    FROM "message" message
    WHERE message."id" = NEW."message_id" AND message."session_id" <> NEW."session_id"
    ON CONFLICT("part_id") DO UPDATE SET
      "message_id" = excluded."message_id",
      "parent_session_id" = excluded."parent_session_id",
      "child_session_id" = excluded."child_session_id";
    INSERT INTO "online_reclaim_cross_part_scope" (
      "session_id", "part_id", "message_id", "parent_session_id", "child_session_id", "role"
    )
    SELECT NEW."session_id", NEW."id", NEW."message_id", message."session_id", NEW."session_id",
      CASE WHEN message."session_id" = NEW."session_id" THEN 'owned' ELSE 'child' END
    FROM "message" message
    WHERE message."id" = NEW."message_id"
    ON CONFLICT("session_id", "part_id") DO UPDATE SET
      "message_id" = excluded."message_id",
      "parent_session_id" = excluded."parent_session_id",
      "child_session_id" = excluded."child_session_id",
      "role" = excluded."role";
    INSERT INTO "online_reclaim_cross_part_scope" (
      "session_id", "part_id", "message_id", "parent_session_id", "child_session_id", "role"
    )
    SELECT message."session_id", NEW."id", NEW."message_id", message."session_id", NEW."session_id", 'parent'
    FROM "message" message
    WHERE message."id" = NEW."message_id" AND message."session_id" <> NEW."session_id"
    ON CONFLICT("session_id", "part_id") DO UPDATE SET
      "message_id" = excluded."message_id",
      "parent_session_id" = excluded."parent_session_id",
      "child_session_id" = excluded."child_session_id",
      "role" = excluded."role";
    INSERT INTO "online_reclaim_cross_part_message" (
      "message_id", "child_session_id", "part_id", "parent_session_id"
    )
    SELECT NEW."message_id", NEW."session_id", NEW."id", message."session_id"
    FROM "message" message
    WHERE message."id" = NEW."message_id" AND message."session_id" <> NEW."session_id"
    ON CONFLICT("message_id", "child_session_id", "part_id") DO UPDATE SET
      "parent_session_id" = excluded."parent_session_id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_part_cross_delete"
  AFTER DELETE ON "part"
  BEGIN
    DELETE FROM "online_reclaim_cross_session_part" WHERE "part_id" = OLD."id";
    DELETE FROM "online_reclaim_cross_part_scope" WHERE "part_id" = OLD."id";
    DELETE FROM "online_reclaim_cross_part_message" WHERE "part_id" = OLD."id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_message_cross_update"
  AFTER UPDATE OF "session_id" ON "message"
  WHEN NEW."session_id" <> OLD."session_id"
  BEGIN
    DELETE FROM "online_reclaim_cross_session_part" WHERE "message_id" = NEW."id";
    DELETE FROM "online_reclaim_cross_part_scope"
    WHERE "part_id" IN (SELECT "id" FROM "part" WHERE "message_id" = NEW."id");
    DELETE FROM "online_reclaim_cross_part_message" WHERE "message_id" = NEW."id";
    INSERT INTO "online_reclaim_cross_session_part" (
      "part_id", "message_id", "parent_session_id", "child_session_id"
    )
    SELECT part."id", NEW."id", NEW."session_id", part."session_id"
    FROM "part" part
    WHERE part."message_id" = NEW."id" AND part."session_id" <> NEW."session_id"
    ON CONFLICT("part_id") DO UPDATE SET
      "message_id" = excluded."message_id",
      "parent_session_id" = excluded."parent_session_id",
      "child_session_id" = excluded."child_session_id";
    INSERT INTO "online_reclaim_cross_part_scope" (
      "session_id", "part_id", "message_id", "parent_session_id", "child_session_id", "role"
    )
    SELECT part."session_id", part."id", NEW."id", NEW."session_id", part."session_id",
      CASE WHEN part."session_id" = NEW."session_id" THEN 'owned' ELSE 'child' END
    FROM "part" part
    WHERE part."message_id" = NEW."id"
    ON CONFLICT("session_id", "part_id") DO UPDATE SET
      "message_id" = excluded."message_id",
      "parent_session_id" = excluded."parent_session_id",
      "child_session_id" = excluded."child_session_id",
      "role" = excluded."role";
    INSERT INTO "online_reclaim_cross_part_scope" (
      "session_id", "part_id", "message_id", "parent_session_id", "child_session_id", "role"
    )
    SELECT NEW."session_id", part."id", NEW."id", NEW."session_id", part."session_id", 'parent'
    FROM "part" part
    WHERE part."message_id" = NEW."id" AND part."session_id" <> NEW."session_id"
    ON CONFLICT("session_id", "part_id") DO UPDATE SET
      "message_id" = excluded."message_id",
      "parent_session_id" = excluded."parent_session_id",
      "child_session_id" = excluded."child_session_id",
      "role" = excluded."role";
    INSERT INTO "online_reclaim_cross_part_message" (
      "message_id", "child_session_id", "part_id", "parent_session_id"
    )
    SELECT NEW."id", part."session_id", part."id", NEW."session_id"
    FROM "part" part
    WHERE part."message_id" = NEW."id" AND part."session_id" <> NEW."session_id"
    ON CONFLICT("message_id", "child_session_id", "part_id") DO UPDATE SET
      "parent_session_id" = excluded."parent_session_id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_queue_alias_scope_insert"
  AFTER INSERT ON "session_prompt_queue_alias"
  BEGIN
    INSERT INTO "online_reclaim_queue_alias_scope" ("alias_rowid", "owner_session_id")
    SELECT NEW.rowid, queue."session_id"
    FROM "session_prompt_queue" queue
    WHERE queue."id" = NEW."item_id"
    ON CONFLICT("alias_rowid") DO UPDATE SET "owner_session_id" = excluded."owner_session_id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_queue_alias_scope_update"
  AFTER UPDATE OF "item_id" ON "session_prompt_queue_alias"
  BEGIN
    DELETE FROM "online_reclaim_queue_alias_scope" WHERE "alias_rowid" = OLD.rowid;
    INSERT INTO "online_reclaim_queue_alias_scope" ("alias_rowid", "owner_session_id")
    SELECT NEW.rowid, queue."session_id"
    FROM "session_prompt_queue" queue
    WHERE queue."id" = NEW."item_id"
    ON CONFLICT("alias_rowid") DO UPDATE SET "owner_session_id" = excluded."owner_session_id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_queue_alias_scope_delete"
  AFTER DELETE ON "session_prompt_queue_alias"
  BEGIN
    DELETE FROM "online_reclaim_queue_alias_scope" WHERE "alias_rowid" = OLD.rowid;
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_queue_scope_update"
  AFTER UPDATE OF "session_id" ON "session_prompt_queue"
  WHEN NEW."session_id" <> OLD."session_id"
  BEGIN
    UPDATE "online_reclaim_queue_alias_scope"
    SET "owner_session_id" = NEW."session_id"
    WHERE "alias_rowid" IN (
      SELECT alias.rowid FROM "session_prompt_queue_alias" alias WHERE alias."item_id" = NEW."id"
    );
    UPDATE "online_reclaim_queue_alias_owner"
    SET "owner_session_id" = NEW."session_id"
    WHERE "item_id" = NEW."id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_queue_alias_owner_insert"
  AFTER INSERT ON "session_prompt_queue_alias"
  BEGIN
    INSERT INTO "online_reclaim_queue_alias_owner" ("item_id", "alias_id", "owner_session_id")
    SELECT NEW."item_id", NEW."id", queue."session_id"
    FROM "session_prompt_queue" queue
    WHERE queue."id" = NEW."item_id"
    ON CONFLICT("item_id", "alias_id") DO UPDATE SET "owner_session_id" = excluded."owner_session_id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_queue_alias_owner_update"
  AFTER UPDATE OF "id", "item_id" ON "session_prompt_queue_alias"
  BEGIN
    DELETE FROM "online_reclaim_queue_alias_owner"
    WHERE "item_id" = OLD."item_id" AND "alias_id" = OLD."id";
    INSERT INTO "online_reclaim_queue_alias_owner" ("item_id", "alias_id", "owner_session_id")
    SELECT NEW."item_id", NEW."id", queue."session_id"
    FROM "session_prompt_queue" queue
    WHERE queue."id" = NEW."item_id"
    ON CONFLICT("item_id", "alias_id") DO UPDATE SET "owner_session_id" = excluded."owner_session_id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_queue_alias_owner_delete"
  AFTER DELETE ON "session_prompt_queue_alias"
  BEGIN
    DELETE FROM "online_reclaim_queue_alias_owner"
    WHERE "item_id" = OLD."item_id" AND "alias_id" = OLD."id";
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_writer_lifetime_insert"
  AFTER INSERT ON "event_writer_process"
  WHEN NEW."boot_id" IS NOT NULL AND NEW."start_ticks" IS NOT NULL
  BEGIN
    INSERT INTO "online_reclaim_writer_lifetime" ("pid", "boot_id", "start_ticks")
    VALUES (NEW."pid", NEW."boot_id", NEW."start_ticks")
    ON CONFLICT("pid", "boot_id", "start_ticks") DO NOTHING;
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_writer_lifetime_update"
  AFTER UPDATE OF "pid", "boot_id", "start_ticks" ON "event_writer_process"
  BEGIN
    DELETE FROM "online_reclaim_writer_lifetime"
    WHERE "pid" = OLD."pid" AND "boot_id" = OLD."boot_id" AND "start_ticks" = OLD."start_ticks";
    INSERT INTO "online_reclaim_writer_lifetime" ("pid", "boot_id", "start_ticks")
    SELECT NEW."pid", NEW."boot_id", NEW."start_ticks"
    WHERE NEW."boot_id" IS NOT NULL AND NEW."start_ticks" IS NOT NULL
    ON CONFLICT("pid", "boot_id", "start_ticks") DO NOTHING;
  END`,
  `CREATE TRIGGER IF NOT EXISTS "online_reclaim_writer_lifetime_delete"
  AFTER DELETE ON "event_writer_process"
  BEGIN
    DELETE FROM "online_reclaim_writer_lifetime"
    WHERE "pid" = OLD."pid" AND "boot_id" = OLD."boot_id" AND "start_ticks" = OLD."start_ticks";
  END`,
] as const

export const requiredTriggerNames = [
  "online_reclaim_part_cross_insert",
  "online_reclaim_part_cross_update",
  "online_reclaim_part_cross_delete",
  "online_reclaim_queue_alias_scope_insert",
  "online_reclaim_queue_alias_scope_update",
  "online_reclaim_queue_alias_scope_delete",
  "online_reclaim_queue_scope_update",
  "online_reclaim_writer_lifetime_insert",
  "online_reclaim_writer_lifetime_update",
  "online_reclaim_writer_lifetime_delete",
  "online_reclaim_queue_alias_owner_insert",
  "online_reclaim_queue_alias_owner_update",
  "online_reclaim_queue_alias_owner_delete",
  "online_reclaim_message_cross_update",
] as const
const proofTables = [
  "online_reclaim_cross_session_part",
  "online_reclaim_queue_alias_scope",
  "online_reclaim_backfill",
  "online_reclaim_stable_backfill",
  "online_reclaim_writer_lifetime",
  "online_reclaim_queue_alias_owner",
  "online_reclaim_cross_part_scope",
  "online_reclaim_cross_part_message",
] as const
const backfillNames = ["cross-session-part-scope"] as const
const stableBackfillNames = ["event-writer-lifetimes", "queue-alias-owners", "cross-session-part-scope"] as const

export function install(db: Database, initiallyComplete: boolean) {
  return Effect.gen(function* () {
    const baseTables = new Set<string>()
    for (const name of ["part", "message", "session_prompt_queue", "session_prompt_queue_alias"]) {
      if (
        yield* db.get<{ name: string }>(sql`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${name}
      `)
      )
        baseTables.add(name)
    }
    if (baseTables.size !== 4) return

    const objects = new Set(
      (yield* db.all<{ type: string; name: string }>(sql`
        SELECT type, name FROM sqlite_master WHERE type IN ('table', 'trigger', 'index')
      `)).map((row) => `${row.type}:${row.name}`),
    )
    if (!objects.has("table:online_reclaim_backfill") || !objects.has("table:online_reclaim_stable_backfill")) {
      yield* installMissing(db, initiallyComplete)
      return
    }
    const completedRows = new Set(
      (yield* db.all<{ name: string }>(sql`
        SELECT name FROM online_reclaim_backfill WHERE name IN (${backfillNames[0]})
      `)).map((row) => row.name),
    )
    const completedStableRows = new Set(
      (yield* db.all<{ name: string }>(sql`
        SELECT name FROM online_reclaim_stable_backfill
      `)).map((row) => row.name),
    )
    const crossHasRows = objects.has("table:online_reclaim_cross_session_part")
      ? Boolean(yield* db.get(sql`SELECT part_id FROM online_reclaim_cross_session_part LIMIT 1`))
      : false
    const aliasesHaveRows = objects.has("table:online_reclaim_queue_alias_scope")
      ? Boolean(yield* db.get(sql`SELECT alias_rowid FROM online_reclaim_queue_alias_scope LIMIT 1`))
      : false
    const aliasOwnersHaveRows = objects.has("table:online_reclaim_queue_alias_owner")
      ? Boolean(yield* db.get(sql`SELECT item_id FROM online_reclaim_queue_alias_owner LIMIT 1`))
      : false
    const crossScopeHasRows = objects.has("table:online_reclaim_cross_part_scope")
      ? Boolean(yield* db.get(sql`SELECT part_id FROM online_reclaim_cross_part_scope LIMIT 1`))
      : false
    const missingEmptyProofIndex = OnlineReclaimIndexes.definitions.some((index) => {
      if (objects.has(`index:${index.name}`)) return false
      if (index.table === "online_reclaim_cross_session_part") return !crossHasRows
      if (index.table === "online_reclaim_queue_alias_scope") return !aliasesHaveRows
      if (index.table === "online_reclaim_queue_alias_owner") return !aliasOwnersHaveRows
      if (index.table === "online_reclaim_cross_part_scope") return !crossScopeHasRows
      return false
    })
    const installed =
      proofTables.every((name) => objects.has(`table:${name}`)) &&
      requiredTriggerNames.every((name) => objects.has(`trigger:${name}`)) &&
      backfillNames.every((name) => completedRows.has(name)) &&
      stableBackfillNames.every((name) => completedStableRows.has(name)) &&
      !missingEmptyProofIndex
    if (installed) return

    yield* installMissing(db, initiallyComplete)
  })
}

function installMissing(db: Database, initiallyComplete: boolean) {
  return db.transaction(
    (tx) =>
      Effect.gen(function* () {
        const baseTables = new Set(
          (yield* tx.all<{ name: string }>(sql`
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name IN ('part', 'message', 'session_prompt_queue', 'session_prompt_queue_alias')
          `)).map((row) => row.name),
        )
        if (baseTables.size !== 4) return

        const crossTable = yield* tx.get<{ name: string }>(sql`
          SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${"online_reclaim_cross_session_part"}
        `)
        const aliasTable = yield* tx.get<{ name: string }>(sql`
          SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${"online_reclaim_queue_alias_scope"}
        `)
        const scopeTable = yield* tx.get<{ name: string }>(sql`
          SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${"online_reclaim_cross_part_scope"}
        `)
        const crossScopeHasRows = scopeTable
          ? Boolean(yield* tx.get(sql`SELECT part_id FROM online_reclaim_cross_part_scope LIMIT 1`))
          : false
        const aliasOwnerTable = yield* tx.get<{ name: string }>(sql`
          SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${"online_reclaim_queue_alias_owner"}
        `)
        const crossHasRows = crossTable
          ? Boolean(yield* tx.get(sql`SELECT part_id FROM online_reclaim_cross_session_part LIMIT 1`))
          : false
        const aliasesHaveRows = aliasTable
          ? Boolean(yield* tx.get(sql`SELECT alias_rowid FROM online_reclaim_queue_alias_scope LIMIT 1`))
          : false
        const aliasOwnersHaveRows = aliasOwnerTable
          ? Boolean(yield* tx.get(sql`SELECT item_id FROM online_reclaim_queue_alias_owner LIMIT 1`))
          : false

        for (const statement of structureStatements) yield* tx.run(statement)
        const stablePartBackfill = yield* tx.get<{ name: string }>(sql`
          SELECT name FROM online_reclaim_stable_backfill WHERE name = ${stableBackfillNames[2]}
        `)
        for (const index of OnlineReclaimIndexes.definitions.filter((item) => {
          if (item.table === "online_reclaim_cross_session_part") return !crossHasRows
          if (item.table === "online_reclaim_queue_alias_scope") return !aliasesHaveRows
          if (item.table === "online_reclaim_queue_alias_owner") return !aliasOwnersHaveRows
          if (item.table === "online_reclaim_cross_part_scope") return !crossScopeHasRows
          return false
        }))
          yield* tx.run(index.statement)
        for (const statement of triggerStatements) yield* tx.run(statement)
        yield* tx.run(sql`
          INSERT OR IGNORE INTO online_reclaim_backfill (name, cursor, high_water, complete)
          VALUES ('cross-session-part-scope', 0, ${initiallyComplete ? 0 : -1}, ${initiallyComplete ? 1 : 0})
        `)
        yield* tx.run(sql`
          INSERT OR IGNORE INTO online_reclaim_backfill (name, cursor, high_water, complete)
          VALUES ('cross-session-parts', 0, ${initiallyComplete ? 0 : -1}, ${initiallyComplete ? 1 : 0})
        `)
        yield* tx.run(sql`
          INSERT OR IGNORE INTO online_reclaim_backfill (name, cursor, high_water, complete)
          VALUES ('queue-alias-scope', 0, ${initiallyComplete ? 0 : -1}, ${initiallyComplete ? 1 : 0})
        `)
        yield* tx.run(sql`
          INSERT OR IGNORE INTO online_reclaim_stable_backfill (
            name, cursor, cursor_secondary, cursor_started,
            high_water, high_water_secondary, high_water_present, complete
          ) VALUES (
            ${stableBackfillNames[0]}, '', '', 0, '', '', 0, ${initiallyComplete ? 1 : 0}
          )
        `)
        yield* tx.run(sql`
          INSERT OR IGNORE INTO online_reclaim_stable_backfill (
            name, cursor, cursor_secondary, cursor_started,
            high_water, high_water_secondary, high_water_present, complete
          ) VALUES (
            ${stableBackfillNames[1]}, '', '', 0, '', '', 0, ${initiallyComplete ? 1 : 0}
          )
        `)
        yield* tx.run(sql`
          INSERT OR IGNORE INTO online_reclaim_stable_backfill (
            name, cursor, cursor_secondary, cursor_started,
            high_water, high_water_secondary, high_water_present, complete
          ) VALUES (
            ${stableBackfillNames[2]}, '', '', 0, '', '', 0, ${initiallyComplete ? 1 : 0}
          )
        `)
        if ((!scopeTable || !stablePartBackfill) && !initiallyComplete) {
          yield* tx.run(sql`
            UPDATE online_reclaim_stable_backfill
            SET cursor = '', cursor_secondary = '', cursor_started = 0,
                high_water = '', high_water_secondary = '', high_water_present = 0, complete = 0
            WHERE name = ${stableBackfillNames[2]}
          `)
          yield* tx.run(sql`
            UPDATE online_reclaim_backfill
            SET cursor = 0, high_water = -1, complete = 0
            WHERE name IN ('cross-session-part-scope', 'cross-session-parts')
          `)
        }
        if (!aliasTable && !initiallyComplete)
          yield* tx.run(sql`
            UPDATE online_reclaim_backfill
            SET cursor = 0, high_water = -1, complete = 0
            WHERE name = 'queue-alias-scope'
          `)
        if (!aliasOwnerTable && !initiallyComplete)
          yield* tx.run(sql`
            UPDATE online_reclaim_stable_backfill
            SET cursor = '', cursor_secondary = '', cursor_started = 0,
                high_water = '', high_water_secondary = '', high_water_present = 0, complete = 0
            WHERE name = 'queue-alias-owners'
          `)
        if (!aliasOwnerTable && !initiallyComplete)
          yield* tx.run(sql`
            UPDATE online_reclaim_backfill
            SET cursor = 0, high_water = -1, complete = 0
            WHERE name = 'queue-alias-scope'
          `)
      }),
    { behavior: "immediate" },
  )
}
