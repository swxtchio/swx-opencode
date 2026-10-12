import { expect, test } from "bun:test"
import { Effect } from "effect"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { DatabaseMigration } from "../src/database/migration"
import aliasCustody from "../src/database/migration/20261012161249_queue-alias-custody"
import { layer } from "../src/database/sqlite.bun"

test("alias upgrade preserves retained queue state and supplies indexed cascading identity custody", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* db.run("PRAGMA foreign_keys=ON")
      yield* db.run(
        "CREATE TABLE session_prompt_queue(id TEXT PRIMARY KEY,session_id TEXT,seq INTEGER,input TEXT,message_id TEXT,time_created INTEGER,time_promoted INTEGER,time_withdrawn INTEGER)",
      )
      yield* db.run("CREATE TABLE session_prompt_queue_sequence(session_id TEXT PRIMARY KEY,seq INTEGER NOT NULL)")
      yield* db.run(
        "INSERT INTO session_prompt_queue VALUES ('que_pending','ses_old',15,'original pending',NULL,1,NULL,NULL),('que_reserved','ses_old',16,'original reserved','msg_reserved',2,NULL,NULL),('que_withdrawn','ses_old',17,'original edit hold',NULL,3,NULL,4)",
      )
      yield* db.run("INSERT INTO session_prompt_queue_sequence VALUES ('ses_old',17)")
      const before = yield* db.all("SELECT * FROM session_prompt_queue ORDER BY seq")
      yield* DatabaseMigration.applyOnly(db, [aliasCustody])
      yield* DatabaseMigration.applyOnly(db, [aliasCustody])
      expect(yield* db.all("SELECT * FROM session_prompt_queue ORDER BY seq")).toEqual(before)
      expect(yield* db.all("SELECT session_id,seq,aliases_indexed FROM session_prompt_queue_sequence")).toEqual([
        { session_id: "ses_old", seq: 17, aliases_indexed: 0 },
      ])
      expect(yield* db.all("SELECT id FROM migration")).toEqual([{ id: aliasCustody.id }])
      yield* db.run("INSERT INTO session_prompt_queue_alias VALUES ('prt_alias','que_pending')")
      const plan = yield* db.all<{ detail: string }>(
        "EXPLAIN QUERY PLAN SELECT q.* FROM session_prompt_queue_alias a JOIN session_prompt_queue q ON q.id=a.item_id WHERE a.id='prt_alias'",
      )
      expect(plan.every((row) => !row.detail.includes("SCAN"))).toBe(true)
      expect(plan.some((row) => row.detail.includes("session_prompt_queue_alias"))).toBe(true)
      yield* db.run("DELETE FROM session_prompt_queue WHERE id='que_pending'")
      expect(yield* db.all("SELECT * FROM session_prompt_queue_alias")).toEqual([])
      expect(yield* db.all("SELECT id FROM session_prompt_queue ORDER BY seq")).toEqual([
        { id: "que_reserved" },
        { id: "que_withdrawn" },
      ])
    }).pipe(Effect.provide(layer({ filename: ":memory:" })), Effect.scoped),
  )
})
