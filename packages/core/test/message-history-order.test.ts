import { expect, test } from "bun:test"
import { Deferred, Effect, Fiber } from "effect"
import { sql } from "drizzle-orm"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Database } from "../src/database/database"
import { DatabaseMigration } from "../src/database/migration"
import historyOrder from "../src/database/migration/20261012124815_message-history-order"
import { layer } from "../src/database/sqlite.bun"
import { Sqlite } from "../src/database/sqlite"

const lookup = "message_session_admission_order_idx"

test("fresh stores provide ordered history lookup alongside partial uniqueness", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const database = yield* Database.Service
      const indexes = yield* database.db.all<{ name: string; sql: string }>(
        sql`SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'message'`,
      )
      expect(indexes.find((index) => index.name === lookup)?.sql).toContain("admission_seq")
      expect(indexes.find((index) => index.name === "message_session_admission_seq_idx")?.sql).toContain(" > 0")
    }).pipe(Effect.provide(Database.layerFromPath(":memory:"))),
  )
})

test("upgrade repairs history sorting and observed read/timer stalls while retaining legacy seq zero", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const client = yield* SqlClient
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* client.unsafe(
        "CREATE TABLE message(id TEXT PRIMARY KEY, session_id TEXT, admission_seq INTEGER, time_created INTEGER, data TEXT)",
      )
      yield* client.unsafe("CREATE INDEX message_session_time_created_id_idx ON message(session_id,time_created,id)")
      yield* client.unsafe(
        "CREATE UNIQUE INDEX message_session_admission_seq_idx ON message(session_id,admission_seq) WHERE admission_seq>0",
      )
      yield* client.unsafe(
        "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<7500) INSERT INTO message SELECT 'msg_'||x,'scratch',x,x,printf('%0900d',x) FROM n",
      )
      yield* client.unsafe("INSERT INTO message VALUES ('legacy_a','scratch',0,0,'a'),('legacy_b','scratch',0,0,'b')")
      const pageSQL = "SELECT * FROM message WHERE session_id=? ORDER BY admission_seq DESC LIMIT 50"
      const plan = () => client.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${pageSQL}`, ["scratch"])
      const measure = () =>
        Effect.gen(function* () {
          const ready = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const reading = yield* Deferred.make<void>()
          let last = performance.now()
          let longest = 0
          const timer = setInterval(() => {
            longest = Math.max(longest, performance.now() - last)
            last = performance.now()
          }, 5)
          yield* Effect.addFinalizer(() => Effect.sync(() => clearInterval(timer)))
          const snapshot = yield* client
            .withTransaction(
              Effect.gen(function* () {
                let before: number | undefined
                let count = 0
                while (true) {
                  const rows = yield* client.unsafe<{ admission_seq: number }>(
                    before === undefined
                      ? pageSQL
                      : "SELECT * FROM message WHERE session_id=? AND admission_seq<? ORDER BY admission_seq DESC LIMIT 50",
                    before === undefined ? ["scratch"] : ["scratch", before],
                  )
                  if (before === undefined) {
                    yield* Deferred.succeed(ready, undefined)
                    yield* Deferred.await(release)
                  }
                  count += rows.length
                  if (rows.length < 50) return count
                  before = rows.at(-1)!.admission_seq
                }
              }),
            )
            .pipe(Effect.forkChild)
          yield* Deferred.await(ready)
          const exact = yield* Effect.gen(function* () {
            const started = performance.now()
            yield* Deferred.succeed(reading, undefined)
            expect(yield* client.unsafe("SELECT id FROM message WHERE id=?", ["legacy_a"])).toEqual([
              { id: "legacy_a" },
            ])
            return performance.now() - started
          }).pipe(Effect.forkChild)
          yield* Deferred.await(reading)
          last = performance.now()
          yield* Deferred.succeed(release, undefined)
          const count = yield* Fiber.join(snapshot)
          const blocked = yield* Fiber.join(exact)
          longest = Math.max(longest, performance.now() - last)
          clearInterval(timer)
          return { count, blocked, longest }
        }).pipe(Effect.scoped)
      expect((yield* plan()).some((row) => row.detail.includes("TEMP B-TREE"))).toBe(true)
      const before = yield* measure()
      yield* DatabaseMigration.applyOnly(db, [historyOrder])
      yield* DatabaseMigration.applyOnly(db, [historyOrder])
      expect((yield* plan()).some((row) => row.detail.includes(lookup))).toBe(true)
      expect((yield* plan()).some((row) => row.detail.includes("TEMP B-TREE"))).toBe(false)
      const after = yield* measure()
      expect(after.count).toBe(before.count)
      expect(after.count).toBe(7502)
      // Relative to independently measured old behavior, not a guessed machine-specific success deadline.
      expect(before.blocked / after.blocked).toBeGreaterThan(5)
      expect(before.longest / after.longest).toBeGreaterThan(5)
      const native = yield* Sqlite.Native
      const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
      if (!(native instanceof sqlite.Database)) throw new Error("expected Bun SQLite")
    if (!("clearQueryCache" in native) || typeof native.clearQueryCache !== "function")
      throw new Error("Bun SQLite cannot release cached query plans before schema rollback")
      native.clearQueryCache()
      yield* client.unsafe(`DROP INDEX ${lookup}`)
      const rollback = yield* measure()
      expect(rollback.blocked / after.blocked).toBeGreaterThan(5)
    }).pipe(Effect.provide(layer({ filename: ":memory:" })), Effect.scoped),
  )
}, 30_000)
