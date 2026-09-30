import { Database } from "@opencode-ai/core/database/database"
import { sql } from "drizzle-orm"
import { Effect } from "effect"

export const produceSqliteBusyError = (database: Database.Interface["db"], filename: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* database.run(sql`CREATE TABLE secret_lock_fixture (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`)
      yield* database.$client.unsafe("PRAGMA busy_timeout = 0").raw

      const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
      const holder = new sqlite.Database(filename)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (holder.inTransaction) holder.run("ROLLBACK")
          holder.close()
        }),
      )
      holder.run("PRAGMA journal_mode = WAL")
      holder.run("BEGIN IMMEDIATE")
      holder.run("INSERT INTO secret_lock_fixture (id, value) VALUES (100, 'holder')")

      return yield* Effect.flip(
        database.run(sql`INSERT INTO secret_lock_fixture (id, value) VALUES (${1}, ${"secret_parameter"})`).pipe(
          Effect.timeoutOrElse({
            duration: "5 seconds",
            orElse: () => Effect.fail(new Error("SQLite busy retries did not stop")),
          }),
        ),
      )
    }),
  )
