import { Database } from "@opencode-ai/core/database/database"
import { sql } from "drizzle-orm"
import { Context, Effect, Layer } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"

// Outlasts the driver's own retry window, so it only catches a retry that never stops.
const retryBackstop = "10 seconds"

const withImmediateSqliteLock = <A, E, R>(
  database: Database.Interface["db"],
  filename: string,
  action: () => Effect.Effect<A, E, R>,
) =>
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

      return yield* action()
    }),
  )

export const produceSqliteBusyError = (database: Database.Interface["db"], filename: string) =>
  withImmediateSqliteLock(database, filename, () =>
    Effect.flip(
      database.run(sql`INSERT INTO secret_lock_fixture (id, value) VALUES (${1}, ${"secret_parameter"})`).pipe(
        Effect.timeoutOrElse({
          duration: retryBackstop,
          orElse: () => Effect.fail(new Error("SQLite busy retries did not stop")),
        }),
      ),
    ),
  )

export const produceSqliteImmediateError = (database: Database.Interface["db"], filename: string) =>
  withImmediateSqliteLock(database, filename, () =>
    Effect.flip(
      database
        .transaction(
          (tx) => tx.run(sql`INSERT INTO secret_lock_fixture (id, value) VALUES (${2}, ${"direct_parameter"})`),
          { behavior: "immediate" },
        )
        .pipe(
          Effect.timeoutOrElse({
            duration: retryBackstop,
            orElse: () => Effect.fail(new Error("SQLite immediate transaction retries did not stop")),
          }),
        ),
    ),
  )

export const produceSqliteLockedError = (filename: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const bun = yield* Effect.promise(() => import("@opencode-ai/core/database/sqlite.bun"))
      const coreSqlite = yield* Effect.promise(() => import("@opencode-ai/core/database/sqlite"))
      const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
      const context = yield* Layer.build(bun.layer({ filename }))
      const client = Context.get(context, SqlClient)
      const native = Context.get(context, coreSqlite.Sqlite.Native) as InstanceType<typeof sqlite.Database>
      yield* client.unsafe("CREATE TABLE secret_lock_fixture (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
      yield* client.unsafe("INSERT INTO secret_lock_fixture (id, value) VALUES (1, 'secret_parameter')").raw
      yield* client.unsafe("PRAGMA busy_timeout = 0").raw

      const cursor = native.query("SELECT id FROM secret_lock_fixture").iterate()
      yield* Effect.addFinalizer(() => Effect.sync(() => cursor.return?.()))
      if (cursor.next().done) return yield* Effect.fail(new Error("SQLite lock cursor returned no row"))

      return yield* Effect.flip(
        client.unsafe("DROP TABLE secret_lock_fixture").raw.pipe(
          Effect.timeoutOrElse({
            duration: retryBackstop,
            orElse: () => Effect.fail(new Error("SQLite locked retries did not stop")),
          }),
        ),
      )
    }),
  )
