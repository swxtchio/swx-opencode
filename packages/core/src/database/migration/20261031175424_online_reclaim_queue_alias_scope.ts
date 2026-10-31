import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261031175424_online_reclaim_queue_alias_scope",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`online_reclaim_queue_alias_scope\` (
          \`alias_rowid\` integer PRIMARY KEY,
          \`owner_session_id\` text NOT NULL
        );
      `)
    })
  },
} satisfies DatabaseMigration.Migration
