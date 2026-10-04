import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261003165428_session_event_retention",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`event_retention\` (
          \`aggregate_id\` text PRIMARY KEY,
          \`state\` text NOT NULL,
          \`progress_table\` text,
          \`progress_id\` text,
          \`evidence\` text NOT NULL,
          \`time_started\` integer NOT NULL,
          \`time_updated\` integer NOT NULL
        );
      `)
    })
  },
} satisfies DatabaseMigration.Migration
