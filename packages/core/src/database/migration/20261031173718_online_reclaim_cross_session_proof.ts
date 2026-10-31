import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261031173718_online_reclaim_cross_session_proof",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`online_reclaim_backfill\` (
          \`name\` text PRIMARY KEY,
          \`cursor\` integer NOT NULL,
          \`high_water\` integer NOT NULL,
          \`complete\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`online_reclaim_cross_session_part\` (
          \`part_id\` text PRIMARY KEY,
          \`message_id\` text NOT NULL,
          \`parent_session_id\` text NOT NULL,
          \`child_session_id\` text NOT NULL
        );
      `)
      yield* tx.run(
        `INSERT OR IGNORE INTO \`online_reclaim_backfill\` (\`name\`, \`cursor\`, \`high_water\`, \`complete\`) VALUES ('cross-session-parts', 0, -1, 0);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
