import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261031224838_chunky_stepford_cuckoos",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`online_reclaim_stable_backfill\` (
          \`name\` text PRIMARY KEY,
          \`cursor\` text NOT NULL,
          \`cursor_secondary\` text NOT NULL,
          \`cursor_started\` integer NOT NULL,
          \`high_water\` text NOT NULL,
          \`high_water_secondary\` text NOT NULL,
          \`high_water_present\` integer NOT NULL,
          \`complete\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`online_reclaim_writer_lifetime\` (
          \`pid\` integer NOT NULL,
          \`boot_id\` text NOT NULL,
          \`start_ticks\` text NOT NULL,
          CONSTRAINT \`online_reclaim_writer_lifetime_pk\` PRIMARY KEY(\`pid\`, \`boot_id\`, \`start_ticks\`)
        );
      `)
    })
  },
} satisfies DatabaseMigration.Migration
