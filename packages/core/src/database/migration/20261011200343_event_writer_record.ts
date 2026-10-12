import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261011200343_event_writer_record",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`event_aggregate_writer\` (
          \`aggregate_id\` text NOT NULL,
          \`writer_id\` text NOT NULL,
          \`time_last_append\` integer NOT NULL,
          CONSTRAINT \`event_aggregate_writer_pk\` PRIMARY KEY(\`aggregate_id\`, \`writer_id\`)
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`event_writer_process\` (
          \`writer_id\` text PRIMARY KEY,
          \`pid\` integer NOT NULL,
          \`boot_id\` text,
          \`start_ticks\` text,
          \`time_registered\` integer NOT NULL
        );
      `)
    })
  },
} satisfies DatabaseMigration.Migration
