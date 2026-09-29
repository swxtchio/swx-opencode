import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260927152632_session_prompt_queue",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`session_prompt_queue_sequence\` (
          \`session_id\` text PRIMARY KEY,
          \`seq\` integer NOT NULL,
          CONSTRAINT \`fk_session_prompt_queue_sequence_session_id_session_id_fk\` FOREIGN KEY (\`session_id\`) REFERENCES \`session\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`session_prompt_queue\` (
          \`id\` text PRIMARY KEY,
          \`session_id\` text NOT NULL,
          \`seq\` integer NOT NULL,
          \`delivery\` text NOT NULL,
          \`input\` text NOT NULL,
          \`message_id\` text,
          \`time_created\` integer NOT NULL,
          \`time_promoted\` integer,
          \`time_withdrawn\` integer,
          CONSTRAINT \`fk_session_prompt_queue_session_id_session_id_fk\` FOREIGN KEY (\`session_id\`) REFERENCES \`session\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(
        `CREATE UNIQUE INDEX \`session_prompt_queue_session_seq_idx\` ON \`session_prompt_queue\` (\`session_id\`,\`seq\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_queue_session_delivery_seq_idx\` ON \`session_prompt_queue\` (\`session_id\`,\`delivery\`,\`seq\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
