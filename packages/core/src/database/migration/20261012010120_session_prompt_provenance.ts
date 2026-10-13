import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261012010120_session_prompt_provenance",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`session_prompt_provenance\` (
          \`message_id\` text PRIMARY KEY,
          \`session_id\` text NOT NULL,
          \`origins\` text NOT NULL,
          CONSTRAINT \`fk_session_prompt_provenance_session_id_session_id_fk\` FOREIGN KEY (\`session_id\`) REFERENCES \`session\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`ALTER TABLE \`session_prompt_queue\` ADD \`origins\` text;`)
      yield* tx.run(
        `CREATE INDEX \`session_prompt_provenance_session_idx\` ON \`session_prompt_provenance\` (\`session_id\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
