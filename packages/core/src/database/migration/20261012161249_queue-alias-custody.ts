import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261012161249_queue-alias-custody",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`session_prompt_queue_alias\` (
          \`id\` text NOT NULL,
          \`item_id\` text NOT NULL,
          CONSTRAINT \`session_prompt_queue_alias_pk\` PRIMARY KEY(\`id\`, \`item_id\`),
          CONSTRAINT \`fk_session_prompt_queue_alias_item_id_session_prompt_queue_id_fk\` FOREIGN KEY (\`item_id\`) REFERENCES \`session_prompt_queue\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(
        `ALTER TABLE \`session_prompt_queue_sequence\` ADD \`aliases_indexed\` integer DEFAULT false NOT NULL;`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_queue_alias_item_idx\` ON \`session_prompt_queue_alias\` (\`item_id\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
