import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261013020615_session_prompt_receipt",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`session_prompt_receipt\` (
          \`session_id\` text NOT NULL,
          \`request_id\` text NOT NULL,
          \`state\` text NOT NULL,
          \`item_id\` text,
          \`message_id\` text,
          \`into_request_id\` text,
          \`by_request_id\` text,
          \`fingerprint\` text NOT NULL,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL,
          CONSTRAINT \`session_prompt_receipt_pk\` PRIMARY KEY(\`session_id\`, \`request_id\`),
          CONSTRAINT \`fk_session_prompt_receipt_session_id_session_id_fk\` FOREIGN KEY (\`session_id\`) REFERENCES \`session\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`ALTER TABLE \`session_prompt_queue\` ADD \`request_id\` text;`)
      yield* tx.run(
        `CREATE INDEX \`session_prompt_receipt_session_item_idx\` ON \`session_prompt_receipt\` (\`session_id\`,\`item_id\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
