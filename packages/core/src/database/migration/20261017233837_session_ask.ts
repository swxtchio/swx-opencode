import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261017233837_session_ask",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`session_ask\` (
          \`id\` text PRIMARY KEY,
          \`session_id\` text NOT NULL,
          \`kind\` text NOT NULL,
          \`directory\` text NOT NULL,
          \`tool\` text,
          \`request\` text NOT NULL,
          \`writer_id\` text NOT NULL,
          \`decision\` text,
          \`recovered\` integer DEFAULT false NOT NULL,
          \`time_created\` integer NOT NULL,
          \`time_settled\` integer,
          \`time_applied\` integer,
          CONSTRAINT \`fk_session_ask_session_id_session_id_fk\` FOREIGN KEY (\`session_id\`) REFERENCES \`session\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`CREATE INDEX \`session_ask_session_idx\` ON \`session_ask\` (\`session_id\`);`)
      yield* tx.run(
        `CREATE INDEX \`session_ask_directory_settled_idx\` ON \`session_ask\` (\`directory\`,\`time_settled\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
