import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260929045002_session-message-admission-order",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`message\` ADD \`admission_seq\` integer DEFAULT 0 NOT NULL;`)
      yield* tx.run(`
        WITH ordered AS (
          SELECT \`id\`, ROW_NUMBER() OVER (PARTITION BY \`session_id\` ORDER BY \`time_created\`, \`id\`) AS seq
          FROM \`message\`
        )
        UPDATE \`message\`
        SET \`admission_seq\` = (SELECT seq FROM ordered WHERE ordered.id = message.id);
      `)
      yield* tx.run(
        `CREATE UNIQUE INDEX \`message_session_admission_seq_idx\` ON \`message\` (\`session_id\`,\`admission_seq\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
