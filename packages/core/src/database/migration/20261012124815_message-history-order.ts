import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261012124815_message-history-order",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(
        `CREATE INDEX \`message_session_admission_order_idx\` ON \`message\` (\`session_id\`,\`admission_seq\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
