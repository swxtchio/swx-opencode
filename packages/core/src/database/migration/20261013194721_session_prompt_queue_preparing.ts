import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261013194721_session_prompt_queue_preparing",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`session_prompt_queue\` ADD \`time_preparing\` integer;`)
    })
  },
} satisfies DatabaseMigration.Migration
