import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261022023441_small-model",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`session\` ADD \`small_model\` text;`)
    })
  },
} satisfies DatabaseMigration.Migration
