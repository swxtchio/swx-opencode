import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261031233939_quiet_blur",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`online_reclaim_cross_part_message\` (
          \`message_id\` text NOT NULL,
          \`child_session_id\` text NOT NULL,
          \`part_id\` text NOT NULL,
          \`parent_session_id\` text NOT NULL,
          CONSTRAINT \`online_reclaim_cross_part_message_pk\` PRIMARY KEY(\`message_id\`, \`child_session_id\`, \`part_id\`)
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`online_reclaim_cross_part_scope\` (
          \`session_id\` text NOT NULL,
          \`part_id\` text NOT NULL,
          \`message_id\` text NOT NULL,
          \`parent_session_id\` text NOT NULL,
          \`child_session_id\` text NOT NULL,
          \`role\` text NOT NULL,
          CONSTRAINT \`online_reclaim_cross_part_scope_pk\` PRIMARY KEY(\`session_id\`, \`part_id\`)
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`online_reclaim_queue_alias_owner\` (
          \`item_id\` text NOT NULL,
          \`alias_id\` text NOT NULL,
          \`owner_session_id\` text NOT NULL,
          CONSTRAINT \`online_reclaim_queue_alias_owner_pk\` PRIMARY KEY(\`item_id\`, \`alias_id\`)
        );
      `)
      yield* tx.run(
        `CREATE INDEX \`online_reclaim_cross_part_scope_owner_idx\` ON \`online_reclaim_cross_part_scope\` (\`session_id\`,\`part_id\`) WHERE "online_reclaim_cross_part_scope"."role" IN ('owned', 'child');`,
      )
      yield* tx.run(
        `CREATE INDEX \`online_reclaim_cross_part_scope_cross_idx\` ON \`online_reclaim_cross_part_scope\` (\`session_id\`,\`part_id\`) WHERE "online_reclaim_cross_part_scope"."role" IN ('parent', 'child');`,
      )
      yield* tx.run(
        `CREATE INDEX \`online_reclaim_queue_alias_owner_session_idx\` ON \`online_reclaim_queue_alias_owner\` (\`owner_session_id\`,\`item_id\`,\`alias_id\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
