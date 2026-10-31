import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261031131024_online_reclaim_indexed_proofs",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(
        `CREATE INDEX \`event_aggregate_unknown_type_idx\` ON \`event\` (\`aggregate_id\`) WHERE "event"."type" NOT IN ('session.created.1', 'session.updated.1', 'session.deleted.1', 'message.updated.1', 'message.removed.1', 'message.part.updated.1', 'message.part.removed.1', 'session.next.agent.switched.1', 'session.next.model.switched.1', 'session.next.titled.1', 'session.next.moved.1', 'session.next.prompted.1', 'session.next.prompt.admitted.1', 'session.next.context.updated.1', 'session.next.synthetic.1', 'session.next.shell.started.1', 'session.next.shell.ended.1', 'session.next.step.started.1', 'session.next.step.ended.2', 'session.next.step.failed.2', 'session.next.text.started.1', 'session.next.text.ended.1', 'session.next.tool.input.started.1', 'session.next.tool.input.ended.1', 'session.next.tool.called.1', 'session.next.tool.progress.1', 'session.next.tool.success.1', 'session.next.tool.failed.1', 'session.next.reasoning.started.1', 'session.next.reasoning.ended.1', 'session.next.retried.1', 'session.next.compaction.started.1', 'session.next.compaction.ended.1', 'session.next.revert.staged.1', 'session.next.revert.cleared.1', 'session.next.revert.committed.1');`,
      )
      yield* tx.run(
        `CREATE INDEX \`message_session_time_updated_id_idx\` ON \`message\` (\`session_id\`,\`time_updated\`,\`id\`);`,
      )
      yield* tx.run(`CREATE INDEX \`part_message_id_session_idx\` ON \`part\` (\`message_id\`,\`session_id\`);`)
      yield* tx.run(
        `CREATE INDEX \`part_session_time_created_id_idx\` ON \`part\` (\`session_id\`,\`time_created\`,\`id\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`part_session_time_updated_id_idx\` ON \`part\` (\`session_id\`,\`time_updated\`,\`id\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_message_session_time_updated_id_idx\` ON \`session_message\` (\`session_id\`,\`time_updated\`,\`id\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`todo_session_time_created_position_idx\` ON \`todo\` (\`session_id\`,\`time_created\`,\`position\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`todo_session_time_updated_position_idx\` ON \`todo\` (\`session_id\`,\`time_updated\`,\`position\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
