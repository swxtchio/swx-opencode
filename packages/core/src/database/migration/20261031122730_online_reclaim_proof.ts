import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261031122730_online_reclaim_proof",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(
        `CREATE INDEX \`event_aggregate_writer_activity_idx\` ON \`event_aggregate_writer\` (\`aggregate_id\`,\`time_last_append\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_ask_session_unsettled_idx\` ON \`session_ask\` (\`session_id\`) WHERE "session_ask"."time_settled" IS NULL;`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_ask_session_time_created_idx\` ON \`session_ask\` (\`session_id\`,\`time_created\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_ask_session_time_settled_idx\` ON \`session_ask\` (\`session_id\`,\`time_settled\`) WHERE "session_ask"."time_settled" IS NOT NULL;`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_queue_session_open_idx\` ON \`session_prompt_queue\` (\`session_id\`) WHERE ("session_prompt_queue"."time_promoted" IS NULL AND "session_prompt_queue"."time_withdrawn" IS NULL) OR "session_prompt_queue"."time_preparing" IS NOT NULL;`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_queue_session_conflicted_idx\` ON \`session_prompt_queue\` (\`session_id\`) WHERE "session_prompt_queue"."time_promoted" IS NOT NULL AND "session_prompt_queue"."time_withdrawn" IS NOT NULL;`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_queue_session_time_created_idx\` ON \`session_prompt_queue\` (\`session_id\`,\`time_created\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_queue_session_time_promoted_idx\` ON \`session_prompt_queue\` (\`session_id\`,\`time_promoted\`) WHERE "session_prompt_queue"."time_promoted" IS NOT NULL;`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_queue_session_time_withdrawn_idx\` ON \`session_prompt_queue\` (\`session_id\`,\`time_withdrawn\`) WHERE "session_prompt_queue"."time_withdrawn" IS NOT NULL;`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_queue_session_time_preparing_idx\` ON \`session_prompt_queue\` (\`session_id\`,\`time_preparing\`) WHERE "session_prompt_queue"."time_preparing" IS NOT NULL;`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_receipt_session_pending_idx\` ON \`session_prompt_receipt\` (\`session_id\`) WHERE "session_prompt_receipt"."state" = 'pending';`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_receipt_session_invalid_idx\` ON \`session_prompt_receipt\` (\`session_id\`) WHERE "session_prompt_receipt"."state" NOT IN ('pending', 'withdrawn', 'delivered', 'merged', 'superseded', 'deleted');`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_receipt_session_time_created_idx\` ON \`session_prompt_receipt\` (\`session_id\`,\`time_created\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_prompt_receipt_session_time_updated_idx\` ON \`session_prompt_receipt\` (\`session_id\`,\`time_updated\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`message_session_invalid_json_idx\` ON \`message\` (\`session_id\`) WHERE json_valid("message"."data") = 0;`,
      )
      yield* tx.run(
        `CREATE INDEX \`message_session_incomplete_assistant_idx\` ON \`message\` (\`session_id\`) WHERE CASE WHEN json_valid("message"."data") THEN json_extract("message"."data", '$.role') = 'assistant' AND COALESCE(json_type("message"."data", '$.time.completed'), 'null') NOT IN ('integer', 'real') ELSE 0 END;`,
      )
      yield* tx.run(
        `CREATE INDEX \`part_session_invalid_json_idx\` ON \`part\` (\`session_id\`) WHERE json_valid("part"."data") = 0;`,
      )
      yield* tx.run(
        `CREATE INDEX \`part_session_pending_running_idx\` ON \`part\` (\`session_id\`) WHERE CASE WHEN json_valid("part"."data") THEN json_extract("part"."data", '$.state.status') IN ('pending', 'running') ELSE 0 END;`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_input_session_time_created_idx\` ON \`session_input\` (\`session_id\`,\`time_created\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_input_session_invalid_delivery_idx\` ON \`session_input\` (\`session_id\`) WHERE "session_input"."delivery" NOT IN ('steer', 'queue');`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_message_session_invalid_json_idx\` ON \`session_message\` (\`session_id\`) WHERE json_valid("session_message"."data") = 0;`,
      )
      yield* tx.run(
        `CREATE INDEX \`session_message_session_incomplete_assistant_idx\` ON \`session_message\` (\`session_id\`) WHERE CASE WHEN json_valid("session_message"."data") THEN "session_message"."type" = 'assistant' AND COALESCE(json_type("session_message"."data", '$.time.completed'), 'null') NOT IN ('integer', 'real') ELSE 0 END;`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
