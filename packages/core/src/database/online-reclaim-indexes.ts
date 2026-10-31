export * as OnlineReclaimIndexes from "./online-reclaim-indexes"

import { Durable } from "@opencode-ai/schema/durable-event-manifest"

const durableTypes = [...Durable.keys()].map((type) => `'${type.replaceAll("'", "''")}'`).join(", ")

// Existing-store builds are intentionally explicit and offline: CREATE INDEX scans the full source table.
export const definitions = [
  {
    table: "event_aggregate_writer",
    name: "event_aggregate_writer_activity_idx",
    statement: `CREATE INDEX IF NOT EXISTS "event_aggregate_writer_activity_idx" ON "event_aggregate_writer" ("aggregate_id","time_last_append");`,
  },
  {
    table: "session_ask",
    name: "session_ask_session_unsettled_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_ask_session_unsettled_idx" ON "session_ask" ("session_id") WHERE "session_ask"."time_settled" IS NULL;`,
  },
  {
    table: "session_ask",
    name: "session_ask_session_time_created_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_ask_session_time_created_idx" ON "session_ask" ("session_id","time_created");`,
  },
  {
    table: "session_ask",
    name: "session_ask_session_time_settled_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_ask_session_time_settled_idx" ON "session_ask" ("session_id","time_settled") WHERE "session_ask"."time_settled" IS NOT NULL;`,
  },
  {
    table: "session_prompt_queue",
    name: "session_prompt_queue_session_open_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_prompt_queue_session_open_idx" ON "session_prompt_queue" ("session_id") WHERE ("session_prompt_queue"."time_promoted" IS NULL AND "session_prompt_queue"."time_withdrawn" IS NULL) OR "session_prompt_queue"."time_preparing" IS NOT NULL;`,
  },
  {
    table: "session_prompt_queue",
    name: "session_prompt_queue_session_conflicted_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_prompt_queue_session_conflicted_idx" ON "session_prompt_queue" ("session_id") WHERE "session_prompt_queue"."time_promoted" IS NOT NULL AND "session_prompt_queue"."time_withdrawn" IS NOT NULL;`,
  },
  {
    table: "session_prompt_queue",
    name: "session_prompt_queue_session_time_created_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_prompt_queue_session_time_created_idx" ON "session_prompt_queue" ("session_id","time_created");`,
  },
  {
    table: "session_prompt_queue",
    name: "session_prompt_queue_session_time_promoted_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_prompt_queue_session_time_promoted_idx" ON "session_prompt_queue" ("session_id","time_promoted") WHERE "session_prompt_queue"."time_promoted" IS NOT NULL;`,
  },
  {
    table: "session_prompt_queue",
    name: "session_prompt_queue_session_time_withdrawn_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_prompt_queue_session_time_withdrawn_idx" ON "session_prompt_queue" ("session_id","time_withdrawn") WHERE "session_prompt_queue"."time_withdrawn" IS NOT NULL;`,
  },
  {
    table: "session_prompt_queue",
    name: "session_prompt_queue_session_time_preparing_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_prompt_queue_session_time_preparing_idx" ON "session_prompt_queue" ("session_id","time_preparing") WHERE "session_prompt_queue"."time_preparing" IS NOT NULL;`,
  },
  {
    table: "session_prompt_receipt",
    name: "session_prompt_receipt_session_pending_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_prompt_receipt_session_pending_idx" ON "session_prompt_receipt" ("session_id") WHERE "session_prompt_receipt"."state" = 'pending';`,
  },
  {
    table: "session_prompt_receipt",
    name: "session_prompt_receipt_session_invalid_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_prompt_receipt_session_invalid_idx" ON "session_prompt_receipt" ("session_id") WHERE "session_prompt_receipt"."state" NOT IN ('pending', 'withdrawn', 'delivered', 'merged', 'superseded', 'deleted');`,
  },
  {
    table: "session_prompt_receipt",
    name: "session_prompt_receipt_session_time_created_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_prompt_receipt_session_time_created_idx" ON "session_prompt_receipt" ("session_id","time_created");`,
  },
  {
    table: "session_prompt_receipt",
    name: "session_prompt_receipt_session_time_updated_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_prompt_receipt_session_time_updated_idx" ON "session_prompt_receipt" ("session_id","time_updated");`,
  },
  {
    table: "message",
    name: "message_session_invalid_json_idx",
    statement: `CREATE INDEX IF NOT EXISTS "message_session_invalid_json_idx" ON "message" ("session_id") WHERE json_valid("message"."data") = 0;`,
  },
  {
    table: "message",
    name: "message_session_incomplete_assistant_idx",
    statement: `CREATE INDEX IF NOT EXISTS "message_session_incomplete_assistant_idx" ON "message" ("session_id") WHERE CASE WHEN json_valid("message"."data") THEN json_extract("message"."data", '$.role') = 'assistant' AND COALESCE(json_type("message"."data", '$.time.completed'), 'null') NOT IN ('integer', 'real') ELSE 0 END;`,
  },
  {
    table: "part",
    name: "part_session_invalid_json_idx",
    statement: `CREATE INDEX IF NOT EXISTS "part_session_invalid_json_idx" ON "part" ("session_id") WHERE json_valid("part"."data") = 0;`,
  },
  {
    table: "part",
    name: "part_session_pending_running_idx",
    statement: `CREATE INDEX IF NOT EXISTS "part_session_pending_running_idx" ON "part" ("session_id") WHERE CASE WHEN json_valid("part"."data") THEN json_extract("part"."data", '$.state.status') IN ('pending', 'running') ELSE 0 END;`,
  },
  {
    table: "session_input",
    name: "session_input_session_time_created_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_input_session_time_created_idx" ON "session_input" ("session_id","time_created");`,
  },
  {
    table: "session_input",
    name: "session_input_session_invalid_delivery_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_input_session_invalid_delivery_idx" ON "session_input" ("session_id") WHERE "session_input"."delivery" NOT IN ('steer', 'queue');`,
  },
  {
    table: "session_message",
    name: "session_message_session_invalid_json_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_message_session_invalid_json_idx" ON "session_message" ("session_id") WHERE json_valid("session_message"."data") = 0;`,
  },
  {
    table: "session_message",
    name: "session_message_session_incomplete_assistant_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_message_session_incomplete_assistant_idx" ON "session_message" ("session_id") WHERE CASE WHEN json_valid("session_message"."data") THEN "session_message"."type" = 'assistant' AND COALESCE(json_type("session_message"."data", '$.time.completed'), 'null') NOT IN ('integer', 'real') ELSE 0 END;`,
  },
  {
    table: "event",
    name: "event_aggregate_unknown_type_idx",
    refreshWhenChanged: true,
    statement: `CREATE INDEX IF NOT EXISTS "event_aggregate_unknown_type_idx" ON "event" ("aggregate_id") WHERE "event"."type" NOT IN (${durableTypes});`,
  },
  {
    table: "message",
    name: "message_session_time_updated_id_idx",
    statement: `CREATE INDEX IF NOT EXISTS "message_session_time_updated_id_idx" ON "message" ("session_id","time_updated","id");`,
  },
  {
    table: "part",
    name: "part_message_id_session_idx",
    statement: `CREATE INDEX IF NOT EXISTS "part_message_id_session_idx" ON "part" ("message_id","session_id");`,
  },
  {
    table: "part",
    name: "part_session_time_created_id_idx",
    statement: `CREATE INDEX IF NOT EXISTS "part_session_time_created_id_idx" ON "part" ("session_id","time_created","id");`,
  },
  {
    table: "part",
    name: "part_session_time_updated_id_idx",
    statement: `CREATE INDEX IF NOT EXISTS "part_session_time_updated_id_idx" ON "part" ("session_id","time_updated","id");`,
  },
  {
    table: "session_message",
    name: "session_message_session_time_updated_id_idx",
    statement: `CREATE INDEX IF NOT EXISTS "session_message_session_time_updated_id_idx" ON "session_message" ("session_id","time_updated","id");`,
  },
  {
    table: "todo",
    name: "todo_session_time_created_position_idx",
    statement: `CREATE INDEX IF NOT EXISTS "todo_session_time_created_position_idx" ON "todo" ("session_id","time_created","position");`,
  },
  {
    table: "todo",
    name: "todo_session_time_updated_position_idx",
    statement: `CREATE INDEX IF NOT EXISTS "todo_session_time_updated_position_idx" ON "todo" ("session_id","time_updated","position");`,
  },
  {
    table: "online_reclaim_cross_session_part",
    name: "online_reclaim_cross_part_parent_idx",
    statement: `CREATE INDEX IF NOT EXISTS "online_reclaim_cross_part_parent_idx" ON "online_reclaim_cross_session_part" ("parent_session_id");`,
  },
  {
    table: "online_reclaim_cross_session_part",
    name: "online_reclaim_cross_part_child_idx",
    statement: `CREATE INDEX IF NOT EXISTS "online_reclaim_cross_part_child_idx" ON "online_reclaim_cross_session_part" ("child_session_id");`,
  },
] satisfies readonly { table: string; name: string; statement: string; refreshWhenChanged?: boolean }[]
