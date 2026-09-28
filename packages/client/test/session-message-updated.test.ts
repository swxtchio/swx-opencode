import { expect, test } from "bun:test"
import { OpenCodeEvent } from "@opencode-ai/protocol/groups/event"
import { Event } from "@opencode-ai/schema/event"
import { SessionV1 } from "@opencode-ai/schema/v1/session"
import { Schema } from "effect"
import type { EventsSubscribeOutput } from "../src/generated/types"

test("SSE event output carries MessageUpdated admission metadata", () => {
  const event: EventsSubscribeOutput = {
    id: Event.ID.create(),
    type: "message.updated",
    durable: { aggregateID: "ses_client_event", seq: 1, version: 1 },
    data: {
      sessionID: "ses_client_event",
      info: {
        id: "msg_client_event",
        sessionID: "ses_client_event",
        role: "user",
        time: { created: 1 },
        agent: "build",
        model: { providerID: "test", modelID: "test-model" },
      },
      reAdmit: true,
      claims: [SessionV1.MessageID.ascending("msg_client_event")],
    },
  }
  const decoded = Schema.decodeUnknownSync(OpenCodeEvent)(event)
  expect(decoded.type).toBe("message.updated")
  if (decoded.type !== "message.updated") throw new Error("expected a MessageUpdated SSE event")
  expect(decoded.data.reAdmit).toBe(true)
  expect(decoded.data.claims).toEqual([SessionV1.MessageID.ascending("msg_client_event")])
})
