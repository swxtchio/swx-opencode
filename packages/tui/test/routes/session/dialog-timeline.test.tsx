import { expect, test } from "bun:test"
import type { Message, Part } from "@opencode-ai/sdk/v2"
import { Schema } from "effect"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { timelineOptions } from "../../../src/routes/session/dialog-timeline"

const sessionID = "ses_timeline"

function group(input: {
  itemID: string
  text: string
  origin?: "tui-composer"
  kind?: string
  constituents?: { itemID: string; text: string; origin?: "tui-composer" }[]
}) {
  return Schema.encodeSync(SessionPromptQueue.Group)(
    Schema.decodeUnknownSync(SessionPromptQueue.Group)({
      version: 1,
      sessionID,
      kind: input.kind ?? "individual",
      constituents: (input.constituents ?? [input]).map((item, index) => ({
        id: item.itemID,
        sessionID,
        seq: index + 1,
        delivery: "queue",
        input: {
          ...(item.origin ? { origin: item.origin } : {}),
          parts: [{ type: "text", text: item.text }],
        },
        time: { created: index + 1 },
      })),
    }),
  )
}

function user(id: string, time: number): Message {
  return {
    id,
    sessionID,
    role: "user",
    time: { created: time },
    agent: "build",
    model: { providerID: "test", modelID: "test-model" },
  }
}

function textPart(messageID: string, text: string, metadata: unknown): Part {
  return {
    id: `prt_${messageID}`,
    messageID,
    sessionID,
    type: "text",
    text,
    metadata: { sessionQueue: metadata },
  }
}

test("timeline options keep TUI-authored prompts and exclude raw and grouped injections", () => {
  const typedID = "msg_typed"
  const criticalID = "msg_critical"
  const peerID = "msg_peer"
  const messages: Message[] = [user(typedID, 1), user(criticalID, 2), user(peerID, 3)]
  const parts: Record<string, Part[]> = {
    [typedID]: [
      textPart(
        typedID,
        "typed-history-token",
        group({ itemID: "que_typed", text: "typed-history-token", origin: "tui-composer" }),
      ),
    ],
    [criticalID]: [
      textPart(
        criticalID,
        "[fm-level:critical]\x1f injected-critical-token",
        group({ itemID: "que_critical", text: "[fm-level:critical]\x1f injected-critical-token" }),
      ),
    ],
    [peerID]: [
      textPart(
        peerID,
        "injected-peer-token",
        group({
          itemID: "que_peer_1",
          text: "injected-peer-token",
          kind: "peer",
          constituents: [
            { itemID: "que_peer_1", text: "injected-peer-token" },
            { itemID: "que_peer_2", text: "another peer constituent" },
          ],
        }),
      ),
    ],
  }

  const options = timelineOptions({ sessionID, messages, parts })
  expect(options.map((option) => option.value)).toEqual([typedID])
  expect(options[0]?.title).toBe("typed-history-token")
  expect(options[0]?.value).not.toBe(criticalID)
  expect(options[0]?.value).not.toBe(peerID)
})
