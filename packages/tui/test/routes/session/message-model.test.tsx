/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import { testRender } from "@opentui/solid"
import type { AssistantMessage, Provider } from "@opencode-ai/sdk/v2"
import { AssistantModelLabel } from "../../../src/routes/session/message-model"
import type { SessionStepMessage } from "../../../src/util/model"

const providers = [
  {
    id: "llmrouter",
    models: {
      auto: { name: "Auto" },
      "luna-max": { name: "luna-max" },
      "glm-5.3-flash": { name: "glm-5.3-flash" },
      "sol-high": { name: "sol-high" },
    },
  },
] as unknown as Provider[]

function message(id: string, sessionID: string, parentID: string, responseModelIDs: string[]): SessionStepMessage {
  return {
    info: {
      id,
      sessionID,
      role: "assistant",
      time: { created: Number(id.replace(/\D/g, "")) || 1 },
      providerID: "llmrouter",
      modelID: "auto",
      parentID,
    } as SessionStepMessage["info"],
    parts: responseModelIDs.map((responseModelID, index) => ({
      id: `${id}-step-${index}`,
      sessionID,
      messageID: id,
      type: "step-finish",
      reason: "stop",
      responseModelID,
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    })),
  }
}

test("assistant message footer renders prior-turn router usage through its own message", async () => {
  const earlier = message("m1", "session-1", "user-1", ["luna-max", "luna-max"])
  const current = message("m2", "session-1", "user-2", ["glm-5.3-flash"])
  const later = message("m3", "session-1", "user-3", ["sol-high"])
  const messages = [earlier, current, later]
  const app = await testRender(
    () => (
      <text>
        <AssistantModelLabel
          message={current.info as AssistantMessage}
          providers={providers}
          messages={messages}
          turnMessages={messages.map((item) => item.info)}
        />
      </text>
    ),
    { width: 100, height: 2 },
  )

  try {
    await app.renderOnce()
    expect(app.captureCharFrame()).toContain("Auto (luna-max:2/67%, glm-5.3-flash:1/33%, sol-high:0/0%)")
  } finally {
    app.renderer.destroy()
  }
})
