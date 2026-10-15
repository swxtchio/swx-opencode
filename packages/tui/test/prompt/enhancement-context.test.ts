import { expect, test } from "bun:test"
import type { UserMessage } from "@opencode-ai/sdk/v2"
import { promptEnhancementContext } from "../../src/util/transcript"

const info: UserMessage = {
  id: "msg_context",
  sessionID: "ses_context",
  role: "user",
  time: { created: 1 },
  agent: "build",
  model: { providerID: "test", modelID: "test-model" },
}

const text = (input: { id: string; text: string; synthetic?: boolean; ignored?: boolean }) => ({
  id: input.id,
  messageID: info.id,
  sessionID: info.sessionID,
  type: "text" as const,
  text: input.text,
  ...(input.synthetic ? { synthetic: true } : {}),
  ...(input.ignored ? { ignored: true } : {}),
})

test("prompt enhancement context keeps the last 100 visible transcript lines", () => {
  const lines = Array.from({ length: 110 }, (_, index) => `visible-${String(index + 1).padStart(3, "0")}`)
  const context = promptEnhancementContext([
    {
      info,
      parts: [
        text({ id: "prt_visible", text: lines.join("\n") }),
        text({ id: "prt_synthetic", text: "synthetic content", synthetic: true }),
        text({ id: "prt_ignored", text: "ignored content", ignored: true }),
      ],
    },
  ])

  expect(context.split("\n")).toHaveLength(100)
  expect(context).toContain("visible-110")
  expect(context).not.toContain("visible-001")
  expect(context).not.toContain("synthetic content")
  expect(context).not.toContain("ignored content")
})

test("prompt enhancement context includes all available visible lines and allows empty history", () => {
  const context = promptEnhancementContext([{ info, parts: [text({ id: "prt_short", text: "one\ntwo" })] }])

  expect(context).toContain("one\ntwo")
  expect(context).toContain("## User")
  expect(promptEnhancementContext([])).toBe("")
})
