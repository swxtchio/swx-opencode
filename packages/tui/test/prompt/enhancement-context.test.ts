import { expect, test } from "bun:test"
import type { AssistantMessage, Part, UserMessage } from "@opencode-ai/sdk/v2"
import { promptEnhancementContext } from "../../src/util/transcript"

const hidden = { thinking: false, toolDetails: false, assistantMetadata: false }

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
  ], hidden)

  expect(context.split("\n")).toHaveLength(100)
  expect(context).toContain("visible-110")
  expect(context).not.toContain("visible-001")
  expect(context).not.toContain("synthetic content")
  expect(context).not.toContain("ignored content")
})

test("prompt enhancement context includes all available visible lines and allows empty history", () => {
  const context = promptEnhancementContext([{ info, parts: [text({ id: "prt_short", text: "one\ntwo" })] }], hidden)

  expect(context).toContain("one\ntwo")
  expect(context).toContain("## User")
  expect(promptEnhancementContext([], hidden)).toBe("")
})

test("prompt enhancement context follows visible reasoning and completed tool details", () => {
  const assistant: AssistantMessage = {
    id: "msg_assistant_context",
    sessionID: info.sessionID,
    role: "assistant",
    parentID: info.id,
    mode: "primary",
    agent: "build",
    modelID: "test-model",
    providerID: "test",
    path: { cwd: "/repo", root: "/repo" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 2, completed: 3 },
  }
  const parts: Part[] = [
    {
      id: "prt_reasoning_context",
      sessionID: info.sessionID,
      messageID: assistant.id,
      type: "reasoning",
      text: "expanded visible reasoning",
      time: { start: 2, end: 3 },
    },
    {
      id: "prt_tool_context",
      sessionID: info.sessionID,
      messageID: assistant.id,
      type: "tool",
      callID: "call_context",
      tool: "bash",
      state: {
        status: "completed",
        input: { command: "printf visible-tool-output" },
        output: "visible-tool-output",
        title: "Print visible output",
        metadata: {},
        time: { start: 2, end: 3 },
      },
    },
  ]
  const visible = promptEnhancementContext([{ info: assistant, parts }], {
    thinking: true,
    toolDetails: true,
    assistantMetadata: true,
  })
  expect(visible).toContain("expanded visible reasoning")
  expect(visible).toContain("visible-tool-output")
  expect(visible).toContain("## Assistant (Build · test-model")

  const collapsed = promptEnhancementContext([{ info: assistant, parts }], hidden)
  expect(collapsed).not.toContain("expanded visible reasoning")
  expect(collapsed).not.toContain("Tool: bash")
  expect(collapsed).not.toContain("visible-tool-output")
  expect(collapsed).toContain("## Assistant")
})
