import { describe, expect, test } from "bun:test"
import type { Part, ReasoningPart } from "@opencode-ai/sdk/v2"
import { groupOpaqueReasoning, isOpaqueReasoning } from "../../../src/routes/session/opaque-reasoning"

// Shapes follow session/processor.ts: encrypted reasoning receives no
// reasoning-delta text, and its provider metadata is written on reasoning-end.
const ids = { sessionID: "ses_test", messageID: "msg_test" }

function encrypted(id: string): ReasoningPart {
  return {
    ...ids,
    id,
    type: "reasoning",
    text: "",
    metadata: { openai: { itemId: `rs_${id}` } },
    time: { start: 1, end: 2 },
  }
}

function summarized(id: string, text: string): ReasoningPart {
  return { ...ids, id, type: "reasoning", text, time: { start: 1, end: 2 } }
}

function emptyReasoning(id: string): ReasoningPart {
  return { ...ids, id, type: "reasoning", text: "", time: { start: 1, end: 2 } }
}

function text(id: string, value: string): Part {
  return { ...ids, id, type: "text", text: value }
}

function tool(id: string): Part {
  return {
    ...ids,
    id,
    type: "tool",
    callID: `call_${id}`,
    tool: "bash",
    state: { status: "completed", input: {}, output: "ok", title: "", metadata: {}, time: { start: 1, end: 2 } },
  }
}

function failedTool(id: string): Part {
  return {
    ...ids,
    id,
    type: "tool",
    callID: `call_${id}`,
    tool: "apply_patch",
    state: { status: "error", input: {}, error: "Patch failed", time: { start: 1, end: 2 } },
  }
}

function stepStart(id: string): Part {
  return { ...ids, id, type: "step-start" }
}

function stepFinish(id: string): Part {
  return {
    ...ids,
    id,
    type: "step-finish",
    reason: "tool-calls",
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  }
}

describe("groupOpaqueReasoning", () => {
  test("collapses a run of consecutive encrypted parts into one row", () => {
    const parts = [encrypted("a"), encrypted("b"), encrypted("c"), encrypted("d")]

    expect(groupOpaqueReasoning(parts)).toEqual([{ type: "opaque-reasoning", parts }])
  })

  test("keeps a lone encrypted part as the original part object", () => {
    const part = encrypted("a")

    const rows = groupOpaqueReasoning([part])

    expect(rows).toHaveLength(1)
    expect(rows[0]).toBe(part)
  })

  test("a tool call ends the run, and the next encrypted part starts a new one", () => {
    const a = encrypted("a")
    const b = encrypted("b")
    const call = tool("call")
    const c = encrypted("c")

    expect(groupOpaqueReasoning([a, b, call, c])).toEqual([{ type: "opaque-reasoning", parts: [a, b] }, call, c])
  })

  test("visible text ends the run, and the next encrypted part starts a new one", () => {
    const a = encrypted("a")
    const b = encrypted("b")
    const answer = text("answer", "Here is the result.")
    const c = encrypted("c")
    const d = encrypted("d")

    expect(groupOpaqueReasoning([a, b, answer, c, d])).toEqual([
      { type: "opaque-reasoning", parts: [a, b] },
      answer,
      { type: "opaque-reasoning", parts: [c, d] },
    ])
  })

  test("a failed tool call ends the run", () => {
    const a = encrypted("a")
    const b = encrypted("b")
    const failed = failedTool("failed")
    const c = encrypted("c")

    expect(groupOpaqueReasoning([a, failed, b, c])).toEqual([a, failed, { type: "opaque-reasoning", parts: [b, c] }])
  })

  test("parts that render no line do not end a run", () => {
    const a = encrypted("a")
    const b = encrypted("b")

    expect(
      groupOpaqueReasoning([a, stepFinish("finish"), stepStart("start"), text("blank", " \n"), emptyReasoning("e"), b]),
    ).toEqual([{ type: "opaque-reasoning", parts: [a, b] }])
  })

  test("summarized reasoning ends the run and renders as its own part", () => {
    const a = encrypted("a")
    const b = encrypted("b")
    const plan = summarized("plan", "Reading the config first")
    const c = encrypted("c")

    expect(groupOpaqueReasoning([a, b, plan, c])).toEqual([{ type: "opaque-reasoning", parts: [a, b] }, plan, c])
  })

  test("a [REDACTED] placeholder with metadata is still encrypted and joins the run", () => {
    const a = encrypted("a")
    const redacted: ReasoningPart = { ...encrypted("redacted"), text: "[REDACTED]" }
    const b = encrypted("b")

    expect(groupOpaqueReasoning([a, redacted, b])).toEqual([{ type: "opaque-reasoning", parts: [a, redacted, b] }])
  })

  test("returns no rows for no parts", () => {
    expect(groupOpaqueReasoning([])).toEqual([])
  })
})

describe("isOpaqueReasoning", () => {
  test("requires provider metadata and no visible text", () => {
    expect(isOpaqueReasoning(encrypted("a"))).toBe(true)
    expect(isOpaqueReasoning(summarized("s", "Reading the config"))).toBe(false)
    expect(isOpaqueReasoning(emptyReasoning("e"))).toBe(false)
  })
})
