import { describe, expect, test } from "bun:test"
import type { Part, ReasoningPart } from "@opencode-ai/sdk/v2"
import {
  groupOpaqueReasoning,
  isOpaqueReasoning,
  type OpaqueReasoningGroups,
} from "../../../src/routes/session/opaque-reasoning"

// Shapes follow session/processor.ts: reasoning-start creates the part with empty
// text and the provider metadata, and finishReasoning adds time.end.
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

function file(id: string): Part {
  return { ...ids, id, type: "file", mime: "text/plain", url: "file:///notes.txt" }
}

// Each row by its id, or as "run:<member ids>" when the row leads a run.
function shape(groups: OpaqueReasoningGroups) {
  return groups.rows.map((row) => {
    const members = groups.runs.get(row.id)
    return members ? `run:${members.map((part) => part.id).join(",")}` : row.id
  })
}

describe("groupOpaqueReasoning", () => {
  test("collapses a run of consecutive encrypted parts into one row", () => {
    const parts = [encrypted("a"), encrypted("b"), encrypted("c"), encrypted("d")]

    const groups = groupOpaqueReasoning(parts)

    expect(shape(groups)).toEqual(["run:a,b,c,d"])
    expect(groups.runs.get("a")).toEqual(parts)
  })

  test("a lone encrypted part is a run of one and keeps its object", () => {
    const part = encrypted("a")

    const groups = groupOpaqueReasoning([part])

    expect(groups.rows).toHaveLength(1)
    expect(groups.rows[0]).toBe(part)
    expect(groups.runs.get("a")).toEqual([part])
  })

  test("a run keeps its first part's object as it grows", () => {
    const first = encrypted("a")

    const before = groupOpaqueReasoning([first, encrypted("b")])
    const after = groupOpaqueReasoning([first, encrypted("b"), encrypted("c")])

    expect(before.rows[0]).toBe(first)
    expect(after.rows[0]).toBe(first)
    expect(after.runs.get("a")).toHaveLength(3)
  })

  test("a tool call ends the run, and the next encrypted part starts a new one", () => {
    const groups = groupOpaqueReasoning([encrypted("a"), encrypted("b"), tool("call"), encrypted("c")])

    expect(shape(groups)).toEqual(["run:a,b", "call", "run:c"])
  })

  test("visible text ends the run, and the next encrypted part starts a new one", () => {
    const groups = groupOpaqueReasoning([
      encrypted("a"),
      encrypted("b"),
      text("answer", "Here is the result."),
      encrypted("c"),
      encrypted("d"),
    ])

    expect(shape(groups)).toEqual(["run:a,b", "answer", "run:c,d"])
  })

  test("a failed tool call ends the run", () => {
    const groups = groupOpaqueReasoning([encrypted("a"), failedTool("failed"), encrypted("b"), encrypted("c")])

    expect(shape(groups)).toEqual(["run:a", "failed", "run:b,c"])
  })

  test("parts that render no line do not end a run, and stay in order", () => {
    const groups = groupOpaqueReasoning([
      encrypted("a"),
      stepFinish("finish"),
      stepStart("start"),
      text("blank", " \n"),
      emptyReasoning("e"),
      encrypted("b"),
    ])

    expect(shape(groups)).toEqual(["run:a,b", "finish", "start", "blank", "e"])
  })

  test("a part type the view does not render yet keeps its place", () => {
    const groups = groupOpaqueReasoning([encrypted("a"), file("attachment"), encrypted("b"), tool("call")])

    expect(shape(groups)).toEqual(["run:a,b", "attachment", "call"])
  })

  test("summarized reasoning ends the run and renders as its own part", () => {
    const groups = groupOpaqueReasoning([
      encrypted("a"),
      encrypted("b"),
      summarized("plan", "Reading the config first"),
      encrypted("c"),
    ])

    expect(shape(groups)).toEqual(["run:a,b", "plan", "run:c"])
  })

  test("a [REDACTED] placeholder with metadata is still encrypted and joins the run", () => {
    const redacted: ReasoningPart = { ...encrypted("redacted"), text: "[REDACTED]" }

    const groups = groupOpaqueReasoning([encrypted("a"), redacted, encrypted("b")])

    expect(shape(groups)).toEqual(["run:a,redacted,b"])
  })

  test("returns no rows for no parts", () => {
    const groups = groupOpaqueReasoning([])

    expect(groups.rows).toEqual([])
    expect(groups.runs.size).toBe(0)
  })
})

describe("isOpaqueReasoning", () => {
  test("requires provider metadata and no visible text", () => {
    expect(isOpaqueReasoning(encrypted("a"))).toBe(true)
    expect(isOpaqueReasoning(summarized("s", "Reading the config"))).toBe(false)
    expect(isOpaqueReasoning(emptyReasoning("e"))).toBe(false)
  })
})
