import { describe, expect, test } from "bun:test"
import { displaySlice } from "../../src/prompt/display"
import type { SessionPromptQueueItem } from "@opencode-ai/sdk/v2"
import { expandedOffset, expandTrackedPastedText, recalledPrompt, stripPromptPartIDs } from "../../src/prompt/part"

describe("prompt part", () => {
  test("strips persisted IDs from reused parts", () => {
    expect(
      stripPromptPartIDs({
        id: "prt_old",
        sessionID: "ses_old",
        messageID: "msg_old",
        type: "file" as const,
        mime: "image/png",
        filename: "tiny.png",
        url: "data:image/png;base64,abc",
      }),
    ).toEqual({
      type: "file",
      mime: "image/png",
      filename: "tiny.png",
      url: "data:image/png;base64,abc",
    })
  })

  test("preserves wide characters around pasted text", () => {
    const marker = "[Pasted ~3 lines]"
    const prefix = "你好你好\n"

    expect(
      expandTrackedPastedText(prefix + marker + "\n阿斯顿法国红酒看来", [
        {
          start: Bun.stringWidth("你好你好") + 1,
          end: Bun.stringWidth("你好你好") + 1 + Bun.stringWidth(marker),
          text: "public:\n\tvoid ExecuteTask();\nprivate:",
        },
      ]),
    ).toBe("你好你好\npublic:\n\tvoid ExecuteTask();\nprivate:\n阿斯顿法国红酒看来")
  })

  test("only expands the tracked placeholder occurrence", () => {
    const marker = "[Pasted ~3 lines]"
    const prefix = `keep ${marker} then `

    expect(
      expandTrackedPastedText(prefix + marker + " tail", [
        {
          start: Bun.stringWidth(prefix),
          end: Bun.stringWidth(prefix + marker),
          text: "alpha\nbeta\ngamma",
        },
      ]),
    ).toBe(`keep ${marker} then alpha\nbeta\ngamma tail`)
  })

  // swxtchio/swx-opencode#249: a withdrawn prompt returns to the composer as it was typed.
  const queued = (parts: SessionPromptQueueItem["input"]["parts"]): SessionPromptQueueItem => ({
    id: "que_recalled",
    sessionID: "ses_recalled",
    seq: 1,
    delivery: "steer",
    time: { created: 1 },
    origins: [{ time: 1 }],
    input: { parts },
  })
  const fileAt = (at: { value: string; start: number; end: number }) => ({
    type: "file" as const,
    mime: "text/plain",
    filename: "app.ts",
    url: "file:///repo/src/app.ts",
    source: { type: "file" as const, path: "src/app.ts", text: at },
  })
  // Where each part's mention marks the recalled text.
  const marked = (recalled: ReturnType<typeof recalledPrompt>) =>
    recalled.parts.map((part) => {
      const at = part.type === "agent" ? part.source : part.type === "file" ? part.source?.text : undefined
      return at ? [at.start, displaySlice(recalled.input, at.start, at.end)] : undefined
    })

  test("a recalled prompt keeps its typed text and every part, not the context the composer added", () => {
    const marker = "[Pasted ~2 lines]"
    const typed = `see ${marker} in 你好 @src/app.ts and @explore`
    // Offsets are the composer's, as an older client sent them before expanding the paste.
    const at = (value: string) => {
      const start = Bun.stringWidth(typed.slice(0, typed.indexOf(value)))
      return { value, start, end: start + Bun.stringWidth(value) }
    }
    const sent = expandTrackedPastedText(typed, [{ ...at(marker), text: "alpha\nbeta" }])
    const recalled = recalledPrompt(
      queued([
        { type: "text", text: "<editor context>", synthetic: true },
        { type: "text", text: sent },
        fileAt(at("@src/app.ts")),
        { type: "agent", name: "explore", source: at("@explore") },
        // An attachment sent with no mention in the text.
        { type: "file", mime: "image/png", filename: "shot.png", url: "data:image/png;base64,AA" },
      ]),
    )

    expect(recalled.input).toBe("see alpha\nbeta in 你好 @src/app.ts and @explore [Image 1]")
    expect(recalled.parts.map((part) => (part.type === "file" ? part.url : part.type))).toEqual([
      "file:///repo/src/app.ts",
      "agent",
      "data:image/png;base64,AA",
    ])
    expect(marked(recalled).map((mark) => mark?.[1])).toEqual(["@src/app.ts", "@explore", "[Image 1]"])
  })

  test("a recalled prompt keeps each of two identical mentions where it was sent", () => {
    const marker = "[Pasted ~3 lines]"
    const typed = `see ${marker} then @src/app.ts and @src/app.ts`
    const occurrence = (from: number) => {
      const start = Bun.stringWidth(typed.slice(0, typed.indexOf("@src/app.ts", from)))
      return { value: "@src/app.ts", start, end: start + Bun.stringWidth("@src/app.ts") }
    }
    const first = occurrence(0)
    const second = occurrence(typed.indexOf("@src/app.ts") + 1)
    const pasted = [
      { start: 4, end: 4 + marker.length, text: "a paste of three lines\nthat runs on and on\nfor long enough" },
    ]
    const sent = expandTrackedPastedText(typed, pasted)
    // As the composer sends them: each mention shifted past the expanded paste.
    const shifted = (at: typeof first) => ({
      ...at,
      start: expandedOffset(at.start, pasted),
      end: expandedOffset(at.end, pasted),
    })
    const expected = [...sent.matchAll(/@src\/app\.ts/g)].map((match) => [match.index, "@src/app.ts"])

    const recalled = recalledPrompt(
      queued([{ type: "text", text: sent }, fileAt(shifted(first)), fileAt(shifted(second))]),
    )
    expect(marked(recalled)).toEqual(expected)
    // Sent unshifted, as an older client did, both are nearest the first copy;
    // each still takes its own.
    const relocated = recalledPrompt(queued([{ type: "text", text: sent }, fileAt(first), fileAt(second)]))
    expect(marked(relocated)).toEqual(expected)
  })
})
