import { describe, expect, test } from "bun:test"
import { displaySlice } from "../../src/prompt/display"
import { expandTrackedPastedText, recalledPrompt, stripPromptPartIDs } from "../../src/prompt/part"

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
  test("a recalled prompt keeps its typed text and mentions, not the context the composer added", () => {
    const marker = "[Pasted ~2 lines]"
    const typed = `see ${marker} in 你好 @src/app.ts and @explore`
    // Offsets are the composer's, taken before the paste was expanded for sending.
    const at = (value: string) => {
      const start = Bun.stringWidth(typed.slice(0, typed.indexOf(value)))
      return { value, start, end: start + Bun.stringWidth(value) }
    }
    const sent = expandTrackedPastedText(typed, [{ ...at(marker), text: "alpha\nbeta" }])
    const recalled = recalledPrompt({
      id: "que_recalled",
      sessionID: "ses_recalled",
      seq: 1,
      delivery: "steer",
      time: { created: 1 },
      origins: [{ time: 1 }],
      input: {
        parts: [
          { type: "text", text: "<editor context>", synthetic: true },
          { type: "text", text: sent },
          {
            type: "file",
            mime: "text/plain",
            filename: "app.ts",
            url: "file:///repo/src/app.ts",
            source: { type: "file", path: "src/app.ts", text: at("@src/app.ts") },
          },
          { type: "agent", name: "explore", source: at("@explore") },
          { type: "file", mime: "text/plain", url: "file:///repo/gone.ts" },
        ],
      },
    })

    expect(recalled.input).toBe("see alpha\nbeta in 你好 @src/app.ts and @explore")
    expect(recalled.parts.map((part) => part.type)).toEqual(["file", "agent"])
    // Each mention marks its text where the recalled text now holds it.
    const [file, agent] = recalled.parts
    if (file?.type !== "file" || agent?.type !== "agent") throw new Error("mentions were not recalled")
    expect(displaySlice(recalled.input, file.source!.text.start, file.source!.text.end)).toBe("@src/app.ts")
    expect(displaySlice(recalled.input, agent.source!.start, agent.source!.end)).toBe("@explore")
  })
})
