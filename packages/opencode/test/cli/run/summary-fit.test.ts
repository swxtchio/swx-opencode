import { describe, expect, test } from "bun:test"
import { elide, fitSummaryModel, splitModel } from "@/cli/cmd/run/summary-fit"

const parts = {
  agent: "Build",
  model: "Firerouter glm (glm-5p3-flash → glm-5p3)",
  duration: "1m12s",
}

/** Rebuild the rendered line so assertions are about what a reader sees. */
const render = (width: number) => {
  const model = fitSummaryModel(parts, width)
  return `▣ ${parts.agent} · ${model} · ${parts.duration}`
}

describe("splitModel", () => {
  test("separates a trailing served parenthetical", () => {
    expect(splitModel("Firerouter glm (glm-5p3-flash → glm-5p3)")).toEqual({
      label: "Firerouter glm",
      served: "(glm-5p3-flash → glm-5p3)",
    })
  })

  test.each(["Claude Opus 5", "gpt-5.6-luna"])("leaves %s alone when there is no parenthetical", (model) => {
    expect(splitModel(model)).toEqual({ label: model, served: "" })
  })

  // GOAL: only a TRAILING group is the served part. A parenthetical in the
  // middle of a name is part of the name.
  test("ignores a parenthetical that is not at the end", () => {
    expect(splitModel("foo (bar) baz")).toEqual({ label: "foo (bar) baz", served: "" })
  })
})

describe("fitSummaryModel", () => {
  // GOAL: #13's core complaint. Truncating the whole line from the right cut
  // the duration first. It must survive at every width where the line renders
  // at all.
  test.each([120, 80, 60, 50, 40, 30])("keeps the duration at %i columns", (width) => {
    expect(render(width)).toContain(parts.duration)
  })

  test.each([120, 80, 60, 50, 40, 30])("keeps the agent at %i columns", (width) => {
    expect(render(width)).toContain("▣ Build")
  })

  // GOAL: the line must actually fit, or nothing above it matters - the
  // terminal would truncate it again and undo the ordering.
  test.each([120, 80, 60, 50, 40, 30])("fits within %i columns", (width) => {
    expect(render(width).length).toBeLessThanOrEqual(width)
  })

  // GOAL: at a comfortable width nothing is shortened at all.
  test("renders everything unchanged when there is room", () => {
    expect(fitSummaryModel(parts, 120)).toBe(parts.model)
    expect(render(120)).toBe("▣ Build · Firerouter glm (glm-5p3-flash → glm-5p3) · 1m12s")
  })

  // GOAL: the served parenthetical outranks the configured label, because it
  // carries what the configured label cannot - the model that actually served
  // the turn.
  test("sacrifices the configured label before the served models", () => {
    const fitted = fitSummaryModel(parts, 50)
    expect(fitted).toContain("glm-5p3-flash")
    expect(fitted).toContain("…")
  })

  // GOAL: with no parenthetical there is nothing to protect, so the label is
  // simply shortened.
  test("shortens a plain model label", () => {
    const plain = { ...parts, model: "a-very-long-model-identifier-indeed" }
    const fitted = fitSummaryModel(plain, 40)
    expect(fitted.endsWith("…")).toBe(true)
    expect(`▣ ${plain.agent} · ${fitted} · ${plain.duration}`.length).toBeLessThanOrEqual(40)
  })

  // GOAL: degrade rather than render a stub. Below the point where the label
  // can keep even one character, it goes entirely.
  test("drops the label rather than leaving one character of it", () => {
    const fitted = fitSummaryModel(parts, 34)
    expect(fitted.startsWith("(")).toBe(true)
  })

  // GOAL: an absurd width must not throw or produce a negative slice.
  test.each([0, 1, 5, 10, -3])("survives a width of %i", (width) => {
    expect(() => fitSummaryModel(parts, width)).not.toThrow()
  })
})

describe("elide", () => {
  test("leaves a value that already fits", () => {
    expect(elide("abc", 5)).toBe("abc")
  })

  test("marks a shortened value", () => {
    expect(elide("abcdef", 4)).toBe("abc…")
  })

  test.each([0, -1])("returns nothing for a width of %i", (width) => {
    expect(elide("abc", width)).toBe("")
  })

  test("uses a single column for the marker at width 1", () => {
    expect(elide("abcdef", 1)).toBe("…")
  })
})
