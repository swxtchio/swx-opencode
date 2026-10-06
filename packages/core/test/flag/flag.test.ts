import { describe, expect, test } from "bun:test"
import { isFffDisabled } from "@opencode-ai/core/flag/flag"

describe("FFF platform default", () => {
  test("keeps native FFF enabled in compiled Windows distributions", () => {
    expect(isFffDisabled(undefined, "win32", true)).toBe(false)
    expect(isFffDisabled(undefined, "win32", false)).toBe(true)
    expect(isFffDisabled(undefined, "linux", true)).toBe(false)
    expect(isFffDisabled("true", "win32", true)).toBe(true)
  })
})
