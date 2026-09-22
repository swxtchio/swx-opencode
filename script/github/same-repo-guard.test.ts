import { describe, expect, test } from "bun:test"
import { checkSameRepository } from "./same-repo-guard"

const TARGET = "anomalyco/opencode"

describe("checkSameRepository", () => {
  // GOAL: upstream's own scheduled runs are untouched by this guard.
  // Success means the guard permits the write when the script runs in the
  // very repository it targets.
  test("permits a write when the target is the repository being run in", () => {
    expect(checkSameRepository(TARGET, { GITHUB_REPOSITORY: TARGET })).toEqual({ ok: true })
  })

  // GOAL: the observed incident cannot recur. This fork's cron targeted
  // anomalyco/opencode while running in swxtchio/swx-opencode and was stopped
  // only by a 403. Success means the guard refuses before any request.
  test("refuses a write aimed at another repository", () => {
    const result = checkSameRepository(TARGET, { GITHUB_REPOSITORY: "swxtchio/swx-opencode" })
    expect(result.ok).toBe(false)
    if (result.ok) throw new Error("unreachable")
    // Assert the contract - both repositories named - not the exact wording.
    expect(result.reason).toContain(TARGET)
    expect(result.reason).toContain("swxtchio/swx-opencode")
  })

  // GOAL: fail closed. An unset GITHUB_REPOSITORY cannot confirm the target,
  // so the write must be refused rather than aimed at the hardcoded default.
  test("refuses when GITHUB_REPOSITORY is unset", () => {
    const result = checkSameRepository(TARGET, {})
    expect(result.ok).toBe(false)
    if (result.ok) throw new Error("unreachable")
    expect(result.reason).toContain("GITHUB_REPOSITORY")
  })

  // GOAL: an empty or whitespace value is absence, not a repository name.
  // Without this, `GITHUB_REPOSITORY=""` would compare unequal to the target
  // and produce the "while running in " message with a blank name.
  test.each(["", "   "])("refuses a blank GITHUB_REPOSITORY (%p) as unset", (value) => {
    const result = checkSameRepository(TARGET, { GITHUB_REPOSITORY: value })
    expect(result.ok).toBe(false)
    if (result.ok) throw new Error("unreachable")
    expect(result.reason).toContain("GITHUB_REPOSITORY is not set")
  })

  // GOAL: the comparison is exact. A repository whose name merely contains the
  // target as a substring, or differs only in case, is a different repository.
  test.each(["anomalyco/opencode-fork", "ANOMALYCO/OPENCODE", "other/anomalyco/opencode"])(
    "refuses %s as not the target repository",
    (actual) => {
      expect(checkSameRepository(TARGET, { GITHUB_REPOSITORY: actual }).ok).toBe(false)
    },
  )
})
