import { describe, expect, test } from "bun:test"
import path from "node:path"
import { checkChangeset } from "./changeset-check"
import { changesetMarker } from "./sync-upstream"

const doc =
  "## Fork changes\n\n- **#46** Sync script. _Fork-only._\n- **#7** Something else. Mentions #47 in passing.\n"
const pr = { number: "46", author: "someone", head: "feature", base: "swxtch" }

describe("checkChangeset", () => {
  test("passes a PR with its own entry", () => {
    expect(checkChangeset(doc, pr).ok).toBe(true)
  })

  // GOAL: only an entry counts. A number mentioned in passing, or one that is a prefix of
  // another entry, does not record the PR.
  test.each(["47", "4"])("fails #%s, which is only mentioned or a prefix of an entry", (number) => {
    const result = checkChangeset(doc, { ...pr, number })
    expect(result.ok).toBe(false)
    expect(result.reason).toContain(`- **#${number}**`)
  })

  test("fails when CHANGESET.md is missing", () => {
    expect(checkChangeset(undefined, pr).ok).toBe(false)
  })

  // GOAL: the exemptions stay narrow: Dependabot, sync branches, other bases, and runs that
  // are not pull requests.
  test.each([
    ["a push", { number: "" }],
    ["a PR to another base", { number: "9", base: "dev", head: "feature" }],
    ["Dependabot", { number: "9", author: "dependabot[bot]", base: "swxtch", head: "dependabot/npm/x" }],
    ["an upstream sync", { number: "9", base: "swxtch", head: "sync-upstream-20260925-120000" }],
  ])("passes %s without an entry", (_, input) => {
    expect(checkChangeset("", input).ok).toBe(true)
  })

  // GOAL: a hand-named branch cannot opt out by starting with the sync prefix.
  test("does not exempt a hand branch named sync-upstream-*", () => {
    expect(checkChangeset("", { ...pr, head: "sync-upstream-script" }).ok).toBe(false)
  })

  // GOAL: the real file carries entries in the shape the check looks for, so a reformat that
  // breaks the pattern fails here instead of on every later PR.
  test("CHANGESET.md records the PRs it lists in the checked shape", async () => {
    const real = await Bun.file(path.join(import.meta.dir, "..", "CHANGESET.md")).text()
    expect(checkChangeset(real, { ...pr, number: "2" }).ok).toBe(true)
    expect(real).toContain(changesetMarker)
  })
})
