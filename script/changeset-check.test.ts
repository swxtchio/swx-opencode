import { describe, expect, test } from "bun:test"
import path from "node:path"
import { checkChangeset } from "./changeset-check"
import { changesetMarker } from "./sync-upstream"

const doc = [
  "## Upstream syncs",
  "",
  "- **2026-09-25** `aaa..bbb`, 3 upstream commits (sync-upstream-20260925-120000). Conflicts: none.",
  "",
  "## Fork changes",
  "",
  "- **#46** Sync script. _Fork-only._",
  "- **#7** Something else. Mentions #47 in passing, and sync-upstream-20260926-000000.",
  "",
].join("\n")
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

  // GOAL: a sync PR is checked, not exempted. It passes only with the sync entry that
  // names its branch, so a branch that merely looks like a sync cannot skip the check.
  test("passes a sync branch whose entry names it", () => {
    expect(checkChangeset(doc, { ...pr, number: "90", head: "sync-upstream-20260925-120000" }).ok).toBe(true)
  })

  test.each([
    ["with no sync entry", "sync-upstream-20260101-000000"],
    ["whose name only appears in prose", "sync-upstream-20260926-000000"],
  ])("fails a sync-shaped branch %s", (_, head) => {
    const result = checkChangeset(doc, { ...pr, number: "90", head })
    expect(result.ok).toBe(false)
    expect(result.reason).toContain(head)
  })

  // GOAL: a hand branch with the sync prefix is an ordinary PR and needs a PR entry.
  test("treats a hand branch named sync-upstream-* as an ordinary PR", () => {
    expect(checkChangeset(doc, { ...pr, head: "sync-upstream-script" }).ok).toBe(true)
    expect(checkChangeset(doc, { ...pr, number: "90", head: "sync-upstream-script" }).ok).toBe(false)
  })

  // GOAL: the remaining exemptions stay narrow: Dependabot, other bases, and runs that are
  // not pull requests.
  test.each([
    ["a push", { number: "" }],
    ["a PR to another base", { number: "9", base: "dev", head: "feature" }],
    ["Dependabot", { number: "9", author: "dependabot[bot]", base: "swxtch", head: "dependabot/npm/x" }],
  ])("passes %s without an entry", (_, input) => {
    expect(checkChangeset("", input).ok).toBe(true)
  })

  // GOAL: the real file carries entries in the shape the check looks for, so a reformat that
  // breaks the pattern fails here instead of on every later PR.
  test("CHANGESET.md records the PRs it lists in the checked shape", async () => {
    const real = await Bun.file(path.join(import.meta.dir, "..", "CHANGESET.md")).text()
    expect(checkChangeset(real, { ...pr, number: "2" }).ok).toBe(true)
    expect(real).toContain(changesetMarker)
  })
})
