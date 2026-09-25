import { afterAll, describe, expect, test } from "bun:test"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { checkChangeset, readBaseChangeset } from "./changeset-check"
import { changesetMarker, localGit } from "./sync-upstream"

const doc = [
  "## Upstream syncs",
  "",
  "- **2026-09-25** `aaa..bbb`, 3 upstream commits (sync-upstream-20260925-120000). Conflicts: none.",
  "- **2026-09-24** `ccc..aaa`, 12 upstream commits. Conflicts: `a.ts` took upstream's",
  "  (sync-upstream-20260924-080000).",
  "- **#91** A fork-change-shaped item in the wrong section.",
  "",
  "## Fork changes",
  "",
  "### Area",
  "",
  "- **#46** Sync script. _Fork-only._",
  "- **#7** Something else. Mentions #47 in passing.",
  "- **2026-09-23** `x..y`, 1 upstream commit (sync-upstream-20260923-000000). Wrong section.",
  "",
].join("\n")
const pr = { number: "46", author: "someone", head: "feature", base: "swxtch" }
const sync = (head: string) => ({ ...pr, number: "90", head })

describe("checkChangeset", () => {
  test("passes a PR that adds its own entry", () => {
    expect(checkChangeset(doc, pr, "").ok).toBe(true)
  })

  // GOAL: only an entry counts. A number mentioned in passing, or one that is a prefix of
  // another entry, does not record the PR.
  test.each(["47", "4"])("fails #%s, which is only mentioned or a prefix of an entry", (number) => {
    const result = checkChangeset(doc, { ...pr, number }, "")
    expect(result.ok).toBe(false)
    expect(result.reason).toContain(`- **#${number}**`)
  })

  // GOAL: an entry counts only in its own section.
  test("fails a fork entry that is only under Upstream syncs", () => {
    expect(checkChangeset(doc, { ...pr, number: "91" }, "").ok).toBe(false)
  })

  test.each([
    ["CHANGESET.md is missing", undefined, ""],
    ["the base cannot be read", doc, undefined],
  ])("fails closed when %s", (_, current, base) => {
    expect(checkChangeset(current, pr, base).ok).toBe(false)
  })

  test("fails a PR number that is not a number", () => {
    expect(checkChangeset(doc, { ...pr, number: "46x" }, "").reason).toContain("not a number")
  })

  // GOAL: a sync PR is checked, not exempted. It passes only by adding the sync entry that
  // names its branch, including one wrapped onto a continuation line.
  test.each(["sync-upstream-20260925-120000", "sync-upstream-20260924-080000"])(
    "passes sync branch %s whose entry names it",
    (head) => {
      expect(checkChangeset(doc, sync(head), "").ok).toBe(true)
    },
  )

  test.each([
    ["with no sync entry", "sync-upstream-20260101-000000"],
    ["whose entry is under Fork changes", "sync-upstream-20260923-000000"],
  ])("fails a sync branch %s", (_, head) => {
    const result = checkChangeset(doc, sync(head), "")
    expect(result.ok).toBe(false)
    expect(result.reason).toContain(head)
  })

  // GOAL: reusing an old sync's branch name does not pass on the entry the base already has.
  test("fails a branch reusing the name of a sync the base already records", () => {
    const result = checkChangeset(doc, sync("sync-upstream-20260925-120000"), doc)
    expect(result.ok).toBe(false)
    expect(result.reason).toContain("base already records")
  })

  // GOAL: newness against the base applies to fork entries too, not only sync entries.
  test("fails a fork PR whose entry the base already has", () => {
    const result = checkChangeset(doc, pr, doc)
    expect(result.ok).toBe(false)
    expect(result.reason).toContain("base already records #46")
  })

  // GOAL: a hand branch with the sync prefix is an ordinary PR and needs a PR entry.
  test("treats a hand branch named sync-upstream-* as an ordinary PR", () => {
    expect(checkChangeset(doc, { ...pr, head: "sync-upstream-script" }, "").ok).toBe(true)
    expect(checkChangeset(doc, { ...pr, number: "90", head: "sync-upstream-script" }, "").ok).toBe(false)
  })

  // GOAL: the remaining exemptions stay narrow, and need no base to decide.
  test.each([
    ["a push", { number: "" }],
    ["a PR to another base", { number: "9", base: "dev", head: "feature" }],
    ["Dependabot", { number: "9", author: "dependabot[bot]", base: "swxtch", head: "dependabot/npm/x" }],
  ])("passes %s without an entry", (_, input) => {
    expect(checkChangeset("", input, undefined).ok).toBe(true)
  })

  // GOAL: the real file carries entries in the shape the check looks for, so a reformat that
  // breaks the pattern fails here instead of on every later PR.
  test("CHANGESET.md records the PRs it lists in the checked shape", async () => {
    const real = await Bun.file(path.join(import.meta.dir, "..", "CHANGESET.md")).text()
    expect(checkChangeset(real, { ...pr, number: "2" }, "").ok).toBe(true)
    expect(real).toContain(changesetMarker)
  })
})

describe("readBaseChangeset", () => {
  const root = mkdtempSync(path.join(tmpdir(), "changeset-base-"))
  afterAll(() => rmSync(root, { recursive: true, force: true }))
  const env = {
    ...process.env,
    GIT_CONFIG_GLOBAL: "/dev/null",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_AUTHOR_NAME: "t",
    GIT_AUTHOR_EMAIL: "t@example.invalid",
    GIT_COMMITTER_NAME: "t",
    GIT_COMMITTER_EMAIL: "t@example.invalid",
  }
  const git = (dir: string, ...args: string[]) => localGit(dir, env)(...args).stdout

  // GOAL: CI checks out depth 1, so the base must be fetched to be read. A base without the
  // file reads as empty; a base that cannot be fetched reads as unknown, never as empty.
  test("fetches the base commit and reads CHANGESET.md from it", () => {
    const src = path.join(root, "src")
    git(root, "init", "-q", "-b", "swxtch", src)
    git(src, "commit", "-q", "--allow-empty", "-m", "before the changeset")
    const without = git(src, "rev-parse", "HEAD")
    writeFileSync(path.join(src, "CHANGESET.md"), "## Fork changes\n")
    git(src, "add", "CHANGESET.md")
    git(src, "commit", "-qm", "add the changeset")
    const withFile = git(src, "rev-parse", "HEAD")
    git(src, "commit", "-q", "--allow-empty", "-m", "the PR head")
    const clone = path.join(root, "clone")
    git(root, "clone", "-q", "--depth=1", `file://${src}`, clone)
    const shallow = localGit(clone, env)

    expect(readBaseChangeset(shallow, withFile)).toBe("## Fork changes")
    expect(readBaseChangeset(shallow, without)).toBe("")
    expect(readBaseChangeset(shallow, "0".repeat(40))).toBeUndefined()
    expect(readBaseChangeset(shallow, "not-a-sha")).toBeUndefined()
  })
})
