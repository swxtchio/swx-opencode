import { describe, expect, test } from "bun:test"
import { readFileSync } from "node:fs"
import { join } from "node:path"

const SCRIPT_DIR = new URL("../", import.meta.url).pathname
const WORKFLOWS = join(SCRIPT_DIR, "../.github/workflows")

/**
 * GOAL: close the two gaps kimi-k3 raised that no other check covers.
 *
 * These tests run from the "Test the fork guards" step, which is the other
 * half of the mutual attestation described in check-workflow-guards.ts: that
 * script asserts these steps exist, and these tests assert the script is still
 * wired in. Dropping either step is then caught by the other.
 */

function scriptFiles(): string[] {
  return [...new Bun.Glob("**/*.ts").scanSync({ cwd: SCRIPT_DIR })]
    .filter((file) => !file.endsWith(".test.ts"))
    .map((file) => join(SCRIPT_DIR, file))
}

describe("the audit stays wired into CI", () => {
  // GOAL: the audit is worth nothing if its step is dropped by an upstream
  // merge conflict resolution. check-workflow-guards.ts asserts both steps
  // exist, but it can only do so while it is still being run - so this
  // asserts it from the other side.
  test.each(["Check workflow repository guards", "Test the fork guards"])(
    'test.yml still runs the step "%s"',
    (step) => {
      const workflow = Bun.YAML.parse(readFileSync(join(WORKFLOWS, "test.yml"), "utf8")) as {
        jobs?: { unit?: { steps?: { name?: string; run?: string }[] } }
      }
      const names = (workflow.jobs?.unit?.steps ?? []).map((s) => s?.name)
      expect(names).toContain(step)
    },
  )

  // GOAL: `bun test` ignores a positional filter that matches nothing as long
  // as another one matches, so renaming a test file would silently drop it
  // from the gate rather than fail. Verified: `bun test guard-expression.test.ts
  // github/renamed-away.test.ts` ran 44 tests across 1 file and passed. The CI
  // step therefore checks the files exist first; this asserts it still does.
  test("the CI step verifies its named test files exist before running them", () => {
    const workflow = readFileSync(join(WORKFLOWS, "test.yml"), "utf8")
    const step = workflow.split("Test the fork guards")[1] ?? ""
    expect(step).toContain("ls ")
    for (const file of [
      "guard-expression.test.ts",
      "github/same-repo-guard.test.ts",
      "github/close-scripts-guard.test.ts",
    ])
      expect(step).toContain(file)
  })
})

/**
 * Whether a source file issues a WRITE to the GitHub API.
 *
 * The verb has to be associated with its URL. A first attempt asked only
 * whether the file contained a write verb anywhere and mentioned
 * api.github.com anywhere, and flagged script/stats.ts - whose only write is a
 * POST to PostHog, with an unrelated GitHub read sixty lines away. A detector
 * that cries wolf gets deleted, so the verb is matched against the nearest
 * preceding URL instead.
 */
export function githubWrites(source: string): boolean {
  const WRITE = /method:\s*["'](POST|PATCH|PUT|DELETE)["']/g

  for (const match of source.matchAll(WRITE)) {
    const window = source.slice(Math.max(0, match.index - WINDOW), match.index)
    // Bare paths have to be extractable too, or a bare-path write has no
    // "nearest URL" at all and is skipped rather than matched.
    const url = /https?:\/\/[^"'`\s]+|\/(?:repos|graphql)[^"'`\s]*/g
    const urls = [...window.matchAll(url)]
    const nearest = urls.at(-1)?.[0]
    if (!nearest) continue
    // `/graphql` as well as `/repos/`: GraphQL mutations are a full GitHub
    // write surface. A full `https://api.github.com/graphql` URL is already
    // caught by the host test, but the bare-path form is not.
    if (nearest.includes("api.github.com")) return true
    if (nearest.startsWith("/repos/") || nearest.startsWith("/graphql")) return true
  }

  return false
}

/** Characters to look back for the URL a request is aimed at. */
const WINDOW = 600

describe("every GitHub-writing script carries the repository guard", () => {
  // GOAL: enforcement is opt-in per call site, so nothing would notice a
  // FUTURE script that adds a hardcoded-target write and forgets the guard.
  // This makes the invariant - a script may only write to the repository it
  // runs in - a property of the directory rather than of two files someone
  // remembered to edit.
  test("no unguarded script issues a GitHub write", () => {
    const offenders = scriptFiles()
      .filter((file) => githubWrites(readFileSync(file, "utf8")))
      .filter((file) => !readFileSync(file, "utf8").includes("same-repo-guard"))
      .map((file) => file.replace(SCRIPT_DIR, ""))

    expect(offenders).toEqual([])
  })

  // GOAL: prove the detector can fire. One that never matches anything is
  // indistinguishable from one that is broken.
  test.each([
    `await fetch("https://api.github.com/repos/x/y/issues/1", { method: "PATCH" })`,
    `await githubRequest(\`/repos/\${owner}/\${name}/issues/1/comments\`, { method: "POST" })`,
    // GraphQL mutations are a write surface too. codex raised this; the
    // full-URL form was already caught, the bare-path form was not.
    `await fetch("https://api.github.com/graphql", { method: "POST" })`,
    `await githubRequest("/graphql", { method: "POST" })`,
  ])("fires on a GitHub write", (sample) => {
    expect(githubWrites(sample)).toBe(true)
  })

  // GOAL: and that it does not fire on the shapes that are NOT GitHub writes -
  // the false positive that made the first version of this useless.
  test.each([
    // stats.ts: a PostHog POST, with an unrelated GitHub read far below.
    `await fetch("https://us.i.posthog.com/i/v0/e/", { method: "POST" })\n${"\n".repeat(60)}await fetch("https://api.github.com/repos/a/b/releases")`,
    // A GitHub read on its own.
    `await fetch("https://api.github.com/repos/a/b/releases")`,
  ])("does not fire on a non-GitHub write or a GitHub read", (sample) => {
    expect(githubWrites(sample)).toBe(false)
  })

  // GOAL: confirm it is scanning the real directory, not an empty set.
  test("it scans the close scripts, which are the known writers", () => {
    const scanned = scriptFiles().map((f) => f.replace(SCRIPT_DIR, ""))
    expect(scanned).toContain("github/close-issues.ts")
    expect(scanned).toContain("github/close-prs.ts")
    expect(scanned).toContain("stats.ts")
  })

  // GOAL: and that the two known writers are detected, so the guarded-import
  // filter above is doing real work rather than passing an empty list.
  test.each(["github/close-issues.ts", "github/close-prs.ts"])("detects the write in %s", (file) => {
    expect(githubWrites(readFileSync(join(SCRIPT_DIR, file), "utf8"))).toBe(true)
  })
})
