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
  test.each([
    ["Check workflow repository guards", "script/check-workflow-guards.ts"],
    ["Test the fork guards", "bun test"],
  ])('test.yml still runs the step "%s", unconditionally', (name, runs) => {
    const workflow = Bun.YAML.parse(readFileSync(join(WORKFLOWS, "test.yml"), "utf8")) as {
      jobs?: { unit?: { steps?: { name?: string; run?: string; if?: unknown }[] } }
    }
    const step = (workflow.jobs?.unit?.steps ?? []).find((candidate) => candidate?.name === name)

    expect(step).toBeDefined()
    // Names alone are not enough - sol's finding. `if: false` on the step
    // disables the audit while leaving the name for a name-only check to find,
    // and the allowlisted job's digest cannot catch it because the disabled
    // step is what computes that digest.
    expect(step && "if" in step).toBe(false)
    expect(step?.run ?? "").toContain(runs)
  })

  // GOAL: `bun test` ignores a positional filter that matches nothing as long
  // as another one matches, so renaming a test file would silently drop it
  // from the gate rather than fail. Verified: `bun test guard-expression.test.ts
  // github/renamed-away.test.ts` ran 44 tests across 1 file and passed. The CI
  // step therefore checks the files exist first; this asserts it still does.
  test("the CI step verifies its named test files exist before running them", () => {
    const workflow = readFileSync(join(WORKFLOWS, "test.yml"), "utf8")
    const step = workflow.split("Test the fork guards")[1] ?? ""
    expect(step).toContain("ls ")
    // Every gate file, INCLUDING this one - glm-5.3 noted that this test
    // asserted the other files were listed but not itself, so its own removal
    // from the step would have gone unnoticed.
    for (const file of [
      "audit-checks.test.ts",
      "guard-expression.test.ts",
      "github/same-repo-guard.test.ts",
      "github/close-scripts-guard.test.ts",
      "github/write-guard-coverage.test.ts",
    ])
      expect(step).toContain(file)
  })
})

/**
 * Whether a source file issues a WRITE to GitHub.
 *
 * Two shapes, because there are two ways these scripts reach GitHub:
 *
 * 1. `fetch`/request options carrying a write method, matched against the
 *    nearest preceding URL. An earlier version asked only whether a file
 *    contained a write verb anywhere and mentioned api.github.com anywhere,
 *    and flagged script/stats.ts - whose only write is a POST to PostHog, with
 *    an unrelated GitHub read sixty lines below. A detector that cries wolf
 *    gets deleted, so the verb has to be associated with its target.
 * 2. The `gh` CLI with an explicit write method, which sol pointed out has no
 *    `method:` property at all and so bypassed the first shape entirely.
 *
 * Known residual: a request whose URL is a variable is invisible to a regex,
 * and `gh` subcommands that write without naming a method (`gh issue close`)
 * are not detected. This is a backstop for a future oversight, not a
 * containment boundary - the workflow guards are that.
 */
export function githubWrites(source: string): boolean {
  return writesViaFetch(source) || writesViaGhCli(source)
}

function writesViaFetch(source: string): boolean {
  // Backticks and a space before the colon are both legal and were both
  // missed. Comments are stripped first: a doc comment mentioning a GitHub URL
  // above an unrelated write was enough to flag it.
  const code = stripComments(source)
  const WRITE = /method\s*:\s*["'`](POST|PATCH|PUT|DELETE)["'`]/g

  for (const match of code.matchAll(WRITE)) {
    // Behind: a bounded window, since the URL is normally the request's first
    // argument. Ahead: only as far as the end of this options object, NOT a
    // second fixed window - a forward window re-created the very false
    // positive this detector was fixed for, matching an unrelated GitHub URL
    // further down the file.
    const before = code.slice(Math.max(0, match.index - WINDOW), match.index)
    const rest = code.slice(match.index)
    const after = rest.slice(0, rest.indexOf("}") === -1 ? 0 : rest.indexOf("}"))
    // Bare paths have to be extractable too, or a bare-path write has no
    // "nearest URL" at all and is skipped rather than matched.
    const url = /https?:\/\/[^"'`\s]+|\/(?:repos|graphql)[^"'`\s]*/g
    const behind = [...before.matchAll(url)].at(-1)?.[0]
    const ahead = [...after.matchAll(new RegExp(url.source, url.flags))].at(0)?.[0]
    if (behind && isGitHubTarget(behind)) return true
    if (ahead && isGitHubTarget(ahead)) return true
  }

  return false
}

/**
 * `gh api --method PATCH /repos/...` and `gh api -X POST ...` are GitHub
 * writes with no request options for the first shape to find.
 */
function writesViaGhCli(source: string): boolean {
  // Punctuation is flattened first so both shapes match: the shell string
  // `gh api --method PATCH ...` and the spawn array
  // `["gh", "api", "-X", "POST"]`, which glm-5.3 pointed out this directory
  // already uses elsewhere.
  const flattened = source.replace(/["'`,\[\]]/g, " ")
  return /\bgh\s+api\b[^\n]{0,200}?(?:--method|-X)\s+(POST|PATCH|PUT|DELETE)\b/i.test(flattened)
}

/**
 * Any github.com host, not just api.github.com: uploads.github.com takes
 * release-asset uploads, which are writes.
 */
function isGitHubTarget(url: string): boolean {
  if (/^https?:\/\/[^/]*\bgithub\.com\b/i.test(url)) return true
  return url.startsWith("/repos/") || url.startsWith("/graphql")
}

export function importsGuard(source: string): boolean {
  return /(?:^|\n)\s*import\s[^\n]*["'][^"'\n]*same-repo-guard["']/.test(source)
}

/** Remove comments so a URL mentioned in prose is not read as a request target. */
function stripComments(source: string): string {
  return source.replace(/\/\*[\s\S]*?\*\//g, " ").replace(/(^|[^:])\/\/[^\n]*/g, "$1")
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
      // An IMPORT, not a mention: sol noted that a comment naming the guard
      // would otherwise exempt an unguarded writer.
      .filter((file) => !importsGuard(readFileSync(file, "utf8")))
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
    // The gh CLI shapes, which have no request options at all.
    "await $`gh api --method PATCH /repos/${repo}/issues/1 -f state=closed`",
    "await $`gh api -X POST /repos/a/b/issues/1/comments -f body=hi`",
    // Release-asset uploads go to a different host.
    `await fetch("https://uploads.github.com/repos/a/b/releases/1/assets", { method: "POST" })`,
  ])("fires on a GitHub write", (sample) => {
    expect(githubWrites(sample)).toBe(true)
  })

  // GOAL: a comment or string mentioning the guard must not exempt a writer -
  // only a real import does.
  test.each(["// this file deliberately does not use same-repo-guard\n", `const note = "same-repo-guard"`])(
    "does not accept a mere mention of the guard as importing it",
    (sample) => {
      expect(importsGuard(sample)).toBe(false)
    },
  )

  test.each([
    `import { requireSameRepository } from "./same-repo-guard"`,
    `import {requireSameRepository} from './same-repo-guard'`,
  ])("accepts a real import of the guard", (sample) => {
    expect(importsGuard(sample)).toBe(true)
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

  // GOAL: the URL may be named AFTER the method within the same options
  // object, which glm-5.3 raised as a false negative.
  test("fires when the target is named after the method in the same object", () => {
    expect(githubWrites(`fetch(u, { method: "POST", url: "https://api.github.com/repos/a/b/issues/1" })`)).toBe(true)
  })

  // GOAL: but looking ahead must stop at that object. A forward window of
  // fixed size re-created the original false positive by matching an unrelated
  // GitHub URL further down the file, so the look-ahead ends at the closing
  // brace.
  test("does not fire on a non-GitHub write followed later by a GitHub read", () => {
    const sample = [
      `fetch("https://us.i.posthog.com/e/", { method: "POST" })`,
      "\n".repeat(60),
      `fetch("https://api.github.com/repos/a/b/releases")`,
    ].join("")
    expect(githubWrites(sample)).toBe(false)
  })

  // GOAL: the spawn-array form of the gh CLI, which this directory already
  // uses elsewhere, has no `method:` literal at all.
  test("fires on a gh api write spawned as an argument array", () => {
    expect(githubWrites(`Bun.spawn(["gh","api","-X","POST","/repos/a/b/issues/1/comments"])`)).toBe(true)
  })

  // GOAL: record what this deliberately does NOT catch, so the boundary is
  // asserted rather than assumed. kimi-k3's point: declining a class is
  // defensible, but then the tests should pin it so it is acknowledged rather
  // than silent. Each of these is a real GitHub write that this detector
  // misses, and the reason it is acceptable is that the WORKFLOW guards are
  // the containment boundary - this is a backstop for a future oversight.
  //
  // If one of these shapes ever appears in script/, this test goes red and
  // whoever sees it has to extend the detector rather than discover the gap
  // later.
  test.each([
    // SDK calls carry no method literal at all.
    `await octokit.rest.issues.createComment({ owner, repo, issue_number: 1, body: "hi" })`,
    `await axios.post("https://api.github.com/repos/a/b/issues/1/comments", { body: "hi" })`,
    // The verb is a variable.
    `await fetch(url, { method: verb })`,
    // gh subcommands that write without naming a method.
    "await $`gh issue close 1 --repo anomalyco/opencode`",
  ])("is known NOT to detect %p", (sample) => {
    expect(githubWrites(sample)).toBe(false)
  })

  // GOAL: the newly supported spellings, which were missed for no good reason.
  test.each([
    'await fetch("https://api.github.com/repos/a/b/issues/1", { method: `PATCH` })',
    `await fetch("https://api.github.com/repos/a/b/issues/1", { method : "PATCH" })`,
  ])("detects %p", (sample) => {
    expect(githubWrites(sample)).toBe(true)
  })

  // GOAL: a GitHub URL inside a COMMENT must not attribute an unrelated write
  // to GitHub - kimi-k3's false-positive case.
  test("does not fire on a non-GitHub write under a comment mentioning GitHub", () => {
    const sample = [
      "/** mirrors https://api.github.com/repos/a/b/webhooks */",
      `await fetch(endpoint, { method: "POST" })`,
    ].join("\n")
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
