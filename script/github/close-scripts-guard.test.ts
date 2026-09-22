import { describe, expect, test } from "bun:test"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

/**
 * GOAL: prove the enforcement is WIRED, not merely that the comparison helper
 * is correct.
 *
 * same-repo-guard.test.ts covers the pure function, and it stays green if the
 * `requireSameRepository(...)` call is deleted from either script - which is
 * the only thing that actually protects upstream. These tests run the real
 * scripts as subprocesses.
 *
 * No network call can occur: every case refuses before any request. The
 * matching-repository case is deliberately NOT exercised here, because that
 * path does reach the GitHub API.
 */

const SCRIPTS = [
  { name: "close-issues.ts", args: [] as string[] },
  { name: "close-prs.ts", args: ["--dry-run"] },
]

const UPSTREAM = "anomalyco/opencode"
const THIS_FORK = "swxtchio/swx-opencode"

async function run(script: string, args: string[], env: Record<string, string | undefined>) {
  // HOME must be a throwaway directory. The environment is replaced rather than
  // inherited, and with HOME unset the gh CLI - which close-prs.ts may invoke
  // for a token - writes its state relative to the cwd, which committed
  // script/.local/state/gh/device-id into the repository once already.
  const home = mkdtempSync(join(tmpdir(), "guard-test-home-"))
  const proc = Bun.spawn(["bun", new URL(script, import.meta.url).pathname, ...args], {
    // A dummy token is supplied so a failure to refuse shows up as a
    // credentials error from the API rather than a missing-token exit.
    env: { PATH: process.env.PATH ?? "", HOME: home, GITHUB_TOKEN: "dummy-not-a-real-token", ...env },
    stdout: "pipe",
    stderr: "pipe",
  })
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ])
  rmSync(home, { recursive: true, force: true })
  return { stdout, stderr, exitCode }
}

describe.each(SCRIPTS)("$name", ({ name, args }) => {
  test("refuses, and exits 0, when run in a different repository than it targets", async () => {
    const result = await run(name, args, { GITHUB_REPOSITORY: THIS_FORK })
    expect(result.stdout).toContain("refusing to act on")
    expect(result.stdout).toContain(UPSTREAM)
    expect(result.stdout).toContain(THIS_FORK)
    // Exit 0: a fork skipping inherited maintenance is correct, not a nightly
    // failure to investigate.
    expect(result.exitCode).toBe(0)
    // The decisive assertion. If enforcement were removed the script would
    // reach the API with the dummy token and report an authentication
    // failure, so the absence of one proves no request was attempted.
    expect(`${result.stdout}${result.stderr}`).not.toContain("Unauthorized")
    expect(`${result.stdout}${result.stderr}`).not.toContain("Bad credentials")
  }, 30_000)

  test("refuses, and exits 0, when GITHUB_REPOSITORY is unset, with no credentials available", async () => {
    const result = await run(name, args, {
      GITHUB_REPOSITORY: undefined,
      GITHUB_TOKEN: undefined,
      GH_TOKEN: undefined,
    })
    expect(result.stdout).toContain("GITHUB_REPOSITORY is not set")
    // Refusal must not depend on credentials being obtainable: the guard runs
    // before the token is read or `gh auth token` is invoked.
    expect(result.exitCode).toBe(0)
  }, 30_000)
})
