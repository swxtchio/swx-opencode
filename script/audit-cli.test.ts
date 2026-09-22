import { describe, expect, test } from "bun:test"
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

/**
 * GOAL: prove the audit's COMMAND LINE fails, not just that its helpers return
 * the right values.
 *
 * codex found that every test added so far imports helper functions, so
 * `main()` itself was unexercised: changing
 *
 *   if (violations.length === 0) return   ->   return
 *
 * makes the audit always exit 0 while all 153 helper tests stay green. That is
 * the tenth instance of this branch's recurring defect - the helpers are
 * present and correct, and nothing asserted the behaviour that uses them.
 *
 * These tests run the real script as a subprocess in a faithful copy of the
 * repository layout. The script reads `../.github/workflows/` relative to its
 * own location and imports nothing local, so a copy at <tmp>/script/ reads
 * <tmp>/.github/workflows/.
 */

const REPO = new URL("../", import.meta.url).pathname
const SCRIPT = "script/check-workflow-guards.ts"

function fixture(): string {
  const root = mkdtempSync(join(tmpdir(), "audit-cli-"))
  mkdirSync(join(root, "script"), { recursive: true })
  mkdirSync(join(root, ".github"), { recursive: true })
  cpSync(join(REPO, ".github/workflows"), join(root, ".github/workflows"), { recursive: true })
  cpSync(join(REPO, ".github/actions"), join(root, ".github/actions"), { recursive: true })
  cpSync(join(REPO, SCRIPT), join(root, SCRIPT))
  return root
}

async function runAudit(root: string) {
  const proc = Bun.spawn(["bun", "run", SCRIPT], {
    cwd: root,
    env: { PATH: process.env.PATH ?? "", HOME: root },
    stdout: "pipe",
    stderr: "pipe",
  })
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ])
  return { stdout, stderr, exitCode }
}

describe("the audit command line", () => {
  // GOAL: establish the harness is faithful. If the copy did not reproduce the
  // repository the failing case below would prove nothing.
  test("exits 0 on the real tree", async () => {
    const root = fixture()
    try {
      const result = await runAudit(root)
      expect(result.stderr).toBe("")
      expect(result.stdout).toContain("guarded")
      expect(result.exitCode).toBe(0)
    } finally {
      rmSync(root, { recursive: true, force: true })
    }
  }, 60_000)

  // GOAL: the decisive one. This fails if `main()` stops failing - which is
  // exactly codex's mutation, and which no helper test can observe.
  test("exits 1, and names the job, when a guard is removed", async () => {
    const root = fixture()
    try {
      const path = join(root, ".github/workflows/unlock.yml")
      const before = readFileSync(path, "utf8")
      const after = before.replace(/^\s*if: github\.repository == 'anomalyco\/opencode'\n/m, "")
      // Guard the fixture itself: a mutation that does not apply would make
      // this test pass for the wrong reason.
      expect(after).not.toBe(before)
      writeFileSync(path, after)

      const result = await runAudit(root)
      expect(result.exitCode).toBe(1)
      expect(result.stderr).toContain("unlock.yml")
      expect(result.stderr).toContain("no condition at all")
    } finally {
      rmSync(root, { recursive: true, force: true })
    }
  }, 60_000)

  // GOAL: and that it fails for the audit-disabling edits too, through the real
  // command line rather than through a helper call.
  test("exits 1 when the audit's own job is made conditional", async () => {
    const root = fixture()
    try {
      const path = join(root, ".github/workflows/test.yml")
      const before = readFileSync(path, "utf8")
      const after = before.replace("  unit:\n", "  unit:\n    if: false\n")
      expect(after).not.toBe(before)
      writeFileSync(path, after)

      const result = await runAudit(root)
      expect(result.exitCode).toBe(1)
      expect(result.stderr).toContain("unconditional")
    } finally {
      rmSync(root, { recursive: true, force: true })
    }
  }, 60_000)
})
