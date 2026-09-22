#!/usr/bin/env bun
/**
 * GOAL: prove that no inherited upstream workflow can take an outward-facing
 * action from this fork.
 *
 * Every job in every workflow must either
 *   - carry the canonical repository guard, so it evaluates false here, or
 *   - be permanently disabled (`if: false`), or
 *   - be named in ALLOWED as a workflow this fork deliberately runs.
 *
 * Success means: exit 0 with every job accounted for. A job added by a future
 * upstream merge is unaccounted for by default, which is the point - the check
 * fails closed rather than silently widening coverage.
 */

const GUARD = "github.repository == 'anomalyco/opencode'"

/**
 * Jobs this fork adapted for its own use and must keep running, keyed
 * `<workflow>::<job>`.
 *
 * Deliberately per-job rather than per-workflow: if it were per-workflow, a job
 * added to test.yml by a future upstream merge would be allowed silently, which
 * is the same shape as the bug this check exists to catch - publish.yml was
 * guarded on four jobs out of five.
 */
const ALLOWED = new Set(["test.yml::unit", "test.yml::e2e", "typecheck.yml::typecheck"])

type Violation = { workflow: string; job: string; reason: string; found: string }

async function main() {
  const dir = new URL("../.github/workflows/", import.meta.url)
  const files = [...new Bun.Glob("*.{yml,yaml}").scanSync({ cwd: Bun.fileURLToPath(dir) })].sort()
  if (files.length === 0) throw new Error("no workflows found - is this running from the repository root?")

  const violations: Violation[] = []
  let guarded = 0
  let disabled = 0
  let allowed = 0
  const matched: string[] = []

  for (const file of files) {
    const parsed = Bun.YAML.parse(await Bun.file(new URL(file, dir)).text()) as {
      jobs?: Record<string, { if?: unknown }>
    }
    const jobs = parsed?.jobs
    if (!jobs || typeof jobs !== "object") {
      violations.push({ workflow: file, job: "-", reason: "workflow declares no jobs", found: "" })
      continue
    }

    for (const [job, body] of Object.entries(jobs)) {
      // `if:` may parse as a boolean when written bare, e.g. `if: false`.
      const condition = typeof body?.if === "string" ? body.if : body?.if === false ? "false" : ""

      if (ALLOWED.has(`${file}::${job}`)) {
        allowed++
        matched.push(`${file}::${job}`)
        continue
      }
      if (condition.trim() === "false") {
        disabled++
        continue
      }
      if (condition.includes(GUARD)) {
        guarded++
        continue
      }
      violations.push({
        workflow: file,
        job,
        reason: condition ? "condition does not include the repository guard" : "no condition at all",
        found: condition,
      })
    }
  }

  console.log(
    `checked ${files.length} workflows: ${guarded} guarded, ${disabled} disabled, ` +
      `${allowed}/${ALLOWED.size} allowed (${[...ALLOWED].join(", ")})`,
  )

  // An allowlist entry that matches nothing is stale - the job was renamed or
  // removed - and a stale entry silently stops protecting whatever replaced it.
  if (allowed !== ALLOWED.size) {
    const seen = new Set(matched)
    const stale = [...ALLOWED].filter((entry) => !seen.has(entry))
    violations.push({
      workflow: "-",
      job: "-",
      reason: `allowlist entries match no job, so they no longer protect anything: ${stale.join(", ")}`,
      found: "",
    })
  }

  if (violations.length === 0) return

  console.error(`\n${violations.length} job(s) can run from this fork:\n`)
  for (const v of violations) {
    console.error(`  ${v.workflow} :: ${v.job}`)
    console.error(`      ${v.reason}`)
    if (v.found) console.error(`      if: ${v.found}`)
  }
  console.error(`\nAdd  if: ${GUARD}  to each job, or add the workflow to ALLOWED in this script`)
  console.error(`with a reason if this fork genuinely needs it to run.`)
  process.exit(1)
}

await main()
