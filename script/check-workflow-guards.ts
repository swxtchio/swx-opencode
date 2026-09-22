#!/usr/bin/env bun

import { existsSync, readFileSync } from "node:fs"
/**
 * GOAL: prove that no inherited upstream workflow can take an outward-facing
 * action from this fork.
 *
 * Delivery path: this runs as a step in test.yml::unit, which today has no
 * `paths:` filter, so every push to the default branch and every pull request
 * is audited. Adding a `paths:` filter to test.yml, or moving this step, would
 * silently stop auditing workflow-only changes - the failure mode being
 * silence rather than a red check.
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
const ALLOWED = new Map([
  ["test.yml::unit", "c8ce2714c491ebb7"],
  ["test.yml::e2e", "b945108295daf472"],
  ["typecheck.yml::typecheck", "b82d5ddbd87d3df7"],
])

/**
 * Digest of the workflow ENVELOPE - everything except `jobs` - for each
 * workflow with an allowlisted job.
 *
 * The per-job digest does not cover `on`, `permissions`, `env` or `defaults`.
 * That leaves the audit able to be switched off without any digest changing:
 * add a `paths:` filter to test.yml and workflow-only changes stop being
 * audited, silently.
 */
const ALLOWED_ACTIONS = new Map([["setup-bun", "0b8495c0a438973b"]])

/**
 * Digest of each local composite action an allowlisted job calls out to.
 *
 * codex raised this as the third drift axis: the job digest pins a job's own
 * YAML and the envelope digest pins everything but the jobs, so both can pass
 * while `./.github/actions/setup-bun` - which every allowlisted job uses -
 * changes underneath them with an upstream merge. What an allowlisted job
 * RUNS is part of what is being trusted.
 */
const ALLOWED_ENVELOPES = new Map([
  ["test.yml", "3fc9bf50af782993"],
  ["typecheck.yml", "91a87487fd18a357"],
])

/**
 * The workflow carrying this audit, and the property that has to hold for the
 * audit to mean anything: it must run on every pull request and every push to
 * the default branch, with nothing narrowing which paths trigger it.
 *
 * Checked structurally rather than by digest alone, because the digest only
 * says "someone changed this" - it does not say which change would be harmful.
 */
const AUDIT_WORKFLOW = "test.yml"

/**
 * This fork's default branch, as a fallback for local runs.
 *
 * Scheduled workflows only ever fire from the default branch, and it is the
 * branch the audit has to cover on push. A hardcoded name rots silently: if
 * the default branch were renamed, a stale `branches: [swxtch]` would keep
 * satisfying the assertion while no longer covering anything, which codex
 * raised. So in CI the real value is read from the event payload and the
 * constant is checked against it.
 */
const DEFAULT_BRANCH = "swxtch"

/**
 * The default branch GitHub itself reports, or undefined outside Actions.
 *
 * `github.event.repository.default_branch` is not an environment variable, but
 * the whole event payload is on disk at GITHUB_EVENT_PATH, so the authoritative
 * value is available rather than assumed.
 */
function reportedDefaultBranch(): string | undefined {
  const path = process.env.GITHUB_EVENT_PATH
  if (!path) return undefined
  try {
    const event = JSON.parse(readFileSync(path, "utf8")) as { repository?: { default_branch?: unknown } }
    const branch = event?.repository?.default_branch
    return typeof branch === "string" && branch ? branch : undefined
  } catch {
    // A malformed or unreadable payload must not fail the audit for an
    // unrelated reason; the constant still applies and its staleness simply
    // goes unchecked on this run.
    return undefined
  }
}

/**
 * The two steps that run this audit and its tests, and the job they live in.
 *
 * They attest to each other. This script asserts both steps exist; the test
 * suite the second step runs asserts the same thing. So dropping either one -
 * the likeliest accident, a conflict resolution during an upstream merge -
 * is caught by the other.
 *
 * The residual is honest: removing BOTH in one edit leaves nothing running to
 * notice, because the digest that would change is computed only by the step
 * being removed. That is a bootstrap limit of any in-CI check, and it takes a
 * deliberate two-part edit that is plainly visible in a diff.
 */
const AUDIT_JOB = "unit"

/**
 * Each step is identified by name and checked for what it actually runs.
 *
 * Names alone are not enough: sol pointed out that adding `if: false` to the
 * audit step disables it while both names remain present, and the allowlisted
 * job's digest cannot help because the disabled step is what computes it. So a
 * step must also carry no condition and still invoke its command.
 */
const AUDIT_STEPS = [
  { name: "Check workflow repository guards", runs: "script/check-workflow-guards.ts" },
  { name: "Test the fork guards", runs: "bun test" },
]

/**
 * Digest of an allowlisted job's definition.
 *
 * An allowlist keyed only by name trusts the job's CONTENTS forever: a future
 * upstream merge could add a publishing step, a `uses:` reusable workflow or
 * `secrets: inherit` to test.yml::unit and this check would still pass. Pinning
 * the definition means any change to an allowed job fails until someone looks
 * at it and updates the digest deliberately.
 */
export function digest(job: unknown): string {
  return Bun.SHA256.hash(JSON.stringify(job, canonical), "hex").slice(0, 16)
}

/** Sort object keys so key order in the YAML cannot change the digest. */
function canonical(_key: string, value: unknown) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return value
  // Codepoint order, not localeCompare: collation is locale-dependent in
  // principle, and a different collation would reorder keys and break every
  // pinned digest with a message that explains nothing.
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
  )
}

type Violation = { workflow: string; job: string; reason: string; found: string }

/**
 * Whether a job is switched off by its `if` value, rather than merely
 * unguarded.
 *
 * Only a YAML boolean `false` counts. Two reviewers disagreed about why, and
 * sol is the one who is right: GitHub evaluates an `if` value as an expression
 * even without `${{ }}`, so `if: 'false'` is the expression `false` and IS
 * falsy - kimi's claim that the quoted form runs was wrong. `if: ${{ 'false' }}`
 * is the truthy case, because there the expression source is a string literal.
 *
 * The strictness is kept deliberately. Disabling a job by relying on how a
 * quoted scalar is re-parsed as an expression is fragile enough that it should
 * be written as a guard, and demanding one is the safe direction: the cost is a
 * false positive carrying a remediation message, where the opposite error lets
 * a job run unaudited.
 *
 * Known limit, raised by glm-5.3: `if: False` and `if: FALSE` also parse to
 * boolean false and are indistinguishable from `false` after parsing, so they
 * are exempted too. No workflow uses them.
 */
export function isDisabled(value: unknown): boolean {
  return value === false
}

/** The `if` value as an expression string, or "" when there is none. */
export function conditionOf(value: unknown): string {
  return typeof value === "string" ? value : ""
}

export type Verdict = "guarded" | "unguarded" | "malformed"

/**
 * Whether `condition` can be true outside the guarded repository.
 *
 * A substring test is not enough, and the difference is exploitable rather
 * than theoretical. `&&` binds tighter than `||` in GitHub expressions, so all
 * of these contain the guard verbatim and all still run here:
 *
 *   github.repository == 'anomalyco/opencode' || github.repository == 'swxtchio/swx-opencode'
 *   github.repository == 'anomalyco/opencode' && github.event.action == 'opened' || true
 *   github.repository == 'anomalyco/opencode' && github.event.issue.title == '(' || true
 *
 * The third hides the `||` behind a paren inside a string literal, so the
 * scanner must know about literals rather than merely counting brackets.
 *
 * The rule: the guard must be the whole condition, or the leading term of a
 * top-level conjunction whose remainder has no `||` outside parentheses.
 * `GUARD && (a || b)` is fine - the disjunction is subordinate to the guard,
 * and deploy.yml already relies on that.
 *
 * A condition whose quotes or parentheses do not balance is reported as
 * `malformed` rather than guessed at. GitHub would fail to evaluate it, so it
 * is not an exposure, but scanning it cannot be trusted either: an
 * unterminated quote makes the scanner treat the rest of the expression as
 * string data and miss a real top-level `||`.
 */
export function guardVerdict(condition: string, guard: string): Verdict {
  // Block scalars arrive with newlines; GitHub treats them as one expression.
  const normalised = stripWrappingParens(stripExpressionSyntax(condition.replace(/\s+/g, " ").trim()))

  const scan = scanTopLevel(normalised)
  if (scan === "malformed") return "malformed"

  // The guard must be the leading term of a top-level conjunction. Parsed
  // rather than pattern-matched, because the obvious regex - optional parens
  // either side - accepts `(GUARD && x) || y`, where the guard is subordinate
  // to a disjunction and the job runs in any repository. Counting the brackets
  // is what distinguishes the two: in that expression the guard is followed by
  // `&&` with its opening bracket still unclosed.
  let rest = normalised
  let opened = 0
  while (rest.startsWith("(")) {
    opened++
    rest = rest.slice(1).trim()
  }

  if (!rest.startsWith(guard)) return "unguarded"
  rest = rest.slice(guard.length).trim()

  let closed = 0
  while (rest.startsWith(")")) {
    closed++
    rest = rest.slice(1).trim()
  }
  // Unequal brackets mean the guard is not a self-contained leading term.
  if (closed !== opened) return "unguarded"

  // Tolerate any spacing around the conjunction: GitHub does not care, and a
  // check that rejects valid conditions gets worked around rather than fixed.
  if (rest === "") return "guarded"
  if (!rest.startsWith("&&")) return "unguarded"

  return scanTopLevel(rest.slice(2)) === "has-or" ? "unguarded" : "guarded"
}

/**
 * Convenience wrapper used by the tests. The audit itself calls guardVerdict,
 * so it can report WHICH problem a condition has rather than just that it
 * failed.
 */
export function isGuarded(condition: string, guard: string): boolean {
  return guardVerdict(condition, guard) === "guarded"
}

/**
 * A job condition may be written either bare or wrapped in `${{ }}`; GitHub
 * treats them identically. Without this, `${{ github.repository == ... }}` is
 * reported as unguarded, with a reason that misdescribes a correct condition.
 */
function stripExpressionSyntax(expression: string): string {
  const wrapped = /^\$\{\{(.*)\}\}$/.exec(expression)
  return wrapped ? wrapped[1].trim() : expression
}

/**
 * Remove parentheses that wrap the entire expression, so `(GUARD && x)` reads
 * as `GUARD && x`. Only a pair whose opening bracket matches the very last
 * character is removed - in `(GUARD && x) || y` the first bracket closes early,
 * so nothing is stripped and the top-level `||` stays visible.
 */
function stripWrappingParens(expression: string): string {
  let current = expression
  while (current.startsWith("(") && current.endsWith(")")) {
    let depth = 0
    let closesAtEnd = false
    let inString = false
    for (let i = 0; i < current.length; i++) {
      const char = current[i]
      if (char === "'") {
        inString = !inString
        continue
      }
      if (inString) continue
      if (char === "(") depth++
      else if (char === ")") {
        depth--
        if (depth === 0) {
          closesAtEnd = i === current.length - 1
          break
        }
      }
    }
    if (!closesAtEnd) return current
    current = current.slice(1, -1).trim()
  }
  return current
}

function scanTopLevel(expression: string): "has-or" | "no-or" | "malformed" {
  let depth = 0
  let inString = false

  for (let i = 0; i < expression.length; i++) {
    const char = expression[i]

    // String literals are skipped, not counted through. GitHub's single-quoted
    // strings have no escape sequences and write a literal quote as '', so a
    // plain toggle is exact: the two quotes of '' toggle out and straight back
    // in.
    if (char === "'") {
      inString = !inString
      continue
    }
    if (inString) continue

    if (char === "(") depth++
    else if (char === ")") depth--
    else if (char === "|" && expression[i + 1] === "|" && depth === 0) return "has-or"

    if (depth < 0) return "malformed"
  }

  if (inString || depth !== 0) return "malformed"
  return "no-or"
}

/**
 * The audit is only worth anything if it actually runs. Its own workflow must
 * fire on every pull request and every push, unnarrowed by a path filter -
 * otherwise a workflow-only change, which is exactly what this audits, would
 * sail through untested.
 */
export function auditWorkflowViolations(file: string, on: unknown): Violation[] {
  const problem = (reason: string): Violation => ({ workflow: file, job: "-", reason, found: "" })

  // Prefer what GitHub reports over what this file says, and say so when they
  // disagree - a stale constant would otherwise be satisfied by an equally
  // stale branch filter, and neither would be covering the default branch.
  const reported = reportedDefaultBranch()
  const defaultBranch = reported ?? DEFAULT_BRANCH
  const found: Violation[] = []
  if (reported && reported !== DEFAULT_BRANCH)
    found.push(
      problem(
        `DEFAULT_BRANCH in this script is "${DEFAULT_BRANCH}" but GitHub reports ` +
          `"${reported}". Update the constant, then re-check which branches the audit covers`,
      ),
    )

  if (!on || typeof on !== "object") return [...found, problem("this audit's own workflow declares no triggers")]

  // `on: [push, pull_request]` is valid YAML shorthand. Treating it as a map
  // made `"push" in on` test array INDICES, so the shorthand was reported as
  // missing both triggers - fail-closed but a false positive, which is the
  // direction that gets a check worked around.
  const triggers: Record<string, unknown> = Array.isArray(on)
    ? Object.fromEntries(on.filter((event): event is string => typeof event === "string").map((event) => [event, null]))
    : (on as Record<string, unknown>)

  for (const event of ["push", "pull_request"]) {
    if (!(event in triggers)) {
      found.push(problem(`this audit's own workflow no longer runs on ${event}, so changes can bypass it`))
      continue
    }
    const config = triggers[event]
    if (!config || typeof config !== "object") continue
    const filters = config as Record<string, unknown>

    for (const filter of ["paths", "paths-ignore"]) {
      if (filter in filters)
        found.push(
          problem(
            `this audit's own workflow has a \`${filter}\` filter on \`${event}\`, ` +
              `so a workflow-only change may not be audited`,
          ),
        )
    }

    // A branch filter that excludes the default branch is the same hole as a
    // path filter, and the envelope digest only reports that something
    // changed - which is precisely where a change gets rubber-stamped.
    // GitHub's schema accepts a scalar here, and `Array.isArray` skipped it -
    // so `branches: dev` was a real filter that the structural check ignored,
    // routing branch changes back onto the envelope digest, which is the
    // rubber-stamp this check exists to bypass. glm-5.3's finding.
    // A push trigger restricted to tags does not fire for branch pushes at
    // all, so the absence of a `branches` key is not the same as "all
    // branches" once `tags` is present.
    if (event === "push" && !("branches" in filters) && ("tags" in filters || "tags-ignore" in filters))
      found.push(
        problem("this audit's own workflow restricts `push` to tags, so pushes to the default branch are not audited"),
      )

    // Narrowing `types` stops the trigger firing for the activity that
    // matters: a pull request being opened or updated.
    const types = asList(filters["types"])
    if (event === "pull_request" && types) {
      const required = ["opened", "synchronize"].filter((type) => !types.includes(type))
      if (required.length)
        found.push(
          problem(
            `this audit's own workflow narrows \`pull_request\` types and no longer fires on ` +
              `${required.join(" or ")}, so a change can reach a pull request unaudited`,
          ),
        )
    }

    const branches = asList(filters["branches"])
    if (branches && !branchesCover(branches, defaultBranch))
      found.push(
        problem(
          `this audit's own workflow no longer runs on \`${event}\` for ` +
            `\`${defaultBranch}\`, so changes to the default branch are not audited`,
        ),
      )
    if ("branches-ignore" in filters)
      found.push(problem(`this audit's own workflow has a \`branches-ignore\` filter on \`${event}\``))
  }

  return found
}

/**
 * Whether a GitHub branch filter list actually includes `branch`.
 *
 * Membership is not enough. GitHub evaluates these as ORDERED globs where the
 * last matching pattern wins, so `[swxtch, "!swxtch"]` contains the branch
 * textually while excluding it in practice - sol's finding. Patterns are
 * therefore walked in order and the final match decides.
 */
/** A GitHub filter value, which may be a scalar or a sequence. */
export function asList(value: unknown): unknown[] | undefined {
  if (Array.isArray(value)) return value
  if (typeof value === "string") return [value]
  return undefined
}

export function branchesCover(patterns: unknown[], branch: string): boolean {
  let covered = false

  for (const pattern of patterns) {
    if (typeof pattern !== "string") continue
    const negated = pattern.startsWith("!")
    const glob = negated ? pattern.slice(1) : pattern
    if (!matchesGlob(glob, branch)) continue
    covered = !negated
  }

  return covered
}

/** GitHub's filter-pattern subset: `*` stops at `/`, `**` does not. */
function matchesGlob(pattern: string, value: string): boolean {
  const source = pattern
    .split("**")
    .map((part) =>
      part
        .split("*")
        .map((literal) => literal.replace(/[.+?^${}()|[\]\\]/g, "\\$&"))
        .join("[^/]*"),
    )
    .join(".*")

  return new RegExp(`^${source}$`).test(value)
}

/**
 * The audit's own job must be ALLOWLISTED, not guarded.
 *
 * kimi-k3's E2, and the purest instance of this branch's recurring defect.
 * Add the repository guard to `test.yml::unit` and drop its ALLOWED row, and
 * this check reports "33 guarded" and exits 0 - while counting its OWN job as
 * guarded, meaning that job never runs in this fork, meaning the audit never
 * runs in this fork. Every other control stayed green: both step names were
 * still there, the envelope digest excludes jobs, and both pairing checks
 * passed.
 *
 * Worse, the failure message coaches the first half of the edit: it tells the
 * reader that each job needs the guard. So the audit's own remediation text
 * prescribed the change that retires it.
 */
export function auditJobModeViolations(file: string, job: unknown, allowlisted: boolean): Violation[] {
  const problem = (reason: string): Violation => ({ workflow: file, job: AUDIT_JOB, reason, found: "" })
  const found: Violation[] = []

  if (!allowlisted)
    found.push(
      problem(
        "the audit's own job is not in ALLOWED. It has to run in this fork, so it must be " +
          "allowlisted rather than guarded - do not add the repository guard to this one",
      ),
    )

  // Unconditional, not merely unguarded.
  //
  // This first checked whether the condition contained the guard - asserting
  // one spelling where the requirement is a behaviour, which is the ninth
  // instance of this branch's recurring defect and the second inside a fix for
  // the eighth. sol found it: `if: false` on this job, or any condition at all,
  // skips it, and GitHub reports a job skipped by its own `if` as SUCCESS even
  // when required. Every other control - the step attestation, the digests,
  // these tests - runs INSIDE this job, so a skip takes all of them with it and
  // the check reports nothing.
  if (job !== null && typeof job === "object" && "if" in job) {
    const condition = conditionOf((job as { if?: unknown }).if)
    found.push(
      problem(
        condition.includes(GUARD)
          ? "the audit's own job carries the repository guard, so it never runs in this fork " +
              "and neither does this check. Allowlist it instead"
          : "the audit's own job has a condition. It must be unconditional: a job skipped by its " +
              "own `if` reports success, and every check in this file runs inside it",
      ),
    )
  }

  return found
}

/**
 * Half of the mutual attestation described at AUDIT_STEPS. If an upstream
 * merge drops the step that runs this audit, nothing would run to notice - the
 * job digest that changes is computed only by the step that was removed. So
 * the audit asserts its own wiring while it still has the chance.
 */
export function auditStepViolations(file: string, job: unknown): Violation[] {
  const steps = (job as { steps?: { name?: unknown; run?: unknown; if?: unknown }[] } | undefined)?.steps
  if (!Array.isArray(steps))
    return [{ workflow: file, job: AUDIT_JOB, reason: "the audit's own job has no steps", found: "" }]

  const problem = (reason: string): Violation => ({ workflow: file, job: AUDIT_JOB, reason, found: "" })
  const found: Violation[] = []

  for (const expected of AUDIT_STEPS) {
    const step = steps.find((candidate) => candidate?.name === expected.name)

    if (!step) {
      found.push(problem(`the step "${expected.name}" is gone, so this check no longer runs in CI`))
      continue
    }

    // A condition on the step is the single-edit way to switch the audit off
    // while leaving its name in place for a name-only assertion to find.
    if ("if" in step)
      found.push(problem(`the step "${expected.name}" has a condition, so it can be skipped without being removed`))

    const run = typeof step.run === "string" ? step.run : ""
    if (!run.includes(expected.runs))
      found.push(problem(`the step "${expected.name}" no longer runs \`${expected.runs}\``))

    // glm-5.3's second bypass: the step runs, prints its violations to the log,
    // and CI stays green because the exit code was swallowed. The audit then
    // looks alive while controlling nothing - which is worse than its absence,
    // since a green check is taken as evidence.
    if (swallowsFailure(run))
      found.push(
        problem(
          `the step "${expected.name}" discards its exit code, so this check reports ` +
            `problems without failing the build`,
        ),
      )
    if ("continue-on-error" in step)
      found.push(problem(`the step "${expected.name}" sets continue-on-error, so its failure does not fail CI`))
  }

  if (typeof job === "object" && job !== null && "continue-on-error" in job)
    found.push(problem("the audit's own job sets continue-on-error, so nothing it finds can fail CI"))

  return found
}

/**
 * Whether a shell snippet discards a failing exit status.
 *
 * `cmd || true`, `cmd || :`, `cmd; true` and `set +e` all keep a step green
 * while its command fails. This is the reflex edit made to get a branch green,
 * so it is the likeliest way the audit ends up running but not gating.
 */
export function swallowsFailure(run: string): boolean {
  // `:` needs its own alternative: `\b` after a colon never matches, since a
  // word boundary requires a word character on one side and `:` is not one.
  if (/\|\|\s*(?:true\b|:(?:\s|$))/.test(run)) return true
  if (/;\s*(?:true|:)\s*$/m.test(run)) return true

  // `|| exit 0` and `; exit 0` discard failure exactly as `|| true` does, and
  // codex found that only the `true`/`:` spellings were matched. `exit 1` is
  // deliberately excluded: it PROPAGATES failure.
  if (/(?:\|\||;)\s*exit\s+0\b/.test(run)) return true

  // A bare `exit 0` line in a gating step's script is unconditional success,
  // whether it precedes the command or follows it.
  if (/(?:^|\n)\s*exit\s+0\s*(?:$|\n)/.test(run)) return true

  if (/\bset\s+\+e\b/.test(run)) return true
  if (/\bset\s+\+o\s+errexit\b/.test(run)) return true
  return false
}

/** The local actions a job's steps call, as bare names. */
function localActionsIn(job: unknown): string[] {
  const steps = (job as { steps?: { uses?: unknown }[] } | undefined)?.steps
  if (!Array.isArray(steps)) return []

  return steps
    .map((step) => step?.uses)
    .filter((uses): uses is string => typeof uses === "string")
    .map((uses) => /^\.\/\.github\/actions\/([^/]+)\/?$/.exec(uses)?.[1])
    .filter((name): name is string => Boolean(name))
}

/**
 * Local composite actions are referenced as `uses: ./.github/actions/<name>`
 * and their file lives at `<name>/action.yml`. Pinning them closes the gap
 * between "this job's YAML is unchanged" and "this job does the same thing".
 */
export function localActionViolations(workflowsDir: URL, used: Set<string>): Violation[] {
  const found: Violation[] = []

  // Discovered from the allowlisted jobs rather than listed by hand: a NEW
  // `uses: ./.github/actions/...` added to an allowlisted job would otherwise
  // be unpinned, which is the same gap one level down.
  for (const name of used)
    if (!ALLOWED_ACTIONS.has(name))
      found.push({
        workflow: `actions/${name}`,
        job: "-",
        reason:
          "an allowlisted job uses a local action that is not pinned. Review what it does, " +
          "then add it to ALLOWED_ACTIONS with the digest a pinned entry prints",
        found: "",
      })

  for (const [name, expected] of ALLOWED_ACTIONS) {
    if (!used.has(name)) {
      found.push({
        workflow: `actions/${name}`,
        job: "-",
        reason: "a local action is pinned but no allowlisted job uses it any more, so the pin guards nothing",
        found: "",
      })
      continue
    }
    const candidates = ["action.yml", "action.yaml"].map((file) => new URL(`../actions/${name}/${file}`, workflowsDir))
    const path = candidates.find((candidate) => existsSync(candidate))

    if (!path) {
      found.push({
        workflow: `actions/${name}`,
        job: "-",
        reason: "a pinned local action is missing, so an allowlisted job calls something that is not there",
        found: "",
      })
      continue
    }

    const actual = digest(Bun.YAML.parse(readFileSync(path, "utf8")))
    if (actual !== expected)
      found.push({
        workflow: `actions/${name}`,
        job: "-",
        reason:
          `a local action used by an allowlisted job changed. Review what it now does, ` +
          `then set its digest to ${actual}`,
        found: "",
      })
  }

  return found
}

async function main() {
  const dir = new URL("../.github/workflows/", import.meta.url)
  const files = [...new Bun.Glob("*.{yml,yaml}").scanSync({ cwd: Bun.fileURLToPath(dir) })].sort()
  if (files.length === 0) throw new Error(`no workflows found in ${Bun.fileURLToPath(dir)}`)

  const violations: Violation[] = []

  // GitHub only runs workflow files directly in .github/workflows, so the flat
  // glob above matches what actually executes; scanning subdirectories instead
  // would report inert files as violations. But a nested file must not be
  // merely invisible to the audit either, so its presence is itself reported.
  const nested = [...new Bun.Glob("*/**/*.{yml,yaml}").scanSync({ cwd: Bun.fileURLToPath(dir) })].sort()
  for (const file of nested)
    violations.push({
      workflow: file,
      job: "-",
      reason:
        "a workflow file in a subdirectory of .github/workflows is not audited by this check. " +
        "GitHub does not run nested workflow files today; move it to the top level or extend this audit",
      found: "",
    })
  let guarded = 0
  let disabledJobs = 0
  let allowed = 0
  const matched: string[] = []
  const usedLocalActions = new Set<string>()

  for (const file of files) {
    const parsed = Bun.YAML.parse(await Bun.file(new URL(file, dir)).text()) as {
      jobs?: Record<string, { if?: unknown }>
      on?: unknown
    }

    const expectedEnvelope = ALLOWED_ENVELOPES.get(file)
    if (expectedEnvelope !== undefined) {
      const { jobs: _jobs, ...envelope } = parsed ?? {}
      const actualEnvelope = digest(envelope)
      if (expectedEnvelope !== actualEnvelope)
        violations.push({
          workflow: file,
          job: "-",
          reason:
            `the triggers or permissions of an allowlisted workflow changed. ` +
            `Confirm it still runs on every change, then set its envelope digest to ${actualEnvelope}`,
          found: "",
        })
    }

    if (file === AUDIT_WORKFLOW) {
      violations.push(...auditWorkflowViolations(file, parsed?.on))
      violations.push(...auditStepViolations(file, parsed?.jobs?.[AUDIT_JOB]))
      violations.push(
        ...auditJobModeViolations(file, parsed?.jobs?.[AUDIT_JOB], ALLOWED.has(`${AUDIT_WORKFLOW}::${AUDIT_JOB}`)),
      )
    }

    const jobs = parsed?.jobs
    if (!jobs || typeof jobs !== "object") {
      violations.push({ workflow: file, job: "-", reason: "workflow declares no jobs", found: "" })
      continue
    }

    for (const [job, body] of Object.entries(jobs)) {
      const disabled = isDisabled(body?.if)
      const condition = conditionOf(body?.if)

      const key = `${file}::${job}`
      if (ALLOWED.has(key)) {
        allowed++
        matched.push(key)
        for (const name of localActionsIn(body)) usedLocalActions.add(name)
        const actual = digest(body)
        if (ALLOWED.get(key) !== actual)
          violations.push({
            workflow: file,
            job,
            reason:
              `allowlisted job changed. Review what it now does - an allowed job is ` +
              `unguarded - then set its digest to ${actual}`,
            found: "",
          })
        continue
      }
      if (disabled) {
        disabledJobs++
        continue
      }
      const verdict = guardVerdict(condition, GUARD)
      if (verdict === "guarded") {
        guarded++
        continue
      }
      violations.push({
        workflow: file,
        job,
        reason:
          verdict === "malformed"
            ? "condition has unbalanced quotes or parentheses, so it cannot be checked"
            : condition
              ? "the repository guard is not the leading term of a top-level conjunction"
              : "no condition at all",
        found: condition,
      })
    }
  }

  console.log(
    `checked ${files.length} workflows: ${guarded} guarded, ${disabledJobs} disabled, ` +
      `${allowed}/${ALLOWED.size} allowed (${[...ALLOWED.keys()].join(", ")})`,
  )

  // An envelope entry naming a workflow that no longer exists, or one with no
  // allowlisted job, protects nothing. The pairing matters in both directions:
  // an allowlisted job needs its workflow's envelope pinned, and a pinned
  // envelope without an allowlisted job is a leftover that hides that fact.
  for (const workflow of ALLOWED_ENVELOPES.keys()) {
    if (!files.includes(workflow)) {
      violations.push({
        workflow,
        job: "-",
        reason: "an envelope digest is pinned for a workflow that no longer exists",
        found: "",
      })
      continue
    }
    if (![...ALLOWED.keys()].some((key) => key.startsWith(`${workflow}::`)))
      violations.push({
        workflow,
        job: "-",
        reason: "an envelope digest is pinned for a workflow with no allowlisted job, so it guards nothing",
        found: "",
      })
  }
  for (const key of ALLOWED.keys()) {
    const [workflow, job] = key.split("::")
    if (!ALLOWED_ENVELOPES.has(workflow))
      violations.push({
        workflow,
        job: job ?? "-",
        reason:
          "this job is allowlisted but its workflow's envelope is not pinned, so its triggers " +
          "and permissions could change unnoticed",
        found: "",
      })
  }

  // An allowlist entry that matches nothing is stale - the job was renamed or
  // removed - and a stale entry silently stops protecting whatever replaced it.
  if (allowed !== ALLOWED.size) {
    const seen = new Set(matched)
    const stale = [...ALLOWED.keys()].filter((entry) => !seen.has(entry))
    violations.push({
      workflow: "-",
      job: "-",
      reason: `allowlist entries match no job, so they no longer protect anything: ${stale.join(", ")}`,
      found: "",
    })
  }

  violations.push(...localActionViolations(dir, usedLocalActions))

  if (violations.length === 0) return

  console.error(`\n${violations.length} job(s) can run from this fork:\n`)
  for (const v of violations) {
    console.error(`  ${v.workflow} :: ${v.job}`)
    console.error(`      ${v.reason}`)
    if (v.found) console.error(`      if: ${v.found}`)
  }
  console.error(`\nEach job needs  if: ${GUARD}  on its own, or as`)
  console.error(`  if: ${GUARD} && (<the existing condition>)`)
  console.error(`The one exception is ${AUDIT_WORKFLOW}::${AUDIT_JOB}, which runs this check: it must stay`)
  console.error(`in ALLOWED and must NOT be guarded, or the check stops running in this fork.`)
  console.error(`Otherwise add the job to ALLOWED in this script, keyed <workflow>::<job>`)
  console.error(`with the digest the failure above prints, if this fork genuinely needs it to run.`)
  process.exit(1)
}

// Only audit when run as a command. Without this the module cannot be imported
// for testing: the audit would execute on import, and its process.exit(1) on
// failure would kill the test run rather than fail a test.
if (import.meta.main) await main()
