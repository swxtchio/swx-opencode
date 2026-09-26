import { describe, expect, test } from "bun:test"
import {
  AUDIT_STEPS,
  asList,
  auditJobModeViolations,
  auditStepViolations,
  auditWorkflowViolations,
  conditionOf,
  isDisabled,
  swallowsFailure,
} from "./check-workflow-guards"

/**
 * GOAL: pin the checks that were previously exercised only against the live
 * tree, which passes vacuously.
 *
 * glm-5.3's sharpest finding: reverting the round-2 fail-open fix - treating a
 * quoted `if: 'false'` as disabled - kept all 67 tests green, because the
 * classification lived inline in main(). A check with no test that can fail is
 * the same defect as a guard that overstates coverage, one level up.
 */

describe("isDisabled", () => {
  // GOAL: only the YAML boolean exempts a job from needing a guard.
  test("accepts the YAML boolean false", () => {
    expect(isDisabled(false)).toBe(true)
  })

  // GOAL: the exact revert glm described must fail this. A string-tolerant
  // check would return true for "false" and reintroduce the round-2 bug.
  test.each([["false"], ["False"], ["'false'"], [0], [null], [""], [true], [undefined]])(
    "does not accept %p as disabling a job",
    (value) => {
      expect(isDisabled(value)).toBe(false)
    },
  )
})

describe("conditionOf", () => {
  test("returns a string condition unchanged", () => {
    expect(conditionOf("github.repository == 'x/y'")).toBe("github.repository == 'x/y'")
  })

  // GOAL: a boolean or absent `if` has no expression text, and must not be
  // stringified into one - that conflation was the round-2 bug.
  test.each([[false], [true], [undefined], [null], [0]])("returns empty for %p", (value) => {
    expect(conditionOf(value)).toBe("")
  })
})

describe("asList", () => {
  // GOAL: GitHub accepts a scalar for a filter, and an Array.isArray gate
  // skipped it entirely - so `branches: dev` was a real filter the structural
  // check ignored.
  test.each([
    [
      ["a", "b"],
      ["a", "b"],
    ],
    ["dev", ["dev"]],
  ] as [unknown, unknown[]][])("normalises %p", (value, expected) => {
    expect(asList(value)).toEqual(expected)
  })

  test.each([[undefined], [null], [{}], [7]])("returns undefined for %p, which is not a filter", (value) => {
    expect(asList(value)).toBeUndefined()
  })
})

describe("swallowsFailure", () => {
  // GOAL: the audit running but not gating is worse than the audit missing,
  // because a green check is read as evidence. These are the shapes that do it.
  test.each([
    "bun run script/check-workflow-guards.ts || true",
    "bun run script/check-workflow-guards.ts || :",
    "bun run script/check-workflow-guards.ts; true",
    "set +e\nbun run script/check-workflow-guards.ts",
  ])("detects %p", (run) => {
    expect(swallowsFailure(run)).toBe(true)
  })

  // GOAL: the spellings codex found unmatched. `|| exit 0` discards failure
  // exactly as `|| true` does, and a bare `exit 0` line makes a gating step
  // unconditionally successful.
  test.each([
    "bun run script/check-workflow-guards.ts || exit 0",
    "bun run script/check-workflow-guards.ts; exit 0",
    "exit 0\nbun run script/check-workflow-guards.ts",
    "bun run script/check-workflow-guards.ts\nexit 0",
    "set +o errexit\nbun run script/check-workflow-guards.ts",
  ])("detects %p", (run) => {
    expect(swallowsFailure(run)).toBe(true)
  })

  test.each([
    "bun run script/check-workflow-guards.ts",
    "ls a b > /dev/null\nbun test a b",
    // `|| exit 1` propagates failure rather than discarding it.
    "bun run script/check-workflow-guards.ts || exit 1",
    // `exit 1` as a line is a failure path, not a swallow.
    "bun test a || exit 1\nexit 1",
  ])("does not fire on %p", (run) => {
    expect(swallowsFailure(run)).toBe(false)
  })
})

describe("auditStepViolations", () => {
  // Built from the pinned definitions, not retyped: the run text is now
  // compared for exact equality, so a hand-written fixture would drift from
  // the thing under test and fail for the wrong reason.
  const good = { steps: AUDIT_STEPS.map((step) => ({ name: step.name, run: step.run })) }

  test("passes a correctly wired job", () => {
    expect(auditStepViolations("test.yml", good)).toEqual([])
  })

  // GOAL: each single-edit bypass the reviewers found must produce a finding.
  test.each([
    ["a missing step", { steps: [good.steps[0]] }],
    ["a step made conditional", { steps: [{ ...good.steps[0], if: false }, good.steps[1]] }],
    ["a step whose command was replaced", { steps: [{ ...good.steps[0], run: "echo skipped" }, good.steps[1]] }],
    // glm-5.3's shapes: substring presence passed all three of these.
    [
      "a step that only cats the script",
      { steps: [{ ...good.steps[0], run: "cat script/check-workflow-guards.ts" }, good.steps[1]] },
    ],
    [
      "a step that pipes failure into echo",
      { steps: [{ ...good.steps[0], run: `${AUDIT_STEPS[0]!.run} || echo "findings above"` }, good.steps[1]] },
    ],
    [
      "a gate file dropped from the invocation but left in the ls line",
      {
        steps: [
          good.steps[0],
          {
            ...good.steps[1],
            run: good.steps[1]!.run.replace(" audit-cli.test.ts", "").replace("ls ", "ls audit-cli.test.ts "),
          },
        ],
      },
    ],
    [
      "a step that discards its exit code",
      { steps: [{ ...good.steps[0], run: "bun run script/check-workflow-guards.ts || true" }, good.steps[1]] },
    ],
    ["a step with continue-on-error", { steps: [{ ...good.steps[0], "continue-on-error": true }, good.steps[1]] }],
    ["a job with continue-on-error", { ...good, "continue-on-error": true }],
    ["a job with no steps", {}],
  ] as [string, unknown][])("reports %s", (_name, job) => {
    expect(auditStepViolations("test.yml", job).length).toBeGreaterThan(0)
  })
})

describe("auditWorkflowViolations", () => {
  const on = { push: { branches: ["swxtch"] }, pull_request: null }

  test("passes triggers that cover every change to the default branch", () => {
    expect(auditWorkflowViolations("test.yml", on)).toEqual([])
  })

  test.each([
    ["no triggers at all", undefined],
    ["push removed", { pull_request: null }],
    ["pull_request removed", { push: { branches: ["swxtch"] } }],
    ["a paths filter", { ...on, pull_request: { paths: ["packages/**"] } }],
    ["a paths-ignore filter", { ...on, pull_request: { "paths-ignore": ["docs/**"] } }],
    ["a branches-ignore filter", { ...on, pull_request: { "branches-ignore": ["x"] } }],
    ["branches missing the default branch", { ...on, push: { branches: ["other"] } }],
    // The scalar form, which an Array.isArray gate skipped entirely.
    ["a scalar branches filter naming another branch", { ...on, push: { branches: "dev" } }],
    // Ordered globs: the later negation wins.
    ["a later negation of the default branch", { ...on, push: { branches: ["swxtch", "!swxtch"] } }],
  ] as [string, unknown][])("reports %s", (_name, triggers) => {
    expect(auditWorkflowViolations("test.yml", triggers).length).toBeGreaterThan(0)
  })

  // GOAL: the scalar form that DOES cover the default branch must pass, or the
  // normalisation would just be a stricter false positive.
  test("accepts a scalar branches filter naming the default branch", () => {
    expect(auditWorkflowViolations("test.yml", { ...on, push: { branches: "swxtch" } })).toEqual([])
  })
})

describe("auditJobModeViolations", () => {
  // GOAL: the audit's own job must run in this fork, which means allowlisted
  // and NOT guarded. kimi-k3's E2 was the purest instance of this branch's
  // recurring defect: guard the audit's own job and drop its allowlist row,
  // and the check reports "33 guarded" and exits 0 while never running here.
  test("passes an allowlisted, unguarded audit job", () => {
    expect(auditJobModeViolations("test.yml", { steps: [] }, true)).toEqual([])
  })

  test("reports an audit job that is not allowlisted", () => {
    expect(auditJobModeViolations("test.yml", { steps: [] }, false).length).toBe(1)
  })

  // GOAL: the exact E2 edit - both halves at once - must be reported, and by
  // both clauses, since either half alone is also wrong.
  test("reports an audit job that carries the repository guard", () => {
    const job = { if: "github.repository == 'anomalyco/opencode'", steps: [] }
    expect(auditJobModeViolations("test.yml", job, true).length).toBe(1)
    expect(auditJobModeViolations("test.yml", job, false).length).toBe(2)
  })

  // GOAL: the requirement is that this job is UNCONDITIONAL, not that its
  // condition avoids one spelling. sol found that the first version asserted
  // the spelling: `if: false`, an owner check, or an event check all skip the
  // job, GitHub reports a job skipped by its own `if` as SUCCESS even when
  // required, and every control in this file runs inside that job - so a skip
  // takes the step attestation, the digests and these tests with it.
  test.each([
    ["if: false", false],
    ["an owner check", "github.repository_owner == 'anomalyco'"],
    ["an event check", "github.event_name == 'schedule'"],
    ["if: true, which is still a condition", true],
    ["an empty condition", ""],
  ] as [string, unknown][])("reports %s on the audit's own job", (_name, condition) => {
    expect(auditJobModeViolations("test.yml", { if: condition, steps: [] }, true).length).toBeGreaterThan(0)
  })
})

describe("auditWorkflowViolations trigger shapes", () => {
  const on = { push: { branches: ["swxtch"] }, pull_request: null }

  // GOAL: `on: [push, pull_request]` is valid YAML shorthand. Treating it as a
  // map made `"push" in on` test array INDICES, so the shorthand was reported
  // as missing both triggers - a false positive, the direction that gets a
  // check worked around rather than fixed.
  test("accepts the array shorthand for triggers", () => {
    expect(auditWorkflowViolations("test.yml", ["push", "pull_request"])).toEqual([])
  })

  // GOAL: a push trigger restricted to tags never fires for branch pushes, so
  // the absence of a `branches` key is not "all branches" once tags appear.
  test("reports a push trigger restricted to tags", () => {
    expect(auditWorkflowViolations("test.yml", { ...on, push: { tags: ["**"] } }).length).toBeGreaterThan(0)
  })

  // GOAL: narrowing `types` stops the trigger firing for the activity that
  // matters - a pull request being opened or updated.
  test.each([[["labeled"]], [["opened"]], [["synchronize"]]])("reports pull_request types %p", (types) => {
    expect(auditWorkflowViolations("test.yml", { ...on, pull_request: { types } }).length).toBeGreaterThan(0)
  })

  test("accepts pull_request types that still cover opened and synchronize", () => {
    expect(auditWorkflowViolations("test.yml", { ...on, pull_request: { types: ["opened", "synchronize"] } })).toEqual(
      [],
    )
  })
})

describe("auditJobModeViolations skip vectors", () => {
  // GOAL: the audit's job must produce a RUNNING instance in this fork, which
  // is a behaviour - not merely "carries no `if` key", which is a structure.
  // glm-5.3's #1: a one-line guarded `gate` job plus `needs: gate` skips this
  // job, and GitHub reports a job whose dependency was skipped as SUCCESS, so
  // the whole audit retires without its `if` ever being touched.
  test("reports a `needs` dependency", () => {
    expect(auditJobModeViolations("test.yml", { needs: ["gate"], steps: [] }, true).length).toBe(1)
  })

  // GOAL: a matrix expanding to nothing skips the job too, with no `if` and no
  // `needs`.
  test.each([
    ["an empty dimension", { settings: [] }],
    ["a dynamically computed matrix", "${{ fromJSON(needs.x.outputs.m) }}"],
  ] as [string, unknown][])("reports %s", (_name, matrix) => {
    expect(auditJobModeViolations("test.yml", { strategy: { matrix }, steps: [] }, true).length).toBe(1)
  })

  // GOAL: and the real matrix, which has one static entry, must still pass -
  // otherwise this is just a stricter false positive on the live tree.
  test("accepts a static non-empty matrix", () => {
    const job = { strategy: { "fail-fast": false, matrix: { settings: [{ name: "linux" }] } }, steps: [] }
    expect(auditJobModeViolations("test.yml", job, true)).toEqual([])
  })
})
