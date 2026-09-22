import { describe, expect, test } from "bun:test"
import { isGuarded } from "./check-workflow-guards"

const GUARD = "github.repository == 'anomalyco/opencode'"

describe("isGuarded", () => {
  // GOAL: the shapes actually present in this repository are accepted, so the
  // check does not force a rewrite of conditions that are already correct.
  test.each([
    GUARD,
    `${GUARD} && (github.event.action == 'opened')`,
    `${GUARD} && (always() && !failure() && !cancelled())`,
    `${GUARD} && (github.ref_name == 'dev' || github.ref_name == 'production')`,
    // Block scalars arrive with newlines and indentation.
    `${GUARD} &&\n      github.event.issue.pull_request &&\n      startsWith(github.event.comment.body, '/review')`,
  ])("accepts %s", (condition) => {
    expect(isGuarded(condition, GUARD)).toBe(true)
  })

  // GOAL: close the bypass codex found in the original substring check. Each of
  // these CONTAINS the guard verbatim and still runs on this fork, so a
  // substring test reports them as guarded. `&&` binds tighter than `||` in
  // GitHub expressions, which is what makes the second and third dangerous.
  test.each([
    `${GUARD} || github.repository == 'swxtchio/swx-opencode'`,
    `${GUARD} && github.event.action == 'opened' || true`,
    `${GUARD} && (a) || github.repository == 'swxtchio/swx-opencode'`,
    `github.repository == 'swxtchio/swx-opencode' || ${GUARD}`,
  ])("rejects %s, which contains the guard but still runs here", (condition) => {
    expect(isGuarded(condition, GUARD)).toBe(false)
  })

  // GOAL: a disjunction nested inside parentheses is subordinate to the guard
  // and must stay allowed - otherwise the check rejects legitimate conditions
  // and gets worked around rather than fixed.
  test("accepts a disjunction nested under the guard at any depth", () => {
    expect(isGuarded(`${GUARD} && ((a || b) && (c || d))`, GUARD)).toBe(true)
  })

  // GOAL: the guard must be the leading term. A trailing guard is a
  // conjunction too, but accepting it would mean parsing precedence for the
  // terms before it; requiring leading position keeps the rule checkable.
  test.each([
    "",
    "always()",
    `github.event.action == 'opened' && ${GUARD}`,
    "github.repository == 'anomalyco/opencode-fork'",
  ])("rejects %s", (condition) => {
    expect(isGuarded(condition, GUARD)).toBe(false)
  })
})
