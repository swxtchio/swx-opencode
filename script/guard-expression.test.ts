import { describe, expect, test } from "bun:test"
import { digest, isGuarded } from "./check-workflow-guards"

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

describe("digest", () => {
  // GOAL: the digest tracks what a job DOES, not how the YAML happens to be
  // ordered. Without this, a cosmetic reordering upstream would fail the check
  // and train whoever hits it to bump the digest without reading the diff -
  // which defeats the point of pinning it.
  test("is stable under key reordering", () => {
    const a = { "runs-on": "ubuntu-latest", steps: [{ run: "echo hi" }] }
    const b = { steps: [{ run: "echo hi" }], "runs-on": "ubuntu-latest" }
    expect(digest(a)).toBe(digest(b))
  })

  // GOAL: the mutation sol described - a publishing step added to an
  // allowlisted job - must change the digest.
  test("changes when a step is added", () => {
    const before = { "runs-on": "ubuntu-latest", steps: [{ run: "bun test" }] }
    const after = { "runs-on": "ubuntu-latest", steps: [{ run: "bun test" }, { run: "npm publish" }] }
    expect(digest(after)).not.toBe(digest(before))
  })

  // GOAL: array order is meaningful - steps run in sequence - so reordering
  // steps must not be treated as the same job.
  test("changes when steps are reordered", () => {
    const a = { steps: [{ run: "one" }, { run: "two" }] }
    const b = { steps: [{ run: "two" }, { run: "one" }] }
    expect(digest(a)).not.toBe(digest(b))
  })
})

describe("isGuarded string-literal handling", () => {
  // GOAL: close the bypass glm-5.3 found. A paren inside a string literal used
  // to raise the paren depth for the remainder of the expression, hiding a
  // genuine top-level `||`. GitHub parses this as `(guard && title == '(')
  // || true`, which is true in every repository.
  test.each([
    `github.repository == 'anomalyco/opencode' && github.event.issue.title == '(' || true`,
    `github.repository == 'anomalyco/opencode' && contains(github.event.head_commit.message, '(') || true`,
    // Closing paren in a literal: drives depth negative, so a later `||` at
    // depth -1 was also missed.
    `github.repository == 'anomalyco/opencode' && github.event.issue.title == ')' || true`,
  ])("rejects a condition hiding a top-level || behind a quoted paren", (condition) => {
    expect(isGuarded(condition, GUARD)).toBe(false)
  })

  // GOAL: the mirror-image false positive. A `||` INSIDE a literal is data,
  // not an operator, so the condition is genuinely guarded and must be
  // accepted - otherwise the check rejects valid conditions and gets worked
  // around rather than fixed.
  test.each([
    `github.repository == 'anomalyco/opencode' && contains(github.event.head_commit.message, 'a||b')`,
    `github.repository == 'anomalyco/opencode' && github.event.issue.title != '||'`,
  ])("accepts a || that is inside a string literal", (condition) => {
    expect(isGuarded(condition, GUARD)).toBe(true)
  })

  // GOAL: GitHub writes a literal quote as '' with no escapes, so the toggle
  // must survive it - '' toggles out and straight back in, leaving the scanner
  // correctly inside the string.
  test("handles a doubled quote inside a literal", () => {
    expect(isGuarded(`${GUARD} && github.event.issue.title == 'it''s (' || true`, GUARD)).toBe(false)
    expect(isGuarded(`${GUARD} && github.event.issue.title == 'it''s fine'`, GUARD)).toBe(true)
  })
})
