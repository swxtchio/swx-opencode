/**
 * Environment-scaled deadlines for tests whose timing depends on machine load
 * rather than on the code under test.
 *
 * #10 catalogued a suite where the red job changed every run. The failures had
 * one shape: a fixed wall-clock bound tuned on a fast, idle machine, tripping
 * on a slower or busier one. A bound is still needed - a hang has to fail
 * eventually - but it belongs at a value derived from the environment, and it
 * must never be the PRIMARY signal that something is wrong.
 *
 * Where a positive signal exists - an exit code, an observable state change -
 * assert that, and let time be only the backstop. See the #27371 test in
 * test/cli/run/run-process.test.ts for the pattern.
 */

/**
 * Multiplier applied to every scaled deadline.
 *
 * GitHub-hosted runners are slower than the Blacksmith images these numbers
 * were originally tuned against, and the unit suite runs subprocess tests
 * concurrently, so CPU contention rather than latency is the binding
 * constraint.
 *
 * Set OPENCODE_TEST_TIMEOUT_SCALE to reproduce a CI timing profile locally, to
 * give a loaded machine more room, or - with a value below 1 - to force a
 * deadline to fire on purpose, which is how the attribution path is tested.
 */
export const TIMEOUT_SCALE = (() => {
  const override = Number(process.env["OPENCODE_TEST_TIMEOUT_SCALE"])
  if (Number.isFinite(override) && override > 0) return override
  // Deliberately not GITHUB_ACTIONS: the unit job runs with
  // GITHUB_ACTIONS=false set explicitly (see .github/workflows/test.yml), so
  // it is not a usable signal for "am I on a CI runner".
  return process.env["CI"] ? 3 : 1
})()

/** `baseMs` scaled for this environment. See TIMEOUT_SCALE. */
export function deadline(baseMs: number): number {
  return Math.round(baseMs * TIMEOUT_SCALE)
}

/** Human-readable note for a failure message, empty when unscaled. */
export function scaleNote(): string {
  return TIMEOUT_SCALE === 1 ? "" : ` (deadlines scaled x${TIMEOUT_SCALE})`
}
