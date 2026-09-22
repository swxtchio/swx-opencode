/**
 * Environment-scaled deadlines for the e2e suite.
 *
 * Mirrors packages/opencode/test/lib/deadline.ts, for the same reason: #10
 * catalogued a suite whose red job changed every run, and the failures share
 * one shape - a fixed wall-clock bound tuned on a fast, idle machine, tripping
 * on a slower or busier one.
 *
 * Measured during that work: repeating one spec ten times took 4.2 minutes on
 * an idle box and 9.6 minutes on a loaded one, and the loaded run failed on a
 * 30-second app-readiness wait that has nothing to do with what the spec
 * asserts.
 */

/**
 * Multiplier applied to e2e deadlines.
 *
 * Set OPENCODE_TEST_TIMEOUT_SCALE to give a loaded machine more room, or to
 * reproduce a CI timing profile locally. Keyed on CI rather than
 * GITHUB_ACTIONS: the unit job sets GITHUB_ACTIONS=false explicitly, so that
 * variable is not a usable signal for "am I on a runner", and using one knob
 * across both suites keeps them adjustable together.
 */
export const TIMEOUT_SCALE = (() => {
  const override = Number(process.env["OPENCODE_TEST_TIMEOUT_SCALE"])
  if (Number.isFinite(override) && override > 0) return override
  return process.env["CI"] ? 3 : 1
})()

/** `baseMs` scaled for this environment. See TIMEOUT_SCALE. */
export function deadline(baseMs: number): number {
  return Math.round(baseMs * TIMEOUT_SCALE)
}
