# Fast-suite observer hardening

## Plan

Scope: [swxtchio/swx-opencode#324](https://github.com/swxtchio/swx-opencode/issues/324), including its user-facing documentation advisory. The full suite remains the release gate; fast mode is a merge-gate speedup, not a security boundary.

- Drive absolute Bun, `bun run --bun`, and `node --test` launches through executable fixture packages and the real `tests/run.sh` boundary.
- Close each silent-success escape by observing extra tests or refusing unsupported launches, preserving the existing Turbo and direct-package positive controls.
- Pin non-build prerequisite admission and a producer-derived non-empty wrong-owner Turbo plan with pre-execution markers.
- Demonstrate each behavioral pin with a behavior-removing mutation, its failed assertion, and restored-green focused verification.
- Qualify the help, manifest header, and existing changeset claim with the observation scope and issue reference; add this PR's fork changeset entry.
- Run focused and fork-maintenance checks, then the applicable lint and commit-identity gates. Publish the single draft PR and hand it to firstmate for review.

## Execution evidence (2026-10-21)

Sampled implementation: `6bbf1877f2a91814505b442a812714ba2dfc6d64`, reachable through [PR #329](https://github.com/swxtchio/swx-opencode/pull/329). These are executable fixture measurements through `script/fast-suite-runner.test.ts`, using real Bun and Turbo, not a live production run. Host: Linux ARM64; Bun 1.4.2, Turbo 2.10.2, Node v22.20.0, strace 6.8. The worktree used a copy of the installed Turbo binary; fixtures independently commit their package configuration and runner before execution.

All focused commands ran from `script` as `bun test fast-suite-runner.test.ts`, with `TMPDIR="$PWD/.local/tmp"` and `TURBO_TELEMETRY_DISABLED=1` to keep scratch state local. Before each red run, only the named production guard was replaced; the fixture copied and committed that mutated runner, so the dirty-checkout refusal could not decide the result. Each guard was restored exactly, verified with `git diff --exit-code -- ../tests/run.sh`, and followed by the same focused command.

### Baseline and restored results

- Baseline: exit 0; `1 pass`, `0 fail`, `263 expect() calls`.
- Every restoration below: exit 0; `1 pass`, `0 fail`, `263 expect() calls`.
- Fork-maintenance command from `script`: `bun test sync-upstream.test.ts changeset-check.test.ts fast-suite-runner.test.ts`; exit 0, `96 pass`, `0 fail`, `544 expect() calls`.
- `bash tests/run.sh --help` from the root: exit 0 and `verdict=completed mode=help exit_code=0`. The rendered equality claim names PATH-resolved Bun test launches and swxtchio/swx-opencode#324, and discloses the Linux strace dependency and release-gate distinction. The manifest and changeset qualifications were reviewed against the runtime outcomes below.
- `bash -n tests/run.sh` and `git diff --check`: exit 0.

### Behavior-removing mutations

| Pin | Exact mutation in `tests/run.sh` | Actual red result | Restored focused result |
| --- | --- | --- | --- |
| Absolute Bun | Replace `if (!captured || !args.includes("--reporter=junit"))` with `if (false)` | Exit 1, failed `expect(launchEscape.exitCode).toBe(1)`: expected 1, received 0. The unlisted Bun test wrote `unlisted`; the runner printed `verdict=completed mode=fast target_count=4 exit_code=0`. | Exit 0, 1 pass / 0 fail |
| Bun delegation | Replace `if [ "$argument" = --bun ] || [ "$argument" = -b ]; then` with `if false; then` | Exit 1, failed `expect(readFileSync(marker, "utf8")).not.toContain("unlisted")`: the received marker included `unlisted` alongside the selected files. This pins the refusal before delegated execution, independently of the later process audit. | Exit 0, 1 pass / 0 fail |
| Node test runner | Replace `if (basename(executable ?? "") === "node" && args.some((argument) => argument === "--test" || argument.startsWith("--test=")))` with `if (false)` | Exit 1, failed `expect(launchEscape.exitCode).toBe(1)`: expected 1, received 0. Node emitted passing TAP output, wrote `unlisted-node`, and the runner printed `verdict=completed mode=fast target_count=4 exit_code=0`. | Exit 0, 1 pass / 0 fail |
| Non-build prerequisite | Replace only `(!commandless && taskName !== "build")` with `false` | Exit 1, failed `expect(readFileSync(buildLog, "utf8")).toBe("")`: received `non-build-prerequisite\n`. The command-bearing `@fixture/dependency#prepare` ran `bun ./probe.ts`; neither adjacent regex arm decided this case. | Exit 0, 1 pass / 0 fail |
| Non-empty wrong-owner plan | Replace only `if (!targetTask && plan.tasks.length > 0)` with `if (false)` | Exit 1, failed `expect(readFileSync(marker, "utf8")).toBe("")`: received `direct-configured\n`. The valid direct package executed despite a non-empty plan lacking its owning task. | Exit 0, 1 pass / 0 fail |

The wrong-owner wrapper obtains its non-empty plan by invoking the real Turbo producer for the example package, then returns those bytes for the selected direct package's dry-run request. The selected direct package has no local Turbo configuration or root-declared test task and already passes the direct-execution positive control; neither neighboring refusal supplies this pin. The non-build fixture similarly inspects Turbo's produced prerequisite command and asserts untouched execution markers before its refusal diagnostics.

The absolute-Bun fixture launches `[process.execPath, "test", "./test/unlisted.test.ts"]` from pretest. The delegation fixture launches `["bun", "run", "--bun", "verify:unlisted"]` from pretest and intentionally ignores the child status: selected tests still execute, while the persistent refusal invalidates fast mode. The Node fixture launches `["node", "--test", "./x.mjs"]` from posttest and confirms both the selected Bun work and the extra Node test actually ran before the launch audit refuses success. Existing positive controls for Turbo/direct execution, preparation, script options, lifecycle failures, and full-mode forwarding remain green.

### Observation scope

JUnit equality applies to PATH-resolved Bun test launches. The additional Linux process audit invalidates unobserved absolute launches of the resolved Bun runtime and executable paths named `node` carrying `--test`; Bun `--bun` delegation is refused by the shim and leaves a refusal receipt even when the caller ignores its status. Renamed/other interpreters, in-process frameworks, and deliberate observer/report tampering remain outside the stated scope. See `tests/run.sh --help` and its residual header. This work does not turn fast mode into a security boundary or replace the full release suite.

### Published CI follow-up (2026-10-21)

At published head `02f7ec5ecad82ba486a11e0e0a2ed7ff80b929c8`, [typecheck run 39543765037](https://github.com/swxtchio/swx-opencode/actions/runs/39543765037) failed during the workspace typecheck: its log reports SIGTERM, task exits 137, and `The operation was canceled`, with no TypeScript diagnostic. The subsequent fork-script typecheck step was not reached. The termination's cause is not established; [swxtchio/swx-opencode#330](https://github.com/swxtchio/swx-opencode/issues/330) records the diagnostic follow-up for the captain's CI decision.

The repository's `bun run typecheck:script` was then run locally on the same fork-script implementation with the installed pinned compiler and types, and exited 0. This supplies a fork-script typecheck result, not a passing workspace CI verdict. The local type-resolution dependencies were linked read-only from the installed project checkout; no runtime implementation or regression changed after the mutation proofs above.

## Absolute-path follow-up plan

- Exercise a real Bun executable copied outside the fixture repository to a different absolute path named `bun`, while keeping the fixture checkout committed and otherwise valid.
- If it silently runs an unlisted test, extend the launch audit to refuse that same-name absolute executable without mistaking the generated observer or its configured launcher for an unobserved test process.
- Pin the alternate-path classification with a behavior-removing mutation that leaves the configured-runtime guard enabled, then restore and run the focused and fork-maintenance checks.
- Keep the same draft PR, update its proof and observation scope, and hand the new head to firstmate.
