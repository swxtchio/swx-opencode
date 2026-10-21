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

JUnit equality applies to PATH-resolved Bun test launches. The additional Linux process audit resolves execution paths from process creation and working-directory events, follows kernel shebang interpreter dispatch, and decodes fd-only execveat targets before classifying Bun tests and Node `--test` launches. Bun `--bun` delegation is refused by the shim and leaves a refusal receipt even when the caller ignores its status; child-tool flags remain recipient-owned. Unrecognized interpreter identities, in-process frameworks, bare Node scripts using `node:test` without `--test` ([#337](https://github.com/swxtchio/swx-opencode/issues/337)), and deliberate observer/report tampering remain outside the stated scope. See `tests/run.sh --help` and its residual header. This work does not turn fast mode into a security boundary or replace the full release suite.

### Published CI follow-up (2026-10-21)

At published head `02f7ec5ecad82ba486a11e0e0a2ed7ff80b929c8`, [typecheck run 39543765037](https://github.com/swxtchio/swx-opencode/actions/runs/39543765037) failed during the workspace typecheck: its log reports SIGTERM, task exits 137, and `The operation was canceled`, with no TypeScript diagnostic. The subsequent fork-script typecheck step was not reached. The termination's cause is not established; [swxtchio/swx-opencode#330](https://github.com/swxtchio/swx-opencode/issues/330) records the diagnostic follow-up for the captain's CI decision.

The repository's `bun run typecheck:script` was then run locally on the same fork-script implementation with the installed pinned compiler and types, and exited 0. This supplied a fork-script typecheck result, not a passing workspace CI verdict. The local type-resolution dependencies were linked read-only from the installed project checkout; the implementation and regression at the subsequent published head `debfb42bdce8aae786b4f488222b41b7297619ae` were still those of the initial mutation-proof pin. That head later passed both [typecheck run 39546180525](https://github.com/swxtchio/swx-opencode/actions/runs/39546180525) and [test run 39546180467](https://github.com/swxtchio/swx-opencode/actions/runs/39546180467).

## Absolute-path follow-up plan

- Exercise a real Bun executable copied outside the fixture repository to a different absolute path named `bun`, while keeping the fixture checkout committed and otherwise valid.
- If it silently runs an unlisted test, extend the launch audit to refuse that same-name absolute executable without mistaking the generated observer or its configured launcher for an unobserved test process.
- Pin the alternate-path classification with a behavior-removing mutation that leaves the configured-runtime guard enabled, then restore and run the focused and fork-maintenance checks.
- Keep the same draft PR, update its proof and observation scope, and hand the new head to firstmate.

## Alternate-path execution evidence (2026-10-21)

Updated implementation pin: `4f8c8dc7ed38f8902792ac086069fe3e21aa0edb`, on the same PR branch and including `origin/swxtch` release-cut commit `ac4e395c8d` through merge `344ee72a87`. The host, real Bun/Turbo execution boundary, focused command, and local scratch environment are the same as in the initial evidence above.

The new fixture copies the running Bun executable into a sibling scratch directory named `alternate/bun`, outside the committed fixture repository, and launches its unlisted test from the real package pretest. Against the prior observer, this executed `unlisted` and printed `verdict=completed mode=fast target_count=4 exit_code=0`; the regression failed at `expect(launchEscape.exitCode).toBe(1)`, expected 1, received 0. This establishes an absolute-Bun escape without changing the executable's name or relying on an uncommitted checkout.

After the fix, only the `namedBun` predicate was replaced with `false`; the `configuredBun` runtime-path predicate remained enabled. The original `process.execPath` cell still passed, then the copied-executable cell failed at that same exit assertion with expected 1, received 0, after unlisted execution and a successful fast verdict. Restoring the predicate and verifying `git diff --exit-code -- ../tests/run.sh` was followed by a green focused run: exit 0, 1 pass, 0 fail, 267 assertions.

All original mutations were also rerun against this updated pin, one at a time, followed by exact restoration and the same focused command. Each restored run exited 0 with 1 pass, 0 fail, and 267 assertions:

| Pin | Behavior-removing mutation | Actual focused failure (exit 1) |
| --- | --- | --- |
| Configured absolute Bun | Bun captured-report conditional → `if (false)` | `expect(launchEscape.exitCode).toBe(1)`: expected 1, received 0; unlisted execution and successful fast verdict. |
| Alternate absolute Bun | `const namedBun = ...` → `const namedBun = false` | `expect(launchEscape.exitCode).toBe(1)`: expected 1, received 0 specifically for `absolute-bun-copy`; the configured-runtime cell remained guarded. |
| Bun delegation | `--bun` / `-b` wrapper conditional → `if false; then` | Marker assertion expected no `unlisted`; received `unlisted` alongside selected-file markers. |
| Node test runner | Node basename / `--test` conditional → `if (false)` | `expect(launchEscape.exitCode).toBe(1)`: expected 1, received 0; passing Node TAP and `unlisted-node` execution marker. |
| Non-build prerequisite | Only `(!commandless && taskName !== "build")` → `false` | Empty build-log assertion received `non-build-prerequisite\n`. |
| Wrong-owner plan | Only `(!targetTask && plan.tasks.length > 0)` → `false` | Empty execution-marker assertion received `direct-configured\n`. |

The refreshed fork-maintenance command `bun test sync-upstream.test.ts changeset-check.test.ts fast-suite-runner.test.ts`, run from `script`, exited 0 with 96 pass, 0 fail, and 548 assertions. `bun run typecheck:script`, focused correctness lint, `bash -n tests/run.sh`, and `git diff --check` exited 0. Rendered help was read again and retains its PATH-resolved Bun JUnit scope and #324 reference. The alternate-path audit leaves the generated observer and configured launcher on their existing report-capture path; the same runtime test still passes its Turbo/direct, lifecycle, preparation, options, and full-forwarding positive controls.

## Round-1 hardening plan

Scope: the captain's `fix-round-1-order.txt`, addressing the selected round-1 reviews at `6cb3b40b03`.

- Reproduce Bun/Node kernel-shebang launches and bare inspector-option launches through the existing committed real-runner fixtures.
- Record process creation and working-directory transitions alongside execs so script interpreter dispatch can be resolved from the actual execution context. Follow Linux's single optional shebang argument and recursive interpreter loading before classifying Bun/Node launches.
- Correct optional inspector arguments in both the shim and the launch audit, with bare and attached-value runtime coverage.
- Fold fd-only exec dispatch and child-tool flag forwarding if their producer-driven fixtures support a small change; explicitly disclose bare Node `node:test` execution and any remaining accepted limitations.
- Demonstrate each changed behavior with an actual guard-removal red run, restore green, run focused/fork-maintenance/type/lint checks, and push only the existing draft for firstmate review.

## Round-1 execution evidence (2026-10-21)

Implementation pin: `f1687c07aaab03cca10dd77ab76705ccf9c68277`, on PR #329. All measurements below drive the committed fixture repository through the real runner on the same Linux ARM64/Bun 1.4.2/Turbo 2.10.2 host. `FAST_SUITE_RUNNER_CASE=<case>` selects a launch cell while retaining the existing runtime positive/refusal controls; the command remains `bun test fast-suite-runner.test.ts` from `script`, with local `TMPDIR` and Turbo telemetry disabled as above.

Before implementation, configured/copied/PATH `--inspect` cells and both Bun/Node shebang cells each executed an unlisted marker and returned `verdict=completed mode=fast target_count=4 exit_code=0`. Every focused regression exited 1 at the launch exit assertion, expected 1, received 0. Separate CLI probes confirmed that bare `--inspect` and `--inspect=127.0.0.1:0` dispatch tests, while a separated `127.0.0.1:0` word is an entrypoint rather than the optional inspector value. Runtime pins use non-waiting `--inspect`; the wait/brk siblings share the corrected bare/attached argument treatment but their debugger-wait behavior is not claimed as tested.

Each counterfactual below modified only the named behavior in the owning production runner. The fixture copied and committed that runner before execution, so no dirty-checkout refusal decided a red. After each mutation group, `git diff --exit-code -- ../tests/run.sh` verified exact restoration and the full focused command passed with 1 pass, 0 fail, and 304 assertions.

| Runtime cell | Behavior removed | Actual focused red (exit 1) |
| --- | --- | --- |
| Bun shebang | Interpreter-resolution loop → `while (false)` | Launch exit assertion expected 1, received 0; `shebang-bun` executed and fast mode completed successfully. |
| Node shebang | Same interpreter-resolution removal | Launch exit assertion expected 1, received 0; native `shebang-node` executed and fast mode completed successfully. |
| Recursive shebang | Same interpreter-resolution removal | Launch exit assertion expected 1, received 0; the nested interpreter's `shebang-bun` test executed and fast mode completed successfully. |
| Relative-script working directory | Successful `chdir` state update → `void strings` | Launch exit assertion expected 1, received 0 for Bun shebang execution; process creation still remained traced. |
| Configured inspector | Restore inspector names to the audit's always-value-taking list | Launch exit assertion expected 1, received 0; `unlisted` executed and fast mode completed successfully. |
| Copied inspector | Same audit-parser removal | Launch exit assertion expected 1, received 0; copied Bun executed `unlisted` and fast mode completed successfully. |
| PATH inspector | Restore inspector names to the shim's always-value-taking list | JUnit mismatch assertion failed: expected `reason=execution-set-mismatch`, received `reason=unobservable-test-launch`. Unlisted execution occurred, but the shim failed to capture it; the separate audit still refused success. |
| fd-only Bun | Replace fd-path reconstruction with early `if (!executable) continue` | Launch exit assertion expected 1, received 0; Python's actual fd exec of Bun ran `unlisted` and fast mode completed successfully. |
| Child-tool flags | Restore an all-argv delegation scan and remove the bunx/x mode exclusion | Successful-fast assertion expected 0, received 1 at the real tool-forwarding cell; the over-scoped refusal blocked `-b` delivery. |

The full focused matrix includes bare and attached inspector positive controls across configured/copied/PATH launches, both shebang families, recursive dispatch, fd-only execution, and real `bunx tsc -b`, `bun x tsc -b`, and `bun run flag-probe -- -b` forwarding to executable fixture consumers. The refreshed fork-maintenance group exited 0 with 96 pass, 0 fail, and 585 assertions. Fork-script typecheck, focused correctness lint, Bash syntax, rendered help, and diff checks exited 0. Bare Node `node:test` execution without `--test` remains explicitly disclosed and tracked in #337; the two cheap implementation advisories were folded into this hardening batch.

## Round-2 target-resolution plan

Scope: the captain's `fix-round-2-order.txt`, at reviewed pin `60706b49f5`.

- Exercise real Bun/Node shebangs through symlinked working directories and `..` paths, including ordinary decoys at the incorrect lexical targets, plus executed scripts removed before the audit.
- Preserve kernel pathname semantics by concatenating raw paths before filesystem canonicalization; canonicalize successful cwd changes and the actual execution/interpreter targets.
- Refuse execution targets that cannot be re-read, and make public claims explicit about audit-time filesystem verification rather than an exec-time interpreter-byte snapshot.
- Pin each corrected path and vanished-target behavior with targeted mutations, strengthen the specific audit-reason assertions, restore full focused green, then publish the same draft head for firstmate.
