# Live executable observation evidence

Snapshot: 2026-10-22, implementation `bc4c5a923e569a4658a4d5bded590cb4f47b726f`, reachable through [draft PR #329](https://github.com/swxtchio/swx-opencode/pull/329). Scope: the round-3 order for [#324](https://github.com/swxtchio/swx-opencode/issues/324). Host: Linux ARM64, Bun 1.4.2, Turbo 2.10.2, Node v22.20.0, Python 3. The observer records procfs executable identity and kernel-expanded argv at ptrace exec stops; it does not re-read a script's later bytes to infer its interpreter.

## Runtime boundary and regression absence

The owner is `script/fast-suite-runner.test.ts`. Its fixture copies and commits the production runner and observer, then executes real Bun/Turbo lifecycle work. Launch markers come from executed programs, not constructed observer records. Commands ran from `script`, with `TMPDIR="$PWD/.local/tmp"` and `TURBO_TELEMETRY_DISABLED=1`. `FAST_SUITE_RUNNER_CASE=<cell>` selects a cell while retaining the existing controls.

Against reviewed implementation `79e1ea54aa`, both `deleted-cwd` and `deleted-fchdir` controls failed at the successful-fast exit assertion: expected 0, received 1. Each child actually completed `/usr/bin/true` before its parent removed the temporary directory. The old audit refused successful chdir/fchdir targets after cleanup. Four separate `overwrite-bun`, `overwrite-node`, `repoint-bun`, and `repoint-node` runs failed at the refusal assertion: expected 1, received 0. Their unlisted shebang tests ran, replacement bytes or repointed symlinks concealed the interpreter from the old audit, and fast mode reported successful completion.

The live observer preserves both deleted-cwd controls and refuses all four replacement/repoint cells with the actual Bun/Node launch diagnostic. It also retains the deletion cells, now classified from the interpreter that executed rather than an unavailable-script diagnostic.

| Counterfactual | Actual behavioral failure |
| --- | --- |
| Replace `const args = entry.argv.slice(1)` with `const args = []` in the production audit | Six separate overwrite/repoint/deletion Bun/Node runs each exited 1 at `expect(launchEscape.exitCode).toBe(1)`, expected 1, received 0. Each unlisted marker executed and the runner printed `verdict=completed mode=fast target_count=4 exit_code=0`. |
| Remove the production completed-record validation | The real observer producer was configured by the fixture to omit its completed record for package/preparation dispatches while retaining the probe receipt. All selected tests ran. The incomplete-observation refusal assertion failed: expected 1, received 0. No hand-built trace replaced the producer. |
| Resume signal-delivery stops with 0 rather than the received signal | The direct observer regression failed at the signal-exit assertion: expected 138 for SIGUSR1, received 0. |
| Remove the live executable pathname from emitted exec records | The direct observer regression failed at the loaded-executable assertion after a non-leader Python thread actually execed `/usr/bin/printf`; expected its real executable path and argv, received an empty executable field. |

Each counterfactual was restored. The final complete focused run on the committed implementation exited 0: **2 pass, 0 fail, 389 assertions**. The fork-maintenance command `bun test sync-upstream.test.ts changeset-check.test.ts fast-suite-runner.test.ts` exited 0: **97 pass, 0 fail, 670 assertions**. These include the original prerequisite/wrong-owner, absolute/copied Bun, delegation, Node, shebang/recursive/fd, inspector, path-resolution, lifecycle, forwarding, and exact-JUnit controls.

During implementation, a maintenance run exposed missing separate death reports for sibling threads retired by exec. The observer was corrected to drain until the kernel returns ECHILD, rather than treating its PID bookkeeping set as the completion authority. Failed or incomplete tracing still cannot produce a successful fast verdict. The green measurements above use that correction; the earlier failed runs are not proof of readiness.

## Real manifest and mechanical checks

After `bun install --frozen-lockfile`, the clean implementation head ran `TMPDIR=/tmp/opencode TURBO_TELEMETRY_DISABLED=1 bash tests/run.sh --fast` from the repository root. It exited 0 with **`verdict=completed mode=fast target_count=9 exit_code=0`**; every selected package passed and no cwd-cleanup refusal occurred.

An earlier real-manifest run with TMPDIR nested inside this checkout completed observation but exited 1 because config fixtures inherited the repository's `.opencode/opencode.jsonc` tool permissions through their ancestor directories. Moving the scratch root outside the project removed that environmental contamination; no config test or product source was changed. The successful run above uses the isolated root.

Fork-script typecheck (`bun run typecheck:script`), focused correctness lint (`bunx oxlint --config .oxlintrc.gate.json script/fast-suite-runner.test.ts`), Python AST parsing, Bash syntax, rendered help, diff checks, and the clean-head commit-identity gate passed. This repository has no `bin/fm-lint.sh`; the project-memory helper reported unchanged. Help, manifest, and changeset wording describe Python 3 and Linux procfs/ptrace, live interpreter/argv records, and the existing bounded observation scope. The full release gate and captain-owned final merge gate remain unchanged.

Prior-round proof is retained in [the observer evidence](fast-suite-observer.md) and historical PR revisions. Bare Node `node:test` execution without `--test` remains the accepted, disclosed residual tracked in [#337](https://github.com/swxtchio/swx-opencode/issues/337).
