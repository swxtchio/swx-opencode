# Retirement target gate — round 2, 2026-10-22

Source sampled: `5bc95cf478ab3f5505cb0e19af3be5707ef7eafd`, reachable through [PR #343](https://github.com/swxtchio/swx-opencode/pull/343). Authority: the round-2 order against `bfdbe712f5`.

The target directory's own absence now gates the entire retirement, including missing-directory descendants. Existing targets return a zero-change `noop` with `noopReason: "target-exists"`; uncertain targets refuse. The gate is checked before apply, under the write lock and immediately before deletion. A restored target preserves the previously selected session rows and reports them retained, with an empty deletion scope.

Unknown session identities whose recorded paths lexically belong to the requested target are preserved and included in `retainedSessions`/`retainedCounts`, rather than omitted as outside scope. The executable regressions use real producer-created sessions, inaccessible paths, dangling symlinks and a missing sibling. The unknown target itself also refuses without mutation.

`noopReason: "no-matching-sessions"` reports only that a missing target currently matches no rows. It does not establish prior retirement. The operator contract now distinguishes that observation from an existing-target no-op and from actual applied retirement. Per-session partial retirement remains supported only after the target is confirmed missing.

## Passing verification

From `packages/opencode`:

```sh
bun test --timeout 60000 test/cli/db/prune.test.ts -t "small retirement|retirement normalizes|existing retirement target|restored retirement target|retirement retains in-scope|no-op retry|teardown CLI|directory uncertainty|directory restored|locked inventory|dead-directory parents|cross-session|live review hold|full vacuum|post-commit checkpoint|SQLite exclusion|CLI apply deletes"
```

The final focused run completed with **21 pass, 0 fail**, observing **210 assertions**. `bun typecheck`, focused correctness lint and formatting checks passed. These tests cover the new target/uncertainty behavior plus preserved normalization, removed-target partial retirement, physical compaction, holders, cascades and committed failures. Firstmate owns the final broad suite after review convergence.

## Per-pin behavior-removal proofs

All mutations were temporary and restored before the final passing run; every red run failed within its bounded test execution.

| Removed behavior | Observed red result |
| --- | --- |
| Target-absence gates in inventory and apply, restoring the pinned over-deletion behavior | Three regressions failed on independent before/after owned-row comparisons: the existing target, restoration at `BEGIN IMMEDIATE`, and restoration after locked inventory all deleted the missing descendant's session/history. |
| Lexical retention for unresolved scope | Both inaccessible and dangling-symlink CLI cases omitted their uncertain session from retained results. |
| Observational no-op reason | Existing-target and teardown CLI regressions lost the reason distinguishing a blocked existing target from a missing target with no matching sessions. |

The former existing-target/missing-child test was rewritten to preserve all rows instead of asserting deletion. Its old expectation is superseded by the target lifecycle requirement. The prior uncertainty fixture also has an explicit timeout so its filesystem cleanup remains bounded under package-local test invocation.
