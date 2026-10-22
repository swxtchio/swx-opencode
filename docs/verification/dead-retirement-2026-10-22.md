# Dead-worktree reclamation verification — 2026-10-22

Final production/test source sampled: `1d386c7e934458342badfdf6b24a7277d5f01a06`, reachable through [draft PR #343](https://github.com/swxtchio/swx-opencode/pull/343), including the merge of `swxtch`'s small-model schema/producer change (#342). The earlier independent rehearsal's production source is retained separately in its raw snapshot. The rehearsal driver is committed beside this evidence in that PR. Environment: Linux ARM64, Bun 1.4.2. All database mutation used fresh private stores under the task checkout's `.scratch`.

## Passing boundary run

From `packages/opencode`, `bun test --timeout 60000 test/cli/db/prune.test.ts` completed with **37 pass, 0 fail**, observing 419 assertions. The suite includes the existing ownership, cascade, unfinished-work, holder, identity and integrity preservation controls as well as the new missing-directory, counts, compaction and teardown behavior. `bun typecheck` passed after the implementation, regression additions and rehearsal driver. Focused correctness lint over the changed TypeScript files passed with no warnings or errors.

The initial whole-file run exposed an obsolete race fixture: it tried to admit a prompt after the cleanup connection already held exclusive SQLite ownership. The fixture now admits through the real queue producer immediately before ownership acquisition, then removes age as a competing blocker. Its guarded behavior is pending-work preservation. The final passing run includes that correction.

One later whole-file run returned an early `refused` result in the late-reader/vacuum test before that assertion retained its full production report. After merging the newer base, another run refused in the existing checkpoint-failure test and the directory-compaction retry; the checkpoint test's rows remained intact. The refusals' causes are unestablished. Full refusal diagnostics were added at these assertions, and the subsequent whole-file run passed. [Issue #344](https://github.com/swxtchio/swx-opencode/issues/344) preserves the intermittent observations for broader process-scan investigation without weakening the unchanged fail-closed liveness policy.

## Behavior-removal evidence

Each mutation was temporary, run through the same owning production boundary, then restored. The production source was checked after restoration, and the whole-file passing run above followed all mutations and final regression refinements. Every red run terminated inside the tests' bounded execution; none required killing a hung test.

| Removed behavior | Focused regression | Observed red result |
| --- | --- | --- |
| Directory confirmation: treat every valid absolute directory as missing | `dead-directory inventory`, `directory uncertainty`, `directory restored`, `dead-directory parents` | Four failures: a live session entered inventory, inaccessible/symlink sessions were deleted, a restored directory's session was applied, and live descendants were selected/deleted. |
| Destructive-decision directory recheck after locked inventory | `destructive decision` | The newly restored directory's session was applied instead of refused. |
| Directory scope filtering | `teardown CLI` | An unrelated missing directory's session entered targeted retirement inventory. |
| Exclusive SQLite ownership: replace exclusive mode with normal mode | `SQLite exclusion` | The late process successfully read and acquired a write transaction during post-commit vacuum; expected both operations blocked. |
| Full vacuum: substitute incremental vacuum | `full vacuum` | NONE-mode cleanup committed deletion but reported `allocated-bytes-not-reclaimed`; the production CLI returned nonzero. |
| Final WAL truncate checkpoint after vacuum | `full vacuum` | The vacuum WAL retained allocated space; measured recovery was negative and the production CLI returned nonzero. |
| Inventory count reporting: return zero owned rows | `dead-directory inventory` | Independently counted producer-owned storage disagreed with `selectedCounts`. |
| Committed deletion count reporting: return zero owned rows | `dead-directory inventory` | Independently counted before-state disagreed with `deletedCounts` after successful deletion. |
| Empty-directory retirement retry support: refuse every empty selection | `teardown CLI`, `a post-commit vacuum failure` | Successful-retirement repeat returned nonzero, and the failed-vacuum retry refused instead of compacting. |
| Post-commit compaction outcome reporting: let vacuum error escape to the pre-commit refusal handler | `a post-commit vacuum failure` | Already committed deletion was reported as refused rather than a committed compaction failure. |
| Pending-work safeguard | `pending prompt admitted` | The formerly protected row was deleted; its result was a committed failure rather than zero-row refusal. |
| Supported retirement routing: remove `RetireCommand` registration | `teardown CLI` | The executable rejected the retirement options instead of reaching the handler. |
| Explicit database targeting: ignore `--database` | `teardown CLI` | The result targeted the deliberately different environment-default filename instead of the commanded store. |

The focused commands used `bun test --timeout 60000 test/cli/db/prune.test.ts -t "<matching regression names>"`. Unchanged preservation controls remain valid without requiring them to fail merely because the new feature is absent.

## Independent private on-disk rehearsal

`bun test/fixture/prune-rehearsal.ts` completed at the timestamp retained in [the raw generated snapshot](dead-retirement-2026-10-22.json). It created producer-owned dead and live worktree sessions, prepared an `auto_vacuum=NONE` store, removed only the dead directory, inventoried through `opencode db prune`, and applied through that same supported command. Before/after allocation was independently measured with filesystem `stat.blocks * 512` across the database and sidecars after the CLI exited.

| Dated observation | Result |
| --- | --- |
| Allocated bytes before | 25,575,424 |
| Allocated bytes after, database plus sidecars | 405,504 |
| Actual deleted session rows | 1 |
| Actual deleted owned rows, including the session | 34 |
| Live session and its Parts | Byte-equivalent queried records retained |
| Integrity check | `ok` |
| Foreign-key violations | None |
| Both truncate checkpoints | Completed, `busy: 0` |
| Full vacuum | Completed |

This is a historical private allocation measurement, not a forecast for a home's storage recovery or temporary-space requirement. The fixture store and isolated home were removed after measurement. No shared-home database, fleet server, remote cache or Firstmate tracked file was changed.
