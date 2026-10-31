# Commanded Session reclamation

`opencode db prune` inventories the targeted local database by default. `--online` selects bounded live-store reclamation for explicit Session IDs; without it, prune keeps the offline path. `opencode db retire` remains the directory-targeted offline teardown entry point. Consult `opencode db prune --help`, `packages/opencode/src/cli/cmd/db-prune.ts`, the [published online producer contract](https://github.com/swxtchio/swx-opencode/issues/353#issuecomment-6537686549), its [limit clarification](https://github.com/swxtchio/swx-opencode/issues/353#issuecomment-6537694254), and the [round-two correction](https://github.com/swxtchio/swx-opencode/issues/353) for the current interface.

## Offline lifecycle

Authority: [swxtchio/swx-opencode#237's 2026-10-22 elevation](https://github.com/swxtchio/swx-opencode/issues/237#issuecomment-6330400385) and the agreed procedure in `/home/byates/fm-llmrouter/data/peer-mail/reply-disk-full-2026-10-22.md`, “The safe prune/vacuum path.” This is an explicitly commanded, per-home offline operation. Completing usage capture, normal worktree removal, age, or a merge does not schedule a bulk prune.

1. Identify the exact local database using `opencode db path` in the owning home's environment. Use its absolute canonical regular-file path with `--database`; this option overrides the environment's default database and never creates a missing store.
2. Preserve the existing Firstmate usage capture through its owner. The cleanup accepts best-effort unavailable or failed measurement reporting without turning those states into zero usage or requiring a certified receipt.
3. Coordinate that home's offline window: stop **all** its OpenCode serves and other users of this database, including readers. Keep them stopped until the cleanup process has exited. Stopping only the torn-down task's server is insufficient when the database is shared by other crews or the supervisor.
4. Run in the same filesystem/mount view as the session owners, with the relevant filesystems mounted; absence in another namespace or on an unmounted volume is not evidence of worktree removal. Confirm that the worktree directories being retired are removed and will not be recreated during the operation. Read-only inventory is separate from apply; apply checks disappearance again under its write lock and immediately before deletion. Existing, empty, invalid, inaccessible and unresolved/dangling-symlink paths remain protected. A protected descendant also protects its parent.
5. Plan temporary space before apply. The agreed path estimates **roughly twice the database size** as free temporary space; this is a planning approximation, not a guaranteed capacity formula. Check both the database and SQLite temporary-file filesystem, with headroom for the WAL, journal, other activity and filesystem allocation. Re-check the named procedure for the fleet's capacity decision.
6. Review the inventory, explicitly apply the chosen scope, inspect the returned deletion and compaction outcome, then restart the home after the command exits. Do not retry by removing the database-holder or integrity safeguards.

The consumer checks use the existing Linux process/lock inspection boundary. An uninspectable holder environment or database directory that admits other users refuses apply. SQLite exclusive locking stays on the cleanup connection through the committed deletion, truncate checkpoint, full `VACUUM`, final truncate checkpoint and result measurement. An arriving conflicting consumer cannot successfully read or write during compaction. The operation affects only the named local store; it does not delete remote or cached copies.

## Online Session reclamation

Use this path when the per-home store must stay available to other OpenCode processes. Name the existing database and every Session explicitly. `--before` is an optional activity filter that only narrows the missing-directory set. Set the reserve from the caller's reviewed capacity policy; the producer checks it again before each delete, vacuum and checkpoint step.

```sh
DB="/absolute/home/data/opencode/opencode.db"
SESSION_ID="ses_example"
RESERVE_BYTES="<caller-selected-positive-bytes>"

opencode db prune --online --database "$DB" --session "$SESSION_ID" \
  --reserve-bytes "$RESERVE_BYTES"

opencode db prune --online --database "$DB" --session "$SESSION_ID" \
  --apply --reserve-bytes "$RESERVE_BYTES"
```

Replace every example value before running it. Inventory is read-only. Review `deletionScope.sessionIDs`, `selectedSessions`, `skippedSessions`, and `reclamation.ready`/`reclamation.reasons` before applying. There is no per-Session validation-row cap; `--batch-rows` and `--batch-bytes` bound each transaction, and byte accounting includes every TEXT/BLOB column of each owned row, including the final Session row. Existing or uncertain directories, unselected descendants, cross-Session cascades, pending work, live writers, unknown storage ownership, an unregistered holder, or an unsupported auto-vacuum mode do not authorize deletion. A registered process may remain connected when it does not own a selected Session; `db retire` does not enter online mode.

The producer contract is one JSON object on stdout per valid call; diagnostics go to stderr. Inventory exits successfully even when readiness is false, so inspect `reclamation.ready` rather than treating exit zero as apply approval. A reader-pinned passive checkpoint is recorded in `reclamation.outstanding` and does not stop further Session deletion; once deletion finishes, outstanding WAL or freelist work makes the apply incomplete. A refused or failed apply exits nonzero; invalid CLI usage exits with usage status and no JSON.

For `action: "failed"` with `committed: true`, rows already reported as changed are committed. Use `deletedSessionIDs`, `partialSessionIDs`, `remainingSessionIDs`, `skippedSessions`, and `batches` to establish exactly what remains; retry only the original explicit scope. A missing Session on a later retry is not evidence that the earlier call deleted it. If deletion is complete but reclaim work remains, continue with the storage-only form, which never deletes or claims a Session:

```sh
opencode db prune --online --database "$DB" --reclaim-only \
  --apply --reserve-bytes "$RESERVE_BYTES"
```

SQLite page/freelist progress and filesystem allocation are separate measurements. Review the per-step checkpoint counters, `remainingFreelistPages`, `remainingWalFrames`, and signed `netAllocatedBytes`; zero, negative, or unavailable allocation change is reported rather than inferred from a freelist decrease. A passive checkpoint can leave frames outstanding while another process holds a reader; the result keeps that work visible for a later reclaim-only retry.

## Offline bulk inventory and apply

Run these from a surviving operator directory, substituting the actual database filename:

```sh
opencode db prune --database "/absolute/home/data/opencode/opencode.db"
opencode db prune --database "/absolute/home/data/opencode/opencode.db" --apply
```

To narrow the scope, repeat `--session <id>` or supply `--directory "/absolute/removed-worktree"`. A directory target includes sessions recorded at that path or below it, with each recorded directory checked independently. Equivalent trailing separators, parent segments and resolvable symlink-prefix spellings use the same filesystem identity; keep the canonical path before teardown if removing a symlink would erase its mapping. `--before <UTC-or-offset-timestamp>` is an optional activity filter within the missing-directory set. It cannot authorize deletion of an old session whose directory still exists. Inventory may show a missing-directory session in `excludedSessions` when a consumer, pending work, relationship or storage-ownership safeguard protects it; inspect its `directoryState` and `reasons`, not just its creation date.

## Teardown handoff

The external caller must retain the database filename and the worktree's absolute recorded directory before removing the worktree. After the caller's usage operation and directory removal, invoke the installed OpenCode binary from a surviving directory during the home's coordinated offline window:

```sh
opencode db retire --database "/absolute/home/data/opencode/opencode.db" \
  --directory "/absolute/removed-worktree"

opencode db retire --database "/absolute/home/data/opencode/opencode.db" \
  --directory "/absolute/removed-worktree" --apply
```

The first invocation is inventory; only the second requests deletion. Do not execute from the deleted worktree, and do not ask a running HTTP server to perform this offline operation. Firstmate's integration point is after its recorded-server stop, usage finalization and successful worktree removal in `bin/fm-teardown.sh`. Its owner must coordinate the remaining database users before invoking this interface. This OpenCode change does not edit Firstmate's teardown implementation.

The target directory's own confirmed absence gates the entire retirement, before apply, under the write lock and immediately before deletion. If the target still exists, the command returns `noop` with `noopReason: "target-exists"`, preserves all matching sessions (including those in missing subdirectories), and skips vacuum. An uncertain target refuses. The caller must complete worktree removal before requesting retirement; exit zero for an existing-target no-op is not confirmation of teardown or retirement.

Only after the target is confirmed missing is eligibility per session. Eligible sessions may retire while another matching session has protected work. The result reports `partial: true`, `retainedSessions` with their reasons/per-table owned rows, and `retainedCounts` for that residue; an applied partial retirement exits zero. Unresolvable session identities whose recorded paths fall lexically under the requested target remain protected and included in the residue rather than classified outside scope. If nothing is eligible and matching sessions remain protected, the command refuses with no deletions and exits nonzero. A caller requiring complete retirement must inspect that residue, resolve the protected work or uncertainty, and retry. Sibling directory trees and sessions outside the explicit scope are not included merely because they share a project.

Repeating a missing-directory target with no matching sessions returns `noop` with `noopReason: "no-matching-sessions"`, no session deletions and vacuum skipped. That observation cannot distinguish a prior completed retirement from a directory that never matched this database; check the reported target and database identities rather than treating no-op as historical confirmation. No-op does not vacuum the whole home database or retry a previously failed vacuum. Full vacuum is tied to an apply that actually deletes rows, and its temporary-space/offline cost scales with the database, not the retired worktree.

## Offline results and retry

- `action: "inventory"`: read-only result. Review `selectedSessions` and `excludedSessions`, their recorded directories/normalized identities and per-table `ownedRows`, and the aggregate `selectedCounts`. `retainedSessions` and `retainedCounts` identify protected sessions inside the requested scope, excluding unrelated or activity-filtered sessions. Counts include the session rows as well as their owned history. `changedRows` remains zero.
- `action: "applied"`: inspect `committed`, `deletedSessionIDs`, `deletedCounts.sessions`, `deletedCounts.ownedRows`, `deletedCounts.byTable` and `changedRows` for the actual committed deletion; inspect `partial`, `retainedSessions` and `retainedCounts` for protected residue. The compaction result lives under `reclamation`: checkpoint, vacuum, final vacuum checkpoint, integrity/foreign-key checks and allocated bytes for the database plus sidecars. Physical reclaim is measured using filesystem allocated blocks, not just SQLite's freelist or logical row sizes. A small retirement can complete successfully without releasing an allocated page; zero or negative measured `reclaimedBytes` is reported honestly rather than turned into a failed deletion.
- `action: "noop"`: inspect `noopReason` and `retirementTarget`. `target-exists` means retirement was not attempted because the target still exists; `no-matching-sessions` means a missing target has no matching rows in this database. Both preserve rows and skip vacuum; neither is a disk-recovery or prior-retirement claim.
- `action: "refused"`: check `refusalReasons` and `reclamation.reasons`. Resolve the named holder/liveness, directory, work-state, selection or storage-ownership problem before retrying the same explicit target. No deletion transaction committed.
- `action: "failed"` with `committed: true`: the reported rows are **already deleted**. A real checkpoint/vacuum, integrity or allocation-read failure is not a rollback. Keep the home offline while diagnosing that failure and arranging separately commanded compaction if needed. Repeating an already completed directory retirement skips vacuum and does not finish the earlier failed compaction. Preserve the original output so committed deletion is not mistaken for missing usage or an unapplied command.

The process exits nonzero for refusal or operation failure. Successful inventory, apply (including reported partial retirement) and no-op retirement exit zero. Allocated-byte reduction is an observed result, not a success gate for small deletions. No result implies recovered storage merely because rows were deleted.

## Private verification

The owning behavior suite is `packages/opencode/test/cli/db/prune.test.ts`; its sessions, prompts, asks and events come from `test/fixture/prune-producer.ts`. From `packages/opencode`, run:

```sh
bun test --timeout 60000 test/cli/db/prune.test.ts
bun typecheck
bun test/fixture/prune-rehearsal.ts
```

The standalone rehearsal creates and removes only a fresh private store under this checkout's `.scratch`, exercises the production CLI on a migrated `auto_vacuum=NONE` database, and writes a dated snapshot to `docs/verification/dead-retirement-2026-10-22.json`. It uses direct vacuum only to prepare the NONE-mode fixture; the production command owns deletion and the measured reclaim. See [the initial allocation evidence](verification/dead-retirement-2026-10-22.md) and [round-2 target/uncertainty verification](verification/dead-retirement-round-2.md). These proofs do not authorize pruning a live home's database.
