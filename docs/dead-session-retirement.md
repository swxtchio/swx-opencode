# Commanded local dead-worktree reclamation

`opencode db prune` inventories the targeted local database by default. `opencode db retire` is its directory-targeted teardown entry point. Consult their `--help` output and `packages/opencode/src/cli/cmd/db-prune.ts` for the current options and decision boundary.

## Operator lifecycle

Authority: [swxtchio/swx-opencode#237's 2026-10-22 elevation](https://github.com/swxtchio/swx-opencode/issues/237#issuecomment-6330400385) and the agreed procedure in `/home/byates/fm-llmrouter/data/peer-mail/reply-disk-full-2026-10-22.md`, “The safe prune/vacuum path.” This is an explicitly commanded, per-home offline operation. Completing usage capture, normal worktree removal, age, or a merge does not schedule a bulk prune.

1. Identify the exact local database using `opencode db path` in the owning home's environment. Use its absolute canonical regular-file path with `--database`; this option overrides the environment's default database and never creates a missing store.
2. Preserve the existing Firstmate usage capture through its owner. The cleanup accepts best-effort unavailable or failed measurement reporting without turning those states into zero usage or requiring a certified receipt.
3. Coordinate that home's offline window: stop **all** its OpenCode serves and other users of this database, including readers. Keep them stopped until the cleanup process has exited. Stopping only the torn-down task's server is insufficient when the database is shared by other crews or the supervisor.
4. Confirm that the worktree directories being retired are removed and will not be recreated during the operation. Read-only inventory is separate from apply; apply checks disappearance again under its write lock and immediately before deletion. Existing, empty, invalid, inaccessible and symlink-uncertain recorded paths remain protected. A protected descendant also protects its parent.
5. Plan temporary space before apply. The agreed path estimates **roughly twice the database size** as free temporary space; this is a planning approximation, not a guaranteed capacity formula. Check both the database and SQLite temporary-file filesystem, with headroom for the WAL, journal, other activity and filesystem allocation. Re-check the named procedure for the fleet's capacity decision.
6. Review the inventory, explicitly apply the chosen scope, inspect the returned deletion and compaction outcome, then restart the home after the command exits. Do not retry by removing the database-holder or integrity safeguards.

The consumer checks use the existing Linux process/lock inspection boundary. An uninspectable holder environment or database directory that admits other users refuses apply. SQLite exclusive locking stays on the cleanup connection through the committed deletion, truncate checkpoint, full `VACUUM`, final truncate checkpoint and result measurement. An arriving conflicting consumer cannot successfully read or write during compaction. The operation affects only the named local store; it does not delete remote or cached copies.

## Bulk inventory and apply

Run these from a surviving operator directory, substituting the actual database filename:

```sh
opencode db prune --database "/absolute/home/data/opencode/opencode.db"
opencode db prune --database "/absolute/home/data/opencode/opencode.db" --apply
```

To narrow the scope, repeat `--session <id>` or supply `--directory "/absolute/removed-worktree"`. `--before <UTC-or-offset-timestamp>` is an optional activity filter within the missing-directory set. It cannot authorize deletion of an old session whose directory still exists. Inventory may show a missing-directory session in `excludedSessions` when a consumer, pending work, relationship or storage-ownership safeguard protects it; inspect its `directoryState` and `reasons`, not just its creation date.

## Teardown handoff

The external caller must retain the database filename and the worktree's absolute recorded directory before removing the worktree. After the caller's usage operation and directory removal, invoke the installed OpenCode binary from a surviving directory during the home's coordinated offline window:

```sh
opencode db retire --database "/absolute/home/data/opencode/opencode.db" \
  --directory "/absolute/removed-worktree"

opencode db retire --database "/absolute/home/data/opencode/opencode.db" \
  --directory "/absolute/removed-worktree" --apply
```

The first invocation is inventory; only the second requests deletion. Do not execute from the deleted worktree, and do not ask a running HTTP server to perform this offline operation. Firstmate's integration point is after its recorded-server stop, usage finalization and successful worktree removal in `bin/fm-teardown.sh`. Its owner must coordinate the remaining database users before invoking this interface. This OpenCode change does not edit Firstmate's teardown implementation.

If the directory still exists or any matching session is protected, retirement refuses with no deleted rows. After successful retirement, repeating the same target returns `noop` with no session deletions; it still completes compaction, allowing a directory-targeted retry to finish a previously failed vacuum without adding a new receipt framework. A new session at another directory is never included merely because it is a child or shares a project.

## Results and retry

- `action: "inventory"`: read-only result. Review `selectedSessions` and `excludedSessions`, their recorded directories and per-table `ownedRows`, and the aggregate `selectedCounts`. Counts include the session rows as well as their owned history. `changedRows` remains zero.
- `action: "applied"`: inspect `committed`, `deletedSessionIDs`, `deletedCounts.sessions`, `deletedCounts.ownedRows`, `deletedCounts.byTable` and `changedRows` for the actual committed deletion. The compaction result lives under `reclamation`: checkpoint, vacuum, final vacuum checkpoint, integrity/foreign-key checks and allocated bytes for the database plus sidecars. Physical reclaim is measured using filesystem allocated blocks, not just SQLite's freelist or logical row sizes.
- `action: "noop"`: directory retirement has no remaining eligible rows and compaction completed. It is a successful harmless retry with zero deleted rows.
- `action: "refused"`: check `refusalReasons` and `reclamation.reasons`. Resolve the named holder/liveness, directory, work-state, selection or storage-ownership problem before retrying the same explicit target. No deletion transaction committed.
- `action: "failed"` with `committed: true`: the reported rows are **already deleted**. Checkpoint/vacuum failure or non-positive measured disk recovery is not a rollback. Keep the home offline while diagnosing space or compaction failure. For a removed directory whose matching sessions have all been retired, repeat `db retire --database ... --directory ... --apply` to retry compaction; protected remaining rows still refuse. Preserve the original output so committed deletion is not mistaken for missing usage or an unapplied command.

The process exits nonzero for refusal or failure. Successful inventory, apply and no-op retirement exit zero. No result implies recovered storage merely because rows were deleted.

## Private verification

The owning behavior suite is `packages/opencode/test/cli/db/prune.test.ts`; its sessions, prompts, asks and events come from `test/fixture/prune-producer.ts`. From `packages/opencode`, run:

```sh
bun test --timeout 60000 test/cli/db/prune.test.ts
bun typecheck
bun test/fixture/prune-rehearsal.ts
```

The standalone rehearsal creates and removes only a fresh private store under this checkout's `.scratch`, exercises the production CLI on a migrated `auto_vacuum=NONE` database, and writes a dated snapshot to `docs/verification/dead-retirement-2026-10-22.json`. It uses direct vacuum only to prepare the NONE-mode fixture; the production command owns deletion and the measured reclaim. See [the dated handoff evidence](verification/dead-retirement-2026-10-22.md) for passing and behavior-removal results. These proofs do not authorize pruning a live home's database.
