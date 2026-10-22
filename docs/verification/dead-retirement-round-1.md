# Retirement review corrections — 2026-10-22

This is a historical round-1 record. [Round 2](dead-retirement-round-2.md) corrects the existing-target/missing-child behavior and requires the target's own absence before any per-session retirement; the current operator contract is `../dead-session-retirement.md`.

Source sampled: `839bdac6a0c0ac5a9475d90d633f028f992c70ef` on [PR #343](https://github.com/swxtchio/swx-opencode/pull/343). Authority: the round-1 fix order against `1cd9a4a616`. This record supersedes the earlier no-op compaction-retry behavior and positive-byte-recovery success gate; the initial allocation rehearsal remains a valid dated observation of its recorded source.

## Delivered behavior

- A small producer-created session retires successfully after completed checkpoint/vacuum/integrity even when measured `reclaimedBytes` is zero. Actual row counts and byte measurements are still reported.
- Directory scope uses consistent filesystem identities for trailing separators, parent segments and resolvable symlink prefixes. The scope includes recorded subdirectories, with slash-boundary matching preserving similarly named sibling directories. Each session's directory is still independently checked for disappearance.
- `partial`, `retainedSessions` and `retainedCounts` expose protected residue inside the requested scope. Eligible sessions retire independently of protected matching sessions; a caller requiring full retirement must inspect that residue. The handoff document now states the applied-partial/empty-protected/no-op results and exit behavior.
- A retirement with no deleted rows skips full vacuum and does not retry a prior failed compaction. The earlier retry contract was corrected in the handoff documentation.
- Component-wise symlink resolution preserves uncertainty instead of allowing a parent segment to hide a dangling link. The legacy-path uncertainty fixture was extended using producer-created session rows and real filesystem paths.

## Passing proof

From `packages/opencode`:

```sh
bun test --timeout 60000 test/cli/db/prune.test.ts -t "small retirement|retirement normalizes|mixed live|no-op retry|teardown CLI|directory uncertainty|directory restored|locked inventory|dead-directory parents|cross-session|live review hold|full vacuum|post-commit checkpoint|SQLite exclusion|CLI apply deletes"
```

The final focused run completed with **17 pass, 0 fail**, observing **168 assertions**. It includes the changed behavior plus related directory restoration, holder, cascade, integrity, physical-compaction and committed-failure controls. `bun typecheck`, focused correctness lint on the changed TypeScript files and Prettier checks passed. The final broad suite remains Firstmate's merge-gate responsibility.

An intermediate focused run encountered the already tracked early-refusal observation in [#344](https://github.com/swxtchio/swx-opencode/issues/344). Full production-report diagnostics were added at the failing vacuum-test admission. The subsequent focused runs passed; the refusal's cause remains unestablished and the liveness safeguards were not relaxed.

## Behavior-removal red proofs

All temporary mutations were restored before the final passing run. Each red run terminated inside its bounded test execution.

| Removed behavior | Regression and observed failure |
| --- | --- |
| Small-retirement success without a positive byte delta | Restoring `allocated-bytes-not-reclaimed` made `small retirement` exit nonzero despite committed deletion, completed vacuum and zero measured byte delta. |
| Filesystem identity normalization | Substituting lexical `path.normalize` made trailing-separator and symlink-alias retirement inventory select no sessions. |
| Consistent empty-retirement matching | Restoring byte-based excluded-session matching made all spelling cases falsely return successful `noop` while a protected matching session remained. |
| Acceptance of parent-segment spellings | Restoring the parent-segment rejection made its production CLI inventory refuse the valid target spelling. |
| Scoped residue reporting | Removing retained-session results made the mixed live/dead retirement report `partial: false` despite retaining the live session. |
| Subdirectory scope | Restoring exact-only directory matching prevented retirement of the missing subdirectory's session in the mixed target. |
| Sibling scope boundary | Removing the slash boundary included a similarly named sibling worktree in retirement inventory. |
| No-op vacuum gating | Bypassing the no-op return and actual-deletion vacuum gate made the no-op retry execute `VACUUM`, detected by the database execution spy. |
| Dangling-link uncertainty during normalization | Replacing component-wise identity resolution with lexical resolution selected and deleted the uncertain dangling-link/parent-segment session. |
