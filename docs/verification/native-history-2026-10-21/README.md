# Native V2 scout — execution blocker, 2026-10-21

Task: [swxtchio/swx-abbe#1227](https://github.com/swxtchio/swx-abbe/issues/1227). Task draft: [swxtchio/swx-opencode#331](https://github.com/swxtchio/swx-opencode/pull/331).

**Producer verdict: not established. Recommendation: hold the release-verification decision and consumer rollout.** The named linux-x64 executable cannot run directly on this task's ARM64 host. This is an execution-placement blocker, not an observed V2 producer failure. This record is provisional; the required completed/failed/interrupted, idempotent admission, restart, pagination and reconnect observations have not run, and no native admission/message/event identities or database writes are claimed.

## Executed artifact observations

The [named release](https://github.com/swxtchio/swx-opencode/releases/tag/v1.18.32-swxtch.d1b232e9) was read through `gh-axi release view v1.18.32-swxtch.d1b232e9 -R swxtchio/swx-opencode --full`. It identifies source `d1b232e940ef155a3e5311794a2baeaa143af1d8` and wait ancestor `4b87478ddf`. Local tag resolution and ancestry comparison succeeded. The release records a native x64 Actions build at [run 39528763022](https://github.com/swxtchio/swx-opencode/actions/runs/39528763022) / artifact `12314511486`; that prior run is provenance and a reproduction lead, not executed acceptance evidence for this task.

This task downloaded the archive and companion from that release. [Raw command observations](artifact-observations.txt) retain archive membership, lengths, digests, checksum comparison and execution refusal. The archive SHA-256 `dde0e4f1bd56664ca1d442951b15983f3c5338d1fa84c1bbee1f9b25587dd6ad`, executable SHA-256 `6891cf61987bd8566660b681217cdfc2605fd684bbf64faaf2cd5072ea0e8e80`, and packaged FFF sidecar SHA-256 `977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5` match the release's recorded values. `sha256sum -c` returned OK.

The extracted executable is `/home/byates/fm-worktrees/fm-llmrouter/swx-abbe-1227-native-history/swx-opencode/.native-history/extracted/opencode`, adjacent to its packaged sidecar. `uname -m` returned `aarch64`; `file` identified the executable as x86-64 ELF. Executing `--version` returned exit 126, `cannot execute binary file: Exec format error`, rather than a version. Neither `qemu-x86_64` nor `qemu-x86_64-static` was found. The archive and extracted files remain in the task worktree's ignored `.native-history/` scratch area; reviewers can redownload the release through the commands in the raw record.

## Required placement decision

Firstmate must name an approved native x64 host and an explicit disposable filesystem root for this task's artifact download, source/fixture staging, config/data/cache, isolated absolute `OPENCODE_DB`, and evidence collection, or redispatch the task to an x64 worktree. The current brief authorizes filesystem writes inside this launched worktree, so a different host/root cannot be selected by the crew. Substituting the ARM64 release would not fulfill the named x64 verification.

The selected source's `packages/core/src/database/database.ts::path` calls `resolvePath` with `Flag.OPENCODE_DB`; `packages/core/src/database/resolve-path.ts::resolvePath` chooses an explicit absolute selector unchanged. This source inspection informs the pending runtime probe; no observed resolved path or physical native store is claimed yet. The existing `packages/opencode/script/release-probe.ts` provides held-tool/continuation and terminal-outcome reproduction leads. Additional exact-retry, complete pagination and event-stream reconnect observations remain required by the committed plan.

## Consumer and cohort gates remain open

The consumer serving-path trace and revision pin are pending, as are executed consumer pointer/cursor-refusal and same-identity store-replacement proofs. Persistent-cohort V1 inventory is unavailable in this task's observations, so rollout readiness is unestablished. ADR0031 requires ADR-owner-approved in-place migration semantics preserving native IDs and order if retained V1 turns exist; this task authorizes no live or destructive migration.

The Abbe issue body was read in full. Its older pipeline-context side-channel/non-retention wording is superseded by ADR0031's **Pipeline snapshot and prompt compaction**, Captain amendment dated 2026-10-11 associated with swxtchio/swx-abbe#1286: supplied snapshots are ordinary native prompt input retained in audit history. Customer projection omission and provider-context compaction are separate obligations. That amendment remains governing in any resumed recommendation.

No production behavior or regression assertions changed. This record contains artifact verification evidence and a blocker recommendation only; it claims no changed-production-behavior absence proof. The task remains draft and is not reported ready for review.
