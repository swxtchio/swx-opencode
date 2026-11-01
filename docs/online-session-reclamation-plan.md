# Online Session reclamation plan

Authority: swxtchio/swx-opencode#353, its [published producer contract](https://github.com/swxtchio/swx-opencode/issues/353#issuecomment-6537686549), and the [round-two correction](https://github.com/swxtchio/swx-opencode/issues/353#issuecomment-6541481027), together with the task brief's “Need” and “Constraints.” This plan resolves the implementation boundary; the shipped CLI and its output remain authoritative for behavior.

## Plan

- Add `opencode db prune --online` as a separate owner from the existing offline prune and retire paths.
- Require an explicit database target and explicit Session IDs for online mutation. Keep the existing missing-directory, activity, storage-ownership, parent, cascade, writer and work-state protections.
- Keep inventory scoped to the requested IDs and avoid whole-history payload materialization or hashing. Prove work state and activity with Session/aggregate-scoped indexes; activity uses a scoped maximum rather than requiring a new whole-source ordering index. Query-plan checks reject whole-table scans, temporary sorts and materialized queue-ID lists. Revalidate monotonic per-table high-water fences (rowid, sequence, or the verified keyset tuple) under each write transaction. Check cross-Session Part references by stable Message ID in the same locked batch before any Message delete can cascade. Page Message and event deletion by `message_session_time_created_id_idx` and `event_aggregate_seq_idx`.
- Reclaim only when the existing store reports incremental auto-vacuum support. Use positive page-bounded vacuum steps with passive checkpoints between them, outside deletion transactions; recheck the caller’s free-space reserve before every mutating step.
- Emit one JSON result on stdout, keep diagnostics on stderr, and preserve committed progress, completed and remaining Sessions, protected residue, outstanding checkpoint/freelist work, per-batch lock duration, and separate page and filesystem-allocation measurements across failures and retries.
- The producer contract is published in [issue #353](https://github.com/swxtchio/swx-opencode/issues/353#issuecomment-6537686549), with its [limit clarification](https://github.com/swxtchio/swx-opencode/issues/353#issuecomment-6537694254) and [round-two correction](https://github.com/swxtchio/swx-opencode/issues/353#issuecomment-6541481027); implementation guidance is recorded in `docs/dead-session-retirement.md` and `CHANGESET.md`. Producer-driven CLI tests cover >10k-row adversarially ordered histories, indexed-proof plan refusals, reader-pinned reclaim debt, Session-row byte/capacity limits, bounded partial work, late cross-Session Parts and live-writer progress.

## Round-three correction sequence

The round-four corrections preserve the no-validation-row-cap contract and make existing-store proof preparation live-safe:

1. Keep whole-table source-index construction out of the live reclamation path. Existing per-Session indexes, proof-table indexes and triggers support it; no source index is rebuilt to reclaim an existing store.
2. Make online `--apply` the only existing-store proof preparation path. It advances cross-Session Part scope, stable queue-alias ownership and writer-lifetime maps in transactions capped at 128 rows, with at most 128 proof transactions per invocation. `proofPreparation` reports completed batches, rows and remaining proofs. Other database holders stay live between batches; an incomplete pass refuses Session deletion until a retry completes it.
3. Key alias ownership by `(item_id, alias_id)` and cross-Session Part proofs by stable Part and Message IDs. Inventory uses the completed maps; each delete transaction rechecks late external Part references before a Message can cascade.
4. Keep the historical writer census outside each `BEGIN IMMEDIATE` delete batch. Its live-safe paged backfill feeds a primary-key point lookup for each current process lifetime during locked revalidation, alongside the durable event/row fences.

Each correction lands with a producer-driven regression in its own commit. After all required findings pass the complete prune suite, Core/OpenCode typechecks, and lint/format checks, update and push the draft PR for round-four review.
