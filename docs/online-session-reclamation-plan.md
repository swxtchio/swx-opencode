# Online Session reclamation plan

Authority: swxtchio/swx-opencode#353, “Ask” and the task brief’s “Need” and “Constraints.” This plan resolves the implementation boundary; the shipped CLI and its output remain authoritative for behavior.

## Plan

- Add `opencode db prune --online` as a separate owner from the existing offline prune and retire paths.
- Require an explicit database target and explicit Session IDs for online mutation. Keep the existing missing-directory, activity, storage-ownership, parent, cascade, writer and work-state protections.
- Keep inventory scoped to the requested IDs and avoid whole-history payload materialization or hashing. Prove historical work state with partial-index existence probes and activity with indexed latest-timestamp seeks; query-plan checks reject scans, temporary sorts and materialized queue-ID lists. Revalidate monotonic per-table high-water fences (rowid, sequence, or the verified keyset tuple) under each write transaction. Check cross-Session Part references by indexed Message ID in the same locked batch before any Message delete can cascade. Page Message and event deletion by `message_session_time_created_id_idx` and `event_aggregate_seq_idx`.
- Reclaim only when the existing store reports incremental auto-vacuum support. Use positive page-bounded vacuum steps with passive checkpoints between them, outside deletion transactions; recheck the caller’s free-space reserve before every mutating step.
- Emit one JSON result on stdout, keep diagnostics on stderr, and preserve committed progress, completed and remaining Sessions, protected residue, outstanding checkpoint/freelist work, per-batch lock duration, and separate page and filesystem-allocation measurements across failures and retries.
- The producer contract is published in [issue #353's contract comment](https://github.com/swxtchio/swx-opencode/issues/353#issuecomment-6537686549), with the round-two correction in the [latest issue comment](https://github.com/swxtchio/swx-opencode/issues/353). Producer-driven CLI tests cover >10k-row adversarially ordered histories, indexed-proof plan refusals, reader-pinned reclaim debt, Session-row byte/capacity limits, bounded partial work, late cross-Session Parts and live-writer progress; `docs/dead-session-retirement.md` and `CHANGESET.md` record the interface.
