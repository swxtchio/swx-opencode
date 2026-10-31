# Online Session reclamation plan

Authority: swxtchio/swx-opencode#353, “Ask” and the task brief’s “Need” and “Constraints.” This plan resolves the implementation boundary; the shipped CLI and its output remain authoritative for behavior.

## Plan

- Add `opencode db prune --online` as a separate owner from the existing offline prune and retire paths.
- Require an explicit database target and explicit Session IDs for online mutation. Keep the existing missing-directory, activity, storage-ownership, parent, cascade, writer and work-state protections.
- Keep inventory scoped to the requested IDs and avoid a whole-store payload walk. Apply bounded row-and-payload deletion batches with normal WAL locking, rechecking holder registration, process identity, selection and Session eligibility inside each write transaction.
- Reclaim only when the existing store reports incremental auto-vacuum support. Use positive page-bounded vacuum steps with passive checkpoints between them, outside deletion transactions; recheck the caller’s free-space reserve before every mutating step.
- Emit one JSON result on stdout, keep diagnostics on stderr, and preserve committed progress, completed and remaining Sessions, protected residue, and separate page and filesystem-allocation measurements across failures and retries.
- Publish the producer contract on #353 before implementation. Add producer-driven CLI coverage for bounded partial work, revalidation and live-writer progress; update operator documentation and `CHANGESET.md`.
