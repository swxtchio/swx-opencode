# Dead-worktree reclamation plan

Authority: swxtchio/swx-opencode#237's 2026-10-22 elevation and the assigned implementation brief.

- Extend the existing `db prune` boundary to select confirmed missing directories, retaining its schema, cascade, writer, work-state and integrity safeguards. Keep any age cutoff an optional narrowing filter; an existing or uncertain directory is always protected.
- Expose directory-targeted retirement through the supported CLI, callable from a surviving working directory against an explicitly named local database after teardown.
- Report selected session identities and owned-row counts; revalidate inside the write transaction, delete bottom-up, and report committed deletion separately from checkpoint/vacuum outcomes.
- Keep the target database offline through checkpoint and full vacuum, including stores without incremental auto-vacuum. Verify holders and SQLite exclusion at that boundary.
- Extend the production-producer prune suite with directory preservation/restoration, retirement retries, counts, offline contention and physical compaction tests. Run focused passing and behavior-removal proofs, package typecheck and crew gates.
- Publish operator lifecycle and handoff instructions and private allocated-space evidence in the same draft PR. Firstmate owns integration and live execution.
