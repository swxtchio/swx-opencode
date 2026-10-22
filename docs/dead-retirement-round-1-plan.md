# Dead-worktree retirement — round 1

Authority: the captain's round-1 fix order for PR #343 and its adjudicated findings against `1cd9a4a616`.

- Treat successful deletion/checkpoint/vacuum/integrity as a completed operation even when allocated bytes do not shrink; keep the measured byte outcome honest and report scoped retained sessions and their owned rows.
- Resolve directory identities through the nearest existing filesystem ancestor, stripping equivalent spelling differences consistently in inventory and empty-retirement detection. Include recorded subdirectories of a worktree target, while confirming each session's directory absence independently.
- Preserve per-session partial retirement, document retained-session results and caller retry obligations, and exercise mixed live/dead and eligible/protected targets through the CLI.
- Return harmless no-op retirement without full vacuum when no rows were deleted. Update the former compaction-retry contract and its regressions to match this commanded advisory change.
- Run focused producer-driven regressions, behavior-removal red proofs and package typecheck; push only the existing branch/draft and file the completion wake after head verification.
