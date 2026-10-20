# Fast-suite review-round 4 plan

1. Set Bun's own working directory to the owning package for every exact manifest-file invocation, keeping direct `bun test` and bypassing `scripts.test`.
2. Keep the JUnit executed-file set check and regression fixtures; exercise package-local preloads beneath the root Bun test guard.
3. Clarify that `scripts.test` is required only for manifest validation, then rerun the focused tests and lint checks.
4. Update the existing draft PR with the round-4 evidence, push the branch, and verify its live head.
