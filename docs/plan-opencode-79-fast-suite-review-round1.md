# Fast-suite review-round plan

1. Use Bun's explicit `./` path mode for each manifest target so substring filters cannot select neighboring tests; prove exact execution through both Turbo and direct package dispatch.
2. Preserve the per-target active-case refusal, prove its regression goes red with the guard disabled, and use a tracked literal-glob target to exercise the reserved-character refusal.
3. Keep dirty-checkout refusal in fast mode while preserving bare full-runner tolerance.
4. Document the TUI, Client, and JS SDK direct-script fallback beside the manifest.
5. Run focused behavioral and mutation proofs, inspect the diff, and update the existing draft PR.
