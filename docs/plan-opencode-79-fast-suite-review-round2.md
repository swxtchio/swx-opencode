# Fast-suite review-round 2 plan

1. Dispatch every validated manifest target in its own package test invocation using Bun's explicit path mode; continue after failures and aggregate a nonzero final verdict.
2. Prove exact execution with unlisted sibling and nested tests, and prove aggregation with a failing target followed by a passing target.
3. Add a tracked whitespace-containing target so removing the whitespace guard makes that guard's regression accept and execute the path.
4. File a separate issue for TUI, Client, and JS SDK test scripts missing from the full Turbo/CI task graph; reference it from the manifest header and PR.
5. Run the focused mutation proofs and checks, review the diff, and update the existing draft PR.
