# Fast-suite review-round 7 plan

1. Make Turbo the execution authority for registered test tasks: discover tasks with Turbo dry-run (including package-local configuration), prepare the union of their resolved dependency tasks once through Turbo, and run all exact manifest files for that package in one scoped test task. Use `bun run test` only when no task exists and no package-local Turbo config would be bypassed.
2. Let Turbo/Bun preserve strict environment filtering, script options, literal environment values, package-local config, and one package lifecycle; keep JUnit execution-set and attributed-failure refusals.
3. Compare the fast task's runtime trace with its owning Turbo invocation. Cover package-local Turbo config, strict env filtering, exact file arguments, preparation failure, lifecycle status, and backslash-bearing script environment values. Record dirty-checkout refusal as `--fast`-only.
4. Run the maintenance suite, script typecheck/lint, ShellCheck, and real `--fast`; mutation-test the preparation failure guard; update the existing PR and wake after pushing.
