# Position-aware launch parsing plan

Scope: the round-5 argv-position family for #324 at reviewed head `41864ee458`.

- Use a shared Bun/Node argv parser at the PATH shim and execution audit. Respect value-taking options, script/eval boundaries, and `--`; classify worker/test modes only at option positions.
- Exercise PATH and absolute Bun launches with `--test-worker` consumed as an option value, plus ordinary Node/Bun code receiving flag-looking user arguments. Preserve real test-launch refusals and native parallel-worker behavior.
- Demonstrate baseline and targeted guard-removal reds for both fail-open and fail-closed directions, then restore the full focused matrix and mechanical checks.
- Record results in the existing draft PR, commit/push only its branch, verify the live head, and file done.
