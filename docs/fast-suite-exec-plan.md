# Execution-time observer plan

Scope: the round-3 order for [#324](https://github.com/swxtchio/swx-opencode/issues/324), on the existing draft [#329](https://github.com/swxtchio/swx-opencode/pull/329).

- Replace retrospective path/header reconstruction with Linux ptrace exec-stop observation. Capture the loaded executable identity and kernel-expanded argv from procfs before resuming each process; follow forks, vforks, and threads, and require an explicit completed observation receipt.
- Classify Bun/Node launches from those live records. Working-directory and script cleanup after execution must not invalidate otherwise valid runs or substitute replacement script bytes for the interpreter that actually ran.
- Exercise overwritten Bun/Node scripts, repointed symlinks, and successful children in subsequently deleted chdir/fchdir working directories through the committed real-runner fixture. Demonstrate the lifecycle controls fail against the reviewed implementation and interpreter cells fail with live observation removed.
- Restore the complete focused runtime matrix, run mechanical checks, update the observation claims and PR evidence, commit/push the same branch, verify its live draft head, and file the ordered wake.
