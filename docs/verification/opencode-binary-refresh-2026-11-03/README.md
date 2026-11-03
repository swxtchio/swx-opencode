# Fleet binary refresh — 2026-11-03

## Plan

- Re-resolve the live `swxtch` head, verify its captured-usage gate at the source boundary, and build from a clean checkout pinned to that full SHA.
- Capture the local `vm001` executable's complete pre-install CLI readbacks, architecture, hashes, database selection, and running-process identities; stop before replacement if the installed gate already passes.
- Run exactly one `fm-buildbox-build.sh --host vm14-001 --architecture arm64 --workdir /tmp -- ...` command for source staging, the production `local-release.sh` build, and focused capture regressions; keep all build, cache, HOME, temporary, and test-fixture writes under one private remote task directory.
- Compare the executable and FFF sidecar hashes at build output, local `GOTMPDIR` transport staging, and `vm001` installation staging; verify a unique predecessor backup before the same-directory atomic executable replacement.
- Capture post-install CLI and process evidence, recheck the live source head, read the lease's acquisition and exact-generation release records, and remove only this task's remote scratch and local transport staging.
- Retain complete command outputs, stderr, exit statuses, hashes, lease history, focused-test results, process comparison, backup and rollback instructions, and cleanup readbacks here; add the PR-numbered `CHANGESET.md` entry and hand off the same draft PR.
