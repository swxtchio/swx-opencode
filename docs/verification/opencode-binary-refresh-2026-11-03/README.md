# Fleet binary refresh — 2026-11-03

## Plan

- Re-resolve the live `swxtch` head, verify its captured-usage gate at the source boundary, and build from a clean checkout pinned to that full SHA.
- Capture the local `vm001` executable's complete pre-install CLI readbacks, architecture, hashes, database selection, and running-process identities; stop before replacement if the installed gate already passes.
- Run exactly one `fm-buildbox-build.sh --host vm14-001 --architecture arm64 --workdir /tmp -- ...` command for source staging, the production `local-release.sh` build, and focused capture regressions; keep all build, cache, HOME, temporary, and test-fixture writes under one private remote task directory.
- Compare the executable and FFF sidecar hashes at build output, local `GOTMPDIR` transport staging, and `vm001` installation staging; verify a unique predecessor backup before the same-directory atomic executable replacement.
- Capture post-install CLI and process evidence, recheck the live source head, read the lease's acquisition and exact-generation release records, and remove only this task's remote scratch and local transport staging.
- Retain complete command outputs, stderr, exit statuses, hashes, lease history, focused-test results, process comparison, backup and rollback instructions, and cleanup readbacks here; add the PR-numbered `CHANGESET.md` entry and hand off the same draft PR.

## Result

The executable at `/home/byates/.local/opencode-swxtch/bin/opencode` now reports `1.18.32-swxtch.21b47f6d`, the live `swxtch` source SHA `21b47f6db3ce1f48ccca6a2aba153bd0405be585`. Its installed SHA-256 is `cec507b89cb00d0ea752b784d4e61d3fb5e97f64b5699b3ac93350c9ab0b873d`; the adjacent `libfff_c-0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42.so` has SHA-256 `0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42`.

The pre-install binary reported `1.18.32-swxtch.765d66be`, SHA-256 `f2ee621cfe0b752c3cd18c4c8e9e0c80b3359d9ded26ffdcaf43da797d420748`; its prune help omitted both usage-capture options. The installed absolute path now returns status 0 for `--version`, `db prune --help`, `db prune --online --help`, `db path`, and `file -L`; both help forms define `--usage-capture-status`, `--usage-capture-session`, `--online`, and `--reserve-bytes`, while `--before` is optional without a required marker. The selected database path remained `/home/byates/fm-llmrouter/data/opencode/opencode.db`.

The previous executable remains at `/home/byates/.local/opencode-swxtch/bin/opencode.1.18.32-swxtch.765d66be.refresh-385-20261103T154122Z`; its bytes, version, database path, and Linux ARM64 ELF identity were verified before replacement. The replacement used a same-directory `mv -f` after a verified predecessor copy; the rollback command is recorded in `installation.txt`. The existing content-addressed FFF library already matched the candidate and was retained.

This evidence is for swxtchio/swx-opencode#385. It carries no closing action for #371; Firstmate owns that closure after independently verifying both live sides.

The single leased helper acquired `vm14-001` as ARM64, ran the production `local-release.sh` path from a clean checkout pinned to the full source SHA, and released the exact lease generation with a retained `release.json`. The helper's terminal status was `1`: `local-release.sh` produced the compiled ELF and matching FFF sidecar, then refused its candidate copy because this payload set `OPENCODE_DB_PATH` to `$TASK_ROOT/data/opencode.db` while `Database.path()` selected `$TASK_ROOT/data/opencode/opencode.db` from `XDG_DATA_HOME`. The source reads `Flag.OPENCODE_DB` and `Global.Path.data`; no production behavior was changed. The compiled output's source version and ARM64 identity were verified, its bytes were transferred to GOTMPDIR and checked against the build-output hashes, and it passed candidate and installed CLI acceptance. The wrapper failure is retained rather than described as a successful `local-release.sh` exit.

The required focused source command passed 19 tests with zero failures, 83 filtered out, and 100 expectations. It exercised the existing caller-confirmed capture regressions against producer-created stores; no test source or production source was changed.

The read-only process comparison used `(PID, /proc stat start time)` and loaded executable `(device, inode, size, SHA-256)` identities. The later post-install snapshot found 64 matching processes still running the predecessor image and four additional readable processes running the new image; their causes are unestablished. `/proc` access was partial, so this report makes no claim that every Session survived. The immediate install record retained a 64/64 process count; its temporary post snapshot was replaced by the later 15:51 readback used for the detailed comparison. No process was stopped, restarted, signalled, or pruned.

The unique remote build scratch, installation staging paths, and task-prefixed GOTMPDIR transport/helper files have absence readbacks in `scratch-cleanup.txt`; the versioned predecessor, installed executable, matching FFF sidecar, and immutable lease release records were retained.

## Evidence index

- [source-and-host.txt](source-and-host.txt) records default-branch selection, the full source pin, landed gate inspection, and host architecture; `source-head-*.stdout` files retain the live-ref reads.
- [preinstall](preinstall/) and [postinstall](postinstall/) retain complete stdout, stderr, status, architecture, `db path`, and command-resolution readbacks.
- [build](build/) retains the exact helper argv/payload, terminal status, helper output, complete `local-release.sh` stdout/stderr/status, clean-source checks, build-output manifest, and payload qualification.
- [candidate](candidate/) retains build-output and GOTMPDIR transfer hashes, candidate CLI acceptance, and staging history.
- [artifact-transport.txt](artifact-transport.txt) compares executable and sidecar hashes at build output, transfer staging, installation staging, and final install.
- [tests](tests/) retains the exact focused command, complete output, exit status, and producer-store cleanup result.
- [lease.txt](lease.txt) and [lease-observation.json](lease-observation.json) record the acquired holder identity match and terminal release history without publishing process IDs.
- [installation.txt](installation.txt), [installation.json](installation.json), [installation-operations.json](installation-operations.json), and [current-install-observation.json](current-install-observation.json) record predecessor backup, replacement command, acceptance, and rollback.
- [operation-log.txt](operation-log.txt) records the ordered installation operations, build qualification, and no-process-control boundary.
- [process-continuity.txt](process-continuity.txt) records the process comparison and its partial-scan qualification.
- [scratch-cleanup.txt](scratch-cleanup.txt) records remote, install-staging, and GOTMPDIR absence readbacks.
