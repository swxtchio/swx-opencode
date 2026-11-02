# Fleet binary refresh

## Plan

- Resolve the live `swxtch` head and verify the selected source contains #343, #380, and #369; build from that clean source checkout so `local-release.sh` derives the release identity from its own `HEAD`.
- Record the install host architecture, current binary readbacks, database path, and running process identities. Immediately before build work on `vm14-001`, confirm `/home/byates/vm14-leases/ACTIVE` is absent, then use only a private `/tmp/fm-opencode-binary-refresh-*` scratch tree and the one-time captain exception.
- Run `./local-release.sh` on `vm14-001` with `OPENCODE_LOCAL_BIN` pointing into the scratch tree. Transfer the executable and matching FFF library into the install directory on `vm001`, verify both hashes, preserve the old executable, and replace the executable atomically.
- Read the installed `--version`, both prune help forms, `db path`, and `file -L` from fresh processes; compare the CLI and database readbacks with the pre-install results, compare process identities, and recheck the live source head before reporting success.
- Retain the source, build, transport, installation, readback, and scratch-removal evidence here. This is an artifact-only refresh from landed source; the task does not call for a production behavior change or unit-test source.

## Result

**Installed and CLI acceptance passed.** The executable at `/home/byates/.local/opencode-swxtch/bin/opencode` now reports `1.18.32-swxtch.765d66be`, built from the live `swxtch` commit `765d66be6b21e66cb79cbe025c521bdda22ee097`. The pre-install executable reported `1.18.32-swxtch.394afd08`; it required `--before` and its prune help did not list `--online`.

Evidence was collected on 2026-11-02 UTC. The default-branch and live-head values came from `gh-axi` repository and branch reads; commit inclusion came from ancestry checks in a clean checkout. The command outputs and statuses below came from fresh processes invoked through the absolute installed path.

## Source and build

- `swxtch` was the fork's default branch. Its live head at build and final recheck was `765d66be6b21e66cb79cbe025c521bdda22ee097`.
- The selected source contains #343 at `4e34ff6f1bacde154debbbe9b1264eab81f34e25`, #380 at `e3f113fe269a113a46d9218e80014bc1da564e80`, and #369 at `765d66be6b21e66cb79cbe025c521bdda22ee097`.
- The clean source checkout was on `vm14-001` (`aarch64`, Linux) and had the selected full commit at `HEAD`; the release script produced `1.18.32-swxtch.765d66be`. This is distinct from the task branch's plan commit `fe924180b3e11e3be9ebfb64c1a75029e9b565e4`.
- `./local-release.sh` completed its frozen dependency install, single-target Linux ARM64 build, smoke test, staged version check, and staged database-path check. An early shell diagnostic attempted `require('./package.json')` before entering the clone and printed `MODULE_NOT_FOUND` to the remote stderr stream; its stdout line appears as `pinned_bun=` in [build.log](build.log). The release script then read Bun 1.4.2 from the clean source checkout and completed successfully.
- The captain's one-time no-lease exception applied. Immediately before staging, `/home/byates/vm14-leases/ACTIVE` was confirmed absent on `vm14-001`; the required STARTING and FINISHED wake events were written. No lease was acquired or released, and nothing under the lease root was modified.
- The remote scratch tree `/tmp/fm-opencode-binary-refresh-pr383-765d66be6b` was removed after transfer; [scratch-cleanup.txt](scratch-cleanup.txt) records the removal.

## Artifact transport and installation

- The build-host and install-host architecture checks both reported `aarch64`. `file -L` identified the executable as ELF 64-bit ARM aarch64 and the FFF companion as an ARM aarch64 shared object.
- The executable SHA-256 was `f2ee621cfe0b752c3cd18c4c8e9e0c80b3359d9ded26ffdcaf43da797d420748` on the build host, in the transfer staging directory, and at the installed path.
- The matching library `libfff_c-0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42.so` had SHA-256 `0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42` on the build host, in transfer staging, and beside the installed executable. It matched the pre-existing companion, so the installed library bytes did not need replacement.
- `OPENCODE_LOCAL_BIN` was unset, selecting its default path `/home/byates/.local/opencode-swxtch/bin/opencode`. The prior executable was copied to `/home/byates/.local/opencode-swxtch/bin/opencode.1.18.32-swxtch.394afd08`; its hash matches the pre-install executable, and the backup still reports the old version and database path.
- The candidate passed version and database-path checks before installation. The final executable was placed with `install -m 755` and switched by a same-directory atomic `mv`; the installed version, database path, and SHA-256 then matched the candidate. No database-prune operation was run.
- The selected database path was `/home/byates/fm-llmrouter/data/opencode/opencode.db` both before and after installation. The environment selection was preserved.

## Installed CLI readbacks

- Pre-install `--version` returned `1.18.32-swxtch.394afd08` with status 0. Its `db prune --help` showed `--before` as required; `db prune --online --help` returned status 0 but did not list `--online`.
- Post-install `--version` returned `1.18.32-swxtch.765d66be` with status 0. `db prune --help` describes `--before` as optional and has no required marker; `db prune --online --help` lists `--online`. Both help commands returned status 0.
- `db path` returned `/home/byates/fm-llmrouter/data/opencode/opencode.db` with status 0 before and after. Post-install `file -L` returned status 0 and identified a Linux ARM64 executable.
- Complete verbatim outputs and individual exit statuses are retained in [preinstall](preinstall/) and [postinstall](postinstall/); binary and companion hashes are retained beside them.

## Sessions and scope

No process stop, restart, signal, database-prune operation, stock-opencode replacement, or compatibility/configuration edit was issued. The shell's `opencode` on `PATH` remained `/home/byates/.bun/bin/opencode`; runtime writes were limited to the requested fork binary, its recovery copy, and temporary staging files inside the fork install directory.

The read-only `/proc` snapshots matched executable processes by PID and start time. The pre-install snapshot had 21 identities; the post-install snapshot had 20, of which 18 matched the pre-install identities. Three pre-install identities were absent and two additional identities were present afterward; the cause of those process exits is not established, so this report does not claim exact one-to-one process continuity. No session-management action was part of the operation. The processes still observed after replacement were running the old executable inode, as expected for processes left running across an atomic pathname replacement.

This was an artifact-only deployment from already-landed source. No unit-test source was added and no test suite was run; the requested acceptance check was the real installed executable's version, help, database-path, and architecture readback. No live pruning behavior or shell cancellation behavior was exercised.

## Evidence index

- [source-and-host.txt](source-and-host.txt) records source selection, architecture, and landed-commit inclusion.
- [build.log](build.log) records the remote `local-release.sh` build and its smoke/readback output.
- [artifact-transport.txt](artifact-transport.txt) records build-host and install-host hashes for the executable and FFF companion.
- [installation.txt](installation.txt) records the previous binary backup, atomic replacement, and database-path readbacks.
- [operation-log.txt](operation-log.txt) records the operation scope and process-control boundary.
- [preinstall](preinstall/) and [postinstall](postinstall/) contain complete CLI outputs, statuses, `file -L`, database paths, and hashes.
- [process-continuity.txt](process-continuity.txt) records the process-snapshot comparison without publishing PID values.
- [scratch-cleanup.txt](scratch-cleanup.txt) records removal of the remote scratch tree.
