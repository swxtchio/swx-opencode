# Fleet binary refresh

## Plan

- Resolve the live `swxtch` head and verify the selected source contains #343, #380, and #369; build from that clean source checkout so `local-release.sh` derives the release identity from its own `HEAD`.
- Record the install host architecture, current binary readbacks, database path, and running process identities. Immediately before build work on `vm14-001`, confirm `/home/byates/vm14-leases/ACTIVE` is absent, then use only a private `/tmp/fm-opencode-binary-refresh-*` scratch tree and the one-time captain exception.
- Run `./local-release.sh` on `vm14-001` with `OPENCODE_LOCAL_BIN` pointing into the scratch tree. Transfer the executable and matching FFF library into the install directory on `vm001`, verify both hashes, preserve the old executable, and replace the executable atomically.
- Read the installed `--version`, both prune help forms, `db path`, and `file -L` from fresh processes; compare the CLI and database readbacks with the pre-install results, confirm running process identities are unchanged, and recheck the live source head before reporting success.
- Retain the source, build, transport, installation, readback, and scratch-removal evidence here. This is an artifact-only refresh from landed source; the task does not call for a production behavior change or unit-test source.
