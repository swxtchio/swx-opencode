# Raw release-cut evidence: 2026-10-25 (832df553)

This directory preserves the native candidate, strengthened Abbe jail, focused test, publication, and downloaded-byte observations for source `832df553518d8bb581952433c72495a472da8c5d`, published as `v1.18.32-swxtch.832df553` through [evidence PR #367](https://github.com/swxtchio/swx-opencode/pull/367). The root `RELEASE-CUT-6-EVIDENCE.md` links the observations to the release gates.

- `identity/` records the source-derived identity and ancestry checks, including the older source adopted in Abbe #1951.
- `inputs/` keeps the exact models.dev snapshots consumed by each native builder.
- `arm64/` keeps the source resolver receipt, selected-source ancestry, frozen install and build logs, native probe JSON and server log, provenance, archive contents, and executable/sidecar digests. `arm-build.sh` and `supervise.sh` are the VM14 lap scripts.
- `x64/` keeps the native Actions candidate provenance, proof JSON, server/build/probe logs, full run log, and the candidate artifact identity. The original Actions artifact is run 40224713450, artifact 12610986098; the published release assets preserve the candidate archive bytes.
- `jail/` keeps the current consumer source hashes, the Dockerfile candidate diff, the `_brain_rig` probe, the V2 request/history/wait evidence for both modes, the V1 control and manual-compaction evidence, cleanup observations, and the two scratch-harness preparation failures before the passing lap. The public provider-request projection retains only the `propose_netlist` definition and matching request/tool-result fields; hashes identify the full provider logs retained in the task-authorized VM14 root. `consumer-boundary.txt` records the remaining Abbe SessionPool compatibility observation.
- `tests/` keeps each whole-file package-local test log, the frozen workspace install, the candidate jail acceptance result, and the first dependency-setup attempt that was resolved by the frozen install.
- `lease/` keeps the VM14 lease-owner records and release-lap summaries.
- `publication/` keeps the pre-publish state, release creation record, tag and Latest API read-back, exact notes-body comparison, downloaded companions, archive equality checks, and extracted-byte comparisons.

The release archive binaries are not committed. The arm64 candidate remains in the authorized VM14 task root, the x64 candidate remains available from its Actions artifact until its recorded expiry, and the published release holds both public archive/companion pairs. Fresh downloaded archives and extracted executables were removed after byte comparison; the worktree keeps the catalogs, logs, companions, hashes, and provenance needed to verify the result.
