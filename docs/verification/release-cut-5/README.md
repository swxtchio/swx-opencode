# Raw release-cut evidence: 2026-10-24 (d7f78881)

This directory keeps the original native, jail-rig and publication observations for `v1.18.32-swxtch.d7f78881`, built from source `d7f78881560aa83e9390f0231562fa79639eacd9`, through [evidence PR #357](https://github.com/swxtchio/swx-opencode/pull/357). Files were copied byte-for-byte from the ignored execution artifacts. See the root `RELEASE-CUT-5-EVIDENCE.md` for the commands and how to read them.

- `x64/`: Actions run 40052511379's candidate probe JSON (`probe.json`, originally `candidate-proof/evidence.json`), producer server log, build/probe logs, provenance, host/Bun/source receipts, and the full run log (`actions-full.log`).
- `arm64/`: the scripts executed on `vm14-001` (`arm-build.sh` under `supervise.sh`), the complete execution log, ancestry and `release-probe-target.ts --candidate` identity receipts, install/build/probe logs, source cleanliness receipts, archive/executable/sidecar digests, the transport digest, and the native probe JSON with its producer log.
- `lease/`: each of the three laps' atomic acquisition with owner metadata, its task-process scan and its owner-matched release, plus the scan script (`scan.py`) and lap 3's Docker leftover check. Lap 1's scan predates `scan.py`; its single entry is that scan's own ssh shell, which `scan.py` then excluded.
- `inputs/`: the actual catalog snapshot each builder consumed. They were fetched independently and happen to be identical.
- `tests/`: the focused small-model run, the `packages/core` run of #347's, #350's and #354's tests, and the `packages/opencode` run of #354's HTTP, OpenAPI and event-manifest tests, at the release source.
- `jail/`: Abbe's jail V2 egress rig on the unpublished arm64 candidate, in all four modes. It includes the probe saved verbatim from the rig doc, and the one-change Dockerfile diff that replaces the release download with the candidate archive. It also has the image build log, the image's executable and sidecar digests, `rig.sh` under `supervise-rig.sh`, and the passing run (`rig-run.log`). The two earlier laps that stopped before any mode ran are kept as found in `lap1-environment-failure/` and `lap2-cached-build-stop/`.
- `publication/`: the pre-publish checks, creation output, `gh-axi release view --full`, tag-specific and latest API output, tag resolution, the exact body comparison, the published checksum companions and the downloaded-byte verification.

The release archives themselves are the four published assets; this directory does not store binaries.
