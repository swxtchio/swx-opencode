# Raw release-cut evidence: 2026-10-23 (cd3f0ce2)

This directory keeps the original native, jail-rig, publication and delivery observations for `v1.18.32-swxtch.cd3f0ce2`, built from source `cd3f0ce2d5e55944df41fcf43a167f77e40c8483`, through [evidence PR #351](https://github.com/swxtchio/swx-opencode/pull/351). Files were copied byte-for-byte from the ignored execution artifacts. See the root `RELEASE-CUT-4-EVIDENCE.md` for the commands and how to read them.

- `x64/`: Actions run 40015678366's candidate probe JSON (`probe.json`, originally `candidate-proof/evidence.json`), producer server log, build/probe logs, provenance, host/Bun/source receipts, and the full run log (`actions-full.log`).
- `arm64/`: the scripts executed on `vm14-001` (`arm-build.sh` under `supervise.sh`), the complete execution log, ancestry and `release-probe-target.ts --candidate` identity receipts, install/build/probe logs, source cleanliness receipts, archive/executable/sidecar digests, the transport digest, and the native probe JSON with its producer log.
- `lease/`: atomic acquisition with owner metadata, the task-process scan and the owner-matched release.
- `inputs/`: the actual catalog snapshot each builder consumed. They were fetched independently and happen to be identical.
- `tests/`: the focused small-model run, and the #347 core config plus #350 session-runner run, at the release source.
- `jail/`: Abbe's jail V2 egress rig on the unpublished arm64 candidate. Includes the probe saved verbatim from the rig doc, the one-change Dockerfile diff that replaces the release download with the candidate archive, the image build log, the image's executable and sidecar digests, and the rig run. It also keeps the diagnostic variant of the probe and its run, which print the V2 tool events behind #352.
- `publication/`: creation output, `gh-axi release view --full`, tag-specific and latest API output, tag resolution, the exact body comparison, the published checksum companions and the downloaded-byte verification.
- `delivery/`: the notice, the SaaS wake-log append receipt, the record of the unestablished Abbe B-16.10 wake log, firstmate's archived relay that delivered the Abbe handoff, and the supplemental peer-message send log and sender-owned outbox record.

The release archives themselves are the four published assets; this directory does not store binaries.
