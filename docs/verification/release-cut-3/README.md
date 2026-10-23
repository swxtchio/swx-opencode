# Raw release-cut evidence: 2026-10-23

This directory keeps the original native, publication and delivery observations for `v1.18.32-swxtch.774d90c7`, built from source `774d90c7fb02b9f0962620971630a4f09c1a886b`, through [evidence PR #349](https://github.com/swxtchio/swx-opencode/pull/349). Files were copied byte-for-byte from the ignored execution artifacts. See the root `RELEASE-CUT-3-EVIDENCE.md` for the commands and how to read them.

- `x64/`: Actions run 39983517672's candidate probe JSON (`probe.json`, originally `candidate-proof/evidence.json`), producer server log, build/probe logs, provenance, host/Bun/source receipts, and the full run log (`actions-full.log`).
- `arm64/`: the scripts executed on `vm14-001` (`arm-build.sh` under `supervise.sh`), the complete execution log, ancestry and `release-probe-target.ts --candidate` identity receipts, install/build/probe logs, source cleanliness receipts, archive/executable/sidecar digests and the native probe JSON with its producer log.
- `lease/`: refused acquisition attempts while other owners held the lease, atomic acquisition with owner metadata, the task-process scan and the owner-matched release.
- `inputs/`: the actual catalog snapshot each builder consumed. They were fetched independently and differ.
- `tests/`: the focused small-model run and the #347 core config run at the release source.
- `publication/`: creation output, `gh-axi release view --full`, tag-specific and latest API output, tag resolution, the exact body comparison, the published checksum companions and the downloaded-byte verification.
- `delivery/`: the notice appended to the Abbe and SaaS task wake logs, the append read-back receipts, and the supplemental peer-message send log and sender-owned outbox records.

The release archives themselves are the four published assets; this directory does not store binaries.
