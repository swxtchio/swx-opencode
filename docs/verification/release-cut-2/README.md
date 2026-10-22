# Raw release-cut evidence: 2026-10-22

This directory preserves the original native and publication observations for `v1.18.32-swxtch.bce37c27`, binary source `bce37c276f4070e3c9d343baa61ce5f35ca38dc9`, through [evidence PR #345](https://github.com/swxtchio/swx-opencode/pull/345). The original ignored execution artifacts were copied byte-for-byte; the binary source and published assets were not rebuilt or replaced. See the root `RELEASE-CUT-2-EVIDENCE.md` for the executed commands, interpretation and firstmate-owned Abbe route.

- `arm64/`: native VM14 probe JSON, producer server log, build/install/probe/test logs and source/digest receipts.
- `x64/`: native Actions probe JSON, producer server log, build/probe logs, provenance and runtime/source receipts; `actions-run.log` retains the original truncated wrapper view, while `actions-full.log` preserves the complete log that wrapper saved separately.
- `lease/`: atomic acquisition, corrected owner-box metadata, task-process cleanup and owner-matched release receipts.
- `publication/`: complete original `gh-axi release view --full`, tag-specific/latest API output and tag resolution.
- `delivery/`: original Abbe pre-type refusal, SaaS send/recovery observations and the confirmed sender-owned SaaS outbox receipt. The root evidence report records firstmate's completion route separately from direct delivery receipts.
- `inputs/models.dev.json`: the actual consumed catalog bytes. The independently fetched ARM64 and x64 snapshots were byte-identical; each target's own digest receipt is retained with its evidence.

`probe.json` preserves the producer-emitted wait registration/resolution records, held-phase observations, durable event IDs and restart replay; `opencode-server.log` is the original corresponding producer log. API/read-back files are original wrapper output, including escaped one-line bodies and any wrapper truncation markers. They have not been reformatted into a different apparent API response. Release archives remain the four published assets rather than committed binary payloads.

`publication/downloaded-byte-verification.txt` is a new round-1 recheck of the retained downloaded assets: both companions passed, and archives, executables and sidecars compared equal to their native-tested candidates before the displayed hashes were recorded. It is a repeated byte check of the existing release, not a rebuild or a new native runtime execution.
