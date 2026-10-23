# Abbe's jail rig, `compact` mode: PR #354 candidate

On 2026-10-23, I ran Abbe's own rig as written on `vm001` (Linux arm64, Docker 29.8.0). This is the rig doc's
`compact` acceptance mode for swxtchio/swx-abbe#1953.

## Inputs

- **Rig doc.** `/home/byates/fm-abbe/data/mvp-b16-10/jail-v2-egress-rig.md` was read with SHA-256 `5d5f7ce5…d99e`
  (`rig-doc.sha256`). The copy as read is `jail-v2-egress-rig.md.read`.
- **Probe.** `jail_v2_egress_probe.py` is its §2 probe, saved verbatim.
- **swx-abbe checkout.** A private clone at the doc's measured head `1c6b3582dab3ecef9081dca516102e4c9786f7f4`,
  kept under the ignored `logs/compact-rig/abbe`. Its `components/orch/services/opencode-variant` is used as it is.
- **OpenCode.** No release carries #354 yet. The image installs the unpublished native arm64 candidate of
  `c0182d4d` (`../arm64/`), version `1.18.32-swxtch.c0182d4d`. Commits after `c0182d4d` change only tests and docs,
  so its production source equals the PR head's.
- **Image build.** It follows the doc's §1 with one change, `Dockerfile.candidate.diff`: a `COPY` of the candidate
  archive replaces the release download. This is the same one-change diff release-cut-4 used.
  - The build was `--no-cache`. `image-build.log` shows the Dockerfile's own digest check printing
    `/tmp/opencode-linux-x64.tar.gz: OK` for `f3959f78…f7a4` and its `--version` check passing.
  - `image-bytes.sha256` shows the image's executable (`36fa7044…`) and sidecar (`0459657e…`) equal to the
    candidate's.

## Run (`rig-run.log`)

The doc's §3 command was run from the checkout root with the doc's environment, `TMPDIR` set to the ignored
`logs/compact-rig/rig-tmp`, and the image above:

```
python3 jail_v2_egress_probe.py v1-control v2 v2-named compact
```

It exited `0`, and every mode printed `RESULT: PASS`. `compact` met all four of the doc's §5 conditions:
1. A `kind=turn` request reached the provider, and `wait` reported the turn `completed`.
2. The compact call answered `204`.
3. The provider received `REQ3 … kind=summary transcript=yes`. That is the summary request, and it carries the turn.
4. The history holds `session.next.compaction.ended` with `reason` `manual`, whose text is the provider's summary.

The session also recorded `session.next.titled` from the `kind=title` request.

The doc's red baseline on `cd3f0ce2` was a compact answering 503, no summary request and no compaction events.

`v2` and `v2-named` reached the provider, and their waits completed. The doc records `failed` on `cd3f0ce2`.

## Not covered

- This run is arm64 only.
- It ran against the candidate, not a published release. The doc's §1 names a release tag. The publication run
  belongs to the release cut that carries #354.
