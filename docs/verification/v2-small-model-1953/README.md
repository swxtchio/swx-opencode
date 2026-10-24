# Candidate jail compaction run: PR #354 head `c0182d4d`

This records review round 2's pre-merge candidate rig run for swxtchio/swx-abbe#1953. It ran on 2026-10-23 on
`vm001` (Linux arm64, Docker 29.8.0) and follows release-cut-4's jail gate (`../release-cut-4/README.md`).

## Candidate archive (`arm64/`)

`arm64/build.sh c0182d4d8c39b52cb954579fd61e89c7e5fab236` performs these steps:
1. Clones this worktree at that commit into the ignored `logs/compact-rig/source`.
2. Installs with the frozen lockfile.
3. Builds the native linux-arm64 single target as `1.18.32-swxtch.c0182d4d`, with release-cut-4's arm64 catalog
   snapshot.
4. Packs the executable and its FFF sidecar into `opencode-linux-arm64.tar.gz`.

`build-execution.log` ends with `ARM-BUILD-PASS`. `selected-bytes.sha256` gives these digests:
- the archive: `f3959f78…f7a4`;
- the executable: `36fa7044…e08a`;
- the sidecar: `0459657e…be42`.

The archive is not published and not stored here.

## Jail image (`jail/`)

- The image was built from a private clone of swx-abbe at `be3692bea1183a820c44b20f8443a826a75102a2`, the head
  release-cut-4's gate used, kept under the ignored `logs/compact-rig/abbe`.
- The rig doc it follows, `/home/byates/fm-abbe/data/mvp-b16-10/jail-v2-egress-rig.md`, read with SHA-256
  `73b543ef…019f`, is unchanged since release-cut-4.
- `Dockerfile.candidate.diff` is byte-identical to release-cut-4's: a `COPY` of the candidate archive replaces the
  release download.
- `image-build.log` shows the Dockerfile's own digest check printing `OK` and its `--version` check passing.
- `image-bytes.sha256` shows the image's `/usr/local/bin/opencode` and sidecar equal to the candidate's.

## Rig run (`jail/rig-run.log`)

`jail_v2_compact_probe.py` is release-cut-4's verbatim copy of the rig doc's probe, plus one mode;
`probe-extension.diff` is the whole difference. The `v2-compact` mode drives the V2 calls:
1. creates the session with the egress model as both `model` and `smallModel`;
2. prompts and waits;
3. sends `POST /api/session/{id}/compact`;
4. reads the durable history.

It passes only when all three hold:
- compact answers `204`;
- a `session.next.compaction.ended` event holds the fake provider's own content (`Proposal prepared for review.`);
- the provider log carries the egress signal.

`python3 jail_v2_compact_probe.py v1-control v2 v2-named v2-compact` exited `0`, and every mode printed
`RESULT: PASS`:
- `v2` and `v2-named` reached the provider, as in release-cut-4. Their waits now resolve `completed`.
- In `v2-compact`, the create echoed the `smallModel`, and the wait completed. Compact answered `204`, and the history
  gained `session.next.compaction.started` and `.ended` with `reason: "manual"` and the provider's text. The provider
  logged a third request, `REQ3 /v1/chat/completions model=accounts/fireworks/models/deepseek-v4p1-flash`, which is
  the summary request, distinct from the turn's.
- The session's title request received the fake provider's first reply, a tool call with no text. The runner dropped
  that title as designed, so `title_events` is empty. This is outside the pass signal.

## Not covered here

- Abbe's own compact mode for the rig doc is being written by its crew and arrives by relay. Its run is the final
  acceptance and is still pending.
- This run is arm64 only.
