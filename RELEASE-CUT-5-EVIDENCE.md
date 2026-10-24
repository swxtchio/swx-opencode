# Release-cut execution evidence: d7f78881

Recorded 2026-10-24 for task `release-cut-5` and [draft PR #357](https://github.com/swxtchio/swx-opencode/pull/357). This cut ships [#354](https://github.com/swxtchio/swx-opencode/pull/354), V2 small-model parity for session create, runner titles and compaction (swxtchio/swx-abbe#1953), on top of [#350](https://github.com/swxtchio/swx-opencode/pull/350) and [#347](https://github.com/swxtchio/swx-opencode/pull/347). Abbe needs it to restore DEV chat (swxtchio/swx-abbe#2030). It supersedes `v1.18.32-swxtch.cd3f0ce2`. The binary source is immutable `d7f78881560aa83e9390f0231562fa79639eacd9`; the pickup identity and ancestry record is in `plans/release-cut-5.md`. `RELEASE-CUT-5-NOTES.md` is the complete published release body. No production or release/probe tooling was changed. Raw observations are under `docs/verification/release-cut-5/`.

## Pickup decisions

- **#353.** The brief named swxtchio/swx-opencode#353 as a merged change to carry. #353 is an open issue (online session reclamation), not a pull request, and nothing on `swxtch` resolves it. This release does not claim it, and `CHANGESET.md` has no #353 entry. The discrepancy was reported on the task wake log at pickup.
- **Version.** The brief asked for a version bump. No file holds the release version: `local-release.sh` derives it as `<packages/opencode/package.json version>-swxtch.<source sha8>`, so the version is `1.18.32-swxtch.d7f78881`. No earlier cut changed `package.json`, and this one does not either.
- **Write scope.** The brief authorizes external writes only for the rig's scratch on `vm14-001` under the lease. The arm64 candidate the rig tests was built in that same root, `/home/byates/vm14-builds/opencode-release-d7f78881`, under the same lease. Nothing was written into another firstmate home. The notice to Abbe and SaaS goes through firstmate; this task's own wake log carries it.

## Native x64 (GitHub Actions)

```sh
gh workflow run opencode-arm-release-x64.yml -R swxtchio/swx-opencode --ref swxtch -f source_commit=d7f78881560aa83e9390f0231562fa79639eacd9 -f release_tag=v1.18.32-swxtch.d7f78881 -f version=1.18.32-swxtch.d7f78881 -f candidate=true
```

[Run 40052511379](https://github.com/swxtchio/swx-opencode/actions/runs/40052511379) succeeded. Its `release-candidate` job passed, and `release-proof` was skipped as intended for an unpublished candidate. The dispatch ref was `swxtch` at the release source, so the harness revision equals the source revision. Provenance records:

- `x86_64` and Bun `1.4.2`;
- the selected lockfile digest;
- catalog digest `146250a1…2fe74`;
- clean source before and after the build, with no overlays.

[Artifact 12543403152](https://github.com/swxtchio/swx-opencode/actions/runs/40052511379/artifacts/12543403152) has transport ZIP digest `99f133d2…5a8d` and expires on 2026-11-07. It was downloaded through the artifacts API. Its companion passed locally, and local extraction reproduced the provenance's executable and sidecar digests.

## Native arm64 (vm14-001)

The lease README (convention v3) was read before admission. `ACTIVE` was free. Lap 1's atomic `mkdir` succeeded at 2026-10-24T02:04:23Z. Its `lease.json`, written by tmp file and `mv`, names:

- home `/home/byates/fm-llmrouter` and task `release-cut-5`;
- owner box `vm001` and owner PID `3966212`, which is this session's Claude process.

`supervise.sh` ran `arm-build.sh` under the root and touched the lease heartbeat every 30 seconds while work lived. `arm-build.sh` is cut-4's, with only the root, source, version and ancestry list changed (#354's merge added). It redirects HOME, XDG, the Bun/npm caches and TMPDIR beneath the root. It then:

1. clones the fork and checks out the source detached;
2. checks the Bun pin and all ancestry;
3. runs the source's `release-probe-target.ts --candidate` with a synthetic dispatch event, which printed the expected tag, source, version and Bun `1.4.2`;
4. installs with `bun install --frozen-lockfile --os=linux --cpu=arm64`;
5. fetches and hashes the catalog, then builds with `OPENCODE_VERSION`, an absolute `MODELS_DEV_API_JSON` and `OPENCODE_BUILD_DIST`, using `build.ts --single --skip-install`;
6. packages exactly `opencode` and its sidecar, validates the companion, extracts the archive and checks `--version`;
7. runs the source's unchanged `release-probe.ts arm64` on the extracted executable.

The build ran from 02:04:34Z to 02:05:52Z and printed `ARM-BUILD-PASS`. Source status was empty before the build, after install, after the build and after the probe. The transport bundle's SHA-256 is `ed72ba6b…126e`, and it matched after the copy. The archive companion passed again after transport, and local extraction reproduced the executable and sidecar digests.

## Focused source tests

These ran on the task's arm64 development host (`vm001`), at the release source plus the plan-only commit, with Bun `1.4.2` after a frozen install.

- From `packages/opencode`, the focused command in `plans/small-model-verification.md` passed 59 tests, with 634 filtered out and zero failures.
- From `packages/core`, these files passed 207 tests with zero failures:
  - #347's `test/config/config.test.ts`, `test/config/environment.test.ts`, `test/config/provider.test.ts` and `test/location-layer.test.ts`;
  - #350's `test/session-runner.test.ts` and `test/session-runner-recorded.test.ts`;
  - #354's `test/session-run-coordinator.test.ts` and `test/session-prompt.test.ts`. #354 also changed `environment`, `location-layer` and both `session-runner` files.
- From `packages/opencode`, #354's `test/server/httpapi-session.test.ts`, `test/server/httpapi-public-openapi.test.ts` and `test/event-manifest.test.ts` passed 102 tests with zero failures.

No test was added or rewritten; these unchanged tests keep their existing roles.

## Abbe jail V2 egress rig (publication gate)

The rig is `/home/byates/fm-abbe/data/mvp-b16-10/jail-v2-egress-rig.md` (SHA-256 `5d5f7ce5…d99e` as read), which now includes the `compact` mode for #1953. It ran on `vm14-001` (arm64, snap Docker 29.8.0) against swx-abbe `1c6b3582dab3ecef9081dca516102e4c9786f7f4`, the doc's measured head. That tree was cloned into this worktree's ignored `logs/` and copied to the root with `git archive`, because `vm14-001` has no GitHub credentials. Its `components/orch/services/opencode-variant` is unchanged at swxtchio/swx-abbe#1951's current head `9639cfb0`.

The rig doc's build downloads the published asset, and that asset did not exist yet. The doc's §1 build was followed with its arm64 asset-name `sed`. `jail/Dockerfile.candidate.diff` records the one further change: a `COPY` of the native-tested arm64 archive replaces the `curl` download. The probe was saved verbatim from the doc as `jail/jail_v2_egress_probe.py`.

Three laps ran. Each took the lease afresh and released it with `release-lease.sh`, after a process scan, before the next began.

1. **Lap 1** (with the build, 02:04:23Z–02:07:40Z). The rig stopped before any mode ran with `ModuleNotFoundError: No module named 'pytest'`. `rig.sh` had redirected HOME to the root, which hid the host's user-site `pytest`. Kept as found in `jail/lap1-environment-failure/`.
2. **Lap 2** (02:07:53Z–02:08:47Z). Without the redirect, the image build was fully cached, so its install step, and with it the Dockerfile's digest check, did not run. `rig.sh`'s own check for the digest `OK` line then stopped the lap before any mode. Kept as found in `jail/lap2-cached-build-stop/`.
3. **Lap 3** (02:08:54Z–02:10:58Z). `rig.sh` added `--no-cache`. The Dockerfile's own digest check (`OPENCODE_ARCHIVE_SHA256=ce5d2122…71f8`) printed `/tmp/opencode-linux-arm64.tar.gz: OK`, and its `--version` check passed. `image-bytes.sha256` shows the image's `/usr/local/bin/opencode` and sidecar equal the native-tested executable and sidecar.

In lap 3 the probe ran as `python3 jail_v2_egress_probe.py v1-control v2 v2-named compact` with the doc's environment. It exited `0`, and every mode printed `RESULT: PASS`:

- `v1-control`, `v2` and `v2-named`: the fake provider logged `REQ1` and `REQ2 /v1/chat/completions model=accounts/fireworks/models/deepseek-v4p1-flash stream=True`. These are #1952's acceptance cases, and they still pass.
- `compact`, #1953's acceptance case, met all four of the doc's §5 conditions:
  1. a `kind=turn` request reached the provider and `wait` reported `completed`;
  2. the compact call answered `204`;
  3. the provider received `REQ3 … kind=summary transcript=yes`;
  4. the history recorded a manual `session.next.compaction.ended` whose text is the provider's summary.
  
  The provider also logged a `kind=title` request, and the history recorded `session.next.titled`. On `cd3f0ce2` the doc records the compact call answering 503.

After lap 3, `docker-leftovers-lap3.txt` shows no candidate image, rig network or container left on `vm14-001`.

Both V2 turn modes' waits resolved `completed` with a text answer, unlike cut-4's `failed` on `Unknown tool: propose_netlist`. This does not show that #352 is fixed. The rig's default provider gives its `propose_netlist` tool call only to the first request it receives, and it does not label requests. #354 now sends a title request for a new V2 session as well, and the turn's history holds no tool call, so the title request probably took the tool-call answer. #352 remains open and was not exercised by this run.

The rig ran on arm64 only. No x64 Docker host was available to this task: `vm001` and `vm14-001` are both arm64. The x64 bytes passed their native battery and were published with that caveat stated in the release notes; Abbe rig-tests the published bits on both architectures.

## Publication and read-back

Before creating the release, the following checks passed (`publication/pre-publish-checks.txt`):

- the exact `swxtchio/swx-opencode` entry was confirmed in `/home/byates/fm-llmrouter/config/gh-repo-allowlist`;
- the remote tag was absent;
- `release view` reported no such release;
- `swxtch` still resolved to the source.

The release was then created with `gh-axi release create v1.18.32-swxtch.d7f78881 -R swxtchio/swx-opencode --target d7f78881560aa83e9390f0231562fa79639eacd9 --latest --title v1.18.32-swxtch.d7f78881 --notes-file RELEASE-CUT-5-NOTES.md`, attaching both validated archive/companion pairs.

The tag-specific and `/releases/latest` APIs returned identical output: release `422461203` with the intended tag and name, and with:

- `target_commitish` equal to the source;
- `draft: false` and `prerelease: false`;
- the four assets, whose GitHub digests match the archives.

`git ls-remote` resolved the tag to the source. The published body is byte-identical to `RELEASE-CUT-5-NOTES.md`. All four assets were downloaded with `gh-axi release download`. Both companions passed, `cmp` found each archive identical to its native-tested candidate, and extraction reproduced both executable and sidecar digests.

## Delivery

This brief authorizes no write into another firstmate home, so this task did not append to an Abbe or SaaS wake log or send peer messages. The verified release notice was appended to this task's own wake log, `/home/byates/fm-llmrouter/state/release-cut-5.wake-events`, for firstmate to relay. It carries the release URL, source, both archive digests, the downloaded-byte result, the arm64 rig pass and the x64 caveat. Abbe's own rig run on the published bits for both architectures is the acceptance bar that follows publication.
