# Release-cut execution evidence: cd3f0ce2

Recorded 2026-10-23 for task `opencode-release-cut-4` and [draft PR #351](https://github.com/swxtchio/swx-opencode/pull/351). This urgent cut ships [#350](https://github.com/swxtchio/swx-opencode/pull/350), which runs a V2 turn only after its Location's plugins have booted. It sits on top of [#347](https://github.com/swxtchio/swx-opencode/pull/347). Abbe B-16.10 (swxtchio/swx-abbe#1951) needs it to restore DEV chat (swxtchio/swx-abbe#2030). Refs swxtchio/swx-opencode#338. The binary source is immutable `cd3f0ce2d5e55944df41fcf43a167f77e40c8483`; the pickup identity and ancestry record is in `plans/release-cut-4.md`. `RELEASE-CUT-4-NOTES.md` is the complete published release body. No production or release/probe tooling was changed. Raw observations are under `docs/verification/release-cut-4/`.

## Pickup decisions

The task brief was retargeted from `opencode-release-cut-2`. It named that task's branch and wake log, gave a malformed expected source SHA, and supplied no VM14 write root. The pickup stopped before any commit or remote write and reported these gaps. Firstmate's grant, in `/home/byates/fm-llmrouter/data/opencode-release-cut-4/brief.md` under "Firstmate grant", confirmed:

- branch `fm/opencode-release-cut-4` and wake log `state/opencode-release-cut-4.wake-events`;
- source `cd3f0ce2d5e55944df41fcf43a167f77e40c8483` with all carried merges as ancestors;
- VM14 write authority for `vm14-001:/home/byates/vm14-builds/opencode-release-cd3f0ce2`, covering staging, install, cache, build, transport, test and proof of this source only, lease-aware;
- that the Abbe jail rig must pass on the packaged bytes before publication.

## Native x64 (GitHub Actions)

```sh
gh workflow run opencode-arm-release-x64.yml -R swxtchio/swx-opencode --ref swxtch -f source_commit=cd3f0ce2d5e55944df41fcf43a167f77e40c8483 -f release_tag=v1.18.32-swxtch.cd3f0ce2 -f version=1.18.32-swxtch.cd3f0ce2 -f candidate=true
```

[Run 40015678366](https://github.com/swxtchio/swx-opencode/actions/runs/40015678366) succeeded. Its `release-candidate` job (120627014387) passed, and `release-proof` was skipped as intended for an unpublished candidate. The dispatch ref was `swxtch` at the release source, so the harness revision equals the source revision. Provenance records:

- `x86_64` and Bun `1.4.2`;
- the selected lockfile digest;
- catalog digest `8c13fedb…0851`;
- clean source before and after the build, with no overlays.

[Artifact 12529080055](https://github.com/swxtchio/swx-opencode/actions/runs/40015678366/artifacts/12529080055) has transport ZIP digest `c99e01a9…e632` and expires on 2026-11-06. It was downloaded with `gh run download`. Its companion passed locally, and local extraction reproduced the provenance's executable and sidecar digests.

## Native arm64 (vm14-001)

The lease README (convention v3) was read before admission. `ACTIVE` was free. Acquisition by atomic `mkdir` succeeded at 2026-10-23T19:59:44Z. `lease.json` names:

- home `/home/byates/fm-llmrouter` and task `opencode-release-cut-4`;
- owner box `vm001` and owner PID `148344`, which is this session's Claude process.

`supervise.sh` ran `arm-build.sh` under the granted root. It touched the lease heartbeat every 30 seconds while the build lived, then recorded result `0`. Both scripts are cut-3's, with only the root, source, version, task name and ancestry list changed. `arm-build.sh` first redirects HOME, XDG, the Bun/npm caches and TMPDIR beneath the root. It then:

1. clones the fork and checks out the source detached;
2. checks the Bun pin and all ancestry, #350 included;
3. runs the source's `release-probe-target.ts --candidate` with a synthetic dispatch event, which printed the expected tag, source, version and Bun `1.4.2`;
4. installs with `bun install --frozen-lockfile --os=linux --cpu=arm64`;
5. fetches and hashes the catalog, then builds with `OPENCODE_VERSION`, an absolute `MODELS_DEV_API_JSON` and `OPENCODE_BUILD_DIST`, using `build.ts --single --skip-install`;
6. packages exactly `opencode` and its sidecar, validates the companion, extracts the archive and checks `--version`;
7. runs the source's unchanged `release-probe.ts arm64` on the extracted executable.

Source status was empty before the build, after install, after the build and after the probe. The run lasted from 19:59:50Z to 20:01:12Z.

A procfs scan of the task root, matching both command line and cwd and excluding only the scan itself, returned `remaining_processes: []`. `release-lease.sh /home/byates/fm-llmrouter opencode-release-cut-4` returned `released: lease matched`, and `ACTIVE` was then absent (20:03:46Z). The transport bundle's SHA-256 is `d250d116…dcca4`, and it matched after the copy. The archive companion passed again after transport, and local extraction reproduced the executable and sidecar digests.

## Focused source tests

These ran on the task's arm64 development host (`vm001`), at the release source plus the plan-only commit, with Bun `1.4.2` after a frozen install.

- From `packages/opencode`, the focused command in `plans/small-model-verification.md` passed 59 tests, with 632 filtered out and zero failures.
- From `packages/core`, these files passed 150 tests with zero failures:
  - #347's `test/config/config.test.ts`, `test/config/environment.test.ts`, `test/config/provider.test.ts` and `test/location-layer.test.ts`;
  - #350's `test/session-runner.test.ts` and `test/session-runner-recorded.test.ts`.

No test was added or rewritten; these unchanged tests keep their existing roles.

## Abbe jail V2 egress rig (publication gate)

The rig is `/home/byates/fm-abbe/data/mvp-b16-10/jail-v2-egress-rig.md` (SHA-256 `73b543ef…019f` as read). It ran on `vm001` (arm64, Docker 29.8.0) against a private clone of swx-abbe at swxtchio/swx-abbe#1951's head `be3692bea1183a820c44b20f8443a826a75102a2`. The clone was kept under this worktree's ignored `logs/`, so snap Docker could read it. Its `components/orch/services/opencode-variant` is unchanged from the doc's measured head `03d7e234`.

The rig doc's build downloads the published asset, and that asset did not exist yet. `jail/Dockerfile.candidate.diff` records the one change made to `jail/Dockerfile.jail`: a `COPY` of the native-tested arm64 archive replaces the `curl` download. The Dockerfile's own digest check (`OPENCODE_ARCHIVE_SHA256=11382c4b…352f`) printed `OK`, and its `--version` check passed. `image-bytes.sha256` shows the image's `/usr/local/bin/opencode` and sidecar equal the native-tested executable and sidecar.

The probe, saved verbatim from the doc, ran as `python3 jail_v2_egress_probe.py v1-control v2 v2-named` with the doc's environment. It exited `0`, and every mode printed `RESULT: PASS`. The fake provider logged `REQ1` and `REQ2 /v1/chat/completions model=accounts/fireworks/models/deepseek-v4p1-flash stream=True` for the location-only `v2` create, for the named `swxtch-egress` create, and for the V1 control. For `774d90c7` the doc records `ModelNotSelectedError` and `ModelUnavailableError` with no provider request.

Both V2 modes resolved their wait `failed`. A diagnostic copy of the probe, which only adds the `session.next.tool.*` history events to its output (`jail_v2_tool_diag.py`, `rig-tool-diag.log`), shows why. The fake provider's `propose_netlist` call settled as `Unknown tool: propose_netlist` with `provider.executed: false`. The jail stages its advisory tools as project-local `.opencode/tools` modules, and V2's `ToolRegistry` does not register them. This lies outside the rig's success signal and is filed as [#352](https://github.com/swxtchio/swx-opencode/issues/352).

The rig ran on arm64 only. No x64 Docker host was available to this task: `vm001` and `vm14-001` are both arm64, and `vm001` has no amd64 emulation.

## Publication and read-back

Before creating the release, the following checks passed:

- the exact `swxtchio/swx-opencode` entry was confirmed in `/home/byates/fm-llmrouter/config/gh-repo-allowlist`;
- the remote tag was absent;
- `release view` reported no such release.

The release was then created with `gh-axi release create v1.18.32-swxtch.cd3f0ce2 -R swxtchio/swx-opencode --target cd3f0ce2d5e55944df41fcf43a167f77e40c8483 --latest --title v1.18.32-swxtch.cd3f0ce2 --notes-file RELEASE-CUT-4-NOTES.md`, attaching both validated archive/companion pairs.

The tag-specific and `/releases/latest` APIs both returned release `422267143` with the intended tag and name, and with:

- `target_commitish` equal to the source;
- `draft: false` and `prerelease: false`;
- the four assets, whose GitHub digests match the archives.

`git ls-remote` resolved the tag to the source. The published body is byte-identical to `RELEASE-CUT-4-NOTES.md`. All four assets were downloaded with `gh-axi release download`. Both companions passed, `cmp` found each archive identical to its native-tested candidate, and extraction reproduced both executable and sidecar digests.

## Delivery

SaaS's producer/image handoff task remains `opencode-arm-release`, recorded in `/home/byates/fm-saas/data/done-archive.md`. At 2026-10-23T20:12:18Z one verified notice line was appended with `>>` to `/home/byates/fm-saas/state/opencode-arm-release.wake-events`, taking it from 3 to 4 lines. Read-back confirmed the last line equals the notice. A supplemental `bin/fm-send.sh peer:/home/byates/fm-saas --no-reply` notification was delivered as `p1792786351-2011828-449f`, with `phase=sent`, `transport=terminal` and `disposition=delivered-no-reply`.

The Abbe B-16.10 recipient could not be established. `/home/byates/fm-abbe/data/backlog.md` still lists `mvp-b16-10` against swxtchio/swx-abbe#1712, but `/home/byates/fm-abbe/state/` has no `mvp-b16-10.meta` or `mvp-b16-10.wake-events`. The log cut-3 appended to is gone, and no other live Abbe task names B-16.10, #1951 or #2030. No file was created there. Two supplemental `peer:/home/byates/fm-abbe` sends were refused before typing, because the Abbe captain's composer was not provably empty (`pending`, then `unknown`). Nothing was typed and no outbox record was reserved. The Abbe handoff is reported to firstmate as an open gap.
