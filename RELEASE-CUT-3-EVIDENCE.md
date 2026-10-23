# Release-cut execution evidence: 774d90c7

Recorded 2026-10-23 for task `opencode-release-cut-3` and [draft PR #349](https://github.com/swxtchio/swx-opencode/pull/349). This urgent cut ships [#347](https://github.com/swxtchio/swx-opencode/pull/347), the V2 env-supplied config and egress default-model fix, which Abbe B-16.10 (swxtchio/swx-abbe#1951) needs to restore DEV chat (swxtchio/swx-abbe#2030). Refs swxtchio/swx-opencode#338. The binary source is immutable `774d90c7fb02b9f0962620971630a4f09c1a886b`; the pickup identity and ancestry record is in `plans/release-cut-3.md`. `RELEASE-CUT-3-NOTES.md` is the complete published release body. No production or release/probe tooling was changed. Raw observations are under `docs/verification/release-cut-3/`.

## Pickup decisions

The task brief was retargeted from `opencode-release-cut-2` and still named that task's branch and wake log. Firstmate's ruling, recorded in `/home/byates/fm-llmrouter/data/opencode-release-cut-3/firstmate-decisions.md`, confirmed branch `fm/opencode-release-cut-3`, wake log `state/opencode-release-cut-3.wake-events` and one new draft PR. The same ruling granted VM14 write authority for `vm14-001:/home/byates/vm14-builds/opencode-release-774d90c7`, covering staging, install, cache, build, transport, test and proof of this source only. Work that needed no grant ran before the ruling: the x64 Actions dispatch, the ancestry checks and the focused tests. Firstmate's ruling confirmed that dispatch.

## Native x64 (GitHub Actions)

```sh
gh-axi workflow run opencode-arm-release-x64.yml -R swxtchio/swx-opencode --ref swxtch --field source_commit=774d90c7fb02b9f0962620971630a4f09c1a886b --field release_tag=v1.18.32-swxtch.774d90c7 --field version=1.18.32-swxtch.774d90c7 --field candidate=true
```

[Run 39983517672](https://github.com/swxtchio/swx-opencode/actions/runs/39983517672) succeeded. Its `release-candidate` job (120516656992) passed, and `release-proof` was skipped as intended for an unpublished candidate. Because the dispatch ref was `swxtch` at the release source, the harness revision equals the source revision. Provenance records `x86_64`, Bun `1.4.2`, the selected lockfile digest, catalog digest `713b2cb3…17d9`, clean source before and after, and no overlays. [Artifact 12515386562](https://github.com/swxtchio/swx-opencode/actions/runs/39983517672/artifacts/12515386562) has transport ZIP digest `d119880b…c3d46a` and expires on 2026-11-06. It was downloaded with `gh run download`, its companion passed locally, and local extraction reproduced the provenance's executable and sidecar digests.

## Native arm64 (vm14-001)

The lease README (convention v3) was read before admission. At pickup, `ACTIVE` was held by `opencode-v2-provider-1952`, then by fm-abbe `native-store-custody-1550`, whose owner process was live. The task paused and polled with atomic `mkdir`; `lease/acquire-attempts.log` records 53 refused attempts. Acquisition succeeded at 2026-10-23T16:35:05Z. `lease.json` names home `/home/byates/fm-llmrouter`, task `opencode-release-cut-3`, owner box `vm001` and owner PID `3106855`, which is this session's Claude process.

`supervise.sh` ran `arm-build.sh` under the granted root. It touched the lease heartbeat every 30 seconds while the build lived, then recorded result `0`. The script redirects HOME, XDG, Bun/npm cache and TMPDIR beneath the root. It then:

1. clones the fork and checks out the source detached;
2. checks the Bun pin and all ancestry;
3. runs the source's `release-probe-target.ts --candidate` with a synthetic dispatch event (it printed the expected tag, source, version and Bun `1.4.2`);
4. installs with `bun install --frozen-lockfile --os=linux --cpu=arm64`;
5. fetches and hashes the catalog, then builds with `OPENCODE_VERSION`, absolute `MODELS_DEV_API_JSON` and `OPENCODE_BUILD_DIST` using `build.ts --single --skip-install`;
6. packages exactly `opencode` and its sidecar, validates the companion, extracts and checks `--version`;
7. runs the source's unchanged `release-probe.ts arm64` on the extracted executable.

Source status was empty before the build, after install, after the build and after the probe. The run lasted from 16:35:18Z to 16:40:35Z.

A procfs scan of the task root, matching both command line and cwd, returned `remaining_processes: []`. `release-lease.sh /home/byates/fm-llmrouter opencode-release-cut-3` returned `released: lease matched`, and `ACTIVE` was then absent (16:41:03Z). The transport bundle's SHA-256 is `df16aae9…7e97067`, and it matched after the copy. The archive companion passed again after transport, and local extraction reproduced the executable and sidecar digests.

## Focused source tests

From `packages/opencode`, on the task's x64 host at the release source with Bun `1.4.2`, the `plans/small-model-verification.md` focused command passed 59 tests, with 632 filtered out and zero failures. From `packages/core`, #347's `test/config/config.test.ts`, `test/config/environment.test.ts`, `test/config/provider.test.ts` and `test/location-layer.test.ts` passed 28 tests with zero failures. No test was added or rewritten; these unchanged tests keep their existing roles.

## Publication and read-back

Before creating the release, the exact `swxtchio/swx-opencode` entry was confirmed in `/home/byates/fm-llmrouter/config/gh-repo-allowlist`, the remote tag was absent, and `release view` reported none. The release was created with `gh-axi release create v1.18.32-swxtch.774d90c7 -R swxtchio/swx-opencode --target 774d90c7fb02b9f0962620971630a4f09c1a886b --latest --notes-file RELEASE-CUT-3-NOTES.md`, attaching both validated archive/companion pairs.

The tag-specific and `/releases/latest` APIs both returned release `422085166` with the intended tag and name, `target_commitish` equal to the source, `draft: false`, `prerelease: false`, and the four assets with GitHub digests matching the archives. `git ls-remote` resolved the tag to the source. The published body is byte-identical to `RELEASE-CUT-3-NOTES.md`. All four assets were downloaded with `gh-axi release download`. Both companions passed, `cmp` found each archive identical to its native-tested candidate, and extraction reproduced both executable and sidecar digests.

## Delivery

The recipients were revalidated from current records. Abbe's B-16.10 task is `mvp-b16-10`: it is the live task with a current meta file in `/home/byates/fm-abbe/state/`, and `data/backlog.md` lists it against swxtchio/swx-abbe#1712. The historical lead `abbe-jail-swx-opencode-441` is not the current B-16.10 owner. SaaS's producer/image handoff task remains `opencode-arm-release`, recorded in `/home/byates/fm-saas/data/done-archive.md` with its retained brief.

At 2026-10-23T16:43:53Z, one verified notice line was appended with `>>` to `/home/byates/fm-abbe/state/mvp-b16-10.wake-events` (13 to 14 lines) and to `/home/byates/fm-saas/state/opencode-arm-release.wake-events` (2 to 3 lines). Read-back confirmed each file's last line equals the notice. The notice gives the release URL, version, source, the #347 reason, both archive URLs and SHA-256 values, both checksum URLs, and the native and published-byte results.

Supplemental peer notifications through `bin/fm-send.sh peer:<home> --no-reply` reached both homes. Their sender-owned outbox records are `p1792773840-2738195-53a3` (Abbe) and `p1792773849-2763976-1834` (SaaS), each `phase=sent`, `transport=terminal`, `message_kind=one-way`, `disposition=delivered-no-reply`. These receipts are separate from the required wake-log appends.
