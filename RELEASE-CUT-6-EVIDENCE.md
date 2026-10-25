# Release-cut execution evidence: 832df553

Recorded 2026-10-25 for release-cut-6 and [draft evidence PR #367](https://github.com/swxtchio/swx-opencode/pull/367). The published release is [`v1.18.32-swxtch.832df553`](https://github.com/swxtchio/swx-opencode/releases/tag/v1.18.32-swxtch.832df553), targeting immutable source `832df553518d8bb581952433c72495a472da8c5d`. The release evidence branch contains only the plan, release notes, verification artifacts, this summary, and its changeset entry; no product or release-probe source changed.

## Pickup and source identity

The isolated worktree was on `fm/release-cut-6`. GitHub reported `swxtch` as the live default branch, and `origin/swxtch` resolved to the selected source at pickup. The authenticated account was `brentyates-swx`; the task had no assigned release issue. The open-PR overlap scan found only the unrelated shell-streaming test PR #366, and no existing PR for this release cut. Draft PR #367 was opened against `swxtch`; its first comment contains the required session and worktree stamp.

The selected source's `packages/opencode/package.json` version is `1.18.32`, root `package.json` pins `bun@1.4.2`, and `bun.lock` SHA-256 is `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151`. `local-release.sh` derives the release as package version plus `-swxtch.` and the source SHA8; `packages/opencode/script/release-probe-target.ts` checked explicit candidate inputs and resolved version `1.18.32-swxtch.832df553` and tag `v1.18.32-swxtch.832df553` on arm64 and in the x64 candidate workflow.

The selected source is the #361 merge commit and has #358 merge commit `4973723074ed4ba2e778e287eacd3d518cc315d7` as its parent. The release-probe V2-wait ancestry check also passes. Abbe #1951's current body names the older adopted tag `v1.18.32-swxtch.d7f78881` at source `d7f78881560aa83e9390f0231562fa79639eacd9`; that source is an ancestor of #358, while #358 and #361 are not ancestors of it. The issue can be rechecked with `gh issue view 1951 -R swxtchio/swx-abbe --json body -q '.body'`. Raw checks are in `identity/source-ancestry.txt` and the arm64 resolver receipts.

Immediately before publication, `gh repo view` still reported `swxtch`, the branch still resolved to the selected source, the repository allowlist contained `swxtchio/swx-opencode`, the remote tag was absent, and `gh-axi release view` returned `NOT_FOUND`. The selected tag and release did not conflict with an existing identity.

## Native candidates

Both native candidates were built from the selected clean source with the source-pinned Bun version, frozen platform dependencies, explicit `OPENCODE_VERSION`, and a retained `MODELS_DEV_API_JSON` snapshot. The two catalog files are byte-identical and each has SHA-256 `5ecb11a065a7772c9b3730ade26a879ec3e3656e57d66355171d1f9b24508c7e`. The x64 workflow harness head was recorded separately from the binary source and happened to equal it.

| Target | Native build and probe | Archive bytes and SHA-256 | Extracted executable SHA-256 | FFF sidecar SHA-256 |
| --- | --- | --- | --- | --- |
| linux/arm64 | `vm14-001`, native Linux `aarch64`, Bun `1.4.2`; `release-probe.ts arm64` passed | `64244547`; `cb28771c648dd31fe9caba5d3cc8657438630afd933dd7a24c4d2612f472be76` | `c6653bf327b45cefa14bdb49043230b93edb74cf91163650e16b2d761c4977b5` | `0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42` |
| linux/x64 | GitHub Actions run [40224713450](https://github.com/swxtchio/swx-opencode/actions/runs/40224713450), native `x86_64`, Bun `1.4.2`; `release-candidate` passed and `release-proof` was skipped | `64727596`; `6156af8b96d6763b889636ee0fe986291edaeb17f9d82cd73270774a05309ae7` | `68225a7ee38c7044f77df5d8ba494c79c1e7c1808d55efd12a300637bf5dd51e` | `977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5` |

Each archive contains exactly `opencode` and its matching sidecar. Both companion checks passed after packaging and transport, both extracted binaries reported the derived version, and both native probes passed. The full x64 provenance and Actions log are under `docs/verification/release-cut-6/x64/`; the arm64 resolver, clean-source receipts, catalog, build logs, provenance, and native probe are under `docs/verification/release-cut-6/arm64/` and `inputs/`.

## Abbe jail V2 tool acceptance

The consumer source was staged from clean Abbe commit `8ac1489e7d3ecad9f9a09431d3df54b6b69ad8e6` into the authorized VM14 task root; the Abbe checkout stayed at that commit and clean. The jail was built from its `Dockerfile.jail` with only the release download replaced by `COPY` of the exact unpublished arm64 candidate. The Dockerfile's digest and version checks remained in the candidate file and passed with `--no-cache`. Candidate image `sha256:7fe44c87982662d608539dd672d5266c54b2add349299927687c0aa47af48cc7` reported version `1.18.32-swxtch.832df553`; its executable and sidecar hashes matched the native-tested candidate.

The added release acceptance test uses Abbe's real `_brain_rig`, the real staged `propose_netlist.js`, and a request-aware fake provider. In both location-only and named-provider modes, the provider advertised `propose_netlist`; a user-turn marker selected the valid call, while the title request received text; the real module's `session.next.tool.success` event persisted the proposal output; the provider continuation received the matching tool-call ID; and `OpenCodeClient.wait_session_v2` completed with the same admitted sequence. The public request projection retains the exact advertised `propose_netlist` definition, turn marker, selected call ID, matching tool-result ID, receipts, waits, complete durable history, and tool output. The full provider request logs remain in the task-authorized VM14 root, with their SHA-256 bindings recorded in `jail/raw-provider-log-hashes.txt` and each mode's JSON. Consumer source hashes, candidate archive hash, and image ID are retained under `jail/lap3-passing/`.

The retained V1 control passed. The manual compaction mode also passed: its turn wait completed, compact returned 204, the provider summary request carried the turn transcript, and history recorded the provider's summary in a manual `session.next.compaction.ended` event. The exact current rig probe was saved from the rig document, whose SHA-256 is recorded in `jail/consumer-source.txt`.

Jail acceptance was run on arm64 only. The checked `vm001` and `vm14-001` hosts are both aarch64, and VM14 BuildKit advertises only `linux/arm64`; no x64 jail result is claimed. The x64 archive did pass its native x64 build and probe, and Abbe's x64 jail acceptance remains downstream.

At Abbe commit `8ac1489e7d3ecad9f9a09431d3df54b6b69ad8e6`, `SessionPool._NativeV2Turn.events` in `components/orch/services/opencode-variant/session_pool.py` refuses a completed tool projection unless `provider.executed` is true. The candidate's real local tool success event records `provider.executed=false`. This jail gate exercises the OpenCode API through the jail and `OpenCodeClient`, not Abbe's gateway/SessionPool projection; that consumer-level compatibility remains an explicit handoff to Abbe before pin adoption. The evidence note and source hash are in `jail/consumer-boundary.txt` and `jail/consumer-boundary-source.txt`.

Four VM14 leases were released with the owner-checked helper. The two scratch-harness laps that stopped before a candidate image was built are preserved in `jail/lap1-preparation-failure/` and `jail/lap2-build-context-failure/`; the passing lap removed its candidate image. The final scan found no task process, image, container, or added network. Lease records, the heartbeat supervisor, Docker snapshots, and cleanup checks are under `lease/` and `jail/`.

## Focused source tests

The first local test attempt could not import `effect` because workspace dependencies were absent. `bun install --frozen-lockfile` then passed without changing `bun.lock`; all required package-local files passed whole with Bun `1.4.2` and the product/probe tree unchanged at source `832df553518d8bb581952433c72495a472da8c5d`.

| Working directory | Command | Final summary line |
| --- | --- | --- |
| `packages/core` | `bun test test/config/config.test.ts` | `Ran 22 tests across 1 file. [3.07s]` |
| `packages/core` | `bun test test/config/agent.test.ts` | `Ran 6 tests across 1 file. [732.00ms]` |
| `packages/core` | `bun test test/config/environment.test.ts` | `Ran 10 tests across 1 file. [1.67s]` |
| `packages/core` | `bun test test/session-runner.test.ts` | `Ran 129 tests across 1 file. [11.49s]` |
| `packages/opencode` | `bun test test/server/httpapi-session.test.ts` | `Ran 82 tests across 1 file. [140.97s]` |
| `packages/core` | `bun test test/location-layer.test.ts` | `Ran 8 tests across 1 file. [4.38s]` |
| `packages/core` | `bun test test/session-runner-tool-registry.test.ts` | `Ran 22 tests across 1 file. [1451.00ms]` |
| `packages/opencode` | `bun test test/tool/registry.test.ts` | `Ran 18 tests across 1 file. [5.58s]` |
| staged Abbe consumer root on VM14 | `python3 -m pytest -q components/orch/services/opencode-variant/tests/test_release_cut_6_candidate.py` | `2 passed in 16.29s` |

Each package test's complete output is in `docs/verification/release-cut-6/tests/`. The VM14 pytest command ran against the candidate image and the committed test file whose hash is recorded in the jail evidence.

The exact VM14 test invocation was run from `/home/byates/vm14-builds/opencode-release-832df553/consumer/swx-abbe`:

```sh
TMPDIR=/home/byates/vm14-builds/opencode-release-832df553/rig-tmp PYTHONDONTWRITEBYTECODE=1 PYTEST_ADDOPTS='-p no:cacheprovider' \
OPENCODE_VARIANT_JAIL_IMAGE=opencode-variant-jail:v2-egress-1.18.32-swxtch.832df553-candidate OPENCODE_SERVER_PASSWORD=local-rig-password \
RELEASE_CUT_6_SOURCE_COMMIT=832df553518d8bb581952433c72495a472da8c5d RELEASE_CUT_6_CONSUMER_COMMIT=8ac1489e7d3ecad9f9a09431d3df54b6b69ad8e6 \
RELEASE_CUT_6_ARCHIVE_SHA256=cb28771c648dd31fe9caba5d3cc8657438630afd933dd7a24c4d2612f472be76 \
RELEASE_CUT_6_IMAGE_ID=sha256:7fe44c87982662d608539dd672d5266c54b2add349299927687c0aa47af48cc7 \
RELEASE_CUT_6_EVIDENCE=/home/byates/vm14-builds/opencode-release-832df553/evidence/jail/tool \
python3 -m pytest -q components/orch/services/opencode-variant/tests/test_release_cut_6_candidate.py
```

## Publication and read-back

The release was created with `gh-axi release create v1.18.32-swxtch.832df553 -R swxtchio/swx-opencode --target 832df553518d8bb581952433c72495a472da8c5d --latest --title v1.18.32-swxtch.832df553 --notes-file RELEASE-CUT-6-NOTES.md`, attaching both archive/companion pairs. GitHub returned release ID `423621088` at [the release URL](https://github.com/swxtchio/swx-opencode/releases/tag/v1.18.32-swxtch.832df553).

The remote tag ref resolves directly to the selected source commit. The release is published with `draft=false` and `prerelease=false`; the tag-specific and `/releases/latest` APIs return the same release ID `423621088`, tag, target, assets, and body. The body is byte-identical to `RELEASE-CUT-6-NOTES.md`. GitHub's metadata for the four published assets is:

| Asset | Bytes | GitHub SHA-256 |
| --- | ---: | --- |
| `opencode-linux-arm64.tar.gz` | 64244547 | `cb28771c648dd31fe9caba5d3cc8657438630afd933dd7a24c4d2612f472be76` |
| `opencode-linux-arm64.tar.gz.sha256` | 94 | `59cdefe3a79e1e8a3ebadadc26790a578e4f7f1cccaada56bf06ba8c20a31ddf` |
| `opencode-linux-x64.tar.gz` | 64727596 | `6156af8b96d6763b889636ee0fe986291edaeb17f9d82cd73270774a05309ae7` |
| `opencode-linux-x64.tar.gz.sha256` | 92 | `ad44ea4c903978d58ca57ad55a42008c9ac92ad6fa8aedc4d5263c48eb7cac6a` |

Every public archive and companion was downloaded afresh with `gh-axi release download`. Both companion checks passed; each downloaded archive compared byte-for-byte with its native-tested candidate; archive contents matched; and extracted executable and sidecar hashes matched. The downloaded arm64 binary printed the derived version on native host `vm001`. The x64 version output is retained from the successful native x64 candidate job, and the downloaded x64 executable and sidecar are byte-identical to that candidate.

## Handoff

No Abbe pin, image, or deployment was changed. The release supplies the tested producer bytes; Abbe's full gateway/SessionPool compatibility and x64 jail acceptance remain consumer work. The evidence PR remains draft for Firstmate's review and merge; this task did not mark it ready or merge it. The task wake log carries the release URL and consumer follow-up for Firstmate to deliver to Abbe.
