# Release-cut execution evidence: bce37c27

Recorded 2026-10-22 for task `opencode-release-cut-2` and [draft PR #345](https://github.com/swxtchio/swx-opencode/pull/345). Refs swxtchio/swx-opencode#338, resolved by #342. The binary source is immutable `bce37c276f4070e3c9d343baa61ce5f35ca38dc9`; the source/ancestry pickup record is in `plans/release-cut-2.md`. `RELEASE-CUT-2-NOTES.md` is the complete published release body, including the new native identities, archive/executable/sidecar hashes and required carried references. No production or release/probe tooling was changed.

## Native ARM64 execution

VM14 write authority is the task-specific firstmate disposition at `/home/byates/fm-llmrouter/data/opencode-release-cut-2/vm14-disposition.md`, recorded 2026-10-22T07:55Z. It authorizes this source's staging, install/cache, build, transport, test and proof under `vm14-001:/home/byates/vm14-builds/opencode-release-bce37c27`; the preceding release's grant was not reused. The new grant was read before staging or load. The remote lease README identified convention v3; `ACTIVE` was absent, and atomic `mkdir /home/byates/vm14-leases/ACTIVE` succeeded at 2026-10-22T07:55:36Z.

The acquired lease's `home` was `/home/byates/fm-llmrouter`, `task` and `task_id` were `opencode-release-cut-2`, and owner PID `3301773` was confirmed as this session's live OpenCode process. The initially recorded owner-box label `vm14-004` was incorrect; `hostname` returned `vm001`, and the task-owned metadata was atomically corrected before the build began. Both receipts are retained. Periodic heartbeats continued while the supervised build/probe process lived.

The fresh source clone checked out the captured source detached, verified all required ancestry, and remained clean through frozen install, native build, packaged probe and focused tests. The build started at 2026-10-22T07:56:21Z. HOME, XDG configuration/data/cache/state, npm/Bun caches and temporary paths were redirected beneath the granted root. The native builder observed `aarch64`, `Bun 1.4.2 linux-arm64`, and the selected lockfile digest. An independently fetched models.dev snapshot was consumed through `MODELS_DEV_API_JSON`; its digest is in the release body. The absolute `OPENCODE_BUILD_DIST` selected only native linux/arm64 through `--single --skip-install`.

Executed commands, with `$root=/home/byates/vm14-builds/opencode-release-bce37c27` and the recorded isolated environment:

```sh
bun install --cwd "$root/source" --frozen-lockfile --os=linux --cpu=arm64
OPENCODE_VERSION=1.18.32-swxtch.bce37c27 MODELS_DEV_API_JSON="$root/evidence/models.dev.json" OPENCODE_BUILD_DIST="$root/dist" bun run "$root/source/packages/opencode/script/build.ts" --single --skip-install
OPENCODE_VERSION=1.18.32-swxtch.bce37c27 bun run "$root/source/packages/opencode/script/release-probe.ts" arm64 "$root/extracted/opencode" "$root/proof"
bun --cwd "$root/source/packages/opencode" test test/provider/provider.test.ts test/config/config.test.ts test/config/v2-compat.test.ts test/session/prompt.test.ts test/session/compaction.test.ts test/session/session.test.ts test/server/httpapi-session.test.ts -t 'small-model|getSmallModel' --timeout 20000
```

The package-local Bun test script supplied its owning `--parallel=2 --timeout 30000 --only-failures` options before the explicit focus/timeout arguments. Its raw result was `59 pass`, `632 filtered out`, `0 fail`, `136 expect() calls`, across seven files in `17.58s`. No new or rewritten regression was introduced; these unchanged tests retain their existing source-owned behavioral and preservation roles.

The supervisor observed child completion and wrote result `0`. A later procfs scan inspected processes by both command line and current directory under this task root, excluding only its own ancestor chain, and returned `remaining_processes: []`. `/home/byates/vm14-leases/release-lease.sh /home/byates/fm-llmrouter opencode-release-cut-2` returned `released: lease matched home=/home/byates/fm-llmrouter task=opencode-release-cut-2`; the follow-up `test ! -d .../ACTIVE` passed. No successor lease was modified.

Raw native evidence, logs, actual catalog snapshot, source-cleanliness receipts, candidate assets and invocation scripts remain under the authorized VM14 task root. Their transport bundle was downloaded into this worktree's `logs/release-cut-2/arm-transport.tar.gz`; remote and local transport SHA-256 matched `bc56ff3bf6fc515731e0bc803f4403ea2c9e59162b897147c1c34f43a4c7a3fd`. Local extracted evidence is under `logs/release-cut-2/arm/`; the archive companion passed again after transport.

## Native x64 execution

The initial workflow dispatch using a SHA as `--ref` was rejected with `VALIDATION_ERROR` before acceptance. The accepted invocation selected the existing task branch as the workflow harness ref and supplied the fixed binary source separately:

```sh
gh-axi workflow run opencode-arm-release-x64.yml -R swxtchio/swx-opencode --ref fm/opencode-release-cut-2 --field source_commit=bce37c276f4070e3c9d343baa61ce5f35ca38dc9 --field release_tag=v1.18.32-swxtch.bce37c27 --field version=1.18.32-swxtch.bce37c27 --field candidate=true
```

[Run 39743812227](https://github.com/swxtchio/swx-opencode/actions/runs/39743812227) returned `success`; native candidate job `119707237605` completed successfully, and the ordinary published-release proof job was skipped as intended for an unpublished candidate. The workflow harness revision was `f66988f0280fd0e069ae54f6c775fc912237e156`. The candidate source was separately checked out at the captured `bce37c27` revision, with no source overlays. The selected source's Bun pin, native architecture, frozen install, complete embedded-web-UI build, archive/sidecar packaging, extracted version and full native acceptance probe passed before upload.

[Artifact 12409939130](https://github.com/swxtchio/swx-opencode/actions/runs/39743812227/artifacts/12409939130), named `opencode-linux-x64-candidate-39743812227`, has reported transport ZIP digest `9a1f569a3e0f08205f52fa79e8c39434f6dbe7b955e64ccc6d12c136594e62f4` and Actions expiry `2026-11-05T07:56:12Z`. The artifact was downloaded with `gh-axi run download 39743812227 -R swxtchio/swx-opencode --dir logs/release-cut-2/x64`; its complete inputs/logs/provenance/native evidence remain in that worktree directory. The archive companion passed after download, and local extraction reproduced the native provenance's executable and sidecar digests. The directly hashed supplied catalog snapshot matched the native digest and the ARM64 snapshot digest.

## Runtime evidence boundary

Both unchanged selected-source `release-probe.ts` executions passed against the exact packaged candidates. `RELEASE-CUT-2-NOTES.md` records the version/FFF fixture, idle/pending/terminal outcomes, held-tool and held-continuation waits, producer registration/resolution identities, completed-history restart and replay, and physical database selectors. The native evidence JSON and raw producer logs are retained with each candidate; historical #331 executions were not substituted for these runs. The native probe's provider records show two completed requests, two failed-case requests and one interrupted request. Its V2 fixture made zero title calls; the separate focused V1 test run is the evidence for the carried independent-title/compaction behavior.

## Publication and read-back

Published [v1.18.32-swxtch.bce37c27](https://github.com/swxtchio/swx-opencode/releases/tag/v1.18.32-swxtch.bce37c27) at `2026-10-22T08:03:44Z`. Immediately before creation, the exact `swxtchio/swx-opencode` target was confirmed in `/home/byates/fm-llmrouter/config/gh-repo-allowlist`, the remote tag was absent, and the release list still contained only earlier releases. The creation command used that explicit `-R`, target `bce37c276f4070e3c9d343baa61ce5f35ca38dc9`, title/tag `v1.18.32-swxtch.bce37c27`, `--latest`, `--notes-file RELEASE-CUT-2-NOTES.md`, and both validated archive/companion pairs.

`gh-axi release view v1.18.32-swxtch.bce37c27 -R swxtchio/swx-opencode --full` returned the complete published body, including all five required sections and this release's outcome/identity table. The tag-specific API and `/releases/latest` both returned release ID `420448399`, the intended tag/name/source, `draft: false`, `prerelease: false`, and four uploaded assets. `git ls-remote origin refs/tags/v1.18.32-swxtch.bce37c27` resolved exactly to the captured source. The read-back commands and full observations are retained under `logs/release-cut-2/`.

| Uploaded asset | GitHub asset ID | Uploaded bytes | GitHub SHA-256 |
| --- | --- | ---: | --- |
| `opencode-linux-arm64.tar.gz` | `665124333` | 64257813 | `000821fca1887f4c1ce0ff5bf35fa29c2b4b84f52664d37d6ae0f6d92cdc1907` |
| `opencode-linux-arm64.tar.gz.sha256` | `665124330` | 94 | `7e93867b7ac492200a9b42aac76cb660e37f4de203065d94ec2f517198e2b115` |
| `opencode-linux-x64.tar.gz` | `665124326` | 64746454 | `45f8587523b67bd76ae9005c4d023722781976f95ed1ccedf37e44bc3fa47675` |
| `opencode-linux-x64.tar.gz.sha256` | `665124327` | 92 | `8b97adeec2dc0212dcc0a81c5bf25f745a2aacb52bcba0ec2f6f06c475f3ef54` |

All four assets were downloaded through `gh-axi release download` into `logs/release-cut-2/published/`. Both companion validations passed, and `cmp` confirmed each archive was byte-identical to its native-tested candidate. Separate extraction directories reproduced the recorded ARM64 and x64 executable and sidecar SHA-256 values; `cmp` also confirmed the extracted executables against their candidate extractions. Successful creation was followed by actual remote read-back and downloaded-byte validation.

## Delivery

SaaS task `opencode-arm-release` was verified in `/home/byates/fm-saas/data/backlog.md` and its retained `/home/byates/fm-saas/data/opencode-arm-release/brief.md`, "Assigned outcome and authority," as the preceding producer/image handoff task. Its existing wake log was read before append. At the recorded `2026-10-22T08:06Z` handoff, the complete verified release notice was appended to `/home/byates/fm-saas/state/opencode-arm-release.wake-events`; read-back showed the new second entry, preserving the preceding notice. It includes this release/tag/source, both complete archive URLs and SHA-256 values, both complete checksum URLs, the new native/59-test results and publication/read-back verification. This proves the task wake-log append; it does not claim consumer adoption or task redispatch.

The supplemental SaaS notification resolved the current peer endpoint from `/home/byates/fm-saas/state/.heartbeat-target` through the supported `peer:/home/byates/fm-saas` selector. The first send typed its payload once but refused a submit key because the ownership guard could not re-prove the byte-zero marker; record `p1792656340-3260963-4305` remained `phase=pending`. The record was read before recovery. The supported sender-side `fm-peer-mail.sh complete p1792656340-3260963-4305` revalidated the retained record/endpoint/marker and completed that same send without retyping. Its read-back at `/home/byates/fm-llmrouter/state/peer-outbox/p1792656340-3260963-4305` is `phase=sent`, `transport=terminal`, `message_kind=one-way`, `disposition=delivered-no-reply`, with payload SHA-256 `a324420cdc6c411795a0c86bebcea062de848feafe8fcb071b245bf06985cd92`. This receipt remains separate from the required wake-log append.

Abbe's current dispatch records and live issue `swxtchio/swx-abbe#1712` identify B-16.10 adoption as owned by `fm-abbe`; no implementation task metadata for that issue was present at this discovery snapshot. The historical `abbe-jail-swx-opencode-441` task remains in Abbe's backlog but owns first-request adoption, so its old wake-log destination has not been assumed to be B-16.10's recipient. The live `abbe-mvp-design-crew` task owns the current B-16.10 dispatch plan; firstmate is asked to confirm its wake log as the handoff destination or supply the actual adoption task destination.

The supplemental Abbe notification used the registered peer `abbe` and its current recorded primary `%123`. `fm-send.sh` refused before typing or outbox reservation because its composer was not provably empty (`pending`), explicitly reporting `NOTHING was typed`. No pending delivery record was created, and no submit/cleanup key or payload retry was sent. The refusal is retained in `logs/release-cut-2/abbe-peer-send.log`. Abbe's required task wake-log append and composer-safe supplemental delivery remain incomplete; this is a firstmate routing/endpoint decision, not a producer-release failure. No ready/done claim is made until the required handoff is resolved.
