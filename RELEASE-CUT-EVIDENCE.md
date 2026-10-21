# Release-cut evidence

Evidence collected 2026-10-21 for the release-cut task. [v1.18.32-swxtch.d1b232e9](https://github.com/swxtchio/swx-opencode/releases/tag/v1.18.32-swxtch.d1b232e9) was published at 2026-10-21T03:12:35Z and confirmed as Latest by GitHub read-back. This dated execution record retains the published release's build, acceptance and delivery evidence. The harness changes and this record are durably available through [PR #326](https://github.com/swxtchio/swx-opencode/pull/326); the release source remains separate from its task-branch head.

## Selected source

- Source: `d1b232e940ef155a3e5311794a2baeaa143af1d8`.
- Derived version: `1.18.32-swxtch.d1b232e9`; published tag: `v1.18.32-swxtch.d1b232e9`. Read `packages/opencode/package.json` at the selected commit and apply `local-release.sh`'s source-SHA convention to reproduce the derivation. `packages/script/src/index.ts` and `packages/opencode/script/build.ts` consume the explicit `OPENCODE_VERSION` build pin.
- The selected root `package.json` pins Bun `1.4.2`. The local harness checks used Bun `1.4.2` (`744846f84`).
- Selected `bun.lock` SHA-256: `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151`, measured with `git show d1b232e940:bun.lock | sha256sum`.
- `git merge-base --is-ancestor 4b87478ddf d1b232e940ef155a3e5311794a2baeaa143af1d8` exited zero.
- GitHub's default-branch read returned `swxtch`. The selected release tag was absent from `git ls-remote origin refs/tags/v1.18.32-swxtch.d1b232e9` at pickup; the release list contained the two prior releases. The subsequent publication and read-back below establish the published tag and Latest designation.

The following GitHub API reads returned `merged: true` and the listed merge commits. Each commit passed `git merge-base --is-ancestor <commit> <selected-source>`:

| Included work | Landed source commit |
| --- | --- |
| [Stall fix #303](https://github.com/swxtchio/swx-opencode/pull/303) | `24024090d26ce916a83cc7871a78fc888ee47ee5` |
| [Auto-compaction #313](https://github.com/swxtchio/swx-opencode/pull/313) | `b20cc22ef44107d63e09daa4def7ca2c15664c9b` |
| [SDK-fixture issue #300](https://github.com/swxtchio/swx-opencode/issues/300), resolved by [#301](https://github.com/swxtchio/swx-opencode/pull/301) | `bf66f27de555be40f64c949699104c9d82e08758` |
| [GLM-5.3 issue #305](https://github.com/swxtchio/swx-opencode/issues/305), resolved by [#306](https://github.com/swxtchio/swx-opencode/pull/306) | `e5a073e157ba62d559d5f0edfb1b81f28cf6e259` |
| [Queue-force work #314](https://github.com/swxtchio/swx-opencode/pull/314) | `d1b232e940ef155a3e5311794a2baeaa143af1d8` |

## Harness validation

The selected source already contains producer-emitted wait registration and resolution logs. Candidate builds use its production implementation without the existing proof workflow's core-source overlay. The candidate job packages the selected source's executable and FFF sidecar and passes the extracted executable to the unchanged `packages/opencode/script/release-probe.ts`; missing producer evidence remains a failure.

Commands ran from `packages/opencode`, with scratch redirected into the task worktree:

```sh
bun test test/release-probe-tools.test.ts
bun typecheck
```

At initial harness head `67cfd0c6dd`, the focused file passed: 11 tests, zero failures. Package typecheck exited zero. That initial regression ran `release-probe-target.ts` as a child against an isolated clone of the executing checkout; it observed successful candidate resolution and the produced `GITHUB_ENV`, then refused a missing tag in ordinary proof mode, a conflicting existing tag, a missing explicit source pin, and an unavailable remote. The fixture correction below supersedes its dependence on that executing checkout.

Bounded absence proof: temporarily replace `const unpublished = candidate && tagLookup.exitCode === 2` with `const unpublished = false`, then run:

```sh
bun test test/release-probe-tools.test.ts -t 'resolves an unpublished candidate'
```

That run exited one with one failed test: the candidate child returned exit one where the regression expected zero. The regression completed without an external timeout. After restoring candidate-mode behavior, the complete focused file again passed and package typecheck again exited zero.

From the repository root, `bun run script/check-workflow-guards.ts` exited zero after reviewing and refreshing the adapted workflow's job and envelope pins. `git diff --check` exited zero. No `bin/fm-lint.sh` is tracked in this project. The project-memory helper reported the existing `AGENTS.md` unchanged.

### Tagged-checkout fixture correction

The live [PR proof run 39529207826](https://github.com/swxtchio/swx-opencode/actions/runs/39529207826) failed at the initial regression's missing-tag assertion. That workflow executes the test from the existing published source checkout, so cloning its Git state retained the very tag the regression expected to be absent. The fixture also depended on full historical ancestry, which the unit workflow's shallow checkout does not supply.

The correction separates the existing filesystem/Git/package identity boundary into `resolveReleaseTarget`, used by both the CLI and regression. The CLI still checks the fixed V2 wait ancestor before emitting `GITHUB_ENV`; that production ancestry check is preserved. The regression now creates its own package files, commits and local Git remote rather than copying the host checkout's tags or history. It drives the real `resolveReleaseTarget` producer for absent-tag admission, ordinary-proof missing-tag refusal, matching-tag acceptance, conflicting-tag refusal, missing-pin refusal and unavailable-remote refusal.

The revised focused file passed 11 tests with zero failures, and package `bun typecheck` exited zero. Replacing the candidate-mode unpublished-tag expression with `false` again made the revised regression fail within its own bound, now with `Could not resolve release tag`; restoring the expression passed. The repository correctness lint command exited zero with no errors, and `git diff --check` passed. The original native x64 archive and its acceptance evidence below remain bound to their unchanged selected release source and original build run.

At correction head `debb5cc9b13a3300d8950eec9c775bfdb6c0c9ff`, the tagged-release [PR proof run 39531281855](https://github.com/swxtchio/swx-opencode/actions/runs/39531281855) completed successfully. A fresh [native candidate run 39531357309](https://github.com/swxtchio/swx-opencode/actions/runs/39531357309) also completed successfully, confirming the CLI's extracted target-resolution path against the fixed new release source and the full unchanged native probe. Its artifact ID is `12315636163`, named `opencode-linux-x64-candidate-39531357309`; GitHub reports transport ZIP digest `6bef3e9fa9bdb482b92fb3178ecc296fd60c0b4a28e12bc96f6688859292a101`. That artifact was downloaded into `.release-cut/x64-harness-recheck/`, and its archive companion passed local validation.

This additional harness-validation build used Bun `1.4.2` and lockfile SHA-256 `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151`. Its independently fetched supplied models.dev snapshot SHA-256 is `5ceb546ecb08f34595f3e07a565735593e7952e92f2e7fbdf25c5766aef6b245`; its archive size is `64703239`, archive SHA-256 `ab7fa520e305aaf9f199a5b24491a433c2fea01ebf8e89f026e6871808e594fe`, executable SHA-256 `bc0ae37f5f060cc580d9dd37973adc4114de44488f9de0a78aa833eefbabe3ff`, and sidecar SHA-256 `977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5`. The originally selected release candidate remains the separately retained archive from run `39528763022`, recorded below.

Live checks read on 2026-10-21 at task-branch head `6c09cdeca6cd85f90629f31b9dda5a2027c42247` reported four passing checks, zero failed checks, and the candidate job skipped for the PR event. The corresponding [test run 39531769952](https://github.com/swxtchio/swx-opencode/actions/runs/39531769952), [typecheck run 39531769885](https://github.com/swxtchio/swx-opencode/actions/runs/39531769885), and [native proof run 39531769886](https://github.com/swxtchio/swx-opencode/actions/runs/39531769886) each completed successfully. This records those executed checks at their pin; firstmate still owns the review and final merge gate.

## Native candidate and publication evidence

### Native x64 candidate

[Actions run 39528763022](https://github.com/swxtchio/swx-opencode/actions/runs/39528763022) completed successfully on 2026-10-21, using harness head `67cfd0c6dd9796f4ccd4ee4d8c654152b35548d4` and selected source `d1b232e940ef155a3e5311794a2baeaa143af1d8`. The `release-candidate` job ran on `ubuntu-latest`, observed `x86_64` and Bun `process.arch=x64`, installed the selected source's Bun `1.4.2`, performed the frozen linux/x64 dependency install, and ran `bun run packages/opencode/script/build.ts --single --skip-install` with the explicit version and catalog pins. The selected source was checked clean before installation, before building, after building, and after the native probe. No source overlay was applied.

Artifact ID `12314511486`, named `opencode-linux-x64-candidate-39528763022`, contains the archive, companion, build log, actual supplied models.dev snapshot, host/Bun records, source cleanliness records, full provenance JSON, probe JSON and producer server log. The artifact's transport ZIP digest reported by GitHub is `a2caf44d734a9cd3537f571309e156f1cfe41e1621758b3921798f60515d5195`; this differs from the release archive digest below. Download with `gh-axi run download 39528763022 -R swxtchio/swx-opencode --name opencode-linux-x64-candidate-39528763022 --dir <scratch>` while retained by Actions. The native artifact is currently retained locally under the task worktree's `.release-cut/x64-candidate/`.

| Observation | Value |
| --- | --- |
| Archive | `opencode-linux-x64.tar.gz` |
| Archive bytes | `64703288` |
| Archive SHA-256 | `dde0e4f1bd56664ca1d442951b15983f3c5338d1fa84c1bbee1f9b25587dd6ad` |
| Extracted executable SHA-256 | `6891cf61987bd8566660b681217cdfc2605fd684bbf64faaf2cd5072ea0e8e80` |
| FFF sidecar | `libfff_c-977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5.so` |
| FFF sidecar SHA-256 | `977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5` |
| Actual supplied models.dev snapshot SHA-256 | `9c87bdfc4b632ad676ea516cadd60b8fd57902b30d099f85bb9e8692a48ff084` |
| Selected lockfile SHA-256 | `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151` |
| Extracted native version | `1.18.32-swxtch.d1b232e9` |

After downloading the Actions artifact, `sha256sum -c opencode-linux-x64.tar.gz.sha256` succeeded and `stat` measured the table's archive size. `tar -tzf` listed only `opencode` and the named FFF sidecar. A second extraction's executable and sidecar digests, plus a direct hash of the downloaded models.dev snapshot, matched the native provenance JSON. `file` identified both extracted payloads as x86-64 ELF.

The native probe passed the complete acceptance battery:

- Packaged FFF search returned `src/nested/needle-target.ts`.
- New-session wait returned HTTP 200 `idle`; admit-only wait returned HTTP 200 `pending` with the admission's message ID and sequence.
- Completed wait remained client-unsettled while the local Bash tool was running and while its final provider continuation was held. Both held phases identify the same producer wait registration. The terminal result names assistant `msg_15c1fefdb001CUo7TzkCzFZQ4v` and admission sequence `1` for user `msg_15c1feac0001NHzrEmMjGKham2`.
- Terminal-tool failure returned `failed`, with durable tool-failure evidence. Interruption returned `interrupted`, with durable cleanup text `Tool execution interrupted`.
- Restart on `/home/runner/work/_temp/candidate-proof/session.sqlite` retained both completed-case message IDs. Exclusive history replay after sequence `1` returned the identical ordered event IDs for sequences `2` through `12`, without another page.
- The two absolute database selectors returned their own paths, successfully opened stores with `db 'SELECT 1'`, and had distinct physical inodes (`9451613` and `9451619` on device `66305`). The conflicting XDG database was absent; the unset selector returned `/home/runner/work/_temp/candidate-proof/database-selection/unset-xdg/opencode/opencode.db`.

Producer-emitted wait records retained from the native probe, verbatim:

```text
timestamp=2026-10-21T01:42:38.250Z level=INFO run=16433bd3 message="Session wait registered" event=session.wait.registered wait_id=b23b32f4-ca01-44e3-b9db-e3e598712ada session_id=ses_ea3e0154cffewmZU3uqvKhUAUV admitted_seq=1 message_id=msg_15c1feac0001NHzrEmMjGKham2 waiter_count=1
timestamp=2026-10-21T01:42:39.354Z level=INFO run=16433bd3 message="Session wait resolved" event=session.wait.resolved wait_id=b23b32f4-ca01-44e3-b9db-e3e598712ada session_id=ses_ea3e0154cffewmZU3uqvKhUAUV admitted_seq=1 message_id=msg_15c1feac0001NHzrEmMjGKham2 outcome=completed
timestamp=2026-10-21T01:42:42.074Z level=INFO run=5026de4c message="Session wait registered" event=session.wait.registered wait_id=ca57b2e9-871f-4d41-a9e6-924288ce7960 session_id=ses_ea3e006f1ffewfQqJNO0gGukU0 admitted_seq=1 message_id=msg_15c1ff926001VVp6DVO4uRajgg waiter_count=1
timestamp=2026-10-21T01:42:42.911Z level=INFO run=5026de4c message="Session wait resolved" event=session.wait.resolved wait_id=ca57b2e9-871f-4d41-a9e6-924288ce7960 session_id=ses_ea3e006f1ffewfQqJNO0gGukU0 admitted_seq=1 message_id=msg_15c1ff926001VVp6DVO4uRajgg outcome=failed
timestamp=2026-10-21T01:42:44.051Z level=INFO run=5026de4c message="Session wait registered" event=session.wait.registered wait_id=1c925326-fbad-4618-8bdf-8295798c38dc session_id=ses_ea3dffe5effes37ZYpq8EYC0Cb admitted_seq=1 message_id=msg_15c2001ac001v0cIzNzdCSD202 waiter_count=1
timestamp=2026-10-21T01:42:44.927Z level=INFO run=5026de4c message="Session wait resolved" event=session.wait.resolved wait_id=1c925326-fbad-4618-8bdf-8295798c38dc session_id=ses_ea3dffe5effes37ZYpq8EYC0Cb admitted_seq=1 message_id=msg_15c2001ac001v0cIzNzdCSD202 outcome=interrupted
```

### Native ARM64 candidate

VM14 host/root/purpose authorization is recorded in `/home/byates/fm-llmrouter/data/opencode-release-cut/vm14-disposition.md`, "FIRSTMATE DISPOSITION," recorded 2026-10-21T06:35Z. That firstmate disposition cites the captain's explicit 2026-10-21T00:57Z "do it" directive and retroactively authorizes the staging checkout, install/cache, build, transport, test and proof writes on `vm14-001` within `/home/byates/vm14-builds/opencode-release-d1b232e9` for this fixed release source only. The execution described below preceded that record; the disposition supplies the task-specific authorization and does not serve as a contemporaneous execution receipt.

The ARM64 work used `/home/byates/vm14-builds/opencode-release-d1b232e9` on `vm14-001`. A fresh source clone checked out immutable commit `d1b232e940ef155a3e5311794a2baeaa143af1d8`; its V2 wait ancestry check passed, and the source was checked clean before installation, after installation, after building, and after the native probe. HOME, temporary directories, dependency/cache writes, catalog snapshots, build outputs, archives, extraction and runtime proof all used that task root. The host record identifies Linux `aarch64`; the Bun record is `Bun 1.4.2 linux-arm64`.

The frozen all-platform dependency install used `--frozen-lockfile --os="*" --cpu="*"` and a cache within the task root. The unmodified selected source ran `bun run packages/opencode/script/build.ts --skip-install` with `OPENCODE_VERSION=1.18.32-swxtch.d1b232e9`, the absolute `MODELS_DEV_API_JSON` snapshot path, and an absolute `OPENCODE_BUILD_DIST` beneath the task root. The selected ARM64 output was packaged, extracted and tested natively through the selected source's unchanged `release-probe.ts`.

| Observation | Value |
| --- | --- |
| Archive | `opencode-linux-arm64.tar.gz` |
| Archive bytes | `64223558` |
| Archive SHA-256 | `816046ddda7f3e3127c0396cff220aef8bf1ed4b1a7f3f7ad624d560941a8466` |
| Extracted executable SHA-256 | `e14e69ea24931b7cdca7d1a5ddbed9fa7419d788d7f97642f287cd3a658f5884` |
| FFF sidecar | `libfff_c-0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42.so` |
| Actual FFF sidecar SHA-256 | `0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42` |
| Actual supplied models.dev snapshot SHA-256 | `5c24a412697daea8a05a642ef87185ca1392e7073d41e20cb37ba7a16f6c1a14` |
| Selected lockfile SHA-256 | `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151` |
| Extracted native version | `1.18.32-swxtch.d1b232e9` |

The full native probe passed. FFF search found `src/nested/needle-target.ts`; the HTTP outcomes were `idle`, `pending`, `completed`, `failed` and `interrupted`. Completed wait remained unresolved across both the held Bash tool and the held final provider continuation, using one producer wait registration. User `msg_15c6ccc8e001M1kNeqs3nlnS5L` and terminal assistant `msg_15c6cce75001zn359DFxVHR4rF` survived restart on the isolated `/home/byates/vm14-builds/opencode-release-d1b232e9/runtime/session.sqlite`. Exclusive replay after admission sequence `1` matched the original durable event IDs for sequences `2` through `12`. The two explicit database stores opened successfully and had distinct inodes `333123` and `333130` on device `64770`; no conflicting XDG database appeared, and the unset selector matched its channel-default path.

Both the archive and companion validated on VM14. The selected archive, companion, build/install logs, supplied snapshot, host/Bun/cleanliness records and native evidence JSON were transported into the task worktree's `.release-cut/arm-candidate/`; its companion also passed local validation after transport.

Producer-emitted ARM64 wait records retained verbatim:

```text
timestamp=2026-10-21T03:06:36.730Z level=INFO run=0ab76658 message="Session wait registered" event=session.wait.registered wait_id=ae029570-ab84-4ae5-a4c7-e785a55e2ae4 session_id=ses_ea393337bffem10KkxBmP1Sxy4 admitted_seq=1 message_id=msg_15c6ccc8e001M1kNeqs3nlnS5L waiter_count=1
timestamp=2026-10-21T03:06:37.068Z level=INFO run=0ab76658 message="Session wait resolved" event=session.wait.resolved wait_id=ae029570-ab84-4ae5-a4c7-e785a55e2ae4 session_id=ses_ea393337bffem10KkxBmP1Sxy4 admitted_seq=1 message_id=msg_15c6ccc8e001M1kNeqs3nlnS5L outcome=completed
timestamp=2026-10-21T03:06:39.448Z level=INFO run=64af5bcc message="Session wait registered" event=session.wait.registered wait_id=e91e6e4e-3abb-406c-8316-dba17b5deda9 session_id=ses_ea3932945ffesv0nJxJzx0ueMD admitted_seq=1 message_id=msg_15c6cd6cb0013S6BSvrirRf1wP waiter_count=1
timestamp=2026-10-21T03:06:39.590Z level=INFO run=64af5bcc message="Session wait resolved" event=session.wait.resolved wait_id=e91e6e4e-3abb-406c-8316-dba17b5deda9 session_id=ses_ea3932945ffesv0nJxJzx0ueMD admitted_seq=1 message_id=msg_15c6cd6cb0013S6BSvrirRf1wP outcome=failed
timestamp=2026-10-21T03:06:40.664Z level=INFO run=64af5bcc message="Session wait registered" event=session.wait.registered wait_id=acaab8b1-f894-4461-a4c0-2500c61c5d55 session_id=ses_ea39323e0ffelKEZD2Ennzagvz admitted_seq=1 message_id=msg_15c6cdc2a001kzm2ihNTWL30Xn waiter_count=1
timestamp=2026-10-21T03:06:41.542Z level=INFO run=64af5bcc message="Session wait resolved" event=session.wait.resolved wait_id=acaab8b1-f894-4461-a4c0-2500c61c5d55 session_id=ses_ea39323e0ffelKEZD2Ennzagvz admitted_seq=1 message_id=msg_15c6cdc2a001kzm2ihNTWL30Xn outcome=interrupted
```

### Publication and read-back

Published [v1.18.32-swxtch.d1b232e9](https://github.com/swxtchio/swx-opencode/releases/tag/v1.18.32-swxtch.d1b232e9) at 2026-10-21T03:12:35Z with `gh-axi release create`, explicit repository `swxtchio/swx-opencode`, target `d1b232e940ef155a3e5311794a2baeaa143af1d8`, title `v1.18.32-swxtch.d1b232e9`, the committed `RELEASE-CUT-NOTES.md` body, `--latest`, and both archive/companion pairs. The local exact repository allowlist was read before the write.

`gh-axi release view v1.18.32-swxtch.d1b232e9 -R swxtchio/swx-opencode --full` returned the complete provenance body, including Release identity, Build provenance, Release assets, Runtime verification and the carried-fix references. The tag-specific API and `/releases/latest` both returned release ID `419105624`, the intended name and tag, the fixed `target_commitish`, `draft: false`, `prerelease: false`, and all four uploaded assets. `git ls-remote origin refs/tags/v1.18.32-swxtch.d1b232e9` returned the exact fixed source commit.

| Uploaded asset | GitHub asset ID | Uploaded bytes |
| --- | --- | ---: |
| `opencode-linux-arm64.tar.gz` | `661203944` | 64223558 |
| `opencode-linux-arm64.tar.gz.sha256` | `661203943` | 94 |
| `opencode-linux-x64.tar.gz` | `661203939` | 64703288 |
| `opencode-linux-x64.tar.gz.sha256` | `661203947` | 92 |

All four assets were downloaded through `gh-axi release download` into the task worktree's `.release-cut/published/`. Both companion validations passed. The archives matched the selected native-tested archives byte-for-byte with `cmp`. Extracting both published archives reproduced their recorded executable and actual FFF sidecar SHA-256 values, so the published bytes remain linked to the completed native acceptance batteries.

Direct archive URLs:

- ARM64: <https://github.com/swxtchio/swx-opencode/releases/download/v1.18.32-swxtch.d1b232e9/opencode-linux-arm64.tar.gz>.
- x64: <https://github.com/swxtchio/swx-opencode/releases/download/v1.18.32-swxtch.d1b232e9/opencode-linux-x64.tar.gz>.

### Delivery

The release URL, version, archive digests and successful read-back were posted as a [supplemental publication comment on draft PR #326](https://github.com/swxtchio/swx-opencode/pull/326#issuecomment-6307125194). This does not establish the PR as the task completion thread. Completion is reported to firstmate through `/home/byates/fm-llmrouter/state/opencode-release-cut.wake-events`; firstmate owns the task completion thread and its delivery path. The PR's session stamp identifies the code/evidence claim only.

At publication, the crew had not verified Abbe/SaaS recipient task IDs. The supervising home's `data/peers.md` identified Abbe as a peer firstmate rather than an assigned task and contained no SaaS registry entry. The routing question was recorded in [swxtchio/swx-firstmate#2257](https://github.com/swxtchio/swx-firstmate/issues/2257); the task records subsequently verified for round 1 are listed below.

The initial supplemental notifications used the existing confirmed informational peer-message path. Abbe resolved from the registered peer `abbe`; SaaS used the supported explicit `peer:/home/byates/fm-saas` selector. Their `.heartbeat-target` records were read, and `fm-send.sh` revalidated endpoint identity at send time. Each payload carried version `1.18.32-swxtch.d1b232e9`, both complete archive download URLs, both archive SHA-256 values, checksum-companion availability and successful native/read-back verification.

| Recipient | Transport | Delivery receipt | Read-back state |
| --- | --- | --- | --- |
| Abbe | `terminal` | `p1792553644-3991099-6753` | `phase=sent`, `disposition=delivered-no-reply` |
| SaaS | `opencode-http` | `p1792553645-3992254-264e` | `phase=sent`, `disposition=delivered-no-reply` |

The sender-owned records were read back at `/home/byates/fm-llmrouter/state/peer-outbox/<receipt>`. Their complete projected-payload SHA-256 values are `d865d692da4c43defebc82d261098863e74bdc2e12f72bbe20c8bb095604110b` for Abbe and `09b0e68d3192d30436016b30aee161860412fb7f5cce771f30756792b673f4d5` for SaaS. No reply was requested or claimed. Issue #2257 was closed on those peer-message receipts, but those receipts did not satisfy the task's required append to the peers' verified task wake logs. Round 1 corrects that delivery contract through the actual wake-log appends recorded below.

#### Round-1 task wake-log append receipts

Before writing, Abbe task `abbe-jail-swx-opencode-441` was verified in `/home/byates/fm-abbe/data/backlog.md`, where it is queued waiting for the fork release tag and digest. SaaS task `opencode-arm-release` was verified in `/home/byates/fm-saas/data/backlog.md` and its retained `/home/byates/fm-saas/data/opencode-arm-release/brief.md`, "Assigned outcome and authority"; that task is recorded as completed and owns the preceding release/image handoff. Verification establishes the task identities, not a new dispatch or activation.

At 2026-10-21T05:23:17Z, the round-1 release notice was appended with `>>` to each verified task's actual wake-log path:

| Peer | Verified task ID | Wake-log append destination | Read-back receipt |
| --- | --- | --- | --- |
| Abbe | `abbe-jail-swx-opencode-441` | `/home/byates/fm-abbe/state/abbe-jail-swx-opencode-441.wake-events` | Appended release notice present at line 1, timestamp `2026-10-21T05:23:17Z` |
| SaaS | `opencode-arm-release` | `/home/byates/fm-saas/state/opencode-arm-release.wake-events` | Appended release notice present at line 1, timestamp `2026-10-21T05:23:17Z` |

Both complete notices were read back after the appends. Each contains the published release URL, version `1.18.32-swxtch.d1b232e9`, fixed source commit, both full archive download URLs, ARM64 archive SHA-256 `816046ddda7f3e3127c0396cff220aef8bf1ed4b1a7f3f7ad624d560941a8466`, x64 archive SHA-256 `dde0e4f1bd56664ca1d442951b15983f3c5338d1fa84c1bbee1f9b25587dd6ad`, checksum-companion availability and native/published-byte verification results. At this read-back snapshot each log contained the single 771-byte appended line, with file SHA-256 `9210c7468d68185cb71af74ac3c8628a9b38bcab38a41db42dd6e83f160f779c`. These file appends satisfy the required wake-log delivery; the earlier peer-message receipts remain supplemental evidence only.

The later `swx-abbe-1227-native-history` dispatch and downstream deployments remain firstmate-owned and have not been launched by this crew.
