# Release-cut evidence

Evidence collected 2026-10-21 for the release-cut task. This is a dated execution record, not a claim that publication has happened. The harness changes and this record are durably available through [PR #326](https://github.com/swxtchio/swx-opencode/pull/326); the release source remains separate from its task-branch head.

## Selected source

- Source: `d1b232e940ef155a3e5311794a2baeaa143af1d8`.
- Derived version: `1.18.32-swxtch.d1b232e9`; intended tag: `v1.18.32-swxtch.d1b232e9`. Read `packages/opencode/package.json` at the selected commit and apply `local-release.sh`'s source-SHA convention to reproduce the derivation. `packages/script/src/index.ts` and `packages/opencode/script/build.ts` consume the explicit `OPENCODE_VERSION` build pin.
- The selected root `package.json` pins Bun `1.4.2`. The local harness checks used Bun `1.4.2` (`744846f84`).
- Selected `bun.lock` SHA-256: `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151`, measured with `git show d1b232e940:bun.lock | sha256sum`.
- `git merge-base --is-ancestor 4b87478ddf d1b232e940ef155a3e5311794a2baeaa143af1d8` exited zero.
- GitHub's default-branch read returned `swxtch`. The selected release tag was absent from `git ls-remote origin refs/tags/v1.18.32-swxtch.d1b232e9` at pickup; the release list contained the two prior releases. Publication must re-check the live state.

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

The focused file passed: 11 tests, zero failures. Package typecheck exited zero. The new regression runs `release-probe-target.ts` as a child against a real isolated Git remote and checkout; it observes successful candidate resolution and the produced `GITHUB_ENV`, then refuses a missing tag in ordinary proof mode, a conflicting existing tag, a missing explicit source pin, and an unavailable remote.

Bounded absence proof: temporarily replace `const unpublished = candidate && tagLookup.exitCode === 2` with `const unpublished = false`, then run:

```sh
bun test test/release-probe-tools.test.ts -t 'resolves an unpublished candidate'
```

That run exited one with one failed test: the candidate child returned exit one where the regression expected zero. The regression completed without an external timeout. After restoring candidate-mode behavior, the complete focused file again passed and package typecheck again exited zero.

From the repository root, `bun run script/check-workflow-guards.ts` exited zero after reviewing and refreshing the adapted workflow's job and envelope pins. `git diff --check` exited zero. No `bin/fm-lint.sh` is tracked in this project. The project-memory helper reported the existing `AGENTS.md` unchanged.

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

### Remaining release steps

A read-only VM14 check returned `aarch64` and Bun `1.4.2`. No ARM64 candidate build has run: the generated task requires current host/root/purpose authorization before VM14 writes. Completion-thread and peer-task routing must also be established before delivery. ARM64 build evidence, publication read-back, and delivery pointers will be added only after their corresponding executions. No historical archive values are reused as candidate evidence.
