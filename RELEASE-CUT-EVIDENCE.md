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

Native build observations, archive digests, extracted executable and sidecar digests, acceptance results, Actions artifact linkage, publication read-back, and delivery pointers will be added only after their corresponding executions. No historical archive values are reused as candidate evidence.
