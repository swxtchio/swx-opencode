# OpenCode fork binary release

Evidence collected 2026-10-21. This release carries the [stall fix #303](https://github.com/swxtchio/swx-opencode/pull/303), [auto-compaction #313](https://github.com/swxtchio/swx-opencode/pull/313), [SDK-fixture issue #300](https://github.com/swxtchio/swx-opencode/issues/300) resolved by [#301](https://github.com/swxtchio/swx-opencode/pull/301), [GLM-5.3 issue #305](https://github.com/swxtchio/swx-opencode/issues/305) resolved by [#306](https://github.com/swxtchio/swx-opencode/pull/306), and [queue-force work #314](https://github.com/swxtchio/swx-opencode/pull/314). Their landed commits are ancestors of the selected source.

## Release identity

- Tag: `v1.18.32-swxtch.d1b232e9`.
- Source: immutable `swxtch` commit `d1b232e940ef155a3e5311794a2baeaa143af1d8`, selected by the release-cut task.
- Version: `1.18.32-swxtch.d1b232e9` on both extracted native binaries. The version follows `local-release.sh`'s `<packages/opencode/package.json version>-swxtch.<source sha8>` convention; `OPENCODE_VERSION` was pinned for each build.
- V2 wait ancestry: `git merge-base --is-ancestor 4b87478ddf d1b232e940ef155a3e5311794a2baeaa143af1d8` passed.
- Source tree: clean before and after each build. Both release candidates were built from the unmodified selected source.
- Bun: `1.4.2` on both builders. `bun.lock` SHA-256: `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151`.

## Build provenance

| Target | Build classification | Build command | models.dev API snapshot SHA-256 |
| --- | --- | --- | --- |
| linux/arm64 | Native on `vm14-001` (`aarch64`, Bun `process.platform=linux`, `process.arch=arm64`) | Frozen all-platform install, then `OPENCODE_VERSION=1.18.32-swxtch.d1b232e9 bun run packages/opencode/script/build.ts --skip-install` | `5c24a412697daea8a05a642ef87185ca1392e7073d41e20cb37ba7a16f6c1a14` |
| linux/x64 | Native on GitHub Actions `ubuntu-latest` (`x86_64`, Bun `process.platform=linux`, `process.arch=x64`), run [39528763022](https://github.com/swxtchio/swx-opencode/actions/runs/39528763022), artifact [12314511486](https://github.com/swxtchio/swx-opencode/actions/runs/39528763022/artifacts/12314511486) | Frozen linux/x64 install, then `OPENCODE_VERSION=1.18.32-swxtch.d1b232e9 bun run packages/opencode/script/build.ts --single --skip-install` | `9c87bdfc4b632ad676ea516cadd60b8fd57902b30d099f85bb9e8692a48ff084` |

Each actual supplied catalog snapshot was bound to its build through `MODELS_DEV_API_JSON`; the recorded per-build digests differ. Both builds use the same selected source, package version, Bun pin and lockfile. Build outputs were directed through `OPENCODE_BUILD_DIST`, and logs, snapshots and isolated runtime evidence were retained separately from tracked source. The ARM all-target build's linux/arm64 output and the Actions-native linux/x64 output are the selected release candidates. The x64 candidate workflow and its input validation are recorded in [release-cut PR #326](https://github.com/swxtchio/swx-opencode/pull/326).

## Release assets

| Target | Archive | Archive SHA-256 | Archive bytes | Extracted binary SHA-256 | FFF native sidecar |
| --- | --- | --- | ---: | --- | --- |
| linux/arm64 | `opencode-linux-arm64.tar.gz` | `816046ddda7f3e3127c0396cff220aef8bf1ed4b1a7f3f7ad624d560941a8466` | 64223558 | `e14e69ea24931b7cdca7d1a5ddbed9fa7419d788d7f97642f287cd3a658f5884` | `libfff_c-0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42.so` |
| linux/x64 | `opencode-linux-x64.tar.gz` | `dde0e4f1bd56664ca1d442951b15983f3c5338d1fa84c1bbee1f9b25587dd6ad` | 64703288 | `6891cf61987bd8566660b681217cdfc2605fd684bbf64faaf2cd5072ea0e8e80` | `libfff_c-977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5.so` |

The actual packaged sidecar SHA-256 values are `0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42` for ARM64 and `977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5` for x64. Each archive contains only `opencode` and its named sidecar. Matching companion assets are `opencode-linux-arm64.tar.gz.sha256` and `opencode-linux-x64.tar.gz.sha256`; both companions validated against their archives before publication.

## Runtime verification

- Both packaged candidates were extracted and executed on their matching native builders. Each printed `1.18.32-swxtch.d1b232e9`; each packaged FFF library loaded and `debug file search needle-target` returned `src/nested/needle-target.ts`.
- The controlled-provider native HTTP proof passed `idle`, admit-only `pending`, `completed`, terminal-tool `failed`, and `interrupted` for both candidates. The completed wait stayed unresolved while the Bash tool was held and while the final provider continuation was held. Its admitted sequence matched the admission receipt and its assistant ID identified the terminal assistant.
- Both native probes retained producer-emitted `session.wait.registered` and `session.wait.resolved` records binding the same server wait ID, session, admitted message and sequence to each held wait and its terminal outcome. No source overlay was needed for those observations.
- Restarting each server on its same isolated absolute `OPENCODE_DB` preserved the completed-case user and assistant messages by ID. Exclusive history replay after the admission sequence returned the identical durable event IDs in strictly increasing sequence order.
- On both architectures, two distinct absolute `OPENCODE_DB` selectors over a conflicting `XDG_DATA_HOME` selected and opened distinct physical SQLite stores using `db path` and `db 'SELECT 1'`; the conflicting root contained no database. With the selector unset, `db path` matched the `latest` channel default under `XDG_DATA_HOME`.

| Native target and outcome | Session ID | Admitted message ID | Producer wait ID |
| --- | --- | --- | --- |
| ARM64 completed | `ses_ea393337bffem10KkxBmP1Sxy4` | `msg_15c6ccc8e001M1kNeqs3nlnS5L` | `ae029570-ab84-4ae5-a4c7-e785a55e2ae4` |
| ARM64 failed | `ses_ea3932945ffesv0nJxJzx0ueMD` | `msg_15c6cd6cb0013S6BSvrirRf1wP` | `e91e6e4e-3abb-406c-8316-dba17b5deda9` |
| ARM64 interrupted | `ses_ea39323e0ffelKEZD2Ennzagvz` | `msg_15c6cdc2a001kzm2ihNTWL30Xn` | `acaab8b1-f894-4461-a4c0-2500c61c5d55` |
| x64 completed | `ses_ea3e0154cffewmZU3uqvKhUAUV` | `msg_15c1feac0001NHzrEmMjGKham2` | `b23b32f4-ca01-44e3-b9db-e3e598712ada` |
| x64 failed | `ses_ea3e006f1ffewfQqJNO0gGukU0` | `msg_15c1ff926001VVp6DVO4uRajgg` | `ca57b2e9-871f-4d41-a9e6-924288ce7960` |
| x64 interrupted | `ses_ea3dffe5effes37ZYpq8EYC0Cb` | `msg_15c2001ac001v0cIzNzdCSD202` | `1c925326-fbad-4618-8bdf-8295798c38dc` |
