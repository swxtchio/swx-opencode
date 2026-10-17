# OpenCode fork binary release

Release for swxtchio/swx-opencode#269. Evidence collected 2026-10-17.

## Release identity

- Tag: `v1.18.32-swxtch.0ff0dfbc`.
- Source: immutable `swxtch` commit `0ff0dfbca1f01765f6bd41d0379d9b97f1cf8d23`; it is the fetched `origin/swxtch` tip selected for this release.
- Version: `1.18.32-swxtch.0ff0dfbc` on both extracted binaries. The version follows `local-release.sh`'s `<packages/opencode/package.json version>-swxtch.<source sha8>` convention; `OPENCODE_VERSION` was pinned for each build.
- V2 wait ancestry: `4b87478ddf` is an ancestor of the selected source commit. `V2Session.wait` was inspected at that revision.
- Source tree: clean before and after each build.
- Bun: `1.4.2` on both builders. `bun.lock` SHA-256: `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151`.

## Build provenance

| Target | Build classification | Build command | models.dev API snapshot SHA-256 |
| --- | --- | --- | --- |
| linux/arm64 | Native on `vm14-001` (`aarch64`, Bun `process.arch=arm64`) | Frozen all-platform install, then `OPENCODE_VERSION=1.18.32-swxtch.0ff0dfbc bun run packages/opencode/script/build.ts --skip-install` | `364bb0582e825d9f7be9529c45fe4f7712ff3dc309f3901df9a2593e0e9fed94` |
| linux/x64 | Native on GitHub Actions `ubuntu-latest` (`x86_64`, Bun `process.arch=x64`), run [38996084960](https://github.com/swxtchio/swx-opencode/actions/runs/38996084960), artifact `12087112630` | Frozen linux/x64 install, then `OPENCODE_VERSION=1.18.32-swxtch.0ff0dfbc bun run packages/opencode/script/build.ts --single --skip-install` | `a3cf3efd1f975529f9f4eab37dd0067570990f49bef9ae003e128b9836a02e14` |

The models.dev API snapshots are per-build inputs and their recorded digests differ. Both builds use the same selected source commit, package version, Bun version, and lockfile.

## Release assets

| Target | Archive | Archive SHA-256 | Archive bytes | Extracted binary SHA-256 | FFF native sidecar |
| --- | --- | --- | ---: | --- | --- |
| linux/arm64 | `opencode-linux-arm64.tar.gz` | `3360cdb84cede93a6cdc49be9873b1bee0fd3ad7a6d788d4fee2d48119e7a1ae` | 64168079 | `02e8f16c04babf3587455d6bfdf9ead23fa02682a35c442fc3fd6bae1d1e14c1` | `libfff_c-0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42.so` |
| linux/x64 | `opencode-linux-x64.tar.gz` | `ce5e6ee0b627f477d5873373bf535a932ba40c02863dc7fd9f45ea81e40e718f` | 64658975 | `10e0ae91a4c6f7391a7c1f31e64f27cd8a00e3f7094372137b92ab08ba2d602c` | `libfff_c-977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5.so` |

Upload the matching companion checksum file for each archive: `opencode-linux-arm64.tar.gz.sha256` and `opencode-linux-x64.tar.gz.sha256`.

## Runtime verification

- The extracted ARM64 asset ran natively on `vm14-001` and printed the tag version. Its packaged FFF library loaded: `debug file search needle-target` returned `src/nested/needle-target.ts`.
- The extracted x64 asset ran natively on the GitHub Actions x86_64 runner and printed the tag version. Its packaged FFF library loaded and found the same fixture.
- The controlled-provider native HTTP proof covered `idle`, admit-only `pending`, `completed`, terminal-tool `failed`, and `interrupted`. The completed wait stayed unresolved while the Bash tool was held and while the final provider continuation was held; its result's `admittedSeq` matched the admission receipt and its assistant ID was terminal.
- After a process restart on the same isolated `OPENCODE_DB`, the user and assistant messages remained readable by message ID. `GET /api/session/{id}/history?after=<admittedSeq>` returned the same durable event IDs in strictly increasing exclusive-sequence order.
- Two distinct absolute `OPENCODE_DB` values over a conflicting `XDG_DATA_HOME` each selected and opened their own physical SQLite store (`db 'SELECT 1'`); the conflict root contained no database. With the selector unset, `db path` matched the `latest` channel default under `XDG_DATA_HOME`.
