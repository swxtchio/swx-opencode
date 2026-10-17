# OpenCode fork release evidence

Refs #269

Evidence snapshot: 2026-10-17. The release is published at [v1.18.32-swxtch.0ff0dfbc](https://github.com/swxtchio/swx-opencode/releases/tag/v1.18.32-swxtch.0ff0dfbc); the producer issue is swxtchio/swx-opencode#269 and the handoff PR remains draft #282.

## Release identity

- Tag `v1.18.32-swxtch.0ff0dfbc` resolves to source commit `0ff0dfbca1f01765f6bd41d0379d9b97f1cf8d23`, which was the fetched `origin/swxtch` tip selected for the build.
- `git merge-base --is-ancestor 4b87478ddf 0ff0dfbca1f01765f6bd41d0379d9b97f1cf8d23` passed, and `V2Session.wait` in `packages/core/src/session.ts` was inspected at the selected source.
- The version derives from `packages/opencode/package.json` version `1.18.32` and `local-release.sh`'s `<package version>-swxtch.<sha8>` convention. `OPENCODE_VERSION` was pinned to `1.18.32-swxtch.0ff0dfbc` for both builds; both extracted binaries reported that exact string.
- The source tree was clean before and after building. `bun.lock` SHA-256 was `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151` (Git blob `ee6854780d8a5f548b792c4698cd7e4c1726fb3c`), and both builders used Bun `1.4.2`.

## Build provenance

| Leg | Host and target | Build classification | Build command | models.dev API snapshot SHA-256 |
| --- | --- | --- | --- | --- |
| ARM | `vm14-001`, `aarch64`, `process.arch=arm64` | Native linux/arm64 | Frozen all-platform dependency install; `OPENCODE_VERSION=1.18.32-swxtch.0ff0dfbc bun run packages/opencode/script/build.ts --skip-install` (without `--single`, so the source's all-target list emitted both Linux targets) | `364bb0582e825d9f7be9529c45fe4f7712ff3dc309f3901df9a2593e0e9fed94` |
| x64 | GitHub Actions run [38996084960](https://github.com/swxtchio/swx-opencode/actions/runs/38996084960), `ubuntu-latest`, `x86_64`, `process.arch=x64` | Native linux/x64 | Frozen linux/x64 dependency install; `OPENCODE_VERSION=1.18.32-swxtch.0ff0dfbc bun run packages/opencode/script/build.ts --single --skip-install` | `a3cf3efd1f975529f9f4eab37dd0067570990f49bef9ae003e128b9836a02e14` |

`Script.version` reads the explicit build pin, and `packages/opencode/script/build.ts` injects that value as the compile-time `OPENCODE_VERSION`. Its target list selects the current platform and architecture only when `--single` is present; `--skip-install` only skips dependency installation. The selected VM14 build therefore used the full target list, while the authorized CI-native x64 build used `--single` on an x64 runner. The models.dev snapshots were fetched separately and their hashes differ; both values are recorded rather than claiming identical catalog inputs.

The VM14 source staging, dependency install, and pinned full-target build used:

```sh
cd /home/byates/vm14-builds/opencode-arm-release/source
/home/byates/.local/bin/bun install --frozen-lockfile --os="*" --cpu="*" --cache-dir=/home/byates/vm14-builds/opencode-arm-release/cache
HOME=/home/byates/vm14-builds/opencode-arm-release/home TMPDIR=/home/byates/vm14-builds/opencode-arm-release/tmp BUN_TMPDIR=/home/byates/vm14-builds/opencode-arm-release/tmp OPENCODE_VERSION=1.18.32-swxtch.0ff0dfbc MODELS_DEV_API_JSON=/home/byates/vm14-builds/opencode-arm-release/proof/models.dev.json OPENCODE_BUILD_DIST=/home/byates/vm14-builds/opencode-arm-release/build/dist /home/byates/.local/bin/bun run packages/opencode/script/build.ts --skip-install
```

The native x64 command and archive packaging are in `.github/workflows/opencode-arm-release-x64.yml`; run `38996084960` records their executed output and environment.

## Published assets and read-back

| Target | Archive | Archive bytes | Archive SHA-256 | Extracted executable SHA-256 | FFF sidecar SHA-256 | Extracted `--version` |
| --- | --- | ---: | --- | --- | --- | --- |
| linux/arm64 | `opencode-linux-arm64.tar.gz` | 64168079 | `3360cdb84cede93a6cdc49be9873b1bee0fd3ad7a6d788d4fee2d48119e7a1ae` | `02e8f16c04babf3587455d6bfdf9ead23fa02682a35c442fc3fd6bae1d1e14c1` | `0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42` | `1.18.32-swxtch.0ff0dfbc` |
| linux/x64 | `opencode-linux-x64.tar.gz` | 64658975 | `ce5e6ee0b627f477d5873373bf535a932ba40c02863dc7fd9f45ea81e40e718f` | `10e0ae91a4c6f7391a7c1f31e64f27cd8a00e3f7094372137b92ab08ba2d602c` | `977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5` | `1.18.32-swxtch.0ff0dfbc` |

Both published checksum files passed `sha256sum -c` after a fresh `gh-axi release download`. The ARM archive's extracted binary was executed again on native `vm14-001` after download and printed the tag version. The x64 archive's extracted executable SHA-256 matches the executable run natively by Actions in run `38996084960`, whose recorded `--version` output is the tag version; the downloaded archive SHA-256 matches that native-run artifact. The archive contains only `opencode` and its matching content-addressed FFF sidecar. Both sidecars loaded during `debug file search needle-target` and found `src/nested/needle-target.ts`.

The VM14 all-target build also emitted an x64 cross-target candidate with archive SHA-256 `b9e2f6a5d2c8a58621f4f8eb4e2941c68ef1f96f1128664dd872101de1fd8ecf` and executable SHA-256 `7c693e47b8b7cf0dd38723940f27d79617d4f53f1a5b5151a18fd1a2e39ad3fa`; it is not published. `qemu-x86_64` 10.0.13 on the ARM host aborted Bun 1.4.2 with JavaScriptCore `MemoryExhaustion` while running `--version`, matching the ARM/QEMU failure documented in [oven-sh/bun#41087](https://github.com/oven-sh/bun/issues/41087). The native x64 Actions asset is authoritative. A previous native x64 Actions run produced archive SHA-256 `52191690f3de72715f2c21bd1446dca3633ae75eebdfb573678ba9fce2da6944`; its extracted executable and sidecar hashes match the selected run, while the tar metadata differed. The selected, read-back archive is from run `38996084960`.

## Producer verification

- At the release source revision, `bun test test/session-runner.test.ts -t 'SessionV2.wait'` passed 31 tests and `bun test test/database/path.test.ts` passed 1 test from `packages/core`.
- From `packages/opencode`, the five required `httpapi-session.test.ts` cases passed: completed native wait, admit-only pending, terminal event replay after HTTP disconnect, failed terminal tool, and interrupted wait after tool cleanup. `bun test test/cli/smokes/read-only.test.ts -t 'db path:'` passed its CLI boundary check.
- The extracted ARM and native x64 archives each passed the full `release-probe.ts` HTTP battery: newly created Session `idle`; admit-only `pending` with its message ID; `completed` with `admittedSeq` equal to the admission receipt and an `assistantMessageID` resolving to the terminal assistant; terminal-tool `failed`; and `interrupted` after persisted tool cleanup. The completion waiter remained unresolved while the Bash tool was running and while the final provider continuation was held.
- After restarting the ARM and x64 servers on their same isolated databases, the admitted user and terminal assistant messages remained readable by message ID. History replay after the admission sequence returned the same durable event IDs in strictly increasing exclusive-sequence order.
- On both architectures, two different absolute `OPENCODE_DB` paths over a conflicting `XDG_DATA_HOME` printed their explicit paths; `db 'SELECT 1'` opened two distinct physical stores and no conflicting XDG database. With `OPENCODE_DB` unset, `db path` matched the `latest` channel default under `XDG_DATA_HOME`.

### Round-3 held-wait producer receipt

- Each main held-wait request carries a unique header derived from the real prompt-admission receipt (session ID, message ID, admitted sequence, and request ID). Before releasing a tool/provider hold or sending interrupt, the probe requires the matching `POST /api/session/{sessionID}/wait` and header to appear in a syscall trace of the producer process, then reads the admitted message back through `GET /api/session/{sessionID}/message/{messageID}` and checks its server response identity.
- The absence-red test selectively delays only the main `completed`, `failed`, or `interrupted` wait until the real provider-release or interrupt response, while independent admit-only pending requests still reach the server. Each attack fails at the missing producer request receipt; the normal full release-probe run exercises the restored green path.

### Regression absence proof

In the authorized staging source, `V2Session.wait` was temporarily replaced with the unconditional `OperationUnavailableError({ operation: "wait" })` body from `f1f278999cac`. The bounded core run returned 27 failures and 4 unaffected passes; the five HTTP wait/terminal-event cases returned 5 failures. The release-probe script itself failed at the newly created Session's HTTP 503 `Session wait outcome is not observable`. No external process kill was used as a test result. The source was restored byte-for-byte, the staging tree read clean, and the focused core/HTTP tests passed again.

## Lease and downstream handoff

The VM14 lease was released through `/home/byates/vm14-leases/release-lease.sh` with the matching `fm-saas/opencode-arm-release` owner record; the authorized `/home/byates/vm14-builds/opencode-arm-release` staging/build/test/proof tree was then removed. `needs-decision: abbe consumer half deferred to its own task`; no Abbe checkout or image registry was written in this task.
