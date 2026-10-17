# OpenCode fork release evidence

Refs #269; #283

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
- The published ARM and native x64 archives passed the earlier idle/pending/outcome/history/database behavior battery. Those release binaries predate the round-4 wait lifecycle log records, so the round-3 syscall trace is not counted as proof that the main wait handler registered or resolved; the round-4 held-wait result below is from instrumented same-source rebuilds, with published x64 bytes executed separately.
- After restarting the ARM and x64 servers on their same isolated databases, the admitted user and terminal assistant messages remained readable by message ID. History replay after the admission sequence returned the same durable event IDs in strictly increasing exclusive-sequence order.
- On both architectures, two different absolute `OPENCODE_DB` paths over a conflicting `XDG_DATA_HOME` printed their explicit paths; `db 'SELECT 1'` opened two distinct physical stores and no conflicting XDG database. With `OPENCODE_DB` unset, `db path` matched the `latest` channel default under `XDG_DATA_HOME`.

### Round-4 producer-internal held-wait attestation

- The round-3 socket trace is no longer an acceptance signal. `SessionRunCoordinator.join` inserts a server-generated wait ID with session/message/admitted-sequence identity into its process-local waiter registry before awaiting active execution, then emits `session.wait.registered`; `SessionV2.wait` emits `session.wait.resolved` with that same identity and the final outcome. The structured records use the existing core logger and contain no prompt text or credentials.
- `packages/opencode/script/release-probe.ts` reads the isolated producer log at `XDG_DATA_HOME/opencode/log/opencode.log`. Before each held-phase release or interrupt it requires a registration record matching the admitted message and sequence; after the HTTP result it requires a resolution record with the same server wait ID, session, message, sequence, and outcome. The positive path does not use socket bytes, request headers, payload fields, or client-local pending state.
- The focused HTTP-boundary test, `logs producer wait registration and resolution for a held HTTP wait`, observed registration while the provider was held and resolution after release with one wait ID and admitted sequence. Running the same test with `OPENCODE_LOG_LEVEL=ERROR` produced its bounded missing-registration failure, demonstrating that absent log emission cannot pass.
- On native ARM64, the full idle/pending/completed/failed/interrupted/restart/database battery passed against a proof-only rebuild of tag source `0ff0dfbca1f01765f6bd41d0379d9b97f1cf8d23` with the wait-log overlay from commits `bbffec0e22` and `8d6fa2b441`. The overlay SHA-256 was `4ea320236a9cc2e83e080999f2f1971feac3bec0480bb43ffe115aea1c2462d2`, computed from `git diff --binary` over the three core wait files; the build used Bun 1.4.2, `--single --skip-install`, and the exact tag version.

| Target and outcome | Session ID | Admitted message ID | Server wait ID | admittedSeq |
| --- | --- | --- | --- | ---: |
| ARM64 — completed | `ses_eb5c33bafffe40EcYk86eo9YE3` | `msg_14a3cc45d001YPCSLP5oYjvRYs` | `c750e8cf-8b22-4168-992e-35d2f11383a3` | 1 |
| ARM64 — failed | `ses_eb5c3317cffedr7l7lim6Pfd6T` | `msg_14a3cce9e001DlmgzrHBMJ1bvy` | `d850b8a4-efcd-4a58-a540-387f46e5b105` | 1 |
| ARM64 — interrupted | `ses_eb5c32830ffefl2DK9BYlb6eQ6` | `msg_14a3cd7e0001GzKmANx7Sq7lJb` | `cd70ecb0-6e67-448f-9296-180a3976f9b2` | 1 |
| x64 — completed | `ses_eb5db86f6ffeg76jcEJ9xm0OX4` | `msg_14a247918001GUApign08gKRz0` | `649d0a21-7530-4840-b34b-62ededf4fb6a` | 1 |
| x64 — failed | `ses_eb5db7630ffe5jSIJFE5qsmzRD` | `msg_14a2489e3001l3d353nM6DpQxK` | `b139f6ad-6a84-4e54-8667-8cd6d4322d50` | 1 |
| x64 — interrupted | `ses_eb5db6f4effeR72eBVODt7GhaE` | `msg_14a2490c0001W1T1JQRpfHvQwf` | `c241ab37-cf63-4640-9877-fc676bfae95f` | 1 |

- The native x64 held-wait battery ran in Actions run [39038343804](https://github.com/swxtchio/swx-opencode/actions/runs/39038343804) at proof-harness head `0e76aea1dcf411303f981d5bfe8de24654ff3f83` on a proof-only same-source rebuild: tag source `0ff0dfbca1f01765f6bd41d0379d9b97f1cf8d23` plus the same wait-log overlay SHA-256 `4ea320236a9cc2e83e080999f2f1971feac3bec0480bb43ffe115aea1c2462d2`; the instrumented proof archive SHA-256 was `aa9321780a83d78ea381cba6b1eaed30158a575c21d158687378b98b40c08c5f`. The workflow artifact is [opencode-linux-x64-round4-proof-39038343804](https://github.com/swxtchio/swx-opencode/actions/runs/39038343804/artifacts/12103391749).
- In that same x64 run, the workflow separately downloaded and executed the published archive: archive SHA-256 `ce5e6ee0b627f477d5873373bf535a932ba40c02863dc7fd9f45ea81e40e718f`, executable SHA-256 `10e0ae91a4c6f7391a7c1f31e64f27cd8a00e3f7094372137b92ab08ba2d602c`, FFF sidecar SHA-256 `977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5`, and exact tag-version read-back; FFF search and explicit database path/`SELECT 1` also passed. The released x64 binary has no round-4 wait-log instrumentation, so a separate socket-only absence-red ran the held probe against those published bytes: `strace` recorded a complete main-wait POST with its HTTP header terminator, but the probe rejected it for lacking a producer registration record.
- The other removal-reds passed in the same x64 workflow: completed/failed/interrupted main-wait withholding and the client-fabricated partial header prefix (including `X-Release-Probe-Wait-Receipt` but no terminating blank line) both failed at the missing producer registration. The syscall traces are retained only as attack-shape evidence, never as successful attestation.
- The x64-attribution residue is tracked in [swxtchio/swx-opencode#283](https://github.com/swxtchio/swx-opencode/issues/283); this PR records its x64 proof-attribution resolution and leaves the separate release-body and root-document placement items there.

### Regression absence proof

In the authorized staging source, `V2Session.wait` was temporarily replaced with the unconditional `OperationUnavailableError({ operation: "wait" })` body from `f1f278999cac`. The bounded core run returned 27 failures and 4 unaffected passes; the five HTTP wait/terminal-event cases returned 5 failures. The release-probe script itself failed at the newly created Session's HTTP 503 `Session wait outcome is not observable`. No external process kill was used as a test result. The source was restored byte-for-byte, the staging tree read clean, and the focused core/HTTP tests passed again.

## Lease and downstream handoff

The VM14 lease was released through `/home/byates/vm14-leases/release-lease.sh` with the matching `fm-saas/opencode-arm-release` owner record; the authorized `/home/byates/vm14-builds/opencode-arm-release` staging/build/test/proof tree was then removed. `needs-decision: abbe consumer half deferred to its own task`; no Abbe checkout or image registry was written in this task.
