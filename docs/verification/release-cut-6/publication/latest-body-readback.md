# OpenCode fork binary release

Evidence collected 2026-10-25. This source-pinned release carries [#358](https://github.com/swxtchio/swx-opencode/pull/358), global and per-agent small-model selection for V2 title and summary requests, and [#361](https://github.com/swxtchio/swx-opencode/pull/361), V2 project-local tool discovery and execution, which closes [#352](https://github.com/swxtchio/swx-opencode/issues/352). Abbe needs these changes for [#1953](https://github.com/swxtchio/swx-abbe/issues/1953) and [#1307](https://github.com/swxtchio/swx-abbe/issues/1307). It supersedes the older source pin `v1.18.32-swxtch.d7f78881` adopted by [Abbe #1951](https://github.com/swxtchio/swx-abbe/issues/1951).

## Release identity

- Tag: `v1.18.32-swxtch.832df553`.
- Immutable source: `832df553518d8bb581952433c72495a472da8c5d`. The #358 merge commit is `4973723074ed4ba2e778e287eacd3d518cc315d7`, the parent of the #361 source commit. Both landed after Abbe #1951's `d7f78881560aa83e9390f0231562fa79639eacd9` pin.
- Version: `1.18.32-swxtch.832df553`, derived from the selected source package version and commit by `local-release.sh`, then checked by `release-probe-target.ts` with explicit candidate inputs.
- The selected source pins Bun `1.4.2`; the build lock SHA-256 is `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151`. Both builders used the retained `MODELS_DEV_API_JSON` snapshot with SHA-256 `5ecb11a065a7772c9b3730ade26a879ec3e3656e57d66355171d1f9b24508c7e` and `OPENCODE_VERSION=1.18.32-swxtch.832df553`.

## Native release candidates

The x64 clean-source `release-candidate` job passed in [Actions run 40224713450](https://github.com/swxtchio/swx-opencode/actions/runs/40224713450); the instrumented `release-proof` job was skipped. Its workflow harness head and binary source were both `832df553518d8bb581952433c72495a472da8c5d`. The arm64 candidate was built on native Linux `aarch64` at `vm14-001`. Both extracted candidates reported the derived version and passed the unchanged native `release-probe.ts`.

| Target | Archive | Bytes | Archive SHA-256 | Executable SHA-256 | FFF sidecar SHA-256 |
| --- | --- | ---: | --- | --- | --- |
| linux/arm64 | `opencode-linux-arm64.tar.gz` | 64244547 | `cb28771c648dd31fe9caba5d3cc8657438630afd933dd7a24c4d2612f472be76` | `c6653bf327b45cefa14bdb49043230b93edb74cf91163650e16b2d761c4977b5` | `0459657eb78fc6a73a358105f2a6ed06aa9920b28bbe83f2d7ff7b72eae2be42` |
| linux/x64 | `opencode-linux-x64.tar.gz` | 64727596 | `6156af8b96d6763b889636ee0fe986291edaeb17f9d82cd73270774a05309ae7` | `68225a7ee38c7044f77df5d8ba494c79c1e7c1808d55efd12a300637bf5dd51e` | `977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5` |

Each archive contains `opencode` and its matching content-addressed `libfff_c-*.so` sidecar, with a companion `.sha256` asset. Both companions validated against their tested archives before publication.

## Abbe jail acceptance

The jail image was built from Abbe source `8ac1489e7d3ecad9f9a09431d3df54b6b69ad8e6` and the exact unpublished arm64 candidate above. The Dockerfile's archive digest and version checks passed. Image `sha256:7fe44c87982662d608539dd672d5266c54b2add349299927687c0aa47af48cc7` contains the same executable and sidecar bytes as that candidate.

The strengthened `_brain_rig` acceptance passed in both location-only and named-provider V2 modes. Each request advertised `propose_netlist`; a turn-specific provider request selected a valid call to Abbe's real staged module, the durable history recorded its successful output, the provider received the tool result and continued, and the wait completed for the admitted message. The title request received text and did not consume the tool call. The V1 control passed. The manual compaction probe's V2 turn completed, compact returned 204, the summary request carried the turn transcript, and history recorded the provider summary as a manual `session.next.compaction.ended` event.

Jail acceptance was run on arm64 only. The available `vm001` and `vm14-001` Docker hosts are arm64, and VM14 BuildKit advertises only `linux/arm64`; no x64 jail result is claimed. The x64 candidate did pass its native x64 build and probe. Abbe's x64 jail acceptance remains downstream.

The jail run exercised the OpenCode V2 API through Abbe's real jail and `OpenCodeClient`, not Abbe's gateway/SessionPool projection. At inspected Abbe source commit `8ac1489e7d3ecad9f9a09431d3df54b6b69ad8e6`, [`SessionPool._NativeV2Turn.events`](https://github.com/swxtchio/swx-abbe/blob/8ac1489e7d3ecad9f9a09431d3df54b6b69ad8e6/components/orch/services/opencode-variant/session_pool.py) requires `provider.executed=true`, while this successful local tool event records `provider.executed=false`. That consumer-level compatibility remains part of Abbe's acceptance before pin adoption.
