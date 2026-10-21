# Native V2 release scout — 2026-10-21

Task: [swxtchio/swx-abbe#1227](https://github.com/swxtchio/swx-abbe/issues/1227). Deliverable: [swxtchio/swx-opencode#331](https://github.com/swxtchio/swx-opencode/pull/331).

**Producer functional verdict: PASS for the published linux-x64 bytes in the recorded environment. Recommendation: the functional producer dependency is satisfied; hold Abbe consumer cutover.** The actual consumer trace exposes a local-tool metadata incompatibility ([Abbe #1559](https://github.com/swxtchio/swx-abbe/issues/1559)). Real V2 consumer replacement/refusal proof ([Abbe #1556](https://github.com/swxtchio/swx-abbe/issues/1556)) and persistent-cohort V1 inventory remain unestablished.

**Current PR handoff: held for CI investigation.** Run `39576061930` on evidence head `8aaf31ae31` failed the unchanged idle-listener shutdown test; typecheck and e2e passed. [Owning CI investigation #335](https://github.com/swxtchio/swx-opencode/issues/335) and [dated evidence](ci-listener-follow-up.md) record the failure and the still-unestablished cause. A forced dependency probe demonstrates a retired-URL oracle hazard, not the actual CI cause. No retry or gate relaxation is used to label that run green. The passing published-binary observations above remain separately recorded.

**Execution qualification:** the ARM64 task host ran an isolated x86_64 Linux guest under full-system QEMU TCG with `BUN_JSC_useDFGJIT=false` and `BUN_JSC_useFTLJIT=false`; base JIT/FFI remained enabled. The unchanged published binary and packaged sidecar executed directly inside that guest. This is functional release-byte verification, not stock native-x64-hardware, performance, deployed-image or ARM rollout attestation. [Startup evidence](full-system-startup.json) and [execution history](full-system-execution.md) retain the earlier unsuccessful direct/user-mode/JIT attempts.

## Release identity and isolated native store

The [release](https://github.com/swxtchio/swx-opencode/releases/tag/v1.18.32-swxtch.d1b232e9) was reread through `gh-axi release view v1.18.32-swxtch.d1b232e9 -R swxtchio/swx-opencode --full`. Immutable source is `d1b232e940ef155a3e5311794a2baeaa143af1d8`; tag resolution and ancestry of wait commit `4b87478ddf` succeeded. The release's earlier native Actions run was a reproduction lead, not this task's executed acceptance evidence.

| Artifact observation | Observed SHA-256 / result |
| --- | --- |
| [`opencode-linux-x64.tar.gz`](https://github.com/swxtchio/swx-opencode/releases/download/v1.18.32-swxtch.d1b232e9/opencode-linux-x64.tar.gz) and release checksum companion | `dde0e4f1bd56664ca1d442951b15983f3c5338d1fa84c1bbee1f9b25587dd6ad`; `sha256sum -c` returned OK |
| Extracted executable | `6891cf61987bd8566660b681217cdfc2605fd684bbf64faaf2cd5072ea0e8e80` |
| Packaged sidecar | `977a96fc9911edd8cabacb098e90229c6a2dde7384f3291e5beccaf1ee93fef5` |
| Executed `--version` | `1.18.32-swxtch.d1b232e9`, exit 0 |

[Artifact commands](artifact-observations.txt) and guest read-back bind those bytes to what ran. The executable path was `/home/byates/fm-worktrees/fm-llmrouter/swx-abbe-1227-native-history/swx-opencode/.native-history/extracted/opencode`, adjacent to its packaged sidecar. Server and restart both selected `/home/byates/fm-worktrees/fm-llmrouter/swx-abbe-1227-native-history/swx-opencode/.native-history/native-guest-run-5/session.sqlite`; `db path` printed it, and that SQLite store received the real admissions/messages/events. No fallback default DB was opened. Its device/inode stayed `64769` / `85745`, including a later direct stat after restart. Config/data/cache/state/tmp, fixture workspace and guest image remained inside the disposable task-worktree storage; no live tenant store was used.

## Executed admissions, waits and terminal facts

Run start was `2026-10-21T06:48:37.591491Z`. [Raw requests, responses, provider requests, native projections/rows and comparisons](release-run.json) report 50 passing observation checks. [Producer logs](producer-wait.log) independently bind wait registration and resolution to the same wait IDs below.

| Outcome | Native Session | Top-level admitted user ID | `admittedSeq` | Wait assistant ID |
| --- | --- | --- | ---: | --- |
| completed | `ses_ea2c5436fffeMEwu2H5erHsb76` | `msg_zzzz_native_20261021` | 1 | `msg_15d3bce09001TbndqGzdbMWOp5` |
| failed | `ses_ea2c3acd4ffecTvsUW6YQLxAXT` | `msg_failed_native_20261021` | 1 | `msg_15d3cb6d6001Zd1hSkJpFkIpTW` |
| interrupted | `ses_ea2c2d8feffeNewiyCUHOgB1Ic` | `msg_interrupted_native_20261021` | 1 | `msg_15d3d9e0a0016cYJI0rCdqArhl` |

- **Held completion:** wait `776212da-7142-4ea8-88f6-6eafe64c2f2a` stayed unresolved across independently held initial provider, local Bash and required final provider continuation phases. Each observation had positive process-local activity, readable admitted user and matching producer registration. Final release yielded HTTP 200 completed covering sequence 1 and the readable terminal assistant above.
- **Local tool:** `call-completed` belonged to intermediate assistant `msg_15d3b34a6001SJ4abEqweQSmfZ`, completed with output `completed released` and exit 0, and required final continuation. Public success event `evt_15d3b48bf001ysYC2dEqc7ufS2` has sequence 7. Its valid part carries `provider.executed: false` because execution was local.
- **Unrecovered failure:** wait `2302e5ef-bfdf-4ba9-b1d4-990649ca3507` returned failed after continuation settled. `call-failed` retained `Unknown tool: not_registered`; `evt_15d3cb8b90018pPoOQWPPWB2qH` at sequence 7 was tool-failed. A later assistant answer did not erase that failure.
- **Interruption:** wait `edc0e63f-3daa-4c55-996b-201f38fb236d` returned interrupted after active interrupt HTTP 204 and native cleanup. `call-interrupted` retained `Tool execution interrupted`; tool-failed `evt_15d3dab03001wPyyBAVKSbtJ3c` at sequence 7 and step-failed `evt_15d3dabac001JxM22kSq1tjjSl` at sequence 8 followed in order.
- **Controls:** fresh `ses_ea2c564feffeoyfD8VV5iPjxnM` returned idle with no admission/public history. Admit-only `msg_pending_native_20261021` in `ses_ea2c54dc4ffeugrrLC0EwZX7vH` returned pending at sequence 1 with matching message ID. Neither was treated as completed work.
- **Exact retry:** the identical top-level ID, prompt and explicit steer delivery retained the original admission; input/message rows were unchanged, with one user turn. An intentionally new turn then used lexically earlier `msg_0000_native_20261021`, admitted at sequence 13 and completed with assistant `msg_15d3c4bc8001Y4LlNf12ZFWce5`.

## Restart, ordered history and exclusive replay

The server processes were positively reaped before replacement. Restart on the same DB preserved all five Session identities and prior user/assistant/tool projections and public event histories; complete before/after observations are in `release-run.json.restart`. No history was seeded or reconstructed.

Completed history traversed six finite pages at limit 3 to all 18 public events and `hasMore: false`; failed and interrupted histories traversed 12 and 8 public events. The [read-only SQLite backup](native-snapshot.sqlite), obtained through SQLite's backup API after reaping, has integrity `ok`. Its rows independently match every returned public ID/sequence and native message order; [evaluation](native-snapshot-evaluation.json) records the comparison. The only observed nonpublic aggregate type was `session.created.1` at sequence 0, excluded by the release's `SessionDurable` manifest. Sequence gaps are valid; comparisons require strictly increasing unique sequence values, not arithmetic consecutiveness.

The event stream disconnected at observed tool-success cursor 7. Before the deliberate second turn, reconnect replay exactly matched the five-event finite suffix after 7; prefix plus suffix reconstructed the first turn without loss/duplicates. After that new turn and server restart, cursor 7 returned the exact eleven-event suffix of the full two-turn history, again matching stream replay and excluding the anchor. Exhausted cursor 18 returned `data: []`, `hasMore: false`.

Native message order followed source sequences `2, 3, 9, 14, 15`, with the lexically earlier second user still after the first. The V2 ascending message API matched SQLite order. This proves completed-history restart, not recovery of ambiguous in-flight provider dispatch. The inspected `V2Session.wait` remains join-only and does not start post-crash execution/recovery.

The later [explicit join-only negative control](join-only-verification.md) also executed that boundary: a real promoted admission with its provider request held was crash-reaped, then wait on the same DB returned typed HTTP 503 `Session wait outcome is not observable`, with no new drain/provider request or native history change. A separate admit-only input stayed pending after restart wait. All ten supplemental observations passed; [raw results](join-only-run.json) and [exact driver](join-only-commands.py) are retained. The original fifty-check completed-history battery and this supplemental crash control remain distinct runs with the same release-byte/environment qualification.

## Consumer cutover conditions

The [actual caller trace](consumer-trace.md) is pinned to clean Abbe `9060ce607b5065f2f5b462e05300334bd983fe8d`, merged main PR #1526. The gateway passes its persisted user ID through adapter → SessionPool → `OpenCodeClient.prompt_session_v2`, top-level. The path validates wait coverage and obtains `native_event_after` from observed V2 history/replay before persisting it in the local durable pointer owner. These are source observations, not live consumer/cohort execution.

**Concrete compatibility hold:** the valid completed local tool above has `provider.executed: false`, while `_NativeV2Turn.events` requires true and would reject it as unexecuted. The pinned release's [local settlement path](https://github.com/swxtchio/swx-opencode/blob/d1b232e940ef155a3e5311794a2baeaa143af1d8/packages/core/src/session/runner/llm.ts) and [tool event publisher](https://github.com/swxtchio/swx-opencode/blob/d1b232e940ef155a3e5311794a2baeaa143af1d8/packages/core/src/session/runner/publish-llm-event.ts) establish that the flag distinguishes provider-hosted from local execution. [Abbe #1559](https://github.com/swxtchio/swx-abbe/issues/1559) tracks correction and a real producer-driven consumer regression; do not fabricate hosted-execution metadata to clear it.

Before rollout, execute the actual consumer path with complete assistant/terminal tools, missing-message/cursor refusal and same-identity store replacement preserving prior sentinel/history, durable order and writer/actor plus idle-turn/sibling-session fences. [Abbe #1556](https://github.com/swxtchio/swx-abbe/issues/1556) tracks that proof gap: the existing replacement fixture runs V1 with no event cursor; the V2 pool cursor fixture fabricates native values. Neither was executed or labeled passed here.

Persistent-cohort V1 inventory was unavailable in these isolated observations, so **cohort rollout readiness is unestablished**. Positively inventory retained stores/bindings before cutover; a V1 cohort needs ADR-owner-approved in-place migration preserving IDs/order. No live/destructive migration occurred or was authorized. Preserve ADR0031's #1286 amendment: supplied snapshots are ordinary native prompt input retained in audit history; customer projection omission and provider-context compaction are separate obligations, not a non-retention side channel. Snapshot-bearing consumer/compaction behavior and the ADR-selected deployed architecture require their own evidence.

## Reproduction and scope

[`probe-commands.py`](probe-commands.py) is the exact manual driver executed against the published binary: `python3 probe-commands.py <absolute-extracted-opencode> <fresh-absolute-proof-root>`. It creates a fixture provider/workspace and obtains admissions/messages/events/outcomes only from the real producer. Its SHA-256 is `47472184f925aa259e914b6c3e6b740aeaea0ef9a23897cf8ef87f341841159d`; raw `release-run.json` is `bdbe16240ecdba4eac471c06c45c3b76e22a2e5c7acab7371d6361b51e6d09bb`; SQLite backup is `b1c8d2e727964109cdd93bcb8e64e98969186e9a66d82ffbd0201670a110c3d9`. The [source-owned resume procedure](resume-procedure.md) also identifies the original release battery and focused behavior owners; those source tests and prior release runs remained leads, not substituted acceptance evidence.

This publishes manual commands, raw executed evidence and the dated recommendation. No production behavior or production regression tests changed; no changed-production-behavior absence proof is claimed. Consumer implementation, stock-hardware deployment and cohort migration remain outside this task. Leave the PR draft for firstmate review.

Cleanup positively reaped the task-owned QEMU guest with exit 0 after evidence transfer; the shutdown receipt matched its launch PID. The isolated guest image and scratch remain inside the task worktree for inspection, with both native server processes already reaped. The retained SQLite copy is forensic evidence, not a complete tenant checkpoint or a consumer replacement-test pass.
