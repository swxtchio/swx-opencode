## Dated producer/consumer mismatch — 2026-10-21

The published `v1.18.32-swxtch.d1b232e9` linux-x64 binary (source `d1b232e940ef155a3e5311794a2baeaa143af1d8`, executable SHA-256 `6891cf61987bd8566660b681217cdfc2605fd684bbf64faaf2cd5072ea0e8e80`) produced a successful local Bash call during the isolated full-system x64 release probe for swxtchio/swx-abbe#1227. Its native assistant part has `state.status: completed`, output `completed released`, structured exit 0 and `provider.executed: false`; native wait returns completed after the required final continuation. Raw producer-owned messages/events/rows are in `docs/verification/native-history-2026-10-21/release-run.json` at https://github.com/swxtchio/swx-opencode/pull/331. This run is explicitly qualified as QEMU TCG with DFG/FTL JIT disabled; it is not native x64 hardware deployment evidence.

At clean Abbe source `9060ce607b5065f2f5b462e05300334bd983fe8d`, `components/orch/services/opencode-variant/session_pool.py::_NativeV2Turn.events` requires `part.provider.executed is True` for every completed successful tool, otherwise raising `invalid_v2_message_projection` with `native V2 wait completed with an unexecuted tool projection`. That predicate rejects the observed valid locally executed terminal tool. The actual Abbe gateway/adapter/pool chain now selects this V2 path; this is a source-backed compatibility finding, not a claimed executed Abbe integration failure.

The release source owns the meaning: `packages/core/src/session/runner/llm.ts` locally settles tool-call events only when `event.providerExecuted` is false/absent; `publish-llm-event.ts` records that provider-locality flag on Tool.Called and Tool.Success. Thus `provider.executed` denotes provider-hosted execution, not proof that a local executor ran. `packages/schema/src/session-message.ts::AssistantTool` declares the provider metadata separately from terminal tool state. Do not rewrite valid local metadata to true or weaken native completed/terminal validation to clear this mismatch.

## Required outcome

- Align the consumer terminal-tool check with the producer's provider-hosted/local execution semantics while retaining admission coverage, native terminal state, model-pin, native history/cursor and writer/actor fences.
- Drive the actual consumer success/projection path with a real producer-owned local terminal tool whose `provider.executed` is false, and require completion only after the native wait and final assistant/tool projections settle.
- Preserve the false locality flag rather than fabricating provider execution, and prove no duplicate native admission on retry.
- Keep pending/running tools, unrecovered tool failure, mismatched wait coverage and unreadable native pointers/cursors as refusals.
- Add a focused producer-driven regression at the existing consumer boundary; removing the guarded consumer correction must terminate and fail, then restoration must pass.

Refs swxtchio/swx-abbe#1227 and swxtchio/swx-abbe#1556. This consumer fix is outside the published-producer verification task and remains a cutover gate.
