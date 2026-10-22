## Dated source finding

During published-producer verification for swxtchio/swx-abbe#1227 on 2026-10-21, the clean Abbe consumer tree at `9060ce607b5065f2f5b462e05300334bd983fe8d` was inspected. The gateway → adapter → SessionPool serving path now supplies a gateway-derived user-message ID and selects the V2 prompt/wait/history authority. However, `components/orch/services/opencode-variant/tests/test_opencode_jail_integration.py::test_native_store_survives_simulated_host_generation_replacement_with_native_pointers` still drives `_NATIVE_STORE_CLIENT_CODE` through V1 `OpenCodeClient.open_turn`, records `native_event_after: None`, and requires V1 restore-message routes. The pool's V2 cursor test uses fabricated native messages/events. Neither establishes real V2 serving-path replacement recovery.

This is an unexecuted consumer proof gap, not a reported failure of the existing V1 preservation test. Producer release execution remains gated by swxtchio/swx-abbe#1227; this issue tracks consumer work outside the producer-verification task. ADR0031's Native session authority / Atomic turn mechanism and the chat architecture's Behavioral consumer-test obligations own the acceptance contract.

## Required outcome

- Exercise the actual authenticated gateway → adapter → SessionPool V2 writer/read path using the selected, attested OpenCode release and ADR0031's selected runtime architecture.
- Obtain admission, messages, tools and durable events through the real producer, using the gateway-derived top-level native user-message ID; no seeded transcript/event substitutes.
- After completed work and positively fenced/stopped old ownership, replace process/host generation over the same retained store and resolve the same public/native binding, prior message IDs, prior sentinel/history and strictly ordered native event sequences.
- Validate completed wait coverage, terminal assistant and required tools before completion, and show `native_event_after` is persisted from observed native events and survives replacement.
- Prove missing V2 message pointers and a missing saved event anchor refuse admission/completion without silently creating another native session.
- Preserve existing V1 identity/sentinel coverage and writer/actor plus idle-turn/sibling-session fences; demonstrate bounded red proofs at the actual owning behavior boundary for new/rewritten regressions.
- Preserve ADR0031's amended snapshot policy: ordinary prompt input retained in native audit history; customer projection omission and provider-context compaction are independent. No live cohort migration is authorized by this issue.

Refs swxtchio/swx-abbe#1227 and swxtchio/swx-abbe#704. Producer verification evidence and the pinned consumer trace: https://github.com/swxtchio/swx-opencode/pull/331.
