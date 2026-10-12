# V1 machine queue dispatch

The fork-owned `packages/opencode/src/session/queue.ts` owns admission, selection and promotion. At an eligible delivery boundary, validated peer envelopes precede automation, regardless of automation age. At most ten pending peers in original sequence order become one numbered user message. Every entry preserves its full literal envelope and admission timestamp. File inputs remain intact and receive their numbered peer attribution. Overflow remains pending for the next peer-first batch. Native and unknown requests remain individual; a direct human steer retains its existing admission-time behavior. Critical peer steers retain their critical framing and immediate delivery. Critical or send-now automation stays individual.

Validated automation coalesces into one delivery per generated family/subtype: heartbeat, watcher wake subtype, watcher lifecycle, framed observer/firstmate level, or keyed nudge. Repeated check-file episodes show their latest admitted text as historical provenance, never as a current measurement. Distinct check files and heartbeat nudge duties remain represented. Unknown framing, synthetic editor context and requests carrying incompatible execution controls (format, system, tool overrides or agent/subtask input parts) are not inferred to be equivalent; those original inputs remain individual. Nothing is retired by age or arbitrary body similarity.

The Firstmate producer owns its minimum three minutes since the last positively delivered watcher update and remeasurement at each eligible update. Retained notices identify candidates, historical observations and acknowledgment obligations. They cannot certify that a condition is still true: fresh producer evidence must establish unchanged, changed, resolved or unknown state. OpenCode only selects and aggregates the queue; it does not probe conditions, implement that pacing or label newest queued text as freshly measured.

Selection reserves original rows against one message ID in an immediate transaction. New arrivals are not added to an already prepared group. Withdrawal or an edit invalidates the entire preparation; the other originals remain pending. Message info, all parts and every constituent's promotion commit together. Notifications are released only after that outer write commits. Failed grouped preparation/write leaves originals available for retry. Native preparation failures retain the existing reject-and-continue behavior.

Machine promotion uses the recipient's current agent, model and variant. It rechecks selection in the write transaction and prepares again if selection changed meanwhile. Original sender settings remain in metadata rather than replacing recipient selection.

## Exact constituent introspection

The normal message response exposes `TextPart.metadata.sessionQueue`:

```json
{
  "version": 1,
  "sessionID": "ses_...",
  "kind": "peer",
  "constituents": [
    {
      "id": "que_...",
      "sessionID": "ses_...",
      "seq": 42,
      "delivery": "queue",
      "input": {
        "messageID": "msg_original_if_supplied",
        "parts": [{ "type": "text", "text": "complete original literal envelope" }]
      },
      "time": { "created": 1791800000000 }
    }
  ]
}
```

`input` preserves the entire original encoded queued input, including parts and supplied settings. The numbered display contains selected historical producer observations; metadata also preserves earlier same-episode automation constituents whose display was superseded. Pending queue responses continue to contain individual originals. Existing projected messages without this metadata retain their existing meaning.

Stable supplied message IDs and validated peer sender/home/msgid identities also get ignored, synthetic text parts with deterministic internal `prt_queue_...` IDs. A peer with an explicit OpenCode message ID gets both transport-ID and sender/home/peer-msgid aliases, so omission or replacement of the transport ID on an exact peer retry does not redeliver it. Each alias contains the same metadata version with its single original constituent. These parts support indexed exact retry after the normal queue consume removes promoted rows; they are excluded from model input. Conflicting reuse fails instead of delivering changed work under an existing identity. They are not a new acknowledgment or receipt route. Part/session retention follows existing message/session ownership; no extra receipt store or audit queue is retained.

A consumer must validate version/session and find the **exact original** constituent, not compare the whole outer numbered text, match a body substring or infer success from HTTP204. Metadata is part of a committed message, not proof the model answered the constituent. Peer replies/acks target each response-owed original peer msgid using existing `re:<original-msgid>` handling; informational entries create no new reply obligation. Outer group IDs confer no authority and cannot acknowledge unrelated requests.

Firstmate's existing `bin/fm-opencode-http-lib.sh` exact pending/session evidence lookup and watcher accepted-payload lookup need a minimal metadata-aware companion before rollout. Its existing sender/outbox lifecycle remains the owner of acknowledgments. No new public requestID/receipt endpoint is introduced, and no public HttpApi shape changes: text metadata already permits structured JSON.

## Ordered history lookup

`message_session_admission_order_idx(session_id, admission_seq)` supplements partial positive-sequence uniqueness. Unfiltered page/snapshot queries can use the lookup index while still including legacy sequence-zero rows. The generated fresh schema and upgrade migration preserve the partial unique index and existing compaction/pagination behavior.

Focused regressions exercise the actual Bun SQLite layer, independently measured old/new/rollback read and timer behavior, fresh/upgrade query plans, real durable queue writes and real provider requests. No live database index build, queue rewrite, process restart or rollout is part of this implementation.
