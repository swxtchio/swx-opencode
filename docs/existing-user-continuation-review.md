# Exact-assistant continuation review package

Repository: `swxtchio/swx-opencode`. Proposed contract for review only; no route, schema, migration, claim writer or owner entry is implemented by this package. Captain approval of the additive external API is required before implementation. This expands the accepted [1981/2024 phase plan](existing-user-continuation-plan.md) at `3ac3c30e4cc03a3aa7af98df1b543b6d68b59570`, preserving the Abbe scope addition at `f02b4244795180e31a5ed7dff6373122248d0c2c`. Runtime baseline remains `5003b09dac11ec34c73e33bbe3be4df66f0b8509`. The [decision register](existing-user-continuation-decisions.md) separates the captain decision, engineering review gates and unresolved historical custody.

The consumer is Firstmate's exact-session reviewer journal owner. The producer remains V1 Session HTTP → SessionPrompt → SessionRunState/Runner → the normal SessionProcessor/provider/tool pipeline. No direct raw `loop`/`drain`, new input admission, failed-root retry, second scheduler, process replacement or historical effect replay is part of this contract. No Sol/Abbe settlement is implied by approval of this package.

## GET: exact assistant baseline

Proposed resource: `GET /session/:sessionID/message/:assistantMessageID/continue`. Reuse existing Authorization, workspace routing and Instance context. Existing session/message/queue GET responses and routes remain unchanged. The producer checks that the requested message is an assistant in that exact session and derives its user from the persisted `parentID`; it does not pair independently selected latest user/assistant records.

The proposed successful inspection response is HTTP 200 with this shape; the notation is a contract specification, not generated or executed output:

```typescript
{
  target: { sessionID: SessionID, userMessageID: MessageID, assistantMessageID: MessageID },
  revision: string,
  owner: { writerID: string },
  disposition: "eligible" | "busy" | "refused" | "unconfirmed",
  reason?: Reason
}
```

`reason` is absent only for eligible; every other disposition has one concrete reason from the table below. `writerID` is the current producer's registered writer identity, not a session execution lease. The opaque revision binds that process lifetime and placement as well as the selected snapshot; another serving owner cannot accept it as its own. Missing/foreign session-message linkage uses the current not-found handling. Authentication, authorization and malformed identifier errors follow the existing middleware/errors. An inspection unable to establish a coherent exact baseline must fail or return unconfirmed; it cannot manufacture an eligible revision from partial reads.

GET performs no claim insertion, prompt admission, queue modification, runner adoption, provider request, tool execution or wake. Normal server/database initialization is not a new recovery operation. The baseline comes from the already selected serving owner; a client must not instantiate another server/store to obtain it. The response is a pre-action observation, not a durable reservation or permission token. Never cache eligibility across a later action.

The producer-defined revision must bind the exact original user/info/parts and persisted admission order; original assistant/info/parts; later user/attempt boundaries; earlier unresolved tool/compaction/subtask custody; pending/reserved/promoted-unconsumed queue identities; root-related withdrawn holds; claim existence; original placement and frozen execution profile; and current writer identity. Completed prior effects remain retained history. Use a coherent producer-owned canonical snapshot/digest, with a versioned opaque wire representation; do not use an assistant creation timestamp or local status alone. No prompt body, complete transcript, command text, credentials or raw environment is returned. The precise encoding/coverage is an engineering gate in the decision register, not an already implemented hash contract.

An unrelated withdrawn hold is retained and is not restored or deleted. A hold that makes this root's custody ambiguous prevents eligibility. A fresh critical user or its empty child cannot clear an earlier running tool's uncertainty. Returning eligible requires positive ownership and no unresolved execution/effect, not just absence from a status map.

## POST: conditional exact-assistant execution

Proposed resource: `POST /session/:sessionID/message/:assistantMessageID/continue`, same middleware/placement. Strict payload:

```typescript
{ userMessageID: MessageID, expectedRevision: string }
```

Reject additional execution-bearing fields: no text, parts, command, new message ID, model/effort override, root substitution, force flag, queue mode or failed-retry flag. The path fixes the retained assistant; the body must match its original parent user. POST re-derives and validates the complete baseline at the atomic execution owner. A preceding eligible GET does not authorize a raced state.

Only a durably committed, locally installed claim produces HTTP 202:

```typescript
{
  disposition: "accepted",
  target: { sessionID: SessionID, userMessageID: MessageID, assistantMessageID: MessageID },
  baselineRevision: string,
  owner: { writerID: string }
}
```

`baselineRevision` is the accepted pre-action revision. No generic operation/message receipt ID is added: the exact assistant uniquely identifies this one-shot claim. The body means the owner accepted this exact attempt, not that a provider request, token, tool result, completion or reviewer verdict exists. Both the claim commit and local ownership installation must precede accepted publication or yielding to an observer.

Guarded conflicts use a proposed typed HTTP 409 error with a discriminated disposition:

```typescript
{
  _tag: "SessionContinuationConflict",
  disposition: "busy" | "refused" | "unconfirmed",
  target: { sessionID: SessionID, userMessageID: MessageID, assistantMessageID: MessageID },
  reason: Reason,
  message: string
}
```

This is a proposed error shape to express the existing plan's distinctions, not a currently declared Schema.TaggedErrorClass. Foreign/missing linkage remains not-found; invalid payload/revision uses existing bad-request handling. An unconfirmed guarded conflict starts no candidate work. A transport loss or generic failure **after** a claim might have committed is delivery uncertainty, not such a pre-action conflict: retain the exact IDs/revision, reobserve and never automatically resubmit. Do not promise no work from a missing response or generic server error.

| Disposition | Proposed reason | Owner meaning and effect |
| --- | --- | --- |
| busy | `runner_busy` | Running, Shell or ShellThenRun won; do not join, defer or execute candidate |
| refused | `revision_changed` | Coherent current baseline differs from expected revision; no claim/provider work |
| refused | `root_mismatch` | Same-session user is not this assistant's original parent; no substitution |
| refused | `root_not_latest` | Original root is superseded by a later admitted user/attempt; no stale-root run |
| refused | `assistant_terminal` | Completion/error/terminal finish/structured result exists; no failed-root retry |
| refused | `assistant_not_empty` | Retained child has output/work outside the approved empty shape |
| refused | `competing_input` | Pending/reserved/unconsumed input or root-ambiguous hold invalidates selection |
| refused | `already_claimed` | Durable claim exists; no second run or exact-match admission fallback |
| refused | `placement_changed` | Session/Instance/workspace/frozen execution profile no longer matches |
| refused | `retention_blocked` | Existing source-owned retention/deletion guard prevents the operation |
| unconfirmed | `execution_custody_unknown` | Native/foreign/provider/background/permission custody is not positively settled |
| unconfirmed | `tool_custody_unknown` | Earlier or selected original tool record has no attributable outcome/handle |

Use a deterministic guard order in implementation: resolve/authenticate the target; reject non-idle owner; validate parent/latest/shape and source-owned write eligibility; compare revision and competing custody; establish positive execution/tool custody; conditionally insert the claim. The transaction rechecks all predicates that can change. An existing claim must always prevent execution even if a different earlier guard is the reported reason. No conflict response is a terminal tool or assistant result. Do not expose a foreign root's transcript in an error.

## Private claim and lifetime

Recommend one Core-owned `session_v1_continuation_claim` row for one original assistant. Its proposed persisted fields are:

| Column | Contract |
| --- | --- |
| `assistant_id` | Primary key; original assistant identity, not a newly allocated attempt |
| `session_id` | Original session; session-owned cleanup/retention linkage |
| `user_message_id` | Original parent root, checked against the retained assistant |
| `baseline_revision` | Exact producer baseline that the claim accepted |
| `writer_id` | Serving process that bound the claim |

A unique conditional insert in the final immediate transaction chooses one winner among concurrent requests using this operation. Do not add an execution-success flag, model token receipt, guessed historical input, watcher lookup or alias framework. Progress/error/tokens/tool results remain in the original assistant and its normal source-owned records. The claim does not establish absence of a legacy/uncooperative foreign owner; unknown foreign execution still prevents eligibility.

Preserve the claim across setup failure, provider failure, successful completion, HTTP cancellation/loss and unknown process exit. A second request never removes or resets it to retry. Recommended cleanup boundary is explicit whole-session deletion/retention with its retained original custody, not standalone assistant/part deletion, compaction, queue consume, or a time-to-live. A message-only cascading foreign key would erase the duplicate barrier while the user/session survives; do not choose that shortcut. The exact source-owned artifact classification, session index/foreign key and migration/rollback proof are review gates before implementation. An old binary can lack this API, but it must not be used to replay a claimed incomplete assistant or drop live claims as a rollout rollback.

This table and its migration do not exist in this package. No live schema write, read, copy, checkpoint, pruning or data repair is performed.

## Idle-only owner transition

Add a private, narrowly named conditional entry to SessionRunState/Runner instead of changing existing ensureRunning semantics. The proposed owner transitions are:

| Starting owner | Action result | State/result custody |
| --- | --- | --- |
| Idle, unchanged eligible baseline, no claim | Commit claim; install one gated candidate run; accepted | Same session/root/assistant and current serving owner |
| Running | busy | Existing run untouched; no join |
| Shell | busy | Existing shell untouched; no delayed run |
| ShellThenRun | busy | Existing shell/pending run untouched |
| Idle, stale/foreign/terminal/queued/nonempty baseline | refused | No candidate claim/run or changed assistant |
| Idle, uncertain execution/tool owner | unconfirmed | Preserve historical state; no dispatch |
| Idle, prior claim | refused | Retain original claim; no restart or second execution |

The owner must inspect and conditionally transition under its existing SynchronizedRef serialization, with the short queue admission boundary and immediate DB transaction protecting the final root/revision/claim checks. Never hold a queue semaphore or database transaction across provider/tool IO. Candidate work must remain behind a gate until commit, owner installation and release of the short admission boundary; do not fork ungated provider setup and later announce ownership. This is a required protocol to prove, not pseudocode that bypasses the real runner.

Bind the winning claim to the successful database outcome before accepted observers can yield. If another normal prompt/shell start wins, the recovery candidate refuses. If recovery wins, subsequent ordinary work observes the same installed owner and follows current source policy; it cannot overwrite this ownership transition. A losing queue/root/assistant change invalidates the revision. A failure before commit leaves no persisted claim/run. A committed claim followed by owner-install/setup failure remains retained and produces an attributable failure or uncertainty; it must not silently delete the barrier or start again.

The runner is process/Instance-local today. New-operation uniqueness is cross-process at the claim insert, but it does not retrofit a clustered execution lease into ordinary prompt/shell callers. Source-owned positive serving-writer/foreign-owner checks therefore remain necessary. Writer registration and PID/boot/start-tick identity are leads to exact process lifetime, not provider/tool completion proof; missing/unreadable evidence produces unconfirmed, never implicit takeover. Do not reuse EventV2 replay owner claims as an execution lease.

After the guarded claim, reuse normal provider/processor preparation with the original selected user and **the existing empty assistant object/ID/parent/creation time**. Disable failed-root retry. Preserve earlier completed effects as history without rerunning them. The actual private control already demonstrates that native processor output can progress this exact child; production implementation must also preserve normal system/profile/tool/permission setup and actual failure causes.

The selected turn ends at its terminal/would-idle boundary without blanket drain or initiating another pending queued turn. Genuine arrivals during that run retain existing steer/safe active-peer continuation and original reply custody; no reclassification, ten-cap bypass, scheduler or tool interrupt is introduced. Subsequent native tool continuations are legitimate new work of this selected turn, not adoption of historical pending/running records. Original target IDs remain the acceptance association even if later participants change final answer attribution; another root's last assistant is not the resumed witness.

## Observation and consumer handoff

Before attempting POST, Firstmate retains the original journal attempt, session/user/assistant linkage, GET revision, serving route/process/store/worktree identity, and immutable round/pin/profile evidence. It does not reconstruct a new user body or substitute another endpoint. Unsupported API/404 or ineligible/unconfirmed inspection stops this operation; no fallback prompt, command, queue restore, raw loop or endpoint rebuild is authorized.

After accepted or uncertain transport, observe the same original assistant and its normal Session events against the **pre-action revision**, not its creation time. Distinguish durable claim/HTTP acceptance, actual first provider request, token/tool progress, terminal native outcome and owed reviewer artifact/verdict. Completed tool output must be attributable to the original call; an error/refusal does not waive a missing review. The same notification/request is not resubmitted because an earlier transport report said unconfirmed.

GLM is only a candidate if the exact idle/empty-child/queue/profile/owner baseline can be positively established; this preparation has no live eligibility result. Sol's four running read records and Abbe's earlier running bash are **unconfirmed historical custody**, not healthy idle witnesses. Sol's original assistant cannot be adopted into a fresh processor's empty tool-call map. Abbe's newly admitted critical user cannot inherit an older assistant's root or tool outcome. Preserve their exact record sets and the separate native handle/result or historical policy prerequisites in the accepted phase plan. The public API decision must not become authorization to settle or replay them.

## Review evidence and implementation boundary

The [committed producer controls](../packages/opencode/test/session/existing-user-resume-controls.test.ts) at `3ac3c30e4cc03a3aa7af98df1b543b6d68b59570` passed eight controls/47 assertions and the OpenCode package typecheck on 2026-10-13. They prove the message-history-empty guard, real same-assistant processor feasibility, actual busy/foreign/stale loop behavior and lack of historical tool adoption. These are prior source/private-runtime results; this documentation package makes no new producer call or raw-loop run, and does not claim that an idle-only API/claim implementation has passed tests.

After captain approval, the future producer proof must include actual generated GET/POST shapes; same-target normal progress; simultaneous requests; a held accepted observer; a busy/shell transition after eligible GET; foreign/stale/changed roots; queue/reservation and older-tool races; claim-commit versus owner-install/setup/cancellation failure windows; original completed-effect history; retention and old-binary rollback; and terminal/would-idle stopping. A losing candidate must make no provider call or input admission. Sol/Abbe-shaped cells must refuse before historical model conversion/redispatch/terminalization. Use source-removal reds for ownership/claim/identity/stop guards, unchanged readiness bounds and actual runner/setup evidence. Do not expand to a full suite or weaken failure evidence with synthetic readiness.

Public declarations belong in the existing V1 HTTP group and schemas, handlers at the existing Session owner, claim storage/migrations in Core, and execution logic in the native SessionPrompt/RunState/Runner chain. Keep Schema → Core/Protocol → Server dependency direction. Generate legacy SDK with `./packages/sdk/js/script/build.ts` and affected Client outputs with `bun run generate` from `packages/client`; do not edit generated files manually. Firstmate consumer work starts only after the implemented producer contract/capture receives independent review. Current package changes are documentation only.

Authoritative code: `SessionPaths`/`SessionApi` and session/queue handlers under `packages/opencode/src/server/routes/instance/httpapi`; `loop`, `drain`, `runLoop`, `lastAssistant` in `packages/opencode/src/session/prompt.ts`; `current`/`ensureRunning` in `session/run-state.ts`; `Runner.make`/`ensureRunning`/`startShell` in `effect/runner.ts`; `SessionQueue.empty`/`exclusive`/`writable`/`consumeAttempt`; `SessionProcessor.create`/`readToolCall`/`completeToolCall`; `MessageV2.latest`/`toModelMessagesEffect`; `ShellTool.run`; and Core `EventWriter`/writer SQL. The accepted phase plan links those exact files and the recorded Firstmate/SaaS witnesses. Direct owner-source reads remain authoritative after the bounded gbrain discovery timeout; no denied graph read or unavailable index is used as a reason to guess.
