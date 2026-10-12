# Captain queue policy and ordered history lookup

Authority: `/home/byates/fm-llmrouter/data/opencode-message-latency-p1/captain-queue-policy.md` and implementation-handoff.md (2026-10-12). Existing branch `fm/opencode-message-latency-p1`; no rollout, live store changes, push, PR, merge, package installation, or full/derived suite.

## Design

Keep `session/queue.ts` the single scheduling owner. At an eligible queued boundary select validated peer envelopes before automation, up to ten original rows in seq order. Deliver complete literal envelopes as numbered entries with admission timestamps, lower-authority framing and explicit acknowledgment of each original peer ID. Leave overflow pending. Native/manual/unknown inputs retain their own message and existing delivery semantics; critical inputs keep immediate meaning.

For validated automation families/subtypes, select one type together. Keep latest admitted text for positively identified same episodes/checks, explicitly labeled as historical observations, retain every distinct actionable episode, and preserve all original constituents for introspection. Classification is based on validated generated framing, never free-form semantic similarity. Admission and promotion use this same selection path, including previously stored rows.

Reserve all selected original rows against one message ID in BEGIN IMMEDIATE. Revalidate every constituent before commit; withdrawal, edits or new higher-priority work invalidate preparation without deleting originals. Preparation/write failures and restart must not lose a group. Persist constituent identity and original encoded input/time in the existing text-part metadata surface (`sessionQueue`, version1); a group is visible through normal message GET, no generic requestID/receipt endpoint or new scheduler. Pending queue GET continues to expose original rows. After promotion/consume, the projected text metadata is the durable exact-constituent lookup. Firstmate's localized HTTP exact pending/session evidence consumer owns metadata-aware constituent confirmation under its accepted coordination; no edits to its primary here.

Machine delivery resolves recipient current model/variant at promotion; retain sender input in constituent metadata rather than letting old automated selections restore it. Explicit native selection stays authoritative.

Add a nonunique `(session_id, admission_seq)` index alongside partial admission uniqueness. Generate migration, schema snapshot/registry with the core generator. Preserve zero-sequence inclusion, legacy pagination and compaction behavior. Test fresh and upgrade stores, actual Bun driver query plan and observable connection/timer regression under a discriminating synthetic workload.

## Focused execution and validation

1. Commit this plan; inventory/exclude `.latency` scratch and reuse existing dependencies within worktree without installs.
2. Add ordered lookup/migration and focused query-plan, upgrade/fresh and actual-driver regression proof, including remove-index red.
3. Add validated dispatch metadata/classification and atomic multi-row queue reservation/promotion; test real durable queue through existing service fixtures and actual model request capture.
4. Exercise ten/eleven peers, peer-before-old-automation, complete numbered framing, same-type latest queued observation and distinct episodes, unknown/manual/critical preservation, concurrent admission/withdrawal, preparation/write failure, restart/retry, current model/effort and exact constituent lookup after consume.
5. Run only focused related queue/history/machine tests, package type/lint checks, targeted behavior-removal reds, and review. Commit intended files only; update report with clean head, evidence and cross-repo needs.

## Parked #212 integration

Inspected parked head `bc7592ea56` and review-current-intent.md. It adds `SessionPromptProvenanceTable`/Origin timestamps, queue editor hold/reorder/consolidate and current-model selection. Its provenance holds timestamps only, not exact constituents, and its requestID/receipt proposal is explicitly unapproved. This branch uses existing text metadata for exact constituent input instead of introducing a competing provenance table. Queue reserve/commit changes overlap directly; integration must merge these into the existing hold/reservation checks and combine origins with constituent metadata, preserving its current-model-at-promotion hook. No parked TUI/retention/compaction work is copied or overwritten.

The independent index fix can land without #212/PR228 or #2081. Grouping cannot be deployed as text-equality-compatible with Firstmate: its minimal companion must inspect `message.parts[].metadata.sessionQueue.constituents`, validate version/session/original identity/input and positively reconcile the exact constituent. HTTP204 remains acceptance only.

## Implementation decisions verified

Index commit `a455b1081f` supplies generated fresh/upgrade lookup and red/green/rollback observations. Grouped delivery wraps info, parts and promotions in BEGIN IMMEDIATE, with EventV2 notifications deferred until successful outer commit. Recipient selection is revalidated inside that transaction; a changed selection cancels preparation and reselects without losing originals. Native failure behavior remains unchanged.

Consume still deletes original queue rows. Exact retry uses ignored synthetic text-part aliases indexed by the existing part primary key, each containing the fixed v1 singleton constituent metadata. Both transport messageID and peer sender/home/msgid aliases are retained; every matching alias/pending original must agree on exact input and delivery. This covers changed/omitted transport IDs and rejects borrowing another peer's ID across pending/delivered/consumed states. The versioned constituent schema is independent of future pending Item fields such as #212 origins. No receipt table, endpoint, requestID proposal or public HttpApi shape change.

Peer file parts remain intact and receive numbered attribution. Explicit execution controls and unknown/synthetic framing retain individual input semantics. Typed watcher/heartbeat/observer/firstmate/nudge groups preserve distinct conditions; identified repeated checks and heartbeat duties choose latest admitted text in a linear episode pass, preserving historical provenance without claiming a fresh measurement.

Focused tests cover real durable writes, actual provider model/effort/file content, editor withdraw/restore, fresh coordinator reservation recovery, rollback notifications, retries and negative identity conflicts. Eight primary behavior-removal proofs and companion index/alias/conflict/file removal proofs fail meaningfully; originals are restored after each. No full or derived suite, install, push, rollout or live actions.

## Watcher ownership clarification1853

Read the owning Firstmate watcher ruling and updated constituent-consumer handoff on2026-10-12. Firstmate remeasures at each eligible watcher update and owns the three-minute minimum pacing. OpenCode queue aggregation certifies neither current external conditions nor measurement freshness. Automation headings explicitly label historical producer observations; original admission times, identities and complete constituent inputs are retained. This is a wording/ownership correction, with no external probe, timer or scheduler.

## Companion prose owner correction

Read `docs/comments.md` and the owning Firstmate coordination before updating the integration guide. Constituent confirmation belongs to the localized HTTP evidence consumer; watcher2089 independently owns pacing and fresh observations. Its acceptance/retirement source supplies the re-check route, not a requirement for a new watcher payload lookup. The guide now references source symbols and the existing committed emitted fixture instead of copying operational sequencing or externally owned numeric rules. Producer and capture pins remain accepted; this correction neither regenerates the capture nor authorizes publication.

## Fresh default integration checkpoint

Merge `origin/swxtch` at `7a3637c7cb39de2c72b8cfced9e35448e8bdbe21` (PR228) into the existing branch without replaying accepted follow-ups. Preserve both chronological migration entries and combined generated schema; run the actual core generator and its drift check. PR228's event writer projection remains inside the event transaction; the queue's deferred notifications remain outside the composite commit. The failed grouped-write regression additionally verifies writer timestamp rollback and successful advancement. A private actual-Bun upgrade from the immutable PR228 full schema preserves synthetic writer rows and applies only the ordered-history index once. Focused queue/provider/event/history/migration/sync checks and package typechecks verify integration.

Capture a new synthetic grouped-delivery fixture only from the clean committed integrated generator/schema, record that producer SHA separately from the capture commit, and preserve the earlier accepted fixture through its immutable commit. Return the final clean head, capture hash and source delta for independent review before publication. This local merge/capture does not activate retention or prune live data.
