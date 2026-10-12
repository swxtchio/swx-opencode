# Report: gated production apply for inactive-Session retention (swxtchio/swx-firstmate#2052)

**No live apply occurred.** Nothing in this work ran `--apply` against `~/.local/share/opencode/opencode.db`. No
OpenCode process was stopped or restarted, and no live file was moved, replaced, vacuumed or redacted. The live store
was only read: the holder scan, a selection census, and a read-only copy of one Session tree for the lock measurement.

**Ordinary prompted V2 Sessions remain ineligible.** The literal `v2-input-promoted-row-present` refusal is kept.
Any tree with a persisted V2 input row (promoted or pending) is never redacted.

**Live apply is still refused by code.** Three gates had no approved contract or producer (D1–D3 below). Each is a
captain-owned decision, raised as `needs-decision` on the wake channel. Captain decision 1 has since settled the
`/sync/history` behaviour (D1), now implemented (invariant 5), but the production refusal is unchanged: production
evidence still supplies no reader-contract, receipt or classification, so every real `opencode db retention --apply`
returns `production-apply-disabled`.

- Branch: `fm/opencode-retention-apply-2052`, based on `origin/swxtch` at `4b87478ddf4a14ffb42cdffde4af48712a9b03b6`.
- Implementation head: `5e0a1d91c9` (`git rev-parse HEAD` before this report commit). The lock measurement ran at
  `39595d762424bdd5ae89bd50de61d47101727388`, which has the same production code as `5e0a1d91c9`; the later commit
  only adds tests.

## Captain-owned decisions that still block enablement

| ID | Gate | Why code alone cannot close it | Options (recommended first) |
| --- | --- | --- | --- |
| D1 | Mixed `/sync/history` reader (swxtchio/swx-opencode#133 item 1). **Decided 2026-10-11 (captain decision 1); see invariant 5.** | `readSyncHistory` returns a bare event array. The SDK publishes it as `sync.history.list`. A server that redacts one aggregate can serve unaffected aggregates correctly only through a new response shape or new error/resync semantics. #133 reserves that choice to the captain. The brief rules out an unversioned envelope and silent omission. | A: a versioned `POST /sync/history/v2` returning `{events, unreplayable:[{aggregateID, seq}]}`. `Workspace.syncHistory` migrates to it, and the legacy route keeps its whole-request refusal. B: a typed 409 that lists the redacted IDs, plus a request-side exclusion list. C: keep the gate refused. |
| D2 | Measurement receipt (swxtchio/swx-firstmate#1815) | The Firstmate producer (`fm_usage_read_opencode_scoped`, `fm_usage_finalize`, `fm_usage_finalize_readback`, `fm_primary_usage_harvest`) retains message-level `source_coverage` per Firstmate task or primary session: message count, digests and message watermark. It has no child-Session tree scope and no final **event** sequence (`bin/fm-usage-lib.sh` lines 1086–1104 and 1420–1425 at fm-llmrouter `2ea3deb7c`). It therefore cannot certify that a candidate tree's final write was measured. This OpenCode task may not change Firstmate's tracked material. | A: a Firstmate task extends the producer to emit a versioned per-tree receipt, holding the tree's session IDs and the final event seq per aggregate, both read in the same read transaction, plus the axes or their unavailable causes, with a readback. This crew then builds the OpenCode consumer for that schema. B: keep the gate refused. |
| D3 | Customer-Session classification | `apply` has always required a `customerBinding` proof, but no production producer emits one. Every tree refuses with `customer-session-classification-unavailable`. The captain's #2052 list of refusal reasons does not mention this gate. | A: a store-level operator attestation (for example `--attest-no-customer-sessions <reviewed-ref>`), recorded in the progress evidence. B: a new per-Session marker producer. C: keep the gate refused. |

There is also an operational prerequisite, which is not a code decision. The liveness proof refuses every tree while any
process holding the database is not a registered process from this build (`unregistered-database-holder:<pid>`). On
2026-10-11 the scan found 33 such holders, all on the old binary. Enabling apply therefore also needs the serving
fleet restarted onto the merged build, which requires a separate captain go. Until that happens, the live store does
not even have the writer-record tables: a dry run against it refuses with `event-writer-process-table-unreadable`.

## Invariants

The focused commands run from the package directory named in each row. Every new or rewritten assertion has a
removal red proof (RP#). Each proof is listed below with its observed result, and each implementation was restored
and re-run green.

### 1. Strictly more than 7 days of inactivity, in one shared boundary — implemented

- `inactivityPolicy` (7 days, reference swxtchio/swx-firstmate#2052) and `inactivityFailures` in
  `packages/opencode/src/cli/cmd/db-retention.ts`. They are called from `candidateFailures`, the single policy
  function that both `inventorySnapshot` and the in-lock `revalidateCandidate` use.
- Last activity is the newest producer timestamp across the whole tree. The sources are Session
  created/updated/archived, message, part, V2 message, V2 input, V1 queue (created/promoted/withdrawn), todo, share,
  and `event_aggregate_writer.time_last_append`. That last column is new: event rows carry no timestamp, so the EventV2
  commit now records the append time (item 2). A missing timestamp column, or a non-integer value in one, refuses the
  tree with `session-activity-evidence-unavailable` or `session-activity-unreadable`. Creation time is just one input;
  archive state proves nothing on its own.
- Test: `packages/opencode/test/cli/db/retention.test.ts` › "only a whole tree inactive for strictly more than seven
  days…". It shows an idle tree selected at boundary+1 ms and refused at exactly 7 days and at 7 days − 1 ms. Refused:
  an old-created but recently active Session, an old parent with a recently active child, a tree whose only recent
  activity is an event append, and an unreadable timestamp. Apply of a stale candidate writes nothing.
  › "activity arriving after inventory refuses the next batch before it writes" covers the boundary inside the lock.
- Red proofs: RP3 (creation-age reversion), RP4 (`>=` boundary) and RP11 (no in-lock policy).

### 2. Live holders, running turns, pending queue and unfinished rows are never selected — implemented

- New fork-owned record in `packages/core/src/event/writer.ts` and `writer.sql.ts`, with migration
  `20261011200343_event_writer_record`:
  - Every process that opens the store through the production `Database` layer registers its pid, kernel boot ID and
    start time (`/proc/<pid>/stat` field 22) in `event_writer_process`.
  - Every committed durable append in `EventV2.commitDurableEvent`, the single append path for V1 and V2 Sessions,
    upserts `(aggregate, writer, time_last_append)` in `event_aggregate_writer` within the same transaction. A fenced
    or rolled-back append records nothing.
- `scanDatabaseHolders` lists every other process holding the database, WAL or shared-memory inode. It combines two
  sources: this user's open descriptors, and `/proc/locks`. Every SQLite WAL connection holds a lock on the -shm DMS
  byte, so `/proc/locks` also covers non-dumpable and other-user processes. The scan is bound to the file's
  device and inode, and `apply` refuses a scan of a different file (`database-holder-scan-target-mismatch`). This check
  replaces the old fixture-only barrier.
- `holderFailures` and `sessionWorkFailures` refuse in these cases:
  - Unregistered holders, and live registered writers of any Session in the tree (`session-held-by-live-process`).
  - Unregistered or unreadable writers.
  - Unfinished V1/V2 assistant rows: `role=assistant` without a numeric `time.completed`. This covers running turns
    and also abandoned crashed ones.
  - V1 queue rows, V2 pending inputs, and workspace-owned aggregates.
  - Unreadable process identity.

  Opening the shared file is not ownership; only a process that wrote the Session holds it, until it exits. All of
  these checks run again inside every batch's `BEGIN IMMEDIATE`.
- The evidence holder scan runs before the lock, so `apply` also rescans the full holder set inside every batch's
  `BEGIN IMMEDIATE` (`lockedHolderFailures`). Every current holder must pass the registration check, and a holder
  missing from the evidence scan refuses the batch (`database-holder-appeared-after-scan:<pid>`).
- Tests in `retention.test.ts`, each on a store written by the real producers (`test/fixture/retention-producer.ts`)
  and held by a separate OS process (`test/fixture/retention-holder.ts`):
  - "a Session written by a live process in another OS process is never selected, and is released when it exits": a
    real `Database`+`EventV2` producer in another process holds the Session. Selection and apply refuse with
    unchanged bytes, and the Session becomes eligible after that process exits.
  - "an unregistered process holding the database refuses every tree until it exits".
  - "a holder or pending work arriving between inventory and a batch refuses it before any write".
  - "a holder that opens the store after the evidence scan refuses the batch before any write": the evidence scan
    names no holder, an unregistered holder then opens the store, and the batch refuses with unchanged bytes.
  - "a holder whose descriptors cannot be read is still found through its SQLite lock in /proc/locks": the holder
    makes itself non-dumpable, so its descriptors cannot be listed, and the scan still names it.
  - "a holder admitted while the batch waits on a contended BEGIN IMMEDIATE refuses it before any write"
    (swxtchio/swx-opencode#226). It uses only positive readiness signals:
    1. A registered contender process that has written no Session holds BEGIN IMMEDIATE and reports `ready`.
    2. The batch's database handle writes a signal file at the instant it issues BEGIN IMMEDIATE, after every
       pre-lock step.
    3. Only then does the contender admit a new unregistered holder. It reports `holder <pid>` once that holder is
       ready, and only then releases the lock (`released`).

    So the holder arrives after anything a pre-lock step could scan, and before the batch can hold the lock. The
    batch refuses with `database-holder-appeared-after-scan:<pid>` and unchanged bytes. With the rescan moved to just
    after `input.evidence`, only this test fails (RP33).
  - "dry run names pending, unfinished, workspace-owned, receipt, and unreadable proofs".
- Core test: `packages/core/test/event.test.ts` › "records this process as the writer of every committed durable
  append, and nothing for a fenced one".
- Red proofs: RP1, RP2, RP5, RP6, RP7, RP10, RP12, RP21, RP22, RP23, RP24, RP25 and RP33.

### 3. Content removed, metrics retained exactly — verified at the existing boundary

- `redactionPlan`, `redactRow` and `retainMetrics` are unchanged. The restart-safe apply test ("apply redacts every
  Session-produced copy in restartable batches and preserves metric values") runs on a store written by the real
  producers in a separate process (`test/fixture/retention-producer.ts`): the production `Database` layer's schema,
  and Session, message, part, V2 step, V2 context, todo and Context Epoch rows from the Session, Todo and EventV2
  producers. It compares the per-Session usage record, which the session-usage projection fills from the producer's
  step-finish part: `cost`, `tokens_input`, `tokens_output`, `tokens_reasoning`, `tokens_cache_read`,
  `tokens_cache_write`, and the `model` and `agent` columns, which these producers leave null. It also compares
  provider, requested model (V1 `modelID` and V2 `model`), serving models, tokens, cost and
  `time.created`/`time.completed` on every message, part and V2 row, and the `db export-usage` records, before and
  after.
- The reduced hand-built schema remains only for refusal cases and the fixture-compaction rehearsals. RP26 shows why
  it was not enough: dropping `model` from the retained metric objects leaves the old reduced-fixture test green and
  turns the producer-built test red, because only the real V2 assistant carries `model: { id, providerID }`.
- Raw sentinels disappear from every inventoried copy. The durable `redacting` marker is written with the first
  changed batch, and the run resumes after a reopen; both are existing behaviour, still asserted.
- The gated path test ("gated apply redacts a Session-produced store only when every gate passes…") observes the
  redaction through `applyGated`.
- Red proofs: RP13 (fabricated zero usage), RP14 (tokens dropped), RP15 (serving models dropped) and RP26.

### 4. Fresh-store measurement receipt — NOT produced; the gate stays refused (D2)

- No receipt was produced and none was read back. The existing #1815 path cannot emit one keyed to an OpenCode Session
  tree and its final event sequence (see D2), and running `fm_usage_finalize` would write Firstmate's shared ledger,
  which this task may not change.
- The production CLI therefore supplies no `handoff` evidence, and every tree refuses with
  `measurement-handoff-receipt-unavailable`. Store-wide receipts are now accepted when they name every Session of the
  tree. The existing checks are kept: final-sequence coverage, the axes or their unavailable causes, and
  `rawHistoryInaccessible`. These remain behind consumer-side validation inputs.
- Tests: the gated-path test refuses an omitted receipt before any write. "dry run names…" refuses a stale
  final-sequence receipt. The existing "a missing measurement receipt refuses…" test is kept.
- Unproved: valid producer-derived coverage enabling a candidate, because no producer exists yet.

### 5. Mixed `/sync/history` reader — implemented per captain decisions; the production gate stays refused

- Decision 1 (2026-10-11): the reader must not fail on pruned data. A request touching a pruned Session returns 200
  with the other Sessions' events, the pruned Session contributes none, and the body stays the unchanged bare event
  array. Its fourth point, a header naming every pruned Session with its last sequence, is superseded by the
  header-bound decision (2026-10-11): list them as ranges, best effort, within a safe header budget.
- `readSyncHistory` (`packages/opencode/src/server/routes/instance/httpapi/handlers/sync.ts`) reads in one
  transaction. Every aggregate whose retention marker is in a state `EventV2.unreplayableRetentionStates` names
  (`redacting`, `complete`) is excluded from the event query through a subquery, so its rows are never read or
  decoded and their count cannot reach SQLite's bound-variable limit. When any is pruned, it also reads every ID the
  store holds (Session rows and event sequences) and returns the pruned ones as ranges.
- Range semantics, against the identifier contract. A Session ID is any string starting with `ses`
  (`packages/schema/src/session-id.ts`). Generated IDs are time-descending plus random, and callers may supply their
  own, so no interval of IDs is reserved and a range can honestly speak only for Sessions this server holds:
  - `first..last` (or a lone ID) means every Session this server holds whose ID sorts, in string order, between
    first and last inclusive is pruned, as of the response.
  - Ranges are maximal runs of pruned IDs over the server's sorted held IDs, so no range spans an unpruned Session
    it holds. A range says nothing about IDs this server does not hold.
  - Endpoints use only `[A-Za-z0-9_-]`; a pruned ID outside that set ends a run and is not listed. That keeps `,`
    and `..` unambiguous, and with ASCII endpoints UTF-16 and byte order agree on which IDs fall in a range.
- Bound and best effort: the header carries the ascending prefix of ranges whose whole line (name, `: `, value,
  CRLF) fits `PrunedSessionsHeaderBudget`, 2048 bytes. That leaves half of a common 4 KiB proxy response-header
  buffer (nginx's default `proxy_buffer_size`) for ordinary headers, well under Node's 16 KiB client limit. What is
  listed is exact; a Session the header does not cover may still be pruned. There is no header when nothing is
  listed. Per-Session last sequence numbers are no longer carried, because a range cannot hold one per Session.
- The contract is documented in the endpoint's OpenAPI description. `packages/sdk/openapi.json` (that line only:
  regenerating it also surfaces unrelated pre-existing drift, left out) and the legacy JS SDK carry it, and
  `packages/client` regenerates unchanged.
- `/sync/replay` onto a redacted aggregate stays refused ("refuses sync replay onto a redacted aggregate without
  changing its rows").
- Tests in `packages/opencode/test/server/httpapi-sync.test.ts` exercise marker-driven exclusion, not an applied
  store. `applyRetentionFixtures` runs a real retention apply on an in-memory copy and copies only the markers it
  wrote back into the served store, whose rows stay unredacted, so the reader must exclude them by marker alone. The
  first four tests use one such pruned Session and one kept peer, each across three payloads (the pruned Session
  omitted, listed past its first event, and listed at its last sequence):
  - "a history request touching a pruned Session returns 200 with the other Sessions' events".
  - "a pruned Session contributes no events to sync history, and its rows are never read": the pruned rows are made
    undecodable first, and the request still succeeds without them.
  - "the X-Pruned-Sessions header names the pruned Session as a range and no unpruned Session": it covers the pruned
    Session, not the kept one, and no held Session inside a range is unpruned.
  - "sync history keeps its bare event array body when a Session is pruned": an array of rows with exactly
    `aggregate_id`, `data`, `id`, `seq`, `type`, equal to the unpruned rows past each cursor.
  - "a standard HTTP client syncs a high-cardinality pruned history without response-header overflow"
    (swxtchio/swx-opencode#224): 700 Sessions, 550 of them pruned by marker (150 interleaved with kept ones, 400
    contiguous). Node's `http` client, with its default 16 KiB header limit, posts over TCP to the real Node HTTP
    server that serves the app. The response is 200 with exactly the kept Sessions' events in a bare array. The
    header line fits 2048 bytes, and its ranges ascend, cover the 400 contiguous Sessions, and cover no kept or other
    unpruned held Session. The header is truncated, yet every pruned Session it leaves out is still absent from the
    body: truncation does not change exclusion (RP36).
  - "pruned rows stay excluded with more pruned Sessions than SQLite binds parameters": the pruned Session's real
    marker, sequence and event rows are copied under 250,100 more aggregate IDs, more than the 250,000 bound
    parameters Bun's SQLite accepts in one statement. Every payload still returns exactly the kept peer's events.
    Binding the pruned IDs as a list fails with `too many SQL variables` (RP34); the subquery binds none.
  - "an allowed cross-origin browser client can read the X-Pruned-Sessions header" (swxtchio/swx-opencode#225): with
    `Origin: http://localhost:3000` the response allows that origin and lists `X-Pruned-Sessions` in
    `Access-Control-Expose-Headers`, which the history handler sets alongside the header itself, as the session
    messages handler does for `Link` and `X-Next-Cursor`. Effect's CORS middleware exposes nothing by default (RP35).
- The retention test fixture (`test/fixture/session-retention.ts`) now prunes many Sessions in one pass, on an
  in-memory copy of the store, so pruning 550 takes seconds rather than paying a procfs holder scan per batch. The
  apply code is unchanged.
- Red proofs: RP27 to RP29 (decision 1), rerun against the range implementation, and RP30 to RP32 and RP34 to RP36.
- The missing-sequence sentinel (`seq ?? -1` for a marker without a sequence row) left with the per-Session
  sequence; the range contract carries no sequence.
- Still refused: `readerContract` evidence is absent in production, so every tree refuses with
  `reader-contract-unreviewed`. Supplying it would change the production refusal, which neither decision allows.
- Workspace consumer (swxtchio/swx-opencode#223): `Workspace.syncHistory` is compatible with the range contract. It
  gets 200, replays retained events, deletes nothing and claims nothing about completeness. Before decision 1 the
  same request failed and its loop retried. Every real use of the header changes cached Session state, so no code
  change was made. The policy fork is on #223 (comment 6115259788):
  - A: an advisory durable "source pruned" mark (recommended once enablement nears).
  - B: redacting the cached copy, which needs authorization because it deletes content.
  - C: leaving it as is.

  Whether to warp a source-pruned Session belongs to the same fork.
- Publication: the PR's `CHANGESET.md` entry is added once its number exists.

### 6. Literal `v2-input-promoted-row-present` — kept

- The refusal now lives in `sessionWorkFailures`, which inventory and every batch share. Any V2 input row for any
  Session in the tree refuses the tree. A pending row refuses with `v2-input-pending`; a promoted one with the literal
  reason.
- Tests:
  - `retention.test.ts` › "a promoted V2 input row refuses its tree literally, including one promoted after inventory":
    apply returns exactly `["v2-input-promoted-row-present"]` and the bytes are unchanged.
  - The producer-driven `packages/opencode/test/session/session.test.ts` › "marks V1 queue and V2 admitted input
    producers ineligible" covers an ordinary prompted V2 Session, created through the real `SessionV2.prompt` and
    promotion path.
- Red proof: RP8.
- **Ordinary prompted V2 Sessions are not redactable under this rule.** Live census (read-only, 2026-10-11 20:23Z):
  all 457 trees in the fresh store are V1 (zero `session_input` rows), so the rule excludes none of them today.

### 7. Production apply only after every gate — implemented as a composed gate path; refused in production today

- Both barriers are replaced:
  - `RetentionCommand --apply` now runs `applyGated`.
  - `apply` no longer refuses non-fixture files, and instead requires a holder scan bound to the open file.
- `applyGated` inventories without a long read transaction. With no eligible tree it returns `refused` before any
  write transaction, with every reason kind plus `production-apply-disabled`. Otherwise it applies each tree,
  revalidating every batch inside its lock. A refusal after committed batches returns `partial` with the honest
  `changedRows`, and nothing further is written.
- `productionEvidence` supplies only the holder scan, so the reader-contract, receipt and classification gates keep
  `production-apply-disabled` on every real call.
- Tests:
  - "retention CLI keeps inventory read-only and refuses apply before any write when no tree passes every gate" runs
    the production CLI handler: no transaction, and bytes unchanged.
  - "apply on a database outside the fixture factory requires a holder scan of that exact file".
  - "gated apply redacts a Session-produced store only when every gate passes, and refuses before any write
    otherwise". This covers omitted liveness, unreadable scan, omitted receipt, omitted classification, unreviewed
    reader, unreadable evidence, and a clock still inside the inactivity window of the producer's writes.
  - "a gate failing after committed batches reports partial progress and stops every further write".
- Red proofs: RP9, RP16 (fixture barrier restored), RP17, RP18 and RP19.
- Unproved: a positive run of the **production CLI handler** itself. It cannot pass until the reader-contract,
  receipt and classification gates have production producers. The positive test exercises `applyGated`, the function the handler calls, on a store the real producers wrote. Its
  customer, receipt and reader inputs are fixture stand-ins, and only the holder scan comes from the real producer.

### 8. One-batch write-lock hold — measured: about 330–400 ms per batch

- `apply` now reports `lockHold` (`batches`, `maxMs`, `totalMs`). It is measured from the moment `BEGIN IMMEDIATE`
  holds the lock (the first statement of the transaction callback) until COMMIT or ROLLBACK returns, so in-lock
  revalidation is included. `applyGated` aggregates it. RP20 covers it.
- Command: `cd packages/opencode && bun script/retention-lock-measure.ts --source ~/.local/share/opencode/opencode.db
  --session ses_edd357636ffeq160x63azLgKTD --batches 6 --batch-size 32`, at head `39595d76`.
- Copy provenance: the script opened the live store read-only and copied one whole Session tree into a fresh private
  store built by the production `Database` layer, then deleted it. It ran on 2026-10-11 at about 20:23Z. The tree was
  the largest idle tree, with the most events among Sessions idle for more than 30 hours: 1 Session, 55,092 events
  (178.5 MB), 3,186 messages (6.1 MB), 13,998 parts (48.4 MB) and 3 todos. The copy took 2.5 s.
- Representativeness: the cost inside the lock is the whole-tree revalidation, which scans every event's JSON. It
  scales with the tree's own rows, not with the rest of the store, so the largest idle tree is the worst eligible case
  observed. The clock was set to the tree's last activity plus 7 days plus 1 minute, so the inactivity gate passed and
  still ran in each batch. Its customer, receipt and reader inputs were stand-ins; their validation is negligible.
  The box was loaded during the run: load average about 14.5 on 16 CPUs.
- Result for 6 consecutive batches of 32 rows each: lock hold **347.3, 332.2, 363.2, 352.9, 385.6 and 396.6 ms**.
  Dry-run inventory of the tree took 0.54 s.
- Exploratory runs at an earlier head with the same apply code: batch size 1 gave 343–393 ms, and batch size 512 gave
  349–408 ms. Hold time is therefore dominated by revalidation, not by the row limit. At 32 rows per batch, this tree
  would hold the lock about 1,700 times. I have set no latency threshold.
- These figures predate the in-lock holder rescan (invariant 2), which adds one full procfs scan to every batch's
  hold. On this box, with 695 processes and load average about 13.9, one `scanDatabaseHolders` call took 29–59 ms
  (median 36 ms over 20 calls). Its cost scales with the process count, not the tree. The tree measurement was not
  repeated, because it reads the live store.

## Verification

| Command (package dir) | Result |
| --- | --- |
| `packages/core`: `bun test test/event.test.ts` | 48 pass, 0 fail |
| `packages/core`: `bun test test/database-migration.test.ts` | 22 pass, 0 fail |
| `packages/core`: `bun script/migration.ts --check` | exit 0 |
| `packages/opencode`: `bun test test/cli/db/retention.test.ts` | 28 pass, 0 fail |
| `packages/opencode`: `bun test test/server/httpapi-sync.test.ts test/session/session.test.ts` | 19 pass, 0 fail |
| Round-1 fixes, `packages/opencode`: `bun test test/cli/db/retention.test.ts` | 30 pass, 0 fail |
| Round-1 fixes, `packages/opencode`: `bun test test/session/session.test.ts -t "retention\|Retention"` | 4 pass, 0 fail |
| Round-1 fixes, `packages/opencode`: `bun test test/server/httpapi-sync.test.ts` | 4 pass, 0 fail, 1 skipped |
| Decision 1, `packages/opencode`: `bun test test/server/httpapi-sync.test.ts` | 7 pass, 0 fail, 1 skipped |
| Header bound, `packages/opencode`: `bun test test/server/httpapi-sync.test.ts` | 8 pass, 0 fail, 1 skipped |
| Header bound, `packages/opencode`: `bun test test/session/session.test.ts -t "retention\|Retention"` | 4 pass, 0 fail |
| Header bound, `packages/opencode`: `bun test test/server/httpapi-session.test.ts -t redacted` | 1 pass |
| Header bound, `packages/opencode`: `bun test test/control-plane/workspace.test.ts -t redacted` | 1 pass |
| Header bound, `packages/opencode`: `bun typecheck` | clean |
| Round-1 review fixes, `packages/opencode`: `bun test test/server/httpapi-sync.test.ts` | 10 pass, 0 fail, 1 skipped |
| Round-1 review fixes, `packages/opencode`: `bun test test/cli/db/retention.test.ts` | 31 pass, 0 fail |
| Round-1 review fixes, `packages/opencode`: session, workspace and session-HTTP redacted-retention tests | 4, 1 and 1 pass |
| Round-1 review fixes, `packages/opencode`: `bun typecheck` | clean |
| Decision 1, `packages/opencode`: `bun test test/control-plane/workspace.test.ts -t "redacted"` | 1 pass |
| Decision 1, `packages/opencode`: `bun test test/server/httpapi-session.test.ts -t "redacted aggregate before returning content"` | 1 pass |
| Decision 1, `packages/core` and `packages/opencode`: `bun typecheck` | clean |
| `packages/opencode`: `bun test test/server/httpapi-session.test.ts -t "redacted aggregate before returning content"` | 1 pass |
| `packages/opencode`: `bun test test/control-plane/workspace.test.ts -t "warp a redacted Session"` | 1 pass |
| `packages/core` and `packages/opencode`: `bun typecheck` | clean |

## Red proofs

Each row records an implementation removal, the focused test run against it, and the result. Every change was then
restored from a byte copy, with `git status` clean afterwards.

| RP | Implementation removed | Focused result |
| --- | --- | --- |
| RP1 | `EventWriter.recordAppend` in the commit | core "records this process…" 0 pass, 1 fail |
| RP2 | `EventWriter.register` in the Database layer | same, 0 pass, 1 fail |
| RP3 | last activity counts only `session.time_created` | "strictly more than seven days" 0 pass, 1 fail |
| RP4 | boundary `>=` instead of `>` | same, 0 pass, 1 fail |
| RP5 | `session-held-by-live-process` | "another OS process" + "arriving between inventory" 0 pass, 2 fail |
| RP6 | `unregistered-database-holder` | "unregistered process holding" 0 pass, 1 fail |
| RP7 | `v1-prompt-queue-row-present` | "dry run names" + "arriving between inventory" 0 pass, 2 fail |
| RP8 | `v2-input-promoted-row-present` | "promoted V2 input row" 0 pass, 1 fail |
| RP9 | `database-holder-scan-target-mismatch` | "outside the fixture factory" 0 pass, 1 fail |
| RP10 | `unfinished-assistant-message` | "dry run names" 0 pass, 1 fail |
| RP11 | `candidateFailures` in `revalidateCandidate` | "activity arriving after inventory" 0 pass, 1 fail |
| RP12 | `event-appended-after-inventory` | "arriving between inventory" 0 pass, 1 fail |
| RP13 | session redaction zeroes `cost`/`tokens_input` | "restartable batches" 0 pass, 1 fail |
| RP14 | `tokens` dropped from retained objects | same, 0 pass, 1 fail |
| RP15 | `responseModelIDs` dropped | same, 0 pass, 1 fail |
| RP16 | fixture-only barrier restored in `apply` | "gated apply redacts" 0 pass, 1 fail |
| RP17 | `applyGated` does not refuse when nothing is eligible | same, 0 pass, 1 fail |
| RP18 | partial progress reported as `refused` | "partial progress" 0 pass, 1 fail |
| RP19 | `applyGated` continues past a refused tree | same, 0 pass, 1 fail |
| RP20 | success-path lock-hold recording | "restartable batches" 0 pass, 1 fail |
| RP21 | `session-writer-unregistered` | "dry run names" 0 pass, 1 fail |
| RP22 | `session-writer-liveness-unreadable` | same, 0 pass, 1 fail |
| RP23 | `session-owned-by-workspace` | same, 0 pass, 1 fail |
| RP24 | in-lock holder rescan (`lockedHolderFailures`) in `revalidateCandidate` | "opens the store after the evidence scan" 0 pass, 1 fail: apply completed 17 rows with the holder present |
| RP25 | `/proc/locks` holders dropped from the scan result | "found through its SQLite lock" 0 pass, 1 fail: `pids: []` |
| RP26 | `model` dropped from the retained metric objects | producer-built "restartable batches" 0 pass, 1 fail; the pre-fix reduced-fixture test at `9d5610eb` 1 pass |
| RP27 | replayability preflight restored in `readSyncHistory` (no-fail path reverted) | `httpapi-sync.test.ts`, range implementation: 3 pass, 5 fail: every pruned-Session test |
| RP28 | `X-Pruned-Sessions` header not set | 6 pass, 2 fail: the range header test and the high-cardinality test |
| RP29 | pruned rows no longer excluded | 5 pass, 3 fail: "contributes no events", "bare event array body" and the high-cardinality test |
| RP30 | complete enumeration restored (a JSON object of every pruned Session) | high-cardinality test fails: Node's client reports `HPE_HEADER_OVERFLOW Parse Error: Header overflow` |
| RP31 | header budget removed | 7 pass, 1 fail: the high-cardinality test, a 4,733-byte header line |
| RP32 | runs allowed to span unpruned Sessions | 6 pass, 2 fail: the range header test and the high-cardinality test, a kept Session covered |
| RP33 | holder rescan moved to just after `input.evidence`, before BEGIN IMMEDIATE (#226 mutant) | `retention.test.ts` 30 pass, 1 fail: only the contended BEGIN IMMEDIATE test |
| RP34 | pruned IDs bound as a list instead of the subquery | `httpapi-sync.test.ts` 9 pass, 1 fail: the parameter-limit test, `SQLiteError: too many SQL variables` |
| RP35 | `Access-Control-Expose-Headers` not set | the cross-origin test 0 pass, 1 fail |
| RP36 | exclusion limited to the first 100 pruned Sessions, like a bounded notification | the high-cardinality test 0 pass, 1 fail |

## Measured selection on the live store (read-only, 2026-10-11T20:23:44Z)

- The writer-record tables are absent from the live store, because no new-build process has opened it yet. 33 holder
  pids found by the holder scan; none is registered.
- 511 Sessions in 457 trees. The oldest Session was created 2026-10-09T21:46:42Z. **0 trees** are inactive for more
  than 7 days; tree idle time is p50 22.9 h, p90 41.2 h, maximum 45.8 h.
- 31 trees carry an unfinished assistant row and 1 has a V1 queue row. These stay excluded even after 7 days.
  Unfinished rows left by crashed processes are never repaired, so those trees stay ineligible permanently.
- Enabling apply therefore selects nothing before about 2026-10-17. Inactive-only redaction cannot bound growth from
  long-lived active primaries (swxtchio/swx-opencode#124). Enablement alone does not prove bounded live growth.
  Logical payload reduction, reusable pages and recovered root bytes remain separate outcomes. None was measured
  here, and a freelist increase is not recovered root bytes.

## Not proved, and why

- Receipt production and readback for the fresh store (D2): the producer cannot emit one, and changes to it are out
  of scope.
- A production customer classification (D3): no producer exists.
- A positive production CLI apply: blocked by the reader-contract evidence gate, D2 and D3.
- Holder-scan blind spot: a process of another user (including root), or a non-inspectable process, that has the file
  open without holding a SQLite lock. Every WAL connection holds one, so this is limited to non-SQLite handles.
- A holder that opens the store after a batch's in-lock rescan and before its COMMIT is not seen by that batch. It
  cannot write until the COMMIT releases the lock, and the next batch's rescan refuses it.
- Writer rows for exited processes are never pruned. They are harmless but grow with process restarts.
- Lock hold on the live store under its real contention was not measured; this was forbidden.
