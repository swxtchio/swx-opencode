# Plan: gated production apply for inactive-Session retention (swx-firstmate#2052)

Authority: captain decisions on swxtchio/swx-firstmate#2052 (2026-10-11), swxtchio/swx-opencode#124 and #133.
No live apply, process restart, file move or compaction is part of this work.

## Gates and where each lives

1. Inactivity boundary (strictly more than 7 days). One policy function in
   `packages/opencode/src/cli/cmd/db-retention.ts`, called by both `inventorySnapshot` and
   `revalidateCandidate`. Last activity is the newest producer timestamp across the whole tree
   (Session, message, part, V2 message, input, V1 queue, todo rows) plus the per-aggregate append
   time recorded by the new writer record (item 2). Any event appended after inventory refuses the batch.
   Unreadable or missing timestamps refuse.
2. Cross-process liveness. New fork-owned core module records
   (a) one row per OS process that opens the database through the production Database layer
   (pid, boot id, kernel start time), and
   (b) one row per (aggregate, writing process) with the last append time, written inside the
   EventV2 durable-commit transaction (the single append choke point for V1 and V2).
   The retention gate scans same-user processes for open handles on the database, WAL and SHM inodes.
   Any holder other than the retention process that is not a registered, still-identical process refuses
   every tree. A tree with any session written by a still-alive registered process refuses. Pending V1 queue rows,
   pending/promoted V2 inputs, and unfinished V1/V2 assistant rows refuse from the database itself.
   Every check is repeated inside each batch's immediate transaction.
3. Metrics retention: extend the existing apply test to compare Session token/cost/model columns,
   message/part/V2 usage values and the usage-export records before and after a gated apply.
4. Measurement receipt: consumer only, and only once the producer contract exists (needs-decision D2).
5. Mixed `/sync/history`: needs a public response decision (needs-decision D1). Reader gate stays refused.
6. Literal `v2-input-promoted-row-present`: kept in inventory and revalidation; add tests incl. arrival
   after inventory.
7. CLI: replace the unconditional `--apply` refusal and the fixture-only barrier with a composed gate
   pipeline; `production-apply-disabled` remains on every call that fails any gate. Customer
   classification has no production producer (needs-decision D3).
8. Lock measurement: time one apply batch from BEGIN IMMEDIATE to COMMIT on an isolated copy
   built from read-only extracted representative Session rows; never on the live store.

## Verification

Focused tests only: `packages/opencode/test/cli/db/retention.test.ts`,
`packages/opencode/test/server/httpapi-sync.test.ts` (after D1), plus the core writer-record test.
Each new or rewritten assertion gets a recorded removal red proof. `bun typecheck` per touched package.

## Round 1 review fixes

1. Holder freshness. `apply` rescans the database holders with `scanDatabaseHolders` inside each batch's
   BEGIN IMMEDIATE transaction. Every holder in that scan must pass the registration check, and any holder
   absent from the caller's evidence scan refuses the batch as `database-holder-appeared-after-scan:<pid>`.
   Regression: a holder that opens the store after the evidence scan refuses before any write; it is red
   with the in-lock rescan removed.
2. Realistic fixtures. A child-process producer (`test/fixture/retention-producer.ts`) writes Session trees
   into a store initialized by the production Database layer, through the real Session, Todo and EventV2
   producers, then exits so its writer registration names a dead process. The positive retention tests
   (metric-preserving restartable apply, exact dry-run tree, gated apply, partial gated apply, apply on a
   scanned file, and the held/unregistered/late holder tests) run on that store. The reduced hand-built
   schema stays only for tests that assert a refusal, and is named and commented as the negative fixture.
3. `/proc/locks` arm. A holder that makes itself non-dumpable, so its descriptors cannot be read, is found
   only through `/proc/locks`; the test is red with that arm removed.

Production enablement stays refused (D1, D2, D3 unchanged); `production-apply-disabled` stays on every
call that fails a gate.

## Scope addition: captain decision 1 (sync history over pruned Sessions)

Source: `data/opencode-retention-fix-r2/scope-addition-decision1.md` in the firstmate home (2026-10-11).

- `readSyncHistory` no longer fails on a redacted aggregate. Within its one read transaction it finds the touched
  aggregates (payload keys plus aggregates with events past their cursor) that carry an unreplayable retention marker,
  excludes their rows in SQL so no redacted row is read or returned, and returns `{ events, pruned }`, where `pruned`
  maps each such Session to its `event_sequence.seq`.
- The `history` handler returns the same bare event array and, when `pruned` is non-empty, sets
  `X-Pruned-Sessions` to the JSON object `{ "<sessionID>": <last seq> }`, the same shape as the request payload
  and the same JSON-object convention as the existing fence header. No header when nothing is pruned.
- The endpoint's OpenAPI description is updated to say so, and the client is regenerated.
- `/sync/replay` onto a redacted aggregate stays refused. The production apply refusal is unchanged.
- Focused tests in `test/server/httpapi-sync.test.ts`, one per required behaviour, each red with the header or
  the no-fail path reverted.

## Scope addition: bounded pruned-Session notification (captain, 2026-10-11)

Source: `data/opencode-retention-fix-r2/header-bound-decision.md` in the firstmate home. Supersedes complete
enumeration in `X-Pruned-Sessions`; the 200 response and bare-array body stay.

- Identifier contract: a Session ID is any string starting with `ses` (`packages/schema/src/session-id.ts`); generated
  ones are time-descending plus random, and callers may supply their own. No ID interval is reserved, so a range can
  honestly speak only for the Sessions this server holds. A range `first..last` therefore means: every Session this
  server holds whose ID sorts, in string order, between first and last inclusive is pruned. Ranges are maximal runs of
  pruned IDs over the server's sorted held IDs, so none spans an unpruned Session it holds. Endpoints use only
  `[A-Za-z0-9_-]`, which keeps the syntax unambiguous and makes UTF-16 and byte order agree on membership.
- Bound: the whole header line (name, `: `, value, CRLF) stays within 2048 bytes, leaving the rest of a common 4 KiB
  proxy response-header buffer for ordinary headers. The header carries the ascending prefix of ranges that fits.
  Last sequence numbers are no longer carried; a range cannot hold one per Session.
- Contract text in the endpoint description, the SDK `openapi.json` line and the legacy JS SDK.
- Regression: a standard Node HTTP client (default 16 KiB header limit) syncs a 550-pruned-Session history over TCP;
  red with complete enumeration restored (header overflow), with the budget removed, and with runs allowed to span
  unpruned Sessions.

## Round-1 review fixes (pin d562be39eb)

Source: `data/opencode-retention-fix-r2/review-round1-fixes.md` in the firstmate home.

1. Header capacity (#224): the range work is committed. Add the missing proofs: the high-cardinality test also
   shows pruned Sessions the truncated header does not cover are still excluded, and a reader test with more
   pruned aggregates than SQLite's bound-parameter limit (32,766) stays correct; red with the exclusion bound as an
   ID list.
2. Holder interleaving (#226): a registered separate process holds BEGIN IMMEDIATE and reports it; apply's handle
   signals at the instant it issues BEGIN IMMEDIATE; only then does the contender admit a new unregistered holder,
   report it ready, and release. Apply must refuse with no write; moving the rescan anywhere before lock acquisition
   (just after `input.evidence`) must turn it red.
3. Browser visibility (#225): the history response that sets X-Pruned-Sessions also sets
   `Access-Control-Expose-Headers: X-Pruned-Sessions`, as the session messages handler does for its paging headers;
   an allowed-Origin regression checks it.
4. Truthful text: the readerContract and productionEvidence comments (reader behaviour decided, production evidence
   absent); the HTTP fixture descriptions (marker-driven exclusion over unredacted rows, not an applied store); the
   missing-sequence sentinel is gone with the range contract; OpenAPI and SDK stay aligned.
5. Workspace consumer (#223): already compatible with the best-effort range contract; any handling of cached
   Sessions needs a client policy, reported on #223 rather than implemented.
6. CHANGESET.md entry: at publication, once the PR number exists.
