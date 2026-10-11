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
