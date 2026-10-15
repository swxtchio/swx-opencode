import { afterAll, expect, spyOn, test } from "bun:test"
import { Database, type SQLQueryBindings } from "bun:sqlite"
import { spawnSync } from "node:child_process"
import { existsSync, writeFileSync } from "node:fs"
import { chmod, copyFile, mkdir, mkdtemp, open, readFile, realpath, rm, stat, writeFile } from "node:fs/promises"
import path from "node:path"
import { Effect } from "effect"
import { Flag } from "@opencode-ai/core/flag/flag"
import { PruneCommand } from "@/cli/cmd/db-prune"
import { MessageV2 } from "@/session/message-v2"
import { MessageID, SessionID } from "@/session/schema"

const packageRoot = path.resolve(import.meta.dir, "../../..")
const worktreeRoot = path.resolve(packageRoot, "../..")
const scratchRoot = path.join(worktreeRoot, ".scratch", "opencode-commanded-prune-tests")
const producer = path.join(packageRoot, "test/fixture/prune-producer.ts")
const entrypoint = path.join(packageRoot, "src/index.ts")

type Store = {
  filename: string
  directory: string
  project: string
  [Symbol.asyncDispose]: () => Promise<void>
}

type ProducerResult = {
  created: { id: string; parentID: string | null; time: number; messageID: string; partID: string }[]
  heldSessionID?: string
}

type Report = {
  action: string
  database: string
  cutoff: string
  selectedSessions: { sessionID: string; reasons: string[] }[]
  excludedSessions: { sessionID: string; reasons: string[] }[]
  deletionScope: { sessionIDs: string[]; wholeSessions: boolean }
  reclamation: {
    ready: boolean
    reasons: string[]
    allocatedBytes?: { status: string; totalBytes?: number }
    allocatedBytesBefore?: { status: string; totalBytes?: number }
    allocatedBytesAfter?: { status: string; totalBytes?: number }
    reclaimedBytes?: number
    allocatedBytesByPath?: {
      path: string
      allocatedBytesBefore: number | null
      allocatedBytesAfter: number | null
      reclaimedBytes: number | null
    }[]
    integrity?: { state: string; details: string[] }
    integrityAfter?: { state: string }
    checkpoint?: { busy: number } | { error: string }
  }
  rawTranscriptWarning?: string
  usageMeasurement?: {
    firstmateRun: string
    measurementStatus: string
    durableLedgerStatus: string
    source: string
  }
  refusalReasons?: string[]
  changedRows: number
  committed?: boolean
  deletedSessionIDs?: string[]
}

const ownedCounts = (filename: string, sessionID: string) => {
  const db = new Database(filename, { readonly: true })
  try {
    return {
      session: count(db, "SELECT count(*) AS rows FROM session WHERE id = ?", sessionID),
      message: count(db, "SELECT count(*) AS rows FROM message WHERE session_id = ?", sessionID),
      part: count(db, "SELECT count(*) AS rows FROM part WHERE session_id = ?", sessionID),
      todo: count(db, "SELECT count(*) AS rows FROM todo WHERE session_id = ?", sessionID),
      sessionMessage: count(db, "SELECT count(*) AS rows FROM session_message WHERE session_id = ?", sessionID),
      sessionInput: count(db, "SELECT count(*) AS rows FROM session_input WHERE session_id = ?", sessionID),
      contextEpoch: count(db, "SELECT count(*) AS rows FROM session_context_epoch WHERE session_id = ?", sessionID),
      queue: count(db, "SELECT count(*) AS rows FROM session_prompt_queue WHERE session_id = ?", sessionID),
      queueSequence: count(db, "SELECT count(*) AS rows FROM session_prompt_queue_sequence WHERE session_id = ?", sessionID),
      queueAlias: count(
        db,
        "SELECT count(*) AS rows FROM session_prompt_queue_alias WHERE item_id IN (SELECT id FROM session_prompt_queue WHERE session_id = ?)",
        sessionID,
      ),
      provenance: count(db, "SELECT count(*) AS rows FROM session_prompt_provenance WHERE session_id = ?", sessionID),
      receipt: count(db, "SELECT count(*) AS rows FROM session_prompt_receipt WHERE session_id = ?", sessionID),
      share: count(db, "SELECT count(*) AS rows FROM session_share WHERE session_id = ?", sessionID),
      eventSequence: count(db, "SELECT count(*) AS rows FROM event_sequence WHERE aggregate_id = ?", sessionID),
      event: count(db, "SELECT count(*) AS rows FROM event WHERE aggregate_id = ?", sessionID),
      eventRetention: count(db, "SELECT count(*) AS rows FROM event_retention WHERE aggregate_id = ?", sessionID),
      eventWriter: count(db, "SELECT count(*) AS rows FROM event_aggregate_writer WHERE aggregate_id = ?", sessionID),
    }
  } finally {
    db.close()
  }
}

afterAll(async () => rm(scratchRoot, { recursive: true, force: true }))

test("CLI inventory is read-only, validates the explicit cutoff, and narrows by Session ID", async () => {
  await using store = await createStore()
  const old = await produce(store, { create: { payloadBytes: 128 } })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  const recent = await produce(store, { create: { payloadBytes: 128 } })
  const sessionIDsBefore = storedSessionIDs(store.filename)
  const before = await readFile(store.filename)
  const inventory = runCLI(store, [
    "--before",
    cutoff,
    "--session",
    old.created[0]!.id,
    "--session",
    recent.created[0]!.id,
  ])
  if (inventory.status !== 0) throw new Error(`inventory CLI failed: ${inventory.stderr}\n${inventory.stdout}`)
  const report = JSON.parse(inventory.stdout) as Report
  expect(report.action).toBe("inventory")
  expect(report.database).toBe(await realpath(store.filename))
  expect(report).not.toHaveProperty("parentSnapshot")
  const aliasedPath = { ...store, filename: `${store.directory}/alias/../opencode.db` }
  const aliasedInventory = runCLI(aliasedPath, ["--before", cutoff])
  expect(aliasedInventory.status).toBe(0)
  expect((JSON.parse(aliasedInventory.stdout) as Report).database).toBe(await realpath(store.filename))
  expect(report.selectedSessions.map((item) => item.sessionID)).toEqual([old.created[0]!.id])
  expect(report.excludedSessions.find((item) => item.sessionID === recent.created[0]!.id)?.reasons).toContain(
    "activity-at-or-after-cutoff",
  )
  expect(report.reclamation.ready).toBe(true)
  expect(report.rawTranscriptWarning).toContain("raw transcripts")
  expect(await readFile(store.filename)).toEqual(before)

  const invalid = runCLI(store, ["--before", "yesterday", "--apply"])
  expect(storedSessionIDs(store.filename)).toEqual(sessionIDsBefore)
  expect(invalid.status).toBe(1)
  const rollover = runCLI(store, ["--before", "2026-02-30T00:00:00Z", "--apply"])
  expect(rollover.status).toBe(1)
  expect((JSON.parse(rollover.stdout) as Report).reclamation.reasons).toContain("invalid-or-missing-cutoff")
  expect(storedSessionIDs(store.filename)).toEqual(sessionIDsBefore)
  const future = runCLI(store, ["--before", new Date(Date.now() + 60_000).toISOString(), "--apply"])
  expect(future.status).toBe(1)
  expect(storedSessionIDs(store.filename)).toEqual(sessionIDsBefore)
  const absent = runCLI(store, ["--apply"])
  expect(absent.status).not.toBe(0)
  expect(storedSessionIDs(store.filename)).toEqual(sessionIDsBefore)
  expect(await readFile(store.filename)).toEqual(before)
})

test("rollover dates and hours are refused even when they would otherwise select an old Session", async () => {
  await using store = await createStore()
  const created = await produce(store, { create: { payloadBytes: 512 * 1024 } })
  const sessionID = created.created[0]!.id
  const oldTime = Date.parse("2026-01-01T00:00:00Z")
  backdateSession(store.filename, sessionID, oldTime)
  const before = ownedCounts(store.filename, sessionID)
  expect(before.session).toBe(1)
  expect(before.message).toBeGreaterThan(0)
  expect(before.part).toBeGreaterThan(0)

  const invalid = runCLI(store, ["--before", "2026-02-30T00:00:00Z", "--session", sessionID, "--apply"])
  const refused = JSON.parse(invalid.stdout) as Report
  expect(ownedCounts(store.filename, sessionID)).toEqual(before)
  expect(invalid.status).toBe(1)
  expect(refused.action).toBe("refused")
  expect(refused.reclamation.reasons).toContain("invalid-or-missing-cutoff")

  const invalidHour = runCLI(store, ["--before", "2026-03-02T24:00:00Z", "--session", sessionID, "--apply"])
  expect(ownedCounts(store.filename, sessionID)).toEqual(before)
  expect(invalidHour.status).toBe(1)
  expect((JSON.parse(invalidHour.stdout) as Report).reclamation.reasons).toContain("invalid-or-missing-cutoff")

  const cleanControl = runCLI(store, ["--before", "2026-03-02T00:00:00Z", "--session", sessionID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect((JSON.parse(cleanControl.stdout) as Report).action).toBe("applied")
  expect(ownedCounts(store.filename, sessionID).session).toBe(0)
})

test("CLI apply deletes only the selected whole old Session and reclaims allocated bytes", async () => {
  await using store = await createStore()
  const unrequested = await produce(store, { create: { payloadBytes: 128 } })
  const old = await produce(store, { create: { payloadBytes: 1024 * 1024, withdrawnV1: true } })
  addCompleteRetentionMarker(store.filename, old.created[0]!.id)
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  const recent = await produce(store, { create: { payloadBytes: 1024 * 1024, withdrawnV1: true } })
  const oldID = old.created[0]!.id
  const unrequestedID = unrequested.created[0]!.id
  const recentID = recent.created[0]!.id
  const oldText = textParts(store.filename, oldID)
  expect(textParts(store.filename, recentID)).toEqual(oldText)
  const ledger = path.join(store.directory, "state", "usage-ledger")
  await mkdir(ledger, { recursive: true })
  const ledgerFile = path.join(ledger, "fixture.jsonl")
  await writeFile(ledgerFile, `${JSON.stringify({ id: "prune-fixture", usage_status: "partial", input_tokens: 12 })}\n`)
  const ledgerBefore = await readFile(ledgerFile)
  const beforeOld = ownedCounts(store.filename, oldID)
  const beforeUnrequested = ownedCounts(store.filename, unrequestedID)
  const beforeRecent = ownedCounts(store.filename, recentID)
  expect(beforeOld.session).toBe(1)
  expect(beforeOld.message).toBeGreaterThan(0)
  expect(beforeOld.part).toBeGreaterThan(0)
  expect(beforeOld.todo).toBeGreaterThan(0)
  expect(beforeOld.sessionMessage).toBeGreaterThan(0)
  expect(beforeOld.contextEpoch).toBe(1)
  expect(beforeOld.eventSequence).toBe(1)
  expect(beforeOld.event).toBeGreaterThan(0)
  expect(beforeOld.eventWriter).toBeGreaterThan(0)
  expect(beforeOld.queue).toBeGreaterThan(0)
  expect(beforeOld.queueSequence).toBe(1)
  expect(beforeOld.receipt).toBe(1)
  expect(beforeOld.eventRetention).toBe(1)
  const beforeAllocated = await allocatedBytes(store.filename)
  const result = runCLI(store, [
    "--before",
    cutoff,
    "--session",
    oldID,
    "--apply",
    "--firstmate-ran-it",
    "--measurement-status",
    "partial",
    "--usage-ledger-status",
    "available",
  ])
  const report = JSON.parse(result.stdout) as Report
  expect(report.reclamation.reclaimedBytes).toBeGreaterThan(0)
  expect(report.action).toBe("applied")
  expect(result.status).toBe(0)
  expect(report).not.toHaveProperty("parentSnapshot")
  expect(report.deletedSessionIDs).toEqual([oldID])
  expect(report.usageMeasurement).toMatchObject({
    firstmateRun: "confirmed",
    measurementStatus: "partial",
    durableLedgerStatus: "available",
    source: expect.stringContaining("do not gate pruning"),
  })
  expect(report.reclamation.allocatedBytesBefore?.totalBytes).toBeGreaterThan(
    report.reclamation.allocatedBytesAfter?.totalBytes ?? Number.POSITIVE_INFINITY,
  )
  expect(report.reclamation.allocatedBytesByPath).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        path: store.filename,
        allocatedBytesBefore: expect.any(Number),
        allocatedBytesAfter: expect.any(Number),
        reclaimedBytes: expect.any(Number),
      }),
    ]),
  )
  expect(report.reclamation.allocatedBytesByPath).toHaveLength(4)
  expect(report.reclamation.allocatedBytesByPath?.reduce((sum, item) => sum + (item.reclaimedBytes ?? 0), 0)).toBe(
    report.reclamation.reclaimedBytes,
  )
  expect(report.reclamation.integrityAfter?.state).toBe("ok")
  expect(ownedCounts(store.filename, oldID)).toEqual({
    session: 0,
    message: 0,
    part: 0,
    todo: 0,
    sessionMessage: 0,
    sessionInput: 0,
    contextEpoch: 0,
    queue: 0,
    queueSequence: 0,
    queueAlias: 0,
    provenance: 0,
    receipt: 0,
    share: 0,
    eventSequence: 0,
    event: 0,
    eventRetention: 0,
    eventWriter: 0,
  })
  expect(ownedCounts(store.filename, recentID)).toEqual(beforeRecent)
  expect(ownedCounts(store.filename, unrequestedID)).toEqual(beforeUnrequested)
  expect(await allocatedBytes(store.filename)).toBeLessThan(beforeAllocated)
  expect(await readFile(ledgerFile)).toEqual(ledgerBefore)
  const db = new Database(store.filename, { readonly: true })
  try {
    expect(db.query("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" })
    expect(db.query("SELECT id FROM session WHERE id = ?").get(recentID)).toBeTruthy()
    expect(db.query("SELECT count(*) AS rows FROM event WHERE aggregate_id = ?").get(oldID)).toEqual({ rows: 0 })
  } finally {
    db.close()
  }
})

test("a post-commit checkpoint failure reports committed deletions", async () => {
  await using store = await createStore()
  const old = await produce(store, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const sessionID = old.created[0]!.id
  const cutoff = new Date(Date.now() - 5).toISOString()
  const before = ownedCounts(store.filename, sessionID)
  const originalQuery = Database.prototype.query
  const query = spyOn(Database.prototype, "query").mockImplementation(function <
    ReturnType,
    ParamsType extends SQLQueryBindings | SQLQueryBindings[],
  >(this: Database, statement: string) {
    if (statement === "PRAGMA wal_checkpoint(TRUNCATE)") throw new Error("fixture-checkpoint-failure")
    return (originalQuery<ReturnType, ParamsType>).call(this, statement)
  })
  try {
    const result = await invokeHandler(store, { before: cutoff, session: [sessionID], apply: true })
    expect(ownedCounts(store.filename, sessionID)).toEqual({
      ...before,
      session: 0,
      message: 0,
      part: 0,
      todo: 0,
      sessionMessage: 0,
      sessionInput: 0,
      contextEpoch: 0,
      queue: 0,
      queueSequence: 0,
      queueAlias: 0,
      provenance: 0,
      receipt: 0,
      share: 0,
      eventSequence: 0,
      event: 0,
      eventRetention: 0,
      eventWriter: 0,
    })
    expect(result.report.action).toBe("failed")
    expect(result.report.committed).toBe(true)
    expect(result.report.changedRows).toBeGreaterThan(0)
    expect(result.report.deletedSessionIDs).toEqual([sessionID])
    expect(result.report.refusalReasons).toContain("wal-checkpoint-failed:fixture-checkpoint-failure")
  } finally {
    query.mockRestore()
  }

})

test("a confirmed Firstmate run with unavailable coverage does not gate apply or invent zero usage", async () => {
  await using store = await createStore()
  const old = await produce(store, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  const sessionID = old.created[0]!.id
  const result = await invokeHandler(store, {
    before: cutoff,
    session: [sessionID],
    apply: true,
    firstmateRanIt: true,
    measurementStatus: "unavailable",
    usageLedgerStatus: "unavailable",
  })
  expect(result.report.action).toBe("applied")
  expect(result.report.usageMeasurement).toEqual({
    firstmateRun: "confirmed",
    measurementStatus: "unavailable",
    durableLedgerStatus: "unavailable",
    source: "reported by the caller; measurement and ledger state do not gate pruning",
  })
  const failedMeasurement = await invokeHandler(store, {
    before: cutoff,
    firstmateRanIt: true,
    measurementStatus: "failed",
    usageLedgerStatus: "failed",
  })
  expect(failedMeasurement.report.usageMeasurement).toEqual({
    firstmateRun: "confirmed",
    measurementStatus: "failed",
    durableLedgerStatus: "failed",
    source: "reported by the caller; measurement and ledger state do not gate pruning",
  })
})

test("recent activity on an old-created Session protects it while an equally old control is deleted", async () => {
  await using store = await createStore()
  const control = await produce(store, { create: { payloadBytes: 512 * 1024 } })
  const touched = await produce(store, { create: {} })
  const controlID = control.created[0]!.id
  const touchedID = touched.created[0]!.id
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  await produce(store, { touch: touchedID })
  const prior = new Database(store.filename, { readonly: true })
  const createdAt = prior.query<{ time_created: number }, [string]>("SELECT time_created FROM session WHERE id = ?").get(touchedID)!.time_created
  const latest = prior
    .query<{ time_last_append: number }, [string]>("SELECT max(time_last_append) AS time_last_append FROM event_aggregate_writer WHERE aggregate_id = ?")
    .get(touchedID)!.time_last_append
  prior.close()
  expect(createdAt).toBeLessThan(Date.parse(cutoff))
  expect(latest).toBeGreaterThanOrEqual(Date.parse(cutoff))

  const result = await invokeHandler(store, { before: cutoff, apply: true })
  if (result.report.action !== "applied") throw new Error(JSON.stringify(result.report, null, 2))
  expect(result.report.action).toBe("applied")
  expect(result.report.deletedSessionIDs).toEqual([controlID])
  expect(ownedCounts(store.filename, controlID).session).toBe(0)
  expect(ownedCounts(store.filename, touchedID).session).toBe(1)
  expect(result.report.excludedSessions.find((item) => item.sessionID === touchedID)?.reasons).toContain(
    "activity-at-or-after-cutoff",
  )
})

test("an eligible parent never deletes a protected child, and explicit selection deletes only its member", async () => {
  await using store = await createStore()
  const selected = await produce(store, { create: { payloadBytes: 512 * 1024 } })
  const parent = await produce(store, { create: {} })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  const child = await produce(store, { create: { parentID: parent.created[0]!.id } })
  const selectedID = selected.created[0]!.id
  const parentID = parent.created[0]!.id
  const childID = child.created[0]!.id
  const result = await invokeHandler(store, { before: cutoff, session: [selectedID, parentID], apply: true })
  expect(result.report.action).toBe("applied")
  expect(result.report.deletedSessionIDs).toEqual([selectedID])
  expect(ownedCounts(store.filename, selectedID).session).toBe(0)
  expect(ownedCounts(store.filename, parentID).session).toBe(1)
  expect(ownedCounts(store.filename, childID).session).toBe(1)
  expect(result.report.excludedSessions.find((item) => item.sessionID === parentID)?.reasons).toContain(
    `protected-descendant:${childID}`,
  )
})

test("a cross-session message cascade refuses before deleting a protected Session's Part", async () => {
  await using source = await createStore()
  const old = await produce(source, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  const protectedSession = await produce(source, { create: { payloadBytes: 128 } })
  await using unsafe = await copyStore(source)
  const oldID = old.created[0]!.id
  const protectedID = protectedSession.created[0]!.id
  const protectedPartID = protectedSession.created[0]!.partID
  const oldMessageID = old.created[0]!.messageID
  const db = new Database(unsafe.filename)
  const cascade = db
    .query<{ on_delete: string }, []>("PRAGMA foreign_key_list(part)")
    .all()
    .find((foreignKey) => foreignKey.on_delete.toUpperCase() === "CASCADE")
  expect(cascade).toBeTruthy()
  db.query("UPDATE part SET message_id = ? WHERE id = ?").run(oldMessageID, protectedPartID)
  db.close()
  const beforeOld = ownedCounts(unsafe.filename, oldID)
  const beforeProtected = ownedCounts(unsafe.filename, protectedID)
  expect(beforeOld.session).toBe(1)
  expect(beforeOld.message).toBeGreaterThan(0)
  expect(beforeProtected.session).toBe(1)
  expect(beforeProtected.part).toBeGreaterThan(0)

  const refused = runCLI(unsafe, ["--before", cutoff, "--session", oldID, "--apply"])
  const report = JSON.parse(refused.stdout) as Report
  expect(ownedCounts(unsafe.filename, protectedID)).toEqual(beforeProtected)
  expect(ownedCounts(unsafe.filename, oldID)).toEqual(beforeOld)
  const verify = new Database(unsafe.filename, { readonly: true })
  try {
    expect(verify.query("SELECT message_id FROM part WHERE id = ?").get(protectedPartID)).toEqual({
      message_id: oldMessageID,
    })
  } finally {
    verify.close()
  }
  expect(refused.status).toBe(1)
  expect(report.action).toBe("refused")
  expect(report.changedRows).toBe(0)
  expect(report.reclamation.reasons.some((reason) => reason.startsWith("cross-session-cascade:part.message_id:"))).toBe(
    true,
  )

  const cleanControl = runCLI(source, ["--before", cutoff, "--session", oldID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect((JSON.parse(cleanControl.stdout) as Report).action).toBe("applied")
  expect(ownedCounts(source.filename, oldID).session).toBe(0)
  expect(ownedCounts(source.filename, protectedID)).toEqual(beforeProtected)
})

test("a selected Part linked to an unselected Message refuses and preserves MessageV2 output", async () => {
  await using source = await createStore()
  const selected = produce(source, { create: { payloadBytes: 512 * 1024 } })
  const retained = produce(source, { create: { payloadBytes: 128 } })
  const selectedID = selected.created[0]!.id
  const retainedID = retained.created[0]!.id
  const retainedMessageID = retained.created[0]!.messageID
  const cutoff = "2026-03-02T00:00:00Z"
  const oldTime = Date.parse("2026-01-01T00:00:00Z")
  backdateSession(source.filename, selectedID, oldTime)
  await using unsafe = await copyStore(source)
  produce(unsafe, { crossPart: { sessionID: selectedID, messageID: retainedMessageID } })
  backdateSession(unsafe.filename, selectedID, oldTime)
  const retainedMessage = {
    sessionID: SessionID.make(retainedID),
    messageID: MessageID.make(retainedMessageID),
  }
  const coreDatabase = await import("@opencode-ai/core/database/database")
  const readParts = (filename: string) =>
    Effect.runPromise(
      Effect.scoped(
        MessageV2.get(retainedMessage).pipe(
          Effect.map((message) => message.parts),
          Effect.provide(coreDatabase.Database.layerFromPath(filename)),
        ),
      ),
    )
  const cleanPartsBefore = await readParts(source.filename)
  expect(cleanPartsBefore).toHaveLength(1)
  const beforeParts = await readParts(unsafe.filename)
  expect(beforeParts).toHaveLength(2)
  expect(beforeParts.some((part) => part.sessionID === selectedID)).toBe(true)
  const beforeSelected = ownedCounts(unsafe.filename, selectedID)
  const beforeRetained = ownedCounts(unsafe.filename, retainedID)

  const refused = runCLI(unsafe, ["--before", cutoff, "--session", selectedID, "--apply"])
  const report = JSON.parse(refused.stdout) as Report
  expect(await readParts(unsafe.filename)).toEqual(beforeParts)
  expect(ownedCounts(unsafe.filename, selectedID)).toEqual(beforeSelected)
  expect(ownedCounts(unsafe.filename, retainedID)).toEqual(beforeRetained)
  expect(refused.status).toBe(1)
  expect(report.action).toBe("refused")
  expect(report.changedRows).toBe(0)
  expect(report.reclamation.reasons.some((reason) => reason.startsWith("cross-session-cascade:part.message_id:"))).toBe(
    true,
  )

  const cleanControl = runCLI(source, ["--before", cutoff, "--session", selectedID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect((JSON.parse(cleanControl.stdout) as Report).deletedSessionIDs).toEqual([selectedID])
  expect(await readParts(source.filename)).toEqual(cleanPartsBefore)
  expect(ownedCounts(source.filename, retainedID)).toEqual(beforeRetained)
})

test("a pending V1 queue prompt refuses explicit deletion while a clean control deletes", () =>
  verifyPendingWork({ pendingV1: true }, "v1-prompt-pending-or-in-custody"),
)

test("a preparing V1 prompt refuses explicit deletion while a clean control deletes", () =>
  verifyPendingWork({ preparingV1: true }, "v1-prompt-pending-or-in-custody"),
)

test("a durable V2 input refuses explicit deletion while a clean control deletes", () =>
  verifyPendingWork({ pendingV2: true }, "v2-input-pending"),
)

test("an unfinished assistant turn refuses explicit deletion while a clean control deletes", () =>
  verifyPendingWork({ unfinished: true }, "assistant-or-tool-work-in-flight"),
)

test("a persisted running tool Part stays protected after its assistant turn completes", async () => {
  await using store = await createStore()
  const protectedSession = produce(store, { create: { runningTool: true } })
  const control = produce(store, { create: {} })
  const protectedID = protectedSession.created[0]!.id
  const controlID = control.created[0]!.id
  const oldTime = Date.parse("2026-01-01T00:00:00Z")
  backdateSession(store.filename, protectedID, oldTime)
  backdateSession(store.filename, controlID, oldTime)

  const before = ownedCounts(store.filename, protectedID)
  expect(before.session).toBe(1)
  expect(before.part).toBeGreaterThan(0)
  const db = new Database(store.filename, { readonly: true })
  const assistant = db
    .query<{ data: string }, [string]>(
      "SELECT data FROM message WHERE session_id = ? AND json_extract(data, '$.role') = 'assistant'",
    )
    .get(protectedID)
  const tool = db
    .query<{ data: string }, [string]>(
      "SELECT data FROM part WHERE session_id = ? AND json_extract(data, '$.type') = 'tool'",
    )
    .get(protectedID)
  db.close()
  expect(assistant && JSON.parse(assistant.data)).toMatchObject({ time: { completed: expect.any(Number) } })
  expect(tool && JSON.parse(tool.data)).toMatchObject({
    type: "tool",
    callID: "prune-custody-fixture",
    state: { status: "running" },
  })

  const cutoff = "2026-03-02T00:00:00Z"
  const refused = runCLI(store, ["--before", cutoff, "--session", protectedID, "--apply"])
  const report = JSON.parse(refused.stdout) as Report
  expect(ownedCounts(store.filename, protectedID)).toEqual(before)
  expect(refused.status).toBe(1)
  expect(report.action).toBe("refused")
  expect(report.excludedSessions.find((item) => item.sessionID === protectedID)?.reasons).toContain(
    "assistant-or-tool-work-in-flight",
  )

  const cleanControl = runCLI(store, ["--before", cutoff, "--session", controlID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect((JSON.parse(cleanControl.stdout) as Report).deletedSessionIDs).toEqual([controlID])
  expect(ownedCounts(store.filename, controlID).session).toBe(0)
  expect(ownedCounts(store.filename, protectedID)).toEqual(before)
})

test("a pending prompt added after inventory is revalidated before the first deletion", async () => {
  await using store = await createStore()
  await using controlStore = await createStore()
  const old = await produce(store, { create: {} })
  const control = await produce(controlStore, { create: { payloadBytes: 512 * 1024 } })
  const oldID = old.created[0]!.id
  const controlID = control.created[0]!.id
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  const before = ownedCounts(store.filename, oldID)
  const originalExec = Database.prototype.exec
  let injected = false
  const exec = spyOn(Database.prototype, "exec").mockImplementation(function (this: Database, sql: string) {
    if (sql === "BEGIN IMMEDIATE" && !injected) {
      injected = true
      produce(store, { queue: oldID })
    }
    return originalExec.call(this, sql)
  })
  try {
    const result = await invokeHandler(store, { before: cutoff, session: [oldID], apply: true })
    expect(injected).toBe(true)
    expect(result.report.action).toBe("refused")
    expect(result.report.changedRows).toBe(0)
    expect(result.report.refusalReasons).toContain("selection-changed-after-inventory")
    expect(ownedCounts(store.filename, oldID).session).toBe(before.session)
    expect(ownedCounts(store.filename, oldID).queue).toBeGreaterThan(0)
  } finally {
    exec.mockRestore()
  }

  const cleanControl = await invokeHandler(controlStore, { before: cutoff, session: [controlID], apply: true })
  expect(cleanControl.report.action).toBe("applied")
  expect(cleanControl.report.deletedSessionIDs).toEqual([controlID])
})

test("a live review hold is a liveness blocker, with a clean private control", async () => {
  await using heldStore = await createStore()
  const heldSession = await produce(heldStore, { create: {} })
  const heldID = heldSession.created[0]!.id
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  const holder = startProducer(heldStore, { holdSessionID: heldID, hold: true })
  try {
    const ready = JSON.parse(await readMatchingLine(holder.stdout, "PRUNE_HOLD_READY ")) as {
      ready: boolean
      heldSessionID: string
    }
    expect(ready).toEqual({ ready: true, heldSessionID: heldID })
    const before = ownedCounts(heldStore.filename, heldID)
    const refused = await invokeHandler(heldStore, { before: cutoff, session: [heldID], apply: true })
    expect(refused.report.action).toBe("refused")
    expect(refused.report.changedRows).toBe(0)
    expect(refused.report.reclamation.reasons.some((reason) => reason.startsWith("database-in-use-by-process:"))).toBe(true)
    expect(ownedCounts(heldStore.filename, heldID)).toEqual(before)
  } finally {
    holder.stdin.write("release\n")
    holder.stdin.end()
    await holder.exited
  }

  await using controlStore = await createStore()
  const control = await produce(controlStore, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const controlResult = await invokeHandler(controlStore, {
    before: new Date(Date.now() - 5).toISOString(),
    session: [control.created[0]!.id],
    apply: true,
  })
  expect(controlResult.report.action).toBe("applied")
})

test("a reader that opens after deletion starts is rechecked before commit", async () => {
  await using store = await createStore()
  const old = await produce(store, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  const sessionID = old.created[0]!.id
  const startFile = path.join(store.directory, "late-reader-start")
  const readyFile = path.join(store.directory, "late-reader-ready")
  const holder = startProducer(store, { holdAfterSignal: { startFile, readyFile } })
  try {
    await readMatchingLine(holder.stdout, "PRUNE_HOLDER_WAITING")
    const before = ownedCounts(store.filename, sessionID)
    const originalExec = Database.prototype.exec
    let readerOpened = false
    const exec = spyOn(Database.prototype, "exec").mockImplementation(function (this: Database, statement: string) {
      if (statement === "PRAGMA incremental_vacuum" && !readerOpened) {
        readerOpened = true
        writeFileSync(startFile, "open\n")
        const deadline = Date.now() + 10_000
        const wait = new Int32Array(new SharedArrayBuffer(4))
        while (!existsSync(readyFile)) {
          if (Date.now() >= deadline) throw new Error("late database reader did not acknowledge its open connection")
          Atomics.wait(wait, 0, 0, 5)
        }
      }
      return originalExec.call(this, statement)
    })
    try {
      const result = await invokeHandler(store, { before: cutoff, session: [sessionID], apply: true })
      expect(ownedCounts(store.filename, sessionID)).toEqual(before)
      expect(readerOpened).toBe(true)
      expect(result.report.action).toBe("refused")
      expect(result.report.changedRows).toBe(0)
      expect(result.report.refusalReasons).toEqual(
        expect.arrayContaining([expect.stringMatching(/database-(?:in-use-by-process|liveness-unreadable):/)]),
      )
    } finally {
      exec.mockRestore()
    }
  } finally {
    if (!existsSync(startFile)) writeFileSync(startFile, "release\n")
    await holder.stdin.write("release\n")
    await holder.stdin.end()
    await holder.exited
  }

  await using controlStore = await createStore()
  const control = await produce(controlStore, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const cleanControl = runCLI(controlStore, [
    "--before",
    new Date(Date.now() - 5).toISOString(),
    "--session",
    control.created[0]!.id,
    "--apply",
  ])
  expect(cleanControl.status).toBe(0)
  expect((JSON.parse(cleanControl.stdout) as Report).action).toBe("applied")
})

test("unreadable liveness refuses without row changes, then deletion works when evidence is readable", async () => {
  await using store = await createStore()
  const created = await produce(store, { create: { payloadBytes: 512 * 1024 } })
  const sessionID = created.created[0]!.id
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  const before = ownedCounts(store.filename, sessionID)
  await chmod(store.directory, 0o755)
  try {
    const refused = await invokeHandler(store, { before: cutoff, session: [sessionID], apply: true })
    expect(refused.report.action).toBe("refused")
    expect(refused.report.changedRows).toBe(0)
    expect(refused.report.reclamation.reasons).toContain("database-liveness-unreadable:database-directory-admits-other-users")
    expect(ownedCounts(store.filename, sessionID)).toEqual(before)
  } finally {
    await chmod(store.directory, 0o700)
  }
  const control = await invokeHandler(store, { before: cutoff, session: [sessionID], apply: true })
  expect(control.report.action).toBe("applied")
  expect(ownedCounts(store.filename, sessionID).session).toBe(0)
})

test("unknown aggregate ownership refuses the whole apply while its clean copy can be pruned", async () => {
  await using source = await createStore()
  const created = await produce(source, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  await using unsafe = await copyStore(source)
  const sessionID = created.created[0]!.id
  const db = new Database(unsafe.filename)
  db.exec("INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('agg_unknown', 0, NULL)")
  db.close()
  const before = ownedCounts(unsafe.filename, sessionID)
  expect(before.session).toBe(1)
  expect(before.eventSequence).toBeGreaterThan(0)
  const orphanBefore = new Database(unsafe.filename, { readonly: true })
  expect(count(orphanBefore, "SELECT count(*) AS rows FROM event_sequence WHERE aggregate_id = ?", "agg_unknown")).toBe(
    1,
  )
  orphanBefore.close()

  const refused = runCLI(unsafe, ["--before", cutoff, "--session", sessionID, "--apply"])
  const report = JSON.parse(refused.stdout) as Report
  expect(ownedCounts(unsafe.filename, sessionID)).toEqual(before)
  const orphanAfter = new Database(unsafe.filename, { readonly: true })
  expect(count(orphanAfter, "SELECT count(*) AS rows FROM event_sequence WHERE aggregate_id = ?", "agg_unknown")).toBe(
    1,
  )
  orphanAfter.close()
  expect(refused.status).toBe(1)
  expect(report.action).toBe("refused")
  expect(report.changedRows).toBe(0)
  expect(report.reclamation.reasons).toContain("unknown-aggregate-owner:event_sequence:agg_unknown")

  const control = runCLI(source, ["--before", cutoff, "--session", sessionID, "--apply"])
  expect(control.status).toBe(0)
  expect((JSON.parse(control.stdout) as Report).action).toBe("applied")
  expect(ownedCounts(source.filename, sessionID).session).toBe(0)
})

test("unclassified Session storage refuses without changing populated rows and a clean control deletes", async () => {
  await using source = await createStore()
  const created = await produce(source, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  await using unsafe = await copyStore(source)
  const sessionID = created.created[0]!.id
  const storage = new Database(unsafe.filename)
  storage.exec("CREATE TABLE plugin_session_storage (session_id TEXT NOT NULL, value TEXT NOT NULL)")
  storage.query("INSERT INTO plugin_session_storage (session_id, value) VALUES (?, ?)").run(sessionID, "retained")
  storage.close()

  const before = ownedCounts(unsafe.filename, sessionID)
  expect(before.session).toBe(1)
  expect(before.message).toBeGreaterThan(0)
  expect(before.part).toBeGreaterThan(0)
  const storageBefore = new Database(unsafe.filename, { readonly: true })
  expect(
    count(storageBefore, "SELECT count(*) AS rows FROM plugin_session_storage WHERE session_id = ?", sessionID),
  ).toBe(1)
  storageBefore.close()

  const refused = runCLI(unsafe, ["--before", cutoff, "--session", sessionID, "--apply"])
  const report = JSON.parse(refused.stdout) as Report
  expect(ownedCounts(unsafe.filename, sessionID)).toEqual(before)
  const storageAfter = new Database(unsafe.filename, { readonly: true })
  expect(
    count(storageAfter, "SELECT count(*) AS rows FROM plugin_session_storage WHERE session_id = ?", sessionID),
  ).toBe(1)
  storageAfter.close()
  expect(refused.status).toBe(1)
  expect(report.action).toBe("refused")
  expect(report.changedRows).toBe(0)
  expect(report.reclamation.reasons).toContain("unclassified-session-storage:plugin_session_storage")

  const cleanControl = runCLI(source, ["--before", cutoff, "--session", sessionID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect((JSON.parse(cleanControl.stdout) as Report).action).toBe("applied")
  expect(ownedCounts(source.filename, sessionID).session).toBe(0)
})

test("unclassified message-linked storage is refused through indirect foreign-key ownership", async () => {
  await using source = await createStore()
  const created = produce(source, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  await using unsafe = await copyStore(source)
  const sessionID = created.created[0]!.id
  const messageID = created.created[0]!.messageID
  const db = new Database(unsafe.filename)
  db.exec(`
    CREATE TABLE plugin_message_storage (
      id TEXT PRIMARY KEY,
      message_id TEXT NOT NULL REFERENCES message(id) ON DELETE CASCADE,
      time_created INTEGER NOT NULL,
      data TEXT NOT NULL
    )
  `)
  db.query("INSERT INTO plugin_message_storage (id, message_id, time_created, data) VALUES (?, ?, ?, ?)").run(
    "message-linked-fixture",
    messageID,
    Date.now(),
    "{}",
  )
  db.close()
  const before = ownedCounts(unsafe.filename, sessionID)
  expect(before.session).toBe(1)
  expect(before.message).toBeGreaterThan(0)
  const fixtureBefore = new Database(unsafe.filename, { readonly: true })
  expect(
    count(fixtureBefore, "SELECT count(*) AS rows FROM plugin_message_storage WHERE id = ?", "message-linked-fixture"),
  ).toBe(1)
  fixtureBefore.close()

  const refused = runCLI(unsafe, ["--before", cutoff, "--session", sessionID, "--apply"])
  const report = JSON.parse(refused.stdout) as Report
  expect(ownedCounts(unsafe.filename, sessionID)).toEqual(before)
  const fixtureAfter = new Database(unsafe.filename, { readonly: true })
  expect(
    count(fixtureAfter, "SELECT count(*) AS rows FROM plugin_message_storage WHERE id = ?", "message-linked-fixture"),
  ).toBe(1)
  fixtureAfter.close()
  expect(refused.status).toBe(1)
  expect(report.action).toBe("refused")
  expect(report.changedRows).toBe(0)
  expect(report.reclamation.reasons).toContain("unclassified-session-storage:plugin_message_storage")

  const cleanControl = runCLI(source, ["--before", cutoff, "--session", sessionID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect((JSON.parse(cleanControl.stdout) as Report).action).toBe("applied")
  expect(ownedCounts(source.filename, sessionID).session).toBe(0)
})

test("an unknown durable event owner refuses with its populated event row intact and a clean control deletes", async () => {
  await using source = await createStore()
  const created = await produce(source, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  await using unsafe = await copyStore(source)
  const sessionID = created.created[0]!.id
  const unknownType = "fixture.unknown.durable.owner"
  const db = new Database(unsafe.filename)
  const nextSequence = db
    .query<{ seq: number }, [string]>("SELECT coalesce(max(seq), 0) + 1 AS seq FROM event WHERE aggregate_id = ?")
    .get(sessionID)!.seq
  db.query("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)").run(
    "unknown-owner-event",
    sessionID,
    nextSequence,
    unknownType,
    "{}",
  )
  db.close()
  const before = ownedCounts(unsafe.filename, sessionID)
  expect(before.session).toBe(1)
  expect(before.event).toBeGreaterThan(0)
  const unknownBefore = new Database(unsafe.filename, { readonly: true })
  expect(count(unknownBefore, "SELECT count(*) AS rows FROM event WHERE type = ?", unknownType)).toBe(1)
  unknownBefore.close()

  const refused = runCLI(unsafe, ["--before", cutoff, "--session", sessionID, "--apply"])
  const report = JSON.parse(refused.stdout) as Report
  expect(ownedCounts(unsafe.filename, sessionID)).toEqual(before)
  const unknownAfter = new Database(unsafe.filename, { readonly: true })
  expect(count(unknownAfter, "SELECT count(*) AS rows FROM event WHERE type = ?", unknownType)).toBe(1)
  unknownAfter.close()
  expect(refused.status).toBe(1)
  expect(report.action).toBe("refused")
  expect(report.changedRows).toBe(0)
  expect(report.reclamation.reasons).toContain(`unknown-durable-event-owner:${unknownType}`)

  const cleanControl = runCLI(source, ["--before", cutoff, "--session", sessionID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect((JSON.parse(cleanControl.stdout) as Report).action).toBe("applied")
  expect(ownedCounts(source.filename, sessionID).session).toBe(0)
})

test("a failed integrity check refuses with zero rows changed and the clean control reclaims", async () => {
  await using source = await createStore()
  const created = await produce(source, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  await using unsafe = await copyStore(source)
  const sessionID = created.created[0]!.id
  await corruptSecondaryIndex(unsafe.filename)
  const before = ownedCounts(unsafe.filename, sessionID)
  expect(before.session).toBe(1)
  expect(before.message).toBeGreaterThan(0)
  expect(before.part).toBeGreaterThan(0)
  expect(before.eventSequence).toBeGreaterThan(0)
  const fixtureBefore = new Database(unsafe.filename, { readonly: true })
  expect(fixtureBefore.query("SELECT count(*) AS rows FROM prune_integrity_fixture").get()).toEqual({ rows: 1 })
  fixtureBefore.close()

  const refused = runCLI(unsafe, ["--before", cutoff, "--session", sessionID, "--apply"])
  const report = JSON.parse(refused.stdout) as Report
  expect(ownedCounts(unsafe.filename, sessionID)).toEqual(before)
  const fixtureAfter = new Database(unsafe.filename, { readonly: true })
  expect(fixtureAfter.query("SELECT count(*) AS rows FROM prune_integrity_fixture").get()).toEqual({ rows: 1 })
  fixtureAfter.close()
  expect(refused.status).toBe(1)
  expect(report.action).toBe("refused")
  expect(report.changedRows).toBe(0)
  expect(report.reclamation.integrity).toMatchObject({ state: "failed" })
  expect(report.reclamation.integrity?.details.some((detail) => detail.includes("missing from index"))).toBe(true)
  expect(report.reclamation.reasons).toContain("integrity-check-failed")

  const control = runCLI(source, ["--before", cutoff, "--session", sessionID, "--apply"])
  expect(control.status).toBe(0)
  expect((JSON.parse(control.stdout) as Report).action).toBe("applied")
  expect(ownedCounts(source.filename, sessionID).session).toBe(0)
})

test("stores without incremental auto-vacuum refuse before deleting rows", async () => {
  await using source = await createStore()
  const created = await produce(source, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  await using unsupported = await copyStore(source)
  const sessionID = created.created[0]!.id
  const db = new Database(unsupported.filename)
  db.exec("PRAGMA auto_vacuum=NONE; VACUUM")
  expect(db.query("PRAGMA auto_vacuum").get()).toEqual({ auto_vacuum: 0 })
  db.close()
  const before = ownedCounts(unsupported.filename, sessionID)
  const refused = await invokeHandler(unsupported, { before: cutoff, session: [sessionID], apply: true })
  expect(refused.report.action).toBe("refused")
  expect(refused.report.changedRows).toBe(0)
  expect(refused.report.reclamation.reasons).toContain("incremental-auto-vacuum-unsupported:0")
  expect(ownedCounts(unsupported.filename, sessionID)).toEqual(before)

  const control = await invokeHandler(source, { before: cutoff, session: [sessionID], apply: true })
  expect(control.report.action).toBe("applied")
  expect(ownedCounts(source.filename, sessionID).session).toBe(0)
})

function count(db: Database, query: string, sessionID: string) {
  return db.query<{ rows: number }, [string]>(query).get(sessionID)?.rows ?? 0
}

function textParts(filename: string, sessionID: string) {
  const db = new Database(filename, { readonly: true })
  try {
    return db
      .query<{ data: string }, [string]>("SELECT data FROM part WHERE session_id = ? AND json_extract(data, '$.type') = 'text' ORDER BY id")
      .all(sessionID)
      .map((row) => (JSON.parse(row.data) as { text: string }).text)
  } finally {
    db.close()
  }
}

function storedSessionIDs(filename: string) {
  const db = new Database(filename, { readonly: true })
  try {
    return db.query<{ id: string }, []>("SELECT id FROM session ORDER BY id").all().map((row) => row.id)
  } finally {
    db.close()
  }
}

function addCompleteRetentionMarker(filename: string, sessionID: string) {
  const db = new Database(filename)
  const now = Date.now()
  db.query(
    "INSERT INTO event_retention (aggregate_id, state, progress_table, progress_id, evidence, time_started, time_updated) VALUES (?, 'complete', NULL, NULL, '{}', ?, ?)",
  ).run(sessionID, now, now)
  db.close()
}

function backdateSession(filename: string, sessionID: string, timestamp: number) {
  const db = new Database(filename)
  try {
    db.query("UPDATE session SET time_created = ?, time_updated = ? WHERE id = ?").run(timestamp, timestamp, sessionID)
    db.query("UPDATE message SET time_created = ?, time_updated = ? WHERE session_id = ?").run(
      timestamp,
      timestamp,
      sessionID,
    )
    db.query("UPDATE part SET time_created = ?, time_updated = ? WHERE session_id = ?").run(
      timestamp,
      timestamp,
      sessionID,
    )
    db.query("UPDATE todo SET time_created = ?, time_updated = ? WHERE session_id = ?").run(
      timestamp,
      timestamp,
      sessionID,
    )
    db.query("UPDATE session_message SET time_created = ?, time_updated = ? WHERE session_id = ?").run(
      timestamp,
      timestamp,
      sessionID,
    )
    db.query("UPDATE session_input SET time_created = ? WHERE session_id = ?").run(timestamp, sessionID)
    db.query(
      `UPDATE session_prompt_queue
       SET time_created = ?,
         time_promoted = CASE WHEN time_promoted IS NULL THEN NULL ELSE ? END,
         time_withdrawn = CASE WHEN time_withdrawn IS NULL THEN NULL ELSE ? END,
         time_preparing = CASE WHEN time_preparing IS NULL THEN NULL ELSE ? END
       WHERE session_id = ?`,
    ).run(timestamp, timestamp, timestamp, timestamp, sessionID)
    db.query("UPDATE session_prompt_receipt SET time_created = ?, time_updated = ? WHERE session_id = ?").run(
      timestamp,
      timestamp,
      sessionID,
    )
    db.query("UPDATE session_share SET time_created = ?, time_updated = ? WHERE session_id = ?").run(
      timestamp,
      timestamp,
      sessionID,
    )
    db.query("UPDATE event_retention SET time_started = ?, time_updated = ? WHERE aggregate_id = ?").run(
      timestamp,
      timestamp,
      sessionID,
    )
    db.query("UPDATE event_aggregate_writer SET time_last_append = ? WHERE aggregate_id = ?").run(
      timestamp,
      sessionID,
    )
  } finally {
    db.close()
  }
}

async function createStore(): Promise<Store> {
  await mkdir(scratchRoot, { recursive: true, mode: 0o700 })
  const directory = await mkdtemp(path.join(scratchRoot, "store-"))
  const project = path.join(directory, "project")
  await mkdir(project)
  return {
    filename: path.join(directory, "opencode.db"),
    directory,
    project,
    [Symbol.asyncDispose]: () => rm(directory, { recursive: true, force: true }),
  }
}

async function verifyPendingWork(
  create: { pendingV1?: boolean; preparingV1?: boolean; pendingV2?: boolean; unfinished?: boolean },
  reason: string,
) {
  await using store = await createStore()
  const protectedSession = await produce(store, { create })
  const control = await produce(store, { create: { payloadBytes: 512 * 1024 } })
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  const protectedID = protectedSession.created[0]!.id
  const controlID = control.created[0]!.id
  const beforeProtected = ownedCounts(store.filename, protectedID)
  const refused = await invokeHandler(store, { before: cutoff, session: [protectedID], apply: true })
  expect(refused.report.action).toBe("refused")
  expect(refused.report.changedRows).toBe(0)
  expect(refused.report.excludedSessions.find((entry) => entry.sessionID === protectedID)?.reasons).toContain(reason)
  expect(ownedCounts(store.filename, protectedID)).toEqual(beforeProtected)
  const controlResult = await invokeHandler(store, { before: cutoff, session: [controlID], apply: true })
  expect(controlResult.report.action).toBe("applied")
  expect(controlResult.report.deletedSessionIDs).toEqual([controlID])
  expect(ownedCounts(store.filename, protectedID)).toEqual(beforeProtected)
}

async function copyStore(source: Store) {
  const store = await createStore()
  await copyFile(source.filename, store.filename)
  return store
}

function produce(store: Store, operation: Record<string, unknown>) {
  const result = spawnSync(
    process.execPath,
    [producer, store.filename, store.project, JSON.stringify(operation)],
    {
      cwd: packageRoot,
      encoding: "utf8",
      env: { ...process.env, OPENCODE_DB: store.filename, OPENCODE_DISABLE_CHANNEL_DB: "1" },
      timeout: 30_000,
    },
  )
  if (result.status !== 0) throw new Error(`prune producer failed: ${result.stderr}`)
  return JSON.parse(result.stdout.trim().split("\n").at(-1)!) as ProducerResult
}

function runCLI(store: Store, args: string[]) {
  return spawnSync(process.execPath, [entrypoint, "db", "prune", ...args], {
    cwd: packageRoot,
    encoding: "utf8",
    env: { ...process.env, OPENCODE_DB: store.filename, OPENCODE_DISABLE_CHANNEL_DB: "1", OPENCODE_PURE: "1" },
    timeout: 30_000,
  })
}

async function invokeHandler(
  store: Store,
  args: {
    before: string
    session?: string[]
    apply?: boolean
    firstmateRanIt?: boolean
    measurementStatus?: "measured" | "partial" | "unavailable" | "failed"
    usageLedgerStatus?: "available" | "unavailable" | "failed"
  },
) {
  const previousDatabase = Flag.OPENCODE_DB
  const previousExitCode = process.exitCode
  const output = spyOn(console, "log").mockImplementation(() => {})
  try {
    Flag.OPENCODE_DB = store.filename
    process.exitCode = 0
    await PruneCommand.handler({ _: [], $0: "test", ...args } as Parameters<typeof PruneCommand.handler>[0])
    return {
      report: JSON.parse(String(output.mock.calls.at(-1)?.[0])) as Report,
      exitCode: process.exitCode,
    }
  } finally {
    output.mockRestore()
    Flag.OPENCODE_DB = previousDatabase
    process.exitCode = previousExitCode ?? 0
  }
}

function startProducer(store: Store, operation: Record<string, unknown>) {
  return Bun.spawn([process.execPath, producer, store.filename, store.project, JSON.stringify(operation)], {
    cwd: packageRoot,
    env: { ...process.env, OPENCODE_DB: store.filename, OPENCODE_DISABLE_CHANNEL_DB: "1" },
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
    timeout: 30_000,
  })
}

async function readMatchingLine(stream: ReadableStream<Uint8Array>, prefix: string) {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let text = ""
  let timeout: Timer | undefined
  try {
    return await Promise.race([
      (async () => {
        while (true) {
          const index = text.indexOf("\n")
          if (index >= 0) {
            const line = text.slice(0, index)
            text = text.slice(index + 1)
            if (line.startsWith(prefix)) return line.slice(prefix.length)
            continue
          }
          const result = await reader.read()
          text += decoder.decode(result.value, { stream: !result.done })
          if (result.done) throw new Error("prune producer closed before its ready line")
        }
      })(),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new Error("prune producer did not become ready")), 10_000)
      }),
    ])
  } finally {
    if (timeout) clearTimeout(timeout)
    reader.releaseLock()
  }
}

async function allocatedBytes(filename: string) {
  const files = [filename, `${filename}-wal`, `${filename}-shm`, `${filename}-journal`]
  return (await Promise.all(files.map(async (file) => {
    try {
      return (await stat(file)).blocks * 512
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT") return 0
      throw error
    }
  }))).reduce((sum, bytes) => sum + bytes, 0)
}

async function corruptSecondaryIndex(filename: string) {
  const db = new Database(filename)
  db.exec(
    "CREATE TABLE prune_integrity_fixture (value TEXT NOT NULL); CREATE INDEX prune_integrity_fixture_value_idx ON prune_integrity_fixture(value)",
  )
  db.query("INSERT INTO prune_integrity_fixture (value) VALUES (?)").run("zzzzzzz")
  const index = db
    .query<
      { rootpage: number },
      []
    >("SELECT rootpage FROM sqlite_master WHERE type = 'index' AND name = 'prune_integrity_fixture_value_idx'")
    .get()
  const pageSize = db.query<{ page_size: number }, []>("PRAGMA page_size").get()!.page_size
  const checkpoint = db.query<{ busy: number }, []>("PRAGMA wal_checkpoint(TRUNCATE)").get()
  db.close()
  if (!index) throw new Error("integrity fixture index was not created")
  if (checkpoint?.busy !== 0) throw new Error("integrity fixture checkpoint did not complete")
  const bytes = await readFile(filename)
  const pageStart = (index.rootpage - 1) * pageSize
  const position = bytes.indexOf(Buffer.from("zzzzzzz"), pageStart)
  if (position < pageStart || position >= pageStart + pageSize) {
    throw new Error("integrity fixture index key was not found")
  }
  const file = await open(filename, "r+")
  // Keep the B-tree readable while making its lone index key disagree with the table row.
  await file.write(Buffer.from("aaaaaaa"), 0, 7, position)
  await file.sync()
  await file.close()
}
