import { afterAll, expect, spyOn, test } from "bun:test"
import { Database } from "bun:sqlite"
import { spawnSync } from "node:child_process"
import { chmod, copyFile, mkdir, mkdtemp, open, readFile, rm, stat, writeFile } from "node:fs/promises"
import path from "node:path"
import { Flag } from "@opencode-ai/core/flag/flag"
import { PruneCommand } from "@/cli/cmd/db-prune"

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
    integrityAfter?: { state: string }
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
  expect(report.selectedSessions.map((item) => item.sessionID)).toEqual([old.created[0]!.id])
  expect(report.excludedSessions.find((item) => item.sessionID === recent.created[0]!.id)?.reasons).toContain(
    "activity-at-or-after-cutoff",
  )
  expect(report.reclamation.ready).toBe(true)
  expect(report.rawTranscriptWarning).toContain("raw transcripts")
  expect(await readFile(store.filename)).toEqual(before)

  const invalid = runCLI(store, ["--before", "yesterday", "--apply"])
  expect(invalid.status).toBe(1)
  const absent = runCLI(store, ["--apply"])
  expect(absent.status).not.toBe(0)
  expect(await readFile(store.filename)).toEqual(before)
})

test("CLI apply deletes only the selected whole old Session and reclaims allocated bytes", async () => {
  await using store = await createStore()
  const old = await produce(store, { create: { payloadBytes: 1024 * 1024, withdrawnV1: true } })
  addCompleteRetentionMarker(store.filename, old.created[0]!.id)
  await Bun.sleep(25)
  const cutoff = new Date(Date.now() - 5).toISOString()
  const recent = await produce(store, { create: { payloadBytes: 1024 * 1024, withdrawnV1: true } })
  const oldID = old.created[0]!.id
  const recentID = recent.created[0]!.id
  const oldText = textParts(store.filename, oldID)
  expect(textParts(store.filename, recentID)).toEqual(oldText)
  const ledger = path.join(store.directory, "state", "usage-ledger")
  await mkdir(ledger, { recursive: true })
  const ledgerFile = path.join(ledger, "fixture.jsonl")
  await writeFile(ledgerFile, `${JSON.stringify({ id: "prune-fixture", usage_status: "partial", input_tokens: 12 })}\n`)
  const ledgerBefore = await readFile(ledgerFile)
  const beforeOld = ownedCounts(store.filename, oldID)
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
  expect(result.status).toBe(0)
  const report = JSON.parse(result.stdout) as Report
  expect(report.action).toBe("applied")
  expect(report.deletedSessionIDs).toEqual([oldID])
  expect(report.usageMeasurement).toMatchObject({
    firstmateRun: "confirmed",
    measurementStatus: "partial",
    durableLedgerStatus: "available",
    source: expect.stringContaining("do not gate pruning"),
  })
  expect(report.reclamation.reclaimedBytes).toBeGreaterThan(0)
  expect(report.reclamation.allocatedBytesBefore?.totalBytes).toBeGreaterThan(
    report.reclamation.allocatedBytesAfter?.totalBytes ?? Number.POSITIVE_INFINITY,
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
  const refused = await invokeHandler(unsafe, { before: cutoff, session: [sessionID], apply: true })
  expect(refused.report.action).toBe("refused")
  expect(refused.report.changedRows).toBe(0)
  expect(refused.report.reclamation.reasons).toContain("unknown-aggregate-owner:event_sequence:agg_unknown")
  expect(ownedCounts(unsafe.filename, sessionID)).toEqual(before)

  const control = await invokeHandler(source, { before: cutoff, session: [sessionID], apply: true })
  expect(control.report.action).toBe("applied")
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
  const refused = await invokeHandler(unsafe, { before: cutoff, session: [sessionID], apply: true })
  expect(refused.report.action).toBe("refused")
  expect(refused.report.changedRows).toBe(0)
  expect(refused.report.reclamation.reasons).toContain("integrity-check-unreadable")
  expect(ownedCounts(unsafe.filename, sessionID)).toEqual(before)

  const control = await invokeHandler(source, { before: cutoff, session: [sessionID], apply: true })
  expect(control.report.action).toBe("applied")
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

function addCompleteRetentionMarker(filename: string, sessionID: string) {
  const db = new Database(filename)
  const now = Date.now()
  db.query(
    "INSERT INTO event_retention (aggregate_id, state, progress_table, progress_id, evidence, time_started, time_updated) VALUES (?, 'complete', NULL, NULL, '{}', ?, ?)",
  ).run(sessionID, now, now)
  db.close()
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
  const db = new Database(filename, { readonly: true })
  const index = db
    .query<{ rootpage: number }, []>("SELECT rootpage FROM sqlite_master WHERE type = 'index' AND name = 'session_project_idx'")
    .get()
  const pageSize = db.query<{ page_size: number }, []>("PRAGMA page_size").get()!.page_size
  db.close()
  if (!index) throw new Error("production Session index was not created")
  const file = await open(filename, "r+")
  await file.write(Buffer.from([0]), 0, 1, (index.rootpage - 1) * pageSize)
  await file.sync()
  await file.close()
}
