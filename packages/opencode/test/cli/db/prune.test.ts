import { afterAll, expect, spyOn, test } from "bun:test"
import { Database, type SQLQueryBindings } from "bun:sqlite"
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs"
import {
  chmod,
  copyFile,
  mkdir,
  mkdtemp,
  open,
  readFile,
  realpath,
  readdir,
  rename,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises"
import path from "node:path"
import { Effect } from "effect"
import { Flag } from "@opencode-ai/core/flag/flag"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { PruneCommand } from "@/cli/cmd/db-prune"
import { MessageV2 } from "@/session/message-v2"
import { MessageID, SessionID } from "@/session/schema"

const packageRoot = path.resolve(import.meta.dir, "../../..")
const worktreeRoot = path.resolve(packageRoot, "../..")
const scratchRoot = path.join(worktreeRoot, ".scratch", "opencode-commanded-prune-tests")
const producer = path.join(packageRoot, "test/fixture/prune-producer.ts")
const entrypoint = path.join(packageRoot, "src/index.ts")
const producedTemplates = new Map<string, Promise<Template>>()

type Store = {
  filename: string
  directory: string
  project: string
  producerPID: number | undefined
  metadata: ProducerResult[]
  seeds: StoreSeed[]
  [Symbol.asyncDispose]: () => Promise<void>
}

type ProducerResult = {
  created: { id: string; parentID: string | null; time: number; messageID: string; partID: string }[]
  heldSessionID?: string
}

type Create = {
  parentID?: string
  payloadBytes?: number
  pendingV1?: boolean
  preparingV1?: boolean
  withdrawnV1?: boolean
  pendingV2?: boolean
  unfinished?: boolean
  runningTool?: boolean
  ask?: boolean
  completeStorage?: boolean
}

type ChildResult = {
  operation: string
  args: string[]
  elapsed: number
  status: number | null
  signal: string | null
  error: Error | undefined
  stdout: string
  stderr: string
  pid: number | undefined
}

type StoreSeed = { operation: Create; result: ProducerResult }

type Template = {
  directory: string
  filename: string
  producerPID: number | undefined
  metadata: ProducerResult[]
}

type RunningChild = {
  child: ReturnType<typeof spawnInteractive>
  readiness: ReadableStream<Uint8Array>
  stdout: Promise<string>
  stderr: Promise<string>
  operation: string
  args: string[]
  started: number
  finish: () => Promise<ChildResult>
  [Symbol.asyncDispose]: () => Promise<void>
}

type Report = {
  action: string
  database: string
  cutoff: string
  selectedSessions: { sessionID: string; directory: string; ownedRows: Record<string, number>; reasons: string[] }[]
  excludedSessions: { sessionID: string; directory: string | null; reasons: string[] }[]
  retainedSessions: { sessionID: string; directory: string; ownedRows: Record<string, number>; reasons: string[] }[]
  retainedCounts: { sessions: number; ownedRows: number }
  partial?: boolean
  selectedCounts: { sessions: number; ownedRows: number }
  deletedCounts?: { sessions: number; ownedRows: number; byTable: Record<string, number> }
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
    vacuum?: { state: string; reason?: string }
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
      queueSequence: count(
        db,
        "SELECT count(*) AS rows FROM session_prompt_queue_sequence WHERE session_id = ?",
        sessionID,
      ),
      queueAlias: count(
        db,
        "SELECT count(*) AS rows FROM session_prompt_queue_alias WHERE item_id IN (SELECT id FROM session_prompt_queue WHERE session_id = ?)",
        sessionID,
      ),
      provenance: count(db, "SELECT count(*) AS rows FROM session_prompt_provenance WHERE session_id = ?", sessionID),
      receipt: count(db, "SELECT count(*) AS rows FROM session_prompt_receipt WHERE session_id = ?", sessionID),
      ask: count(db, "SELECT count(*) AS rows FROM session_ask WHERE session_id = ?", sessionID),
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

afterAll(async () => {
  await Promise.all(
    Array.from(producedTemplates.values(), (template) =>
      template.then(
        (produced) => rm(produced.directory, { recursive: true, force: true }),
        () => undefined,
      ),
    ),
  )
  await rm(scratchRoot, { recursive: true, force: true })
})

test("dead-directory inventory counts and apply preserve live and uncertain directories", async () => {
  await using store = await producedStore([
    { payloadBytes: 1024 * 1024, withdrawnV1: true, ask: true, completeStorage: true },
  ])
  const deadID = store.metadata[0]!.created[0]!.id
  const live = await produceLive(store, { create: [{}, {}, {}] })
  const [liveID, emptyID, invalidID] = live.created.map((item) => item.id)
  backdateSession(store.filename, liveID!, Date.parse("2020-01-01T00:00:00Z"))
  const db = new Database(store.filename)
  db.query("UPDATE session SET directory = '' WHERE id = ?").run(emptyID!)
  db.query("UPDATE session SET directory = 'relative-missing' WHERE id = ?").run(invalidID!)
  const projects = db.query("SELECT * FROM project ORDER BY id").all()
  const writers = db.query("SELECT * FROM event_writer_process ORDER BY writer_id").all()
  db.close()
  const beforeDead = ownedCounts(store.filename, deadID)
  const beforeLive = live.created.map((item) => ownedCounts(store.filename, item.id))
  expect(beforeDead).toMatchObject({ session: 1, receipt: 2, ask: 1, sessionInput: 1 })
  expect(beforeDead.queueAlias).toBeGreaterThan(0)
  expect(beforeDead.provenance).toBeGreaterThan(0)
  const inventory = parseReport(await runCLI(store, []))
  expect(inventory.action).toBe("inventory")
  expect(inventory.selectedSessions.map((item) => item.sessionID)).toEqual([deadID])
  expect(inventory.selectedSessions[0]!.directory).not.toBe(store.project)
  const rows = Object.values(beforeDead).reduce((sum, n) => sum + n, 0)
  expect(inventory.selectedCounts).toEqual({ sessions: 1, ownedRows: rows })
  expect(Object.values(inventory.selectedSessions[0]!.ownedRows).reduce((sum, n) => sum + n, 0)).toBe(rows)
  expect(ownedCounts(store.filename, deadID)).toEqual(beforeDead)
  const protectedExplicit = parseReport(
    await runCLI(store, ["--before", "2026-01-01T00:00:00Z", "--session", liveID!, "--apply"], 1),
  )
  expect(protectedExplicit.changedRows).toBe(0)
  expect(protectedExplicit.excludedSessions.find((item) => item.sessionID === liveID)?.reasons).toContain(
    "session-directory-exists",
  )
  const result = parseReport(await runCLI(store, ["--apply"]))
  expect(result.action).toBe("applied")
  expect(result.deletedCounts).toMatchObject({ sessions: 1, ownedRows: rows })
  expect(result.changedRows).toBe(rows)
  expect(result.deletedCounts?.byTable.session_ask).toBe(beforeDead.ask)
  expect(result.deletedCounts?.byTable.session_prompt_queue_alias).toBe(beforeDead.queueAlias)
  expect(result.deletedCounts?.byTable.session_input).toBe(beforeDead.sessionInput)
  expect(Object.values(ownedCounts(store.filename, deadID)).every((n) => n === 0)).toBe(true)
  expect(live.created.map((item) => ownedCounts(store.filename, item.id))).toEqual(beforeLive)
  const retained = new Database(store.filename, { readonly: true })
  expect(retained.query("SELECT * FROM project ORDER BY id").all()).toEqual(projects)
  expect(retained.query("SELECT * FROM event_writer_process ORDER BY writer_id").all()).toEqual(writers)
  expect(retained.query("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" })
  expect(retained.query("PRAGMA foreign_key_check").all()).toEqual([])
  retained.close()
}, 60000)

test("directory uncertainty from inaccessible paths and dangling symlinks never authorizes deletion", async () => {
  await using store = await producedStore([{}, { payloadBytes: 512 * 1024 }, {}, {}])
  const [blockedID, deadID, symlinkID, ambiguousID] = store.metadata.map((item) => item.created[0]!.id)
  const blocked = path.join(store.directory, "inaccessible")
  const dangling = path.join(store.directory, "dangling")
  await mkdir(blocked)
  await symlink(path.join(store.directory, "absent-target"), dangling)
  const db = new Database(store.filename)
  db.query("UPDATE session SET directory = ? WHERE id = ?").run(path.join(blocked, "missing"), blockedID!)
  db.query("UPDATE session SET directory = ? WHERE id = ?").run(path.join(dangling, "missing"), symlinkID!)
  db.query("UPDATE session SET directory = ? WHERE id = ?").run(
    `${dangling}${path.sep}..${path.sep}missing`,
    ambiguousID!,
  )
  db.close()
  const beforeBlocked = ownedCounts(store.filename, blockedID!)
  const beforeSymlink = ownedCounts(store.filename, symlinkID!)
  const beforeAmbiguous = ownedCounts(store.filename, ambiguousID!)
  await chmod(blocked, 0)
  try {
    const result = parseReport(await runCLI(store, ["--apply"]))
    expect(result.deletedSessionIDs).toEqual([deadID!])
    expect(ownedCounts(store.filename, blockedID!)).toEqual(beforeBlocked)
    expect(ownedCounts(store.filename, symlinkID!)).toEqual(beforeSymlink)
    expect(ownedCounts(store.filename, ambiguousID!)).toEqual(beforeAmbiguous)
  } finally {
    await chmod(blocked, 0o700)
  }
})

test("a directory restored after inventory protects its producer-created session", async () => {
  await using store = await producedStore([{ payloadBytes: 512 * 1024 }])
  const id = store.metadata[0]!.created[0]!.id
  const read = new Database(store.filename, { readonly: true })
  const directory = read
    .query<{ directory: string }, [string]>("SELECT directory FROM session WHERE id = ?")
    .get(id)!.directory
  read.close()
  const before = ownedCounts(store.filename, id)
  const original = Database.prototype.exec
  let restored = false
  const exec = spyOn(Database.prototype, "exec").mockImplementation(function (this: Database, sql: string) {
    if (sql === "BEGIN IMMEDIATE" && !restored) {
      restored = true
      mkdirSync(directory)
    }
    return original.call(this, sql)
  })
  try {
    const result = await invokeHandler(store, { apply: true })
    expect(restored).toBe(true)
    expect(result.report.action).toBe("refused")
    expect(result.report.refusalReasons).toContain("selection-changed-after-inventory")
    expect(ownedCounts(store.filename, id)).toEqual(before)
  } finally {
    exec.mockRestore()
    await rm(directory, { recursive: true, force: true })
  }
})

test("dead-directory parents preserve their live-directory descendants", async () => {
  await using store = await producedStore([{}, { payloadBytes: 512 * 1024 }])
  const parentID = store.metadata[0]!.created[0]!.id
  const deadID = store.metadata[1]!.created[0]!.id
  const child = await produceLive(store, { create: { parentID } })
  const childID = child.created[0]!.id
  const beforeParent = ownedCounts(store.filename, parentID)
  const beforeChild = ownedCounts(store.filename, childID)
  const result = parseReport(await runCLI(store, ["--apply"]))
  expect(result.deletedSessionIDs).toEqual([deadID])
  expect(ownedCounts(store.filename, parentID)).toEqual(beforeParent)
  expect(ownedCounts(store.filename, childID)).toEqual(beforeChild)
  expect(result.excludedSessions.find((item) => item.sessionID === parentID)?.reasons).toContain(
    `protected-descendant:${childID}`,
  )
})

test("a directory restored after locked inventory is checked at the destructive decision", async () => {
  await using store = await producedStore([{ payloadBytes: 512 * 1024 }])
  const id = store.metadata[0]!.created[0]!.id
  const read = new Database(store.filename, { readonly: true })
  const directory = read
    .query<{ directory: string }, [string]>("SELECT directory FROM session WHERE id = ?")
    .get(id)!.directory
  read.close()
  const before = ownedCounts(store.filename, id)
  const original = Database.prototype.query
  let checks = 0
  const query = spyOn(Database.prototype, "query").mockImplementation(function <
    ReturnType,
    ParamsType extends SQLQueryBindings | SQLQueryBindings[],
  >(this: Database, statement: string) {
    if (statement === "PRAGMA integrity_check" && ++checks === 3) mkdirSync(directory)
    return (original<ReturnType, ParamsType>).call(this, statement)
  })
  try {
    const result = await invokeHandler(store, { apply: true })
    expect(existsSync(directory)).toBe(true)
    expect(result.report.action).toBe("refused")
    expect(result.report.refusalReasons).toContain("session-directory-restored-before-delete")
    expect(ownedCounts(store.filename, id)).toEqual(before)
  } finally {
    query.mockRestore()
    await rm(directory, { recursive: true, force: true })
  }
})

test("teardown CLI retires only the removed directory and repeated retirement is harmless", async () => {
  await using store = await createStore()
  const dead = await produceLive(store, { create: { payloadBytes: 512 * 1024, withdrawnV1: true, ask: true } })
  const id = dead.created[0]!.id
  const before = ownedCounts(store.filename, id)
  const target = store.project
  const retirement = { command: "retire", databaseDefault: path.join(store.directory, "not-the-target.db") }
  const refused = parseReport(
    await runCLI(store, ["--database", store.filename, "--directory", target, "--apply"], 1, retirement),
  )
  expect(refused.action).toBe("refused")
  expect(refused.database).toBe(store.filename)
  expect(ownedCounts(store.filename, id)).toEqual(before)
  const liveDirectory = path.join(store.directory, "live-worktree")
  await mkdir(liveDirectory)
  const live = await produceLive({ ...store, project: liveDirectory }, { create: {} })
  const liveID = live.created[0]!.id
  const liveBefore = ownedCounts(store.filename, liveID)
  const unrelatedDirectory = store.project + "-sibling"
  await mkdir(unrelatedDirectory)
  const unrelated = await produceLive({ ...store, project: unrelatedDirectory }, { create: {} })
  const unrelatedID = unrelated.created[0]!.id
  const unrelatedBefore = ownedCounts(store.filename, unrelatedID)
  await rm(unrelatedDirectory, { recursive: true })
  await rm(target, { recursive: true })
  const inventory = parseReport(
    await runCLI(store, ["--database", store.filename, "--directory", target], 0, retirement),
  )
  expect(inventory.action).toBe("inventory")
  expect(inventory.deletionScope.sessionIDs).toEqual([id])
  expect(ownedCounts(store.filename, id)).toEqual(before)
  const result = parseReport(
    await runCLI(store, ["--database", store.filename, "--directory", target, "--apply"], 0, retirement),
  )
  expect(result.action).toBe("applied")
  expect(result.deletedCounts?.sessions).toBe(1)
  expect(Object.values(ownedCounts(store.filename, id)).every((n) => n === 0)).toBe(true)
  const repeated = parseReport(
    await runCLI(store, ["--database", store.filename, "--directory", target, "--apply"], 0, retirement),
  )
  expect(repeated.action).toBe("noop")
  expect(repeated.changedRows).toBe(0)
  expect(repeated.reclamation.vacuum).toEqual({ state: "skipped", reason: "no-deletions" })
  expect(ownedCounts(store.filename, liveID)).toEqual(liveBefore)
  expect(ownedCounts(store.filename, unrelatedID)).toEqual(unrelatedBefore)
}, 60000)

test("small retirement completes even without positive allocated-byte recovery", async () => {
  await using store = await createStore()
  const setup = new Database(store.filename)
  setup.exec("PRAGMA page_size=65536; VACUUM")
  setup.close()
  const created = await produceLive(store, { create: {} })
  const id = created.created[0]!.id
  const before = ownedCounts(store.filename, id)
  await rm(store.project, { recursive: true })
  const result = parseReport(await runCLI(store, ["--directory", store.project, "--apply"], 0, { command: "retire" }))
  expect(result.action).toBe("applied")
  expect(result.reclamation.vacuum?.state).toBe("complete")
  expect(result.reclamation.reclaimedBytes).toBeLessThanOrEqual(0)
  expect(result.deletedCounts?.ownedRows).toBe(Object.values(before).reduce((sum, n) => sum + n, 0))
  expect(Object.values(ownedCounts(store.filename, id)).every((n) => n === 0)).toBe(true)
  expect(result.retainedCounts).toEqual({ sessions: 0, ownedRows: 0 })
}, 60000)

test.each(["trailing separator", "parent segment", "resolved symlink"])(
  "retirement normalizes %s for selection and retained-target detection",
  async (spelling) => {
    await using store = await createStore()
    const nested = path.join(store.project, "repo")
    await mkdir(nested)
    const alias = path.join(store.directory, "alias")
    await symlink(store.directory, alias)
    const recorded = spelling === "trailing separator" ? store.project : nested
    const created = await produceLive({ ...store, project: recorded }, { create: [{}, { pendingV1: true }] })
    const eligibleID = created.created[0]!.id
    const protectedID = created.created[1]!.id
    const protectedBefore = ownedCounts(store.filename, protectedID)
    const target =
      spelling === "trailing separator"
        ? store.project + path.sep
        : spelling === "parent segment"
          ? `${store.project}${path.sep}..${path.sep}project`
          : path.join(alias, "project")
    await rm(store.project, { recursive: true })
    const inventory = parseReport(await runCLI(store, ["--directory", target], 0, { command: "retire" }))
    expect(inventory.deletionScope.sessionIDs).toEqual([eligibleID])
    expect(inventory.retainedSessions.map((item) => item.sessionID)).toEqual([protectedID])
    const applied = parseReport(await runCLI(store, ["--directory", target, "--apply"], 0, { command: "retire" }))
    expect(applied.action).toBe("applied")
    expect(applied.partial).toBe(true)
    expect(applied.deletedSessionIDs).toEqual([eligibleID])
    expect(Object.values(ownedCounts(store.filename, eligibleID)).every((n) => n === 0)).toBe(true)
    expect(ownedCounts(store.filename, protectedID)).toEqual(protectedBefore)
    expect(applied.retainedCounts.ownedRows).toBe(Object.values(protectedBefore).reduce((sum, n) => sum + n, 0))
    const retry = parseReport(await runCLI(store, ["--directory", target, "--apply"], 1, { command: "retire" }))
    expect(retry.action).toBe("refused")
    expect(retry.changedRows).toBe(0)
    expect(retry.retainedSessions.map((item) => item.sessionID)).toEqual([protectedID])
    expect(ownedCounts(store.filename, protectedID)).toEqual(protectedBefore)
  },
  60000,
)

test("mixed live and dead directory targets retire independently and report retained rows", async () => {
  await using store = await createStore()
  const live = await produceLive(store, { create: {} })
  const liveID = live.created[0]!.id
  const liveBefore = ownedCounts(store.filename, liveID)
  const nested = path.join(store.project, "removed-task")
  await mkdir(nested)
  const dead = await produceLive({ ...store, project: nested }, { create: {} })
  const deadID = dead.created[0]!.id
  await rm(nested, { recursive: true })
  const applied = parseReport(await runCLI(store, ["--directory", store.project, "--apply"], 0, { command: "retire" }))
  expect(applied.action).toBe("applied")
  expect(applied.partial).toBe(true)
  expect(applied.deletedSessionIDs).toEqual([deadID])
  expect(Object.values(ownedCounts(store.filename, deadID)).every((n) => n === 0)).toBe(true)
  expect(ownedCounts(store.filename, liveID)).toEqual(liveBefore)
  expect(applied.retainedSessions.map((item) => item.sessionID)).toEqual([liveID])
  expect(applied.retainedSessions[0]!.reasons).toContain("session-directory-exists")
  expect(applied.retainedCounts).toEqual({
    sessions: 1,
    ownedRows: Object.values(liveBefore).reduce((sum, n) => sum + n, 0),
  })
}, 60000)

test(
  "CLI inventory is read-only, validates the explicit cutoff, and narrows by Session ID",
  async () => {
    await using store = await producedStore([{ payloadBytes: 128 }, { payloadBytes: 128 }])
    const old = produce(store, { create: { payloadBytes: 128 } })
    const recent = produce(store, { create: { payloadBytes: 128 } })
    const cutoffAt = Date.now() - 5
    backdateSession(store.filename, old.created[0]!.id, cutoffAt - 1_000)
    backdateSession(store.filename, recent.created[0]!.id, cutoffAt + 1)
    const cutoff = new Date(cutoffAt).toISOString()
    const sessionIDsBefore = storedSessionIDs(store.filename)
    const before = await readFile(store.filename)
    const inventory = await runCLI(store, [
      "--before",
      cutoff,
      "--session",
      old.created[0]!.id,
      "--session",
      recent.created[0]!.id,
    ])
    const report = parseReport(inventory)
    expect(report.action).toBe("inventory")
    expect(report.database).toBe(await realpath(store.filename))
    expect(report).not.toHaveProperty("parentSnapshot")
    const aliasedPath = { ...store, filename: `${store.directory}/alias/../opencode.db` }
    const aliasedInventory = await runCLI(aliasedPath, ["--before", cutoff])
    expect(aliasedInventory.status).toBe(0)
    expect(parseReport(aliasedInventory).database).toBe(await realpath(store.filename))
    expect(report.selectedSessions.map((item) => item.sessionID)).toEqual([old.created[0]!.id])
    expect(report.excludedSessions.find((item) => item.sessionID === recent.created[0]!.id)?.reasons).toContain(
      "activity-at-or-after-cutoff",
    )
    expect(report.reclamation.ready).toBe(true)
    expect(report.rawTranscriptWarning).toContain("raw transcripts")
    expect(await readFile(store.filename)).toEqual(before)

    const invalid = await runCLI(store, ["--before", "yesterday", "--apply"], 1)
    expect(storedSessionIDs(store.filename)).toEqual(sessionIDsBefore)
    expect(invalid.status).toBe(1)
    const rollover = await runCLI(store, ["--before", "2026-02-30T00:00:00Z", "--apply"], 1)
    expect(rollover.status).toBe(1)
    expect(parseReport(rollover).reclamation.reasons).toContain("invalid-or-missing-cutoff")
    expect(storedSessionIDs(store.filename)).toEqual(sessionIDsBefore)
    const future = await runCLI(store, ["--before", new Date(Date.now() + 60_000).toISOString(), "--apply"], 1)
    expect(future.status).toBe(1)
    expect(storedSessionIDs(store.filename)).toEqual(sessionIDsBefore)
    const absent = await runCLI(store, ["--directory", "", "--apply"], "nonzero")
    expect(absent.status).not.toBe(0)
    expect(storedSessionIDs(store.filename)).toEqual(sessionIDsBefore)
    expect(await readFile(store.filename)).toEqual(before)
  },
  { timeout: 60_000 },
)

test("rollover dates and hours are refused even when they would otherwise select an old Session", async () => {
  await using store = await producedStore([{ payloadBytes: 512 * 1024 }])
  const created = produce(store, { create: { payloadBytes: 512 * 1024 } })
  const sessionID = created.created[0]!.id
  const oldTime = Date.parse("2026-01-01T00:00:00Z")
  backdateSession(store.filename, sessionID, oldTime)
  const before = ownedCounts(store.filename, sessionID)
  expect(before.session).toBe(1)
  expect(before.message).toBeGreaterThan(0)
  expect(before.part).toBeGreaterThan(0)

  const invalid = await runCLI(store, ["--before", "2026-02-30T00:00:00Z", "--session", sessionID, "--apply"], 1)
  const refused = parseReport(invalid)
  expect(ownedCounts(store.filename, sessionID)).toEqual(before)
  expect(invalid.status).toBe(1)
  expect(refused.action).toBe("refused")
  expect(refused.reclamation.reasons).toContain("invalid-or-missing-cutoff")

  const invalidHour = await runCLI(store, ["--before", "2026-03-02T24:00:00Z", "--session", sessionID, "--apply"], 1)
  expect(ownedCounts(store.filename, sessionID)).toEqual(before)
  expect(invalidHour.status).toBe(1)
  expect(parseReport(invalidHour).reclamation.reasons).toContain("invalid-or-missing-cutoff")

  const cleanControl = await runCLI(store, ["--before", "2026-03-02T00:00:00Z", "--session", sessionID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect(parseReport(cleanControl).action).toBe("applied")
  expect(ownedCounts(store.filename, sessionID).session).toBe(0)
})

test("CLI apply deletes only the selected whole old Session and reclaims allocated bytes", async () => {
  await using store = await producedStore([
    { payloadBytes: 128 },
    { payloadBytes: 1024 * 1024, withdrawnV1: true },
    { payloadBytes: 1024 * 1024, withdrawnV1: true },
  ])
  const unrequested = produce(store, { create: { payloadBytes: 128 } })
  const old = produce(store, { create: { payloadBytes: 1024 * 1024, withdrawnV1: true } })
  addCompleteRetentionMarker(store.filename, old.created[0]!.id)
  const recent = produce(store, { create: { payloadBytes: 1024 * 1024, withdrawnV1: true } })
  const oldID = old.created[0]!.id
  const unrequestedID = unrequested.created[0]!.id
  const recentID = recent.created[0]!.id
  const cutoffAt = Date.now() - 5
  backdateSession(store.filename, unrequestedID, cutoffAt - 1_000)
  backdateSession(store.filename, oldID, cutoffAt - 1_000)
  backdateSession(store.filename, recentID, cutoffAt + 1)
  const cutoff = new Date(cutoffAt).toISOString()
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
  const result = await runCLI(store, [
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
  const report = parseReport(result)
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
    ask: 0,
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
  await using store = await producedStore([{ payloadBytes: 512 * 1024 }])
  const old = produce(store, { create: { payloadBytes: 512 * 1024 } })
  const sessionID = old.created[0]!.id
  const cutoffAt = Date.now() - 5
  backdateSession(store.filename, sessionID, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
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
    if (!result.report.committed) throw new Error(JSON.stringify(result.report, null, 2))
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
  await using store = await producedStore([{ payloadBytes: 512 * 1024 }])
  const old = produce(store, { create: { payloadBytes: 512 * 1024 } })
  const cutoffAt = Date.now() - 5
  backdateSession(store.filename, old.created[0]!.id, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
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
  await using store = await producedStore([{ payloadBytes: 512 * 1024 }, {}])
  const control = produce(store, { create: { payloadBytes: 512 * 1024 } })
  const touched = produce(store, { create: {} })
  const controlID = control.created[0]!.id
  const touchedID = touched.created[0]!.id
  const cutoffAt = Date.now() - 5
  backdateSession(store.filename, controlID, cutoffAt - 1_000)
  backdateSession(store.filename, touchedID, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
  await produceLive(store, { touch: touchedID })
  const prior = new Database(store.filename, { readonly: true })
  const createdAt = prior
    .query<{ time_created: number }, [string]>("SELECT time_created FROM session WHERE id = ?")
    .get(touchedID)!.time_created
  const latest = prior
    .query<
      { time_last_append: number },
      [string]
    >("SELECT max(time_last_append) AS time_last_append FROM event_aggregate_writer WHERE aggregate_id = ?")
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
  await using store = await producedStore([{ payloadBytes: 512 * 1024 }, {}])
  const selected = produce(store, { create: { payloadBytes: 512 * 1024 } })
  const parent = produce(store, { create: {} })
  const cutoffAt = Date.now() - 5
  backdateSession(store.filename, selected.created[0]!.id, cutoffAt - 1_000)
  backdateSession(store.filename, parent.created[0]!.id, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
  const child = await produceLive(store, { create: { parentID: parent.created[0]!.id } })
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
  await using source = await producedStore([{ payloadBytes: 512 * 1024 }, { payloadBytes: 128 }])
  const old = produce(source, { create: { payloadBytes: 512 * 1024 } })
  const protectedSession = produce(source, { create: { payloadBytes: 128 } })
  const cutoffAt = Date.now() - 5
  backdateSession(source.filename, old.created[0]!.id, cutoffAt - 1_000)
  backdateSession(source.filename, protectedSession.created[0]!.id, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
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

  const refused = await runCLI(unsafe, ["--before", cutoff, "--session", oldID, "--apply"], 1)
  const report = parseReport(refused)
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

  const cleanControl = await runCLI(source, ["--before", cutoff, "--session", oldID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect(parseReport(cleanControl).action).toBe("applied")
  expect(ownedCounts(source.filename, oldID).session).toBe(0)
  expect(ownedCounts(source.filename, protectedID)).toEqual(beforeProtected)
})

test("a selected Part linked to an unselected Message refuses and preserves MessageV2 output", async () => {
  await using source = await producedStore([{ payloadBytes: 512 * 1024 }, { payloadBytes: 128 }])
  const selected = produce(source, { create: { payloadBytes: 512 * 1024 } })
  const retained = produce(source, { create: { payloadBytes: 128 } })
  const selectedID = selected.created[0]!.id
  const retainedID = retained.created[0]!.id
  const retainedMessageID = retained.created[0]!.messageID
  const cutoff = "2026-03-02T00:00:00Z"
  const oldTime = Date.parse("2026-01-01T00:00:00Z")
  backdateSession(source.filename, selectedID, oldTime)
  await using unsafe = await copyStore(source)
  await produceLive(unsafe, { crossPart: { sessionID: selectedID, messageID: retainedMessageID } })
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

  const refused = await runCLI(unsafe, ["--before", cutoff, "--session", selectedID, "--apply"], 1)
  const report = parseReport(refused)
  expect(await readParts(unsafe.filename)).toEqual(beforeParts)
  expect(ownedCounts(unsafe.filename, selectedID)).toEqual(beforeSelected)
  expect(ownedCounts(unsafe.filename, retainedID)).toEqual(beforeRetained)
  expect(refused.status).toBe(1)
  expect(report.action).toBe("refused")
  expect(report.changedRows).toBe(0)
  expect(report.reclamation.reasons.some((reason) => reason.startsWith("cross-session-cascade:part.message_id:"))).toBe(
    true,
  )

  const cleanControl = await runCLI(source, ["--before", cutoff, "--session", selectedID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect(parseReport(cleanControl).deletedSessionIDs).toEqual([selectedID])
  expect(await readParts(source.filename)).toEqual(cleanPartsBefore)
  expect(ownedCounts(source.filename, retainedID)).toEqual(beforeRetained)
})

test("a pending V1 queue prompt refuses explicit deletion while a clean control deletes", () =>
  verifyPendingWork({ pendingV1: true }, "v1-prompt-pending-or-in-custody"))

test("a preparing V1 prompt refuses explicit deletion while a clean control deletes", () =>
  verifyPendingWork({ preparingV1: true }, "v1-prompt-pending-or-in-custody"))

test("a durable V2 input refuses explicit deletion while a clean control deletes", () =>
  verifyPendingWork({ pendingV2: true }, "v2-input-pending"))

test("an unfinished assistant turn refuses explicit deletion while a clean control deletes", () =>
  verifyPendingWork({ unfinished: true }, "assistant-or-tool-work-in-flight"))

test("an outstanding supervised ask refuses deletion, and once settled it is deleted with its Session", async () => {
  await using store = await producedStore([{ ask: true, payloadBytes: 512 * 1024 }, { payloadBytes: 512 * 1024 }])
  const asked = await produce(store, { create: { ask: true, payloadBytes: 512 * 1024 } })
  const control = await produce(store, { create: { payloadBytes: 512 * 1024 } })
  const askedID = asked.created[0]!.id
  const controlID = control.created[0]!.id
  const cutoffAt = Date.now() - 5
  backdateSession(store.filename, askedID, cutoffAt - 1_000)
  backdateSession(store.filename, controlID, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
  const before = ownedCounts(store.filename, askedID)
  expect(before.ask).toBe(1)

  // The producer's ask, reopened as one a server still waits on.
  const db = new Database(store.filename)
  const settled = db
    .query<
      { decision: string; time_settled: number },
      [string]
    >("SELECT decision, time_settled FROM session_ask WHERE session_id = ?")
    .get(askedID)!
  db.query("UPDATE session_ask SET decision = NULL, time_settled = NULL, time_created = ? WHERE session_id = ?").run(
    cutoffAt - 1_000,
    askedID,
  )
  db.close()
  const refused = await invokeHandler(store, { before: cutoff, session: [askedID], apply: true })
  expect(refused.report.action).toBe("refused")
  expect(refused.report.changedRows).toBe(0)
  expect(refused.report.excludedSessions.find((entry) => entry.sessionID === askedID)?.reasons).toContain(
    "supervised-ask-pending",
  )
  expect(ownedCounts(store.filename, askedID)).toEqual(before)
  const controlResult = await invokeHandler(store, { before: cutoff, session: [controlID], apply: true })
  expect(controlResult.report.deletedSessionIDs).toEqual([controlID])

  const reopened = new Database(store.filename)
  reopened
    .query("UPDATE session_ask SET decision = ?, time_settled = ? WHERE session_id = ?")
    .run(settled.decision, cutoffAt - 1_000, askedID)
  reopened.close()
  const applied = await invokeHandler(store, { before: cutoff, session: [askedID], apply: true })
  expect(applied.report.action).toBe("applied")
  expect(applied.report.deletedSessionIDs).toEqual([askedID])
  expect(ownedCounts(store.filename, askedID)).toMatchObject({ session: 0, ask: 0 })
}, 60_000)

test(
  "a persisted running tool Part stays protected after its assistant turn completes",
  async () => {
    await using store = await producedStore([{ runningTool: true }, { payloadBytes: 512 * 1024 }])
    const protectedSession = produce(store, { create: { runningTool: true } })
    const control = produce(store, { create: { payloadBytes: 512 * 1024 } })
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
      .query<
        { data: string },
        [string]
      >("SELECT data FROM message WHERE session_id = ? AND json_extract(data, '$.role') = 'assistant'")
      .get(protectedID)
    const tool = db
      .query<
        { data: string },
        [string]
      >("SELECT data FROM part WHERE session_id = ? AND json_extract(data, '$.type') = 'tool'")
      .get(protectedID)
    db.close()
    expect(assistant && JSON.parse(assistant.data)).toMatchObject({ time: { completed: expect.any(Number) } })
    expect(tool && JSON.parse(tool.data)).toMatchObject({
      type: "tool",
      callID: "prune-custody-fixture",
      state: { status: "running" },
    })

    const cutoff = "2026-03-02T00:00:00Z"
    const refused = await runCLI(store, ["--before", cutoff, "--session", protectedID, "--apply"], 1)
    const report = parseReport(refused)
    expect(ownedCounts(store.filename, protectedID)).toEqual(before)
    expect(refused.status).toBe(1)
    expect(report.action).toBe("refused")
    expect(report.excludedSessions.find((item) => item.sessionID === protectedID)?.reasons).toContain(
      "assistant-or-tool-work-in-flight",
    )

    const controlInventory = await runCLI(store, ["--before", cutoff, "--session", controlID])
    const controlInventoryReport = parseReport(controlInventory)
    if (
      controlInventory.status !== 0 ||
      controlInventoryReport.selectedSessions.length !== 1 ||
      controlInventoryReport.selectedSessions[0]?.sessionID !== controlID ||
      !controlInventoryReport.reclamation.ready
    ) {
      throw new Error(`clean control inventory was not ready: ${controlInventory.stdout}`)
    }

    const cleanControl = await runCLI(store, ["--before", cutoff, "--session", controlID, "--apply"])
    if (cleanControl.status !== 0) {
      throw new Error(
        `clean control CLI failed: ${JSON.stringify({
          status: cleanControl.status,
          signal: cleanControl.signal,
          error: cleanControl.error?.message,
          stderr: cleanControl.stderr,
          stdout: cleanControl.stdout,
        })}`,
      )
    }
    expect(cleanControl.status).toBe(0)
    const cleanReport = parseReport(cleanControl)
    expect(cleanReport.action).toBe("applied")
    expect(cleanReport.deletedSessionIDs).toEqual([controlID])
    expect(cleanReport.reclamation.reclaimedBytes).toBeGreaterThan(0)
    expect(ownedCounts(store.filename, controlID).session).toBe(0)
    expect(ownedCounts(store.filename, protectedID)).toEqual(before)
  },
  { timeout: 60_000 },
)

test("a pending prompt admitted as offline cleanup acquires ownership remains protected", async () => {
  await using store = await producedStore([{}])
  await using controlStore = await producedStore([{ payloadBytes: 512 * 1024 }])
  const old = produce(store, { create: {} })
  const control = produce(controlStore, { create: { payloadBytes: 512 * 1024 } })
  const oldID = old.created[0]!.id
  const controlID = control.created[0]!.id
  const cutoffAt = Date.now() - 5
  backdateSession(store.filename, oldID, cutoffAt - 1_000)
  backdateSession(controlStore.filename, controlID, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
  const before = ownedCounts(store.filename, oldID)
  const startFile = path.join(store.directory, "queue-admission-start")
  const readyFile = path.join(store.directory, "queue-admission-ready")
  const writeStartedFile = path.join(store.directory, "queue-admission-ready-write-started")
  const writeReleaseFile = path.join(store.directory, "queue-admission-ready-write-release")
  await using admission = startProducer(store, {
    queueAfterSignal: {
      sessionID: oldID,
      startFile,
      readyFile,
      readinessWriteGate: { createdFile: writeStartedFile, releaseFile: writeReleaseFile },
    },
  })
  await readMatchingLine(admission, "PRUNE_QUEUE_WAITING")
  const originalExec = Database.prototype.exec
  let injected = false
  let readinessDuringWrite: string | undefined
  const exec = spyOn(Database.prototype, "exec").mockImplementation(function (this: Database, sql: string) {
    if (sql === "PRAGMA locking_mode = EXCLUSIVE" && !injected) {
      injected = true
      writeFileSync(startFile, "admit\n")
      const deadline = Date.now() + 10_000
      const wait = new Int32Array(new SharedArrayBuffer(4))
      while (!existsSync(writeStartedFile)) {
        if (Date.now() >= deadline)
          throw new Error(`queue producer ${admission.child.pid} did not reach its readiness write`)
        Atomics.wait(wait, 0, 0, 5)
      }
      readinessDuringWrite = existsSync(readyFile) ? readFileSync(readyFile, "utf8") : undefined
      writeFileSync(writeReleaseFile, "write\n")
      while (!existsSync(readyFile)) {
        if (Date.now() >= deadline) throw new Error(`queue producer ${admission.child.pid} did not publish readiness`)
        Atomics.wait(wait, 0, 0, 5)
      }
      const result = JSON.parse(readFileSync(readyFile, "utf8")) as { ready: boolean; error?: string }
      if (!result.ready) throw new Error(`queue producer failed before revalidation: ${result.error}`)
      backdateSession(store.filename, oldID, cutoffAt - 1_000)
    }
    return originalExec.call(this, sql)
  })
  try {
    const result = await invokeHandler(store, { before: cutoff, session: [oldID], apply: true })
    expect(injected).toBe(true)
    expect(readinessDuringWrite).toBeUndefined()
    expect(result.report.action).toBe("refused")
    expect(result.report.changedRows).toBe(0)
    expect(result.report.excludedSessions.find((item) => item.sessionID === oldID)?.reasons).toContain(
      "v1-prompt-pending-or-in-custody",
    )
    expect(ownedCounts(store.filename, oldID).session).toBe(before.session)
    expect(ownedCounts(store.filename, oldID).queue).toBeGreaterThan(0)
  } finally {
    exec.mockRestore()
    if (!existsSync(startFile)) writeFileSync(startFile, "release\n")
    if (!existsSync(writeReleaseFile)) writeFileSync(writeReleaseFile, "release\n")
    assertChildOutcome(await admission.finish(), 0)
  }

  const cleanControl = await invokeHandler(controlStore, { before: cutoff, session: [controlID], apply: true })
  expect(cleanControl.report.action).toBe("applied")
  expect(cleanControl.report.deletedSessionIDs).toEqual([controlID])
})

test("a failed queue producer publishes its complete readiness error atomically", async () => {
  await using store = await producedStore([{}])
  const startFile = path.join(store.directory, "failed-queue-admission-start")
  const readyFile = path.join(store.directory, "failed-queue-admission-ready")
  const writeStartedFile = path.join(store.directory, "failed-queue-admission-write-started")
  const writeReleaseFile = path.join(store.directory, "failed-queue-admission-write-release")
  await using failedAdmission = startProducer(store, {
    queueAfterSignal: {
      sessionID: "ses_missing",
      startFile,
      readyFile,
      readinessWriteGate: { createdFile: writeStartedFile, releaseFile: writeReleaseFile },
    },
  })
  let readinessDuringWrite: string | undefined
  try {
    await readMatchingLine(failedAdmission, "PRUNE_QUEUE_WAITING")
    writeFileSync(startFile, "admit\n")
    const deadline = Date.now() + 10_000
    const wait = new Int32Array(new SharedArrayBuffer(4))
    while (!existsSync(writeStartedFile)) {
      if (Date.now() >= deadline) throw new Error("failed queue producer did not reach its readiness write")
      Atomics.wait(wait, 0, 0, 5)
    }
    readinessDuringWrite = existsSync(readyFile) ? readFileSync(readyFile, "utf8") : undefined
    writeFileSync(writeReleaseFile, "write\n")
    expect(readinessDuringWrite).toBeUndefined()
    while (!existsSync(readyFile)) {
      if (Date.now() >= deadline) throw new Error("failed queue producer did not publish its readiness error")
      Atomics.wait(wait, 0, 0, 5)
    }
    const readiness = JSON.parse(readFileSync(readyFile, "utf8")) as { ready: boolean; error?: string }
    expect(readiness.ready).toBe(false)
    expect(readiness.error?.length).toBeGreaterThan(0)
  } finally {
    if (!existsSync(startFile)) writeFileSync(startFile, "release\n")
    if (!existsSync(writeReleaseFile)) writeFileSync(writeReleaseFile, "release\n")
  }
  assertChildOutcome(await failedAdmission.finish(), "nonzero")
})

test("a live review hold is a liveness blocker, with a clean private control", async () => {
  await using heldStore = await producedStore([{ payloadBytes: 512 * 1024 }])
  const heldSession = produce(heldStore, { create: { payloadBytes: 512 * 1024 } })
  const heldID = heldSession.created[0]!.id
  const cutoffAt = Date.now() - 5
  backdateSession(heldStore.filename, heldID, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
  await using holder = startProducer(heldStore, { holdSessionID: heldID, hold: true })
  try {
    const ready = JSON.parse(await readMatchingLine(holder, "PRUNE_HOLD_READY ")) as {
      ready: boolean
      heldSessionID: string
    }
    expect(ready).toEqual({ ready: true, heldSessionID: heldID })
    const before = ownedCounts(heldStore.filename, heldID)
    const refused = await invokeHandler(heldStore, { before: cutoff, session: [heldID], apply: true })
    expect(refused.report.action).toBe("refused")
    expect(refused.report.changedRows).toBe(0)
    expect(refused.report.reclamation.reasons.some((reason) => reason.startsWith("database-in-use-by-process:"))).toBe(
      true,
    )
    expect(ownedCounts(heldStore.filename, heldID)).toEqual(before)
  } finally {
    holder.child.stdin.write("release\n")
    holder.child.stdin.end()
    assertChildOutcome(await holder.finish(), 0)
  }

  const controlResult = await invokeHandler(heldStore, {
    before: cutoff,
    session: [heldID],
    apply: true,
  })
  expect(controlResult.report.action).toBe("applied")
  expect(ownedCounts(heldStore.filename, heldID).session).toBe(0)
})

test("a post-commit vacuum failure reports deletion and a no-op retry skips compaction", async () => {
  await using store = await producedStore([{ payloadBytes: 1024 * 1024 }])
  const id = store.metadata[0]!.created[0]!.id
  const read = new Database(store.filename, { readonly: true })
  const directory = read
    .query<{ directory: string }, [string]>("SELECT directory FROM session WHERE id = ?")
    .get(id)!.directory
  read.close()
  const original = Database.prototype.exec
  const exec = spyOn(Database.prototype, "exec").mockImplementation(function (this: Database, sql: string) {
    if (sql === "VACUUM") throw new Error("private-vacuum-failure")
    return original.call(this, sql)
  })
  try {
    const failed = await invokeHandler(store, { directory, apply: true })
    if (failed.report.action !== "failed") throw new Error(JSON.stringify(failed.report, null, 2))
    expect(failed.report.action).toBe("failed")
    expect(failed.report.committed).toBe(true)
    expect(failed.report.changedRows).toBeGreaterThan(0)
    expect(failed.report.deletedSessionIDs).toEqual([id])
    expect(failed.report.reclamation.vacuum).toEqual({ state: "failed", reason: "private-vacuum-failure" })
    expect(Object.values(ownedCounts(store.filename, id)).every((n) => n === 0)).toBe(true)
  } finally {
    exec.mockRestore()
  }
  const retryExec = spyOn(Database.prototype, "exec")
  try {
    const retry = await invokeHandler(store, { directory, apply: true })
    if (retry.report.action !== "noop") throw new Error(JSON.stringify(retry.report, null, 2))
    expect(retry.report.action).toBe("noop")
    expect(retry.report.changedRows).toBe(0)
    expect(retryExec.mock.calls.some(([sql]) => sql === "VACUUM")).toBe(false)
    expect(retry.report.committed).toBe(false)
    expect(retry.report.reclamation.vacuum).toEqual({ state: "skipped", reason: "no-deletions" })
    expect(retry.report.reclamation).not.toHaveProperty("reclaimedBytes")
  } finally {
    retryExec.mockRestore()
  }
})

test("SQLite exclusion blocks late readers and writers through post-commit vacuum", async () => {
  await using store = await producedStore([{ payloadBytes: 512 * 1024 }])
  const sessionID = store.metadata[0]!.created[0]!.id
  const original = Database.prototype.exec
  const probes: { read: boolean; write: boolean }[] = []
  const exec = spyOn(Database.prototype, "exec").mockImplementation(function (this: Database, statement: string) {
    if (statement === "VACUUM") {
      const child = Bun.spawnSync(
        [
          process.execPath,
          "-e",
          `
        const { Database } = require('bun:sqlite');
        const db = new Database(process.argv[1]);
        db.exec('PRAGMA busy_timeout=5');
        const result = {read:false, write:false};
        try { db.query('SELECT id FROM session').all(); result.read=true } catch {}
        try { db.exec('BEGIN IMMEDIATE; ROLLBACK'); result.write=true } catch {}
        db.close(); console.log(JSON.stringify(result));
      `,
          store.filename,
        ],
        { timeout: 5000 },
      )
      expect(child.exitCode).toBe(0)
      probes.push(JSON.parse(child.stdout.toString()))
    }
    return original.call(this, statement)
  })
  try {
    const result = await invokeHandler(store, { session: [sessionID], apply: true })
    if (result.report.action !== "applied") throw new Error(JSON.stringify(result.report, null, 2))
    expect(result.report.action).toBe("applied")
    expect(probes).toEqual([{ read: false, write: false }])
    expect(ownedCounts(store.filename, sessionID).session).toBe(0)
  } finally {
    exec.mockRestore()
  }
})

test("unreadable liveness refuses without row changes, then deletion works when evidence is readable", async () => {
  await using store = await producedStore([{ payloadBytes: 512 * 1024 }])
  const created = produce(store, { create: { payloadBytes: 512 * 1024 } })
  const sessionID = created.created[0]!.id
  const cutoffAt = Date.now() - 5
  backdateSession(store.filename, sessionID, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
  const before = ownedCounts(store.filename, sessionID)
  await chmod(store.directory, 0o755)
  try {
    const refused = await invokeHandler(store, { before: cutoff, session: [sessionID], apply: true })
    expect(refused.report.action).toBe("refused")
    expect(refused.report.changedRows).toBe(0)
    expect(refused.report.reclamation.reasons).toContain(
      "database-liveness-unreadable:database-directory-admits-other-users",
    )
    expect(ownedCounts(store.filename, sessionID)).toEqual(before)
  } finally {
    await chmod(store.directory, 0o700)
  }
  const control = await invokeHandler(store, { before: cutoff, session: [sessionID], apply: true })
  expect(control.report.action).toBe("applied")
  expect(ownedCounts(store.filename, sessionID).session).toBe(0)
})

test("unknown aggregate ownership refuses the whole apply while its clean copy can be pruned", async () => {
  await using source = await producedStore([{ payloadBytes: 512 * 1024 }])
  const created = produce(source, { create: { payloadBytes: 512 * 1024 } })
  const cutoffAt = Date.now() - 5
  backdateSession(source.filename, created.created[0]!.id, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
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

  const refused = await runCLI(unsafe, ["--before", cutoff, "--session", sessionID, "--apply"], 1)
  const report = parseReport(refused)
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

  const control = await runCLI(source, ["--before", cutoff, "--session", sessionID, "--apply"])
  expect(control.status).toBe(0)
  expect(parseReport(control).action).toBe("applied")
  expect(ownedCounts(source.filename, sessionID).session).toBe(0)
})

test("unclassified Session storage refuses without changing populated rows and a clean control deletes", async () => {
  await using source = await producedStore([{ payloadBytes: 512 * 1024 }])
  const created = produce(source, { create: { payloadBytes: 512 * 1024 } })
  const cutoffAt = Date.now() - 5
  backdateSession(source.filename, created.created[0]!.id, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
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

  const refused = await runCLI(unsafe, ["--before", cutoff, "--session", sessionID, "--apply"], 1)
  const report = parseReport(refused)
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

  const cleanControl = await runCLI(source, ["--before", cutoff, "--session", sessionID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect(parseReport(cleanControl).action).toBe("applied")
  expect(ownedCounts(source.filename, sessionID).session).toBe(0)
})

test("unclassified message-linked storage is refused through indirect foreign-key ownership", async () => {
  await using source = await producedStore([{ payloadBytes: 512 * 1024 }])
  const created = produce(source, { create: { payloadBytes: 512 * 1024 } })
  const cutoffAt = Date.now() - 5
  backdateSession(source.filename, created.created[0]!.id, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
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

  const refused = await runCLI(unsafe, ["--before", cutoff, "--session", sessionID, "--apply"], 1)
  const report = parseReport(refused)
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

  const cleanControl = await runCLI(source, ["--before", cutoff, "--session", sessionID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect(parseReport(cleanControl).action).toBe("applied")
  expect(ownedCounts(source.filename, sessionID).session).toBe(0)
})

test("an unknown durable event owner refuses with its populated event row intact and a clean control deletes", async () => {
  await using source = await producedStore([{ payloadBytes: 512 * 1024 }])
  const created = produce(source, { create: { payloadBytes: 512 * 1024 } })
  const cutoffAt = Date.now() - 5
  backdateSession(source.filename, created.created[0]!.id, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
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

  const refused = await runCLI(unsafe, ["--before", cutoff, "--session", sessionID, "--apply"], 1)
  const report = parseReport(refused)
  expect(ownedCounts(unsafe.filename, sessionID)).toEqual(before)
  const unknownAfter = new Database(unsafe.filename, { readonly: true })
  expect(count(unknownAfter, "SELECT count(*) AS rows FROM event WHERE type = ?", unknownType)).toBe(1)
  unknownAfter.close()
  expect(refused.status).toBe(1)
  expect(report.action).toBe("refused")
  expect(report.changedRows).toBe(0)
  expect(report.reclamation.reasons).toContain(`unknown-durable-event-owner:${unknownType}`)

  const cleanControl = await runCLI(source, ["--before", cutoff, "--session", sessionID, "--apply"])
  expect(cleanControl.status).toBe(0)
  expect(parseReport(cleanControl).action).toBe("applied")
  expect(ownedCounts(source.filename, sessionID).session).toBe(0)
})

test("a failed integrity check refuses with zero rows changed and the clean control reclaims", async () => {
  await using source = await producedStore([{ payloadBytes: 512 * 1024 }])
  const created = produce(source, { create: { payloadBytes: 512 * 1024 } })
  const cutoffAt = Date.now() - 5
  backdateSession(source.filename, created.created[0]!.id, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
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

  const refused = await runCLI(unsafe, ["--before", cutoff, "--session", sessionID, "--apply"], 1)
  const report = parseReport(refused)
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

  const control = await runCLI(source, ["--before", cutoff, "--session", sessionID, "--apply"])
  expect(control.status).toBe(0)
  expect(parseReport(control).action).toBe("applied")
  expect(ownedCounts(source.filename, sessionID).session).toBe(0)
})

test("full vacuum reclaims a migrated NONE store while its live session remains readable", async () => {
  await using source = await producedStore([{ payloadBytes: 512 * 1024 }])
  const created = produce(source, { create: { payloadBytes: 512 * 1024 } })
  const cutoffAt = Date.now() - 5
  backdateSession(source.filename, created.created[0]!.id, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
  await using unsupported = await copyStore(source)
  const sessionID = created.created[0]!.id
  const db = new Database(unsupported.filename)
  db.exec("PRAGMA auto_vacuum=NONE; VACUUM")
  expect(db.query("PRAGMA auto_vacuum").get()).toEqual({ auto_vacuum: 0 })
  db.close()
  const live = await produceLive(unsupported, { create: { payloadBytes: 128 } })
  const liveID = live.created[0]!.id
  const text = textParts(unsupported.filename, liveID)
  const liveBefore = ownedCounts(unsupported.filename, liveID)
  const before = await allocatedBytes(unsupported.filename)
  const applied = parseReport(await runCLI(unsupported, ["--before", cutoff, "--session", sessionID, "--apply"]))
  expect(applied.action).toBe("applied")
  expect(applied.reclamation.vacuum?.state).toBe("complete")
  expect(applied.reclamation.reclaimedBytes).toBeGreaterThan(0)
  expect(await allocatedBytes(unsupported.filename)).toBeLessThan(before)
  expect(Object.values(ownedCounts(unsupported.filename, sessionID)).every((n) => n === 0)).toBe(true)
  expect(ownedCounts(unsupported.filename, liveID)).toEqual(liveBefore)
  expect(textParts(unsupported.filename, liveID)).toEqual(text)
  const verify = new Database(unsupported.filename, { readonly: true })
  expect(verify.query("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" })
  expect(verify.query("PRAGMA foreign_key_check").all()).toEqual([])
  verify.close()
})

test("same-shape prune fixtures acquired together or later own their stores and metadata", async () => {
  const creates = [{ payloadBytes: 128 }]
  const acquired = await Promise.all([producedStore(creates), producedStore(creates), producedStore(creates)])
  await using edited = acquired[0]
  await using peer = acquired[1]
  await using other = acquired[2]
  const expected = structuredClone(peer.metadata)
  const expectedIDs = expected.flatMap((result) => result.created.map((item) => item.id)).toSorted()
  const paths = [edited.filename, peer.filename, other.filename]
  expect(new Set(paths).size).toBe(paths.length)
  expect(edited.project).not.toBe(peer.project)
  expect(peer.project).not.toBe(other.project)
  expect(edited.metadata).toEqual(expected)
  expect(other.metadata).toEqual(expected)
  expect(writerIdentity(edited.filename)).toEqual(writerIdentity(peer.filename))
  expect(writerIdentity(peer.filename)).toEqual(writerIdentity(other.filename))

  const producerPID = edited.producerPID
  if (producerPID === undefined) throw new Error("prune template did not retain its producer process identity")
  const writer = writerIdentity(edited.filename)[0]
  expect(writer?.pid).toBe(producerPID)
  expect(writer?.boot_id).toBeTruthy()
  expect(writer?.start_ticks).toBeTruthy()
  const current = EventWriter.readIdentity(producerPID)
  expect(
    current === undefined || current.bootID !== writer?.boot_id || current.startTicks !== writer?.start_ticks,
  ).toBe(true)

  edited.metadata[0]!.created[0]!.id = "ses_edited_metadata"
  edited.metadata[0]!.created.push({
    id: "ses_edited_metadata_extra",
    parentID: null,
    time: 0,
    messageID: "msg_edited_metadata",
    partID: "prt_edited_metadata",
  })
  const db = new Database(edited.filename)
  db.exec("CREATE TABLE prune_private_copy_mutation (value TEXT NOT NULL)")
  db.close()

  await using later = await producedStore(creates)
  for (const fixture of [peer, other, later]) {
    expect(fixture.metadata).toEqual(expected)
    expect(storedSessions(fixture.filename)).toEqual({ integrity: "ok", sessionIDs: expectedIDs })
  }
  expect(storedSessions(edited.filename)).toEqual({ integrity: "ok", sessionIDs: expectedIDs })
  const peers = new Database(peer.filename, { readonly: true })
  expect(
    peers.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'prune_private_copy_mutation'").get(),
  ).toBeNull()
  peers.close()
})

test("a producer timeout removes its template, exits the child, and leaves another shape usable", async () => {
  const scratch = path.join(scratchRoot, "producer-failure")
  await mkdir(scratch, { recursive: true, mode: 0o700 })
  const creates = [{ payloadBytes: 128 }]
  await using before = await producedStore(creates, { scratch })
  const existing = (await readdir(scratch)).toSorted()
  const failure = await producedStore([{ payloadBytes: 256 }], { scratch, timeout: 1 }).then(
    () => undefined,
    (error: unknown) => error,
  )

  expect(failure).toBeInstanceOf(ChildFailure)
  if (!(failure instanceof ChildFailure)) throw new Error("producer timeout did not retain child failure context")
  expect(failure.result.operation).toBe("prune producer")
  expect(failure.result.args).toContain(producer)
  expect(failure.result.status).toBeNull()
  expect(failure.result.signal).not.toBeNull()
  expect(failure.result.error?.message).toContain("timed out")
  expect(failure.result.elapsed).toBeGreaterThan(0)
  expect(await readdir(scratch).then((entries) => entries.toSorted())).toEqual(existing)
  if (failure.result.pid === undefined) throw new Error("producer timeout did not retain the child PID")
  expect(processExists(failure.result.pid)).toBe(false)

  await using after = await producedStore(creates, { scratch })
  expect(after.metadata).toEqual(before.metadata)
  for (const fixture of [before, after]) {
    expect(storedSessions(fixture.filename)).toEqual({
      integrity: "ok",
      sessionIDs: fixture.metadata.flatMap((result) => result.created.map((item) => item.id)).toSorted(),
    })
  }
})

test("a failed private copy removes only its directory and leaves the template usable", async () => {
  const scratch = path.join(scratchRoot, "copy-failure")
  await mkdir(scratch, { recursive: true, mode: 0o700 })
  const creates = [{ payloadBytes: 128 }]
  await using kept = await producedStore(creates, { scratch })
  const existing = (await readdir(scratch)).toSorted()
  const templateDirectory = path.join(scratch, existing.find((name) => name.startsWith("template-"))!)
  const template = path.join(templateDirectory, "opencode.db")
  await rename(template, `${template}.aside`)
  const failure = await producedStore(creates, { scratch }).then(
    () => undefined,
    (error: unknown) => error,
  )
  await rename(`${template}.aside`, template)

  expect(failure).toMatchObject({ code: "ENOENT" })
  expect((await readdir(scratch)).toSorted()).toEqual(existing)
  await using after = await producedStore(creates, { scratch })
  expect(after.metadata).toEqual(kept.metadata)
  for (const fixture of [kept, after]) {
    expect(storedSessions(fixture.filename)).toEqual({
      integrity: "ok",
      sessionIDs: fixture.metadata.flatMap((result) => result.created.map((item) => item.id)).toSorted(),
    })
  }
})

test("child failures and empty reports retain exit context and finish bounded cleanup", async () => {
  await using store = await createStore()
  const timeoutFailure = await produceLive(store, { create: { payloadBytes: 128 } }, 1).then(
    () => undefined,
    (error: unknown) => error,
  )
  expect(timeoutFailure).toBeInstanceOf(ChildFailure)
  if (!(timeoutFailure instanceof ChildFailure)) throw new Error("producer timeout was decoded as output")
  expect(timeoutFailure.result.status).toBeNull()
  expect(timeoutFailure.result.signal).not.toBeNull()
  expect(timeoutFailure.result.error?.message).toContain("timed out")
  expect(timeoutFailure.result.stdout).toBe("")
  expect(timeoutFailure.result.stderr).toBe("")
  expect(timeoutFailure.message).toContain("arguments:")
  expect(timeoutFailure.message).toContain("result.error: child timed out")
  if (timeoutFailure.result.pid === undefined) throw new Error("producer timeout did not retain the child PID")
  expect(processExists(timeoutFailure.result.pid)).toBe(false)

  const failedCLI = path.join(store.directory, "failed-cli.js")
  await writeFile(
    failedCLI,
    "process.stdout.write('partial CLI stdout\\n'); process.stderr.write('CLI failure\\n'); process.exit(17)",
  )
  const cliFailure = await runCLI(store, ["--before", "2026-03-02T00:00:00Z"], 0, { entrypoint: failedCLI }).then(
    () => undefined,
    (error: unknown) => error,
  )
  expect(cliFailure).toBeInstanceOf(ChildFailure)
  if (!(cliFailure instanceof ChildFailure)) throw new Error("CLI exit status was not checked before returning")
  expect(cliFailure.result.status).toBe(17)
  expect(cliFailure.result.signal).toBeNull()
  expect(cliFailure.result.stdout).toContain("partial CLI stdout")
  expect(cliFailure.result.stderr).toContain("CLI failure")
  expect(cliFailure.message).toContain("operation: opencode db prune")

  const prettyCLI = path.join(store.directory, "pretty-cli.js")
  await writeFile(prettyCLI, 'console.log(JSON.stringify({ action: "inventory" }, null, 2))')
  const completedCLI = await runCLI(store, ["--before", "2026-03-02T00:00:00Z"], 0, { entrypoint: prettyCLI })
  expect(parseReport(completedCLI).action).toBe("inventory")

  await using premature = startChild(
    "readiness probe",
    [
      process.execPath,
      "-e",
      "process.stdout.write('ordinary output\\n'); process.stderr.write('probe error\\n'); process.exit(7)",
    ],
    { cwd: packageRoot, env: process.env },
  )
  const readinessFailure = await readMatchingLine(premature, "PRUNE_READY").then(
    () => undefined,
    (error: unknown) => error,
  )
  expect(readinessFailure).toBeInstanceOf(ChildFailure)
  if (!(readinessFailure instanceof ChildFailure)) throw new Error("child EOF was accepted as readiness")
  expect(readinessFailure.result.status).toBe(7)
  expect(readinessFailure.result.signal).toBeNull()
  expect(readinessFailure.result.error).toBeUndefined()
  expect(readinessFailure.result.stdout).toContain("ordinary output")
  expect(readinessFailure.result.stderr).toContain("probe error")
  expect(readinessFailure.message).toContain("readiness failed")

  const script =
    "process.stdout.write('partial stdout\\n'); process.stderr.write('partial stderr\\n'); process.on('SIGTERM', () => {}); process.stdout.write('READY\\n'); setInterval(() => {}, 1000); setTimeout(() => process.exit(42), 1000)"
  await using uncooperative = startChild("controlled CLI child", [process.execPath, "-e", script], {
    cwd: packageRoot,
    env: process.env,
  })
  await readMatchingLine(uncooperative, "READY")
  const terminated = await uncooperative.finish()
  expect(terminated.status).toBeNull()
  expect(terminated.signal).toBe("SIGKILL")
  expect(terminated.error).toBeUndefined()
  expect(terminated.stdout).toContain("partial stdout")
  expect(terminated.stderr).toContain("partial stderr")
  const terminatedFailure = await Promise.resolve()
    .then(() => assertChildOutcome(terminated, 0))
    .then(
      () => undefined,
      (error: unknown) => error,
    )
  expect(terminatedFailure).toBeInstanceOf(ChildFailure)
  if (terminated.pid === undefined) throw new Error("terminated child did not retain the child PID")
  expect(processExists(terminated.pid)).toBe(false)

  const empty = await runChild("empty report child", process.execPath, ["-e", "process.exit(0)"], {
    cwd: packageRoot,
    env: process.env,
    timeout: 1_000,
  })
  assertChildOutcome(empty, 0)
  const outputFailure = await Promise.resolve()
    .then(() => parseRequiredJSON(empty, "fixture report"))
    .then(
      () => undefined,
      (error: unknown) => error,
    )
  expect(outputFailure).toBeInstanceOf(ChildOutputFailure)
  if (!(outputFailure instanceof ChildOutputFailure)) throw new Error("empty child output was accepted")
  expect(outputFailure.result.status).toBe(0)
  expect(outputFailure.result.signal).toBeNull()
  expect(outputFailure.result.error).toBeUndefined()
  expect(outputFailure.result.stdout).toBe("")
  expect(outputFailure.result.stderr).toBe("")
  expect(outputFailure.message).toContain("required JSON output was empty")
  expect(outputFailure.message).toContain("status: 0")
  expect(outputFailure.message).toContain("result.error: <absent>")

  const missing = await runChild("missing executable", path.join(store.directory, "missing-opencode"), [], {
    cwd: packageRoot,
    env: process.env,
    timeout: 1_000,
  })
  const spawnFailure = await Promise.resolve()
    .then(() => assertChildOutcome(missing, 0))
    .then(
      () => undefined,
      (error: unknown) => error,
    )
  expect(spawnFailure).toBeInstanceOf(ChildFailure)
  if (!(spawnFailure instanceof ChildFailure)) throw new Error("spawn failure was accepted")
  expect(spawnFailure.result.status).toBeNull()
  expect(spawnFailure.result.signal).toBeNull()
  expect(spawnFailure.result.error).toBeInstanceOf(Error)
  expect(spawnFailure.result.stdout).toBe("")
  expect(spawnFailure.result.stderr).toBe("")
  expect(spawnFailure.message).toContain("signal: <absent>")
  expect(spawnFailure.message).toContain("result.error:")
  expect(spawnFailure.message).toContain('stdout: ""')
  expect(spawnFailure.message).toContain('stderr: ""')
})

function count(db: Database, query: string, sessionID: string) {
  return db.query<{ rows: number }, [string]>(query).get(sessionID)?.rows ?? 0
}

function textParts(filename: string, sessionID: string) {
  const db = new Database(filename, { readonly: true })
  try {
    return db
      .query<{ data: string }, [string]>(
        "SELECT data FROM part WHERE session_id = ? AND json_extract(data, '$.type') = 'text' ORDER BY id",
      )
      .all(sessionID)
      .map((row) => (JSON.parse(row.data) as { text: string }).text)
  } finally {
    db.close()
  }
}

function storedSessionIDs(filename: string) {
  const db = new Database(filename, { readonly: true })
  try {
    return db
      .query<{ id: string }, []>("SELECT id FROM session ORDER BY id")
      .all()
      .map((row) => row.id)
  } finally {
    db.close()
  }
}

function storedSessions(filename: string) {
  const db = new Database(filename, { readonly: true })
  try {
    return {
      integrity: db.query<{ integrity_check: string }, []>("PRAGMA integrity_check").get()?.integrity_check,
      sessionIDs: db
        .query<{ id: string }, []>("SELECT id FROM session ORDER BY id")
        .all()
        .map((row) => row.id),
    }
  } finally {
    db.close()
  }
}

function writerIdentity(filename: string) {
  const db = new Database(filename, { readonly: true })
  try {
    return db
      .query<
        { writer_id: string; pid: number | null; boot_id: string | null; start_ticks: string | null },
        []
      >("SELECT writer_id, pid, boot_id, start_ticks FROM event_writer_process ORDER BY writer_id")
      .all()
  } finally {
    db.close()
  }
}

function processExists(pid: number) {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ESRCH") return false
    throw error
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
    db.query("UPDATE event_aggregate_writer SET time_last_append = ? WHERE aggregate_id = ?").run(timestamp, sessionID)
  } finally {
    db.close()
  }
}

async function createStore(): Promise<Store> {
  await mkdir(scratchRoot, { recursive: true, mode: 0o700 })
  const directory = await mkdtemp(path.join(scratchRoot, "store-"))
  const project = path.join(directory, "project")
  try {
    await mkdir(project)
    return {
      filename: path.join(directory, "opencode.db"),
      directory,
      project,
      producerPID: undefined,
      metadata: [],
      seeds: [],
      [Symbol.asyncDispose]: () => rm(directory, { recursive: true, force: true }),
    }
  } catch (error) {
    await rm(directory, { recursive: true, force: true })
    throw error
  }
}

async function producedStore(creates: Create[], options: { scratch?: string; timeout?: number } = {}) {
  const scratch = options.scratch ?? scratchRoot
  const timeout = options.timeout ?? 30_000
  await mkdir(scratch, { recursive: true, mode: 0o700 })
  const key = JSON.stringify([creates, scratch, timeout])
  const template = producedTemplates.get(key) ?? produceTemplate(creates, scratch, timeout)
  producedTemplates.set(key, template)
  const produced = await template
  const directory = await mkdtemp(path.join(scratch, "store-"))
  const project = path.join(directory, "project")
  const filename = path.join(directory, "opencode.db")
  try {
    await mkdir(project)
    await copyFile(produced.filename, filename)
  } catch (error) {
    await rm(directory, { recursive: true, force: true })
    throw error
  }
  const metadata = structuredClone(produced.metadata)
  return {
    filename,
    directory,
    project,
    producerPID: produced.producerPID,
    metadata,
    seeds: creates.map((operation, index) => ({ operation: structuredClone(operation), result: metadata[index]! })),
    [Symbol.asyncDispose]: () => rm(directory, { recursive: true, force: true }),
  }
}

async function produceTemplate(creates: Create[], scratch: string, timeout: number): Promise<Template> {
  const directory = await mkdtemp(path.join(scratch, "template-"))
  const filename = path.join(directory, "opencode.db")
  const project = path.join(directory, "project")
  try {
    await mkdir(project)
    const args = [producer, filename, project, JSON.stringify({ create: creates })]
    const result = await runChild("prune producer", process.execPath, args, {
      cwd: packageRoot,
      env: { ...process.env, OPENCODE_DB: filename, OPENCODE_DISABLE_CHANNEL_DB: "1" },
      timeout,
    })
    assertChildOutcome(result, 0)
    const produced = parseRequiredJSON<ProducerResult>(
      result,
      "prune producer",
      result.stdout.trim().split("\n").at(-1) ?? "",
    )
    if (produced.created.length !== creates.length) {
      throw new ChildOutputFailure(
        result,
        "prune producer",
        `expected ${creates.length} created Sessions, found ${produced.created.length}`,
      )
    }
    if (await Bun.file(`${filename}-wal`).exists()) {
      throw new ChildOutputFailure(result, "prune producer", "the exited producer left an uncheckpointed WAL")
    }
    await rm(project, { recursive: true })
    return {
      directory,
      filename,
      producerPID: result.pid,
      metadata: produced.created.map((created) => ({ created: [created] })),
    }
  } catch (error) {
    await rm(directory, { recursive: true, force: true })
    throw error
  }
}

async function verifyPendingWork(create: Create, reason: string) {
  await using store = await producedStore([create, { payloadBytes: 512 * 1024 }])
  const protectedSession = await produce(store, { create })
  const control = await produce(store, { create: { payloadBytes: 512 * 1024 } })
  const protectedID = protectedSession.created[0]!.id
  const controlID = control.created[0]!.id
  const cutoffAt = Date.now() - 5
  backdateSession(store.filename, protectedID, cutoffAt - 1_000)
  backdateSession(store.filename, controlID, cutoffAt - 1_000)
  const cutoff = new Date(cutoffAt).toISOString()
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
  try {
    const db = new Database(source.filename)
    const checkpoint = db.query<{ busy: number }, []>("PRAGMA wal_checkpoint(TRUNCATE)").get()
    db.close()
    if (checkpoint?.busy !== 0) throw new Error(`prune source checkpoint did not complete: ${source.filename}`)
    if (await Bun.file(`${source.filename}-wal`).exists())
      throw new Error(`prune source left a WAL: ${source.filename}-wal`)
    await copyFile(source.filename, store.filename)
    return { ...store, producerPID: source.producerPID, metadata: structuredClone(source.metadata), seeds: [] }
  } catch (error) {
    await store[Symbol.asyncDispose]()
    throw error
  }
}

function produce(store: Store, operation: { create: Create }) {
  const seed = store.seeds.shift()
  if (!seed || JSON.stringify(seed.operation) !== JSON.stringify(operation.create)) {
    throw new Error(
      `prune fixture acquisition did not include the requested initial Session: ${JSON.stringify(operation)}`,
    )
  }
  return structuredClone(seed.result)
}

async function produceLive(store: Store, operation: Record<string, unknown>, timeout = 30_000) {
  const result = await runChild(
    "prune producer",
    process.execPath,
    [producer, store.filename, store.project, JSON.stringify(operation)],
    {
      cwd: packageRoot,
      env: { ...process.env, OPENCODE_DB: store.filename, OPENCODE_DISABLE_CHANNEL_DB: "1" },
      timeout,
    },
  )
  assertChildOutcome(result, 0)
  return parseRequiredJSON<ProducerResult>(result, "prune producer", result.stdout.trim().split("\n").at(-1) ?? "")
}

async function runCLI(
  store: Store,
  args: string[],
  expectedStatus: number | "nonzero" = 0,
  options: { entrypoint?: string; timeout?: number; command?: string; databaseDefault?: string } = {},
) {
  const result = await runChild(
    "opencode db prune",
    process.execPath,
    [options.entrypoint ?? entrypoint, "db", options.command ?? "prune", ...args],
    {
      cwd: packageRoot,
      env: {
        ...process.env,
        OPENCODE_DB: options.databaseDefault ?? store.filename,
        OPENCODE_DISABLE_CHANNEL_DB: "1",
        OPENCODE_PURE: "1",
      },
      timeout: options.timeout ?? 30_000,
    },
  )
  assertChildOutcome(result, expectedStatus)
  return result
}

function parseReport(result: ChildResult) {
  return parseRequiredJSON<Report>(result, "prune CLI")
}

async function runChild(
  operation: string,
  command: string,
  args: string[],
  options: { cwd: string; env: NodeJS.ProcessEnv; timeout: number },
): Promise<ChildResult> {
  const invocation = [command, ...args]
  const started = performance.now()
  const spawned = trySpawn(() => spawnCaptured(invocation, options))
  if ("error" in spawned) {
    return {
      operation,
      args: invocation,
      elapsed: performance.now() - started,
      status: null,
      signal: null,
      error: spawned.error,
      stdout: "",
      stderr: "",
      pid: undefined,
    }
  }
  await using owned = spawned.child
  let timedOut = false
  const timeout = setTimeout(() => {
    timedOut = true
    owned.kill("SIGTERM")
  }, options.timeout)
  const forceKill = setTimeout(() => {
    if (timedOut) owned.kill("SIGKILL")
  }, options.timeout + 250)
  try {
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(owned.stdout).text(),
      new Response(owned.stderr).text(),
      owned.exited,
    ])
    const signal = owned.signalCode ?? null
    return {
      operation,
      args: invocation,
      elapsed: performance.now() - started,
      status: signal === null ? exitCode : null,
      signal,
      error: timedOut ? new Error(`child timed out after ${options.timeout}ms`) : undefined,
      stdout,
      stderr,
      pid: owned.pid,
    }
  } finally {
    clearTimeout(timeout)
    clearTimeout(forceKill)
  }
}

function assertChildOutcome(result: ChildResult, expectedStatus: number | "nonzero") {
  const statusMatches =
    expectedStatus === "nonzero" ? result.status !== null && result.status !== 0 : result.status === expectedStatus
  if (statusMatches && result.signal === null && result.error === undefined) return
  throw new ChildFailure(result, expectedStatus)
}

function parseRequiredJSON<T>(result: ChildResult, label: string, output = result.stdout.trim()): T {
  if (!output) throw new ChildOutputFailure(result, label, "required JSON output was empty")
  try {
    return JSON.parse(output) as T
  } catch (error) {
    throw new ChildOutputFailure(result, label, `required JSON output was malformed: ${asError(error).message}`)
  }
}

function describeChild(result: ChildResult) {
  return [
    `operation: ${result.operation}`,
    `arguments: ${JSON.stringify(result.args)}`,
    `elapsed: ${result.elapsed.toFixed(1)}ms`,
    `status: ${result.status ?? "<absent>"}`,
    `signal: ${result.signal ?? "<absent>"}`,
    `result.error: ${result.error?.message ?? "<absent>"}`,
    `stdout: ${JSON.stringify(result.stdout)}`,
    `stderr: ${JSON.stringify(result.stderr)}`,
  ].join("\n")
}

class ChildFailure extends Error {
  constructor(
    readonly result: ChildResult,
    expectedStatus: number | "nonzero" | undefined,
    reason?: string,
  ) {
    super([reason ?? `child did not complete with expected status ${expectedStatus}`, describeChild(result)].join("\n"))
    this.name = "ChildFailure"
  }
}

class ChildOutputFailure extends Error {
  constructor(
    readonly result: ChildResult,
    label: string,
    reason: string,
  ) {
    super(`${label} failed: ${reason}\n${describeChild(result)}`)
    this.name = "ChildOutputFailure"
  }
}

function asError(error: unknown) {
  return error instanceof Error ? error : new Error(String(error))
}

function spawnCaptured(args: string[], options: { cwd: string; env: NodeJS.ProcessEnv }) {
  return Bun.spawn(args, {
    cwd: options.cwd,
    env: options.env,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  })
}

function spawnInteractive(args: string[], options: { cwd: string; env: NodeJS.ProcessEnv }) {
  return Bun.spawn(args, {
    cwd: options.cwd,
    env: options.env,
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  })
}

function trySpawn<Child>(spawn: () => Child) {
  try {
    return { child: spawn() } as const
  } catch (error) {
    return { error: asError(error) } as const
  }
}

function startProducer(store: Store, operation: Record<string, unknown>) {
  return startChild(
    "prune producer",
    [process.execPath, producer, store.filename, store.project, JSON.stringify(operation)],
    {
      cwd: packageRoot,
      env: { ...process.env, OPENCODE_DB: store.filename, OPENCODE_DISABLE_CHANNEL_DB: "1" },
    },
  )
}

function startChild(operation: string, args: string[], options: { cwd: string; env: NodeJS.ProcessEnv }): RunningChild {
  const started = performance.now()
  const spawned = trySpawn(() => spawnInteractive(args, options))
  if ("error" in spawned) {
    throw new ChildFailure(
      {
        operation,
        args,
        elapsed: performance.now() - started,
        status: null,
        signal: null,
        error: spawned.error,
        stdout: "",
        stderr: "",
        pid: undefined,
      },
      undefined,
      "could not start child",
    )
  }
  const child = spawned.child
  const [readiness, captured] = child.stdout.tee()
  const stdout = new Response(captured).text()
  const stderr = new Response(child.stderr).text()
  let completed: Promise<ChildResult> | undefined
  const finish = () => {
    completed ??= finishChild(child, stdout, stderr, operation, args, started)
    return completed
  }
  return {
    child,
    readiness,
    stdout,
    stderr,
    operation,
    args,
    started,
    finish,
    [Symbol.asyncDispose]: async () => {
      await finish()
    },
  }
}

async function finishChild(
  child: ReturnType<typeof spawnInteractive>,
  stdout: Promise<string>,
  stderr: Promise<string>,
  operation: string,
  args: string[],
  started: number,
): Promise<ChildResult> {
  if (!(await waitForExit(child, 250))) {
    child.kill("SIGTERM")
    if (!(await waitForExit(child, 250))) child.kill("SIGKILL")
  }
  const [exitCode, capturedOut, capturedErr] = await Promise.all([child.exited, stdout, stderr])
  const signal = child.signalCode ?? null
  return {
    operation,
    args,
    elapsed: performance.now() - started,
    status: signal === null ? exitCode : null,
    signal,
    error: undefined,
    stdout: capturedOut,
    stderr: capturedErr,
    pid: child.pid,
  }
}

function waitForExit(child: ReturnType<typeof spawnInteractive>, timeout: number) {
  return Promise.race([child.exited.then(() => true), Bun.sleep(timeout).then(() => false)])
}

async function readMatchingLine(producer: RunningChild, prefix: string) {
  const reader = producer.readiness.getReader()
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
          if (result.done) throw new Error("child closed before its readiness line")
        }
      })(),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new Error("child did not publish its readiness line")), 20_000)
      }),
    ])
  } catch (error) {
    const result = await producer.finish()
    throw new ChildFailure(result, undefined, `${prefix.trim()} readiness failed: ${asError(error).message}`)
  } finally {
    if (timeout) clearTimeout(timeout)
    reader.releaseLock()
  }
}

async function invokeHandler(
  store: Store,
  args: {
    before?: string
    directory?: string
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

async function allocatedBytes(filename: string) {
  const files = [filename, `${filename}-wal`, `${filename}-shm`, `${filename}-journal`]
  return (
    await Promise.all(
      files.map(async (file) => {
        try {
          return (await stat(file)).blocks * 512
        } catch (error) {
          if (error instanceof Error && "code" in error && error.code === "ENOENT") return 0
          throw error
        }
      }),
    )
  ).reduce((sum, bytes) => sum + bytes, 0)
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
