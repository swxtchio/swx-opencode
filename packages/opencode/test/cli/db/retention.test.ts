import { expect, spyOn, test } from "bun:test"
import { spawnSync } from "node:child_process"
import { Database } from "bun:sqlite"
import { Context, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { Flag } from "@opencode-ai/core/flag/flag"
import { mkdtemp, readFile, rm, stat, statfs, symlink, writeFile } from "node:fs/promises"
import path from "node:path"
import { eq } from "drizzle-orm"
import { Service, layerFromPath } from "@opencode-ai/core/database/database"
import { EventRetentionTable } from "@opencode-ai/core/event/sql"
import { SessionContextEpoch } from "@opencode-ai/core/session/context-epoch"
import { SessionV2 } from "@opencode-ai/core/session"
import { SystemContext } from "@opencode-ai/core/system-context/index"
import {
  apply,
  compactRestoreFixture,
  createFixtureDatabase,
  inventory,
  RetentionCommand,
  type RetentionEvidence,
  type SqliteAccess,
} from "@/cli/cmd/db-retention"
import { readExport } from "@/cli/cmd/db-export-usage"
import { tmpdir } from "../../fixture/fixture"

const session = (db: Database, id: string, timeCreated: number, parentID: string | null = null) => {
  db.query(
    "INSERT INTO session (id, parent_id, project_id, workspace_id, slug, directory, path, title, version, share_url, summary_diffs, metadata, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write, revert, permission, agent, model, time_created, time_updated, time_archived) VALUES (?, ?, 'proj', NULL, 'slug', '/work', NULL, 'session title sentinel', 'v1', NULL, NULL, NULL, 12.5, 13, 17, 2, 3, 4, NULL, NULL, 'build', ?, ?, ?, NULL)",
  ).run(id, parentID, JSON.stringify({ id: "requested-model", providerID: "provider-a" }), timeCreated, timeCreated)
}

const event = (db: Database, sessionID: string, seq = 0) => {
  db.query("INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?, ?, NULL)").run(sessionID, seq)
  db.query("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)").run(
    `evt_${sessionID}`,
    sessionID,
    seq,
    "session.next.context.updated.1",
    JSON.stringify({
      sessionID,
      timestamp: 10,
      messageID: "msg_a",
      text: "event sentinel",
      info: {
        providerID: "provider-a",
        modelID: "requested-model",
        responseModelIDs: ["served-a", "served-b"],
        cost: 12.5,
        tokens: { input: 13, output: 17, reasoning: 2, cache: { read: 3, write: 4 } },
        text: "nested sentinel",
      },
    }),
  )
}

function fixture() {
  const db = new Database(":memory:")
  createSchema(db)
  return db
}

function createSchema(db: Database) {
  db.exec(`
    CREATE TABLE session (
      id TEXT PRIMARY KEY, parent_id TEXT, project_id TEXT NOT NULL, workspace_id TEXT, slug TEXT NOT NULL,
      directory TEXT NOT NULL, path TEXT, title TEXT NOT NULL, version TEXT NOT NULL, share_url TEXT,
      summary_additions INTEGER, summary_deletions INTEGER, summary_files INTEGER, summary_diffs TEXT,
      metadata TEXT, cost REAL NOT NULL, tokens_input INTEGER NOT NULL, tokens_output INTEGER NOT NULL,
      tokens_reasoning INTEGER NOT NULL, tokens_cache_read INTEGER NOT NULL, tokens_cache_write INTEGER NOT NULL,
      revert TEXT, permission TEXT, agent TEXT, model TEXT, time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL, time_archived INTEGER
    );
    CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, owner_id TEXT);
    CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT NOT NULL, seq INTEGER NOT NULL, type TEXT NOT NULL, data TEXT NOT NULL);
    CREATE TABLE event_retention (aggregate_id TEXT PRIMARY KEY, state TEXT NOT NULL, progress_table TEXT, progress_id TEXT, evidence TEXT NOT NULL, time_started INTEGER NOT NULL, time_updated INTEGER NOT NULL);
    CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, data TEXT NOT NULL);
    CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT NOT NULL, session_id TEXT NOT NULL, data TEXT NOT NULL);
    CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, seq INTEGER NOT NULL, data TEXT NOT NULL);
    CREATE TABLE session_input (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, promoted_seq INTEGER, prompt TEXT NOT NULL);
    CREATE TABLE session_context_epoch (session_id TEXT PRIMARY KEY, baseline TEXT NOT NULL, snapshot TEXT NOT NULL);
    CREATE TABLE session_prompt_queue (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, input TEXT NOT NULL);
    CREATE TABLE session_prompt_queue_sequence (session_id TEXT PRIMARY KEY, seq INTEGER NOT NULL);
    CREATE TABLE todo (session_id TEXT NOT NULL, position INTEGER NOT NULL, content TEXT NOT NULL);
    CREATE TABLE session_share (session_id TEXT PRIMARY KEY, id TEXT NOT NULL, secret TEXT NOT NULL, url TEXT NOT NULL);
  `)
}

const access = (db: Database) => db as unknown as SqliteAccess

async function retentionCompactionFixture(sessionID: string) {
  const fixture = await createFixtureDatabase()
  createSchema(fixture.db)
  session(fixture.db, sessionID, 10)
  event(fixture.db, sessionID)
  return fixture
}

async function fileSnapshot(filename: string) {
  const [bytes, file] = await Promise.all([readFile(filename), stat(filename)])
  return { bytes, device: file.dev, inode: file.ino, size: file.size }
}

const evidence = (sessionIDs: string[], finalSequence: Record<string, number>, now = 1_000): RetentionEvidence => ({
  customerBinding: {
    proofID: "fixture-unbound-session-classification",
    durable: true,
    sessionIDs,
    customerBoundSessionIDs: [],
    nonCustomerSessionIDs: sessionIDs,
  },
  policy: {
    reviewed: true,
    cutoffEpochMs: 100,
    reviewedReference: "swxtchio/swx-opencode#97",
    policyDigest: "fixture-policy-digest",
    readerContractReviewed: true,
    readerContractID: "fixture-reader-contract",
  },
  liveness: {
    proofID: "fixture-live-snapshot",
    observedAtEpochMs: now - 1,
    sessionIDs,
    aggregateOwners: Object.fromEntries(sessionIDs.map((id) => [id, null])),
    servingProcesses: [],
    canResume: false,
    unfinishedOwnedWork: false,
    validThroughEpochMs: now + 10_000,
  },
  handoff: {
    receiptID: "fixture-firstmate-receipt",
    durable: true,
    sessionIDs,
    finalSequence,
    axes: {
      billing: { status: "retained" },
      provider: { status: "retained" },
      servingModel: { status: "retained" },
      routeAttribution: { status: "retained" },
      reportedCost: { status: "retained" },
      inputTokens: { status: "retained" },
      outputTokens: { status: "retained" },
      reasoningTokens: { status: "retained" },
      cacheReadTokens: { status: "retained" },
      cacheWriteTokens: { status: "retained" },
      correctness: { status: "unavailable", cause: "fixture has no correctness producer" },
      performance: { status: "unavailable", cause: "fixture has no performance producer" },
    },
    report: {
      windowStart: "2026-10-01T00:00:00Z",
      windowEnd: "2026-10-02T00:00:00Z",
      resultDigest: "fixture-report-digest",
      denominators: { sessions: sessionIDs.length, messages: sessionIDs.length },
      unavailableCauses: {
        correctness: "fixture has no correctness producer",
        performance: "fixture has no performance producer",
      },
      rawHistoryInaccessible: true,
    },
  },
})

function addCopies(db: Database, sessionID: string) {
  db.query("INSERT INTO message (id, session_id, data) VALUES (?, ?, ?)").run(
    `msg_${sessionID}`,
    sessionID,
    JSON.stringify({
      role: "assistant",
      providerID: "provider-a",
      modelID: "requested-model",
      responseModelIDs: ["served-a", "served-b"],
      cost: 12.5,
      tokens: { input: 13, output: 17, reasoning: 2, cache: { read: 3, write: 4 } },
      text: "message sentinel",
    }),
  )
  db.query("INSERT INTO part (id, message_id, session_id, data) VALUES (?, ?, ?, ?)").run(
    `part_${sessionID}`,
    `msg_${sessionID}`,
    sessionID,
    JSON.stringify({
      type: "step-finish",
      providerID: "provider-a",
      modelID: "requested-model",
      responseModelID: "served-a",
      cost: 12.5,
      tokens: { input: 13, output: 17, reasoning: 2, cache: { read: 3, write: 4 } },
      text: "part sentinel",
    }),
  )
  db.query("INSERT INTO session_message (id, session_id, seq, data) VALUES (?, ?, 1, ?)").run(
    `v2_${sessionID}`,
    sessionID,
    JSON.stringify({
      role: "assistant",
      providerID: "provider-a",
      modelID: "requested-model",
      responseModelIDs: ["served-a"],
      cost: 12.5,
      tokens: { input: 13, output: 17, reasoning: 2, cache: { read: 3, write: 4 } },
      text: "v2 message sentinel",
    }),
  )
  db.query("INSERT INTO session_context_epoch (session_id, baseline, snapshot) VALUES (?, ?, ?)").run(
    sessionID,
    "baseline",
    JSON.stringify({ text: "context sentinel" }),
  )
  db.query("INSERT INTO todo (session_id, position, content) VALUES (?, 1, ?)").run(sessionID, "todo sentinel")
  db.query(
    "INSERT INTO session_share (session_id, id, secret, url) VALUES (?, 'share', 'secret sentinel', 'share sentinel')",
  ).run(sessionID)
}

test("best-effort inventory does not open a read transaction", () => {
  const db = fixture()
  try {
    session(db, "ses_unproven", 10)
    event(db, "ses_unproven")
    let transactions = 0
    const observed = new Proxy(access(db), {
      get(target, property) {
        if (property === "transaction")
          return () => {
            transactions += 1
            throw new Error("best-effort inventory opened a transaction")
          }
        const value = Reflect.get(target, property, target)
        return typeof value === "function" ? value.bind(target) : value
      },
    })

    const result = inventory(observed, {}, 1_000, { transactional: false })

    expect(transactions).toBe(0)
    expect(result.trees[0]?.eligible).toBe(false)
    expect(result.trees[0]?.reasons).toContain("customer-session-classification-unavailable")
  } finally {
    db.close()
  }
})

test("retention CLI keeps inventory read-only and refuses apply before opening the database", async () => {
  await using fixtureDB = await createFixtureDatabase()
  createSchema(fixtureDB.db)
  session(fixtureDB.db, "ses_cli", 10)
  event(fixtureDB.db, "ses_cli")
  fixtureDB.db.query("UPDATE event SET data = 'not-json' WHERE aggregate_id = 'ses_cli'").run()
  fixtureDB.db.close()
  const previousDB = Flag.OPENCODE_DB
  const previousExitCode = process.exitCode
  Flag.OPENCODE_DB = fixtureDB.filename
  const output = spyOn(console, "log").mockImplementation(() => {})
  const transaction = spyOn(Database.prototype, "transaction")
  try {
    process.exitCode = 0
    await RetentionCommand.handler({ _: [], $0: "test", apply: false })
    const dryRun = JSON.parse(String(output.mock.calls[0]?.[0])) as ReturnType<typeof inventory>
    expect(dryRun.trees[0]?.eligible).toBe(false)
    expect(dryRun.trees[0]?.reasons).toContain("reviewed-age-boundary-unavailable")
    expect(dryRun.trees[0]?.reasons).toContain("cross-process-liveness-proof-unavailable")
    expect(dryRun.trees[0]?.reasons).toContain("event-payload-ownership-proof-unavailable:ses_cli")
    expect(transaction).not.toHaveBeenCalled()

    const before = new Database(fixtureDB.filename, { readonly: true })
    const beforeEvent = before
      .query<{ data: string }, [string]>("SELECT data FROM event WHERE aggregate_id = ?")
      .get("ses_cli")?.data
    before.close()
    await RetentionCommand.handler({ _: [], $0: "test", apply: true })
    const applyResult = JSON.parse(String(output.mock.calls[1]?.[0])) as {
      action: string
      changedRows: number
      reasons: string[]
    }
    expect(applyResult.action).toBe("refused")
    expect(applyResult.changedRows).toBe(0)
    expect(applyResult.reasons).toContain("production-apply-disabled")
    expect(transaction).not.toHaveBeenCalled()
    const after = new Database(fixtureDB.filename, { readonly: true })
    expect(
      after.query<{ data: string }, [string]>("SELECT data FROM event WHERE aggregate_id = ?").get("ses_cli")?.data,
    ).toBe(beforeEvent)
    expect(after.query("SELECT state FROM event_retention").get()).toBeNull()
    after.close()
    expect(process.exitCode).toBe(1)
  } finally {
    transaction.mockRestore()
    output.mockRestore()
    Flag.OPENCODE_DB = previousDB
    process.exitCode = previousExitCode ?? 0
  }
})

test("dry run reports exact trees and refusals without changing SQLite", () => {
  const db = fixture()
  try {
    session(db, "ses_parent", 10)
    session(db, "ses_child", 20, "ses_parent")
    event(db, "ses_parent")
    db.query("INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('unknown-aggregate', 0, NULL)").run()
    const before = db.serialize()

    const proof = evidence(["ses_child", "ses_parent"], { ses_child: -1, ses_parent: 0 })
    const dryRun = inventory(access(db), proof, 1_000)

    expect(dryRun.unknownAggregates).toEqual(["unknown-aggregate"])
    expect(dryRun.trees[0]).toMatchObject({
      rootSessionID: "ses_parent",
      sessionIDs: ["ses_child", "ses_parent"],
      eligible: true,
    })
    expect(dryRun.trees[0]?.aggregates).toContainEqual({
      aggregateID: "ses_parent",
      seq: 0,
      ownerID: null,
      rows: 1,
      eventTypes: ["session.next.context.updated.1"],
      bytes: Buffer.byteLength(
        db.query<{ data: string }, [string]>("SELECT data FROM event WHERE aggregate_id = ?").get("ses_parent")!.data,
      ),
    })
    expect(db.serialize().equals(before)).toBe(true)

    const failedProof = evidence(["ses_child", "ses_parent"], { ses_child: -1, ses_parent: 0 })
    const refused = inventory(
      access(db),
      {
        ...failedProof,
        liveness: { ...failedProof.liveness!, servingProcesses: ["opencode:pid-1"] },
      },
      1_000,
    )
    expect(refused.trees[0]?.reasons).toContain("serving-process-owns-session")
    expect(db.serialize().equals(before)).toBe(true)
  } finally {
    db.close()
  }
})

test("dry run names stale, unfinished, pending, owner-conflict, cutoff, and unreadable proofs", () => {
  const db = fixture()
  try {
    session(db, "ses_old", 10)
    session(db, "ses_new", 200)
    event(db, "ses_old")
    db.query("INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('ses_orphan', 0, 'owner-a')").run()
    const valid = evidence(["ses_old"], { ses_old: 0 })
    const stale = inventory(access(db), { ...valid, liveness: { ...valid.liveness!, validThroughEpochMs: 999 } }, 1_000)
    expect(stale.trees.find((item) => item.rootSessionID === "ses_old")?.reasons).toContain(
      "cross-process-liveness-proof-stale",
    )
    expect(stale.unknownAggregates).toContain("ses_orphan")

    const unfinished = inventory(
      access(db),
      { ...valid, liveness: { ...valid.liveness!, unfinishedOwnedWork: true } },
      1_000,
    )
    expect(unfinished.trees.find((item) => item.rootSessionID === "ses_old")?.reasons).toContain(
      "unfinished-owned-work",
    )

    db.query(
      "INSERT INTO session_input (id, session_id, promoted_seq, prompt) VALUES ('pending', 'ses_old', NULL, '{}')",
    ).run()
    const queued = inventory(access(db), valid, 1_000)
    expect(queued.trees.find((item) => item.rootSessionID === "ses_old")?.reasons).toContain("v2-input-pending")
    db.query("DELETE FROM session_input").run()

    db.query("UPDATE event_sequence SET owner_id = 'owner-a' WHERE aggregate_id = 'ses_old'").run()
    const ownerConflict = inventory(access(db), valid, 1_000)
    expect(ownerConflict.trees.find((item) => item.rootSessionID === "ses_old")?.reasons).toContain(
      "cross-process-aggregate-owner-snapshot-mismatch:ses_old",
    )
    db.query("UPDATE event_sequence SET owner_id = NULL WHERE aggregate_id = 'ses_old'").run()

    const staleReceipt = inventory(
      access(db),
      { ...valid, handoff: { ...valid.handoff!, finalSequence: { ses_old: -1 } } },
      1_000,
    )
    expect(staleReceipt.trees.find((item) => item.rootSessionID === "ses_old")?.reasons).toContain(
      "measurement-handoff-does-not-cover-final-write:ses_old",
    )

    const newer = inventory(access(db), { ...valid, policy: { ...valid.policy!, cutoffEpochMs: 50 } }, 1_000)
    expect(newer.trees.find((item) => item.rootSessionID === "ses_new")?.reasons).toContain(
      "session-after-reviewed-cutoff:ses_new",
    )

    const unreadable = inventory(access(db), { ...valid, evidenceError: "receipt store unavailable" }, 1_000)
    expect(unreadable.trees.find((item) => item.rootSessionID === "ses_old")?.reasons).toContain(
      "evidence-unreadable:receipt store unavailable",
    )
  } finally {
    db.close()
  }
})

test("a missing measurement receipt refuses an otherwise proven tree without changing rows", () => {
  const db = fixture()
  try {
    session(db, "ses_old", 10)
    event(db, "ses_old")
    const valid = evidence(["ses_old"], { ses_old: 0 })
    const tree = inventory(access(db), valid, 1_000).trees[0]!
    const noReceipt = { policy: valid.policy, liveness: valid.liveness }
    const before = db.serialize()

    const dryRun = inventory(access(db), noReceipt, 1_000)
    expect(dryRun.trees[0]?.reasons).toContain("measurement-handoff-receipt-unavailable")
    const result = apply(access(db), { tree, evidence: () => noReceipt, now: () => 1_000 })

    expect(result.state).toBe("refused")
    expect(result.changedRows).toBe(0)
    expect(db.serialize().equals(before)).toBe(true)
  } finally {
    db.close()
  }
})

test("customer-bound and unclassified Session trees remain ineligible", () => {
  const db = fixture()
  try {
    session(db, "ses_customer", 10)
    event(db, "ses_customer")
    const valid = evidence(["ses_customer"], { ses_customer: 0 })
    const before = db.serialize()
    const customerBound = {
      ...valid,
      customerBinding: {
        ...valid.customerBinding!,
        customerBoundSessionIDs: ["ses_customer"],
        nonCustomerSessionIDs: [],
      },
    }
    const boundTree = inventory(access(db), customerBound, 1_000).trees[0]!
    expect(boundTree.reasons).toContain("customer-bound-session-retention-workflow-unapproved:ses_customer")
    const refused = apply(access(db), { tree: boundTree, evidence: () => customerBound, now: () => 1_000 })
    expect(refused.state).toBe("refused")
    expect(refused.changedRows).toBe(0)
    expect(db.serialize().equals(before)).toBe(true)

    const unknown = inventory(access(db), { ...valid, customerBinding: undefined }, 1_000)
    expect(unknown.trees[0]?.reasons).toContain("customer-session-classification-unavailable")
    expect(db.serialize().equals(before)).toBe(true)
  } finally {
    db.close()
  }
})

test("unknown event ownership, malformed event data, and new Session tables remain ineligible", () => {
  const db = fixture()
  try {
    session(db, "ses_old", 10)
    event(db, "ses_old")
    db.query("UPDATE event_sequence SET seq = 1 WHERE aggregate_id = 'ses_old'").run()
    db.query(
      "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES ('evt_unknown', 'ses_old', 1, 'future.event.1', ?)",
    ).run(JSON.stringify({ sessionID: "ses_old", text: "future event sentinel" }))
    const proof = evidence(["ses_old"], { ses_old: 1 })
    const unknownEvent = inventory(access(db), proof, 1_000)
    expect(unknownEvent.trees[0]?.reasons).toContain("event-aggregate-owner-unknown:ses_old:future.event.1")

    db.query("DELETE FROM event WHERE id = 'evt_unknown'").run()
    db.query(
      "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES ('evt_invalid_json', 'ses_old', 1, 'session.next.context.updated.1', 'not-json')",
    ).run()
    const unreadableEvent = inventory(access(db), proof, 1_000)
    expect(unreadableEvent.trees[0]?.reasons).toContain("event-data-unreadable:ses_old")

    db.exec("CREATE TABLE future_session_copy (session_id TEXT NOT NULL, content TEXT NOT NULL)")
    db.query("INSERT INTO future_session_copy (session_id, content) VALUES ('ses_old', 'unclassified sentinel')").run()
    db.exec("CREATE TABLE future_event_copy (aggregate_id TEXT NOT NULL, content TEXT NOT NULL)")
    db.query(
      "INSERT INTO future_event_copy (aggregate_id, content) VALUES ('ses_old', 'unclassified event sentinel')",
    ).run()
    const unknownCopy = inventory(access(db), proof, 1_000)
    expect(unknownCopy.refusals).toContain("unclassified-session-owned-table:future_session_copy")
    expect(unknownCopy.refusals).toContain("unclassified-session-owned-table:future_event_copy")
    expect(unknownCopy.trees[0]?.eligible).toBe(false)
  } finally {
    db.close()
  }
})

test("apply redacts every copy in restartable batches and preserves metric values", async () => {
  await using fixtureDB = await createFixtureDatabase()
  const first = fixtureDB.db
  createSchema(first)
  session(first, "ses_old", 10)
  event(first, "ses_old")
  addCopies(first, "ses_old")
  const exportBefore = readExport(first)
  const proof = evidence(["ses_old"], { ses_old: 0 })
  const initial = inventory(access(first), proof, 1_000)
  expect(initial.trees[0]?.eligible).toBe(true)
  const metricSnapshot = [
    JSON.parse(
      first.query<{ data: string }, [string]>("SELECT data FROM message WHERE id = ?").get("msg_ses_old")!.data,
    ),
    JSON.parse(first.query<{ data: string }, [string]>("SELECT data FROM part WHERE id = ?").get("part_ses_old")!.data),
    JSON.parse(
      first.query<{ data: string }, [string]>("SELECT data FROM session_message WHERE id = ?").get("v2_ses_old")!.data,
    ),
  ].map((data) => ({
    providerID: data.providerID,
    modelID: data.modelID,
    responseModelID: data.responseModelID,
    responseModelIDs: data.responseModelIDs,
    cost: data.cost,
    tokens: data.tokens,
  }))

  const firstBatch = apply(access(first), {
    tree: initial.trees[0]!,
    evidence: () => proof,
    now: () => 1_000,
    batchSize: 1,
    maxBatches: 1,
  })
  expect(firstBatch.state).toBe("in-progress")
  expect(
    first.query<{ state: string }, [string]>("SELECT state FROM event_retention WHERE aggregate_id = ?").get("ses_old")
      ?.state,
  ).toBe("redacting")
  first.close()

  const resumed = await fixtureDB.reopen()
  try {
    const result = apply(access(resumed), {
      tree: initial.trees[0]!,
      evidence: () => proof,
      now: () => 1_000,
      batchSize: 1,
    })
    expect(result.state).toBe("complete")
    expect(result.changedRows).toBeGreaterThan(firstBatch.changedRows)
    expect(
      resumed
        .query<{ state: string }, [string]>("SELECT state FROM event_retention WHERE aggregate_id = ?")
        .get("ses_old")?.state,
    ).toBe("complete")
    const afterMetricSnapshot = [
      JSON.parse(
        resumed.query<{ data: string }, [string]>("SELECT data FROM message WHERE id = ?").get("msg_ses_old")!.data,
      ),
      JSON.parse(
        resumed.query<{ data: string }, [string]>("SELECT data FROM part WHERE id = ?").get("part_ses_old")!.data,
      ),
      JSON.parse(
        resumed.query<{ data: string }, [string]>("SELECT data FROM session_message WHERE id = ?").get("v2_ses_old")!
          .data,
      ),
    ].map((data) => ({
      providerID: data.providerID,
      modelID: data.modelID,
      responseModelID: data.responseModelID,
      responseModelIDs: data.responseModelIDs,
      cost: data.cost,
      tokens: data.tokens,
    }))
    expect(afterMetricSnapshot).toEqual(metricSnapshot)
    const exportAfter = readExport(resumed)
    const usageAxes = (archive: ReturnType<typeof readExport>) =>
      archive.records.map((record) => ({
        providerID: record["providerID"],
        modelID: record["modelID"],
        servedModelIDs: record["servedModelIDs"],
        tokens: record["tokens"],
        reportedCost: record["reportedCost"],
      }))
    expect(usageAxes(exportAfter)).toEqual(usageAxes(exportBefore))
    expect(exportAfter.reportedCostTotal).toBe(exportBefore.reportedCostTotal)
    expect(exportAfter.check.ok).toBe(exportBefore.check.ok)
    expect({
      sessions: exportAfter.sessions.length,
      records: exportAfter.records.length,
      orphanMessages: exportAfter.orphanMessages,
    }).toEqual({
      sessions: exportBefore.sessions.length,
      records: exportBefore.records.length,
      orphanMessages: exportBefore.orphanMessages,
    })
    expect(resumed.query("SELECT count(*) AS rows FROM session_share").get()).toEqual({ rows: 0 })
    expect(resumed.query("SELECT content FROM todo WHERE session_id = 'ses_old'").get()).toEqual({ content: "" })
    expect(resumed.query("SELECT prompt FROM session_input WHERE session_id = 'ses_old'").get()).toBeNull()
    expect(resumed.query("SELECT snapshot FROM session_context_epoch WHERE session_id = 'ses_old'").get()).toEqual({
      snapshot: "{}",
    })
    expect(resumed.query("SELECT baseline FROM session_context_epoch WHERE session_id = 'ses_old'").get()).toEqual({
      baseline: "",
    })

    const rawCopies = [
      ...resumed
        .query<{ data: string }, []>("SELECT data FROM event")
        .all()
        .map((row) => row.data),
      ...resumed
        .query<{ data: string }, []>("SELECT data FROM message")
        .all()
        .map((row) => row.data),
      ...resumed
        .query<{ data: string }, []>("SELECT data FROM part")
        .all()
        .map((row) => row.data),
      ...resumed
        .query<{ data: string }, []>("SELECT data FROM session_message")
        .all()
        .map((row) => row.data),
      ...resumed
        .query<{ prompt: string }, []>("SELECT prompt FROM session_input")
        .all()
        .map((row) => row.prompt),
      ...resumed
        .query<{ snapshot: string }, []>("SELECT snapshot FROM session_context_epoch")
        .all()
        .map((row) => row.snapshot),
      ...resumed
        .query<{ baseline: string }, []>("SELECT baseline FROM session_context_epoch")
        .all()
        .map((row) => row.baseline),
      ...resumed
        .query<{ content: string }, []>("SELECT content FROM todo")
        .all()
        .map((row) => row.content),
      ...resumed
        .query<{ title: string; directory: string }, []>("SELECT title, directory FROM session")
        .all()
        .map((row) => JSON.stringify(row)),
    ]
    expect(rawCopies.join("\n")).not.toContain("sentinel")
    expect(resumed.query("SELECT id, seq FROM event WHERE aggregate_id = 'ses_old'").get()).toEqual({
      id: "evt_ses_old",
      seq: 0,
    })
  } finally {
    resumed.close()
  }
})

test("failed apply evidence refuses before any row changes", () => {
  const db = fixture()
  try {
    session(db, "ses_old", 10)
    event(db, "ses_old")
    addCopies(db, "ses_old")
    const valid = evidence(["ses_old"], { ses_old: 0 })
    const candidate = inventory(access(db), valid, 1_000).trees[0]!
    const before = db.serialize()

    const result = apply(access(db), { tree: candidate, evidence: () => ({}), now: () => 1_000 })

    expect(result.state).toBe("refused")
    expect(result.changedRows).toBe(0)
    expect(result.reasons.join("\n")).toContain("reviewed-age-boundary-unavailable")
    expect(db.serialize().equals(before)).toBe(true)
  } finally {
    db.close()
  }
})

test("apply is unavailable for database files outside the isolated fixture directory", async () => {
  const filename = path.join(process.cwd(), `.retention-nonfixture-${process.pid}.sqlite`)
  const db = new Database(filename)
  try {
    createSchema(db)
    session(db, "ses_old", 10)
    event(db, "ses_old")
    const valid = evidence(["ses_old"], { ses_old: 0 })
    const tree = inventory(access(db), valid, 1_000).trees[0]!

    const result = apply(access(db), { tree, evidence: () => valid, now: () => 1_000 })

    expect(result.state).toBe("refused")
    expect(result.changedRows).toBe(0)
    expect(result.reasons).toContain("apply-only-supported-for-isolated-fixtures")
    expect(db.query("SELECT state FROM event_retention").get()).toBeNull()
    expect(
      db.query<{ data: string }, []>("SELECT data FROM event WHERE aggregate_id = 'ses_old'").get()?.data,
    ).toContain("event sentinel")
  } finally {
    db.close()
    await rm(filename, { force: true })
  }
})

test(
  "apply refuses a temporary symlink to a database target without altering it",
  async () => {
    await using linkRoot = await tmpdir()
    const targetDirectory = await mkdtemp(path.join(process.cwd(), ".retention-identity-target-"))
    const targetFilename = path.join(targetDirectory, "target.sqlite")
    const aliasFilename = path.join(linkRoot.path, "alias.sqlite")
    const target = new Database(targetFilename)
    createSchema(target)
    session(target, "ses_symlink", 10)
    event(target, "ses_symlink")
    const proof = evidence(["ses_symlink"], { ses_symlink: 0 })
    target.close()
    try {
      await symlink(targetFilename, aliasFilename)
      const alias = new Database(aliasFilename)
      try {
        const tree = inventory(access(alias), proof, 1_000).trees[0]!
        expect(tree.eligible).toBe(true)
        const before = alias.serialize()
        const result = apply(access(alias), { tree, evidence: () => proof, now: () => 1_000 })
        expect(result.state).toBe("refused")
        expect(result.changedRows).toBe(0)
        expect(result.reasons).toContain("apply-only-supported-for-isolated-fixtures")
        expect(alias.serialize().equals(before)).toBe(true)
      } finally {
        alias.close()
      }
      const reopened = new Database(targetFilename, { readonly: true })
      try {
        expect(
          reopened.query<{ data: string }, [string]>("SELECT data FROM event WHERE aggregate_id = ?").get("ses_symlink")
            ?.data,
        ).toContain("event sentinel")
        expect(reopened.query("SELECT state FROM event_retention").get()).toBeNull()
      } finally {
        reopened.close()
      }
    } finally {
      await rm(targetDirectory, { recursive: true, force: true })
    }
  },
  { timeout: 10_000 },
)

test(
  "apply refuses an unowned regular temporary database without altering it",
  async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "unowned.sqlite")
    const db = new Database(filename)
    createSchema(db)
    session(db, "ses_unowned", 10)
    event(db, "ses_unowned")
    const proof = evidence(["ses_unowned"], { ses_unowned: 0 })
    try {
      const tree = inventory(access(db), proof, 1_000).trees[0]!
      expect(tree.eligible).toBe(true)
      const before = db.serialize()
      const result = apply(access(db), { tree, evidence: () => proof, now: () => 1_000 })
      expect(result.state).toBe("refused")
      expect(result.changedRows).toBe(0)
      expect(result.reasons).toContain("apply-only-supported-for-isolated-fixtures")
      expect(db.serialize().equals(before)).toBe(true)
      expect(
        db.query<{ data: string }, [string]>("SELECT data FROM event WHERE aggregate_id = ?").get("ses_unowned")?.data,
      ).toContain("event sentinel")
      expect(db.query("SELECT state FROM event_retention").get()).toBeNull()
    } finally {
      db.close()
    }
  },
  { timeout: 10_000 },
)

test(
  "compaction refuses a temporary database not issued by the fixture factory",
  async () => {
    await using tmp = await tmpdir()
    const sourcePath = path.join(tmp.path, "unissued-compaction.sqlite")
    const backupPath = path.join(tmp.path, "unissued-backup.sqlite")
    const stagingPath = path.join(tmp.path, "unissued-staging.sqlite")
    const db = new Database(sourcePath)
    createSchema(db)
    session(db, "ses_unissued", 10)
    event(db, "ses_unissued")
    const before = db.serialize()
    const result = await compactRestoreFixture({
      sourcePath,
      backupPath,
      stagingPath,
      expectedSourceDeviceID: "fixture-source",
      expectedBackupDeviceID: "fixture-backup",
      expectedStagingDeviceID: "fixture-staging",
      retainedSessionID: "ses_unissued",
    })

    expect(result.state).toBe("refused")
    expect(result.changedFiles).toBe(0)
    expect(result.reasons).toContain("source-fixture-identity-unverified")
    expect(db.serialize().equals(before)).toBe(true)
    expect(await Bun.file(backupPath).exists()).toBe(false)
    expect(await Bun.file(stagingPath).exists()).toBe(false)
    db.close()
  },
  { timeout: 10_000 },
)

test(
  "fixture compaction preserves a caller-owned destination on refusal",
  async () => {
    await using sourceFixture = await retentionCompactionFixture("ses_existing_destination")
    const sourcePath = sourceFixture.filename
    const sourceBefore = await fileSnapshot(sourcePath)
    const sourceDeviceID = String(sourceBefore.device)
    const backupPath = path.join(path.dirname(sourcePath), "caller-owned-backup.sqlite")
    const stagingPath = path.join(path.dirname(sourcePath), "unused-stage.sqlite")
    await writeFile(backupPath, "caller-owned backup bytes", { flag: "wx" })
    const backupBefore = await fileSnapshot(backupPath)

    const result = await compactRestoreFixture({
      sourcePath,
      sourceFixture,
      backupPath,
      stagingPath,
      expectedSourceDeviceID: sourceDeviceID,
      expectedBackupDeviceID: sourceDeviceID,
      expectedStagingDeviceID: sourceDeviceID,
      retainedSessionID: "ses_existing_destination",
    })

    expect(result.state).toBe("refused")
    expect(result.changedFiles).toBe(0)
    expect(result.reasons).toContain(`destination-already-exists:${backupPath}`)
    expect(await fileSnapshot(backupPath)).toEqual(backupBefore)
    expect(await fileSnapshot(sourcePath)).toEqual(sourceBefore)
    expect(await Bun.file(stagingPath).exists()).toBe(false)
  },
  { timeout: 10_000 },
)

test(
  "fixture compaction preserves the source when a destination aliases it",
  async () => {
    await using sourceFixture = await retentionCompactionFixture("ses_source_alias")
    const sourcePath = sourceFixture.filename
    const sourceBefore = await fileSnapshot(sourcePath)
    const sourceDeviceID = String(sourceBefore.device)
    const stagingPath = path.join(path.dirname(sourcePath), "alias-stage.sqlite")

    const result = await compactRestoreFixture({
      sourcePath,
      sourceFixture,
      backupPath: sourcePath,
      stagingPath,
      expectedSourceDeviceID: sourceDeviceID,
      expectedBackupDeviceID: sourceDeviceID,
      expectedStagingDeviceID: sourceDeviceID,
      retainedSessionID: "ses_source_alias",
    })

    expect(result.state).toBe("refused")
    expect(result.changedFiles).toBe(0)
    expect(result.reasons).toContain("compaction-paths-conflict")
    expect(await fileSnapshot(sourcePath)).toEqual(sourceBefore)
    expect(await Bun.file(stagingPath).exists()).toBe(false)
  },
  { timeout: 10_000 },
)

test(
  "fixture compaction preserves a backup path created by a separate process after preflight",
  async () => {
    await using sourceFixture = await retentionCompactionFixture("ses_destination_arrival")
    const sourcePath = sourceFixture.filename
    const sourceBefore = await fileSnapshot(sourcePath)
    const sourceDeviceID = String(sourceBefore.device)
    const stagingDirectory = await mkdtemp("/dev/shm/opencode-retention-arrival-")
    await using cleanupStaging = { [Symbol.asyncDispose]: () => rm(stagingDirectory, { recursive: true, force: true }) }
    const backupPath = path.join(stagingDirectory, "racing-backup.sqlite")
    const stagingPath = path.join(stagingDirectory, "stage.sqlite")
    const destinationDeviceID = String((await stat(stagingDirectory)).dev)
    const racingBytes = "separate-process destination sentinel"
    let creatorStatus: number | null | undefined
    let racingFileBefore: Awaited<ReturnType<typeof fileSnapshot>> | undefined

    const result = await compactRestoreFixture({
      sourcePath,
      sourceFixture,
      backupPath,
      stagingPath,
      expectedSourceDeviceID: sourceDeviceID,
      expectedBackupDeviceID: destinationDeviceID,
      expectedStagingDeviceID: destinationDeviceID,
      retainedSessionID: "ses_destination_arrival",
      afterDestinationPreflight: async () => {
        const creator = spawnSync(
          process.execPath,
          [
            "-e",
            `require("node:fs").writeFileSync(${JSON.stringify(backupPath)}, ${JSON.stringify(racingBytes)}, { flag: "wx" })`,
          ],
          { encoding: "utf8", timeout: 5_000 },
        )
        creatorStatus = creator.status
        if (creator.status === 0) racingFileBefore = await fileSnapshot(backupPath)
      },
    })

    expect(creatorStatus).toBe(0)
    expect(result.state).toBe("refused")
    expect(result.changedFiles).toBe(0)
    expect(result.reasons.some((reason) => reason.includes("EEXIST"))).toBe(true)
    if (!racingFileBefore) throw new Error("separate process did not create the competing backup")
    expect(racingFileBefore.bytes.toString()).toBe(racingBytes)
    expect(await fileSnapshot(backupPath)).toEqual(racingFileBefore)
    expect(await fileSnapshot(sourcePath)).toEqual(sourceBefore)
    expect(await Bun.file(stagingPath).exists()).toBe(false)
  },
  { timeout: 10_000 },
)

test("apply revalidates evidence inside every batch and keeps partial history unreplayable", () => {
  const db = fixture()
  try {
    session(db, "ses_old", 10)
    event(db, "ses_old")
    addCopies(db, "ses_old")
    const valid = evidence(["ses_old"], { ses_old: 0 })
    const tree = inventory(access(db), valid, 1_000).trees[0]!
    const proofs = [valid, { ...valid, evidenceError: "liveness source stopped responding" }]

    const result = apply(access(db), {
      tree,
      evidence: () => proofs.shift() ?? {},
      now: () => 1_000,
      batchSize: 1,
    })

    expect(result.state).toBe("in-progress")
    expect(result.changedRows).toBe(1)
    expect(result.reasons).toContain("evidence-unreadable:liveness source stopped responding")
    expect(db.query("SELECT state FROM event_retention WHERE aggregate_id = 'ses_old'").get()).toEqual({
      state: "redacting",
    })
    expect(db.query<{ data: string }, []>("SELECT data FROM part WHERE session_id = 'ses_old'").get()?.data).toContain(
      "part sentinel",
    )
  } finally {
    db.close()
  }
})

test("apply refuses aggregate-owner changes after inventory before changing rows", () => {
  const db = fixture()
  try {
    session(db, "ses_owner", 10)
    event(db, "ses_owner")
    const proof = evidence(["ses_owner"], { ses_owner: 0 })
    const tree = inventory(access(db), proof, 1_000).trees[0]!
    expect(tree.eligible).toBe(true)
    db.query("UPDATE event_sequence SET owner_id = 'new-owner' WHERE aggregate_id = 'ses_owner'").run()
    const before = db.serialize()

    const result = apply(access(db), { tree, evidence: () => proof, now: () => 1_000 })

    expect(result.state).toBe("refused")
    expect(result.changedRows).toBe(0)
    expect(result.reasons).toContain("cross-process-aggregate-owner-snapshot-mismatch:ses_owner")
    expect(db.serialize().equals(before)).toBe(true)
  } finally {
    db.close()
  }
})

test("apply refuses a newly added session-owned raw table before changing rows", () => {
  const db = fixture()
  try {
    session(db, "ses_schema", 10)
    event(db, "ses_schema")
    const proof = evidence(["ses_schema"], { ses_schema: 0 })
    const tree = inventory(access(db), proof, 1_000).trees[0]!
    expect(tree.eligible).toBe(true)
    db.exec("CREATE TABLE future_session_copy (session_id TEXT NOT NULL, body TEXT NOT NULL)")
    db.query("INSERT INTO future_session_copy VALUES ('ses_schema', 'new raw sentinel')").run()
    const before = db.serialize()

    const result = apply(access(db), { tree, evidence: () => proof, now: () => 1_000 })

    expect(result.state).toBe("refused")
    expect(result.changedRows).toBe(0)
    expect(result.reasons).toContain("unclassified-session-owned-table:future_session_copy")
    expect(db.serialize().equals(before)).toBe(true)
  } finally {
    db.close()
  }
})

test("apply refuses event data corruption after inventory before changing rows", () => {
  const db = fixture()
  try {
    session(db, "ses_event", 10)
    event(db, "ses_event")
    const proof = evidence(["ses_event"], { ses_event: 0 })
    const tree = inventory(access(db), proof, 1_000).trees[0]!
    expect(tree.eligible).toBe(true)
    db.query("UPDATE event SET data = 'not-json' WHERE aggregate_id = 'ses_event'").run()
    const before = db.serialize()

    const result = apply(access(db), { tree, evidence: () => proof, now: () => 1_000 })

    expect(result.state).toBe("refused")
    expect(result.changedRows).toBe(0)
    expect(result.reasons).toContain("event-data-unreadable:ses_event")
    expect(db.serialize().equals(before)).toBe(true)
  } finally {
    db.close()
  }
})

test(
  "retention marker fences a real Context Epoch producer paused across the first batch",
  async () => {
    await using fixture = await createFixtureDatabase()
    const sessionID = "ses_context_race"
    const now = 1_000
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const services = yield* Layer.build(layerFromPath(fixture.filename))
            const writer = Context.get(services, Service).db
            session(fixture.db, sessionID, 10)
            event(fixture.db, sessionID)
            const proof = evidence([sessionID], { [sessionID]: 0 }, now)
            const tree = inventory(access(fixture.db), proof, now).trees.find(
              (candidate) => candidate.rootSessionID === sessionID,
            )
            if (!tree?.eligible)
              return yield* Effect.die(new Error(`Retention fixture refused: ${tree?.reasons.join(",")}`))

            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<SystemContext.SystemContext>()
            const producerContext = Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined)
              return yield* Deferred.await(release)
            })
            const producer = SessionContextEpoch.initialize(writer, producerContext, SessionV2.ID.make(sessionID))
            const fiber = yield* producer.pipe(Effect.forkChild({ startImmediately: true }))
            yield* Effect.addFinalizer(() =>
              Deferred.succeed(release, SystemContext.empty).pipe(
                Effect.andThen(Fiber.interrupt(fiber)),
                Effect.asVoid,
              ),
            )
            yield* Effect.race(
              Deferred.await(entered),
              Effect.sleep("2 seconds").pipe(
                Effect.andThen(Effect.fail(new Error("Context Epoch producer did not start"))),
              ),
            )

            const firstBatch = apply(access(fixture.db), {
              tree,
              evidence: () => proof,
              now: () => now,
              batchSize: 1,
              maxBatches: 1,
            })
            expect(firstBatch.state).toBe("in-progress")
            expect(firstBatch.changedRows).toBe(1)
            expect(
              yield* writer
                .select({ state: EventRetentionTable.state })
                .from(EventRetentionTable)
                .where(eq(EventRetentionTable.aggregate_id, sessionID))
                .get(),
            ).toEqual({ state: "redacting" })
            yield* Deferred.succeed(release, SystemContext.empty)

            const result = yield* Fiber.join(fiber).pipe(Effect.exit)
            expect(Exit.isFailure(result)).toBe(true)
            expect(
              fixture.db
                .query<
                  { session_id: string },
                  [string]
                >("SELECT session_id FROM session_context_epoch WHERE session_id = ?")
                .get(sessionID),
            ).toBeNull()
          }),
        ),
      )
    } finally {
      fixture.db.close()
    }
  },
  { timeout: 10_000 },
)

test(
  "fixture compaction rehearses backup, VACUUM, and restore under an exclusive writer fence",
  async () => {
    await using sourceFixture = await createFixtureDatabase()
    const sourcePath = sourceFixture.filename
    const stagingDirectory = await mkdtemp("/dev/shm/opencode-retention-stage-")
    await using cleanupStaging = { [Symbol.asyncDispose]: () => rm(stagingDirectory, { recursive: true, force: true }) }
    const beforeDB = sourceFixture.db
    createSchema(beforeDB)
    session(beforeDB, "ses_physical", 10)
    beforeDB.query("INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('ses_physical', 599, NULL)").run()
    const raw = "tool output sentinel ".repeat(800)
    const addEvents = beforeDB.transaction(() => {
      for (let seq = 0; seq < 600; seq++) {
        beforeDB
          .query("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, 'ses_physical', ?, ?, ?)")
          .run(
            `evt_physical_${seq}`,
            seq,
            "session.next.context.updated.1",
            JSON.stringify({ sessionID: "ses_physical", timestamp: seq, messageID: "msg_a", text: raw }),
          )
      }
    })
    addEvents()
    const beforeRedactionStat = await stat(sourcePath)
    const beforeRedaction = {
      size: beforeRedactionStat.size,
      blocks: beforeRedactionStat.blocks,
      pageCount: beforeDB.query<{ page_count: number }, []>("PRAGMA page_count").get()!.page_count,
      freelistCount: beforeDB.query<{ freelist_count: number }, []>("PRAGMA freelist_count").get()!.freelist_count,
      integrityCheck: beforeDB.query<{ integrity_check: string }, []>("PRAGMA integrity_check").get()!.integrity_check,
      retainedSession: beforeDB
        .query<
          {
            id: string
            cost: number
            tokens_input: number
            tokens_output: number
            tokens_reasoning: number
            tokens_cache_read: number
            tokens_cache_write: number
          },
          [string]
        >(
          "SELECT id, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write FROM session WHERE id = ?",
        )
        .get("ses_physical"),
    }
    beforeDB.close()

    const redactionDB = await sourceFixture.reopen()
    const proof = evidence(["ses_physical"], { ses_physical: 599 })
    const beforeApply = inventory(access(redactionDB), proof, 1_000).trees.find(
      (item) => item.rootSessionID === "ses_physical",
    )!
    const redaction = apply(access(redactionDB), {
      tree: beforeApply,
      evidence: () => proof,
      now: () => 1_000,
      batchSize: 32,
    })
    expect(redaction.state).toBe("complete")

    const afterRedactionStat = await stat(sourcePath)
    const afterRedactionDB = new Database(sourcePath, { readonly: true })
    const afterRedaction = {
      pageCount: afterRedactionDB.query<{ page_count: number }, []>("PRAGMA page_count").get()!.page_count,
      freelistCount: afterRedactionDB.query<{ freelist_count: number }, []>("PRAGMA freelist_count").get()!
        .freelist_count,
      integrityCheck: afterRedactionDB.query<{ integrity_check: string }, []>("PRAGMA integrity_check").get()!
        .integrity_check,
      retainedSession: afterRedactionDB
        .query<
          {
            id: string
            cost: number
            tokens_input: number
            tokens_output: number
            tokens_reasoning: number
            tokens_cache_read: number
            tokens_cache_write: number
          },
          [string]
        >(
          "SELECT id, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write FROM session WHERE id = ?",
        )
        .get("ses_physical"),
    }
    afterRedactionDB.close()
    expect(beforeRedaction.freelistCount).toBe(0)
    expect(afterRedactionStat.size).toBe(beforeRedactionStat.size)
    expect(afterRedaction.freelistCount).toBeGreaterThan(beforeRedaction.freelistCount)

    const sourceDeviceID = String((await stat(sourcePath)).dev)
    const destinationDeviceID = String((await stat(stagingDirectory)).dev)
    const stagingFilesystem = await statfs(stagingDirectory)
    const availableBytes = stagingFilesystem.bavail * stagingFilesystem.bsize
    const backupPath = path.join(stagingDirectory, "backup.sqlite")
    const stagingPath = path.join(stagingDirectory, "compacted.sqlite")
    const insufficient = await compactRestoreFixture({
      sourcePath,
      sourceFixture,
      backupPath: path.join(stagingDirectory, "capacity-backup.sqlite"),
      stagingPath: path.join(stagingDirectory, "capacity-stage.sqlite"),
      expectedSourceDeviceID: sourceDeviceID,
      expectedBackupDeviceID: destinationDeviceID,
      expectedStagingDeviceID: destinationDeviceID,
      retainedSessionID: "ses_physical",
      capacityFloorBytes: availableBytes + 1,
    })
    expect(insufficient.state).toBe("refused")
    expect(insufficient.changedFiles).toBe(0)
    expect(insufficient.reasons.some((reason) => reason.startsWith("destination-capacity-insufficient:"))).toBe(true)

    const unverifiedIdentity = await compactRestoreFixture({
      sourcePath,
      sourceFixture,
      backupPath: path.join(stagingDirectory, "identity-backup.sqlite"),
      stagingPath: path.join(stagingDirectory, "identity-stage.sqlite"),
      expectedSourceDeviceID: sourceDeviceID,
      expectedBackupDeviceID: "unverified-device",
      expectedStagingDeviceID: destinationDeviceID,
      retainedSessionID: "ses_physical",
    })
    expect(unverifiedIdentity.state).toBe("refused")
    expect(unverifiedIdentity.changedFiles).toBe(0)
    expect(unverifiedIdentity.reasons).toContain("backup-filesystem-identity-mismatch")

    const holder = new Database(sourcePath, { readonly: true })
    const held = await compactRestoreFixture({
      sourcePath,
      sourceFixture,
      backupPath,
      stagingPath,
      expectedSourceDeviceID: sourceDeviceID,
      expectedBackupDeviceID: destinationDeviceID,
      expectedStagingDeviceID: destinationDeviceID,
      retainedSessionID: "ses_physical",
    })
    holder.close()
    expect(held.state).toBe("refused")
    expect(held.changedFiles).toBe(0)
    expect(held.reasons).toContain("source-inode-still-open")

    const rootDeviceID = String((await stat("/")).dev)
    const rootDestination = await compactRestoreFixture({
      sourcePath,
      sourceFixture,
      backupPath: path.join(path.dirname(sourcePath), "root-backup.sqlite"),
      stagingPath: path.join(path.dirname(sourcePath), "root-stage.sqlite"),
      expectedSourceDeviceID: sourceDeviceID,
      expectedBackupDeviceID: rootDeviceID,
      expectedStagingDeviceID: rootDeviceID,
      retainedSessionID: "ses_physical",
    })
    expect(rootDestination.state).toBe("refused")
    expect(rootDestination.changedFiles).toBe(0)
    expect(rootDestination.reasons).toContain("staging-destination-is-root")

    const beforeReplaceStat = await stat(sourcePath)
    let fixtureWriterBlocked = false
    let sqliteWriterBlocked = false
    let separateProcessWriterBlocked = false
    const writerArrival = await compactRestoreFixture({
      sourcePath,
      sourceFixture,
      backupPath,
      stagingPath,
      expectedSourceDeviceID: sourceDeviceID,
      expectedBackupDeviceID: destinationDeviceID,
      expectedStagingDeviceID: destinationDeviceID,
      retainedSessionID: "ses_physical",
      afterExclusiveLock: async () => {
        try {
          await sourceFixture.reopen()
        } catch {
          fixtureWriterBlocked = true
        }
        let arrivingWriter: Database | undefined
        try {
          arrivingWriter = new Database(sourcePath)
          arrivingWriter.query("SELECT seq FROM event_sequence WHERE aggregate_id = 'ses_physical'").get()
        } catch {
          sqliteWriterBlocked = true
        } finally {
          arrivingWriter?.close()
        }
        const separateProcessWriter = spawnSync(
          process.execPath,
          [
            "-e",
            `try { const { Database } = require("bun:sqlite"); const db = new Database(${JSON.stringify(sourcePath)}); db.query("UPDATE event_sequence SET seq = seq + 1 WHERE aggregate_id = ?").run("ses_physical"); db.close(); process.exit(0) } catch (error) { console.error(error); process.exit(1) }`,
          ],
          { encoding: "utf8", timeout: 5_000 },
        )
        separateProcessWriterBlocked =
          separateProcessWriter.status === 1 && /database is locked/i.test(`${separateProcessWriter.stdout}\n${separateProcessWriter.stderr}`)
      },
    })
    expect(writerArrival.state).toBe("refused")
    expect(writerArrival.changedFiles).toBe(0)
    expect(writerArrival.reasons).toContain("writer-arrival-blocked-under-exclusive-fence")
    expect(separateProcessWriterBlocked).toBe(true)
    expect(fixtureWriterBlocked).toBe(true)
    expect(sqliteWriterBlocked).toBe(true)
    expect(await Bun.file(path.join(path.dirname(sourcePath), ".retention-compaction.lock")).exists()).toBe(false)
    expect({
      device: (await stat(sourcePath)).dev,
      inode: (await stat(sourcePath)).ino,
      size: (await stat(sourcePath)).size,
      seq: sourceFixture.db
        .query<{ seq: number }, []>("SELECT seq FROM event_sequence WHERE aggregate_id = 'ses_physical'")
        .get()!.seq,
    }).toEqual({
      device: beforeReplaceStat.dev,
      inode: beforeReplaceStat.ino,
      size: beforeReplaceStat.size,
      seq: 599,
    })

    // Keep a cached source statement live across the swap; compaction must finalize it before checking the old inode.
    using preparedSourceStatement = sourceFixture.db.query<{ seq: number }, [string]>(
      "SELECT seq FROM event_sequence WHERE aggregate_id = ?",
    )
    const preparedSourceRows = preparedSourceStatement.iterate("ses_physical")
    expect(preparedSourceRows.next()).toEqual({ value: { seq: 599 }, done: false })

    const compacted = await compactRestoreFixture({
      sourcePath,
      sourceFixture,
      backupPath,
      stagingPath,
      expectedSourceDeviceID: sourceDeviceID,
      expectedBackupDeviceID: destinationDeviceID,
      expectedStagingDeviceID: destinationDeviceID,
      retainedSessionID: "ses_physical",
    })
    expect(compacted.state, compacted.reasons.join(", ")).toBe("complete")
    expect(() => preparedSourceRows.next()).toThrow()
    expect(compacted.changedFiles).toBe(3)
    expect(compacted.reasons).toEqual([])
    expect(compacted.sourceDeviceID).not.toBe(compacted.stagingDeviceID)
    expect(compacted.measurements).toBeDefined()
    expect(compacted.measurements!.before.retainedSession).toEqual(beforeRedaction.retainedSession ?? undefined)
    expect(compacted.measurements!.backup.retainedSession).toEqual(beforeRedaction.retainedSession ?? undefined)
    expect(compacted.measurements!.staging.retainedSession).toEqual(beforeRedaction.retainedSession ?? undefined)
    expect(compacted.measurements!.restored.retainedSession).toEqual(beforeRedaction.retainedSession ?? undefined)
    expect(compacted.measurements!.backup.integrityCheck).toBe("ok")
    expect(compacted.measurements!.staging.integrityCheck).toBe("ok")
    expect(compacted.measurements!.restored.integrityCheck).toBe("ok")
    expect(compacted.measurements!.staging.freelistCount).toBe(0)
    expect(compacted.measurements!.restored.freelistCount).toBe(0)
    expect(compacted.measurements!.restored.size).toBeLessThan(beforeRedaction.size)
    expect(compacted.measurements!.restored.blocks).toBeLessThan(beforeRedaction.blocks)
    expect(compacted.measurements!.restored.pageCount).toBeLessThan(afterRedaction.pageCount)
    expect(await Bun.file(backupPath).exists()).toBe(true)
    expect(await Bun.file(stagingPath).exists()).toBe(true)
    expect(await Bun.file(path.join(path.dirname(sourcePath), ".retention-compaction.lock")).exists()).toBe(false)
    const restoredStat = await stat(sourcePath)
    expect(restoredStat.size).toBeLessThan(beforeRedaction.size)
    expect(restoredStat.blocks).toBeLessThan(beforeRedaction.blocks)
    expect(sourceFixture.db.query<{ integrity_check: string }, []>("PRAGMA integrity_check").get()!.integrity_check).toBe(
      "ok",
    )
    expect(
      sourceFixture.db
        .query<{ id: string; tokens_input: number }, [string]>("SELECT id, tokens_input FROM session WHERE id = ?")
        .get("ses_physical"),
    ).toEqual({ id: "ses_physical", tokens_input: 13 })

    expect(beforeRedaction.integrityCheck).toBe("ok")
    expect(beforeRedaction.freelistCount).toBe(0)
    expect(afterRedaction.integrityCheck).toBe("ok")
    expect(afterRedaction.freelistCount).toBeGreaterThan(beforeRedaction.freelistCount)
    expect(afterRedactionStat.size).toBe(beforeRedactionStat.size)
    expect(afterRedactionStat.blocks).toBe(beforeRedactionStat.blocks)
    expect(afterRedaction.pageCount).toBeGreaterThan(0)
    expect(afterRedaction.retainedSession).toEqual(beforeRedaction.retainedSession ?? null)
  },
  { timeout: 60_000 },
)
