import { expect, spyOn, test } from "bun:test"
import { spawnSync } from "node:child_process"
import { Database } from "bun:sqlite"
import { Context, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { Flag } from "@opencode-ai/core/flag/flag"
import { chmod, mkdtemp, readFile, rm, stat, statfs, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { eq } from "drizzle-orm"
import { Service, layerFromPath } from "@opencode-ai/core/database/database"
import { EventRetentionTable } from "@opencode-ai/core/event/sql"
import { SessionContextEpoch } from "@opencode-ai/core/session/context-epoch"
import { SessionV2 } from "@opencode-ai/core/session"
import { SystemContext } from "@opencode-ai/core/system-context/index"
import {
  apply,
  applyGated,
  compactRestoreFixture,
  createFixtureDatabase,
  inactivityPolicy,
  inventory,
  RetentionCommand,
  scanDatabaseHolders,
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
    CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, time_created INTEGER NOT NULL DEFAULT 10, time_updated INTEGER NOT NULL DEFAULT 10, data TEXT NOT NULL);
    CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT NOT NULL, session_id TEXT NOT NULL, time_created INTEGER NOT NULL DEFAULT 10, time_updated INTEGER NOT NULL DEFAULT 10, data TEXT NOT NULL);
    CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, type TEXT NOT NULL DEFAULT 'assistant', seq INTEGER NOT NULL, time_created INTEGER NOT NULL DEFAULT 10, time_updated INTEGER NOT NULL DEFAULT 10, data TEXT NOT NULL);
    CREATE TABLE session_input (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, promoted_seq INTEGER, prompt TEXT NOT NULL, time_created INTEGER NOT NULL DEFAULT 10);
    CREATE TABLE session_context_epoch (session_id TEXT PRIMARY KEY, baseline TEXT NOT NULL, snapshot TEXT NOT NULL);
    CREATE TABLE session_prompt_queue (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, input TEXT NOT NULL, time_created INTEGER NOT NULL DEFAULT 10, time_promoted INTEGER, time_withdrawn INTEGER);
    CREATE TABLE session_prompt_queue_sequence (session_id TEXT PRIMARY KEY, seq INTEGER NOT NULL);
    CREATE TABLE todo (session_id TEXT NOT NULL, position INTEGER NOT NULL, content TEXT NOT NULL, time_created INTEGER NOT NULL DEFAULT 10, time_updated INTEGER NOT NULL DEFAULT 10);
    CREATE TABLE session_share (session_id TEXT PRIMARY KEY, id TEXT NOT NULL, secret TEXT NOT NULL, url TEXT NOT NULL, time_created INTEGER NOT NULL DEFAULT 10, time_updated INTEGER NOT NULL DEFAULT 10);
    CREATE TABLE event_writer_process (writer_id TEXT PRIMARY KEY, pid INTEGER NOT NULL, boot_id TEXT, start_ticks TEXT, time_registered INTEGER NOT NULL);
    CREATE TABLE event_aggregate_writer (aggregate_id TEXT NOT NULL, writer_id TEXT NOT NULL, time_last_append INTEGER NOT NULL, PRIMARY KEY (aggregate_id, writer_id));
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

// Every fixture row is timestamped 10 ms after the epoch, so NOW is just past the inactivity window.
const NOW = inactivityPolicy.windowMs + 1_000

const evidence = (
  sessionIDs: string[],
  finalSequence: Record<string, number>,
  filename = ":memory:",
): RetentionEvidence => ({
  customerBinding: {
    proofID: "fixture-unbound-session-classification",
    durable: true,
    sessionIDs,
    customerBoundSessionIDs: [],
    nonCustomerSessionIDs: sessionIDs,
  },
  readerContract: { reviewed: true, id: "fixture-reader-contract" },
  holders: scanDatabaseHolders(filename),
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
      time: { created: 10, completed: 11 },
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
      time: { created: 10, completed: 11 },
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

    const result = inventory(observed, {}, NOW, { transactional: false })

    expect(transactions).toBe(0)
    expect(result.trees[0]?.eligible).toBe(false)
    expect(result.trees[0]?.reasons).toContain("customer-session-classification-unavailable")
  } finally {
    db.close()
  }
})

test("retention CLI keeps inventory read-only and refuses apply before any write when no tree passes every gate", async () => {
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
    expect(dryRun.trees[0]?.reasons).toEqual([
      "customer-session-classification-unavailable",
      "event-payload-ownership-proof-unavailable:ses_cli",
      "measurement-handoff-receipt-unavailable",
      "reader-contract-unreviewed",
    ])
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
    const dryRun = inventory(access(db), proof, NOW)

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

    const refused = inventory(access(db), { ...proof, holders: { state: "unreadable", reason: "fixture" } }, NOW)
    expect(refused.trees[0]?.reasons).toContain("database-holder-scan-unreadable:fixture")
    expect(db.serialize().equals(before)).toBe(true)
  } finally {
    db.close()
  }
})

test("dry run names pending, unfinished, workspace-owned, receipt, and unreadable proofs", () => {
  const db = fixture()
  try {
    session(db, "ses_old", 10)
    event(db, "ses_old")
    db.query("INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('ses_orphan', 0, 'owner-a')").run()
    const valid = evidence(["ses_old"], { ses_old: 0 })
    const reasons = () => inventory(access(db), valid, NOW).trees.find((item) => item.rootSessionID === "ses_old")!.reasons
    expect(reasons()).toEqual([])
    expect(inventory(access(db), valid, NOW).unknownAggregates).toContain("ses_orphan")

    db.query(
      "INSERT INTO message (id, session_id, data) VALUES ('msg_running', 'ses_old', ?)",
    ).run(JSON.stringify({ role: "assistant", time: { created: 10 } }))
    expect(reasons()).toContain("unfinished-assistant-message:ses_old")
    db.query("DELETE FROM message").run()
    db.query("INSERT INTO session_message (id, session_id, type, seq, data) VALUES ('v2_running', 'ses_old', 'assistant', 1, ?)").run(
      JSON.stringify({ time: { created: 10 } }),
    )
    expect(reasons()).toContain("unfinished-assistant-message:ses_old")
    db.query("DELETE FROM session_message").run()

    db.query(
      "INSERT INTO session_input (id, session_id, promoted_seq, prompt) VALUES ('pending', 'ses_old', NULL, '{}')",
    ).run()
    expect(reasons()).toContain("v2-input-pending")
    db.query("DELETE FROM session_input").run()
    db.query("INSERT INTO session_prompt_queue (id, session_id, input) VALUES ('queued', 'ses_old', '{}')").run()
    expect(reasons()).toContain("v1-prompt-queue-row-present")
    db.query("DELETE FROM session_prompt_queue").run()

    db.query("UPDATE event_sequence SET owner_id = 'owner-a' WHERE aggregate_id = 'ses_old'").run()
    expect(reasons()).toContain("session-owned-by-workspace:ses_old")
    db.query("UPDATE event_sequence SET owner_id = NULL WHERE aggregate_id = 'ses_old'").run()

    const staleReceipt = inventory(
      access(db),
      { ...valid, handoff: { ...valid.handoff!, finalSequence: { ses_old: -1 } } },
      NOW,
    )
    expect(staleReceipt.trees.find((item) => item.rootSessionID === "ses_old")?.reasons).toContain(
      "measurement-handoff-does-not-cover-final-write:ses_old",
    )

    const unreadable = inventory(access(db), { ...valid, evidenceError: "receipt store unavailable" }, NOW)
    expect(unreadable.trees.find((item) => item.rootSessionID === "ses_old")?.reasons).toContain(
      "evidence-unreadable:receipt store unavailable",
    )
    expect(reasons()).toEqual([])
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
    const tree = inventory(access(db), valid, NOW).trees[0]!
    const noReceipt = { ...valid, handoff: undefined }
    const before = db.serialize()

    const dryRun = inventory(access(db), noReceipt, NOW)
    expect(dryRun.trees[0]?.reasons).toContain("measurement-handoff-receipt-unavailable")
    const result = apply(access(db), { tree, evidence: () => noReceipt, now: () => NOW })

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
    const boundTree = inventory(access(db), customerBound, NOW).trees[0]!
    expect(boundTree.reasons).toContain("customer-bound-session-retention-workflow-unapproved:ses_customer")
    const refused = apply(access(db), { tree: boundTree, evidence: () => customerBound, now: () => NOW })
    expect(refused.state).toBe("refused")
    expect(refused.changedRows).toBe(0)
    expect(db.serialize().equals(before)).toBe(true)

    const unknown = inventory(access(db), { ...valid, customerBinding: undefined }, NOW)
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
    const unknownEvent = inventory(access(db), proof, NOW)
    expect(unknownEvent.trees[0]?.reasons).toContain("event-aggregate-owner-unknown:ses_old:future.event.1")

    db.query("DELETE FROM event WHERE id = 'evt_unknown'").run()
    db.query(
      "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES ('evt_invalid_json', 'ses_old', 1, 'session.next.context.updated.1', 'not-json')",
    ).run()
    const unreadableEvent = inventory(access(db), proof, NOW)
    expect(unreadableEvent.trees[0]?.reasons).toContain("event-data-unreadable:ses_old")

    db.exec("CREATE TABLE future_session_copy (session_id TEXT NOT NULL, content TEXT NOT NULL)")
    db.query("INSERT INTO future_session_copy (session_id, content) VALUES ('ses_old', 'unclassified sentinel')").run()
    db.exec("CREATE TABLE future_event_copy (aggregate_id TEXT NOT NULL, content TEXT NOT NULL)")
    db.query(
      "INSERT INTO future_event_copy (aggregate_id, content) VALUES ('ses_old', 'unclassified event sentinel')",
    ).run()
    const unknownCopy = inventory(access(db), proof, NOW)
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
  // The Session row is the per-session usage record the session-usage projection maintains.
  const sessionUsage = (db: Database) =>
    db
      .query(
        "SELECT cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write, model, agent FROM session WHERE id = 'ses_old'",
      )
      .get()
  const usageBefore = sessionUsage(first)
  expect(usageBefore).toEqual({
    cost: 12.5,
    tokens_input: 13,
    tokens_output: 17,
    tokens_reasoning: 2,
    tokens_cache_read: 3,
    tokens_cache_write: 4,
    model: JSON.stringify({ id: "requested-model", providerID: "provider-a" }),
    agent: "build",
  })
  const proof = evidence(["ses_old"], { ses_old: 0 }, fixtureDB.filename)
  const initial = inventory(access(first), proof, NOW)
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
    time: data.time,
  }))

  const firstBatch = apply(access(first), {
    tree: initial.trees[0]!,
    evidence: () => proof,
    now: () => NOW,
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
      now: () => NOW,
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
      time: data.time,
    }))
    expect(afterMetricSnapshot).toEqual(metricSnapshot)
    expect(sessionUsage(resumed)).toEqual(usageBefore)
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
    const candidate = inventory(access(db), valid, NOW).trees[0]!
    const before = db.serialize()

    const result = apply(access(db), { tree: candidate, evidence: () => ({}), now: () => NOW })

    expect(result.state).toBe("refused")
    expect(result.changedRows).toBe(0)
    expect(result.reasons.join("\n")).toContain("measurement-handoff-receipt-unavailable")
    expect(db.serialize().equals(before)).toBe(true)
  } finally {
    db.close()
  }
})

test("apply on a database outside the fixture factory requires a holder scan of that exact file", async () => {
  await using tmp = await tmpdir()
  // The production data directory admits only its owner, which the holder scan requires.
  await chmod(tmp.path, 0o700)
  const filename = path.join(tmp.path, "production-shaped.sqlite")
  const db = new Database(filename)
  try {
    createSchema(db)
    session(db, "ses_old", 10)
    event(db, "ses_old")
    const valid = evidence(["ses_old"], { ses_old: 0 }, filename)
    const tree = inventory(access(db), valid, NOW).trees[0]!
    expect(tree.reasons).toEqual([])
    const before = db.serialize()

    const otherFile = { ...valid, holders: scanDatabaseHolders(":memory:") }
    const mismatched = apply(access(db), { tree, evidence: () => otherFile, now: () => NOW })
    expect(mismatched.state).toBe("refused")
    expect(mismatched.changedRows).toBe(0)
    expect(mismatched.reasons).toContain("database-holder-scan-target-mismatch")
    expect(db.serialize().equals(before)).toBe(true)

    const result = apply(access(db), { tree, evidence: () => valid, now: () => NOW })
    expect(result.state).toBe("complete")
    expect(db.query("SELECT state FROM event_retention").get()).toEqual({ state: "complete" })
    expect(
      db.query<{ data: string }, []>("SELECT data FROM event WHERE aggregate_id = 'ses_old'").get()?.data,
    ).not.toContain("event sentinel")
  } finally {
    db.close()
  }
})

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
    const tree = inventory(access(db), valid, NOW).trees[0]!
    const proofs = [valid, { ...valid, evidenceError: "liveness source stopped responding" }]

    const result = apply(access(db), {
      tree,
      evidence: () => proofs.shift() ?? {},
      now: () => NOW,
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
    const tree = inventory(access(db), proof, NOW).trees[0]!
    expect(tree.eligible).toBe(true)
    db.query("UPDATE event_sequence SET owner_id = 'new-owner' WHERE aggregate_id = 'ses_owner'").run()
    const before = db.serialize()

    const result = apply(access(db), { tree, evidence: () => proof, now: () => NOW })

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
    const tree = inventory(access(db), proof, NOW).trees[0]!
    expect(tree.eligible).toBe(true)
    db.exec("CREATE TABLE future_session_copy (session_id TEXT NOT NULL, body TEXT NOT NULL)")
    db.query("INSERT INTO future_session_copy VALUES ('ses_schema', 'new raw sentinel')").run()
    const before = db.serialize()

    const result = apply(access(db), { tree, evidence: () => proof, now: () => NOW })

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
    const tree = inventory(access(db), proof, NOW).trees[0]!
    expect(tree.eligible).toBe(true)
    db.query("UPDATE event SET data = 'not-json' WHERE aggregate_id = 'ses_event'").run()
    const before = db.serialize()

    const result = apply(access(db), { tree, evidence: () => proof, now: () => NOW })

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
    const now = NOW
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const services = yield* Layer.build(layerFromPath(fixture.filename))
            const writer = Context.get(services, Service).db
            session(fixture.db, sessionID, 10)
            event(fixture.db, sessionID)
            const proof = evidence([sessionID], { [sessionID]: 0 }, fixture.filename)
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
    const proof = evidence(["ses_physical"], { ses_physical: 599 }, sourcePath)
    const beforeApply = inventory(access(redactionDB), proof, NOW).trees.find(
      (item) => item.rootSessionID === "ses_physical",
    )!
    const redaction = apply(access(redactionDB), {
      tree: beforeApply,
      evidence: () => proof,
      now: () => NOW,
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

    // Keep a half-iterated source query cached so compaction must release it before checking and replacing the source inode.
    using cachedSourceStatement = sourceFixture.db.query<{ seq: number }, [string]>(
      "SELECT seq FROM event_sequence WHERE aggregate_id = ?",
    )
    const cachedSourceRows = cachedSourceStatement.iterate("ses_physical")
    expect(cachedSourceRows.next()).toEqual({ value: { seq: 599 }, done: false })

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
    expect(() => cachedSourceRows.next()).toThrow()
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

const DAY = 24 * 60 * 60 * 1000

// A refused tree skips the event-payload JSON scan and says so; the remaining reasons name the refusing gate.
const gateReasons = (tree: { reasons: readonly string[] }) =>
  tree.reasons.filter((reason) => !reason.startsWith("event-payload-ownership-proof-unavailable:"))

test("only a whole tree inactive for strictly more than seven days since its last activity is selected", () => {
  const db = fixture()
  try {
    const idleAt = 50 * DAY
    const boundary = idleAt + inactivityPolicy.windowMs
    session(db, "ses_idle", idleAt)
    event(db, "ses_idle")
    // Created long before the boundary but used recently: creation age alone would select it.
    session(db, "ses_reused", 10)
    event(db, "ses_reused")
    db.query("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)").run(
      "msg_recent",
      "ses_reused",
      10,
      boundary,
      JSON.stringify({ role: "user", time: { created: 10 } }),
    )
    // An old parent whose child was active recently is one tree, so the parent is protected too.
    session(db, "ses_parent", 10)
    session(db, "ses_child", 10, "ses_parent")
    db.query("INSERT INTO todo (session_id, position, content, time_created, time_updated) VALUES (?, 1, ?, ?, ?)").run(
      "ses_child",
      "child todo",
      10,
      boundary,
    )
    // Event appends carry no timestamp of their own; the writer record times them.
    session(db, "ses_appended", 10)
    event(db, "ses_appended")
    db.query(
      "INSERT INTO event_aggregate_writer (aggregate_id, writer_id, time_last_append) VALUES ('ses_appended', 'writer-gone', ?)",
    ).run(boundary)
    db.query(
      "INSERT INTO event_writer_process (writer_id, pid, boot_id, start_ticks, time_registered) VALUES ('writer-gone', 2147483646, 'boot', '1', 10)",
    ).run()
    const proof = (ids: string[]) => evidence(ids, Object.fromEntries(ids.map((id) => [id, ids.includes("ses_parent") ? -1 : 0])))
    const tree = (now: number, root: string, ids: string[]) =>
      inventory(access(db), proof(ids), now).trees.find((item) => item.rootSessionID === root)!

    expect(gateReasons(tree(boundary + 1, "ses_idle", ["ses_idle"]))).toEqual([])
    for (const now of [boundary, boundary - 1]) {
      expect(gateReasons(tree(now, "ses_idle", ["ses_idle"]))).toEqual(["session-tree-active-within-inactivity-window"])
    }
    const later = boundary + 1
    expect(gateReasons(tree(later, "ses_reused", ["ses_reused"]))).toEqual([
      "session-tree-active-within-inactivity-window",
    ])
    expect(gateReasons(tree(later, "ses_parent", ["ses_child", "ses_parent"]))).toEqual([
      "session-tree-active-within-inactivity-window",
    ])
    expect(gateReasons(tree(later, "ses_appended", ["ses_appended"]))).toEqual([
      "session-tree-active-within-inactivity-window",
    ])
    expect(gateReasons(tree(boundary + inactivityPolicy.windowMs + 1, "ses_reused", ["ses_reused"]))).toEqual([])

    const before = db.serialize()
    const reused = tree(later, "ses_reused", ["ses_reused"])
    const refused = apply(access(db), { tree: { ...reused, eligible: true, reasons: [] }, evidence: () => proof(["ses_reused"]), now: () => later })
    expect(refused).toMatchObject({ state: "refused", changedRows: 0 })
    expect(refused.reasons).toContain("session-tree-active-within-inactivity-window")
    expect(db.serialize().equals(before)).toBe(true)

    db.query("UPDATE message SET time_updated = 'yesterday' WHERE id = 'msg_recent'").run()
    expect(gateReasons(tree(boundary + inactivityPolicy.windowMs + 1, "ses_reused", ["ses_reused"]))).toEqual([
      "session-activity-unreadable:message.time_updated",
    ])
  } finally {
    db.close()
  }
})

test("activity arriving after inventory refuses the next batch before it writes", () => {
  const db = fixture()
  try {
    session(db, "ses_old", 10)
    event(db, "ses_old")
    addCopies(db, "ses_old")
    const proof = evidence(["ses_old"], { ses_old: 0 })
    const tree = inventory(access(db), proof, NOW).trees[0]!
    expect(tree.eligible).toBe(true)
    const first = apply(access(db), { tree, evidence: () => proof, now: () => NOW, batchSize: 1, maxBatches: 1 })
    expect(first).toMatchObject({ state: "in-progress", changedRows: 1 })
    db.query("UPDATE part SET time_updated = ? WHERE session_id = 'ses_old'").run(NOW - 1)
    const before = db.serialize()

    const result = apply(access(db), { tree, evidence: () => proof, now: () => NOW, batchSize: 1 })

    expect(result).toMatchObject({ state: "refused", changedRows: 0 })
    expect(result.reasons).toContain("session-tree-active-within-inactivity-window")
    expect(db.serialize().equals(before)).toBe(true)
  } finally {
    db.close()
  }
})

async function productionStore() {
  // A fresh private directory, like the production data directory, initialized by the production layer.
  const directory = await mkdtemp(path.join(os.tmpdir(), "opencode-retention-store-"))
  const filename = path.join(directory, "opencode.db")
  await Effect.runPromise(Effect.scoped(Layer.build(layerFromPath(filename))))
  return { filename, [Symbol.asyncDispose]: () => rm(directory, { recursive: true, force: true }) }
}

function productionSession(db: Database, id: string, time: number) {
  db.query(
    "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, 'proj', 'slug', '/work', 'held session sentinel', 'v1', ?, ?)",
  ).run(id, time, time)
}

async function holdSession(filename: string, sessionID: string, mode: "write" | "open") {
  const holder = Bun.spawn(
    [process.execPath, path.join(import.meta.dir, "../../fixture/retention-holder.ts"), filename, sessionID, mode],
    { stdin: "pipe", stdout: "pipe", stderr: "pipe", cwd: path.join(import.meta.dir, "../../..") },
  )
  const reader = holder.stdout.getReader()
  let output = ""
  while (!output.includes("ready")) {
    const chunk = await reader.read()
    if (chunk.done) throw new Error(`holder exited before ready: ${await new Response(holder.stderr).text()}`)
    output += new TextDecoder().decode(chunk.value)
  }
  reader.releaseLock()
  return {
    pid: holder.pid,
    release: async () => {
      holder.stdin.end()
      await holder.exited
    },
    [Symbol.asyncDispose]: async () => {
      holder.kill()
      await holder.exited
    },
  }
}

const eventRows = (db: Database, sessionID: string) =>
  db.query<{ data: string }, [string]>("SELECT data FROM event WHERE aggregate_id = ? ORDER BY seq").all(sessionID)

test(
  "a Session written by a live process in another OS process is never selected, and is released when it exits",
  async () => {
    await using store = await productionStore()
    const db = new Database(store.filename)
    try {
      productionSession(db, "ses_held", 10)
      await using holder = await holdSession(store.filename, "ses_held", "write")
      const proof = () => evidence(["ses_held"], { ses_held: 0 }, store.filename)
      expect(proof().holders).toEqual(expect.objectContaining({ state: "complete", pids: [holder.pid] }))

      const held = inventory(access(db), proof(), Date.now() + NOW).trees.find((item) => item.rootSessionID === "ses_held")!
      expect(held.eligible).toBe(false)
      expect(gateReasons(held)).toEqual(["session-held-by-live-process:ses_held"])
      const before = db.serialize()
      const refused = apply(access(db), {
        tree: { ...held, eligible: true, reasons: [] },
        evidence: proof,
        now: () => Date.now() + NOW,
      })
      expect(refused).toMatchObject({ state: "refused", changedRows: 0 })
      expect(refused.reasons).toContain("session-held-by-live-process:ses_held")
      expect(db.serialize().equals(before)).toBe(true)

      await holder.release()
      const released = inventory(access(db), proof(), Date.now() + NOW).trees.find(
        (item) => item.rootSessionID === "ses_held",
      )!
      expect(released.reasons).toEqual([])
    } finally {
      db.close()
    }
  },
  { timeout: 30_000 },
)

test(
  "an unregistered process holding the database refuses every tree until it exits",
  async () => {
    await using store = await productionStore()
    const db = new Database(store.filename)
    try {
      productionSession(db, "ses_quiet", 10)
      await using holder = await holdSession(store.filename, "ses_quiet", "open")
      const proof = () => evidence(["ses_quiet"], { ses_quiet: -1 }, store.filename)
      const refused = inventory(access(db), proof(), Date.now() + NOW)
      expect(gateReasons(refused.trees[0]!)).toEqual([`unregistered-database-holder:${holder.pid}`])
      await holder.release()
      expect(inventory(access(db), proof(), Date.now() + NOW).trees[0]?.reasons).toEqual([])
    } finally {
      db.close()
    }
  },
  { timeout: 30_000 },
)

test(
  "a holder or pending work arriving between inventory and a batch refuses it before any write",
  async () => {
    await using store = await productionStore()
    const db = new Database(store.filename)
    try {
      productionSession(db, "ses_late", 10)
      const proof = () => evidence(["ses_late"], { ses_late: -1 }, store.filename)
      const now = () => Date.now() + NOW
      const tree = inventory(access(db), proof(), now()).trees[0]!
      expect(tree.reasons).toEqual([])

      await using holder = await holdSession(store.filename, "ses_late", "write")
      const eventsBefore = eventRows(db, "ses_late")
      const before = db.serialize()
      const result = apply(access(db), { tree, evidence: proof, now })
      expect(result).toMatchObject({ state: "refused", changedRows: 0 })
      expect(result.reasons).toContain("session-held-by-live-process:ses_late")
      expect(result.reasons).toContain("event-appended-after-inventory:ses_late")
      expect(db.serialize().equals(before)).toBe(true)
      expect(eventRows(db, "ses_late")).toEqual(eventsBefore)
      await holder.release()

      const current = inventory(access(db), { ...proof(), handoff: { ...proof().handoff!, finalSequence: { ses_late: 0 } } }, now()).trees[0]!
      expect(current.reasons).toEqual([])
      db.query(
        "INSERT INTO session_prompt_queue (id, session_id, seq, delivery, input, time_created) VALUES ('queued', 'ses_late', 1, 'queue', '{}', 10)",
      ).run()
      const queuedBefore = db.serialize()
      const queued = apply(access(db), {
        tree: current,
        evidence: () => ({ ...proof(), handoff: { ...proof().handoff!, finalSequence: { ses_late: 0 } } }),
        now,
      })
      expect(queued).toMatchObject({ state: "refused", changedRows: 0 })
      expect(queued.reasons).toContain("v1-prompt-queue-row-present")
      expect(db.serialize().equals(queuedBefore)).toBe(true)
    } finally {
      db.close()
    }
  },
  { timeout: 30_000 },
)

test("a promoted V2 input row refuses its tree literally, including one promoted after inventory", () => {
  const db = fixture()
  try {
    session(db, "ses_prompted", 10)
    event(db, "ses_prompted")
    addCopies(db, "ses_prompted")
    const proof = evidence(["ses_prompted"], { ses_prompted: 0 })
    const tree = inventory(access(db), proof, NOW).trees[0]!
    expect(tree.eligible).toBe(true)
    db.query(
      "INSERT INTO session_input (id, session_id, promoted_seq, prompt) VALUES ('input_promoted', 'ses_prompted', 1, ?)",
    ).run(JSON.stringify({ text: "promoted prompt sentinel" }))
    expect(gateReasons(inventory(access(db), proof, NOW).trees[0]!)).toEqual(["v2-input-promoted-row-present"])
    const before = db.serialize()

    const result = apply(access(db), { tree, evidence: () => proof, now: () => NOW })

    expect(result).toMatchObject({ state: "refused", changedRows: 0 })
    expect(result.reasons).toEqual(["v2-input-promoted-row-present"])
    expect(db.serialize().equals(before)).toBe(true)
  } finally {
    db.close()
  }
})

test("gated apply redacts a production-shaped store only when every gate passes, and refuses before any write otherwise", async () => {
  await using tmp = await tmpdir()
  await chmod(tmp.path, 0o700)
  const filename = path.join(tmp.path, "opencode.db")
  const db = new Database(filename)
  try {
    createSchema(db)
    session(db, "ses_old", 10)
    event(db, "ses_old")
    addCopies(db, "ses_old")
    const valid = evidence(["ses_old"], { ses_old: 0 }, filename)
    const before = db.serialize()
    const failures: Array<[string, RetentionEvidence]> = [
      ["cross-process-liveness-proof-unavailable", { ...valid, holders: undefined }],
      ["database-holder-scan-unreadable", { ...valid, holders: { state: "unreadable", reason: "procfs gone" } }],
      ["measurement-handoff-receipt-unavailable", { ...valid, handoff: undefined }],
      ["customer-session-classification-unavailable", { ...valid, customerBinding: undefined }],
      ["reader-contract-unreviewed", { ...valid, readerContract: undefined }],
      ["evidence-unreadable", { ...valid, evidenceError: "receipt unreadable" }],
    ]
    for (const [reason, failed] of failures) {
      const result = applyGated(access(db), () => failed, () => NOW)
      expect(result).toMatchObject({ action: "refused", changedRows: 0, eligibleTrees: 0 })
      expect(result.reasons).toContain(reason)
      expect(result.reasons).toContain("production-apply-disabled")
      expect(db.serialize().equals(before)).toBe(true)
    }

    const applied = applyGated(access(db), () => valid, () => NOW)
    expect(applied).toMatchObject({ action: "applied", eligibleTrees: 1, completedSessionIDs: ["ses_old"], reasons: [] })
    expect(applied.changedRows).toBeGreaterThan(0)
    expect(db.query("SELECT state FROM event_retention WHERE aggregate_id = 'ses_old'").get()).toEqual({
      state: "complete",
    })
    expect(eventRows(db, "ses_old").map((row) => row.data).join("\n")).not.toContain("sentinel")
  } finally {
    db.close()
  }
})

test("a gate failing after committed batches reports partial progress and stops every further write", async () => {
  await using tmp = await tmpdir()
  await chmod(tmp.path, 0o700)
  const filename = path.join(tmp.path, "opencode.db")
  const db = new Database(filename)
  try {
    createSchema(db)
    for (const id of ["ses_a", "ses_b"]) {
      session(db, id, 10)
      event(db, id)
      addCopies(db, id)
    }
    // The customer classification and receipt cover the whole store here, as a store-wide producer
    // would; the holder scan is the real producer until it stops answering after two batches.
    let calls = 0
    const proof = (): RetentionEvidence => {
      calls += 1
      const store = evidence(["ses_a", "ses_b"], { ses_a: 0, ses_b: 0 }, filename)
      return calls <= 3 ? store : { ...store, holders: { state: "unreadable", reason: "scan stopped" } }
    }
    const sessionB = () =>
      [eventRows(db, "ses_b"), db.query("SELECT data FROM part WHERE session_id = 'ses_b'").all()].map((rows) =>
        JSON.stringify(rows),
      )
    const bBefore = sessionB()

    const result = applyGated(access(db), proof, () => NOW)

    expect(result.action).toBe("partial")
    expect(result.changedRows).toBeGreaterThan(0)
    expect(result.reasons).toContain("database-holder-scan-unreadable:scan stopped")
    expect(result.reasons).toContain("production-apply-disabled")
    expect(db.query("SELECT aggregate_id, state FROM event_retention").all()).toEqual([
      { aggregate_id: "ses_a", state: "redacting" },
    ])
    expect(sessionB()).toEqual(bBefore)
  } finally {
    db.close()
  }
})
