import { expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { mkdtemp, rm, stat, statfs, symlink } from "node:fs/promises"
import path from "node:path"
import {
  apply,
  compactRestoreFixture,
  createFixtureDatabase,
  inventory,
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
  db.query("INSERT INTO session_input (id, session_id, promoted_seq, prompt) VALUES (?, ?, 1, ?)").run(
    `input_${sessionID}`,
    sessionID,
    JSON.stringify({ text: "input sentinel" }),
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
    const unknownCopy = inventory(access(db), proof, 1_000)
    expect(unknownCopy.refusals).toContain("unclassified-session-owned-table:future_session_copy")
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
    expect(resumed.query("SELECT prompt FROM session_input WHERE id = 'input_ses_old'").get()).toEqual({ prompt: "{}" })
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

test(
  "fixture compaction backs up, restores, and returns allocated bytes on a distinct filesystem",
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
      pageCount: beforeDB.query<{ page_count: number }, []>("PRAGMA page_count").get()!.page_count,
      freelistCount: beforeDB.query<{ freelist_count: number }, []>("PRAGMA freelist_count").get()!.freelist_count,
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
    redactionDB.close()

    const afterRedactionStat = await stat(sourcePath)
    const afterRedactionDB = new Database(sourcePath, { readonly: true })
    const afterRedaction = {
      pageCount: afterRedactionDB.query<{ page_count: number }, []>("PRAGMA page_count").get()!.page_count,
      freelistCount: afterRedactionDB.query<{ freelist_count: number }, []>("PRAGMA freelist_count").get()!
        .freelist_count,
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

    const compacted = await compactRestoreFixture({
      sourcePath,
      backupPath,
      stagingPath,
      expectedSourceDeviceID: sourceDeviceID,
      expectedBackupDeviceID: destinationDeviceID,
      expectedStagingDeviceID: destinationDeviceID,
      retainedSessionID: "ses_physical",
    })
    expect(compacted.reasons).toEqual([])
    expect(compacted.state).toBe("complete")
    expect(compacted.changedFiles).toBe(4)
    expect(compacted.measurements?.backup?.integrityCheck).toBe("ok")
    expect(compacted.measurements?.staging?.integrityCheck).toBe("ok")
    expect(compacted.measurements?.restored?.integrityCheck).toBe("ok")
    expect(compacted.measurements?.staging?.autoVacuum).toBe(2)
    expect(compacted.measurements?.restored?.size).toBeLessThan(beforeRedactionStat.size)
    expect(compacted.measurements?.restored?.allocatedBytes).toBeLessThan(beforeRedactionStat.blocks * 512)
    expect(compacted.measurements?.restored?.pageCount).toBeLessThan(beforeRedaction.pageCount)
    expect(compacted.measurements?.restored?.freelistCount).toBe(0)
    expect(compacted.measurements?.restored?.retainedSession).toEqual(compacted.measurements?.backup?.retainedSession)
  },
  { timeout: 60_000 },
)
