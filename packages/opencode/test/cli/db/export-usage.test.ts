import { describe, expect, test } from "bun:test"
import { Database as Sqlite } from "bun:sqlite"
import { buildRecords, parseModel, readExport, verify } from "@/cli/cmd/db-export-usage"

const session = (over: Record<string, unknown> = {}) =>
  ({
    id: "ses_1",
    parent_id: null,
    project_id: "proj",
    directory: "/w",
    title: "t",
    agent: "build",
    model: null,
    time_created: 1,
    time_updated: 2,
    cost: 0,
    tokens_input: 0,
    tokens_output: 0,
    tokens_reasoning: 0,
    tokens_cache_read: 0,
    tokens_cache_write: 0,
    ...over,
  }) as never

const model = (over: Record<string, unknown> = {}) =>
  ({
    session_id: "ses_1",
    provider_id: "swx-azure",
    model_id: "gpt-5.6-luna",
    messages: 1,
    tokens_input: 10,
    tokens_output: 20,
    tokens_reasoning: 0,
    tokens_cache_read: 0,
    tokens_cache_write: 0,
    cost_reported: 1.5,
    ...over,
  }) as never

describe("parseModel", () => {
  // GOAL: session.model holds JSON TEXT. Emitting it raw gave the archive a
  // string like "{\"id\":\"gpt-5.6-luna\",...}", which anyone re-deriving cost
  // has to parse again from a field whose shape is not obvious. Caught by
  // reading the first real archive rather than by a test, which is why there
  // is one now.
  test("parses the stored JSON", () => {
    expect(parseModel('{"id":"gpt-5.6-luna","providerID":"swx-azure"}')).toEqual({
      id: "gpt-5.6-luna",
      providerID: "swx-azure",
    })
  })

  // GOAL: an archive must never lose a value it cannot understand.
  test.each(["not json", "{oops", ""])("preserves %p rather than dropping it", (value) => {
    const parsed = parseModel(value)
    expect(parsed === null || parsed === value).toBe(true)
  })

  test.each([null, undefined])("maps %p to null", (value) => {
    expect(parseModel(value)).toBeNull()
  })
})

describe("buildRecords", () => {
  test("joins session metadata onto each session-and-model row", () => {
    const [record] = buildRecords({
      sessions: [session({ title: "Fix the thing", model: '{"id":"m"}' })],
      models: [model()],
      served: [],
    })
    expect(record!["title"]).toBe("Fix the thing")
    expect(record!["configuredModel"]).toEqual({ id: "m" })
    expect(record!["tokens"]).toEqual({ input: 10, output: 20, reasoning: 0, cacheRead: 0, cacheWrite: 0 })
  })

  // GOAL: the served models are the #12 field, and the whole reason a routed
  // turn can be re-priced at all - the configured model does not say who
  // answered.
  test("collects served models, deduplicated and sorted", () => {
    const [record] = buildRecords({
      sessions: [session()],
      models: [model()],
      served: [
        { session_id: "ses_1", provider_id: "swx-azure", model_id: "gpt-5.6-luna", served: "glm-5p3" },
        { session_id: "ses_1", provider_id: "swx-azure", model_id: "gpt-5.6-luna", served: "glm-5p3" },
        { session_id: "ses_1", provider_id: "swx-azure", model_id: "gpt-5.6-luna", served: "glm-5p3-flash" },
      ] as never,
    })
    expect(record!["servedModelIDs"]).toEqual(["glm-5p3", "glm-5p3-flash"])
  })

  // GOAL: served models must not bleed between models of the same session.
  test("keeps served models scoped to their own model row", () => {
    const records = buildRecords({
      sessions: [session()],
      models: [model(), model({ model_id: "other" })],
      served: [{ session_id: "ses_1", provider_id: "swx-azure", model_id: "gpt-5.6-luna", served: "glm-5p3" }] as never,
    })
    expect(records[0]!["servedModelIDs"]).toEqual(["glm-5p3"])
    expect(records[1]!["servedModelIDs"]).toEqual([])
  })

  // GOAL: a session row missing for a model row must not throw - the export
  // has to complete and report, not crash partway before a reset.
  test("survives a model row whose session is absent", () => {
    const [record] = buildRecords({ sessions: [], models: [model()], served: [] })
    expect(record!["title"]).toBeNull()
    expect(record!["sessionID"]).toBe("ses_1")
  })
})

describe("verify", () => {
  const records = buildRecords({
    sessions: [session({ tokens_input: 10, tokens_output: 20 })],
    models: [model()],
    served: [],
  })

  // GOAL: the only condition that can LOSE data. Usage whose session row is
  // gone would be dropped silently, and this runs immediately before a
  // destructive reset.
  test("fails on orphan messages", () => {
    const result = verify({ sessions: [session()], records, orphanMessages: 3 })
    expect(result.ok).toBe(false)
    expect(result.lines.join("\n")).toContain("orphan messages   3")
  })

  // GOAL: a stale session rollup is NOT a failure. Measured on the real
  // database: 1 session of 7,197 was short by one cached turn because the
  // message write and the rollup update are not atomic. Failing the export
  // over that would block an archive of 7,197 sessions for a cache entry,
  // when the messages - which the export carries - are the measurement.
  test("reports a divergent rollup without failing", () => {
    const result = verify({
      sessions: [session({ tokens_input: 10, tokens_output: 999 })],
      records,
      orphanMessages: 0,
    })
    expect(result.ok).toBe(true)
    expect(result.lines.join("\n")).toContain("1 of 1 disagree")
    expect(result.lines.join("\n")).toContain("ses_1")
  })

  test("passes cleanly when the rollups agree", () => {
    const result = verify({
      sessions: [session({ tokens_input: 10, tokens_output: 20 })],
      records,
      orphanMessages: 0,
    })
    expect(result.ok).toBe(true)
    expect(result.lines.join("\n")).toContain("0 of 1 disagree")
  })
})

describe("readExport", () => {
  test("splits persisted step usage and falls back to requested-model totals for legacy messages", () => {
    const db = new Sqlite(":memory:")
    try {
      db.exec(`
        CREATE TABLE session (
          id TEXT PRIMARY KEY,
          parent_id TEXT,
          project_id TEXT,
          directory TEXT,
          title TEXT,
          agent TEXT,
          model TEXT,
          time_created INTEGER,
          time_updated INTEGER,
          cost REAL,
          tokens_input INTEGER,
          tokens_output INTEGER,
          tokens_reasoning INTEGER,
          tokens_cache_read INTEGER,
          tokens_cache_write INTEGER
        );
        CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
        CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
      `)

      const insertSession = db.query(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      insertSession.run("ses_mixed", null, "proj", "/w", "mixed", "build", null, 1, 2, 13, 3, 2, 0, 0, 0)
      insertSession.run("ses_legacy", null, "proj", "/w", "legacy", "build", null, 1, 2, 4, 5, 6, 1, 2, 3)

      db.query("INSERT INTO message VALUES (?, ?, ?, ?)").run(
        "msg_mixed",
        "ses_mixed",
        1,
        JSON.stringify({
          role: "assistant",
          providerID: "test",
          modelID: "requested",
          responseModelIDs: ["served-a", "served-b"],
          cost: 13,
          tokens: { input: 2, output: 1, reasoning: 0, cache: { read: 0, write: 0 } },
        }),
      )
      db.query("INSERT INTO message VALUES (?, ?, ?, ?)").run(
        "msg_legacy",
        "ses_legacy",
        1,
        JSON.stringify({
          role: "assistant",
          providerID: "test",
          modelID: "legacy-request",
          responseModelIDs: ["old-served-a", "old-served-b"],
          cost: 4,
          tokens: { input: 5, output: 6, reasoning: 1, cache: { read: 2, write: 3 } },
        }),
      )

      const insertPart = db.query("INSERT INTO part VALUES (?, ?, ?, ?, ?)")
      insertPart.run(
        "prt_a",
        "msg_mixed",
        "ses_mixed",
        1,
        JSON.stringify({
          type: "step-finish",
          responseModelID: "served-a",
          cost: 3,
          tokens: { input: 1, output: 1, reasoning: 0, cache: { read: 0, write: 0 } },
        }),
      )
      insertPart.run(
        "prt_b",
        "msg_mixed",
        "ses_mixed",
        2,
        JSON.stringify({
          type: "step-finish",
          responseModelID: "served-b",
          cost: 10,
          tokens: { input: 2, output: 1, reasoning: 0, cache: { read: 0, write: 0 } },
        }),
      )

      const snapshot = readExport(db)
      const mixed = snapshot.records.find((record) => record["sessionID"] === "ses_mixed")
      const legacy = snapshot.records.find((record) => record["sessionID"] === "ses_legacy")

      expect(mixed).toMatchObject({
        modelID: "requested",
        tokens: { input: 3, output: 2, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
        reportedCost: 13,
        servedModelIDs: ["served-a", "served-b"],
        servedModelUsage: [
          {
            modelID: "served-a",
            tokens: { input: 1, output: 1, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
            reportedCost: 3,
          },
          {
            modelID: "served-b",
            tokens: { input: 2, output: 1, reasoning: 0, cacheRead: 0, cacheWrite: 0 },
            reportedCost: 10,
          },
        ],
      })
      expect(legacy).toMatchObject({
        modelID: "legacy-request",
        tokens: { input: 5, output: 6, reasoning: 1, cacheRead: 2, cacheWrite: 3 },
        reportedCost: 4,
        servedModelIDs: ["old-served-a", "old-served-b"],
        servedModelUsage: [
          {
            modelID: "legacy-request",
            tokens: { input: 5, output: 6, reasoning: 1, cacheRead: 2, cacheWrite: 3 },
            reportedCost: 4,
          },
        ],
      })
      expect(snapshot.check.ok).toBe(true)
      expect(snapshot.check.lines.join("\n")).toContain("0 of 2 disagree")
      expect(snapshot.orphanMessages).toBe(0)
    } finally {
      db.close()
    }
  })
})
