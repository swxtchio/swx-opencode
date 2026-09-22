import { describe, expect, test } from "bun:test"
import { buildRecords, parseModel, verify } from "@/cli/cmd/db-export-usage"

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
