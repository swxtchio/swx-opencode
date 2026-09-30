import { describe, expect, test } from "bun:test"
import { asc, eq, sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import sessionMessageAdmissionOrderMigration from "@opencode-ai/core/database/migration/20260929045002_session-message-admission-order"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { MessageTable, SessionTable } from "@opencode-ai/core/session/sql"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Effect, Option } from "effect"
import path from "path"
import { Session as SessionNs } from "@/session/session"
import { MessageV2 } from "../../src/session/message-v2"
import { MessageID, PartID, SessionID } from "../../src/session/schema"

import { NotFoundError } from "@/storage/storage"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import type { Provider } from "@/provider/provider"
import { tmpdir } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(LayerNode.compile(LayerNode.group([SessionNs.node, MessageV2.node, SessionProjector.node])))

const model: Provider.Model = {
  id: ModelV2.ID.make("test-model"),
  providerID: ProviderV2.ID.make("test"),
  api: {
    id: "test-model",
    url: "https://example.com",
    npm: "@ai-sdk/openai",
  },
  name: "Test Model",
  capabilities: {
    temperature: true,
    reasoning: false,
    attachment: false,
    toolcall: true,
    input: {
      text: true,
      audio: false,
      image: false,
      video: false,
      pdf: false,
    },
    output: {
      text: true,
      audio: false,
      image: false,
      video: false,
      pdf: false,
    },
    interleaved: false,
  },
  cost: {
    input: 0,
    output: 0,
    cache: {
      read: 0,
      write: 0,
    },
  },
  limit: {
    context: 0,
    input: 0,
    output: 0,
  },
  status: "active",
  options: {},
  headers: {},
  release_date: "2026-01-01",
}

const withSession = <A, E, R>(
  fn: (input: { session: SessionNs.Interface; sessionID: SessionID }) => Effect.Effect<A, E, R>,
) =>
  Effect.acquireUseRelease(
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const created = yield* session.create({})
      return { session, sessionID: created.id }
    }),
    fn,
    (input) => input.session.remove(input.sessionID).pipe(Effect.ignore),
  )

// Helper functions using Effect.gen
const fill = Effect.fn("Test.fill")(function* (
  sessionID: SessionID,
  count: number,
  time = (i: number) => Date.now() + i,
) {
  const session = yield* SessionNs.Service
  const ids = [] as MessageID[]
  for (let i = 0; i < count; i++) {
    const id = MessageID.ascending()
    ids.push(id)
    yield* session.updateMessage({
      id,
      sessionID,
      role: "user",
      time: { created: time(i) },
      agent: "test",
      model: { providerID: "test", modelID: "test" },
      tools: {},
      mode: "",
    } as unknown as SessionV1.Info)
    yield* session.updatePart({
      id: PartID.ascending(),
      sessionID,
      messageID: id,
      type: "text",
      text: `m${i}`,
    })
  }
  return ids
})

const addUser = Effect.fn("Test.addUser")(function* (sessionID: SessionID, text?: string) {
  const session = yield* SessionNs.Service
  const id = MessageID.ascending()
  yield* session.updateMessage({
    id,
    sessionID,
    role: "user",
    time: { created: Date.now() },
    agent: "test",
    model: { providerID: "test", modelID: "test" },
    tools: {},
    mode: "",
  } as unknown as SessionV1.Info)
  if (text) {
    yield* session.updatePart({
      id: PartID.ascending(),
      sessionID,
      messageID: id,
      type: "text",
      text,
    })
  }
  return id
})

const addAssistant = Effect.fn("Test.addAssistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  opts?: { summary?: boolean; finish?: string; error?: SessionV1.Assistant["error"] },
) {
  const session = yield* SessionNs.Service
  const id = MessageID.ascending()
  yield* session.updateMessage({
    id,
    sessionID,
    role: "assistant",
    time: { created: Date.now() },
    parentID,
    modelID: ModelV2.ID.make("test"),
    providerID: ProviderV2.ID.make("test"),
    mode: "",
    agent: "default",
    path: { cwd: "/", root: "/" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    summary: opts?.summary,
    finish: opts?.finish,
    error: opts?.error,
  } as unknown as SessionV1.Info)
  return id
})

const addCompactionPart = Effect.fn("Test.addCompactionPart")(function* (
  sessionID: SessionID,
  messageID: MessageID,
  tailStartID?: MessageID,
) {
  const session = yield* SessionNs.Service
  yield* session.updatePart({
    id: PartID.ascending(),
    sessionID,
    messageID,
    type: "compaction",
    auto: true,
    tail_start_id: tailStartID,
    } as any)
})

describe("mixed-version message admission migration", () => {
  test("reproduces the previous full-index failure for repeated legacy inserts", async () => {
    const sqlite = await import("bun:sqlite")
    const previous = new sqlite.Database(":memory:")
    try {
      previous.exec(`
        CREATE TABLE message (
          id TEXT PRIMARY KEY,
          session_id TEXT NOT NULL,
          admission_seq INTEGER NOT NULL DEFAULT 0
        );
        CREATE UNIQUE INDEX message_session_admission_seq_idx ON message (session_id, admission_seq);
        INSERT INTO message (id, session_id, admission_seq) VALUES ('msg_existing', 'ses_legacy', 1);
      `)
      const oldInsert = previous.query("INSERT INTO message (id, session_id) VALUES (?, ?)")
      oldInsert.run("msg_legacy_one", "ses_legacy")
      expect(() => oldInsert.run("msg_legacy_two", "ses_legacy")).toThrow(
        /UNIQUE constraint failed: message\.session_id, message\.admission_seq/,
      )
    } finally {
      previous.close()
    }
  })

  test("keeps legacy and current writers ordered and model-visible through migration", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "mixed-version.sqlite")
    const sqlite = await import("bun:sqlite")
    const legacy = new sqlite.Database(filename)
    const layer = AppNodeBuilder.build(
      LayerNode.group([EventV2.node, SessionProjector.node, MessageV2.node]),
      [[Database.node, Database.layerFromPath(filename)]],
    )

    try {
      await Effect.runPromise(
        Effect.gen(function* () {
          const { db } = yield* Database.Service
          const events = yield* EventV2.Service
          const sessionID = SessionID.make("ses_mixed_version")
          const projectID = ProjectV2.ID.global

          yield* db
            .insert(ProjectTable)
            .values({ id: projectID, worktree: AbsolutePath.make(tmp.path), sandboxes: [] })
            .run()
          yield* db
            .insert(SessionTable)
            .values({
              id: sessionID,
              project_id: projectID,
              slug: "mixed-version",
              directory: AbsolutePath.make(tmp.path),
              title: "Mixed-version migration test",
              version: "test",
            })
            .run()

          yield* db.run(sql`DROP TRIGGER IF EXISTS message_admission_seq_legacy_insert`)
          yield* db.run(sql`DROP INDEX IF EXISTS message_session_admission_seq_idx`)
          yield* db.run(sql`ALTER TABLE message DROP COLUMN admission_seq`)
          yield* db.run(sql`DELETE FROM migration WHERE id = ${sessionMessageAdmissionOrderMigration.id}`)

          const legacySessionID = sessionID as unknown as SessionV1.User["sessionID"]
          const oldMessageInsert = legacy.query(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
          )
          const oldPartInsert = legacy.query(
            "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
          )
          const oldReader = legacy.query(
            "SELECT id, session_id, time_created, time_updated, data FROM message WHERE session_id = ? ORDER BY time_created, id",
          )
          const writeLegacy = (id: string, partID: string, created: number, text: string) => {
            oldMessageInsert.run(
              id,
              legacySessionID,
              created,
              created,
              JSON.stringify({
                role: "user",
                time: { created },
                agent: "legacy",
                model: { providerID: "test", modelID: "test-model" },
                tools: {},
                mode: "",
              }),
            )
            oldPartInsert.run(
              partID,
              id,
              legacySessionID,
              created,
              created,
              JSON.stringify({ type: "text", text }),
            )
          }
          const writeCurrent = (id: string, partID: string, created: number, text: string) => {
            const info = {
              id: SessionV1.MessageID.make(id),
              sessionID: legacySessionID,
              role: "user",
              time: { created },
              agent: "current",
              model: { providerID: "test", modelID: "test-model" },
              tools: {},
              mode: "",
            } as unknown as SessionV1.User
            return Effect.gen(function* () {
              yield* events.publish(SessionV1.Event.MessageUpdated, { sessionID: legacySessionID, info })
              yield* events.publish(SessionV1.Event.PartUpdated, {
                sessionID: legacySessionID,
                part: {
                  id: SessionV1.PartID.make(partID),
                  sessionID: legacySessionID,
                  messageID: info.id,
                  type: "text",
                  text,
                } as unknown as SessionV1.Part,
                time: created,
              })
            })
          }

          yield* Effect.sync(() => writeLegacy("msg_legacy_before", "prt_legacy_before", 900, "legacy before migration"))

          yield* DatabaseMigration.applyOnly(db, [sessionMessageAdmissionOrderMigration])

          yield* Effect.sync(() => writeLegacy("msg_legacy_one", "prt_legacy_one", 700, "legacy after migration one"))
          yield* writeCurrent("msg_current_one", "prt_current_one", 500, "current after legacy one")
          yield* Effect.sync(() => writeLegacy("msg_legacy_two", "prt_legacy_two", 300, "legacy after migration two"))
          yield* writeCurrent("msg_current_two", "prt_current_two", 100, "current after legacy two")

          const expectedIDs = [
            MessageID.make("msg_legacy_before"),
            MessageID.make("msg_legacy_one"),
            MessageID.make("msg_current_one"),
            MessageID.make("msg_legacy_two"),
            MessageID.make("msg_current_two"),
          ]
          const ordered = yield* db
            .select({ id: MessageTable.id, admission_seq: MessageTable.admission_seq })
            .from(MessageTable)
            .where(eq(MessageTable.session_id, sessionID))
            .orderBy(asc(MessageTable.admission_seq))
            .all()
          expect(ordered.map((row) => row.id)).toEqual(expectedIDs)
          expect(ordered.map((row) => row.admission_seq)).toEqual([1, 2, 3, 4, 5])
          expect(oldReader.all(legacySessionID)).toHaveLength(expectedIDs.length)

          const latest = yield* MessageV2.page({ sessionID, limit: 3 })
          expect(latest.items.map((item) => item.info.id)).toEqual(expectedIDs.slice(2))
          expect(latest.more).toBe(true)
          if (!latest.cursor) throw new Error("expected a cursor for the earlier mixed-version messages")
          const earlier = yield* MessageV2.page({ sessionID, limit: 3, before: latest.cursor })
          expect(earlier.items.map((item) => item.info.id)).toEqual(expectedIDs.slice(0, 2))
          expect(earlier.more).toBe(false)

          const modelMessages = yield* MessageV2.toModelMessagesEffect([...earlier.items, ...latest.items], model)
          const serialized = JSON.stringify(modelMessages)
          const visibleText = [
            "legacy before migration",
            "legacy after migration one",
            "current after legacy one",
            "legacy after migration two",
            "current after legacy two",
          ]
          const positions = visibleText.map((text) => serialized.indexOf(text))
          expect(positions.every((position) => position >= 0)).toBe(true)
          expect(positions).toEqual([...positions].sort((a, b) => a - b))
        }).pipe(Effect.scoped, Effect.provide(layer)),
      )
    } finally {
      legacy.close()
    }
  })
})

describe("MessageV2.page", () => {
  it.instance("returns page result", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        yield* fill(sessionID, 2)

        const result = yield* MessageV2.page({ sessionID, limit: 10 })
        expect(result).toBeDefined()
        expect(result.items).toBeArray()
      }),
    ),
  )

  it.instance("pages backward with opaque cursors", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 6)

        const a = yield* MessageV2.page({ sessionID, limit: 2 })
        expect(a.items.map((item) => item.info.id)).toEqual(ids.slice(-2))
        expect(a.items.every((item) => item.parts.length === 1)).toBe(true)
        expect(a.more).toBe(true)
        expect(a.cursor).toBeTruthy()

        const b = yield* MessageV2.page({ sessionID, limit: 2, before: a.cursor! })
        expect(b.items.map((item) => item.info.id)).toEqual(ids.slice(-4, -2))
        expect(b.more).toBe(true)
        expect(b.cursor).toBeTruthy()

        const c = yield* MessageV2.page({ sessionID, limit: 2, before: b.cursor! })
        expect(c.items.map((item) => item.info.id)).toEqual(ids.slice(0, 2))
        expect(c.more).toBe(false)
        expect(c.cursor).toBeUndefined()
      }),
    ),
  )

  it.instance("keeps legacy time cursor bounds after deleting the anchor message", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const times = [10, 60, 20, 50, 30, 40, 70]
        const ids = yield* fill(sessionID, times.length, (index: number) => times[index] ?? 0)
        const anchor = ids[3]
        const expectedMiddle = ids.slice(4, 6)
        const expectedOldest = [ids[0], ids[2]]
        if (!anchor || !expectedMiddle[0] || !expectedMiddle[1] || !expectedOldest[0] || !expectedOldest[1])
          throw new Error("expected pagination fixture messages")
        const legacyCursor = MessageV2.cursor.encode({ id: anchor, time: times[3] ?? 0 })

        yield* session.removeMessage({ sessionID, messageID: anchor })

        const middle = yield* MessageV2.page({ sessionID, limit: 2, before: legacyCursor })
        expect(middle.items.map((item) => item.info.id)).toEqual(expectedMiddle)
        expect(middle.more).toBe(true)
        if (!middle.cursor) throw new Error("expected a cursor for the remaining legacy page")
        expect(MessageV2.cursor.decode(middle.cursor)).toEqual({ id: expectedMiddle[0], time: times[4] })

        const oldest = yield* MessageV2.page({ sessionID, limit: 2, before: middle.cursor })
        expect(oldest.items.map((item) => item.info.id)).toEqual(expectedOldest)
        expect(oldest.more).toBe(false)
        expect(oldest.cursor).toBeUndefined()
      }),
    ),
  )

  it.instance("returns items in chronological order within a page", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 4)

        const result = yield* MessageV2.page({ sessionID, limit: 4 })
        expect(result.items.map((item) => item.info.id)).toEqual(ids)
      }),
    ),
  )

  it.instance("returns empty items for session with no messages", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const result = yield* MessageV2.page({ sessionID, limit: 10 })
        expect(result.items).toEqual([])
        expect(result.more).toBe(false)
        expect(result.cursor).toBeUndefined()
      }),
    ),
  )

  it.instance("fails with NotFoundError for non-existent session", () =>
    Effect.gen(function* () {
      const fake = "non-existent-session" as SessionID
      const error = yield* Effect.flip(MessageV2.page({ sessionID: fake, limit: 10 }))
      expect(error).toBeInstanceOf(NotFoundError)
      expect(error.message).toBe(`Session not found: ${fake}`)
    }),
  )

  it.instance("handles exact limit boundary", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 3)

        const result = yield* MessageV2.page({ sessionID, limit: 3 })
        expect(result.items.map((item) => item.info.id)).toEqual(ids)
        expect(result.more).toBe(false)
        expect(result.cursor).toBeUndefined()
      }),
    ),
  )

  it.instance("limit of 1 returns single newest message", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 5)

        const result = yield* MessageV2.page({ sessionID, limit: 1 })
        expect(result.items).toHaveLength(1)
        expect(result.items[0].info.id).toBe(ids[ids.length - 1])
        expect(result.more).toBe(true)
      }),
    ),
  )

  it.instance("hydrates multiple parts per message", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const [id] = yield* fill(sessionID, 1)

        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: id,
          type: "text",
          text: "extra",
        })

        const result = yield* MessageV2.page({ sessionID, limit: 10 })
        expect(result.items).toHaveLength(1)
        expect(result.items[0].parts).toHaveLength(2)
      }),
    ),
  )

  it.instance("accepts cursors from fractional timestamps", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 4, (i: number) => 1000.5 + i)

        const a = yield* MessageV2.page({ sessionID, limit: 2 })
        const b = yield* MessageV2.page({ sessionID, limit: 2, before: a.cursor! })

        expect(a.items.map((item) => item.info.id)).toEqual(ids.slice(-2))
        expect(b.items.map((item) => item.info.id)).toEqual(ids.slice(0, 2))
      }),
    ),
  )

  it.instance("messages with same timestamp are ordered by id", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 4, () => 1000)

        const a = yield* MessageV2.page({ sessionID, limit: 2 })
        expect(a.items.map((item) => item.info.id)).toEqual(ids.slice(-2))
        expect(a.more).toBe(true)

        const b = yield* MessageV2.page({ sessionID, limit: 2, before: a.cursor! })
        expect(b.items.map((item) => item.info.id)).toEqual(ids.slice(0, 2))
        expect(b.more).toBe(false)
      }),
    ),
  )

  it.instance("does not return messages from other sessions", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const a = yield* session.create({})
      const b = yield* session.create({})
      yield* fill(a.id, 3)
      yield* fill(b.id, 2)

      const resultA = yield* MessageV2.page({ sessionID: a.id, limit: 10 })
      const resultB = yield* MessageV2.page({ sessionID: b.id, limit: 10 })
      expect(resultA.items).toHaveLength(3)
      expect(resultB.items).toHaveLength(2)
      expect(resultA.items.every((item) => item.info.sessionID === a.id)).toBe(true)
      expect(resultB.items.every((item) => item.info.sessionID === b.id)).toBe(true)

      yield* session.remove(a.id)
      yield* session.remove(b.id)
    }),
  )

  it.instance("large limit returns all messages without cursor", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 10)

        const result = yield* MessageV2.page({ sessionID, limit: 100 })
        expect(result.items).toHaveLength(10)
        expect(result.items.map((item) => item.info.id)).toEqual(ids)
        expect(result.more).toBe(false)
        expect(result.cursor).toBeUndefined()
      }),
    ),
  )
})

describe("MessageV2.stream", () => {
  it.instance("yields items newest first", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 5)

        const items = yield* MessageV2.stream(sessionID)
        expect(items.map((item) => item.info.id)).toEqual(ids.slice().reverse())
      }),
    ),
  )

  it.instance("yields nothing for empty session", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const items = yield* MessageV2.stream(sessionID)
        expect(items).toHaveLength(0)
      }),
    ),
  )

  it.instance("yields single message", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 1)

        const items = yield* MessageV2.stream(sessionID)
        expect(items).toHaveLength(1)
        expect(items[0].info.id).toBe(ids[0])
      }),
    ),
  )

  it.instance("hydrates parts for each yielded message", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        yield* fill(sessionID, 3)

        const items = yield* MessageV2.stream(sessionID)
        for (const item of items) {
          expect(item.parts).toHaveLength(1)
          expect(item.parts[0].type).toBe("text")
        }
      }),
    ),
  )

  it.instance("handles sets exceeding internal page size", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 60)

        const items = yield* MessageV2.stream(sessionID)
        expect(items).toHaveLength(60)
        expect(items[0].info.id).toBe(ids[ids.length - 1])
        expect(items[59].info.id).toBe(ids[0])
      }),
    ),
  )

  it.instance("returns an Effect", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        yield* fill(sessionID, 1)

        const result = yield* MessageV2.stream(sessionID)
        expect(result).toHaveLength(1)
      }),
    ),
  )
})

describe("MessageV2.parts", () => {
  it.instance("returns parts for a message", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const [id] = yield* fill(sessionID, 1)

        const result = yield* MessageV2.parts(id)
        expect(result).toHaveLength(1)
        expect(result[0].type).toBe("text")
        expect((result[0] as SessionV1.TextPart).text).toBe("m0")
      }),
    ),
  )

  it.instance("returns empty array for message with no parts", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const id = yield* addUser(sessionID)

        const result = yield* MessageV2.parts(id)
        expect(result).toEqual([])
      }),
    ),
  )

  it.instance("returns multiple parts in order", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const [id] = yield* fill(sessionID, 1)

        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: id,
          type: "text",
          text: "second",
        })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: id,
          type: "text",
          text: "third",
        })

        const result = yield* MessageV2.parts(id)
        expect(result).toHaveLength(3)
        expect((result[0] as SessionV1.TextPart).text).toBe("m0")
        expect((result[1] as SessionV1.TextPart).text).toBe("second")
        expect((result[2] as SessionV1.TextPart).text).toBe("third")
      }),
    ),
  )

  it.instance("returns empty for non-existent message id", () =>
    Effect.gen(function* () {
      yield* SessionNs.Service
      const result = yield* MessageV2.parts(MessageID.ascending())
      expect(result).toEqual([])
    }),
  )

  it.instance("parts contain sessionID and messageID", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const [id] = yield* fill(sessionID, 1)

        const result = yield* MessageV2.parts(id)
        expect(result[0].sessionID).toBe(sessionID)
        expect(result[0].messageID).toBe(id)
      }),
    ),
  )
})

describe("MessageV2.get", () => {
  it.instance("returns message with hydrated parts", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const [id] = yield* fill(sessionID, 1)

        const result = yield* MessageV2.get({ sessionID, messageID: id })
        expect(result.info.id).toBe(id)
        expect(result.info.sessionID).toBe(sessionID)
        expect(result.info.role).toBe("user")
        expect(result.parts).toHaveLength(1)
        expect((result.parts[0] as SessionV1.TextPart).text).toBe("m0")
      }),
    ),
  )

  it.instance("fails with NotFoundError for non-existent message", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const messageID = MessageID.ascending()
        const error = yield* Effect.flip(MessageV2.get({ sessionID, messageID }))
        expect(error).toBeInstanceOf(NotFoundError)
        expect(error.message).toBe(`Message not found: ${messageID}`)
      }),
    ),
  )

  it.instance("scopes by session id", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const a = yield* session.create({})
      const b = yield* session.create({})
      const [id] = yield* fill(a.id, 1)

      const error = yield* Effect.flip(MessageV2.get({ sessionID: b.id, messageID: id }))
      expect(error).toBeInstanceOf(NotFoundError)
      expect(error.message).toBe(`Message not found: ${id}`)
      const result = yield* MessageV2.get({ sessionID: a.id, messageID: id })
      expect(result.info.id).toBe(id)

      yield* session.remove(a.id)
      yield* session.remove(b.id)
    }),
  )

  it.instance("returns message with multiple parts", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const [id] = yield* fill(sessionID, 1)

        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: id,
          type: "text",
          text: "extra",
        })

        const result = yield* MessageV2.get({ sessionID, messageID: id })
        expect(result.parts).toHaveLength(2)
      }),
    ),
  )

  it.instance("returns assistant message with correct role", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const uid = yield* addUser(sessionID, "hello")
        const aid = yield* addAssistant(sessionID, uid)

        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: aid,
          type: "text",
          text: "response",
        })

        const result = yield* MessageV2.get({ sessionID, messageID: aid })
        expect(result.info.role).toBe("assistant")
        expect(result.parts).toHaveLength(1)
        expect((result.parts[0] as SessionV1.TextPart).text).toBe("response")
      }),
    ),
  )

  it.instance("returns message with zero parts", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const id = yield* addUser(sessionID)

        const result = yield* MessageV2.get({ sessionID, messageID: id })
        expect(result.info.id).toBe(id)
        expect(result.parts).toEqual([])
      }),
    ),
  )
})

describe("Session.messages", () => {
  it.instance("returns all messages in chronological order across pages", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 55)
        const result = yield* session.messages({ sessionID })
        expect(result.map((item) => item.info.id)).toEqual(ids)
      }),
    ),
  )

  it.instance("fails with NotFoundError for non-existent session", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const fake = "non-existent-session" as SessionID
      const error = yield* Effect.flip(session.messages({ sessionID: fake }))
      expect(error).toBeInstanceOf(NotFoundError)
      expect(error.message).toBe(`Session not found: ${fake}`)
    }),
  )
})

describe("Session.findMessage", () => {
  it.instance("searches newest-first", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 3)
        const result = yield* session.findMessage(sessionID, () => true)
        expect(Option.isSome(result) ? result.value.info.id : undefined).toBe(ids.at(-1))
      }),
    ),
  )

  it.instance("fails with NotFoundError for non-existent session", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const fake = "non-existent-session" as SessionID
      const error = yield* Effect.flip(session.findMessage(fake, () => true))
      expect(error).toBeInstanceOf(NotFoundError)
      expect(error.message).toBe(`Session not found: ${fake}`)
    }),
  )
})

describe("MessageV2.filterCompacted", () => {
  it.instance("returns all messages when no compaction", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const ids = yield* fill(sessionID, 5)

        const result = MessageV2.filterCompacted(yield* MessageV2.stream(sessionID))
        expect(result).toHaveLength(5)
        // reversed from newest-first to chronological
        expect(result.map((item) => item.info.id)).toEqual(ids)
      }),
    ),
  )

  it.instance("stops at compaction boundary and returns chronological order", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        // Chronological: u1(+compaction part), a1(summary, parentID=u1), u2, a2
        // Stream (newest first): a2, u2, a1(adds u1 to completed), u1(in completed + compaction) -> break
        const u1 = yield* addUser(sessionID, "first question")
        const a1 = yield* addAssistant(sessionID, u1, { summary: true, finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a1,
          type: "text",
          text: "summary",
        })
        yield* addCompactionPart(sessionID, u1)

        const u2 = yield* addUser(sessionID, "new question")
        const a2 = yield* addAssistant(sessionID, u2)
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a2,
          type: "text",
          text: "new response",
        })

        const result = MessageV2.filterCompacted(yield* MessageV2.stream(sessionID))
        // Includes compaction boundary: u1, a1, u2, a2
        expect(result[0].info.id).toBe(u1)
        expect(result.length).toBe(4)
      }),
    ),
  )

  it.live("handles empty iterable", () =>
    Effect.sync(() => {
      const result = MessageV2.filterCompacted([])
      expect(result).toEqual([])
    }),
  )

  it.instance("does not break on compaction part without matching summary", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const u1 = yield* addUser(sessionID, "hello")
        yield* addCompactionPart(sessionID, u1)
        yield* addUser(sessionID, "world")

        const result = MessageV2.filterCompacted(yield* MessageV2.stream(sessionID))
        expect(result).toHaveLength(2)
      }),
    ),
  )

  it.instance("skips assistant with error even if marked as summary", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const u1 = yield* addUser(sessionID, "hello")
        yield* addCompactionPart(sessionID, u1)

        const error = new SessionV1.APIError({
          message: "boom",
          isRetryable: true,
        }).toObject() as SessionV1.Assistant["error"]
        yield* addAssistant(sessionID, u1, { summary: true, finish: "end_turn", error })
        yield* addUser(sessionID, "retry")

        const result = MessageV2.filterCompacted(yield* MessageV2.stream(sessionID))
        // Error assistant doesn't add to completed, so compaction boundary never triggers
        expect(result).toHaveLength(3)
      }),
    ),
  )

  it.instance("skips assistant without finish even if marked as summary", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const u1 = yield* addUser(sessionID, "hello")
        yield* addCompactionPart(sessionID, u1)

        // summary=true but no finish
        yield* addAssistant(sessionID, u1, { summary: true })
        yield* addUser(sessionID, "next")

        const result = MessageV2.filterCompacted(yield* MessageV2.stream(sessionID))
        expect(result).toHaveLength(3)
      }),
    ),
  )

  it.instance("retains original tail when compaction stores tail_start_id", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const u1 = yield* addUser(sessionID, "first")
        const a1 = yield* addAssistant(sessionID, u1, { finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a1,
          type: "text",
          text: "first reply",
        })

        const u2 = yield* addUser(sessionID, "second")
        const a2 = yield* addAssistant(sessionID, u2, { finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a2,
          type: "text",
          text: "second reply",
        })

        const c1 = yield* addUser(sessionID)
        yield* addCompactionPart(sessionID, c1, u2)
        const s1 = yield* addAssistant(sessionID, c1, { summary: true, finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: s1,
          type: "text",
          text: "summary",
        })

        const u3 = yield* addUser(sessionID, "third")
        const a3 = yield* addAssistant(sessionID, u3, { finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a3,
          type: "text",
          text: "third reply",
        })

        const result = MessageV2.filterCompacted(yield* MessageV2.stream(sessionID))

        expect(result.map((item) => item.info.id)).toEqual([c1, s1, u2, a2, u3, a3])
      }),
    ),
  )

  it.instance("fork remaps compaction tail_start_id for filterCompacted", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const created = yield* session.create({})

      const u1 = yield* addUser(created.id, "first")
      const a1 = yield* addAssistant(created.id, u1, { finish: "end_turn" })
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: created.id,
        messageID: a1,
        type: "text",
        text: "first reply",
      })

      const u2 = yield* addUser(created.id, "second")
      const a2 = yield* addAssistant(created.id, u2, { finish: "end_turn" })
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: created.id,
        messageID: a2,
        type: "text",
        text: "second reply",
      })

      const c1 = yield* addUser(created.id)
      yield* addCompactionPart(created.id, c1, u2)
      const s1 = yield* addAssistant(created.id, c1, { summary: true, finish: "end_turn" })
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: created.id,
        messageID: s1,
        type: "text",
        text: "summary",
      })

      const u3 = yield* addUser(created.id, "third")
      const a3 = yield* addAssistant(created.id, u3, { finish: "end_turn" })
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: created.id,
        messageID: a3,
        type: "text",
        text: "third reply",
      })

      const parentFiltered = MessageV2.filterCompacted(yield* MessageV2.stream(created.id))
      expect(parentFiltered.map((item) => item.info.id)).toEqual([c1, s1, u2, a2, u3, a3])

      const forked = yield* session.fork({ sessionID: created.id })
      const childFiltered = MessageV2.filterCompacted(yield* MessageV2.stream(forked.id))
      expect(childFiltered).toHaveLength(parentFiltered.length)

      const tailPart = childFiltered.flatMap((m) => m.parts).find((p) => p.type === "compaction")
      expect(tailPart?.type).toBe("compaction")
      if (!tailPart || tailPart.type !== "compaction") throw new Error("Expected forked compaction part")
      expect(tailPart.tail_start_id).toBeDefined()
      expect(childFiltered.some((m) => m.info.id === tailPart.tail_start_id)).toBe(true)

      yield* session.remove(forked.id)
      yield* session.remove(created.id)
    }),
  )

  it.instance("retains an assistant tail when compaction starts inside a turn", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const u1 = yield* addUser(sessionID, "first")
        const a1 = yield* addAssistant(sessionID, u1, { finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a1,
          type: "text",
          text: "first reply",
        })

        const u2 = yield* addUser(sessionID, "second")
        const a2 = yield* addAssistant(sessionID, u2, { finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a2,
          type: "text",
          text: "second reply",
        })
        const a3 = yield* addAssistant(sessionID, u2, { finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a3,
          type: "text",
          text: "tail reply",
        })

        const c1 = yield* addUser(sessionID)
        yield* addCompactionPart(sessionID, c1, a3)
        const s1 = yield* addAssistant(sessionID, c1, { summary: true, finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: s1,
          type: "text",
          text: "summary",
        })

        const u3 = yield* addUser(sessionID, "third")
        const a4 = yield* addAssistant(sessionID, u3, { finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a4,
          type: "text",
          text: "third reply",
        })

        const result = MessageV2.filterCompacted(yield* MessageV2.stream(sessionID))

        expect(result.map((item) => item.info.id)).toEqual([c1, s1, a3, u3, a4])
      }),
    ),
  )

  it.instance("prefers latest compaction boundary when repeated compactions exist", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const u1 = yield* addUser(sessionID, "first")
        const a1 = yield* addAssistant(sessionID, u1, { finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a1,
          type: "text",
          text: "first reply",
        })

        const u2 = yield* addUser(sessionID, "second")
        const a2 = yield* addAssistant(sessionID, u2, { finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a2,
          type: "text",
          text: "second reply",
        })

        const c1 = yield* addUser(sessionID)
        yield* addCompactionPart(sessionID, c1, u2)
        const s1 = yield* addAssistant(sessionID, c1, { summary: true, finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: s1,
          type: "text",
          text: "summary one",
        })

        const u3 = yield* addUser(sessionID, "third")
        const a3 = yield* addAssistant(sessionID, u3, { finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a3,
          type: "text",
          text: "third reply",
        })

        const c2 = yield* addUser(sessionID)
        yield* addCompactionPart(sessionID, c2, u3)
        const s2 = yield* addAssistant(sessionID, c2, { summary: true, finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: s2,
          type: "text",
          text: "summary two",
        })

        const u4 = yield* addUser(sessionID, "fourth")
        const a4 = yield* addAssistant(sessionID, u4, { finish: "end_turn" })
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: a4,
          type: "text",
          text: "fourth reply",
        })

        const result = MessageV2.filterCompacted(yield* MessageV2.stream(sessionID))

        expect(result.map((item) => item.info.id)).toEqual([c2, s2, u3, a3, u4, a4])
      }),
    ),
  )

  test("works with array input", () => {
    // filterCompacted accepts any Iterable, not just generators
    const id = MessageID.ascending()
    const items: SessionV1.WithParts[] = [
      {
        info: {
          id,
          sessionID: "s1",
          role: "user",
          time: { created: 1 },
          agent: "test",
          model: { providerID: "test", modelID: "test" },
        } as unknown as SessionV1.Info,
        parts: [{ type: "text", text: "hello" }] as unknown as SessionV1.Part[],
      },
    ]
    const result = MessageV2.filterCompacted(items)
    expect(result).toHaveLength(1)
    expect(result[0].info.id).toBe(id)
  })
})

describe("MessageV2.cursor", () => {
  test("encode/decode roundtrip", () => {
    const input = { id: MessageID.ascending(), time: 1234567890 }
    const encoded = MessageV2.cursor.encode(input)
    const decoded = MessageV2.cursor.decode(encoded)
    expect(decoded.id).toBe(input.id)
    expect(decoded.time).toBe(input.time)
  })

  test("encode/decode with fractional time", () => {
    const input = { id: MessageID.ascending(), time: 1234567890.5 }
    const encoded = MessageV2.cursor.encode(input)
    const decoded = MessageV2.cursor.decode(encoded)
    expect(decoded.time).toBe(1234567890.5)
  })

  test("encoded cursor is base64url", () => {
    const encoded = MessageV2.cursor.encode({ id: MessageID.ascending(), time: 0 })
    expect(encoded).toMatch(/^[A-Za-z0-9_-]+$/)
  })
})

describe("MessageV2 consistency", () => {
  it.instance("page hydration matches get for each message", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        yield* fill(sessionID, 3)

        const paged = yield* MessageV2.page({ sessionID, limit: 10 })
        for (const item of paged.items) {
          const got = yield* MessageV2.get({ sessionID, messageID: item.info.id as MessageID })
          expect(got.info).toEqual(item.info)
          expect(got.parts).toEqual(item.parts)
        }
      }),
    ),
  )

  it.instance("parts from get match standalone parts call", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const [id] = yield* fill(sessionID, 1)

        const got = yield* MessageV2.get({ sessionID, messageID: id })
        const standalone = yield* MessageV2.parts(id)
        expect(got.parts).toEqual(standalone)
      }),
    ),
  )

  it.instance("stream collects same messages as exhaustive page iteration", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        yield* fill(sessionID, 7)

        const streamed = yield* MessageV2.stream(sessionID)

        const paged = [] as SessionV1.WithParts[]
        let cursor: string | undefined
        while (true) {
          const result = yield* MessageV2.page({ sessionID, limit: 3, before: cursor })
          for (let i = result.items.length - 1; i >= 0; i--) {
            paged.push(result.items[i])
          }
          if (!result.more || !result.cursor) break
          cursor = result.cursor
        }

        expect(streamed.map((m) => m.info.id)).toEqual(paged.map((m) => m.info.id))
      }),
    ),
  )

  it.instance("filterCompacted of full stream returns same as Array.from when no compaction", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        yield* fill(sessionID, 4)

        const stream = yield* MessageV2.stream(sessionID)
        const filtered = MessageV2.filterCompacted(stream)
        const all = stream.toReversed()

        expect(filtered.map((m) => m.info.id)).toEqual(all.map((m) => m.info.id))
      }),
    ),
  )
})
