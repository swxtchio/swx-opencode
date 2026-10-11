import { afterEach, describe, expect, mock } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Context, DateTime, Effect, Layer } from "effect"
import { eq, sql } from "drizzle-orm"
import { Flag } from "@opencode-ai/core/flag/flag"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { EventTable } from "@opencode-ai/core/event/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SyncPaths } from "../../src/server/routes/instance/httpapi/groups/sync"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { Session } from "@/session/session"
import { EventV2Bridge } from "@/event-v2-bridge"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"
import { applyRetentionFixture } from "../fixture/session-retention"
import { readSyncHistory } from "../../src/server/routes/instance/httpapi/handlers/sync"

type HistoryRow = { id: string; aggregate_id: string; seq: number; type: string; data: Record<string, unknown> }

// One Session pruned by a real retention apply and one kept peer, both written by the real producers. The
// payloads touch the pruned Session three ways: by omission, past its first event, and at its last sequence.
const prunedStore = Effect.gen(function* () {
  const tmp = yield* TestInstance
  const pruned = yield* Session.use.create({ title: "pruned sync sentinel" })
  const kept = yield* Session.use.create({ title: "kept sync peer" })
  const events = yield* EventV2Bridge.Service
  for (const [session, text] of [
    [pruned, "pruned sync event sentinel"],
    [kept, "kept sync event"],
  ] as const) {
    yield* events.publish(SessionEvent.Synthetic, {
      sessionID: session.id,
      messageID: SessionMessage.ID.create(),
      timestamp: yield* DateTime.now,
      text,
    })
  }
  const { db } = yield* Database.Service
  const prunedLast = yield* EventV2.latestSequence(db, pruned.id)
  yield* applyRetentionFixture(pruned.id)
  const keptEvents = (yield* db
    .select()
    .from(EventTable)
    .where(eq(EventTable.aggregate_id, kept.id))
    .orderBy(EventTable.seq)
    .all()).map((row) => JSON.parse(JSON.stringify(row)) as HistoryRow)
  expect(keptEvents.length).toBeGreaterThan(0)
  const history = (payload: Record<string, number>) =>
    requestInDirectory(SyncPaths.history, tmp.directory, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    })
  return {
    db,
    pruned,
    kept,
    prunedLast,
    keptEvents,
    history,
    payloads: [{}, { [pruned.id]: 0, [kept.id]: 0 }, { [pruned.id]: prunedLast }] as Record<string, number>[],
  }
})

const originalWorkspaces = Flag.OPENCODE_EXPERIMENTAL_WORKSPACES
const context = Context.empty() as Context.Context<unknown>
const it = testEffect(
  Layer.mergeAll(LayerNode.compile(LayerNode.group([Session.node, EventV2Bridge.node, Database.node])), httpApiLayer),
)

afterEach(async () => {
  mock.restore()
  Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = originalWorkspaces
  await disposeAllInstances()
  await resetDatabase()
})

describe("sync HttpApi", () => {
  it.instance(
    "serves sync routes",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const session = yield* Session.use.create({ title: "sync" })

        const started = yield* requestInDirectory(SyncPaths.start, tmp.directory, { method: "POST", headers })
        expect(started.status).toBe(200)
        expect(yield* started.json).toBe(true)

        const history = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({}),
        })
        expect(history.status).toBe(200)
        const rows = (yield* history.json) as Array<{
          id: string
          aggregate_id: string
          seq: number
          type: string
          data: Record<string, unknown>
        }>
        expect(rows.map((row) => row.aggregate_id)).toContain(session.id)

        const replayed = yield* requestInDirectory(SyncPaths.replay, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({
            directory: tmp.directory,
            events: rows
              .filter((row) => row.aggregate_id === session.id)
              .map((row) => ({
                id: row.id,
                aggregateID: row.aggregate_id,
                seq: row.seq,
                type: row.type,
                data: row.data,
              })),
          }),
        })
        expect(replayed.status).toBe(200)
        expect(yield* replayed.json).toEqual({ sessionID: session.id })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "refuses sync replay onto a redacted aggregate without changing its rows",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const session = yield* Session.use.create({ title: "sync retention sentinel" })
        const { db } = yield* Database.Service
        yield* applyRetentionFixture(session.id)

        const before = yield* db.select().from(EventTable).where(eq(EventTable.aggregate_id, session.id)).all()
        const sequence = yield* EventV2.latestSequence(db, session.id)
        const replay = yield* requestInDirectory(SyncPaths.replay, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({
            directory: tmp.directory,
            events: [
              {
                id: "evt_unreadable_replay",
                aggregateID: session.id,
                seq: sequence + 1,
                type: EventV2.versionedType(SessionV1.Event.MessageRemoved.type, 1),
                data: { sessionID: session.id, messageID: SessionV1.MessageID.ascending() },
              },
            ],
          }),
        })
        expect(replay.status).not.toBe(200)
        expect(yield* replay.text).not.toContain("sync retention sentinel")
        expect(yield* db.select().from(EventTable).where(eq(EventTable.aggregate_id, session.id)).all()).toEqual(before)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "a history request touching a pruned Session returns 200 with the other Sessions' events",
    () =>
      Effect.gen(function* () {
        const store = yield* prunedStore
        for (const payload of store.payloads) {
          const response = yield* store.history(payload)
          expect(response.status).toBe(200)
          const rows = (yield* response.json) as HistoryRow[]
          expect(rows.filter((row) => row.aggregate_id === store.kept.id)).toEqual(
            store.keptEvents.filter((row) => row.seq > (payload[store.kept.id] ?? -1)),
          )
        }
      }),
    { git: true, config: { formatter: false, lsp: false } },
    { timeout: 10_000 },
  )

  it.instance(
    "a pruned Session contributes no events to sync history, and its rows are never read",
    () =>
      Effect.gen(function* () {
        const store = yield* prunedStore
        // Rows that can no longer be decoded prove the reader excludes the pruned Session without reading them.
        yield* store.db.run(sql`UPDATE event SET data = 'not-json' WHERE aggregate_id = ${store.pruned.id}`)
        for (const payload of store.payloads) {
          const response = yield* store.history(payload)
          expect(response.status).toBe(200)
          const rows = (yield* response.json) as HistoryRow[]
          expect(rows.filter((row) => row.aggregate_id === store.pruned.id)).toEqual([])
          const direct = yield* readSyncHistory(store.db, payload)
          expect(direct.events.filter((row) => row.aggregate_id === store.pruned.id)).toEqual([])
        }
      }),
    { git: true, config: { formatter: false, lsp: false } },
    { timeout: 10_000 },
  )

  it.instance(
    "the X-Pruned-Sessions header lists each pruned Session with its last sequence number",
    () =>
      Effect.gen(function* () {
        const store = yield* prunedStore
        for (const payload of store.payloads) {
          const response = yield* store.history(payload)
          expect(response.status).toBe(200)
          expect(JSON.parse(response.headers["x-pruned-sessions"] ?? "null")).toEqual({
            [store.pruned.id]: store.prunedLast,
          })
        }
        // A cursor past the pruned Session's last sequence still touches it, so it is still reported.
        const beyond = yield* store.history({ [store.pruned.id]: store.prunedLast + 1, [store.kept.id]: 0 })
        expect(beyond.status).toBe(200)
        expect(JSON.parse(beyond.headers["x-pruned-sessions"] ?? "null")).toEqual({
          [store.pruned.id]: store.prunedLast,
        })
      }),
    { git: true, config: { formatter: false, lsp: false } },
    { timeout: 10_000 },
  )

  it.instance(
    "sync history keeps its bare event array body when a Session is pruned",
    () =>
      Effect.gen(function* () {
        const store = yield* prunedStore
        for (const payload of store.payloads) {
          const response = yield* store.history(payload)
          expect(response.status).toBe(200)
          const body = (yield* response.json) as unknown
          expect(Array.isArray(body)).toBe(true)
          const rows = body as Record<string, unknown>[]
          expect(rows.length).toBeGreaterThan(0)
          for (const row of rows)
            expect(Object.keys(row).toSorted()).toEqual(["aggregate_id", "data", "id", "seq", "type"])
          expect(rows).toEqual(
            (yield* store.db.select().from(EventTable).orderBy(EventTable.seq).all())
              .filter((row) => row.aggregate_id !== store.pruned.id)
              .filter((row) => (payload[row.aggregate_id] ?? -1) < row.seq)
              .map((row) => JSON.parse(JSON.stringify(row))),
          )
        }
      }),
    { git: true, config: { formatter: false, lsp: false } },
    { timeout: 10_000 },
  )

  it.instance(
    "validates seq values",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const cases = [
          {
            path: SyncPaths.history,
            body: { aggregate: -1 },
          },
          {
            path: SyncPaths.history,
            body: { aggregate: 1.5 },
          },
          {
            path: SyncPaths.replay,
            body: {
              directory: tmp.directory,
              events: [{ id: "event", aggregateID: "session", seq: -1, type: "session.created", data: {} }],
            },
          },
          {
            path: SyncPaths.replay,
            body: {
              directory: tmp.directory,
              events: [{ id: "event", aggregateID: "session", seq: 1.5, type: "session.created", data: {} }],
            },
          },
          {
            path: SyncPaths.replay,
            body: {
              directory: tmp.directory,
              events: [{ id: "event", aggregateID: "session", seq: 0, type: "session.created", data: {} }],
            },
          },
        ]

        for (const item of cases) {
          const response = yield* requestInDirectory(item.path, tmp.directory, {
            method: "POST",
            headers,
            body: JSON.stringify(item.body),
          })
          expect(response.status).toBe(400)
        }
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance.skip(
    "returns structured validation errors",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const response = yield* Effect.promise(() =>
          HttpApiApp.webHandler().handler(
            new Request(`http://localhost${SyncPaths.history}`, {
              method: "POST",
              headers: { "x-opencode-directory": tmp.directory, "content-type": "application/json" },
              body: JSON.stringify({ aggregate: -1 }),
            }),
            context,
          ),
        )

        expect(response.status).toBe(400)
        expect(response.headers.get("content-type") ?? "").toContain("application/json")
        const body = (yield* Effect.promise(() => response.json())) as Record<string, unknown>
        expect(body.success).toBe(false)
        expect(Array.isArray(body.error) || Array.isArray(body.errors)).toBe(true)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )
})
