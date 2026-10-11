import { afterEach, describe, expect, mock } from "bun:test"
import http from "node:http"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Context, DateTime, Effect, Layer } from "effect"
import { HttpServer } from "effect/unstable/http"
import { eq, sql } from "drizzle-orm"
import { Flag } from "@opencode-ai/core/flag/flag"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import {
  PrunedSessionsHeader,
  PrunedSessionsHeaderBudget,
  SyncPaths,
} from "../../src/server/routes/instance/httpapi/groups/sync"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { Session } from "@/session/session"
import { EventV2Bridge } from "@/event-v2-bridge"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"
import { applyRetentionFixture, applyRetentionFixtures } from "../fixture/session-retention"
import { readSyncHistory } from "../../src/server/routes/instance/httpapi/handlers/sync"

type HistoryRow = { id: string; aggregate_id: string; seq: number; type: string; data: Record<string, unknown> }

// The documented X-Pruned-Sessions syntax: comma-separated `first..last` ranges or lone IDs.
const parseRanges = (header: string | undefined) =>
  (header ?? "")
    .split(",")
    .filter((entry) => entry.length > 0)
    .map((entry) => {
      const [first, last] = entry.split("..")
      return { first: first!, last: last ?? first! }
    })

const covered = (ranges: { first: string; last: string }[], id: string) =>
  ranges.some((range) => range.first <= id && id <= range.last)

// Every Session ID the store holds, which is what a range speaks for.
const heldIDs = (db: Database.Interface["db"]) =>
  Effect.gen(function* () {
    const sessions = yield* db.select({ id: SessionTable.id }).from(SessionTable).all()
    const aggregates = yield* db.select({ id: EventSequenceTable.aggregate_id }).from(EventSequenceTable).all()
    return [...sessions, ...aggregates].map((row) => row.id)
  })

// Node's own HTTP client with its default 16 KiB response-header limit, as a standard client would send it.
function postWithNodeHttp(url: URL, directory: string, body: string) {
  return new Promise<{ status?: number; headers: http.IncomingHttpHeaders; body: string; error?: string }>(
    (resolve) => {
      const request = http.request(
        url,
        {
          method: "POST",
          headers: { "content-type": "application/json", "x-opencode-directory": directory },
        },
        (response) => {
          const chunks = new Array<Buffer>()
          response.on("data", (chunk: Buffer) => chunks.push(chunk))
          response.on("end", () =>
            resolve({
              status: response.statusCode,
              headers: response.headers,
              body: Buffer.concat(chunks).toString("utf8"),
            }),
          )
        },
      )
      request.on("error", (error: NodeJS.ErrnoException) =>
        resolve({ headers: {}, body: "", error: `${error.code ?? ""} ${error.message}` }),
      )
      request.end(body)
    },
  )
}

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
    "the X-Pruned-Sessions header names the pruned Session as a range and no unpruned Session",
    () =>
      Effect.gen(function* () {
        const store = yield* prunedStore
        for (const payload of [...store.payloads, { [store.pruned.id]: store.prunedLast + 1, [store.kept.id]: 0 }]) {
          const response = yield* store.history(payload)
          expect(response.status).toBe(200)
          const ranges = parseRanges(response.headers["x-pruned-sessions"])
          expect(covered(ranges, store.pruned.id)).toBe(true)
          expect(covered(ranges, store.kept.id)).toBe(false)
          for (const id of yield* heldIDs(store.db)) {
            if (covered(ranges, id)) expect(id).toBe(store.pruned.id)
          }
        }
      }),
    { git: true, config: { formatter: false, lsp: false } },
    { timeout: 10_000 },
  )

  it.instance(
    "a standard HTTP client syncs a high-cardinality pruned history without response-header overflow",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const { db } = yield* Database.Service
        // 700 Sessions: across the first 300, every other one is kept, so the pruned ones there cannot be
        // coalesced; the last 400 are all pruned. 550 pruned Sessions in all, enough that naming each one would
        // overflow Node's 16 KiB response-header limit.
        const sessions = new Array<{ id: string; prune: boolean }>()
        for (let index = 0; index < 700; index++) {
          const session = yield* Session.use.create({ title: `high-cardinality ${index}` })
          sessions.push({ id: session.id, prune: index >= 300 || index % 2 === 0 })
        }
        const pruned = sessions.filter((session) => session.prune).map((session) => session.id)
        expect(pruned).toHaveLength(550)
        yield* applyRetentionFixtures(pruned)
        const prunedSet = new Set(pruned)
        const kept = sessions.filter((session) => !session.prune).map((session) => session.id)

        // The suite's app is served by a real Node HTTP server on a loopback port; request it over TCP.
        const address = (yield* HttpServer.HttpServer).address
        if (address._tag !== "TcpAddress") throw new Error("sync app is not served over TCP")
        const response = yield* Effect.promise(() =>
          postWithNodeHttp(new URL(SyncPaths.history, `http://127.0.0.1:${address.port}`), tmp.directory, "{}"),
        )

        expect(response.error).toBeUndefined()
        expect(response.status).toBe(200)
        const body = JSON.parse(response.body) as HistoryRow[]
        expect(Array.isArray(body)).toBe(true)
        expect(body.some((row) => prunedSet.has(row.aggregate_id))).toBe(false)
        expect(new Set(body.map((row) => row.aggregate_id))).toEqual(new Set(kept))
        const header = response.headers["x-pruned-sessions"]
        expect(typeof header).toBe("string")
        const line = `${PrunedSessionsHeader}: ${header}\r\n`
        expect(Buffer.byteLength(line)).toBeLessThanOrEqual(PrunedSessionsHeaderBudget)
        const ranges = parseRanges(header as string)
        expect(ranges.length).toBeGreaterThan(1)
        // Ranges, not a list: the budget cannot hold 400 IDs one by one, yet the contiguous block is covered.
        expect(pruned.filter((id) => covered(ranges, id)).length).toBeGreaterThanOrEqual(400)
        expect(ranges.map((range) => range.first)).toEqual(ranges.map((range) => range.first).toSorted())
        for (const id of kept) expect(covered(ranges, id)).toBe(false)
        for (const id of yield* heldIDs(db)) {
          if (covered(ranges, id)) expect(prunedSet.has(id)).toBe(true)
        }
      }),
    { git: true, config: { formatter: false, lsp: false } },
    { timeout: 60_000 },
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
