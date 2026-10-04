import { afterEach, describe, expect, mock } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Cause, Context, DateTime, Effect, Exit, Layer } from "effect"
import { eq, sql } from "drizzle-orm"
import { Flag } from "@opencode-ai/core/flag/flag"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionDurable } from "@opencode-ai/schema/durable-event-manifest"
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
    "refuses sync history and replay for a redacted aggregate before returning or decoding rows",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const session = yield* Session.use.create({ title: "sync retention sentinel" })
        const { db } = yield* Database.Service
        yield* applyRetentionFixture(session.id)

        const before = yield* db.select().from(EventTable).where(eq(EventTable.aggregate_id, session.id)).all()
        const sequence = yield* db
          .select({ seq: EventSequenceTable.seq })
          .from(EventSequenceTable)
          .where(eq(EventSequenceTable.aggregate_id, session.id))
          .get()
        const history = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({}),
        })
        expect(history.status).not.toBe(200)
        expect(yield* history.text).not.toContain("sync retention sentinel")
        const latestSequence = before.at(-1)?.seq ?? -1
        for (const cursor of [-1, latestSequence]) {
          const currentHistory = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
            method: "POST",
            headers,
            body: JSON.stringify({ [session.id]: cursor }),
          })
          expect(currentHistory.status).not.toBe(200)
          expect(yield* currentHistory.text).not.toContain("sync retention sentinel")
        }

        const replay = yield* requestInDirectory(SyncPaths.replay, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({
            directory: tmp.directory,
            events: [
              {
                id: "evt_unreadable_replay",
                aggregateID: session.id,
                seq: (sequence?.seq ?? -1) + 1,
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
    "preflights mixed sync aggregates before decoding a malformed marked event",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const redacted = yield* Session.use.create({ title: "mixed redacted sync" })
        const safe = yield* Session.use.create({ title: "safe sync peer" })
        yield* Session.use.updateMessage({
          id: SessionV1.MessageID.ascending(),
          role: "user",
          sessionID: safe.id,
          agent: "build",
          model: { providerID: ProviderV2.ID.make("sync-test"), modelID: ModelV2.ID.make("sync-test") },
          time: { created: Date.now() },
        })
        const events = yield* EventV2Bridge.Service
        yield* events.publish(SessionEvent.Synthetic, {
          sessionID: redacted.id,
          messageID: SessionMessage.ID.create(),
          timestamp: yield* DateTime.now,
          text: "producer-emitted event before retention",
        })
        yield* events.publish(SessionEvent.Synthetic, {
          sessionID: safe.id,
          messageID: SessionMessage.ID.create(),
          timestamp: yield* DateTime.now,
          text: "safe sync event",
        })
        const { db } = yield* Database.Service
        const currentSequence = yield* EventV2.latestSequence(db, redacted.id)
        yield* applyRetentionFixture(redacted.id)
        yield* db.run(sql`UPDATE event SET data = 'not-json' WHERE aggregate_id = ${redacted.id}`)

        for (const cursor of [-1, currentSequence]) {
          const payload = { [redacted.id]: cursor, [safe.id]: -1 }
          const result = yield* readSyncHistory(db, payload).pipe(Effect.exit)
          expect(Exit.isFailure(result)).toBe(true)
          if (Exit.isFailure(result))
            expect(Cause.squash(result.cause)).toBeInstanceOf(EventV2.UnreplayableAggregateError)

          const response = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
            method: "POST",
            headers,
            body: JSON.stringify(payload),
          })
          expect(response.status).not.toBe(200)
          expect(yield* response.text).not.toContain("not-json")
        }

        const safeHistory = yield* EventV2.readAggregate(db, {
          aggregateID: safe.id,
          after: -1,
          limit: 10,
          manifest: SessionDurable,
        })
        expect(safeHistory.events.length).toBeGreaterThan(0)
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
