import { describe, expect } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionInput } from "@opencode-ai/core/session/input"
import { Prompt } from "@opencode-ai/core/session/prompt"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionDurable } from "@opencode-ai/schema/durable-event-manifest"
import path from "node:path"
import { Cause, DateTime, Deferred, Effect, Exit, Fiber, Layer, Stream } from "effect"
import { and, desc, eq } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { EventRetentionTable, EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import {
  PartTable,
  MessageTable,
  SessionInputTable,
  SessionMessageTable,
  SessionTable,
  TodoTable,
} from "@opencode-ai/core/session/sql"
import { Session as SessionNs } from "@/session/session"
import { MessageV2 } from "../../src/session/message-v2"
import { MessageID, PartID, type SessionID } from "../../src/session/schema"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { provideInstance, tmpdirScoped } from "../fixture/fixture"
import { spanHold } from "../fixture/span-hold"
import { testEffect } from "../lib/effect"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { EventV2Bridge } from "@/event-v2-bridge"
import { GlobalBus } from "@/bus/global"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceStore } from "@/project/instance-store"
import { InstanceBootstrap } from "@/project/bootstrap"
import { SessionQueue } from "@/session/queue"
import { Todo } from "@/session/todo"
import { DbRetention, type RetentionEvidence, type SqliteAccess } from "@/cli/cmd/db-retention"
import { applyRetentionFixture } from "../fixture/session-retention"
import { readExport } from "@/cli/cmd/db-export-usage"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      SessionNs.node,
      EventV2Bridge.node,
      SessionProjector.node,
      Database.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
    ]),
    [
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalWorkspaces: false })],
      [
        InstanceBootstrap.node,
        Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void })),
      ],
    ],
  ),
)
const retentionIt = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      SessionNs.node,
      SessionQueue.node,
      Todo.node,
      SessionV2.node,
      SessionStore.node,
      EventV2Bridge.node,
      SessionProjector.node,
      Database.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
    ]),
    [
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalWorkspaces: false })],
      [
        InstanceBootstrap.node,
        Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void })),
      ],
      [SessionExecution.node, SessionExecution.noopLayer],
    ],
  ),
)

const awaitDeferred = <T>(deferred: Deferred.Deferred<T>, message: string) =>
  Effect.race(
    Deferred.await(deferred),
    Effect.sleep("2 seconds").pipe(Effect.flatMap(() => Effect.fail(new Error(message)))),
  )

const awaitDeferredEffect = (wait: Effect.Effect<void>, message: string) =>
  Effect.race(wait, Effect.sleep("2 seconds").pipe(Effect.flatMap(() => Effect.fail(new Error(message)))))

const remove = (id: SessionID) => SessionNs.use.remove(id)

function retentionEvidence(sessionID: string, seq: number, ownerID: string | null, now: number): RetentionEvidence {
  return {
    customerBinding: {
      proofID: "producer-fixture-unbound-session",
      durable: true,
      sessionIDs: [sessionID],
      customerBoundSessionIDs: [],
      nonCustomerSessionIDs: [sessionID],
    },
    policy: {
      reviewed: true,
      cutoffEpochMs: now + 60_000,
      reviewedReference: "swxtchio/swx-opencode#97",
      policyDigest: "fixture-policy-digest",
      readerContractReviewed: true,
      readerContractID: "fixture-reader-contract",
    },
    liveness: {
      proofID: "producer-fixture-liveness",
      observedAtEpochMs: now,
      sessionIDs: [sessionID],
      aggregateOwners: { [sessionID]: ownerID },
      servingProcesses: [],
      canResume: false,
      unfinishedOwnedWork: false,
      validThroughEpochMs: now + 60_000,
    },
    handoff: {
      receiptID: "producer-fixture-receipt",
      durable: true,
      sessionIDs: [sessionID],
      finalSequence: { [sessionID]: seq },
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
        resultDigest: "producer-fixture-report",
        denominators: { sessions: 1 },
        unavailableCauses: {
          correctness: "fixture has no correctness producer",
          performance: "fixture has no performance producer",
        },
        rawHistoryInaccessible: true,
      },
    },
  }
}

function exportFixtureDatabase(db: Database.Interface["db"], filename: string) {
  return Effect.gen(function* () {
    const bytes = yield* (db.$client as unknown as { export: Effect.Effect<Uint8Array> }).export
    yield* Effect.promise(() => Bun.write(filename, bytes))
    const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
    return new sqlite.Database(filename)
  })
}

describe("session.created event", () => {
  it.instance("should emit session.created event when session is created", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const events = yield* EventV2Bridge.Service
      const received = yield* Deferred.make<SessionNs.Info>()

      const unsub = yield* events.listen((event) => {
        if (event.type === SessionNs.Event.Created.type)
          Deferred.doneUnsafe(
            received,
            Effect.succeed((event.data as typeof SessionNs.Event.Created.data.Type).info as SessionNs.Info),
          )
        return Effect.void
      })
      yield* Effect.addFinalizer(() => unsub)

      const info = yield* session.create({})
      const receivedInfo = yield* awaitDeferred(received, "timed out waiting for session.created")

      expect(receivedInfo.id).toBe(info.id)
      expect(receivedInfo.projectID).toBe(info.projectID)
      expect(receivedInfo.directory).toBe(info.directory)
      expect(receivedInfo.path).toBe(info.path)
      expect(receivedInfo.title).toBe(info.title)

      yield* session.remove(info.id)
    }),
  )

  it.instance("session.created event should be emitted before session.updated", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const source = yield* EventV2Bridge.Service
      const events: string[] = []
      const received = yield* Deferred.make<string[]>()
      const push = (event: string) => {
        events.push(event)
        if (events.includes("created") && events.includes("updated")) {
          Deferred.doneUnsafe(received, Effect.succeed(events))
        }
      }

      const unsubscribe = yield* source.listen((event) => {
        if (event.type === SessionNs.Event.Created.type) push("created")
        if (event.type === SessionNs.Event.Updated.type) push("updated")
        return Effect.void
      })
      yield* Effect.addFinalizer(() => unsubscribe)

      const info = yield* session.create({})
      yield* session.setTitle({ sessionID: info.id, title: "updated" })
      const receivedEvents = yield* awaitDeferred(received, "timed out waiting for session created/updated events")

      expect(receivedEvents).toContain("created")
      expect(receivedEvents).toContain("updated")
      expect(receivedEvents.indexOf("created")).toBeLessThan(receivedEvents.indexOf("updated"))

      yield* session.remove(info.id)
    }),
  )

  it.instance("emits legacy global sync payload", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const received = yield* Deferred.make<{ syncEvent: EventV2.SerializedEvent }>()
      const listener = (event: { payload: { type?: string; syncEvent?: EventV2.SerializedEvent } }) => {
        if (event.payload.type === "sync" && event.payload.syncEvent)
          Deferred.doneUnsafe(received, Effect.succeed({ syncEvent: event.payload.syncEvent }))
      }
      GlobalBus.on("event", listener)
      yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", listener)))

      const info = yield* session.create({})
      const event = yield* awaitDeferred(received, "timed out waiting for legacy global sync event")

      expect(event.syncEvent).toMatchObject({
        type: EventV2.versionedType(SessionNs.Event.Created.type, 1),
        seq: 0,
        aggregateID: info.id,
        data: { sessionID: info.id },
      })

      yield* session.remove(info.id)
    }),
  )
})

describe("step-finish token propagation via event", () => {
  it.instance(
    "non-zero tokens propagate through PartUpdated event",
    () =>
      Effect.gen(function* () {
        const session = yield* SessionNs.Service
        const events = yield* EventV2Bridge.Service
        const info = yield* session.create({})

        const messageID = MessageID.ascending()
        yield* session.updateMessage({
          id: messageID,
          sessionID: info.id,
          role: "user",
          time: { created: Date.now() },
          agent: "user",
          model: { providerID: "test", modelID: "test" },
          tools: {},
          mode: "",
        } as unknown as SessionV1.Info)

        // Event subscribers receive readonly Schema.Type payloads; `SessionV1.Part`
        // is the mutable domain type. Cast bridges the two — safe because the
        // test only reads the value afterwards.
        const received = yield* Deferred.make<SessionV1.Part>()
        const unsub = yield* events.listen((event) => {
          if (event.type === MessageV2.Event.PartUpdated.type)
            Deferred.doneUnsafe(
              received,
              Effect.succeed((event.data as typeof MessageV2.Event.PartUpdated.data.Type).part as SessionV1.Part),
            )
          return Effect.void
        })
        yield* Effect.addFinalizer(() => unsub)

        const tokens = {
          total: 1500,
          input: 500,
          output: 800,
          reasoning: 200,
          cache: { read: 100, write: 50 },
        }

        const partInput = {
          id: PartID.ascending(),
          messageID,
          sessionID: info.id,
          type: "step-finish" as const,
          reason: "stop",
          cost: 0.005,
          tokens,
        }

        yield* session.updatePart(partInput)
        const receivedPart = yield* awaitDeferred(received, "timed out waiting for message.part.updated")

        expect(receivedPart.type).toBe("step-finish")
        const finish = receivedPart as SessionV1.StepFinishPart
        expect(finish.tokens.input).toBe(500)
        expect(finish.tokens.output).toBe(800)
        expect(finish.tokens.reasoning).toBe(200)
        expect(finish.tokens.total).toBe(1500)
        expect(finish.tokens.cache.read).toBe(100)
        expect(finish.tokens.cache.write).toBe(50)
        expect(finish.cost).toBe(0.005)
        expect(receivedPart).not.toBe(partInput)

        yield* session.remove(info.id)
      }),
    { timeout: 30000 },
  )
})

describe("Session retention producer path", () => {
  retentionIt.instance("redacts Session-produced message and part copies and fences the modified aggregate", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const events = yield* EventV2Bridge.Service
      const { db } = yield* Database.Service
      const info = yield* session.create({ title: "retention title sentinel" })
      const messageID = MessageID.ascending()
      const assistantTime = { created: Date.now(), completed: Date.now() }
      yield* session.updateMessage({
        id: messageID,
        sessionID: info.id,
        role: "user",
        time: { created: Date.now() },
        agent: "user",
        model: { providerID: "provider-a", modelID: "requested-model" },
      } as SessionV1.User)
      const assistantMessageID = MessageID.ascending()
      yield* session.updateMessage({
        id: assistantMessageID,
        sessionID: info.id,
        role: "assistant",
        time: assistantTime,
        parentID: messageID,
        modelID: ModelV2.ID.make("requested-model"),
        providerID: ProviderV2.ID.make("firerouter"),
        mode: "build",
        agent: "build",
        path: { cwd: info.directory, root: info.directory },
        responseModelIDs: ["served-model"],
        cost: 0.25,
        tokens: { total: 32, input: 12, output: 17, reasoning: 3, cache: { read: 5, write: 7 } },
      } as SessionV1.Assistant)
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: info.id,
        messageID: assistantMessageID,
        type: "text",
        text: "producer raw sentinel",
      } as SessionV1.TextPart)
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: info.id,
        messageID: assistantMessageID,
        type: "step-finish",
        reason: "stop",
        responseModelID: "served-model",
        cost: 0.25,
        tokens: { total: 32, input: 12, output: 17, reasoning: 3, cache: { read: 5, write: 7 } },
      } as SessionV1.StepFinishPart)
      yield* events.publish(SessionEvent.ContextUpdated, {
        sessionID: SessionV2.ID.make(info.id),
        messageID: SessionMessage.ID.create(),
        timestamp: DateTime.makeUnsafe(Date.now()),
        text: "v2 context sentinel",
      })

      const exportDirectory = yield* tmpdirScoped()
      const originalMessageData = yield* db
        .select({ data: MessageTable.data })
        .from(MessageTable)
        .where(eq(MessageTable.id, assistantMessageID))
        .get()
      if (!originalMessageData) throw new Error("Session.updateMessage did not project its assistant")
      const originalMessageTime = JSON.parse(JSON.stringify(originalMessageData.data)).time
      expect(originalMessageTime).toEqual(assistantTime)
      const beforeExportDB = yield* exportFixtureDatabase(db, path.join(exportDirectory, "usage-before.sqlite"))
      yield* Effect.addFinalizer(() => Effect.sync(() => beforeExportDB.close()))
      const exportBefore = readExport(beforeExportDB)

      const { database: native, result, tree, eventIdentitiesBefore } = yield* applyRetentionFixture(info.id)
      expect(tree.eligible).toBe(true)
      expect(tree.aggregates[0]?.rows).toBeGreaterThan(0)
      expect(result.state).toBe("complete")
      const retainedPart = JSON.parse(
        native
          .query<
            { data: string },
            [string]
          >("SELECT data FROM part WHERE session_id = ? AND json_extract(data, '$.type') = 'step-finish'")
          .get(info.id)!.data,
      ) as {
        responseModelID?: string
        cost: number
        tokens: { input: number; output: number; reasoning: number; cache: { read: number; write: number } }
      }
      expect(retainedPart).toMatchObject({
        responseModelID: "served-model",
        cost: 0.25,
        tokens: { input: 12, output: 17, reasoning: 3, cache: { read: 5, write: 7 } },
      })
      const retainedMessage = native
        .query<{ data: string }, [string]>("SELECT data FROM message WHERE id = ?")
        .get(assistantMessageID)
      expect(JSON.parse(retainedMessage!.data).time).toEqual(originalMessageTime)
      const exportAfter = readExport(native)
      const usageAxes = (archive: ReturnType<typeof readExport>) =>
        archive.records.map((record) => ({
          messages: record["messages"],
          providerID: record["providerID"],
          modelID: record["modelID"],
          servedModelIDs: record["servedModelIDs"],
          tokens: record["tokens"],
          reportedCost: record["reportedCost"],
        }))
      expect(usageAxes(exportAfter)).toEqual(usageAxes(exportBefore))
      expect(exportAfter.reportedCostTotal).toBe(exportBefore.reportedCostTotal)
      expect(exportAfter.check.ok).toBe(exportBefore.check.ok)
      const rawCopies = [
        ...native
          .query<{ data: string }, [string]>("SELECT data FROM event WHERE aggregate_id = ?")
          .all(info.id)
          .map((row) => row.data),
        ...native
          .query<{ data: string }, [string]>("SELECT data FROM message WHERE session_id = ?")
          .all(info.id)
          .map((row) => row.data),
        ...native
          .query<{ data: string }, [string]>("SELECT data FROM part WHERE session_id = ?")
          .all(info.id)
          .map((row) => row.data),
        ...native
          .query<{ data: string }, [string]>("SELECT data FROM session_message WHERE session_id = ?")
          .all(info.id)
          .map((row) => row.data),
        native
          .query<{ title: string; directory: string }, [string]>("SELECT title, directory FROM session WHERE id = ?")
          .get(info.id),
      ]
      expect(JSON.stringify(rawCopies)).not.toContain("producer raw sentinel")
      expect(JSON.stringify(rawCopies)).not.toContain("v2 context sentinel")
      expect(JSON.stringify(rawCopies)).not.toContain("retention title sentinel")
      expect(native.query("SELECT id, seq FROM event WHERE aggregate_id = ? ORDER BY seq").all(info.id)).toEqual(
        eventIdentitiesBefore,
      )

      const historyPage = yield* EventV2.readAggregate(db, {
        aggregateID: info.id,
        after: -1,
        limit: 100,
        manifest: SessionDurable,
      }).pipe(Effect.exit)
      expect(Exit.isFailure(historyPage)).toBe(true)
      expect(String(historyPage)).toContain(`Aggregate ${info.id} is unreplayable`)
      const currentCursor = yield* EventV2.latestSequence(db, info.id)
      const historyAfterCurrentCursor = yield* EventV2.readAggregate(db, {
        aggregateID: info.id,
        after: currentCursor,
        limit: 100,
        manifest: SessionDurable,
      }).pipe(Effect.exit)
      expect(Exit.isFailure(historyAfterCurrentCursor)).toBe(true)
      expect(String(historyAfterCurrentCursor)).toContain(`Aggregate ${info.id} is unreplayable`)
      const history = yield* events.durable({ aggregateID: info.id }).pipe(Stream.runCollect, Effect.exit)
      expect(Exit.isFailure(history)).toBe(true)
      expect(String(history)).toContain(`Aggregate ${info.id} is unreplayable`)
      const replayRow = yield* db
        .select()
        .from(EventTable)
        .where(eq(EventTable.aggregate_id, info.id))
        .orderBy(desc(EventTable.seq))
        .get()
      const replay = yield* events
        .replayAll([
          {
            id: replayRow!.id,
            aggregateID: replayRow!.aggregate_id,
            seq: replayRow!.seq,
            type: replayRow!.type,
            data: replayRow!.data,
          },
        ])
        .pipe(Effect.exit)
      expect(Exit.isFailure(replay)).toBe(true)
      expect(String(replay)).toContain(`Aggregate ${info.id} is unreplayable`)
      const messages = yield* session.messages({ sessionID: info.id }).pipe(Effect.exit)
      expect(Exit.isFailure(messages)).toBe(true)
      const projectedPage = yield* MessageV2.page({ sessionID: info.id, limit: 20 }).pipe(Effect.exit)
      expect(Exit.isFailure(projectedPage)).toBe(true)
      const projectedSnapshot = yield* MessageV2.snapshot(info.id).pipe(Effect.exit)
      expect(Exit.isFailure(projectedSnapshot)).toBe(true)
      const projectedParts = yield* MessageV2.parts(messageID).pipe(Effect.exit)
      expect(Exit.isFailure(projectedParts)).toBe(true)
      const v2Session = yield* SessionV2.Service
      const context = yield* v2Session.context(SessionV2.ID.make(info.id)).pipe(Effect.exit)
      expect(Exit.isFailure(context)).toBe(true)
      if (Exit.isFailure(context))
        expect(Cause.squash(context.cause)).toBeInstanceOf(EventV2.UnreplayableAggregateError)
      const v2Messages = yield* v2Session.messages({ sessionID: SessionV2.ID.make(info.id) }).pipe(Effect.exit)
      expect(Exit.isFailure(v2Messages)).toBe(true)
      const v2Message = yield* v2Session
        .message({ sessionID: SessionV2.ID.make(info.id), messageID: SessionMessage.ID.make(messageID) })
        .pipe(Effect.exit)
      expect(Exit.isFailure(v2Message)).toBe(true)

      const lateWrite = yield* session
        .updatePart({
          id: PartID.ascending(),
          sessionID: info.id,
          messageID,
          type: "text",
          text: "late raw sentinel",
        } as SessionV1.TextPart)
        .pipe(Effect.exit)
      expect(Exit.isFailure(lateWrite)).toBe(true)
      expect(
        yield* db.select({ id: PartTable.id }).from(PartTable).where(eq(PartTable.session_id, info.id)).all(),
      ).toHaveLength(2)
      const queue = yield* SessionQueue.Service
      const queuedWrite = yield* queue
        .admit({ sessionID: info.id, delivery: "queue", parts: [{ type: "text", text: "late queue sentinel" }] })
        .pipe(Effect.exit)
      expect(Exit.isFailure(queuedWrite)).toBe(true)
      expect(
        yield* db
          .select({ id: SessionPromptQueueTable.id })
          .from(SessionPromptQueueTable)
          .where(eq(SessionPromptQueueTable.session_id, info.id))
          .all(),
      ).toHaveLength(0)
      const admittedWrite = yield* v2Session
        .prompt({
          sessionID: SessionV2.ID.make(info.id),
          prompt: Prompt.make({ text: "late V2 input sentinel" }),
          resume: false,
        })
        .pipe(Effect.exit)
      expect(Exit.isFailure(admittedWrite)).toBe(true)
      expect(
        yield* db
          .select({ id: SessionInputTable.id })
          .from(SessionInputTable)
          .where(eq(SessionInputTable.session_id, info.id))
          .all(),
      ).toHaveLength(0)
      const todo = yield* Todo.Service
      const todoWrite = yield* todo
        .update({
          sessionID: info.id,
          todos: [{ content: "late todo sentinel", status: "pending", priority: "high" }],
        })
        .pipe(Effect.exit)
      expect(Exit.isFailure(todoWrite)).toBe(true)
      expect(
        yield* db
          .select({ position: TodoTable.position })
          .from(TodoTable)
          .where(eq(TodoTable.session_id, info.id))
          .all(),
      ).toHaveLength(0)

      const other = yield* session.create({})
      const otherMessageID = MessageID.ascending()
      yield* session.updateMessage({
        id: otherMessageID,
        sessionID: other.id,
        role: "user",
        time: { created: Date.now() },
        agent: "user",
        model: { providerID: "provider-a", modelID: "requested-model" },
      } as SessionV1.User)
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: other.id,
        messageID: otherMessageID,
        type: "text",
        text: "other session remains writable",
      } as SessionV1.TextPart)
      expect(
        yield* db.select({ id: PartTable.id }).from(PartTable).where(eq(PartTable.session_id, other.id)).all(),
      ).toHaveLength(1)

      yield* session.remove(info.id)
      expect(yield* db.select().from(SessionTable).where(eq(SessionTable.id, info.id)).get()).toBeUndefined()
      expect(
        yield* db
          .select({ state: EventRetentionTable.state })
          .from(EventRetentionTable)
          .where(eq(EventRetentionTable.aggregate_id, info.id))
          .get(),
      ).toEqual({ state: "complete" })
    }),
    { timeout: 10_000 },
  )

  retentionIt.instance(
    "redacts producer-emitted tool arguments and shell output while retaining route and usage metrics",
    () =>
      Effect.gen(function* () {
        const session = yield* SessionNs.Service
        const events = yield* EventV2Bridge.Service
        const { db } = yield* Database.Service
        const info = yield* session.create({ title: "producer retention source" })
        const sessionID = SessionV2.ID.make(info.id)
        const timestamp = yield* DateTime.now
        const assistantMessageID = SessionMessage.ID.create()

        yield* events.publish(SessionEvent.Step.Started, {
          sessionID,
          timestamp,
          assistantMessageID,
          agent: "build",
          model: { id: ModelV2.ID.make("configured-route"), providerID: ProviderV2.ID.make("route-provider") },
        })
        yield* events.publish(SessionEvent.Tool.Input.Started, {
          sessionID,
          timestamp,
          assistantMessageID,
          callID: "call-retention-tool",
          name: "read",
        })
        yield* events.publish(SessionEvent.Tool.Input.Ended, {
          sessionID,
          timestamp,
          assistantMessageID,
          callID: "call-retention-tool",
          text: "tool input ended raw sentinel",
        })
        yield* events.publish(SessionEvent.Tool.Called, {
          sessionID,
          timestamp,
          assistantMessageID,
          callID: "call-retention-tool",
          tool: "read",
          input: { path: "tool called raw sentinel" },
          provider: { executed: false },
        })
        yield* events.publish(SessionEvent.Shell.Started, {
          sessionID,
          timestamp,
          messageID: SessionMessage.ID.create(),
          callID: "call-retention-shell",
          command: "shell command raw sentinel",
        })
        yield* events.publish(SessionEvent.Shell.Ended, {
          sessionID,
          timestamp,
          callID: "call-retention-shell",
          output: "shell output raw sentinel",
        })
        yield* events.publish(SessionEvent.Step.Ended, {
          sessionID,
          timestamp,
          assistantMessageID,
          finish: "stop",
          cost: 0.75,
          tokens: { input: 21, output: 8, reasoning: 3, cache: { read: 5, write: 2 } },
        })

        const beforeV2Assistant = yield* db
          .select({ data: SessionMessageTable.data })
          .from(SessionMessageTable)
          .where(and(eq(SessionMessageTable.session_id, info.id), eq(SessionMessageTable.type, "assistant")))
          .get()
        if (!beforeV2Assistant) throw new Error("SessionEvent producers did not project an assistant")
        const beforeV2Time = JSON.parse(JSON.stringify(beforeV2Assistant.data)).time
        expect(beforeV2Time).toEqual({
          created: DateTime.toEpochMillis(timestamp),
          completed: DateTime.toEpochMillis(timestamp),
        })

        const before = yield* Effect.all(
          [
            db.select({ data: EventTable.data }).from(EventTable).where(eq(EventTable.aggregate_id, info.id)).all(),
            db
              .select({ data: SessionMessageTable.data })
              .from(SessionMessageTable)
              .where(eq(SessionMessageTable.session_id, info.id))
              .all(),
          ],
          { concurrency: "unbounded" },
        )
        const beforeJSON = JSON.stringify(before)
        for (const sentinel of [
          "tool input ended raw sentinel",
          "tool called raw sentinel",
          "shell command raw sentinel",
          "shell output raw sentinel",
        ]) {
          expect(beforeJSON).toContain(sentinel)
        }

        const applied = yield* applyRetentionFixture(info.id)
        const redacted = applied.database
        const copies = [
          ...redacted
            .query<{ data: string }, [string]>("SELECT data FROM event WHERE aggregate_id = ?")
            .all(info.id)
            .map((row) => row.data),
          ...redacted
            .query<{ data: string }, [string]>("SELECT data FROM session_message WHERE session_id = ?")
            .all(info.id)
            .map((row) => row.data),
        ]
        const copyJSON = JSON.stringify(copies)
        for (const sentinel of [
          "tool input ended raw sentinel",
          "tool called raw sentinel",
          "shell command raw sentinel",
          "shell output raw sentinel",
        ]) {
          expect(copyJSON).not.toContain(sentinel)
        }
        const assistant = redacted
          .query<
            { data: string },
            [string]
          >("SELECT data FROM session_message WHERE session_id = ? AND type = 'assistant'")
          .get(info.id)
        expect(JSON.parse(assistant!.data).model).toEqual({
          id: "configured-route",
          providerID: "route-provider",
        })
        expect(JSON.parse(assistant!.data).time).toEqual(beforeV2Time)
        expect(JSON.parse(assistant!.data)).toMatchObject({
          cost: 0.75,
          tokens: { input: 21, output: 8, reasoning: 3, cache: { read: 5, write: 2 } },
        })
        expect(applied.tree.retainedMetricFields).toContain("model.id (requested route)")
        expect(applied.tree.retainedMetricFields).toContain("tokens.cache.read")
      }),
    { timeout: 10_000 },
  )

  retentionIt.instance("fences V1 queue reads and mutations during retention", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const queue = yield* SessionQueue.Service
      const { db } = yield* Database.Service
      const info = yield* session.create({})
      const item = yield* queue.admit({
        sessionID: info.id,
        delivery: "queue",
        parts: [{ type: "text", text: "queue retention sentinel" }],
      })
      yield* db.insert(EventRetentionTable).values({
        aggregate_id: info.id,
        state: "redacting",
        progress_table: "event",
        progress_id: "evt_retention",
        evidence: {},
        time_started: 1,
        time_updated: 1,
      })

      const prepared = { called: false }
      const promoted = yield* queue
        .promote({
          sessionID: info.id,
          delivery: "queue",
          prepare: () => Effect.sync(() => void (prepared.called = true)),
          write: () => Effect.void,
          rejected: () => Effect.void,
        })
        .pipe(Effect.exit)
      expect(Exit.isFailure(promoted)).toBe(true)
      expect(prepared.called).toBe(false)

      const withdrawn = yield* queue.withdraw(info.id, item.id).pipe(Effect.exit)
      expect(Exit.isFailure(withdrawn)).toBe(true)
      expect(String(withdrawn)).toContain(`Aggregate ${info.id} is unreplayable`)
      expect(
        yield* db
          .select({ withdrawn: SessionPromptQueueTable.time_withdrawn })
          .from(SessionPromptQueueTable)
          .where(eq(SessionPromptQueueTable.id, item.id))
          .get(),
      ).toEqual({ withdrawn: null })
    }),
  )

  retentionIt.instance("marks V1 queue and V2 admitted input producers ineligible", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const queue = yield* SessionQueue.Service
      const v2 = yield* SessionV2.Service
      const events = yield* EventV2Bridge.Service
      const { db } = yield* Database.Service
      const directory = yield* tmpdirScoped()
      const databasePath = path.join(directory, "retention-inputs.sqlite")
      const queued = yield* session.create({})
      yield* queue.admit({ sessionID: queued.id, delivery: "queue", parts: [{ type: "text", text: "queue sentinel" }] })
      const admitted = yield* session.create({})
      yield* v2.prompt({
        sessionID: SessionV2.ID.make(admitted.id),
        prompt: Prompt.make({ text: "v2 input sentinel" }),
        resume: false,
      })
      const promoted = yield* session.create({})
      yield* v2.prompt({
        sessionID: SessionV2.ID.make(promoted.id),
        prompt: Prompt.make({ text: "promoted V2 input sentinel" }),
        resume: false,
      })
      yield* SessionInput.promoteSteers(db, events, SessionV2.ID.make(promoted.id), Number.MAX_SAFE_INTEGER)
      const native = yield* exportFixtureDatabase(db, databasePath)
      yield* Effect.addFinalizer(() => Effect.sync(() => native.close()))
      const now = Date.now()
      const queuedProof = retentionEvidence(
        queued.id,
        yield* EventV2.latestSequence(db, queued.id),
        (yield* db
          .select({ ownerID: EventSequenceTable.owner_id })
          .from(EventSequenceTable)
          .where(eq(EventSequenceTable.aggregate_id, queued.id))
          .get())?.ownerID ?? null,
        now,
      )
      const queuedTree = DbRetention.inventory(native as unknown as SqliteAccess, queuedProof, now).trees.find(
        (item) => item.rootSessionID === queued.id,
      )
      expect(queuedTree?.reasons).toContain("v1-prompt-queue-row-present")
      expect(queuedTree?.eligible).toBe(false)

      const admittedProof = retentionEvidence(
        admitted.id,
        yield* EventV2.latestSequence(db, admitted.id),
        (yield* db
          .select({ ownerID: EventSequenceTable.owner_id })
          .from(EventSequenceTable)
          .where(eq(EventSequenceTable.aggregate_id, admitted.id))
          .get())?.ownerID ?? null,
        now,
      )
      const admittedTree = DbRetention.inventory(native as unknown as SqliteAccess, admittedProof, now).trees.find(
        (item) => item.rootSessionID === admitted.id,
      )
      expect(admittedTree?.reasons).toContain("v2-input-pending")
      expect(admittedTree?.eligible).toBe(false)

      const promotedProof = retentionEvidence(
        promoted.id,
        yield* EventV2.latestSequence(db, promoted.id),
        (yield* db
          .select({ ownerID: EventSequenceTable.owner_id })
          .from(EventSequenceTable)
          .where(eq(EventSequenceTable.aggregate_id, promoted.id))
          .get())?.ownerID ?? null,
        now,
      )
      const promotedTree = DbRetention.inventory(native as unknown as SqliteAccess, promotedProof, now).trees.find(
        (item) => item.rootSessionID === promoted.id,
      )
      expect(promotedTree?.reasons).toContain("v2-input-promoted-row-present")
      expect(promotedTree?.eligible).toBe(false)
    }),
  )
})

describe("Session", () => {
  it.live("remove works without an instance", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const dir = yield* tmpdirScoped({ git: true })
      const info = yield* provideInstance(dir)(session.create({ title: "remove-without-instance" }))

      const removeExit = yield* remove(info.id).pipe(Effect.exit)
      expect(Exit.isSuccess(removeExit)).toBe(true)

      const getExit = yield* session.get(info.id).pipe(Effect.exit)
      expect(Exit.isFailure(getExit)).toBe(true)
    }),
  )

  it.instance("persists metadata and copies it on fork by default", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const meta = { source: "sdk", trace: { id: "abc" } }
      const created = yield* Effect.acquireRelease(session.create({ title: "with-meta", metadata: meta }), (info) =>
        session.remove(info.id).pipe(Effect.ignore),
      )
      const saved = yield* session.get(created.id)
      const fork = yield* Effect.acquireRelease(session.fork({ sessionID: created.id }), (info) =>
        session.remove(info.id).pipe(Effect.ignore),
      )

      expect(saved.metadata).toEqual(meta)
      expect(fork.metadata).toEqual(meta)
      expect(fork.metadata).not.toBe(meta)
    }),
  )

  it.instance("forks the chronological prefix across mixed message ID ordering", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const created = yield* Effect.acquireRelease(session.create({}), (info) =>
        session.remove(info.id).pipe(Effect.ignore),
      )
      const ids = ["msg_z9-before", "msg_z1-before-wrap", "msg_a0-after-wrap", "msg_a1-after"]
      for (const [index, id] of ids.entries()) {
        yield* session.updateMessage({
          id: MessageID.make(id),
          sessionID: created.id,
          role: "user",
          time: { created: index + 1 },
          agent: "user",
          model: { providerID: "test", modelID: "test" },
        } as SessionV1.User)
      }

      const beforeWrap = yield* Effect.acquireRelease(
        session.fork({ sessionID: created.id, messageID: MessageID.make(ids[1]!) }),
        (info) => session.remove(info.id).pipe(Effect.ignore),
      )
      const afterWrap = yield* Effect.acquireRelease(
        session.fork({ sessionID: created.id, messageID: MessageID.make(ids[2]!) }),
        (info) => session.remove(info.id).pipe(Effect.ignore),
      )

      expect((yield* session.messages({ sessionID: beforeWrap.id })).map((msg) => msg.info.time.created)).toEqual([1])
      expect((yield* session.messages({ sessionID: afterWrap.id })).map((msg) => msg.info.time.created)).toEqual([1, 2])
    }),
  )

  it.instance("omits metadata when not provided", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const created = yield* Effect.acquireRelease(session.create({ title: "empty-meta" }), (info) =>
        session.remove(info.id).pipe(Effect.ignore),
      )
      const saved = yield* session.get(created.id)

      expect(created.metadata).toBeUndefined()
      expect(saved.metadata).toBeUndefined()
    }),
  )
})

describe("Session writes racing removal", () => {
  const seedMessage = (session: SessionNs.Interface, sessionID: SessionID, created: number) =>
    session.updateMessage({
      id: MessageID.ascending(),
      sessionID,
      role: "user",
      time: { created },
      agent: "user",
      model: { providerID: "test", modelID: "test" },
    } as SessionV1.User)

  const seedPart = (session: SessionNs.Interface, sessionID: SessionID, messageID: MessageID) =>
    session.updatePart({
      id: PartID.ascending(),
      sessionID,
      messageID,
      type: "text",
      text: "part",
    } as SessionV1.TextPart)

  const aggregate = (sessionID: SessionID) =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      return {
        seq: yield* EventV2.latestSequence(db, sessionID),
        events: (yield* db.select().from(EventTable).where(eq(EventTable.aggregate_id, sessionID)).all()).length,
      }
    })

  it.instance("refuses an in-flight write that commits after the Session was removed", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const bus = yield* EventV2Bridge.Service
      const info = yield* session.create({ title: "removed-while-writing" })
      const message = yield* seedMessage(session, info.id, 1)
      const part = yield* seedPart(session, info.id, message.id)
      const obtained = yield* Deferred.make<void>()
      const removed = yield* Deferred.make<void>()

      // The writer obtains the Session like the HTTP message and part delete handlers, then waits for removal to complete.
      const writer = yield* Effect.gen(function* () {
        yield* session.get(info.id)
        yield* Deferred.succeed(obtained, undefined)
        yield* Deferred.await(removed)
        const removeMessage = yield* session
          .removeMessage({ sessionID: info.id, messageID: message.id })
          .pipe(Effect.asVoid, Effect.exit)
        const removePart = yield* session
          .removePart({ sessionID: info.id, messageID: message.id, partID: part.id })
          .pipe(Effect.asVoid, Effect.exit)
        return { removeMessage, removePart }
      }).pipe(Effect.forkChild)
      // On every exit, let a parked writer go and stop waiting for it.
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(removed, undefined).pipe(Effect.andThen(Fiber.interrupt(writer))),
      )

      yield* awaitDeferred(obtained, "timed out waiting for the writer to obtain the Session")
      yield* session.remove(info.id)
      const notified = new Array<string>()
      const unsubscribe = yield* bus.listen((event) =>
        Effect.sync(() => {
          if ((event.data as { sessionID?: string } | undefined)?.sessionID === info.id) notified.push(event.type)
        }),
      )
      yield* Effect.addFinalizer(() => unsubscribe)
      yield* Deferred.succeed(removed, undefined)
      const result = yield* Fiber.join(writer)

      for (const exit of [result.removeMessage, result.removePart]) {
        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionProjector.SessionNotProjected)
      }
      expect(notified).toEqual([])
      expect(Exit.isFailure(yield* session.get(info.id).pipe(Effect.exit))).toBe(true)
      expect(yield* aggregate(info.id)).toEqual({ seq: -1, events: 0 })
    }),
  )

  it.instance("refuses a title write whose own read saw the Session before removal committed", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const bus = yield* EventV2Bridge.Service
      const info = yield* session.create({ title: "removed-during-patch" })
      const spans = spanHold()
      // Session.patch reads the Session and then publishes; hold setTitle between the two.
      const hold = yield* spans.arm({ name: "Session.get", parent: "Session.setTitle" })
      const writer = yield* session
        .setTitle({ sessionID: info.id, title: "late" })
        .pipe(Effect.withTracer(spans.tracer), Effect.exit, Effect.forkChild)
      // On every exit, let a held writer go and stop waiting for it.
      yield* Effect.addFinalizer(() => hold.disarm.pipe(Effect.andThen(Fiber.interrupt(writer))))

      yield* awaitDeferredEffect(hold.reached, "timed out waiting for setTitle to read the Session")
      yield* session.remove(info.id)
      const notified = new Array<string>()
      const unsubscribe = yield* bus.listen((event) =>
        Effect.sync(() => {
          if ((event.data as { sessionID?: string } | undefined)?.sessionID === info.id) notified.push(event.type)
        }),
      )
      yield* Effect.addFinalizer(() => unsubscribe)
      yield* hold.release
      const exit = yield* Fiber.join(writer)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionProjector.SessionNotProjected)
      expect(notified).toEqual([])
      expect(Exit.isFailure(yield* session.get(info.id).pipe(Effect.exit))).toBe(true)
      expect(yield* aggregate(info.id)).toEqual({ seq: -1, events: 0 })
    }),
  )

  it.instance("commits ordinary writes while the Session exists", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const info = yield* Effect.acquireRelease(session.create({ title: "active-writes" }), (created) =>
        session.remove(created.id).pipe(Effect.ignore),
      )
      const message = yield* seedMessage(session, info.id, 1)
      const part = yield* seedPart(session, info.id, message.id)
      const before = yield* aggregate(info.id)

      yield* session.setTitle({ sessionID: info.id, title: "renamed" })
      yield* session.removePart({ sessionID: info.id, messageID: message.id, partID: part.id })
      yield* session.removeMessage({ sessionID: info.id, messageID: message.id })

      expect(yield* aggregate(info.id)).toEqual({ seq: before.seq + 3, events: before.events + 3 })
      expect((yield* session.get(info.id)).title).toBe("renamed")
      expect(yield* session.messages({ sessionID: info.id })).toEqual([])
    }),
  )
})
