import { Workspace } from "@/control-plane/workspace"
import * as InstanceState from "@/effect/instance-state"
import { Session } from "@/session/session"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { EventV2Bridge } from "@/event-v2-bridge"
import { EventRetentionTable, EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { asc } from "drizzle-orm"
import { and } from "drizzle-orm"
import { eq } from "drizzle-orm"
import { inArray } from "drizzle-orm"
import { lte } from "drizzle-orm"
import { not } from "drizzle-orm"
import { notInArray } from "drizzle-orm"
import { or } from "drizzle-orm"
import { Effect, Scope } from "effect"
import { HttpEffect, HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder, HttpApiError } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import { HistoryPayload, PrunedSessionsHeader, ReplayPayload, SessionPayload } from "../groups/sync"

/**
 * Reads every event past the caller's cursors. A Session whose history retention has redacted (pruned) contributes
 * no events and is reported in `pruned` with its last sequence number instead, so one pruned Session never fails
 * the read for the others. Its rows are excluded in SQL and never read.
 */
export function readSyncHistory(db: Database.Interface["db"], payload: typeof HistoryPayload.Type) {
  const exclude = Object.entries(payload)
  const past =
    exclude.length > 0
      ? not(or(...exclude.map(([id, seq]) => and(eq(EventTable.aggregate_id, id), lte(EventTable.seq, seq))))!)
      : undefined
  return db
    .transaction((tx) =>
      Effect.gen(function* () {
        const aggregates = yield* tx
          .select({ aggregateID: EventTable.aggregate_id })
          .from(EventTable)
          .where(past)
          .groupBy(EventTable.aggregate_id)
          .all()
          .pipe(Effect.orDie)
        const touched = new Set([...Object.keys(payload), ...aggregates.map((event) => event.aggregateID)])
        const pruned = (yield* tx
          .select({ aggregateID: EventRetentionTable.aggregate_id, seq: EventSequenceTable.seq })
          .from(EventRetentionTable)
          .leftJoin(EventSequenceTable, eq(EventSequenceTable.aggregate_id, EventRetentionTable.aggregate_id))
          .where(inArray(EventRetentionTable.state, [...EventV2.unreplayableRetentionStates]))
          .orderBy(asc(EventRetentionTable.aggregate_id))
          .all()
          .pipe(Effect.orDie)).filter((row) => touched.has(row.aggregateID))
        const events = yield* tx
          .select()
          .from(EventTable)
          .where(
            and(
              past,
              pruned.length > 0
                ? notInArray(
                    EventTable.aggregate_id,
                    pruned.map((row) => row.aggregateID),
                  )
                : undefined,
            ),
          )
          .orderBy(asc(EventTable.seq))
          .all()
          .pipe(Effect.orDie)
        return {
          events,
          pruned: Object.fromEntries(pruned.map((row) => [row.aggregateID, row.seq ?? -1])),
        }
      }),
    )
    .pipe(Effect.orDie)
}

export const syncHandlers = HttpApiBuilder.group(InstanceHttpApi, "sync", (handlers) =>
  Effect.gen(function* () {
    const workspace = yield* Workspace.Service
    const session = yield* Session.Service
    const scope = yield* Scope.Scope
    const events = yield* EventV2Bridge.Service
    const { db } = yield* Database.Service

    const start = Effect.fn("SyncHttpApi.start")(function* () {
      yield* workspace
        .startWorkspaceSyncing((yield* InstanceState.context).project.id)
        .pipe(Effect.ignore, Effect.forkIn(scope))
      return true
    })

    const replay = Effect.fn("SyncHttpApi.replay")(function* (ctx: { payload: typeof ReplayPayload.Type }) {
      const payload: EventV2.SerializedEvent[] = ctx.payload.events.map((event) => ({
        id: event.id,
        aggregateID: event.aggregateID,
        seq: event.seq,
        type: event.type,
        data: { ...event.data },
      }))
      const source = payload[0].aggregateID
      yield* Effect.logInfo("sync replay requested", {
        sessionID: source,
        events: payload.length,
        first: payload[0]?.seq,
        last: payload.at(-1)?.seq,
        directory: ctx.payload.directory,
      })
      const ownerID = yield* InstanceState.workspaceID
      yield* events.replayAll(payload, { ownerID, strictOwner: true })
      yield* Effect.logInfo("sync replay complete", {
        sessionID: source,
        events: payload.length,
        first: payload[0]?.seq,
        last: payload.at(-1)?.seq,
      })
      return { sessionID: source }
    })

    const steal = Effect.fn("SyncHttpApi.steal")(function* (ctx: { payload: typeof SessionPayload.Type }) {
      const workspaceID = yield* InstanceState.workspaceID
      if (!workspaceID) return yield* new HttpApiError.BadRequest({})

      yield* session.setWorkspace({ sessionID: ctx.payload.sessionID, workspaceID })

      yield* Effect.logInfo("sync session stolen", { sessionID: ctx.payload.sessionID, workspaceID })

      return { sessionID: ctx.payload.sessionID }
    })

    const history = Effect.fn("SyncHttpApi.history")(function* (ctx: { payload: typeof HistoryPayload.Type }) {
      const result = yield* readSyncHistory(db, ctx.payload)
      if (Object.keys(result.pruned).length > 0) {
        // The body stays the bare event array; the pruned Sessions travel in a header.
        yield* HttpEffect.appendPreResponseHandler((_request, response) =>
          Effect.succeed(HttpServerResponse.setHeader(response, PrunedSessionsHeader, JSON.stringify(result.pruned))),
        )
      }
      return result.events
    })

    return handlers.handle("start", start).handle("replay", replay).handle("steal", steal).handle("history", history)
  }),
)
