import { Workspace } from "@/control-plane/workspace"
import * as InstanceState from "@/effect/instance-state"
import { Session } from "@/session/session"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { EventV2Bridge } from "@/event-v2-bridge"
import { EventRetentionTable, EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { SessionTable } from "@opencode-ai/core/session/sql"
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
import {
  HistoryPayload,
  PrunedSessionsHeader,
  PrunedSessionsHeaderBudget,
  ReplayPayload,
  SessionPayload,
} from "../groups/sync"

/**
 * Reads every event past the caller's cursors. A Session whose history retention has redacted (pruned) contributes
 * no events, so a pruned Session never fails the read for the others; its rows are excluded in SQL and never read.
 * `pruned` describes the pruned Sessions as ascending ranges over the IDs this store holds (see prunedRanges).
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
        const unreplayable = inArray(EventRetentionTable.state, [...EventV2.unreplayableRetentionStates])
        const events = yield* tx
          .select()
          .from(EventTable)
          .where(
            and(
              past,
              notInArray(
                EventTable.aggregate_id,
                tx.select({ id: EventRetentionTable.aggregate_id }).from(EventRetentionTable).where(unreplayable),
              ),
            ),
          )
          .orderBy(asc(EventTable.seq))
          .all()
          .pipe(Effect.orDie)
        const pruned = yield* tx
          .select({ id: EventRetentionTable.aggregate_id })
          .from(EventRetentionTable)
          .where(unreplayable)
          .all()
          .pipe(Effect.orDie)
        if (pruned.length === 0) return { events, pruned: [] }
        const held = yield* Effect.all(
          [
            tx.select({ id: SessionTable.id }).from(SessionTable).all(),
            tx.select({ id: EventSequenceTable.aggregate_id }).from(EventSequenceTable).all(),
          ],
          { concurrency: 1 },
        ).pipe(Effect.orDie)
        return {
          events,
          pruned: prunedRanges(
            [...held.flat(), ...pruned].map((row) => row.id),
            new Set(pruned.map((row) => row.id)),
          ),
        }
      }),
    )
    .pipe(Effect.orDie)
}

// Characters a range endpoint may use. Every generated Session ID qualifies; an ID outside this set (callers may
// supply their own) cannot be written unambiguously in the header, so it is never an endpoint.
const rangeEndpoint = /^[A-Za-z0-9_-]+$/

/**
 * Coalesces the pruned IDs into maximal runs over every ID this store holds, in ascending string order. A range
 * never spans an unpruned ID this store holds, so it never claims that one was pruned. With ASCII endpoints the
 * IDs between them are the same in UTF-16 and byte order, so any client comparison agrees.
 */
function prunedRanges(held: readonly string[], pruned: ReadonlySet<string>) {
  const ranges = new Array<{ first: string; last: string }>()
  let open = false
  for (const id of Array.from(new Set(held)).toSorted()) {
    if (!pruned.has(id) || !rangeEndpoint.test(id)) {
      open = false
      continue
    }
    if (open) ranges[ranges.length - 1] = { first: ranges[ranges.length - 1]!.first, last: id }
    else ranges.push({ first: id, last: id })
    open = true
  }
  return ranges
}

/**
 * Writes the ascending prefix of the ranges that fits the header budget, counting the whole header line. Best
 * effort: what is listed is exact, and nothing is implied about pruned Sessions after the last listed range.
 */
function prunedSessionsHeader(ranges: readonly { first: string; last: string }[]) {
  const entries = new Array<string>()
  let bytes = PrunedSessionsHeader.length + ": \r\n".length
  for (const range of ranges) {
    const entry = range.first === range.last ? range.first : `${range.first}..${range.last}`
    const size = entry.length + (entries.length > 0 ? 1 : 0)
    if (bytes + size > PrunedSessionsHeaderBudget) break
    entries.push(entry)
    bytes += size
  }
  return entries.length > 0 ? entries.join(",") : undefined
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
      const pruned = prunedSessionsHeader(result.pruned)
      if (pruned !== undefined) {
        // The body stays the bare event array; the pruned Sessions travel in a header, exposed to allowed
        // cross-origin browser clients because it is not CORS-safelisted.
        yield* HttpEffect.appendPreResponseHandler((_request, response) =>
          Effect.succeed(
            HttpServerResponse.setHeaders(response, {
              [PrunedSessionsHeader]: pruned,
              "Access-Control-Expose-Headers": PrunedSessionsHeader,
            }),
          ),
        )
      }
      return result.events
    })

    return handlers.handle("start", start).handle("replay", replay).handle("steal", steal).handle("history", history)
  }),
)
