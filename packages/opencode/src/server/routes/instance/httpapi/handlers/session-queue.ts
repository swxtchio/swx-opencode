import { Config } from "@/config/config"
import { EventV2Bridge } from "@/event-v2-bridge"
import { SessionPrompt } from "@/session/prompt"
import { SessionQueue } from "@/session/queue"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import { NamedError } from "@opencode-ai/core/util/error"
import { Cause, Effect, Option, Scope } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import { QueueItemNotPendingError, QueueItemNotWithdrawnError } from "../errors"
import { HoldPayload, RemovePayload, ReorderPayload, RestorePayload, UpdatePayload } from "../groups/session-queue"
import * as SessionError from "./session-errors"

export const sessionQueueHandlers = HttpApiBuilder.group(InstanceHttpApi, "sessionQueue", (handlers) =>
  Effect.gen(function* () {
    const session = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const promptSvc = yield* SessionPrompt.Service
    const events = yield* EventV2Bridge.Service
    const scope = yield* Scope.Scope
    const config = yield* Config.Service

    const requireSession = (sessionID: SessionID) => SessionError.mapStorageNotFound(session.get(sessionID))

    const notPending = (sessionID: SessionID, itemID: string) =>
      new QueueItemNotPendingError({ sessionID, itemID, message: `Queue item is not pending: ${itemID}` })

    // Restore and send-now wake the session the way prompt_async starts one:
    // the drain runs in the background and a failure becomes a session error.
    const wake = (sessionID: SessionID) =>
      promptSvc.loop({ sessionID }).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            yield* Effect.logError("session queue wake failed", { sessionID, cause })
            yield* events.publish(Session.Event.Error, {
              sessionID,
              error: new NamedError.Unknown({ message: Cause.pretty(cause) }).toObject(),
            })
          }),
        ),
        Effect.forkIn(scope, { startImmediately: true }),
      )

    const list = Effect.fn("SessionQueueHttpApi.list")(function* (ctx: { params: { sessionID: SessionID } }) {
      yield* requireSession(ctx.params.sessionID)
      return yield* queue.list(ctx.params.sessionID)
    })

    const withdraw = Effect.fn("SessionQueueHttpApi.withdraw")(function* (ctx: {
      params: { sessionID: SessionID; itemID: SessionQueue.ItemID }
    }) {
      yield* requireSession(ctx.params.sessionID)
      const item = yield* queue.withdraw(ctx.params.sessionID, ctx.params.itemID)
      if (Option.isNone(item)) return yield* notPending(ctx.params.sessionID, ctx.params.itemID)
      return item.value
    })

    const restore = Effect.fn("SessionQueueHttpApi.restore")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof RestorePayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      const item = yield* queue.restore(ctx.params.sessionID, ctx.payload.id)
      if (Option.isNone(item))
        return yield* new QueueItemNotWithdrawnError({
          sessionID: ctx.params.sessionID,
          itemID: ctx.payload.id,
          message: `Queue item is not withdrawn: ${ctx.payload.id}`,
        })
      yield* wake(ctx.params.sessionID)
      return item.value
    })

    const update = Effect.fn("SessionQueueHttpApi.update")(function* (ctx: {
      params: { sessionID: SessionID; itemID: SessionQueue.ItemID }
      payload: typeof UpdatePayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      const item = yield* queue.update({
        sessionID: ctx.params.sessionID,
        itemID: ctx.params.itemID,
        delivery: ctx.payload.delivery,
      })
      if (Option.isNone(item)) return yield* notPending(ctx.params.sessionID, ctx.params.itemID)
      yield* wake(ctx.params.sessionID)
      return item.value
    })

    // When a review hold ends, the session is woken to deliver what it kept
    // pending. A session with nothing pending is left alone: a new one has no
    // message for a run to answer.
    const resume = (sessionID: SessionID) =>
      queue.list(sessionID).pipe(Effect.flatMap((items) => (items.length > 0 ? wake(sessionID) : Effect.void)))

    // The release that ends a hold resumes the session; one that lapses instead is
    // resumed by the watch the hold's first call starts.
    const hold = Effect.fn("SessionQueueHttpApi.hold")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof HoldPayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      const result = yield* queue.hold(ctx.params.sessionID, ctx.payload.holder)
      if (result.started)
        yield* queue.released(ctx.params.sessionID).pipe(
          Effect.flatMap((end) => (end === "lapsed" ? resume(ctx.params.sessionID) : Effect.void)),
          Effect.forkIn(scope, { startImmediately: true }),
        )
      return result.items
    })

    const release = Effect.fn("SessionQueueHttpApi.release")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof HoldPayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      if (yield* queue.release(ctx.params.sessionID, ctx.payload.holder)) yield* resume(ctx.params.sessionID)
    })

    const remove = Effect.fn("SessionQueueHttpApi.remove")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof RemovePayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      const item = yield* queue.remove(ctx.params.sessionID, ctx.payload.id)
      if (Option.isNone(item)) return yield* notPending(ctx.params.sessionID, ctx.payload.id)
      return item.value
    })

    const reorder = Effect.fn("SessionQueueHttpApi.reorder")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof ReorderPayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      const items = yield* queue.reorder(ctx.params.sessionID, ctx.payload.items)
      if (Option.isSome(items)) return items.value
      const pending = new Set((yield* queue.list(ctx.params.sessionID)).map((item) => item.id))
      return yield* notPending(
        ctx.params.sessionID,
        ctx.payload.items.find((id) => !pending.has(id)) ?? ctx.payload.items.join(","),
      )
    })

    const consolidate = Effect.fn("SessionQueueHttpApi.consolidate")(function* (ctx: {
      params: { sessionID: SessionID }
    }) {
      yield* requireSession(ctx.params.sessionID)
      return yield* queue.consolidate(ctx.params.sessionID, (yield* config.get()).machine_message_markers)
    })

    return handlers
      .handle("list", list)
      .handle("withdraw", withdraw)
      .handle("restore", restore)
      .handle("update", update)
      .handle("hold", hold)
      .handle("release", release)
      .handle("remove", remove)
      .handle("reorder", reorder)
      .handle("consolidate", consolidate)
  }),
)
