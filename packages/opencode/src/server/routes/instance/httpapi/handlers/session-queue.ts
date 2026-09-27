import { EventV2Bridge } from "@/event-v2-bridge"
import { SessionPrompt } from "@/session/prompt"
import { SessionQueue } from "@/session/queue"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import { NamedError } from "@opencode-ai/core/util/error"
import { Cause, Effect, Option, Scope } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import { notFound, QueueItemNotPendingError } from "../errors"
import { RestorePayload, UpdatePayload } from "../groups/session-queue"
import * as SessionError from "./session-errors"

export const sessionQueueHandlers = HttpApiBuilder.group(InstanceHttpApi, "sessionQueue", (handlers) =>
  Effect.gen(function* () {
    const session = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const promptSvc = yield* SessionPrompt.Service
    const events = yield* EventV2Bridge.Service
    const scope = yield* Scope.Scope

    const requireSession = (sessionID: SessionID) => SessionError.mapStorageNotFound(session.get(sessionID))

    const notPending = (sessionID: SessionID, itemID: SessionQueue.ItemID) =>
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
      if (Option.isNone(item)) return yield* notFound(`Withdrawn queue item not found: ${ctx.payload.id}`)
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

    return handlers
      .handle("list", list)
      .handle("withdraw", withdraw)
      .handle("restore", restore)
      .handle("update", update)
  }),
)
