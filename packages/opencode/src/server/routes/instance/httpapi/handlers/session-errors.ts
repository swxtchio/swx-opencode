import { NotFoundError as StorageNotFoundError } from "@/storage/storage"
import type { Session } from "@/session/session"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Effect } from "effect"
import * as ApiError from "../errors"

export function mapStorageNotFound<A, R>(self: Effect.Effect<A, StorageNotFoundError, R>) {
  return self.pipe(Effect.mapError((error) => ApiError.notFound(error.message)))
}

export function mapBusy<A, R>(self: Effect.Effect<A, Session.BusyError, R>) {
  return self.pipe(
    Effect.catchTag("SessionBusyError", (error) =>
      Effect.fail(
        new ApiError.SessionBusyError({
          sessionID: error.sessionID,
          message: `Session is busy: ${error.sessionID}`,
        }),
      ),
    ),
  )
}

/**
 * Maps exactly two defects to the route's not-found answer, both meaning this Session was removed while the request
 * was writing to it: `SessionProjector.SessionNotProjected`, raised when a write's commit lands after removal, and a
 * storage `NotFoundError` that a V1 setter's `orDie` turned into a defect when its own read found the Session gone.
 * Typed failures and every other defect pass through unchanged.
 */
export function mapRemovedDuringWrite<A, E, R>(self: Effect.Effect<A, E, R>) {
  return self.pipe(
    Effect.catchDefect((defect) => {
      if (defect instanceof SessionProjector.SessionNotProjected)
        return Effect.fail(ApiError.notFound(`Session not found: ${defect.sessionID}`))
      if (StorageNotFoundError.isInstance(defect)) return Effect.fail(ApiError.notFound(defect.message))
      return Effect.die(defect)
    }),
  )
}
