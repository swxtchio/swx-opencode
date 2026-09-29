import { NamedError } from "@opencode-ai/core/util/error"
import { ConfigErrorV1 } from "@opencode-ai/core/v1/config/error"
import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { Cause, Effect, Option } from "effect"
import { isSqlError } from "effect/unstable/sql/SqlError"
import { HttpRouter, HttpServerError, HttpServerRespondable, HttpServerResponse } from "effect/unstable/http"

// Keep typed HttpApi failures on their declared error path; this boundary only replaces defect-only empty 500s.
export const errorLayer = HttpRouter.middleware<{ handles: unknown }>()((effect) =>
  effect.pipe(
    Effect.catchCause((cause) => {
      const defect = cause.reasons.filter(Cause.isDieReason).find((reason) => {
        if (HttpServerResponse.isHttpServerResponse(reason.defect)) return false
        if (HttpServerError.isHttpServerError(reason.defect)) return false
        if (HttpServerRespondable.isRespondable(reason.defect)) return false
        return true
      })
      if (!defect) return Effect.failCause(cause)

      const error = defect.defect
      if (
        ConfigErrorV1.JsonError.isInstance(error) ||
        ConfigErrorV1.InvalidError.isInstance(error) ||
        ConfigErrorV1.FrontmatterError.isInstance(error) ||
        ConfigErrorV1.DirectoryTypoError.isInstance(error) ||
        ConfigErrorV1.RemoteAuthError.isInstance(error)
      ) {
        return Effect.succeed(HttpServerResponse.jsonUnsafe(error.toObject(), { status: 400 }))
      }

      const ref = `err_${crypto.randomUUID().slice(0, 8)}`
      const drizzleCause = error instanceof EffectDrizzleQueryError ? error.cause : undefined
      const drizzleError = Cause.isCause(drizzleCause)
        ? Option.getOrUndefined(Cause.findErrorOption(drizzleCause))
        : undefined
      const sqlError = isSqlError(error) ? error : isSqlError(drizzleError) ? drizzleError : undefined
      const lockReason = sqlError?.reason._tag === "LockTimeoutError" ? sqlError.reason : undefined
      const lockCause = lockReason?.cause
      const locked =
        typeof lockCause === "object" &&
        lockCause !== null &&
        (("code" in lockCause && typeof lockCause.code === "string" && lockCause.code.startsWith("SQLITE_LOCKED")) ||
          ("code" in lockCause && typeof lockCause.code === "number" && (lockCause.code & 0xff) === 6) ||
          ("errcode" in lockCause && typeof lockCause.errcode === "number" && (lockCause.errcode & 0xff) === 6))

      return Effect.logError("failed", { ref, error, cause: Cause.pretty(cause) }).pipe(
        Effect.as(
          HttpServerResponse.jsonUnsafe(
            new NamedError.Unknown({
              message: lockReason
                ? locked
                  ? "Database is locked (SQLITE_LOCKED)"
                  : "Database is locked (SQLITE_BUSY)"
                : "Unexpected server error. Check server logs for details.",
              ref,
            }).toObject(),
            { status: 500 },
          ),
        ),
      )
    }),
  ),
).layer
