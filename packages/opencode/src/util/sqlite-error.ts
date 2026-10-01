import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { Cause, Option } from "effect"
import { isSqlError } from "effect/unstable/sql/SqlError"

export function sqliteLockMessage(error: unknown) {
  const drizzleCause = error instanceof EffectDrizzleQueryError ? error.cause : undefined
  const drizzleError = Cause.isCause(drizzleCause)
    ? Option.getOrUndefined(Cause.findErrorOption(drizzleCause))
    : undefined
  const sqlError = isSqlError(error) ? error : isSqlError(drizzleError) ? drizzleError : undefined
  const lockReason = sqlError?.reason._tag === "LockTimeoutError" ? sqlError.reason : undefined
  if (!lockReason) return

  const lockCause = lockReason.cause
  const locked =
    typeof lockCause === "object" &&
    lockCause !== null &&
    (("code" in lockCause && typeof lockCause.code === "string" && lockCause.code.startsWith("SQLITE_LOCKED")) ||
      ("code" in lockCause && typeof lockCause.code === "number" && (lockCause.code & 0xff) === 6) ||
      ("errcode" in lockCause && typeof lockCause.errcode === "number" && (lockCause.errcode & 0xff) === 6))
  return locked ? "Database is locked (SQLITE_LOCKED)" : "Database is locked (SQLITE_BUSY)"
}
