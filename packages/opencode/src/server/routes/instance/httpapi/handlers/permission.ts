import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { AskRecovery } from "@/ask/recovery"
import type { AskLedger } from "@/ask/ledger"
import { Permission } from "@/permission"
import type { SessionAsk } from "@opencode-ai/schema/session-ask"
import { Effect } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import { AskUnavailableError, PermissionNotFoundError } from "../errors"

export const permissionHandlers = HttpApiBuilder.group(InstanceHttpApi, "permission", (handlers) =>
  Effect.gen(function* () {
    const svc = yield* Permission.Service
    const recovery = yield* AskRecovery.Service

    const list = Effect.fn("PermissionHttpApi.list")(function* () {
      return yield* svc.list()
    })

    const reply = Effect.fn("PermissionHttpApi.reply")(function* (ctx: {
      params: { requestID: PermissionV1.ID }
      payload: PermissionV1.ReplyBody
    }) {
      const settled = yield* settlePermission(svc, recovery, { requestID: ctx.params.requestID, ...ctx.payload })
      if (settled.disposition === "settled") return true
      return yield* new PermissionNotFoundError({
        requestID: settled.requestID,
        message: `Permission request already settled: ${settled.requestID}`,
      })
    })

    const answer = Effect.fn("PermissionHttpApi.answer")(function* (ctx: {
      params: { requestID: PermissionV1.ID }
      payload: PermissionV1.ReplyBody
    }) {
      const settled = yield* settlePermission(svc, recovery, { requestID: ctx.params.requestID, ...ctx.payload })
      return {
        requestID: settled.requestID,
        sessionID: settled.sessionID,
        disposition: settled.disposition,
        decision: settled.decision,
        recovered: settled.recovered,
      } satisfies SessionAsk.Result
    })

    return handlers.handle("list", list).handle("reply", reply).handle("answer", answer)
  }),
)

// Settles through the service, and recovery settles and applies a decision the service handed back. Shared with the
// deprecated session permission route.
export const settlePermission = Effect.fn("PermissionHttpApi.settle")(function* (
  svc: Permission.Interface,
  recovery: AskRecovery.Interface,
  input: PermissionV1.ReplyInput,
) {
  return yield* svc.reply(input).pipe(
    Effect.flatMap(recovery.apply),
    Effect.catchTag("Permission.NotFoundError", (error) =>
      Effect.fail(
        new PermissionNotFoundError({
          requestID: String(error.requestID),
          message: `Permission request not found: ${error.requestID}`,
        }),
      ),
    ),
    Effect.catchTag("SessionAsk.UnavailableError", (error) =>
      Effect.fail(
        new AskUnavailableError({ requestID: error.requestID, reason: error.reason, message: error.message }),
      ),
    ),
  )
})
