import { SessionQueue } from "@/session/queue"
import { SessionID } from "@/session/schema"
import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiError, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/unstable/httpapi"
import { Authorization } from "../middleware/authorization"
import { InstanceContextMiddleware } from "../middleware/instance-context"
import { WorkspaceRoutingMiddleware, WorkspaceRoutingQuery } from "../middleware/workspace-routing"
import { ApiNotFoundError, QueueItemNotPendingError, QueueItemNotWithdrawnError } from "../errors"
import { described } from "./metadata"

// Fork-owned V1 prompt queue routes (swxtchio/swx-opencode#68). Deliberately
// not upstream v2's /api/session/:id/inbox.
const root = "/session/:sessionID/queue"
export const SessionQueuePaths = {
  list: root,
  withdraw: `${root}/:itemID`,
  update: `${root}/:itemID`,
  rewrite: `${root}/rewrite/:itemID`,
  restore: `${root}/restore`,
  hold: `${root}/hold`,
  release: `${root}/release`,
  remove: `${root}/remove`,
  reorder: `${root}/reorder`,
  consolidate: `${root}/consolidate`,
  consolidateSelected: `${root}/consolidate/:itemID`,
  force: `${root}/force/:itemID`,
  holdStatus: `${root}/hold/:holder`,
  receipt: `${root}/receipt/:requestID`,
} as const

export const RestorePayload = Schema.Struct({ id: SessionQueue.ItemID })
export const UpdatePayload = Schema.Struct({ delivery: SessionQueue.Delivery })
export const HoldPayload = Schema.Struct({ holder: Schema.String.check(Schema.isNonEmpty()) })
export const RewritePayload = Schema.Struct({ expectedText: Schema.String, text: Schema.String })
export const RemovePayload = Schema.Struct({ id: SessionQueue.ItemID })
export const ReorderPayload = Schema.Struct({ items: Schema.Array(SessionQueue.ItemID) })
export const ForceResult = Schema.Struct({
  outcome: Schema.Literals(["forced", "another-review", "hold-unconfirmed", "preparing", "unavailable"]),
  items: Schema.Array(SessionQueue.Item),
})
export const HoldStatusResult = Schema.Struct({ held: Schema.Boolean, items: Schema.Array(SessionQueue.Item) })
export const RewriteResult = Schema.Struct({
  outcome: Schema.Literals(["rewritten", "stale", "preparing", "unavailable", "no-text", "empty", "missing-reference"]),
  items: Schema.Array(SessionQueue.Item),
})
export const SelectedConsolidationResult = Schema.Struct({
  outcome: Schema.Literals(["consolidated", "unchanged", "preparing", "incompatible", "unavailable"]),
  items: Schema.Array(SessionQueue.Item),
})

export const SessionQueueApi = HttpApi.make("sessionQueue").add(
  HttpApiGroup.make("sessionQueue")
    .add(
      HttpApiEndpoint.get("list", SessionQueuePaths.list, {
        params: { sessionID: SessionID },
        query: WorkspaceRoutingQuery,
        success: described(Schema.Array(SessionQueue.Item), "Pending queue items in delivery order"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.list",
          summary: "List queued prompts",
          description:
            "List the prompts admitted to a session that have not reached the model yet: pending steers first, then queued prompts, each in the order it is delivered.",
        }),
      ),
      HttpApiEndpoint.delete("withdraw", SessionQueuePaths.withdraw, {
        params: { sessionID: SessionID, itemID: SessionQueue.ItemID },
        query: WorkspaceRoutingQuery,
        success: described(SessionQueue.Item, "Withdrawn queue item"),
        error: [HttpApiError.BadRequest, ApiNotFoundError, QueueItemNotPendingError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.withdraw",
          summary: "Withdraw queued prompt",
          description:
            "Take a pending prompt out of the queue, for example to edit it. Fails with QueueItemNotPending when it was already delivered, withdrawn or never existed. A withdrawn prompt can be restored.",
        }),
      ),
      HttpApiEndpoint.post("restore", SessionQueuePaths.restore, {
        params: { sessionID: SessionID },
        query: WorkspaceRoutingQuery,
        payload: RestorePayload,
        success: described(SessionQueue.Item, "Restored queue item"),
        error: [HttpApiError.BadRequest, ApiNotFoundError, QueueItemNotWithdrawnError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.restore",
          summary: "Restore withdrawn prompt",
          description:
            "Return a withdrawn prompt to the queue with its original id and position, and wake the session so it is delivered. Fails with QueueItemNotWithdrawn when the item is not withdrawn: still pending, already delivered or never existed.",
        }),
      ),
      HttpApiEndpoint.patch("update", SessionQueuePaths.update, {
        params: { sessionID: SessionID, itemID: SessionQueue.ItemID },
        query: WorkspaceRoutingQuery,
        payload: UpdatePayload,
        success: described(SessionQueue.Item, "Updated queue item"),
        error: [HttpApiError.BadRequest, ApiNotFoundError, QueueItemNotPendingError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.update",
          summary: "Change queued prompt delivery",
          description:
            "Change how a pending prompt is delivered and wake the session. Setting steer sends a queued prompt at the next step.",
        }),
      ),
      HttpApiEndpoint.post("rewrite", SessionQueuePaths.rewrite, {
        params: { sessionID: SessionID, itemID: SessionQueue.ItemID },
        query: WorkspaceRoutingQuery,
        payload: RewritePayload,
        success: described(RewriteResult, "Rewrite result and current pending prompts"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.rewrite",
          summary: "Rewrite a queued prompt's text",
          description:
            "Replace editable text only when the prepared pending item's editable text still matches the expected draft. Preserves the queue item and receipts; returns stale, preparing or unavailable without changing the text when the row cannot be rewritten.",
        }),
      ),
      HttpApiEndpoint.post("force", SessionQueuePaths.force, {
        params: { sessionID: SessionID, itemID: SessionQueue.ItemID },
        query: WorkspaceRoutingQuery,
        payload: HoldPayload,
        success: described(ForceResult, "Force result and current pending prompts"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.force",
          summary: "Force a selected queued prompt",
          description:
            "Give only this prepared pending prompt steer timing. The holder must own a confirmed review hold, which this action releases only when no other live review hold remains. A preparing result leaves the item pending; an unavailable result reports a stale or missing item, and another-review leaves it unchanged. A forced result wakes the session so it reaches the next safe provider step or starts a turn when idle.",
        }),
      ),
      HttpApiEndpoint.get("holdStatus", SessionQueuePaths.holdStatus, {
        params: { sessionID: SessionID, holder: Schema.String.check(Schema.isNonEmpty()) },
        query: WorkspaceRoutingQuery,
        success: described(HoldStatusResult, "Current review-hold state and pending prompts"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.hold_status",
          summary: "Inspect a review hold",
          description:
            "Read whether this holder currently owns a live review hold and list the current pending prompts. This does not acquire or renew the hold.",
        }),
      ),
      HttpApiEndpoint.post("hold", SessionQueuePaths.hold, {
        params: { sessionID: SessionID },
        query: WorkspaceRoutingQuery,
        payload: HoldPayload,
        success: described(Schema.Array(SessionQueue.Item), "Pending queue items in delivery order"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.hold",
          summary: "Hold queued prompts for review",
          description:
            "Start or renew a review hold for this holder. While a session has a hold, none of its queued prompts reaches the model, so the returned list stays as it is until the hold ends. A hold the holder does not renew lapses after six minutes.",
        }),
      ),
      HttpApiEndpoint.post("release", SessionQueuePaths.release, {
        params: { sessionID: SessionID },
        query: WorkspaceRoutingQuery,
        payload: HoldPayload,
        success: described(HttpApiSchema.NoContent, "Hold released"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.release",
          summary: "Release a review hold",
          description:
            "End this holder's review hold. Once no hold is left, the session is woken and its queued prompts are delivered.",
        }),
      ),
      HttpApiEndpoint.post("remove", SessionQueuePaths.remove, {
        params: { sessionID: SessionID },
        query: WorkspaceRoutingQuery,
        payload: RemovePayload,
        success: described(SessionQueue.Item, "Deleted queue item"),
        error: [HttpApiError.BadRequest, ApiNotFoundError, QueueItemNotPendingError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.remove",
          summary: "Delete queued prompt",
          description:
            "Delete a pending prompt for good; unlike a withdrawn one it cannot be restored. Fails with QueueItemNotPending when it was already delivered, withdrawn or never existed.",
        }),
      ),
      HttpApiEndpoint.post("reorder", SessionQueuePaths.reorder, {
        params: { sessionID: SessionID },
        query: WorkspaceRoutingQuery,
        payload: ReorderPayload,
        success: described(Schema.Array(SessionQueue.Item), "Pending queue items in delivery order"),
        error: [HttpApiError.BadRequest, ApiNotFoundError, QueueItemNotPendingError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.reorder",
          summary: "Reorder queued prompts",
          description:
            "Deliver the listed pending prompts in the listed order. Steers still come before queued prompts, and no prompt changes how it is delivered. Fails with QueueItemNotPending, changing nothing, when any of them is not pending.",
        }),
      ),
      HttpApiEndpoint.post("consolidate", SessionQueuePaths.consolidate, {
        params: { sessionID: SessionID },
        query: WorkspaceRoutingQuery,
        success: described(Schema.Array(SessionQueue.Item), "Pending queue items in delivery order"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.consolidate",
          summary: "Consolidate queued prompts",
          description:
            "Consolidate pending prompts only within one delivery class: merge plain text whose case and whitespace normalized text is equal or one is a prefix of the other, and merge compatible commands with the same leading verb and a containing or distinctive-token-overlapping target into numbered steps in delivery order.",
        }),
      ),
      HttpApiEndpoint.post("consolidateSelected", SessionQueuePaths.consolidateSelected, {
        params: { sessionID: SessionID, itemID: SessionQueue.ItemID },
        query: WorkspaceRoutingQuery,
        success: described(SelectedConsolidationResult, "Selected consolidation result and current pending prompts"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.consolidate_selected",
          summary: "Consolidate the selected queued prompt",
          description:
            "When the selected item is a human-composer prompt, consolidate every ready pending human-composer prompt with compatible delivery and execution context in admission order, preserving each original input and receipt. Preparing or incompatible human sets remain intact. A non-human selection uses the standard queue consolidation behavior.",
        }),
      ),
      HttpApiEndpoint.get("receipt", SessionQueuePaths.receipt, {
        params: { sessionID: SessionID, requestID: SessionQueue.RequestID },
        query: WorkspaceRoutingQuery,
        success: described(SessionQueue.Receipt, "What became of the request"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "session.queue.receipt",
          summary: "Get a request's receipt",
          description:
            "Say what became of a prompt sent with this request ID: pending in the queue, delivered as a message, merged into another request's prompt, superseded by a newer request, or deleted undelivered. Receipts last as long as the session. Fails with NotFound when the session has no such request.",
        }),
      ),
    )
    .annotateMerge(
      OpenApi.annotations({
        title: "session queue",
        description: "Prompts admitted to a session that have not reached the model yet.",
      }),
    )
    .middleware(InstanceContextMiddleware)
    .middleware(WorkspaceRoutingMiddleware)
    .middleware(Authorization),
)
