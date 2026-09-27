import { SessionQueue } from "@/session/queue"
import { SessionID } from "@/session/schema"
import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiError, HttpApiGroup, OpenApi } from "effect/unstable/httpapi"
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
  restore: `${root}/restore`,
} as const

export const RestorePayload = Schema.Struct({ id: SessionQueue.ItemID })
export const UpdatePayload = Schema.Struct({ delivery: SessionQueue.Delivery })

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
            "List the prompts admitted to a session that have not reached the model yet: pending steers first, then queued prompts, each in admission order.",
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
