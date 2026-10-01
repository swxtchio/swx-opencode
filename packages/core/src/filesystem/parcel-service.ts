export * as ParcelService from "./parcel-service"

// The parcel subscription service run by parcel-worker.ts; see that file for why
// it runs on its own thread.
import type ParcelWatcher from "@parcel/watcher"
import { ParcelBinding } from "./parcel-binding"

export type Request = { id: number } & (
  | { type: "subscribe"; directory: string; ignore: string[]; backend: ParcelWatcher.BackendType }
  | { type: "unsubscribe" }
)

export type Reply =
  | { type: "subscribed" }
  | { type: "failed"; message: string }
  | { type: "updates"; updates: ParcelWatcher.Event[] }
  | { type: "error"; message: string }
  | { type: "unsubscribed" }

export type Response = { id: number } & Reply

// The side of a worker_threads MessagePort that serve() uses.
export interface Port {
  on(event: "message", listener: (request: Request) => void): unknown
  postMessage(response: Response): void
}

// Serves parcel subscriptions to the watcher over `port`.
export function serve(port: Port) {
  const subscriptions = new Map<number, ParcelWatcher.AsyncSubscription>()
  // Ids unsubscribed before their subscribe() settled, so a late subscription
  // is released instead of leaking.
  const cancelled = new Set<number>()
  const pending = new Set<number>()

  port.on("message", async (request) => {
    const post = (reply: Reply) => port.postMessage({ id: request.id, ...reply } satisfies Response)
    if (request.type === "unsubscribe") {
      const subscription = subscriptions.get(request.id)
      if (pending.has(request.id)) cancelled.add(request.id)
      await subscription?.unsubscribe().catch(() => {})
      subscriptions.delete(request.id)
      post({ type: "unsubscribed" })
      return
    }

    const binding = ParcelBinding.load()
    if (!binding) return post({ type: "failed", message: "native @parcel/watcher binding is unavailable" })
    pending.add(request.id)
    await binding
      .subscribe(
        request.directory,
        (error, updates) => {
          if (error) post({ type: "error", message: error.message })
          if (updates.length)
            post({ type: "updates", updates: updates.map((item) => ({ type: item.type, path: item.path })) })
        },
        { ignore: request.ignore, backend: request.backend },
      )
      .then(
        async (subscription) => {
          pending.delete(request.id)
          if (cancelled.delete(request.id)) return subscription.unsubscribe().catch(() => {})
          subscriptions.set(request.id, subscription)
          post({ type: "subscribed" })
        },
        (error: unknown) => {
          pending.delete(request.id)
          cancelled.delete(request.id)
          post({ type: "failed", message: error instanceof Error ? error.message : String(error) })
        },
      )
  })
}
