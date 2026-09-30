// Worker entrypoint that owns every @parcel/watcher subscription in the process.
//
// parcel builds its shared native backend synchronously inside subscribe(), on
// the calling thread. When the kernel refuses an inotify instance there,
// InotifyBackend::start() throws before it signals that it started and
// Backend::run() waits for that signal forever. Running subscribe() here parks
// only this worker, so the server thread keeps answering and reports the watch
// as unconfirmed instead of freezing.
//
// One worker serves the whole process because parcel's shared-backend registry
// is not synchronized: subscriptions from several threads could race on it.
import type ParcelWatcher from "@parcel/watcher"
import { parentPort } from "worker_threads"
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

const port = parentPort
if (port) {
  const subscriptions = new Map<number, ParcelWatcher.AsyncSubscription>()
  // Ids unsubscribed before their subscribe() settled, so a late subscription
  // is released instead of leaking.
  const cancelled = new Set<number>()
  const pending = new Set<number>()

  port.on("message", async (request: Request) => {
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
