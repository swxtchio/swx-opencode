// Worker entrypoint that owns one @parcel/watcher subscription.
//
// parcel builds its shared native backend synchronously inside subscribe(), on
// the calling thread. When the kernel refuses an inotify instance there,
// InotifyBackend::start() throws before it signals that it started and
// Backend::run() waits for that signal forever. Running subscribe() here parks
// only this worker, so the server thread keeps answering and reports the watch
// as unconfirmed instead of freezing.
import type ParcelWatcher from "@parcel/watcher"
import { parentPort } from "worker_threads"
import { ParcelBinding } from "./parcel-binding"

export type Request =
  | { type: "subscribe"; directory: string; ignore: string[]; backend: ParcelWatcher.BackendType }
  | { type: "unsubscribe" }

export type Response =
  | { type: "subscribed" }
  | { type: "failed"; message: string }
  | { type: "updates"; updates: ParcelWatcher.Event[] }
  | { type: "error"; message: string }
  | { type: "unsubscribed" }

const port = parentPort
if (port) {
  const post = (response: Response) => port.postMessage(response)
  const state: { subscription?: ParcelWatcher.AsyncSubscription } = {}

  port.on("message", async (request: Request) => {
    if (request.type === "unsubscribe") {
      await state.subscription?.unsubscribe().catch(() => {})
      state.subscription = undefined
      post({ type: "unsubscribed" })
      return
    }

    const binding = ParcelBinding.load()
    if (!binding) return post({ type: "failed", message: "native @parcel/watcher binding is unavailable" })
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
        (subscription) => {
          state.subscription = subscription
          post({ type: "subscribed" })
        },
        (error: unknown) => post({ type: "failed", message: error instanceof Error ? error.message : String(error) }),
      )
  })
}
