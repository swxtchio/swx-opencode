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
import { parentPort } from "worker_threads"
import { ParcelService } from "./parcel-service"

if (parentPort) ParcelService.serve(parentPort)
