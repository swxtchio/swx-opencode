// Runs the real parcel worker, but lets the test decide when its "subscribed"
// acknowledgements reach the watcher. The subscription, its acknowledgement and
// its events all come from the real worker and parcel; the test owns only the
// order in which acknowledgements are delivered.
//
// Protocol on BroadcastChannel(GATE):
//   worker -> test  { type: "held", id }      an acknowledgement is being held
//   test -> worker  { type: "release" }       deliver held acknowledgements, stop holding
//   test -> worker  { type: "error", message } report a parcel callback error for every
//                                              acknowledged subscription (see below)
import { BroadcastChannel, parentPort } from "worker_threads"
import type { Response } from "@opencode-ai/core/filesystem/parcel-worker"

export const GATE = "opencode-watcher-gate"

const port = parentPort
if (port) {
  const channel = new BroadcastChannel(GATE)
  const deliver = port.postMessage.bind(port)
  const gate = { open: false, held: [] as Response[], acknowledged: new Set<number>() }

  port.postMessage = (response: Response) => {
    if (response.type === "subscribed" && !gate.open) {
      gate.held.push(response)
      channel.postMessage({ type: "held", id: response.id })
      return
    }
    if (response.type === "subscribed") gate.acknowledged.add(response.id)
    deliver(response)
  }

  channel.onmessage = (event: { data: { type: string; message?: string } }) => {
    if (event.data.type === "release") {
      gate.open = true
      gate.held.splice(0).forEach((response) => port.postMessage(response))
    }
    // Linux inotify never reports a callback error on a subscription that keeps
    // delivering; macOS FSEvents does (swxtchio/swx-opencode#93). This stands in
    // for that one reply, in the shape parcel-worker.ts posts for it, while the
    // real subscription keeps producing events.
    if (event.data.type === "error")
      gate.acknowledged.forEach((id) => deliver({ id, type: "error", message: event.data.message ?? "" }))
  }
  channel.unref()

  await import("@opencode-ai/core/filesystem/parcel-worker")
}
