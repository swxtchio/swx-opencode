// Runs the real parcel subscription service, but lets the test decide when its
// "subscribed" acknowledgements reach the watcher. The subscription, its
// acknowledgement and its events all come from the real service and parcel; the
// test owns only the order in which acknowledgements are delivered.
//
// Protocol on BroadcastChannel(GATE):
//   worker -> test  { type: "held", id }      an acknowledgement is being held
//   test -> worker  { type: "release" }       deliver held acknowledgements, stop holding
//   test -> worker  { type: "error", message } report a parcel callback error for every
//                                              acknowledged subscription (see below)
import { BroadcastChannel, parentPort } from "worker_threads"
import { ParcelService } from "@opencode-ai/core/filesystem/parcel-service"

export const GATE = "opencode-watcher-gate"

const port = parentPort
if (port) {
  const channel = new BroadcastChannel(GATE)
  const gate = { open: false, held: [] as ParcelService.Response[], acknowledged: new Set<number>() }
  const deliver = (response: ParcelService.Response) => {
    if (response.type === "subscribed") gate.acknowledged.add(response.id)
    port.postMessage(response)
  }

  channel.onmessage = (event: { data: { type: string; message?: string } }) => {
    if (event.data.type === "release") {
      gate.open = true
      gate.held.splice(0).forEach(deliver)
    }
    // Linux inotify never reports a callback error on a subscription that keeps
    // delivering; macOS FSEvents does (swxtchio/swx-opencode#93). This stands in
    // for that one reply, in the shape the service posts for it, while the real
    // subscription keeps producing events.
    if (event.data.type === "error")
      gate.acknowledged.forEach((id) => port.postMessage({ id, type: "error", message: event.data.message ?? "" }))
  }
  channel.unref()

  // The service sees an ordinary port; only its acknowledgements pass the gate.
  ParcelService.serve({
    on: (event, listener) => port.on(event, listener),
    postMessage: (response) => {
      if (response.type !== "subscribed" || gate.open) return deliver(response)
      gate.held.push(response)
      channel.postMessage({ type: "held", id: response.id })
    },
  })
}
