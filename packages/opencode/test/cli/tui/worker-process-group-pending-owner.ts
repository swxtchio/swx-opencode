import { ProcessGroup } from "../../../src/cli/tui/process-group"
import type { EventWriter } from "@opencode-ai/core/event/writer"

const pgid = Number(process.env.SWX_TUI_PENDING_PGID)
const readyFile = process.env.SWX_TUI_PENDING_READY_FILE
if (!Number.isSafeInteger(pgid) || pgid <= 0 || !readyFile) throw new Error("pending owner fixture arguments are invalid")

let receive: ((event: ProcessGroup.Event) => void) | undefined
let tracker: ProcessGroup.WorkerTracker | undefined
const worker = { onerror: null as ((event: ErrorEvent) => unknown) | null, terminate: () => {} }

void ProcessGroup.makeOwner({
  client: {
    subscribe(handler) {
      receive = handler
      return () => {
        receive = undefined
      }
    },
    acknowledge(eventID) {
      return Promise.resolve(tracker?.acknowledge(eventID) ?? false)
    },
    shutdown: async () => undefined,
  },
  worker,
})

tracker = ProcessGroup.makeWorkerTracker(
  (event) => receive?.(event),
  5_000,
  () =>
    new Promise<{ members: EventWriter.ProcessIdentity[]; complete: boolean }>(() => {}),
)
void tracker.observe({
  action: "acquire",
  pgid,
  cleanupOwed: true,
  identity: { pid: pgid, bootID: null, startTicks: null },
  members: [],
  membersPending: true,
  membersSnapshotComplete: false,
  generation: 1,
})
await Bun.write(readyFile, "ready")
process.exit(0)
