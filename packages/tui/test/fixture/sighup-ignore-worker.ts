import { EventWriter } from "@opencode-ai/core/event/writer"
import { registerTerminalSighup } from "../../src/terminal-signal"

const readyFile = process.env.SWX_TUI_SIGHUP_READY_FILE
const resultFile = process.env.SWX_TUI_SIGHUP_RESULT_FILE
const acknowledgementFile = process.env.SWX_TUI_SIGHUP_ACK_FILE
if (!readyFile || !resultFile || !acknowledgementFile) throw new Error("SIGHUP fixture paths are missing")

const identity = EventWriter.readIdentity(process.pid)
if (!identity?.bootID || !identity.startTicks) throw new Error("nohup SIGHUP worker has no start identity")
const previousSighupListeners = new Set(process.listeners("SIGHUP"))
let signalHandled = 0
const unregister = registerTerminalSighup(() => signalHandled++)
const acknowledgement = Promise.withResolvers<void>()
const shutdown = Promise.withResolvers<void>()

process.once("SIGUSR1", () => {
  void Bun.write(acknowledgementFile, "ready").then(
    () => acknowledgement.resolve(),
    (error) => acknowledgement.reject(error),
  )
})
process.once("SIGUSR2", () => shutdown.resolve())

await Bun.write(
  readyFile,
  JSON.stringify({
    pid: identity.pid,
    stdinTTY: process.stdin.isTTY === true,
    stdoutTTY: process.stdout.isTTY === true,
    terminal: process.stdin.isTTY === true || process.stdout.isTTY === true,
    sighupHandlerAdded: process.listeners("SIGHUP").some((listener) => !previousSighupListeners.has(listener)),
  }),
)
await acknowledgement.promise
await shutdown.promise
unregister()
await Bun.write(
  resultFile,
  JSON.stringify({
    rendererDestroyed: false,
    disposed: 0,
    sighupListenersRestored: process.listeners("SIGHUP").every((listener) => previousSighupListeners.has(listener)),
    terminalTitleCleared: false,
    signalHandled,
  }),
)
