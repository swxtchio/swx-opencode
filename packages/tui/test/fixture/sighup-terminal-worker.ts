import { createTestRenderer } from "@opentui/core/testing"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { Global } from "@opencode-ai/core/global"
import { Effect } from "effect"
import { run } from "../../src/app"
import { createTuiResolvedConfig } from "./tui-runtime"
import { createEventSource, createFetch, directory } from "./tui-sdk"

const readyFile = process.env.SWX_TUI_SIGHUP_READY_FILE
const resultFile = process.env.SWX_TUI_SIGHUP_RESULT_FILE
if (!readyFile || !resultFile) throw new Error("terminal SIGHUP worker paths are missing")

const setup = await createTestRenderer({ width: 80, height: 24, useThread: false })
const previousSighupListeners = new Set(process.listeners("SIGHUP"))
const titles: string[] = []
const setTitle = setup.renderer.setTerminalTitle.bind(setup.renderer)
setup.renderer.setTerminalTitle = (title) => {
  titles.push(title)
  setTitle(title)
}
const started = Promise.withResolvers<void>()
let disposed = 0
const task = Effect.runPromise(
  run({
    url: "http://test",
    directory,
    config: createTuiResolvedConfig({ plugin_enabled: {} }),
    fetch: createFetch().fetch,
    events: createEventSource().source,
    args: {},
    pluginHost: {
      async start() {
        const identity = EventWriter.readIdentity(process.pid)
        if (!identity?.bootID || !identity.startTicks)
          throw new Error("terminal SIGHUP worker has no start identity")
        await Bun.write(
          readyFile,
          JSON.stringify({
            pid: process.pid,
            stdinTTY: process.stdin.isTTY === true,
            stdoutTTY: process.stdout.isTTY === true,
            terminal: process.stdin.isTTY === true || process.stdout.isTTY === true,
            sighupHandlerAdded: process.listeners("SIGHUP").some((listener) => !previousSighupListeners.has(listener)),
          }),
        )
        started.resolve()
      },
      async dispose() {
        disposed++
      },
    },
    createRenderer: async () => setup.renderer,
  }).pipe(Effect.provide(AppNodeBuilder.build(Global.node))),
)

try {
  await started.promise
  await task
  await Bun.write(
    resultFile,
    JSON.stringify({
      rendererDestroyed: setup.renderer.isDestroyed,
      disposed,
      sighupListenersRestored: process.listeners("SIGHUP").every((listener) => previousSighupListeners.has(listener)),
      terminalTitleCleared: titles.at(-1) === "",
    }),
  )
} finally {
  if (!setup.renderer.isDestroyed) setup.renderer.destroy()
}
