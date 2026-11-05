import { EventWriter } from "@opencode-ai/core/event/writer"
import { spawn } from "node:child_process"

const recordFile = process.env.SWX_TUI_GROUP_RECORD_FILE
if (!recordFile) throw new Error("TUI process-group record path is missing")

const resistTerm = process.env.SWX_TUI_GROUP_RESIST_TERM === "1"
if (resistTerm) process.on("SIGTERM", () => {})

const descendant = spawn(
  process.execPath,
  [
    "-e",
    resistTerm
      ? 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1000)'
      : "setInterval(() => {}, 1000)",
  ],
  { stdio: "ignore" },
)
await new Promise<void>((resolve, reject) => {
  descendant.once("spawn", resolve)
  descendant.once("error", reject)
})

const owner = EventWriter.readIdentity(process.ppid)
const leader = EventWriter.readIdentity(process.pid)
const child = descendant.pid === undefined ? undefined : EventWriter.readIdentity(descendant.pid)
if (!owner?.bootID || !owner.startTicks) throw new Error("TUI process owner has no recorded start identity")
if (!leader?.bootID || !leader.startTicks) throw new Error("TUI shell group leader has no recorded start identity")
if (!child?.bootID || !child.startTicks) throw new Error("TUI shell group descendant has no recorded start identity")

await Bun.write(
  recordFile,
  JSON.stringify({
    owner,
    leader,
    descendant: child,
    pgid: process.pid,
  }),
)
setInterval(() => {}, 1_000)
await new Promise<void>(() => {})
