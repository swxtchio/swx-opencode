import fs from "node:fs/promises"
import path from "node:path"
import { FileFinder } from "@ff-labs/fff-bun"

const root = process.argv[2]
if (!root) throw new Error("missing FFF fixture root")
const picker = FileFinder.create({
  basePath: path.join(root, "project"),
  frecencyDbPath: path.join(root, "db", "frecency.mdb"),
  historyDbPath: path.join(root, "db", "history.mdb"),
})
if (!picker.ok) throw new Error(picker.error)
const ready = await picker.value.waitForIndexReady(10_000)
if (!ready.ok || !ready.value) throw new Error(`index failed to become ready: ${JSON.stringify(ready)}`)
let progress = picker.value.getScanProgress()
if (!progress.ok) throw new Error(progress.error)
const deadline = Date.now() + 10_000
while (!progress.value.isWatcherReady && Date.now() < deadline) {
  await Bun.sleep(25)
  progress = picker.value.getScanProgress()
  if (!progress.ok) throw new Error(progress.error)
}
if (!progress.value.isWatcherReady) throw new Error("FFF watcher did not become ready")
if (process.platform === "linux") {
  const tasks = await fs.readdir("/proc/self/task")
  const names = await Promise.all(
    tasks.map((task) => fs.readFile(`/proc/self/task/${task}/comm`, "utf8").catch(() => "")),
  )
  if (!names.some((name) => name.trim().startsWith("notify-rs"))) throw new Error("native FFF watcher thread not found")
}
console.log("watcher-ready=true; forcing an abort in the native-owning subject")
// This disposable process models the fatal owner outcome without reproducing the native panic site.
process.abort()
