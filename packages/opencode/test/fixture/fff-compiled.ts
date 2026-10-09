import { FileFinder } from "@ff-labs/fff-bun"

const basePath = process.argv[2]
const mode = process.argv[3]
if (!basePath || (mode !== "complete" && mode !== "hold")) throw new Error("expected base path and mode")

const created = FileFinder.create({
  basePath,
  aiMode: true,
  disableMmapCache: true,
  disableContentIndexing: true,
  disableWatch: true,
})
if (!created.ok) throw new Error(created.error)

const scanned = await created.value.waitForScan(15_000)
if (!scanned.ok || !scanned.value) throw new Error("FFF scan did not finish")

const file = created.value.fileSearch("needle-target", { pageSize: 10 })
if (!file.ok || !file.value.items.some((item) => item.relativePath.endsWith("needle-target.ts"))) {
  throw new Error("FFF file search did not find the indexed file")
}

const directory = created.value.directorySearch("nested", { pageSize: 10 })
if (!directory.ok || !directory.value.items.some((item) => item.relativePath.includes("nested"))) {
  throw new Error("FFF directory search did not find the indexed directory")
}

const glob = created.value.glob("src/nested/*.ts", { pageSize: 10 })
if (!glob.ok || !glob.value.items.some((item) => item.relativePath.endsWith("needle-target.ts"))) {
  throw new Error("FFF glob did not find the indexed file")
}

const grep = created.value.grep("picker-content", { mode: "plain", pageSize: 10 })
if (!grep.ok || !grep.value.items.some((item) => item.lineContent.includes("picker-content"))) {
  throw new Error("FFF grep did not find the indexed content")
}

if (mode === "hold") {
  process.stdout.write("ready\n")
  setInterval(() => {}, 1_000)
  await new Promise(() => {})
}

created.value.destroy()
process.stdout.write("ready\n")
