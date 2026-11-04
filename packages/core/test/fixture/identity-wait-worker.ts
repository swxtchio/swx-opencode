import { createInterface } from "node:readline"

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity })

process.stdout.write(`${JSON.stringify({ type: "ready" })}\n`)
lines.on("line", (line) => {
  const command = JSON.parse(line) as { type: string }
  if (command.type !== "release-refresh") return
  lines.close()
  process.stdin.pause()
  process.exit(0)
})
