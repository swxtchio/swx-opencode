// Child side of the watcher tests' liveness protocol: one "started" line as soon
// as the child runs, then a "pong" line, answered on this JavaScript thread, for
// every "ping" line the parent writes to stdin. A parked thread stops answering.
// Lines are written synchronously: Bun buffers console output to a pipe, and a
// buffered pong would look like a parked thread.
import { writeSync } from "fs"

const line = (value: unknown) => writeSync(1, JSON.stringify(value) + "\n")

export function announce() {
  line({ type: "started" })
  process.stdin.on("data", (chunk) => {
    for (const item of chunk.toString().split("\n")) if (item === "ping") line({ type: "pong" })
  })
}

export function result(value: unknown) {
  line({ type: "result", value })
  process.exit(0)
}
