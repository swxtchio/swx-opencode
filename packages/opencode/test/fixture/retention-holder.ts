// A separate OS process that holds a retention fixture database until its stdin closes.
//   write: opens the store through the production Database and EventV2 layers and appends one durable
//          Session event, so it is a registered process that has written the Session.
//   open:  opens the store with plain SQLite and never registers, standing in for a build that predates
//          the writer record.
//   hidden: as open, after making itself non-dumpable, so the kernel refuses to list its descriptors and only
//          its SQLite lock in /proc/locks reveals it (Linux only).
//   contend <signal file>: opens the store through the production Database layer, so it is registered without
//          writing any Session, then holds BEGIN IMMEDIATE on a second connection. Once <signal file> exists it
//          admits a new unregistered holder (an "open" child), prints "holder <pid>" when that child is ready,
//          releases the write lock and prints "released".
// It prints "ready" once the hold is in place.
import { dlopen, FFIType } from "bun:ffi"
import { Database as Sqlite } from "bun:sqlite"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionV1 } from "@opencode-ai/core/v1/session"

const [filename, sessionID, mode] = process.argv.slice(2)
if (!filename || !sessionID || !["write", "open", "hidden", "contend"].includes(mode ?? ""))
  throw new Error("usage: <db> <session> <write|open|hidden|contend <signal file>>")

const released = new Promise<void>((resolve) => {
  process.stdin.on("end", () => resolve())
  process.stdin.on("close", () => resolve())
  process.stdin.resume()
})

if (mode === "hidden") {
  const libc = dlopen("libc.so.6", {
    prctl: { args: [FFIType.i32, FFIType.u64, FFIType.u64, FFIType.u64, FFIType.u64], returns: FFIType.i32 },
  })
  // PR_SET_DUMPABLE = 4
  if (libc.symbols.prctl(4, 0, 0, 0, 0) !== 0) throw new Error("prctl(PR_SET_DUMPABLE, 0) failed")
}

if (mode === "open" || mode === "hidden") {
  const db = new Sqlite(filename)
  db.query("SELECT count(*) FROM session").get()
  console.log("ready")
  await released
  db.close()
  process.exit(0)
}

if (mode === "contend") {
  const signal = process.argv[5]
  if (!signal) throw new Error("contend needs a signal file")
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // Building the production layer registers this process.
        yield* Database.Service
        const lock = new Sqlite(filename)
        lock.exec("BEGIN IMMEDIATE")
        console.log("ready")
        // Poll for the signal; the deadline is only a backstop so a broken test fails instead of hanging.
        const deadline = Date.now() + 30_000
        while (!(yield* Effect.promise(() => Bun.file(signal).exists()))) {
          if (Date.now() > deadline) throw new Error("no BEGIN IMMEDIATE signal")
          yield* Effect.sleep("5 millis")
        }
        const holder = Bun.spawn([process.execPath, import.meta.path, filename, sessionID, "open"], {
          stdin: "pipe",
          stdout: "pipe",
          stderr: "inherit",
        })
        const reader = holder.stdout.getReader()
        let output = ""
        while (!output.includes("ready")) {
          const chunk = yield* Effect.promise(() => reader.read())
          if (chunk.done) throw new Error("admitted holder exited before ready")
          output += new TextDecoder().decode(chunk.value)
        }
        console.log(`holder ${holder.pid}`)
        lock.exec("ROLLBACK")
        lock.close()
        console.log("released")
        yield* Effect.promise(() => released)
        holder.stdin.end()
        yield* Effect.promise(() => holder.exited)
      }).pipe(Effect.provide(Database.layerFromPath(filename))),
    ),
  )
  process.exit(0)
}

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      yield* events.publish(SessionV1.Event.MessageRemoved, {
        sessionID: sessionID as never,
        messageID: SessionV1.MessageID.ascending(),
      })
      console.log("ready")
      yield* Effect.promise(() => released)
    }).pipe(Effect.provide(EventV2.layerWith().pipe(Layer.provide(Database.layerFromPath(filename))))),
  ),
)
process.exit(0)
