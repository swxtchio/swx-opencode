// A separate OS process that holds a retention fixture database until its stdin closes.
//   write: opens the store through the production Database and EventV2 layers and appends one durable
//          Session event, so it is a registered process that has written the Session.
//   open:  opens the store with plain SQLite and never registers, standing in for a build that predates
//          the writer record.
// It prints "ready" once the hold is in place.
import { Database as Sqlite } from "bun:sqlite"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionV1 } from "@opencode-ai/core/v1/session"

const [filename, sessionID, mode] = process.argv.slice(2)
if (!filename || !sessionID || (mode !== "write" && mode !== "open")) throw new Error("usage: <db> <session> <mode>")

const released = new Promise<void>((resolve) => {
  process.stdin.on("end", () => resolve())
  process.stdin.on("close", () => resolve())
  process.stdin.resume()
})

if (mode === "open") {
  const db = new Sqlite(filename)
  db.query("SELECT count(*) FROM session").get()
  console.log("ready")
  await released
  db.close()
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
