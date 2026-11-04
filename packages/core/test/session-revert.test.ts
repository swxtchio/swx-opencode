import { $ } from "bun"
import { describe, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { DateTime, Effect, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { Global } from "@opencode-ai/core/global"
import { Location } from "@opencode-ai/core/location"
import { LocationServiceMap } from "@opencode-ai/core/location-services"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { AbsolutePath, RelativePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionInput } from "@opencode-ai/core/session/input"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { Prompt } from "@opencode-ai/core/session/prompt"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionStore } from "@opencode-ai/core/session/store"
import { Snapshot } from "@opencode-ai/core/snapshot"
import { tmpdir } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

const it = testEffect(Layer.empty)

describe("SessionV2.revert files", () => {
  it.live("undoes a projected file rename and clears it without losing later edits", () =>
    Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
    ).pipe(
      Effect.flatMap((tmp) => {
        const directory = path.join(tmp.path, "project")
        const original = path.join(directory, "old name.txt")
        const renamed = path.join(directory, "new name.txt")
        const unrelated = path.join(directory, "unrelated.txt")
        const location = Location.Ref.make({ directory: AbsolutePath.make(directory) })
        const app = AppNodeBuilder.build(
          LayerNode.group([
            Database.node,
            EventV2.node,
            LocationServiceMap.node,
            SessionProjector.node,
            SessionStore.node,
            SessionV2.node,
          ]),
          [
            [Global.node, Global.layerWith({ data: path.join(tmp.path, "data"), config: path.join(tmp.path, "config") })],
            [SessionExecution.node, SessionExecution.noopLayer],
          ],
        )

        return Effect.gen(function* () {
          yield* Effect.promise(async () => {
            await fs.mkdir(directory, { recursive: true })
            await Bun.write(original, "Preserve this content.\n")
            await Bun.write(unrelated, "Unrelated content.\n")
            await $`git init -q`.cwd(directory).quiet()
            await $`git config core.fsmonitor false`.cwd(directory).quiet()
            await $`git config commit.gpgsign false`.cwd(directory).quiet()
            await $`git config user.email test@opencode.test`.cwd(directory).quiet()
            await $`git config user.name Test`.cwd(directory).quiet()
            await $`git add .`.cwd(directory).quiet()
            await $`git commit -m initial`.cwd(directory).quiet()
          })

          const session = yield* SessionV2.Service
          const created = yield* session.create({ location })
          const prompt = yield* session.prompt({
            sessionID: created.id,
            prompt: Prompt.make({ text: "Rename the file" }),
            resume: false,
          })
          const database = (yield* Database.Service).db
          const events = yield* EventV2.Service
          yield* SessionInput.promoteSteers(database, events, created.id, Number.MAX_SAFE_INTEGER)

          yield* Effect.gen(function* () {
            const snapshot = yield* Snapshot.Service
            const before = yield* snapshot.capture()
            if (!before) throw new Error("Initial snapshot missing")

            const assistantMessageID = SessionMessage.ID.create()
            yield* events.publish(SessionEvent.Step.Started, {
              sessionID: created.id,
              assistantMessageID,
              timestamp: yield* DateTime.now,
              agent: "build",
              model: {
                id: ModelV2.ID.make("test-model"),
                providerID: ProviderV2.ID.make("test-provider"),
              },
              snapshot: before,
            })
            yield* Effect.promise(() => fs.rename(original, renamed))
            const after = yield* snapshot.capture()
            if (!after) throw new Error("Renamed snapshot missing")
            const files = yield* snapshot.files({ from: before, to: after })

            expect(files).toEqual([RelativePath.make("new name.txt"), RelativePath.make("old name.txt")])
            yield* events.publish(SessionEvent.Step.Ended, {
              sessionID: created.id,
              assistantMessageID,
              timestamp: yield* DateTime.now,
              finish: "stop",
              cost: 0,
              tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
              snapshot: after,
              files,
            })

            expect(yield* session.messages({ sessionID: created.id, order: "asc" })).toMatchObject([
              { type: "user", id: prompt.id },
              {
                type: "assistant",
                id: assistantMessageID,
                snapshot: { start: before, end: after, files },
              },
            ])

            yield* Effect.promise(() => Bun.write(unrelated, "Keep this later edit.\n"))
            const reverted = yield* session.revert.stage({ sessionID: created.id, messageID: prompt.id })
            expect({
              original: yield* Effect.promise(() => Bun.file(original).exists()),
              renamed: yield* Effect.promise(() => Bun.file(renamed).exists()),
            }).toEqual({ original: true, renamed: false })
            expect(yield* Effect.promise(() => Bun.file(original).text())).toBe("Preserve this content.\n")
            expect(reverted.files?.map((file) => [file.path, file.status])).toEqual([
              [RelativePath.make("new name.txt"), "deleted"],
              [RelativePath.make("old name.txt"), "added"],
            ])
            expect(yield* Effect.promise(() => Bun.file(unrelated).text())).toBe("Keep this later edit.\n")

            yield* session.revert.clear(created.id)
            expect(yield* Effect.promise(() => Bun.file(original).exists())).toBe(false)
            expect(yield* Effect.promise(() => Bun.file(renamed).text())).toBe("Preserve this content.\n")
            expect(yield* Effect.promise(() => Bun.file(unrelated).text())).toBe("Keep this later edit.\n")
            expect((yield* session.get(created.id)).revert).toBeUndefined()
          }).pipe(Effect.provide(LocationServiceMap.Service.get(created.location)))
        }).pipe(Effect.provide(app))
      }),
    ),
  )
})
