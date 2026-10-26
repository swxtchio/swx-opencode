import { expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Context, Deferred, Effect, Fiber, Layer } from "effect"
import { Credential } from "@opencode-ai/core/credential"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Database } from "@opencode-ai/core/database/database"
import { Global } from "@opencode-ai/core/global"
import { Integration } from "@opencode-ai/schema/integration"
import { testEffect } from "./lib/effect"

const it = testEffect(Layer.empty)
const id = Credential.ID.make("cred_refresh")
const value = Credential.OAuth.make({
  type: "oauth",
  methodID: Integration.MethodID.make("oauth"),
  access: "new-access",
  refresh: "new-refresh",
  expires: Number.MAX_SAFE_INTEGER,
})

it.effect("shares failures but allows a later refresh", () =>
  withTemporaryDirectory((directory) =>
    Effect.scoped(
      Effect.gen(function* () {
        const layer = AppNodeBuilder.build(Credential.node, [
          [Database.node, Database.layerFromPath(path.join(directory, "credentials.db"))],
          [Global.node, Global.layerWith({ state: path.join(directory, "state") })],
        ])
        const context = yield* Layer.build(layer)
        const credentials = Context.get(context, Credential.Service)
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const error = new Error("token endpoint unavailable")
        let calls = 0
        const operation = Effect.gen(function* () {
          calls++
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
          return yield* Effect.fail(error)
        })
        const first = yield* credentials.lock(id, operation).pipe(Effect.flip, Effect.forkScoped)
        yield* Deferred.await(entered)
        const second = yield* credentials.lock(id, operation).pipe(Effect.flip, Effect.forkScoped)
        yield* Effect.yieldNow
        yield* Deferred.succeed(release, undefined)
        expect(yield* Fiber.join(first)).toBe(error)
        expect(yield* Fiber.join(second)).toBe(error)
        expect(calls).toBe(1)
        expect(yield* credentials.lock(id, Effect.succeed(value))).toEqual(value)
      }),
    ),
  ),
)

function withTemporaryDirectory<A, E, R>(use: (directory: string) => Effect.Effect<A, E, R>) {
  return Effect.acquireUseRelease(
    Effect.promise(async () => {
      const root = process.env.GOTMPDIR
      if (!root) throw new Error("GOTMPDIR is required for OAuth refresh test scratch")
      await fs.access(root)
      return fs.mkdtemp(path.join(root, "credential-lock-"))
    }),
    (directory) => Effect.scoped(use(directory)),
    (directory) => Effect.promise(() => fs.rm(directory, { recursive: true, force: true })),
  )
}
