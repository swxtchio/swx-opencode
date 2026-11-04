import { afterAll, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Context, Deferred, Effect, Fiber, Layer, Option } from "effect"
import { Credential } from "@opencode-ai/core/credential"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Database } from "@opencode-ai/core/database/database"
import { Global } from "@opencode-ai/core/global"
import { Integration } from "@opencode-ai/schema/integration"
import { testEffect } from "./lib/effect"

const it = testEffect(Layer.empty)
const worktreeRoot = path.resolve(import.meta.dir, "../../..")
const scratchRoot = path.join(
  process.env.GOTMPDIR ?? path.join(worktreeRoot, ".scratch"),
  "credential-lock-tests",
)

afterAll(() => fs.rm(scratchRoot, { recursive: true, force: true }))
const id = Credential.ID.make("cred_refresh")
const value = Credential.OAuth.make({
  type: "oauth",
  methodID: Integration.MethodID.make("oauth"),
  access: "new-access",
  refresh: "new-refresh",
  expires: Number.MAX_SAFE_INTEGER,
})

it.live("shares failures but allows a later refresh", () =>
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
        const secondStarted = yield* Deferred.make<void>()
        let calls = 0
        const operation = Effect.gen(function* () {
          calls++
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
          return yield* Effect.fail(error)
        })
        const first = yield* credentials.lock(id, operation).pipe(Effect.flip, Effect.forkScoped)
        yield* Deferred.await(entered)
        const second = yield* Effect.gen(function* () {
          yield* Deferred.succeed(secondStarted, undefined)
          return yield* credentials.lock(id, operation).pipe(Effect.flip)
        }).pipe(Effect.forkScoped)
        yield* Deferred.await(secondStarted)
        yield* Deferred.succeed(release, undefined)
        const firstResult = yield* Fiber.join(first).pipe(Effect.timeoutOption("4 seconds"))
        const secondResult = yield* Fiber.join(second).pipe(Effect.timeoutOption("4 seconds"))
        expect(Option.isSome(firstResult)).toBe(true)
        expect(Option.isSome(secondResult)).toBe(true)
        if (!Option.isSome(firstResult) || !Option.isSome(secondResult)) return
        expect(firstResult.value).toBe(error)
        expect(secondResult.value).toBe(error)
        expect(calls).toBe(1)
        expect(yield* credentials.lock(id, Effect.succeed(value))).toEqual(value)
      }),
    ),
  ),
  10_000,
)

function withTemporaryDirectory<A, E, R>(use: (directory: string) => Effect.Effect<A, E, R>) {
  return Effect.acquireUseRelease(
    Effect.promise(async () => {
      await fs.mkdir(scratchRoot, { recursive: true, mode: 0o700 })
      return fs.mkdtemp(path.join(scratchRoot, "case-"))
    }),
    (directory) => Effect.scoped(use(directory)),
    (directory) => Effect.promise(() => fs.rm(directory, { recursive: true, force: true })),
  )
}
