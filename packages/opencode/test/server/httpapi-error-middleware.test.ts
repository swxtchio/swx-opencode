import { NodeHttpServer, NodeServices } from "@effect/platform-node"
import { Database } from "@opencode-ai/core/database/database"
import { NamedError } from "@opencode-ai/core/util/error"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { describe, expect } from "bun:test"
import { ConfigErrorV1 } from "@opencode-ai/core/v1/config/error"
import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { Cause, Context, Effect, Layer, Option } from "effect"
import { isSqlError } from "effect/unstable/sql/SqlError"
import { HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { errorLayer } from "../../src/server/routes/instance/httpapi/middleware/error"
import { MessageV2 } from "../../src/session/message-v2"
import { NotFoundError } from "../../src/storage/storage"
import path from "path"
import { tmpdir } from "../fixture/fixture"
import { produceSqliteImmediateError, produceSqliteLockedError } from "../fixture/sqlite-lock"
import { testEffect } from "../lib/effect"

const it = testEffect(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))

function expectUnknownErrorBody(body: unknown) {
  expect(body).toMatchObject({
    name: "UnknownError",
    data: { message: "Unexpected server error. Check server logs for details." },
  })
  expect((body as { data?: { ref?: unknown } }).data?.ref).toMatch(/^err_[0-9a-f-]{8}$/)
}

describe("HttpApi error middleware", () => {
  it.live("returns a safe body for unknown 500 defects", () =>
    Effect.gen(function* () {
      yield* HttpRouter.add("GET", "/boom", Effect.die(new Error("secret stack marker"))).pipe(
        Layer.provide(errorLayer),
        HttpRouter.serve,
        Layer.build,
      )

      const response = yield* HttpClientRequest.get("/boom").pipe(HttpClient.execute)
      const body = yield* response.json

      expect(response.status).toBe(500)
      expectUnknownErrorBody(body)
      expect(JSON.stringify(body)).not.toContain("secret stack marker")
    }),
  )

  it.live("returns a safe body for named defects", () =>
    Effect.gen(function* () {
      yield* HttpRouter.add(
        "GET",
        "/named",
        Effect.die(new NamedError.Unknown({ message: "secret named marker" })),
      ).pipe(Layer.provide(errorLayer), HttpRouter.serve, Layer.build)

      const response = yield* HttpClientRequest.get("/named").pipe(HttpClient.execute)
      const body = yield* response.json

      expect(response.status).toBe(500)
      expectUnknownErrorBody(body)
      expect(JSON.stringify(body)).not.toContain("secret named marker")
    }),
  )

  it.live(
    "maps a produced exhausted SQLite lock through errorLayer and MessageV2",
    () =>
      Effect.gen(function* () {
        const tmp = yield* Effect.promise(() => tmpdir())
        yield* Effect.addFinalizer(() => Effect.promise(() => tmp[Symbol.asyncDispose]()))
        const filename = path.join(tmp.path, "http-lock.sqlite")
        const context = yield* Layer.build(Database.layerFromPath(filename))
        const database = Context.get(context, Database.Service).db
        const error = yield* produceSqliteImmediateError(database, filename)
        expect(error).not.toBeInstanceOf(EffectDrizzleQueryError)
        expect(isSqlError(error)).toBe(true)
        if (!isSqlError(error)) return
        expect(error.reason._tag).toBe("LockTimeoutError")
        expect(error.reason.cause).toMatchObject({ code: "SQLITE_BUSY" })
        expect(error.message).not.toContain("secret_lock_fixture")
        expect(error.message).not.toContain("direct_parameter")

        const converted = MessageV2.fromError(error, { providerID: ProviderV2.ID.make("test") })
        expect(converted).toMatchObject({
          name: "UnknownError",
          data: { message: "Database is locked (SQLITE_BUSY)" },
        })
        expect(JSON.stringify(converted)).not.toContain("secret_lock_fixture")
        expect(JSON.stringify(converted)).not.toContain("secret_parameter")
        expect(JSON.stringify(converted)).not.toContain("direct_parameter")

        yield* HttpRouter.add("GET", "/sqlite-lock", Effect.die(error)).pipe(
          Layer.provide(errorLayer),
          HttpRouter.serve,
          Layer.build,
        )

        const response = yield* HttpClientRequest.get("/sqlite-lock").pipe(HttpClient.execute)
        const body = yield* response.json
        const serialized = JSON.stringify(body)

        expect(response.status).toBe(500)
        expect(body).toMatchObject({
          name: "UnknownError",
          data: { message: "Database is locked (SQLITE_BUSY)" },
        })
        expect((body as { data?: { ref?: unknown } }).data?.ref).toMatch(/^err_[0-9a-f-]{8}$/)
        expect(serialized).not.toContain("secret_lock_fixture")
        expect(serialized).not.toContain("secret_parameter")
        expect(serialized).not.toContain("direct_parameter")
      }),
    20_000,
  )

  it.live("maps a produced direct SQLITE_LOCKED error distinctly", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      yield* Effect.addFinalizer(() => Effect.promise(() => tmp[Symbol.asyncDispose]()))
      const error = yield* produceSqliteLockedError(path.join(tmp.path, "sqlite-locked.sqlite"))
      expect(error).not.toBeInstanceOf(EffectDrizzleQueryError)
      expect(isSqlError(error)).toBe(true)
      if (!isSqlError(error)) return
      expect(error.reason._tag).toBe("LockTimeoutError")
      expect(error.reason.cause).toMatchObject({ code: "SQLITE_LOCKED" })
      expect(error.message).not.toContain("secret_lock_fixture")
      expect(error.message).not.toContain("secret_parameter")

      const converted = MessageV2.fromError(error, { providerID: ProviderV2.ID.make("test") })
      expect(converted).toMatchObject({
        name: "UnknownError",
        data: { message: "Database is locked (SQLITE_LOCKED)" },
      })
      expect(JSON.stringify(converted)).not.toContain("secret_lock_fixture")
      expect(JSON.stringify(converted)).not.toContain("secret_parameter")

      yield* HttpRouter.add("GET", "/sqlite-locked", Effect.die(error)).pipe(
        Layer.provide(errorLayer),
        HttpRouter.serve,
        Layer.build,
      )
      const response = yield* HttpClientRequest.get("/sqlite-locked").pipe(HttpClient.execute)
      const body = yield* response.json
      const serialized = JSON.stringify(body)

      expect(response.status).toBe(500)
      expect(body).toMatchObject({
        name: "UnknownError",
        data: { message: "Database is locked (SQLITE_LOCKED)" },
      })
      expect((body as { data?: { ref?: unknown } }).data?.ref).toMatch(/^err_[0-9a-f-]{8}$/)
      expect(serialized).not.toContain("secret_lock_fixture")
      expect(serialized).not.toContain("secret_parameter")
    }),
  )

  it.live("returns invalid config defects as structured client errors", () =>
    Effect.gen(function* () {
      const configError = new ConfigErrorV1.InvalidError({
        path: "/tmp/opencode.json",
        issues: [{ message: "Expected object", path: ["provider", "anthropic", "options"] }],
      })

      yield* HttpRouter.add("GET", "/config-error", Effect.die(configError)).pipe(
        Layer.provide(errorLayer),
        HttpRouter.serve,
        Layer.build,
      )

      const response = yield* HttpClientRequest.get("/config-error").pipe(HttpClient.execute)
      const body = yield* response.json
      const serialized = JSON.stringify(body)

      expect(response.status).toBe(400)
      expect(body).toMatchObject({
        name: "ConfigInvalidError",
        data: {
          path: "/tmp/opencode.json",
          issues: [{ message: "Expected object", path: ["provider", "anthropic", "options"] }],
        },
      })
      expect(serialized).toContain("/tmp/opencode.json")
      expect(serialized).toContain("anthropic")
    }),
  )

  it.live("returns remote auth defects as structured client errors", () =>
    Effect.gen(function* () {
      const configError = new ConfigErrorV1.RemoteAuthError({
        url: "https://example.com",
        remote: "https://config.example.com/opencode.json",
      })

      yield* HttpRouter.add("GET", "/remote-auth-error", Effect.die(configError)).pipe(
        Layer.provide(errorLayer),
        HttpRouter.serve,
        Layer.build,
      )

      const response = yield* HttpClientRequest.get("/remote-auth-error").pipe(HttpClient.execute)
      const body = yield* response.json

      expect(response.status).toBe(400)
      expect(body).toEqual(configError.toObject())
    }),
  )

  it.live("does not map storage not-found defects to 404", () =>
    Effect.gen(function* () {
      yield* HttpRouter.add(
        "GET",
        "/missing",
        Effect.die(new NotFoundError({ message: "Resource not found: secret" })),
      ).pipe(Layer.provide(errorLayer), HttpRouter.serve, Layer.build)

      const response = yield* HttpClientRequest.get("/missing").pipe(HttpClient.execute)
      const body = yield* response.json

      expect(response.status).toBe(500)
      expectUnknownErrorBody(body)
    }),
  )
})
