import { NodeHttpServer, NodeServices } from "@effect/platform-node"
import { Database } from "@opencode-ai/core/database/database"
import { NamedError } from "@opencode-ai/core/util/error"
import { describe, expect } from "bun:test"
import { ConfigErrorV1 } from "@opencode-ai/core/v1/config/error"
import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { Cause, Context, Deferred, Effect, Fiber, Layer, Option } from "effect"
import { isSqlError } from "effect/unstable/sql/SqlError"
import { HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { errorLayer } from "../../src/server/routes/instance/httpapi/middleware/error"
import { NotFoundError } from "../../src/storage/storage"
import path from "path"
import { tmpdir } from "../fixture/fixture"
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
    "identifies SQLite lock defects without exposing native details",
    () =>
      Effect.gen(function* () {
        const tmp = yield* Effect.promise(() => tmpdir())
        yield* Effect.addFinalizer(() => Effect.promise(() => tmp[Symbol.asyncDispose]()))
        const filename = path.join(tmp.path, "http-lock.sqlite")
        const writerContext = yield* Layer.build(Database.layerFromPath(filename))
        const writer = Context.get(writerContext, Database.Service).db
        yield* writer.run("CREATE TABLE http_lock_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
        const holder = new sqlite.Database(filename)
        yield* Effect.addFinalizer(() => Effect.sync(() => holder.close()))
        holder.run("PRAGMA journal_mode = WAL")
        const readStarted = yield* Deferred.make<void>()
        const continueWrite = yield* Deferred.make<void>()

        const staleWrite = yield* writer.$client
          .withTransaction(
            Effect.gen(function* () {
              yield* writer.$client.unsafe("SELECT COUNT(*) FROM http_lock_test").values
              yield* Deferred.succeed(readStarted, undefined)
              yield* Deferred.await(continueWrite)
              return yield* Effect.flip(
                writer.run("INSERT INTO http_lock_test (id, value) VALUES (1, 'secret_marker')").pipe(
                  Effect.timeoutOrElse({
                    duration: "5 seconds",
                    orElse: () => Effect.fail(new Error("Drizzle snapshot write did not stop")),
                  }),
                ),
              )
            }),
          )
          .pipe(Effect.forkChild({ startImmediately: true }))

        yield* Deferred.await(readStarted)
        holder.run("INSERT INTO http_lock_test (id, value) VALUES (100, 'holder write')")
        yield* Deferred.succeed(continueWrite, undefined)
        const error = yield* Fiber.join(staleWrite)
        expect(error).toBeInstanceOf(EffectDrizzleQueryError)
        if (!(error instanceof EffectDrizzleQueryError)) return
        expect(error.query).toContain("secret_marker")
        const cause = error.cause
        const failure = Cause.isCause(cause) ? Option.getOrUndefined(Cause.findErrorOption(cause)) : undefined
        expect(isSqlError(failure)).toBe(true)
        if (!isSqlError(failure)) return
        expect(failure.reason.cause).toMatchObject({ code: "SQLITE_BUSY_SNAPSHOT" })

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
        expect(serialized).not.toContain("secret_marker")
      }),
    20_000,
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
