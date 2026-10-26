import { spawn, type ChildProcess } from "child_process"
import fs from "fs/promises"
import path from "path"
import { expect } from "bun:test"
import { Cause, Clock, Context, Deferred, Effect, Exit, Fiber, Layer, Option } from "effect"
import * as TestClock from "effect/testing/TestClock"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Credential } from "@opencode-ai/core/credential"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { Global } from "@opencode-ai/core/global"
import { Integration } from "@opencode-ai/core/integration"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { testEffect } from "./lib/effect"

const it = testEffect(Layer.empty)

it.effect("shares successful and failed refreshes across Locations, then retries", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const lockSignals = yield* Effect.forEach([0, 1, 2, 3, 4], () => Deferred.make<void>())
      const lockQueue = [...lockSignals]
      const onLock = () =>
        Effect.suspend(() => {
          const signal = lockQueue.shift()
          return signal ? Deferred.succeed(signal, undefined).pipe(Effect.asVoid) : Effect.void
        })
      const services = yield* locations(directory, onLock)
      expect(services.first).not.toBe(services.second)

      const successID = Integration.ID.make("refresh-success")
      const successMethod = Integration.MethodID.make("oauth")
      const failureID = Integration.ID.make("refresh-failure")
      const failureMethod = Integration.MethodID.make("oauth")
      const successEntered = yield* Deferred.make<void>()
      const successRelease = yield* Deferred.make<void>()
      const failureEntered = yield* Deferred.make<void>()
      const failureRelease = yield* Deferred.make<void>()
      const refreshFailure = new Error("token endpoint unavailable")
      let successCalls = 0
      let failureCalls = 0

      const successImplementation = oauthImplementation(successID, successMethod, (value) =>
        Effect.gen(function* () {
          successCalls++
          yield* Deferred.succeed(successEntered, undefined)
          yield* Deferred.await(successRelease)
          return rotated(value, "success")
        }),
      )
      const failureImplementation = oauthImplementation(failureID, failureMethod, (value) =>
        Effect.gen(function* () {
          failureCalls++
          if (failureCalls === 1) {
            yield* Deferred.succeed(failureEntered, undefined)
            yield* Deferred.await(failureRelease)
            return yield* Effect.fail(refreshFailure)
          }
          return rotated(value, "retry")
        }),
      )
      yield* Effect.forEach([services.first, services.second], (integration) =>
        integration.transform((editor) => {
          editor.method.update(successImplementation)
          editor.method.update(failureImplementation)
        }),
        { discard: true },
      )

      const successCredential = yield* services.credentials.create({
        integrationID: successID,
        value: expiredOAuth(successMethod),
      })
      const successConnection = credentialConnection(successCredential)
      const successFirst = yield* services.first.connection.resolve(successConnection).pipe(Effect.forkScoped)
      yield* Deferred.await(lockSignals[0]!)
      yield* Deferred.await(successEntered)
      const successSecond = yield* services.second.connection.resolve(successConnection).pipe(Effect.forkScoped)
      yield* Deferred.await(lockSignals[1]!)
      expect(successCalls).toBe(1)
      yield* Deferred.succeed(successRelease, undefined)

      const successValues = yield* Effect.all([Fiber.join(successFirst), Fiber.join(successSecond)])
      expect(successValues[0]).toEqual(successValues[1])
      expect(successValues[0]).toEqual(rotated(expiredOAuth(successMethod), "success"))
      expect((yield* services.credentials.get(successCredential.id))?.value).toEqual(successValues[0])

      const failureCredential = yield* services.credentials.create({
        integrationID: failureID,
        value: expiredOAuth(failureMethod),
      })
      const failureConnection = credentialConnection(failureCredential)
      const failureFirst = yield* services.first.connection.resolve(failureConnection).pipe(Effect.exit, Effect.forkScoped)
      yield* Deferred.await(lockSignals[2]!)
      yield* Deferred.await(failureEntered)
      const failureSecond = yield* services.second.connection.resolve(failureConnection).pipe(Effect.exit, Effect.forkScoped)
      yield* Deferred.await(lockSignals[3]!)
      yield* Deferred.succeed(failureRelease, undefined)

      const failures = yield* Effect.all([Fiber.join(failureFirst), Fiber.join(failureSecond)])
      expect(Exit.isFailure(failures[0])).toBe(true)
      expect(Exit.isFailure(failures[1])).toBe(true)
      if (!Exit.isFailure(failures[0]) || !Exit.isFailure(failures[1])) throw new Error("Expected shared refresh failure")
      const firstError = Cause.squash(failures[0].cause)
      const secondError = Cause.squash(failures[1].cause)
      expect(firstError).toBeInstanceOf(Integration.AuthorizationError)
      expect(secondError).toBeInstanceOf(Integration.AuthorizationError)
      if (!(firstError instanceof Integration.AuthorizationError) || !(secondError instanceof Integration.AuthorizationError))
        throw new Error("Expected Integration authorization errors")
      expect(firstError.cause).toBe(refreshFailure)
      expect(secondError.cause).toBe(refreshFailure)
      expect(failureCalls).toBe(1)

      const retry = yield* services.first.connection.resolve(failureConnection)
      yield* Deferred.await(lockSignals[4]!)
      expect(retry).toEqual(rotated(expiredOAuth(failureMethod), "retry"))
      expect((yield* services.credentials.get(failureCredential.id))?.value).toEqual(retry)
      expect(failureCalls).toBe(2)
    }),
  ),
)

it.effect("interrupting the initiating caller does not cancel the shared refresh", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const firstLock = yield* Deferred.make<void>()
      const secondLock = yield* Deferred.make<void>()
      const lockSignals = [firstLock, secondLock]
      const onLock = () =>
        Effect.suspend(() => {
          const signal = lockSignals.shift()
          return signal ? Deferred.succeed(signal, undefined).pipe(Effect.asVoid) : Effect.void
        })
      const services = yield* locations(directory, onLock)
      const integrationID = Integration.ID.make("refresh-cancel-owner")
      const methodID = Integration.MethodID.make("oauth")
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      yield* Effect.acquireUseRelease(
        Effect.void,
        () =>
          Effect.gen(function* () {
            let calls = 0
            let refreshInterrupted = false
            const implementation = oauthImplementation(integrationID, methodID, (value) =>
              Effect.gen(function* () {
                calls++
                yield* Deferred.succeed(entered, undefined)
                yield* Deferred.await(release).pipe(
                  Effect.onInterrupt(() => Effect.sync(() => (refreshInterrupted = true))),
                )
                return rotated(value, "surviving")
              }),
            )
            yield* Effect.forEach([services.first, services.second], (integration) =>
              integration.transform((editor) => editor.method.update(implementation)),
              { discard: true },
            )
            const stored = yield* services.credentials.create({ integrationID, value: expiredOAuth(methodID) })
            const connection = credentialConnection(stored)

            const initiating = yield* services.first.connection.resolve(connection).pipe(Effect.forkScoped)
            yield* Deferred.await(firstLock)
            yield* Deferred.await(entered)
            const surviving = yield* services.second.connection.resolve(connection).pipe(Effect.forkScoped)
            yield* Deferred.await(secondLock)
            yield* Fiber.interrupt(initiating)
            expect(Exit.isFailure(yield* Fiber.await(initiating))).toBe(true)
            yield* Deferred.succeed(release, undefined)

            const value = yield* Fiber.join(surviving)
            expect(value).toEqual(rotated(expiredOAuth(methodID), "surviving"))
            expect((yield* services.credentials.get(stored.id))?.value).toEqual(value)
            expect(calls).toBe(1)
            expect(refreshInterrupted).toBe(false)
          }),
        () => Deferred.succeed(release, undefined).pipe(Effect.asVoid),
      )
    }),
  ),
)

it.effect("interrupting a joining caller settles that wait without cancelling another caller's refresh", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const firstLock = yield* Deferred.make<void>()
      const secondLock = yield* Deferred.make<void>()
      const lockSignals = [firstLock, secondLock]
      const onLock = () =>
        Effect.suspend(() => {
          const signal = lockSignals.shift()
          return signal ? Deferred.succeed(signal, undefined).pipe(Effect.asVoid) : Effect.void
        })
      const services = yield* locations(directory, onLock)
      const integrationID = Integration.ID.make("refresh-cancel-waiter")
      const methodID = Integration.MethodID.make("oauth")
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      yield* Effect.acquireUseRelease(
        Effect.void,
        () =>
          Effect.gen(function* () {
            let calls = 0
            const implementation = oauthImplementation(integrationID, methodID, (value) =>
              Effect.gen(function* () {
                calls++
                yield* Deferred.succeed(entered, undefined)
                yield* Deferred.await(release)
                return rotated(value, "waiter")
              }),
            )
            yield* Effect.forEach([services.first, services.second], (integration) =>
              integration.transform((editor) => editor.method.update(implementation)),
              { discard: true },
            )
            const stored = yield* services.credentials.create({ integrationID, value: expiredOAuth(methodID) })
            const connection = credentialConnection(stored)

            const owner = yield* services.first.connection.resolve(connection).pipe(Effect.forkScoped)
            yield* Deferred.await(firstLock)
            yield* Deferred.await(entered)
            const waiter = yield* services.second.connection.resolve(connection).pipe(Effect.forkScoped)
            yield* Deferred.await(secondLock)
            yield* Fiber.interrupt(waiter).pipe(Effect.forkScoped)
            const boundedWait = yield* Fiber.await(waiter).pipe(Effect.timeoutOption("1 second"), Effect.forkScoped)
            yield* Effect.yieldNow
            yield* TestClock.adjust("1 second")
            expect(Option.isSome(yield* Fiber.join(boundedWait))).toBe(true)
            yield* Deferred.succeed(release, undefined)

            const value = yield* Fiber.join(owner)
            expect(value).toEqual(rotated(expiredOAuth(methodID), "waiter"))
            expect(Exit.isFailure(yield* Fiber.await(waiter))).toBe(true)
            expect((yield* services.credentials.get(stored.id))?.value).toEqual(value)
            expect(calls).toBe(1)
          }),
        () => Deferred.succeed(release, undefined).pipe(Effect.asVoid),
      )
    }),
  ),
)

it.effect("bounds refresh and allows a subsequent resolver to own the released lock", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const lockSignals = yield* Effect.forEach([0, 1], () => Deferred.make<void>())
      const lockQueue = [...lockSignals]
      const onLock = () =>
        Effect.suspend(() => {
          const signal = lockQueue.shift()
          return signal ? Deferred.succeed(signal, undefined).pipe(Effect.asVoid) : Effect.void
        })
      const services = yield* locations(directory, onLock)
      const integrationID = Integration.ID.make("refresh-timeout")
      const methodID = Integration.MethodID.make("oauth")
      const entered = yield* Deferred.make<void>()
      const refreshInterrupted = yield* Deferred.make<void>()
      const retried = yield* Deferred.make<void>()
      const resolverSettled = yield* Deferred.make<void>()
      const clock = yield* Clock.Clock
      yield* Effect.acquireUseRelease(
        Effect.void,
        () =>
          Effect.gen(function* () {
            const stored = yield* services.credentials.create({ integrationID, value: expiredOAuth(methodID) })
            const connection = credentialConnection(stored)
            yield* services.first.transform((editor) =>
              editor.method.update({
                ...oauthImplementation(integrationID, methodID),
                refresh: () =>
                  Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Effect.never),
                    Effect.onInterrupt(() => Deferred.succeed(refreshInterrupted, undefined).pipe(Effect.asVoid)),
                  ),
              }),
            )

            const stuck = yield* services.first.connection.resolve(connection).pipe(
              Effect.provideService(Clock.Clock, clock),
              Effect.onExit(() => Deferred.succeed(resolverSettled, undefined).pipe(Effect.asVoid)),
              Effect.flip,
              Effect.forkScoped,
            )
            yield* Deferred.await(lockSignals[0]!)
            yield* Deferred.await(entered)
            const boundedDeadline = yield* Deferred.await(refreshInterrupted)
              .pipe(Effect.timeoutOption("31 seconds"), Effect.forkScoped)
            yield* Effect.yieldNow
            yield* TestClock.adjust("30 seconds")
            yield* Effect.yieldNow
            yield* TestClock.adjust("1 second")
            expect(Option.isSome(yield* Fiber.join(boundedDeadline))).toBe(true)
            expect(yield* waitForDeferred(resolverSettled, 2_000)).toBe(true)
            expect(yield* Fiber.join(stuck)).toBeInstanceOf(Integration.AuthorizationError)

            yield* services.first.transform((editor) =>
              editor.method.update(
                oauthImplementation(integrationID, methodID, (value) =>
                  Deferred.succeed(retried, undefined).pipe(Effect.as(rotated(value, "retry"))),
                ),
              ),
            )
            const retry = yield* services.first.connection.resolve(connection).pipe(Effect.exit, Effect.forkScoped)
            const retryLock = yield* Deferred.await(lockSignals[1]!).pipe(Effect.timeoutOption("1 second"), Effect.forkScoped)
            yield* Effect.yieldNow
            yield* TestClock.adjust("1 second")
            expect(Option.isSome(yield* Fiber.join(retryLock))).toBe(true)
            expect(yield* waitForDeferred(retried, 2_000)).toBe(true)
            const retryExit = yield* Fiber.join(retry)
            expect(Exit.isSuccess(retryExit)).toBe(true)
            if (!Exit.isSuccess(retryExit)) return
            yield* Deferred.await(retried)
            expect(retryExit.value).toEqual(rotated(expiredOAuth(methodID), "retry"))
            expect((yield* services.credentials.get(stored.id))?.value).toEqual(retryExit.value)
          }),
        () => TestClock.adjust("6 minutes"),
      )
    }),
  ),
)

it.effect("preserves environment, missing, key, fresh OAuth, and no-refresh resolution behavior", () =>
  withTemporaryDirectory((directory) =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const name = "INTEGRATION_REFRESH_PRESERVATION_TEST"
        const previous = process.env[name]
        process.env[name] = "environment-key"
        return previous
      }),
      (previous) =>
        Effect.gen(function* () {
          const services = yield* locations(directory, () => Effect.void)
          const freshID = Integration.ID.make("refresh-preservation-fresh")
          const freshMethod = Integration.MethodID.make("oauth")
          const noRefreshID = Integration.ID.make("refresh-preservation-no-refresh")
          const noRefreshMethod = Integration.MethodID.make("oauth")
          let refreshCalls = 0
          yield* services.first.transform((editor) => {
            editor.method.update(
              oauthImplementation(freshID, freshMethod, (value) => {
                refreshCalls++
                return Effect.succeed(rotated(value, "unexpected"))
              }),
            )
            editor.method.update(oauthImplementation(noRefreshID, noRefreshMethod))
          })
          const key = yield* services.credentials.create({
            integrationID: Integration.ID.make("refresh-preservation-key"),
            value: Credential.Key.make({ type: "key", key: "stored-key" }),
          })
          const fresh = yield* services.credentials.create({
            integrationID: freshID,
            value: Credential.OAuth.make({
              ...expiredOAuth(freshMethod),
              access: "fresh-access",
              refresh: "fresh-refresh",
              expires: Number.MAX_SAFE_INTEGER,
            }),
          })
          const noRefresh = yield* services.credentials.create({
            integrationID: noRefreshID,
            value: expiredOAuth(noRefreshMethod),
          })

          expect(
            yield* services.first.connection.resolve({
              type: "env",
              name: "INTEGRATION_REFRESH_PRESERVATION_TEST",
            }),
          ).toEqual(Credential.Key.make({ type: "key", key: "environment-key" }))
          expect(yield* services.first.connection.resolve(credentialConnection(key))).toEqual(key.value)
          expect(
            yield* services.first.connection.resolve({
              type: "credential",
              id: Credential.ID.make("missing-refresh-credential"),
              label: "missing",
            }),
          ).toBeUndefined()
          expect(yield* services.first.connection.resolve(credentialConnection(fresh))).toEqual(fresh.value)
          expect(yield* services.first.connection.resolve(credentialConnection(noRefresh))).toEqual(noRefresh.value)
          expect(refreshCalls).toBe(0)
        }),
      (previous) =>
        Effect.sync(() => {
          if (previous === undefined) delete process.env.INTEGRATION_REFRESH_PRESERVATION_TEST
          else process.env.INTEGRATION_REFRESH_PRESERVATION_TEST = previous
        }),
    ),
  ),
)

it.live("serializes resolver refreshes across processes and re-reads the current credential", () =>
  withTemporaryDirectory((directory) =>
    Effect.gen(function* () {
      const scenarios = ["rotated", "removed", "key"] as const
      yield* Effect.forEach(
        scenarios,
        (scenario) =>
          Effect.gen(function* () {
            const scenarioDirectory = path.join(directory, scenario)
            yield* Effect.promise(() => fs.mkdir(scenarioDirectory))
            const databasePath = path.join(scenarioDirectory, "credentials.db")
            const lockRoot = path.join(scenarioDirectory, "state")
            const integrationID = Integration.ID.make(`refresh-process-${scenario}`)
            const methodID = Integration.MethodID.make("oauth")
            const store = yield* Layer.build(Layer.fresh(storageLayer(databasePath, lockRoot)))
            const credentials = Context.get(store, Credential.Service)
            const created = yield* credentials.create({ integrationID, value: expiredOAuth(methodID) })
            const outcome = yield* Effect.promise(() =>
              runWorkerPair(
                {
                  databasePath,
                  lockRoot,
                  integrationID,
                  methodID,
                  credentialID: created.id,
                  label: created.label,
                  firstFails: scenario !== "rotated",
                },
                async () => {
                  if (scenario === "removed") await Effect.runPromise(credentials.remove(created.id))
                  if (scenario === "key")
                    await Effect.runPromise(
                      credentials.update(created.id, {
                        value: Credential.Key.make({ type: "key", key: "current-key" }),
                      }),
                    )
                },
              ),
            )

            if (scenario === "rotated") {
              expect(outcome.first.success).toBe(true)
              expect(outcome.second.success).toBe(true)
              expect(outcome.first.value).toMatchObject({ access: "first-access", refresh: "first-refresh" })
              expect(outcome.second.value).toEqual(outcome.first.value)
              expect((yield* credentials.get(created.id))?.value).toEqual(outcome.first.value)
              return
            }

            expect(outcome.first.success).toBe(false)
            expect(outcome.first.errorType).toBe("Integration.Authorization")
            expect(outcome.second.success).toBe(true)
            if (scenario === "removed") {
              expect(outcome.second.value).toBeUndefined()
              expect(yield* credentials.get(created.id)).toBeUndefined()
              return
            }
            expect(outcome.second.value).toBeUndefined()
            expect((yield* credentials.get(created.id))?.value).toEqual(
              Credential.Key.make({ type: "key", key: "current-key" }),
            )
          }),
        { discard: true },
      )
    }),
  ),
  30_000,
)

function storageLayer(databasePath: string, lockRoot: string) {
  return AppNodeBuilder.build(LayerNode.group([Credential.node, EventV2.node]), [
    [Database.node, Database.layerFromPath(databasePath)],
    [Global.node, Global.layerWith({ state: lockRoot })],
  ])
}

function locations(
  directory: string,
  onLock: (id: Credential.ID) => Effect.Effect<void>,
) {
  return Effect.gen(function* () {
    const memo = yield* Layer.makeMemoMap
    const scope = yield* Effect.scope
    const clock = yield* Clock.Clock
    const clockLayer = Layer.succeed(Clock.Clock, clock)
    const storage = yield* Layer.buildWithMemoMap(
      Layer.provideMerge(
        storageLayer(path.join(directory, "credentials.db"), path.join(directory, "state")),
        clockLayer,
      ),
      memo,
      scope,
    )
    const credentials = Context.get(storage, Credential.Service)
    const events = Context.get(storage, EventV2.Service)
    const observed = observeCredentialLock(credentials, onLock)
    const integrationLayer = () =>
      AppNodeBuilder.build(Integration.node, [
        [Credential.node, Layer.succeed(Credential.Service, observed)],
        [EventV2.node, Layer.succeed(EventV2.Service, events)],
      ]).pipe(Layer.provideMerge(clockLayer))
    const first = Context.get(
      yield* Layer.buildWithMemoMap(Layer.fresh(integrationLayer()), memo, scope),
      Integration.Service,
    )
    const second = Context.get(
      yield* Layer.buildWithMemoMap(Layer.fresh(integrationLayer()), memo, scope),
      Integration.Service,
    )
    return { credentials, first, second }
  })
}

function observeCredentialLock(
  credentials: Credential.Interface,
  onLock: (id: Credential.ID) => Effect.Effect<void>,
) {
  return Credential.Service.of({
    all: credentials.all,
    list: credentials.list,
    get: credentials.get,
    lock: (id, operation) => onLock(id).pipe(Effect.andThen(credentials.lock(id, operation))),
    create: credentials.create,
    update: credentials.update,
    remove: credentials.remove,
  })
}

function oauthImplementation(
  integrationID: Integration.ID,
  methodID: Integration.MethodID,
  refresh?: (value: Credential.OAuth) => Effect.Effect<Credential.OAuth, unknown>,
): Integration.OAuthImplementation {
  return {
    integrationID,
    method: { id: methodID, type: "oauth", label: "OAuth" },
    authorize: () => Effect.die("unused sign-in"),
    ...(refresh ? { refresh } : {}),
  }
}

function expiredOAuth(methodID: Integration.MethodID) {
  return Credential.OAuth.make({
    type: "oauth",
    methodID,
    access: "old-access",
    refresh: "old-refresh",
    expires: 1,
  })
}

function rotated(value: Credential.OAuth, label: string) {
  return Credential.OAuth.make({
    ...value,
    access: `${label}-access`,
    refresh: `${label}-refresh`,
    expires: Number.MAX_SAFE_INTEGER,
  })
}

function credentialConnection(credential: Credential.Info) {
  return {
    type: "credential" as const,
    id: credential.id,
    label: credential.label,
  }
}

function withTemporaryDirectory<A, E, R>(use: (directory: string) => Effect.Effect<A, E, R>) {
  return Effect.acquireUseRelease(
    Effect.promise(async () => {
      const root = process.env.GOTMPDIR
      if (!root) throw new Error("GOTMPDIR is required for OAuth refresh test scratch")
      await fs.access(root)
      return fs.mkdtemp(path.join(root, "oauth-refresh-"))
    }),
    (directory) => Effect.scoped(use(directory)),
    (directory) => Effect.promise(() => fs.rm(directory, { recursive: true, force: true })),
  )
}

type WorkerInput = {
  readonly databasePath: string
  readonly lockRoot: string
  readonly integrationID: Integration.ID
  readonly methodID: Integration.MethodID
  readonly credentialID: Credential.ID
  readonly label: string
  readonly firstFails: boolean
}

type WorkerMessage = {
  readonly type: string
  readonly access?: string
  readonly refresh?: string
  readonly success?: boolean
  readonly value?: Credential.Value
  readonly errorType?: string
  readonly message?: string
}
type WorkerClose = { readonly code: number | null; readonly signal: NodeJS.Signals | null }
type WorkerWaiter = {
  readonly type: string
  readonly resolve: (message: WorkerMessage) => void
  readonly reject: (error: Error) => void
  timer?: ReturnType<typeof setTimeout>
}

type WorkerSession = {
  readonly child: ChildProcess
  readonly pid: number
  readonly started: string
  readonly seen: WorkerMessage[]
  readonly closed: Promise<WorkerClose>
  readonly next: (type: string, timeout?: number) => Promise<WorkerMessage>
  readonly send: (type: string) => void
}

class WorkerMessageTimeout extends Error {}

async function runWorkerPair(input: WorkerInput, beforeRelease: () => Promise<void>) {
  const workerPath = path.join(import.meta.dir, "fixture", "integration-refresh-worker.ts")
  const coreDirectory = path.resolve(import.meta.dir, "..")
  const workers: WorkerSession[] = []
  try {
    const first = await spawnRefreshWorker(workerPath, coreDirectory, { ...input, role: "first" as const })
    workers.push(first)
    expect((await first.next("ready")).access).toBe("old-access")
    first.send("resolve")
    await first.next("resolver-started")
    await first.next("lock-entered")
    first.send("continue-lock")
    await first.next("lock-started")
    expect((await first.next("refresh-entered")).refresh).toBe("old-refresh")

    const second = await spawnRefreshWorker(workerPath, coreDirectory, { ...input, role: "second" as const })
    workers.push(second)
    expect((await second.next("ready")).access).toBe("old-access")
    second.send("resolve")
    await second.next("resolver-started")
    await second.next("lock-entered")
    second.send("continue-lock")
    await second.next("lock-started")

    const earlyRefresh = await optionalMessage(second, "refresh-entered", 1_000)
    if (earlyRefresh) throw new Error("The second process refreshed while the first resolver still held its refresh")
    await beforeRelease()
    first.send("release-refresh")

    const firstResult = await first.next("result")
    const secondResult = await second.next("result")
    const [firstClose, secondClose] = await Promise.all([
      within(first.closed, "first OAuth refresh worker did not exit"),
      within(second.closed, "second OAuth refresh worker did not exit"),
    ])
    expect(firstClose).toEqual({ code: 0, signal: null })
    expect(secondClose).toEqual({ code: 0, signal: null })
    expect(first.seen.filter((message) => message.type === "refresh-entered")).toHaveLength(1)
    expect(second.seen.filter((message) => message.type === "refresh-entered")).toEqual([])
    return { first: firstResult, second: secondResult }
  } finally {
    await Promise.all(
      workers.map((worker) => {
        worker.send("continue-lock")
        worker.send("release-refresh")
        return stopRefreshWorker(worker)
      }),
    )
  }
}

async function optionalMessage(worker: WorkerSession, type: string, timeout: number) {
  try {
    return await worker.next(type, timeout)
  } catch (error) {
    if (error instanceof WorkerMessageTimeout) return undefined
    throw error
  }
}

async function spawnRefreshWorker(workerPath: string, coreDirectory: string, input: WorkerInput & { role: string }) {
  const child = spawn(process.execPath, [workerPath, JSON.stringify(input)], {
    cwd: coreDirectory,
    stdio: ["pipe", "pipe", "pipe"],
  })
  const session = observeWorker(child)
  await new Promise<void>((resolve, reject) => {
    if (child.pid !== undefined) return resolve()
    child.once("spawn", () => resolve())
    child.once("error", reject)
  })
  if (child.pid === undefined) throw new Error("OAuth refresh worker has no PID")
  return { ...session, pid: child.pid, started: await processStartIdentity(child.pid) }
}

function observeWorker(child: ChildProcess) {
  const queued: WorkerMessage[] = []
  const seen: WorkerMessage[] = []
  const waiters = new Array<WorkerWaiter>()
  let buffer = ""
  let stderr = ""
  let close: WorkerClose | undefined
  let childError: Error | undefined
  const closed = new Promise<WorkerClose>((resolve) => {
    child.once("close", (code, signal) => {
      close = { code, signal }
      resolve(close)
      for (const waiter of waiters.splice(0)) {
        if (waiter.timer) clearTimeout(waiter.timer)
        waiter.reject(new Error(`OAuth refresh worker closed before ${waiter.type}; stderr: ${stderr}`))
      }
    })
  })

  child.stdout?.on("data", (chunk: Buffer) => {
    buffer += chunk.toString()
    for (;;) {
      const newline = buffer.indexOf("\n")
      if (newline === -1) break
      const line = buffer.slice(0, newline)
      buffer = buffer.slice(newline + 1)
      if (!line) continue
      const message = JSON.parse(line) as WorkerMessage
      seen.push(message)
      const index = waiters.findIndex((waiter) => waiter.type === message.type)
      if (index === -1) {
        queued.push(message)
        continue
      }
      const [waiter] = waiters.splice(index, 1)
      if (waiter?.timer) clearTimeout(waiter.timer)
      waiter?.resolve(message)
    }
  })
  child.stderr?.on("data", (chunk: Buffer) => (stderr += chunk.toString()))
  child.on("error", (error) => {
    childError = error
    for (const waiter of waiters.splice(0)) {
      if (waiter.timer) clearTimeout(waiter.timer)
      waiter.reject(error)
    }
  })

  return {
    child,
    seen,
    closed,
    next(type: string, timeout = 5_000) {
      const index = queued.findIndex((message) => message.type === type)
      if (index !== -1) return Promise.resolve(queued.splice(index, 1)[0]!)
      if (childError) return Promise.reject(childError)
      if (close) return Promise.reject(new Error(`OAuth refresh worker closed before ${type}; stderr: ${stderr}`))
      return new Promise<WorkerMessage>((resolve, reject) => {
        const waiter: WorkerWaiter = { type, resolve, reject }
        waiter.timer = setTimeout(() => {
          const index = waiters.indexOf(waiter)
          if (index !== -1) waiters.splice(index, 1)
          reject(new WorkerMessageTimeout(`Timed out waiting for ${type}; stderr: ${stderr}`))
        }, timeout)
        waiters.push(waiter)
      })
    },
    send(type: string) {
      if (child.stdin && !child.stdin.destroyed && child.exitCode === null && child.signalCode === null)
        child.stdin.write(`${JSON.stringify({ type })}\n`)
    },
  }
}

async function processStartIdentity(pid: number) {
  const stat = await fs.readFile(`/proc/${pid}/stat`, "utf8")
  const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)
  const identity = fields[19]
  if (!identity) throw new Error(`Could not read process start identity for PID ${pid}`)
  return identity
}

async function stopRefreshWorker(worker: WorkerSession) {
  if (worker.child.exitCode !== null || worker.child.signalCode !== null) {
    await within(worker.closed, `OAuth refresh worker ${worker.pid} did not report close`)
    return
  }
  const identity = await processStartIdentity(worker.pid).catch(() => undefined)
  if (identity === undefined) {
    await within(worker.closed, `OAuth refresh worker ${worker.pid} did not close after its PID disappeared`)
    return
  }
  if (identity !== worker.started) throw new Error(`Refusing to stop reused PID ${worker.pid}`)
  worker.child.kill("SIGTERM")
  await within(worker.closed, `OAuth refresh worker ${worker.pid} did not stop`)
}

function within<A>(promise: Promise<A>, message: string, timeout = 5_000) {
  return new Promise<A>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), timeout)
    promise.then(
      (value) => {
        clearTimeout(timer)
        resolve(value)
      },
      (error) => {
        clearTimeout(timer)
        reject(error)
      },
    )
  })
}

function waitForDeferred(deferred: Deferred.Deferred<void>, timeoutMs: number) {
  return Effect.gen(function* () {
    const deadline = Date.now() + timeoutMs
    while (!(yield* Deferred.isDone(deferred)) && Date.now() < deadline) {
      yield* Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)))
    }
    return yield* Deferred.isDone(deferred)
  })
}
