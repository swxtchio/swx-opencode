import { afterEach, describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppProcess } from "@opencode-ai/core/process"
import { stat } from "node:fs/promises"
import { Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { HttpClient, HttpClientResponse } from "effect/unstable/http"
import { eq } from "drizzle-orm"
import { GlobalBus, type GlobalEvent } from "@/bus/global"
import { ExperimentalPaths } from "../../src/server/routes/instance/httpapi/groups/experimental"
import { Session } from "@/session/session"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { Database } from "@opencode-ai/core/database/database"
import { AccountV2 } from "@opencode-ai/core/account"
import { AccountTable } from "@opencode-ai/core/account/sql"
import { Worktree } from "../../src/worktree"
import { registerDisposer } from "../../src/effect/instance-registry"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import { Project } from "../../src/project/project"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance, tmpdirScoped } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { awaitWithTimeout } from "../lib/effect"
import { httpApiLayer, httpApiLayerWithAppReplacements, requestInDirectory } from "./httpapi-layer"

const it = testEffect(Layer.mergeAll(LayerNode.compile(LayerNode.group([Session.node, Database.node])), httpApiLayer))
const testWorktreeMutations = process.platform === "win32" ? it.instance.skip : it.instance
let failWorktreeReset: string | undefined

const failingAppProcess = Layer.effect(
  AppProcess.Service,
  Effect.gen(function* () {
    const appProcess = yield* AppProcess.Service
    return AppProcess.Service.of({
      ...appProcess,
      run: (command, options) => {
        if (
          failWorktreeReset &&
          command._tag === "StandardCommand" &&
          command.command === "git" &&
          command.args[0] === "reset" &&
          command.args[1] === "--hard" &&
          command.options.cwd !== failWorktreeReset
        ) {
          failWorktreeReset = undefined
          return Effect.succeed({
            command: "git reset --hard",
            exitCode: 1,
            stdout: Buffer.alloc(0),
            stderr: Buffer.from("simulated HttpApi checkout failure"),
            stdoutTruncated: false,
            stderrTruncated: false,
          } satisfies AppProcess.RunResult)
        }
        return appProcess.run(command, options)
      },
    })
  }),
).pipe(Layer.provide(LayerNode.compile(AppProcess.node)))

const failedWorktreeIt = testEffect(
  Layer.mergeAll(
    LayerNode.compile(LayerNode.group([Session.node, Database.node])),
    httpApiLayerWithAppReplacements([[AppProcess.node, failingAppProcess]]),
  ),
)
const failedWorktreeMutation = process.platform === "win32" ? failedWorktreeIt.instance.skip : failedWorktreeIt.instance
let admissionDirectory: string | undefined
let admissionSignal: Deferred.Deferred<void> | undefined
let admissionReloadSignal: Deferred.Deferred<void> | undefined
const observingInstanceStore = Layer.effect(
  InstanceStore.Service,
  Effect.gen(function* () {
    const store = yield* InstanceStore.Service
    return InstanceStore.Service.of({
      ...store,
      load: (input) => {
        if (input.directory !== admissionDirectory || !admissionSignal) return store.load(input)
        const signal = admissionSignal
        admissionSignal = undefined
        return Effect.gen(function* () {
          yield* Deferred.succeed(signal, undefined)
          return yield* store.load(input)
        })
      },
      reload: (input) => {
        if (input.directory !== admissionDirectory || !admissionReloadSignal) return store.reload(input)
        const signal = admissionReloadSignal
        admissionReloadSignal = undefined
        return Effect.gen(function* () {
          yield* Deferred.succeed(signal, undefined)
          return yield* store.reload(input)
        })
      },
    })
  }),
).pipe(
  Layer.provide(
    LayerNode.compile(InstanceStore.node, [[InstanceStore.bootstrapNode, InstanceBootstrap.node]]),
  ),
)
type ProjectInitGate = {
  directory: string
  firstStarted: Deferred.Deferred<void>
  releaseFirst: Deferred.Deferred<void>
  secondStarted: Deferred.Deferred<void>
  releaseSecond: Deferred.Deferred<void>
  calls: number
}
let projectInitGate: ProjectInitGate | undefined
const overlappingProject = Layer.effect(
  Project.Service,
  Effect.gen(function* () {
    const project = yield* Project.Service
    return Project.Service.of({
      ...project,
      initGit: (input) => {
        const gate = projectInitGate
        if (!gate || input.directory !== gate.directory) return project.initGit(input)
        const call = gate.calls++
        return Effect.gen(function* () {
          if (call === 0) {
            yield* Deferred.succeed(gate.firstStarted, undefined)
            yield* Deferred.await(gate.releaseFirst)
          }
          if (call === 1) {
            yield* Deferred.succeed(gate.secondStarted, undefined)
            yield* Deferred.await(gate.releaseSecond)
          }
          return yield* project.initGit(input)
        })
      },
    })
  }),
).pipe(Layer.provide(LayerNode.compile(Project.node)))
const admissionIt = testEffect(
  Layer.mergeAll(
    LayerNode.compile(LayerNode.group([Session.node, Database.node])),
    httpApiLayerWithAppReplacements([
      [InstanceStore.node, observingInstanceStore],
      [Project.node, overlappingProject],
    ]),
  ),
)

function request(path: string, directory: string, init: RequestInit = {}) {
  return requestInDirectory(path, directory, init)
}

function createSession(input?: Session.CreateInput) {
  return Session.use.create(input)
}

function json<T>(response: HttpClientResponse.HttpClientResponse) {
  return response.json.pipe(Effect.map((value) => value as T))
}

function waitReady(input: { directory?: string; name?: string }) {
  return Effect.gen(function* () {
    const ready = yield* Deferred.make<void>()
    const on = (event: GlobalEvent) => {
      if (event.payload.type !== Worktree.Event.Ready.type) return
      if (input.directory && event.directory !== input.directory) return
      if (input.name && event.payload.properties.name !== input.name) return
      Deferred.doneUnsafe(ready, Effect.void)
    }

    GlobalBus.on("event", on)
    yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", on)))

    return yield* Deferred.await(ready).pipe(
      Effect.timeoutOrElse({
        duration: "10 seconds",
        orElse: () => Effect.fail(new Error("timed out waiting for worktree.ready")),
      }),
    )
  })
}

function watchWorktreeTerminal() {
  return Effect.gen(function* () {
    const events: GlobalEvent[] = []
    const waiters = new Map<string, Deferred.Deferred<GlobalEvent>>()
    const on = (event: GlobalEvent) => {
      if (event.payload.type !== Worktree.Event.Ready.type && event.payload.type !== Worktree.Event.Failed.type) return
      events.push(event)
      const waiting = event.directory ? waiters.get(event.directory) : undefined
      if (waiting) Deferred.doneUnsafe(waiting, Effect.succeed(event))
    }
    GlobalBus.on("event", on)
    yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", on)))

    return (directory: string) =>
      Effect.gen(function* () {
        const waiting = yield* Deferred.make<GlobalEvent>()
        const existing = yield* Effect.sync(() => {
          const event = events.find((item) => item.directory === directory)
          if (event) return event
          waiters.set(directory, waiting)
          return undefined
        })
        if (existing) return existing
        return yield* awaitWithTimeout(
          Deferred.await(waiting),
          `worktree create did not publish a terminal event for ${directory}`,
          "5 seconds",
        )
      })
  })
}

function insertAccount() {
  return Effect.acquireRelease(
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(AccountTable)
        .values({
          id: AccountV2.ID.make("account-test"),
          email: "test@example.com",
          url: "https://console.example.com",
          access_token: AccountV2.AccessToken.make("access"),
          refresh_token: AccountV2.RefreshToken.make("refresh"),
          time_created: Date.now(),
          time_updated: Date.now(),
        })
        .run()
        .pipe(Effect.orDie)
      return "account-test"
    }),
    (id) =>
      Database.Service.use(({ db }) =>
        db
          .delete(AccountTable)
          .where(eq(AccountTable.id, AccountV2.ID.make(id)))
          .run()
          .pipe(Effect.orDie),
      ),
  )
}

function setSessionUpdated(session: Session.Info, updated: number) {
  return Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .update(SessionTable)
      .set({ time_updated: updated })
      .where(eq(SessionTable.id, session.id))
      .run()
      .pipe(Effect.orDie)
  })
}

function withCreatedWorktree(
  directory: string,
  use: (info: Worktree.Info) => Effect.Effect<void, unknown, HttpClient.HttpClient>,
) {
  const name = "api-test"
  const headers = { "content-type": "application/json" }
  return Effect.acquireUseRelease(
    Effect.gen(function* () {
      const ready = yield* waitReady({ name }).pipe(Effect.forkScoped)
      const created = yield* request(ExperimentalPaths.worktree, directory, {
        method: "POST",
        headers,
        body: JSON.stringify({ name }),
      })

      expect(created.status).toBe(200)
      const info = yield* json<Worktree.Info>(created)
      expect(info).toMatchObject({ name, branch: "opencode/api-test" })
      yield* Fiber.join(ready)
      return info
    }),
    use,
    (info) =>
      Effect.gen(function* () {
        const removed = yield* request(ExperimentalPaths.worktree, directory, {
          method: "DELETE",
          headers,
          body: JSON.stringify({ directory: info.directory }),
        })
        if (removed.status !== 200) return yield* Effect.fail(new Error(`failed to remove worktree: ${removed.status}`))
        const ok = yield* json<boolean>(removed)
        if (!ok) return yield* Effect.fail(new Error(`failed to remove worktree ${info.directory}`))
      }),
  )
}

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

describe("experimental HttpApi", () => {
  admissionIt.live(
    "admits overlapping initGit reloads and project reads while a reload disposer is pending",
    () =>
      Effect.gen(function* () {
        const directory = yield* tmpdirScoped()
        const firstInitStarted = yield* Deferred.make<void>()
        const releaseFirstInit = yield* Deferred.make<void>()
        const secondInitStarted = yield* Deferred.make<void>()
        const releaseSecondInit = yield* Deferred.make<void>()
        const disposerStarted = yield* Deferred.make<void>()
        const disposerFinished = yield* Deferred.make<void>()
        let releaseDisposer: (() => void) | undefined
        let unregister: (() => void) | undefined
        let disposeCalls = 0
        admissionDirectory = directory
        admissionSignal = undefined
        admissionReloadSignal = undefined
        projectInitGate = {
          directory,
          firstStarted: firstInitStarted,
          releaseFirst: releaseFirstInit,
          secondStarted: secondInitStarted,
          releaseSecond: releaseSecondInit,
          calls: 0,
        }
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            if (releaseDisposer) yield* Effect.sync(releaseDisposer)
            yield* Deferred.succeed(releaseFirstInit, undefined)
            yield* Deferred.succeed(releaseSecondInit, undefined)
            if (unregister) yield* Effect.sync(unregister)
            yield* Effect.sync(() => {
              admissionDirectory = undefined
              admissionSignal = undefined
              admissionReloadSignal = undefined
              projectInitGate = undefined
            })
          }),
        )

        unregister = yield* Effect.sync(() =>
          registerDisposer((target) => {
            if (target !== directory || ++disposeCalls > 1) return Promise.resolve()
            return new Promise<void>((resolve) => {
              releaseDisposer = resolve
              Deferred.doneUnsafe(disposerStarted, Effect.void)
            }).then(() => {
              Deferred.doneUnsafe(disposerFinished, Effect.void)
            })
          }),
        )
        const first = yield* request("/project/git/init", directory, { method: "POST" }).pipe(
          Effect.forkScoped({ startImmediately: true }),
        )
        const second = yield* request("/project/git/init", directory, { method: "POST" }).pipe(
          Effect.forkScoped({ startImmediately: true }),
        )
        yield* awaitWithTimeout(
          Effect.all([Deferred.await(firstInitStarted), Deferred.await(secondInitStarted)], { concurrency: "unbounded" }),
          "both project init requests did not enter the production handler",
          "15 seconds",
        )
        yield* Deferred.succeed(releaseFirstInit, undefined)
        yield* awaitWithTimeout(Deferred.await(disposerStarted), "first initGit did not reach reload disposal", "15 seconds")
        const successorReloadStarted = yield* Deferred.make<void>()
        admissionReloadSignal = successorReloadStarted
        yield* Deferred.succeed(releaseSecondInit, undefined)
        yield* awaitWithTimeout(
          Deferred.await(successorReloadStarted),
          "second initGit did not request its overlapping reload",
          "15 seconds",
        )
        expect(second.pollUnsafe()).toBeUndefined()

        const requestLoadStarted = yield* Deferred.make<void>()
        admissionSignal = requestLoadStarted
        const current = yield* request("/project/current", directory).pipe(Effect.forkScoped({ startImmediately: true }))
        yield* awaitWithTimeout(
          Deferred.await(requestLoadStarted),
          "concurrent project request did not enter InstanceStore.load",
          "15 seconds",
        )
        expect(current.pollUnsafe()).toBeUndefined()

        if (releaseDisposer) yield* Effect.sync(releaseDisposer)
        releaseDisposer = undefined
        yield* awaitWithTimeout(Deferred.await(disposerFinished), "reload disposer did not settle")
        if (unregister) {
          yield* Effect.sync(unregister)
          unregister = undefined
        }
        const [initialized, successor, admitted] = yield* Effect.all(
          [
            awaitWithTimeout(Fiber.await(first), "first project.initGit request did not finish", "25 seconds"),
            awaitWithTimeout(Fiber.await(second), "second project.initGit request did not finish", "25 seconds"),
            awaitWithTimeout(Fiber.await(current), "concurrent project request did not finish", "25 seconds"),
          ],
          { concurrency: "unbounded" },
        )
        expect(Exit.isSuccess(initialized)).toBe(true)
        expect(Exit.isSuccess(successor)).toBe(true)
        expect(Exit.isSuccess(admitted)).toBe(true)
        if (Exit.isSuccess(initialized)) expect(initialized.value.status).toBe(200)
        if (Exit.isSuccess(successor)) expect(successor.value.status).toBe(200)
        if (Exit.isSuccess(admitted)) expect(admitted.value.status).toBe(200)
      }),
    { timeout: 120_000 },
  )

  it.instance(
    "serves read-only experimental endpoints through the default server app",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const directory = tmp.directory
        const [consoleState, consoleOrgs, toolList, toolIDs, worktrees, resources] = yield* Effect.all(
          [
            request(ExperimentalPaths.console, directory),
            request(ExperimentalPaths.consoleOrgs, directory),
            request(`${ExperimentalPaths.tool}?provider=opencode&model=gpt-5`, directory),
            request(ExperimentalPaths.toolIDs, directory),
            request(ExperimentalPaths.worktree, directory),
            request(ExperimentalPaths.resource, directory),
          ],
          { concurrency: "unbounded" },
        )

        expect(consoleState.status).toBe(200)
        expect(yield* json(consoleState)).toEqual({
          consoleManagedProviders: [],
          switchableOrgCount: 0,
        })

        expect(consoleOrgs.status).toBe(200)
        expect(yield* json(consoleOrgs)).toEqual({ orgs: [] })

        expect(toolList.status).toBe(200)
        expect(yield* json<unknown[]>(toolList)).toContainEqual(
          expect.objectContaining({
            id: "bash",
            description: expect.any(String),
            parameters: expect.any(Object),
          }),
        )

        expect(toolIDs.status).toBe(200)
        expect(yield* json(toolIDs)).toContain("bash")

        expect(worktrees.status).toBe(200)
        expect(yield* json(worktrees)).toEqual([])

        expect(resources.status).toBe(200)
        expect(yield* json(resources)).toEqual({})
      }),
    {
      config: {
        formatter: false,
        lsp: false,
        mcp: {
          demo: {
            type: "local",
            command: ["echo", "demo"],
            enabled: false,
          },
        },
      },
    },
  )

  it.instance("returns declared worktree errors", () =>
    Effect.gen(function* () {
      const tmp = yield* TestInstance
      const response = yield* request(ExperimentalPaths.worktree, tmp.directory, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      })

      expect(response.status).toBe(400)
      expect(yield* json(response)).toEqual({
        name: "WorktreeNotGitError",
        data: { message: "Worktrees are only supported for git projects" },
      })
    }),
  )

  it.instance(
    "serves Console org switch through the default server app",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const accountID = yield* insertAccount()
        const switched = yield* request(ExperimentalPaths.consoleSwitch, tmp.directory, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ accountID, orgID: "org-test" }),
        })

        expect(switched.status).toBe(200)
        expect(yield* json(switched)).toBe(true)
      }),
    { config: { formatter: false, lsp: false } },
  )

  it.instance(
    "serves global session list through the default server app",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const first = yield* createSession({ title: "page-one" })
        const second = yield* createSession({ title: "page-two" })
        yield* setSessionUpdated(first, 1)
        yield* setSessionUpdated(second, 2)

        const page = yield* request(
          `${ExperimentalPaths.session}?${new URLSearchParams({ directory: tmp.directory, limit: "1" })}`,
          tmp.directory,
        )
        expect(page.status).toBe(200)
        expect(page.headers["x-next-cursor"]).toBeTruthy()

        const body = yield* json<Session.GlobalInfo[]>(page)
        expect(body.map((session) => session.id)).toEqual([second.id])
        expect(body[0].project?.id).toBe(second.projectID)

        const next = yield* request(
          `${ExperimentalPaths.session}?${new URLSearchParams({
            directory: tmp.directory,
            limit: "10",
            cursor: body[0].time.updated.toString(),
          })}`,
          tmp.directory,
        )
        expect(next.status).toBe(200)
        expect((yield* json<Session.GlobalInfo[]>(next)).map((session) => session.id)).toContain(first.id)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  testWorktreeMutations(
    "serves worktree mutations through the default server app",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        yield* withCreatedWorktree(tmp.directory, (info) =>
          Effect.gen(function* () {
            const listed = yield* request(ExperimentalPaths.worktree, tmp.directory)
            expect(listed.status).toBe(200)
            expect(yield* json(listed)).toContain(info.directory)

            const reset = yield* request(ExperimentalPaths.worktreeReset, tmp.directory, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ directory: info.directory }),
            })

            expect(reset.status).toBe(200)
            expect(yield* json(reset)).toBe(true)
          }),
        )

        const afterRemove = yield* request(ExperimentalPaths.worktree, tmp.directory)
        expect(afterRemove.status).toBe(200)
        expect(yield* json(afterRemove)).toEqual([])
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  failedWorktreeMutation(
    "reports a background worktree failure and cleans it through the HttpApi",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const terminal = yield* watchWorktreeTerminal()
        failWorktreeReset = tmp.directory
        yield* Effect.addFinalizer(() => Effect.sync(() => (failWorktreeReset = undefined)).pipe(Effect.asVoid))

        const createdResult = yield* Effect.exit(
          awaitWithTimeout(
            request(ExperimentalPaths.worktree, tmp.directory, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ name: "api-failed-checkout" }),
            }),
            "HttpApi worktree.create did not return after setup",
            "5 seconds",
          ),
        )
        expect(Exit.isSuccess(createdResult)).toBe(true)
        if (Exit.isFailure(createdResult)) return
        const created = createdResult.value
        expect(created.status).toBe(200)
        const info = yield* json<Worktree.Info>(created)
        yield* Effect.addFinalizer(() =>
          request(ExperimentalPaths.worktree, tmp.directory, {
            method: "DELETE",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ directory: info.directory }),
          }).pipe(Effect.ignore),
        )

        const failedResult = yield* Effect.exit(
          awaitWithTimeout(terminal(info.directory), "HttpApi worktree failure was not observed", "5 seconds"),
        )
        expect(Exit.isSuccess(failedResult)).toBe(true)
        if (Exit.isFailure(failedResult)) return
        const failed = failedResult.value
        expect(failed.payload.type).toBe(Worktree.Event.Failed.type)
        expect(failed.payload.properties.message).toContain("simulated HttpApi checkout failure")

        const removedResult = yield* Effect.exit(
          awaitWithTimeout(
            request(ExperimentalPaths.worktree, tmp.directory, {
              method: "DELETE",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ directory: info.directory }),
            }),
            "HttpApi worktree.remove did not complete after the failed create",
            "5 seconds",
          ),
        )
        expect(Exit.isSuccess(removedResult)).toBe(true)
        if (Exit.isFailure(removedResult)) return
        const removed = removedResult.value
        expect(removed.status).toBe(200)
        expect(yield* json<boolean>(removed)).toBe(true)
        const listedResult = yield* Effect.exit(
          awaitWithTimeout(
            request(ExperimentalPaths.worktree, tmp.directory),
            "HttpApi worktree.list did not complete after cleanup",
            "5 seconds",
          ),
        )
        expect(Exit.isSuccess(listedResult)).toBe(true)
        if (Exit.isFailure(listedResult)) return
        const listed = listedResult.value
        expect(listed.status).toBe(200)
        expect(yield* json<Worktree.Info[]>(listed)).toEqual([])
        const directoryExists = yield* Effect.promise(() => stat(info.directory).then(() => true, () => false))
        expect(directoryExists).toBe(false)
      }),
      { git: true, config: { formatter: false, lsp: false } },
      { timeout: 15_000 },
  )
})
