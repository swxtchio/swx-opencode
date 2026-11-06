import { expect } from "bun:test"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { Effect, Option } from "effect"
import { readFileSync } from "node:fs"
import path from "node:path"
import { cliIt, deadline } from "../../lib/cli-process"
import { withTimeout } from "../../../src/util/timeout"

function sameProcess(identity: EventWriter.ProcessIdentity) {
  if (!identity.bootID || !identity.startTicks) return false
  const current = EventWriter.readIdentity(identity.pid)
  return current?.bootID === identity.bootID && current.startTicks === identity.startTicks
}

function processGroupMember(pgid: number, identity: EventWriter.ProcessIdentity | undefined) {
  if (!identity || !sameProcess(identity)) return false
  try {
    const stat = readFileSync(`/proc/${identity.pid}/stat`, "utf8")
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ")
    const current = EventWriter.readIdentity(identity.pid)
    return current?.bootID === identity.bootID && current.startTicks === identity.startTicks && Number(fields[2]) === pgid
  } catch {
    return false
  }
}

cliIt.live(
  "runMini completes a held attached request beyond Bun's idle timeout",
  ({ home }) =>
    Effect.gen(function* () {
      if (process.platform !== "linux") return

      const readyFile = path.join(home, "run-mini-ready.json")
      const requestFile = path.join(home, "run-mini-request.json")
      const controlFile = path.join(home, "run-mini-control.json")
      const received = Promise.withResolvers<{ url: string; method: string }>()
      const controlExpired = Promise.withResolvers<void>()
      const sessionCancelled = Promise.withResolvers<void>()
      const releaseSession = Promise.withResolvers<void>()
      let sessionWasCancelled = false
      const encoder = new TextEncoder()
      const server = Bun.serve({
        port: 0,
        fetch: (request) => {
          const url = new URL(request.url)
          if (url.pathname === "/control")
            return new Response(
              new ReadableStream<Uint8Array>({
                start(controller) {
                  controller.enqueue(encoder.encode("control-ready:"))
                },
                pull: () => new Promise<void>(() => {}),
                cancel() {
                  controlExpired.resolve()
                },
              }),
              { headers: { "content-type": "text/plain" } },
            )
          const result = { url: url.pathname, method: request.method }
          received.resolve(result)
          if (url.pathname.includes("ses_transport_missing"))
            return new Response(
              new ReadableStream<Uint8Array>({
                start(controller) {
                  controller.enqueue(encoder.encode('{"error":"'))
                },
                pull(controller) {
                  return releaseSession.promise.then(() => {
                    if (sessionWasCancelled) return
                    controller.enqueue(encoder.encode('not found"}'))
                    controller.close()
                  })
                },
                cancel() {
                  sessionWasCancelled = true
                  sessionCancelled.resolve()
                },
              }),
              { status: 404, headers: { "content-type": "application/json" } },
            )
          return Response.json({ error: "not found" }, { status: 404 })
        },
      })
      yield* Effect.addFinalizer(() =>
        Effect.promise(async () => {
          releaseSession.resolve()
          await server.stop(true)
        }).pipe(Effect.asVoid),
      )

      const workerFile = path.join(import.meta.dir, "mini-attach-worker.ts")
      const env = {
        ...process.env,
        HOME: home,
        OPENCODE_TEST_HOME: home,
        XDG_CONFIG_HOME: path.join(home, ".config"),
        XDG_DATA_HOME: path.join(home, ".local/share"),
        XDG_STATE_HOME: path.join(home, ".local/state"),
        XDG_CACHE_HOME: path.join(home, ".cache"),
        OPENCODE_DB: ":memory:",
        OPENCODE_CONFIG_CONTENT: "{}",
        OPENCODE_DISABLE_PROJECT_CONFIG: "1",
        OPENCODE_PURE: "1",
        OPENCODE_DISABLE_AUTOUPDATE: "1",
        OPENCODE_DISABLE_AUTOCOMPACT: "1",
        OPENCODE_DISABLE_MODELS_FETCH: "1",
        OPENCODE_AUTH_CONTENT: "{}",
        BUN_CONFIG_HTTP_IDLE_TIMEOUT: "2",
        SWX_MINI_ATTACH_URL: server.url.toString(),
        SWX_MINI_READY_FILE: readyFile,
        SWX_MINI_REQUEST_FILE: requestFile,
        SWX_MINI_CONTROL_URL: new URL("/control", server.url).toString(),
        SWX_MINI_CONTROL_FILE: controlFile,
      }
      const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`
      const command = `${quote(process.execPath)} ${quote(workerFile)}`
      const child = Bun.spawn(["script", "-q", "-e", "-c", command, "/dev/null"], {
        cwd: process.cwd(),
        env,
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
        detached: true,
      })
      const scriptIdentity = EventWriter.readIdentity(child.pid)
      let miniIdentity: EventWriter.ProcessIdentity | undefined
      const output = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()])

      try {
        if (!scriptIdentity?.bootID || !scriptIdentity.startTicks)
          throw new Error("runMini PTY launcher has no recorded start identity; cleanup skipped")
        const ready = yield* Effect.promise(async () => {
          const readyDeadline = Date.now() + 15_000
          while (Date.now() < readyDeadline) {
            try {
              const value: unknown = await Bun.file(readyFile).json()
              if (
                typeof value === "object" &&
                value !== null &&
                "pid" in value &&
                typeof value.pid === "number" &&
                "terminal" in value &&
                typeof value.terminal === "boolean"
              )
                return { pid: value.pid, terminal: value.terminal }
            } catch {}
            if (!sameProcess(scriptIdentity)) throw new Error("runMini worker exited before PTY readiness")
            await new Promise((resolve) => setTimeout(resolve, 20))
          }
          throw new Error("runMini worker did not report its PTY readiness")
        })
        if (!ready.terminal) throw new Error("runMini worker did not receive a terminal")

        miniIdentity = EventWriter.readIdentity(ready.pid)
        if (!miniIdentity?.bootID || !miniIdentity.startTicks || !sameProcess(miniIdentity))
          throw new Error("runMini worker has no current start identity")

        const request = yield* Effect.promise(() => received.promise).pipe(Effect.timeout("10 seconds"))
        expect(request.method).toBe("GET")
        const record: unknown = yield* Effect.promise(() => Bun.file(requestFile).json())
        if (
          typeof record !== "object" ||
          record === null ||
          !("url" in record) ||
          typeof record.url !== "string" ||
          !("timeoutDisabled" in record) ||
          typeof record.timeoutDisabled !== "boolean"
        )
          throw new Error("runMini worker did not record its attached fetch options")
        expect(record.url).toContain("ses_transport_missing")
        expect(record.timeoutDisabled).toBe(true)

        const outcome = yield* Effect.race(
          Effect.promise(() => controlExpired.promise).pipe(Effect.as("control-expired" as const)),
          Effect.promise(() => sessionCancelled.promise).pipe(Effect.as("session-cancelled" as const)),
        ).pipe(Effect.timeout("25 seconds"))
        expect(outcome).toBe("control-expired")
        expect(sessionWasCancelled).toBe(false)
        const controlResult = yield* Effect.promise(async () => {
          const deadline = Date.now() + 3_000
          while (Date.now() < deadline) {
            try {
              const value: unknown = await Bun.file(controlFile).json()
              if (typeof value === "object" && value !== null && "type" in value) return value
            } catch {}
            await Bun.sleep(20)
          }
          throw new Error("mini control fetch did not record its idle-timeout result")
        }).pipe(Effect.timeout("5 seconds"))
        if (
          typeof controlResult !== "object" ||
          controlResult === null ||
          !("type" in controlResult) ||
          !("name" in controlResult)
        )
          throw new Error("mini control fetch result is malformed")
        expect(controlResult.type).toBe("failed")
        expect(controlResult.name).toBe("TimeoutError")

        releaseSession.resolve()
        const exitCode = yield* Effect.promise(() => child.exited).pipe(Effect.timeout("15 seconds"))
        const [stdout, stderr] = yield* Effect.promise(() => withTimeout(output, 5_000))
        expect(exitCode).toBe(1)
        expect(`${stdout}\n${stderr}`).toContain("Session not found")
      } finally {
        if (miniIdentity && sameProcess(miniIdentity)) {
          try {
            process.kill(miniIdentity.pid, "SIGKILL")
          } catch {}
        }
        if (scriptIdentity && sameProcess(scriptIdentity)) {
          try {
            process.kill(scriptIdentity.pid, "SIGKILL")
          } catch {}
        }
        const stopped = yield* Effect.promise(() => child.exited).pipe(Effect.timeoutOption("5 seconds"))
        const drained = yield* Effect.promise(() => withTimeout(output, 5_000)).pipe(Effect.timeoutOption("5 seconds"))
        if (Option.isNone(stopped)) throw new Error("runMini worker survived test cleanup")
        if (Option.isNone(drained)) throw new Error("runMini worker output did not drain after cleanup")
      }
    }),
  deadline(60_000),
)

cliIt.live(
  "runMini's successful interactive attach uses a no-timeout SDK client and preserves cancellation",
  ({ home }) =>
    Effect.gen(function* () {
      if (process.platform !== "linux") return

      const sessionID = "ses_mini_interactive"
      const requestFile = path.join(home, "run-mini-interactive-requests.json")
      const controlFile = path.join(home, "run-mini-interactive-control.json")
      const resultFile = path.join(home, "run-mini-interactive-result.json")
      const identityFile = path.join(home, "run-mini-interactive-identity.json")
      const healthStarted = Promise.withResolvers<void>()
      const healthCancelled = Promise.withResolvers<void>()
      const controlExpired = Promise.withResolvers<void>()
      const cancelHealthStarted = Promise.withResolvers<void>()
      const cancelHealthCancelled = Promise.withResolvers<void>()
      const releaseHealth = Promise.withResolvers<void>()
      const encoder = new TextEncoder()
      let healthRequests = 0
      let healthWasCancelled = false
      const server = Bun.serve({
        port: 0,
        idleTimeout: 30,
        fetch: (request) => {
          const url = new URL(request.url)
          if (url.pathname === `/session/${sessionID}`)
            return Response.json({ id: sessionID, title: "Attached session", directory: home })
          if (url.pathname === "/config") return Response.json({ share: "manual" })
          if (url.pathname === "/global/health") {
            healthRequests += 1
            if (healthRequests === 1) {
              healthStarted.resolve()
              return new Response(
                new ReadableStream<Uint8Array>({
                  start(controller) {
                    controller.enqueue(encoder.encode('{"healthy":true,"version":"'))
                  },
                  pull(controller) {
                    return releaseHealth.promise.then(() => {
                      if (healthWasCancelled) return
                      controller.enqueue(encoder.encode('test"}'))
                      controller.close()
                    })
                  },
                  cancel() {
                    healthWasCancelled = true
                    healthCancelled.resolve()
                  },
                }),
                { headers: { "content-type": "application/json" } },
              )
            }
            cancelHealthStarted.resolve()
            return new Response(
              new ReadableStream<Uint8Array>({
                start(controller) {
                  controller.enqueue(encoder.encode("cancel-ready:"))
                },
                pull: () => new Promise<void>(() => {}),
                cancel() {
                  cancelHealthCancelled.resolve()
                },
              }),
              { headers: { "content-type": "text/plain" } },
            )
          }
          if (url.pathname === "/control")
            return new Response(
              new ReadableStream<Uint8Array>({
                start(controller) {
                  controller.enqueue(encoder.encode("control-ready:"))
                },
                pull: () => new Promise<void>(() => {}),
                cancel() {
                  controlExpired.resolve()
                },
              }),
              { headers: { "content-type": "text/plain" } },
            )
          return Response.json({ error: `unexpected path: ${url.pathname}` }, { status: 404 })
        },
      })
      yield* Effect.addFinalizer(() =>
        Effect.promise(async () => {
          releaseHealth.resolve()
          await server.stop(true)
        }).pipe(Effect.asVoid),
      )

      const workerFile = path.join(import.meta.dir, "mini-attach-interactive-worker.test.ts")
      const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`
      const command = `${quote(process.execPath)} test --timeout 35_000 ${quote(workerFile)} -t ${quote(
        "runMini interactive attached runtime uses its no-timeout SDK for successful and cancellable calls",
      )}`
      const child = Bun.spawn(["script", "-q", "-e", "-c", command, "/dev/null"], {
        cwd: process.cwd(),
        env: {
          ...process.env,
          HOME: home,
          OPENCODE_TEST_HOME: home,
          XDG_CONFIG_HOME: path.join(home, ".config"),
          XDG_DATA_HOME: path.join(home, ".local/share"),
          XDG_STATE_HOME: path.join(home, ".local/state"),
          XDG_CACHE_HOME: path.join(home, ".cache"),
          OPENCODE_DB: ":memory:",
          OPENCODE_CONFIG_CONTENT: "{}",
          OPENCODE_DISABLE_PROJECT_CONFIG: "1",
          OPENCODE_PURE: "1",
          OPENCODE_DISABLE_AUTOUPDATE: "1",
          OPENCODE_DISABLE_AUTOCOMPACT: "1",
          OPENCODE_DISABLE_MODELS_FETCH: "1",
          OPENCODE_AUTH_CONTENT: "{}",
          BUN_CONFIG_HTTP_IDLE_TIMEOUT: "2",
          SWX_MINI_INTERACTIVE_CHILD: "1",
          SWX_MINI_INTERACTIVE_ATTACH_URL: server.url.toString(),
          SWX_MINI_INTERACTIVE_SESSION: sessionID,
          SWX_MINI_INTERACTIVE_REQUEST_FILE: requestFile,
          SWX_MINI_INTERACTIVE_CONTROL_URL: new URL("/control", server.url).toString(),
          SWX_MINI_INTERACTIVE_CONTROL_FILE: controlFile,
          SWX_MINI_INTERACTIVE_RESULT_FILE: resultFile,
          SWX_MINI_INTERACTIVE_IDENTITY_FILE: identityFile,
        },
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
        detached: true,
      })
      const childIdentity = EventWriter.readIdentity(child.pid)
      const output = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()])
      let commandIdentity: EventWriter.ProcessIdentity | undefined
      yield* Effect.addFinalizer(() =>
        Effect.promise(async () => {
          if (processGroupMember(child.pid, commandIdentity) || processGroupMember(child.pid, childIdentity)) {
            try {
              process.kill(-child.pid, "SIGKILL")
            } catch {}
          }
          const exited = await withTimeout(child.exited, 5_000).then(
            () => true,
            () => false,
          )
          const drained = await withTimeout(output, 5_000).then(
            () => true,
            () => false,
          )
          if (!exited) throw new Error("interactive Mini worker survived identity-owned cleanup")
          if (!drained) throw new Error("interactive Mini worker output did not drain after cleanup")
        }).pipe(Effect.asVoid),
      )

      if (!childIdentity?.bootID || !childIdentity.startTicks)
        throw new Error("interactive Mini attach launcher has no recorded start identity; cleanup skipped")
      commandIdentity = yield* Effect.promise(async () => {
        const deadline = Date.now() + 10_000
        while (Date.now() < deadline) {
          try {
            const value: unknown = await Bun.file(identityFile).json()
            if (
              typeof value === "object" &&
              value !== null &&
              "pid" in value &&
              typeof value.pid === "number" &&
              "bootID" in value &&
              typeof value.bootID === "string" &&
              "startTicks" in value &&
              typeof value.startTicks === "string"
            ) {
              const current = EventWriter.readIdentity(value.pid)
              if (
                current?.bootID &&
                current.startTicks &&
                current.bootID === value.bootID &&
                current.startTicks === value.startTicks
              )
                return current
            }
          } catch {}
          await Bun.sleep(20)
        }
        throw new Error("interactive Mini command did not publish its process identity; cleanup skipped")
      })

      const ready = yield* Effect.race(
        Effect.promise(() => healthStarted.promise).pipe(Effect.as("health" as const)),
        Effect.promise(() => healthCancelled.promise).pipe(Effect.as("cancelled" as const)),
      ).pipe(Effect.timeout("30 seconds"))
      expect(ready).toBe("health")
      const first = yield* Effect.race(
        Effect.promise(() => controlExpired.promise).pipe(Effect.as("control-expired" as const)),
        Effect.promise(() => healthCancelled.promise).pipe(Effect.as("health-cancelled" as const)),
      ).pipe(Effect.timeout("10 seconds"))
      expect(first).toBe("control-expired")
      const control = yield* Effect.promise(async () => {
        const deadline = Date.now() + 3_000
        while (Date.now() < deadline) {
          try {
            const value: unknown = await Bun.file(controlFile).json()
            if (typeof value === "object" && value !== null && "type" in value) return value
          } catch {}
          await Bun.sleep(20)
        }
        throw new Error("interactive Mini control fetch did not record its result")
      }).pipe(Effect.timeout("5 seconds"))
      if (
        typeof control !== "object" ||
        control === null ||
        !("type" in control) ||
        !("name" in control)
      )
        throw new Error("interactive Mini control result is malformed")
      expect(control.type).toBe("failed")
      expect(control.name).toBe("TimeoutError")
      expect(healthRequests).toBe(1)
      expect(healthWasCancelled).toBe(false)
      releaseHealth.resolve()

      yield* Effect.promise(() => cancelHealthStarted.promise).pipe(Effect.timeout("10 seconds"))
      yield* Effect.promise(() => cancelHealthCancelled.promise).pipe(Effect.timeout("5 seconds"))
      const exitCode = yield* Effect.promise(() => child.exited).pipe(Effect.timeout("15 seconds"))
      const [stdout, stderr] = yield* Effect.promise(() => withTimeout(output, 5_000))
      if (exitCode !== 0) throw new Error(`interactive Mini worker exited ${exitCode}: ${stdout}\n${stderr}`)
      expect(`${stdout}\n${stderr}`).toContain("pass")
      const request: unknown = yield* Effect.promise(() => Bun.file(requestFile).json())
      expect(request).toEqual([
        { timeoutDisabled: true, hasSignal: false },
        { timeoutDisabled: true, hasSignal: true },
      ])
      const result: unknown = yield* Effect.promise(() => Bun.file(resultFile).json())
      expect(result).toMatchObject({ status: 200, healthy: true, abortRequested: true })
    }),
  deadline(60_000),
)
