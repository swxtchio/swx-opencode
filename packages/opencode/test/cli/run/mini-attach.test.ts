import { expect } from "bun:test"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { Effect, Option } from "effect"
import path from "node:path"
import { cliIt, deadline } from "../../lib/cli-process"

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
      if (!scriptIdentity?.bootID || !scriptIdentity.startTicks)
        throw new Error("runMini PTY launcher has no recorded start identity")
      let miniIdentity: EventWriter.ProcessIdentity | undefined
      const sameProcess = (identity: EventWriter.ProcessIdentity) => {
        const current = EventWriter.readIdentity(identity.pid)
        return current?.bootID === identity.bootID && current?.startTicks === identity.startTicks
      }
      const output = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()])

      try {
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
        const [stdout, stderr] = yield* Effect.promise(() => output)
        expect(exitCode).toBe(1)
        expect(`${stdout}\n${stderr}`).toContain("Session not found")
      } finally {
        if (miniIdentity && sameProcess(miniIdentity)) {
          try {
            process.kill(miniIdentity.pid, "SIGKILL")
          } catch {}
        }
        if (sameProcess(scriptIdentity)) {
          try {
            process.kill(scriptIdentity.pid, "SIGKILL")
          } catch {}
        }
        const stopped = yield* Effect.promise(() => child.exited).pipe(Effect.timeoutOption("5 seconds"))
        if (Option.isNone(stopped)) throw new Error("runMini worker survived test cleanup")
        yield* Effect.promise(() => output).pipe(Effect.ignore)
      }
    }),
    deadline(40_000),
)
