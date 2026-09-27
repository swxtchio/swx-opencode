import { AppProcess } from "@opencode-ai/core/process"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { ChildProcess } from "effect/unstable/process"
import { Clock, Context, Effect, Layer } from "effect"
import path from "path"

export interface RunInput {
  readonly command: string
  readonly args: string[]
  readonly cwd: string
}

export interface MaintenanceInterface {
  readonly withLock: <A, E, R>(file: string, self: Effect.Effect<A, E, R>) => Effect.Effect<A, E | Error, R>
  readonly run: (input: RunInput) => Effect.Effect<{ exitCode: number; stderr: string }>
  readonly now: Effect.Effect<number>
  readonly random: Effect.Effect<number>
}

export class MaintenanceService extends Context.Service<MaintenanceService, MaintenanceInterface>()(
  "@opencode/SnapshotMaintenance",
) {}

const layer: Layer.Layer<MaintenanceService, never, FSUtil.Service | AppProcess.Service> = Layer.effect(
  MaintenanceService,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    const appProcess = yield* AppProcess.Service

    const withLock: MaintenanceInterface["withLock"] = (file, self) =>
      Effect.acquireUseRelease(
        fs.ensureDir(path.dirname(file)).pipe(
          Effect.orDie,
          Effect.andThen(
            Effect.tryPromise({
              try: (signal) => acquire(file, signal),
              catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
            }),
          ),
        ),
        () => self,
        (handle) => Effect.promise(handle.release),
      )

    const run: MaintenanceInterface["run"] = (input) =>
      appProcess.run(ChildProcess.make(input.command, input.args, { cwd: input.cwd })).pipe(
        Effect.map((result) => ({ exitCode: result.exitCode, stderr: result.stderr.toString("utf8") })),
        Effect.catch((cause) =>
          Effect.succeed({ exitCode: 1, stderr: cause instanceof Error ? cause.message : String(cause) }),
        ),
      )

    return MaintenanceService.of({
      withLock,
      run,
      now: Clock.currentTimeMillis,
      random: Effect.sync(() => Math.random()),
    })
  }),
)

export const maintenanceNode = LayerNode.make({
  service: MaintenanceService,
  layer,
  deps: [FSUtil.node, AppProcess.node],
})

async function acquire(file: string, signal: AbortSignal) {
  const command =
    process.platform === "linux"
      ? ["flock", "--exclusive", file, "sh", "-c", 'printf "locked\\n"; exec cat']
      : process.platform === "darwin"
        ? ["lockf", "-k", file, "sh", "-c", 'printf "locked\\n"; exec cat']
        : process.platform === "win32"
          ? [
              "powershell.exe",
              "-NoProfile",
              "-NonInteractive",
              "-EncodedCommand",
              Buffer.from(
                `$ErrorActionPreference = 'Stop'\n$stream = [System.IO.File]::Open('${file.replaceAll("'", "''")}', [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::ReadWrite)\nwhile ($true) { try { $stream.Lock(0, 1); break } catch [System.IO.IOException] { Start-Sleep -Milliseconds 25 } }\n[Console]::Out.WriteLine('locked')\n[Console]::Out.Flush()\n[Console]::In.ReadLine() | Out-Null\n$stream.Unlock(0, 1)\n$stream.Dispose()`,
                "utf16le",
              ).toString("base64"),
            ]
          : undefined
  if (!command) throw new Error(`snapshot maintenance advisory locks are unavailable on ${process.platform}`)

  // The holder process owns the OS lock so an exit releases it without lease recovery.
  const child = Bun.spawn(command, { stdin: "pipe", stdout: "pipe", stderr: "pipe", signal })
  const reader = child.stdout.getReader()
  let ready = ""
  try {
    while (!ready.includes("\n")) {
      const next = await reader.read()
      if (next.done) {
        const stderr = await new Response(child.stderr).text()
        throw new Error(`failed to acquire snapshot maintenance lock (exit ${await child.exited}): ${stderr}`)
      }
      ready += new TextDecoder().decode(next.value)
    }

    return {
      release: async () => {
        child.stdin.end()
        const code = await child.exited
        if (code !== 0) throw new Error(`snapshot maintenance lock exited with code ${code}`)
      },
    }
  } catch (error) {
    child.kill()
    throw error
  }
}

export * as SnapshotMaintenance from "."
