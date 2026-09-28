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
  readonly tryWithLock: <A, E, R>(file: string, self: Effect.Effect<A, E, R>) => Effect.Effect<A | undefined, E, R>
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
    const acquireLock = (file: string, tryOnly: boolean) =>
      fs.ensureDir(path.dirname(file)).pipe(
        Effect.mapError((cause) => (cause instanceof Error ? cause : new Error(String(cause)))),
        Effect.andThen(
          Effect.tryPromise({
            try: (signal) => acquire(file, signal, tryOnly),
            catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
          }),
        ),
      )

    const withLock: MaintenanceInterface["withLock"] = (file, self) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const handle = yield* restore(acquireLock(file, false))
          if (!handle) return yield* Effect.die(new Error("blocking snapshot lock acquisition returned no handle"))
          return yield* Effect.acquireUseRelease(
            Effect.succeed(handle),
            () => restore(self),
            (lock) => Effect.promise(lock.release),
          )
        }),
      )

    const tryWithLock: MaintenanceInterface["tryWithLock"] = (file, self) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const handle = yield* restore(acquireLock(file, true)).pipe(Effect.catch(() => Effect.succeed(undefined)))
          if (!handle) return undefined
          return yield* Effect.acquireUseRelease(
            Effect.succeed(handle),
            () => restore(self),
            (lock) => Effect.promise(lock.release),
          )
        }),
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
      tryWithLock,
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

export function lockCommand(
  platform: NodeJS.Platform,
  file: string,
  which: (command: string) => string | null,
  tryOnly = false,
) {
  if (platform === "linux") {
    const flock = which("flock")
    if (!flock) throw new Error("snapshot advisory locks require the Linux flock utility")
    return [
      flock,
      "-x",
      ...(tryOnly ? ["-n"] : []),
      file,
      "sh",
      "-c",
      'printf "locked\\n"; exec cat',
    ]
  }

  if (platform === "darwin") {
    const perl = which("perl")
    if (!perl) throw new Error("snapshot advisory locks require the macOS Perl runtime")
    const script = tryOnly
      ? 'use strict; use Errno qw(EWOULDBLOCK EAGAIN); open my $lock, ">>", $ARGV[0] or die $!; if (!flock($lock, LOCK_EX|LOCK_NB)) { exit 75 if $! == EWOULDBLOCK || $! == EAGAIN; die $!; } $| = 1; print "locked\\n"; <STDIN>;'
      : 'use strict; open my $lock, ">>", $ARGV[0] or die $!; flock($lock, LOCK_EX) or die $!; $| = 1; print "locked\\n"; <STDIN>;'
    return [
      perl,
      "-MFcntl=:flock",
      "-e",
      script,
      file,
    ]
  }

  if (platform === "win32") {
    const powershell = which("powershell.exe")
    if (!powershell) throw new Error("snapshot advisory locks require Windows PowerShell")
    const lock = tryOnly
      ? "try { $stream.Lock(0, 1) } catch [System.IO.IOException] { exit 75 }"
      : "while ($true) { try { $stream.Lock(0, 1); break } catch [System.IO.IOException] { Start-Sleep -Milliseconds 25 } }"
    return [
      powershell,
      "-NoProfile",
      "-NonInteractive",
      "-EncodedCommand",
      Buffer.from(
        `$ErrorActionPreference = 'Stop'\n$stream = [System.IO.File]::Open('${file.replaceAll("'", "''")}', [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::ReadWrite)\n${lock}\n[Console]::Out.WriteLine('locked')\n[Console]::Out.Flush()\n[Console]::In.ReadLine() | Out-Null\n$stream.Unlock(0, 1)\n$stream.Dispose()`,
        "utf16le",
      ).toString("base64"),
    ]
  }

  throw new Error(`snapshot advisory locks are unavailable on ${platform}`)
}

export type LockRuntime = {
  readonly platform?: NodeJS.Platform
  readonly which?: (command: string) => string | null
  readonly spawn?: (
    command: string[],
    options: { readonly stdin: "pipe"; readonly stdout: "pipe"; readonly stderr: "pipe"; readonly signal: AbortSignal },
  ) => ReturnType<typeof Bun.spawn>
}

export async function acquire(file: string, signal: AbortSignal, tryOnly: boolean, runtime: LockRuntime = {}) {
  const command = lockCommand(runtime.platform ?? process.platform, file, runtime.which ?? Bun.which, tryOnly)
  const options = { stdin: "pipe", stdout: "pipe", stderr: "pipe", signal } as const
  const child = runtime.spawn ? runtime.spawn(command, options) : Bun.spawn(command, options)
  const reader = child.stdout.getReader()
  let ready = ""
  try {
    while (!ready.includes("\n")) {
      const next = await reader.read()
      if (next.done) {
        const exitCode = await child.exited
        if (tryOnly && exitCode !== 0) return
        const stderr = await new Response(child.stderr).text()
        throw new Error(`failed to acquire snapshot maintenance lock (exit ${exitCode}): ${stderr}`)
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
