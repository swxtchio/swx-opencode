import { AppProcess } from "@opencode-ai/core/process"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { ChildProcess } from "effect/unstable/process"
import { Cause, Clock, Context, Effect, Layer, Option, Semaphore } from "effect"
import path from "path"

export interface RunInput {
  readonly command: string
  readonly args: string[]
  readonly cwd: string
}

export type LockRole = "box" | "repo" | "local"

export type LockRequest =
  | { readonly role: "box" | "repo"; readonly file: string }
  | { readonly role: "local"; readonly semaphore: Semaphore.Semaphore }

export type LockAttempt<A> =
  | { readonly status: "acquired"; readonly value: A }
  | { readonly status: "contended" }
  | { readonly status: "unavailable" }

export interface LockLease {
  readonly release: () => Promise<{ exitCode: number; stderr: string }>
}

export type FileLockAttempt =
  | { readonly status: "acquired"; readonly lease: LockLease }
  | { readonly status: "contended" }

export interface MaintenanceInterface {
  readonly withLocks: <A, E, R>(
    locks: readonly LockRequest[],
    self: Effect.Effect<A, E, R>,
  ) => Effect.Effect<LockAttempt<A>, E, R>
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
    const acquireFile = (request: Extract<LockRequest, { role: "box" | "repo" }>) =>
      fs.ensureDir(path.dirname(request.file)).pipe(
        Effect.mapError((cause) => (cause instanceof Error ? cause : new Error(String(cause)))),
        Effect.andThen(
          Effect.tryPromise({
            try: (signal) => acquire(request.file, signal, true),
            catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
          }).pipe(
            Effect.map((attempt) =>
              attempt.status === "acquired" ? { status: "acquired" as const, lease: attempt } : attempt,
            ),
          ),
        ),
      )

    const withLocks: MaintenanceInterface["withLocks"] = (locks, self) =>
      withSnapshotLocks(locks, self, acquireFile)

    const run: MaintenanceInterface["run"] = (input) =>
      appProcess.run(ChildProcess.make(input.command, input.args, { cwd: input.cwd })).pipe(
        Effect.map((result) => ({ exitCode: result.exitCode, stderr: result.stderr.toString("utf8") })),
        Effect.catch((cause) =>
          Effect.succeed({ exitCode: 1, stderr: cause instanceof Error ? cause.message : String(cause) }),
        ),
      )

    return MaintenanceService.of({
      withLocks,
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

// Keep maintenance admission ahead of repo and local ownership to prevent lock-order cycles.
export function assertLockOrder(locks: readonly LockRequest[]) {
  if (!locks.length) throw new Error("snapshot work requires an acquisition lock")
  const order: readonly LockRole[] = ["box", "repo", "local"]
  let previous = -1
  let hasBox = false
  let hasRepo = false
  for (const request of locks) {
    const current = order.indexOf(request.role)
    if (current < previous) throw new Error("snapshot locks must be acquired in box, repo, local order")
    if (request.role === "box") hasBox = true
    if (request.role === "repo") {
      if (!hasBox) throw new Error("snapshot repo locks require box admission first")
      hasRepo = true
    }
    if (request.role === "local" && !hasRepo) throw new Error("snapshot local locks require a repo lock first")
    previous = current
  }
}

export function withSnapshotLocks<A, E, R>(
  locks: readonly LockRequest[],
  self: Effect.Effect<A, E, R>,
  acquireFile: (request: Extract<LockRequest, { role: "box" | "repo" }>) => Effect.Effect<FileLockAttempt, Error>,
): Effect.Effect<LockAttempt<A>, E, R> {
  const files = locks.filter((request): request is Extract<LockRequest, { role: "box" | "repo" }> => request.role !== "local")
  const locals = locks.filter((request): request is Extract<LockRequest, { role: "local" }> => request.role === "local")
  const acquireAll = Effect.gen(function* () {
    const handles: { readonly request: Extract<LockRequest, { role: "box" | "repo" }>; readonly lease: LockLease }[] = []
    for (const request of files) {
      const attempt = yield* acquireFile(request).pipe(
        Effect.map((value) => ({ status: "result" as const, value })),
        Effect.catch((cause) => Effect.succeed({ status: "failure" as const, cause })),
      )
      if (attempt.status === "failure") return { status: "unavailable" as const, handles, request, cause: attempt.cause }
      if (attempt.value.status === "contended") return { status: "contended" as const, handles }
      handles.push({ request, lease: attempt.value.lease })
    }
    return { status: "acquired" as const, handles }
  })
  const releaseAll = (resource: Effect.Success<typeof acquireAll>) =>
    Effect.forEach(
      resource.handles.slice().reverse(),
      (item) =>
        Effect.promise(item.lease.release).pipe(
          Effect.catchCause((cause) => Effect.succeed({ exitCode: 1, stderr: Cause.pretty(cause) })),
          Effect.flatMap((result) =>
            result.exitCode === 0
              ? Effect.void
              : Effect.logError("snapshot advisory lock release failed", {
                  role: item.request.role,
                  file: item.request.file,
                  exitCode: result.exitCode,
                  stderr: result.stderr,
                }),
          ),
        ),
      { concurrency: 1 },
    ).pipe(Effect.asVoid)

  return Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      yield* Effect.sync(() => assertLockOrder(locks))
      return yield* Effect.acquireUseRelease(
        acquireAll,
        (resource) => {
          if (resource.status === "contended") return Effect.succeed({ status: "contended" as const })
          if (resource.status === "unavailable") {
            return Effect.logError("snapshot advisory lock unavailable", {
              role: resource.request.role,
              file: resource.request.file,
              cause: resource.cause instanceof Error ? resource.cause.message : String(resource.cause),
            }).pipe(Effect.as({ status: "unavailable" as const }))
          }

          const runWithLocals = (index: number): Effect.Effect<LockAttempt<A>, E, R> => {
            if (index >= locals.length) {
              return restore(self).pipe(Effect.map((value) => ({ status: "acquired" as const, value })))
            }
            return locals[index]!.semaphore.withPermitsIfAvailable(1)(runWithLocals(index + 1)).pipe(
              Effect.map(Option.getOrElse(() => ({ status: "contended" as const }))),
            )
          }

          return runWithLocals(0)
        },
        releaseAll,
      )
    }),
  )
}

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
        const stderr = await new Response(child.stderr).text()
        if (tryOnly && exitCode !== 0 && !stderr.trim()) return { status: "contended" as const }
        throw new Error(`failed to acquire snapshot maintenance lock (exit ${exitCode}): ${stderr}`)
      }
      ready += new TextDecoder().decode(next.value)
    }

    return {
      status: "acquired" as const,
      release: async () => {
        child.stdin.end()
        const code = await child.exited
        const stderr = await new Response(child.stderr).text()
        return { exitCode: code, stderr }
      },
    }
  } catch (error) {
    child.kill()
    throw error
  }
}
