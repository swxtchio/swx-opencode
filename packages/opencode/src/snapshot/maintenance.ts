/**
 * Snapshot maintenance keeps user tracking outside the box-wide gc cooldown so a skipped gc cannot drop snapshot writes.
 *
 * | Path | Locks in order | Wait | Contention | Infrastructure failure | Work failure |
 * | --- | --- | --- | --- | --- | --- |
 * | `Snapshot.track` (user-facing) | per-repo and local | bounded wait windows; never skip | retry until tracked | loud log and skip | surface work failures |
 * | `Snapshot.cleanup` admission | `gc.lock` | long wait | wait, then recheck; expiry warns and skips | loud log and abort the pass | surface per repo |
 * | cleanup per-repo gc section | `gc.lock` → per-repo | short | skip this repo this pass | loud log and skip repo | log and continue other repos |
 * | reap | `gc.lock` → per-repo | short | skip | loud log and skip | log and continue other repos |
 */
import { AppProcess } from "@opencode-ai/core/process"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { ChildProcess } from "effect/unstable/process"
import { Cause, Clock, Context, Duration, Effect, Layer, Option, Semaphore } from "effect"
import path from "path"

export interface RunInput {
  readonly command: string
  readonly args: string[]
  readonly cwd: string
}

export type LockRole = "box" | "repo" | "local"

export type LockRequest =
  | {
      readonly role: "box" | "repo"
      readonly file: string
      readonly wait?: boolean
      readonly waitMillis?: number
    }
  | { readonly role: "local"; readonly semaphore: Semaphore.Semaphore; readonly wait?: boolean }

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
            try: (signal) => acquire(request.file, signal, !request.wait),
            catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
          }).pipe(
            Effect.map((attempt) =>
              attempt.status === "acquired" ? { status: "acquired" as const, lease: attempt } : attempt,
            ),
          ),
        ),
      )

    const withLocks: MaintenanceInterface["withLocks"] = (locks, self) => withSnapshotLocks(locks, self, acquireFile)

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

// When a path needs box admission, take it before repo and local locks; repo-only work stays independent.
export function assertLockOrder(locks: readonly LockRequest[]) {
  if (!locks.length) throw new Error("snapshot work requires an acquisition lock")
  const order: readonly LockRole[] = ["box", "repo", "local"]
  let previous = -1
  let hasRepo = false
  for (const [index, request] of locks.entries()) {
    const current = order.indexOf(request.role)
    if (current < previous) throw new Error("snapshot locks must be acquired in box, repo, local order")
    const waitsForFile = request.role !== "local" && (request.wait || request.waitMillis !== undefined)
    if (waitsForFile && index !== 0) {
      throw new Error("waiting snapshot file locks must be acquired first")
    }
    if (request.role === "repo") hasRepo = true
    if (request.role === "local" && !hasRepo) throw new Error("snapshot local locks require a repo lock first")
    previous = current
  }
}

export function withSnapshotLocks<A, E, R>(
  locks: readonly LockRequest[],
  self: Effect.Effect<A, E, R>,
  acquireFile: (request: Extract<LockRequest, { role: "box" | "repo" }>) => Effect.Effect<FileLockAttempt, Error>,
): Effect.Effect<LockAttempt<A>, E, R> {
  const files = locks.filter(
    (request): request is Extract<LockRequest, { role: "box" | "repo" }> => request.role !== "local",
  )
  const locals = locks.filter((request): request is Extract<LockRequest, { role: "local" }> => request.role === "local")

  return Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      yield* Effect.sync(() => assertLockOrder(locks))
      const acquireAll = Effect.gen(function* () {
        const handles: {
          readonly request: Extract<LockRequest, { role: "box" | "repo" }>
          readonly lease: LockLease
        }[] = []
        for (const request of files) {
          const startedAt = request.waitMillis === undefined ? undefined : yield* Clock.currentTimeNanos
          while (true) {
            const remaining =
              startedAt === undefined
                ? undefined
                : BigInt(request.waitMillis!) * 1_000_000n - ((yield* Clock.currentTimeNanos) - startedAt)
            if (remaining !== undefined && remaining <= 0n) return { status: "contended" as const, handles }
            const acquire =
              request.wait || startedAt !== undefined ? restore(acquireFile(request)) : acquireFile(request)
            const acquisition =
              request.wait && remaining !== undefined
                ? Effect.raceFirst(
                    acquire.pipe(Effect.map((value) => ({ tag: "lock" as const, value }))),
                    restore(Effect.sleep(Duration.nanos(remaining)).pipe(Effect.as({ tag: "timeout" as const }))),
                  )
                : acquire.pipe(Effect.map((value) => ({ tag: "lock" as const, value })))
            const attempt = yield* acquisition.pipe(
              Effect.map((value) => ({ status: "result" as const, value })),
              Effect.catch((cause) => Effect.succeed({ status: "failure" as const, cause })),
            )
            if (attempt.status === "failure") {
              return { status: "unavailable" as const, handles, request, cause: attempt.cause }
            }
            if (attempt.value.tag === "timeout") return { status: "contended" as const, handles }
            if (attempt.value.value.status === "acquired") {
              handles.push({ request, lease: attempt.value.value.lease })
              break
            }
            if (request.wait) {
              return {
                status: "unavailable" as const,
                handles,
                request,
                cause: new Error("waiting snapshot lock reported contention"),
              }
            }
            if (startedAt === undefined) return { status: "contended" as const, handles }
            const sleepMillis = remaining! < 10_000_000n ? remaining! : 10_000_000n
            yield* restore(Effect.sleep(Duration.nanos(sleepMillis)))
          }
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
            if (locals[index]!.wait) {
              return restore(locals[index]!.semaphore.withPermits(1)(runWithLocals(index + 1)))
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
    return [flock, "-x", ...(tryOnly ? ["-n"] : []), file, "sh", "-c", 'printf "locked\\n"; exec cat']
  }

  if (platform === "darwin") {
    const perl = which("perl")
    if (!perl) throw new Error("snapshot advisory locks require the macOS Perl runtime")
    const script = tryOnly
      ? 'use strict; use Errno qw(EWOULDBLOCK EAGAIN); open my $lock, ">>", $ARGV[0] or die $!; if (!flock($lock, LOCK_EX|LOCK_NB)) { exit 75 if $! == EWOULDBLOCK || $! == EAGAIN; die $!; } $| = 1; print "locked\\n"; <STDIN>;'
      : 'use strict; open my $lock, ">>", $ARGV[0] or die $!; flock($lock, LOCK_EX) or die $!; $| = 1; print "locked\\n"; <STDIN>;'
    return [perl, "-MFcntl=:flock", "-e", script, file]
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
