import { afterEach, describe, expect } from "bun:test"
import fs from "node:fs/promises"
import { readFileSync, readdirSync } from "node:fs"
import os from "node:os"
import path from "node:path"
import { Deferred, Duration, Effect, Exit, Fiber, Option, Stream } from "effect"
import type * as PlatformError from "effect/PlatformError"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import type { ChildProcessHandle } from "effect/unstable/process/ChildProcessSpawner"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { testEffect } from "../lib/effect"

const live = LayerNode.compile(CrossSpawnSpawner.node)
const fx = testEffect(live)

function js(code: string, opts?: ChildProcess.CommandOptions) {
  return ChildProcess.make("node", ["-e", code], opts)
}

function decodeByteStream(stream: Stream.Stream<Uint8Array, PlatformError.PlatformError>) {
  return Stream.runCollect(stream).pipe(
    Effect.map((chunks) => {
      const total = chunks.reduce((acc, x) => acc + x.length, 0)
      const out = new Uint8Array(total)
      let off = 0
      for (const chunk of chunks) {
        out.set(chunk, off)
        off += chunk.length
      }
      return new TextDecoder("utf-8").decode(out).trim()
    }),
  )
}

function alive(pid: number) {
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

async function tmpdir() {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-core-test-"))
  return {
    path: dir,
    async [Symbol.asyncDispose]() {
      await fs.rm(dir, { recursive: true, force: true })
    },
  }
}

async function gone(pid: number, timeout = 5_000) {
  const end = Date.now() + timeout
  while (Date.now() < end) {
    if (!alive(pid)) return true
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  return !alive(pid)
}

function groupAlive(pgid: number) {
  try {
    process.kill(-pgid, 0)
    return true
  } catch (error) {
    return !(error instanceof Error && "code" in error && error.code === "ESRCH")
  }
}

async function groupGone(pgid: number, timeout = 5_000) {
  const end = Date.now() + timeout
  while (Date.now() < end) {
    if (!groupAlive(pgid)) return true
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  return !groupAlive(pgid)
}

function processIdentity(pid: number) {
  const identity = EventWriter.readIdentity(pid)
  if (identity?.bootID && identity.startTicks) return identity
  const started = Bun.spawnSync(["ps", "-p", String(pid), "-o", "lstart="]).stdout.toString().trim()
  return started ? { pid, bootID: "ps", startTicks: started } : undefined
}

function sameProcess(identity: EventWriter.ProcessIdentity) {
  const current = processIdentity(identity.pid)
  return current?.bootID === identity.bootID && current?.startTicks === identity.startTicks
}

type RecordedProcessGroup = {
  pgid: number | undefined
  members: EventWriter.ProcessIdentity[]
}

const cleanupRecordedProcessGroup = (group: RecordedProcessGroup) =>
  Effect.promise(async () => {
    if (group.pgid === undefined) return
    if (group.members.some(sameProcess)) {
      try {
        process.kill(-group.pgid, "SIGKILL")
      } catch {}
    }
    if (!(await groupGone(group.pgid))) throw new Error(`owned process group ${group.pgid} survived test cleanup`)
  }).pipe(Effect.asVoid)

type OwnedProcessGroup = {
  pgid: number
  members: Map<number, EventWriter.ProcessIdentity>
}

const ownedProcessGroups = new Map<number, OwnedProcessGroup>()

const recordProcess = (pgid: number, identity: EventWriter.ProcessIdentity) => {
  const group = ownedProcessGroups.get(pgid) ?? { pgid, members: new Map<number, EventWriter.ProcessIdentity>() }
  ownedProcessGroups.set(pgid, group)
  group.members.set(identity.pid, identity)
}

const processGroupMembers = (pgid: number) => {
  if (process.platform === "linux") {
    return readdirSync("/proc").flatMap((entry) => {
      if (!/^\d+$/.test(entry)) return []
      const pid = Number(entry)
      let stat: string
      try {
        stat = readFileSync(`/proc/${pid}/stat`, "utf8")
      } catch {
        return []
      }
      const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ")
      return Number(fields[2]) === pgid ? [pid] : []
    })
  }
  const result = Bun.spawnSync(["ps", "-e", "-o", "pid=,pgid="])
  if (result.exitCode !== 0) throw new Error(`could not inspect process group ${pgid}`)
  return result.stdout
    .toString()
    .split("\n")
    .flatMap((line) => {
      const [pid, group] = line.trim().split(/\s+/).map(Number)
      return pid && group === pgid ? [pid] : []
    })
}

const observeProcessGroup = (group: OwnedProcessGroup, members = processGroupMembers(group.pgid)) => {
  if (!members.some((pid) => group.members.has(pid) && sameProcess(group.members.get(pid)!))) return
  for (const pid of members) {
    if (group.members.has(pid)) continue
    const identity = processIdentity(pid)
    if (identity) group.members.set(pid, identity)
  }
}

const ownedProcessMembers = (group: OwnedProcessGroup, members = processGroupMembers(group.pgid)) =>
  members.filter((pid) => {
    const identity = group.members.get(pid)
    return identity && sameProcess(identity)
  })

afterEach(async () => {
  const groups = Array.from(ownedProcessGroups.values())
  const survivors: Array<{ group: OwnedProcessGroup; pids: number[] }> = []
  let remaining: number[] = []
  try {
    for (const group of groups) {
      if (!(await groupGone(group.pgid, 250)))
        survivors.push({ group, pids: ownedProcessMembers(group) })
    }
  } finally {
    for (const survivor of survivors) {
      const group = survivor.group
      const members = processGroupMembers(group.pgid)
      if (members.length === 0) continue
      observeProcessGroup(group, members)
      if (ownedProcessMembers(group, members).length > 0) {
        try {
          process.kill(-group.pgid, "SIGKILL")
        } catch {}
      }
      await groupGone(group.pgid)
    }
    remaining = survivors.flatMap((survivor) => processGroupMembers(survivor.group.pgid))
    ownedProcessGroups.clear()
  }
  expect({ survivors: survivors.map((survivor) => ({ pgid: survivor.group.pgid, pids: survivor.pids })), remaining }).toEqual({
    survivors: [],
    remaining: [],
  })
})

const resistantDescendant =
  'process.on("SIGTERM", () => {}); process.stdout.write("ready"); setInterval(() => {}, 10_000)'

const processTreeSource = (child: string, onTerm?: string) =>
  [
    ...(onTerm ? [onTerm] : []),
    `const child = require("node:child_process").spawn(process.execPath, ["-e", ${JSON.stringify(child)}], { stdio: ["ignore", "pipe", "ignore"] })`,
    'child.stdout.once("data", () => process.stdout.write(`child-ready:${child.pid}\\n`))',
    "setInterval(() => {}, 10_000)",
  ].join(";\n")

const termExitingParent = (child: string) =>
  processTreeSource(child, 'process.on("SIGTERM", () => process.exit(0))')

const timeoutProbeMode = process.env.SWX_CORE_SPAWNER_TIMEOUT_PROBE === "1"
const timeoutProbeFile = process.env.SWX_CORE_SPAWNER_TIMEOUT_FILE

const processTreeReady = (handle: ChildProcessHandle) =>
  Effect.gen(function* () {
    const ready = yield* Deferred.make<number>()
    let output = ""
    yield* Effect.forkScoped(
      Stream.runForEach(Stream.decodeText(handle.stdout), (chunk) => {
        output += chunk
        const match = output.match(/child-ready:(\d+)/)
        if (!match) return Effect.void
        return Deferred.succeed(ready, Number(match[1])).pipe(Effect.asVoid)
      }),
    )
    return yield* Deferred.await(ready).pipe(Effect.timeout("5 seconds"))
  })

describe("cross-spawn spawner", () => {
  describe("basic spawning", () => {
    fx.effect(
      "captures stdout",
      Effect.gen(function* () {
        const out = yield* ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.string(ChildProcess.make(process.execPath, ["-e", 'process.stdout.write("ok")'])),
        )
        expect(out).toBe("ok")
      }),
    )

    fx.effect(
      "captures multiple lines",
      Effect.gen(function* () {
        const handle = yield* js('console.log("line1"); console.log("line2"); console.log("line3")')
        const out = yield* decodeByteStream(handle.stdout)
        expect(out).toBe("line1\nline2\nline3")
      }),
    )

    fx.effect(
      "returns exit code",
      Effect.gen(function* () {
        const handle = yield* js("process.exit(0)")
        const code = yield* handle.exitCode
        expect(code).toBe(ChildProcessSpawner.ExitCode(0))
      }),
    )

    fx.effect(
      "returns non-zero exit code",
      Effect.gen(function* () {
        const handle = yield* js("process.exit(42)")
        const code = yield* handle.exitCode
        expect(code).toBe(ChildProcessSpawner.ExitCode(42))
      }),
    )
  })

  describe("cwd option", () => {
    fx.effect(
      "uses cwd when spawning commands",
      Effect.gen(function* () {
        const tmp = yield* Effect.acquireRelease(
          Effect.promise(() => tmpdir()),
          (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
        )
        const out = yield* ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.string(
            ChildProcess.make(process.execPath, ["-e", "process.stdout.write(process.cwd())"], { cwd: tmp.path }),
          ),
        )
        expect(yield* Effect.promise(() => fs.realpath(out))).toBe(yield* Effect.promise(() => fs.realpath(tmp.path)))
      }),
    )

    fx.effect(
      "fails for invalid cwd",
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
            svc.spawn(ChildProcess.make("echo", ["test"], { cwd: "/nonexistent/directory/path" })),
          ),
        )
        expect(Exit.isFailure(exit)).toBe(true)
      }),
    )
  })

  describe("env option", () => {
    fx.effect(
      "passes environment variables with extendEnv",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write(process.env.TEST_VAR ?? "")', {
          env: { TEST_VAR: "test_value" },
          extendEnv: true,
        })
        const out = yield* decodeByteStream(handle.stdout)
        expect(out).toBe("test_value")
      }),
    )

    fx.effect(
      "passes multiple environment variables",
      Effect.gen(function* () {
        const handle = yield* js(
          "process.stdout.write(`${process.env.VAR1}-${process.env.VAR2}-${process.env.VAR3}`)",
          {
            env: { VAR1: "one", VAR2: "two", VAR3: "three" },
            extendEnv: true,
          },
        )
        const out = yield* decodeByteStream(handle.stdout)
        expect(out).toBe("one-two-three")
      }),
    )
  })

  describe("stderr", () => {
    fx.effect(
      "captures stderr output",
      Effect.gen(function* () {
        const handle = yield* js('process.stderr.write("error message")')
        const err = yield* decodeByteStream(handle.stderr)
        expect(err).toBe("error message")
      }),
    )

    fx.effect(
      "captures both stdout and stderr",
      Effect.gen(function* () {
        const handle = yield* js(
          [
            "let pending = 2",
            "const done = () => {",
            "  pending -= 1",
            "  if (pending === 0) setTimeout(() => process.exit(0), 0)",
            "}",
            'process.stdout.write("stdout\\n", done)',
            'process.stderr.write("stderr\\n", done)',
          ].join("\n"),
        )
        const [stdout, stderr] = yield* Effect.all([decodeByteStream(handle.stdout), decodeByteStream(handle.stderr)], {
          concurrency: 2,
        })
        expect(stdout).toBe("stdout")
        expect(stderr).toBe("stderr")
      }),
    )
  })

  describe("combined output (all)", () => {
    fx.effect(
      "captures stdout via .all when no stderr",
      Effect.gen(function* () {
        const handle = yield* ChildProcess.make("echo", ["hello from stdout"])
        const all = yield* decodeByteStream(handle.all)
        expect(all).toBe("hello from stdout")
      }),
    )

    fx.effect(
      "captures stderr via .all when no stdout",
      Effect.gen(function* () {
        const handle = yield* js('process.stderr.write("hello from stderr")')
        const all = yield* decodeByteStream(handle.all)
        expect(all).toBe("hello from stderr")
      }),
    )
  })

  describe("stdin", () => {
    fx.effect(
      "allows providing standard input to a command",
      Effect.gen(function* () {
        const input = "a b c"
        const stdin = Stream.make(Buffer.from(input, "utf-8"))
        const handle = yield* js(
          'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out))',
          { stdin },
        )
        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode
        expect(out).toBe("a b c")
      }),
    )

    // Bun's child stdin reports "finish" and only then fails the buffered write
    // with EPIPE, after the sink has dropped its error listener. Unhandled, that
    // error is an uncaught exception that fails whatever test is running.
    fx.effect(
      "ignores input the command closed its stdin before reading",
      Effect.gen(function* () {
        const closed = Promise.withResolvers<void>()
        const stdin = Stream.fromEffect(
          Effect.promise(() => closed.promise).pipe(Effect.as(new TextEncoder().encode("unread"))),
        )
        const handle = yield* ChildProcess.make("sh", ["-c", "exec 0<&-; echo closed; sleep 1"], { stdin })
        const out = yield* handle.stdout.pipe(
          Stream.decodeText(),
          Stream.tap((text) => Effect.sync(() => text.includes("closed") && closed.resolve())),
          Stream.mkString,
        )
        expect(out.trim()).toBe("closed")
        expect(yield* handle.exitCode).toBe(ChildProcessSpawner.ExitCode(0))
      }),
    )
  })

  describe("process control", () => {
    fx.effect(
      "kills a running process",
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          Effect.gen(function* () {
            const handle = yield* js("setTimeout(() => {}, 10_000)")
            yield* handle.kill()
            return yield* handle.exitCode
          }),
        )
        expect(Exit.isFailure(exit) ? true : exit.value !== ChildProcessSpawner.ExitCode(0)).toBe(true)
      }),
    )

    fx.live(
      "kill without forceKillAfter returns when its leader exits",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const group: RecordedProcessGroup = { pgid: undefined, members: [] }
        yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(group))
        const handle = yield* js(termExitingParent(resistantDescendant))
        group.pgid = Number(handle.pid)
        const leader = processIdentity(group.pgid)
        if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
        group.members.push(leader)
        recordProcess(group.pgid, leader)
        const descendant = processIdentity(yield* processTreeReady(handle))
        if (!descendant?.bootID || !descendant.startTicks)
          throw new Error("spawned descendant has no recorded start identity")
        group.members.push(descendant)
        recordProcess(group.pgid, descendant)
        expect({ leader: sameProcess(leader), descendant: sameProcess(descendant) }).toEqual({
          leader: true,
          descendant: true,
        })

        const completion = yield* handle.kill().pipe(Effect.timeoutOption("500 millis"))
        expect(Option.isSome(completion)).toBe(true)
        expect({ leader: sameProcess(leader), descendant: sameProcess(descendant) }).toEqual({
          leader: false,
          descendant: true,
        })
      }),
      8_000,
    )

    fx.live(
      "scope release without forceKillAfter returns when its leader exits",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const group: RecordedProcessGroup = { pgid: undefined, members: [] }
        yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(group))
        yield* Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* js(termExitingParent(resistantDescendant))
            group.pgid = Number(handle.pid)
            const leader = processIdentity(group.pgid)
            if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
            group.members.push(leader)
            recordProcess(group.pgid, leader)
            const descendant = processIdentity(yield* processTreeReady(handle))
            if (!descendant?.bootID || !descendant.startTicks)
              throw new Error("spawned descendant has no recorded start identity")
            group.members.push(descendant)
            recordProcess(group.pgid, descendant)
            expect({ leader: sameProcess(leader), descendant: sameProcess(descendant) }).toEqual({
              leader: true,
              descendant: true,
            })
          }),
        )
        expect(group.pgid === undefined ? false : groupAlive(group.pgid)).toBe(true)
        expect(group.members.some((identity) => sameProcess(identity) && identity.pid !== group.pgid)).toBe(true)
      }),
      8_000,
    )

    fx.effect(
      "kills a child when scope exits",
      Effect.gen(function* () {
        const pid = yield* Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* js("setInterval(() => {}, 10_000)")
            return Number(handle.pid)
          }),
        )
        const done = yield* Effect.promise(() => gone(pid))
        expect(done).toBe(true)
      }),
    )

    fx.live(
      "scope exit releases a child whose output consumer was interrupted before it read",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const spawned = yield* Deferred.make<number>()
        // Interrupted straight after spawn, handle.all's merged sides may never start reading;
        // the scope must release the child whether or not it has written anything yet.
        const consumer = yield* Effect.gen(function* () {
          const handle = yield* ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
            svc.spawn(
              ChildProcess.make("/bin/sh", ["-c", "printf ready; sleep 30"], {
                stdin: "ignore",
                forceKillAfter: "3 seconds",
              }),
            ),
          )
          yield* Deferred.succeed(spawned, Number(handle.pid))
          yield* Stream.runForEach(handle.all, () => Effect.void)
        }).pipe(Effect.scoped, Effect.forkDetach)
        const pid = yield* Deferred.await(spawned)
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (alive(pid)) process.kill(-pid, "SIGKILL")
          }),
        )

        // Detached, so a release that never finishes fails this bound instead of hanging the test.
        const released = yield* Fiber.interrupt(consumer).pipe(
          Effect.forkDetach,
          Effect.flatMap(Fiber.await),
          Effect.timeoutOption("5 seconds"),
        )
        expect(Option.isSome(released)).toBe(true)
        expect(alive(pid)).toBe(false)
      }),
      15_000,
    )

    fx.effect(
      "forceKillAfter escalates for stubborn processes",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const started = Date.now()
        const exit = yield* Effect.exit(
          Effect.gen(function* () {
            const handle = yield* js('process.on("SIGTERM", () => {}); setInterval(() => {}, 10_000)')
            yield* handle.kill({ forceKillAfter: 100 })
            return yield* handle.exitCode
          }),
        )

        expect(Date.now() - started).toBeLessThan(1_000)
        expect(Exit.isFailure(exit) ? true : exit.value !== ChildProcessSpawner.ExitCode(0)).toBe(true)
      }),
    )

    fx.live(
      "forceKillAfter zero forms escalate immediately",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const run = Effect.fnUntraced(function* (forceKillAfter: Duration.Input) {
          const group: RecordedProcessGroup = { pgid: undefined, members: [] }
          yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(group))
          const handle = yield* js(termExitingParent(resistantDescendant))
          group.pgid = Number(handle.pid)
          const leader = processIdentity(group.pgid)
          if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
          group.members.push(leader)
          recordProcess(group.pgid, leader)
          const descendant = processIdentity(yield* processTreeReady(handle))
          if (!descendant?.bootID || !descendant.startTicks)
            throw new Error("spawned descendant has no recorded start identity")
          group.members.push(descendant)
          recordProcess(group.pgid, descendant)
          expect({ leader: sameProcess(leader), descendant: sameProcess(descendant) }).toEqual({
            leader: true,
            descendant: true,
          })

          const completion = yield* handle.kill({ forceKillAfter }).pipe(Effect.timeoutOption("2 seconds"))
          expect(Option.isSome(completion)).toBe(true)
          expect({ groupAlive: groupAlive(group.pgid), descendant: sameProcess(descendant) }).toEqual({
            groupAlive: false,
            descendant: false,
          })
        })
        yield* Effect.all([run(0), run("0 millis")], { concurrency: 1 })
      }),
    )

    fx.live(
      "Bun timeout boundary fixture worker",
      Effect.gen(function* () {
        if (!timeoutProbeMode || process.platform === "win32") return
        if (!timeoutProbeFile) throw new Error("timeout probe file path was not supplied")

        const handle = yield* js(termExitingParent(resistantDescendant))
        const pgid = Number(handle.pid)
        const leader = processIdentity(pgid)
        if (!leader?.bootID || !leader.startTicks) throw new Error("timeout worker leader has no start identity")
        recordProcess(pgid, leader)
        const descendant = processIdentity(yield* processTreeReady(handle))
        if (!descendant?.bootID || !descendant.startTicks) throw new Error("timeout worker child has no start identity")
        recordProcess(pgid, descendant)
        if (!sameProcess(leader) || !sameProcess(descendant)) throw new Error("timeout worker fixture was not live")
        yield* Effect.promise(() => Bun.write(timeoutProbeFile, JSON.stringify({ pgid, members: [leader, descendant] })))
        yield* Effect.never
      }),
      5_000,
    )

    fx.live(
      "Bun test timeout cleans a TERM-resistant descendant",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const file = `${process.cwd()}/.cross-spawn-timeout-probe.json`
        yield* Effect.promise(() => fs.rm(file, { force: true }))
        yield* Effect.addFinalizer(() => Effect.promise(() => fs.rm(file, { force: true })).pipe(Effect.asVoid))
        const worker = Bun.spawn(
          [
            process.execPath,
            "test",
            "--timeout",
            "15000",
            "./test/effect/cross-spawn-spawner.test.ts",
            "-t",
            "Bun timeout boundary fixture worker",
          ],
          {
            cwd: process.cwd(),
            env: {
              ...process.env,
              SWX_CORE_SPAWNER_TIMEOUT_PROBE: "1",
              SWX_CORE_SPAWNER_TIMEOUT_FILE: file,
            },
            stdin: "ignore",
            stdout: "pipe",
            stderr: "pipe",
          },
        )
        const workerIdentity = processIdentity(worker.pid)
        if (!workerIdentity?.bootID || !workerIdentity.startTicks) throw new Error("timeout worker has no start identity")
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (!sameProcess(workerIdentity)) return
            try {
              process.kill(worker.pid, "SIGKILL")
            } catch {}
          }),
        )
        const output = Promise.all([new Response(worker.stdout).text(), new Response(worker.stderr).text()])
        const probe = yield* Effect.promise(async () => {
          const deadline = Date.now() + 10_000
          while (Date.now() < deadline) {
            if (await Bun.file(file).exists())
              return (await Bun.file(file).json()) as { pgid: number; members: EventWriter.ProcessIdentity[] }
            await new Promise((resolve) => setTimeout(resolve, 20))
          }
        })
        if (!probe) throw new Error("timeout worker did not publish its ready process identities")
        for (const member of probe.members) recordProcess(probe.pgid, member)
        yield* Effect.addFinalizer(() =>
          cleanupRecordedProcessGroup({ pgid: probe.pgid, members: probe.members }),
        )
        expect(probe.members.every(sameProcess)).toBe(true)

        const exitCode = yield* Effect.promise(() => worker.exited).pipe(Effect.timeout("12 seconds"))
        const outputText = (yield* Effect.promise(() => output)).join("\n")
        expect(exitCode).not.toBe(0)
        expect(outputText).toContain("timed out after")
        expect(groupAlive(probe.pgid)).toBe(false)
        expect(probe.members.every((member) => !sameProcess(member))).toBe(true)
      }),
      20_000,
    )

    fx.live(
      "forceKillAfter reaps a TERM-resistant descendant after its leader exits",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const handle = yield* js(termExitingParent(resistantDescendant))
        const leader = processIdentity(Number(handle.pid))
        if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
        const owned = [leader]
        const pgid = Number(handle.pid)
        recordProcess(pgid, leader)
        yield* Effect.addFinalizer(() =>
          Effect.promise(async () => {
            if (owned.some(sameProcess)) {
              try {
                process.kill(-pgid, "SIGKILL")
              } catch {}
            }
            if (!(await groupGone(pgid))) throw new Error(`owned process group ${pgid} survived test cleanup`)
          }).pipe(Effect.asVoid),
        )

        const descendant = processIdentity(yield* processTreeReady(handle))
        if (!descendant?.bootID || !descendant.startTicks) throw new Error("spawned descendant has no recorded start identity")
        owned.push(descendant)
        recordProcess(pgid, descendant)
        expect({ leader: sameProcess(leader), descendant: sameProcess(descendant) }).toEqual({
          leader: true,
          descendant: true,
        })

        yield* handle.kill({ forceKillAfter: "100 millis" })
        expect({
          groupAlive: groupAlive(pgid),
          leaderAlive: sameProcess(leader),
          descendantAlive: sameProcess(descendant),
        }).toEqual({ groupAlive: false, leaderAlive: false, descendantAlive: false })
      }),
    )

    fx.live(
      "scope release reaps a TERM-resistant descendant after its leader exits",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const owned: EventWriter.ProcessIdentity[] = []
        let pgid: number | undefined
        yield* Effect.addFinalizer(() =>
          Effect.promise(async () => {
            if (pgid === undefined) return
            if (owned.some(sameProcess)) {
              try {
                process.kill(-pgid, "SIGKILL")
              } catch {}
            }
            if (!(await groupGone(pgid))) throw new Error(`owned process group ${pgid} survived test cleanup`)
          }).pipe(Effect.asVoid),
        )

        yield* Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* js(termExitingParent(resistantDescendant), { forceKillAfter: 0 })
            pgid = Number(handle.pid)
            const leader = processIdentity(pgid)
            if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
            owned.push(leader)
            recordProcess(pgid, leader)
            const descendant = processIdentity(yield* processTreeReady(handle))
            if (!descendant?.bootID || !descendant.startTicks)
              throw new Error("spawned descendant has no recorded start identity")
            owned.push(descendant)
            recordProcess(pgid, descendant)
            expect({ leader: sameProcess(leader), descendant: sameProcess(descendant) }).toEqual({
              leader: true,
              descendant: true,
            })
          }),
        )

        if (pgid === undefined) throw new Error("scoped process group was not recorded")
        expect({
          groupAlive: groupAlive(pgid),
          owned: owned.map(sameProcess),
        }).toEqual({ groupAlive: false, owned: [false, false] })
      }),
    )

    fx.live(
      "scope release reaps descendants when a signaled leader exited before the scope closed",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const owned: EventWriter.ProcessIdentity[] = []
        let pgid: number | undefined
        yield* Effect.addFinalizer(() =>
          Effect.promise(async () => {
            if (pgid === undefined) return
            if (owned.some(sameProcess)) {
              try {
                process.kill(-pgid, "SIGKILL")
              } catch {}
            }
            if (!(await groupGone(pgid))) throw new Error(`owned process group ${pgid} survived test cleanup`)
          }).pipe(Effect.asVoid),
        )

        yield* Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* js(processTreeSource(resistantDescendant), { forceKillAfter: "100 millis" })
            pgid = Number(handle.pid)
            const leader = processIdentity(pgid)
            if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
            owned.push(leader)
            recordProcess(pgid, leader)
            const descendant = processIdentity(yield* processTreeReady(handle))
            if (!descendant?.bootID || !descendant.startTicks)
              throw new Error("spawned descendant has no recorded start identity")
            owned.push(descendant)
            recordProcess(pgid, descendant)
            expect({ leader: sameProcess(leader), descendant: sameProcess(descendant) }).toEqual({
              leader: true,
              descendant: true,
            })

            process.kill(pgid, "SIGTERM")
            const exited = yield* handle.exitCode.pipe(Effect.exit)
            expect(Exit.isFailure(exited)).toBe(true)
          }),
        )

        if (pgid === undefined) throw new Error("scoped process group was not recorded")
        expect({
          groupAlive: groupAlive(pgid),
          owned: owned.map(sameProcess),
        }).toEqual({ groupAlive: false, owned: [false, false] })
      }),
    )

    fx.effect(
      "isRunning reflects process state",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("done")')
        yield* handle.exitCode
        const running = yield* handle.isRunning
        expect(running).toBe(false)
      }),
    )
  })

  describe("error handling", () => {
    fx.effect(
      "fails for invalid command",
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          Effect.gen(function* () {
            const handle = yield* ChildProcess.make("nonexistent-command-12345")
            return yield* handle.exitCode
          }),
        )
        expect(Exit.isFailure(exit) ? true : exit.value !== ChildProcessSpawner.ExitCode(0)).toBe(true)
      }),
    )
  })

  describe("pipeline", () => {
    fx.effect(
      "pipes stdout of one command to stdin of another",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("hello world")').pipe(
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out.toUpperCase()))',
            ),
          ),
        )
        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode
        expect(out).toBe("HELLO WORLD")
      }),
    )

    fx.effect(
      "three-stage pipeline",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("hello world")').pipe(
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out.toUpperCase()))',
            ),
          ),
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out.replaceAll(" ", "-")))',
            ),
          ),
        )
        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode
        expect(out).toBe("HELLO-WORLD")
      }),
    )

    fx.effect(
      "pipes stderr with { from: 'stderr' }",
      Effect.gen(function* () {
        const handle = yield* js('process.stderr.write("error")').pipe(
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out))',
            ),
            { from: "stderr" },
          ),
        )
        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode
        expect(out).toBe("error")
      }),
    )

    fx.effect(
      "pipes combined output with { from: 'all' }",
      Effect.gen(function* () {
        const handle = yield* js('process.stdout.write("stdout\\n"); process.stderr.write("stderr\\n")').pipe(
          ChildProcess.pipeTo(
            js(
              'process.stdin.setEncoding("utf8"); let out = ""; process.stdin.on("data", (chunk) => out += chunk); process.stdin.on("end", () => process.stdout.write(out))',
            ),
            { from: "all" },
          ),
        )
        const out = yield* decodeByteStream(handle.stdout)
        yield* handle.exitCode
        expect(out).toContain("stdout")
        expect(out).toContain("stderr")
      }),
    )
  })

  describe("Windows-specific", () => {
    fx.effect(
      "uses shell routing on Windows",
      Effect.gen(function* () {
        if (process.platform !== "win32") return

        const out = yield* ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.string(
            ChildProcess.make("set", ["OPENCODE_TEST_SHELL"], {
              shell: true,
              extendEnv: true,
              env: { OPENCODE_TEST_SHELL: "ok" },
            }),
          ),
        )
        expect(out).toContain("OPENCODE_TEST_SHELL=ok")
      }),
    )

    fx.effect(
      "runs cmd scripts with spaces on Windows without shell",
      Effect.gen(function* () {
        if (process.platform !== "win32") return

        const tmp = yield* Effect.acquireRelease(
          Effect.promise(() => tmpdir()),
          (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
        )
        const dir = path.join(tmp.path, "with space")
        const file = path.join(dir, "echo cmd.cmd")

        yield* Effect.promise(() => fs.mkdir(dir, { recursive: true }))
        yield* Effect.promise(() => fs.writeFile(file, "@echo off\r\nif %~1==--stdio exit /b 0\r\nexit /b 7\r\n"))

        const code = yield* ChildProcessSpawner.ChildProcessSpawner.use((svc) =>
          svc.exitCode(
            ChildProcess.make(file, ["--stdio"], {
              stdin: "pipe",
              stdout: "pipe",
              stderr: "pipe",
            }),
          ),
        )
        expect(code).toBe(ChildProcessSpawner.ExitCode(0))
      }),
    )
  })
})
