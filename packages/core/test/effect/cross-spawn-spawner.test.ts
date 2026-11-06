import { afterEach, describe, expect } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Option, Stream } from "effect"
import type * as PlatformError from "effect/PlatformError"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import type { ChildProcessHandle } from "effect/unstable/process/ChildProcessSpawner"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { ProcessGroup } from "@opencode-ai/core/process-group"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { testEffect } from "../lib/effect"
import { groupAlive, processChildren, processGroupMembers, processIdentity, sameProcess } from "../fixture/process-group"

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

async function groupGone(pgid: number, timeout = 5_000) {
  const end = Date.now() + timeout
  while (Date.now() < end) {
    if (!groupAlive(pgid)) return true
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  return !groupAlive(pgid)
}

const writeAtomically = (filename: string, contents: string) =>
  Effect.promise(async () => {
    const temporary = `${filename}.tmp`
    await Bun.write(temporary, contents)
    await fs.rename(temporary, filename)
  })

const positivePID = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0

const ownerProbe = (value: unknown): { pgid: number; child: number } | undefined => {
  if (typeof value !== "object" || value === null) return
  const result = value as Record<string, unknown>
  if (!positivePID(result.pgid) || !positivePID(result.child)) return
  return { pgid: result.pgid, child: result.child }
}

const processIdentityRecord = (value: unknown): value is EventWriter.ProcessIdentity => {
  if (typeof value !== "object" || value === null) return false
  const identity = processIdentity(value)
  return Boolean(identity?.bootID && identity.startTicks)
}

const identityProbe = (value: unknown): { pgid: number; members: EventWriter.ProcessIdentity[] } | undefined => {
  if (typeof value !== "object" || value === null) return
  const result = value as Record<string, unknown>
  if (!positivePID(result.pgid) || !Array.isArray(result.members) || !result.members.every(processIdentityRecord))
    return
  return { pgid: result.pgid, members: result.members }
}

const readOwnerProbe = async (filename: string, timeout = 10_000) => {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    try {
      const result = ownerProbe(await Bun.file(filename).json())
      if (result) return result
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  throw new Error("fixture owner record was not atomically published with valid PIDs")
}

const readIdentityRecord = async (filename: string) => {
  try {
    return identityProbe(await Bun.file(filename).json())
  } catch {
    return undefined
  }
}

const readPositivePID = async (filename: string, timeout = 5_000) => {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    try {
      const value = Number((await Bun.file(filename).text()).trim())
      if (positivePID(value)) return value
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  throw new Error("fixture PID was not atomically published as a positive integer")
}

const readReadySignal = async (filename: string, timeout = 5_000) => {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    try {
      if ((await Bun.file(filename).text()).trim() === "ready") return
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  throw new Error("fixture ready signal was not atomically published")
}

async function processExited(identity: EventWriter.ProcessIdentity, timeout = 5_000) {
  const end = Date.now() + timeout
  while (Date.now() < end) {
    if (!sameProcess(identity)) return true
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  return !sameProcess(identity)
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

const recoverDetachedChild = (parent: EventWriter.ProcessIdentity, group: RecordedProcessGroup) =>
  Effect.gen(function* () {
    if (!sameProcess(parent)) throw new Error("pending-close fixture parent identity changed before recovery")
    const children = processChildren(parent.pid)
    if (children.length !== 1) throw new Error(`expected one pending-close fixture child, found ${children.length}`)
    const child = processIdentity(children[0])
    if (!child?.bootID || !child.startTicks || !sameProcess(child))
      throw new Error("pending-close fixture child has no current start identity")
    if (!sameProcess(parent) || !processChildren(parent.pid).includes(child.pid))
      throw new Error("pending-close fixture child was not owned by the recorded live parent")
    if (!processGroupMembers(child.pid).includes(child.pid))
      throw new Error("pending-close fixture child did not own its detached process group")

    group.pgid = child.pid
    group.members.push(child)
    recordProcess(child.pid, child)
    yield* cleanupRecordedProcessGroup(group)
    return child
  })

const acquireCloseProbeChild = (filename: string, parent: EventWriter.ProcessIdentity, group: RecordedProcessGroup) =>
  Effect.gen(function* () {
    const acquisition = yield* Effect.exit(Effect.promise(() => readPositivePID(filename)))
    if (Exit.isFailure(acquisition)) return { identity: yield* recoverDetachedChild(parent, group), recovered: true }

    const identity = processIdentity(acquisition.value)
    if (!identity?.bootID || !identity.startTicks) throw new Error("detached descendant has no recorded start identity")
    group.pgid = acquisition.value
    group.members.push(identity)
    recordProcess(acquisition.value, identity)
    return { identity, recovered: false }
  })

const cleanupPendingCloseProbe = (parent: EventWriter.ProcessIdentity, group: RecordedProcessGroup) =>
  Effect.gen(function* () {
    if (group.pgid === undefined && sameProcess(parent)) yield* recoverDetachedChild(parent, group)
    yield* cleanupRecordedProcessGroup(group)
    if (!sameProcess(parent)) return
    try {
      process.kill(parent.pid, "SIGKILL")
    } catch {}
    if (!(yield* Effect.promise(() => processExited(parent))))
      throw new Error(`pending-close fixture parent ${parent.pid} survived test cleanup`)
  })

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
      if (!(await groupGone(group.pgid, 250))) survivors.push({ group, pids: ownedProcessMembers(group) })
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
  expect({
    survivors: survivors.map((survivor) => ({ pgid: survivor.group.pgid, pids: survivor.pids })),
    remaining,
  }).toEqual({
    survivors: [],
    remaining: [],
  })
})

const resistantDescendant =
  'process.on("SIGTERM", () => {}); process.stdout.write("ready"); setInterval(() => {}, 10_000)'
const silentResistantDescendant = 'process.on("SIGTERM", () => {}); setInterval(() => {}, 10_000)'

const atomicWriteSource = (filename: string, contents: string, releaseFile?: string, afterPublish = "") => {
  if (!releaseFile)
    return `(()=>{const fs=require("node:fs");const temporary=${JSON.stringify(filename)}+".tmp";fs.writeFileSync(temporary,${contents});fs.renameSync(temporary,${JSON.stringify(filename)})})()`
  return `(()=>{const fs=require("node:fs");const filename=${JSON.stringify(filename)};const temporary=filename+".tmp";const value=${contents};fs.writeFileSync(temporary,value.slice(0,Math.max(1,Math.floor(value.length/2))));const publish=setInterval(()=>{if(!fs.existsSync(${JSON.stringify(releaseFile)}))return;clearInterval(publish);fs.writeFileSync(temporary,value);fs.renameSync(temporary,filename);${afterPublish}},10)})()`
}

const pendingCloseParentSource = (file: string, exitFile: string, publishPID = true) =>
  [
    `const child = require("node:child_process").spawn(process.execPath, ["-e", ${JSON.stringify('process.on("SIGTERM", () => {}); setInterval(() => {}, 10_000)')}], { detached: true, stdio: "inherit" })`,
    "child.unref()",
    publishPID
      ? atomicWriteSource(file, "String(child.pid)")
      : `require("node:fs").writeFileSync(${JSON.stringify(`${file}.tmp`)}, "")`,
    `const close = setInterval(() => { if (!require("node:fs").existsSync(${JSON.stringify(exitFile)})) return; clearInterval(close); process.exit(0) }, 10)`,
  ].join(";\n")

const processTreeSource = (
  child: string,
  onTerm?: string,
  ownershipFile?: string,
  publishChildStarted = true,
  readyFile?: string,
  ownerReleaseFile?: string,
) => {
  const childReady = readyFile
    ? [
        'child.stdout.once("data", () => {',
        "process.stdout.write(`child-ready:${child.pid}\\n`)",
        atomicWriteSource(readyFile, '"ready"'),
        "})",
      ].join("; ")
    : 'child.stdout.once("data", () => process.stdout.write(`child-ready:${child.pid}\\n`))'
  return [
    ...(onTerm ? [onTerm] : []),
    `const child = require("node:child_process").spawn(process.execPath, ["-e", ${JSON.stringify(child)}], { stdio: ["ignore", "pipe", "ignore"] })`,
    ...(ownershipFile
      ? [
          atomicWriteSource(
            ownershipFile,
            "JSON.stringify({ pgid: process.pid, child: child.pid })",
            ownerReleaseFile,
            publishChildStarted ? "process.stdout.write(`child-started:${child.pid}\\n`)" : "",
          ),
        ]
      : []),
    ...(publishChildStarted && !ownerReleaseFile ? ["process.stdout.write(`child-started:${child.pid}\\n`)"] : []),
    childReady,
    "setInterval(() => {}, 10_000)",
  ].join(";\n")
}

const termExitingParent = (
  child: string,
  exitCode = 0,
  ownershipFile?: string,
  publishChildStarted = true,
  readyFile?: string,
  ownerReleaseFile?: string,
) =>
  processTreeSource(
    child,
    `process.on("SIGTERM", () => process.exit(${exitCode}))`,
    ownershipFile,
    publishChildStarted,
    readyFile,
    ownerReleaseFile,
  )

const timeoutProbeMode = process.env.SWX_CORE_SPAWNER_PROBE_MODE ?? ""
const timeoutProbeOwnerFile = process.env.SWX_CORE_SPAWNER_OWNER_FILE
const timeoutProbeFile = process.env.SWX_CORE_SPAWNER_PROBE_FILE
const timeoutProbeReadyFile = process.env.SWX_CORE_SPAWNER_READY_FILE

const processTreeReady = (
  handle: ChildProcessHandle,
  pgid: number,
  onStarted?: (identity: EventWriter.ProcessIdentity) => Effect.Effect<void>,
  readinessTimeout: Duration.Input = "5 seconds",
) =>
  Effect.gen(function* () {
    const group = ownedProcessGroups.get(pgid)
    if (!group) throw new Error(`process group ${pgid} was not recorded before readiness wait`)
    yield* Effect.addFinalizer(() => Effect.sync(() => observeProcessGroup(group)))
    observeProcessGroup(group)

    const childStarted = yield* Deferred.make<number>()
    const ready = yield* Deferred.make<number>()
    let output = ""
    yield* Effect.forkScoped(
      Stream.runForEach(Stream.decodeText(handle.stdout), (chunk) => {
        output += chunk
        const started = output.match(/child-started:(\d+)/)
        const match = output.match(/child-ready:(\d+)/)
        return Effect.gen(function* () {
          if (started) yield* Deferred.succeed(childStarted, Number(started[1]))
          if (match) yield* Deferred.succeed(ready, Number(match[1]))
        })
      }),
    )
    const childPID = yield* Deferred.await(childStarted).pipe(
      Effect.timeoutOrElse({
        duration: "5 seconds",
        orElse: () => Effect.fail(new Error("child-started identity marker was not observed")),
      }),
    )
    const identity = yield* Effect.promise(async () => {
      const deadline = Date.now() + 5_000
      while (Date.now() < deadline) {
        const current = processIdentity(childPID)
        if (current) return current
        await new Promise((resolve) => setTimeout(resolve, 20))
      }
      throw new Error("spawned descendant has no recorded start identity")
    })
    recordProcess(pgid, identity)
    if (onStarted) yield* onStarted(identity)
    const readyPID = yield* Deferred.await(ready).pipe(
      Effect.timeoutOrElse({
        duration: readinessTimeout,
        orElse: () => Effect.fail(new Error("child readiness marker was not produced after a recorded child")),
      }),
    )
    if (readyPID !== childPID)
      return yield* Effect.fail(new Error("child readiness PID did not match its recorded identity"))
    return identity
  })

type OwnerExitMode = "live" | "cleanup-owed" | "released"

const ownerExitProbe = (mode: OwnerExitMode) =>
  Effect.gen(function* () {
    if (process.platform === "win32") return

    const temp = yield* Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (temp) => Effect.promise(() => temp[Symbol.asyncDispose]()).pipe(Effect.asVoid),
    )
    const identityFile = path.join(temp.path, "group.json")
    const releaseFile = path.join(temp.path, "release")
    const worker = Bun.spawn(
      [
        process.execPath,
        "test",
        "--timeout",
        "30000",
        "./test/effect/cross-spawn-spawner.test.ts",
        "-t",
        "Core process-tree boundary worker",
      ],
      {
        cwd: process.cwd(),
        env: {
          ...process.env,
          SWX_CORE_SPAWNER_PROBE_MODE: `owner-exit-${mode}`,
          SWX_CORE_SPAWNER_PROBE_FILE: identityFile,
          SWX_CORE_SPAWNER_READY_FILE: releaseFile,
        },
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
      },
    )
    const workerIdentity = processIdentity(worker.pid)
    if (!workerIdentity?.bootID || !workerIdentity.startTicks)
      throw new Error("process-tree owner-exit worker has no recorded start identity")
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        if (!sameProcess(workerIdentity)) return
        try {
          process.kill(workerIdentity.pid, "SIGKILL")
        } catch {}
      }),
    )
    const output = Promise.all([new Response(worker.stdout).text(), new Response(worker.stderr).text()])
    const probe = yield* Effect.promise(async () => {
      const deadline = Date.now() + 15_000
      while (Date.now() < deadline) {
        const result = await readIdentityRecord(identityFile)
        if (result) return result
        await new Promise((resolve) => setTimeout(resolve, 20))
      }
      throw new Error(`owner-exit fixture did not publish its ${mode} process group`)
    })
    if (probe.members.length !== 2) throw new Error(`owner-exit fixture recorded ${probe.members.length} group members`)
    const pgid = probe.pgid
    const leader = probe.members.find((member) => member.pid === pgid)
    const descendant = probe.members.find((member) => member.pid !== pgid)
    if (!leader || !descendant) throw new Error("owner-exit fixture did not record its leader and descendant")
    const ownership: RecordedProcessGroup = { pgid, members: probe.members }
    ownership.members.forEach((member) => recordProcess(pgid, member))
    yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(ownership))

    const beforeExit = {
      groupAlive: groupAlive(pgid),
      leaderAlive: sameProcess(leader),
      descendantAlive: sameProcess(descendant),
    }
    expect(beforeExit).toEqual(
      mode === "live"
        ? { groupAlive: true, leaderAlive: true, descendantAlive: true }
        : { groupAlive: true, leaderAlive: false, descendantAlive: true },
    )

    yield* writeAtomically(releaseFile, "ready")
    const exitCode = yield* Effect.promise(() => worker.exited).pipe(Effect.timeout("20 seconds"))
    const outputText = (yield* Effect.promise(() => output)).join("\n")
    if (exitCode !== 0) throw new Error(`owner-exit worker failed (${exitCode}): ${outputText}`)

    if (mode === "released") {
      expect({
        groupAlive: groupAlive(pgid),
        leaderAlive: sameProcess(leader),
        descendantAlive: sameProcess(descendant),
      }).toEqual({ groupAlive: true, leaderAlive: false, descendantAlive: true })
      yield* cleanupRecordedProcessGroup(ownership)
      expect(yield* Effect.promise(() => groupGone(pgid))).toBe(true)
      return
    }

    const gone = yield* Effect.promise(() => groupGone(pgid))
    expect({
      groupGone: gone,
      leaderAlive: sameProcess(leader),
      descendantAlive: sameProcess(descendant),
    }).toEqual({ groupGone: true, leaderAlive: false, descendantAlive: false })
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
    fx.live(
      "decides to use the live-PGID fallback when identity is unavailable",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const recorded: RecordedProcessGroup = { pgid: undefined, members: [] }
        yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(recorded))
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
        const handle = yield* spawner.spawn(
          js('process.on("SIGTERM", () => {}); setInterval(() => {}, 1000)', {
            detached: true,
            stdin: "ignore",
            stdout: "ignore",
            stderr: "ignore",
            forceKillAfter: "3 seconds",
          }),
        )
        const pgid = Number(handle.pid)
        recorded.pgid = pgid
        const identity = processIdentity(pgid)
        if (!identity) throw new Error("identity fallback fixture has no process identity")
        recorded.members.push(identity)
        recordProcess(pgid, identity)

        expect(
          ProcessGroup.decideGroupOwnership({ pgid, identities: [identity], readIdentity: () => undefined }),
        ).toEqual({ action: "signal", evidence: "unavailable" })
        expect(groupAlive(pgid)).toBe(true)
      }),
    )

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
        const pgid = Number(handle.pid)
        group.pgid = pgid
        const leader = processIdentity(pgid)
        if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
        group.members.push(leader)
        recordProcess(pgid, leader)
        const descendant = yield* processTreeReady(handle, pgid)
        if (!descendant?.bootID || !descendant.startTicks)
          throw new Error("spawned descendant has no recorded start identity")
        group.members.push(descendant)
        recordProcess(pgid, descendant)
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
            const pgid = Number(handle.pid)
            group.pgid = pgid
            const leader = processIdentity(pgid)
            if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
            group.members.push(leader)
            recordProcess(pgid, leader)
            const descendant = yield* processTreeReady(handle, pgid)
            if (!descendant?.bootID || !descendant.startTicks)
              throw new Error("spawned descendant has no recorded start identity")
            group.members.push(descendant)
            recordProcess(pgid, descendant)
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
          const pgid = Number(handle.pid)
          group.pgid = pgid
          const leader = processIdentity(pgid)
          if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
          group.members.push(leader)
          recordProcess(pgid, leader)
          const descendant = yield* processTreeReady(handle, pgid)
          if (!descendant?.bootID || !descendant.startTicks)
            throw new Error("spawned descendant has no recorded start identity")
          group.members.push(descendant)
          recordProcess(pgid, descendant)
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
      "forceKillAfter signals detached-false leaders directly",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const handle = yield* js('process.on("SIGTERM", () => process.exit(0)); setInterval(() => {}, 10_000)', {
          detached: false,
        })
        const leader = processIdentity(Number(handle.pid))
        if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (!sameProcess(leader)) return
            try {
              process.kill(leader.pid, "SIGKILL")
            } catch {}
          }),
        )

        const completion = yield* handle.kill({ forceKillAfter: 100 }).pipe(Effect.timeoutOption("1 second"))
        expect(Option.isSome(completion)).toBe(true)
        expect(sameProcess(leader)).toBe(false)
      }),
      5_000,
    )

    fx.live(
      "reports group members when SIGKILL does not remove them within the final bound",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const group: RecordedProcessGroup = { pgid: undefined, members: [] }
        yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(group))
        const handle = yield* js(termExitingParent(resistantDescendant))
        const pgid = Number(handle.pid)
        group.pgid = pgid
        const leader = processIdentity(pgid)
        if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
        group.members.push(leader)
        recordProcess(pgid, leader)
        const descendant = yield* processTreeReady(handle, pgid)
        if (!descendant?.bootID || !descendant.startTicks)
          throw new Error("spawned descendant has no recorded start identity")
        group.members.push(descendant)
        recordProcess(pgid, descendant)
        expect({ leader: sameProcess(leader), descendant: sameProcess(descendant) }).toEqual({
          leader: true,
          descendant: true,
        })

        const originalKill = process.kill.bind(process)
        const rejectSignal = { value: true }
        const patchedKill: typeof process.kill = (pid, signal) => {
          if (rejectSignal.value && pid === -pgid && signal === "SIGKILL") {
            rejectSignal.value = false
            throw Object.assign(new Error("injected SIGKILL permission denial"), { code: "EPERM" })
          }
          return originalKill(pid, signal)
        }
        yield* Effect.addFinalizer(() => Effect.sync(() => void (process.kill = originalKill)))
        process.kill = patchedKill

        const started = Date.now()
        const exit = yield* Effect.exit(handle.kill({ forceKillAfter: 0 }))
        expect(Exit.isFailure(exit)).toBe(true)
        expect(Date.now() - started).toBeLessThan(7_000)
        if (Exit.isFailure(exit)) expect(Cause.prettyErrors(exit.cause).join("\n")).toContain(String(descendant.pid))
      }),
      15_000,
    )

    fx.live(
      "Core process-tree boundary worker",
      Effect.gen(function* () {
        if (!timeoutProbeMode || process.platform === "win32") return
        if (timeoutProbeMode.startsWith("owner-exit-")) {
          if (!timeoutProbeFile || !timeoutProbeReadyFile)
            throw new Error("owner-exit probe paths were not supplied")
          const mode = timeoutProbeMode.slice("owner-exit-".length)
          if (mode !== "live" && mode !== "cleanup-owed" && mode !== "released")
            throw new Error(`unknown owner-exit mode: ${mode}`)
          const source =
            mode === "released"
              ? processTreeSource(resistantDescendant, 'process.on("SIGUSR1", () => process.exit(0))')
              : termExitingParent(resistantDescendant)
          const handle = yield* js(source)
          const pgid = Number(handle.pid)
          const leader = processIdentity(pgid)
          if (!leader?.bootID || !leader.startTicks) throw new Error("owner-exit worker leader has no start identity")
          recordProcess(pgid, leader)
          const descendant = yield* processTreeReady(handle, pgid)

          if (mode === "cleanup-owed") {
            yield* handle.kill({ forceKillAfter: "2 minutes" }).pipe(Effect.forkChild)
            if (!(yield* Effect.promise(() => processExited(leader))))
              throw new Error("cleanup-owed leader did not exit before owner handoff")
          }
          if (mode === "released") {
            process.kill(pgid, "SIGUSR1")
            expect(yield* handle.exitCode.pipe(Effect.timeout("5 seconds"))).toBe(ChildProcessSpawner.ExitCode(0))
          }

          const beforeExit = {
            groupAlive: groupAlive(pgid),
            leaderAlive: sameProcess(leader),
            descendantAlive: sameProcess(descendant),
          }
          expect(beforeExit).toEqual(
            mode === "live"
              ? { groupAlive: true, leaderAlive: true, descendantAlive: true }
              : { groupAlive: true, leaderAlive: false, descendantAlive: true },
          )
          yield* writeAtomically(timeoutProbeFile, JSON.stringify({ pgid, members: [leader, descendant] }))
          yield* Effect.promise(() => readReadySignal(timeoutProbeReadyFile, 20_000))
          process.exit(0)
        }
        if (!timeoutProbeOwnerFile || !timeoutProbeFile || !timeoutProbeReadyFile)
          throw new Error("process-tree probe paths were not supplied")

        const child = timeoutProbeMode === "readiness" ? silentResistantDescendant : resistantDescendant
        const publishChildStarted = timeoutProbeMode !== "child-started"
        const handle = yield* js(
          termExitingParent(child, 0, timeoutProbeOwnerFile, publishChildStarted, timeoutProbeReadyFile),
        )
        const pgid = Number(handle.pid)
        const leader = processIdentity(pgid)
        if (!leader?.bootID || !leader.startTicks) throw new Error("timeout worker leader has no start identity")
        recordProcess(pgid, leader)
        const descendant = yield* processTreeReady(
          handle,
          pgid,
          (identity) =>
            writeAtomically(timeoutProbeFile, JSON.stringify({ pgid, members: [leader, identity] })).pipe(
              Effect.asVoid,
            ),
          timeoutProbeMode === "readiness" ? "1 second" : "5 seconds",
        )
        if (!sameProcess(leader) || !sameProcess(descendant)) throw new Error("timeout worker fixture was not live")
        if (timeoutProbeMode === "timeout") yield* Effect.never
      }),
      timeoutProbeMode === "timeout" ? 5_000 : timeoutProbeMode.startsWith("owner-exit-") ? 30_000 : 15_000,
    )

    fx.live(
      "Core fixture cleanup handles Bun timeouts and readiness failures",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const run = Effect.fnUntraced(function* (mode: "timeout" | "readiness" | "child-started") {
          const stem = `.cross-spawn-${mode}-${process.pid}`
          const ownerFile = path.join(process.cwd(), `${stem}-owner.json`)
          const identityFile = path.join(process.cwd(), `${stem}-identity.json`)
          const readyFile = path.join(process.cwd(), `${stem}-ready`)
          const files = [ownerFile, identityFile, readyFile]
          const artifacts = [...files, ...files.map((file) => `${file}.tmp`)]
          yield* Effect.promise(() => Promise.all(artifacts.map((file) => fs.rm(file, { force: true }))))
          yield* Effect.addFinalizer(() =>
            Effect.promise(() => Promise.all(artifacts.map((file) => fs.rm(file, { force: true })))).pipe(
              Effect.asVoid,
            ),
          )

          const worker = Bun.spawn(
            [
              process.execPath,
              "test",
              "--timeout",
              "20000",
              "./test/effect/cross-spawn-spawner.test.ts",
              "-t",
              "Core process-tree boundary worker",
            ],
            {
              cwd: process.cwd(),
              env: {
                ...process.env,
                SWX_CORE_SPAWNER_PROBE_MODE: mode,
                SWX_CORE_SPAWNER_OWNER_FILE: ownerFile,
                SWX_CORE_SPAWNER_PROBE_FILE: identityFile,
                SWX_CORE_SPAWNER_READY_FILE: readyFile,
              },
              stdin: "ignore",
              stdout: "pipe",
              stderr: "pipe",
            },
          )
          const workerIdentity = processIdentity(worker.pid)
          if (!workerIdentity?.bootID || !workerIdentity.startTicks)
            throw new Error("process-tree worker has no start identity")
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              if (!sameProcess(workerIdentity)) return
              try {
                process.kill(worker.pid, "SIGKILL")
              } catch {}
            }),
          )
          const output = Promise.all([new Response(worker.stdout).text(), new Response(worker.stderr).text()])
          const owner = yield* Effect.promise(() => readOwnerProbe(ownerFile))
          const leader = processIdentity(owner.pgid)
          const descendant = processIdentity(owner.child)
          if (!leader?.bootID || !leader.startTicks || !descendant?.bootID || !descendant.startTicks)
            throw new Error("producer PIDs have no readable start identity")
          recordProcess(owner.pgid, leader)
          recordProcess(owner.pgid, descendant)
          const ownership: RecordedProcessGroup = { pgid: owner.pgid, members: [leader, descendant] }
          yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(ownership))
          expect({ leader: sameProcess(leader), descendant: sameProcess(descendant) }).toEqual({
            leader: true,
            descendant: true,
          })

          if (mode !== "readiness") yield* Effect.promise(() => readReadySignal(readyFile))

          const exitCode = yield* Effect.promise(() => worker.exited).pipe(Effect.timeout("15 seconds"))
          const outputText = (yield* Effect.promise(() => output)).join("\n")
          expect(exitCode).not.toBe(0)
          expect(outputText).toContain(
            mode === "timeout"
              ? "timed out after"
              : mode === "readiness"
                ? "child readiness marker was not produced after a recorded child"
                : "child-started identity marker was not observed",
          )
          expect(groupAlive(owner.pgid)).toBe(false)
          expect(ownership.members.every((member) => !sameProcess(member))).toBe(true)

          const identityRecord = yield* Effect.promise(() => readIdentityRecord(identityFile))
          expect(identityRecord !== undefined).toBe(mode !== "child-started")
          if (identityRecord)
            expect(identityRecord.members.map((member) => member.pid)).toEqual([owner.pgid, owner.child])
        })

        yield* run("timeout")
        yield* run("readiness")
        yield* run("child-started")
      }),
      30_000,
    )

    fx.live(
      "abrupt owner exit kills a live detached group",
      ownerExitProbe("live"),
      30_000,
    )

    fx.live(
      "abrupt owner exit keeps a cleanup-owed group after its leader exits",
      ownerExitProbe("cleanup-owed"),
      30_000,
    )

    fx.live(
      "abrupt owner exit leaves descendants released by leader exit",
      ownerExitProbe("released"),
      30_000,
    )

    fx.live(
      "detached spawn releases its owner entry when acquire acknowledgement rejects or is delayed",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
        for (const mode of ["rejected", "delayed"] as const) {
          let pgid: number | undefined
          const events: CrossSpawnSpawner.ProcessGroupEvent[] = []
          const releaseObserved = Promise.withResolvers<void>()
          const group: RecordedProcessGroup = { pgid: undefined, members: [] }
          yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(group))
          CrossSpawnSpawner.setProcessGroupObserver(async (event) => {
            events.push(event)
            if (event.action === "release") releaseObserved.resolve()
            if (event.action !== "acquire" || event.cleanupOwed) return
            pgid = event.pgid
            group.pgid = event.pgid
            const leader = processIdentity(event.pgid)
            if (leader) {
              group.members.push(leader)
              recordProcess(event.pgid, leader)
            }
            expect(groupAlive(event.pgid)).toBe(true)
            if (mode === "delayed") await new Promise((resolve) => setTimeout(resolve, 25))
            throw new Error(`injected ${mode} process-group acknowledgement failure`)
          })
          yield* Effect.addFinalizer(() => Effect.sync(() => CrossSpawnSpawner.setProcessGroupObserver(undefined)))
          const result = yield* Effect.exit(
            spawner.spawn(js("setInterval(() => {}, 1000)", { detached: true })).pipe(Effect.timeout("10 seconds")),
          )
          expect(Exit.isFailure(result)).toBe(true)
          if (Exit.isFailure(result))
            expect(Cause.prettyErrors(result.cause).join("\n")).toContain("ChildProcess.spawn")
          if (pgid === undefined) throw new Error(`${mode} acquire acknowledgement was not observed`)
          yield* Effect.promise(() => releaseObserved.promise).pipe(Effect.timeout("5 seconds"))
          expect(events.at(-1)?.action).toBe("release")
          expect(events.at(-1)?.identity).toEqual(events[0]?.identity)
          expect(yield* Effect.promise(() => groupGone(pgid!))).toBe(true)
        }
      }),
      30_000,
    )

    fx.live(
      "a late initial-acquire failure does not re-own a leader-exited released group",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
        for (const mode of ["rejected", "delayed"] as const) {
          const initialObserved = Promise.withResolvers<CrossSpawnSpawner.ProcessGroupEvent>()
          const releaseObserved = Promise.withResolvers<void>()
          const allowFailure = Promise.withResolvers<void>()
          const events: CrossSpawnSpawner.ProcessGroupEvent[] = []
          const owner = new Map<number, CrossSpawnSpawner.ProcessGroupEvent>()
          const group: RecordedProcessGroup = { pgid: undefined, members: [] }
          yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(group))
          CrossSpawnSpawner.setProcessGroupObserver(async (event) => {
            events.push(event)
            if (event.action === "acquire") owner.set(event.pgid, event)
            else if (owner.get(event.pgid)?.identity === event.identity) owner.delete(event.pgid)
            if (event.action === "release") releaseObserved.resolve()
            if (event.action === "acquire" && !event.cleanupOwed) {
              group.pgid = event.pgid
              const leader = processIdentity(event.pgid)
              if (leader) {
                group.members.push(leader)
                recordProcess(event.pgid, leader)
              }
              initialObserved.resolve(event)
              await allowFailure.promise
              if (mode === "delayed") await new Promise((resolve) => setTimeout(resolve, 25))
              throw new Error(`injected ${mode} initial-acquire acknowledgement failure`)
            }
          })
          yield* Effect.addFinalizer(() => Effect.sync(() => CrossSpawnSpawner.setProcessGroupObserver(undefined)))
          const spawning = yield* spawner
            .spawn(js("setTimeout(() => process.exit(0), 100)", { detached: true }))
            .pipe(Effect.forkChild)
          const initial = yield* Effect.promise(() => initialObserved.promise).pipe(Effect.timeout("5 seconds"))
          const pgid = initial.pgid
          yield* Effect.promise(() => releaseObserved.promise).pipe(Effect.timeout("5 seconds"))
          expect(yield* Effect.promise(() => groupGone(pgid))).toBe(true)

          const originalKill = process.kill.bind(process)
          const signalled: Array<NodeJS.Signals | number | undefined> = []
          const patchedKill: typeof process.kill = (pid, signal) => {
            if (pid === -pgid && signal === "SIGKILL") {
              signalled.push(signal)
              return true
            }
            return originalKill(pid, signal)
          }
          yield* Effect.addFinalizer(() => Effect.sync(() => void (process.kill = originalKill)))
          process.kill = patchedKill
          allowFailure.resolve()
          const result = yield* Fiber.join(spawning).pipe(Effect.timeout("5 seconds"), Effect.exit)
          expect(Exit.isFailure(result)).toBe(true)
          expect(events.map((event) => event.action)).toEqual(["acquire", "release"])
          expect(owner.size).toBe(0)
          expect(signalled).toEqual([])
          expect(events.at(-1)?.identity).toEqual(initial.identity)
        }
      }),
      30_000,
    )

    fx.live(
      "group cleanup signals before a delayed ownership acknowledgement and releases without waiting for its ack",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const events: { action: string; cleanupOwed: boolean; groupAlive: boolean }[] = []
        const cleanupAnnounced = Promise.withResolvers<void>()
        const allowSignal = Promise.withResolvers<void>()
        const releaseAnnounced = Promise.withResolvers<void>()
        const allowReleaseAck = Promise.withResolvers<void>()
        const releaseAckFinished = Promise.withResolvers<void>()
        const signalIssued = Promise.withResolvers<void>()
        const signalState = { sent: false }
        let releaseAckSettled = false
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            allowSignal.resolve()
            allowReleaseAck.resolve()
          }),
        )
        CrossSpawnSpawner.setProcessGroupObserver(async (event) => {
          events.push({ action: event.action, cleanupOwed: event.cleanupOwed, groupAlive: groupAlive(event.pgid) })
          if (event.action === "acquire" && event.cleanupOwed) {
            cleanupAnnounced.resolve()
            await allowSignal.promise
          }
          if (event.action === "release") {
            releaseAnnounced.resolve()
            await allowReleaseAck.promise
            releaseAckSettled = true
            releaseAckFinished.resolve()
          }
        })
        yield* Effect.addFinalizer(() => Effect.sync(() => CrossSpawnSpawner.setProcessGroupObserver(undefined)))
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
        const handle = yield* spawner.spawn(js("setInterval(() => {}, 1000)", { detached: true }))
        const pgid = Number(handle.pid)
        const leader = processIdentity(pgid)
        if (!leader?.bootID || !leader.startTicks) throw new Error("spawned group leader has no identity")
        expect(processGroupMembers(pgid).includes(leader.pid)).toBe(true)
        recordProcess(pgid, leader)
        yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup({ pgid, members: [leader] }))
        const originalKill = process.kill.bind(process)
        const patchedKill: typeof process.kill = (pid, signal) => {
          if (pid === -pgid && signal === "SIGUSR2") {
            signalState.sent = true
            signalIssued.resolve()
          }
          return originalKill(pid, signal)
        }
        yield* Effect.addFinalizer(() => Effect.sync(() => void (process.kill = originalKill)))
        process.kill = patchedKill
        const killing = yield* handle.kill({ killSignal: "SIGUSR2", forceKillAfter: "2 seconds" }).pipe(Effect.forkChild)
        yield* Effect.promise(() => cleanupAnnounced.promise).pipe(Effect.timeout("5 seconds"))
        expect(events.some((event) => event.action === "acquire" && !event.cleanupOwed && event.groupAlive)).toBe(true)
        expect(events.some((event) => event.action === "acquire" && event.cleanupOwed && event.groupAlive)).toBe(true)
        yield* Effect.promise(() => signalIssued.promise).pipe(Effect.timeout("3 seconds"))
        expect(signalState.sent).toBe(true)
        yield* Effect.promise(() => releaseAnnounced.promise).pipe(Effect.timeout("5 seconds"))

        const release = events.find((event) => event.action === "release")
        expect(release?.groupAlive).toBe(false)
        yield* Fiber.join(killing).pipe(Effect.timeout("5 seconds"))
        expect(releaseAckSettled).toBe(false)
        allowReleaseAck.resolve()
        yield* Effect.promise(() => releaseAckFinished.promise).pipe(Effect.timeout("5 seconds"))
        expect(signalState.sent).toBe(true)
        expect(yield* Effect.promise(() => groupGone(pgid))).toBe(true)
      }),
      20_000,
    )

    fx.live(
      "group cleanup still signals and releases after its cleanup-owed acknowledgement rejects",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const group: RecordedProcessGroup = { pgid: undefined, members: [] }
        const releaseObserved = Promise.withResolvers<void>()
        yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(group))
        CrossSpawnSpawner.setProcessGroupObserver(async (event) => {
          if (event.action === "acquire" && event.cleanupOwed)
            throw new Error("injected cleanup-owed acknowledgement rejection")
          if (event.action === "release") releaseObserved.resolve()
        })
        yield* Effect.addFinalizer(() => Effect.sync(() => CrossSpawnSpawner.setProcessGroupObserver(undefined)))
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
        const handle = yield* spawner.spawn(js("setInterval(() => {}, 1000)", { detached: true }))
        const pgid = Number(handle.pid)
        group.pgid = pgid
        const leader = processIdentity(pgid)
        if (!leader?.bootID || !leader.startTicks) throw new Error("spawned group leader has no identity")
        group.members.push(leader)
        recordProcess(pgid, leader)

        const result = yield* Effect.exit(handle.kill({ forceKillAfter: "2 seconds" }).pipe(Effect.timeout("5 seconds")))
        expect(Exit.isSuccess(result)).toBe(true)
        yield* Effect.promise(() => releaseObserved.promise).pipe(Effect.timeout("5 seconds"))
        expect(yield* Effect.promise(() => groupGone(pgid))).toBe(true)
      }),
      15_000,
    )

    fx.live(
      "fixture ownership stays private until its partial record is atomically published",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const directory = yield* Effect.promise(() => fs.mkdtemp(path.join(os.tmpdir(), "opencode-owner-publication-")))
        const ownerFile = path.join(directory, "owner.json")
        const releaseFile = path.join(directory, "release")
        yield* Effect.addFinalizer(() =>
          Effect.promise(() => fs.rm(directory, { recursive: true, force: true })).pipe(Effect.asVoid),
        )

        const handle = yield* js(termExitingParent(resistantDescendant, 0, ownerFile, true, undefined, releaseFile))
        const pgid = Number(handle.pid)
        const group: RecordedProcessGroup = { pgid, members: [] }
        const leader = processIdentity(pgid)
        if (!leader?.bootID || !leader.startTicks) throw new Error("fixture leader has no recorded start identity")
        group.members.push(leader)
        recordProcess(pgid, leader)
        yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(group))

        const readiness = yield* processTreeReady(handle, pgid).pipe(Effect.forkChild)
        const ownerRead = yield* Effect.promise(() => readOwnerProbe(ownerFile)).pipe(Effect.forkChild)
        const partial = yield* Effect.promise(async () => {
          const deadline = Date.now() + 3_000
          while (Date.now() < deadline) {
            for (const filename of [ownerFile, `${ownerFile}.tmp`]) {
              try {
                const value = await Bun.file(filename).text()
                if (value) return { filename, value }
              } catch {}
            }
            await new Promise((resolve) => setTimeout(resolve, 20))
          }
          throw new Error("fixture did not hold a partial owner record")
        })
        expect(partial.filename).toBe(`${ownerFile}.tmp`)
        expect(() => JSON.parse(partial.value)).toThrow()
        expect(yield* Effect.promise(() => Bun.file(ownerFile).exists())).toBe(false)
        expect(Option.isNone(yield* Fiber.await(ownerRead).pipe(Effect.timeoutOption("100 millis")))).toBe(true)

        yield* writeAtomically(releaseFile, "release")
        const owner = yield* Fiber.join(ownerRead).pipe(Effect.timeout("3 seconds"))
        const descendant = processIdentity(owner.child)
        if (!descendant?.bootID || !descendant.startTicks)
          throw new Error("fixture descendant has no recorded start identity")
        group.members.push(descendant)
        recordProcess(pgid, descendant)
        expect(owner.pgid).toBe(pgid)
        expect((yield* Fiber.join(readiness).pipe(Effect.timeout("3 seconds"))).pid).toBe(owner.child)

        const completion = yield* handle.kill({ forceKillAfter: "100 millis" }).pipe(Effect.timeoutOption("2 seconds"))
        expect(Option.isSome(completion)).toBe(true)
        expect(yield* Effect.promise(() => groupGone(pgid))).toBe(true)
        expect(group.members.every((member) => !sameProcess(member))).toBe(true)
      }),
      12_000,
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

        const descendant = yield* processTreeReady(handle, pgid)
        if (!descendant?.bootID || !descendant.startTicks)
          throw new Error("spawned descendant has no recorded start identity")
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
            const descendant = yield* processTreeReady(handle, pgid)
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
            const descendant = yield* processTreeReady(handle, pgid)
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

    fx.live(
      "scope release preserves a requested kill after leader exit and kill-fiber interruption",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const run = Effect.fnUntraced(function* (exitCode: number) {
          const group: RecordedProcessGroup = { pgid: undefined, members: [] }
          yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(group))
          yield* Effect.scoped(
            Effect.gen(function* () {
              const handle = yield* js(termExitingParent(resistantDescendant, exitCode))
              group.pgid = Number(handle.pid)
              const leader = processIdentity(group.pgid)
              if (!leader?.bootID || !leader.startTicks)
                throw new Error("spawned leader has no recorded start identity")
              group.members.push(leader)
              recordProcess(group.pgid, leader)
              const descendant = yield* processTreeReady(handle, group.pgid!)
              if (!descendant?.bootID || !descendant.startTicks)
                throw new Error("spawned descendant has no recorded start identity")
              group.members.push(descendant)
              recordProcess(group.pgid, descendant)

              const killing = yield* handle.kill({ forceKillAfter: "2 seconds" }).pipe(Effect.forkChild)
              expect(yield* Effect.promise(() => processExited(leader))).toBe(true)
              expect(sameProcess(descendant)).toBe(true)
              const interrupted = yield* Fiber.interrupt(killing).pipe(Effect.timeoutOption("1 second"))
              expect(Option.isSome(interrupted)).toBe(true)
            }),
          )

          expect(group.pgid === undefined ? false : groupAlive(group.pgid)).toBe(false)
          expect(group.members.every((member) => !sameProcess(member))).toBe(true)
        })

        const zero = yield* Effect.exit(run(0))
        const nonzero = yield* Effect.exit(run(17))
        expect({ zero: Exit.isSuccess(zero), nonzero: Exit.isSuccess(nonzero) }).toEqual({ zero: true, nonzero: true })
      }),
      20_000,
    )

    fx.live(
      "scope release ignores a failed signal to an exited leader with a pending close",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const file = path.join(process.cwd(), `.cross-spawn-close-probe-${process.pid}.json`)
        const exitFile = path.join(process.cwd(), `.cross-spawn-close-probe-${process.pid}.exit`)
        const artifacts = [file, `${file}.tmp`, exitFile]
        const detachedGroup: RecordedProcessGroup = { pgid: undefined, members: [] }
        yield* Effect.addFinalizer(() => cleanupRecordedProcessGroup(detachedGroup))
        yield* Effect.promise(() => Promise.all(artifacts.map((item) => fs.rm(item, { force: true }))))
        yield* Effect.addFinalizer(() =>
          Effect.promise(() => Promise.all(artifacts.map((item) => fs.rm(item, { force: true })))).pipe(Effect.asVoid),
        )

        const parentSource = pendingCloseParentSource(file, exitFile)
        const exit = yield* Effect.exit(
          Effect.scoped(
            Effect.gen(function* () {
              const handle = yield* js(parentSource, { forceKillAfter: "3 seconds" })
              const leader = processIdentity(Number(handle.pid))
              if (!leader?.bootID || !leader.startTicks)
                throw new Error("spawned leader has no recorded start identity")
              recordProcess(leader.pid, leader)
              yield* Effect.addFinalizer(() => {
                if (detachedGroup.pgid !== undefined || !sameProcess(leader)) return Effect.void
                return recoverDetachedChild(leader, detachedGroup).pipe(Effect.asVoid)
              })
              const acquired = yield* acquireCloseProbeChild(file, leader, detachedGroup)
              expect(acquired.recovered).toBe(false)
              const detached = acquired.identity
              expect({ leader: sameProcess(leader), detached: sameProcess(detached) }).toEqual({
                leader: true,
                detached: true,
              })
              yield* writeAtomically(exitFile, "exit")
              expect(yield* Effect.promise(() => processExited(leader))).toBe(true)
              expect(yield* handle.isRunning).toBe(true)
            }),
          ),
        )

        expect(Exit.isSuccess(exit)).toBe(true)
        expect(detachedGroup.members.every(sameProcess)).toBe(true)
      }),
      15_000,
    )

    fx.live(
      "reaps a detached child when pending-close PID acquisition expires",
      Effect.gen(function* () {
        if (process.platform === "win32") return

        const file = path.join(process.cwd(), `.cross-spawn-close-failure-${process.pid}.json`)
        const exitFile = path.join(process.cwd(), `.cross-spawn-close-failure-${process.pid}.exit`)
        const artifacts = [file, `${file}.tmp`, exitFile]
        const detachedGroup: RecordedProcessGroup = { pgid: undefined, members: [] }
        yield* Effect.promise(() => Promise.all(artifacts.map((item) => fs.rm(item, { force: true }))))
        yield* Effect.addFinalizer(() =>
          Effect.promise(() => Promise.all(artifacts.map((item) => fs.rm(item, { force: true })))).pipe(Effect.asVoid),
        )

        const parentSource = pendingCloseParentSource(file, exitFile, false)
        const handle = yield* js(parentSource, { forceKillAfter: "3 seconds" })
        const leader = processIdentity(Number(handle.pid))
        if (!leader?.bootID || !leader.startTicks) throw new Error("spawned leader has no recorded start identity")
        recordProcess(leader.pid, leader)
        yield* Effect.addFinalizer(() => cleanupPendingCloseProbe(leader, detachedGroup))

        const recovered = yield* acquireCloseProbeChild(file, leader, detachedGroup)
        expect(recovered.recovered).toBe(true)
        expect({ groupAlive: groupAlive(recovered.identity.pid), child: sameProcess(recovered.identity) }).toEqual({
          groupAlive: false,
          child: false,
        })

        yield* writeAtomically(exitFile, "exit")
        expect(yield* Effect.promise(() => processExited(leader))).toBe(true)
      }),
      12_000,
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
        yield* writeAtomically(file, "@echo off\r\nif %~1==--stdio exit /b 0\r\nexit /b 7\r\n")

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
