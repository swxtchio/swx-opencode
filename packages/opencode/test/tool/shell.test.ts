import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { EventTable } from "@opencode-ai/core/event/sql"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { and, asc, eq } from "drizzle-orm"
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Layer, Queue, Stream } from "effect"
import { adjust, layer, make } from "effect/testing/TestClock"
import type { TestClock } from "effect/testing/TestClock"
import { ChildProcessSpawner } from "effect/unstable/process"
import type * as Scope from "effect/Scope"
import os from "os"
import path from "path"
import { Config } from "@/config/config"
import { Shell } from "@opencode-ai/core/shell"
import { OUTPUT_WRITE_INTERVAL_MS, ShellTool } from "../../src/tool/shell"
import { Filesystem } from "@/util/filesystem"
import { provideInstance, provideTmpdirServer, testInstanceStoreLayer, tmpdirScoped } from "../fixture/fixture"
import { Permission } from "../../src/permission"
import { Agent } from "../../src/agent/agent"
import { Truncate } from "@/tool/truncate"
import { SessionID, MessageID } from "../../src/session/schema"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Plugin } from "../../src/plugin"
import { pollWithTimeout, testEffect } from "../lib/effect"
import { Tool } from "@/tool/tool"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceStore } from "@/project/instance-store"
import { Provider } from "@/provider/provider"
import { MCP } from "@/mcp"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Session } from "@/session/session"
import { MessageV2 } from "@/session/message-v2"
import { SessionProcessor } from "@/session/processor"
import { SessionStatus } from "@/session/status"
import { SessionSummary } from "@/session/summary"
import { SessionTools } from "@/session/tools"
import { ToolRegistry } from "@/tool/registry"
import { ShellID } from "@/tool/shell/id"
import { TestLLMServer } from "../lib/llm-server"

const shellLayer = Layer.mergeAll(
  LayerNode.compile(
    LayerNode.group([
      CrossSpawnSpawner.node,
      FSUtil.node,
      Plugin.node,
      Truncate.node,
      Config.node,
      Agent.node,
      RuntimeFlags.node,
    ]),
  ),
  testInstanceStoreLayer,
)
const it = testEffect(shellLayer)
type ShellTestServices =
  | (typeof shellLayer extends Layer.Layer<infer ROut, infer _E, infer _RIn> ? ROut : never)
  | InstanceStore.Service
  | Scope.Scope

const initShell = Effect.fn("ShellToolTest.init")(function* () {
  const info = yield* ShellTool
  return yield* info.init()
})

const initBash = initShell

const run = Effect.fn("ShellToolTest.run")(function* (
  args: Tool.InferParameters<typeof ShellTool>,
  next: Tool.Context = ctx,
) {
  const bash = yield* initShell()
  return yield* bash.execute(args, next)
})

const runIn = <A, E, R>(directory: string, self: Effect.Effect<A, E, R>) => self.pipe(provideInstance(directory))

const fail = Effect.fn("ShellToolTest.fail")(function* (
  args: Tool.InferParameters<typeof ShellTool>,
  next: Tool.Context = ctx,
) {
  const exit = yield* run(args, next).pipe(Effect.exit)
  if (Exit.isFailure(exit)) {
    const err = Cause.squash(exit.cause)
    return err instanceof Error ? err : new Error(String(err))
  }
  throw new Error("expected command to fail")
})

const ctx = {
  sessionID: SessionID.make("ses_test"),
  messageID: MessageID.make("msg_test"),
  callID: "",
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
}

Shell.acceptable.reset()
const quote = (text: string) => `"${text}"`
const squote = (text: string) => `'${text}'`
const projectRoot = path.join(__dirname, "../..")
const bin = quote(process.execPath.replaceAll("\\", "/"))
const bash = (() => {
  const shell = Shell.acceptable()
  if (Shell.name(shell) === "bash") return shell
  return Shell.gitbash()
})()
const shells = (() => {
  if (process.platform !== "win32") {
    const shell = Shell.acceptable()
    return [{ label: Shell.name(shell), shell }]
  }

  const list = [bash, Bun.which("pwsh"), Bun.which("powershell"), process.env.COMSPEC || Bun.which("cmd.exe")]
    .filter((shell): shell is string => Boolean(shell))
    .map((shell) => ({ label: Shell.name(shell), shell }))

  return list.filter(
    (item, i) => list.findIndex((other) => other.shell.toLowerCase() === item.shell.toLowerCase()) === i,
  )
})()
const PS = new Set(["pwsh", "powershell"])
const ps = shells.filter((item) => PS.has(item.label))
const cmdShell = shells.find((item) => item.label === "cmd")

const sh = () => Shell.name(Shell.acceptable())
const evalarg = (text: string) => (sh() === "cmd" ? quote(text) : squote(text))

const fill = (mode: "lines" | "bytes", n: number) => {
  const code =
    mode === "lines"
      ? "console.log(Array.from({length:Number(Bun.argv[1])},(_,i)=>i+1).join(String.fromCharCode(10)))"
      : "process.stdout.write(String.fromCharCode(97).repeat(Number(Bun.argv[1])))"
  const text = `${bin} -e ${evalarg(code)} ${n}`
  if (PS.has(sh())) return `& ${text}`
  return text
}

type StagedOutput = {
  command: string
  stages: string[]
  release: (index: number) => Effect.Effect<void>
}

type StagedInterval = { stageCount: number; adjustments: number[] }

const stagedOutput = Effect.fn("ShellToolTest.stagedOutput")(function* (directory: string, stages: string[]) {
  const scriptPath = path.join(directory, "staged-output.mjs")
  const script = [
    'import { watch } from "node:fs"',
    `const directory = ${JSON.stringify(directory)}`,
    `const stages = ${JSON.stringify(stages)}`,
    "const waitFor = (name) => new Promise((resolve) => {",
    "  const watcher = watch(directory, (_event, filename) => {",
    "    if (String(filename) !== name) return",
    "    watcher.close()",
    "    resolve()",
    "  })",
    "})",
    "for (let index = 0; index < stages.length; index++) {",
    "  const ready = waitFor(`release-${index}`)",
    "  process.stdout.write(stages[index])",
    "  await ready",
    "}",
  ].join("\n")
  yield* Effect.promise(() => Bun.write(scriptPath, script))
  return {
    command: `${PS.has(sh()) ? "& " : ""}${bin} ${evalarg(scriptPath)}`,
    stages,
    release: (index: number) =>
      Effect.promise(() => Bun.write(path.join(directory, `release-${index}`), "")).pipe(Effect.asVoid),
  } satisfies StagedOutput
})

const glob = (p: string) =>
  process.platform === "win32" ? Filesystem.normalizePathPattern(p) : p.replaceAll("\\", "/")

const forms = (dir: string) => {
  if (process.platform !== "win32") return [dir]
  const full = Filesystem.normalizePath(dir)
  const slash = full.replaceAll("\\", "/")
  const root = slash.replace(/^[A-Za-z]:/, "")
  return Array.from(new Set([full, slash, root, root.toLowerCase()]))
}

const withShell = <A, E, R>(item: { label: string; shell: string }, self: Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const prev = process.env.SHELL
      process.env.SHELL = item.shell
      Shell.acceptable.reset()
      Shell.preferred.reset()
      return prev
    }),
    () => self,
    (prev) =>
      Effect.sync(() => {
        if (prev === undefined) delete process.env.SHELL
        else process.env.SHELL = prev
        Shell.acceptable.reset()
        Shell.preferred.reset()
      }),
  )

const each = (
  name: string,
  fn: (item: { label: string; shell: string }) => Effect.Effect<void, unknown, ShellTestServices>,
) => {
  for (const item of shells) {
    it.live(`${name} [${item.label}]`, () => withShell(item, fn(item)))
  }
}

const capture = (requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">>, stop?: Error) => ({
  ...ctx,
  ask: (req: Omit<PermissionV1.Request, "id" | "sessionID" | "tool">) =>
    Effect.sync(() => {
      requests.push(req)
      if (stop) throw stop
    }),
})

const mustTruncate = (result: {
  metadata: { truncated?: boolean; exit?: number | null } & Record<string, unknown>
  output: string
}) => {
  if (result.metadata.truncated) return
  throw new Error(
    [`shell: ${process.env.SHELL || ""}`, `exit: ${String(result.metadata.exit)}`, "output:", result.output].join("\n"),
  )
}

type Chunk = { text: string }

// The queue entry follows the child chunk through ShellTool's runForEach callback, so waiters know persistence scheduling has run.
const tappedShell = Effect.fn("ShellToolTest.tappedShell")(function* (
  chunks: Chunk[],
  consumed?: Queue.Queue<string>,
  clock?: TestClock,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  const tapped = ChildProcessSpawner.make((command) =>
    spawner.spawn(command).pipe(
      Effect.map((handle) =>
        ChildProcessSpawner.makeHandle({
          ...handle,
          kill: (options) => (clock ? clock.withLive(handle.kill(options)) : handle.kill(options)),
          all: handle.all.pipe(
            Stream.flatMap((bytes) => {
              const text = Buffer.from(bytes).toString("utf-8")
              const stream = Stream.make(bytes).pipe(Stream.tap(() => Effect.sync(() => chunks.push({ text }))))
              if (!consumed) return stream
              return stream.pipe(Stream.concat(Stream.fromEffectDrain(Queue.offer(consumed, text).pipe(Effect.asVoid))))
            }),
          ),
        }),
      ),
    ),
  )
  const tool = yield* Tool.init(
    yield* ShellTool.pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, tapped)),
  )
  if (!clock) return tool
  return {
    ...tool,
    execute: (params: Tool.InferParameters<typeof ShellTool>, context: Tool.Context) =>
      tool.execute(params, context).pipe(Effect.provideService(Clock.Clock, clock)),
  }
})

const waitForConsumed = (
  queue: Queue.Queue<string>,
  output: { value: string },
  text: string,
  match: "exact" | "contains",
  message: string,
) =>
  Effect.gen(function* () {
    while (match === "exact" ? output.value.length < text.length : !output.value.includes(text)) {
      output.value += yield* Queue.take(queue)
    }
    if (match === "exact" && output.value !== text) return yield* Effect.fail(new Error(message))
    return output.value
  }).pipe(
    Effect.timeoutOrElse({
      duration: "10 seconds",
      orElse: () => Effect.fail(new Error(message)),
    }),
  )

describe("tool.shell", () => {
  each("basic", () =>
    runIn(
      projectRoot,
      Effect.gen(function* () {
        const result = yield* run({
          command: "echo test",
        })
        expect(result.metadata.exit).toBe(0)
        expect(result.metadata.output).toContain("test")
      }),
    ),
  )

  it.live("falls back from terminal-only configured shell", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped({ config: { shell: "fish" } })
      yield* runIn(
        tmp,
        Effect.gen(function* () {
          const bash = yield* initBash()
          const fallback = Shell.name(Shell.acceptable("fish"))
          expect(fallback).not.toBe("fish")
          expect(bash.description).toContain(fallback)

          const result = yield* bash.execute(
            {
              command: "echo fallback",
            },
            ctx,
          )
          expect(result.metadata.exit).toBe(0)
          expect(result.output).toContain("fallback")
        }),
      )
    }),
  )
})

describe("tool.shell permissions", () => {
  each("asks for bash permission with correct pattern", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      yield* runIn(
        tmp,
        Effect.gen(function* () {
          const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
          yield* run(
            {
              command: "echo hello",
            },
            capture(requests),
          )
          expect(requests.length).toBe(1)
          expect(requests[0].permission).toBe("bash")
          expect(requests[0].patterns).toContain("echo hello")
        }),
      )
    }),
  )

  each("asks for bash permission with multiple commands", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      yield* runIn(
        tmp,
        Effect.gen(function* () {
          const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
          yield* run(
            {
              command: "echo foo && echo bar",
            },
            capture(requests),
          )
          expect(requests.length).toBe(1)
          expect(requests[0].permission).toBe("bash")
          expect(requests[0].patterns).toContain("echo foo")
          expect(requests[0].patterns).toContain("echo bar")
        }),
      )
    }),
  )

  for (const item of ps) {
    it.live(`parses PowerShell conditionals for permission prompts [${item.label}]`, () =>
      withShell(
        item,
        runIn(
          projectRoot,
          Effect.gen(function* () {
            const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
            yield* run(
              {
                command: "Write-Host foo; if ($?) { Write-Host bar }",
              },
              capture(requests),
            )
            const bashReq = requests.find((r) => r.permission === "bash")
            expect(bashReq).toBeDefined()
            expect(bashReq!.patterns).toContain("Write-Host foo")
            expect(bashReq!.patterns).toContain("Write-Host bar")
            expect(bashReq!.always).toContain("Write-Host *")
          }),
        ),
      ),
    )
  }

  for (const item of ps) {
    it.live(`uses PowerShell cmdlet prefixes for always-allow prompts [${item.label}]`, () =>
      withShell(
        item,
        Effect.gen(function* () {
          const tmp = yield* tmpdirScoped()
          yield* runIn(
            tmp,
            Effect.gen(function* () {
              const err = new Error("stop after permission")
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              expect(
                yield* fail(
                  {
                    command: "Remove-Item -Recurse tmp",
                  },
                  capture(requests, err),
                ),
              ).toMatchObject({ message: err.message })
              const bashReq = requests.find((r) => r.permission === "bash")
              expect(bashReq).toBeDefined()
              expect(bashReq!.always).toContain("Remove-Item *")
              expect(bashReq!.always).not.toContain("Remove-Item -Recurse *")
            }),
          )
        }),
      ),
    )
  }

  each("asks for external_directory permission for wildcard external paths", () =>
    runIn(
      projectRoot,
      Effect.gen(function* () {
        const err = new Error("stop after permission")
        const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
        const file = process.platform === "win32" ? `${process.env.WINDIR!.replaceAll("\\", "/")}/*` : "/etc/*"
        const want = process.platform === "win32" ? glob(path.join(process.env.WINDIR!, "*")) : "/etc/*"
        expect(
          yield* fail(
            {
              command: `cat ${file}`,
            },
            capture(requests, err),
          ),
        ).toMatchObject({ message: err.message })
        const extDirReq = requests.find((r) => r.permission === "external_directory")
        expect(extDirReq).toBeDefined()
        expect(extDirReq!.patterns).toContain(want)
      }),
    ),
  )

  if (process.platform === "win32") {
    if (bash) {
      it.live("asks for nested bash command permissions [bash]", () =>
        withShell(
          { label: "bash", shell: bash },
          Effect.gen(function* () {
            const outerTmp = yield* tmpdirScoped()
            yield* Effect.promise(() => Bun.write(path.join(outerTmp, "outside.txt"), "x"))
            yield* runIn(
              projectRoot,
              Effect.gen(function* () {
                const file = path.join(outerTmp, "outside.txt").replaceAll("\\", "/")
                const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
                yield* run(
                  {
                    command: `echo $(cat "${file}")`,
                  },
                  capture(requests),
                )
                const extDirReq = requests.find((r) => r.permission === "external_directory")
                const bashReq = requests.find((r) => r.permission === "bash")
                expect(extDirReq).toBeDefined()
                expect(extDirReq!.patterns).toContain(glob(path.join(outerTmp, "*")))
                expect(bashReq).toBeDefined()
                expect(bashReq!.patterns).toContain(`cat "${file}"`)
              }),
            )
          }),
        ),
      )
    }

    for (const item of ps) {
      it.live(`asks for external_directory permission for PowerShell paths after switches [${item.label}]`, () =>
        withShell(
          item,
          runIn(
            projectRoot,
            Effect.gen(function* () {
              const err = new Error("stop after permission")
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              expect(
                yield* fail(
                  {
                    command: `Copy-Item -PassThru "${process.env.WINDIR!.replaceAll("\\", "/")}/win.ini" ./out`,
                  },
                  capture(requests, err),
                ),
              ).toMatchObject({ message: err.message })
              const extDirReq = requests.find((r) => r.permission === "external_directory")
              expect(extDirReq).toBeDefined()
              expect(extDirReq!.patterns).toContain(glob(path.join(process.env.WINDIR!, "*")))
            }),
          ),
        ),
      )
    }

    for (const item of ps) {
      it.live(`asks for nested PowerShell command permissions [${item.label}]`, () =>
        withShell(
          item,
          runIn(
            projectRoot,
            Effect.gen(function* () {
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              const file = `${process.env.WINDIR!.replaceAll("\\", "/")}/win.ini`
              yield* run(
                {
                  command: `Write-Output $(Get-Content ${file})`,
                },
                capture(requests),
              )
              const extDirReq = requests.find((r) => r.permission === "external_directory")
              const bashReq = requests.find((r) => r.permission === "bash")
              expect(extDirReq).toBeDefined()
              expect(extDirReq!.patterns).toContain(glob(path.join(process.env.WINDIR!, "*")))
              expect(bashReq).toBeDefined()
              expect(bashReq!.patterns).toContain(`Get-Content ${file}`)
            }),
          ),
        ),
      )
    }

    for (const item of ps) {
      it.live(`asks for external_directory permission for drive-relative PowerShell paths [${item.label}]`, () =>
        withShell(
          item,
          Effect.gen(function* () {
            const tmp = yield* tmpdirScoped()
            yield* runIn(
              tmp,
              Effect.gen(function* () {
                const err = new Error("stop after permission")
                const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
                expect(
                  yield* fail(
                    {
                      command: 'Get-Content "C:../outside.txt"',
                    },
                    capture(requests, err),
                  ),
                ).toMatchObject({ message: err.message })
                expect(requests[0]?.permission).toBe("external_directory")
                if (requests[0]?.permission !== "external_directory") return
                expect(requests[0].patterns).toContain(glob(path.join(path.dirname(tmp), "*")))
              }),
            )
          }),
        ),
      )
    }

    for (const item of ps) {
      it.live(`asks for external_directory permission for $HOME PowerShell paths [${item.label}]`, () =>
        withShell(
          item,
          runIn(
            projectRoot,
            Effect.gen(function* () {
              const err = new Error("stop after permission")
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              expect(
                yield* fail(
                  {
                    command: 'Get-Content "$HOME/.ssh/config"',
                  },
                  capture(requests, err),
                ),
              ).toMatchObject({ message: err.message })
              expect(requests[0]?.permission).toBe("external_directory")
              if (requests[0]?.permission !== "external_directory") return
              expect(requests[0].patterns).toContain(glob(path.join(os.homedir(), ".ssh", "*")))
            }),
          ),
        ),
      )
    }

    for (const item of ps) {
      it.live(`asks for external_directory permission for $PWD PowerShell paths [${item.label}]`, () =>
        withShell(
          item,
          Effect.gen(function* () {
            const tmp = yield* tmpdirScoped()
            yield* runIn(
              tmp,
              Effect.gen(function* () {
                const err = new Error("stop after permission")
                const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
                expect(
                  yield* fail(
                    {
                      command: 'Get-Content "$PWD/../outside.txt"',
                    },
                    capture(requests, err),
                  ),
                ).toMatchObject({ message: err.message })
                expect(requests[0]?.permission).toBe("external_directory")
                if (requests[0]?.permission !== "external_directory") return
                expect(requests[0].patterns).toContain(glob(path.join(path.dirname(tmp), "*")))
              }),
            )
          }),
        ),
      )
    }

    for (const item of ps) {
      it.live(`asks for external_directory permission for $PSHOME PowerShell paths [${item.label}]`, () =>
        withShell(
          item,
          runIn(
            projectRoot,
            Effect.gen(function* () {
              const err = new Error("stop after permission")
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              expect(
                yield* fail(
                  {
                    command: 'Get-Content "$PSHOME/outside.txt"',
                  },
                  capture(requests, err),
                ),
              ).toMatchObject({ message: err.message })
              expect(requests[0]?.permission).toBe("external_directory")
              if (requests[0]?.permission !== "external_directory") return
              expect(requests[0].patterns).toContain(glob(path.join(path.dirname(item.shell), "*")))
            }),
          ),
        ),
      )
    }

    for (const item of ps) {
      it.live(`asks for external_directory permission for missing PowerShell env paths [${item.label}]`, () =>
        withShell(
          item,
          Effect.acquireUseRelease(
            Effect.sync(() => {
              const key = "OPENCODE_TEST_MISSING"
              const prev = process.env[key]
              delete process.env[key]
              return { key, prev }
            }),
            ({ key }) =>
              runIn(
                projectRoot,
                Effect.gen(function* () {
                  const err = new Error("stop after permission")
                  const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
                  const root = path.parse(process.env.WINDIR!).root.replace(/[\\/]+$/, "")
                  expect(
                    yield* fail(
                      {
                        command: `Get-Content -Path "${root}$env:${key}\\Windows\\win.ini"`,
                      },
                      capture(requests, err),
                    ),
                  ).toMatchObject({ message: err.message })
                  const extDirReq = requests.find((r) => r.permission === "external_directory")
                  expect(extDirReq).toBeDefined()
                  expect(extDirReq!.patterns).toContain(glob(path.join(process.env.WINDIR!, "*")))
                }),
              ),
            ({ key, prev }) =>
              Effect.sync(() => {
                if (prev === undefined) delete process.env[key]
                else process.env[key] = prev
              }),
          ),
        ),
      )
    }

    for (const item of ps) {
      it.live(`asks for external_directory permission for PowerShell env paths [${item.label}]`, () =>
        withShell(
          item,
          runIn(
            projectRoot,
            Effect.gen(function* () {
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              yield* run(
                {
                  command: "Get-Content $env:WINDIR/win.ini",
                },
                capture(requests),
              )
              const extDirReq = requests.find((r) => r.permission === "external_directory")
              expect(extDirReq).toBeDefined()
              expect(extDirReq!.patterns).toContain(
                Filesystem.normalizePathPattern(path.join(process.env.WINDIR!, "*")),
              )
            }),
          ),
        ),
      )
    }

    for (const item of ps) {
      it.live(`asks for external_directory permission for PowerShell FileSystem paths [${item.label}]`, () =>
        withShell(
          item,
          runIn(
            projectRoot,
            Effect.gen(function* () {
              const err = new Error("stop after permission")
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              expect(
                yield* fail(
                  {
                    command: `Get-Content -Path FileSystem::${process.env.WINDIR!.replaceAll("\\", "/")}/win.ini`,
                  },
                  capture(requests, err),
                ),
              ).toMatchObject({ message: err.message })
              expect(requests[0]?.permission).toBe("external_directory")
              if (requests[0]?.permission !== "external_directory") return
              expect(requests[0].patterns).toContain(
                Filesystem.normalizePathPattern(path.join(process.env.WINDIR!, "*")),
              )
            }),
          ),
        ),
      )
    }

    for (const item of ps) {
      it.live(`asks for external_directory permission for braced PowerShell env paths [${item.label}]`, () =>
        withShell(
          item,
          runIn(
            projectRoot,
            Effect.gen(function* () {
              const err = new Error("stop after permission")
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              expect(
                yield* fail(
                  {
                    command: "Get-Content ${env:WINDIR}/win.ini",
                  },
                  capture(requests, err),
                ),
              ).toMatchObject({ message: err.message })
              expect(requests[0]?.permission).toBe("external_directory")
              if (requests[0]?.permission !== "external_directory") return
              expect(requests[0].patterns).toContain(
                Filesystem.normalizePathPattern(path.join(process.env.WINDIR!, "*")),
              )
            }),
          ),
        ),
      )
    }

    for (const item of ps) {
      it.live(`treats Set-Location like cd for permissions [${item.label}]`, () =>
        withShell(
          item,
          runIn(
            projectRoot,
            Effect.gen(function* () {
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              yield* run(
                {
                  command: "Set-Location C:/Windows",
                },
                capture(requests),
              )
              const extDirReq = requests.find((r) => r.permission === "external_directory")
              const bashReq = requests.find((r) => r.permission === "bash")
              expect(extDirReq).toBeDefined()
              expect(extDirReq!.patterns).toContain(
                Filesystem.normalizePathPattern(path.join(process.env.WINDIR!, "*")),
              )
              expect(bashReq).toBeUndefined()
            }),
          ),
        ),
      )
    }

    for (const item of ps) {
      it.live(`does not add nested PowerShell expressions to permission prompts [${item.label}]`, () =>
        withShell(
          item,
          runIn(
            projectRoot,
            Effect.gen(function* () {
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              yield* run(
                {
                  command: "Write-Output ('a' * 3)",
                },
                capture(requests),
              )
              const bashReq = requests.find((r) => r.permission === "bash")
              expect(bashReq).toBeDefined()
              expect(bashReq!.patterns).not.toContain("a * 3")
              expect(bashReq!.always).not.toContain("a *")
            }),
          ),
        ),
      )
    }
  }

  if (process.platform === "win32" && cmdShell) {
    it.live("asks for external_directory permission for cmd file commands [cmd]", () =>
      withShell(
        cmdShell,
        runIn(
          projectRoot,
          Effect.gen(function* () {
            const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
            yield* run(
              {
                command: `TYPE "${path.join(process.env.WINDIR!, "win.ini")}"`,
              },
              capture(requests),
            )
            const extDirReq = requests.find((r) => r.permission === "external_directory")
            expect(extDirReq).toBeDefined()
            expect(extDirReq!.patterns).toContain(Filesystem.normalizePathPattern(path.join(process.env.WINDIR!, "*")))
          }),
        ),
      ),
    )
  }

  each("asks for external_directory permission when cd to parent", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      yield* runIn(
        tmp,
        Effect.gen(function* () {
          const err = new Error("stop after permission")
          const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
          expect(
            yield* fail(
              {
                command: "cd ../",
              },
              capture(requests, err),
            ),
          ).toMatchObject({ message: err.message })
          const extDirReq = requests.find((r) => r.permission === "external_directory")
          expect(extDirReq).toBeDefined()
        }),
      )
    }),
  )

  each("asks for external_directory permission when workdir is outside project", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      yield* runIn(
        tmp,
        Effect.gen(function* () {
          const err = new Error("stop after permission")
          const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
          expect(
            yield* fail(
              {
                command: "echo ok",
                workdir: os.tmpdir(),
              },
              capture(requests, err),
            ),
          ).toMatchObject({ message: err.message })
          const extDirReq = requests.find((r) => r.permission === "external_directory")
          expect(extDirReq).toBeDefined()
          expect(extDirReq!.patterns).toContain(glob(path.join(os.tmpdir(), "*")))
        }),
      )
    }),
  )

  if (process.platform === "win32") {
    it.live("normalizes external_directory workdir variants on Windows", () =>
      Effect.gen(function* () {
        const err = new Error("stop after permission")
        const outerTmp = yield* tmpdirScoped()
        const tmp = yield* tmpdirScoped()
        yield* runIn(
          tmp,
          Effect.gen(function* () {
            const want = Filesystem.normalizePathPattern(path.join(outerTmp, "*"))

            for (const dir of forms(outerTmp)) {
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              expect(
                yield* fail(
                  {
                    command: "echo ok",
                    workdir: dir,
                  },
                  capture(requests, err),
                ),
              ).toMatchObject({ message: err.message })

              const extDirReq = requests.find((r) => r.permission === "external_directory")
              expect({ dir, patterns: extDirReq?.patterns, always: extDirReq?.always }).toEqual({
                dir,
                patterns: [want],
                always: [want],
              })
            }
          }),
        )
      }),
    )

    if (bash) {
      it.live("uses Git Bash /tmp semantics for external workdir", () =>
        withShell(
          { label: "bash", shell: bash },
          runIn(
            projectRoot,
            Effect.gen(function* () {
              const err = new Error("stop after permission")
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              const want = glob(path.join(os.tmpdir(), "*"))
              expect(
                yield* fail(
                  {
                    command: "echo ok",
                    workdir: "/tmp",
                  },
                  capture(requests, err),
                ),
              ).toMatchObject({ message: err.message })
              expect(requests[0]).toMatchObject({
                permission: "external_directory",
                patterns: [want],
                always: [want],
              })
            }),
          ),
        ),
      )

      it.live("uses Git Bash /tmp semantics for external file paths", () =>
        withShell(
          { label: "bash", shell: bash },
          runIn(
            projectRoot,
            Effect.gen(function* () {
              const err = new Error("stop after permission")
              const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
              const want = glob(path.join(os.tmpdir(), "*"))
              expect(
                yield* fail(
                  {
                    command: "cat /tmp/opencode-does-not-exist",
                  },
                  capture(requests, err),
                ),
              ).toMatchObject({ message: err.message })
              expect(requests[0]).toMatchObject({
                permission: "external_directory",
                patterns: [want],
                always: [want],
              })
            }),
          ),
        ),
      )
    }
  }

  each("asks for external_directory permission when file arg is outside project", () =>
    Effect.gen(function* () {
      const outerTmp = yield* tmpdirScoped()
      yield* Effect.promise(() => Bun.write(path.join(outerTmp, "outside.txt"), "x"))
      const tmp = yield* tmpdirScoped()
      yield* runIn(
        tmp,
        Effect.gen(function* () {
          const err = new Error("stop after permission")
          const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
          const filepath = path.join(outerTmp, "outside.txt")
          expect(
            yield* fail(
              {
                command: `cat ${filepath}`,
              },
              capture(requests, err),
            ),
          ).toMatchObject({ message: err.message })
          const extDirReq = requests.find((r) => r.permission === "external_directory")
          const expected = glob(path.join(outerTmp, "*"))
          expect(extDirReq).toBeDefined()
          expect(extDirReq!.patterns).toContain(expected)
          expect(extDirReq!.always).toContain(expected)
          expect(extDirReq!.metadata).toMatchObject({
            command: `cat ${filepath}`,
            directories: [outerTmp],
            patterns: [expected],
          })
        }),
      )
    }),
  )

  each("does not ask for external_directory permission when rm inside project", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      yield* Effect.promise(() => Bun.write(path.join(tmp, "tmpfile"), "x"))
      yield* runIn(
        tmp,
        Effect.gen(function* () {
          const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
          yield* run(
            {
              command: `rm -rf ${path.join(tmp, "nested")}`,
            },
            capture(requests),
          )
          const extDirReq = requests.find((r) => r.permission === "external_directory")
          expect(extDirReq).toBeUndefined()
        }),
      )
    }),
  )

  each("includes always patterns for auto-approval", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      yield* runIn(
        tmp,
        Effect.gen(function* () {
          const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
          yield* run(
            {
              command: "git log --oneline -5",
            },
            capture(requests),
          )
          expect(requests.length).toBe(1)
          expect(requests[0].always.length).toBeGreaterThan(0)
          expect(requests[0].always.some((item) => item.endsWith("*"))).toBe(true)
        }),
      )
    }),
  )

  each("does not ask for bash permission when command is cd only", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      yield* runIn(
        tmp,
        Effect.gen(function* () {
          const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
          yield* run(
            {
              command: "cd .",
            },
            capture(requests),
          )
          const bashReq = requests.find((r) => r.permission === "bash")
          expect(bashReq).toBeUndefined()
        }),
      )
    }),
  )

  each("matches redirects in permission pattern", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      yield* runIn(
        tmp,
        Effect.gen(function* () {
          const err = new Error("stop after permission")
          const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
          expect(yield* fail({ command: "echo test > output.txt" }, capture(requests, err))).toMatchObject({
            message: err.message,
          })
          const bashReq = requests.find((r) => r.permission === "bash")
          expect(bashReq).toBeDefined()
          expect(bashReq!.patterns).toContain("echo test > output.txt")
        }),
      )
    }),
  )

  each("always pattern has space before wildcard to not include different commands", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      yield* runIn(
        tmp,
        Effect.gen(function* () {
          const requests: Array<Omit<PermissionV1.Request, "id" | "sessionID" | "tool">> = []
          yield* run({ command: "ls -la" }, capture(requests))
          const bashReq = requests.find((r) => r.permission === "bash")
          expect(bashReq).toBeDefined()
          expect(bashReq!.always[0]).toBe("ls *")
        }),
      )
    }),
  )
})

describe("tool.shell abort", () => {
  it.live(
    "preserves output when aborted",
    () =>
      runIn(
        projectRoot,
        Effect.gen(function* () {
          const controller = new AbortController()
          const collected: string[] = []
          const res = yield* run(
            {
              command: `echo before && sleep 30`,
            },
            {
              ...ctx,
              abort: controller.signal,
              metadata: (input) =>
                Effect.sync(() => {
                  const output = (input.metadata as { output?: string })?.output
                  if (output && output.includes("before") && !controller.signal.aborted) {
                    collected.push(output)
                    controller.abort()
                  }
                }),
            },
          )
          expect(res.output).toContain("before")
          expect(res.output).toContain("User aborted the command")
          expect(collected.length).toBeGreaterThan(0)
        }),
      ),
    15_000,
  )

  it.live(
    "terminates command on timeout",
    () =>
      runIn(
        projectRoot,
        Effect.gen(function* () {
          const result = yield* run({
            command: `sleep 60`,
            timeout: 500,
          })
          expect(result.output).toContain("shell tool terminated command after exceeding timeout")
          expect(result.output).toContain("retry with a larger timeout value in milliseconds")
        }),
      ),
    15_000,
  )

  it.live(
    "uses RuntimeFlags bashDefaultTimeoutMs when timeout is omitted",
    () =>
      runIn(
        projectRoot,
        Effect.gen(function* () {
          const tool = yield* initShell()
          expect(tool.description).toContain("commands will time out after 500ms")
          const result = yield* tool.execute(
            {
              command: `sleep 60`,
            },
            ctx,
          )
          expect(result.output).toContain("exceeding timeout 500 ms")
        }),
      ).pipe(Effect.provide(RuntimeFlags.layer({ bashDefaultTimeoutMs: 500 }))),
    15_000,
  )

  if (process.platform !== "win32") {
    it.live("captures stderr in output", () =>
      runIn(
        projectRoot,
        Effect.gen(function* () {
          const result = yield* run({
            command: `echo stdout_msg && echo stderr_msg >&2`,
          })
          expect(result.output).toContain("stdout_msg")
          expect(result.output).toContain("stderr_msg")
          expect(result.metadata.exit).toBe(0)
        }),
      ),
    )
  }

  it.live("returns non-zero exit code", () =>
    runIn(
      projectRoot,
      Effect.gen(function* () {
        const result = yield* run({
          command: `exit 42`,
        })
        expect(result.metadata.exit).toBe(42)
      }),
    ),
  )

  it.live("streams metadata updates progressively", () =>
    runIn(
      projectRoot,
      Effect.gen(function* () {
        const tmp = yield* tmpdirScoped()
        const staged = yield* stagedOutput(tmp, ["first\n", "second\n"])
        const chunks: Chunk[] = []
        const shell = yield* tappedShell(chunks)
        const updates: string[] = []
        const first = Deferred.makeUnsafe<string>()
        const second = Deferred.makeUnsafe<string>()
        const settled = { value: false }
        const expected = staged.stages.join("")
        // The child cannot reach its next output or exit until the preceding metadata stage is observed.
        const execution = yield* Effect.gen(function* () {
          const result = yield* shell.execute(
            { command: staged.command },
            {
              ...ctx,
              metadata: (input) =>
                Effect.gen(function* () {
                  const output = String((input.metadata as { output?: string })?.output ?? "")
                  if (!output) return
                  updates.push(output)
                  if (output === staged.stages[0]) yield* Deferred.succeed(first, output)
                  if (output === expected) yield* Deferred.succeed(second, output)
                }),
            },
          )
          settled.value = true
          return result
        }).pipe(Effect.forkChild)

        const waitForMetadata = (signal: typeof first, stage: string) =>
          Deferred.await(signal).pipe(
            Effect.timeoutOrElse({
              duration: "10 seconds",
              orElse: () =>
                Effect.fail(
                  new Error(
                    `${stage} shell metadata stage was not published; consumed ${chunks.map((chunk) => chunk.text).join("")}; metadata ${updates.join(" | ")}`,
                  ),
                ),
            }),
          )

        expect(yield* waitForMetadata(first, "first")).toBe(staged.stages[0])
        expect(updates).toEqual([staged.stages[0]])
        expect(settled.value).toBe(false)
        yield* staged.release(0)

        expect(yield* waitForMetadata(second, "second")).toBe(expected)
        expect(updates).toEqual([staged.stages[0], expected])
        expect(settled.value).toBe(false)
        yield* staged.release(1)

        const result = yield* Fiber.join(execution)
        expect(settled.value).toBe(true)
        expect(result.output).toBe(expected)
        expect(result.metadata.output).toBe(expected)
        expect(updates).toEqual([staged.stages[0], expected])
      }),
    ),
  )

  if (process.platform !== "win32") {
    it.live("writes the unwritten tail when the run is interrupted", () =>
      runIn(
        projectRoot,
        Effect.gen(function* () {
          const chunks: Chunk[] = []
          const consumed = yield* Queue.unbounded<string>()
          const downstream = { value: "" }
          const testClock = yield* make()
          const shell = yield* tappedShell(chunks, consumed, testClock)
          const updates: string[] = []
          const running = yield* shell
            .execute(
              { command: "echo interrupted-tail; sleep 30" },
              {
                ...ctx,
                metadata: (input) =>
                  Effect.sync(() => void updates.push(String((input.metadata as { output?: string })?.output ?? ""))),
              },
            )
            .pipe(Effect.forkChild)
          yield* waitForConsumed(
            consumed,
            downstream,
            "interrupted-tail",
            "contains",
            "ShellTool did not consume interrupted-tail before the run was interrupted",
          )
          yield* testClock.adjust(OUTPUT_WRITE_INTERVAL_MS - 1)
          expect(updates).not.toContain("interrupted-tail\n")
          yield* Fiber.interrupt(running)
          expect(updates.at(-1)).toBe("interrupted-tail\n")
        }),
      ),
    )
  }
})

// A shell whose command ignores SIGTERM, so only a SIGKILL ends it. The tool runs through a spawner that signals once
// it has consumed the command's first output, by which point the command has recorded its PID.
const sigtermResistant = Effect.fn("ShellToolTest.sigtermResistant")(function* () {
  const dir = yield* tmpdirScoped()
  const pidFile = path.join(dir, "pid")
  const consumed = Deferred.makeUnsafe<void>()
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  const signalling = ChildProcessSpawner.make((command) =>
    spawner.spawn(command).pipe(
      Effect.map((handle) =>
        ChildProcessSpawner.makeHandle({
          ...handle,
          all: handle.all.pipe(
            Stream.flatMap((chunk) =>
              Stream.make(chunk).pipe(Stream.concat(Stream.fromEffectDrain(Deferred.succeed(consumed, undefined)))),
            ),
          ),
        }),
      ),
    ),
  )
  const tool = yield* Tool.init(
    yield* ShellTool.pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, signalling)),
  )
  const program = `process.on("SIGTERM", () => {}); require("fs").writeFileSync(${JSON.stringify(pidFile)}, String(process.pid)); console.log("ready"); setInterval(() => {}, 1000)`
  const pid = Effect.promise(() => Bun.file(pidFile).text()).pipe(Effect.map(Number))
  // Reaps the command whatever the case leaves behind, so a run held open by it can end.
  yield* Effect.addFinalizer(() =>
    pid.pipe(
      Effect.flatMap((value) =>
        Effect.sync(() => {
          for (const target of [-value, value])
            try {
              process.kill(target, "SIGKILL")
            } catch {}
        }),
      ),
      Effect.ignore,
    ),
  )
  return {
    tool,
    consumed,
    command: `exec ${bin} -e ${squote(program)}`,
    alive: pid.pipe(
      Effect.map((value) => {
        try {
          process.kill(value, 0)
          return true
        } catch {
          return false
        }
      }),
    ),
  }
})

// Refuses every progress write once refusing is set, as SessionTools does for a call it no longer owns.
const refusing = (input: { abort: AbortSignal; refuse: () => boolean; refused: { count: number } }) => ({
  ...ctx,
  callID: "refused",
  abort: input.abort,
  metadata: () =>
    Effect.suspend(() => {
      if (!input.refuse()) return Effect.void
      input.refused.count++
      return Effect.die(new Tool.CallNotOwnedError({ callID: "refused" }))
    }),
})

describe("tool.shell teardown", () => {
  const unix = process.platform === "win32" ? it.live.skip : it.live

  unix(
    "reaps a command that ignores SIGTERM when the abort's output write is refused",
    () =>
      runIn(
        projectRoot,
        Effect.gen(function* () {
          const shell = yield* sigtermResistant()
          const controller = new AbortController()
          const refused = { count: 0 }
          const execution = yield* shell.tool
            .execute(
              { command: shell.command },
              refusing({ abort: controller.signal, refuse: () => controller.signal.aborted, refused }),
            )
            .pipe(Effect.exit, Effect.forkDetach)
          yield* Deferred.await(shell.consumed).pipe(Effect.timeout("5 seconds"))
          controller.abort()
          const completion = yield* Fiber.await(execution).pipe(Effect.timeoutOption("6 seconds"))
          expect({
            completion: completion._tag === "Some" ? "finished" : "timeout",
            alive: yield* shell.alive,
          }).toEqual({
            completion: "finished",
            alive: false,
          })
          // The abort's write of the consumed output was the one refused.
          expect(refused.count).toBeGreaterThan(0)
        }),
      ),
    25_000,
  )

  unix(
    "reaps a command that ignores SIGTERM when its run is interrupted and its output write is refused",
    () =>
      runIn(
        projectRoot,
        Effect.gen(function* () {
          const shell = yield* sigtermResistant()
          let interrupting = false
          const refused = { count: 0 }
          const execution = yield* shell.tool
            .execute(
              { command: shell.command },
              refusing({ abort: new AbortController().signal, refuse: () => interrupting, refused }),
            )
            .pipe(Effect.forkDetach)
          yield* Deferred.await(shell.consumed).pipe(Effect.timeout("5 seconds"))
          interrupting = true
          const completion = yield* Fiber.interrupt(execution).pipe(Effect.timeoutOption("6 seconds"))
          expect({
            completion: completion._tag === "Some" ? "finished" : "timeout",
            alive: yield* shell.alive,
          }).toEqual({
            completion: "finished",
            alive: false,
          })
          expect(refused.count).toBeGreaterThan(0)
        }),
      ),
    25_000,
  )
})

describe("tool.shell truncation", () => {
  it.live("truncates output exceeding line limit", () =>
    runIn(
      projectRoot,
      Effect.gen(function* () {
        const lineCount = Truncate.MAX_LINES + 500
        const result = yield* run({
          command: fill("lines", lineCount),
        })
        mustTruncate(result)
        expect(result.output).toMatch(/\.\.\.output truncated\.\.\./)
        expect(result.output).toMatch(/Full output saved to:\s+\S+/)
      }),
    ),
  )

  it.live("truncates output exceeding byte limit", () =>
    runIn(
      projectRoot,
      Effect.gen(function* () {
        const byteCount = Truncate.MAX_BYTES + 10000
        const result = yield* run({
          command: fill("bytes", byteCount),
        })
        mustTruncate(result)
        expect(result.output).toMatch(/\.\.\.output truncated\.\.\./)
        expect(result.output).toMatch(/Full output saved to:\s+\S+/)
      }),
    ),
  )

  it.live("does not truncate small output", () =>
    runIn(
      projectRoot,
      Effect.gen(function* () {
        const result = yield* run({
          command: fill("lines", 1),
        })
        expect((result.metadata as { truncated?: boolean }).truncated).toBe(false)
        expect(result.output).toContain("1")
      }),
    ),
  )

  it.live("full output is saved to file when truncated", () =>
    runIn(
      projectRoot,
      Effect.gen(function* () {
        const lineCount = Truncate.MAX_LINES + 100
        const result = yield* run({
          command: fill("lines", lineCount),
        })
        mustTruncate(result)

        const filepath = (result.metadata as { outputPath?: string }).outputPath
        expect(filepath).toBeTruthy()

        const saved = yield* (yield* FSUtil.Service).readFileString(filepath!)
        const lines = saved.trim().split(/\r?\n/)
        expect(lines.length).toBe(lineCount)
        expect(lines[0]).toBe("1")
        expect(lines[lineCount - 1]).toBe(String(lineCount))
      }),
    ),
  )
})

// Running output reaches the Session store through the production path: SessionTools' tool context, the processor's
// updateToolCall and Session.updatePart, where each part update is one durable event. The spawner is tapped so a test
// sees the chunks the shell actually consumed, since the OS may merge several child writes into one read.
const storeLayer = Layer.mergeAll(
  LayerNode.compile(
    LayerNode.group([
      SessionProcessor.node,
      Session.node,
      SessionProjector.node,
      Provider.node,
      Database.node,
      EventV2Bridge.node,
      SessionStatus.node,
      CrossSpawnSpawner.node,
      FSUtil.node,
      Plugin.node,
      Truncate.node,
      Config.node,
      Agent.node,
      RuntimeFlags.node,
      LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] }),
    ]),
    [
      [
        SessionSummary.node,
        Layer.succeed(
          SessionSummary.Service,
          SessionSummary.Service.of({
            summarize: () => Effect.void,
            diff: () => Effect.succeed([]),
            computeDiff: () => Effect.succeed([]),
          }),
        ),
      ],
    ],
  ),
  testInstanceStoreLayer,
)
const itStore = testEffect(storeLayer)

const storeRef = { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") }
const storeAgent: Agent.Info = {
  name: "build",
  mode: "primary",
  options: {},
  permission: [{ permission: "*", pattern: "*", action: "allow" }],
}
const storeConfig = (url: string) => ({
  provider: {
    test: {
      name: "Test",
      id: "test",
      env: [],
      npm: "@ai-sdk/openai-compatible",
      models: {
        "test-model": {
          id: "test-model",
          name: "Test Model",
          attachment: false,
          reasoning: false,
          temperature: false,
          tool_call: true,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: { apiKey: "test-key", baseURL: url },
    },
  },
})

type ToolWrite = { part: SessionV1.ToolPart; time: number }
type RunningOutputObservation = {
  intervalIndex: number
  adjustmentIndex: number
  expected: string
  previous: string
  part?: SessionV1.ToolPart
  chunks: string[]
  settled: boolean
  snapshots: ToolWrite[]
}

const readToolWrites = Effect.fn("ShellToolTest.readToolWrites")(function* (sessionID: SessionID) {
  const database = yield* Database.Service
  const updates = yield* database.db
    .select({ data: EventTable.data })
    .from(EventTable)
    .where(
      and(
        eq(EventTable.aggregate_id, sessionID),
        eq(
          EventTable.type,
          EventV2.versionedType(SessionV1.Event.PartUpdated.type, SessionV1.Event.PartUpdated.durable!.version),
        ),
      ),
    )
    .orderBy(asc(EventTable.seq))
    .all()
    .pipe(Effect.orDie)
  return updates
    .map((row) => row.data as { part: SessionV1.Part; time: number })
    .filter((data): data is ToolWrite => data.part.type === "tool")
})

// Runs one shell tool call through a real Session and returns its durable part updates. `interruptOn` waits for a
// consumed output marker; `staged` keeps the process quiet until its running output is readable from the store.
const throughSession = Effect.fn("ShellToolTest.throughSession")(function* (input: {
  dir: string
  llm: TestLLMServer["Service"]
  command: string
  timeout?: number
  interruptOn?: string
  staged?: StagedOutput
  stagedIntervals?: StagedInterval[]
  clocked?: boolean
  checkUnwritten?: (input: { part?: SessionV1.ToolPart; snapshots: ToolWrite[]; chunks: string[] }) => void
  checkRunning?: (input: RunningOutputObservation) => void
}) {
  const sessions = yield* Session.Service
  const processors = yield* SessionProcessor.Service
  const provider = yield* Provider.Service
  const chunks: Chunk[] = []
  const consumed = yield* Queue.unbounded<string>()
  const downstream = { value: "" }
  const testClock = input.clocked ? yield* make() : undefined
  const shell = yield* tappedShell(chunks, consumed, testClock)

  yield* input.llm.tool(ShellID.ToolID, { command: input.command, description: "store test", timeout: input.timeout })
  const chat = yield* sessions.create({})
  const user = yield* sessions.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: chat.id,
    agent: "build",
    model: storeRef,
    time: { created: Date.now() },
  })
  const assistant = yield* sessions.updateMessage({
    id: MessageID.ascending(),
    role: "assistant",
    sessionID: chat.id,
    mode: "build",
    agent: "build",
    path: { cwd: input.dir, root: input.dir },
    cost: 0,
    tokens: { total: 0, input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: storeRef.modelID,
    providerID: storeRef.providerID,
    parentID: user.id,
    time: { created: Date.now() },
  } satisfies SessionV1.Assistant)
  const model = yield* provider.getModel(storeRef.providerID, storeRef.modelID)
  const handle = yield* processors.create({ assistantMessage: assistant, sessionID: chat.id, model })
  const tools = yield* SessionTools.resolve({
    agent: storeAgent,
    model,
    session: chat,
    processor: handle,
    bypassAgentCheck: false,
    messages: [],
    promptOps: {} as never,
  }).pipe(
    Effect.provideService(
      ToolRegistry.Service,
      ToolRegistry.Service.of({
        ids: () => Effect.succeed([shell.id]),
        all: () => Effect.succeed([shell]),
        named: () => Effect.die("unused"),
        tools: () => Effect.succeed([shell]),
      }),
    ),
    Effect.provideService(
      Permission.Service,
      Permission.Service.of({
        ask: () => Effect.void,
        reply: () => Effect.die(new Error("unexpected permission reply")),
        list: () => Effect.succeed([]),
        grant: () => Effect.die(new Error("unexpected permission grant")),
      }),
    ),
    Effect.provideService(
      MCP.Service,
      MCP.Service.of({
        tools: () => Effect.succeed({}),
        clients: () => Effect.succeed({}),
      } as Partial<MCP.Interface> as MCP.Interface),
    ),
  )

  const completed = { value: false }
  const run = yield* handle
    .process({
      user: {
        id: user.id,
        sessionID: chat.id,
        role: "user",
        time: user.time,
        agent: user.agent,
        model: storeRef,
      } satisfies SessionV1.User,
      sessionID: chat.id,
      model,
      agent: storeAgent,
      system: [],
      messages: [{ role: "user", content: "run the command" }],
      tools,
    })
    .pipe(
      Effect.tap(() => Effect.sync(() => void (completed.value = true))),
      Effect.forkChild,
    )
  if (input.interruptOn) {
    const marker = input.interruptOn
    const ready = waitForConsumed(
      consumed,
      downstream,
      marker,
      "contains",
      `ShellTool did not consume ${marker} before interruption`,
    )
    if (!testClock) {
      yield* ready
      yield* Fiber.interrupt(run)
    } else {
      yield* ready
      yield* testClock.adjust(OUTPUT_WRITE_INTERVAL_MS - 1)
      const part = (yield* MessageV2.parts(assistant.id)).find(
        (part): part is SessionV1.ToolPart => part.type === "tool",
      )
      const snapshots = (yield* readToolWrites(chat.id)).filter(
        (write) => write.part.state.status === "running" && runningOutput(write.part) !== "",
      )
      input.checkUnwritten?.({ part, snapshots, chunks: chunks.map((chunk) => chunk.text) })
      yield* Fiber.interrupt(run)
    }
  }

  const staged = input.staged
  if (staged) {
    const intervals = input.stagedIntervals ?? [
      { stageCount: staged.stages.length, adjustments: [OUTPUT_WRITE_INTERVAL_MS] },
    ]
    let stageIndex = 0
    yield* Effect.gen(function* () {
      for (const [intervalIndex, interval] of intervals.entries()) {
        const firstStage = stageIndex
        for (let index = 0; index < interval.stageCount; index++) {
          const expected = staged.stages.slice(0, stageIndex + 1).join("")
          yield* waitForConsumed(
            consumed,
            downstream,
            expected,
            "exact",
            `ShellTool did not consume staged output through ${stageIndex + 1}`,
          )
          if (index < interval.stageCount - 1) yield* staged.release(stageIndex)
          stageIndex++
        }

        const expected = staged.stages.slice(0, stageIndex).join("")
        const previous = staged.stages.slice(0, firstStage).join("")
        for (const [adjustmentIndex, duration] of interval.adjustments.entries()) {
          yield* testClock ? testClock.adjust(duration) : adjust(duration)
          const finalAdjustment = adjustmentIndex === interval.adjustments.length - 1
          const part = finalAdjustment
            ? yield* pollWithTimeout(
                MessageV2.parts(assistant.id).pipe(
                  Effect.map((parts) => {
                    const tool = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
                    return tool?.state.status === "running" && runningOutput(tool) === expected ? tool : undefined
                  }),
                ),
                "the accumulated shell output was not readable from the running Session store",
                "10 seconds",
              )
            : (yield* MessageV2.parts(assistant.id)).find((part): part is SessionV1.ToolPart => part.type === "tool")
          const snapshots = (yield* readToolWrites(chat.id)).filter(
            (write) => write.part.state.status === "running" && runningOutput(write.part) !== "",
          )
          input.checkRunning?.({
            intervalIndex,
            adjustmentIndex,
            expected,
            previous,
            part,
            chunks: chunks.map((chunk) => chunk.text),
            settled: completed.value,
            snapshots,
          })
        }
        yield* staged.release(stageIndex - 1)
      }
    }).pipe(Effect.onError(() => Fiber.interrupt(run)))
  }
  yield* Fiber.await(run)

  const writes = yield* readToolWrites(chat.id)
  const parts = yield* MessageV2.parts(assistant.id)
  return {
    chunks,
    writes,
    // A running snapshot carries consumed output; the shell's initialization write carries an empty one.
    snapshots: writes.filter((write) => write.part.state.status === "running" && runningOutput(write.part) !== ""),
    settled: parts.find((part): part is SessionV1.ToolPart => part.type === "tool"),
  }
})

const runningOutput = (part: SessionV1.ToolPart) =>
  part.state.status === "running" ? String(part.state.metadata?.output ?? "") : ""

const lines = (prefix: string, count: number) =>
  Array.from({ length: count }, (_, i) => `${prefix}-${i + 1}\n`).join("")

const unixStore = process.platform === "win32" ? itStore.live.skip : itStore.live

describe("tool.shell running output persistence", () => {
  unixStore(
    "coalesces a burst of chunks into one running write that lands while the command is quiet",
    () =>
      provideTmpdirServer(
        ({ dir, llm }) =>
          Effect.gen(function* () {
            const stages = Array.from({ length: 8 }, (_, index) => `burst-${index + 1}\n`)
            const staged = yield* stagedOutput(dir, stages)
            const expected = stages.join("")
            const result = yield* throughSession({
              dir,
              llm,
              command: staged.command,
              staged,
              clocked: true,
              stagedIntervals: [{ stageCount: stages.length, adjustments: [OUTPUT_WRITE_INTERVAL_MS - 1, 1] }],
              checkRunning: (during) => {
                expect(during.chunks.length).toBeGreaterThan(1)
                expect(during.chunks.join("")).toBe(expected)
                expect(during.settled).toBe(false)
                expect(during.part?.state.status).toBe("running")
                if (during.adjustmentIndex === 0) {
                  expect(during.snapshots).toHaveLength(0)
                  expect(during.part ? runningOutput(during.part) : "").toBe("")
                  return
                }
                expect(during.snapshots).toHaveLength(1)
                expect(during.part ? runningOutput(during.part) : "").toBe(expected)
                expect(runningOutput(during.snapshots[0]!.part)).toBe(expected)
              },
            })

            expect(result.chunks.length).toBeGreaterThan(1)
            expect(result.chunks.map((chunk) => chunk.text).join("")).toBe(expected)
            expect(result.snapshots).toHaveLength(1)
            expect(runningOutput(result.snapshots[0]!.part)).toBe(expected)

            expect(result.settled?.state.status).toBe("completed")
            if (result.settled?.state.status !== "completed") return
            expect(result.settled.state.output).toBe(expected)
            expect(result.settled.state.metadata.output).toBe(expected)
          }),
        { config: storeConfig },
      ),
    30_000,
  )

  unixStore(
    "bounds running writes of a sustained stream by interval and settles its exact output",
    () =>
      provideTmpdirServer(
        ({ dir, llm }) =>
          Effect.gen(function* () {
            const stages = Array.from({ length: 40 }, (_, index) => `line-${index + 1}\n`)
            const staged = yield* stagedOutput(dir, stages)
            const intervals = Array.from({ length: 4 }, () => ({
              stageCount: 10,
              adjustments: [OUTPUT_WRITE_INTERVAL_MS - 1, 1],
            }))
            const expected = stages.join("")
            const result = yield* throughSession({
              dir,
              llm,
              command: staged.command,
              staged,
              clocked: true,
              stagedIntervals: intervals,
              checkRunning: (state) => {
                const expectedWrites = state.intervalIndex + (state.adjustmentIndex === 0 ? 0 : 1)
                expect(state.part?.state.status).toBe("running")
                expect(state.settled).toBe(false)
                expect(state.chunks.length).toBeGreaterThan(1)
                expect(state.chunks.join("")).toBe(state.expected)
                expect(state.snapshots).toHaveLength(expectedWrites)
                if (state.adjustmentIndex === 0) {
                  expect(state.part ? runningOutput(state.part) : "").toBe(state.previous)
                  if (state.previous) expect(runningOutput(state.snapshots.at(-1)!.part)).toBe(state.previous)
                  return
                }
                expect(state.part ? runningOutput(state.part) : "").toBe(state.expected)
                expect(runningOutput(state.snapshots.at(-1)!.part)).toBe(state.expected)
                if (state.previous)
                  expect(runningOutput(state.snapshots.at(-1)!.part).startsWith(state.previous)).toBe(true)
              },
            })

            expect(result.chunks.length).toBeGreaterThan(intervals.length)
            expect(result.chunks.map((chunk) => chunk.text).join("")).toBe(expected)
            expect(result.snapshots).toHaveLength(intervals.length)
            // Each committed snapshot extends the one before it, so no older output replaced newer output.
            result.snapshots.slice(1).forEach((write, i) => {
              expect(runningOutput(write.part).startsWith(runningOutput(result.snapshots[i]!.part))).toBe(true)
            })

            expect(result.settled?.state.status).toBe("completed")
            if (result.settled?.state.status !== "completed") return
            expect(result.settled.state.output).toBe(expected)
            expect(result.settled.state.metadata).toMatchObject({ output: expected, exit: 0, truncated: false })
          }),
        { config: storeConfig },
      ),
    15_000,
  )

  unixStore(
    "persists the final output of a command that exits before its next write and nothing replaces it",
    () =>
      provideTmpdirServer(
        ({ dir, llm }) =>
          Effect.gen(function* () {
            const result = yield* throughSession({
              dir,
              llm,
              command: "for i in 1 2 3 4 5; do echo fast-$i; done",
            })
            const expected = lines("fast", 5)
            expect(result.settled?.state.status).toBe("completed")
            if (result.settled?.state.status !== "completed") return
            expect(result.settled.state.output).toBe(expected)
            expect(result.settled.state.metadata.output).toBe(expected)

            // Advance two intervals, then confirm the Session part still equals its terminal value at reread.
            yield* adjust(OUTPUT_WRITE_INTERVAL_MS * 2)
            const parts = yield* MessageV2.parts(result.settled.messageID)
            expect(parts.find((part) => part.type === "tool")).toEqual(result.settled)
            const terminal = result.writes.findIndex((write) => write.part.state.status === "completed")
            expect(terminal).toBe(result.writes.length - 1)
          }),
        { config: storeConfig },
      ).pipe(Effect.provide(layer())),
    15_000,
  )

  unixStore(
    "keeps the uncommitted tail when cleanup settles an interrupted command",
    () =>
      provideTmpdirServer(
        ({ dir, llm }) =>
          Effect.gen(function* () {
            // Ignoring SIGTERM keeps the command running past cleanup's wait, so cleanup settles it from the store.
            const result = yield* throughSession({
              dir,
              llm,
              command: "trap '' TERM; echo tail-marker; while :; do sleep 0.1; done",
              interruptOn: "tail-marker",
              clocked: true,
              checkUnwritten: (before) => {
                expect(before.part?.state.status).toBe("running")
                expect(before.part ? runningOutput(before.part) : "").toBe("")
                expect(before.snapshots).toHaveLength(0)
                expect(before.chunks.join("")).toBe("tail-marker\n")
              },
            })
            expect(result.settled?.state.status).toBe("error")
            if (result.settled?.state.status !== "error") return
            expect(result.settled.state.metadata?.interrupted).toBe(true)
            expect(result.settled.state.metadata?.output).toBe("tail-marker\n")
          }),
        { config: storeConfig },
      ),
    30_000,
  )

  unixStore(
    "keeps output a command writes while it shuts down when cleanup settles it",
    () =>
      provideTmpdirServer(
        ({ dir, llm }) =>
          Effect.gen(function* () {
            // The SIGTERM handler writes after the abort's own write, and the command outlives cleanup's wait.
            const result = yield* throughSession({
              dir,
              llm,
              command: "trap 'echo term-tail' TERM; echo tail-marker; while :; do sleep 0.05; done",
              interruptOn: "tail-marker",
            })
            expect(result.chunks.map((chunk) => chunk.text).join("")).toContain("term-tail\n")
            expect(result.settled?.state.status).toBe("error")
            if (result.settled?.state.status !== "error") return
            expect(result.settled.state.metadata?.interrupted).toBe(true)
            const output = String(result.settled.state.metadata?.output)
            expect(output.startsWith("tail-marker\n")).toBe(true)
            expect(output).toContain("term-tail\n")
          }),
        { config: storeConfig },
      ),
    30_000,
  )

  unixStore(
    "keeps output a timed-out command writes while it shuts down when an interruption settles it",
    () =>
      provideTmpdirServer(
        ({ dir, llm }) =>
          Effect.gen(function* () {
            // The timeout starts the kill, and the interruption arrives during it: the marker comes from the SIGTERM
            // handler, and the command outlives cleanup's wait.
            const result = yield* throughSession({
              dir,
              llm,
              command:
                "trap 'echo timeout-tail; echo abort-marker' TERM; echo before-timeout; while :; do sleep 0.05; done",
              timeout: 1000,
              interruptOn: "abort-marker",
            })
            expect(result.settled?.state.status).toBe("error")
            if (result.settled?.state.status !== "error") return
            expect(result.settled.state.metadata?.interrupted).toBe(true)
            const output = String(result.settled.state.metadata?.output)
            expect(output.startsWith("before-timeout\n")).toBe(true)
            expect(output).toContain("timeout-tail\nabort-marker\n")
          }),
        { config: storeConfig },
      ),
    30_000,
  )
})
