import { expect } from "bun:test"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Agent } from "@/agent/agent"
import { MCP } from "@/mcp"
import { Permission } from "@/permission"
import { Provider } from "@/provider/provider"
import { Session } from "@/session/session"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { SessionProcessor } from "@/session/processor"
import { SessionTools } from "@/session/tools"
import { Tool } from "@/tool/tool"
import { ToolRegistry } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"
import { Plugin } from "@/plugin"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Effect, Layer, Schema } from "effect"
import { testEffect } from "../lib/effect"

const callID = "call-test"
const sessionID = SessionID.make("ses_test")
const messageID = MessageID.ascending()
const partID = PartID.ascending()

const agent: Agent.Info = {
  name: "build",
  mode: "primary",
  options: {},
  permission: [{ permission: "*", pattern: "*", action: "allow" }],
}

const model = {
  providerID: ProviderV2.ID.make("test"),
  api: { id: "test-model" },
} as Provider.Model

function fakeMcp() {
  return MCP.Service.of({
    tools: () => Effect.succeed({}),
    clients: () => Effect.succeed({}),
  } as Partial<MCP.Interface> as MCP.Interface)
}

const fakePlugin = Plugin.Service.of({
  init: () => Effect.void,
  list: () => Effect.succeed([]),
  trigger: (_name, _input, output) => Effect.succeed(output),
} satisfies Plugin.Interface)

const fakePermission = Permission.Service.of({
  ask: () => Effect.void,
  reply: () => Effect.void,
  list: () => Effect.succeed([]),
} satisfies Permission.Interface)

const fakeTruncate = Truncate.Service.of({
  cleanup: () => Effect.void,
  write: () => Effect.succeed("output.txt"),
  output: (text: string) => Effect.succeed({ content: text, truncated: false }),
  limits: () => Effect.succeed({ maxLines: 2000, maxBytes: 50 * 1024 }),
} satisfies Truncate.Interface)

const layer = Layer.mergeAll(
  Layer.succeed(Plugin.Service, fakePlugin),
  Layer.succeed(Permission.Service, fakePermission),
  Layer.succeed(MCP.Service, fakeMcp()),
  Layer.succeed(Truncate.Service, fakeTruncate),
  RuntimeFlags.layer(),
  Layer.succeed(
    ToolRegistry.Service,
    ToolRegistry.Service.of({
      ids: () => Effect.succeed(["timing"]),
      all: () => Effect.succeed([]),
      named: () => Effect.die("unused"),
      tools: () =>
        Effect.succeed([
          {
            id: "timing",
            description: "updates metadata more than once",
            parameters: Schema.Struct({}),
            jsonSchema: { type: "object", properties: {} },
            execute: (_args, ctx) =>
              Effect.gen(function* () {
                yield* ctx.metadata({ metadata: { output: "first" } })
                yield* ctx.metadata({ metadata: { output: "second" } })
                return { title: "timing", metadata: {}, output: "done" }
              }),
          } satisfies Tool.Def,
        ]),
    }),
  ),
)

const it = testEffect(layer)

it.effect("preserves running tool start time across metadata updates", () =>
  Effect.gen(function* () {
    const state: SessionV1.ToolPart = {
      id: partID,
      sessionID,
      messageID,
      type: "tool",
      tool: "timing",
      callID,
      state: {
        status: "running",
        input: {},
        time: { start: 100 },
      },
    }
    const updates: number[] = []
    const processor = {
      message: {
        id: messageID,
        sessionID,
        role: "assistant",
        parentID: MessageID.ascending(),
        agent: "build",
        mode: "build",
        path: { cwd: "/tmp", root: "/tmp" },
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        modelID: ModelV2.ID.make("test-model"),
        providerID: ProviderV2.ID.make("test"),
        time: { created: 1 },
      } satisfies SessionV1.Assistant,
      updateToolCall: (_toolCallID, update) =>
        Effect.sync(() => {
          const next = update(state)
          state.state = next.state
          if (state.state.status === "running") updates.push(state.state.time.start)
          return state
        }),
      admitToolCall: () => Effect.succeed(state),
      completeToolCall: () => Effect.void,
    } satisfies Pick<SessionProcessor.Handle, "message" | "updateToolCall" | "admitToolCall" | "completeToolCall">

    const tools = yield* SessionTools.resolve({
      agent,
      model,
      session: { id: sessionID, permission: [] } as unknown as Session.Info,
      processor,
      bypassAgentCheck: false,
      messages: [],
      promptOps: {} as never,
    })
    const execute = tools.timing.execute
    if (!execute) throw new Error("timing tool is missing execute")

    yield* Effect.promise(() =>
      execute(
        {},
        {
          toolCallId: callID,
          abortSignal: new AbortController().signal,
          messages: [],
        },
      ),
    )

    expect(updates).toEqual([100, 100])
    expect(state.state.status).toBe("running")
    if (state.state.status === "running") {
      expect(state.state.time.start).toBe(100)
    }
  }),
)

it.effect("runs no plugin hook or tool body for a call its processor does not admit", () =>
  Effect.gen(function* () {
    const hooks: string[] = []
    const effects: string[] = []
    const admitted: SessionV1.ToolPart = {
      id: partID,
      sessionID,
      messageID,
      type: "tool",
      tool: "probe",
      callID,
      state: { status: "running", input: {}, time: { start: 1 } },
    }
    const processor = {
      message: {
        id: messageID,
        sessionID,
        role: "assistant",
        parentID: MessageID.ascending(),
        agent: "build",
        mode: "build",
        path: { cwd: "/tmp", root: "/tmp" },
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        modelID: ModelV2.ID.make("test-model"),
        providerID: ProviderV2.ID.make("test"),
        time: { created: 1 },
      } satisfies SessionV1.Assistant,
      updateToolCall: () => Effect.succeed(admitted),
      // The processor refuses admission by failing the execution, as it does for an unowned or ended call.
      admitToolCall: (toolCallID: string) =>
        toolCallID.startsWith("refused")
          ? Effect.die(new Error(`Tool call ${toolCallID} was not admitted for execution`))
          : Effect.succeed(admitted),
      completeToolCall: () => Effect.void,
    } satisfies Pick<SessionProcessor.Handle, "message" | "updateToolCall" | "admitToolCall" | "completeToolCall">
    const client = {
      getServerCapabilities: () => ({ resources: {} }),
      callTool: async () => {
        effects.push("mcp")
        return { content: [{ type: "text", text: "called" }] }
      },
    }

    const tools = yield* SessionTools.resolve({
      agent,
      model,
      session: { id: sessionID, permission: [] } as unknown as Session.Info,
      processor,
      bypassAgentCheck: false,
      messages: [],
      promptOps: {} as never,
    }).pipe(
      Effect.provideService(
        Plugin.Service,
        Plugin.Service.of({
          init: () => Effect.void,
          list: () => Effect.succeed([]),
          trigger: (name, input, output) =>
            Effect.sync(() => {
              hooks.push(`${name}:${(input as { callID: string }).callID}`)
              return output
            }),
        }),
      ),
      Effect.provideService(
        MCP.Service,
        MCP.Service.of({
          clients: () => Effect.succeed({ srv: client }),
          tools: () =>
            Effect.succeed({
              srv_lookup: {
                def: { name: "lookup", inputSchema: { type: "object", properties: {} } },
                client,
              },
            }),
          readResource: () =>
            Effect.sync(() => {
              effects.push("resource")
              return { contents: [{ uri: "res://a", text: "a" }] }
            }),
        } as unknown as MCP.Interface),
      ),
      Effect.provideService(
        ToolRegistry.Service,
        ToolRegistry.Service.of({
          ids: () => Effect.succeed(["probe"]),
          all: () => Effect.succeed([]),
          named: () => Effect.die("unused"),
          tools: () =>
            Effect.succeed([
              {
                id: "probe",
                description: "records its effect",
                parameters: Schema.Struct({}),
                jsonSchema: { type: "object", properties: {} },
                execute: () =>
                  Effect.sync(() => {
                    effects.push("probe")
                    return { title: "probe", metadata: {}, output: "done" }
                  }),
              } satisfies Tool.Def,
            ]),
        }),
      ),
    )
    const run = (name: string, toolCallID: string, args: Record<string, unknown>) => {
      const execute = tools[name]?.execute
      if (!execute) throw new Error(`${name} is missing execute`)
      return Effect.promise(() =>
        Promise.resolve(
          execute(args, { toolCallId: toolCallID, abortSignal: new AbortController().signal, messages: [] }),
        ).then(
          () => "fulfilled",
          () => "rejected",
        ),
      )
    }
    const calls = [
      ["probe", {}],
      ["srv_lookup", {}],
      ["read_mcp_resource", { server: "srv", uri: "res://a" }],
    ] as const

    for (const [name, args] of calls) expect(yield* run(name, `refused-${name}`, args)).toBe("rejected")
    expect(hooks).toEqual([])
    expect(effects).toEqual([])

    for (const [name, args] of calls) expect(yield* run(name, `admitted-${name}`, args)).toBe("fulfilled")
    expect(hooks.filter((hook) => hook.startsWith("tool.execute.before"))).toEqual(
      calls.map(([name]) => `tool.execute.before:admitted-${name}`),
    )
    expect(effects).toEqual(["probe", "mcp", "resource"])
  }),
)
