/**
 * Reproducer for snapshot race condition with instant tool execution.
 *
 * When the mock LLM returns a tool call response instantly, the AI SDK
 * processes the tool call and executes the tool (e.g. apply_patch) before
 * the processor's start-step handler can capture a pre-tool snapshot.
 * Both the "before" and "after" snapshots end up with the same git tree
 * hash, so computeDiff returns empty and the session summary shows 0 files.
 *
 * This is a real bug: the snapshot system assumes it can capture state
 * before tools run by hooking into start-step, but the AI SDK executes
 * tools internally during multi-step processing before emitting events.
 */
import { expect } from "bun:test"
import { Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import fs from "fs/promises"
import path from "path"
import { Session } from "@/session/session"
import { SessionPrompt } from "../../src/session/prompt"
import { SessionSummary } from "../../src/session/summary"
import { MessageV2 } from "../../src/session/message-v2"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { provideTmpdirServer } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { TestLLMServer } from "../lib/llm-server"
import { deadline, scaleNote } from "../lib/deadline"

import { LSP } from "@/lsp/lsp"
import { MCP } from "../../src/mcp"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { RuntimeFlags } from "@/effect/runtime-flags"

const mcp = Layer.succeed(
  MCP.Service,
  MCP.Service.of({
    status: () => Effect.succeed({}),
    clients: () => Effect.succeed({}),
    instructions: () => Effect.succeed([]),
    tools: () => Effect.succeed({}),
    prompts: () => Effect.succeed({}),
    resources: () => Effect.succeed({}),
    resourceTemplates: () => Effect.succeed({}),
    add: () => Effect.succeed({ status: { status: "disabled" as const } }),
    connect: () => Effect.void,
    disconnect: () => Effect.void,
    getPrompt: () => Effect.succeed(undefined),
    readResource: () => Effect.succeed(undefined),
    startAuth: () => Effect.die("unexpected MCP auth"),
    authenticate: () => Effect.die("unexpected MCP auth"),
    finishAuth: () => Effect.die("unexpected MCP auth"),
    removeAuth: () => Effect.void,
    supportsOAuth: () => Effect.succeed(false),
    hasStoredTokens: () => Effect.succeed(false),
    getAuthStatus: () => Effect.succeed("not_authenticated" as const),
  }),
)

const lsp = Layer.succeed(
  LSP.Service,
  LSP.Service.of({
    init: () => Effect.void,
    status: () => Effect.succeed([]),
    hasClients: () => Effect.succeed(false),
    touchFile: () => Effect.void,
    diagnostics: () => Effect.succeed({}),
    hover: () => Effect.succeed(undefined),
    definition: () => Effect.succeed([]),
    references: () => Effect.succeed([]),
    implementation: () => Effect.succeed([]),
    documentSymbol: () => Effect.succeed([]),
    workspaceSymbol: () => Effect.succeed([]),
    prepareCallHierarchy: () => Effect.succeed([]),
    incomingCalls: () => Effect.succeed([]),
    outgoingCalls: () => Effect.succeed([]),
  }),
)

const root = LayerNode.group([
  SessionPrompt.node,
  Session.node,
  SessionProjector.node,
  SessionSummary.node,
  Database.node,
  CrossSpawnSpawner.node,
  LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] }),
])
const it = testEffect(
  LayerNode.compile(root, [
    [MCP.node, mcp],
    [LSP.node, lsp],
    [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true })],
  ]),
)

const providerCfg = (url: string) => ({
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
      options: {
        apiKey: "test-key",
        baseURL: url,
      },
    },
  },
})

it.live(
  "tool execution produces non-empty session diff (snapshot race)",
  () =>
    provideTmpdirServer(
      Effect.fnUntraced(function* ({ dir, llm }) {
        const prompt = yield* SessionPrompt.Service
        const sessions = yield* Session.Service
        const summary = yield* SessionSummary.Service

        const session = yield* sessions.create({
          title: "snapshot race test",
          permission: [{ permission: "*", pattern: "*", action: "allow" }],
        })

        // Use bash tool (always registered) to create a file
        const command = `echo 'snapshot race test content' > ${path.join(dir, "race-test.txt")}`
        yield* llm.toolMatch((hit) => JSON.stringify(hit.body).includes("create the file"), "bash", {
          command,
        })
        yield* llm.textMatch((hit) => JSON.stringify(hit.body).includes("bash"), "done")

        // Seed user message
        yield* prompt.prompt({
          sessionID: session.id,
          agent: "build",
          noReply: true,
          parts: [{ type: "text", text: "create the file" }],
        })

        // Run the agent loop
        const result = yield* prompt.loop({ sessionID: session.id })
        expect(result.info.role).toBe("assistant")

        // Verify the file was created
        const filePath = path.join(dir, "race-test.txt")
        const fileExists = yield* Effect.promise(() =>
          fs
            .access(filePath)
            .then(() => true)
            .catch(() => false),
        )
        expect(fileExists).toBe(true)

        // Verify the tool call completed (in the first assistant message)
        const allMsgs = yield* MessageV2.filterCompactedEffect(session.id)
        const user = allMsgs.find(
          (msg): msg is SessionV1.WithParts & { info: SessionV1.User } => msg.info.role === "user",
        )
        const tool = allMsgs
          .flatMap((m) => m.parts)
          .find((p): p is SessionV1.ToolPart => p.type === "tool" && p.tool === "bash")
        expect(tool?.state.status).toBe("completed")
        if (!user) throw new Error("Expected user message")

        // Poll for the turn diff — summarize() is fire-and-forget, so there is no
        // completion event to await and polling for the observable result is the
        // right shape. What was wrong was the budget: 50 attempts at 100ms is 5
        // seconds, which is ample on an idle machine and not ample in a full
        // concurrent suite. #10 records this test passing in isolation and
        // failing in a full run, which is that and not a race in the code.
        const budgetMs = deadline(15_000)
        const intervalMs = 100
        const attempts = Math.ceil(budgetMs / intervalMs)

        let diff: Array<{ file?: string }> = []
        let polls = 0
        for (; polls < attempts; polls++) {
          diff = yield* summary.diff({ sessionID: session.id, messageID: user.info.id })
          if (diff.length > 0) break
          yield* Effect.sleep(`${intervalMs} millis`)
        }

        // Attribute an exhausted budget rather than reporting `0 is not > 0`,
        // which reads as "the diff is empty" when it may mean "the diff had not
        // been computed yet". The file itself, and its tool call, are already
        // asserted above, so reaching here with no diff after the full budget is
        // the only ambiguous outcome left.
        if (diff.length === 0)
          throw new Error(
            `no turn diff after ${polls} polls over ${budgetMs}ms${scaleNote()}. The file was created and the ` +
              `bash tool completed, so the tool ran: this is either summarize() being slower than the budget ` +
              `under load, or the snapshot race this test guards. Raise OPENCODE_TEST_TIMEOUT_SCALE to tell them ` +
              `apart before concluding it is the race.`,
          )
        expect(diff.length).toBeGreaterThan(0)
      }),
      { git: true, config: providerCfg },
    ),
  // The bun-test budget has to comfortably contain the poll budget above, or
  // the poll can never exhaust and report its diagnostic - it just gets killed
  // by the outer timeout with "this test timed out after 5000ms", which says
  // nothing about what was being waited for. Measured: with only the poll
  // raised, that is exactly what happened.
  deadline(45_000),
)
