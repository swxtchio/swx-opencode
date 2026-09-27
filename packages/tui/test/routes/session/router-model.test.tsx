/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import type { TuiPluginApi } from "@opencode-ai/plugin/tui"
import { createTestRenderer } from "@opentui/core/testing"
import type { AssistantMessage, GlobalEvent, Part, Provider, Session, UserMessage } from "@opencode-ai/sdk/v2"
import { Effect } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Global } from "@opencode-ai/core/global"
import { createTuiResolvedConfig } from "../../fixture/tui-runtime"
import { createEventSource, createFetch, directory, json } from "../../fixture/tui-sdk"

const sessionID = "ses_router_label"
type SessionMessageWithParts = { info: UserMessage | AssistantMessage; parts: Part[] }

const routerProvider = {
  id: "llmrouter",
  name: "LLMRouter",
  source: "api",
  env: [],
  options: {},
  models: {
    auto: { id: "auto", providerID: "llmrouter", name: "Auto" },
    "luna-max": { id: "luna-max", providerID: "llmrouter", name: "luna-max" },
    "glm-5.3-flash": { id: "glm-5.3-flash", providerID: "llmrouter", name: "glm-5.3-flash" },
    "sol-high": { id: "sol-high", providerID: "llmrouter", name: "sol-high" },
  },
} as unknown as Provider

function sessionInfo(): Session {
  return {
    id: sessionID,
    title: "Router session",
    slug: "router-session",
    projectID: "proj_test",
    directory,
    version: "1.0.0",
    time: { created: 1, updated: 1 },
  } as Session
}

function userMessage(index: number): SessionMessageWithParts {
  const id = `user-${String(index).padStart(3, "0")}`
  return {
    info: {
      id,
      sessionID,
      role: "user",
      time: { created: index * 2 },
      agent: "build",
      model: { providerID: "llmrouter", modelID: "auto" },
    } satisfies UserMessage,
    parts: [
      {
        id: `${id}-text`,
        sessionID,
        messageID: id,
        type: "text",
        text: "hello",
      } satisfies Part,
    ],
  }
}

function assistantMessage(index: number, responseModelID: string): SessionMessageWithParts {
  const id = `assistant-${String(index).padStart(3, "0")}`
  const parentID = `user-${String(index).padStart(3, "0")}`
  return {
    info: {
      id,
      sessionID,
      role: "assistant",
      time: { created: index * 2 + 1, completed: index * 2 + 2 },
      parentID,
      providerID: "llmrouter",
      modelID: "auto",
      responseModelIDs: [responseModelID],
      mode: "build",
      agent: "build",
      path: { cwd: directory, root: directory },
      cost: 0,
      tokens: { input: 1, output: 1, reasoning: 0, cache: { read: 0, write: 0 } },
      finish: "stop",
    } satisfies AssistantMessage,
    parts: [
      {
        id: `${id}-step`,
        sessionID,
        messageID: id,
        type: "step-finish",
        reason: "stop",
        responseModelID,
        cost: 0,
        tokens: { input: 1, output: 1, reasoning: 0, cache: { read: 0, write: 0 } },
      },
    ] satisfies Part[],
  }
}

function directAssistantMessage(index: number): AssistantMessage {
  return {
    id: `assistant-${String(index).padStart(3, "0")}`,
    sessionID,
    role: "assistant",
    time: { created: index * 2 + 1, completed: index * 2 + 2 },
    parentID: `user-${String(index).padStart(3, "0")}`,
    providerID: "openai",
    modelID: "gpt-5",
    responseModelIDs: ["gpt-5"],
    mode: "build",
    agent: "build",
    path: { cwd: directory, root: directory },
    cost: 0,
    tokens: { input: 1, output: 1, reasoning: 0, cache: { read: 0, write: 0 } },
    finish: "stop",
  }
}

function initialMessages(): SessionMessageWithParts[] {
  return Array.from({ length: 100 }, (_, index) => index + 1).flatMap((index) => [
    userMessage(index),
    assistantMessage(index, "luna-max"),
  ]).concat([userMessage(101), assistantMessage(101, "glm-5.3-flash")])
}

function event(payload: GlobalEvent["payload"]): GlobalEvent {
  return { directory, project: "proj_test", payload }
}

async function startSession(input: { historyError?: boolean } = {}) {
  const setup = await createTestRenderer({ width: 160, height: 45, useThread: false })
  const events = createEventSource()
  const initial = initialMessages()
  const state = { messages: initial, historyReads: 0, omitLatestFromHistory: !input.historyError }
  const calls = createFetch((url) => {
    if (url.pathname === "/config/providers") return json({ providers: [routerProvider], default: { llmrouter: "auto" } })
    if (url.pathname === "/provider") return json({ all: [], default: {}, connected: [] })
    if (url.pathname === `/session/${sessionID}`) return json(sessionInfo())
    if (url.pathname === `/session/${sessionID}/message`) {
      if (url.searchParams.get("limit") === "100") return json(state.messages.slice(-100))
      state.historyReads++
      if (input.historyError) return json({ error: { name: "Unavailable" } }, { status: 500 })
      return json(
        state.omitLatestFromHistory ? state.messages.slice(0, -1) : state.messages,
      )
    }
    if (url.pathname === `/session/${sessionID}/todo` || url.pathname === `/session/${sessionID}/diff`) return json([])
    return undefined
  }, events)
  let api: TuiPluginApi | undefined
  let started!: () => void
  const ready = new Promise<void>((resolve) => {
    started = resolve
  })
  const task = Effect.runPromise(
    (await import("../../../src/app"))
      .run({
        url: "http://test",
        directory,
        createRenderer: async () => setup.renderer,
        config: createTuiResolvedConfig({ plugin_enabled: {} }),
        fetch: calls.fetch,
        events: events.source,
        args: { sessionID },
        pluginHost: {
          async start(input) {
            api = input.api
            started()
          },
          async dispose() {},
        },
      })
      .pipe(Effect.provide(AppNodeBuilder.build(Global.node))),
  )
  await ready

  return {
    renderer: setup,
    state,
    emit: events.emit,
    sessionMessages: () => api?.state.session.messages(sessionID) ?? [],
    async stop() {
      try {
        api?.keymap.dispatchCommand("app.exit")
        await task
      } finally {
        if (!setup.renderer.isDestroyed) setup.renderer.destroy()
      }
    },
  }
}

async function waitForFrame(renderer: Awaited<ReturnType<typeof createTestRenderer>>, value: string) {
  const until = Date.now() + 5000
  while (Date.now() < until) {
    await renderer.renderOnce()
    const frame = renderer.captureCharFrame()
    if (frame.includes(value)) return frame
    await Bun.sleep(10)
  }
  throw new Error(`session footer did not render ${value}`)
}

async function waitForHistoryReads(state: { historyReads: number }, previous: number) {
  const until = Date.now() + 5000
  while (Date.now() < until) {
    if (state.historyReads > previous) return
    await Bun.sleep(10)
  }
  throw new Error("router session history was not refreshed")
}

async function waitForSessionMessageAbsence(
  renderer: Awaited<ReturnType<typeof createTestRenderer>>,
  messages: () => readonly { id: string }[],
  id: string,
) {
  const until = Date.now() + 5000
  while (Date.now() < until) {
    await renderer.renderOnce()
    if (!messages().some((message) => message.id === id)) return
    await Bun.sleep(10)
  }
  throw new Error(`session Sync window still contained ${id}`)
}

test("Session footer loads all turns and refreshes after message removal and later steps", async () => {
  const app = await startSession()
  try {
    await waitForFrame(app.renderer, "Auto (luna-max:100/99%, glm-5.3-flash:1/1%, sol-high:0/0%)")

    app.state.omitLatestFromHistory = false
    const afterInitialFetch = app.state.historyReads
    app.state.messages = app.state.messages.filter((item) => item.info.id !== "assistant-001")
    app.emit(
      event({
        id: "evt_removed_old_router_step",
        type: "message.removed",
        properties: { sessionID, messageID: "assistant-001" },
      }),
    )
    await waitForHistoryReads(app.state, afterInitialFetch)
    await waitForFrame(app.renderer, "Auto (luna-max:99/99%, glm-5.3-flash:1/1%, sol-high:0/0%)")

    const beforeNewStep = app.state.historyReads
    const user = userMessage(102)
    const assistant = assistantMessage(102, "luna-max")
    app.state.messages.push(user, assistant)
    app.emit(event({ id: "evt_user_102", type: "message.updated", properties: { sessionID, info: user.info } }))
    app.emit(
      event({
        id: "evt_assistant_102",
        type: "message.updated",
        properties: { sessionID, info: assistant.info },
      }),
    )
    app.emit(
      event({
        id: "evt_step_102",
        type: "message.part.updated",
        properties: { sessionID, part: assistant.parts[0]!, time: 102 },
      }),
    )
    await waitForHistoryReads(app.state, beforeNewStep)
    await waitForFrame(app.renderer, "Auto (luna-max:100/99%, glm-5.3-flash:1/1%, sol-high:0/0%)")

    const laterDirectMessages = Array.from({ length: 101 }, (_, index) => directAssistantMessage(index + 103))
    app.state.messages.push(...laterDirectMessages.map((info) => ({ info, parts: [] })))
    for (const info of laterDirectMessages) {
      app.emit(event({ id: `evt_${info.id}`, type: "message.updated", properties: { sessionID, info } }))
    }
    await waitForSessionMessageAbsence(app.renderer, app.sessionMessages, "assistant-102")

    const beforeEvictedStep = app.state.historyReads
    const finalUser = userMessage(204)
    const finalAssistant = assistantMessage(204, "glm-5.3-flash")
    app.state.messages.push(finalUser, finalAssistant)
    app.emit(event({ id: "evt_user_204", type: "message.updated", properties: { sessionID, info: finalUser.info } }))
    app.emit(
      event({
        id: "evt_assistant_204",
        type: "message.updated",
        properties: { sessionID, info: finalAssistant.info },
      }),
    )
    await waitForHistoryReads(app.state, beforeEvictedStep)
    await waitForFrame(app.renderer, "Auto (luna-max:100/98%, glm-5.3-flash:2/2%, sol-high:0/0%)")
  } finally {
    await app.stop()
  }
}, 15_000)

test("Session footer falls back to served ids when its full history request fails", async () => {
  const app = await startSession({ historyError: true })
  try {
    await waitForFrame(app.renderer, "Auto (glm-5.3-flash)")
  } finally {
    await app.stop()
  }
}, 15_000)
