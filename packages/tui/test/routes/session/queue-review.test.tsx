/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import type { TuiPluginApi } from "@opencode-ai/plugin/tui"
import { createTestRenderer } from "@opentui/core/testing"
import type { Provider, Session, SessionPromptQueueItem } from "@opencode-ai/sdk/v2"
import { Effect } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Global } from "@opencode-ai/core/global"
import { createTuiResolvedConfig } from "../../fixture/tui-runtime"
import { createEventSource, createFetch, directory, json } from "../../fixture/tui-sdk"

// The pending-prompt row in the running app's session prompt, and the
// session.queued_prompts command that opens the same review view
// (swxtchio/swx-opencode#212).

const sessionID = "ses_queue_app"

const provider = {
  id: "test",
  name: "Test",
  source: "api",
  env: [],
  options: {},
  models: { model: { id: "model", providerID: "test", name: "Model" } },
} as unknown as Provider

const transcript = {
  info: {
    id: "msg_user",
    sessionID,
    role: "user",
    time: { created: 1 },
    agent: "build",
    model: { providerID: "test", modelID: "model" },
  },
  parts: [{ id: "prt_user", sessionID, messageID: "msg_user", type: "text", text: "already in the transcript" }],
}

const items: SessionPromptQueueItem[] = [
  {
    id: "que_steer",
    sessionID,
    seq: 2,
    delivery: "steer",
    input: { parts: [{ type: "text", text: "steer the running turn" }] },
    time: { created: 2 },
  },
  {
    id: "que_queue",
    sessionID,
    seq: 3,
    delivery: "queue",
    input: { parts: [{ type: "text", text: "queued for later" }] },
    time: { created: 3 },
  },
]

async function startSession() {
  const setup = await createTestRenderer({ width: 160, height: 45, useThread: false })
  const events = createEventSource()
  const requests: string[] = []
  const calls = createFetch((url) => {
    if (url.pathname === "/config/providers") return json({ providers: [provider], default: { test: "model" } })
    if (url.pathname === `/session/${sessionID}`)
      return json({
        id: sessionID,
        title: "Queue session",
        slug: "queue-session",
        projectID: "proj_test",
        directory,
        version: "1.0.0",
        time: { created: 1, updated: 1 },
      } as Session)
    if (url.pathname === `/session/${sessionID}/queue/release`) return new Response(null, { status: 204 })
    if (url.pathname.startsWith(`/session/${sessionID}/queue`)) return json(items)
    if (url.pathname === `/session/${sessionID}/message`) return json([transcript])
    if ([`/session/${sessionID}/todo`, `/session/${sessionID}/diff`].includes(url.pathname)) return json([])
    return undefined
  }, events)
  const fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(input, init)
    const path = new URL(request.url).pathname
    if (path.startsWith(`/session/${sessionID}/queue/`)) requests.push(path.split("/").pop()!)
    return calls.fetch(request)
  }) as typeof globalThis.fetch
  let api: TuiPluginApi | undefined
  let started!: () => void
  const ready = new Promise<void>((resolve) => (started = resolve))
  const task = Effect.runPromise(
    (await import("../../../src/app"))
      .run({
        url: "http://test",
        directory,
        createRenderer: async () => setup.renderer,
        config: createTuiResolvedConfig({ plugin_enabled: {} }),
        fetch,
        events: events.source,
        args: { sessionID },
        pluginHost: {
          // As the real host does, so the session_prompt slot renders the Prompt.
          async start(input) {
            api = input.api
            input.runtime.setupSlots(input.api)
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
    requests,
    dispatch: (command: string) => api?.keymap.dispatchCommand(command),
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

async function until(fn: () => boolean | Promise<boolean>, what: string) {
  const deadline = Date.now() + 5000
  while (Date.now() < deadline) {
    if (await fn()) return
    await Bun.sleep(10)
  }
  throw new Error(`timed out waiting for ${what}`)
}

test("the session prompt shows pending prompts, and session.queued_prompts opens their held review", async () => {
  const app = await startSession()
  const frame = async () => {
    await app.renderer.renderOnce()
    return app.renderer.captureCharFrame()
  }
  try {
    await until(async () => (await frame()).includes("2 pending · 1 steer · 1 queue"), "the pending row")

    app.dispatch("session.queued_prompts")
    await until(async () => (await frame()).includes("Queued prompts"), "the review view")
    const screen = await frame()
    const steer = screen.indexOf("steer the running turn")
    expect(steer).toBeGreaterThanOrEqual(0)
    expect(steer).toBeLessThan(screen.indexOf("queued for later"))
    await until(() => app.requests.includes("hold"), "the hold request")

    app.renderer.mockInput.pressEscape()
    await until(() => app.requests.includes("release"), "the release request")
    expect(app.requests).toEqual(["hold", "release"])
  } finally {
    await app.stop()
  }
})
