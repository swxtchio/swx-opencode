/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import type { TuiPluginApi } from "@opencode-ai/plugin/tui"
import { createTestRenderer } from "@opentui/core/testing"
import type { GlobalEvent, Provider, Session, SessionPromptQueueItem } from "@opencode-ai/sdk/v2"
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
  models: { model: { id: "model", providerID: "test", name: "Model", variants: { low: {}, high: {} } } },
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
    origins: [{ time: 2 }],
  },
  {
    id: "que_queue",
    sessionID,
    seq: 3,
    delivery: "queue",
    input: { parts: [{ type: "text", text: "queued for later" }] },
    time: { created: 3 },
    origins: [{ time: 3 }],
  },
]

// A send the scripted server has not answered yet: the prompt request runs
// until the turn that answers it ends.
type HeldPrompt = { requestID?: string; parts: { type: string; text?: string }[]; answer: (response: Response) => void }

async function startSession(
  options: {
    queue?: SessionPromptQueueItem[]
    // Answers a withdrawal in place of the scripted queue, which hands over any pending item.
    withdraw?: (itemID: string) => Response | Promise<Response>
  } = {},
) {
  const setup = await createTestRenderer({ width: 160, height: 45, useThread: false })
  const events = createEventSource()
  const queue = { items: options.queue ?? items }
  const requests: string[] = []
  const selections: unknown[] = []
  const prompts: HeldPrompt[] = []
  const restored: unknown[] = []
  // The server publishes every change to the pending list.
  const publish = (next: SessionPromptQueueItem[]) => {
    queue.items = next
    events.emit({
      directory,
      project: "proj_test",
      payload: { id: `evt_${Math.random()}`, type: "session.queue.updated", properties: { sessionID, items: next } },
    } as GlobalEvent)
  }
  const withdraw = (itemID: string) => {
    const found = queue.items.find((item) => item.id === itemID)
    if (!found) return json({ _tag: "QueueItemNotPending", sessionID, itemID }, { status: 404 })
    publish(queue.items.filter((item) => item.id !== itemID))
    return json(found)
  }
  const calls = createFetch((url) => {
    if (url.pathname === "/config/providers") return json({ providers: [provider], default: { test: "model" } })
    if (url.pathname === "/agent") return json([{ name: "build", mode: "primary", permission: [], options: {} }])
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
    if (url.pathname.startsWith(`/session/${sessionID}/queue`)) return json(queue.items)
    if (url.pathname === `/session/${sessionID}/message`) return json([transcript])
    if ([`/session/${sessionID}/todo`, `/session/${sessionID}/diff`].includes(url.pathname)) return json([])
    return undefined
  }, events)
  const fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(input, init)
    const path = new URL(request.url).pathname
    if (path.startsWith(`/session/${sessionID}/queue/`)) requests.push(path.split("/").pop()!)
    if (path === `/session/${sessionID}` && request.method === "PATCH") selections.push(await request.clone().json())
    if (path === `/session/${sessionID}/message` && request.method === "POST") {
      const body = await request.clone().json()
      return new Promise<Response>((answer) => prompts.push({ ...body, answer }))
    }
    if (path === `/session/${sessionID}/queue/restore`) {
      restored.push(await request.clone().json())
      return json(queue.items[0])
    }
    if (path.startsWith(`/session/${sessionID}/queue/que_`) && request.method === "DELETE")
      return (options.withdraw ?? withdraw)(path.split("/").pop()!)
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
    selections,
    prompts,
    restored,
    publish,
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

test("an effort the user picks becomes the open session's selection, which its pending prompts run at", async () => {
  const app = await startSession()
  try {
    await app.renderer.renderOnce()
    await until(async () => {
      await app.renderer.renderOnce()
      return app.renderer.captureCharFrame().includes("2 pending")
    }, "the session")
    // Restoring the transcript's model is not a choice, so nothing is selected yet.
    expect(app.selections).toEqual([])

    // Saved efforts outlive a run, so the cycle starts anywhere; each pick is sent.
    app.dispatch("variant.cycle")
    await until(() => app.selections.length === 1, "the first selection update")
    app.dispatch("variant.cycle")
    await until(() => app.selections.length === 2, "the second selection update")
    const picked = app.selections as { model: { providerID: string; id: string; variant: string } }[]
    expect(picked.map((selection) => [selection.model.providerID, selection.model.id])).toEqual([
      ["test", "model"],
      ["test", "model"],
    ])
    expect(["low", "high", "default"]).toContain(picked[0]!.model.variant)
    expect(picked[1]!.model.variant).not.toBe(picked[0]!.model.variant)
  } finally {
    await app.stop()
  }
})

test("each prompt the user sends names its own request", async () => {
  const app = await startSession()
  try {
    await until(async () => {
      await app.renderer.renderOnce()
      return app.renderer.captureCharFrame().includes("2 pending")
    }, "the session")
    for (const [index, text] of ["check the build", "check the build"].entries()) {
      await app.renderer.mockInput.typeText(text)
      app.renderer.mockInput.pressEnter()
      await until(() => app.prompts.length === index + 1, `prompt ${index + 1}`)
    }
    expect(app.prompts.map((prompt) => prompt.parts.at(-1)?.text)).toEqual(["check the build", "check the build"])
    const [first, second] = app.prompts.map((prompt) => prompt.requestID)
    expect(first).toMatch(/^[A-Za-z0-9._:-]{1,128}$/)
    expect(second).toMatch(/^[A-Za-z0-9._:-]{1,128}$/)
    // The same words sent twice are two requests, not a retry of one.
    expect(second).not.toBe(first)
  } finally {
    await app.stop()
  }
})

// Submission-time visibility and UP recall (swxtchio/swx-opencode#249).

async function screen(app: Awaited<ReturnType<typeof startSession>>) {
  await app.renderer.renderOnce()
  return app.renderer.captureCharFrame().split("\n")
}

// Lines of the pending row mark each prompt QUEUED; any other line holding the
// text is the composer's.
const queuedLines = (lines: string[], text: string) =>
  lines.filter((line) => line.includes("QUEUED") && line.includes(text))
const composerLines = (lines: string[], text: string) =>
  lines.filter((line) => !line.includes("QUEUED") && line.includes(text))

function pendingItem(id: string, text: string, extra: Partial<SessionPromptQueueItem> = {}): SessionPromptQueueItem {
  return {
    id,
    sessionID,
    seq: 10,
    delivery: "steer",
    input: { parts: [{ type: "text", text }] },
    time: { created: 10 },
    origins: [{ time: 10 }],
    ...extra,
  }
}

test("a prompt sent while its session is busy shows QUEUED from the moment the composer clears", async () => {
  const app = await startSession({ queue: [] })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("already in the transcript")), "session")
    await app.renderer.mockInput.typeText("check the build")
    app.renderer.mockInput.pressEnter()
    // The server has neither answered the send nor listed it yet.
    await until(() => app.prompts.length === 1, "the send")
    await until(async () => queuedLines(await screen(app), "check the build").length === 1, "the QUEUED prompt")
    expect(composerLines(await screen(app), "check the build")).toEqual([])
    expect((await screen(app)).some((line) => line.includes("1 pending · 1 steer · 0 queue"))).toBe(true)

    // The queue lists it under the request it was sent as, and shows it once.
    const requestID = app.prompts[0]!.requestID!
    app.publish([pendingItem("que_mine", "check the build", { requestID, origins: [{ time: 10, requestID }] })])
    await until(async () => (await screen(app)).some((line) => line.includes("1 pending")), "the listed prompt")
    expect(queuedLines(await screen(app), "check the build")).toHaveLength(1)

    // Promoted, it leaves the pending row; nothing brings the sent copy back.
    app.publish([])
    await until(async () => !(await screen(app)).some((line) => line.includes("pending")), "the promotion")
    expect(queuedLines(await screen(app), "check the build")).toEqual([])
    app.prompts[0]!.answer(json(transcript))
  } finally {
    await app.stop()
  }
})

test("a prompt the queue merged into an earlier copy shows only as that copy", async () => {
  const app = await startSession({ queue: [] })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("already in the transcript")), "session")
    await app.renderer.mockInput.typeText("check the build")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the send")
    await until(async () => queuedLines(await screen(app), "check the build").length === 1, "the QUEUED prompt")

    // An identical copy was already pending: this request is a repeat of it.
    const requestID = app.prompts[0]!.requestID!
    app.publish([
      pendingItem("que_first", "check the build", {
        requestID: "first-send",
        origins: [{ time: 5, requestID: "first-send", repeats: [{ time: 10, requestID }] }],
      }),
    ])
    await until(async () => queuedLines(await screen(app), "check the build").length === 1, "the merged copy")
    expect((await screen(app)).some((line) => line.includes("1 pending"))).toBe(true)
    app.prompts[0]!.answer(json(transcript))
  } finally {
    await app.stop()
  }
})

test("UP on an empty composer takes back the last pending prompt, and its edited resend is a new request", async () => {
  const app = await startSession({ queue: [items[0]!] })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("1 pending")), "session")
    await app.renderer.mockInput.typeText("check the build")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the send")
    // The queue holds the prompt as the composer sent it.
    const requestID = app.prompts[0]!.requestID!
    const parts = app.prompts[0]!.parts as SessionPromptQueueItem["input"]["parts"]
    app.publish([items[0]!, pendingItem("que_mine", "check the build", { requestID, input: { parts } })])
    await until(async () => (await screen(app)).some((line) => line.includes("2 pending")), "the listed prompt")

    app.renderer.mockInput.pressArrow("up")
    await until(async () => composerLines(await screen(app), "check the build").length === 1, "the recalled prompt")
    // Only the last pending prompt was withdrawn; the earlier one stays queued.
    expect(app.requests).toEqual(["que_mine"])
    const lines = await screen(app)
    expect(queuedLines(lines, "check the build")).toEqual([])
    expect(queuedLines(lines, "steer the running turn")).toHaveLength(1)
    expect(lines.some((line) => line.includes("1 pending"))).toBe(true)

    // Its first send ends withdrawn, which is not a failure to report.
    app.prompts[0]!.answer(
      json({ _tag: "PromptWithdrawn", sessionID, itemID: "que_mine", message: "withdrawn" }, { status: 409 }),
    )
    await app.renderer.mockInput.typeText(" again")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 2, "the resend")
    expect(app.prompts[1]!.parts.at(-1)?.text).toBe("check the build again")
    expect(app.prompts[1]!.requestID).not.toBe(requestID)
    expect((await screen(app)).some((line) => line.includes("Failed to send prompt"))).toBe(false)
    app.prompts[1]!.answer(json(transcript))
  } finally {
    await app.stop()
  }
})

test("UP copies nothing when the last pending prompt was promoted before its withdrawal", async () => {
  const app = await startSession({
    queue: [items[1]!],
    // Promotion won: the prompt is a message now, so the queue refuses to give it up.
    withdraw: (itemID) => json({ _tag: "QueueItemNotPending", sessionID, itemID }, { status: 404 }),
  })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("1 pending")), "session")
    app.renderer.mockInput.pressArrow("up")
    await until(async () => (await screen(app)).some((line) => line.includes("no longer pending")), "the notice")
    expect(app.requests).toEqual(["que_queue"])
    expect(composerLines(await screen(app), "queued for later")).toEqual([])

    // The composer stayed empty, so sending sends nothing.
    app.renderer.mockInput.pressEnter()
    await app.renderer.mockInput.typeText("next")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the next send")
    expect(app.prompts[0]!.parts.at(-1)?.text).toBe("next")
    app.prompts[0]!.answer(json(transcript))
  } finally {
    await app.stop()
  }
})

test("UP in a composer holding text leaves the pending prompts queued", async () => {
  const app = await startSession({ queue: [items[1]!] })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("1 pending")), "session")
    await app.renderer.mockInput.typeText("draft")
    // The first UP moves the cursor to the start; the second would recall.
    app.renderer.mockInput.pressArrow("up")
    app.renderer.mockInput.pressArrow("up")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the send")
    expect(app.prompts[0]!.parts.at(-1)?.text).toBe("draft")
    expect(app.requests).toEqual([])
    app.prompts[0]!.answer(json(transcript))
  } finally {
    await app.stop()
  }
})

test("a prompt withdrawn while the composer was typed in goes back to the queue", async () => {
  let answer!: (response: Response) => void
  const withdrawal = new Promise<Response>((resolve) => (answer = resolve))
  const app = await startSession({ queue: [items[1]!], withdraw: () => withdrawal })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("1 pending")), "session")
    app.renderer.mockInput.pressArrow("up")
    await until(() => app.requests.includes("que_queue"), "the withdrawal")
    await app.renderer.mockInput.typeText("draft")
    answer(json(items[1]))
    await until(() => app.restored.length === 1, "the restore")
    expect(app.restored).toEqual([{ id: "que_queue" }])
    expect(composerLines(await screen(app), "queued for later")).toEqual([])
    expect(composerLines(await screen(app), "draft")).toHaveLength(1)
  } finally {
    await app.stop()
  }
})
