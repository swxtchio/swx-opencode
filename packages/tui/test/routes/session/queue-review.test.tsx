/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import type { TuiPluginApi } from "@opencode-ai/plugin/tui"
import { createTestRenderer } from "@opentui/core/testing"
import type { GlobalEvent, Provider, Session, SessionPromptQueueItem, SessionStatus } from "@opencode-ai/sdk/v2"
import { Effect, Schema } from "effect"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Global } from "@opencode-ai/core/global"
import path from "node:path"
import { tmpdir } from "../../fixture/fixture"
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
type HeldPrompt = {
  requestID?: string
  delivery?: "steer" | "queue"
  origin?: "tui-composer"
  parts: { type: string; text?: string }[]
  answer: (response: Response) => void
}
type EnhancementRequest = { model: string; draft: string; context: string }

function queueGroup(input: {
  itemID: string
  message: string
  origin?: "tui-composer"
  kind?: string
  constituents?: { itemID: string; message: string; origin?: "tui-composer" }[]
}) {
  return Schema.encodeSync(SessionPromptQueue.Group)(
    Schema.decodeUnknownSync(SessionPromptQueue.Group)({
      version: 1,
      sessionID,
      kind: input.kind ?? "individual",
      constituents: (input.constituents ?? [input]).map((item, index) => ({
          id: item.itemID,
          sessionID,
          seq: index + 1,
          delivery: "queue",
          input: {
            ...(item.origin ? { origin: item.origin } : {}),
            parts: [{ type: "text", text: item.message }],
          },
          time: { created: 1 },
        })),
    }),
  )
}

// The scripted server keeps real custody of its queue: a withdrawal takes an
// item out and a restore puts it back, each publishing the list. A test's
// stand-in for one of them gets `apply`, which does what the server would, so
// it can let the change land and still lose or fail the answer.
type Apply = () => Response
async function startSession(
  options: {
    queue?: SessionPromptQueueItem[]
    messages?: unknown[]
    status?: SessionStatus
    tuiConfig?: ReturnType<typeof createTuiResolvedConfig>
    enhance?: (payload: EnhancementRequest) => Response | Promise<Response>
    withdraw?: (itemID: string, apply: Apply) => Response | Promise<Response>
    restore?: (itemID: string, apply: Apply) => Response | Promise<Response>
    list?: (apply: Apply) => Response | Promise<Response>
  } = {},
) {
  const setup = await createTestRenderer({ width: 160, height: 45, useThread: false, kittyKeyboard: true })
  // Sent prompts are appended to the prompt history under state; keep it out
  // of the user's own and of the next test's.
  const state = await tmpdir()
  const events = createEventSource()
  // `merged` maps a request the queue merged to the request it went into.
  const queue = {
    items: options.queue ?? items,
    withdrawn: [] as SessionPromptQueueItem[],
    merged: {} as Record<string, string>,
  }
  const requests: string[] = []
  const paths: string[] = []
  const selections: unknown[] = []
  const enhancements: EnhancementRequest[] = []
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
  const withdraw = (itemID: string) => () => {
    const found = queue.items.find((item) => item.id === itemID)
    if (!found) return json({ _tag: "QueueItemNotPending", sessionID, itemID }, { status: 404 })
    queue.withdrawn.push(found)
    publish(queue.items.filter((item) => item.id !== itemID))
    return json(found)
  }
  const restore = (itemID: string) => () => {
    const found = queue.withdrawn.find((item) => item.id === itemID)
    if (!found) return json({ _tag: "QueueItemNotWithdrawn", sessionID, itemID }, { status: 404 })
    queue.withdrawn = queue.withdrawn.filter((item) => item.id !== itemID)
    publish([...queue.items, found].toSorted((a, b) => a.seq - b.seq))
    return json(found)
  }
  const receipt = (requestID: string) => {
    const into = queue.merged[requestID]
    if (into) return json({ state: "merged", into })
    const pending = queue.items.find((item) => item.requestID === requestID)
    if (pending) return json({ state: "pending", itemID: pending.id })
    const withdrawn = queue.withdrawn.find((item) => item.requestID === requestID)
    if (withdrawn) return json({ state: "withdrawn", itemID: withdrawn.id })
    return json({ name: "NotFoundError", data: { message: "No such request" } }, { status: 404 })
  }
  const calls = createFetch((url) => {
    if (url.pathname === "/config/providers") return json({ providers: [provider], default: { test: "model" } })
    if (url.pathname === "/agent") return json([{ name: "build", mode: "primary", permission: [], options: {} }])
    if (url.pathname === "/session/status" && options.status) return json({ [sessionID]: options.status })
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
    if (url.pathname === `/session/${sessionID}/message`) return json(options.messages ?? [transcript])
    if ([`/session/${sessionID}/todo`, `/session/${sessionID}/diff`].includes(url.pathname)) return json([])
    return undefined
  }, events)
  const fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(input, init)
    const path = new URL(request.url).pathname
    paths.push(path)
    if (path.startsWith(`/session/${sessionID}/queue/`)) requests.push(path.split("/").pop()!)
    if (path === `/session/${sessionID}` && request.method === "PATCH") selections.push(await request.clone().json())
    if (path === `/session/${sessionID}/message` && request.method === "POST") {
      const body = await request.clone().json()
      return new Promise<Response>((answer) => prompts.push({ ...body, answer }))
    }
    if (path === "/prompt/enhance" && request.method === "POST") {
      const body = (await request.clone().json()) as EnhancementRequest
      enhancements.push(body)
      return options.enhance ? options.enhance(body) : json("rewritten draft")
    }
    if (path === `/session/${sessionID}/queue/restore`) {
      const body = (await request.clone().json()) as { id: string }
      restored.push(body)
      return options.restore ? options.restore(body.id, restore(body.id)) : restore(body.id)()
    }
    if (path.startsWith(`/session/${sessionID}/queue/receipt/`)) return receipt(path.split("/").pop()!)
    if (path === `/session/${sessionID}/queue` && request.method === "GET" && options.list)
      return options.list(() => json(queue.items))
    if (path.startsWith(`/session/${sessionID}/queue/que_`) && request.method === "DELETE") {
      const itemID = path.split("/").pop()!
      return options.withdraw ? options.withdraw(itemID, withdraw(itemID)) : withdraw(itemID)()
    }
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
        config: options.tuiConfig ?? createTuiResolvedConfig({ plugin_enabled: {} }),
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
      .pipe(
        Effect.provide(AppNodeBuilder.build(Global.node, [[Global.node, Global.layerWith({ state: state.path })]])),
      ),
  )
  await ready

  return {
    renderer: setup,
    requests,
    paths,
    selections,
    enhancements,
    commands: () =>
      api?.keymap.getCommandEntries({ namespace: "palette", visibility: "reachable" }).map((entry) => entry.command.name) ?? [],
    prompts,
    restored,
    publish,
    queue,
    state: state.path,
    dispatch: (command: string) => api?.keymap.dispatchCommand(command),
    async stop() {
      try {
        api?.keymap.dispatchCommand("app.exit")
        await task
      } finally {
        if (!setup.renderer.isDestroyed) setup.renderer.destroy()
        await state[Symbol.asyncDispose]()
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
    expect(app.prompts.map((prompt) => [prompt.origin, prompt.delivery])).toEqual([
      ["tui-composer", "steer"],
      ["tui-composer", "steer"],
    ])
    // The same words sent twice are two requests, not a retry of one.
    expect(second).not.toBe(first)
  } finally {
    await app.stop()
  }
})

test("busy composer submits mark queued prompts as TUI-authored", async () => {
  const app = await startSession({ queue: [], status: { type: "busy" } })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("already in the transcript")), "session")
    for (const [index, text] of ["A", "B"].entries()) {
      if (index === 0) await app.renderer.mockInput.typeText(text)
      else await app.renderer.mockInput.pasteBracketedText("B\ncontinued B")
      app.renderer.mockInput.pressEnter()
      await until(() => app.prompts.length === index + 1, `prompt ${index + 1}`)
    }
    expect(app.prompts.map((prompt) => [prompt.parts.at(-1)?.text, prompt.origin, prompt.delivery])).toEqual([
      ["A", "tui-composer", "queue"],
      ["B\ncontinued B", "tui-composer", "queue"],
    ])
    const items = app.prompts.map((prompt, index) => {
      const text = prompt.parts.at(-1)?.text ?? ""
      return pendingItem(`que_composer_${index}`, text, {
        requestID: prompt.requestID,
        seq: index + 1,
        delivery: prompt.delivery ?? "steer",
        input: { origin: prompt.origin, parts: [{ type: "text", text }] },
      })
    })
    app.publish(items)
    await until(async () => (await screen(app)).some((line) => line.includes("2 pending")), "the two queue rows")
    app.renderer.mockInput.pressKey("x", { ctrl: true })
    app.renderer.mockInput.pressKey("q")
    await until(async () => (await screen(app)).some((line) => line.includes("Queued prompts")), "queue review")
    const review = (await screen(app)).join("\n")
    expect(review).toContain("A")
    expect(review).toContain("B")
    expect(review).toContain("continued B")
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
})

// Submission-time visibility and UP recall (swxtchio/swx-opencode#249).

async function screen(app: Awaited<ReturnType<typeof startSession>>) {
  await app.renderer.renderOnce()
  return app.renderer.captureCharFrame().split("\n")
}

test("timeline dialog only offers TUI-authored prompts and filters injections", async () => {
  const critical = "[fm-level:critical]\x1f injected-critical-token"
  const peer = "[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791809000-1-abcd\x1f injected-peer-token"
  const typedID = "msg_typed"
  const messages = [
    {
      ...transcript,
      info: { ...transcript.info, id: typedID, time: { created: 2 } },
      parts: [
        {
          ...transcript.parts[0],
          id: "prt_typed",
          messageID: typedID,
          text: "typed-history-token",
          metadata: { sessionQueue: queueGroup({ itemID: "que_typed", message: "typed-history-token", origin: "tui-composer" }) },
        },
      ],
    },
    {
      ...transcript,
      info: { ...transcript.info, id: "msg_critical", time: { created: 3 } },
      parts: [
        {
          ...transcript.parts[0],
          id: "prt_critical",
          messageID: "msg_critical",
          text: critical,
          metadata: { sessionQueue: queueGroup({ itemID: "que_critical", message: critical }) },
        },
      ],
    },
    {
      ...transcript,
      info: { ...transcript.info, id: "msg_peer", time: { created: 4 } },
      parts: [
        {
          ...transcript.parts[0],
          id: "prt_peer",
          messageID: "msg_peer",
          text: peer,
          metadata: {
            sessionQueue: queueGroup({
              itemID: "que_peer_1",
              message: peer,
              kind: "peer",
              constituents: [
                { itemID: "que_peer_1", message: peer },
                { itemID: "que_peer_2", message: "another injected peer" },
              ],
            }),
          },
        },
      ],
    },
  ]
  const app = await startSession({ queue: [], messages })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("typed-history-token")), "typed transcript")
    app.renderer.mockInput.pressKey("r", { ctrl: true })
    await until(async () => (await screen(app)).some((line) => line.includes("Timeline")), "the timeline dialog")
    const options = (await screen(app)).join("\n")
    expect(options).toContain("typed-history-token")
    expect(options).not.toContain("injected-critical-token")
    expect(options).not.toContain("injected-peer-token")

    await Bun.sleep(10)
    await app.renderer.mockInput.typeText("injected-critical-token")
    await until(async () => (await screen(app)).some((line) => line.includes("No results found")), "no injection match")
    app.renderer.mockInput.pressEnter()
    expect((await screen(app)).some((line) => line.includes("Message Actions"))).toBe(false)
    app.renderer.mockInput.pressEscape()
    app.renderer.mockInput.pressKey("r", { ctrl: true })
    await until(async () => (await screen(app)).some((line) => line.includes("Timeline")), "the reopened timeline")
    await Bun.sleep(10)
    await app.renderer.mockInput.typeText("typed-history-token")
    app.renderer.mockInput.pressEnter()
    await until(async () => (await screen(app)).some((line) => line.includes("Message Actions")), "the message actions")
  } finally {
    await app.stop()
  }
})

test("session rename remains available from its command menu", async () => {
  const app = await startSession()
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("already in the transcript")), "session")
    app.dispatch("command.palette.show")
    await until(async () => (await screen(app)).some((line) => line.includes("Commands")), "the command menu")
    await until(() => app.commands().includes("session.rename"), "rename command in the palette")
    app.dispatch("session.rename")
    await until(async () => (await screen(app)).some((line) => line.includes("Rename Session")), "rename dialog")
  } finally {
    await app.stop()
  }
})

test("ctrl+e rewrites the draft from visible context without sending until a later submit", async () => {
  const lines = Array.from({ length: 110 }, (_, index) => `visible-${String(index + 1).padStart(3, "0")}`)
  const messageID = "msg_context"
  const messages = [
    {
      ...transcript,
      info: { ...transcript.info, id: messageID, time: { created: 2 } },
      parts: [
        {
          ...transcript.parts[0],
          id: "prt_context",
          messageID,
          text: lines.join("\n"),
        },
        {
          id: "prt_context_synthetic",
          sessionID,
          messageID,
          type: "text",
          text: "hidden synthetic context",
          synthetic: true,
        },
        {
          id: "prt_context_ignored",
          sessionID,
          messageID,
          type: "text",
          text: "hidden ignored context",
          ignored: true,
        },
      ],
    },
  ]
  const app = await startSession({
    queue: [],
    messages,
    tuiConfig: createTuiResolvedConfig({ plugin_enabled: {}, prompt_enhance_model: "test/rewrite-model" }),
    enhance: () => json("clean rewritten draft"),
  })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("visible-110")), "visible context")
    await app.renderer.mockInput.typeText("rough draft")
    await until(async () => (await screen(app)).some((line) => line.includes("rough draft")), "the current draft")
    await until(() => app.commands().includes("prompt.enhance"), "the enabled enhance command")
    app.renderer.mockInput.pressKey("e", { ctrl: true })
    await until(() => app.paths.some((path) => path.endsWith("/prompt/enhance")), "the rewrite request")
    expect(app.enhancements).toHaveLength(1)

    expect(app.enhancements[0]).toMatchObject({ model: "test/rewrite-model", draft: "rough draft" })
    expect(app.enhancements[0]!.context.split("\n")).toHaveLength(100)
    expect(app.enhancements[0]!.context).toContain("visible-110")
    expect(app.enhancements[0]!.context).not.toContain("visible-001")
    expect(app.enhancements[0]!.context).not.toContain("hidden synthetic context")
    expect(app.enhancements[0]!.context).not.toContain("hidden ignored context")
    expect(app.prompts).toHaveLength(0)
    await until(async () => {
      const frame = (await screen(app)).join("\n")
      return frame.includes("clean rewritten draft") && frame.includes("Prompt enhanced")
    }, "the revised composer draft and confirmation")

    await app.renderer.mockInput.typeText(" edited")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the later submit")
    expect(app.prompts[0]!.parts.at(-1)?.text).toBe("clean rewritten draft edited")
    app.prompts[0]!.answer(json(transcript))
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
})

test("failed prompt enhancement leaves the composer draft untouched and shows an error", async () => {
  const app = await startSession({
    queue: [],
    enhance: () => json({ name: "InternalServerError", message: "provider unavailable" }, { status: 500 }),
  })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("already in the transcript")), "session")
    await app.renderer.mockInput.typeText("keep this draft")
    app.renderer.mockInput.pressKey("e", { ctrl: true })
    await until(() => app.paths.some((path) => path.endsWith("/prompt/enhance")), "the failed rewrite request")
    expect(app.enhancements).toHaveLength(1)
    await until(async () => (await screen(app)).some((line) => line.includes("Failed to enhance prompt")), "the error toast")
    const frame = (await screen(app)).join("\n")
    expect(frame).toContain("keep this draft")
    expect(app.prompts).toHaveLength(0)
  } finally {
    await app.stop()
  }
})

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

const shows = async (app: Awaited<ReturnType<typeof startSession>>, text: string) =>
  (await screen(app)).some((line) => line.includes(text))
// A notice wraps inside its toast, so it is read across lines.
const said = async (app: Awaited<ReturnType<typeof startSession>>, text: string) =>
  (await screen(app)).join(" ").replace(/[┃│]/g, " ").replace(/\s+/g, " ").includes(text)
const queuedIDs = (app: Awaited<ReturnType<typeof startSession>>) => app.queue.items.map((item) => item.id)
const deletes = (app: Awaited<ReturnType<typeof startSession>>, itemID: string) =>
  app.requests.filter((request) => request === itemID).length

test("a prompt withdrawn while the composer was typed in goes back to the queue", async () => {
  let answer!: () => void
  const answered = new Promise<void>((resolve) => (answer = resolve))
  const app = await startSession({ queue: [items[1]!], withdraw: (_, apply) => answered.then(apply) })
  try {
    await until(() => shows(app, "1 pending"), "session")
    app.renderer.mockInput.pressArrow("up")
    await until(() => app.requests.includes("que_queue"), "the withdrawal")
    await app.renderer.mockInput.typeText("draft")
    answer()
    await until(() => said(app, "it stays queued"), "the notice")
    // The queue holds it again, and the composer only what was typed.
    expect(queuedIDs(app)).toEqual(["que_queue"])
    expect(queuedLines(await screen(app), "queued for later")).toHaveLength(1)
    expect(composerLines(await screen(app), "queued for later")).toEqual([])
    expect(composerLines(await screen(app), "draft")).toHaveLength(1)
    // Sent, the draft is not stashed into the next test's composer.
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the draft")
    app.prompts[0]!.answer(json(transcript))
  } finally {
    await app.stop()
  }
})

test("a withdrawal whose answer is lost copies nothing and returns the prompt to the queue", async () => {
  const app = await startSession({
    queue: [items[1]!],
    // The server withdrew it, but the answer never arrived.
    withdraw: (_, apply) => {
      apply()
      throw new TypeError("fetch failed")
    },
  })
  try {
    await until(() => shows(app, "1 pending"), "session")
    app.renderer.mockInput.pressArrow("up")
    await until(() => said(app, "it stays queued"), "the notice")
    expect(app.restored).toEqual([{ id: "que_queue" }])
    expect(queuedIDs(app)).toEqual(["que_queue"])
    expect(queuedLines(await screen(app), "queued for later")).toHaveLength(1)
    expect(composerLines(await screen(app), "queued for later")).toEqual([])
  } finally {
    await app.stop()
  }
})

test("a prompt the server confirms is still withdrawn after a failed restore is kept in the prompt stash", async () => {
  const app = await startSession({
    queue: [{ ...items[1], requestID: "queued-request" }],
    withdraw: (_, apply) => {
      apply()
      throw new TypeError("fetch failed")
    },
    restore: () => json({ name: "UnknownError", data: { message: "restore failed" } }, { status: 500 }),
  })
  try {
    await until(() => shows(app, "1 pending"), "session")
    app.renderer.mockInput.pressArrow("up")
    await until(() => said(app, "its text is in the prompt stash"), "the notice")
    // Withdrawn and not restored: neither queued nor in the composer, but not lost.
    expect(queuedIDs(app)).toEqual([])
    expect(await said(app, "it stays queued")).toBe(false)
    expect(composerLines(await screen(app), "queued for later")).toEqual([])
    const stashed = Bun.file(path.join(app.state, "prompt-stash.jsonl"))
    await until(
      async () => (await stashed.exists()) && (await stashed.text()).includes("queued for later"),
      "the stash",
    )
    expect(
      (await stashed.text())
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line).input),
    ).toEqual(["queued for later"])
  } finally {
    await app.stop()
  }
})

test("a restore whose answer is lost, while the queue's list is refused, copies nothing", async () => {
  let refused = false
  const app = await startSession({
    queue: [{ ...items[1], requestID: "queued-request" }],
    // Both changes land and both answers are lost; the list is refused.
    withdraw: (_, apply) => {
      apply()
      throw new TypeError("fetch failed")
    },
    restore: (_, apply) => {
      apply()
      throw new TypeError("fetch failed")
    },
    list: (apply) =>
      refused ? json({ name: "UnknownError", data: { message: "list failed" } }, { status: 500 }) : apply(),
  })
  try {
    await until(() => shows(app, "1 pending"), "session")
    refused = true
    app.renderer.mockInput.pressArrow("up")
    // Its receipt says the restore landed, so it is queued and nowhere else.
    await until(() => said(app, "it stays queued"), "the notice")
    expect(queuedIDs(app)).toEqual(["que_queue"])
    expect(await Bun.file(path.join(app.state, "prompt-stash.jsonl")).exists()).toBe(false)
    expect(composerLines(await screen(app), "queued for later")).toEqual([])
  } finally {
    await app.stop()
  }
})

test("a prompt its restore put back and the queue then delivered is not said to stay queued", async () => {
  const app = await startSession({
    queue: [{ ...items[1], requestID: "queued-request" }],
    withdraw: (_, apply) => {
      apply()
      throw new TypeError("fetch failed")
    },
    // The restore wakes the session, which delivers it at once.
    restore: (_, apply) => {
      const answer = apply()
      app.publish([])
      return answer
    },
  })
  try {
    await until(() => shows(app, "1 pending"), "session")
    app.renderer.mockInput.pressArrow("up")
    await until(() => said(app, "is no longer pending"), "the notice")
    expect(await said(app, "it stays queued")).toBe(false)
    expect(await Bun.file(path.join(app.state, "prompt-stash.jsonl")).exists()).toBe(false)
  } finally {
    await app.stop()
  }
})

test("UP takes back a prompt still being sent that the queue merged into another, through each merge", async () => {
  const app = await startSession({ queue: [] })
  try {
    await until(() => shows(app, "already in the transcript"), "session")
    await app.renderer.mockInput.typeText("check the build")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the send")
    const requestID = app.prompts[0].requestID!
    // Merged twice over into a carrier whose listing this client missed.
    app.queue.items = [
      pendingItem("que_carrier", "check the lint, then check the build", {
        requestID: "carrier",
        origins: [
          { time: 10, requestID: "carrier" },
          { time: 11, requestID: "middle" },
          { time: 12, requestID },
        ],
      }),
    ]
    app.queue.merged = { [requestID]: "middle", middle: "carrier" }
    app.renderer.mockInput.pressArrow("up")
    await until(
      async () => composerLines(await screen(app), "check the lint, then check the build").length === 1,
      "the recall",
    )
    expect(app.requests.filter((request) => request.startsWith("que_"))).toEqual(["que_carrier"])
    expect(await said(app, "no longer pending")).toBe(false)
    // Sent, the draft is not stashed into the next test's composer.
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 2, "the resend")
    app.prompts[1].answer(json(transcript))
  } finally {
    await app.stop()
  }
})

test("a second UP while a recall is still restoring takes nothing", async () => {
  let release!: () => void
  const released = new Promise<void>((resolve) => (release = resolve))
  const app = await startSession({
    queue: [items[1]!],
    // The first withdrawal never reaches the server; its restore is slow.
    withdraw: (itemID, apply) => {
      if (deletes(app, itemID) === 1) throw new TypeError("fetch failed")
      return apply()
    },
    restore: (_, apply) => released.then(apply),
  })
  try {
    await until(() => shows(app, "1 pending"), "session")
    app.renderer.mockInput.pressArrow("up")
    await until(() => app.restored.length === 1, "the restore")
    app.renderer.mockInput.pressArrow("up")
    // Time for a second recall to start, were one allowed.
    await Bun.sleep(200)
    release()
    await until(() => said(app, "it stays queued"), "the notice")
    expect(deletes(app, "que_queue")).toBe(1)
    expect(queuedIDs(app)).toEqual(["que_queue"])
    expect(composerLines(await screen(app), "queued for later")).toEqual([])
  } finally {
    await app.stop()
  }
})

test("a second UP after a withdrawal's list but before its answer copies nothing from history", async () => {
  let release!: () => void
  const released = new Promise<void>((resolve) => (release = resolve))
  const app = await startSession({
    queue: [],
    // The withdrawal lands and its list is published; its answer waits.
    withdraw: (_, apply) => {
      const answer = apply()
      return released.then(() => answer)
    },
  })
  try {
    await until(() => shows(app, "already in the transcript"), "session")
    // Sent from this composer, so prompt history holds it too.
    await app.renderer.mockInput.typeText("check the build")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the send")
    app.publish([pendingItem("que_mine", "check the build", { requestID: app.prompts[0].requestID })])
    await until(async () => queuedLines(await screen(app), "check the build").length === 1, "the listed prompt")

    app.renderer.mockInput.pressArrow("up")
    await until(async () => deletes(app, "que_mine") === 1 && !(await shows(app, "QUEUED")), "the withdrawal's list")
    app.renderer.mockInput.pressArrow("up")
    expect(composerLines(await screen(app), "check the build")).toEqual([])

    release()
    await until(async () => composerLines(await screen(app), "check the build").length === 1, "the recall")
    expect(app.restored).toEqual([])
    expect(queuedIDs(app)).toEqual([])
    // Sent, the draft is not stashed into the next test's composer.
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 2, "the resend")
    app.prompts[1].answer(json(transcript))
  } finally {
    await app.stop()
  }
})

test("a sent steer stays shown and is the one UP takes back, behind older queued prompts", async () => {
  const older = [3, 4, 5, 6].map((at) =>
    pendingItem(`que_older${at}`, `older ${at}`, { delivery: "queue", seq: at, time: { created: at } }),
  )
  const app = await startSession({ queue: older })
  try {
    await until(() => shows(app, "4 pending"), "session")
    await app.renderer.mockInput.typeText("check the build")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the send")
    const requestID = app.prompts[0]!.requestID!
    // The queue lists pending steers first, ahead of the older queued prompts.
    const mine = pendingItem("que_mine", "check the build", { requestID, seq: 7, time: { created: 20 } })
    app.publish([mine, ...older])
    await until(() => shows(app, "5 pending"), "the listed prompt")
    const lines = await screen(app)
    expect(queuedLines(lines, "check the build")).toHaveLength(1)
    expect(lines.some((line) => line.includes("2 sent earlier"))).toBe(true)

    app.renderer.mockInput.pressArrow("up")
    await until(async () => composerLines(await screen(app), "check the build").length === 1, "the recall")
    expect(app.requests).toEqual(["que_mine"])
    expect(queuedIDs(app)).toEqual(older.map((item) => item.id))
  } finally {
    await app.stop()
  }
})

test("UP takes back a prompt still being sent once the queue admits it", async () => {
  const app = await startSession({ queue: [] })
  try {
    await until(() => shows(app, "already in the transcript"), "session")
    await app.renderer.mockInput.typeText("check the build")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the send")
    const requestID = app.prompts[0]!.requestID!
    app.renderer.mockInput.pressArrow("up")
    // It asks the server until the send is admitted.
    await until(() => app.requests.includes(requestID), "the receipt")
    expect(composerLines(await screen(app), "check the build")).toEqual([])
    app.queue.items = [pendingItem("que_mine", "check the build", { requestID })]
    await until(async () => composerLines(await screen(app), "check the build").length === 1, "the recall")
    expect(app.requests.filter((request) => request.startsWith("que_"))).toEqual(["que_mine"])
    expect(queuedLines(await screen(app), "check the build")).toEqual([])
  } finally {
    await app.stop()
  }
})

test("a send the server refuses before listing it leaves no QUEUED prompt", async () => {
  const app = await startSession({ queue: [] })
  try {
    await until(() => shows(app, "already in the transcript"), "session")
    await app.renderer.mockInput.typeText("rejected input")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the send")
    await until(async () => queuedLines(await screen(app), "rejected input").length === 1, "the QUEUED prompt")
    app.prompts[0]!.answer(json({ name: "BadRequest", data: { message: "refused" } }, { status: 400 }))
    await until(() => shows(app, "Failed to send prompt"), "the failure")
    expect(queuedLines(await screen(app), "rejected input")).toEqual([])
  } finally {
    await app.stop()
  }
})

test("a send answered without its listing ever arriving leaves no QUEUED prompt", async () => {
  const app = await startSession({ queue: [] })
  try {
    await until(() => shows(app, "already in the transcript"), "session")
    await app.renderer.mockInput.typeText("check the build")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the send")
    await until(async () => queuedLines(await screen(app), "check the build").length === 1, "the QUEUED prompt")
    app.prompts[0]!.answer(json(transcript))
    await until(async () => queuedLines(await screen(app), "check the build").length === 0, "the settled send")
    expect(await shows(app, "pending")).toBe(false)
  } finally {
    await app.stop()
  }
})

test("the review view's list shows a sent prompt it carries once, though its listing event was missed", async () => {
  const app = await startSession({ queue: [] })
  try {
    await until(() => shows(app, "already in the transcript"), "session")
    await app.renderer.mockInput.typeText("check the build")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the send")
    // Admitted, but its event never reached the client.
    app.queue.items = [pendingItem("que_mine", "check the build", { requestID: app.prompts[0]!.requestID! })]
    app.dispatch("session.queued_prompts")
    await until(() => shows(app, "delivery paused"), "the review view")
    app.renderer.mockInput.pressEscape()
    await until(async () => !(await shows(app, "Queued prompts")), "the closed view")
    expect(queuedLines(await screen(app), "check the build")).toHaveLength(1)
    expect(await shows(app, "1 pending")).toBe(true)
  } finally {
    await app.stop()
  }
})

test("a recalled prompt keeps its attachments and each mention, and resends them", async () => {
  const text = "compare @a.ts with @a.ts"
  const mention = (start: number) => ({ value: "@a.ts", start, end: start + 5 })
  const file = (start: number) => ({
    type: "file" as const,
    mime: "text/plain",
    filename: "a.ts",
    url: "file:///repo/a.ts",
    source: { type: "file" as const, path: "a.ts", text: mention(start) },
  })
  const image = { type: "file" as const, mime: "image/png", filename: "shot.png", url: "data:image/png;base64,AA" }
  const app = await startSession({
    queue: [pendingItem("que_peer", text, { input: { parts: [{ type: "text", text }, file(8), file(19), image] } })],
  })
  try {
    await until(() => shows(app, "1 pending"), "session")
    app.renderer.mockInput.pressArrow("up")
    await until(async () => composerLines(await screen(app), "compare").length === 1, "the recall")
    await app.renderer.mockInput.typeText(" now")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the resend")
    const parts = app.prompts[0]!.parts as SessionPromptQueueItem["input"]["parts"]
    const sent = parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")
    expect(sent).toBe("compare @a.ts with @a.ts [Image 1] now")
    const files = parts.flatMap((part) => (part.type === "file" ? [part] : []))
    expect(files.map((part) => [part.url, part.source?.text.start, part.source?.text.value])).toEqual([
      ["file:///repo/a.ts", 8, "@a.ts"],
      ["file:///repo/a.ts", 19, "@a.ts"],
      ["data:image/png;base64,AA", 25, "[Image 1]"],
    ])
    app.prompts[0]!.answer(json(transcript))
  } finally {
    await app.stop()
  }
})

test("a mention sent after an expanded paste marks the text it was sent with", async () => {
  const text = "compare @a.ts"
  const file = {
    type: "file" as const,
    mime: "text/plain",
    filename: "a.ts",
    url: "file:///repo/a.ts",
    source: { type: "file" as const, path: "a.ts", text: { value: "@a.ts", start: 8, end: 13 } },
  }
  const app = await startSession({
    queue: [pendingItem("que_peer", text, { input: { parts: [{ type: "text", text }, file] } })],
  })
  try {
    await until(() => shows(app, "1 pending"), "session")
    app.renderer.mockInput.pressArrow("up")
    await until(async () => composerLines(await screen(app), "compare").length === 1, "the recall")
    app.renderer.mockInput.pressKey("HOME")
    await app.renderer.mockInput.pasteBracketedText("one\ntwo\nthree")
    await until(() => shows(app, "[Pasted ~3 lines]"), "the paste")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the send")
    const parts = app.prompts[0]!.parts as SessionPromptQueueItem["input"]["parts"]
    const sent = parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")
    expect(sent).toBe("one\ntwo\nthree compare @a.ts")
    const at = parts.flatMap((part) => (part.type === "file" && part.source ? [part.source.text] : []))
    expect(at).toEqual([{ value: "@a.ts", start: sent.indexOf("@a.ts"), end: sent.indexOf("@a.ts") + 5 }])
    app.prompts[0]!.answer(json(transcript))
  } finally {
    await app.stop()
  }
})
