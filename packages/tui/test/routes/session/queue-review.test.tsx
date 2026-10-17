/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import type { TuiPluginApi } from "@opencode-ai/plugin/tui"
import { createTestRenderer } from "@opentui/core/testing"
import { InputRenderable, TextareaRenderable } from "@opentui/core"
import type {
  Config,
  GlobalEvent,
  Message,
  Part,
  Provider,
  Session,
  SessionPromptQueueItem,
  SessionStatus,
} from "@opencode-ai/sdk/v2"
import { Effect } from "effect"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { TuiEvent } from "@opencode-ai/schema/tui-event"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Global } from "@opencode-ai/core/global"
import path from "node:path"
import { tmpdir } from "../../fixture/fixture"
import { createTuiResolvedConfig } from "../../fixture/tui-runtime"
import { createEventSource, createFetch, directory, json } from "../../fixture/tui-sdk"
import { preserveMachineAppended } from "../../../src/prompt/part"
import { timelineOptions } from "../../../src/routes/session/dialog-timeline"

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
  model?: { providerID: string; modelID: string }
  agent?: string
  variant?: string
  parts: { type: string; text?: string; metadata?: Record<string, unknown>; synthetic?: boolean; ignored?: boolean }[]
  answer: (response: Response) => void
}
type EnhancementRequest = { model: string; draft: string; context: string }

// The scripted server keeps real custody of its queue: a withdrawal takes an
// item out and a restore puts it back, each publishing the list. A test's
// stand-in for one of them gets `apply`, which does what the server would, so
// it can let the change land and still lose or fail the answer.
type Apply = () => Response
async function startSession(
  options: {
    sessionID?: string
    state?: string
    home?: boolean
    queue?: SessionPromptQueueItem[]
    messages?: unknown[]
    messagesBySession?: Record<string, unknown[]>
    providers?: Provider[]
    revertMessageID?: string
    serverConfig?: Config
    status?: SessionStatus
    tuiConfig?: ReturnType<typeof createTuiResolvedConfig>
    enhance?: (payload: EnhancementRequest) => Response | Promise<Response>
    withdraw?: (itemID: string, apply: Apply) => Response | Promise<Response>
    restore?: (itemID: string, apply: Apply) => Response | Promise<Response>
    list?: (apply: Apply) => Response | Promise<Response>
  } = {},
) {
  const activeSessionID = options.sessionID ?? sessionID
  const setup = await createTestRenderer({ width: 160, height: 45, useThread: false, kittyKeyboard: true })
  // Sent prompts are appended to the prompt history under state; keep it out
  // of the user's own and of the next test's.
  const scratch = options.state ? undefined : await tmpdir()
  const state = options.state ?? scratch!.path
  const events = createEventSource()
  // `merged` maps a request the queue merged to the request it went into.
  const queue = {
    items: options.queue ?? items,
    withdrawn: [] as SessionPromptQueueItem[],
    merged: {} as Record<string, string>,
  }
  const requests: string[] = []
  const paths: string[] = []
  const executions: string[] = []
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
      payload: {
        id: `evt_${Math.random()}`,
        type: "session.queue.updated",
        properties: { sessionID: activeSessionID, items: next },
      },
    } as GlobalEvent)
  }
  const appendPrompt = (text: string) =>
    events.emit({
      directory,
      project: "proj_test",
      payload: { id: "evt_prompt_append", type: TuiEvent.PromptAppend.type, properties: { text } },
    } as GlobalEvent)
  const selectSession = (selected: string) =>
    events.emit({
      directory,
      project: "proj_test",
      payload: { id: "evt_session_select", type: TuiEvent.SessionSelect.type, properties: { sessionID: selected } },
    } as GlobalEvent)
  const withdraw = (itemID: string) => () => {
    const found = queue.items.find((item) => item.id === itemID)
    if (!found) return json({ _tag: "QueueItemNotPending", sessionID: activeSessionID, itemID }, { status: 404 })
    queue.withdrawn.push(found)
    publish(queue.items.filter((item) => item.id !== itemID))
    return json(found)
  }
  const restore = (itemID: string) => () => {
    const found = queue.withdrawn.find((item) => item.id === itemID)
    if (!found) return json({ _tag: "QueueItemNotWithdrawn", sessionID: activeSessionID, itemID }, { status: 404 })
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
    if (url.pathname === "/config") return json(options.serverConfig ?? {})
    if (url.pathname === "/config/providers")
      return json({ providers: options.providers ?? [provider], default: { test: "model" } })
    if (url.pathname === "/agent") return json([{ name: "build", mode: "primary", permission: [], options: {} }])
    if (url.pathname === "/session/status" && options.status) return json({ [activeSessionID]: options.status })
    const requestedSessionID = url.pathname.match(/^\/session\/(ses_[^/]+)$/)?.[1]
    if (requestedSessionID)
      return json({
        id: requestedSessionID,
        title: "Queue session",
        slug: "queue-session",
        projectID: "proj_test",
        directory,
        version: "1.0.0",
        time: { created: 1, updated: 1 },
        ...(options.revertMessageID && requestedSessionID === activeSessionID
          ? { revert: { messageID: options.revertMessageID } }
          : {}),
      } as Session)
    const messageSessionID = url.pathname.match(/^\/session\/(ses_[^/]+)\/message$/)?.[1]
    if (messageSessionID) return json(options.messagesBySession?.[messageSessionID] ?? options.messages ?? [transcript])
    if (url.pathname === `/session/${activeSessionID}/queue/release`) return new Response(null, { status: 204 })
    if (url.pathname.startsWith(`/session/${activeSessionID}/queue`)) return json(queue.items)
    if (url.pathname === `/session/${activeSessionID}/message`) return json(options.messages ?? [transcript])
    if ([`/session/${activeSessionID}/todo`, `/session/${activeSessionID}/diff`].includes(url.pathname)) return json([])
    return undefined
  }, events)
  const fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(input, init)
    const path = new URL(request.url).pathname
    paths.push(path)
    if (request.method === "POST" && /\/(command|shell)$/.test(path)) executions.push(path)
    if (path === "/session" && request.method === "POST")
      return json({
        id: activeSessionID,
        directory,
        projectID: "proj_test",
        title: "Created session",
        time: { created: 1, updated: 1 },
      })
    if (path.startsWith(`/session/${activeSessionID}/queue/`)) requests.push(path.split("/").pop()!)
    if (path === `/session/${activeSessionID}` && request.method === "PATCH")
      selections.push(await request.clone().json())
    if (/^\/session\/ses_[^/]+\/message$/.test(path) && request.method === "POST") {
      const body = (await request.clone().json()) as Omit<HeldPrompt, "answer">
      return new Promise<Response>((answer) => prompts.push({ ...body, answer }))
    }
    if (path === `/session/${activeSessionID}/fork` && request.method === "POST") return json({ id: "ses_forked" })
    if (path.startsWith(`/session/${activeSessionID}/`) && /\/(abort|revert)$/.test(path))
      return new Response(null, { status: 204 })
    if (path === "/prompt/enhance" && request.method === "POST") {
      const body = (await request.clone().json()) as EnhancementRequest
      enhancements.push(body)
      return options.enhance ? options.enhance(body) : json("rewritten draft")
    }
    if (path === `/session/${activeSessionID}/queue/restore`) {
      const body = (await request.clone().json()) as { id: string }
      restored.push(body)
      return options.restore ? options.restore(body.id, restore(body.id)) : restore(body.id)()
    }
    if (path.startsWith(`/session/${activeSessionID}/queue/receipt/`)) return receipt(path.split("/").pop()!)
    if (path === `/session/${activeSessionID}/queue` && request.method === "GET" && options.list)
      return options.list(() => json(queue.items))
    if (path.startsWith(`/session/${activeSessionID}/queue/que_`) && request.method === "DELETE") {
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
        args: options.home ? {} : { sessionID: activeSessionID },
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
      .pipe(Effect.provide(AppNodeBuilder.build(Global.node, [[Global.node, Global.layerWith({ state })]]))),
  )
  await ready

  return {
    renderer: setup,
    requests,
    paths,
    executions,
    selections,
    enhancements,
    commands: () =>
      api?.keymap
        .getCommandEntries({ namespace: "palette", visibility: "reachable" })
        .map((entry) => entry.command.name) ?? [],
    prompts,
    restored,
    publish,
    appendPrompt,
    selectSession,
    queue,
    state,
    dispatch: (command: string) => api?.keymap.dispatchCommand(command),
    async stop() {
      try {
        setup.mockInput.pressEscape()
        await setup.renderOnce()
        api?.keymap.dispatchCommand("prompt.clear")
        api?.keymap.dispatchCommand("app.exit")
        await task
      } finally {
        if (!setup.renderer.isDestroyed) setup.renderer.destroy()
        await scratch?.[Symbol.asyncDispose]()
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
}, 20_000)

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
}, 20_000)

test("a TUI append remains machine-provenanced when submitted from the composer", async () => {
  const text = "firstmate injected prompt"
  const app = await startSession({ queue: [], status: { type: "busy" } })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("already in the transcript")), "session")
    app.appendPrompt(text)
    await until(async () => (await screen(app)).some((line) => line.includes(text)), "the appended prompt")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the prompt send")
    expect(app.prompts[0]).toMatchObject({ origin: "tui-composer", delivery: "queue" })
    expect(app.prompts[0]!.parts.at(-1)).toMatchObject({
      text,
      metadata: { [SessionPromptQueue.TuiPromptAppendMetadataKey]: true },
    })
    const emitted = app.prompts[0]!.parts.at(-1)!
    const projected = {
      ...emitted,
      id: "prt_appended_prompt",
      sessionID,
      messageID: "msg_appended_prompt",
    } as Part
    expect(preserveMachineAppended([projected], { input: text, parts: [] })).toEqual({
      input: text,
      parts: [],
      machineAppended: true,
    })
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
}, 20_000)

// Submission-time visibility and UP recall (swxtchio/swx-opencode#249).

async function screen(app: Awaited<ReturnType<typeof startSession>>) {
  await app.renderer.renderOnce()
  return app.renderer.captureCharFrame().split("\n")
}

async function openMessageActions(app: Awaited<ReturnType<typeof startSession>>, text: string) {
  const lines = await screen(app)
  const y = lines.findIndex((line) => line.includes(text))
  if (y === -1) throw new Error(`message is not visible: ${text}`)
  const x = lines[y]!.indexOf(text)
  await app.renderer.mockMouse.click(x, y)
  await until(async () => (await screen(app)).some((line) => line.includes("Message Actions")), "message actions")
}

async function clickVisible(app: Awaited<ReturnType<typeof startSession>>, text: string) {
  const lines = await screen(app)
  const y = lines.findIndex((line) => line.includes(text))
  if (y === -1) throw new Error(`text is not visible: ${text}`)
  await app.renderer.mockMouse.click(lines[y]!.indexOf(text), y)
}

const composerLines = (lines: string[], text: string) =>
  lines.filter((line) => !line.includes("QUEUED") && line.includes(text))

function composer(app: Awaited<ReturnType<typeof startSession>>) {
  const focused = app.renderer.renderer.currentFocusedRenderable
  expect(focused).toBeInstanceOf(TextareaRenderable)
  return focused as TextareaRenderable
}

async function searchHistory(app: Awaited<ReturnType<typeof startSession>>, text: string) {
  app.renderer.mockInput.pressKey("r", { ctrl: true })
  await until(() => app.renderer.renderer.currentFocusedRenderable instanceof InputRenderable, "the search input focus")
  await app.renderer.mockInput.typeText(text)
  await until(() => {
    const focused = app.renderer.renderer.currentFocusedRenderable
    return focused instanceof InputRenderable && focused.value === text
  }, "the search query")
  return (await screen(app)).join("\n")
}

test("recall and search isolate sessions across switching and reopening shared composer history", async () => {
  await using state = await tmpdir()
  const a = "ses_history_a"
  const b = "ses_history_b"
  const first = "A-exclusive-history-token"
  const second = "B-exclusive-history-token"
  const draft = "A retained draft exclusive token"
  const messagesBySession = { [a]: [{ ...transcript, info: { ...transcript.info, sessionID: a } }], [b]: [] }
  const app = await startSession({ state: state.path, sessionID: a, queue: [], messagesBySession })
  try {
    await until(async () => (await screen(app)).join("\n").includes("already in the transcript"), "session A")
    await app.renderer.mockInput.typeText(first)
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "A's producer")
    app.prompts[0]!.answer(json(transcript))
    await until(async () => !(await screen(app)).join("\n").includes("pending"), "the completed A send")
    app.renderer.mockInput.pressArrow("up")
    await app.renderer.renderOnce()
    expect(composer(app).plainText).toBe(first)
    app.dispatch("prompt.clear")
    expect(await searchHistory(app, first)).not.toContain("No results found")
    app.renderer.mockInput.pressEscape()
    await until(() => app.renderer.renderer.currentFocusedRenderable instanceof TextareaRenderable, "composer focus")
    await app.renderer.mockInput.typeText(draft)
    app.dispatch("prompt.clear")

    app.selectSession(b)
    await until(async () => (await screen(app)).join("\n").includes(b), "fresh session B")
    app.renderer.mockInput.pressArrow("up")
    await app.renderer.renderOnce()
    expect(composer(app).plainText).toBe("")
    expect(await searchHistory(app, first)).toContain("No results found")
    app.renderer.mockInput.pressEscape()
    await until(() => app.renderer.renderer.currentFocusedRenderable instanceof TextareaRenderable, "B composer focus")
    await app.renderer.mockInput.typeText(second)
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 2, "B's producer")
    app.prompts[1]!.answer(json(transcript))
    await until(async () => !(await screen(app)).join("\n").includes("pending"), "the completed B send")
    app.renderer.mockInput.pressArrow("up")
    await app.renderer.renderOnce()
    expect(composer(app).plainText).toBe(second)
    app.dispatch("prompt.clear")
    expect(await searchHistory(app, second)).not.toContain("No results found")
    app.renderer.mockInput.pressEscape()
    app.selectSession(a)
    await until(async () => (await screen(app)).join("\n").includes(a), "A selected again")
    app.renderer.mockInput.pressArrow("up")
    await app.renderer.renderOnce()
    expect(composer(app).plainText).toBe(draft)
    app.dispatch("prompt.clear")
    expect(await searchHistory(app, second)).toContain("No results found")
    app.renderer.mockInput.pressEscape()
    await until(
      async () => (await Bun.file(path.join(state.path, "prompt-history.jsonl")).text()).includes(second),
      "persisted history",
    )
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
  for (const [id, own, other] of [
    [a, draft, second],
    [b, second, first],
  ]) {
    const reopened = await startSession({ state: state.path, sessionID: id, queue: [], messagesBySession })
    try {
      await until(async () => (await screen(reopened)).join("\n").includes(id!), "the reopened session")
      reopened.renderer.mockInput.pressArrow("up")
      await reopened.renderer.renderOnce()
      expect(composer(reopened).plainText).toBe(own!)
      reopened.dispatch("prompt.clear")
      expect(await searchHistory(reopened, own!)).not.toContain("No results found")
      reopened.renderer.mockInput.pressEscape()
      expect(await searchHistory(reopened, other!)).toContain("No results found")
    } finally {
      await reopened.stop()
    }
  }
}, 30_000)

test("history from a new-session submission is owned by the created session", async () => {
  await using state = await tmpdir()
  const app = await startSession({ state: state.path, home: true, queue: [] })
  const text = "new-session-owned-history-token"
  try {
    await until(() => app.renderer.renderer.currentFocusedRenderable instanceof TextareaRenderable, "the home composer")
    await app.renderer.mockInput.typeText(text)
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the new session send")
    app.prompts[0]!.answer(json(transcript))
    await until(async () => (await screen(app)).join("\n").includes(sessionID), "the created session")
    await until(async () => !(await screen(app)).join("\n").includes("pending"), "the settled prompt")
    app.renderer.mockInput.pressArrow("up")
    await app.renderer.renderOnce()
    expect(composer(app).plainText).toBe(text)
    app.dispatch("prompt.clear")
    expect(await searchHistory(app, text)).not.toContain("No results found")
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
}, 20_000)

test("search reaches unrendered recall history and ENTER appends original multiline text without sending", async () => {
  await using state = await tmpdir()
  const original = "older-unrendered-history-token\noriginal second line"
  const marked = "CUSTOM-HOLD: unrendered-machine-token"
  const seeded = await startSession({ state: state.path, queue: [] })
  try {
    await until(
      async () => (await screen(seeded)).join("\n").includes("already in the transcript"),
      "the producer session",
    )
    for (const [index, text] of [marked, original].entries()) {
      await seeded.renderer.mockInput.pasteBracketedText(text)
      seeded.renderer.mockInput.pressEnter()
      await until(() => seeded.prompts.length === index + 1, "history production")
      seeded.prompts[index]!.answer(json(transcript))
      await until(async () => !(await screen(seeded)).join("\n").includes("pending"), "settled send")
    }
    await until(
      async () =>
        (await Bun.file(path.join(state.path, "prompt-history.jsonl")).text()).includes("original second line"),
      "the retained prompt",
    )
  } finally {
    seeded.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await seeded.stop()
  }
  const app = await startSession({
    state: state.path,
    queue: [],
    serverConfig: { machine_message_markers: { hold: [{ type: "prefix", value: "CUSTOM-HOLD:" }] } },
  })
  try {
    await until(
      async () => (await screen(app)).join("\n").includes("already in the transcript"),
      "only the newer loaded transcript",
    )
    expect((await screen(app)).join("\n")).not.toContain("older-unrendered-history-token")
    app.renderer.mockInput.pressArrow("up")
    await app.renderer.renderOnce()
    expect(composer(app).plainText).toBe(original)
    app.dispatch("prompt.clear")
    expect(await searchHistory(app, "unrendered-machine-token")).toContain("No results found")
    app.renderer.mockInput.pressEscape()
    await until(
      () => app.renderer.renderer.currentFocusedRenderable instanceof TextareaRenderable,
      "the editable draft",
    )
    await app.renderer.mockInput.typeText("existing draft ")
    await app.renderer.mockInput.pasteBracketedText("one\ntwo\nthree")
    await until(async () => (await screen(app)).join("\n").includes("[Pasted ~3 lines]"), "the draft's tracked paste")
    const existing = composer(app).plainText
    app.renderer.mockInput.pressKey("a", { ctrl: true })
    app.renderer.mockInput.pressArrow("right")
    expect(composer(app).cursorOffset).toBeLessThan(composer(app).plainText.length)
    expect(await searchHistory(app, "older-unrendered-history-token")).not.toContain("No results found")
    app.renderer.mockInput.pressEnter()
    await app.renderer.renderOnce()
    expect(composer(app).plainText).toBe(existing + original)
    expect((await screen(app)).join("\n")).not.toContain("Timeline")
    await app.renderer.mockInput.typeText(" plus my edit")
    expect(composer(app).plainText).toBe(existing + original + " plus my edit")
    expect(app.prompts).toHaveLength(0)
    expect(app.executions).toEqual([])
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "explicit later submit")
    expect(app.prompts[0]!.parts.at(-1)?.text).toBe("existing draft one\ntwo\nthree " + original + " plus my edit")
    expect(app.prompts[0]!.parts.at(-1)?.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey]).toBeUndefined()
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
}, 30_000)

async function submitComposerPrompt(text: string, appended = false) {
  const app = await startSession({ queue: [], status: { type: "busy" } })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("already in the transcript")), "session")
    if (appended) app.appendPrompt(text)
    else await app.renderer.mockInput.typeText(text)
    await until(async () => (await screen(app)).some((line) => line.includes(text)), "the composer text")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the real composer submission")
    const submitted = app.prompts[0]!
    expect(composerLines(await screen(app), text)).toEqual([])
    expect(submitted).toMatchObject({ origin: "tui-composer", delivery: "queue" })
    const { answer: _answer, ...input } = submitted
    submitted.answer(json(transcript))
    return input
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
}

async function queueTimelineFixture(
  composerInput: Omit<HeldPrompt, "answer">,
  appendedInput: Omit<HeldPrompt, "answer">,
) {
  const fixtureModulePath: string = "../../../../opencode/test/session/queue-timeline-fixture"
  return (await import(fixtureModulePath)).queueTimelineFixture(
    composerInput as SessionPromptQueue.Input,
    appendedInput as SessionPromptQueue.Input,
  )
}

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
}, 20_000)

test("timeline search uses queue-produced human provenance and excludes injected user messages", async () => {
  const fixture = await queueTimelineFixture(
    await submitComposerPrompt("typed-history-token"),
    await submitComposerPrompt("firstmate appended history token", true),
  )
  const fixtureMessages = fixture.messages as { info: { id: string }; parts: unknown[] }[]
  const candidates = timelineOptions({
    sessionID: fixture.sessionID,
    messages: fixtureMessages.map((message) => message.info as unknown as Message),
    parts: Object.fromEntries(fixtureMessages.map((message) => [message.info.id, message.parts as unknown as Part[]])),
    markers: { hold: [{ type: "prefix", value: "CUSTOM-HOLD:" }] },
  })
  const titles = candidates.map((candidate) => candidate.title)
  expect(titles).toContain("typed-history-token")
  expect(titles).toContain("direct-composer-history-token")
  expect(titles).not.toContain("legacy-app-history-token")
  expect(titles).not.toContain("pre-upgrade-history-token")
  expect(titles).not.toContain("injected-history-token")
  expect(titles).not.toContain("injected-critical-history-token")
  expect(titles).not.toContain("configured-injected-history-token")
  expect(titles).not.toContain("configured-direct-injection-token")
  expect(titles.some((title) => title.includes("configured-injected-history-token"))).toBe(false)
  expect(titles.some((title) => title.includes("Complete peer 31"))).toBe(false)
  expect(titles).not.toContain("firstmate appended history token")
  const app = await startSession({
    sessionID: fixture.sessionID,
    queue: [],
    messages: fixture.messages,
    serverConfig: { machine_message_markers: { hold: [{ type: "prefix", value: "CUSTOM-HOLD:" }] } },
  })
  try {
    await until(
      async () => (await screen(app)).some((line) => line.includes("firstmate appended history token")),
      "the queue-produced transcript",
    )
    app.renderer.mockInput.pressKey("r", { ctrl: true })
    await until(async () => (await screen(app)).some((line) => line.includes("Timeline")), "the timeline dialog")

    await Bun.sleep(10)
    app.renderer.mockInput.typeText("injected-critical-history-token")
    await until(async () => (await screen(app)).some((line) => line.includes("No results found")), "no injection match")
    app.renderer.mockInput.pressEnter()
    expect((await screen(app)).some((line) => line.includes("Message Actions"))).toBe(false)
    app.renderer.mockInput.pressEscape()
    app.renderer.mockInput.pressKey("r", { ctrl: true })
    await until(async () => (await screen(app)).some((line) => line.includes("Timeline")), "the reopened timeline")
    await Bun.sleep(10)
    app.renderer.mockInput.typeText("configured-injected-history-token")
    await until(
      async () => (await screen(app)).some((line) => line.includes("No results found")),
      "configured marker exclusion",
    )
    app.renderer.mockInput.pressEnter()
    expect((await screen(app)).some((line) => line.includes("Message Actions"))).toBe(false)
    app.renderer.mockInput.pressEscape()
    app.renderer.mockInput.pressKey("r", { ctrl: true })
    await until(async () => (await screen(app)).some((line) => line.includes("Timeline")), "the reopened timeline")
    await Bun.sleep(10)
    await app.renderer.mockInput.typeText("pre-upgrade-history-token")
    await until(
      async () => (await screen(app)).some((line) => line.includes("No results found")),
      "unknown history exclusion",
    )
    app.renderer.mockInput.pressEnter()
    expect((await screen(app)).some((line) => line.includes("Message Actions"))).toBe(false)
    app.renderer.mockInput.pressEscape()
    app.renderer.mockInput.pressKey("r", { ctrl: true })
    await until(async () => (await screen(app)).some((line) => line.includes("Timeline")), "the reopened timeline")
    await Bun.sleep(10)
    await app.renderer.mockInput.typeText("legacy-app-history-token")
    await until(
      async () => (await screen(app)).some((line) => line.includes("No results found")),
      "originless queue exclusion",
    )
    app.renderer.mockInput.pressEnter()
    expect((await screen(app)).some((line) => line.includes("Message Actions"))).toBe(false)
    app.renderer.mockInput.pressEscape()
    app.renderer.mockInput.pressKey("r", { ctrl: true })
    await until(async () => (await screen(app)).some((line) => line.includes("Timeline")), "the reopened timeline")
    await Bun.sleep(10)
    await app.renderer.mockInput.typeText("direct-composer-history-token")
    app.renderer.mockInput.pressEnter()
    await app.renderer.renderOnce()
    expect(app.prompts).toHaveLength(0)
    expect(composer(app).plainText).toContain("direct-composer-history-token")
    app.renderer.mockInput.pressEscape()
    app.renderer.mockInput.pressKey("r", { ctrl: true })
    await until(async () => (await screen(app)).some((line) => line.includes("Timeline")), "the reopened timeline")
    await Bun.sleep(10)
    await app.renderer.mockInput.typeText("typed-history-token")
    app.renderer.mockInput.pressEnter()
    await app.renderer.renderOnce()
    expect(app.prompts).toHaveLength(0)
    expect(composer(app).plainText).toContain("typed-history-token")
  } finally {
    await app.stop()
  }
}, 30_000)

test("timeline fails closed for a queue-produced prompt without composer origin", async () => {
  const fixture = await queueTimelineFixture(
    await submitComposerPrompt("typed-history-token"),
    await submitComposerPrompt("firstmate appended history token", true),
  )
  const app = await startSession({ sessionID: fixture.sessionID, queue: [], messages: fixture.messages })
  try {
    await until(
      async () => (await screen(app)).some((line) => line.includes("firstmate appended history token")),
      "the queue-produced transcript",
    )
    app.renderer.mockInput.pressKey("r", { ctrl: true })
    await until(async () => (await screen(app)).some((line) => line.includes("Timeline")), "the timeline dialog")
    await Bun.sleep(10)
    app.renderer.mockInput.typeText("legacy-app-history-token")
    await until(
      async () => (await screen(app)).some((line) => line.includes("No results found")),
      "originless prompt exclusion",
    )
    app.renderer.mockInput.pressEnter()
    expect((await screen(app)).some((line) => line.includes("Message Actions"))).toBe(false)
  } finally {
    await app.stop()
  }
}, 30_000)

test("undo keeps appended machine provenance when the draft is submitted again", async () => {
  const fixture = await queueTimelineFixture(
    await submitComposerPrompt("typed-history-token"),
    await submitComposerPrompt("firstmate appended history token", true),
  )
  const app = await startSession({ sessionID: fixture.sessionID, queue: [], messages: fixture.messages })
  try {
    await until(
      async () => (await screen(app)).some((line) => line.includes("firstmate appended history token")),
      "the appended message",
    )
    app.dispatch("session.undo")
    await until(
      async () => composerLines(await screen(app), "firstmate appended history token").length > 0,
      "the restored draft",
    )
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the resent prompt")
    expect(app.prompts[0]!.parts.at(-1)).toMatchObject({
      text: "firstmate appended history token",
      metadata: { [SessionPromptQueue.TuiPromptAppendMetadataKey]: true },
    })
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
}, 30_000)

test("message Revert keeps appended machine provenance when the draft is submitted again", async () => {
  const fixture = await queueTimelineFixture(
    await submitComposerPrompt("typed-history-token"),
    await submitComposerPrompt("firstmate appended history token", true),
  )
  const app = await startSession({ sessionID: fixture.sessionID, queue: [], messages: fixture.messages })
  try {
    await until(
      async () => (await screen(app)).some((line) => line.includes("firstmate appended history token")),
      "the appended message",
    )
    await openMessageActions(app, "firstmate appended history token")
    app.renderer.mockInput.pressEnter()
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the resent prompt")
    expect(app.prompts[0]!.parts.at(-1)).toMatchObject({
      text: "firstmate appended history token",
      metadata: { [SessionPromptQueue.TuiPromptAppendMetadataKey]: true },
    })
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
}, 30_000)

test("message Fork keeps appended machine provenance in the new composer", async () => {
  const fixture = await queueTimelineFixture(
    await submitComposerPrompt("typed-history-token"),
    await submitComposerPrompt("firstmate appended history token", true),
  )
  const app = await startSession({ sessionID: fixture.sessionID, queue: [], messages: fixture.messages })
  try {
    await until(
      async () => (await screen(app)).some((line) => line.includes("firstmate appended history token")),
      "the appended message",
    )
    await openMessageActions(app, "firstmate appended history token")
    app.renderer.mockInput.pressArrow("down")
    app.renderer.mockInput.pressArrow("down")
    app.renderer.mockInput.pressEnter()
    await until(() => app.paths.some((path) => path.endsWith("/fork")), "the fork request")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the new-session prompt")
    expect(app.prompts[0]!.parts.at(-1)).toMatchObject({
      text: "firstmate appended history token",
      metadata: { [SessionPromptQueue.TuiPromptAppendMetadataKey]: true },
    })
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
}, 30_000)

test("timeline Fork keeps appended machine provenance in the new composer", async () => {
  const fixture = await queueTimelineFixture(
    await submitComposerPrompt("typed-history-token"),
    await submitComposerPrompt("firstmate appended history token", true),
  )
  const app = await startSession({ sessionID: fixture.sessionID, queue: [], messages: fixture.messages })
  try {
    await until(
      async () => (await screen(app)).some((line) => line.includes("firstmate appended history token")),
      "the appended message",
    )
    app.dispatch("session.fork")
    await until(async () => (await screen(app)).some((line) => line.includes("Fork session")), "the fork timeline")
    await app.renderer.mockInput.typeText("firstmate appended history token")
    app.renderer.mockInput.pressArrow("down")
    app.renderer.mockInput.pressEnter()
    await until(() => app.paths.some((path) => path.endsWith("/fork")), "the fork request")
    await until(() => app.paths.includes("/session/ses_forked"), "the new session")
    await until(() => app.commands().includes("prompt.enhance"), "the new composer")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the new-session prompt")
    expect(app.prompts[0]!.parts.at(-1)).toMatchObject({
      text: "firstmate appended history token",
      metadata: { [SessionPromptQueue.TuiPromptAppendMetadataKey]: true },
    })
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
}, 30_000)

test("ctrl+e keeps its line-end action in a managed dialog input", async () => {
  const app = await startSession()
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("already in the transcript")), "session")
    app.dispatch("command.palette.show")
    await until(async () => (await screen(app)).some((line) => line.includes("Commands")), "the command menu")
    app.dispatch("session.rename")
    await until(async () => (await screen(app)).some((line) => line.includes("Rename Session")), "rename dialog")
    await Bun.sleep(10)

    app.renderer.mockInput.pressKey("a", { ctrl: true })
    app.renderer.mockInput.typeText("renamed ")
    app.renderer.mockInput.pressKey("e", { ctrl: true })
    app.renderer.mockInput.typeText(" tail")

    expect((await screen(app)).join("\n")).toContain("renamed Queue session tail")
    expect(app.enhancements).toHaveLength(0)
    expect(app.prompts).toHaveLength(0)
  } finally {
    await app.stop()
  }
}, 20_000)

test("ctrl+e without an override follows the live non-Google session model and later model changes", async () => {
  const app = await startSession({
    queue: [],
    providers: [
      {
        ...provider,
        models: {
          ...provider.models,
          second: { ...provider.models.model!, id: "second", name: "Second Model", variants: {} },
        },
      },
    ],
    enhance: (payload) => json(payload.draft),
  })
  try {
    await until(async () => (await screen(app)).join("\n").includes("already in the transcript"), "session model")
    await app.renderer.mockInput.typeText("editable current-model draft")
    app.renderer.mockInput.pressKey("e", { ctrl: true })
    await until(() => app.enhancements.length === 1, "current-model request")
    expect(app.enhancements[0]!.model).toBe("test/model")
    await until(() => app.commands().includes("prompt.enhance"), "completed enhancement")
    app.dispatch("model.list")
    await until(() => app.renderer.renderer.currentFocusedRenderable instanceof InputRenderable, "model picker focus")
    await app.renderer.mockInput.typeText("Second Model")
    await until(async () => (await screen(app)).join("\n").includes("Second Model"), "the second model option")
    app.renderer.mockInput.pressEnter()
    await until(
      () => app.selections.some((selection) => (selection as { model?: { id: string } }).model?.id === "second"),
      "selected second model",
    )
    await until(
      () => app.renderer.renderer.currentFocusedRenderable instanceof TextareaRenderable,
      "draft focus after model change",
    )
    app.renderer.mockInput.pressKey("e", { ctrl: true })
    await until(() => app.enhancements.length === 2, "changed-model request")
    expect(app.enhancements[1]!.model).toBe("test/second")
    expect(app.prompts).toHaveLength(0)
  } finally {
    await app.stop()
  }
}, 20_000)

test("ctrl+e explicit overrides win even when the override equals the approved fallback", async () => {
  for (const model of ["test/explicit-enhance", "google/gemini-3.8-flash"]) {
    const app = await startSession({
      queue: [],
      tuiConfig: createTuiResolvedConfig({ plugin_enabled: {}, prompt_enhance_model: model }),
    })
    try {
      await until(
        async () => (await screen(app)).join("\n").includes("already in the transcript"),
        "non-Google current model",
      )
      await app.renderer.mockInput.typeText("override draft")
      app.renderer.mockInput.pressKey("e", { ctrl: true })
      await until(() => app.enhancements.length === 1, "override request")
      expect(app.enhancements[0]!.model).toBe(model)
      expect(app.prompts).toHaveLength(0)
    } finally {
      await app.stop()
    }
  }
}, 20_000)

test("ctrl+e uses approved Flash only without a resolved current model and preserves a failed draft", async () => {
  const app = await startSession({
    queue: [],
    providers: [{ ...provider, models: {} }],
    messages: [],
    enhance: () => json({ name: "InternalServerError", message: "fallback provider unavailable" }, { status: 500 }),
  })
  try {
    await until(async () => (await screen(app)).join("\n").includes(sessionID), "session without a provider")
    await until(
      () => app.renderer.renderer.currentFocusedRenderable instanceof TextareaRenderable,
      "the fallback composer focus",
    )
    await app.renderer.mockInput.typeText("keep fallback failure editable")
    await until(() => app.commands().includes("prompt.enhance"), "the fallback enhancement command")
    app.renderer.mockInput.pressKey("e", { ctrl: true })
    await until(() => app.enhancements.length === 1, "fallback request")
    expect(app.enhancements[0]!.model).toBe("google/gemini-3.8-flash")
    await until(
      async () => (await screen(app)).join("\n").includes("Failed to enhance prompt"),
      "fallback failure notice",
    )
    expect(composer(app).plainText).toBe("keep fallback failure editable")
    await app.renderer.mockInput.typeText(" still editing")
    expect(composer(app).plainText).toBe("keep fallback failure editable still editing")
    expect(app.prompts).toHaveLength(0)
  } finally {
    await app.stop()
  }
}, 20_000)

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
    {
      ...transcript,
      info: { ...transcript.info, id: "msg_reverted_context", time: { created: 3 } },
      parts: [
        {
          ...transcript.parts[0],
          id: "prt_reverted_context",
          messageID: "msg_reverted_context",
          text: "reverted-only context",
        },
      ],
    },
  ]
  const app = await startSession({
    queue: [],
    messages,
    revertMessageID: "msg_reverted_context",
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
    expect(app.enhancements[0]!.context).not.toContain("reverted-only context")
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
}, 20_000)

test("ctrl+e context follows the session's expanded thinking and tool visibility settings", async () => {
  const assistantID = "msg_visible_assistant"
  const messages = [
    transcript,
    {
      info: {
        id: assistantID,
        sessionID,
        role: "assistant",
        parentID: transcript.info.id,
        mode: "primary",
        agent: "build",
        modelID: "model",
        providerID: "test",
        path: { cwd: directory, root: directory },
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        time: { created: 2, completed: 3 },
      },
      parts: [
        {
          id: "prt_visible_reasoning",
          sessionID,
          messageID: assistantID,
          type: "reasoning",
          text: "expanded reasoning visible in the session",
          time: { start: 2, end: 3 },
        },
        {
          id: "prt_visible_tool",
          sessionID,
          messageID: assistantID,
          type: "tool",
          callID: "call_visible_tool",
          tool: "bash",
          state: {
            status: "completed",
            input: { command: "printf visible tool output" },
            output: "visible tool output",
            title: "Print output",
            metadata: {},
            time: { start: 2, end: 3 },
          },
        },
      ],
    },
  ]
  const app = await startSession({ queue: [], messages })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("already in the transcript")), "session")
    await app.renderer.mockInput.typeText("rewrite from visible session")
    app.renderer.mockInput.pressKey("e", { ctrl: true })
    await until(() => app.enhancements.length === 1, "the first enhancement request")
    expect(app.enhancements[0]!.context).toContain("visible tool output")
    expect(app.enhancements[0]!.context).not.toContain("expanded reasoning visible in the session")
    expect(app.enhancements[0]!.context).toContain("## Assistant (Build · Model")

    app.dispatch("session.toggle.thinking")
    app.dispatch("session.toggle.actions")
    await app.renderer.mockInput.pressKey("e", { ctrl: true })
    await until(() => app.enhancements.length === 2, "the second enhancement request")
    expect(app.enhancements[1]!.context).toContain("expanded reasoning visible in the session")
    expect(app.enhancements[1]!.context).not.toContain("visible tool output")
    expect(app.prompts).toHaveLength(0)
  } finally {
    await app.stop()
  }
}, 20_000)

test("ctrl+e context follows each reasoning and tool part's rendered expansion", async () => {
  const assistantID = "msg_part_projection"
  const shellOutput = Array.from({ length: 12 }, (_, index) =>
    index === 11 ? `shell line ${index + 1} shell-tail-visible-token` : `shell line ${index + 1}`,
  ).join("\n")
  const genericOutput = Array.from({ length: 5 }, (_, index) =>
    index === 4 ? `generic line ${index + 1} generic-tail-visible-token` : `generic line ${index + 1}`,
  ).join("\n")
  const messages = [
    transcript,
    {
      info: {
        id: assistantID,
        sessionID,
        role: "assistant",
        parentID: transcript.info.id,
        mode: "primary",
        agent: "build",
        modelID: "model",
        providerID: "test",
        path: { cwd: directory, root: directory },
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        time: { created: 2, completed: 3 },
      },
      parts: [
        {
          id: "prt_local_reasoning",
          sessionID,
          messageID: assistantID,
          type: "reasoning",
          text: "**Local thought**\n\nexpanded reasoning visible in the session",
          time: { start: 2, end: 3 },
        },
        {
          id: "prt_shell_projection",
          sessionID,
          messageID: assistantID,
          type: "tool",
          callID: "call_shell_projection",
          tool: "bash",
          state: {
            status: "completed",
            input: { command: "printf shell-output" },
            output: "state output is not rendered by Shell",
            title: "Print output",
            metadata: { output: shellOutput },
            time: { start: 2, end: 3 },
          },
        },
        {
          id: "prt_generic_projection",
          sessionID,
          messageID: assistantID,
          type: "tool",
          callID: "call_generic_projection",
          tool: "custom_tool",
          state: {
            status: "completed",
            input: { query: "generic tool title" },
            output: genericOutput,
            title: "Custom tool",
            metadata: {},
            time: { start: 2, end: 3 },
          },
        },
      ],
    },
  ]
  const app = await startSession({ queue: [], messages })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("shell line 1")), "rendered shell output")
    await app.renderer.mockInput.typeText("rewrite from the visible parts")
    app.renderer.mockInput.pressKey("e", { ctrl: true })
    await until(() => app.enhancements.length === 1, "collapsed part context")
    expect(app.enhancements[0]!.context).toContain("shell line 1")
    expect(app.enhancements[0]!.context).not.toContain("shell-tail-visible-token")
    expect(app.enhancements[0]!.context).not.toContain("state output is not rendered by Shell")
    expect(app.enhancements[0]!.context).not.toContain("expanded reasoning visible in the session")
    expect(app.enhancements[0]!.context).not.toContain("generic line 1")
    expect(app.enhancements[0]!.context).not.toContain("generic-tail-visible-token")

    await clickVisible(app, "Thought: Local thought")
    await clickVisible(app, "Click to expand")
    await until(async () => {
      const frame = (await screen(app)).join("\n")
      return frame.includes("expanded reasoning visible in the session") && frame.includes("shell-tail-visible-token")
    }, "expanded reasoning and shell output")
    app.dispatch("prompt.enhance")
    await until(() => app.enhancements.length === 2, "expanded part context")
    expect(app.enhancements[1]!.context).toContain("expanded reasoning visible in the session")
    expect(app.enhancements[1]!.context).toContain("shell-tail-visible-token")
    expect(app.enhancements[1]!.context).not.toContain("state output is not rendered by Shell")
    expect(app.enhancements[1]!.context).not.toContain("generic-tail-visible-token")

    app.dispatch("session.toggle.generic_tool_output")
    await until(async () => (await screen(app)).some((line) => line.includes("generic line 1")), "generic tool output")
    expect((await screen(app)).join("\n")).not.toContain("generic-tail-visible-token")
    app.dispatch("prompt.enhance")
    await until(() => app.enhancements.length === 3, "collapsed generic part context")
    expect(app.enhancements[2]!.context).toContain("generic line 1")
    expect(app.enhancements[2]!.context).not.toContain("generic-tail-visible-token")
    await clickVisible(app, "Click to expand")
    await until(
      async () => (await screen(app)).some((line) => line.includes("generic-tail-visible-token")),
      "expanded generic output",
    )
    app.dispatch("prompt.enhance")
    await until(() => app.enhancements.length === 4, "expanded generic part context")
    expect(app.enhancements[3]!.context).toContain("generic-tail-visible-token")
    expect(app.prompts).toHaveLength(0)
  } finally {
    await app.stop()
  }
}, 20_000)

test("ctrl+e context includes visible tool errors and diagnostics", async () => {
  const assistantID = "msg_tool_error_projection"
  const diagnosticPath = path.join(directory, "visible-diagnostic.ts")
  const messages = [
    transcript,
    {
      info: {
        id: assistantID,
        sessionID,
        role: "assistant",
        parentID: transcript.info.id,
        mode: "primary",
        agent: "build",
        modelID: "model",
        providerID: "test",
        path: { cwd: directory, root: directory },
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        time: { created: 2, completed: 3 },
      },
      parts: [
        {
          id: "prt_inline_tool_error",
          sessionID,
          messageID: assistantID,
          type: "tool",
          callID: "call_inline_tool_error",
          tool: "webfetch",
          state: {
            status: "error",
            input: { url: "https://example.test" },
            error: "expanded inline tool error token",
            metadata: {},
            time: { start: 2, end: 3 },
          },
        },
        {
          id: "prt_block_tool_error",
          sessionID,
          messageID: assistantID,
          type: "tool",
          callID: "call_block_tool_error",
          tool: "bash",
          state: {
            status: "error",
            input: { command: "failing-command" },
            error: "visible BlockTool error token",
            metadata: { output: "visible shell output token" },
            time: { start: 2, end: 3 },
          },
        },
        {
          id: "prt_diagnostic_tool",
          sessionID,
          messageID: assistantID,
          type: "tool",
          callID: "call_diagnostic_tool",
          tool: "write",
          state: {
            status: "completed",
            input: { filePath: diagnosticPath, content: "const value = 1" },
            output: "write output",
            title: "Write diagnostic file",
            metadata: {
              diagnostics: {
                [diagnosticPath]: [
                  {
                    severity: 1,
                    range: { start: { line: 0, character: 0 } },
                    message: "visible tool diagnostic token",
                  },
                ],
              },
            },
            time: { start: 2, end: 3 },
          },
        },
        {
          id: "prt_apply_patch_error",
          sessionID,
          messageID: assistantID,
          type: "tool",
          callID: "call_apply_patch_error",
          tool: "apply_patch",
          state: {
            status: "error",
            input: { patch: "*** Begin Patch\n*** End Patch" },
            error: "patch rejected",
            metadata: {},
            time: { start: 2, end: 3 },
          },
        },
        {
          id: "prt_todo_write_error",
          sessionID,
          messageID: assistantID,
          type: "tool",
          callID: "call_todo_write_error",
          tool: "todowrite",
          state: {
            status: "error",
            input: { todos: [{ content: "finish review", status: "in_progress" }] },
            error: "todo update rejected",
            metadata: {},
            time: { start: 2, end: 3 },
          },
        },
      ],
    },
  ]
  const app = await startSession({ queue: [], messages })
  try {
    await until(async () => {
      const frame = (await screen(app)).join("\n")
      return (
        frame.includes("visible BlockTool error token") &&
        frame.includes("visible tool diagnostic token") &&
        frame.includes("Patch failed") &&
        frame.includes("Todo update failed")
      )
    }, "visible block error and diagnostics")
    await app.renderer.mockInput.typeText("rewrite from visible tool errors")
    app.renderer.mockInput.pressKey("e", { ctrl: true })
    await until(() => app.enhancements.length === 1, "context with visible errors")
    expect(app.enhancements[0]!.context).toContain("visible BlockTool error token")
    expect(app.enhancements[0]!.context).toContain("visible shell output token")
    expect(app.enhancements[0]!.context).toContain("Error [1:1] visible tool diagnostic token")
    expect(app.enhancements[0]!.context).toContain("Patch failed")
    expect(app.enhancements[0]!.context).toContain("Todo update failed")
    expect(app.enhancements[0]!.context).not.toContain("expanded inline tool error token")

    await clickVisible(app, "WebFetch https://example.test")
    await until(
      async () => (await screen(app)).some((line) => line.includes("expanded inline tool error token")),
      "expanded inline error",
    )
    app.dispatch("prompt.enhance")
    await until(() => app.enhancements.length === 2, "context with the expanded inline error")
    expect(app.enhancements[1]!.context).toContain("expanded inline tool error token")
    expect(app.prompts).toHaveLength(0)
  } finally {
    await app.stop()
  }
}, 20_000)

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
    await until(
      async () => (await screen(app)).some((line) => line.includes("Failed to enhance prompt")),
      "the error toast",
    )
    const frame = (await screen(app)).join("\n")
    expect(frame).toContain("keep this draft")
    expect(app.prompts).toHaveLength(0)
  } finally {
    await app.stop()
  }
}, 20_000)

test("prompt enhancement discards a response after the draft changes", async () => {
  let answer!: (response: Response) => void
  const pending = new Promise<Response>((resolve) => (answer = resolve))
  const app = await startSession({ queue: [], enhance: () => pending })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("already in the transcript")), "session")
    await app.renderer.mockInput.typeText("draft before enhancement")
    app.renderer.mockInput.pressKey("e", { ctrl: true })
    await until(() => app.paths.some((path) => path.endsWith("/prompt/enhance")), "the rewrite request")
    await app.renderer.mockInput.typeText(" with a newer edit")
    app.renderer.mockInput.pressKey("a", { ctrl: true })
    app.renderer.mockInput.pressKey("e", { ctrl: true, shift: true })
    await app.renderer.mockInput.typeText("draft before enhancement")
    answer(json("stale rewritten draft"))
    await until(
      async () => (await screen(app)).some((line) => line.includes("Prompt changed while enhancement was running")),
      "the stale response notice",
    )

    const frame = (await screen(app)).join("\n")
    expect(frame).toContain("draft before enhancement")
    expect(frame).not.toContain("stale rewritten draft")
    expect(app.prompts).toHaveLength(0)
  } finally {
    await app.stop()
  }
}, 20_000)

test("prompt enhancement discards a response after switching sessions", async () => {
  let answer!: (response: Response) => void
  const pending = new Promise<Response>((resolve) => (answer = resolve))
  const app = await startSession({ queue: [], enhance: () => pending })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("already in the transcript")), "session")
    await app.renderer.mockInput.typeText("session one draft")
    app.renderer.mockInput.pressKey("e", { ctrl: true })
    await until(() => app.paths.some((path) => path.endsWith("/prompt/enhance")), "the rewrite request")
    app.selectSession("ses_other_session")
    await until(
      async () => (await screen(app)).some((line) => line.includes("ses_other_session")),
      "the selected session",
    )
    answer(json("stale first-session rewrite"))
    await until(
      async () => (await screen(app)).some((line) => line.includes("Prompt changed while enhancement was running")),
      "the stale response notice",
    )

    const frame = (await screen(app)).join("\n")
    expect(frame).toContain("session one draft")
    expect(frame).not.toContain("stale first-session rewrite")
    expect(app.prompts).toHaveLength(0)
  } finally {
    await app.stop()
  }
}, 20_000)

// Lines of the pending row mark each prompt QUEUED; any other line holding the
// text is the composer's.
const queuedLines = (lines: string[], text: string) =>
  lines.filter((line) => line.includes("QUEUED") && line.includes(text))

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

test("UP recall keeps an unmarked queued prompt outside composer authorship", async () => {
  const text = "prompt from another client"
  const app = await startSession({ queue: [pendingItem("que_unmarked", text, { delivery: "queue" })] })
  try {
    await until(async () => (await screen(app)).some((line) => line.includes("1 pending")), "the pending prompt")
    app.renderer.mockInput.pressArrow("up")
    await until(async () => composerLines(await screen(app), text).length === 1, "the recalled prompt")
    app.renderer.mockInput.pressEnter()
    await until(() => app.prompts.length === 1, "the resend")

    expect(app.prompts[0]!.parts.at(-1)).toMatchObject({
      text,
      metadata: { [SessionPromptQueue.TuiPromptAppendMetadataKey]: true },
    })
  } finally {
    app.prompts.forEach((prompt) => prompt.answer(json(transcript)))
    await app.stop()
  }
}, 20_000)

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
