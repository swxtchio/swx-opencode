/** @jsxImportSource @opentui/solid */
import { createDefaultOpenTuiKeymap } from "@opentui/keymap/opentui"
import { expect, test } from "bun:test"
import { createSignal, For, onCleanup, Show } from "solid-js"
import { useRenderer } from "@opentui/solid"
import type { AssistantMessage, GlobalEvent, Part, ReasoningPart } from "@opencode-ai/sdk/v2"
import { resolve, TuiConfigProvider } from "../../../src/config"
import { LocalProvider } from "../../../src/context/local"
import { LocationProvider } from "../../../src/context/location"
import { RouteProvider } from "../../../src/context/route"
import { ThemeProvider } from "../../../src/context/theme"
import { useSync } from "../../../src/context/sync"
import { OpencodeKeymapProvider, registerOpencodeKeymap } from "../../../src/keymap"
import { AssistantMessageView, SessionContext } from "../../../src/routes/session"
import { DialogProvider } from "../../../src/ui/dialog"
import { ToastProvider } from "../../../src/ui/toast"
import { directory } from "../../fixture/tui-sdk"
import { tmpdir } from "../../fixture/fixture"
import { SPINNER_FRAMES } from "../../../src/component/spinner"
import { mount, wait } from "../../cli/cmd/tui/sync-fixture"

const sessionID = "ses_opaque_render"

const assistant = {
  sessionID,
  role: "assistant" as const,
  agent: "build",
  modelID: "model",
  providerID: "test",
  mode: "build",
  parentID: "msg_user",
  path: { cwd: directory, root: directory },
  cost: 0,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  time: { created: 1 },
}

function message(id: string, extra: Partial<AssistantMessage> = {}): AssistantMessage {
  return { ...assistant, id, ...extra }
}

function partID(messageID: string, index: number) {
  return `prt_${messageID}_${String(index).padStart(3, "0")}`
}

// Shapes follow session/processor.ts: reasoning-start creates the part with empty
// text and the provider metadata, and finishReasoning adds time.end.
function encrypted(messageID: string, index: number, time: ReasoningPart["time"]): ReasoningPart {
  return {
    id: partID(messageID, index),
    sessionID,
    messageID,
    type: "reasoning",
    text: "",
    metadata: { openai: { itemId: `rs_${index}` } },
    time,
  }
}

function text(messageID: string, index: number, value: string): Part {
  return { id: partID(messageID, index), sessionID, messageID, type: "text", text: value }
}

function tool(messageID: string, index: number): Part {
  return {
    id: partID(messageID, index),
    sessionID,
    messageID,
    type: "tool",
    callID: `call_${index}`,
    tool: "bash",
    state: {
      status: "completed",
      input: { command: "ls" },
      output: "ok",
      title: "ls",
      metadata: {},
      time: { start: 1, end: 2 },
    },
  }
}

function global(payload: GlobalEvent["payload"]): GlobalEvent {
  return { directory: "/tmp/other", project: "proj_test", payload }
}

function partUpdated(part: Part) {
  return global({
    id: crypto.randomUUID(),
    type: "message.part.updated",
    properties: { sessionID, part, time: 0 },
  })
}

// Mirrors the session route's message loop: each assistant message reads its parts from the sync store.
function Transcript(props: { messages: AssistantMessage[] }) {
  const sync = useSync()
  const renderer = useRenderer()
  const keymap = createDefaultOpenTuiKeymap(renderer)
  const config = resolve({}, { terminalSuspend: false })
  onCleanup(registerOpencodeKeymap(keymap, renderer, config))
  const sessionContext = {
    width: 100,
    sessionID,
    conceal: () => false,
    thinkingMode: () => "show" as const,
    showThinking: () => true,
    showTimestamps: () => false,
    showDetails: () => true,
    showGenericToolOutput: () => false,
    diffWrapMode: () => "word" as const,
    providers: () => new Map(),
    sync,
    tui: config,
  }

  return (
    <TuiConfigProvider config={config}>
      <ThemeProvider mode="dark">
        <RouteProvider initialRoute={{ type: "session", sessionID }}>
          <OpencodeKeymapProvider keymap={keymap}>
            <ToastProvider>
              <DialogProvider>
                <LocalProvider>
                  <LocationProvider>
                    <SessionContext.Provider value={sessionContext}>
                      <box flexDirection="column">
                        <For each={props.messages}>
                          {(item) => (
                            <AssistantMessageView
                              last={false}
                              message={item}
                              parts={sync.data.part[item.id] ?? []}
                              modelMessages={undefined}
                            />
                          )}
                        </For>
                      </box>
                    </SessionContext.Provider>
                  </LocationProvider>
                </LocalProvider>
              </DialogProvider>
            </ToastProvider>
          </OpencodeKeymapProvider>
        </RouteProvider>
      </ThemeProvider>
    </TuiConfigProvider>
  )
}

function thoughtLines(frame: string) {
  return frame.split("\n").filter((line) => line.includes("Thought"))
}

// Polls the rendered frame until it shows what the test expects, bounded so a wrong frame still fails.
async function frameWhen(app: Awaited<ReturnType<typeof mount>>["app"], ready: (frame: string) => boolean) {
  const start = Date.now()
  while (true) {
    await app.renderOnce()
    const frame = app.captureCharFrame()
    if (ready(frame)) return frame
    if (Date.now() - start > 2000) throw new Error(`timed out waiting for the expected frame:\n${frame}`)
    await Bun.sleep(10)
  }
}

test("history: a loaded run of encrypted parts renders one count line", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")
  const [shown, setShown] = createSignal(false)
  const history = message("msg_history")
  const parts = [
    encrypted("msg_history", 1, { start: 1000, end: 2000 }),
    encrypted("msg_history", 2, { start: 3000, end: 5000 }),
    encrypted("msg_history", 3, { start: 6000, end: 9000 }),
    encrypted("msg_history", 4, { start: 9500, end: 11300 }),
  ]
  const { app, emit, sync } = await mount(undefined, tmp.path, true, () => (
    <Show when={shown()}>
      <Transcript messages={[history]} />
    </Show>
  ))

  try {
    emit(global({ id: "evt_history_message", type: "message.updated", properties: { sessionID, info: history } }))
    for (const part of parts) {
      emit(partUpdated(part))
    }
    await wait(() => sync.data.part[history.id]?.length === parts.length)

    setShown(true)
    const frame = await frameWhen(app, (rendered) => thoughtLines(rendered).length > 0)

    const lines = thoughtLines(frame)
    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain("Thought 4 · 7.8s")
  } finally {
    app.renderer.destroy()
  }
})

test("live: a run updates in place as parts arrive", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")
  const live = message("msg_live")
  const { app, emit, sync } = await mount(undefined, tmp.path, true, () => <Transcript messages={[live]} />)

  try {
    emit(global({ id: "evt_live_message", type: "message.updated", properties: { sessionID, info: live } }))

    const first = encrypted("msg_live", 1, { start: 1000, end: 2000 })
    emit(partUpdated(first))
    await wait(() => sync.data.part[live.id]?.length === 1)
    const single = await frameWhen(app, (rendered) => thoughtLines(rendered).length > 0)
    expect(thoughtLines(single)).toEqual([expect.stringContaining("Thought · 1.0s")])

    const second = encrypted("msg_live", 2, { start: 3000, end: 5000 })
    emit(partUpdated(second))
    await wait(() => sync.data.part[live.id]?.length === 2)
    const pair = thoughtLines(await frameWhen(app, (rendered) => rendered.includes("Thought 2 · 3.0s")))
    expect(pair).toHaveLength(1)

    emit(global({ id: "evt_live_busy", type: "session.status", properties: { sessionID, status: { type: "busy" } } }))
    const third = encrypted("msg_live", 3, { start: 6000, end: 9000 })
    const inFlight = { ...third, time: { start: 6000 } }
    emit(partUpdated(inFlight))
    await wait(() => sync.data.part[live.id]?.length === 3)
    const pending = thoughtLines(await frameWhen(app, (rendered) => rendered.includes("Thought 3 · 3.0s")))
    expect(pending).toHaveLength(1)
    expect(SPINNER_FRAMES.some((glyph) => pending[0].includes(glyph))).toBe(true)

    emit(partUpdated(third))
    await wait(() => {
      const stored = sync.data.part[live.id]?.find((part) => part.id === third.id)
      return stored?.type === "reasoning" && stored.time.end === 9000
    })
    const done = thoughtLines(await frameWhen(app, (rendered) => rendered.includes("Thought 3 · 6.0s")))
    expect(done).toHaveLength(1)
    expect(SPINNER_FRAMES.some((glyph) => done[0].includes(glyph))).toBe(false)
  } finally {
    app.renderer.destroy()
  }
})

test("a tool call and visible text each end the run and start a new line", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")
  const turn = message("msg_breaks")
  const parts: Part[] = [
    encrypted("msg_breaks", 1, { start: 1000, end: 2000 }),
    encrypted("msg_breaks", 2, { start: 3000, end: 5000 }),
    tool("msg_breaks", 3),
    encrypted("msg_breaks", 4, { start: 6000, end: 10000 }),
    encrypted("msg_breaks", 5, { start: 10500, end: 11000 }),
    text("msg_breaks", 6, "Here is the answer."),
    encrypted("msg_breaks", 7, { start: 12000, end: 12500 }),
  ]
  const { app, emit, sync } = await mount(undefined, tmp.path, true, () => <Transcript messages={[turn]} />)

  try {
    emit(global({ id: "evt_breaks_message", type: "message.updated", properties: { sessionID, info: turn } }))
    for (const part of parts) {
      emit(partUpdated(part))
    }
    await wait(() => sync.data.part[turn.id]?.length === parts.length)

    const frame = await frameWhen(
      app,
      (rendered) => rendered.includes("Thought · 500ms") && rendered.includes("Here is the answer."),
    )
    const lines = thoughtLines(frame)
    expect(lines).toHaveLength(3)
    expect(lines[0]).toContain("Thought 2 · 3.0s")
    expect(lines[1]).toContain("Thought 2 · 4.5s")
    expect(lines[2]).toContain("Thought · 500ms")
    const all = frame.split("\n")
    const firstRun = all.findIndex((line) => line.includes("Thought 2 · 3.0s"))
    const secondRun = all.findIndex((line) => line.includes("Thought 2 · 4.5s"))
    const lastRun = all.findIndex((line) => line.includes("Thought · 500ms"))
    const answer = all.findIndex((line) => line.includes("Here is the answer."))
    expect(all.slice(firstRun + 1, secondRun).some((line) => line.includes("ls"))).toBe(true)
    expect(secondRun).toBeLessThan(answer)
    expect(answer).toBeLessThan(lastRun)
  } finally {
    app.renderer.destroy()
  }
})

test("plain reasoning between two runs keeps its own header and body", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")
  const turn = message("msg_plain")
  const plain: ReasoningPart = {
    id: partID("msg_plain", 3),
    sessionID,
    messageID: "msg_plain",
    type: "reasoning",
    text: "Checking the tests first.",
    time: { start: 5000, end: 6000 },
  }
  const parts: Part[] = [
    encrypted("msg_plain", 1, { start: 1000, end: 2000 }),
    encrypted("msg_plain", 2, { start: 3000, end: 5000 }),
    plain,
    encrypted("msg_plain", 4, { start: 6500, end: 7000 }),
  ]
  const { app, emit, sync } = await mount(undefined, tmp.path, true, () => <Transcript messages={[turn]} />)

  try {
    emit(global({ id: "evt_plain_message", type: "message.updated", properties: { sessionID, info: turn } }))
    for (const part of parts) {
      emit(partUpdated(part))
    }
    await wait(() => sync.data.part[turn.id]?.length === parts.length)

    const frame = await frameWhen(
      app,
      (rendered) => rendered.includes("Checking the tests first.") && rendered.includes("Thought · 500ms"),
    )
    const lines = thoughtLines(frame)
    expect(lines).toHaveLength(3)
    expect(lines[0]).toContain("Thought 2 · 3.0s")
    expect(lines[1]).toContain("Thought: 1.0s")
    expect(lines[2]).toContain("Thought · 500ms")
    const all = frame.split("\n")
    const plainHeader = all.findIndex((line) => line.includes("Thought: 1.0s"))
    const body = all.findIndex((line) => line.includes("Checking the tests first."))
    expect(body).toBeGreaterThan(plainHeader)
  } finally {
    app.renderer.destroy()
  }
})

test("a message error ends the run, and the next message starts a new line", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")
  const failed = message("msg_failed", { error: { name: "UnknownError", data: { message: "Upstream failed" } } })
  const next = message("msg_next")
  const failedParts = [
    encrypted("msg_failed", 1, { start: 1000, end: 2000 }),
    encrypted("msg_failed", 2, { start: 3000, end: 5000 }),
  ]
  const nextParts = [encrypted("msg_next", 1, { start: 6000, end: 7000 })]
  const { app, emit, sync } = await mount(undefined, tmp.path, true, () => <Transcript messages={[failed, next]} />)

  try {
    for (const info of [failed, next]) {
      emit(global({ id: `evt_${info.id}`, type: "message.updated", properties: { sessionID, info } }))
    }
    for (const part of [...failedParts, ...nextParts]) {
      emit(partUpdated(part))
    }
    await wait(() => sync.data.part[next.id]?.length === nextParts.length)

    const frame = await frameWhen(app, (rendered) => rendered.includes("Upstream failed"))
    const lines = thoughtLines(frame)
    expect(lines).toHaveLength(2)
    expect(lines[0]).toContain("Thought 2 · 3.0s")
    expect(lines[1]).toContain("Thought · 1.0s")
    expect(frame.indexOf(lines[0])).toBeLessThan(frame.indexOf("Upstream failed"))
    expect(frame.indexOf("Upstream failed")).toBeLessThan(frame.indexOf(lines[1]))
  } finally {
    app.renderer.destroy()
  }
})
