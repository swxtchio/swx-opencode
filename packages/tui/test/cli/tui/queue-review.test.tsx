/** @jsxImportSource @opentui/solid */
import { createDefaultOpenTuiKeymap } from "@opentui/keymap/opentui"
import { testRender, useRenderer } from "@opentui/solid"
import { afterEach, expect, jest, test } from "bun:test"
import type { GlobalEvent, SessionPromptQueueItem } from "@opencode-ai/sdk/v2"
import { mkdir } from "node:fs/promises"
import path from "node:path"
import { onCleanup, onMount } from "solid-js"
import { tmpdir } from "../../fixture/fixture"
import { createTuiResolvedConfig } from "../../fixture/tui-runtime"
import { TestTuiContexts } from "../../fixture/tui-environment"
import { createEventSource, createFetch, directory, json } from "../../fixture/tui-sdk"
import { ArgsProvider } from "../../../src/context/args"
import { ExitProvider } from "../../../src/context/exit"
import { KVProvider } from "../../../src/context/kv"
import { PermissionProvider } from "../../../src/context/permission"
import { ProjectProvider } from "../../../src/context/project"
import { SDKProvider } from "../../../src/context/sdk"
import { SyncProvider, useSync } from "../../../src/context/sync"
import { ThemeProvider } from "../../../src/context/theme"
import { TuiConfigProvider } from "../../../src/config"
import { OpencodeKeymapProvider, registerOpencodeKeymap } from "../../../src/keymap"
import { DialogProvider, useDialog } from "../../../src/ui/dialog"
import { Toast, ToastProvider } from "../../../src/ui/toast"
import { PromptQueueRow } from "../../../src/component/prompt/queue"
import { DialogQueue, REVIEW_IDLE_MS } from "../../../src/component/dialog-queue"

// The review view of pending prompts (swxtchio/swx-opencode#212), rendered with
// the production sync, SDK and dialog providers against a scripted server. What
// the server does with each request is covered in packages/opencode.

const sessionID = "ses_queue_review"
const session = { id: sessionID, title: "review", time: { created: 0, updated: 0 }, version: "1", directory }

function item(id: string, delivery: "steer" | "queue", text: string, seq: number): SessionPromptQueueItem {
  return {
    id,
    sessionID,
    seq,
    delivery,
    input: { parts: [{ type: "text", text }] },
    time: { created: seq },
    origins: [{ time: seq }],
  }
}

const userMessage = {
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

async function wait(fn: () => boolean | Promise<boolean>, timeout = 3000) {
  const start = Date.now()
  while (!(await fn())) {
    if (Date.now() - start > timeout) throw new Error("timed out waiting for condition")
    await Bun.sleep(10)
  }
}

type QueueRequest = { method: string; path: string; body: Record<string, unknown> | undefined }

// Answers a queue request in place of the scripted server, e.g. late or failing.
type Serve = (path: string) => Promise<Response> | undefined

async function mount(
  initial: SessionPromptQueueItem[],
  serve?: Serve,
  // Runs before the session's queue is first read.
  beforeSync?: (sync: ReturnType<typeof useSync>) => void,
) {
  await using tmp = await tmpdir()
  const state = path.join(tmp.path, "state")
  await mkdir(state, { recursive: true })
  await Bun.write(path.join(state, "kv.json"), "{}")
  const events = createEventSource()
  const queue = { items: initial }
  const requests: QueueRequest[] = []
  const base = createFetch((url) => {
    if (url.pathname === `/session/${sessionID}`) return json(session)
    if (url.pathname === `/session/${sessionID}/message`) return json([userMessage])
    if (url.pathname === `/session/${sessionID}/todo`) return json([])
    if (url.pathname === `/session/${sessionID}/diff`) return json([])
    const served = serve?.(url.pathname)
    if (served) return served
    if (url.pathname === `/session/${sessionID}/queue/release`) return new Response(null, { status: 204 })
    if (url.pathname === `/session/${sessionID}/queue/remove`) return json(queue.items[0])
    if (url.pathname.startsWith(`/session/${sessionID}/queue`)) return json(queue.items)
  })
  const fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(input, init)
    const url = new URL(request.url)
    if (url.pathname.startsWith(`/session/${sessionID}/queue`)) {
      const text = request.method === "GET" ? "" : await request.clone().text()
      requests.push({ method: request.method, path: url.pathname, body: text ? JSON.parse(text) : undefined })
    }
    return base.fetch(request)
  }) as typeof globalThis.fetch

  let sync!: ReturnType<typeof useSync>
  let dialog!: ReturnType<typeof useDialog>
  let ready!: () => void
  const mounted = new Promise<void>((resolve) => (ready = resolve))

  function Probe() {
    sync = useSync()
    dialog = useDialog()
    onMount(ready)
    return (
      <box>
        <PromptQueueRow sessionID={sessionID} />
        <Toast />
      </box>
    )
  }

  function Harness() {
    const renderer = useRenderer()
    const keymap = createDefaultOpenTuiKeymap(renderer)
    const config = createTuiResolvedConfig({ leader_timeout: 1000 })
    onCleanup(registerOpencodeKeymap(keymap, renderer, config))
    return (
      <TestTuiContexts directory={tmp.path} paths={{ home: tmp.path, state, worktree: tmp.path }}>
        <OpencodeKeymapProvider keymap={keymap}>
          <TuiConfigProvider config={config}>
            <ArgsProvider>
              <KVProvider>
                <ThemeProvider mode="dark">
                  <ToastProvider>
                    <SDKProvider url="http://test" directory={directory} fetch={fetch} events={events.source}>
                      <PermissionProvider>
                        <ProjectProvider>
                          <ExitProvider exit={() => {}}>
                            <SyncProvider>
                              <DialogProvider>
                                <Probe />
                              </DialogProvider>
                            </SyncProvider>
                          </ExitProvider>
                        </ProjectProvider>
                      </PermissionProvider>
                    </SDKProvider>
                  </ToastProvider>
                </ThemeProvider>
              </KVProvider>
            </ArgsProvider>
          </TuiConfigProvider>
        </OpencodeKeymapProvider>
      </TestTuiContexts>
    )
  }

  const app = await testRender(() => <Harness />, { kittyKeyboard: true, width: 100, height: 40 })
  await mounted
  await wait(() => sync.status === "complete")
  beforeSync?.(sync)
  await sync.session.sync(sessionID)

  const frame = async () => {
    await app.renderOnce()
    return app.captureCharFrame()
  }
  const find = async (needle: string) => {
    const lines = (await frame()).split("\n")
    const y = lines.findIndex((line) => line.includes(needle))
    if (y < 0) throw new Error(`"${needle}" is not on screen`)
    return { x: lines[y]!.indexOf(needle), y }
  }
  return {
    app,
    sync,
    dialog,
    requests,
    frame,
    queued: (method: string) => requests.filter((request) => request.path.endsWith(`/queue/${method}`)),
    // The server publishes every change to the pending list.
    publish(items: SessionPromptQueueItem[]) {
      queue.items = items
      events.emit({
        directory,
        project: "proj_test",
        payload: { id: `evt_${Math.random()}`, type: "session.queue.updated", properties: { sessionID, items } },
      } as GlobalEvent)
    },
    async open() {
      const holds = requests.filter((request) => request.path.endsWith("/queue/hold")).length
      const row = await find("pending")
      await app.mockMouse.click(row.x, row.y)
      await wait(() => requests.filter((request) => request.path.endsWith("/queue/hold")).length > holds)
      expect(dialog.stack.length).toBe(1)
    },
    async click(needle: string) {
      const target = await find(needle)
      await app.mockMouse.click(target.x, target.y)
    },
  }
}

let cleanup: (() => void) | undefined
afterEach(() => {
  jest.useRealTimers()
  cleanup?.()
  cleanup = undefined
})

async function setup(
  initial: SessionPromptQueueItem[],
  serve?: Serve,
  beforeSync?: (sync: ReturnType<typeof useSync>) => void,
) {
  const view = await mount(initial, serve, beforeSync)
  cleanup = () => view.app.renderer.destroy()
  return view
}

const steer = item("que_1", "steer", "steer the running turn", 2)
const queued = item("que_2", "queue", "queued for later", 3)

test("the row under the prompt counts pending prompts by category, not transcript messages", async () => {
  const view = await setup([steer, queued])
  expect(view.sync.data.message[sessionID]?.length).toBe(1)
  expect(await view.frame()).toContain("2 pending · 1 steer · 1 queue")

  view.publish([queued])
  await wait(() => view.sync.data.session_queue[sessionID]?.length === 1)
  expect(await view.frame()).toContain("1 pending · 0 steer · 1 queue")

  view.publish([])
  await wait(() => view.sync.data.session_queue[sessionID]?.length === 0)
  expect(await view.frame()).not.toContain("pending")
})

test("activating the row opens the pending prompts in delivery order with their categories, held until closed", async () => {
  const later = item("que_3", "queue", "queued after that", 4)
  const view = await setup([steer, queued, later])
  await view.open()

  const lines = (await view.frame()).split("\n")
  const listed = [
    ["steer the running turn", "steer"],
    ["queued for later", "queue"],
    ["queued after that", "queue"],
  ].map(([text, category]) => {
    // The row under the prompt lists them too; the view is drawn below it.
    const y = lines.findLastIndex((line) => line.includes(text!))
    expect(lines[y]).toContain(category!)
    return y
  })
  expect(listed.every((y) => y >= 0)).toBe(true)
  expect(listed).toEqual(listed.toSorted((a, b) => a - b))

  const [hold] = view.queued("hold")
  expect(hold?.method).toBe("POST")
  expect(view.queued("release")).toEqual([])

  view.app.mockInput.pressEscape()
  await wait(() => view.queued("release").length === 1)
  expect(view.dialog.stack.length).toBe(0)
  expect(view.queued("release")[0]?.body).toEqual(hold?.body)
})

test("every route that closes the view releases its hold", async () => {
  const view = await setup([steer])

  await view.open()
  view.dialog.clear()
  await wait(() => view.queued("release").length === 1)

  await view.open()
  view.dialog.replace(() => <text>another dialog</text>)
  await wait(() => view.queued("release").length === 2)
  view.dialog.clear()

  await view.open()
  // Opening the view by mouse leaves an empty selection, which the backdrop
  // takes for the end of a drag; a dialog opened by key starts without one.
  view.app.renderer.clearSelection()
  await view.app.mockMouse.click(2, 35)
  await wait(() => view.queued("release").length === 3)
  expect(view.queued("hold").map((request) => request.body)).toEqual(
    view.queued("release").map((request) => request.body),
  )
})

test("five minutes without activity close the view and release its hold; activity restarts that interval", async () => {
  const view = await setup([steer, queued])
  // Fake timers stop Bun.sleep, so this waits on the event loop's own turns.
  const settle = async (done: () => boolean) => {
    for (let turn = 0; turn < 1000 && !done(); turn++) await new Promise<void>((resolve) => queueMicrotask(resolve))
    expect(done()).toBe(true)
  }
  // Opened as the row opens it, since a mouse click waits on timers too.
  jest.useFakeTimers()
  DialogQueue.show(view.dialog, sessionID)
  await settle(() => view.queued("hold").length === 1)

  jest.advanceTimersByTime(REVIEW_IDLE_MS - 60_000)
  expect(view.dialog.stack.length).toBe(1)
  // Any key four minutes in, not only one that moves the selection, renews the
  // server's hold and restarts the interval.
  view.app.mockInput.pressKey("x")
  await settle(() => view.queued("hold").length === 2)
  jest.advanceTimersByTime(REVIEW_IDLE_MS - 1)
  expect(view.dialog.stack.length).toBe(1)
  expect(view.queued("release")).toEqual([])

  jest.advanceTimersByTime(1)
  await settle(() => view.queued("release").length === 1)
  expect(view.dialog.stack.length).toBe(0)
})

test("delete removes the selected pending prompt", async () => {
  const view = await setup([steer, queued])
  await view.open()
  view.app.mockInput.pressArrow("down")
  view.app.mockInput.pressKey("d", { ctrl: true })
  await wait(() => view.queued("remove").length === 1)
  expect(view.queued("remove")[0]?.body).toEqual({ id: queued.id })

  view.publish([steer])
  await wait(async () => !(await view.frame()).includes("queued for later"))
  expect(await view.frame()).toContain("steer the running turn")
})

test("reorder moves a pending prompt within its category and never past the other", async () => {
  const second = item("que_4", "queue", "second queued", 5)
  const view = await setup([steer, queued, second])
  await view.open()

  // The first row is a steer, and the next one a queued prompt: nothing to move past.
  view.app.mockInput.pressArrow("down", { meta: true })
  view.app.mockInput.pressArrow("down")
  view.app.mockInput.pressArrow("down")
  view.app.mockInput.pressArrow("up", { meta: true })
  await wait(() => view.queued("reorder").length === 1)
  expect(view.queued("reorder")[0]?.body).toEqual({ items: [second.id, queued.id] })
})

test("the consolidate button asks the server to consolidate the pending prompts", async () => {
  const view = await setup([steer, queued])
  await view.open()
  await view.click("consolidate")
  await wait(() => view.queued("consolidate").length === 1)
  view.app.mockInput.pressKey("o", { ctrl: true })
  await wait(() => view.queued("consolidate").length === 2)
})

// A response the test releases when it chooses.
function later() {
  let answer!: (response: Response) => void
  const response = new Promise<Response>((resolve) => (answer = resolve))
  return { response, answer }
}

// Fake timers stop Bun.sleep, so these wait on the event loop's own turns.
async function settle(done: () => boolean) {
  for (let turn = 0; turn < 1000 && !done(); turn++) await new Promise<void>((resolve) => queueMicrotask(resolve))
  expect(done()).toBe(true)
}

test("the view says delivery is paused, and allows edits, only once the server confirms the hold", async () => {
  const pending = later()
  let holds = 0
  const view = await setup([steer, queued], (path) =>
    path.endsWith("/queue/hold") && holds++ === 0 ? pending.response : undefined,
  )
  await view.open()
  await wait(async () => (await view.frame()).includes("pausing delivery"))
  expect(await view.frame()).not.toContain("(delivery paused)")
  view.app.mockInput.pressKey("d", { ctrl: true })
  view.app.mockInput.pressKey("o", { ctrl: true })

  // The confirmed hold's snapshot is the list the view shows.
  pending.answer(json([queued]))
  await wait(async () => (await view.frame()).includes("(delivery paused)"))
  await wait(async () => !(await view.frame()).includes("steer the running turn"))
  expect(view.queued("remove")).toEqual([])
  expect(view.queued("consolidate")).toEqual([])
  view.app.mockInput.pressKey("o", { ctrl: true })
  await wait(() => view.queued("consolidate").length === 1)
})

test("a hold the server refuses says delivery is not paused", async () => {
  const view = await setup([steer], (path) =>
    path.endsWith("/queue/hold") ? Promise.resolve(json({ name: "Unavailable" }, { status: 503 })) : undefined,
  )
  await view.open()
  await wait(async () => (await view.frame()).includes("delivery NOT paused"))
  expect(await view.frame()).toContain("Could not pause delivery")
})

test("a renewal the server refuses says delivery is not paused and locks edits until one is confirmed", async () => {
  let holds = 0
  const view = await setup([steer, queued], (path) =>
    path.endsWith("/queue/hold") && holds++ === 1
      ? Promise.resolve(json({ name: "Unavailable" }, { status: 503 }))
      : undefined,
  )
  const shows = async (text: string) => {
    for (let turn = 0; turn < 1000; turn++) {
      if ((await view.frame()).includes(text)) return
      await new Promise<void>((resolve) => queueMicrotask(resolve))
    }
    expect(await view.frame()).toContain(text)
  }
  jest.useFakeTimers()
  DialogQueue.show(view.dialog, sessionID)
  await settle(() => view.queued("hold").length === 1)
  await shows("(delivery paused)")

  // Activity a minute later renews the hold, and the server refuses.
  jest.advanceTimersByTime(60_000)
  view.app.mockInput.pressKey("x")
  await settle(() => view.queued("hold").length === 2)
  await shows("delivery NOT paused")
  view.app.mockInput.pressKey("d", { ctrl: true })
  view.app.mockInput.pressKey("o", { ctrl: true })
  expect(view.queued("remove")).toEqual([])
  expect(view.queued("consolidate")).toEqual([])

  // The next renewal it confirms pauses delivery again, and edits with it.
  jest.advanceTimersByTime(60_000)
  view.app.mockInput.pressKey("x")
  await settle(() => view.queued("hold").length === 3)
  await shows("(delivery paused)")
  view.app.mockInput.pressKey("o", { ctrl: true })
  await settle(() => view.queued("consolidate").length === 1)
})

test("a close while the hold is unanswered releases at once, and again when the hold lands", async () => {
  const pending = later()
  const view = await setup([steer], (path) => (path.endsWith("/queue/hold") ? pending.response : undefined))
  await view.open()
  view.app.mockInput.pressEscape()
  await wait(() => view.queued("release").length === 1)
  pending.answer(json([steer]))
  await wait(() => view.queued("release").length === 2)
})

test("a close with a renewal outstanding releases at once, and again when the renewal lands", async () => {
  const renewal = later()
  let holds = 0
  const view = await setup([steer], (path) =>
    path.endsWith("/queue/hold") && holds++ === 1 ? renewal.response : undefined,
  )
  jest.useFakeTimers()
  DialogQueue.show(view.dialog, sessionID)
  await settle(() => view.queued("hold").length === 1)
  jest.advanceTimersByTime(60_000)
  view.app.mockInput.pressKey("x")
  await settle(() => view.queued("hold").length === 2)

  view.app.mockInput.pressEscape()
  await settle(() => view.queued("release").length === 1)
  renewal.answer(json([steer]))
  await settle(() => view.queued("release").length === 2)
})

test("scrolling the review with the mouse wheel restarts the inactivity interval", async () => {
  const view = await setup([steer, queued])
  await view.open()
  const lines = (await view.frame()).split("\n")
  // The row under the prompt lists it too; the view is drawn below it.
  const y = lines.findLastIndex((line) => line.includes("queued for later"))
  const x = lines[y]!.indexOf("queued for later")
  view.dialog.clear()
  await wait(() => view.queued("release").length === 1)

  jest.useFakeTimers()
  DialogQueue.show(view.dialog, sessionID)
  await settle(() => view.queued("hold").length === 2)
  jest.advanceTimersByTime(REVIEW_IDLE_MS - 60_000)
  // Mouse events hit-test the last rendered frame, and the ages have ticked since.
  expect(await view.frame()).toContain("queued for later")
  void view.app.mockMouse.scroll(x, y, "down")
  jest.advanceTimersByTime(REVIEW_IDLE_MS - 1)
  expect(view.dialog.stack.length).toBe(1)
  jest.advanceTimersByTime(1)
  await settle(() => view.queued("release").length === 2)
  expect(view.dialog.stack.length).toBe(0)
})

test("each pending prompt shows when it was first queued, how long ago, its repeats and merged steps", async () => {
  const now = Date.now()
  const minutes = (count: number) => now - count * 60_000
  const repeated = {
    ...item("que_4", "queue", "check the build", 5),
    time: { created: minutes(5) },
    origins: [{ time: minutes(17), repeats: [{ time: minutes(5) }] }],
  }
  const merged = {
    ...item("que_5", "queue", "run lint:\n1. run lint\n2. --fix", 6),
    time: { created: minutes(30) },
    origins: [{ time: minutes(40) }, { time: minutes(30) }],
  }
  const view = await setup([repeated, merged])
  jest.useFakeTimers({ now })
  DialogQueue.show(view.dialog, sessionID)
  await settle(() => view.queued("hold").length === 1)
  // The row under the prompt lists them too; the view is drawn below it.
  const line = async (text: string) => (await view.frame()).split("\n").findLast((row) => row.includes(text)) ?? ""
  expect(await line("check the build")).toContain("17m ago · sent 2×")
  expect(await line("run lint:")).toContain("40m ago · 2 merged")

  // The ages count on while the view stays open.
  jest.advanceTimersByTime(60_000)
  expect(await line("check the build")).toContain("18m ago")
})

// swxtchio/swx-opencode#249: a prompt being sent shows until a list of the
// queue carries its request, whichever way that list arrives.
test("reading a session's queue shows a prompt being sent once, as the item that carries it", async () => {
  const sent = { ...item("que_sent", "steer", "check the build", 4), requestID: "sent-request" }
  const view = await setup([steer, sent], undefined, (sync) =>
    sync.queue.sending(sessionID, { requestID: "sent-request", text: "check the build" }),
  )
  expect(view.sync.data.session_sending[sessionID]).toEqual([])
  const lines = (await view.frame()).split("\n")
  expect(lines.filter((line) => line.includes("QUEUED") && line.includes("check the build"))).toHaveLength(1)
  expect(lines.some((line) => line.includes("2 pending"))).toBe(true)
})
