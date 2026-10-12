import { expect } from "bun:test"
import { createTestRenderer } from "@opentui/core/testing"
import type { TuiPluginApi } from "@opencode-ai/plugin/tui"
import { createOpencodeClient } from "@opencode-ai/sdk/v2"
import { run } from "@opencode-ai/tui"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"
import { SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import { eq } from "drizzle-orm"
import { Deferred, Effect, Layer } from "effect"
import { HttpServer } from "effect/unstable/http"
import { InstanceBootstrap } from "../../../src/project/bootstrap"
import { InstanceStore } from "../../../src/project/instance-store"
import { Session } from "@/session/session"
import { tmpdirScoped } from "../../fixture/fixture"
import { createTuiResolvedConfig } from "../../fixture/tui-runtime"
import { pollWithTimeout, testEffect } from "../../lib/effect"
import { TestLLMServer } from "../../lib/llm-server"
import { testProviderConfig } from "../../lib/test-provider"
import { httpApiLayer } from "../../server/httpapi-layer"

// The review view of pending prompts (swxtchio/swx-opencode#212), driven
// through the running TUI against the real server: real admission over HTTP,
// the durable queue, real promotion, and what the model is finally sent.

const appLayer = AppNodeBuilder.build(
  LayerNode.group([FSUtil.node, CrossSpawnSpawner.node, InstanceStore.node, Database.node, Session.node]),
  [
    [
      InstanceStore.bootstrapNode,
      Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void })),
    ],
  ],
)
const it = testEffect(Layer.mergeAll(appLayer, httpApiLayer))

type SessionPromptQueueRow = typeof SessionPromptQueueTable.$inferSelect

const model = { providerID: "test", modelID: "test-model" }

// Boots the TUI on `sessionID` against the in-process server, and gives the
// test the SDK, the renderer and the durable queue rows of the session.
const boot = Effect.fnUntraced(function* (input: { directory: string; sessionID?: string }) {
  const server = yield* HttpServer.HttpServer
  const base = HttpServer.formatAddress(server.address)
  const fetch = Object.assign(
    async (request: RequestInfo | URL, init?: RequestInit) => {
      const source = request instanceof Request ? request : new Request(request, init)
      const url = new URL(source.url)
      const target = new URL(`${url.pathname}${url.search}`, base)
      if (url.pathname !== "/global/event") return globalThis.fetch(new Request(target, source))
      // The SDK's event stream leaves a rejected cancel unhandled when the app
      // aborts it at exit (#219), so the app reads a stream that just ends instead.
      const upstream = new AbortController()
      const response = await globalThis.fetch(new Request(target, { headers: source.headers, signal: upstream.signal }))
      const reader = response.body!.getReader()
      const stop = () => {
        upstream.abort()
        reader.cancel().catch(() => undefined)
      }
      source.signal.addEventListener("abort", stop)
      return new Response(
        new ReadableStream<Uint8Array>({
          async pull(controller) {
            const chunk = await reader.read().catch(() => ({ done: true as const, value: undefined }))
            if (chunk.done) return controller.close()
            controller.enqueue(chunk.value)
          },
          cancel: stop,
        }),
        { status: response.status, headers: response.headers },
      )
    },
    { preconnect: globalThis.fetch.preconnect },
  ) satisfies typeof globalThis.fetch
  const sdk = createOpencodeClient({ baseUrl: "http://localhost", directory: input.directory, fetch })
  const sessionID = input.sessionID ?? (yield* Effect.promise(() => sdk.session.create({ title: "review" }))).data!.id
  const { db } = yield* Database.Service

  const renderer = yield* Effect.promise(() => createTestRenderer({ width: 140, height: 45, useThread: false }))
  let api: TuiPluginApi | undefined
  const started = yield* Deferred.make<void>()
  const task = Effect.runPromise(
    run({
      url: "http://localhost",
      directory: input.directory,
      createRenderer: async () => renderer.renderer,
      config: createTuiResolvedConfig({ plugin_enabled: {} }),
      fetch,
      args: { sessionID },
      pluginHost: {
        // As the real host does, so the session_prompt slot renders the Prompt.
        async start(host) {
          api = host.api
          host.runtime.setupSlots(host.api)
          Deferred.doneUnsafe(started, Effect.void)
        },
        async dispose() {},
      },
    }).pipe(Effect.provide(AppNodeBuilder.build(Global.node))),
  )
  yield* Deferred.await(started)
  yield* Effect.addFinalizer(() =>
    Effect.promise(async () => {
      api?.keymap.dispatchCommand("app.exit")
      await task.catch(() => undefined)
      if (!renderer.renderer.isDestroyed) renderer.renderer.destroy()
    }),
  )

  const frame = Effect.promise(async () => {
    await renderer.renderOnce()
    return renderer.captureCharFrame()
  })
  return {
    sdk,
    sessionID,
    renderer,
    dispatch: (command: string) => api?.keymap.dispatchCommand(command),
    frame,
    screen: (text: string, what: string) =>
      pollWithTimeout(
        frame.pipe(Effect.map((screen) => (screen.includes(text) ? screen : undefined))),
        `${what} never showed "${text}"`,
        "10 seconds",
      ),
    prompt: (text: string, delivery?: "steer" | "queue") =>
      Effect.promise(() =>
        sdk.session.promptAsync({
          sessionID,
          agent: "build",
          model,
          ...(delivery ? { delivery } : {}),
          parts: [{ type: "text", text }],
        }),
      ),
    rows: db
      .select()
      .from(SessionPromptQueueTable)
      .where(eq(SessionPromptQueueTable.session_id, sessionID as SessionPromptQueueRow["session_id"]))
      .all()
      .pipe(Effect.orDie),
    idle: pollWithTimeout(
      Effect.promise(() => sdk.session.status()).pipe(
        Effect.map((status) => (status.data?.[sessionID] === undefined ? true : undefined)),
      ),
      "the session never went idle",
      "10 seconds",
    ),
    open: Effect.gen(function* () {
      const screen = yield* frame
      const lines = screen.split("\n")
      const y = lines.findIndex((line) => line.includes(" pending · "))
      if (y < 0) throw new Error("the pending row is not on screen")
      yield* Effect.promise(() => renderer.mockMouse.click(lines[y]!.indexOf("pending"), y))
    }),
  }
})

const lastUsers = (llm: TestLLMServer["Service"]) =>
  llm.inputs.pipe(
    Effect.map((inputs) =>
      inputs.map((input) => {
        const last = Array.isArray(input.messages) ? input.messages.at(-1) : undefined
        if (last && typeof last.content === "string") return last.content
        // A prompt that waited also carries the note of when it was queued.
        if (last && Array.isArray(last.content))
          return last.content
            .flatMap((part: { type?: string; text?: string }) =>
              part.type === "text" && !part.text?.startsWith("[Queued prompt") ? [part.text] : [],
            )
            .join("")
        return JSON.stringify(last)
      }),
    ),
  )

const delivered = (llm: TestLLMServer["Service"], count: number) =>
  pollWithTimeout(
    lastUsers(llm).pipe(Effect.map((users) => (users.length >= count ? users : undefined))),
    `the model never received ${count} turns`,
    "15 seconds",
  )

const texts = (rows: readonly SessionPromptQueueRow[]) =>
  rows
    .filter((row) => row.time_promoted === null && row.time_withdrawn === null)
    .toSorted((a, b) => (a.delivery === b.delivery ? a.seq - b.seq : a.delivery === "steer" ? -1 : 1))
    .map((row) => row.input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join(""))

it.live(
  "the review counts, holds, deletes, reorders and consolidates real pending prompts, and its close delivers them",
  () => {
    const gate = Deferred.makeUnsafe<void>()
    return Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
      const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
      const tui = yield* boot({ directory })

      yield* tui.prompt("start")
      yield* llm.wait(1)
      // Queued behind the running turn, these wait for it to end.
      yield* Effect.forEach(
        ["drop this one", "run lint", "write the changelog", "run lint --fix", "write the changelog"],
        (text) => tui.prompt(text, "queue"),
        { discard: true },
      )
      yield* tui.screen("5 pending · 0 steer · 5 queue", "the prompt's row")

      // Activating the row pauses delivery; the confirmed hold says so.
      yield* tui.open
      yield* tui.screen("(delivery paused)", "the review view")
      // Unheld, a steer is delivered into the running turn at admission.
      yield* tui.prompt("steer while reviewing")
      const listed = yield* tui.screen("6 pending · 1 steer · 5 queue", "the prompt's row")
      const order = ["steer while reviewing", "drop this one", "run lint", "write the changelog", "run lint --fix"].map(
        (text) => listed.indexOf(text),
      )
      expect(order.every((at) => at >= 0)).toBe(true)
      expect(order).toEqual(order.toSorted((a, b) => a - b))

      // The turn ends where it would deliver them, and goes idle with all six pending.
      yield* Deferred.succeed(gate, undefined)
      yield* tui.idle
      expect(yield* llm.calls).toBe(1)
      expect(texts(yield* tui.rows)).toEqual([
        "steer while reviewing",
        "drop this one",
        "run lint",
        "write the changelog",
        "run lint --fix",
        "write the changelog",
      ])

      // Delete the second row; its durable row is gone, not withdrawn.
      // The selection follows its item as the list changes, so each move starts from the top.
      tui.renderer.mockInput.pressKey("HOME")
      tui.renderer.mockInput.pressArrow("down")
      tui.renderer.mockInput.pressKey("d", { ctrl: true })
      yield* pollWithTimeout(
        tui.rows.pipe(
          Effect.map((rows) => (rows.some((row) => texts([row])[0] === "drop this one") ? undefined : true)),
        ),
        "the deleted prompt's row was never removed",
      )

      // Consolidate: one changelog, and the two lint commands as one with steps.
      tui.renderer.mockInput.pressKey("o", { ctrl: true })
      yield* pollWithTimeout(
        tui.rows.pipe(Effect.map((rows) => (texts(rows).length === 3 ? texts(rows) : undefined))),
        "the queue was never consolidated",
      ).pipe(
        Effect.tap((pending) =>
          Effect.sync(() =>
            expect(pending).toEqual([
              "steer while reviewing",
              "run lint:\n1. run lint\n2. --fix",
              "write the changelog",
            ]),
          ),
        ),
      )
      yield* pollWithTimeout(
        tui.frame.pipe(Effect.map((screen) => (screen.includes("run lint --fix") ? undefined : true))),
        "the view never showed the consolidated list",
      )

      // Move the changelog ahead of the lint command.
      tui.renderer.mockInput.pressKey("HOME")
      tui.renderer.mockInput.pressArrow("down")
      tui.renderer.mockInput.pressArrow("down")
      tui.renderer.mockInput.pressArrow("up", { meta: true })
      yield* pollWithTimeout(
        tui.rows.pipe(Effect.map((rows) => (texts(rows)[1] === "write the changelog" ? true : undefined))),
        "the changelog was never moved",
      )
      expect(yield* llm.calls).toBe(1)

      // Closing the view is what resumes delivery, in the order the review left.
      yield* llm.text("steer done")
      yield* llm.text("changelog done")
      yield* llm.text("lint done")
      tui.renderer.mockInput.pressEscape()
      expect(yield* delivered(llm, 4)).toEqual([
        "start",
        "steer while reviewing",
        "write the changelog",
        "run lint:\n1. run lint\n2. --fix",
      ])
      yield* tui.idle
      expect((yield* lastUsers(llm)).filter((text) => text === "write the changelog")).toHaveLength(1)
      expect(JSON.stringify(yield* llm.inputs)).not.toContain("drop this one")
      // Each prompt the review held reached the model saying when it was queued.
      const notes = (yield* llm.inputs).map((input) =>
        JSON.stringify((input.messages as unknown[]).at(-1)).includes("[Queued prompt"),
      )
      expect(notes).toEqual([false, true, true, true])
      expect(texts(yield* tui.rows)).toEqual([])
    }).pipe(Effect.ensuring(Deferred.succeed(gate, undefined)), Effect.provide(TestLLMServer.layer))
  },
  60_000,
)

// Captures the review's five-minute idle timer, set from dialog-queue, so the
// test can fire it instead of waiting; every other timer runs as usual.
// The exact interval is proven with fake timers in packages/tui.
function captureIdleTimer() {
  const real = { set: globalThis.setTimeout, clear: globalThis.clearTimeout }
  const pending = new Set<{ readonly fire: () => void }>()
  globalThis.setTimeout = ((handler: () => void, ms?: number, ...rest: unknown[]) => {
    // The provider's stream chunk timeout is five minutes too.
    if (ms !== 5 * 60 * 1000 || !new Error().stack?.includes("dialog-queue")) return real.set(handler, ms, ...rest)
    const timer = { fire: handler }
    pending.add(timer)
    return timer
  }) as unknown as typeof setTimeout
  globalThis.clearTimeout = ((timer: unknown) => {
    if (pending.delete(timer as { fire: () => void })) return
    real.clear(timer as Parameters<typeof clearTimeout>[0])
  }) as typeof clearTimeout
  return {
    pending: () => pending.size,
    fire: () => [...pending].forEach((timer) => (pending.delete(timer), timer.fire())),
    restore: () => Object.assign(globalThis, { setTimeout: real.set, clearTimeout: real.clear }),
  }
}

it.live(
  "five minutes without activity close the review and resume delivery",
  () => {
    const gate = Deferred.makeUnsafe<void>()
    const idle = captureIdleTimer()
    return Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.hold("task done", Effect.runPromise(Deferred.await(gate)))
      const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
      const tui = yield* boot({ directory })

      yield* tui.prompt("start")
      yield* llm.wait(1)
      yield* tui.prompt("queued while reviewing", "queue")
      yield* tui.screen("1 pending · 0 steer · 1 queue", "the prompt's row")
      tui.dispatch("session.queued_prompts")
      yield* tui.screen("(delivery paused)", "the review view")
      yield* Deferred.succeed(gate, undefined)
      yield* tui.idle
      yield* llm.text("queued done")

      // Until the interval runs out the view stays open and delivery paused.
      expect(idle.pending()).toBe(1)
      expect(texts(yield* tui.rows)).toEqual(["queued while reviewing"])
      expect(yield* llm.calls).toBe(1)
      idle.fire()
      expect(yield* delivered(llm, 2)).toEqual(["start", "queued while reviewing"])
      expect(yield* tui.frame).not.toContain("(delivery paused)")
    }).pipe(
      Effect.ensuring(Effect.sync(() => idle.restore())),
      Effect.ensuring(Deferred.succeed(gate, undefined)),
      Effect.provide(TestLLMServer.layer),
    )
  },
  60_000,
)
