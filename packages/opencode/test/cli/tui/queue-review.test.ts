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
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import path from "node:path"
import { eq } from "drizzle-orm"
import { Deferred, Effect, Layer, Schema } from "effect"
import { HttpServer } from "effect/unstable/http"
import { InstanceBootstrap } from "../../../src/project/bootstrap"
import { InstanceStore } from "../../../src/project/instance-store"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import { provideInstanceEffect, tmpdirScoped } from "../../fixture/fixture"
import { createTuiResolvedConfig } from "../../fixture/tui-runtime"
import { pollWithTimeout, testEffect } from "../../lib/effect"
import { reply, TestLLMServer } from "../../lib/llm-server"
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
  const queueRequests: { method: string; path: string; body?: string }[] = []
  const loseForceResponse = { armed: false }
  const fetch = Object.assign(
    async (request: RequestInfo | URL, init?: RequestInit) => {
      const source = request instanceof Request ? request : new Request(request, init)
      const url = new URL(source.url)
      const target = new URL(`${url.pathname}${url.search}`, base)
      if (url.pathname.includes("/queue/")) {
        const body = source.method === "GET" ? "" : await source.clone().text()
        queueRequests.push({ method: source.method, path: url.pathname, ...(body ? { body } : {}) })
      }
      if (url.pathname !== "/global/event" && url.pathname.includes("/queue/force/") && loseForceResponse.armed) {
        loseForceResponse.armed = false
        const response = await globalThis.fetch(new Request(target, source))
        if (response.ok) {
          if (response.body) await response.body.cancel().catch(() => undefined)
          throw new Error("simulated connection loss after force commit")
        }
        return response
      }
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

  const renderer = yield* Effect.promise(() =>
    createTestRenderer({ width: 140, height: 45, useThread: false, kittyKeyboard: true }),
  )
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
    queueRequests,
    loseNextForceResponse: () => (loseForceResponse.armed = true),
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
    compose: (text: string) =>
      Effect.promise(async () => {
        await renderer.mockInput.typeText(text)
        api?.keymap.dispatchCommand("prompt.submit")
      }),
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
      // Queued behind the running turn, these wait for it to end; the second
      // changelog takes the first in as it is admitted.
      yield* Effect.forEach(
        ["drop this one", "run lint", "write the changelog", "run lint --fix", "write the changelog"],
        (text) => tui.prompt(text, "queue"),
        { discard: true },
      )
      yield* tui.screen("4 pending · 0 steer · 4 queue", "the prompt's row")

      // Activating the row pauses delivery; the confirmed hold says so.
      yield* tui.open
      yield* tui.screen("(delivery paused)", "the review view")
      // Unheld, a steer is delivered into the running turn at admission.
      yield* tui.prompt("steer while reviewing")
      const listed = yield* tui.screen("5 pending · 1 steer · 4 queue", "the prompt's row")
      const order = ["steer while reviewing", "drop this one", "run lint", "write the changelog", "run lint --fix"].map(
        (text) => listed.indexOf(text),
      )
      expect(order.every((at) => at >= 0)).toBe(true)
      expect(order).toEqual(order.toSorted((a, b) => a - b))

      // The turn ends where it would deliver them, and goes idle with all five pending.
      yield* Deferred.succeed(gate, undefined)
      yield* tui.idle
      expect(yield* llm.calls).toBe(1)
      expect(texts(yield* tui.rows)).toEqual([
        "steer while reviewing",
        "drop this one",
        "run lint",
        "write the changelog",
        "run lint --fix",
      ])

      // Delete the second row; its durable row is gone, not withdrawn.
      // The selection follows its item as the list changes, so each move starts from the top.
      const doomed = (yield* tui.rows).find((row) => texts([row])[0] === "drop this one")!
      tui.renderer.mockInput.pressKey("HOME")
      tui.renderer.mockInput.pressArrow("down")
      tui.renderer.mockInput.pressKey("d", { ctrl: true })
      yield* pollWithTimeout(
        tui.rows.pipe(Effect.map((rows) => (rows.some((row) => row.id === doomed.id) ? undefined : true))),
        "the deleted prompt's row was never removed",
      )
      // Nothing is left to restore, as there would be for a withdrawn prompt.
      const restored = yield* Effect.promise(() =>
        tui.sdk.session.queue.restore({ sessionID: tui.sessionID, id: doomed.id }),
      )
      expect(restored.response.status).toBe(404)
      yield* tui.screen("4 pending · 1 steer · 3 queue", "the review list after deleting a prompt")

      // Consolidate: the two lint commands as one with steps.
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
      // Every prompt, those the review held among them, reached the model saying when it was queued.
      const notes = (yield* llm.inputs).map((input) =>
        JSON.stringify((input.messages as unknown[]).at(-1)).includes("[Queued prompt"),
      )
      expect(notes).toEqual([true, true, true, true])
      expect(texts(yield* tui.rows)).toEqual([])
    }).pipe(Effect.ensuring(Deferred.succeed(gate, undefined)), Effect.provide(TestLLMServer.layer))
  },
  60_000,
)

it.live(
  "Ctrl+f reconciles a lost response and forces only the selected composer prompt into the next tool continuation",
  () => {
    const toolGate = Deferred.makeUnsafe<void>()
    const continuationGate = Deferred.makeUnsafe<void>()
    return Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const base = testProviderConfig(llm.url)
      const selectableModel = (id: string) => ({
        ...base.provider.test.models["test-model"],
        id,
        name: id,
        variants: { low: { reasoningEffort: "low" }, high: { reasoningEffort: "high" } },
      })
      const config = {
        ...base,
        provider: {
          ...base.provider,
          test: {
            ...base.provider.test,
            models: {
              ...base.provider.test.models,
              "model-a": selectableModel("model-a"),
              "model-b": selectableModel("model-b"),
            },
          },
        },
      }
      const directory = yield* tmpdirScoped({ git: true, config })
      const file = path.join(directory, "force-tool-input.txt")
      yield* Effect.promise(() => Bun.write(file, "tool continuation completed"))
      yield* llm.push(
        reply()
          .wait(Effect.runPromise(Deferred.await(toolGate)))
          .tool("read", { filePath: file })
          .item(),
        reply()
          .wait(Effect.runPromise(Deferred.await(continuationGate)))
          .text("selected prompt answered")
          .stop()
          .item(),
      )
      const tui = yield* boot({ directory })

      yield* tui.prompt("start the running turn")
      yield* pollWithTimeout(
        lastUsers(llm).pipe(Effect.map((users) => (users.includes("start the running turn") ? true : undefined))),
        "the initial provider request never started",
      )
      yield* tui.compose("earlier composer prompt")
      yield* tui.compose("selected composer prompt")
      const admitted = yield* pollWithTimeout(
        tui.rows.pipe(
          Effect.map((rows) => {
            const pending = rows.filter((row) => row.time_promoted === null && row.time_withdrawn === null)
            return pending.length === 2 && pending.every((row) => row.time_preparing === null) ? pending : undefined
          }),
        ),
        "the composer prompts were not fully prepared in the queue",
      )
      const earlier = admitted.find((row) => texts([row]).includes("earlier composer prompt"))
      const selected = admitted.find((row) => texts([row]).includes("selected composer prompt"))
      if (!earlier || !selected) throw new Error("the composer prompts were not admitted in order")
      expect(earlier.input.origin).toBe("tui-composer")
      expect(selected.input.origin).toBe("tui-composer")

      yield* tui.open
      yield* tui.screen("(delivery paused)", "the review view")
      tui.renderer.mockInput.pressArrow("down")
      tui.loseNextForceResponse()
      tui.renderer.mockInput.pressKey("f", { ctrl: true })
      yield* tui.screen("Could not force the queued prompts", "the lost force response")
      const forced = yield* pollWithTimeout(
        tui.rows.pipe(
          Effect.map((rows) => {
            const current = new Map(rows.map((row) => [row.id, row]))
            return current.get(selected.id)?.delivery === "steer" && current.get(earlier.id)?.delivery === "queue"
              ? [current.get(selected.id), current.get(earlier.id)]
              : undefined
          }),
        ),
        "Ctrl+f did not give steer timing to only the selected item",
      )
      expect(forced.map((row) => row?.time_promoted)).toEqual([null, null])
      yield* pollWithTimeout(
        tui.frame.pipe(Effect.map((screen) => (screen.includes("(delivery paused)") ? undefined : screen))),
        "lost force response left the review showing its released hold",
      )
      yield* tui.screen("(delivery NOT paused)", "the reconciled review hold state")
      expect(
        tui.queueRequests.some(
          (request) => request.method === "GET" && request.path.includes(`/session/${tui.sessionID}/queue/hold/`),
        ),
      ).toBe(true)

      yield* Deferred.succeed(toolGate, undefined)
      const continuation = yield* pollWithTimeout(
        llm.inputs.pipe(
          Effect.map((inputs) => {
            const provider = inputs.filter(
              (input) => !JSON.stringify(input).includes("Generate a title for this conversation"),
            )
            return provider.length >= 2 ? provider[1] : undefined
          }),
        ),
        "the provider did not continue after the tool call",
      )
      const request = JSON.stringify(continuation)
      expect(request).toContain("selected composer prompt")
      expect(request).not.toContain("earlier composer prompt")
      expect(texts(yield* tui.rows)).toEqual(["earlier composer prompt"])
      const persisted = yield* tui.rows
      expect(persisted.find((row) => row.id === selected.id)?.time_promoted).not.toBeNull()
      expect(persisted.find((row) => row.id === earlier.id)?.time_promoted).toBeNull()

      yield* Deferred.succeed(continuationGate, undefined)
      yield* tui.idle
      const selectedRequest = selected.request_id
      if (!selectedRequest) throw new Error("the selected composer request lost its receipt identity")
      const selectedReceipt = yield* Effect.promise(() =>
        tui.sdk.session.queue.receipt({ sessionID: tui.sessionID, requestID: selectedRequest }),
      )
      expect(selectedReceipt.data?.state).toBe("delivered")
    }).pipe(
      Effect.ensuring(Deferred.succeed(toolGate, undefined)),
      Effect.ensuring(Deferred.succeed(continuationGate, undefined)),
      Effect.provide(TestLLMServer.layer),
    )
  },
  60_000,
)

it.live(
  "selected human consolidation preserves composer originals and leaves marked automation separate",
  () => {
    const startGate = Deferred.makeUnsafe<void>()
    const carrierGate = Deferred.makeUnsafe<void>()
    return Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const base = testProviderConfig(llm.url)
      const selectableModel = (id: string) => ({
        ...base.provider.test.models["test-model"],
        id,
        name: id,
        variants: { low: { reasoningEffort: "low" }, high: { reasoningEffort: "high" } },
      })
      const config = {
        ...base,
        provider: {
          ...base.provider,
          test: {
            ...base.provider.test,
            models: {
              ...base.provider.test.models,
              "model-a": selectableModel("model-a"),
              "model-b": selectableModel("model-b"),
            },
          },
        },
      }
      const directory = yield* tmpdirScoped({ git: true, config })
      yield* llm.hold("initial turn done", Effect.runPromise(Deferred.await(startGate)))
      const tui = yield* boot({ directory })
      const automation = "OBSERVER: machine appended work"

      yield* tui.prompt("start the running turn")
      yield* pollWithTimeout(
        lastUsers(llm).pipe(Effect.map((users) => (users.includes("start the running turn") ? true : undefined))),
        "the initial provider request never started",
      )
      yield* tui.compose("first composer instruction")
      yield* tui.compose(automation)
      yield* tui.compose("last composer instruction")

      const originals = yield* pollWithTimeout(
        tui.rows.pipe(
          Effect.map((rows) => {
            const pending = rows.filter((row) => row.time_promoted === null && row.time_withdrawn === null)
            return pending.length === 3 && pending.every((row) => row.time_preparing === null) ? pending : undefined
          }),
        ),
        "composer and appended prompts were not prepared in the queue",
      )
      const first = originals.find((row) => texts([row]).includes("first composer instruction"))
      const last = originals.find((row) => texts([row]).includes("last composer instruction"))
      const marked = originals.find((row) => texts([row]).some((text) => text.includes("machine appended work")))
      if (!first || !last || !marked) throw new Error("a composer or marked automation row was lost")
      expect(first.input.origin).toBe("tui-composer")
      expect(last.input.origin).toBe("tui-composer")
      expect(marked.input.origin).toBe("tui-composer")
      expect(texts([marked])).toContain(automation)

      yield* tui.open
      yield* tui.screen("(delivery paused)", "the review view")
      tui.renderer.mockInput.pressArrow("down")
      tui.renderer.mockInput.pressArrow("down")
      tui.renderer.mockInput.pressArrow("up", { meta: true })
      yield* pollWithTimeout(
        tui.rows.pipe(
          Effect.map((rows) => {
            const firstRow = rows.find((row) => row.id === first.id)
            const lastRow = rows.find((row) => row.id === last.id)
            const machineRow = rows.find((row) => row.id === marked.id)
            return firstRow && lastRow && machineRow && firstRow.seq < lastRow.seq && lastRow.seq < machineRow.seq
              ? true
              : undefined
          }),
        ),
        "the later human prompt never moved ahead of the machine row",
      )
      yield* pollWithTimeout(
        tui.frame.pipe(
          Effect.map((screen) => {
            const lastText = screen.indexOf("last composer instruction")
            const machineText = screen.indexOf("machine appended work")
            return lastText !== -1 && machineText !== -1 && lastText < machineText ? true : undefined
          }),
        ),
        "the review view never displayed the first reorder",
      )
      tui.renderer.mockInput.pressKey("HOME")
      tui.renderer.mockInput.pressArrow("down")
      tui.renderer.mockInput.pressArrow("up", { meta: true })
      yield* pollWithTimeout(
        tui.rows.pipe(
          Effect.map((rows) => {
            const firstRow = rows.find((row) => row.id === first.id)
            const lastRow = rows.find((row) => row.id === last.id)
            const machineRow = rows.find((row) => row.id === marked.id)
            return firstRow && lastRow && machineRow && lastRow.seq < firstRow.seq && firstRow.seq < machineRow.seq
              ? rows
              : undefined
          }),
        ),
        "the later human prompt never reached the first display position",
      )
      yield* pollWithTimeout(
        tui.frame.pipe(
          Effect.map((screen) => {
            const lastText = screen.indexOf("last composer instruction")
            const firstText = screen.indexOf("first composer instruction")
            return lastText !== -1 && firstText !== -1 && lastText < firstText ? true : undefined
          }),
        ),
        "the review view never displayed the second reorder",
      )
      tui.renderer.mockInput.pressArrow("down")
      tui.renderer.mockInput.pressKey("o", { ctrl: true })
      const consolidated = yield* pollWithTimeout(
        tui.rows.pipe(
          Effect.map((rows) => {
            const pending = rows.filter((row) => row.time_promoted === null && row.time_withdrawn === null)
            return pending.length === 2 && pending.some((row) => row.id === first.id) ? pending : undefined
          }),
        ),
        "the selected human prompts were not consolidated",
      )
      expect(
        tui.queueRequests.some(
          (request) =>
            request.method === "POST" && request.path === `/session/${tui.sessionID}/queue/consolidate/${first.id}`,
        ),
      ).toBe(true)
      const carrier = consolidated.find((row) => row.id === first.id)
      const machine = consolidated.find((row) => row.id === marked.id)
      if (!carrier || !machine) throw new Error("consolidation lost the human carrier or machine row")
      yield* tui.screen("2 pending · 0 steer · 2 queue", "the consolidated queue summary")
      tui.renderer.mockInput.pressKey("HOME")
      yield* tui.screen("first composer instruction", "the selected human carrier")
      expect(carrier.delivery).toBe("queue")
      expect(machine.delivery).toBe("queue")
      expect(texts([machine])).toEqual([automation])
      expect(machine.input.parts).toEqual(marked.input.parts)
      const visible = carrier.input.parts.filter(
        (part): part is Extract<(typeof carrier.input.parts)[number], { type: "text" }> =>
          part.type === "text" && !part.synthetic && !part.ignored,
      )
      expect(visible.map((part) => part.text).join("")).toBe("first composer instruction\nlast composer instruction")
      const firstVisible = visible[0]
      if (!firstVisible) throw new Error("the consolidated human text was missing")
      const group = Schema.decodeUnknownSync(SessionPromptQueue.Group)(firstVisible.metadata?.sessionQueue)
      expect(group.constituents.map((item) => item.id)).toEqual([first.id, last.id])
      expect(group.constituents.map((item) => item.input.origin)).toEqual(["tui-composer", "tui-composer"])
      expect(group.constituents.map((item) => item.input.model?.modelID.toString())).toEqual([
        "test-model",
        "test-model",
      ])
      const promptInputs = (yield* llm.inputs).filter(
        (input) => !JSON.stringify(input).includes("Generate a title for this conversation"),
      )
      expect(promptInputs).toHaveLength(1)

      yield* Deferred.succeed(startGate, undefined)
      yield* tui.idle
      expect(texts(yield* tui.rows)).toEqual(["first composer instruction\nlast composer instruction", automation])
      const selection = yield* Effect.promise(() =>
        tui.sdk.session.update({
          sessionID: tui.sessionID,
          model: { providerID: "test", id: "model-b", variant: "high" },
        }),
      )
      expect(selection.error).toBeUndefined()
      yield* llm.push(
        reply()
          .wait(Effect.runPromise(Deferred.await(carrierGate)))
          .text("consolidated human prompt answered")
          .stop()
          .item(),
      )
      yield* llm.text("machine automation answered")

      tui.renderer.mockInput.pressKey("f", { ctrl: true })
      yield* tui.screen("The selected prompt will reach the next safe step.", "the accepted force")
      const forceRequest = tui.queueRequests.find(
        (request) => request.method === "POST" && request.path === `/session/${tui.sessionID}/queue/force/${first.id}`,
      )
      if (!forceRequest?.body) throw new Error("force did not send the selected item and hold to the server")
      expect(forceRequest.body).toContain('"holder":"')
      yield* pollWithTimeout(
        tui.rows.pipe(
          Effect.map((rows) => {
            const pending = rows.filter((row) => row.time_promoted === null && row.time_withdrawn === null)
            return pending.length === 1 && pending[0]?.id === marked.id ? true : undefined
          }),
        ),
        "forcing the consolidated human carrier changed or consumed the unselected machine row",
      )
      const continuation = yield* pollWithTimeout(
        llm.inputs.pipe(
          Effect.map((inputs) => {
            const provider = inputs.filter(
              (input) => !JSON.stringify(input).includes("Generate a title for this conversation"),
            )
            return provider.length >= 2 ? provider[1] : undefined
          }),
        ),
        "the provider did not receive the consolidated prompt during tool continuation",
      )
      const request = JSON.stringify(continuation)
      const firstAt = request.indexOf("first composer instruction")
      const lastAt = request.indexOf("last composer instruction")
      expect(firstAt).toBeGreaterThanOrEqual(0)
      expect(lastAt).toBeGreaterThan(firstAt)
      expect(request).not.toContain("machine appended work")
      expect(continuation.model).toBe("model-b")
      expect(continuation.reasoning_effort).toBe("high")
      expect(texts(yield* tui.rows)).toEqual([automation])
      const messages = yield* Session.use
        .messages({ sessionID: SessionID.make(tui.sessionID) })
        .pipe(provideInstanceEffect(directory), Effect.orDie)
      const humanMessage = messages.find(
        (message) =>
          message.info.role === "user" &&
          message.parts.some((part) => part.type === "text" && part.text === "first composer instruction"),
      )
      const deliveredPart = humanMessage?.parts.find(
        (part) => part.type === "text" && !part.synthetic && !part.ignored && part.metadata?.sessionQueue,
      )
      if (!deliveredPart || deliveredPart.type !== "text") throw new Error("the delivered group metadata was missing")
      const deliveredGroup = Schema.decodeUnknownSync(SessionPromptQueue.Group)(deliveredPart.metadata?.sessionQueue)
      expect(deliveredGroup.constituents.map((item) => item.id)).toEqual([first.id, last.id])
      expect(humanMessage?.info.role === "user" ? humanMessage.info.model : undefined).toMatchObject({
        modelID: "model-b",
        variant: "high",
      })

      yield* Deferred.succeed(carrierGate, undefined)
      yield* tui.idle
      const firstRequestID = first.request_id
      const lastRequestID = last.request_id
      const machineRequestID = marked.request_id
      if (!firstRequestID || !lastRequestID || !machineRequestID)
        throw new Error("a composer request lost its receipt identity")
      const firstReceipt = yield* Effect.promise(() =>
        tui.sdk.session.queue.receipt({ sessionID: tui.sessionID, requestID: firstRequestID }),
      )
      const lastReceipt = yield* Effect.promise(() =>
        tui.sdk.session.queue.receipt({ sessionID: tui.sessionID, requestID: lastRequestID }),
      )
      const machineReceipt = yield* Effect.promise(() =>
        tui.sdk.session.queue.receipt({ sessionID: tui.sessionID, requestID: machineRequestID }),
      )
      expect(firstReceipt.data?.state).toBe("delivered")
      expect(lastReceipt.data).toMatchObject({ state: "merged", into: firstRequestID })
      expect(machineReceipt.data?.state).toBe("delivered")
    }).pipe(
      Effect.ensuring(Deferred.succeed(startGate, undefined)),
      Effect.ensuring(Deferred.succeed(carrierGate, undefined)),
      Effect.provide(TestLLMServer.layer),
    )
  },
  60_000,
)

it.live(
  "Ctrl+f starts the selected pending prompt when the session is idle",
  () => {
    const gate = Deferred.makeUnsafe<void>()
    return Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.hold("initial turn done", Effect.runPromise(Deferred.await(gate)))
      yield* llm.text("forced idle answer")
      const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
      const tui = yield* boot({ directory })
      yield* tui.prompt("start the active turn")
      yield* pollWithTimeout(
        lastUsers(llm).pipe(Effect.map((users) => (users.includes("start the active turn") ? true : undefined))),
        "the initial provider request never started",
      )
      yield* tui.compose("queued for the idle session")
      yield* pollWithTimeout(
        tui.rows.pipe(
          Effect.map((rows) => {
            const pending = rows.filter((row) => row.time_promoted === null && row.time_withdrawn === null)
            return pending.length === 1 && pending[0]?.time_preparing === null ? pending : undefined
          }),
        ),
        "the composer prompt never became ready in the queue",
      )
      yield* tui.open
      yield* tui.screen("(delivery paused)", "the idle review view")
      yield* Deferred.succeed(gate, undefined)
      yield* tui.idle
      expect(texts(yield* tui.rows)).toEqual(["queued for the idle session"])

      tui.renderer.mockInput.pressKey("f", { ctrl: true })
      const input = yield* pollWithTimeout(
        llm.inputs.pipe(
          Effect.map((inputs) => inputs.find((input) => JSON.stringify(input).includes("queued for the idle session"))),
        ),
        "forcing an idle item never started a provider request",
      )
      expect(JSON.stringify(input)).toContain("queued for the idle session")
      yield* tui.idle
      expect((yield* lastUsers(llm)).some((text) => text.includes("queued for the idle session"))).toBe(true)
      expect(texts(yield* tui.rows)).toEqual([])
    }).pipe(Effect.ensuring(Deferred.succeed(gate, undefined)), Effect.provide(TestLLMServer.layer))
  },
  60_000,
)

it.live(
  "Ctrl+e enhances the selected composer prompt in place through prompt enhancement",
  () => {
    const gate = Deferred.makeUnsafe<void>()
    return Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.hold("initial turn done", Effect.runPromise(Deferred.await(gate)))
      const directory = yield* tmpdirScoped({ git: true, config: testProviderConfig(llm.url) })
      const tui = yield* boot({ directory })

      yield* tui.prompt("start enhancement context")
      yield* pollWithTimeout(
        lastUsers(llm).pipe(Effect.map((users) => (users.includes("start enhancement context") ? true : undefined))),
        "the enhancement context turn never started",
      )
      yield* tui.compose("rough draft")
      const [selected] = yield* pollWithTimeout(
        tui.rows.pipe(
          Effect.map((rows) => {
            const pending = rows.filter((row) => row.time_promoted === null && row.time_withdrawn === null)
            return pending.length === 1 && pending[0]?.time_preparing === null ? pending : undefined
          }),
        ),
        "the composer prompt never became ready for enhancement",
      )
      const requestID = selected?.request_id
      if (!selected || !requestID) throw new Error("the selected composer prompt lost its request identity")

      yield* tui.open
      yield* tui.screen("(delivery paused)", "the enhancement review view")
      yield* llm.text("polished draft")
      tui.renderer.mockInput.pressKey("e", { ctrl: true })
      yield* tui.screen("Queued prompt enhanced.", "the enhancement result")
      const enhancement = yield* pollWithTimeout(
        llm.inputs.pipe(Effect.map((inputs) => inputs.find((input) => JSON.stringify(input).includes("rough draft")))),
        "the selected draft never reached the existing enhancement endpoint",
      )
      expect(enhancement.model).toBe("test-model")
      expect(JSON.stringify(enhancement)).toContain("start enhancement context")
      const rewrite = tui.queueRequests.find(
        (request) =>
          request.method === "POST" && request.path === `/session/${tui.sessionID}/queue/rewrite/${selected.id}`,
      )
      if (!rewrite?.body) throw new Error("enhancement did not rewrite the selected queue item")
      expect(JSON.parse(rewrite.body)).toEqual({ expectedText: "rough draft", text: "polished draft" })

      const rewritten = yield* pollWithTimeout(
        tui.rows.pipe(
          Effect.map((rows) => {
            const item = rows.find((row) => row.id === selected.id)
            return item && texts([item]).includes("polished draft") ? item : undefined
          }),
        ),
        "the queue row did not retain the enhanced text",
      )
      expect(rewritten.seq).toBe(selected.seq)
      expect(rewritten.delivery).toBe(selected.delivery)
      expect(rewritten.request_id).toBe(selected.request_id)
      const pendingReceipt = yield* Effect.promise(() =>
        tui.sdk.session.queue.receipt({ sessionID: tui.sessionID, requestID }),
      )
      expect(pendingReceipt.data?.state).toBe("pending")

      yield* llm.text("polished prompt answered")
      yield* Deferred.succeed(gate, undefined)
      yield* tui.idle
      tui.renderer.mockInput.pressEscape()
      const delivered = yield* pollWithTimeout(
        llm.inputs.pipe(
          Effect.map((inputs) =>
            inputs.find((input) => JSON.stringify(input).includes("polished draft") && input.model === "test-model"),
          ),
        ),
        "the rewritten prompt was not delivered after the review hold ended",
      )
      expect(JSON.stringify(delivered)).not.toContain("rough draft")
      yield* tui.idle
      const deliveredReceipt = yield* Effect.promise(() =>
        tui.sdk.session.queue.receipt({ sessionID: tui.sessionID, requestID }),
      )
      expect(deliveredReceipt.data?.state).toBe("delivered")
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
