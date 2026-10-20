import { TextareaRenderable } from "@opentui/core"
import { createTestRenderer } from "@opentui/core/testing"
import { expect, mock, test } from "bun:test"
import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import type { TuiPluginApi } from "@opencode-ai/plugin/tui"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Global } from "@opencode-ai/core/global"
import { Effect, Schema } from "effect"
import { mkdir } from "node:fs/promises"
import path from "node:path"
import { tmpdir } from "../fixture/fixture"
import { createTuiResolvedConfig } from "../fixture/tui-runtime"
import { createEventSource, createFetch, directory, json } from "../fixture/tui-sdk"

test("command-palette compaction threshold persists and reloads through the production command", async () => {
  await using tmp = await tmpdir()
  const state = path.join(tmp.path, "state")
  const configFile = path.join(tmp.path, "config.json")
  await mkdir(state, { recursive: true })
  await Bun.write(path.join(state, "kv.json"), "{}")

  let current: Awaited<ReturnType<typeof createTestRenderer>> | undefined
  const core = await import("@opentui/core")
  await mock.module("@opentui/core", () => ({
    ...core,
    createCliRenderer: async () => {
      if (!current) throw new Error("renderer was not selected")
      return current.renderer
    },
  }))
  const { run } = await import("../../src/app")
  const updates: unknown[] = []
  const savedConfig = async () => {
    if (!(await Bun.file(configFile).exists())) return Schema.decodeUnknownSync(ConfigV1.Info)({})
    return Schema.decodeUnknownSync(ConfigV1.Info)(await Bun.file(configFile).json())
  }

  const mount = async () => {
    const setup = await createTestRenderer({ width: 100, height: 32, useThread: false, kittyKeyboard: true })
    current = setup
    const events = createEventSource()
    const initialConfig = await savedConfig()
    const base = createFetch((url) => (url.pathname === "/config" ? json(initialConfig) : undefined), events)
    const fetch: typeof globalThis.fetch = Object.assign(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const request = input instanceof Request ? input : new Request(input, init)
        const url = new URL(request.url)
        if (request.method === "PATCH" && url.pathname === "/config") {
          const body = Schema.decodeUnknownSync(ConfigV1.Info)(await request.clone().json())
          updates.push(body)
          const existing = await savedConfig()
          const next = {
            ...existing,
            compaction: {
              ...existing.compaction,
              ...body.compaction,
            },
          }
          await Bun.write(configFile, JSON.stringify(next, null, 2))
          return json(next)
        }
        return base.fetch(request)
      },
      { preconnect: async () => {} },
    )

    let api!: TuiPluginApi
    let started!: () => void
    const ready = new Promise<void>((resolve) => (started = resolve))
    const task = Effect.runPromise(
      run({
        url: "http://test",
        directory,
        config: createTuiResolvedConfig({ plugin_enabled: {} }),
        fetch,
        events: events.source,
        args: {},
        pluginHost: {
          async start(input) {
            api = input.api
            input.runtime.setupSlots(input.api)
            started()
          },
          async dispose() {},
        },
      }).pipe(Effect.provide(AppNodeBuilder.build(Global.node, [[Global.node, Global.layerWith({ state })]]))),
    )
    await ready
    await setup.renderOnce()

    return {
      api,
      setup,
      async stop() {
        api.keymap.dispatchCommand("app.exit")
        await task
        if (!setup.renderer.isDestroyed) setup.renderer.destroy()
      },
    }
  }

  let app: Awaited<ReturnType<typeof mount>> | undefined
  try {
    app = await mount()
    await until(() => app!.setup.renderer.currentFocusedEditor instanceof TextareaRenderable, "app startup")
    expect(
      app.api.keymap
        .getCommandEntries({ namespace: "palette", visibility: "reachable" })
        .some((entry) => entry.command.name === "session.compaction.threshold"),
    ).toBe(true)

    app.api.keymap.dispatchCommand("session.compaction.threshold")
    await until(() => app!.setup.captureCharFrame().includes("Current: 95%"), "threshold dialog")
    await app.setup.mockInput.typeText("72")
    app.setup.mockInput.pressEnter()
    await until(
      () => updates.length === 1 && app!.setup.captureCharFrame().includes("Auto-compaction threshold set to 72%"),
      "threshold save confirmation",
    )
    expect(updates).toEqual([{ compaction: { threshold_percent: 72 } }])

    await app.stop()
    app = await mount()
    app.api.keymap.dispatchCommand("session.compaction.threshold")
    await until(() => app!.setup.captureCharFrame().includes("Current: 72%"), "reloaded threshold")
  } finally {
    if (app && !app.setup.renderer.isDestroyed) await app.stop()
    mock.restore()
  }
})

async function until(fn: () => boolean, what: string) {
  const deadline = Date.now() + 5000
  while (Date.now() < deadline) {
    if (fn()) return
    await Bun.sleep(10)
  }
  throw new Error(`timed out waiting for ${what}`)
}
