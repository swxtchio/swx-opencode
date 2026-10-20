import { TextareaRenderable } from "@opentui/core"
import { createTestRenderer } from "@opentui/core/testing"
import { createOpencodeClient } from "@opencode-ai/sdk/v2"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Global } from "@opencode-ai/core/global"
import type { TuiPluginApi } from "@opencode-ai/plugin/tui"
import { Effect } from "effect"
import { expect, mock } from "bun:test"
import { mkdir } from "node:fs/promises"
import path from "node:path"
import { cliIt } from "../../../opencode/test/lib/cli-process"
import { reply } from "../../../opencode/test/lib/llm-server"
import { eventSource } from "../fixture/tui-sdk"
import { createTuiResolvedConfig } from "../fixture/tui-runtime"

cliIt.live(
  "command-palette compaction threshold reaches the backend config and processor",
  ({ home, opencode, llm }) =>
    Effect.gen(function* () {
      const server = yield* opencode.serve({
        env: { OPENCODE_DISABLE_AUTOCOMPACT: "0", OPENCODE_DISABLE_PROJECT_CONFIG: "0" },
      })
      yield* Effect.promise(async () => {
        const state = path.join(home, "tui-state")
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
        const fetch: typeof globalThis.fetch = Object.assign(
          async (input: RequestInfo | URL, init?: RequestInit) => globalThis.fetch(input, init),
          { preconnect: async () => {} },
        )
        const client = createOpencodeClient({ baseUrl: server.url, directory: home, fetch })

        const mount = async () => {
          const setup = await createTestRenderer({ width: 100, height: 32, useThread: false, kittyKeyboard: true })
          current = setup

          let api!: TuiPluginApi
          let started!: () => void
          const ready = new Promise<void>((resolve) => (started = resolve))
          const task = Effect.runPromise(
            run({
              url: server.url,
              directory: home,
              config: createTuiResolvedConfig({ plugin_enabled: {} }),
              fetch,
              events: eventSource(),
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

          await until(() => {
            app!.api.keymap.dispatchCommand("session.compaction.threshold")
            return app!.setup.captureCharFrame().includes("Current: 95%")
          }, "threshold dialog", 30_000)
          await app.setup.mockInput.typeText("72")
          app.setup.mockInput.pressEnter()
          await until(
            () => app!.setup.captureCharFrame().includes("Auto-compaction threshold set to 72%"),
            "threshold save confirmation",
          )

          await app.stop()
          app = await mount()
          await until(() => {
            app!.api.keymap.dispatchCommand("session.compaction.threshold")
            return app!.setup.captureCharFrame().includes("Current: 72%")
          }, "reloaded threshold", 30_000)
          await app.stop()
          app = undefined

          const config = await client.config.get({}, { throwOnError: true })
          expect(config.data.compaction?.threshold_percent).toBe(72)

          await Effect.runPromise(
            llm.push(
              reply().text("threshold answer").usage({ input: 75_000, output: 100 }).stop(),
              reply().text("threshold summary").stop(),
              reply().text("continued automatically after threshold compaction").stop(),
            ),
          )
          const session = (
            await client.session.create(
              { directory: home, title: "Threshold setting reaches execution" },
              { throwOnError: true },
            )
          ).data
          await client.session.prompt(
            {
              sessionID: session.id,
              directory: home,
              agent: "build",
              model: { providerID: "test", modelID: "test-model" },
              parts: [{ type: "text", text: "cross the configured auto-compaction threshold" }],
            },
            { throwOnError: true },
          )
          const messages = (
            await client.session.messages({ sessionID: session.id, directory: home }, { throwOnError: true })
          ).data
          const compaction = messages.flatMap((message) => message.parts).find((part) => part.type === "compaction")
          expect(compaction).toMatchObject({ auto: true, trigger: "threshold", resume: true })
          expect(
            messages.some((message) =>
              message.parts.some(
                (part) => part.type === "text" && part.text === "continued automatically after threshold compaction",
              ),
            ),
          ).toBe(true)
        } finally {
          if (app && !app.setup.renderer.isDestroyed) await app.stop()
          mock.restore()
        }
      })
    }),
  90_000,
)

async function until(fn: () => boolean, what: string, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (fn()) return
    await Bun.sleep(50)
  }
  throw new Error(`timed out waiting for ${what}`)
}
