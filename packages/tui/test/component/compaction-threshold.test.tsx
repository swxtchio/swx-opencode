import { TextareaRenderable } from "@opentui/core"
import { createTestRenderer } from "@opentui/core/testing"
import { createOpencodeClient } from "@opencode-ai/sdk/v2"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Global } from "@opencode-ai/core/global"
import type { TuiPluginApi } from "@opencode-ai/plugin/tui"
import { expect, mock, test } from "bun:test"
import { Effect } from "effect"
import { mkdir } from "node:fs/promises"
import path from "node:path"
import { tmpdir } from "../fixture/fixture"
import { eventSource } from "../fixture/tui-sdk"
import { createTuiResolvedConfig } from "../fixture/tui-runtime"

test("command-palette compaction threshold reaches the backend config and processor", async () => {
  await using tmp = await tmpdir()
  const home = tmp.path
  const state = path.join(home, "state")
  const llmReplies = [
    { text: "threshold answer", usage: { input: 75_000, output: 100 } },
    { text: "threshold summary" },
    { text: "continued automatically after threshold compaction" },
  ]
  let llmReplyIndex = 0
  const llm = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      if (new URL(request.url).pathname !== "/v1/chat/completions") return new Response("not found", { status: 404 })
      const title = JSON.stringify(await request.json()).includes("Generate a title for this conversation")
      const response = title ? { text: "Test title" } : (llmReplies[llmReplyIndex++] ?? { text: "unexpected request" })
      const chunks = [
        {
          id: "chatcmpl-test",
          object: "chat.completion.chunk",
          choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }],
        },
        {
          id: "chatcmpl-test",
          object: "chat.completion.chunk",
          choices: [{ index: 0, delta: { content: response.text }, finish_reason: null }],
        },
        {
          id: "chatcmpl-test",
          object: "chat.completion.chunk",
          choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
          ...(response.usage
            ? {
                usage: {
                  prompt_tokens: response.usage.input,
                  completion_tokens: response.usage.output,
                  total_tokens: response.usage.input + response.usage.output,
                },
              }
            : {}),
        },
      ]
      return new Response(
        [...chunks.map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`), "data: [DONE]\n\n"].join(""),
        { headers: { "content-type": "text/event-stream" } },
      )
    },
  })

  const configContent = JSON.stringify({
    formatter: false,
    lsp: false,
    provider: {
      test: {
        name: "Test",
        id: "test",
        env: [],
        npm: "@ai-sdk/openai-compatible",
        models: {
          "test-model": {
            id: "test-model",
            name: "Test Model",
            attachment: false,
            reasoning: false,
            temperature: false,
            tool_call: true,
            release_date: "2025-01-01",
            limit: { context: 100_000, output: 10_000 },
            cost: { input: 0, output: 0 },
            options: {},
          },
        },
        options: { apiKey: "test-key", baseURL: `http://127.0.0.1:${llm.port}/v1` },
      },
    },
  })

  let server: Awaited<ReturnType<typeof startOpenCodeServer>> | undefined
  let app:
    | {
        api: TuiPluginApi
        setup: Awaited<ReturnType<typeof createTestRenderer>>
        stop: () => Promise<void>
      }
    | undefined
  let current: Awaited<ReturnType<typeof createTestRenderer>> | undefined
  try {
    server = await startOpenCodeServer(home, configContent)
    const url = server.url
    await mkdir(state, { recursive: true })
    await Bun.write(path.join(state, "kv.json"), "{}")

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
    const client = createOpencodeClient({ baseUrl: url, directory: home, fetch })

    const mount = async () => {
      const setup = await createTestRenderer({ width: 100, height: 32, useThread: false, kittyKeyboard: true })
      current = setup

      let api!: TuiPluginApi
      let started!: () => void
      const ready = new Promise<void>((resolve) => (started = resolve))
      const task = Effect.runPromise(
        run({
          url,
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

    app = await mount()
    await until(() => app!.setup.renderer.currentFocusedEditor instanceof TextareaRenderable, "app startup")
    expect(
      app.api.keymap
        .getCommandEntries({ namespace: "palette", visibility: "reachable" })
        .some((entry) => entry.command.name === "session.compaction.threshold"),
    ).toBe(true)

    await until(
      () => {
        app!.api.keymap.dispatchCommand("session.compaction.threshold")
        return app!.setup.captureCharFrame().includes("Current: 95%")
      },
      "threshold dialog",
      30_000,
    )
    await app.setup.mockInput.typeText("72")
    app.setup.mockInput.pressEnter()
    await until(
      () => app!.setup.captureCharFrame().includes("Auto-compaction threshold set to 72%"),
      "threshold save confirmation",
    )

    await app.stop()
    app = await mount()
    await until(
      () => {
        app!.api.keymap.dispatchCommand("session.compaction.threshold")
        return app!.setup.captureCharFrame().includes("Current: 72%")
      },
      "reloaded threshold",
      30_000,
    )
    await app.stop()
    app = undefined

    const config = await client.config.get({}, { throwOnError: true })
    expect(config.data.compaction?.threshold_percent).toBe(72)

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
    const messages = (await client.session.messages({ sessionID: session.id, directory: home }, { throwOnError: true }))
      .data
    const compaction = messages.flatMap((message) => message.parts).find((part) => part.type === "compaction")
    expect(compaction).toMatchObject({ auto: true, trigger: "threshold", resume: true })
    expect(
      messages.some((message) =>
        message.parts.some(
          (part) => part.type === "text" && part.text === "continued automatically after threshold compaction",
        ),
      ),
    ).toBe(true)
    expect(llmReplyIndex).toBe(3)
  } finally {
    if (app && !app.setup.renderer.isDestroyed) await app.stop()
    mock.restore()
    await llm.stop(true)
    if (server) await server.stop()
  }
})

async function startOpenCodeServer(home: string, configContent: string) {
  const proc = Bun.spawn(
    ["bun", "run", path.resolve(import.meta.dir, "../../../opencode/src/index.ts"), "serve", "--port", "0"],
    {
      cwd: home,
      env: {
        ...process.env,
        HOME: home,
        XDG_CONFIG_HOME: path.join(home, ".config"),
        XDG_DATA_HOME: path.join(home, ".local/share"),
        XDG_STATE_HOME: path.join(home, ".local/state"),
        XDG_CACHE_HOME: path.join(home, ".cache"),
        OPENCODE_TEST_HOME: home,
        OPENCODE_DB: ":memory:",
        OPENCODE_CONFIG: "",
        OPENCODE_CONFIG_DIR: "",
        OPENCODE_CONFIG_CONTENT: configContent,
        OPENCODE_DISABLE_PROJECT_CONFIG: "0",
        OPENCODE_PURE: "1",
        OPENCODE_DISABLE_AUTOUPDATE: "1",
        OPENCODE_DISABLE_AUTOCOMPACT: "0",
        OPENCODE_DISABLE_MODELS_FETCH: "1",
        OPENCODE_AUTH_CONTENT: "{}",
      },
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    },
  )
  const stderr = new Response(proc.stderr).text()
  const reader = proc.stdout.getReader()
  let timeout: Timer | undefined
  let ready = false
  try {
    const url = await Promise.race([
      (async () => {
        const decoder = new TextDecoder()
        let output = ""
        while (true) {
          const result = await reader.read()
          if (result.done) throw new Error(`OpenCode server exited before ready: ${await stderr}`)
          output += decoder.decode(result.value, { stream: true })
          const match = output.match(/listening on (http:\/\/[^\s]+)/)
          if (!match?.[1]) continue
          void drain(reader)
          return match[1]
        }
      })(),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new Error("OpenCode server readiness timed out")), 30_000)
      }),
    ])
    ready = true
    return {
      url,
      async stop() {
        proc.kill()
        await proc.exited
        await stderr
      },
    }
  } finally {
    if (timeout) clearTimeout(timeout)
    if (!ready) {
      proc.kill()
      await proc.exited
      await stderr
    }
  }
}

async function drain(reader: ReadableStreamDefaultReader<Uint8Array>) {
  while (!(await reader.read()).done) {}
}

async function until(fn: () => boolean, what: string, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (fn()) return
    await Bun.sleep(50)
  }
  throw new Error(`timed out waiting for ${what}`)
}
