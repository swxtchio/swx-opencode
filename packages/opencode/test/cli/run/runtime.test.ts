import { afterEach, describe, expect, mock, spyOn, test } from "bun:test"
import { OpencodeClient } from "@opencode-ai/sdk/v2"
import { runInteractiveMode } from "@/cli/cmd/run/runtime"
import type { FooterApi, RunProvider } from "@/cli/cmd/run/types"

type SessionMessage = NonNullable<Awaited<ReturnType<OpencodeClient["session"]["messages"]>>["data"]>[number]

const provider: RunProvider = {
  id: "openai",
  name: "OpenAI",
  source: "api",
  env: [],
  options: {},
  models: {
    "gpt-5": {
      id: "gpt-5",
      providerID: "openai",
      api: {
        id: "openai",
        url: "https://openai.test",
        npm: "@ai-sdk/openai",
      },
      name: "Little Frank",
      capabilities: {
        temperature: true,
        reasoning: true,
        attachment: true,
        toolcall: true,
        input: {
          text: true,
          audio: false,
          image: false,
          video: false,
          pdf: false,
        },
        output: {
          text: true,
          audio: false,
          image: false,
          video: false,
          pdf: false,
        },
        interleaved: false,
      },
      cost: {
        input: 0,
        output: 0,
        cache: {
          read: 0,
          write: 0,
        },
      },
      limit: {
        context: 128000,
        output: 8192,
      },
      status: "active",
      options: {},
      headers: {},
      release_date: "2026-01-01",
    },
  },
}

const transportProviders: RunProvider[][] = []

function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

function ok<T>(data: T) {
  return Promise.resolve({
    data,
    error: undefined,
    request: new Request("https://opencode.test"),
    response: new Response(),
  })
}

function footer(): FooterApi {
  let closed = false
  const closes = new Set<() => void>()

  const notify = () => {
    for (const fn of closes) fn()
  }

  return {
    get isClosed() {
      return closed
    },
    onPrompt: () => () => {},
    onQueuedRemove: () => () => {},
    onClose(fn) {
      if (closed) {
        fn()
        return () => {}
      }

      closes.add(fn)
      return () => {
        closes.delete(fn)
      }
    },
    event() {},
    append() {},
    idle() {
      return Promise.resolve()
    },
    close() {
      if (closed) {
        return
      }

      closed = true
      notify()
    },
    destroy() {
      if (closed) {
        return
      }

      closed = true
      notify()
    },
  }
}

afterEach(() => {
  mock.restore()
  transportProviders.length = 0
})

// A provider whose two models declare different efforts, for the --effort tests.
function effortProvider(): RunProvider {
  const base = provider.models["gpt-5"]!
  return {
    ...provider,
    models: {
      "gpt-5": { ...base, variants: { low: {}, high: {} } },
      "gpt-4": { ...base, id: "gpt-4", variants: { low: {} } },
    },
  }
}

// Runs the interactive runtime with `variant` as --effort, and hands the test the lifecycle
// callbacks plus everything the runtime sent to the footer.
async function withEffort(variant: string, drive: (app: EffortApp) => Promise<void>) {
  const sdk = new OpencodeClient()
  const ready = defer<void>()
  // Closing before the eager transport exists makes the runtime fail as "runtime closed".
  const transported = defer<void>()
  spyOn(sdk.config, "providers").mockImplementation(() => ok({ providers: [effortProvider()], default: {} }))
  spyOn(sdk.session, "messages").mockImplementation(() => ok([]))
  spyOn(sdk.session, "get").mockRejectedValue(new Error("not needed"))
  spyOn(sdk.app, "agents").mockImplementation(() => ok([]))
  spyOn(sdk.experimental.resource, "list").mockImplementation(() => ok({}))
  spyOn(sdk.command, "list").mockImplementation(() => ok([]))
  const shell = footer()
  const app: EffortApp = { errors: [], variants: [], callbacks: undefined, close: () => shell.close() }
  const task = runInteractiveMode(
    {
      sdk,
      directory: "/tmp",
      sessionID: "ses-1",
      sessionTitle: "Session",
      resume: false,
      replay: false,
      replayLimit: 100,
      agent: "build",
      model: { providerID: "openai", modelID: "gpt-5" },
      variant,
      files: [],
      thinking: false,
      backgroundSubagents: false,
    },
    {
      createRuntimeLifecycle: async (input) => {
        app.callbacks = input
        return {
          footer: {
            ...shell,
            get isClosed() {
              return shell.isClosed
            },
            append: (commit) => {
              if (commit.kind === "error") app.errors.push(commit.text)
            },
            event: (event) => {
              if (event.type !== "variants") return
              app.variants.push(event.current)
              ready.resolve()
            },
          },
          onResize: () => () => {},
          refreshTheme: () => {},
          resetForReplay: () => Promise.resolve(),
          close: () => Promise.resolve(),
        }
      },
      streamTransport: Promise.resolve({
        createSessionTransport: async () => {
          transported.resolve()
          return {
            runPromptTurn: async () => {},
            selectSubagent: () => {},
            replayOnResize: async () => false,
            close: async () => {},
          }
        },
        formatUnknownError: (error: unknown) => String(error),
      }),
    },
  )
  await Promise.all([ready.promise, transported.promise])
  await drive(app)
  app.close()
  await task
}

type EffortApp = {
  errors: string[]
  variants: (string | undefined)[]
  callbacks:
    | Parameters<NonNullable<Parameters<typeof runInteractiveMode>[1]>["createRuntimeLifecycle"] & {}>[0]
    | undefined
  close: () => void
}

describe("run interactive runtime --effort", () => {
  // GOAL: an unknown --effort is refused at launch with the choices listed, not shown as active
  // and left to fail on submit.
  test("refuses an unknown effort at launch", async () => {
    await withEffort("hgih", async (app) => {
      expect(app.errors).toEqual(['Unknown effort "hgih" for openai/gpt-5. Available: high, low'])
      expect(app.variants.at(-1)).toBeUndefined()
    })
  })

  // GOAL: --effort applies to each model that declares it, is skipped (with a warning) for one
  // that does not, and stops applying once the user picks an effort in the app.
  test("follows models that declare it until an in-app choice", async () => {
    await withEffort("high", async (app) => {
      expect(app.variants.at(-1)).toBe("high")
      const other = await app.callbacks?.onModelSelect?.({ providerID: "openai", modelID: "gpt-4" })
      expect(other && "variant" in other ? other.variant : "missing").toBeUndefined()
      expect(app.errors).toEqual(['Unknown effort "high" for openai/gpt-4. Available: low'])
      const back = await app.callbacks?.onModelSelect?.({ providerID: "openai", modelID: "gpt-5" })
      expect(back && "variant" in back ? back.variant : "missing").toBe("high")

      await app.callbacks?.onVariantSelect?.("low")
      await app.callbacks?.onModelSelect?.({ providerID: "openai", modelID: "gpt-4" })
      const again = await app.callbacks?.onModelSelect?.({ providerID: "openai", modelID: "gpt-5" })
      expect(again && "variant" in again ? again.variant : "missing").not.toBe("high")
    })
  })
})

describe("run interactive runtime", () => {
  test("waits for provider metadata before eager replay transport bootstrap", async () => {
    const providersStarted = defer<void>()
    const providers = defer<void>()

    const sdk = new OpencodeClient()
    spyOn(sdk.config, "providers").mockImplementation(async () => {
      providersStarted.resolve()
      await providers.promise
      return ok({ providers: [provider], default: {} })
    })
    spyOn(sdk.session, "messages").mockImplementation(() =>
      ok([
        {
          info: {
            id: "msg-user-1",
            sessionID: "ses-1",
            role: "user",
            time: {
              created: 1,
            },
            agent: "build",
            model: {
              providerID: "openai",
              modelID: "gpt-5",
              variant: undefined,
            },
          },
          parts: [
            {
              id: "part-user-1",
              sessionID: "ses-1",
              messageID: "msg-user-1",
              type: "text",
              text: "hello",
            },
          ],
        } satisfies SessionMessage,
      ]),
    )
    spyOn(sdk.session, "get").mockRejectedValue(new Error("not needed"))
    spyOn(sdk.app, "agents").mockImplementation(() => ok([]))
    spyOn(sdk.experimental.resource, "list").mockImplementation(() => ok({}))
    spyOn(sdk.command, "list").mockImplementation(() => ok([]))

    const task = runInteractiveMode(
      {
        sdk,
        directory: "/tmp",
        sessionID: "ses-1",
        sessionTitle: "Session",
        resume: true,
        replay: true,
        replayLimit: 100,
        agent: "build",
        model: {
          providerID: "openai",
          modelID: "gpt-5",
        },
        variant: undefined,
        files: [],
        thinking: true,
        backgroundSubagents: false,
      },
      {
        createRuntimeLifecycle: async () => ({
          footer: footer(),
          onResize: () => () => {},
          refreshTheme: () => {},
          resetForReplay: () => Promise.resolve(),
          close: () => Promise.resolve(),
        }),
        streamTransport: Promise.resolve({
          createSessionTransport: async (input: { providers?: () => RunProvider[]; footer: FooterApi }) => {
            transportProviders.push(input.providers?.() ?? [])
            setTimeout(() => {
              input.footer.close()
            }, 0)
            return {
              runPromptTurn: async () => {},
              selectSubagent: () => {},
              replayOnResize: async () => false,
              close: async () => {},
            }
          },
          formatUnknownError: (error: unknown) => (error instanceof Error ? error.message : String(error)),
        }),
      },
    )

    await providersStarted.promise

    expect(transportProviders).toEqual([])

    providers.resolve()

    await task

    expect(transportProviders).toEqual([[provider]])
  })
})
