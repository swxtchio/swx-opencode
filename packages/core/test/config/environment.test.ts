import path from "path"
import fs from "fs/promises"
import { describe, expect } from "bun:test"
import { LLM, type Model } from "@opencode-ai/llm"
import { DateTime, Effect, Layer } from "effect"
import { Headers } from "effect/unstable/http"
import { Config } from "@opencode-ai/core/config"
import { ConfigProviderPlugin } from "@opencode-ai/core/config/plugin/provider"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Global } from "@opencode-ai/core/global"
import { Location } from "@opencode-ai/core/location"
import { AgentV2 } from "@opencode-ai/core/agent"
import { ModelV2 } from "@opencode-ai/core/model"
import { PluginV2 } from "@opencode-ai/core/plugin"
import { PluginHost } from "@opencode-ai/core/plugin/host"
import { Policy } from "@opencode-ai/core/policy"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionRunnerModel } from "@opencode-ai/core/session/runner/model"
import { location } from "../fixture/location"
import { tmpdir } from "../fixture/tmpdir"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "../plugin/fixture"

const it = testEffect(PluginTestLayer)

const providerID = ProviderV2.ID.make("swxtch-egress")
const modelID = ModelV2.ID.make("accounts/fireworks/models/deepseek-v4p1-flash")

// The OpenCode jail's V1 config: the image names it with OPENCODE_CONFIG, and its
// entrypoint renders the proxy values into OPENCODE_CONFIG_CONTENT.
const jail = (options: { baseURL: string; apiKey: string }, model?: string) => ({
  $schema: "https://opencode.ai/config.json",
  autoupdate: false,
  plugin: [],
  ...(model ? { model } : {}),
  provider: {
    "swxtch-egress": {
      npm: "@ai-sdk/openai-compatible",
      name: "swXtch egress proxy",
      options,
      models: { [modelID]: { name: "DeepSeek V4.1 Flash" } },
    },
  },
})

const placeholders = { baseURL: "{env:OPENCODE_PROXY_BASE_URL}", apiKey: "{env:OPENCODE_PROXY_CAPABILITY}" }
const rendered = { baseURL: "https://egress.test/v1", apiKey: "capability" }

// Loads the jail's config sources through the real Config loader and the config provider plugin into the catalog.
const loadJail = Effect.fn(function* (content: unknown, global?: unknown) {
  const tmp = yield* Effect.acquireRelease(
    Effect.promise(() => tmpdir()),
    (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
  )
  const file = path.join(tmp.path, "jail", "opencode.json")
  const directory = path.join(tmp.path, "agent")
  yield* Effect.promise(async () => {
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.mkdir(path.join(tmp.path, "global"), { recursive: true })
    await fs.mkdir(directory, { recursive: true })
    await fs.writeFile(file, JSON.stringify(jail(placeholders)))
    if (global) await fs.writeFile(path.join(tmp.path, "global", "opencode.json"), JSON.stringify(global))
  })
  const config = yield* Config.Service.pipe(
    Effect.provide(
      AppNodeBuilder.build(LayerNode.group([Config.node, Policy.node]), [
        [
          Location.node,
          Layer.succeed(
            Location.Service,
            Location.Service.of(
              location({ directory: AbsolutePath.make(directory) }, { projectDirectory: AbsolutePath.make(directory) }),
            ),
          ),
        ],
        [Global.node, Global.layerWith({ config: path.join(tmp.path, "global") })],
        [Config.node, Config.nodeWith({ file, content: JSON.stringify(content) })],
      ]),
    ),
  )
  const plugin = yield* PluginV2.Service
  const host = yield* PluginHost.make(plugin)
  yield* ConfigProviderPlugin.Plugin.effect(host).pipe(Effect.provideService(Config.Service, config))
  return yield* SessionRunnerModel.Service.pipe(Effect.provide(SessionRunnerModel.locationLayer))
})

const session = (model?: SessionV2.Info["model"], smallModel?: SessionV2.Info["smallModel"]) =>
  SessionV2.Info.make({
    id: SessionV2.ID.make("ses_jail_egress"),
    projectID: ProjectV2.ID.global,
    title: "test",
    model,
    smallModel,
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: DateTime.makeUnsafe(0), updated: DateTime.makeUnsafe(0) },
    location: { directory: AbsolutePath.make("/project") },
  })

const authorization = Effect.fn(function* (resolved: Model) {
  const headers = yield* resolved.route.auth.apply({
    request: LLM.request({ model: resolved, prompt: "Hello" }),
    method: "POST",
    url: `${rendered.baseURL}/chat/completions`,
    body: "{}",
    headers: Headers.empty,
  })
  return headers.authorization
})

describe("environment-supplied config", () => {
  it.effect("resolves a V2 session's named jail egress model at the rendered proxy", () =>
    Effect.gen(function* () {
      const models = yield* loadJail(jail(rendered))

      const resolved = yield* models.resolve(session({ providerID, id: modelID }))

      expect(resolved).toMatchObject({ provider: providerID, id: modelID })
      expect(resolved.route.endpoint).toMatchObject({ baseURL: rendered.baseURL })
      expect(yield* authorization(resolved)).toBe(`Bearer ${rendered.apiKey}`)
    }),
  )

  it.effect("selects the configured egress default for a V2 session that names no model", () =>
    Effect.gen(function* () {
      // A global provider loads first and so sorts ahead of the egress one; only the configured default selects egress.
      const models = yield* loadJail(jail(rendered, `${providerID}/${modelID}`), {
        provider: {
          other: {
            npm: "@ai-sdk/openai-compatible",
            options: { baseURL: "https://other.test/v1", apiKey: "other" },
            models: { chat: { name: "Other" } },
          },
        },
      })

      const resolved = yield* models.resolve(session())

      expect(resolved).toMatchObject({ provider: providerID, id: modelID })
      expect(resolved.route.endpoint).toMatchObject({ baseURL: rendered.baseURL })
    }),
  )
})

describe("auxiliary models", () => {
  const smallID = ModelV2.ID.make("accounts/fireworks/models/small-flash")
  const agentID = ModelV2.ID.make("accounts/fireworks/models/agent-pick")
  // The jail's provider with two more models, as a pool would configure a conversation's snapshotted small model.
  const content = {
    ...jail(rendered),
    provider: {
      "swxtch-egress": {
        ...jail(rendered).provider["swxtch-egress"],
        models: {
          [modelID]: { name: "DeepSeek V4.1 Flash" },
          [smallID]: { name: "Small flash" },
          [agentID]: { name: "Agent pick" },
        },
      },
    },
  }
  const primary = { providerID, id: modelID }

  it.effect("resolves a V2 session's small model at the rendered proxy", () =>
    Effect.gen(function* () {
      const models = yield* loadJail(content)
      const selected = session(primary, { providerID, id: smallID })

      const resolved = yield* models.auxiliary({
        session: selected,
        agent: undefined,
        primary: models.resolve(selected),
        infer: false,
      })

      expect(resolved).toMatchObject({ provider: providerID, id: smallID })
      expect(resolved.route.endpoint).toMatchObject({ baseURL: rendered.baseURL })
      expect(yield* authorization(resolved)).toBe(`Bearer ${rendered.apiKey}`)
    }),
  )

  it.effect("prefers the auxiliary agent's own model over the session small model", () =>
    Effect.gen(function* () {
      const models = yield* loadJail(content)
      const selected = session(primary, { providerID, id: smallID })

      const resolved = yield* models.auxiliary({
        session: selected,
        agent: { ...AgentV2.Info.empty(AgentV2.ID.make("title")), model: { providerID, id: agentID } },
        primary: models.resolve(selected),
        infer: true,
      })

      expect(resolved).toMatchObject({ provider: providerID, id: agentID })
    }),
  )

  it.effect("falls back to the primary when no small model is selected or inferred", () =>
    Effect.gen(function* () {
      const models = yield* loadJail(content)
      const selected = session(primary)

      for (const infer of [false, true]) {
        const resolved = yield* models.auxiliary({
          session: selected,
          agent: undefined,
          primary: models.resolve(selected),
          infer,
        })
        expect(resolved).toMatchObject({ provider: providerID, id: modelID })
      }
    }),
  )

  it.effect("fails without a substitute when the session small model is unavailable", () =>
    Effect.gen(function* () {
      const models = yield* loadJail(content)
      const missing = ModelV2.ID.make("accounts/fireworks/models/missing")
      const selected = session(primary, { providerID, id: missing })

      const failure = yield* models
        .auxiliary({ session: selected, agent: undefined, primary: models.resolve(selected), infer: true })
        .pipe(Effect.flip)

      expect(failure).toMatchObject({
        _tag: "SessionRunnerModel.ModelUnavailableError",
        providerID,
        modelID: missing,
      })
    }),
  )
})
