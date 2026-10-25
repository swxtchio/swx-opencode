import path from "path"
import fs from "fs/promises"
import { describe, expect } from "bun:test"
import { LLM, type Model } from "@opencode-ai/llm"
import { Cause, DateTime, Effect, Exit, Layer } from "effect"
import { Headers } from "effect/unstable/http"
import { Config } from "@opencode-ai/core/config"
import { ConfigAgentPlugin } from "@opencode-ai/core/config/plugin/agent"
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

// Exercise production config loading into the provider catalog and the selected agents used by the resolver.
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
  yield* ConfigAgentPlugin.Plugin.effect(host).pipe(
    Effect.provideService(Config.Service, config),
    Effect.provide(Global.layerWith({ config: path.join(tmp.path, "global") })),
  )
  return {
    agents: yield* AgentV2.Service,
    models: yield* SessionRunnerModel.Service.pipe(
      Effect.provide(SessionRunnerModel.locationLayer),
      Effect.provideService(Config.Service, config),
    ),
  }
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
      const { models } = yield* loadJail(jail(rendered))

      const resolved = yield* models.resolve(session({ providerID, id: modelID }))

      expect({ provider: String(resolved.provider), id: String(resolved.id) }).toEqual({
        provider: String(providerID),
        id: String(modelID),
      })
      expect(resolved.route.endpoint).toMatchObject({ baseURL: rendered.baseURL })
      expect(yield* authorization(resolved)).toBe(`Bearer ${rendered.apiKey}`)
    }),
  )

  it.effect("selects the configured egress default for a V2 session that names no model", () =>
    Effect.gen(function* () {
      // A global provider loads first and so sorts ahead of the egress one; only the configured default selects egress.
      const { models } = yield* loadJail(jail(rendered, `${providerID}/${modelID}`), {
        provider: {
          other: {
            npm: "@ai-sdk/openai-compatible",
            options: { baseURL: "https://other.test/v1", apiKey: "other" },
            models: { chat: { name: "Other" } },
          },
        },
      })

      const resolved = yield* models.resolve(session())

      expect({ provider: String(resolved.provider), id: String(resolved.id) }).toEqual({
        provider: String(providerID),
        id: String(modelID),
      })
      expect(resolved.route.endpoint).toMatchObject({ baseURL: rendered.baseURL })
    }),
  )
})

describe("auxiliary models", () => {
  const smallID = ModelV2.ID.make("accounts/fireworks/models/small-flash")
  const agentID = ModelV2.ID.make("accounts/fireworks/models/agent-pick")
  const primaryOneSmallID = ModelV2.ID.make("accounts/fireworks/models/primary-one-small")
  const primaryTwoSmallID = ModelV2.ID.make("accounts/fireworks/models/primary-two-small")
  const globalProviderID = ProviderV2.ID.make("global-small")
  const globalSmallID = ModelV2.ID.make("models/small/utility")
  // The jail's provider with configured small models, as a pool would configure a conversation's snapshotted small model.
  const content = {
    ...jail(rendered),
    provider: {
      "swxtch-egress": {
        ...jail(rendered).provider["swxtch-egress"],
        models: {
          [modelID]: { name: "DeepSeek V4.1 Flash" },
          [smallID]: { name: "Small flash" },
          [agentID]: { name: "Agent pick" },
          [primaryOneSmallID]: { name: "Primary one small" },
          [primaryTwoSmallID]: { name: "Primary two small" },
        },
      },
    },
    agent: {
      "primary-one": { small_model: `${providerID}/${primaryOneSmallID}` },
      "primary-two": { small_model: `${providerID}/${primaryTwoSmallID}` },
    },
  }
  const global = {
    small_model: `${globalProviderID}/${globalSmallID}`,
    provider: {
      [globalProviderID]: {
        npm: "@ai-sdk/openai-compatible",
        options: rendered,
        models: { [globalSmallID]: { name: "Global utility small" } },
      },
    },
  }
  const unavailableGlobal = { ...global, small_model: `${globalProviderID}/models/unavailable` }
  const primary = { providerID, id: modelID }

  it.effect("prefers a session small model over an unavailable global selection", () =>
    Effect.gen(function* () {
      const { models } = yield* loadJail(content, unavailableGlobal)
      const selected = session(primary, { providerID, id: smallID })

      const resolved = yield* models.auxiliary({
        session: selected,
        agent: undefined,
        originAgent: undefined,
        primary: models.resolve(selected),
        infer: false,
      })

      expect({ provider: String(resolved.provider), id: String(resolved.id) }).toEqual({
        provider: String(providerID),
        id: String(smallID),
      })
      expect(resolved.route.endpoint).toMatchObject({ baseURL: rendered.baseURL })
      expect(yield* authorization(resolved)).toBe(`Bearer ${rendered.apiKey}`)
    }),
  )

  it.effect("prefers the auxiliary agent model over its small_model and other selections", () =>
    Effect.gen(function* () {
      const { models, agents } = yield* loadJail(content, global)
      const selected = session(primary, { providerID, id: smallID })
      const originAgent = yield* agents.get(AgentV2.ID.make("primary-one"))
      if (!originAgent) throw new Error("expected configured primary agent primary-one")
      const missing = ModelV2.ID.make("accounts/fireworks/models/unavailable")

      const resolved = yield* models.auxiliary({
        session: selected,
        agent: {
          ...AgentV2.Info.empty(AgentV2.ID.make("title")),
          model: { providerID, id: agentID },
          smallModel: { providerID, id: missing },
        },
        originAgent,
        primary: models.resolve(selected),
        infer: true,
      })

      expect({ provider: String(resolved.provider), id: String(resolved.id) }).toEqual({
        provider: String(providerID),
        id: String(agentID),
      })
    }),
  )

  it.effect("prefers an auxiliary agent small_model over originating, session, and global selections", () =>
    Effect.gen(function* () {
      const { models, agents } = yield* loadJail(content, global)
      const selected = session(primary, { providerID, id: smallID })
      const originAgent = yield* agents.get(AgentV2.ID.make("primary-one"))
      if (!originAgent) throw new Error("expected configured primary agent primary-one")

      const resolved = yield* models.auxiliary({
        session: selected,
        agent: {
          ...AgentV2.Info.empty(AgentV2.ID.make("compaction")),
          smallModel: { providerID, id: agentID },
        },
        originAgent,
        primary: models.resolve(selected),
        infer: false,
      })

      expect({ provider: String(resolved.provider), id: String(resolved.id) }).toEqual({
        provider: String(providerID),
        id: String(agentID),
      })
    }),
  )

  it.effect("uses distinct originating primary-agent small models before session and global choices", () =>
    Effect.gen(function* () {
      const { models, agents } = yield* loadJail(content, global)
      const selected = session(primary, { providerID, id: smallID })
      const origins = [
        { id: "primary-one", smallModel: primaryOneSmallID },
        { id: "primary-two", smallModel: primaryTwoSmallID },
      ]

      for (const origin of origins) {
        const originAgent = yield* agents.get(AgentV2.ID.make(origin.id))
        if (!originAgent) throw new Error(`expected configured primary agent ${origin.id}`)
        const resolved = yield* models.auxiliary({
          session: selected,
          agent: undefined,
          originAgent,
          primary: models.resolve(selected),
          infer: true,
        })

        expect({ provider: String(resolved.provider), id: String(resolved.id) }).toEqual({
          provider: String(providerID),
          id: String(origin.smallModel),
        })
      }
    }),
  )

  it.effect("uses the effective global small model from another provider with its full model ID", () =>
    Effect.gen(function* () {
      const { models } = yield* loadJail(content, global)
      const selected = session(primary)

      const resolved = yield* models.auxiliary({
        session: selected,
        agent: undefined,
        originAgent: undefined,
        primary: models.resolve(selected),
        infer: true,
      })

      expect({ provider: String(resolved.provider), id: String(resolved.id) }).toEqual({
          provider: String(globalProviderID),
          id: String(globalSmallID),
      })
      expect(resolved.route.endpoint).toMatchObject({ baseURL: rendered.baseURL })
      expect(yield* authorization(resolved)).toBe(`Bearer ${rendered.apiKey}`)
    }),
  )

  it.effect("fails without a substitute when the global small_model is unavailable", () =>
    Effect.gen(function* () {
      const { models } = yield* loadJail(content, unavailableGlobal)
      const selected = session(primary)
      const result = yield* models
        .auxiliary({
          session: selected,
          agent: undefined,
          originAgent: undefined,
          primary: models.resolve(selected),
          infer: true,
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(result)).toBe(true)
      if (Exit.isFailure(result))
        expect(Cause.squash(result.cause)).toMatchObject({
          _tag: "SessionRunnerModel.ModelUnavailableError",
          providerID: globalProviderID,
          modelID: ModelV2.ID.make("models/unavailable"),
        })
    }),
  )

  it.effect("falls back to the primary when no small model is selected or inferred", () =>
    Effect.gen(function* () {
      const { models } = yield* loadJail(content)
      const selected = session(primary)

      for (const infer of [false, true]) {
        const resolved = yield* models.auxiliary({
          session: selected,
          agent: undefined,
          originAgent: undefined,
          primary: models.resolve(selected),
          infer,
        })
        expect({ provider: String(resolved.provider), id: String(resolved.id) }).toEqual({
          provider: String(providerID),
          id: String(modelID),
        })
      }
    }),
  )

  it.effect("fails without a substitute when the session small model is unavailable", () =>
    Effect.gen(function* () {
      const { models } = yield* loadJail(content, global)
      const missing = ModelV2.ID.make("accounts/fireworks/models/missing")
      const selected = session(primary, { providerID, id: missing })

      const failure = yield* models
        .auxiliary({
          session: selected,
          agent: undefined,
          originAgent: undefined,
          primary: models.resolve(selected),
          infer: true,
        })
        .pipe(Effect.flip)

      expect(failure).toMatchObject({
        _tag: "SessionRunnerModel.ModelUnavailableError",
        providerID,
        modelID: missing,
      })
    }),
  )
})
