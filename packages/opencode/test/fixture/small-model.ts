import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { LLMEvent, Usage } from "@opencode-ai/llm"
import { LLM } from "@/session/llm"
import { Layer, Stream } from "effect"
import { testProviderConfig } from "../lib/test-provider"

export function smallModelConfig(url = "https://example.com", infer = false): Partial<ConfigV1.Info> {
  const configured = testProviderConfig(url).provider.test
  const primary = {
    ...configured,
    models: {
      ...configured.models,
      ...(infer
        ? { "inferred-small": { ...configured.models["test-model"], id: "inferred-small", family: "gpt-nano" } }
        : {}),
    },
  }
  return {
    model: "test/test-model",
    formatter: false,
    lsp: false,
    snapshot: false,
    provider: {
      test: primary,
      azure: primary,
      "azure-cognitive-services": primary,
      auxiliary: {
        ...primary,
        models: Object.fromEntries(
          ["global-small", "agent-small", "session-small", "other-small", "dedicated-small"].map((id) => [
            id,
            { ...primary.models["test-model"], id, name: id },
          ]),
        ),
      },
    },
  }
}

export function smallSelection(id: string) {
  return { providerID: ProviderV2.ID.make("auxiliary"), id: ModelV2.ID.make(id) }
}

export function recordSmallModelRequests() {
  const requests: LLM.StreamInput[] = []
  return {
    requests,
    layer: Layer.succeed(
      LLM.Service,
      LLM.Service.of({
        stream: (input) => {
          requests.push(input)
          const usage = new Usage({ inputTokens: 1, outputTokens: 1, totalTokens: 2 })
          return Stream.make(
            LLMEvent.textStart({ id: "text" }),
            LLMEvent.textDelta({ id: "text", text: "Auxiliary response" }),
            LLMEvent.textEnd({ id: "text" }),
            LLMEvent.stepFinish({ index: 0, reason: "stop", usage }),
            LLMEvent.finish({ reason: "stop", usage }),
          )
        },
      }),
    ),
  }
}

export const smallModelCases = [
  { name: "global", config: { small_model: "auxiliary/global-small" }, expected: "global-small" },
  {
    name: "agent over global and session",
    config: { small_model: "auxiliary/global-small", agent: { build: { small_model: "auxiliary/agent-small" } } },
    session: "session-small",
    expected: "agent-small",
  },
  {
    name: "session over global",
    config: { small_model: "auxiliary/global-small" },
    session: "session-small",
    expected: "session-small",
  },
  {
    name: "agent only",
    config: { agent: { build: { small_model: "auxiliary/agent-small" } } },
    expected: "agent-small",
  },
  { name: "session only", config: {}, session: "session-small", expected: "session-small" },
] satisfies { name: string; config: Partial<ConfigV1.Info>; session?: string; expected: string }[]
