import { describe, expect, test } from "bun:test"
import type { Provider } from "@opencode-ai/sdk/v2"
import { parse, servedAcrossSession, servedName, type SessionStepMessage } from "../../src/util/model"

describe("util.model", () => {
  test("splits provider from a nested model identifier", () => {
    expect(parse("provider/org/model")).toEqual({ providerID: "provider", modelID: "org/model" })
    expect(parse("invalid")).toEqual({ providerID: "invalid", modelID: "" })
  })

  // A router route slug is not itself a model, so the served id is the only
  // record of what ran - and of what the turn actually cost.
  const providers = [
    {
      id: "firerouter",
      models: {
        "firerouter/glm-5p3/glm-5p3-flash": { name: "Firerouter glm" },
        "glm-5p3": { name: "GLM-5.3" },
      },
    },
    {
      id: "fireworks",
      models: {
        "accounts/fireworks/models/glm-5p3": { name: "GLM-5.3" },
        // An alias id for the same model, so the suppression below is testing
        // two different ids that resolve to one name - not two equal ids.
        "glm-5p3": { name: "GLM-5.3" },
      },
    },
    {
      id: "llmrouter",
      models: {
        auto: { name: "Auto" },
        "luna-max": { name: "luna-max" },
        "glm-5.3-flash": { name: "glm-5.3-flash" },
        "sol-high": { name: "sol-high" },
      },
    },
  ] as unknown as Provider[]

  function servedStep(
    id: string,
    sessionID: string,
    parentID: string,
    responseModelIDs: string[],
    extra: Record<string, unknown> = {},
  ): SessionStepMessage {
    return {
      info: {
        id,
        sessionID,
        role: "assistant",
        time: { created: Number(id.replace(/\D/g, "")) || 1 },
        providerID: "llmrouter",
        modelID: "auto",
        parentID,
        summary: extra.summary as boolean | undefined,
      } as SessionStepMessage["info"],
      parts: responseModelIDs.map((responseModelID, index) => ({
        id: `${id}-step-${index}`,
        sessionID,
        messageID: id,
        type: "step-finish",
        reason: "stop",
        responseModelID,
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      })),
    }
  }

  test("shows only the configured name when nothing recorded what served the turn", () => {
    expect(servedName(providers, "firerouter", "firerouter/glm-5p3/glm-5p3-flash", undefined)).toBe("Firerouter glm")
    expect(servedName(providers, "firerouter", "firerouter/glm-5p3/glm-5p3-flash", [])).toBe("Firerouter glm")
  })

  test("stays byte-identical for a direct provider, which echoes its own id back", () => {
    expect(
      servedName(providers, "fireworks", "accounts/fireworks/models/glm-5p3", ["accounts/fireworks/models/glm-5p3"]),
    ).toBe("GLM-5.3")
  })

  test("appends the model a router actually picked", () => {
    expect(servedName(providers, "firerouter", "firerouter/glm-5p3/glm-5p3-flash", ["glm-5p3-flash"])).toBe(
      "Firerouter glm (glm-5p3-flash)",
    )
  })

  test("appends every model a multi-step turn used, in the order they ran", () => {
    expect(servedName(providers, "firerouter", "firerouter/glm-5p3/glm-5p3-flash", ["glm-5p3-flash", "glm-5p3"])).toBe(
      "Firerouter glm (glm-5p3-flash \u2192 GLM-5.3)",
    )
  })

  test("resolves a served id to its display name when the catalog knows it", () => {
    expect(servedName(providers, "firerouter", "firerouter/glm-5p3/glm-5p3-flash", ["glm-5p3"])).toBe(
      "Firerouter glm (GLM-5.3)",
    )
  })

  // Raised in review: two DIFFERENT ids that share a display name are still
  // different models - different versions, routes or prices - so the served id
  // is shown. Only an id identical to the requested one is redundant.
  test("disambiguates a served id whose display name collides with the configured one", () => {
    expect(servedName(providers, "fireworks", "accounts/fireworks/models/glm-5p3", ["glm-5p3"])).toBe(
      "GLM-5.3 (GLM-5.3 [glm-5p3])",
    )
  })

  test("counts the llmrouter auto pool by session request and keeps configured order", () => {
    expect(
      servedName(
        providers,
        "llmrouter",
        "auto",
        ["luna-max"],
        [...Array.from({ length: 9 }, () => "luna-max"), "glm-5.3-flash"],
      ),
    ).toBe("Auto (luna-max:9/90%, glm-5.3-flash:1/10%, sol-high:0/0%)")
  })

  test("uses router identity after the configured friendly name changes", () => {
    const renamed = [
      {
        ...providers[2],
        models: { ...providers[2]!.models, auto: { name: "Automatic routing" } },
      },
    ] as unknown as Provider[]
    expect(servedName(renamed, "llmrouter", "auto", undefined, ["luna-max"])).toBe(
      "Automatic routing (luna-max:1/100%, glm-5.3-flash:0/0%, sol-high:0/0%)",
    )
  })

  test("shows configured members with zero shares when there was no router usage", () => {
    expect(servedName(providers, "llmrouter", "auto", undefined, [])).toBe(
      "Auto (luna-max:0/0%, glm-5.3-flash:0/0%, sol-high:0/0%)",
    )
  })

  test("keeps an unknown served id visible and in the request total", () => {
    expect(
      servedName(providers, "llmrouter", "auto", undefined, ["outside-pool", "luna-max", "outside-pool", "glm-5.3-flash"]),
    ).toBe("Auto (luna-max:1/25%, glm-5.3-flash:1/25%, sol-high:0/0%, outside-pool:2/50%)")
  })

  test("rounds member shares to a total of 100 percent", () => {
    expect(servedName(providers, "llmrouter", "auto", undefined, ["luna-max", "glm-5.3-flash", "sol-high"])).toBe(
      "Auto (luna-max:1/34%, glm-5.3-flash:1/33%, sol-high:1/33%)",
    )
  })

  test("keeps a same-named auto model on another provider's existing label path", () => {
    const other = [{ id: "other", models: { auto: { name: "Auto" }, "luna-max": { name: "Luna Max" } } }] as unknown as Provider[]
    expect(servedName(other, "other", "auto", ["luna-max"], ["luna-max", "luna-max"])).toBe("Auto (Luna Max)")
  })

  test("keeps another llmrouter model with a similar name on the existing label path", () => {
    const other = [
      { id: "llmrouter", models: { auto: { name: "Auto" }, "auto-lite": { name: "Auto" }, member: { name: "Member" } } },
    ] as unknown as Provider[]
    expect(servedName(other, "llmrouter", "auto-lite", ["member"], ["member", "member"])).toBe("Auto (Member)")
  })

  test("counts repeated step-finish records through the current message only", () => {
    const earlier = servedStep("m1", "session-1", "user-1", ["luna-max", "luna-max"])
    const summary = servedStep("m-summary", "session-1", "user-1", ["glm-5.3-flash"], { summary: true })
    const current = servedStep("m2", "session-1", "user-2", ["glm-5.3-flash"])
    const later = servedStep("m3", "session-1", "user-3", ["sol-high"])
    const otherSession = servedStep("m4", "session-2", "user-4", ["sol-high"])
    const all = [earlier, summary, current, later, otherSession]
    const requests = servedAcrossSession(all, current.info.id)

    expect(requests).toEqual(["luna-max", "luna-max", "glm-5.3-flash"])
    expect(servedName(providers, "llmrouter", "auto", undefined, requests)).toBe(
      "Auto (luna-max:2/67%, glm-5.3-flash:1/33%, sol-high:0/0%)",
    )
    expect(servedAcrossSession(all, otherSession.info.id)).toEqual(["sol-high"])
  })

  test("excludes a compaction message even without its summary flag", () => {
    const compaction = {
      ...servedStep("m-summary", "session-1", "user-1", ["glm-5.3-flash"]),
      parts: [
        {
          id: "m-summary-compaction",
          sessionID: "session-1",
          messageID: "m-summary",
          type: "compaction",
        } as SessionStepMessage["parts"][number],
      ],
    }
    const current = servedStep("m2", "session-1", "user-2", ["luna-max"])
    expect(servedAcrossSession([compaction, current], current.info.id)).toEqual(["luna-max"])
  })
})
