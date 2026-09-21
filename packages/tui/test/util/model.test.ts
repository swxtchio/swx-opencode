import { describe, expect, test } from "bun:test"
import type { Provider } from "@opencode-ai/sdk/v2"
import { parse, servedName } from "../../src/util/model"

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
  ] as unknown as Provider[]

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

  test("suppresses a served id that resolves to the configured name anyway", () => {
    expect(servedName(providers, "fireworks", "accounts/fireworks/models/glm-5p3", ["glm-5p3"])).toBe("GLM-5.3")
  })
})
