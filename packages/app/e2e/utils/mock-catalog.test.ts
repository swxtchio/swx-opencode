import { describe, expect, test } from "bun:test"
import { providerCatalog } from "./mock-server"
import { normalizeProviderList } from "../../src/context/global-sync/utils"

/**
 * GOAL: the e2e mock's v2 catalog routes must return something
 * `normalizeProviderList` can actually consume.
 *
 * The mock had no `/api/provider`, `/api/model` or `/api/model/default` route
 * at all, so those requests hit the catch-all `{}` and the app threw
 * "Cannot read properties of undefined (reading 'all')" on every v2 spec that
 * booted a project (#26). The first fix for that then threw
 * "undefined is not an object (evaluating 'model.time.released')" instead,
 * because the derived models were missing fields the normaliser reads
 * unconditionally.
 *
 * Success means: for every provider shape the specs actually use, feeding the
 * mock's derived catalog through the real normaliser does not throw and
 * preserves the models. Asserting the contract, not the wiring — a route
 * returning 200 proves nothing if the payload crashes the consumer.
 */
const config = (provider: unknown) =>
  ({ directory: "C:/Mock", project: { id: "proj" }, provider }) as unknown as Parameters<typeof providerCatalog>[0]

function normalize(provider: unknown) {
  const catalog = providerCatalog(config(provider))
  return normalizeProviderList(catalog.providers as never, catalog.models as never, catalog.defaultModel as never)
}

describe("mock server v2 catalog", () => {
  test("an empty provider list normalizes to an empty catalog", () => {
    const result = normalize({ all: [], connected: [], default: {} })
    expect(result.all.size).toBe(0)
  })

  test("a provider with a model survives the normaliser", () => {
    const result = normalize({
      all: [{ id: "opencode", name: "OpenCode", models: { test: { id: "test", name: "Test", limit: { context: 200_000 } } } }],
      connected: ["opencode"],
      default: { providerID: "opencode", modelID: "test" },
    })
    expect([...result.all.keys()]).toEqual(["opencode"])
    expect(Object.keys(result.all.get("opencode")!.models)).toEqual(["test"])
    expect(result.defaultModel).toEqual({ providerID: "opencode", modelID: "test" })
  })

  test("model variants are carried across as the array the normaliser expects", () => {
    const result = normalize({
      all: [
        {
          id: "opencode",
          name: "OpenCode",
          models: {
            "thinking-model": { id: "thinking-model", name: "Thinking", limit: { context: 200_000 }, variants: { high: {} } },
          },
        },
      ],
      connected: ["opencode"],
      default: { providerID: "opencode", modelID: "thinking-model" },
    })
    expect(Object.keys(result.all.get("opencode")!.models["thinking-model"]!.variants ?? {})).toEqual(["high"])
  })

  test("a function-valued provider config is resolved", () => {
    const result = normalize(() => ({ all: [{ id: "p", name: "P", models: {} }], connected: [], default: {} }))
    expect([...result.all.keys()]).toEqual(["p"])
  })

  test("a missing provider config does not throw", () => {
    expect(() => normalize(undefined)).not.toThrow()
  })
})
