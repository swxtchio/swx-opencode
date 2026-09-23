import { describe, expect, test } from "bun:test"
import { mockOpenCodeServer, providerCatalog } from "./mock-server"
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
  test("every derived model is a payload a real server could return", () => {
    // The normaliser is tolerant: it accepted an ISO string for a numeric
    // `time.released`, a missing `status` and a missing `limit.output`. So
    // asserting "it does not throw" proves nothing about wire fidelity.
    // These assert the ModelInfo contract's required fields directly.
    const catalog = providerCatalog(
      config({
        all: [
          {
            id: "opencode",
            name: "OpenCode",
            models: { test: { id: "test", name: "Test", limit: { context: 200_000 } } },
          },
        ],
        connected: ["opencode"],
        default: { providerID: "opencode", modelID: "test" },
      }),
    )
    for (const provider of catalog.providers) {
      expect(typeof provider.id).toBe("string")
      expect(typeof provider.name).toBe("string")
      expect(typeof provider.package).toBe("string")
    }
    expect(catalog.models.length).toBeGreaterThan(0)
    for (const model of catalog.models) {
      expect(typeof model.time.released).toBe("number")
      expect(["alpha", "beta", "deprecated", "active"]).toContain(model.status)
      expect(typeof model.enabled).toBe("boolean")
      expect(typeof model.limit.context).toBe("number")
      expect(typeof model.limit.output).toBe("number")
      expect(Array.isArray(model.cost)).toBe(true)
      expect(Array.isArray(model.variants)).toBe(true)
      expect(Array.isArray(model.capabilities.input)).toBe(true)
      expect(typeof model.capabilities.tools).toBe("boolean")
    }
  })
})

/**
 * GOAL: the three v2 catalog ROUTES exist and answer with the right envelope.
 *
 * The tests above exercise `providerCatalog()` directly, which means deleting
 * or renaming the routes that serve it would leave them all green — the exact
 * hole that let `/api/provider` fall through to the catch-all `{}` in the
 * first place. These drive the real route handler through a stub page, so the
 * wiring itself is covered without needing a browser.
 */
describe("mock server v2 catalog routes", () => {
  async function fulfilFor(pathname: string) {
    let handler: ((route: unknown) => Promise<unknown>) | undefined
    const page = {
      route: (_pattern: string, fn: (route: unknown) => Promise<unknown>) => {
        handler = fn
        return Promise.resolve()
      },
      on: () => {},
    } as unknown as Parameters<typeof mockOpenCodeServer>[0]

    await mockOpenCodeServer(page, config({
      all: [{ id: "opencode", name: "OpenCode", models: { test: { id: "test", name: "Test", limit: { context: 200_000 } } } }],
      connected: ["opencode"],
      default: { providerID: "opencode", modelID: "test" },
    }) as unknown as Parameters<typeof mockOpenCodeServer>[1])

    expect(handler).toBeDefined()
    let fulfilled: { status?: number; body?: string } | undefined
    const port = process.env.PLAYWRIGHT_SERVER_PORT ?? "4096"
    await handler!({
      request: () => ({ url: () => `http://127.0.0.1:${port}${pathname}`, method: () => "GET", postDataJSON: () => ({}) }),
      fulfill: async (response: { status?: number; body?: string }) => {
        fulfilled = response
      },
      fallback: async () => {
        throw new Error(`route fell through to fallback: ${pathname}`)
      },
    })
    expect(fulfilled, `${pathname} was not fulfilled`).toBeDefined()
    return JSON.parse(fulfilled!.body ?? "null")
  }

  test("/api/provider returns the provider array in a location envelope", async () => {
    const body = await fulfilFor("/api/provider")
    expect(body).toHaveProperty("location")
    expect(Array.isArray(body.data)).toBe(true)
    expect(body.data.map((p: { id: string }) => p.id)).toEqual(["opencode"])
  })

  test("/api/model returns the model array in a location envelope", async () => {
    const body = await fulfilFor("/api/model")
    expect(body).toHaveProperty("location")
    expect(Array.isArray(body.data)).toBe(true)
    expect(body.data.map((m: { id: string }) => m.id)).toEqual(["test"])
  })

  test("/api/model/default returns the configured default", async () => {
    const body = await fulfilFor("/api/model/default")
    expect(body).toHaveProperty("location")
    expect(body.data?.id).toBe("test")
  })

  test("none of the three routes answers with the catch-all empty object", async () => {
    // The original bug: these paths hit `json(route, {})` at the bottom of the
    // handler, so `providers.data` was undefined and the app crashed.
    for (const pathname of ["/api/provider", "/api/model", "/api/model/default"]) {
      const body = await fulfilFor(pathname)
      expect(body, `${pathname} returned the catch-all empty object`).not.toEqual({})
      expect(body?.data, `${pathname} has no data field`).toBeDefined()
    }
  })
})
