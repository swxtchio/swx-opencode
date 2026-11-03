import { describe, expect, test } from "bun:test"
import { fileURLToPath } from "node:url"
import { sanitizeServerActionRequest } from "../src/lib/server-action"

const serverFunctionHandler = fileURLToPath(
  new URL("./server/server-functions-handler.js", import.meta.resolve("@solidjs/start")),
)
const seroval = await import(Bun.resolveSync("seroval", serverFunctionHandler))
const webPlugins = await import(Bun.resolveSync("seroval-plugins/web", serverFunctionHandler))
const serverFunctionPlugins = [
  webPlugins.CustomEventPlugin,
  webPlugins.DOMExceptionPlugin,
  webPlugins.EventPlugin,
  webPlugins.FormDataPlugin,
  webPlugins.HeadersPlugin,
  webPlugins.ReadableStreamPlugin,
  webPlugins.RequestPlugin,
  webPlugins.ResponsePlugin,
  webPlugins.URLSearchParamsPlugin,
  webPlugins.URLPlugin,
]

describe("server action referer", () => {
  test("preserves same-origin return locations", () => {
    const request = new Request("https://dev.opencode.ai/_server?id=action", {
      headers: { referer: "https://dev.opencode.ai/auth?next=%2Fconsole" },
    })

    expect(sanitizeServerActionRequest(request)).toBe(request)
  })

  test("replaces unsafe return locations with the request origin", () => {
    const referers = ["https://evil.example/phishing-login", "not a url", undefined]

    expect(
      referers.map((referer) =>
        sanitizeServerActionRequest(
          new Request("https://dev.opencode.ai/_server?id=action", {
            headers: referer === undefined ? undefined : { referer },
          }),
        ).headers.get("referer"),
      ),
    ).toEqual(["https://dev.opencode.ai", "https://dev.opencode.ai", "https://dev.opencode.ai"])
  })

  test("does not change other routes", () => {
    const request = new Request("https://dev.opencode.ai/auth", {
      headers: { referer: "https://evil.example/phishing-login" },
    })

    expect(sanitizeServerActionRequest(request)).toBe(request)
  })
})

describe("server function payloads", () => {
  test("rejects a forged Promise resolver before invoking a callable plugin", () => {
    const invoked: unknown[] = []
    const plugin = {
      tag: "test/PromiseResolver",
      deserialize() {
        return {
          resolve(value: unknown) {
            invoked.push(value)
          },
        }
      },
    }
    const payload = {
      f: 0,
      m: [1],
      t: {
        t: 9,
        i: 0,
        l: 2,
        a: [
          { t: 25, i: 1, c: plugin.tag, s: null },
          { t: 23, i: 1, a: [{ t: 4, i: 2 }, { t: 1, s: "attacker-controlled" }] },
        ],
        o: 0,
      },
    }

    expect(() => seroval.fromJSON(payload, { plugins: [...serverFunctionPlugins, plugin] })).toThrow()
    expect(invoked).toEqual([])
  })

  test("round-trips server-function arguments and results with the installed codecs", () => {
    const args = [
      {
        model: "gpt-6",
        endpoint: new URL("https://example.test/models/gpt-6"),
        query: new URLSearchParams("source=console&limit=2"),
      },
    ]
    const decodedArgs = seroval.fromJSON(seroval.toJSON(args, { plugins: serverFunctionPlugins }), {
      plugins: serverFunctionPlugins,
    })
    expect(decodedArgs[0].model).toBe("gpt-6")
    expect(decodedArgs[0].endpoint.href).toBe("https://example.test/models/gpt-6")
    expect(decodedArgs[0].query.toString()).toBe("source=console&limit=2")

    const result = {
      found: true,
      url: new URL("https://example.test/models/gpt-6"),
    }
    const decodedResult = seroval.fromCrossJSON(seroval.toCrossJSON(result, { plugins: serverFunctionPlugins }), {
      plugins: serverFunctionPlugins,
    })
    expect(decodedResult.found).toBe(true)
    expect(decodedResult.url.href).toBe("https://example.test/models/gpt-6")
  })
})
