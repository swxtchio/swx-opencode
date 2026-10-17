import { connect, type Socket } from "node:net"

type Scenario = "completed" | "failed" | "interrupted"

const suppressedScenario = requireScenario(Bun.env.RELEASE_PROBE_WAIT_SUPPRESS_SCENARIO)
const attack = Bun.env.RELEASE_PROBE_WAIT_ATTACK ?? "late-dispatch"
const nativeFetch = globalThis.fetch
const scenarioSessions = new Map<string, Scenario>()
const released = new Set<Scenario>()
const releaseWaiters = new Map<Scenario, Set<() => void>>()

globalThis.fetch = new Proxy(nativeFetch, {
  apply(target, thisArg, args) {
    const [input, init] = args as [RequestInfo | URL, RequestInit?]
    return gatedFetch(target, input, init)
  },
})

async function gatedFetch(fetcher: typeof globalThis.fetch, input: RequestInfo | URL, init?: RequestInit) {
  const request = input instanceof Request ? input : undefined
  const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url)
  const method = init?.method ?? request?.method ?? "GET"
  const sessionID = /^\/api\/session\/([^/]+)\/(prompt|wait|interrupt)$/.exec(url.pathname)?.[1]
  if (method === "POST" && url.pathname.endsWith("/prompt") && sessionID) {
    const scenario = promptScenario(init?.body)
    if (scenario === suppressedScenario) scenarioSessions.set(sessionID, scenario)
  }
  if (method === "POST" && url.pathname.endsWith("/wait") && sessionID && scenarioSessions.get(sessionID) === suppressedScenario) {
    const signal = init?.signal ?? request?.signal
    const partial = attack === "partial-prefix" ? await sendPartialWaitPrefix(url, init?.headers, signal) : undefined
    try {
      await waitForRelease(suppressedScenario, signal)
    } finally {
      partial?.destroy()
    }
  }

  const response = await fetcher(input, init)
  if (response.status === 204 && method === "POST") {
    const providerRelease = /^\/control\/release\/(completed|failed)$/.exec(url.pathname)?.[1]
    if (providerRelease === suppressedScenario) releaseScenario(suppressedScenario)
    if (url.pathname.endsWith("/interrupt") && sessionID && scenarioSessions.get(sessionID) === "interrupted")
      releaseScenario("interrupted")
  }
  return response
}

function requireScenario(input: string | undefined): Scenario {
  if (input === "completed" || input === "failed" || input === "interrupted") return input
  throw new Error(`Wait-dispatch preloader requires a valid suppression scenario, got ${input}`)
}

function sendPartialWaitPrefix(url: URL, initHeaders?: HeadersInit, signal?: AbortSignal | null) {
  return new Promise<Socket>((resolve, reject) => {
    const socket = connect({ host: url.hostname, port: Number(url.port) })
    let finished = false
    let timer: Timer | undefined
    const cleanup = () => {
      if (timer) clearTimeout(timer)
      socket.off("connect", onConnect)
      socket.off("error", onError)
      signal?.removeEventListener("abort", onAbort)
    }
    const fail = (error: Error) => {
      if (finished) return
      finished = true
      cleanup()
      socket.destroy()
      reject(error)
    }
    const onConnect = () => {
      if (finished) return
      finished = true
      cleanup()
      const headers = new Headers(initHeaders)
      socket.on("error", () => {})
      socket.write(
        [
          `POST ${url.pathname} HTTP/1.1`,
          `Host: ${url.host}`,
          `Authorization: ${headers.get("authorization") ?? ""}`,
          `X-OpenCode-Directory: ${headers.get("x-opencode-directory") ?? ""}`,
          "X-Release-Probe-Wait-Receipt: client-fabricated",
          "Content-Length: 0",
        ].join("\r\n") + "\r\n",
      )
      resolve(socket)
    }
    const onError = (error: Error) => fail(error)
    const onAbort = () => fail(signal?.reason instanceof Error ? signal.reason : new Error("partial wait prefix aborted"))
    timer = setTimeout(() => fail(new Error(`timed out connecting partial wait prefix to ${url.host}`)), 5_000)
    socket.once("connect", onConnect)
    socket.once("error", onError)
    signal?.addEventListener("abort", onAbort, { once: true })
  })
}

function promptScenario(body: BodyInit | null | undefined): Scenario | undefined {
  if (typeof body !== "string") return undefined
  try {
    const prompt = (JSON.parse(body) as { prompt?: { text?: unknown }; resume?: unknown }).prompt?.text
    if (typeof prompt !== "string") return undefined
    const selected = (["completed", "failed", "interrupted"] as const).find((scenario) => prompt.includes(`case:${scenario}`))
    if (selected && (JSON.parse(body) as { resume?: unknown }).resume !== false) return selected
    return undefined
  } catch {
    return undefined
  }
}

function waitForRelease(scenario: Scenario, signal?: AbortSignal | null) {
  if (released.has(scenario)) return Promise.resolve()
  if (signal?.aborted) return Promise.reject(signal.reason ?? new Error("wait dispatch suppression aborted"))
  return new Promise<void>((resolve, reject) => {
    const waiters = releaseWaiters.get(scenario) ?? new Set<() => void>()
    let timer: Timer | undefined
    const cleanup = () => {
      if (timer) clearTimeout(timer)
      signal?.removeEventListener("abort", onAbort)
      waiters.delete(onRelease)
      if (waiters.size === 0) releaseWaiters.delete(scenario)
    }
    const onRelease = () => {
      cleanup()
      resolve()
    }
    const onAbort = () => {
      cleanup()
      reject(signal?.reason ?? new Error("wait dispatch suppression aborted"))
    }
    waiters.add(onRelease)
    releaseWaiters.set(scenario, waiters)
    signal?.addEventListener("abort", onAbort, { once: true })
    timer = setTimeout(() => {
      cleanup()
      reject(new Error(`wait dispatch remained suppressed beyond its bound: ${scenario}`))
    }, 30_000)
  })
}

function releaseScenario(scenario: Scenario) {
  released.add(scenario)
  for (const release of releaseWaiters.get(scenario) ?? []) release()
}
