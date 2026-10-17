const startFile = requireMarker(Bun.env.RELEASE_PROBE_WAIT_START_FILE)
const releaseFile = requireMarker(Bun.env.RELEASE_PROBE_WAIT_RELEASE_FILE)
const nativeFetch = globalThis.fetch

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
  if (method === "POST" && url.pathname.startsWith("/api/session/") && url.pathname.endsWith("/wait")) {
    if ((await Bun.file(startFile).exists()) && !(await Bun.file(releaseFile).exists()))
      await awaitRelease(releaseFile, init?.signal ?? request?.signal)
  }
  return fetcher(input, init)
}

function requireMarker(input: string | undefined) {
  if (!input) throw new Error("Wait-dispatch preloader requires its start and release markers")
  return input
}

async function awaitRelease(releaseFile: string, signal?: AbortSignal | null) {
  const deadline = Date.now() + 20_000
  while (!(await Bun.file(releaseFile).exists())) {
    if (signal?.aborted) throw signal.reason ?? new Error("wait dispatch was aborted before producer receipt")
    if (Date.now() >= deadline) throw new Error("wait-dispatch suppression exceeded its bound")
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}
