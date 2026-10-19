// Long-running session and event-stream work can remain idle while still being valid; leave its
// lifetime to caller signals and domain deadlines while preserving every other request option.
export const noTimeoutFetch = ((request: RequestInfo | URL, init?: RequestInit) =>
  fetch(request, { ...init, timeout: false } as BunFetchRequestInit)) as typeof fetch
