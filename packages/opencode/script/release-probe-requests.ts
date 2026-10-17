export type ResponseText = { status: number; text: string }

export type OwnedRequest = {
  result: Promise<ResponseText>
  done: Promise<void>
  abort: () => void
}

const active = new Map<AbortController, OwnedRequest>()

export function startRequest(url: string, init: RequestInit, timeoutMs: number, label: string): OwnedRequest {
  const controller = new AbortController()
  let timer: Timer | undefined
  const result = (async () => {
    try {
      const response = await fetch(url, { ...init, signal: controller.signal })
      return { status: response.status, text: await response.text() }
    } catch (error) {
      if (controller.signal.aborted) throw new Error(`Request timed out or was aborted: ${label}`, { cause: error })
      throw error
    }
  })()
  const done = result
    .then(() => undefined, () => undefined)
    .finally(() => {
      if (timer) clearTimeout(timer)
      active.delete(controller)
    })
  const request: OwnedRequest = {
    result,
    done,
    abort: () => {
      if (timer) clearTimeout(timer)
      controller.abort(new Error(`Request aborted: ${label}`))
    },
  }
  active.set(controller, request)
  timer = setTimeout(() => controller.abort(new Error(`Request timed out after ${timeoutMs}ms: ${label}`)), timeoutMs)
  return request
}

export async function requestText(url: string, init: RequestInit, timeoutMs: number, label: string) {
  const request = startRequest(url, init, timeoutMs, label)
  try {
    return await request.result
  } finally {
    request.abort()
    await request.done
  }
}

export async function abortAllRequests() {
  const requests = [...active.values()]
  for (const request of requests) request.abort()
  await Promise.all(requests.map((request) => request.done))
}
