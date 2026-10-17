// Test helpers for `--questions emit` (swxtchio/swx-opencode#286): the operator's receiver command and the JSON
// requests a supervisor sends back.
import { chmod } from "node:fs/promises"
import path from "node:path"
import { Duration, Effect, Schedule } from "effect"
import { HttpClient, HttpClientRequest } from "effect/unstable/http"
import { deadline } from "./cli-process"

// The operator's receiver: a script run without arguments that appends each ask to a file.
export const writeReceiver = Effect.fn("writeReceiver")(function* (home: string, body: string) {
  const out = path.join(home, `asks-${crypto.randomUUID()}.jsonl`)
  const file = path.join(home, `receiver-${crypto.randomUUID()}.sh`)
  yield* Effect.promise(async () => {
    await Bun.write(file, `#!/bin/sh\n${body.replaceAll("$OUT", out)}\n`)
    await chmod(file, 0o755)
  })
  return {
    path: file,
    // Waits until the receiver has been handed `count` asks.
    read: (count: number) =>
      Effect.promise(async () => {
        const text = await Bun.file(out)
          .text()
          .catch(() => "")
        return (
          text
            .split("\n")
            .filter(Boolean)
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            .map((line) => JSON.parse(line) as any)
        )
      }).pipe(
        Effect.filterOrFail(
          (asks) => asks.length >= count,
          () => new Error(`receiver has not received ${count} asks`),
        ),
        Effect.retry(Schedule.spaced("50 millis")),
        Effect.timeout(Duration.millis(deadline(20_000))),
      ),
  }
})

export const json = Effect.fn("json")(function* (request: HttpClientRequest.HttpClientRequest, directory: string) {
  const client = yield* HttpClient.HttpClient
  const response = yield* client.execute(request.pipe(HttpClientRequest.setUrlParam("directory", directory)))
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return { status: response.status, body: (yield* response.json) as any }
})

// Subscribes to a server's instance event stream, so a test can wait for an event an answer publishes.
export const watchEvents = Effect.fn("watchEvents")(function* (url: string, directory: string) {
  const controller = new AbortController()
  yield* Effect.addFinalizer(() => Effect.sync(() => controller.abort()))
  const response = yield* Effect.promise(() =>
    fetch(`${url}/event?directory=${encodeURIComponent(directory)}`, { signal: controller.signal }),
  )
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const seen: any[] = []
  const reader = response.body!.pipeThrough(new TextDecoderStream()).getReader()
  void (async () => {
    let buffer = ""
    for (;;) {
      const next = await reader.read().catch(() => ({ done: true as const, value: undefined }))
      if (next.done) return
      buffer += next.value
      const lines = buffer.split("\n")
      buffer = lines.pop() ?? ""
      for (const line of lines) if (line.startsWith("data:")) seen.push(JSON.parse(line.slice(5)))
    }
  })()
  return {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    until: (predicate: (event: any) => boolean) =>
      Effect.sync(() => seen.find(predicate)).pipe(
        Effect.filterOrFail(
          (event) => event !== undefined,
          () => new Error("event not published"),
        ),
        Effect.retry(Schedule.spaced("50 millis")),
        Effect.timeout(Duration.millis(deadline(20_000))),
      ),
  }
})
