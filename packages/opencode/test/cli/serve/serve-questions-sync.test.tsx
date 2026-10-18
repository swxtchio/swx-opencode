/** @jsxImportSource @opentui/solid */
// An attached TUI's projection of a supervised ask (swxtchio/swx-opencode#286): when recovery closes an ask whose
// call can never take a decision, the close is published, and the TUI's own sync store drops the pending ask. The
// store is the TUI's; its events are the servers' own event streams.
import { describe, expect } from "bun:test"
import { Database } from "bun:sqlite"
import path from "node:path"
import { testRender } from "@opentui/solid"
import { onMount } from "solid-js"
import { createOpencodeClient, type GlobalEvent } from "@opencode-ai/sdk/v2"
import { ArgsProvider } from "@opencode-ai/tui/context/args"
import { ExitProvider } from "@opencode-ai/tui/context/exit"
import { KVProvider } from "@opencode-ai/tui/context/kv"
import { PermissionProvider } from "@opencode-ai/tui/context/permission"
import { ProjectProvider } from "@opencode-ai/tui/context/project"
import { TuiPathsProvider, TuiStartupProvider, TuiTerminalEnvironmentProvider } from "@opencode-ai/tui/context/runtime"
import { SDKProvider, type EventSource } from "@opencode-ai/tui/context/sdk"
import { SyncProvider, useSync } from "@opencode-ai/tui/context/sync"
import { Effect, Schedule } from "effect"
import { HttpClientRequest } from "effect/unstable/http"
import { json, writeReceiver } from "../../lib/ask-receiver"
import { cliIt, deadline, testModelID } from "../../lib/cli-process"
import { testProviderConfig } from "../../lib/test-provider"

const question = {
  question: "What should the banner say?",
  header: "Text",
  options: [{ label: "Hello", description: "A greeting" }],
}

describe("an attached TUI's projection of supervised asks", () => {
  cliIt.live(
    "drops a pending question when recovery closes it as undelivered",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "projection.db")
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions-command", receiver.path]
        yield* llm.tool("question", { questions: [question] })
        const first = yield* opencode.serve({ extraArgs, env: { OPENCODE_DB: file } })
        const tui = yield* attach(first.url, home)
        const sessionID = yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        yield* until(
          () => tui.sync.data.question[sessionID]?.some((item) => item.id === emitted.id) === true,
          "the pending question",
        )

        first.kill()
        yield* Effect.promise(() => first.exited)
        const sqlite = new Database(file)
        sqlite.run("PRAGMA foreign_keys = ON")
        sqlite.query("DELETE FROM message WHERE id = ?").run(emitted.origin.messageID)
        sqlite.close()
        // The same port, as an operator restarts it: the TUI reconnects with its projection unchanged.
        const second = yield* opencode.serve({
          extraArgs,
          env: { OPENCODE_DB: file },
          port: Number(new URL(first.url).port),
        })
        yield* tui.reconnect(second.url)
        expect(tui.sync.data.question[sessionID]?.map((item) => item.id)).toEqual([emitted.id])

        const closed = yield* json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Hello"]] }),
          ),
          home,
        )
        expect(closed.body).toMatchObject({ disposition: "settled", decision: { type: "undelivered" } })
        yield* until(() => (tui.sync.data.question[sessionID] ?? []).length === 0, "no pending question")
      }),
    120_000,
  )

  cliIt.live(
    "drops a pending permission when recovery closes a part-less approval as undelivered",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "projection-permission.db")
        const env = {
          OPENCODE_DB: file,
          OPENCODE_CONFIG_CONTENT: JSON.stringify({ ...testProviderConfig(llm.url), permission: { bash: "ask" } }),
        }
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions-command", receiver.path]
        yield* llm.tool("bash", { command: "echo workflow", description: "Stand in for a workflow tool" })
        const first = yield* opencode.serve({ extraArgs, env })
        const tui = yield* attach(first.url, home)
        const sessionID = yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        yield* until(
          () => tui.sync.data.permission[sessionID]?.some((item) => item.id === emitted.id) === true,
          "the pending permission",
        )

        first.kill()
        yield* Effect.promise(() => first.exited)
        // A GitLab Duo workflow approval's origin names its own request, since it has no tool part.
        const sqlite = new Database(file)
        sqlite.run("PRAGMA busy_timeout = 10000")
        sqlite
          .query("UPDATE session_ask SET request = json_set(request, '$.tool.callID', id) WHERE id = ?")
          .run(emitted.id)
        sqlite.close()
        const second = yield* opencode.serve({ extraArgs, env, port: Number(new URL(first.url).port) })
        yield* tui.reconnect(second.url)
        expect(tui.sync.data.permission[sessionID]?.map((item) => item.id)).toEqual([emitted.id])

        const closed = yield* json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ reply: "once" }),
          ),
          home,
        )
        expect(closed.body).toMatchObject({ disposition: "settled", decision: { type: "undelivered" } })
        yield* until(() => (tui.sync.data.permission[sessionID] ?? []).length === 0, "no pending permission")
      }),
    120_000,
  )
})

// Mounts the TUI's providers against a running server, as an attached TUI does. Its events come from the server's
// own event stream through a relay the test reconnects, as the TUI does after a restart, keeping its store.
const attach = Effect.fn("attach")(function* (url: string, directory: string) {
  let sync!: ReturnType<typeof useSync>
  let mounted!: () => void
  const ready = new Promise<void>((resolve) => {
    mounted = resolve
  })
  const relay = eventRelay()
  yield* Effect.addFinalizer(() => Effect.sync(() => relay.close()))
  function Probe() {
    const context = useSync()
    onMount(() => {
      sync = context
      mounted()
    })
    return <box />
  }
  const app = yield* Effect.promise(() =>
    testRender(() => (
      <TuiPathsProvider
        value={{ cwd: directory, home: directory, state: path.join(directory, ".tui-state"), worktree: directory }}
      >
        <TuiTerminalEnvironmentProvider value={{ platform: "linux" }}>
          <TuiStartupProvider value={{ skipInitialLoading: false }}>
            <ArgsProvider>
              <KVProvider>
                <SDKProvider url={url} directory={directory} events={relay.source}>
                  <PermissionProvider>
                    <ProjectProvider>
                      <ExitProvider exit={() => {}}>
                        <SyncProvider>
                          <Probe />
                        </SyncProvider>
                      </ExitProvider>
                    </ProjectProvider>
                  </PermissionProvider>
                </SDKProvider>
              </KVProvider>
            </ArgsProvider>
          </TuiStartupProvider>
        </TuiTerminalEnvironmentProvider>
      </TuiPathsProvider>
    )),
  )
  yield* Effect.addFinalizer(() => Effect.sync(() => app.renderer.destroy()))
  yield* Effect.promise(() => ready)
  const reconnect = Effect.fn("reconnect")(function* (target: string) {
    const before = relay.connections()
    yield* Effect.promise(() => relay.connect(target))
    yield* until(() => relay.connections() > before, `a connection to ${target}`)
  })
  yield* reconnect(url)
  yield* until(() => sync.status === "complete", "a complete sync")
  return { sync, reconnect }
})

// Relays a server's global event stream to the TUI, counting the server.connected event each connection opens with.
// A stream is never aborted (the SDK's event client rejects unobserved when it is); a superseded one is ignored, and
// each ends when its server stops.
function eventRelay() {
  let handler: ((event: GlobalEvent) => void) | undefined
  let active: object | undefined
  let connections = 0
  return {
    source: {
      subscribe: async (next) => {
        handler = next
        return () => {
          if (handler === next) handler = undefined
        }
      },
    } satisfies EventSource,
    connect: async (url: string) => {
      const current = {}
      active = current
      const events = await createOpencodeClient({ baseUrl: url }).global.event({ sseMaxRetryAttempts: 0 })
      void (async () => {
        for await (const event of events.stream) {
          if (active !== current) return
          if (event.payload.type === "server.connected") connections++
          handler?.(event)
        }
      })().catch(() => {})
    },
    connections: () => connections,
    close: () => {
      active = undefined
      handler = undefined
    },
  }
}

// Polls an observable condition of the attached TUI until it holds, bounded so a stale projection fails here.
const until = (condition: () => boolean, expected = "the expected state") =>
  Effect.sync(condition).pipe(
    Effect.filterOrFail(
      (held) => held,
      () => new Error(`the attached TUI's projection never reached ${expected}`),
    ),
    Effect.retry({ schedule: Schedule.spaced("50 millis"), times: Math.ceil(deadline(20_000) / 50) }),
  )

const startTurn = Effect.fn("startTurn")(function* (url: string, directory: string) {
  const created = yield* json(
    HttpClientRequest.post(`${url}/session`).pipe(HttpClientRequest.bodyJsonUnsafe({ title: "projection" })),
    directory,
  )
  const sessionID = created.body.id as string
  const [providerID, modelID] = testModelID.split("/")
  yield* json(
    HttpClientRequest.post(`${url}/session/${sessionID}/prompt_async`).pipe(
      HttpClientRequest.bodyJsonUnsafe({ model: { providerID, modelID }, parts: [{ type: "text", text: "ask me" }] }),
    ),
    directory,
  )
  return sessionID
})
