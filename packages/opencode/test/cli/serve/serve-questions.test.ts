// Subprocess tests for `opencode serve --questions emit` (swxtchio/swx-opencode#286): a detached server with no
// human pane emits each ask to the operator's command, keeps it pending, and settles it from the answer route,
// across a server restart on the same database.
import { describe, expect } from "bun:test"
import { constants } from "node:fs"
import { open } from "node:fs/promises"
import { Database } from "bun:sqlite"
import path from "node:path"
import { Duration, Effect, Schedule } from "effect"
import { HttpClient, HttpClientRequest } from "effect/unstable/http"
import { json, watchEvents, writeReceiver } from "../../lib/ask-receiver"
import { cliIt, deadline, testModelID } from "../../lib/cli-process"
import { testProviderConfig } from "../../lib/test-provider"

const questions = [
  {
    question: "Which colors should the banner use?",
    header: "Colors",
    multiple: true,
    options: [
      { label: "Red", description: "Warm" },
      { label: "Blue", description: "Cool" },
      { label: "Green", description: "Calm" },
    ],
  },
  {
    question: "What should the banner say?",
    header: "Text",
    options: [{ label: "Hello", description: "A greeting" }],
  },
]

describe("opencode serve --questions emit (subprocess)", () => {
  cliIt.live(
    "emits a question, keeps it pending, and an answer from the emitted route continues the turn",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const server = yield* opencode.serve({
          extraArgs: ["--questions", "emit", "--questions-command", receiver.path],
        })
        yield* llm.tool("question", { questions })
        yield* llm.text("banner done")

        const sessionID = yield* startTurn(server.url, home)
        const [emitted] = yield* receiver.read(1)

        expect(emitted.type).toBe("opencode.ask")
        expect(emitted.kind).toBe("question")
        expect(emitted.sessionID).toBe(sessionID)
        expect(emitted.server).toBe(server.url)
        expect(emitted.directory).toBe(home)
        expect(emitted.origin.tool).toBe("question")
        expect(emitted.origin.callID).toBeString()
        expect(emitted.question.questions).toMatchObject(questions)
        expect(emitted.answer).toEqual({
          method: "POST",
          path: `/question/${emitted.id}/answer`,
          query: { directory: home },
        })

        // Emission is not an answer: the request is listed and the provider has not been asked to continue.
        const listed = yield* json(HttpClientRequest.get(`${server.url}/question`), home)
        expect(listed.body.map((item: { id: string }) => item.id)).toEqual([emitted.id])
        expect(yield* llm.calls).toBe(1)
        const events = yield* watchEvents(server.url, home)

        // The receiver answers from the payload alone: two selections, then custom text.
        const answers = [
          [emitted.question.questions[0].options[0].label, emitted.question.questions[0].options[1].label],
          ["Welcome aboard"],
        ]
        const settled = yield* json(
          HttpClientRequest.post(`${emitted.server}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers }),
          ),
          emitted.answer.query.directory,
        )
        expect(settled.status).toBe(200)
        expect(settled.body).toEqual({
          requestID: emitted.id,
          sessionID,
          disposition: "settled",
          decision: { type: "answered", answers },
          recovered: false,
        })

        // An attached human's prompt is cleared by the same event a human reply publishes.
        const replied = yield* events.until(
          (event) => event.type === "question.replied" && event.properties.requestID === emitted.id,
        )
        expect(replied.properties).toMatchObject({ sessionID, answers })
        yield* llm.wait(2).pipe(Effect.timeout(Duration.millis(deadline(20_000))))
        const part = yield* toolPart(server.url, home, sessionID, emitted.origin.callID, "completed")
        expect(part.state.metadata.answers).toEqual(answers)
        expect(JSON.stringify((yield* llm.inputs)[1])).toContain(
          `\\"Which colors should the banner use?\\"=\\"Red, Blue\\", \\"What should the banner say?\\"=\\"Welcome aboard\\"`,
        )

        // A second resolver changes nothing and is told the winner's decision.
        const duplicate = yield* json(
          HttpClientRequest.post(`${server.url}/question/${emitted.id}/answer`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ reject: true }),
          ),
          home,
        )
        expect(duplicate.body).toMatchObject({ disposition: "duplicate", decision: { type: "answered", answers } })
        expect(yield* llm.calls).toBe(2)

        const unknown = yield* json(
          HttpClientRequest.post(`${server.url}/question/que_unknown/answer`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Red"]] }),
          ),
          home,
        )
        expect(unknown.status).toBe(404)
        expect(unknown.body).toMatchObject({ _tag: "QuestionNotFoundError", requestID: "que_unknown" })
      }),
    90_000,
  )

  cliIt.live(
    "a failed transport leaves the ask pending and its answer still completes the call",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'\nexit 3")
        const server = yield* opencode.serve({
          extraArgs: ["--questions", "emit", "--questions-command", receiver.path],
        })
        yield* llm.tool("question", { questions: [questions[1]] })
        yield* llm.text("done")

        const sessionID = yield* startTurn(server.url, home)
        const [emitted] = yield* receiver.read(1)
        const listed = yield* json(HttpClientRequest.get(`${server.url}/question`), home)
        expect(listed.body.map((item: { id: string }) => item.id)).toEqual([emitted.id])

        const settled = yield* json(
          HttpClientRequest.post(`${server.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Hello"]] }),
          ),
          home,
        )
        expect(settled.body.disposition).toBe("settled")
        yield* toolPart(server.url, home, sessionID, emitted.origin.callID, "completed")
        yield* llm.wait(2).pipe(Effect.timeout(Duration.millis(deadline(20_000))))
      }),
    90_000,
  )

  cliIt.live(
    "an answer completes the call while the receiver is still holding its delivery",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const fifo = path.join(home, "release")
        expect(Bun.spawnSync(["mkfifo", fifo]).exitCode).toBe(0)
        // Released at test end, so the held receiver never outlives the test. Non-blocking: with no reader left the
        // open fails instead of waiting.
        yield* Effect.addFinalizer(() =>
          Effect.tryPromise(async () => {
            const handle = await open(fifo, constants.O_WRONLY | constants.O_NONBLOCK)
            await handle.write("go\n")
            await handle.close()
          }).pipe(Effect.ignore),
        )
        const receiver = yield* writeReceiver(home, `cat >> '$OUT'\nread line < '${fifo}'`)
        const server = yield* opencode.serve({
          extraArgs: ["--questions", "emit", "--questions-command", receiver.path],
        })
        yield* llm.tool("question", { questions: [questions[1]] })
        yield* llm.text("done")

        const sessionID = yield* startTurn(server.url, home)
        const [emitted] = yield* receiver.read(1)
        const settled = yield* json(
          HttpClientRequest.post(`${server.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Hello"]] }),
          ),
          home,
        )
        expect(settled.body.disposition).toBe("settled")
        yield* toolPart(server.url, home, sessionID, emitted.origin.callID, "completed")
        yield* llm.wait(2).pipe(Effect.timeout(Duration.millis(deadline(20_000))))
      }),
    90_000,
  )

  cliIt.live(
    "an answer after a server restart settles the same ask, completes its call and continues the Session",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const env = { OPENCODE_DB: path.join(home, "restart.db") }
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions", "emit", "--questions-command", receiver.path]
        yield* llm.tool("question", { questions: [questions[1]] })
        yield* llm.text("continued after restart")

        const first = yield* opencode.serve({ extraArgs, env })
        const sessionID = yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        const pending = yield* json(HttpClientRequest.get(`${first.url}/question`), home)
        expect(pending.body.map((item: { id: string }) => item.id)).toEqual([emitted.id])
        first.kill()
        yield* Effect.promise(() => first.exited)

        const second = yield* opencode.serve({ extraArgs, env })
        const listed = yield* json(HttpClientRequest.get(`${second.url}/question`), home)
        expect(listed.body.map((item: { id: string }) => item.id)).toEqual([emitted.id])
        const events = yield* watchEvents(second.url, home)

        const settled = yield* json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Welcome back"]] }),
          ),
          home,
        )
        expect(settled.body).toEqual({
          requestID: emitted.id,
          sessionID,
          disposition: "settled",
          decision: { type: "answered", answers: [["Welcome back"]] },
          recovered: true,
        })
        const part = yield* toolPart(second.url, home, sessionID, emitted.origin.callID, "completed")
        expect(part.state.output).toContain(`"What should the banner say?"="Welcome back"`)
        yield* events.until((event) => event.type === "question.replied" && event.properties.requestID === emitted.id)
        yield* llm.wait(2).pipe(Effect.timeout(Duration.millis(deadline(20_000))))
        expect(JSON.stringify((yield* llm.inputs)[1])).toContain("Welcome back")
        yield* assistantText(second.url, home, sessionID, "continued after restart")
        const after = yield* json(HttpClientRequest.get(`${second.url}/question`), home)
        expect(after.body).toEqual([])
        second.kill()
        yield* Effect.promise(() => second.exited)

        // Settled state survives a restart too: a repeated answer has no new effect.
        const third = yield* opencode.serve({ extraArgs, env })
        const duplicate = yield* json(
          HttpClientRequest.post(`${third.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ reject: true }),
          ),
          home,
        )
        expect(duplicate.body).toMatchObject({
          disposition: "duplicate",
          decision: { type: "answered", answers: [["Welcome back"]] },
          recovered: true,
        })
        const final = yield* toolPart(third.url, home, sessionID, emitted.origin.callID, "completed")
        expect(final.state.output).toBe(part.state.output)
        expect(yield* llm.calls).toBe(2)
      }),
    120_000,
  )

  cliIt.live(
    "an answer that reaches another live server on the same database is refused and settles nothing",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const env = { OPENCODE_DB: path.join(home, "shared.db") }
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions", "emit", "--questions-command", receiver.path]
        yield* llm.tool("question", { questions: [questions[1]] })
        yield* llm.text("done")
        const owner = yield* opencode.serve({ extraArgs, env })
        const other = yield* opencode.serve({ extraArgs, env })
        const sessionID = yield* startTurn(owner.url, home)
        const [emitted] = yield* receiver.read(1)

        const refused = yield* json(
          HttpClientRequest.post(`${other.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Hello"]] }),
          ),
          home,
        )
        expect(refused.status).toBe(503)
        expect(refused.body).toMatchObject({
          _tag: "AskUnavailableError",
          requestID: emitted.id,
          reason: "owned-by-live-process",
        })

        const settled = yield* json(
          HttpClientRequest.post(`${owner.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Hello"]] }),
          ),
          home,
        )
        expect(settled.body).toMatchObject({ disposition: "settled", recovered: false })
        yield* toolPart(owner.url, home, sessionID, emitted.origin.callID, "completed")
      }),
    90_000,
  )

  cliIt.live(
    "an answer whose ledger cannot be read is an explicit failure, and the ask stays answerable",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "store.db")
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        yield* llm.tool("question", { questions: [questions[1]] })
        yield* llm.text("done")
        const server = yield* opencode.serve({
          extraArgs: ["--questions", "emit", "--questions-command", receiver.path],
          env: { OPENCODE_DB: file },
        })
        const sessionID = yield* startTurn(server.url, home)
        const [emitted] = yield* receiver.read(1)
        const answer = json(
          HttpClientRequest.post(`${server.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Hello"]] }),
          ),
          home,
        )

        const sqlite = new Database(file)
        // The server may hold the write lock for its own Session writes; wait for it rather than fail.
        sqlite.run("PRAGMA busy_timeout = 10000")
        sqlite.run("ALTER TABLE session_ask RENAME TO session_ask_unavailable")
        const failed = yield* answer
        sqlite.run("ALTER TABLE session_ask_unavailable RENAME TO session_ask")
        sqlite.close()
        expect(failed.status).toBe(503)
        expect(failed.body).toMatchObject({
          _tag: "AskUnavailableError",
          requestID: emitted.id,
          reason: "store-unavailable",
        })

        expect((yield* answer).body).toMatchObject({ disposition: "settled" })
        yield* toolPart(server.url, home, sessionID, emitted.origin.callID, "completed")
      }),
    90_000,
  )

  cliIt.live(
    "a permission granted after a restart reports the call as not run and lets the model retry it",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const marker = path.join(home, "granted-marker")
        const env = {
          OPENCODE_DB: path.join(home, "permission.db"),
          OPENCODE_CONFIG_CONTENT: JSON.stringify({ ...testProviderConfig(llm.url), permission: { bash: "ask" } }),
        }
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions", "emit", "--questions-command", receiver.path]
        yield* llm.tool("bash", { command: `touch ${marker}`, description: "Create a marker" })
        yield* llm.text("will retry")

        const first = yield* opencode.serve({ extraArgs, env })
        const sessionID = yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        expect(emitted.kind).toBe("permission")
        first.kill()
        yield* Effect.promise(() => first.exited)

        const second = yield* opencode.serve({ extraArgs, env })
        const settled = yield* json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ reply: "once" }),
          ),
          home,
        )
        expect(settled.body).toMatchObject({
          disposition: "settled",
          decision: { type: "replied", reply: "once" },
          recovered: true,
        })
        const part = yield* toolPart(second.url, home, sessionID, emitted.origin.callID, "error")
        expect(part.state.error).toContain("did not run")
        yield* llm.wait(2).pipe(Effect.timeout(Duration.millis(deadline(20_000))))
        expect(JSON.stringify((yield* llm.inputs)[1])).toContain("Call the tool again to perform it")
        expect(yield* Effect.promise(() => Bun.file(marker).exists())).toBe(false)
      }),
    120_000,
  )

  cliIt.live(
    "a plan accepted after a restart switches to the build agent once",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const env = { OPENCODE_DB: path.join(home, "plan.db"), OPENCODE_EXPERIMENTAL_PLAN_MODE: "1" }
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions", "emit", "--questions-command", receiver.path]
        yield* llm.tool("plan_exit", {})
        yield* llm.text("building")

        const first = yield* opencode.serve({ extraArgs, env })
        const sessionID = yield* startTurn(first.url, home, "plan")
        const [emitted] = yield* receiver.read(1)
        expect(emitted.origin.tool).toBe("plan_exit")
        first.kill()
        yield* Effect.promise(() => first.exited)

        const second = yield* opencode.serve({ extraArgs, env })
        const answer = json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Yes"]] }),
          ),
          home,
        )
        expect((yield* answer).body).toMatchObject({ disposition: "settled", recovered: true })
        const part = yield* toolPart(second.url, home, sessionID, emitted.origin.callID, "completed")
        expect(part.state.title).toBe("Switching to build agent")
        yield* llm.wait(2).pipe(Effect.timeout(Duration.millis(deadline(20_000))))
        expect(JSON.stringify((yield* llm.inputs)[1])).toContain("has been approved, you can now edit files")
        yield* assistantText(second.url, home, sessionID, "building")

        expect((yield* answer).body).toMatchObject({ disposition: "duplicate" })
        const builds = (yield* messages(second.url, home, sessionID)).filter(
          (message) => message.info.role === "user" && message.info.agent === "build",
        )
        expect(builds).toHaveLength(1)
        expect(yield* llm.calls).toBe(2)
      }),
    120_000,
  )

  cliIt.live(
    "a store upgraded from before the ledger records, lists and settles emitted asks",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "upgraded.db")
        const env = { OPENCODE_DB: file }
        const old = yield* opencode.serve({ env })
        old.kill()
        yield* Effect.promise(() => old.exited)
        const sqlite = new Database(file)
        sqlite.run("DROP TABLE session_ask")
        sqlite.run("DELETE FROM migration WHERE id = '20261017192742_session_ask'")
        sqlite.close()

        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        yield* llm.tool("question", { questions: [questions[1]] })
        yield* llm.text("done")
        const server = yield* opencode.serve({
          extraArgs: ["--questions", "emit", "--questions-command", receiver.path],
          env,
        })
        const sessionID = yield* startTurn(server.url, home)
        const [emitted] = yield* receiver.read(1)
        const listed = yield* json(HttpClientRequest.get(`${server.url}/question`), home)
        expect(listed.body.map((item: { id: string }) => item.id)).toEqual([emitted.id])
        const settled = yield* json(
          HttpClientRequest.post(`${server.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Hello"]] }),
          ),
          home,
        )
        expect(settled.body).toMatchObject({ disposition: "settled", sessionID })
        yield* toolPart(server.url, home, sessionID, emitted.origin.callID, "completed")
      }),
    90_000,
  )

  cliIt.live(
    "an answer after the restarted Session went on without it is recorded but leaves the old call alone",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const env = { OPENCODE_DB: path.join(home, "moved-on.db") }
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions", "emit", "--questions-command", receiver.path]
        yield* llm.tool("question", { questions: [questions[1]] })
        yield* llm.text("went on without the answer")

        const first = yield* opencode.serve({ extraArgs, env })
        const sessionID = yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        first.kill()
        yield* Effect.promise(() => first.exited)

        const second = yield* opencode.serve({ extraArgs, env })
        const [providerID, modelID] = testModelID.split("/")
        yield* json(
          HttpClientRequest.post(`${second.url}/session/${sessionID}/prompt_async`).pipe(
            HttpClientRequest.bodyJsonUnsafe({
              model: { providerID, modelID },
              parts: [{ type: "text", text: "go on" }],
            }),
          ),
          home,
        )
        yield* assistantText(second.url, home, sessionID, "went on without the answer")

        const settled = yield* json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Hello"]] }),
          ),
          home,
        )
        expect(settled.body).toMatchObject({ disposition: "settled", recovered: true })
        const part = (yield* messages(second.url, home, sessionID))
          .flatMap((message) => message.parts)
          .find((item) => item.callID === emitted.origin.callID)
        expect(part.state.status).not.toBe("completed")
        expect(yield* llm.calls).toBe(2)
      }),
    120_000,
  )
})

const startTurn = Effect.fn("startTurn")(function* (url: string, directory: string, agent?: string) {
  const created = yield* json(
    HttpClientRequest.post(`${url}/session`).pipe(HttpClientRequest.bodyJsonUnsafe({ title: "supervised" })),
    directory,
  )
  const sessionID = created.body.id as string
  const [providerID, modelID] = testModelID.split("/")
  const sent = yield* json(
    HttpClientRequest.post(`${url}/session/${sessionID}/prompt_async`).pipe(
      HttpClientRequest.bodyJsonUnsafe({
        model: { providerID, modelID },
        ...(agent ? { agent } : {}),
        parts: [{ type: "text", text: "ask me" }],
      }),
    ),
    directory,
  )
  expect(sent.status).toBeLessThan(300)
  return sessionID
})

const messages = (url: string, directory: string, sessionID: string) =>
  json(HttpClientRequest.get(`${url}/session/${sessionID}/message`), directory).pipe(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Effect.map((result) => result.body as Array<{ info: any; parts: any[] }>),
  )

// Waits for the call's stored part to reach `status`.
const toolPart = (url: string, directory: string, sessionID: string, callID: string, status: string) =>
  messages(url, directory, sessionID).pipe(
    Effect.map((list) => list.flatMap((message) => message.parts).find((part) => part.callID === callID)),
    Effect.filterOrFail(
      (part) => part?.state?.status === status,
      () => new Error(`tool call ${callID} is not ${status}`),
    ),
    Effect.retry(Schedule.spaced("50 millis")),
    Effect.timeout(Duration.millis(deadline(20_000))),
  )

const assistantText = (url: string, directory: string, sessionID: string, text: string) =>
  messages(url, directory, sessionID).pipe(
    Effect.filterOrFail(
      (list) => list.some((message) => message.parts.some((part) => part.type === "text" && part.text === text)),
      () => new Error(`no assistant text ${text}`),
    ),
    Effect.retry(Schedule.spaced("50 millis")),
    Effect.timeout(Duration.millis(deadline(20_000))),
  )
