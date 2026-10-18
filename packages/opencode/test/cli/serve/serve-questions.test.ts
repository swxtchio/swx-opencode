// Subprocess tests for `opencode serve --questions-command` (swxtchio/swx-opencode#286): a detached server with no
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
import { raw } from "../../lib/llm-server"
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

describe("opencode serve --questions-command (subprocess)", () => {
  cliIt.live(
    "emits a question, keeps it pending, and an answer from the emitted route continues the turn",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const server = yield* opencode.serve({
          extraArgs: ["--questions-command", receiver.path],
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
          extraArgs: ["--questions-command", receiver.path],
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
          extraArgs: ["--questions-command", receiver.path],
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
        const extraArgs = ["--questions-command", receiver.path]
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
        const extraArgs = ["--questions-command", receiver.path]
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
          extraArgs: ["--questions-command", receiver.path],
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
        const extraArgs = ["--questions-command", receiver.path]
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
        const extraArgs = ["--questions-command", receiver.path]
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
        sqlite.run("DELETE FROM migration WHERE id = '20261018005127_session_ask'")
        sqlite.close()

        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        yield* llm.tool("question", { questions: [questions[1]] })
        yield* llm.text("done")
        const server = yield* opencode.serve({
          extraArgs: ["--questions-command", receiver.path],
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
    "an answer after the restarted Session went on without it closes the ask as undelivered and leaves the old call alone",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const env = { OPENCODE_DB: path.join(home, "moved-on.db") }
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions-command", receiver.path]
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
        expect(settled.body).toEqual({
          requestID: emitted.id,
          sessionID,
          disposition: "settled",
          decision: { type: "undelivered" },
          recovered: false,
        })
        const pending = yield* json(HttpClientRequest.get(`${second.url}/question`), home)
        expect(pending.body).toEqual([])
        const part = (yield* messages(second.url, home, sessionID))
          .flatMap((message) => message.parts)
          .find((item) => item.callID === emitted.origin.callID)
        expect(part.state.status).not.toBe("completed")
        expect(yield* llm.calls).toBe(2)
      }),
    120_000,
  )

  cliIt.live(
    "an answer after a restart whose call cannot be written settles nothing, and a retry applies it",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "atomic.db")
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions-command", receiver.path]
        yield* llm.tool("question", { questions: [questions[1]] })
        yield* llm.text("continued once applied")
        const first = yield* opencode.serve({ extraArgs, env: { OPENCODE_DB: file } })
        const sessionID = yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        first.kill()
        yield* Effect.promise(() => first.exited)

        const second = yield* opencode.serve({ extraArgs, env: { OPENCODE_DB: file } })
        const answer = json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Applied"]] }),
          ),
          home,
        )
        const sqlite = new Database(file)
        sqlite.run("PRAGMA busy_timeout = 10000")
        sqlite.run("CREATE TRIGGER hold_part BEFORE UPDATE ON part BEGIN SELECT RAISE(ABORT, 'part held'); END")
        const failed = yield* answer
        sqlite.run("DROP TRIGGER hold_part")
        sqlite.close()
        expect(failed.status).toBe(503)
        expect(failed.body).toMatchObject({ requestID: emitted.id, reason: "store-unavailable" })
        const listed = yield* json(HttpClientRequest.get(`${second.url}/question`), home)
        expect(listed.body.map((item: { id: string }) => item.id)).toEqual([emitted.id])

        expect((yield* answer).body).toMatchObject({ disposition: "settled", recovered: true })
        const part = yield* toolPart(second.url, home, sessionID, emitted.origin.callID, "completed")
        expect(part.state.output).toContain(`"What should the banner say?"="Applied"`)
        yield* assistantText(second.url, home, sessionID, "continued once applied")
      }),
    120_000,
  )

  cliIt.live(
    "a permission granted live whose call ran is never rewritten after the restart",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const marker = path.join(home, "ran-marker")
        const env = {
          OPENCODE_DB: path.join(home, "executed.db"),
          OPENCODE_CONFIG_CONTENT: JSON.stringify({ ...testProviderConfig(llm.url), permission: { bash: "ask" } }),
        }
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions-command", receiver.path]
        yield* llm.tool("bash", { command: `touch ${marker} && sleep 30`, description: "Run a long command" })
        const first = yield* opencode.serve({ extraArgs, env })
        const sessionID = yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        const granted = yield* json(
          HttpClientRequest.post(`${first.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ reply: "once" }),
          ),
          home,
        )
        expect(granted.body).toMatchObject({ disposition: "settled", recovered: false })
        // The granted call is running: its marker exists, and it is still sleeping when its server dies.
        yield* Effect.promise(() => Bun.file(marker).exists()).pipe(
          Effect.filterOrFail(
            (exists) => exists,
            () => new Error("the granted call never ran"),
          ),
          Effect.retry(Schedule.spaced("50 millis")),
          Effect.timeout(Duration.millis(deadline(20_000))),
        )
        first.kill()
        yield* Effect.promise(() => first.exited)

        const second = yield* opencode.serve({ extraArgs, env })
        const repeated = yield* json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ reply: "once" }),
          ),
          home,
        )
        expect(repeated.body).toEqual({
          requestID: emitted.id,
          sessionID,
          disposition: "duplicate",
          decision: { type: "replied", reply: "once" },
          recovered: false,
        })
        const part = (yield* messages(second.url, home, sessionID))
          .flatMap((message) => message.parts)
          .find((item) => item.callID === emitted.origin.callID)
        expect(part.state.status).toBe("running")
        expect(yield* llm.calls).toBe(1)
      }),
    120_000,
  )

  cliIt.live(
    "an answer after a restart whose call's part is missing is refused as not applied, and the ask stays pending",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "gone.db")
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions-command", receiver.path]
        yield* llm.tool("question", { questions: [questions[1]] })
        const first = yield* opencode.serve({ extraArgs, env: { OPENCODE_DB: file } })
        const sessionID = yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        first.kill()
        yield* Effect.promise(() => first.exited)
        const sqlite = new Database(file)
        sqlite
          .query("DELETE FROM part WHERE session_id = ? AND json_extract(data, '$.callID') = ?")
          .run(sessionID, emitted.origin.callID)
        sqlite.close()

        const second = yield* opencode.serve({ extraArgs, env: { OPENCODE_DB: file } })
        const settled = yield* json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Hello"]] }),
          ),
          home,
        )
        expect(settled.status).toBe(409)
        expect(settled.body).toMatchObject({ _tag: "AskNotAppliedError", requestID: emitted.id, reason: "call-gone" })
        // The answer was not consumed: the ask is still pending, and another answer is refused the same way.
        const listed = yield* json(HttpClientRequest.get(`${second.url}/question`), home)
        expect(listed.body.map((item: { id: string }) => item.id)).toEqual([emitted.id])
        const repeated = yield* json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ reject: true }),
          ),
          home,
        )
        expect(repeated.status).toBe(409)
        expect(repeated.body).toMatchObject({ reason: "call-gone" })
        expect(yield* llm.calls).toBe(1)
      }),
    120_000,
  )

  cliIt.live(
    "an ask whose Session is aborted is cancelled and takes no later answer",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        yield* llm.tool("question", { questions: [questions[1]] })
        const server = yield* opencode.serve({ extraArgs: ["--questions-command", receiver.path] })
        const sessionID = yield* startTurn(server.url, home)
        const [emitted] = yield* receiver.read(1)
        yield* json(HttpClientRequest.post(`${server.url}/session/${sessionID}/abort`), home)
        yield* json(HttpClientRequest.get(`${server.url}/question`), home).pipe(
          Effect.filterOrFail(
            (listed) => listed.body.length === 0,
            () => new Error("the aborted ask is still outstanding"),
          ),
          Effect.retry(Schedule.spaced("50 millis")),
          Effect.timeout(Duration.millis(deadline(20_000))),
        )
        const late = yield* json(
          HttpClientRequest.post(`${server.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Too late"]] }),
          ),
          home,
        )
        expect(late.body).toMatchObject({ disposition: "duplicate", decision: { type: "cancelled" } })
      }),
    90_000,
  )

  cliIt.live(
    "a GitLab Duo workflow approval answered after its server died closes as undelivered and stops blocking prune",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "workflow.db")
        const env = {
          OPENCODE_DB: file,
          OPENCODE_CONFIG_CONTENT: JSON.stringify({ ...testProviderConfig(llm.url), permission: { bash: "ask" } }),
        }
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions-command", receiver.path]
        yield* llm.tool("bash", { command: "echo workflow", description: "Stand in for a workflow tool" })
        const first = yield* opencode.serve({ extraArgs, env })
        const sessionID = yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        first.kill()
        yield* Effect.promise(() => first.exited)
        // A workflow approval's origin names its own request, since it has no tool part.
        const sqlite = new Database(file)
        sqlite.run("PRAGMA busy_timeout = 10000")
        sqlite
          .query(
            "UPDATE session_ask SET request = json_set(request, '$.permission', 'workflow_tool_approval', '$.tool.callID', id) WHERE id = ?",
          )
          .run(emitted.id)
        const outstanding = () =>
          sqlite
            .query<
              { count: number },
              [string]
            >("SELECT count(*) AS count FROM session_ask WHERE session_id = ? AND time_settled IS NULL")
            .get(sessionID)!.count
        expect(outstanding()).toBe(1)

        const second = yield* opencode.serve({ extraArgs, env })
        const answer = json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ reply: "once" }),
          ),
          home,
        )
        const closed = yield* answer
        expect(closed.body).toEqual({
          requestID: emitted.id,
          sessionID,
          disposition: "settled",
          decision: { type: "undelivered" },
          recovered: false,
        })
        expect(outstanding()).toBe(0)
        sqlite.close()
        const listed = yield* json(HttpClientRequest.get(`${second.url}/permission`), home)
        expect(listed.body).toEqual([])
        expect((yield* answer).body).toMatchObject({ disposition: "duplicate", decision: { type: "undelivered" } })
      }),
    120_000,
  )

  cliIt.live(
    "an answer after a restart whose asking message was deleted closes the ask as undelivered",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "deleted.db")
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions-command", receiver.path]
        yield* llm.tool("question", { questions: [questions[1]] })
        const first = yield* opencode.serve({ extraArgs, env: { OPENCODE_DB: file } })
        yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        first.kill()
        yield* Effect.promise(() => first.exited)
        const sqlite = new Database(file)
        sqlite.run("PRAGMA foreign_keys = ON")
        sqlite.query("DELETE FROM message WHERE id = ?").run(emitted.origin.messageID)
        sqlite.close()

        const second = yield* opencode.serve({ extraArgs, env: { OPENCODE_DB: file } })
        const closed = yield* json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Hello"]] }),
          ),
          home,
        )
        expect(closed.body).toMatchObject({
          disposition: "settled",
          decision: { type: "undelivered" },
          recovered: false,
        })
        const listed = yield* json(HttpClientRequest.get(`${second.url}/question`), home)
        expect(listed.body).toEqual([])
      }),
    120_000,
  )

  cliIt.live(
    "an ask aborted while its cancellation cannot be recorded never takes an answer, even after a restart",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "aborted.db")
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions-command", receiver.path]
        yield* llm.tool("question", { questions: [questions[1]] })
        const first = yield* opencode.serve({ extraArgs, env: { OPENCODE_DB: file } })
        const sessionID = yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        const sqlite = new Database(file)
        sqlite.run("PRAGMA busy_timeout = 10000")
        sqlite.run("CREATE TRIGGER hold_ask BEFORE UPDATE ON session_ask BEGIN SELECT RAISE(ABORT, 'ask held'); END")
        yield* json(HttpClientRequest.post(`${first.url}/session/${sessionID}/abort`), home)
        yield* toolPart(first.url, home, sessionID, emitted.origin.callID, "error")
        const refused = yield* json(
          HttpClientRequest.post(`${first.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Too late"]] }),
          ),
          home,
        )
        expect(refused.status).toBe(503)
        const listed = yield* json(HttpClientRequest.get(`${first.url}/question`), home)
        expect(listed.body).toEqual([])
        expect(sqlite.query("SELECT time_settled FROM session_ask WHERE id = ?").get(emitted.id)).toEqual({
          time_settled: null,
        })
        first.kill()
        yield* Effect.promise(() => first.exited)
        sqlite.run("DROP TRIGGER hold_ask")
        sqlite.close()

        const second = yield* opencode.serve({ extraArgs, env: { OPENCODE_DB: file } })
        const late = yield* json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Too late"]] }),
          ),
          home,
        )
        expect(late.body).toMatchObject({ disposition: "duplicate", decision: { type: "cancelled" }, recovered: false })
        const part = yield* toolPart(second.url, home, sessionID, emitted.origin.callID, "error")
        expect(part.state.error).not.toContain("Too late")
        expect(yield* llm.calls).toBe(1)
      }),
    120_000,
  )

  cliIt.live(
    "a plan accepted live whose switch fails settles nothing, and a retried Yes switches once",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "plan-live.db")
        const env = { OPENCODE_DB: file, OPENCODE_EXPERIMENTAL_PLAN_MODE: "1" }
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        yield* llm.tool("plan_exit", {})
        yield* llm.text("building")
        const server = yield* opencode.serve({ extraArgs: ["--questions-command", receiver.path], env })
        const sessionID = yield* startTurn(server.url, home, "plan")
        const [emitted] = yield* receiver.read(1)
        const accept = json(
          HttpClientRequest.post(`${server.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Yes"]] }),
          ),
          home,
        )
        const builds = messages(server.url, home, sessionID).pipe(
          Effect.map((list) =>
            list.filter((message) => message.info.role === "user" && message.info.agent === "build"),
          ),
        )
        const sqlite = new Database(file)
        sqlite.run("PRAGMA busy_timeout = 10000")
        sqlite.run(
          "CREATE TRIGGER hold_instruction BEFORE INSERT ON part WHEN NEW.data LIKE '%has been approved%' BEGIN SELECT RAISE(ABORT, 'instruction held'); END",
        )
        const failed = yield* accept
        sqlite.run("DROP TRIGGER hold_instruction")
        sqlite.close()
        expect(failed.status).toBe(409)
        expect(failed.body).toMatchObject({ _tag: "AskNotAppliedError", requestID: emitted.id, reason: "apply-failed" })
        expect(yield* builds).toEqual([])
        const listed = yield* json(HttpClientRequest.get(`${server.url}/question`), home)
        expect(listed.body.map((item: { id: string }) => item.id)).toEqual([emitted.id])
        const waiting = (yield* messages(server.url, home, sessionID))
          .flatMap((message) => message.parts)
          .find((part) => part.callID === emitted.origin.callID)
        expect(waiting.state.status).toBe("running")

        expect((yield* accept).body).toMatchObject({ disposition: "settled", decision: { answers: [["Yes"]] } })
        yield* toolPart(server.url, home, sessionID, emitted.origin.callID, "completed")
        expect(yield* builds).toHaveLength(1)
        yield* assistantText(server.url, home, sessionID, "building")
      }),
    120_000,
  )

  cliIt.live(
    "a plan accepted after a restart whose switch fails settles nothing, and a retry applies it",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "plan-recovery.db")
        const env = { OPENCODE_DB: file, OPENCODE_EXPERIMENTAL_PLAN_MODE: "1" }
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions-command", receiver.path]
        yield* llm.tool("plan_exit", {})
        yield* llm.text("building after the restart")
        const first = yield* opencode.serve({ extraArgs, env })
        const sessionID = yield* startTurn(first.url, home, "plan")
        const [emitted] = yield* receiver.read(1)
        first.kill()
        yield* Effect.promise(() => first.exited)

        const second = yield* opencode.serve({ extraArgs, env })
        const accept = json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Yes"]] }),
          ),
          home,
        )
        const sqlite = new Database(file)
        sqlite.run("PRAGMA busy_timeout = 10000")
        sqlite.run(
          "CREATE TRIGGER hold_instruction BEFORE INSERT ON part WHEN NEW.data LIKE '%has been approved%' BEGIN SELECT RAISE(ABORT, 'instruction held'); END",
        )
        const failed = yield* accept
        sqlite.run("DROP TRIGGER hold_instruction")
        sqlite.close()
        expect(failed.status).toBe(409)
        expect(failed.body).toMatchObject({ _tag: "AskNotAppliedError", reason: "apply-failed" })
        const listed = yield* json(HttpClientRequest.get(`${second.url}/question`), home)
        expect(listed.body.map((item: { id: string }) => item.id)).toEqual([emitted.id])

        expect((yield* accept).body).toMatchObject({ disposition: "settled", recovered: true })
        yield* toolPart(second.url, home, sessionID, emitted.origin.callID, "completed")
        yield* assistantText(second.url, home, sessionID, "building after the restart")
      }),
    120_000,
  )

  cliIt.live(
    "a recovered call whose continuation was lost with its server continues on the next answer",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const file = path.join(home, "wake.db")
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const extraArgs = ["--questions-command", receiver.path]
        yield* llm.tool("question", { questions: [questions[1]] })
        yield* llm.text("continued after the lost wake")
        const first = yield* opencode.serve({ extraArgs, env: { OPENCODE_DB: file } })
        const sessionID = yield* startTurn(first.url, home)
        const [emitted] = yield* receiver.read(1)
        first.kill()
        yield* Effect.promise(() => first.exited)
        // A recovery committed the call's outcome, and its process died before the wake that continues the turn.
        const sqlite = new Database(file)
        const stored = sqlite
          .query<
            { id: string; data: string },
            [string, string]
          >("SELECT id, data FROM part WHERE session_id = ? AND json_extract(data, '$.callID') = ?")
          .get(sessionID, emitted.origin.callID)!
        const data = JSON.parse(stored.data)
        data.state = {
          status: "completed",
          input: data.state.input,
          output: 'User has answered your questions: "What should the banner say?"="Recovered".',
          title: "Asked 1 question",
          metadata: { answers: [["Recovered"]] },
          time: { start: data.state.time.start, end: Date.now() },
        }
        sqlite.query("UPDATE part SET data = ? WHERE id = ?").run(JSON.stringify(data), stored.id)
        sqlite
          .query("UPDATE session_ask SET decision = ?, time_settled = ?, recovered = 1 WHERE id = ?")
          .run(JSON.stringify({ type: "answered", answers: [["Recovered"]] }), Date.now(), emitted.id)
        sqlite.close()

        const second = yield* opencode.serve({ extraArgs, env: { OPENCODE_DB: file } })
        const repeated = yield* json(
          HttpClientRequest.post(`${second.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Recovered"]] }),
          ),
          home,
        )
        expect(repeated.body).toMatchObject({ disposition: "duplicate", recovered: true })
        yield* assistantText(second.url, home, sessionID, "continued after the lost wake")
        expect(JSON.stringify((yield* llm.inputs)[1])).toContain("Recovered")
      }),
    120_000,
  )

  cliIt.live(
    "a doom loop ask is emitted with the call that repeated",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const notes = path.join(home, "notes.txt")
        yield* Effect.promise(() => Bun.write(notes, "notes"))
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const server = yield* opencode.serve({ extraArgs: ["--questions-command", receiver.path] })
        // One response repeating the same call three times, the doom loop threshold.
        const chunk = (delta: unknown, finish?: string) => ({
          id: "chatcmpl-test",
          object: "chat.completion.chunk",
          choices: [{ delta, ...(finish ? { finish_reason: finish } : {}) }],
        })
        const call = (index: number) =>
          chunk({
            tool_calls: [
              {
                index,
                id: `call_loop_${index}`,
                type: "function",
                function: { name: "read", arguments: JSON.stringify({ filePath: notes }) },
              },
            ],
          })
        yield* llm.push(
          raw({ chunks: [chunk({ role: "assistant" }), call(0), call(1), call(2), chunk({}, "tool_calls")] }),
        )
        yield* llm.text("stopped looping")
        const sessionID = yield* startTurn(server.url, home)
        const [emitted] = yield* receiver.read(1)

        expect(emitted).toMatchObject({ kind: "permission", sessionID, permission: { permission: "doom_loop" } })
        expect(emitted.origin.messageID).toStartWith("msg_")
        const part = yield* toolPart(server.url, home, sessionID, emitted.origin.callID, "running")
        expect(part.tool).toBe("read")
        const settled = yield* json(
          HttpClientRequest.post(`${server.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ reply: "once" }),
          ),
          home,
        )
        expect(settled.body).toMatchObject({ disposition: "settled" })
        yield* assistantText(server.url, home, sessionID, "stopped looping")
      }),
    90_000,
  )

  cliIt.live(
    "a server bound to a wildcard address names its loopback URL to the receiver",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const server = yield* opencode.serve({ hostname: "0.0.0.0", extraArgs: ["--questions-command", receiver.path] })
        yield* llm.tool("question", { questions: [questions[1]] })
        yield* llm.text("done")
        yield* startTurn(server.url, home)
        const [emitted] = yield* receiver.read(1)
        expect(emitted.server).toBe(`http://127.0.0.1:${new URL(server.url).port}`)
        const settled = yield* json(
          HttpClientRequest.post(`${emitted.server}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["Hello"]] }),
          ),
          home,
        )
        expect(settled.body).toMatchObject({ disposition: "settled" })
      }),
    90_000,
  )

  cliIt.live("refuses to start when the questions command is not an executable", ({ opencode, home }) =>
    Effect.gen(function* () {
      const missing = path.join(home, "no-such-receiver")
      const result = yield* opencode.spawn(["serve", "--port", "0", "--questions-command", missing])
      opencode.expectExit(result, 1, "serve with a missing questions command")
      expect(result.stderr).toContain(`--questions-command is not an executable: ${missing}`)
      expect(result.stdout).not.toContain("listening")
    }),
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
