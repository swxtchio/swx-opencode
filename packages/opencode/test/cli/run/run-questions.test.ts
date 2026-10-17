// Subprocess tests for `opencode run --questions emit` (swxtchio/swx-opencode#286): a non-interactive run keeps its
// stock refusal of questions without the flag, and with it reaches the question, plan and permission producers,
// emits each ask, and goes on once the answer arrives on the emitted route.
import { describe, expect } from "bun:test"
import { access } from "node:fs/promises"
import path from "node:path"
import { Duration, Effect, Schedule } from "effect"
import { HttpClientRequest } from "effect/unstable/http"
import { json, writeReceiver } from "../../lib/ask-receiver"
import { cliIt, deadline } from "../../lib/cli-process"

const question = {
  question: "Which file should I create?",
  header: "File",
  options: [
    { label: "notes.md", description: "Markdown notes" },
    { label: "todo.txt", description: "Plain list" },
  ],
}

// The agent's provider requests, without the title generator's.
const agent = (inputs: Record<string, unknown>[]) => inputs.filter((input) => Array.isArray(input.tools))

// The tool names the provider was offered on the agent's first request.
const offered = (inputs: Record<string, unknown>[]) =>
  ((agent(inputs)[0]?.tools ?? []) as Array<{ function: { name: string } }>).map((tool) => tool.function.name)

describe("opencode run --questions emit (subprocess)", () => {
  cliIt.live(
    "without the flag a run keeps refusing questions, plan exits and permission asks",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const marker = path.join(home, "stock-marker")
        yield* llm.tool("bash", { command: `touch ${marker}`, description: "Create a marker" })
        yield* llm.text("done")
        const result = yield* opencode.run("stock run", {
          permission: { bash: "ask" },
          env: { OPENCODE_EXPERIMENTAL_PLAN_MODE: "1" },
          agent: "plan",
          extraArgs: ["--dir", home],
        })
        opencode.expectExit(result, 0)
        expect(result.stderr).toContain("auto-rejecting")
        expect(offered(yield* llm.inputs)).toContain("bash")
        expect(offered(yield* llm.inputs)).not.toContain("question")
        expect(offered(yield* llm.inputs)).not.toContain("plan_exit")
        expect(yield* exists(marker)).toBe(false)
      }),
    60_000,
  )

  cliIt.live(
    "a local run emits the question tool's ask and continues after its answer",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        yield* llm.tool("question", { questions: [question] })
        yield* llm.text("created the chosen file")
        const run = yield* opencode.startRun("ask before creating", {
          extraArgs: ["--questions", "emit", "--questions-command", receiver.path],
        })

        const [emitted] = yield* receiver.read(1)
        expect(offered(yield* llm.inputs)).toContain("question")
        expect(emitted.kind).toBe("question")
        expect(emitted.origin.tool).toBe("question")
        expect(emitted.server).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/)
        const settled = yield* json(
          HttpClientRequest.post(`${emitted.server}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [[emitted.question.questions[0].options[1].label]] }),
          ),
          emitted.answer.query.directory,
        )
        expect(settled.body).toMatchObject({ disposition: "settled", decision: { answers: [["todo.txt"]] } })

        const result = yield* run.result
        opencode.expectExit(result, 0)
        expect(result.stderr).toContain(`question asked: File (${emitted.id}); waiting for its answer`)
        expect(result.stdout).toContain("created the chosen file")
        expect(JSON.stringify(agent(yield* llm.inputs)[1])).toContain(
          `\\"Which file should I create?\\"=\\"todo.txt\\"`,
        )
      }),
    60_000,
  )

  cliIt.live(
    "plan acceptance is emitted, switches to the build agent once, and a second answer changes nothing",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const release = Promise.withResolvers<void>()
        yield* Effect.addFinalizer(() => Effect.sync(() => release.resolve()))
        yield* llm.tool("plan_exit", {})
        yield* llm.hold("implementing the plan", release.promise)
        const run = yield* opencode.startRun("finish planning", {
          agent: "plan",
          env: { OPENCODE_EXPERIMENTAL_PLAN_MODE: "1" },
          extraArgs: ["--questions", "emit", "--questions-command", receiver.path],
        })

        const [emitted] = yield* receiver.read(1)
        expect(emitted.origin.tool).toBe("plan_exit")
        expect(emitted.question.questions[0].header).toBe("Build Agent")
        const answer = (answers: string[][]) =>
          json(
            HttpClientRequest.post(`${emitted.server}${emitted.answer.path}`).pipe(
              HttpClientRequest.bodyJsonUnsafe({ answers }),
            ),
            emitted.answer.query.directory,
          )
        expect((yield* answer([["Yes"]])).body.disposition).toBe("settled")
        const continued = yield* llm.inputs.pipe(
          Effect.map(agent),
          Effect.filterOrFail(
            (inputs) => inputs.length >= 2,
            () => new Error("the build agent has not continued"),
          ),
          Effect.retry(Schedule.spaced("50 millis")),
          Effect.timeout(Duration.millis(deadline(20_000))),
        )
        expect(JSON.stringify(continued[1])).toContain("has been approved, you can now edit files")

        // The turn is held open, so the second resolver races a live Session.
        const second = yield* answer([["Yes"]])
        expect(second.body).toMatchObject({ disposition: "duplicate", decision: { answers: [["Yes"]] } })
        const messages = yield* json(
          HttpClientRequest.get(`${emitted.server}/session/${emitted.sessionID}/message`),
          emitted.answer.query.directory,
        )
        const approvals = (messages.body as Array<{ info: { role: string; agent: string } }>).filter(
          (message) => message.info.role === "user" && message.info.agent === "build",
        )
        expect(approvals).toHaveLength(1)

        release.resolve()
        opencode.expectExit(yield* run.result, 0)
        expect(agent(yield* llm.inputs)).toHaveLength(2)
      }),
    60_000,
  )

  cliIt.live(
    "a permission ask waits for its answer instead of being auto-rejected, and the answer decides the call",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const args = ["--dir", home, "--questions", "emit", "--questions-command", receiver.path]
        const allowedMarker = path.join(home, "allowed-marker")
        const rejectedMarker = path.join(home, "rejected-marker")
        yield* llm.tool("bash", { command: `touch ${allowedMarker}`, description: "Create a marker" })
        yield* llm.text("done")
        const allowed = yield* opencode.startRun("allowed", { permission: { bash: "ask" }, extraArgs: args })
        const [ask] = yield* receiver.read(1)
        expect(ask.kind).toBe("permission")
        expect(ask.permission.permission).toBe("bash")
        expect(ask.permission.patterns).toEqual([`touch ${allowedMarker}`])
        expect(ask.answer.path).toBe(`/permission/${ask.id}/answer`)
        const once = yield* json(
          HttpClientRequest.post(`${ask.server}${ask.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ reply: "once" }),
          ),
          ask.answer.query.directory,
        )
        expect(once.body).toMatchObject({ disposition: "settled", decision: { type: "replied", reply: "once" } })
        const result = yield* allowed.result
        opencode.expectExit(result, 0)
        expect(result.stderr).not.toContain("auto-rejecting")
        expect(yield* exists(allowedMarker)).toBe(true)

        yield* llm.tool("bash", { command: `touch ${rejectedMarker}`, description: "Create a marker" })
        yield* llm.text("done")
        const rejected = yield* opencode.startRun("rejected", { permission: { bash: "ask" }, extraArgs: args })
        const [, second] = yield* receiver.read(2)
        const reject = yield* json(
          HttpClientRequest.post(`${second.server}${second.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ reply: "reject", message: "use a different name" }),
          ),
          second.answer.query.directory,
        )
        expect(reject.body.decision).toEqual({ type: "replied", reply: "reject", message: "use a different name" })
        yield* rejected.result
        expect(yield* exists(rejectedMarker)).toBe(false)
        expect(JSON.stringify(agent(yield* llm.inputs).at(-1))).toContain("use a different name")
      }),
    60_000,
  )

  cliIt.live(
    "an attached run leaves asks to the emitting server instead of refusing them",
    ({ opencode, home, llm }) =>
      Effect.gen(function* () {
        const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
        const server = yield* opencode.serve({
          extraArgs: ["--questions", "emit", "--questions-command", receiver.path],
        })
        yield* llm.tool("question", { questions: [question] })
        yield* llm.text("attached and answered")
        const run = yield* opencode.startRun("attached ask", {
          extraArgs: ["--attach", server.url, "--dir", home, "--questions", "emit"],
        })
        const [emitted] = yield* receiver.read(1)
        expect(emitted.server).toBe(server.url)
        yield* json(
          HttpClientRequest.post(`${server.url}${emitted.answer.path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ answers: [["notes.md"]] }),
          ),
          emitted.answer.query.directory,
        )
        opencode.expectExit(yield* run.result, 0)
        expect(JSON.stringify(agent(yield* llm.inputs)[1])).toContain(
          `\\"Which file should I create?\\"=\\"notes.md\\"`,
        )
      }),
    60_000,
  )

  cliIt.live(
    "refuses an emit run it cannot serve",
    ({ opencode, home }) =>
      Effect.gen(function* () {
        const missing = yield* opencode.run("x", { extraArgs: ["--questions", "emit"] })
        opencode.expectExit(missing, 1)
        expect(missing.stderr).toContain("--questions emit requires --questions-command")

        const receiver = yield* writeReceiver(home, "cat >/dev/null")
        const auto = yield* opencode.run("x", {
          extraArgs: ["--questions", "emit", "--questions-command", receiver.path, "--auto"],
        })
        opencode.expectExit(auto, 1)
        expect(auto.stderr).toContain("--auto cannot be used with --questions emit")

        const stray = yield* opencode.run("x", { extraArgs: ["--questions-command", receiver.path] })
        opencode.expectExit(stray, 1)
        expect(stray.stderr).toContain("--questions-command requires --questions emit")
      }),
    60_000,
  )
})

const exists = (file: string) =>
  Effect.promise(() =>
    access(file).then(
      () => true,
      () => false,
    ),
  )
