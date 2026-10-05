// Subprocess integration tests for `opencode run` (non-interactive mode).
// These exercise the real CLI binary against a TestLLMServer running in the
// same process. See `test/lib/cli-process.ts` for the harness — each test uses
// `opencode.run(message, opts?)` to spawn `bun src/index.ts run ...` with
// `OPENCODE_CONFIG_CONTENT` providing the test provider config inline.
import { describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Effect } from "effect"
import { awaitWithTimeout } from "../../lib/effect"
import { SqliteProbe } from "@opencode-ai/core/database/sqlite-probe"
import { reply } from "../../lib/llm-server"
import { cliIt, deadline } from "../../lib/cli-process"

const sqliteLockHolderScript = `
import { Context, Effect, Layer } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { layer } from "@opencode-ai/core/database/sqlite.bun"

const filename = process.env["OPENCODE_DB"]
if (!filename) throw new Error("SQLite lock holder has no database path")
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const context = yield* Layer.build(layer({ filename, disableWAL: true }))
      const client = Context.get(context, SqlClient)
      yield* client.withTransaction(
        Effect.gen(function* () {
          yield* client.unsafe("INSERT INTO opencode_probe_lock (id, value) VALUES (?, ?)", [1, "probe-private-holder-value"]).raw
          // Exercise runValues success events while the production transaction holds the write lock.
          yield* client.unsafe("SAVEPOINT opencode_probe_savepoint").values
          yield* client.unsafe("RELEASE opencode_probe_savepoint").values
          console.log("SQLITE_PROBE_LOCK_HELD")
          yield* Effect.promise(() => new Promise((resolve) => process.stdin.once("data", resolve)))
        }),
      )
    }),
  ),
)
`

const sqliteLockClientScript = `
import { Context, Effect, Exit, Layer } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { layer } from "@opencode-ai/core/database/sqlite.bun"

const filename = process.env["OPENCODE_DB"]
if (!filename) throw new Error("SQLite lock client has no database path")
const outcomes = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const context = yield* Layer.build(layer({ filename, disableWAL: true }))
      const client = Context.get(context, SqlClient)
      const runResult = yield* client
        .unsafe("INSERT INTO opencode_probe_lock (id, value) VALUES (?, ?)", [2, "probe-private-run-value"])
        .raw.pipe(Effect.exit)
      const valuesResult = yield* client
        .unsafe("INSERT INTO opencode_probe_lock (id, value) VALUES (?, ?) RETURNING id", [
          3,
          "probe-private-values-value",
        ])
        .values.pipe(Effect.exit)
      return { runResult, valuesResult }
    }),
  ),
)
if (Exit.isSuccess(outcomes.runResult) || Exit.isSuccess(outcomes.valuesResult))
  throw new Error("SQLite lock client unexpectedly wrote while the holder transaction was open")
process.exitCode = 1
`

describe("SQLite CI probe redaction", () => {
  test("only accepts the targeted run-process probe IDs", () => {
    expect(SqliteProbe.isAllowedProbeID("run-process-success")).toBe(true)
    expect(SqliteProbe.isAllowedProbeID("run-process-sqlite-holder")).toBe(true)
    expect(SqliteProbe.isAllowedProbeID("run-process-sqlite-client")).toBe(true)
    expect(SqliteProbe.isAllowedProbeID("free-form-input")).toBe(false)
  })

  test("redacts SQL values and comments while retaining safe identifiers", () => {
    expect(
      SqliteProbe.sanitizeStatement(
        `INSERT INTO "session" ("id", "value") VALUES (?1, 'secret_parameter') -- private comment`,
      ),
    ).toBe('INSERT INTO "session" ("id", "value") VALUES (?1, ?)')
    expect(SqliteProbe.sanitizeStatement('SELECT "private@example.test" AS "value"')).toBe('SELECT ? AS "value"')
    expect(SqliteProbe.sanitizeStatement("SELECT 0xdeadbeef, 42")).toBe("SELECT ?, ?")
  })

  test("bounds the normalized statement", () => {
    expect(SqliteProbe.sanitizeStatement(`SELECT ${"x".repeat(500)}`)).toHaveLength(192)
  })

  test("normalizes SQLite lock codes without returning error messages", () => {
    expect(SqliteProbe.sqliteErrorCode({ code: "SQLITE_BUSY" })).toBe("SQLITE_BUSY")
    expect(SqliteProbe.sqliteErrorCode({ errcode: 5 })).toBe("SQLITE_BUSY")
    expect(SqliteProbe.sqliteErrorCode({ errcode: 6 })).toBe("SQLITE_LOCKED")
    expect(SqliteProbe.sqliteErrorCode({ code: "EIO", message: "private path" })).toBeUndefined()
  })
})

const sqliteProbeEnabled = process.env["OPENCODE_SQLITE_PROBE"] === "1"

function sqliteProbeOptions(probeID: string) {
  const env: Record<string, string> = sqliteProbeEnabled ? { OPENCODE_SQLITE_PROBE_ID: probeID } : {}
  return { env }
}

async function readSqliteProbeLog() {
  const logPath = process.env["OPENCODE_SQLITE_PROBE_LOG"]
  if (!logPath) throw new Error("OPENCODE_SQLITE_PROBE_LOG is required when the SQLite probe is enabled")
  const text = await Bun.file(logPath).text()
  return {
    text,
    records: text
      .split("\n")
      .filter((line) => line.startsWith("{") && line.endsWith("}"))
      .map((line) => JSON.parse(line) as Record<string, unknown>),
  }
}

function expectSqliteProbe(probeID: string) {
  return Effect.promise(async () => {
    if (!sqliteProbeEnabled) return
    const log = await readSqliteProbeLog()
    expect(
      log.records.some(
        (record) =>
          record.event === "database_open" &&
          record.probe_id === probeID &&
          record.driver === "bun:sqlite" &&
          record.database_path === ":memory:" &&
          typeof record.pid === "number" &&
          typeof record.connection_id === "number",
      ),
    ).toBe(true)
  })
}

async function readProbeReadyLine(stream: ReadableStream<Uint8Array>) {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let buffered = ""
  try {
    while (true) {
      const result = await reader.read()
      if (result.done) throw new Error("SQLite lock holder exited before its readiness signal")
      buffered += decoder.decode(result.value, { stream: true })
      const newline = buffered.indexOf("\n")
      if (newline >= 0) return buffered.slice(0, newline).trim()
    }
  } finally {
    reader.releaseLock()
  }
}

if (sqliteProbeEnabled) {
  test(
    "reports the real SQLite busy statement and lock-holder transaction",
    async () => {
      const probeID = "run-process-sqlite-client"
      const holderProbeID = "run-process-sqlite-holder"
      const directory = await mkdtemp(path.join(os.tmpdir(), "opencode-sqlite-probe-"))
      const databasePath = path.join(directory, "locked.sqlite")
      try {
        const setup = new Database(databasePath)
        try {
          setup.run("PRAGMA journal_mode = WAL")
          setup.run("CREATE TABLE opencode_probe_lock (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        } finally {
          setup.close()
        }

        const lockHolder = Bun.spawn(["bun", "-e", sqliteLockHolderScript], {
          cwd: process.cwd(),
          env: {
            ...process.env,
            OPENCODE_DB: databasePath,
            OPENCODE_SQLITE_PROBE: "1",
            OPENCODE_SQLITE_PROBE_ID: holderProbeID,
          },
          stdin: "pipe",
          stdout: "pipe",
          stderr: "ignore",
        })
        try {
          const ready = await Effect.runPromise(
            awaitWithTimeout(
              Effect.promise(() => readProbeReadyLine(lockHolder.stdout)),
              "SQLite lock holder did not confirm its production transaction",
              "5 seconds",
            ),
          )
          expect(ready).toBe("SQLITE_PROBE_LOCK_HELD")

          const busyClient = Bun.spawn(["bun", "-e", sqliteLockClientScript], {
            cwd: process.cwd(),
            env: {
              ...process.env,
              OPENCODE_DB: databasePath,
              OPENCODE_SQLITE_PROBE: "1",
              OPENCODE_SQLITE_PROBE_ID: probeID,
            },
            stdin: "ignore",
            stdout: "ignore",
            stderr: "ignore",
          })
          try {
            const exitCode = await Effect.runPromise(
              awaitWithTimeout(
                Effect.promise(() => busyClient.exited),
                "SQLite lock client did not exit",
                "10 seconds",
              ),
            )
            expect(exitCode).not.toBe(0)

            await lockHolder.stdin.write("release\n")
            lockHolder.stdin.end()
            await Effect.runPromise(
              awaitWithTimeout(
                Effect.promise(() => lockHolder.exited),
                "SQLite lock holder did not exit after release",
                "5 seconds",
              ),
            )

            const log = await readSqliteProbeLog()
            const records = log.records
            expect(Buffer.byteLength(log.text)).toBeLessThan(2 * 1024 * 1024)
            expect(
              log.text
                .split("\n")
                .filter(Boolean)
                .every((line) => line.length <= 768),
            ).toBe(true)
            const holderOpen = records.find(
              (record) => record.event === "database_open" && record.probe_id === holderProbeID,
            )
            const holderClient = records.find(
              (record) => record.event === "client_open" && record.probe_id === holderProbeID,
            )
            const holderBegin = records.find(
              (record) =>
                record.event === "transaction_begin" &&
                record.probe_id === holderProbeID &&
                record.operation === "BEGIN",
            )
            const holderSavepoint = records.find(
              (record) =>
                record.event === "transaction_begin" &&
                record.probe_id === holderProbeID &&
                record.operation === "SAVEPOINT",
            )
            const holderRelease = records.find(
              (record) =>
                record.event === "transaction_end" &&
                record.probe_id === holderProbeID &&
                record.operation === "RELEASE",
            )
            const holderCommit = records.find(
              (record) =>
                record.event === "transaction_end" &&
                record.probe_id === holderProbeID &&
                record.operation === "COMMIT",
            )
            const clientDatabase = records.find(
              (record) => record.event === "database_open" && record.probe_id === probeID,
            )
            const clientOpen = records.find((record) => record.event === "client_open" && record.probe_id === probeID)
            const busyRun = records.find(
              (record) =>
                record.event === "statement_error" &&
                record.probe_id === probeID &&
                record.statement === "INSERT INTO opencode_probe_lock (id, value) VALUES (?, ?)" &&
                typeof record.error_code === "string" &&
                record.error_code.startsWith("SQLITE_BUSY"),
            )
            const busyValues = records.find(
              (record) =>
                record.event === "statement_error" &&
                record.probe_id === probeID &&
                record.statement === "INSERT INTO opencode_probe_lock (id, value) VALUES (?, ?) RETURNING id" &&
                typeof record.error_code === "string" &&
                record.error_code.startsWith("SQLITE_BUSY"),
            )
            expect(holderOpen).toBeDefined()
            expect(holderClient).toBeDefined()
            expect(holderBegin).toBeDefined()
            expect(holderSavepoint).toBeDefined()
            expect(holderRelease).toBeDefined()
            expect(holderCommit).toBeDefined()
            expect(clientDatabase).toBeDefined()
            expect(clientOpen).toBeDefined()
            expect(busyRun).toBeDefined()
            expect(busyValues).toBeDefined()
            if (
              !holderOpen ||
              !holderClient ||
              !holderBegin ||
              !holderSavepoint ||
              !holderRelease ||
              !holderCommit ||
              !clientDatabase ||
              !clientOpen ||
              !busyRun ||
              !busyValues
            )
              return

            expect(holderOpen.pid).toBe(lockHolder.pid)
            expect(holderClient.pid).toBe(lockHolder.pid)
            expect(holderBegin.client_id).toBe(holderClient.client_id)
            expect(holderSavepoint.client_id).toBe(holderClient.client_id)
            expect(holderRelease.client_id).toBe(holderClient.client_id)
            expect(holderCommit.client_id).toBe(holderClient.client_id)
            expect(holderRelease.transaction_ids).toContain(holderSavepoint.transaction_id)
            expect(holderCommit.transaction_ids).toContain(holderBegin.transaction_id)
            expect(clientDatabase.pid).toBe(busyClient.pid)
            expect(clientOpen.pid).toBe(busyClient.pid)
            expect(busyRun.pid).toBe(busyClient.pid)
            expect(busyValues.pid).toBe(busyClient.pid)
            expect(busyRun.error_code).toBe("SQLITE_BUSY")
            expect(busyValues.error_code).toBe("SQLITE_BUSY")
            expect(busyRun.database_key).toBe(clientDatabase.database_key)
            expect(busyRun.database_key).toBe(holderOpen.database_key)
            expect(busyRun.database_path).toBe(`<file:${clientDatabase.database_key}>`)
            expect(busyRun.database_kind).toBe("file")
            expect(busyRun.client_id).toBe(clientOpen.client_id)
            expect(busyRun.operation).toBe("INSERT")
            expect(busyRun.statement).toBe("INSERT INTO opencode_probe_lock (id, value) VALUES (?, ?)")
            expect(busyValues.client_id).toBe(clientOpen.client_id)
            expect(busyValues.database_key).toBe(clientDatabase.database_key)
            expect(busyValues.operation).toBe("INSERT")
            expect(busyValues.statement).toBe("INSERT INTO opencode_probe_lock (id, value) VALUES (?, ?) RETURNING id")
            expect(log.text).not.toContain(databasePath)
            expect(log.text).not.toContain("probe-private-holder-value")
            expect(log.text).not.toContain("probe-private-run-value")
            expect(log.text).not.toContain("probe-private-values-value")
            const holderCandidate = expect.objectContaining({
              pid: holderOpen.pid,
              connection_id: holderOpen.connection_id,
              client_id: holderBegin.client_id,
              transaction_id: holderBegin.transaction_id,
            })
            expect(busyRun.lock_candidates).toEqual(expect.arrayContaining([holderCandidate]))
            expect(busyValues.lock_candidates).toEqual(expect.arrayContaining([holderCandidate]))
          } finally {
            if (busyClient.exitCode === null) {
              busyClient.kill()
              await busyClient.exited
            }
          }
        } finally {
          if (lockHolder.exitCode === null) {
            await lockHolder.stdin.write("release\n")
            lockHolder.stdin.end()
            try {
              await Effect.runPromise(
                awaitWithTimeout(
                  Effect.promise(() => lockHolder.exited),
                  "SQLite lock holder did not exit",
                  "5 seconds",
                ),
              )
            } catch {
              lockHolder.kill()
              await lockHolder.exited
            }
          }
        }
      } finally {
        await rm(directory, { recursive: true, force: true })
      }
    },
    { timeout: 30_000 },
  )
}

describe("opencode run (non-interactive subprocess)", () => {
  // Happy path: prompt completes, output reaches stdout, process exits 0.
  // If this fails, all the others likely will too — debug here first.
  cliIt.concurrent(
    "exits 0 and writes the response to stdout on a successful prompt",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.text("hello from the test llm")
        const probeID = "run-process-success"
        const result = yield* opencode.run("say hi", sqliteProbeOptions(probeID))
        yield* expectSqliteProbe(probeID)
        opencode.expectExit(result, 0)
        expect(result.stdout).toBe("hello from the test llm\n")
      }),
    deadline(60_000),
  )

  cliIt.concurrent(
    "prints each completed text part in order around a tool continuation",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.push(
          reply().text("  before tool  ").tool("bash", {
            command: "printf tool-output",
            description: "Print deterministic output",
          }),
        )
        yield* llm.text("  after tool  ")

        const result = yield* opencode.run("use a tool", {
          extraArgs: ["--dangerously-skip-permissions"],
        })

        opencode.expectExit(result, 0)
        expect(result.stdout).toBe("before tool\nafter tool\n")
      }),
    deadline(60_000),
  )

  cliIt.concurrent(
    "prints reasoning before text only with --thinking",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.reason("  considering  ", { text: "  answer  " })
        const thinkingProbeID = "run-process-thinking"
        const thinking = yield* opencode.run("think", {
          extraArgs: ["--thinking"],
          ...sqliteProbeOptions(thinkingProbeID),
        })
        yield* expectSqliteProbe(thinkingProbeID)
        opencode.expectExit(thinking, 0)
        expect(thinking.stdout).toBe("Thinking: considering\nanswer\n")

        yield* llm.reason("hidden", { text: "visible" })
        const plainProbeID = "run-process-thinking-plain"
        const plain = yield* opencode.run("think again", sqliteProbeOptions(plainProbeID))
        yield* expectSqliteProbe(plainProbeID)
        opencode.expectExit(plain, 0)
        expect(plain.stdout).toBe("visible\n")
      }),
    deadline(60_000),
  )

  // Regression for #27371: an unknown model used to hang the process forever
  // waiting on a session.status === idle event that never arrived. The fix
  // makes the SDK call surface an error promptly so the process exits nonzero.
  // We assert the CLI exited with a real error code of its OWN accord. The
  // harness synthesizes exitCode -1 when it has to kill the run, so a hang
  // fails this assertion, while `not.toBe(0)` alone would have passed on one.
  //
  // Deliberately NOT a wall-clock assertion: the previous form bounded the
  // duration by the same value as the harness kill, so a slow-but-healthy run
  // under parallel load failed as if it had hung. Elapsed time is the harness
  // timeout's job (a bounded backstop); the exit code is the real signal.
  cliIt.concurrent(
    "exits nonzero without hanging when the model is unknown (regression for #27371)",
    ({ opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.run("say hi", {
          model: "test/nonexistent-model",
          timeoutMs: deadline(25_000),
        })
        expect(result.exitCode).toBeGreaterThan(0)
      }),
    deadline(30_000),
  )

  // The server reports a prompt validation error by publishing session.error and then
  // failing the request, which reaches the CLI only as a generic 500 ("Unexpected server
  // error"). The CLI must print the published error instead, or #29's promise that an
  // unknown effort names the valid choices is broken, and so is every other such error.
  cliIt.concurrent(
    "prints the real error for an unknown effort, not the generic server error",
    ({ opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.run("say hi", { extraArgs: ["--effort", "no-such-effort"] })
        expect(result.exitCode).toBeGreaterThan(0)
        expect(result.stderr).toContain('Unknown effort "no-such-effort"')
        expect(result.stderr).not.toContain("Unexpected server error")
      }),
    deadline(60_000),
  )

  cliIt.concurrent(
    "prints the real error for an unknown command, not the generic server error",
    ({ opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.run("x", { extraArgs: ["--command", "no-such-command"] })
        expect(result.exitCode).toBeGreaterThan(0)
        expect(result.stderr).toContain('Command not found: "no-such-command"')
        expect(result.stderr).not.toContain("Unexpected server error")
      }),
    deadline(60_000),
  )

  // GOAL: the attach path, where the event stream is a separate connection to another
  // process, still shows the published error rather than the generic 500.
  cliIt.live(
    "prints the real error for an unknown effort in attach mode",
    ({ opencode }) =>
      Effect.gen(function* () {
        const server = yield* opencode.serve()
        const result = yield* opencode.run("say hi", {
          extraArgs: ["--attach", server.url, "--effort", "no-such-effort", "--"],
        })
        expect(result.exitCode).toBeGreaterThan(0)
        expect(result.stderr).toContain('Unknown effort "no-such-effort"')
        expect(result.stderr).not.toContain("Unexpected server error")
      }),
    deadline(60_000),
  )

  // GOAL: in json mode the one error record is the published error, not the 500 body.
  cliIt.concurrent(
    "emits the real error as the json error record for an unknown effort",
    ({ opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.run("say hi", { extraArgs: ["--format", "json", "--effort", "no-such-effort"] })
        expect(result.exitCode).toBeGreaterThan(0)
        const errors = result.stdout
          .split("\n")
          .filter(Boolean)
          .map((line) => JSON.parse(line))
          .filter((record) => record.type === "error")
        expect(errors).toHaveLength(1)
        expect(JSON.stringify(errors[0])).toContain('Unknown effort \\"no-such-effort\\"')
      }),
    deadline(60_000),
  )

  // GOAL: --command forwards the effort into the same validation, and its error surfaces too.
  cliIt.concurrent(
    "prints the real error for an unknown effort on a --command run",
    ({ opencode }) =>
      Effect.gen(function* () {
        const probeID = "run-process-command-effort"
        const result = yield* opencode.run("x", {
          extraArgs: ["--command", "init", "--effort", "no-such-effort"],
          ...sqliteProbeOptions(probeID),
        })
        yield* expectSqliteProbe(probeID)
        expect(result.exitCode).toBeGreaterThan(0)
        expect(result.stderr).toContain('Unknown effort "no-such-effort"')
        expect(result.stderr).not.toContain("Unexpected server error")
      }),
    deadline(60_000),
  )

  // The test provider's SSE error item is interpreted by the SDK as an unknown
  // finish, not a fatal provider/session error. Unknown finishes should continue
  // the prompt loop so a subsequent response can complete the run.
  cliIt.concurrent(
    "unknown stream finish preserves partial output and continues",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.push(
          reply().text("partial response").tool("bash", {
            command: "printf tool",
            description: "Print deterministic output",
          }),
        )
        yield* llm.fail("upstream provider exploded mid-stream")
        yield* llm.text("recovered")
        const probeID = "run-process-unknown-finish"
        const result = yield* opencode.run("trigger midstream error", {
          timeoutMs: deadline(30_000),
          ...sqliteProbeOptions(probeID),
        })
        yield* expectSqliteProbe(probeID)
        opencode.expectExit(result, 0)
        expect(result.stdout).toBe("partial response\nrecovered\n")
        expect(result.stderr).not.toContain("upstream provider exploded mid-stream")
      }),
    deadline(60_000),
  )

  // --format json puts one JSON object per line on stdout for each emitted
  // event. Consumers (CI scripts, tooling) parse this stream. Asserts the
  // shape so a future event-emit change has to update this expectation.
  cliIt.concurrent(
    "--format json emits parseable line-delimited JSON to stdout",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.text("structured output")
        const probeID = "run-process-json-output"
        const result = yield* opencode.run("say hi", { format: "json", ...sqliteProbeOptions(probeID) })
        yield* expectSqliteProbe(probeID)
        opencode.expectExit(result, 0)

        const events = opencode.parseJsonEvents(result.stdout)
        expect(events.length).toBeGreaterThan(0)
        for (const evt of events) {
          expect(typeof evt.type).toBe("string")
          expect(typeof evt.sessionID).toBe("string")
        }
        expect(events.map((event) => event.type)).toEqual(["step_start", "text", "step_finish"])
        expect(events.map(({ timestamp: _, sessionID: __, ...event }) => event)).toEqual([
          { type: "step_start", part: expect.objectContaining({ type: "step-start" }) },
          {
            type: "text",
            part: expect.objectContaining({ type: "text", text: "structured output" }),
          },
          { type: "step_finish", part: expect.objectContaining({ type: "step-finish" }) },
        ])
        expect(result.stdout.endsWith("\n")).toBe(true)
        expect(
          result.stdout
            .split("\n")
            .slice(0, -1)
            .every((line) => line.length > 0),
        ).toBe(true)
      }),
    deadline(60_000),
  )

  cliIt.concurrent(
    "--format json emits a pure error record for a rejected prompt request",
    ({ opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.run("use an unknown model", {
          model: "test/nonexistent-model",
          format: "json",
        })

        expect(result.exitCode).not.toBe(0)
        const events = opencode.parseJsonEvents(result.stdout)
        expect(events.map((event) => event.type)).toEqual(["error"])
        expect(events[0]).toEqual({
          type: "error",
          timestamp: expect.any(Number),
          sessionID: expect.any(String),
          error: expect.any(Object),
        })
        expect(result.stdout.split("\n").filter(Boolean)).toHaveLength(1)
      }),
    deadline(30_000),
  )

  cliIt.concurrent(
    "--format json preserves reasoning, tool, and continuation ordering",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.push(
          reply().reason("reasoning").text("before").tool("bash", {
            command: "printf tool",
            description: "Print deterministic output",
          }),
        )
        yield* llm.text("after")

        const result = yield* opencode.run("exercise json records", {
          format: "json",
          extraArgs: ["--thinking", "--dangerously-skip-permissions"],
        })

        opencode.expectExit(result, 0)
        const events = opencode.parseJsonEvents(result.stdout)
        expect(events.map((event) => event.type)).toEqual([
          "step_start",
          "reasoning",
          "text",
          "tool_use",
          "step_finish",
          "step_start",
          "text",
          "step_finish",
        ])
        expect(events.find((event) => event.type === "reasoning")?.part).toEqual(
          expect.objectContaining({ type: "reasoning", text: "reasoning" }),
        )
        expect(events.find((event) => event.type === "tool_use")?.part).toEqual(
          expect.objectContaining({
            type: "tool",
            tool: "bash",
            state: expect.objectContaining({ status: "completed" }),
          }),
        )
        expect(
          result.stdout
            .split("\n")
            .slice(0, -1)
            .every((line) => line.startsWith("{")),
        ).toBe(true)
      }),
    deadline(60_000),
  )

  cliIt.concurrent(
    "--format json records an unknown stream finish and continuation",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.push(
          reply().text("partial json").tool("bash", {
            command: "printf tool",
            description: "Print deterministic output",
          }),
        )
        yield* llm.fail("provider failed")
        yield* llm.text("recovered")
        const result = yield* opencode.run("fail after output", { format: "json" })

        const events = opencode.parseJsonEvents(result.stdout)
        opencode.expectExit(result, 0)
        expect(events.map((event) => event.type)).toEqual([
          "step_start",
          "text",
          "tool_use",
          "step_finish",
          "step_start",
          "step_finish",
          "step_start",
          "text",
          "step_finish",
        ])
        expect(events[1]?.part).toEqual(expect.objectContaining({ type: "text", text: "partial json" }))
        expect(events[5]?.part).toEqual(expect.objectContaining({ type: "step-finish", reason: "unknown" }))
        expect(events[7]?.part).toEqual(expect.objectContaining({ type: "text", text: "recovered" }))
        expect(events.at(-1)?.part).toEqual(expect.objectContaining({ type: "step-finish", reason: "stop" }))
      }),
    deadline(60_000),
  )

  cliIt.concurrent(
    "rejects requested permissions by default and allows them with the dangerous flag",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.tool("bash", { command: "rm -f denied-file", description: "Remove a test file" })
        yield* llm.text("continued after rejection")
        const deniedProbeID = "run-process-permission-ask"
        const denied = yield* opencode.run("request permission", {
          permission: { bash: "ask" },
          ...sqliteProbeOptions(deniedProbeID),
        })
        yield* expectSqliteProbe(deniedProbeID)
        opencode.expectExit(denied, 0)
        expect(denied.stderr).toContain("permission requested: bash")
        expect(denied.stdout).toBe("")

        yield* llm.reset
        yield* llm.tool("bash", { command: "rm -f allowed-file", description: "Remove a test file" })
        yield* llm.text("continued after approval")
        const allowedProbeID = "run-process-permission-allow"
        const allowed = yield* opencode.run("request permission", {
          permission: { bash: "ask" },
          extraArgs: ["--dangerously-skip-permissions"],
          ...sqliteProbeOptions(allowedProbeID),
        })
        yield* expectSqliteProbe(allowedProbeID)
        opencode.expectExit(allowed, 0)
        expect(allowed.stderr).not.toContain("permission requested: bash")
        expect(allowed.stdout).toContain("continued after approval")

        yield* llm.reset
        yield* llm.tool("bash", { command: "touch explicitly-denied", description: "Create a denied marker" })
        yield* llm.text("continued after explicit denial")
        const explicitlyDeniedProbeID = "run-process-permission-deny"
        const explicitlyDenied = yield* opencode.run("request denied permission", {
          permission: { bash: "deny" },
          extraArgs: ["--dangerously-skip-permissions"],
          ...sqliteProbeOptions(explicitlyDeniedProbeID),
        })
        yield* expectSqliteProbe(explicitlyDeniedProbeID)
        opencode.expectExit(explicitlyDenied, 0)
        expect(explicitlyDenied.stdout).toContain("continued after explicit denial")
        expect(yield* Effect.promise(() => Bun.file(`${home}/explicitly-denied`).exists())).toBe(false)
      }),
    deadline(60_000),
  )

  cliIt.live(
    "attach mode sends client-local file contents without a shared path",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        const source = `${home}/client-only.txt`
        const sentinel = "client-only attachment sentinel"
        yield* Effect.promise(() => Bun.write(source, sentinel))
        yield* llm.text("attachment received")
        const server = yield* opencode.serve()

        const result = yield* opencode.run("read the attachment", {
          extraArgs: ["--attach", server.url, `--file=${source}`, "--"],
        })

        opencode.expectExit(result, 0)
        const input = JSON.stringify(yield* llm.inputs)
        expect(input).toContain(sentinel)
        expect(input).not.toContain(`file://${source}`)
      }),
    deadline(60_000),
  )

  cliIt.concurrent(
    "attach mode rejects local directories before prompt admission",
    ({ home, opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.run("read the directory", {
          extraArgs: ["--attach", "http://127.0.0.1:1", `--file=${home}`, "--"],
        })

        expect(result.exitCode).not.toBe(0)
        expect(result.stderr).toContain("Cannot attach local directory without a shared filesystem")
      }),
    deadline(30_000),
  )

  cliIt.live(
    "SIGINT interrupts an active non-interactive run without leaking the process",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.hang
        const run = yield* opencode.startRun("wait forever")
        yield* llm.wait(1)
        run.interrupt()
        const result = yield* run.result

        expect(result.exitCode).not.toBe(0)
        // Promptness with real headroom: bounding this by the test budget
        // itself (both were 30_000) meant the assertion could only fail by
        // racing the thing that would already have killed the test. A third of
        // the budget still catches a hang while leaving room for a slow run.
        expect(result.durationMs).toBeLessThan(deadline(10_000))
      }),
    deadline(30_000),
  )
})
