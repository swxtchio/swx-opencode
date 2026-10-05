import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Effect } from "effect"
import { awaitWithTimeout } from "../../lib/effect"
import { deadline } from "../../lib/deadline"

const isolationScript = `
const databasePath = process.env["OPENCODE_DB"]
if (!databasePath) throw new Error("ambient database path was not set")
const expected = {
  OPENCODE_DB: databasePath,
  XDG_DATA_HOME: process.env["XDG_DATA_HOME"],
  XDG_CONFIG_HOME: process.env["XDG_CONFIG_HOME"],
  XDG_STATE_HOME: process.env["XDG_STATE_HOME"],
  XDG_CACHE_HOME: process.env["XDG_CACHE_HOME"],
  OPENCODE_DISABLE_SHARE: process.env["OPENCODE_DISABLE_SHARE"],
}
const { Flag } = await import("@opencode-ai/core/flag/flag")
const flagDatabase = Flag.OPENCODE_DB
await import("./test/server/httpapi-exercise/environment")
for (const [key, value] of Object.entries(expected)) {
  if (process.env[key] !== value)
    throw new Error("httpapi environment import changed " + key + ": expected " + value + ", got " + process.env[key])
}
if (Flag.OPENCODE_DB !== flagDatabase) throw new Error("httpapi environment import changed Flag.OPENCODE_DB")

const { Effect } = await import("effect")
const { withCliFixture } = await import("./test/lib/cli-process")
const result = await Effect.runPromise(
  Effect.scoped(
    withCliFixture(({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.text("isolated response")
        return yield* opencode.run("say hi")
      }),
    ),
  ),
)
if (result.exitCode !== 0 || !result.stdout.includes("isolated response"))
  throw new Error("isolated CLI run failed (exit " + result.exitCode + "): " + result.stderr)
const { existsSync } = await import("node:fs")
if (existsSync(databasePath)) throw new Error("CLI child inherited the ambient file database")
console.log("CLI_DATABASE_ISOLATION_OK")
`

test(
  "httpapi imports leave worker globals alone and CLI children pin their database",
  async () => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "opencode-cli-database-isolation-"))
    const databasePath = path.join(directory, "shared.sqlite")
    const child = Bun.spawn(["bun", "-e", isolationScript], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        OPENCODE_DB: databasePath,
        XDG_DATA_HOME: path.join(directory, "data"),
        XDG_CONFIG_HOME: path.join(directory, "config"),
        XDG_STATE_HOME: path.join(directory, "state"),
        XDG_CACHE_HOME: path.join(directory, "cache"),
        OPENCODE_DISABLE_SHARE: "false",
        OPENCODE_HTTPAPI_EXERCISE_DB: path.join(directory, "exercise.sqlite"),
        OPENCODE_HTTPAPI_EXERCISE_GLOBAL: path.join(directory, "exercise-global"),
      },
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    })
    try {
      const exitCode = await Effect.runPromise(
        awaitWithTimeout(
          Effect.promise(() => child.exited),
          "database isolation child did not exit",
          deadline(60_000),
        ),
      )
      const stdout = await new Response(child.stdout).text()
      const stderr = await new Response(child.stderr).text()
      if (exitCode !== 0) throw new Error(`database isolation child exited ${exitCode}: ${stderr}`)
      expect(exitCode).toBe(0)
      expect(stdout).toContain("CLI_DATABASE_ISOLATION_OK")
    } finally {
      if (child.exitCode === null) {
        child.kill()
        await child.exited
      }
      await rm(directory, { recursive: true, force: true })
    }
  },
  // Leave 30 scaled seconds between the child backstop and test cleanup.
  { timeout: deadline(90_000) },
)
