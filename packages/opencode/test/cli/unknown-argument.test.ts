import { describe, expect } from "bun:test"
import { Effect } from "effect"
import { cliIt, deadline } from "../lib/cli-process"

describe("the default (TUI) command", () => {
  // GOAL: `opencode --effort high` is accepted, not rejected as an unknown argument, and
  // --effort appears in `opencode --help`. #29 added it to `run` only.
  cliIt.concurrent(
    "accepts --effort and lists it in --help",
    ({ opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.spawn(["--effort", "high", "--help"])
        expect(result.exitCode).toBe(0)
        expect(result.stderr).toContain("--effort")
        expect(result.stderr).not.toContain("Unknown argument")
      }),
    deadline(60_000),
  )
})

describe("an unknown CLI argument", () => {
  // GOAL: yargs rejects the argument and the help is shown, but the reason has to be printed
  // too. Before, `opencode --effort high` showed only the help, with no hint that --effort
  // was the problem.
  cliIt.concurrent(
    "is named after the help text, and the process exits nonzero",
    ({ opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.spawn(["--bogusflag"])
        expect(result.exitCode).toBe(1)
        expect(result.stderr).toContain("Unknown argument: bogusflag")
        expect(result.stderr.lastIndexOf("Unknown argument")).toBeGreaterThan(result.stderr.lastIndexOf("--help"))
      }),
    deadline(60_000),
  )
})
