import type { Argv, InferredOptionTypes } from "yargs"

// `--questions emit` (swxtchio/swx-opencode#286): the operator's choice, on `serve`, to deliver every ask a model
// raises to a supervisor command instead of leaving it to a human-only modal. Only argv sets it.
const options = {
  questions: {
    type: "string" as const,
    choices: ["emit"],
    describe:
      "emit question choices and free text, plan acceptance and permission requests to --questions-command; " +
      "each stays pending until answered on its answer route or by a human",
  },
  "questions-command": {
    type: "string" as const,
    describe: "executable run without a shell for each emitted ask, with the ask as JSON on stdin",
  },
}

export type QuestionOptions = InferredOptionTypes<typeof options>

export function withQuestionOptions<T>(yargs: Argv<T>) {
  return yargs.options(options)
}

// The emission command resolved to an executable path, undefined when emission is off, or the reason the options
// cannot be used.
export function resolveQuestionCommand(args: QuestionOptions): { error: string } | { command: string } | undefined {
  const command = args["questions-command"]
  if (args.questions !== "emit") {
    if (command !== undefined) return { error: "--questions-command requires --questions emit" }
    return undefined
  }
  if (!command) return { error: "--questions emit requires --questions-command" }
  const resolved = Bun.which(command)
  if (!resolved) return { error: `--questions-command is not an executable: ${command}` }
  return { command: resolved }
}
