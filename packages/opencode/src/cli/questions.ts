import type { Argv, InferredOptionTypes } from "yargs"

// `--questions-command` (swxtchio/swx-opencode#286): the operator's choice, on `serve`, to deliver every ask a model
// raises to a supervisor command instead of leaving it to a human-only modal. Only argv sets it.
const options = {
  "questions-command": {
    type: "string" as const,
    describe:
      "emit question choices and free text, plan acceptance and permission requests to this executable, run " +
      "without a shell with the ask as JSON on stdin; each stays pending until answered on its answer route or by a " +
      "human",
  },
}

export type QuestionOptions = InferredOptionTypes<typeof options>

export function withQuestionOptions<T>(yargs: Argv<T>) {
  return yargs.options(options)
}

// The emission command resolved to an executable path, undefined when emission is off, or the reason it cannot be
// used.
export function resolveQuestionCommand(args: QuestionOptions): { error: string } | { command: string } | undefined {
  const command = args["questions-command"]
  if (command === undefined) return undefined
  const resolved = command ? Bun.which(command) : null
  if (!resolved) return { error: `--questions-command is not an executable: ${command}` }
  return { command: resolved }
}
