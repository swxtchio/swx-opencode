import { Effect } from "effect"
import { effectCmd } from "../effect-cmd"
import { withNetworkOptions, resolveNetworkOptions } from "../network"
import { resolveQuestionCommand, withQuestionOptions } from "../questions"
import { UI } from "../ui"
import { Flag } from "@opencode-ai/core/flag/flag"

export const ServeCommand = effectCmd({
  command: "serve",
  builder: (yargs) => withQuestionOptions(withNetworkOptions(yargs)),
  describe: "starts a headless opencode server",
  // Server loads instances per-request via x-opencode-directory header — no
  // need for an ambient project InstanceContext at startup.
  instance: false,
  handler: Effect.fn("Cli.serve")(function* (args) {
    const questions = resolveQuestionCommand(args)
    if (questions && "error" in questions) {
      UI.error(questions.error)
      process.exit(1)
    }
    const { Server } = yield* Effect.promise(() => import("../../server/server"))
    if (!Flag.OPENCODE_SERVER_PASSWORD) {
      console.log("Warning: OPENCODE_SERVER_PASSWORD is not set; server is unsecured.")
    }
    const opts = yield* resolveNetworkOptions(args)
    const server = yield* Effect.promise(() => Server.listen(opts))
    if (questions) {
      const { AskEmit } = yield* Effect.promise(() => import("../../ask/emit"))
      AskEmit.configure({ command: questions.command, server: server.url.origin })
    }
    console.log(`opencode server listening on http://${server.hostname}:${server.port}`)

    yield* Effect.never
  }),
})
