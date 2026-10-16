import { Agent } from "@/agent/agent"
import { Command } from "@/command"
import * as InstanceState from "@/effect/instance-state"
import { Format } from "@/format"
import { Global } from "@opencode-ai/core/global"
import { NamedError } from "@opencode-ai/core/util/error"
import { LSP } from "@/lsp/lsp"
import { Provider } from "@/provider/provider"
import { Vcs } from "@/project/vcs"
import { Skill } from "@/skill"
import { SessionPrompt } from "@/session/prompt"
import { Effect } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import {
  ApiVcsApplyError,
  PromptEnhanceModelNotFoundError,
  PromptEnhancePayload,
  PromptEnhanceProviderError,
} from "../groups/instance"
import { markInstanceForDisposal } from "../lifecycle"

export const instanceHandlers = HttpApiBuilder.group(InstanceHttpApi, "instance", (handlers) =>
  Effect.gen(function* () {
    const agent = yield* Agent.Service
    const command = yield* Command.Service
    const format = yield* Format.Service
    const lsp = yield* LSP.Service
    const skill = yield* Skill.Service
    const vcs = yield* Vcs.Service
    const prompt = yield* SessionPrompt.Service

    const dispose = Effect.fn("InstanceHttpApi.dispose")(function* () {
      yield* markInstanceForDisposal(yield* InstanceState.context)
      return true
    })

    const getPath = Effect.fn("InstanceHttpApi.path")(function* () {
      const ctx = yield* InstanceState.context
      return {
        home: Global.Path.home,
        state: Global.Path.state,
        config: Global.Path.config,
        worktree: ctx.worktree,
        directory: ctx.directory,
      }
    })

    const promptEnhance = Effect.fn("InstanceHttpApi.promptEnhance")(function* (ctx: {
      payload: typeof PromptEnhancePayload.Type
    }) {
      return yield* prompt.enhance(ctx.payload).pipe(
        Effect.mapError((error) => {
          if (Provider.ModelNotFoundError.isInstance(error))
            return new PromptEnhanceModelNotFoundError({ model: ctx.payload.model, message: error.message })
          return new PromptEnhanceProviderError({
            model: ctx.payload.model,
            message: promptEnhanceErrorMessage(error),
          })
        }),
      )
    })

    const getVcs = Effect.fn("InstanceHttpApi.vcs")(function* () {
      const [branch, default_branch] = yield* Effect.all([vcs.branch(), vcs.defaultBranch()], {
        concurrency: "unbounded",
      })
      return { branch, default_branch }
    })

    const getVcsStatus = Effect.fn("InstanceHttpApi.vcsStatus")(function* () {
      return yield* vcs.status()
    })

    const getVcsDiff = Effect.fn("InstanceHttpApi.vcsDiff")(function* (ctx: {
      query: { mode: Vcs.Mode; context?: number }
    }) {
      return yield* vcs.diff(ctx.query.mode, { context: ctx.query.context })
    })

    const getVcsDiffRaw = Effect.fn("InstanceHttpApi.vcsDiffRaw")(function* () {
      return yield* vcs.diffRaw()
    })

    const applyVcs = Effect.fn("InstanceHttpApi.vcsApply")(function* (ctx: { payload: Vcs.ApplyInput }) {
      return yield* vcs.apply(ctx.payload).pipe(
        Effect.mapError(
          (error) =>
            new ApiVcsApplyError({
              name: "VcsApplyError",
              data: {
                message: error.message,
                reason: error.reason,
              },
            }),
        ),
      )
    })

    const getCommand = Effect.fn("InstanceHttpApi.command")(function* () {
      return yield* command.list()
    })

    const getAgent = Effect.fn("InstanceHttpApi.agent")(function* () {
      return yield* agent.list()
    })

    const getSkill = Effect.fn("InstanceHttpApi.skill")(function* () {
      return yield* skill.all()
    })

    const getLsp = Effect.fn("InstanceHttpApi.lsp")(function* () {
      return yield* lsp.status()
    })

    const getFormatter = Effect.fn("InstanceHttpApi.formatter")(function* () {
      return yield* format.status()
    })

    return handlers
      .handle("dispose", dispose)
      .handle("path", getPath)
      .handle("promptEnhance", promptEnhance)
      .handle("vcs", getVcs)
      .handle("vcsStatus", getVcsStatus)
      .handle("vcsDiff", getVcsDiff)
      .handle("vcsDiffRaw", getVcsDiffRaw)
      .handle("vcsApply", applyVcs)
      .handle("command", getCommand)
      .handle("agent", getAgent)
      .handle("skill", getSkill)
      .handle("lsp", getLsp)
      .handle("formatter", getFormatter)
  }),
)

function promptEnhanceErrorMessage(error: unknown) {
  if (error instanceof NamedError) {
    const data = error.toObject().data
    if (data && typeof data === "object" && "message" in data && typeof data.message === "string") return data.message
  }
  if (error instanceof Error) return error.message
  return String(error)
}
