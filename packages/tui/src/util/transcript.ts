import type { AssistantMessage, Part, Provider, UserMessage } from "@opencode-ai/sdk/v2"
import { Locale } from "./locale"
import * as Model from "./model"

export type TranscriptOptions = {
  thinking: boolean
  toolDetails: boolean
  assistantMetadata: boolean
  providers?: Provider[]
}

export type SessionInfo = {
  id: string
  title: string
  time: {
    created: number
    updated: number
  }
}

export type MessageWithParts = {
  info: UserMessage | AssistantMessage
  parts: Part[]
}

export type TranscriptPresentation = ReadonlyMap<string, string>

export type PromptEnhancementContext = {
  messages: MessageWithParts[]
  options: Pick<TranscriptOptions, "thinking" | "toolDetails" | "assistantMetadata" | "providers">
  presentation: TranscriptPresentation
}

export type PromptEnhancement = {
  model: () => string
  context?: () => PromptEnhancementContext
}

export function promptEnhancementModel(
  configured: string | undefined,
  current: { providerID: string; modelID: string } | undefined,
) {
  return configured ?? (current ? `${current.providerID}/${current.modelID}` : "google/gemini-3.8-flash")
}

export function formatTranscript(
  session: SessionInfo,
  messages: MessageWithParts[],
  options: TranscriptOptions,
): string {
  const providers = Model.index(options.providers)
  let transcript = `# ${session.title}\n\n`
  transcript += `**Session ID:** ${session.id}\n`
  transcript += `**Created:** ${new Date(session.time.created).toLocaleString()}\n`
  transcript += `**Updated:** ${new Date(session.time.updated).toLocaleString()}\n\n`
  transcript += `---\n\n`

  for (const msg of messages.toSorted(
    (a, b) => a.info.time.created - b.info.time.created || a.info.id.localeCompare(b.info.id),
  )) {
    transcript += formatMessage(msg.info, msg.parts, options, providers)
    transcript += `---\n\n`
  }

  return transcript
}

export function promptEnhancementContext(
  messages: MessageWithParts[],
  options: Pick<TranscriptOptions, "thinking" | "toolDetails" | "assistantMetadata" | "providers">,
  presentation?: TranscriptPresentation,
) {
  return messages
    .toSorted((a, b) => a.info.time.created - b.info.time.created || a.info.id.localeCompare(b.info.id))
    .map((message) =>
      formatMessage(
        message.info,
        message.parts.filter((part) => {
          if (part.type === "text") return !part.synthetic && !part.ignored
          if (presentation && (part.type === "reasoning" || part.type === "tool"))
            return presentation.has(part.id)
          if (part.type === "tool" && !options.toolDetails && part.state.status === "completed") return false
          return true
        }),
        options,
        undefined,
        presentation,
      ),
    )
    .join("")
    .split("\n")
    .slice(-100)
    .join("\n")
}

export function formatMessage(
  msg: UserMessage | AssistantMessage,
  parts: Part[],
  options: TranscriptOptions,
  providers?: Provider[] | ReadonlyMap<string, Provider>,
  presentation?: TranscriptPresentation,
): string {
  let result = ""

  if (msg.role === "user") {
    result += `## User\n\n`
  } else {
    result += formatAssistantHeader(msg, options.assistantMetadata, providers ?? options.providers)
  }

  for (const part of parts) {
    if (presentation && (part.type === "reasoning" || part.type === "tool")) {
      const rendered = presentation.get(part.id)
      if (rendered) result += `${rendered}\n\n`
      continue
    }
    result += formatPart(part, options)
  }

  return result
}

export function formatAssistantHeader(
  msg: AssistantMessage,
  includeMetadata: boolean,
  providers?: Provider[] | ReadonlyMap<string, Provider>,
): string {
  if (!includeMetadata) {
    return `## Assistant\n\n`
  }

  const duration =
    msg.time.completed && msg.time.created ? ((msg.time.completed - msg.time.created) / 1000).toFixed(1) + "s" : ""

  const modelName = Model.servedName(providers, msg.providerID, msg.modelID, msg.responseModelIDs)

  return `## Assistant (${Locale.titlecase(msg.agent)} · ${modelName}${duration ? ` · ${duration}` : ""})\n\n`
}

export function formatPart(part: Part, options: TranscriptOptions): string {
  if (part.type === "text" && !part.synthetic) {
    return `${part.text}\n\n`
  }

  if (part.type === "reasoning") {
    if (options.thinking) {
      return `_Thinking:_\n\n${part.text}\n\n`
    }
    return ""
  }

  if (part.type === "tool") {
    let result = `**Tool: ${part.tool}**\n`
    if (options.toolDetails && part.state.input) {
      result += `\n**Input:**\n\`\`\`json\n${JSON.stringify(part.state.input, null, 2)}\n\`\`\`\n`
    }
    if (options.toolDetails && part.state.status === "completed" && part.state.output) {
      result += `\n**Output:**\n\`\`\`\n${part.state.output}\n\`\`\`\n`
    }
    if (options.toolDetails && part.state.status === "error" && part.state.error) {
      result += `\n**Error:**\n\`\`\`\n${part.state.error}\n\`\`\`\n`
    }
    result += `\n`
    return result
  }

  return ""
}
