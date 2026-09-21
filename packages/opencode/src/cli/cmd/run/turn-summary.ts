import * as Locale from "@/util/locale"
import type { SessionMessages } from "./session.shared"
import type { RunProvider, StreamCommit } from "./types"
import { servedAcrossTurn, servedModelLabel } from "./variant.shared"

export function turnSummaryCommit(input: {
  agent: string
  model: string
  duration: string
  messageID?: string
}): StreamCommit {
  return {
    kind: "system",
    text: `▣ ${input.agent} · ${input.model} · ${input.duration}`,
    phase: "final",
    source: "system",
    summary: {
      agent: input.agent,
      model: input.model,
      duration: input.duration,
    },
    messageID: input.messageID,
  }
}

export function messageTurnSummaryCommit(
  message: SessionMessages[number],
  providers?: RunProvider[],
  all?: SessionMessages,
): StreamCommit | undefined {
  const info = message.info
  if (info.role !== "assistant") {
    return
  }

  const completed = info.time.completed
  if (typeof completed !== "number" || completed <= info.time.created) {
    return
  }

  // One prompt produces one assistant message per step and the summary renders
  // on the last of them, so gather the whole turn rather than that one message.
  const model = servedModelLabel(providers, info.providerID, info.modelID, servedAcrossTurn(all, info))

  return turnSummaryCommit({
    agent: Locale.titlecase(info.agent),
    model,
    duration: Locale.duration(completed - info.time.created),
    messageID: info.id,
  })
}
