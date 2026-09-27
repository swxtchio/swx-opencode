import { createMemo } from "solid-js"
import type { AssistantMessage, Message, Provider } from "@opencode-ai/sdk/v2"
import { servedAcrossSession, servedAcrossTurn, servedName, type SessionStepMessage } from "../../util/model"

export function AssistantModelLabel(props: {
  message: AssistantMessage
  providers: Provider[] | ReadonlyMap<string, Provider>
  messages: readonly SessionStepMessage[]
  turnMessages: readonly Message[]
}) {
  const model = createMemo(() =>
    servedName(
      props.providers,
      props.message.providerID,
      props.message.modelID,
      servedAcrossTurn(props.turnMessages, props.message),
      props.message.providerID === "llmrouter" && props.message.modelID === "auto"
        ? servedAcrossSession(props.messages, props.message.id)
        : undefined,
    ),
  )

  return <>{model()}</>
}
