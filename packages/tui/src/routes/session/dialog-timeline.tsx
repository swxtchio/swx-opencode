import { createMemo, onMount } from "solid-js"
import { Option, Schema } from "effect"
import { useSync } from "../../context/sync"
import { DialogSelect, type DialogSelectOption } from "../../ui/dialog-select"
import type { Message, Part, TextPart } from "@opencode-ai/sdk/v2"
import { Locale } from "../../util/locale"
import { DialogMessage } from "./dialog-message"
import { useDialog } from "../../ui/dialog"
import type { PromptInfo } from "../../component/prompt/history"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { classify } from "@opencode-ai/schema/session-machine-message"

export function DialogTimeline(props: {
  sessionID: string
  onMove: (messageID: string) => void
  setPrompt?: (prompt: PromptInfo) => void
}) {
  const sync = useSync()
  const dialog = useDialog()

  onMount(() => {
    dialog.setSize("large")
  })

  const options = createMemo(() =>
    timelineOptions({
      sessionID: props.sessionID,
      messages: sync.data.message[props.sessionID] ?? [],
      parts: sync.data.part,
      setPrompt: props.setPrompt,
    }),
  )

  return <DialogSelect onMove={(option) => props.onMove(option.value)} title="Timeline" options={options()} />
}

export function timelineOptions(input: {
  sessionID: string
  messages: Message[]
  parts: Record<string, Part[]>
  setPrompt?: (prompt: PromptInfo) => void
}) {
  const result: DialogSelectOption<string>[] = []
  for (const message of input.messages) {
    if (message.role !== "user") continue
    const parts = input.parts[message.id] ?? []
    const groups = parts.flatMap((part) => {
      if (part.type !== "text") return []
      const group = Schema.decodeUnknownOption(SessionPromptQueue.Group)(part.metadata?.sessionQueue)
      return Option.isSome(group) ? [group.value] : []
    })
    if (
      !groups.length ||
      groups.some((group) => {
        if ((group.kind !== "human" && group.kind !== "individual") || !group.constituents.length) return true
        return group.constituents.some((item) => {
          const text = item.input.parts
            .flatMap((part) => (part.type === "text" && !part.synthetic && !part.ignored ? [part.text] : []))
            .join("")
          const appended = item.input.parts.some(
            (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey] === true,
          )
          const classified = classify(text) !== undefined
          return appended || classified || (item.input.origin !== undefined && item.input.origin !== "tui-composer")
        })
      })
    )
      continue
    const text = parts.filter((part): part is TextPart => part.type === "text" && !part.synthetic && !part.ignored)
    if (!text.length) continue
    result.push({
      title: text
        .map((part) => part.text)
        .join("")
        .replace(/\n/g, " "),
      value: message.id,
      footer: Locale.time(message.time.created),
      onSelect: (dialog) => {
        dialog.replace(() => (
          <DialogMessage messageID={message.id} sessionID={input.sessionID} setPrompt={input.setPrompt} />
        ))
      },
    })
  }
  return result.reverse()
}
