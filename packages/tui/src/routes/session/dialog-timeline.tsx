import { createMemo, onMount } from "solid-js"
import { Option, Schema } from "effect"
import { useSync } from "../../context/sync"
import { DialogSelect, type DialogSelectOption } from "../../ui/dialog-select"
import type { Config, Message, Part, TextPart } from "@opencode-ai/sdk/v2"
import { Locale } from "../../util/locale"
import { useDialog } from "../../ui/dialog"
import type { PromptHistoryEntry } from "../../prompt/history"
import { expandTrackedPastedText } from "../../prompt/part"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { classify } from "@opencode-ai/schema/session-machine-message"

export function DialogTimeline(props: {
  sessionID: string
  onMove: (messageID: string) => void
  appendPrompt: (text: string) => void
  history: readonly PromptHistoryEntry[]
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
      markers: sync.data.config.machine_message_markers,
      history: props.history,
    }),
  )

  return (
    <DialogSelect
      onMove={(option) => option.value.messageID && props.onMove(option.value.messageID)}
      onSelect={(option) => {
        dialog.clear()
        props.appendPrompt(option.value.text)
      }}
      title="Timeline"
      options={options()}
    />
  )
}

export function timelineOptions(input: {
  sessionID: string
  messages: Message[]
  parts: Record<string, Part[]>
  markers?: Config["machine_message_markers"]
  history?: readonly PromptHistoryEntry[]
}) {
  const result: DialogSelectOption<{ text: string; messageID?: string }>[] = []
  for (const message of input.messages) {
    if (message.role !== "user" || message.sessionID !== input.sessionID) continue
    const parts = input.parts[message.id] ?? []
    const groups = parts.flatMap((part) => {
      if (part.type !== "text") return []
      const group = Schema.decodeUnknownOption(SessionPromptQueue.Group)(part.metadata?.sessionQueue)
      return Option.isSome(group) ? [group.value] : []
    })
    const visibleText = parts.filter(
      (part): part is TextPart => part.type === "text" && !part.synthetic && !part.ignored,
    )
    const machineAppended = parts.some(
      (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey] === true,
    )
    const hasDirectComposerTransport = parts.some(
      (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiComposerOriginMetadataKey] === true,
    )
    // Transport provenance is accepted only after append and machine markers have been checked.
    const machineClassified = (value: string) => classify(value, input.markers) !== undefined
    const unsearchable = groups.length
      ? groups.some((group) => {
          if ((group.kind !== "human" && group.kind !== "individual") || !group.constituents.length) return true
          return group.constituents.some((item) => {
            const text = item.input.parts
              .flatMap((part) => (part.type === "text" && !part.synthetic && !part.ignored ? [part.text] : []))
              .join("")
            const appended = item.input.parts.some(
              (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey] === true,
            )
            const hasComposerTransport =
              item.input.origin === "tui-composer" ||
              item.input.parts.some(
                (part) =>
                  part.type === "text" && part.metadata?.[SessionPromptQueue.TuiComposerOriginMetadataKey] === true,
              )
            return appended || machineClassified(text) || !hasComposerTransport
          })
        })
      : machineAppended ||
        machineClassified(visibleText.map((part) => part.text).join("")) ||
        !hasDirectComposerTransport
    if (unsearchable || !visibleText.length) continue
    result.push({
      title: visibleText
        .map((part) => part.text)
        .join("")
        .replace(/\n/g, " "),
      value: { messageID: message.id, text: visibleText.map((part) => part.text).join("") },
      footer: Locale.time(message.time.created),
    })
  }
  for (const entry of input.history ?? []) {
    if (entry.sessionID !== input.sessionID || entry.origin !== "tui-composer" || entry.machineAppended) continue
    const text = expandTrackedPastedText(
      entry.input,
      entry.parts.flatMap((part) =>
        part.type === "text" && part.source?.text
          ? [{ start: part.source.text.start, end: part.source.text.end, text: part.text }]
          : [],
      ),
    )
    if (!text.trim() || classify(text, input.markers)) continue
    if (
      entry.parts.some((part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey])
    )
      continue
    if (result.some((option) => option.value.text === text)) continue
    result.push({ title: text.replace(/\n/g, " "), value: { text } })
  }
  return result.reverse()
}
