import path from "path"
import { onMount } from "solid-js"
import { Option, Schema } from "effect"
import { createStore, produce, unwrap } from "solid-js/store"
import type { AgentPart, Config, FilePart, Message, Part, TextPart } from "@opencode-ai/sdk/v2"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { classify } from "@opencode-ai/schema/session-machine-message"
import { createSimpleContext } from "../context/helper"
import { useTuiPaths } from "../context/runtime"
import { appendText, readText, writeText } from "../util/persistence"
import { expandTrackedPastedText } from "./part"

export type PromptInfo = {
  input: string
  mode?: "normal" | "shell"
  machineAppended?: true
  parts: (
    | Omit<FilePart, "id" | "messageID" | "sessionID">
    | Omit<AgentPart, "id" | "messageID" | "sessionID">
    | (Omit<TextPart, "id" | "messageID" | "sessionID"> & {
        source?: {
          text: {
            start: number
            end: number
            value: string
          }
        }
      })
  )[]
}

export const MAX_HISTORY_ENTRIES = 50

export type PromptHistoryEntry = PromptInfo & { sessionID: string; origin: "tui-composer" }
type PromptHistoryRecord = PromptInfo & { sessionID?: string; origin?: string }

type HistoryMessage = { info: Message; parts: Part[] }

export function parsePromptHistory(text: string) {
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      try {
        const entry = JSON.parse(line) as Partial<PromptHistoryRecord> | null
        if (!entry || typeof entry.input !== "string" || !Array.isArray(entry.parts)) return
        if ("sessionID" in entry && (typeof entry.sessionID !== "string" || !entry.sessionID)) return
        return entry as PromptHistoryRecord
      } catch {
        return undefined
      }
    })
    .filter((line): line is PromptHistoryRecord => line !== undefined)
    .slice(-MAX_HISTORY_ENTRIES)
}

export function isDuplicateEntry(previous: PromptInfo | undefined, next: PromptInfo): boolean {
  if (!previous) return false
  return JSON.stringify(previous) === JSON.stringify(next)
}

export const { use: usePromptHistory, provider: PromptHistoryProvider } = createSimpleContext({
  name: "PromptHistory",
  init: () => {
    const paths = useTuiPaths()
    const historyPath = path.join(paths.state, "prompt-history.jsonl")
    let nextKey = 0
    const [store, setStore] = createStore({
      index: 0,
      history: [] as { key: number; entry: PromptHistoryRecord }[],
      recovered: {} as Record<string, Record<number, PromptHistoryEntry>>,
    })

    onMount(async () => {
      const lines = parsePromptHistory(await readText(historyPath).catch(() => ""))
      setStore(
        "history",
        lines.map((entry) => ({ key: nextKey++, entry })),
      )

      // Keep older unowned rows available for transcript-based attribution.
      if (lines.length > 0)
        writeText(historyPath, lines.map((line) => JSON.stringify(line)).join("\n") + "\n").catch(() => {})
    })

    const list = (sessionID: string | undefined, markers?: Config["machine_message_markers"]) => {
      if (!sessionID) return []
      return store.history.flatMap((row) => {
        const entry =
          row.entry.sessionID === sessionID && row.entry.origin === "tui-composer"
            ? row.entry
            : row.entry.sessionID === undefined
              ? store.recovered[sessionID]?.[row.key]
              : undefined
        if (!entry || isMachinePrompt(entry, markers)) return []
        return [{ ...entry, sessionID, origin: "tui-composer" as const }]
      })
    }

    return {
      hasLegacy() {
        return store.history.some((row) => row.entry.sessionID === undefined)
      },
      list,
      recover(sessionID: string, messages: HistoryMessage[], markers?: Config["machine_message_markers"]) {
        // Without fork prompt metadata, use standard user messages for stock-server compatibility.
        const stock = !messages.some((message) =>
          message.parts.some(
            (part) =>
              part.type === "text" &&
              (part.metadata?.sessionQueue !== undefined ||
                part.metadata?.[SessionPromptQueue.TuiComposerOriginMetadataKey] === true ||
                part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey] === true),
          ),
        )
        const signatures = messages
          .flatMap((message) => {
            if (message.info.sessionID !== sessionID || message.info.role !== "user") return []
            return composerMessageSignatures(message.parts, markers, stock)
          })
          .reduce((result, signature) => {
            result.set(signature, (result.get(signature) ?? 0) + 1)
            return result
          }, new Map<string, number>())
        const recovered = Object.fromEntries(
          store.history.flatMap((row) => {
            if (row.entry.sessionID !== undefined || isMachinePrompt(row.entry, markers)) return []
            if (row.entry.origin !== undefined && row.entry.origin !== "tui-composer") return []
            const signature = promptSignature(promptText(row.entry), row.entry.parts)
            const count = signatures.get(signature) ?? 0
            if (!count) return []
            signatures.set(signature, count - 1)
            return [[row.key, { ...row.entry, sessionID, origin: "tui-composer" as const }]]
          }),
        ) as Record<number, PromptHistoryEntry>
        setStore("recovered", sessionID, recovered)
      },
      reset() {
        setStore("index", 0)
      },
      move(
        sessionID: string | undefined,
        direction: 1 | -1,
        input: string,
        markers?: Config["machine_message_markers"],
      ): PromptInfo | undefined {
        const history = list(sessionID, markers)
        if (!history.length) return undefined
        const current = history.at(store.index)
        if (!current) return undefined
        if (current.input !== input && input.length) return
        setStore(
          produce((draft) => {
            const next = store.index + direction
            if (Math.abs(next) > history.length) return
            if (next > 0) return
            draft.index = next
          }),
        )
        if (store.index === 0) return { input: "", parts: [] }
        return history.at(store.index)
      },
      append(sessionID: string | undefined, item: PromptInfo, markers?: Config["machine_message_markers"]) {
        if (!sessionID || isMachinePrompt(item, markers)) return
        const entry: PromptHistoryEntry = { ...structuredClone(unwrap(item)), sessionID, origin: "tui-composer" }
        if (isDuplicateEntry(store.history.at(-1)?.entry, entry)) {
          setStore("index", 0)
          return
        }
        let trimmed = false
        setStore(
          produce((draft) => {
            draft.history.push({ key: nextKey++, entry })
            if (draft.history.length > MAX_HISTORY_ENTRIES) {
              draft.history = draft.history.slice(-MAX_HISTORY_ENTRIES)
              trimmed = true
            }
            draft.index = 0
          }),
        )

        if (trimmed) {
          writeText(historyPath, store.history.map((row) => JSON.stringify(row.entry)).join("\n") + "\n").catch(() => {})
          return
        }
        appendText(historyPath, JSON.stringify(entry) + "\n").catch(() => {})
      },
    }
  },
})

function isMachinePrompt(item: PromptInfo, markers?: Config["machine_message_markers"]) {
  return (
    item.machineAppended === true ||
    classify(promptText(item), markers) !== undefined ||
    item.parts.some(
      (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey] === true,
    )
  )
}

function promptText(item: PromptInfo) {
  return expandTrackedPastedText(
    item.input,
    item.parts.flatMap((part) =>
      part.type === "text" && part.source?.text
        ? [{ start: part.source.text.start, end: part.source.text.end, text: part.text }]
        : [],
    ),
  )
}

function composerMessageSignatures(parts: Part[], markers: Config["machine_message_markers"] | undefined, stock: boolean) {
  const groups = parts.flatMap((part) => {
    if (part.type !== "text") return []
    const group = Schema.decodeUnknownOption(SessionPromptQueue.Group)(part.metadata?.sessionQueue)
    return Option.isSome(group) ? [group.value] : []
  })
  if (groups.length)
    return groups.flatMap((group) => {
      if ((group.kind !== "human" && group.kind !== "individual") || !group.constituents.length) return []
      return group.constituents.flatMap((item) => {
        const text = item.input.parts
          .flatMap((part) => (part.type === "text" && !part.synthetic && !part.ignored ? [part.text] : []))
          .join("")
        const appended = item.input.parts.some(
          (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey] === true,
        )
        const composer =
          item.input.origin === "tui-composer" ||
          item.input.parts.some(
            (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiComposerOriginMetadataKey] === true,
          )
        if (!text.trim() || appended || classify(text, markers) || !composer) return []
        return [promptSignature(text, item.input.parts)]
      })
    })

  const text = parts
    .flatMap((part) => (part.type === "text" && !part.synthetic && !part.ignored ? [part.text] : []))
    .join("")
  if (!text.trim() || classify(text, markers)) return []
  if (
    parts.some(
      (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey] === true,
    )
  )
    return []
  const composer = parts.some(
    (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiComposerOriginMetadataKey] === true,
  )
  const queueMetadata = parts.some((part) => part.type === "text" && part.metadata?.sessionQueue !== undefined)
  if (!stock && (queueMetadata || !composer)) return []
  return [promptSignature(text, parts)]
}

function promptSignature(text: string, parts: readonly unknown[]) {
  return JSON.stringify([text, attachmentSignatures(parts)])
}

function attachmentSignatures(parts: readonly unknown[]) {
  return parts.flatMap((part): unknown[] => {
    if (!part || typeof part !== "object" || !("type" in part)) return []
    const value = part as Record<string, unknown>
    if (value.type === "file") return [{ type: value.type, mime: value.mime, filename: value.filename, url: value.url }]
    if (value.type === "agent") return [{ type: value.type, name: value.name, source: value.source }]
    return []
  })
}
