import path from "path"
import { onMount } from "solid-js"
import { Option, Schema } from "effect"
import { createStore, produce, unwrap } from "solid-js/store"
import { createHash } from "node:crypto"
import type { AgentPart, Config, FilePart, Message, Part, TextPart } from "@opencode-ai/sdk/v2"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { classify } from "@opencode-ai/schema/session-machine-message"
import { createSimpleContext } from "../context/helper"
import { useTuiPaths } from "../context/runtime"
import { appendText, readText, writeJsonAtomic, writeText } from "../util/persistence"
import { expandTrackedPastedText, stripPromptPartIDs } from "./part"

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
type PromptHistoryRow = { key: number; entry: PromptHistoryRecord; raw?: string }
type ParsedPromptHistoryRow = { entry: PromptHistoryRecord; raw?: string }
type PromptHistoryHydrationEntry = { sessionID: string; signatures: readonly string[] }
const PromptHistoryHydration = Schema.Struct({
  version: Schema.Literal(2),
  sessions: Schema.Array(Schema.Struct({ sessionID: Schema.String, signatures: Schema.Array(Schema.String) })),
})

type HistoryMessage = { info: Message; parts: Part[] }

function parsePromptHistoryRows(text: string) {
  return text
    .split("\n")
    .filter(Boolean)
    .flatMap((raw): ParsedPromptHistoryRow[] => {
      try {
        const entry = JSON.parse(raw) as Partial<PromptHistoryRecord> | null
        if (!entry || typeof entry.input !== "string" || !Array.isArray(entry.parts)) return []
        if ("sessionID" in entry && (typeof entry.sessionID !== "string" || !entry.sessionID)) return []
        return [{ entry: entry as PromptHistoryRecord, ...(entry.sessionID === undefined ? { raw } : {}) }]
      } catch {
        return []
      }
    })
    .slice(-MAX_HISTORY_ENTRIES)
}

export function parsePromptHistory(text: string) {
  return parsePromptHistoryRows(text).map((row) => row.entry)
}

function parsePromptHistoryHydration(text: string) {
  const json = Schema.decodeUnknownOption(Schema.UnknownFromJsonString)(text)
  if (Option.isNone(json)) return []
  const hydration = Schema.decodeUnknownOption(PromptHistoryHydration)(json.value)
  if (Option.isNone(hydration)) return []
  return [
    ...new Map(
      hydration.value.sessions
        .filter((session) => session.sessionID)
        .map((session) => [session.sessionID, session] as const),
    ).values(),
  ]
}

function retainedPromptHistoryHydration(rows: PromptHistoryRow[], sessionID: string): PromptHistoryHydrationEntry {
  return {
    sessionID,
    signatures: rows.flatMap((row) =>
      row.entry.sessionID === sessionID && row.entry.origin === "tui-composer"
        ? [promptHistoryFingerprint(row.entry)]
        : [],
    ),
  }
}

function updatePromptHistoryHydration(
  sessions: PromptHistoryHydrationEntry[],
  rows: PromptHistoryRow[],
  sessionID: string,
) {
  return [...sessions.filter((session) => session.sessionID !== sessionID), retainedPromptHistoryHydration(rows, sessionID)]
}

function validatePromptHistoryHydration(sessions: PromptHistoryHydrationEntry[], rows: PromptHistoryRow[]) {
  return sessions.filter((session) => {
    const available = new Map<string, number>()
    for (const row of rows) {
      if (row.entry.sessionID !== session.sessionID || row.entry.origin !== "tui-composer") continue
      const signature = promptHistoryFingerprint(row.entry)
      available.set(signature, (available.get(signature) ?? 0) + 1)
    }
    return session.signatures.every((signature) => {
      const count = available.get(signature) ?? 0
      if (!count) return false
      available.set(signature, count - 1)
      return true
    })
  })
}

function promptHistoryFingerprint(entry: PromptHistoryRecord) {
  return createHash("sha256")
    .update(JSON.stringify([entry.mode, entry.machineAppended, promptSignature(promptText(entry), entry.parts)]))
    .digest("hex")
}

function serializePromptHistoryRows(rows: Pick<PromptHistoryRow, "entry" | "raw">[]) {
  return rows.map((row) => row.raw ?? JSON.stringify(row.entry)).join("\n") + "\n"
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
    const hydrationPath = path.join(paths.state, "prompt-history-hydrated.json")
    let nextKey = 0
    let persistence = Promise.resolve()
    function enqueue(write: () => Promise<void>) {
      const next = persistence.then(write)
      persistence = next.catch(() => {})
      return next
    }

    const [store, setStore] = createStore({
      index: 0,
      ready: false,
      history: [] as PromptHistoryRow[],
      hydrated: [] as PromptHistoryHydrationEntry[],
    })

    onMount(async () => {
      const [historyText, hydrationText] = await Promise.all([
        readText(historyPath).catch(() => ""),
        readText(hydrationPath).catch(() => ""),
      ])
      const parsed = parsePromptHistoryRows(historyText)
      const loaded = parsed.map((row) => ({ ...row, key: nextKey++ }))
      const pending = store.history.map((row) => ({ key: nextKey++, entry: row.entry }))
      const history = [...loaded, ...pending].slice(-MAX_HISTORY_ENTRIES)
      const previousHydration = parsePromptHistoryHydration(hydrationText)
      const validated = validatePromptHistoryHydration(previousHydration, history)
      const hydrated = [...new Set(pending.flatMap((row) => (row.entry.sessionID ? [row.entry.sessionID] : [])))].reduce(
        (result, sessionID) =>
          result.some((session) => session.sessionID === sessionID)
            ? updatePromptHistoryHydration(result, history, sessionID)
            : result,
        validated,
      )
      setStore("history", history)
      setStore("hydrated", hydrated)

      // Keep ownerless rows byte-for-byte until ordinary retention evicts them.
      if (history.length > 0 || JSON.stringify(previousHydration) !== JSON.stringify(hydrated))
        enqueue(async () => {
          if (JSON.stringify(previousHydration) !== JSON.stringify(hydrated))
            await writeJsonAtomic(hydrationPath, { version: 2, sessions: hydrated })
          if (history.length > 0) await writeText(historyPath, serializePromptHistoryRows(history))
        }).catch(() => {})
      setStore("ready", true)
    })

    const list = (sessionID: string | undefined, markers?: Config["machine_message_markers"]) => {
      if (!sessionID) return []
      return store.history.flatMap((row) => {
        const entry = row.entry.sessionID === sessionID && row.entry.origin === "tui-composer" ? row.entry : undefined
        if (!entry || isMachinePrompt(entry, markers)) return []
        return [entry as PromptHistoryEntry]
      })
    }

    return {
      needsRecovery(sessionID: string | undefined) {
        return !!sessionID && store.ready && !store.hydrated.some((session) => session.sessionID === sessionID)
      },
      list,
      async recover(sessionID: string, messages: HistoryMessage[], markers?: Config["machine_message_markers"]) {
        const prompts = messages.flatMap((message) => {
          if (message.info.sessionID !== sessionID || message.info.role !== "user") return []
          return composerMessagePrompts(message.parts, markers)
        })
        const history = mergePromptHistory(store.history, sessionID, prompts, () => nextKey++)
        const hydrated = validatePromptHistoryHydration(
          updatePromptHistoryHydration(store.hydrated, history, sessionID),
          history,
        )
        setStore("history", history)
        setStore("index", 0)
        const persisted = await enqueue(() =>
          writeJsonAtomic(hydrationPath, { version: 2, sessions: hydrated }).then(() =>
            writeText(historyPath, serializePromptHistoryRows(history)),
          ),
        ).then(
          () => true,
          () => false,
        )
        if (persisted) setStore("hydrated", hydrated)
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

        if (!store.ready) return
        enqueue(async () => {
          const current = store.hydrated
          const updated = current.some((session) => session.sessionID === sessionID)
            ? updatePromptHistoryHydration(current, store.history, sessionID)
            : current
          const hydrated = validatePromptHistoryHydration(updated, store.history)
          const hydrationChanged = JSON.stringify(current) !== JSON.stringify(hydrated)
          if (hydrationChanged) await writeJsonAtomic(hydrationPath, { version: 2, sessions: hydrated })
          if (trimmed) await writeText(historyPath, serializePromptHistoryRows(store.history))
          else await appendText(historyPath, JSON.stringify(entry) + "\n")
          if (hydrationChanged) setStore("hydrated", hydrated)
        }).catch(() => {})
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

function composerMessagePrompts(parts: Part[], markers: Config["machine_message_markers"] | undefined) {
  // Machine marks, not composer origin, decide admission; unmarked pre-#272 automation may surface.
  const groups = [
    ...new Map(
      parts.flatMap((part) => {
        if (part.type !== "text") return []
        const group = Schema.decodeUnknownOption(SessionPromptQueue.Group)(part.metadata?.sessionQueue)
        return Option.isSome(group) ? [[JSON.stringify(group.value), group.value] as const] : []
      }),
    ).values(),
  ]
  if (groups.length)
    return groups.flatMap((group) => {
      if ((group.kind !== "human" && group.kind !== "individual") || !group.constituents.length) return []
      return group.constituents.flatMap((item) => {
        const prompt = queuePrompt(item.input)
        return isHumanPrompt(prompt, markers) ? [prompt] : []
      })
    })

  if (parts.some((part) => part.type === "text" && part.metadata?.sessionQueue !== undefined)) return []
  const prompt = messagePrompt(parts)
  return isHumanPrompt(prompt, markers) ? [prompt] : []
}

function queuePrompt(input: SessionPromptQueue.QueuedInput): PromptInfo {
  const machineAppended = input.parts.some(
    (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey] === true,
  )
  return {
    input: input.parts
      .flatMap((part) => (part.type === "text" && !part.synthetic && !part.ignored ? [part.text] : []))
      .join(""),
    parts: input.parts.flatMap((part): PromptInfo["parts"] => {
      if (part.type === "file")
        return [
          {
            type: "file",
            mime: part.mime,
            url: part.url,
            ...(part.filename ? { filename: part.filename } : {}),
            ...(part.source ? { source: part.source } : {}),
          },
        ]
      if (part.type === "agent")
        return [{ type: "agent", name: part.name, ...(part.source ? { source: part.source } : {}) }]
      return []
    }),
    ...(machineAppended ? { machineAppended: true as const } : {}),
  }
}

function messagePrompt(parts: Part[]): PromptInfo {
  const machineAppended = parts.some(
    (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey] === true,
  )
  return {
    input: parts
      .flatMap((part) => (part.type === "text" && !part.synthetic && !part.ignored ? [part.text] : []))
      .join(""),
    parts: parts.flatMap((part): PromptInfo["parts"] =>
      part.type === "file" || part.type === "agent"
        ? [stripPromptPartIDs(part) as unknown as PromptInfo["parts"][number]]
        : [],
    ),
    ...(machineAppended ? { machineAppended: true as const } : {}),
  }
}

function isHumanPrompt(prompt: PromptInfo, markers?: Config["machine_message_markers"]) {
  return (prompt.input.trim().length > 0 || prompt.parts.length > 0) && !isMachinePrompt(prompt, markers)
}

function mergePromptHistory(
  rows: PromptHistoryRow[],
  sessionID: string,
  prompts: PromptInfo[],
  nextKey: () => number,
) {
  const remaining = prompts.reduce((result, prompt) => {
    const signature = promptSignature(promptText(prompt), prompt.parts)
    result.set(signature, (result.get(signature) ?? 0) + 1)
    return result
  }, new Map<string, number>())
  const reusable = new Map<string, PromptHistoryRow[]>()
  const matched = new Set<number>()
  for (const row of rows) {
    const owned = row.entry.sessionID === sessionID && row.entry.origin === "tui-composer"
    const legacy = row.entry.sessionID === undefined &&
      (row.entry.origin === undefined || row.entry.origin === "tui-composer") &&
      !isMachinePrompt(row.entry)
    if (!owned && !legacy) continue
    const signature = promptSignature(promptText(row.entry), row.entry.parts)
    const count = remaining.get(signature) ?? 0
    if (!count) continue
    remaining.set(signature, count - 1)
    matched.add(row.key)
    reusable.set(signature, [...(reusable.get(signature) ?? []), row])
  }

  const recovered = prompts.map((prompt): PromptHistoryRow => {
    const signature = promptSignature(promptText(prompt), prompt.parts)
    const cached = reusable.get(signature)?.shift()
    if (cached)
      return {
        key: cached.key,
        entry: { ...cached.entry, sessionID, origin: "tui-composer" },
      }
    return {
      key: nextKey(),
      entry: { ...structuredClone(prompt), sessionID, origin: "tui-composer" },
    }
  })
  const leftover = rows.filter((row) => row.entry.sessionID === sessionID && !matched.has(row.key))
  const removed = new Set([...matched, ...rows.flatMap((row) => (row.entry.sessionID === sessionID ? [row.key] : []))])
  const first = rows.findIndex((row) => removed.has(row.key))
  const at = first === -1 ? rows.length : rows.slice(0, first).filter((row) => !removed.has(row.key)).length
  const rest = rows.filter((row) => !removed.has(row.key))
  return [...rest.slice(0, at), ...recovered, ...leftover, ...rest.slice(at)].slice(-MAX_HISTORY_ENTRIES)
}

function promptSignature(text: string, parts: readonly unknown[]) {
  return JSON.stringify([text, attachmentSignatures(parts)])
}

function attachmentSignatures(parts: readonly unknown[]) {
  return parts.flatMap((part): unknown[] => {
    if (!part || typeof part !== "object" || !("type" in part)) return []
    const value = part as Record<string, unknown>
    if (value.type === "file") return [{ type: value.type, mime: value.mime, filename: value.filename, url: value.url }]
    if (value.type === "agent") {
      const source = value.source && typeof value.source === "object" ? (value.source as Record<string, unknown>) : undefined
      return [{ type: value.type, name: value.name, source: source?.value }]
    }
    return []
  })
}
