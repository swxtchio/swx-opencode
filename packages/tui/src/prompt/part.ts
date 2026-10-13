import type { SessionPromptQueueItem } from "@opencode-ai/sdk/v2"
import { displaySlice, promptOffsetWidth } from "./display"
import type { PromptInfo } from "./history"

export function stripPromptPartIDs<Part extends { id: string; messageID: string; sessionID: string }>(part: Part) {
  const { id: _id, messageID: _messageID, sessionID: _sessionID, ...rest } = part
  return rest
}

export function expandPastedTextPlaceholders(text: string, parts: readonly unknown[]) {
  return parts.reduce<string>((result, part) => {
    if (!isPastedTextPart(part)) return result
    return result.replace(part.source.text.value, part.text)
  }, text)
}

function isPastedTextPart(part: unknown): part is { type: "text"; text: string; source: { text: { value: string } } } {
  if (!part || typeof part !== "object" || !("type" in part) || part.type !== "text") return false
  if (!("text" in part) || typeof part.text !== "string" || !("source" in part)) return false
  const source = part.source
  if (!source || typeof source !== "object" || !("text" in source)) return false
  const text = source.text
  return Boolean(text && typeof text === "object" && "value" in text && typeof text.value === "string")
}

export function expandTrackedPastedText(text: string, ranges: { start: number; end: number; text: string }[]) {
  return ranges
    .slice()
    .sort((a, b) => b.start - a.start)
    .reduce((result, part) => displaySlice(result, 0, part.start) + part.text + displaySlice(result, part.end), text)
}

// What a withdrawn queue item returns to the composer (swxtchio/swx-opencode#249):
// its typed text with the file and agent mentions the text still holds, never
// the synthetic context a client added when it sent the prompt.
export function recalledPrompt(item: SessionPromptQueueItem): PromptInfo {
  const input = item.input.parts
    .flatMap((part) => (part.type === "text" && !part.synthetic ? [part.text] : []))
    .join("\n")
  return {
    input,
    parts: item.input.parts.flatMap((part): PromptInfo["parts"] => {
      if (part.type === "file") {
        const text = part.source && mention(input, part.source.text)
        return part.source && text ? [{ ...withoutID(part), source: { ...part.source, text } }] : []
      }
      if (part.type === "agent") {
        const source = part.source && mention(input, part.source)
        return source ? [{ ...withoutID(part), source }] : []
      }
      return []
    }),
  }
}

// A mention keeps its place while the text holds it there. Pasted text the
// prompt expanded before it moves it, to where the text first holds it.
function mention<At extends { value: string; start: number; end: number }>(text: string, at: At) {
  if (!at.value) return undefined
  if (displaySlice(text, at.start, at.end) === at.value) return at
  const index = text.indexOf(at.value)
  if (index < 0) return undefined
  const start = promptOffsetWidth(text.slice(0, index))
  return { ...at, start, end: start + promptOffsetWidth(at.value) }
}

function withoutID<Part extends { id?: string }>(part: Part) {
  const { id: _id, ...rest } = part
  return rest
}
