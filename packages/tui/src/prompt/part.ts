import type { SessionPromptQueueItem } from "@opencode-ai/sdk/v2"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
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

// Where a composer offset lands once tracked pasted text is expanded, so a
// mention sent with the expanded text still marks its own text there.
export function expandedOffset(offset: number, ranges: readonly { start: number; end: number; text: string }[]) {
  return ranges.reduce(
    (at, range) => (range.end <= offset ? at + promptOffsetWidth(range.text) - (range.end - range.start) : at),
    offset,
  )
}

// What a withdrawn queue item returns to the composer (swxtchio/swx-opencode#249):
// its typed text and every file and agent part, never the synthetic context a
// client added when it sent the prompt. A part the text does not mention, such
// as an attachment sent without one, gets a mention at the end of the text, so
// the composer keeps it as it keeps a pasted attachment.
export function recalledPrompt(item: SessionPromptQueueItem): PromptInfo {
  const typed = item.input.parts
    .flatMap((part) => (part.type === "text" && !part.synthetic ? [part.text] : []))
    .join("\n")
  const machineAppended =
    item.input.origin !== "tui-composer" ||
    item.input.parts.some(
      (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey] === true,
    )
  return item.input.parts.reduce<PromptInfo>(
    (prompt, part) => {
      if (part.type !== "file" && part.type !== "agent") return prompt
      const claimed = prompt.parts.flatMap((kept) => {
        const at = kept.type === "agent" ? kept.source : kept.type === "file" ? kept.source?.text : undefined
        return at ? [at] : []
      })
      const sent = part.type === "agent" ? part.source : part.source?.text
      const found = sent && mention(prompt.input, sent, claimed)
      const value = sent?.value || (part.type === "agent" ? `@${part.name}` : attachmentLabel(part, prompt.parts))
      const input = found ? prompt.input : appended(prompt.input, value)
      const start = found ? found.start : promptOffsetWidth(input) - promptOffsetWidth(value)
      const at = found ?? { value, start, end: start + promptOffsetWidth(value) }
      if (part.type === "agent") return { input, parts: [...prompt.parts, { ...withoutID(part), source: at }] }
      const source = part.source
        ? { ...part.source, text: at }
        : { type: "file" as const, path: part.filename ?? "", text: at }
      return { input, parts: [...prompt.parts, { ...withoutID(part), source }] }
    },
    {
      input: typed,
      parts: [],
      ...(machineAppended ? { machineAppended: true } : {}),
    },
  )
}

// Where the text holds a mention: at its own place while the text holds it
// there, otherwise at the nearest copy of it no other part has taken.
function mention<At extends { value: string; start: number; end: number }>(
  text: string,
  at: At,
  claimed: readonly { start: number; end: number }[],
) {
  if (!at.value) return undefined
  const free = (start: number, end: number) => claimed.every((other) => end <= other.start || start >= other.end)
  if (displaySlice(text, at.start, at.end) === at.value && free(at.start, at.end)) return at
  const width = promptOffsetWidth(at.value)
  return [...text.matchAll(new RegExp(RegExp.escape(at.value), "g"))]
    .map((match) => promptOffsetWidth(text.slice(0, match.index)))
    .filter((start) => free(start, start + width))
    .toSorted((a, b) => Math.abs(a - at.start) - Math.abs(b - at.start))
    .map((start) => ({ ...at, start, end: start + width }))
    .at(0)
}

function appended(text: string, value: string) {
  if (!text || /\s$/.test(text)) return text + value
  return `${text} ${value}`
}

// The name the composer gives an attachment, as pasting one does.
function attachmentLabel(part: { mime: string; filename?: string }, kept: PromptInfo["parts"]) {
  const pdf = part.mime === "application/pdf"
  if (!pdf && !part.mime.startsWith("image/")) return `[File ${part.filename ?? "attachment"}]`
  const count = kept.filter(
    (other) => other.type === "file" && (pdf ? other.mime === part.mime : other.mime.startsWith("image/")),
  ).length
  return pdf ? `[PDF ${count + 1}]` : `[Image ${count + 1}]`
}

function withoutID<Part extends { id?: string }>(part: Part) {
  const { id: _id, ...rest } = part
  return rest
}
