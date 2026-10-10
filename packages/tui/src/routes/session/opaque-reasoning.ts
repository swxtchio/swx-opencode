import type { Part, ReasoningPart } from "@opencode-ai/sdk/v2"

export type OpaqueReasoningRun = { type: "opaque-reasoning"; parts: ReasoningPart[] }

// OpenRouter encrypts some reasoning blocks and marks them with a placeholder.
export function reasoningText(part: ReasoningPart) {
  return part.text.replace("[REDACTED]", "").trim()
}

// Encrypted reasoning from OpenAI Responses has provider metadata and no text,
// so it has nothing to show but a duration.
export function isOpaqueReasoning(part: ReasoningPart) {
  return !reasoningText(part) && Boolean(part.metadata)
}

// A run ends at a part that renders a line of its own. Parts that render no
// line are skipped without ending the run, so a step boundary between two
// encrypted parts still shows as one line.
export function groupOpaqueReasoning(parts: readonly Part[]): Array<Part | OpaqueReasoningRun> {
  const breakers = parts.flatMap((part, index) => (breaksRun(part) ? [index] : []))
  const starts = [0, ...breakers.map((index) => index + 1)]
  return starts.flatMap((start, index) => {
    const end = breakers[index] ?? parts.length
    return [...collapseRun(parts.slice(start, end)), ...parts.slice(end, end + 1)]
  })
}

function breaksRun(part: Part) {
  if (part.type === "tool") return true
  if (part.type === "text") return part.text.trim() !== ""
  if (part.type === "reasoning") return reasoningText(part) !== ""
  return false
}

function collapseRun(segment: readonly Part[]): Array<ReasoningPart | OpaqueReasoningRun> {
  const run = segment.flatMap((part) => (part.type === "reasoning" && isOpaqueReasoning(part) ? [part] : []))
  if (run.length < 2) return run
  return [{ type: "opaque-reasoning", parts: run }]
}
