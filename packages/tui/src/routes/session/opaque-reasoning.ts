import type { Part, ReasoningPart } from "@opencode-ai/sdk/v2"

// A run's row is its first part, the same object on every recompute, so the view keyed on it persists while
// the run grows. The later members are not rows; they reach the view through `runs`.
export type OpaqueReasoningGroups = {
  rows: Part[]
  runs: Map<string, ReasoningPart[]>
}

// OpenRouter encrypts some reasoning blocks and marks them with a placeholder.
export function reasoningText(part: ReasoningPart) {
  return part.text.replace("[REDACTED]", "").trim()
}

// Encrypted reasoning from OpenAI Responses has provider metadata and no text,
// so it has nothing to show but a duration.
export function isOpaqueReasoning(part: ReasoningPart) {
  return !reasoningText(part) && Boolean(part.metadata)
}

// A run ends at a part that renders a line of its own. Every other part stays in
// order, so a part type the view does not render yet still reaches the view.
export function groupOpaqueReasoning(parts: readonly Part[]): OpaqueReasoningGroups {
  const breakers = parts.flatMap((part, index) => (breaksRun(part) ? [index] : []))
  const starts = [0, ...breakers.map((index) => index + 1)]
  const runs = new Map<string, ReasoningPart[]>()
  const rows = starts.flatMap((start, index) => {
    const end = breakers[index] ?? parts.length
    const segment = parts.slice(start, end)
    const members = segment.filter(isOpaquePart)
    const leader = members[0]
    if (leader) runs.set(leader.id, members)
    return [...segment.filter((part) => !isOpaquePart(part) || part === leader), ...parts.slice(end, end + 1)]
  })
  return { rows, runs }
}

function breaksRun(part: Part) {
  if (part.type === "tool") return true
  if (part.type === "text") return part.text.trim() !== ""
  if (part.type === "reasoning") return reasoningText(part) !== ""
  return false
}

function isOpaquePart(part: Part): part is ReasoningPart {
  return part.type === "reasoning" && isOpaqueReasoning(part)
}
