/**
 * Lay out the turn-summary line within a terminal width.
 *
 * The line is `▣ {agent} · {model} · {duration}`, and it used to render as a
 * single right-truncated text node. Truncation from the right cuts the tail
 * first, so at narrow widths the DURATION disappeared, then the served-model
 * parenthetical, then the configured model.
 *
 * #13 notes that the resulting priority order is defensible but accidental -
 * the most valuable new information is cut last - and that nothing enforced or
 * tested it. This makes it explicit, and inverts it where it was wrong:
 *
 *   1. `▣ {agent}` always survives; it is short and identifies the turn.
 *   2. The duration always survives. It is a handful of characters and it is
 *      the thing a reader is most often scanning for.
 *   3. The served-model parenthetical survives ahead of the configured label,
 *      because "what actually served this" is the information the configured
 *      label cannot give you.
 *   4. The configured label absorbs whatever shortfall is left.
 */

export type SummaryParts = {
  readonly agent: string
  readonly model: string
  readonly duration: string
}

/** Marker used when a field is shortened. One column, unlike "...". */
const ELLIPSIS = "…"

/**
 * Split a model label into its configured part and a trailing parenthetical.
 *
 *   "Firerouter glm (glm-5p3-flash → glm-5p3)"
 *     -> { label: "Firerouter glm", served: "(glm-5p3-flash → glm-5p3)" }
 */
export function splitModel(model: string): { label: string; served: string } {
  const match = /^(.*?)\s*(\([^()]*\))$/.exec(model.trim())
  if (!match) return { label: model.trim(), served: "" }
  return { label: match[1]!.trim(), served: match[2]! }
}

/** Shorten to `width` columns, ending with an ellipsis when anything is cut. */
export function elide(value: string, width: number): string {
  if (width <= 0) return ""
  if (value.length <= width) return value
  if (width === 1) return ELLIPSIS
  return value.slice(0, width - 1).trimEnd() + ELLIPSIS
}

/**
 * Fit the summary into `width` columns, returning the model text to render.
 *
 * The caller keeps the `▣ `, the agent and the duration verbatim; only the
 * model is shortened, which is what makes the priority order above hold.
 */
export function fitSummaryModel(parts: SummaryParts, width: number): string {
  // "▣ " + agent + " · " + model + " · " + duration
  const fixed = 2 + parts.agent.length + 3 + 3 + parts.duration.length
  const available = width - fixed
  if (available >= parts.model.length) return parts.model
  if (available <= 0) return ""

  const { label, served } = splitModel(parts.model)
  if (!served) return elide(label, available)

  // Keep the served parenthetical whole for as long as it fits, shrinking the
  // configured label around it.
  const labelRoom = available - served.length - 1
  if (labelRoom >= 1) return `${elide(label, labelRoom)} ${served}`

  // Not even the parenthetical fits beside a single label character: drop the
  // label entirely rather than render a stub, and shorten the parenthetical.
  return elide(served, available)
}
