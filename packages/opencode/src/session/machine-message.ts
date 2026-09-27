import { ConfigV1 } from "@opencode-ai/core/v1/config/config"

type Marker = typeof ConfigV1.MachineMessageMarker.Type
type MarkerConfig = NonNullable<ConfigV1.Info["machine_message_markers"]>

const DEFAULT_HOLD_MARKERS: Marker[] = [
  { type: "prefix", value: `[fm-from-peer]\x1f` },
  { type: "prefix", value: `[fm-from-firstmate]\x1f` },
  { type: "prefix", value: "\x1f" },
  { type: "prefix", value: "WATCHER FIRED [" },
  { type: "prefix", value: "OBSERVER: " },
  { type: "fleet-heartbeat" },
]

const HEARTBEAT_PROMPT =
  "Fleet heartbeat. Run one supervision cycle from live state, not memory: read the live fleet, backlog, and open work fresh; identify who is blocked only on firstmate, and take the highest-value in-scope action that advances convergence. Escalate destructive, irreversible, or security-sensitive decisions to the captain. See docs/fleet-operating-process.md."
const HEARTBEAT_NUDGE_ANCHOR = " See docs/fleet-operating-process.md."
const HEARTBEAT_INLINE_PAYLOAD_MAX_CHARS = 700
const HEARTBEAT_LONG_SUMMARY_MAX_CHARS = 240
const HEARTBEAT_TIMESTAMP_PREFIX = [
  { digits: 4 },
  { literal: "-" },
  { digits: 2 },
  { literal: "-" },
  { digits: 2 },
  { literal: "T" },
  { digits: 2 },
  { literal: ":" },
  { digits: 2 },
  { literal: "Z · " },
] as const
const HEARTBEAT_RAM_PREFIX = [
  { literal: "System RAM: " },
  { number: "decimal" },
  { literal: "\x2f" },
  { number: "decimal" },
  { literal: " GiB used (" },
  { number: "integer" },
  { literal: "%) · " },
  { number: "decimal" },
  { literal: " GiB available · " },
] as const
const HEARTBEAT_UNAVAILABLE_CONTEXT_PREFIX = [
  { literal: "Session transcript: unavailable (reason=" },
  { number: "word" },
  { literal: ") · " },
] as const
const HEARTBEAT_MEASURED_CONTEXT_PREFIX = [
  { literal: "Session transcript: harness=" },
  { number: "word" },
  { literal: " session=" },
  { number: "word" },
  { literal: " bytes=" },
  { number: "integer" },
  { literal: " MB=" },
  { number: "decimal" },
  { literal: " severity=" },
  { choices: ["safe", "warning", "high", "critical"] },
  { literal: " · " },
] as const

type PrefixToken =
  | { readonly literal: string }
  | { readonly digits: number }
  | { readonly number: "integer" | "decimal" | "word" }
  | { readonly choices: readonly string[] }

export function classify(input: string, config?: MarkerConfig) {
  if (config?.critical?.some((marker) => matches(marker, input))) return "critical" as const
  if ([...DEFAULT_HOLD_MARKERS, ...(config?.hold ?? [])].some((marker) => matches(marker, input)))
    return "hold" as const
}

function matches(marker: Marker, input: string) {
  if (marker.type === "prefix") return marker.value.length > 0 && input.startsWith(marker.value)
  return isFleetHeartbeat(input)
}

function isFleetHeartbeat(input: string) {
  if (input.includes("\n") || input.includes("\r")) return false
  const envelope = input.match(/^(.*) \[fm-heartbeat-receipt:([a-zA-Z0-9._-]+)\]$/)
  if (!envelope) return false
  const body = envelope[1]
  if (characterCount(input) <= HEARTBEAT_INLINE_PAYLOAD_MAX_CHARS && isGeneratedPayload(body)) return true
  return isGeneratedSummary(body)
}

function isGeneratedPayload(input: string) {
  let body = input
  const timestamp = body.match(/^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}Z) · /)
  if (timestamp) body = body.slice(timestamp[0].length)

  if (body.startsWith("System RAM: ")) {
    const ram = body.match(
      /^System RAM: [0-9]+\.[0-9]\x2f[0-9]+\.[0-9] GiB used \([0-9]+%\) · [0-9]+\.[0-9] GiB available · (.*)$/,
    )
    if (!ram) return false
    body = ram[1]
  }

  if (body.startsWith("Session transcript: ")) {
    const transcript = body.match(
      /^Session transcript: (unavailable \(reason=[^\s]+\)|harness=[^\s]+ session=[^\s]+ bytes=[0-9]+ MB=[0-9]+\.[0-9]+ severity=(safe|warning|high|critical)) · (.*)$/,
    )
    if (!transcript) return false
    body = transcript[3]
  }

  if (body === HEARTBEAT_PROMPT) return true
  const core = HEARTBEAT_PROMPT.slice(0, -HEARTBEAT_NUDGE_ANCHOR.length)
  const nudgePrefix = `${core} Periodic nudge: `
  if (!body.startsWith(nudgePrefix) || !body.endsWith(HEARTBEAT_NUDGE_ANCHOR)) return false
  const duty = body.slice(nudgePrefix.length, -HEARTBEAT_NUDGE_ANCHOR.length)
  return duty.length > 0
}

function isGeneratedSummary(input: string) {
  const summary = input.match(
    /^Heartbeat summary: ([^\u0000-\u001f\u007f]+)… Full message: (\x2f[^\u0000-\u001f\u007f]+)$/,
  )
  return (
    summary !== null &&
    characterCount(summary[1]) === HEARTBEAT_LONG_SUMMARY_MAX_CHARS &&
    isGeneratedPayloadPrefix(summary[1])
  )
}

function isGeneratedPayloadPrefix(input: string) {
  let body = input
  const timestamp = body.match(/^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}Z · )/)
  if (timestamp) body = body.slice(timestamp[0].length)
  else if (isPrefixOfTokens(body, HEARTBEAT_TIMESTAMP_PREFIX)) return true

  if (body.startsWith("System RAM: ")) {
    const ram = body.match(
      /^System RAM: [0-9]+\.[0-9]\x2f[0-9]+\.[0-9] GiB used \([0-9]+%\) · [0-9]+\.[0-9] GiB available · /,
    )
    if (!ram) return isPrefixOfTokens(body, HEARTBEAT_RAM_PREFIX)
    body = body.slice(ram[0].length)
  }

  if (body.startsWith("Session transcript: ")) {
    const transcript = body.match(
      /^Session transcript: (unavailable \(reason=[^\s]+\)|harness=[^\s]+ session=[^\s]+ bytes=[0-9]+ MB=[0-9]+\.[0-9]+ severity=(safe|warning|high|critical)) · /,
    )
    if (!transcript)
      return (
        isPrefixOfTokens(body, HEARTBEAT_UNAVAILABLE_CONTEXT_PREFIX) ||
        isPrefixOfTokens(body, HEARTBEAT_MEASURED_CONTEXT_PREFIX)
      )
    body = body.slice(transcript[0].length)
  }

  return HEARTBEAT_PROMPT.startsWith(body)
}

function isPrefixOfTokens(input: string, tokens: readonly PrefixToken[]) {
  let rest = input
  for (const token of tokens) {
    if (rest.length === 0) return true
    if ("literal" in token) {
      if (token.literal.startsWith(rest)) return true
      if (!rest.startsWith(token.literal)) return false
      rest = rest.slice(token.literal.length)
      continue
    }
    if ("digits" in token) {
      const digits = rest.match(/^[0-9]+/)?.[0] ?? ""
      if (rest.length < token.digits) return digits.length === rest.length
      if (digits.length < token.digits) return false
      rest = rest.slice(token.digits)
      continue
    }
    if ("choices" in token) {
      const value = token.choices.find((choice) => rest.startsWith(choice))
      if (!value) return token.choices.some((choice) => choice.startsWith(rest))
      if (rest.length <= value.length) return true
      rest = rest.slice(value.length)
      continue
    }
    if (token.number === "word") {
      const whitespace = rest.search(/\s/)
      if (whitespace === -1) return true
      if (whitespace === 0) return false
      rest = rest.slice(whitespace)
      continue
    }
    const value = rest.match(token.number === "decimal" ? /^[0-9]+\.[0-9]/ : /^[0-9]+/)?.[0]
    if (value) {
      if (value.length === rest.length) return true
      rest = rest.slice(value.length)
      continue
    }
    if (token.number === "decimal") return /^[0-9]+\.?$/.test(rest)
    return false
  }
  return rest.length === 0
}

function characterCount(input: string) {
  return Array.from(input).length
}

export * as MachineMessage from "./machine-message"
