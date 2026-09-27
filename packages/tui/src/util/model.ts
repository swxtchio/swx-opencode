import type { Message, Part, Provider } from "@opencode-ai/sdk/v2"

export type SessionStepMessage = {
  info: Message
  parts: Part[]
}

export function parse(value: string) {
  const [providerID, ...modelID] = value.split("/")
  return { providerID, modelID: modelID.join("/") }
}

export function index(list: Provider[] | undefined) {
  return new Map((list ?? []).map((item) => [item.id, item] as const))
}

export function get(list: Provider[] | ReadonlyMap<string, Provider> | undefined, providerID: string, modelID: string) {
  const provider =
    list instanceof Map
      ? list.get(providerID)
      : Array.isArray(list)
        ? list.find((item) => item.id === providerID)
        : undefined
  return provider?.models[modelID]
}

export function name(
  list: Provider[] | ReadonlyMap<string, Provider> | undefined,
  providerID: string,
  modelID: string,
) {
  return get(list, providerID, modelID)?.name ?? modelID
}

// The configured model name stays the base label. llmrouter/auto adds a session request breakdown; every other model keeps the existing turn suffix.
//
// Direct-provider suppression compares raw IDs, never resolved display names: a provider echoing its requested ID is redundant, while distinct IDs sharing a friendly name still need disambiguation.
//
// Served ids are joined in the order they first appeared, so a multi-step turn that switched models reads as the sequence it actually was rather than collapsing to whichever model happened to finish.
export function servedName(
  list: Provider[] | ReadonlyMap<string, Provider> | undefined,
  providerID: string,
  modelID: string,
  responseModelIDs: readonly string[] | undefined,
  sessionResponseModelIDs?: readonly string[],
) {
  const base = name(list, providerID, modelID)
  if (providerID === "llmrouter" && modelID === "auto") {
    const provider =
      list instanceof Map
        ? list.get(providerID)
        : Array.isArray(list)
          ? list.find((item) => item.id === providerID)
          : undefined
    return routerUsageLabel(provider, base, sessionResponseModelIDs ?? [])
  }
  const served = responseModelIDs ?? []
  if (served.length === 0) return base
  if (served.length === 1 && served[0] === modelID) return base
  const resolve = (id: string) => name(list, providerID, id)
  // When two DIFFERENT ids resolve to the same friendly name, the name alone
  // cannot tell them apart - and they may differ in version, route or price -
  // so the raw id is appended to the colliding entries.
  const labels = served.map((id) => {
    const label = resolve(id)
    const collides = label === base || served.some((other) => other !== id && resolve(other) === label)
    return collides ? `${label} [${id}]` : label
  })
  return `${base} (${labels.join(" → ")})`
}

function routerUsageLabel(provider: Provider | undefined, base: string, responseModelIDs: readonly string[]) {
  const configured = Object.entries(provider?.models ?? {}).filter(([id]) => id !== "auto")
  const configuredIDs = new Set(configured.map(([id]) => id))
  const unknown = [...new Set(responseModelIDs.filter((id) => !configuredIDs.has(id)))]
  const ids = [...configured.map(([id]) => id), ...unknown]
  if (ids.length === 0) return base

  const counts = responseModelIDs.reduce((out, id) => out.set(id, (out.get(id) ?? 0) + 1), new Map<string, number>())
  const total = responseModelIDs.length
  const shares = ids.map((id, index) => {
    const exact = total === 0 ? 0 : ((counts.get(id) ?? 0) * 100) / total
    return { id, count: counts.get(id) ?? 0, share: Math.floor(exact), fraction: exact - Math.floor(exact), index }
  })
  const remaining = total === 0 ? 0 : 100 - shares.reduce((sum, item) => sum + item.share, 0)
  const bonus = new Set(
    shares
      .toSorted((a, b) => b.fraction - a.fraction || a.index - b.index)
      .slice(0, remaining)
      .map((item) => item.index),
  )

  return `${base} (${shares
    .map(
      (item) =>
        `${provider?.models[item.id]?.name ?? item.id}:${item.count}/${item.share + (bonus.has(item.index) ? 1 : 0)}%`,
    )
    .join(", ")})`
}

// Every model that served one TURN, in first-seen order.
//
// A turn is not one assistant message: opencode creates a new assistant
// message per step, and the footer renders only on the last of them (the
// earlier ones finish with "tool-calls", so they are not `final`). Reading
// that one message would therefore report only the last step's model and hide
// the very sequence a router turn exists to show, so every assistant message
// sharing this one's parent user message is folded in.
export function servedAcrossTurn(
  messages: readonly TurnMessage[],
  message: TurnMessage & { parentID?: string },
): string[] {
  if (message.parentID === undefined) return [...(message.responseModelIDs ?? [])]
  const out: string[] = []
  for (const item of messages) {
    if (item.role !== "assistant" || item.parentID !== message.parentID) continue
    if (item.summary === true) continue
    if (item.providerID !== message.providerID || item.modelID !== message.modelID) continue
    for (const id of item.responseModelIDs ?? []) {
      if (!out.includes(id)) out.push(id)
    }
  }
  return out
}

export function servedAcrossSession(messages: readonly SessionStepMessage[], messageID: string): string[] {
  const index = messages.findIndex((item) => item.info.id === messageID)
  if (index === -1) return []
  const message = messages[index]!
  const target = message.info
  if (target.role !== "assistant") return []
  return messages.slice(0, index + 1).flatMap((item) => {
    const info = item.info
    if (info.role !== "assistant") return []
    if (info.sessionID !== target.sessionID) return []
    if (info.providerID !== target.providerID || info.modelID !== target.modelID) return []
    if (info.summary === true || item.parts.some((part) => part.type === "compaction")) return []
    return item.parts.flatMap((part) =>
      part.type === "step-finish" && part.responseModelID !== undefined ? [part.responseModelID] : [],
    )
  })
}

type TurnMessage = {
  role: string
  parentID?: string
  providerID?: string
  modelID?: string
  // A user message carries an OBJECT here and an assistant a boolean, so this
  // stays wide and the check below is an explicit `=== true`.
  summary?: unknown
  responseModelIDs?: readonly string[]
}
