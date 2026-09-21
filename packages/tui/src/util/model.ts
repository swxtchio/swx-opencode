import type { Provider } from "@opencode-ai/sdk/v2"

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

// The model the user CONFIGURED, decorated with the model(s) that actually
// served the turn whenever those differ.
//
// A router provider (Fireworks FireRouter, Azure model-router) is sent a route
// slug and answers with whichever member model it picked, so the configured
// name on its own hides both what actually ran and what actually got billed.
// A direct provider echoes its own id straight back, which is why the
// single-and-equal case below returns the bare name: every non-routed footer
// stays byte-identical to what it rendered before. That test compares raw IDS,
// never resolved display names - two DIFFERENT ids sharing a friendly name are
// different models, versions or prices, and hiding one behind the other would
// misreport the turn.
//
// Served ids are joined in the order they first appeared, so a multi-step turn
// that switched models reads as the sequence it actually was rather than
// collapsing to whichever model happened to finish.
export function servedName(
  list: Provider[] | ReadonlyMap<string, Provider> | undefined,
  providerID: string,
  modelID: string,
  responseModelIDs: readonly string[] | undefined,
) {
  const base = name(list, providerID, modelID)
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
