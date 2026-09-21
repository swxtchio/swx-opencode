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
// stays byte-identical to what it rendered before.
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
  const served = (responseModelIDs ?? []).map((id) => name(list, providerID, id))
  if (served.length === 0) return base
  if (served.length === 1 && served[0] === base) return base
  return `${base} (${served.join(" → ")})`
}
