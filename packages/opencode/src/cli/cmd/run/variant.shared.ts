// Model variant resolution and persistence.
//
// Variants are provider-specific reasoning effort levels (e.g., "high", "max").
// Resolution priority: CLI --variant flag > saved preference > session history.
//
// The saved variant persists across sessions in ~/.local/state/opencode/model.json
// so your last-used variant sticks. Cycling (ctrl+t) updates both the active
// variant and the persisted file.
import path from "path"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Context, Effect, Layer } from "effect"
import { makeRuntime } from "@/effect/run-service"
import { Global } from "@opencode-ai/core/global"
import { isRecord } from "@/util/record"
import { createSession, sessionVariant, type RunSession, type SessionMessages } from "./session.shared"
import type { RunInput, RunProvider, TurnModel } from "./types"

const MODEL_FILE = path.join(Global.Path.state, "model.json")

type ModelState = Record<string, unknown> & {
  variant?: Record<string, string | undefined>
}
type VariantService = {
  readonly resolveSavedVariant: (model: RunInput["model"]) => Effect.Effect<string | undefined>
  readonly saveVariant: (model: RunInput["model"], variant: string | undefined) => Effect.Effect<void>
}
type VariantRuntime = {
  resolveSavedVariant(model: RunInput["model"]): Promise<string | undefined>
  saveVariant(model: RunInput["model"], variant: string | undefined): Promise<void>
}

class Service extends Context.Service<Service, VariantService>()("@opencode/RunVariant") {}

function modelKey(provider: string, model: string): string {
  return `${provider}/${model}`
}

function variantKey(model: NonNullable<RunInput["model"]>): string {
  return modelKey(model.providerID, model.modelID)
}

export function modelInfo(providers: RunProvider[] | undefined, model: NonNullable<RunInput["model"]>) {
  const provider = providers?.find((item) => item.id === model.providerID)
  return {
    provider: provider?.name ?? model.providerID,
    model: provider?.models[model.modelID]?.name ?? model.modelID,
  }
}

export function formatModelLabel(
  model: NonNullable<RunInput["model"]>,
  variant: string | undefined,
  providers?: RunProvider[],
): string {
  const names = modelInfo(providers, model)
  const label = variant ? ` · ${variant}` : ""
  return `${names.model} · ${names.provider}${label}`
}

export function cycleVariant(current: string | undefined, variants: string[]): string | undefined {
  if (variants.length === 0) {
    return undefined
  }

  if (!current) {
    return variants[0]
  }

  const idx = variants.indexOf(current)
  if (idx === -1 || idx === variants.length - 1) {
    return undefined
  }

  return variants[idx + 1]
}

export function pickVariant(model: RunInput["model"], input: RunSession | SessionMessages): string | undefined {
  return sessionVariant(Array.isArray(input) ? createSession(input) : input, model)
}

function fitVariant(value: string | undefined, variants: string[]): string | undefined {
  if (!value) {
    return undefined
  }

  if (variants.length === 0 || variants.includes(value)) {
    return value
  }

  return undefined
}

// Picks the active variant. CLI flag wins, then saved preference, then session
// history. fitVariant() checks saved and session values against the available
// variants list -- if the provider doesn't offer a variant, it drops.
export function resolveVariant(
  input: string | undefined,
  session: string | undefined,
  saved: string | undefined,
  variants: string[],
): string | undefined {
  if (input !== undefined) {
    return input
  }

  const fallback = fitVariant(saved, variants)
  const current = fitVariant(session, variants)
  if (current !== undefined) {
    return current
  }

  return fallback
}

function state(value: unknown): ModelState {
  if (!isRecord(value)) {
    return {}
  }

  const variant = isRecord(value.variant)
    ? Object.fromEntries(
        Object.entries(value.variant).flatMap(([key, item]) => {
          if (typeof item !== "string") {
            return []
          }

          return [[key, item] as const]
        }),
      )
    : undefined

  return {
    ...value,
    variant,
  }
}

function createLayer(fs = AppNodeBuilder.build(FSUtil.node)) {
  return Layer.fresh(
    Layer.effect(
      Service,
      Effect.gen(function* () {
        const file = yield* FSUtil.Service

        const read = Effect.fn("RunVariant.read")(function* () {
          return yield* file.readJson(MODEL_FILE).pipe(
            Effect.map(state),
            Effect.catchCause(() => Effect.succeed(state(undefined))),
          )
        })

        const resolveSavedVariant = Effect.fn("RunVariant.resolveSavedVariant")(function* (model: RunInput["model"]) {
          if (!model) {
            return undefined
          }

          return (yield* read()).variant?.[variantKey(model)]
        })

        const saveVariant = Effect.fn("RunVariant.saveVariant")(function* (
          model: RunInput["model"],
          variant: string | undefined,
        ) {
          if (!model) {
            return
          }

          const current = yield* read()
          const next = {
            ...current.variant,
          }
          const key = variantKey(model)
          if (variant) {
            next[key] = variant
          }

          if (!variant) {
            delete next[key]
          }

          yield* file
            .writeJson(MODEL_FILE, {
              ...current,
              variant: next,
            })
            .pipe(Effect.orElseSucceed(() => undefined))
        })

        return Service.of({
          resolveSavedVariant,
          saveVariant,
        })
      }),
    ).pipe(Layer.provide(fs)),
  )
}

/** @internal Exported for testing. */
export function createVariantRuntime(fs = AppNodeBuilder.build(FSUtil.node)): VariantRuntime {
  const runtime = makeRuntime(Service, createLayer(fs))
  return {
    resolveSavedVariant: (model) => runtime.runPromise((svc) => svc.resolveSavedVariant(model)).catch(() => undefined),
    saveVariant: (model, variant) => runtime.runPromise((svc) => svc.saveVariant(model, variant)).catch(() => {}),
  }
}

const runtime = createVariantRuntime()

export async function resolveSavedVariant(model: RunInput["model"]): Promise<string | undefined> {
  return runtime.resolveSavedVariant(model)
}

export function saveVariant(model: RunInput["model"], variant: string | undefined): void {
  void runtime.saveVariant(model, variant)
}

// The configured model label, decorated with what served the request. The llmrouter/auto identity gets session request counts; every other identity keeps the turn label's existing raw-ID suppression and display-name rules.
export function servedModelLabel(
  providers: RunProvider[] | undefined,
  providerID: string,
  modelID: string,
  responseModelIDs: readonly string[] | undefined,
  sessionResponseModelIDs?: readonly string[],
): string {
  const provider = providers?.find((item) => item.id === providerID)
  const resolve = (id: string) => provider?.models[id]?.name ?? id
  const base = resolve(modelID)
  if (providerID === "llmrouter" && modelID === "auto" && sessionResponseModelIDs !== undefined) {
    return routerUsageLabel(provider, base, sessionResponseModelIDs)
  }
  const served = responseModelIDs ?? []
  if (served.length === 0) return base
  if (served.length === 1 && served[0] === modelID) return base
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

// Keep this formatter aligned with packages/tui/src/util/model.ts routerUsageLabel.
function routerUsageLabel(provider: RunProvider | undefined, base: string, responseModelIDs: readonly string[]) {
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
        `${configuredIDs.has(item.id) ? provider?.models[item.id]?.name ?? item.id : item.id}:${item.count}/${item.share + (bonus.has(item.index) ? 1 : 0)}%`,
    )
    .join(", ")})`
}

// Fold one assistant message's model record into the turn's running record.
//
// A single prompt produces one assistant message PER STEP (session/prompt.ts
// loops and creates a new Assistant each pass) while the run CLI prints ONE
// summary for the whole prompt, so served ids accumulate in first-seen order
// rather than the last message winning.
//
// This deliberately has no notion of a turn boundary: the caller resets the
// record at turn.send, which is the only authoritative boundary. Deriving one
// from message ids looked right and was not - auto-compaction mints a
// synthetic user message mid-turn, which would have dropped every model used
// before the compaction.
function accumulateTurnModel(prev: TurnModel | undefined, next: TurnModel | undefined): TurnModel | undefined {
  if (!next) return prev
  if (!prev) return next
  // A genuine identity change mid-turn starts over rather than blending two
  // models' served lists, which would attribute one model's work to the other.
  if (prev.providerID !== next.providerID || prev.modelID !== next.modelID) return next
  const served = [...prev.served]
  for (const id of next.served) {
    if (!served.includes(id)) served.push(id)
  }
  return { ...next, served }
}

// Which model label a finished turn's summary should carry.
//
// The turn's OWN recorded identity is the only acceptable answer. The composer
// selection is deliberately NOT a fallback: it is mutable while a turn is in
// flight, so a turn that failed before producing any assistant message would be
// confidently labelled with a model the user picked afterwards - the exact
// misattribution this whole change exists to remove. turn.send seeds the
// dispatched identity, so an absent record means the identity is genuinely
// unknown and the summary says so.
export function turnSummaryModel(input: {
  turnModel: TurnModel | undefined
  providers: RunProvider[] | undefined
  messages?: SessionMessages
}): string {
  if (!input.turnModel) return "unknown model"
  const message = input.turnModel.messageID
    ? input.messages?.find((item) => item.info.id === input.turnModel?.messageID)
    : undefined
  return servedModelLabel(
    input.providers,
    input.turnModel.providerID,
    input.turnModel.modelID,
    input.turnModel.served,
    input.turnModel.providerID === "llmrouter" && input.turnModel.modelID === "auto" && message
      ? servedAcrossSession(input.messages, message)
      : undefined,
  )
}

// Every model that served one TURN, in first-seen order. Sibling of
// packages/tui/src/util/model.ts servedAcrossTurn; see that comment for why a
// turn is not a single assistant message, and why compaction/subtask messages
// on the same parent are excluded.
export function servedAcrossTurn(all: { info: TurnMessage }[] | undefined, info: TurnMessage): string[] {
  if (!all || info.parentID === undefined) return [...(info.responseModelIDs ?? [])]
  const out: string[] = []
  for (const item of all) {
    if (item.info.role !== "assistant" || item.info.parentID !== info.parentID) continue
    if (item.info.summary === true) continue
    if (item.info.providerID !== info.providerID || item.info.modelID !== info.modelID) continue
    for (const id of item.info.responseModelIDs ?? []) {
      if (!out.includes(id)) out.push(id)
    }
  }
  return out
}

// Count per-request step records from this session up through the message being labeled. The info-level responseModelIDs array intentionally deduplicates members, so request frequency comes from each step-finish part.
// Keep the count source and cut-off aligned with packages/tui/src/util/model.ts servedAcrossSession.
export function servedAcrossSession(
  all: SessionMessages | undefined,
  message: SessionMessages[number],
): string[] | undefined {
  if (!all) return undefined
  const index = all.findIndex((item) => item.info.id === message.info.id)
  if (index === -1) return undefined
  const target = all[index]!.info
  if (target.role !== "assistant") return []
  return all.slice(0, index + 1).flatMap((item) => {
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
  role?: string
  parentID?: string
  providerID?: string
  modelID?: string
  // A user message carries an OBJECT here and an assistant a boolean, so this
  // stays wide and the check below is an explicit `=== true`.
  summary?: unknown
  responseModelIDs?: readonly string[]
}

// The turn-model lifecycle as one pure reducer, so both transitions are
// pinned by tests rather than only the accumulating one.
//
// "send" REPLACES: a new turn must never inherit the previous turn's models,
// and expressing that as an accumulate would silently carry them forward.
// "observe" ACCUMULATES: each assistant message is one step of the turn.
export function reduceTurnModel(
  prev: TurnModel | undefined,
  event:
    | { kind: "send"; dispatched: { providerID: string; modelID: string } | undefined }
    | { kind: "observe"; observed: TurnModel | undefined },
): TurnModel | undefined {
  if (event.kind === "send") {
    return event.dispatched
      ? { providerID: event.dispatched.providerID, modelID: event.dispatched.modelID, served: [] }
      : undefined
  }
  return accumulateTurnModel(prev, event.observed)
}
