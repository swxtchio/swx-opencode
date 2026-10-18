import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ConfigPermissionV1 } from "@opencode-ai/core/v1/config/permission"
import { InstanceState } from "@/effect/instance-state"
import { Wildcard } from "@opencode-ai/core/util/wildcard"
import { Deferred, Effect, Layer, Context } from "effect"
import os from "os"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { EventV2Bridge } from "@/event-v2-bridge"
import type { SessionAsk } from "@opencode-ai/schema/session-ask"
import { AskEmit } from "@/ask/emit"
import { AskLedger } from "@/ask/ledger"

export const Event = PermissionV1.Event

export interface Interface {
  readonly ask: (input: PermissionV1.AskInput) => Effect.Effect<void, PermissionV1.Error>
  readonly reply: (
    input: PermissionV1.ReplyInput,
  ) => Effect.Effect<AskLedger.Resolution, PermissionV1.NotFoundError | AskLedger.UnavailableError>
  readonly list: () => Effect.Effect<ReadonlyArray<PermissionV1.Request>>
  // Allows a request's `always` patterns for the rest of the instance, as an `always` reply does.
  readonly grant: (request: PermissionV1.Request) => Effect.Effect<void>
}

interface PendingEntry {
  info: PermissionV1.Request
  deferred: Deferred.Deferred<void, PermissionV1.RejectedError | PermissionV1.CorrectedError>
}

interface State {
  pending: Map<PermissionV1.ID, PendingEntry>
  approved: PermissionV1.Rule[]
}

export function evaluate(permission: string, pattern: string, ...rulesets: PermissionV1.Ruleset[]): PermissionV1.Rule {
  return (
    rulesets
      .flat()
      .findLast((rule) => Wildcard.match(permission, rule.permission) && Wildcard.match(pattern, rule.pattern)) ?? {
      action: "ask",
      permission,
      pattern: "*",
    }
  )
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Permission") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const ledger = yield* AskLedger.Service
    const state = yield* InstanceState.make<State>(
      Effect.fn("Permission.state")(function* (ctx) {
        void ctx
        const state = {
          pending: new Map<PermissionV1.ID, PendingEntry>(),
          approved: [],
        }

        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            for (const item of state.pending.values()) {
              yield* ledger.cancel(item.info.id)
              yield* Deferred.fail(item.deferred, new PermissionV1.RejectedError())
            }
            state.pending.clear()
          }),
        )

        return state
      }),
    )

    const ask = Effect.fn("Permission.ask")(function* (input: PermissionV1.AskInput) {
      const { approved, pending } = yield* InstanceState.get(state)
      const { ruleset, ...request } = input
      let needsAsk = false

      for (const pattern of request.patterns) {
        const rule = evaluate(request.permission, pattern, ruleset, approved)
        yield* Effect.logInfo("evaluated", { permission: request.permission, pattern, action: rule })
        if (rule.action === "deny") {
          return yield* new PermissionV1.DeniedError({
            ruleset: ruleset.filter((rule) => Wildcard.match(request.permission, rule.permission)),
          })
        }
        if (rule.action === "allow") continue
        needsAsk = true
      }

      if (!needsAsk) return

      const id = request.id ?? PermissionV1.ID.ascending()
      const info: PermissionV1.Request = {
        id,
        sessionID: request.sessionID,
        permission: request.permission,
        patterns: request.patterns,
        metadata: request.metadata,
        always: request.always,
        tool: request.tool,
      }
      yield* Effect.logInfo("asking", { id, permission: info.permission, patterns: info.patterns })
      // A supervised ask is recovered through its tool call after a restart, so one without a call is refused.
      if (AskEmit.current() && !info.tool) {
        yield* Effect.logError("supervised permission refused: it has no tool call", {
          id,
          permission: info.permission,
        })
        return yield* new PermissionV1.RejectedError()
      }

      const deferred = yield* Deferred.make<void, PermissionV1.RejectedError | PermissionV1.CorrectedError>()
      // Waiting before the ask is recorded or published, so no resolver can find it without its waiter.
      pending.set(id, { info, deferred })
      return yield* Effect.gen(function* () {
        yield* ledger.admit({ id, sessionID: info.sessionID, kind: "permission", request: info })
        yield* events.publish(Event.Asked, info)
        return yield* Deferred.await(deferred)
      }).pipe(
        // However the ask ends, aborted or failed at any step included, the waiter is dropped and a still-pending
        // ledger row is cancelled. An answer arriving in between finds no waiter and cancels the row itself.
        Effect.ensuring(
          Effect.sync(() => {
            pending.delete(id)
          }),
        ),
        Effect.onExit(() => ledger.cancel(id)),
      )
    })

    const reply = Effect.fn("Permission.reply")(function* (input: PermissionV1.ReplyInput) {
      const { pending } = yield* InstanceState.get(state)
      const decision: typeof SessionAsk.Replied.Type = {
        type: "replied",
        reply: input.reply,
        ...(input.message !== undefined ? { message: input.message } : {}),
      }

      if (!AskEmit.current()) {
        const existing = pending.get(input.requestID)
        if (!existing) return yield* new PermissionV1.NotFoundError({ requestID: input.requestID })
        yield* deliver(existing, input)
        return {
          requestID: input.requestID,
          sessionID: existing.info.sessionID,
          disposition: "settled",
          decision,
          recovered: false,
        } satisfies AskLedger.Resolution
      }

      // Settling and handing the reply to the waiter are one step, so a cancelled caller cannot leave a settled ask
      // whose waiter never receives it. A resolution handed back for recovery is not settled yet.
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const outcome = yield* ledger.decide({
            id: input.requestID,
            kind: "permission",
            decision,
            live: () => pending.has(input.requestID),
          })
          if (outcome.type === "unknown") return yield* new PermissionV1.NotFoundError({ requestID: input.requestID })
          if (outcome.type !== "won") return outcome.resolution
          const existing = pending.get(input.requestID)
          if (existing) yield* deliver(existing, input)
          return outcome.resolution
        }),
      )
    })

    const grant = Effect.fn("Permission.grant")(function* (request: PermissionV1.Request) {
      const { approved } = yield* InstanceState.get(state)
      for (const pattern of request.always) {
        approved.push({ permission: request.permission, pattern, action: "allow" })
      }
    })

    // Under emission a won settlement reaches its waiter even when its event cannot be published.
    const release = (event: Effect.Effect<void>, complete: Effect.Effect<void>) =>
      AskEmit.current() ? event.pipe(Effect.ensuring(complete)) : event.pipe(Effect.andThen(complete))

    // Under emission a related request is settled only when its own ledger row is won, so it never takes one
    // decision in memory while the ledger records another resolver's.
    const claim = (id: PermissionV1.ID, reply: PermissionV1.Reply) =>
      AskEmit.current()
        ? ledger.settle({ id, decision: { type: "replied", reply }, delivered: true, recovered: false }).pipe(
            Effect.map((settled) => settled.won),
            Effect.catchTag("SessionAsk.UnavailableError", () => Effect.succeed(false)),
          )
        : Effect.succeed(true)

    const deliver = Effect.fn("Permission.deliver")(function* (existing: PendingEntry, input: PermissionV1.ReplyInput) {
      const { approved, pending } = yield* InstanceState.get(state)
      pending.delete(input.requestID)
      const replied = events.publish(Event.Replied, {
        sessionID: existing.info.sessionID,
        requestID: existing.info.id,
        reply: input.reply,
      })

      if (input.reply === "reject") {
        yield* release(
          replied,
          Deferred.fail(
            existing.deferred,
            input.message
              ? new PermissionV1.CorrectedError({ feedback: input.message })
              : new PermissionV1.RejectedError(),
          ),
        )

        for (const [id, item] of pending.entries()) {
          if (item.info.sessionID !== existing.info.sessionID) continue
          if (!(yield* claim(id, "reject"))) continue
          pending.delete(id)
          yield* release(
            events.publish(Event.Replied, {
              sessionID: item.info.sessionID,
              requestID: item.info.id,
              reply: "reject",
            }),
            Deferred.fail(item.deferred, new PermissionV1.RejectedError()),
          )
        }
        return
      }

      yield* release(replied, Deferred.succeed(existing.deferred, undefined))
      if (input.reply === "once") return

      for (const pattern of existing.info.always) {
        approved.push({
          permission: existing.info.permission,
          pattern,
          action: "allow",
        })
      }

      for (const [id, item] of pending.entries()) {
        if (item.info.sessionID !== existing.info.sessionID) continue
        const ok = item.info.patterns.every(
          (pattern) => evaluate(item.info.permission, pattern, approved).action === "allow",
        )
        if (!ok) continue
        if (!(yield* claim(id, "always"))) continue
        pending.delete(id)
        yield* release(
          events.publish(Event.Replied, {
            sessionID: item.info.sessionID,
            requestID: item.info.id,
            reply: "always",
          }),
          Deferred.succeed(item.deferred, undefined),
        )
      }
    })

    const list = Effect.fn("Permission.list")(function* () {
      const pending = (yield* InstanceState.get(state)).pending
      const live = Array.from(pending.values(), (item) => item.info)
      if (!AskEmit.current()) return live
      // Under emission the ledger is the registry, so an ask a restarted server still owes an answer stays listed.
      return yield* ledger
        .pending("permission", (id) => live.some((info) => info.id === id))
        .pipe(
          Effect.map((rows) => rows.map((row) => row.request as PermissionV1.Request)),
          Effect.catchTag("SessionAsk.UnavailableError", (error) =>
            Effect.logWarning("ask ledger unreadable; listing live permissions", { error: error.message }).pipe(
              Effect.as(live),
            ),
          ),
        )
    })

    return Service.of({ ask, reply, list, grant })
  }),
)

function expand(pattern: string): string {
  if (pattern.startsWith("~/")) return os.homedir() + pattern.slice(1)
  if (pattern === "~") return os.homedir()
  if (pattern.startsWith("$HOME/")) return os.homedir() + pattern.slice(5)
  if (pattern.startsWith("$HOME")) return os.homedir() + pattern.slice(5)
  return pattern
}

export function fromConfig(permission: ConfigPermissionV1.Info) {
  const ruleset: PermissionV1.Rule[] = []
  for (const [key, value] of Object.entries(permission)) {
    if (typeof value === "string") {
      ruleset.push({ permission: key, action: value, pattern: "*" })
      continue
    }
    ruleset.push(
      ...Object.entries(value).map(([pattern, action]) => ({ permission: key, pattern: expand(pattern), action })),
    )
  }
  return ruleset
}

export function merge(...rulesets: PermissionV1.Ruleset[]): PermissionV1.Rule[] {
  return rulesets.flat()
}

export function disabled(tools: string[], ruleset: PermissionV1.Ruleset): Set<string> {
  const edits = ["edit", "write", "apply_patch"]
  const reads = ["list_mcp_resources", "list_mcp_resource_templates", "read_mcp_resource"]
  return new Set(
    tools.filter((tool) => {
      const permission = edits.includes(tool) ? "edit" : reads.includes(tool) ? "read" : tool
      const rule = ruleset.findLast((rule) => Wildcard.match(permission, rule.permission))
      return rule?.pattern === "*" && rule.action === "deny"
    }),
  )
}

export function visibleTools<T>(tools: Record<string, T>, ruleset: PermissionV1.Ruleset): Record<string, T> {
  const hidden = disabled(Object.keys(tools), ruleset)
  return Object.fromEntries(Object.entries(tools).filter(([name]) => !hidden.has(name)))
}

export const node = LayerNode.make({ service: Service, layer: layer, deps: [EventV2Bridge.node, AskLedger.node] })

export * as Permission from "."
