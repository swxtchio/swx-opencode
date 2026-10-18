export * as AskLedger from "./ledger"

import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { SessionAskTable } from "@opencode-ai/core/session/ask.sql"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionAsk } from "@opencode-ai/schema/session-ask"
import { and, asc, eq, isNull } from "drizzle-orm"
import { Cause, Context, Effect, Layer, Option, Schedule, Schema } from "effect"
import { InstanceState } from "@/effect/instance-state"
import type { SessionID } from "@/session/schema"
import { AskEmit } from "./emit"

export type Row = typeof SessionAskTable.$inferSelect

// The ledger could not establish an answer's request state, or the request is
// waited on by a process that cannot be reached from here. Nothing was settled.
export class UnavailableError extends Schema.TaggedErrorClass<UnavailableError>()("SessionAsk.UnavailableError", {
  requestID: Schema.String,
  reason: Schema.Literals(["store-unavailable", "owned-by-live-process"]),
}) {
  override get message() {
    if (this.reason === "owned-by-live-process")
      return `Ask ${this.requestID} is waited on by another live process; answer it on that process's server`
    return `Ask ${this.requestID} state could not be read or written`
  }
}

// The answer was accepted but could not be applied, so it is not reported as settled. With `apply-failed` the
// producer's effect failed and nothing was settled: the ask stays pending for another answer. With `call-gone` the call
// the ask belongs to is gone or its Session went on after a restart: the ask is closed as cancelled, nothing applied.
export class NotAppliedError extends Schema.TaggedErrorClass<NotAppliedError>()("SessionAsk.NotAppliedError", {
  requestID: Schema.String,
  reason: Schema.Literals(["apply-failed", "call-gone"]),
}) {
  override get message() {
    if (this.reason === "call-gone")
      return `Ask ${this.requestID} belongs to a call that is gone or that its Session went on without; it was closed and the answer was not applied`
    return `Ask ${this.requestID} could not apply the answer; nothing was settled and the ask is still pending`
  }
}

// Passes a typed ledger failure through and turns any other cause but an interruption into an explicit store failure.
export function settling(requestID: string) {
  return <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, UnavailableError | NotAppliedError, R> =>
    self.pipe(
      Effect.catchCause((cause): Effect.Effect<never, UnavailableError | NotAppliedError> => {
        if (Cause.hasInterruptsOnly(cause)) return Effect.interrupt
        const error = Option.getOrUndefined(Cause.findErrorOption(cause))
        if (error instanceof UnavailableError || error instanceof NotAppliedError) return Effect.fail(error)
        return Effect.logWarning("ask ledger store failed", { requestID, cause: Cause.pretty(cause) }).pipe(
          Effect.andThen(Effect.fail(new UnavailableError({ requestID, reason: "store-unavailable" }))),
        )
      }),
    )
}

export type Resolution = {
  readonly requestID: string
  readonly sessionID: SessionID
  readonly disposition: SessionAsk.Disposition
  readonly decision: SessionAsk.Decision
  // Whether recovery wrote the decision into the stored tool call. A decision settled with nothing to apply it to, or
  // one its live waiter received, is never reported as recovered.
  readonly recovered: boolean
  // Set when the process that waited on the ask is gone. Nothing has acted on the answer yet, and a still-pending ask
  // is not settled: the caller hands the resolution to AskRecovery, which settles it and applies the accepted decision
  // to the stored tool call in one transaction, and returns the final resolution.
  readonly recover?: Row
}

export interface Interface {
  // Records an ask its producer now waits on, then emits it. A no-op unless emission is configured.
  readonly admit: (input: {
    id: string
    sessionID: SessionID
    kind: SessionAsk.Kind
    tool?: string
    request: Row["request"]
  }) => Effect.Effect<void>
  // The ask of this kind in this instance's directory, so another directory's ask is unknown here.
  readonly find: (id: string, kind: SessionAsk.Kind) => Effect.Effect<Row | undefined, UnavailableError>
  readonly owner: (row: Row) => Effect.Effect<"self" | "alive" | "gone" | "unknown">
  // Settles a pending ask once. `won` is false when another resolver settled it first. `delivered` records that the
  // decision goes to the live waiter, `recovered` that recovery writes it into the stored call in the same transaction.
  readonly settle: (input: {
    id: string
    decision: SessionAsk.Decision
    delivered: boolean
    recovered: boolean
  }) => Effect.Effect<{ won: boolean; row: Row }, UnavailableError>
  // Settles an emitted ask with its first decision. `live` says whether this process still waits on it; `apply` is the
  // producer's effect for a live decision, run in the transaction that settles it, so the ask settles only once that
  // effect succeeded and otherwise stays pending (`NotAppliedError`). Without a
  // waiter, a pending ask whose owning process is gone is handed back for recovery, and so is one recovery already
  // applied, whose continuation may have been lost; a decision a live waiter received is never handed back. A pending
  // ask this process owns lost its waiter and is cancelled, and one another live process owns is refused.
  readonly decide: (input: {
    id: string
    kind: SessionAsk.Kind
    decision: SessionAsk.Decision
    live: () => boolean
    apply?: () => Effect.Effect<void, unknown>
  }) => Effect.Effect<
    { type: "unknown" } | { type: "duplicate" | "won" | "recover"; resolution: Resolution },
    UnavailableError | NotAppliedError
  >
  // Ends a still-pending ask whose waiter stopped waiting. A no-op unless emission is configured. The write is retried;
  // until it lands, the ask stays unlisted and an answer to it records the cancellation or is refused (see `decide`).
  readonly cancel: (id: string) => Effect.Effect<void>
  // Pending asks of this directory that this process can settle: a gone process's, or its own while `live` says it
  // still waits on them.
  readonly pending: (
    kind: SessionAsk.Kind,
    live: (id: string) => boolean,
  ) => Effect.Effect<ReadonlyArray<Row>, UnavailableError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/AskLedger") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = (yield* Database.Service).db
    // SQL failures surface as defects, so every cause but an interruption becomes an explicit store failure.
    const unavailable =
      (requestID: string) =>
      <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, UnavailableError, R> =>
        self.pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : Effect.logWarning("ask ledger store failed", { requestID, cause: Cause.pretty(cause) }).pipe(
                  Effect.andThen(Effect.fail(new UnavailableError({ requestID, reason: "store-unavailable" }))),
                ),
          ),
        )

    const admit = Effect.fn("AskLedger.admit")(function* (input: Parameters<Interface["admit"]>[0]) {
      const settings = AskEmit.current()
      if (!settings) return
      const ctx = yield* InstanceState.context
      const workspaceID = yield* InstanceState.workspaceID
      // Immediate, like every queue write: a deferred transaction would pin the read snapshot it checks writability in.
      yield* db
        .transaction(
          (tx) =>
            Effect.gen(function* () {
              yield* EventV2.assertWritableIn(tx, input.sessionID)
              yield* tx
                .insert(SessionAskTable)
                .values({
                  id: input.id,
                  session_id: input.sessionID,
                  kind: input.kind,
                  directory: ctx.directory,
                  tool: input.tool,
                  request: input.request,
                  writer_id: EventWriter.id(),
                  time_created: Date.now(),
                })
                .run()
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
      const session = yield* db
        .select({ parent: SessionTable.parent_id })
        .from(SessionTable)
        .where(eq(SessionTable.id, input.sessionID))
        .get()
        .pipe(Effect.orDie)
      const query = { directory: ctx.directory, ...(workspaceID ? { workspace: workspaceID } : {}) }
      const payload: SessionAsk.Emitted = {
        type: "opencode.ask",
        version: 1,
        kind: input.kind,
        id: input.id,
        sessionID: input.sessionID,
        ...(session?.parent ? { parentSessionID: session.parent } : {}),
        directory: ctx.directory,
        ...(workspaceID ? { workspaceID } : {}),
        ...(settings.server ? { server: settings.server } : {}),
        origin: {
          messageID: input.request.tool?.messageID,
          callID: input.request.tool?.callID,
          tool: input.tool,
        },
        ...(input.kind === "question" ? { question: input.request } : {}),
        ...(input.kind === "permission" ? { permission: input.request } : {}),
        answer: { method: "POST", path: `/${input.kind}/${input.id}/answer`, query },
      } as SessionAsk.Emitted
      // Detached from the ask: a slow or stuck receiver never delays the request or its answer.
      yield* AskEmit.send(settings.command, payload).pipe(Effect.forkDetach({ startImmediately: true }))
    })

    const find = Effect.fn("AskLedger.find")(function* (id: string, kind: SessionAsk.Kind) {
      const directory = yield* InstanceState.directory
      return yield* db
        .select()
        .from(SessionAskTable)
        .where(
          and(eq(SessionAskTable.id, id), eq(SessionAskTable.kind, kind), eq(SessionAskTable.directory, directory)),
        )
        .get()
        .pipe(unavailable(id))
    })

    const owner = (row: Row) => EventWriter.liveness(db, row.writer_id)

    const settle = Effect.fn("AskLedger.settle")(function* (input: Parameters<Interface["settle"]>[0]) {
      return yield* db
        .transaction(
          (tx) =>
            Effect.gen(function* () {
              const row = yield* tx.select().from(SessionAskTable).where(eq(SessionAskTable.id, input.id)).get()
              if (!row) return yield* Effect.die(new Error(`Ask ${input.id} vanished while settling`))
              if (row.time_settled !== null) return { won: false, row }
              const settled = {
                ...row,
                decision: input.decision,
                delivered: input.delivered,
                recovered: input.recovered,
                time_settled: Date.now(),
              }
              yield* tx
                .update(SessionAskTable)
                .set({
                  decision: settled.decision,
                  delivered: settled.delivered,
                  recovered: settled.recovered,
                  time_settled: settled.time_settled,
                })
                .where(and(eq(SessionAskTable.id, input.id), isNull(SessionAskTable.time_settled)))
                .run()
              return { won: true, row: settled }
            }),
          { behavior: "immediate" },
        )
        .pipe(unavailable(input.id))
    })

    const pending = Effect.fn("AskLedger.pending")(function* (kind: SessionAsk.Kind, live: (id: string) => boolean) {
      const directory = yield* InstanceState.directory
      const rows = yield* db
        .select()
        .from(SessionAskTable)
        .where(
          and(
            eq(SessionAskTable.directory, directory),
            eq(SessionAskTable.kind, kind),
            isNull(SessionAskTable.time_settled),
          ),
        )
        .orderBy(asc(SessionAskTable.id))
        .all()
        .pipe(unavailable(kind))
      const holders = yield* Effect.forEach(rows, (row) => owner(row))
      return rows.filter((row, index) => holders[index] === "gone" || (holders[index] === "self" && live(row.id)))
    })

    const decide = Effect.fn("AskLedger.decide")(function* (input: Parameters<Interface["decide"]>[0]) {
      const row = yield* find(input.id, input.kind)
      if (!row) return { type: "unknown" as const }
      if (input.live()) {
        const apply = input.apply
        const settled = yield* db
          .transaction(
            () =>
              Effect.gen(function* () {
                const claimed = yield* settle({
                  id: input.id,
                  decision: input.decision,
                  delivered: true,
                  recovered: false,
                })
                if (claimed.won && apply)
                  yield* apply().pipe(
                    Effect.catchCause((cause) =>
                      Effect.logWarning("ask answer could not be applied", {
                        id: input.id,
                        cause: Cause.pretty(cause),
                      }).pipe(
                        Effect.andThen(
                          Effect.fail(new NotAppliedError({ requestID: input.id, reason: "apply-failed" })),
                        ),
                      ),
                    ),
                  )
                return claimed
              }),
            { behavior: "immediate" },
          )
          .pipe(settling(input.id))
        if (!settled.won) return { type: "duplicate" as const, resolution: resolution(settled.row, "duplicate") }
        return { type: "won" as const, resolution: resolution(settled.row, "settled") }
      }
      const holder = yield* owner(row)
      if (holder === "gone") {
        if (row.time_settled === null)
          return {
            type: "recover" as const,
            resolution: { ...resolution(row, "settled"), decision: input.decision, recover: row },
          }
        // Recovery wrote this call, but the wake that continues it may have died with its process.
        if (row.recovered)
          return { type: "recover" as const, resolution: { ...resolution(row, "duplicate"), recover: row } }
        return { type: "duplicate" as const, resolution: resolution(row, "duplicate") }
      }
      if (row.time_settled !== null) return { type: "duplicate" as const, resolution: resolution(row, "duplicate") }
      if (holder !== "self")
        return yield* new UnavailableError({ requestID: input.id, reason: "owned-by-live-process" })
      // This process recorded the ask but no longer waits on it, so no answer can reach the call: end it. This also
      // records an aborted ask's cancellation that could not be written, or refuses the answer while it still cannot.
      const ended = yield* settle({ id: input.id, decision: { type: "cancelled" }, delivered: false, recovered: false })
      return { type: "duplicate" as const, resolution: resolution(ended.row, "duplicate") }
    })

    const cancel = Effect.fn("AskLedger.cancel")(function* (id: string) {
      if (!AskEmit.current()) return
      yield* db
        .update(SessionAskTable)
        .set({ decision: { type: "cancelled" }, time_settled: Date.now() })
        .where(and(eq(SessionAskTable.id, id), isNull(SessionAskTable.time_settled)))
        .run()
        .pipe(
          unavailable(id),
          Effect.retry({ times: 4, schedule: Schedule.exponential("25 millis") }),
          Effect.catchTag("SessionAsk.UnavailableError", () =>
            Effect.logError("ask cancel not recorded; its answers are refused until it is", { id }),
          ),
        )
    })

    return Service.of({ admit, find, owner, settle, decide, cancel, pending })
  }),
)

function resolution(row: Row, disposition: SessionAsk.Disposition): Resolution {
  return {
    requestID: row.id,
    sessionID: row.session_id,
    disposition,
    decision: row.decision ?? { type: "cancelled" },
    recovered: row.recovered,
  }
}

// Under emission an ask ends when the call that raised it is aborted, so its row is cancelled instead of staying
// answerable for a call that is gone. Tool executions are not interrupted by an abort, so without this an aborted
// ask would wait forever. Without emission the producer keeps its stock behaviour.
export function abortable<E2>(signal: AbortSignal | undefined, aborted: () => E2) {
  return <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, E | E2, R> => {
    if (!signal || !AskEmit.current()) return self
    return Effect.raceFirst(
      self,
      Effect.callback<never, E2>((resume) => {
        const end = () => resume(Effect.fail(aborted()))
        if (signal.aborted) end()
        else signal.addEventListener("abort", end, { once: true })
        return Effect.sync(() => signal.removeEventListener("abort", end))
      }),
    )
  }
}

export const node = LayerNode.make({ service: Service, layer, deps: [Database.node] })
