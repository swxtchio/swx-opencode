import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import {
  SessionPromptProvenanceTable,
  SessionPromptQueueSequenceTable,
  SessionPromptQueueTable,
  SessionPromptReceiptTable,
} from "@opencode-ai/core/session/prompt-queue.sql"
import { MessageTable } from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { and, asc, desc, eq, inArray, isNotNull, isNull, ne, sql } from "drizzle-orm"
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schema,
  Scope,
  Semaphore,
  Struct,
} from "effect"
import { isDeepStrictEqual } from "node:util"
import { EventV2Bridge } from "@/event-v2-bridge"
import { sqliteLockMessage } from "@/util/sqlite-error"
import { MachineMessage } from "./machine-message"
import { MessageV2 } from "./message-v2"
import { MessageID, PartID, SessionID } from "./schema"

type DatabaseService = Database.Interface["db"]
type DatabaseTransaction = Parameters<Parameters<DatabaseService["transaction"]>[0]>[0]
type DatabaseAccess = DatabaseService | DatabaseTransaction

// Durable V1 prompt queue (swxtchio/swx-opencode#68). Every prompt except noReply
// is admitted here before it becomes a V1 user message, so the loop decides when
// it reaches the model: a steer at the next safe step, a queued item only when
// the run would otherwise go idle.

export const Item = SessionPromptQueue.Item
export type Item = SessionPromptQueue.Item
export const ItemID = SessionPromptQueue.ItemID
export type ItemID = SessionPromptQueue.ItemID
export const Delivery = SessionPromptQueue.Delivery
export type Delivery = SessionPromptQueue.Delivery
export const Event = SessionPromptQueue.Event
export const Receipt = SessionPromptQueue.Receipt
export type Receipt = SessionPromptQueue.Receipt
export const RequestID = SessionPromptQueue.RequestID
export type RequestID = SessionPromptQueue.RequestID

export type AdmitInput = SessionPromptQueue.Input & { readonly sessionID: SessionID }
export type PromotedInput = SessionPromptQueue.QueuedInput & {
  readonly sessionID: SessionID
  readonly messageID: MessageID
}

/** A prompt's own item was withdrawn, by an editor, before it could be delivered. */
export class WithdrawnError extends Schema.TaggedErrorClass<WithdrawnError>()("SessionQueueWithdrawnError", {
  sessionID: Schema.String,
  itemID: Schema.String,
}) {}

/** A request admitted before was sent again with a different prompt (#212). */
export class RequestConflictError extends Schema.TaggedErrorClass<RequestConflictError>()(
  "SessionQueueRequestConflictError",
  {
    sessionID: Schema.String,
    requestID: Schema.String,
  },
) {}

/**
 * A consume whose write could not begin in any of its attempts, on another
 * connection's write lock or past the acquisition backstop. The run stops with
 * it, and every queued row stays as it was for the next wake.
 */
export class ConsumeLockedError extends Schema.TaggedErrorClass<ConsumeLockedError>()(
  "SessionQueueConsumeLockedError",
  {
    sessionID: Schema.String,
    attempts: Schema.Number,
    message: Schema.String,
  },
) {}

// Each consume attempt waits out the statement lock window, so a lock released
// within about this many windows lets the run go on.
const consumeAttempts = 5
// Bounds an attempt's wait for the session permit, the shared connection and
// BEGIN together: its own statement window plus one other statement's window
// ahead of it on the connection. The body, once begun, is not timed.
const consumeAcquireBackstop = Duration.seconds(10)
// A review hold's lease (#212). Its client renews it on activity and closes its
// view after 5 minutes without any, so a live view always renews in time; the
// lease only ends a hold whose client vanished without releasing it.
export const holdLease = Duration.minutes(6)
export type HoldEnd = "released" | "lapsed"

export interface Interface {
  readonly admit: (input: AdmitInput) => Effect.Effect<Item>
  /**
   * Admits a prompt, unless it names a request this session already has (#212).
   * The same prompt again is a retry: nothing is admitted, and `own` is a new
   * waiter's id that follows whatever carries the request now. A different
   * prompt under that request fails with RequestConflictError. An admitted
   * prompt then retires the pending items it positively replaces; see
   * `admission`. `markers` are the configured machine-message markers.
   */
  readonly request: (
    input: AdmitInput,
    markers?: ConfigV1.Info["machine_message_markers"],
  ) => Effect.Effect<
    | { readonly kind: "admitted"; readonly own: Item }
    | { readonly kind: "retry"; readonly own: ItemID; readonly receipt: Receipt },
    RequestConflictError
  >
  /** What became of a request; None when the session has no such request. */
  readonly receipt: (sessionID: SessionID, requestID: RequestID) => Effect.Effect<Option.Option<Receipt>>
  /** Fails when the prompt names a request this session has under a different prompt. */
  readonly check: (input: AdmitInput) => Effect.Effect<void, RequestConflictError>
  /** Records a request whose prompt became a message without being queued, as a noReply prompt does. */
  readonly delivered: (input: AdmitInput, messageID: MessageID) => Effect.Effect<void>
  /**
   * Prepare under the session lock, recheck run state around a direct write,
   * and queue it if a run starts while the message is being persisted.
   */
  readonly writeOrAdmit: <P extends SessionV1.WithParts, E>(input: {
    readonly admission: AdmitInput
    readonly forceQueue?: boolean
    readonly isBusy: Effect.Effect<boolean>
    readonly prepare: () => Effect.Effect<P, E>
    readonly write: (prepared: P) => Effect.Effect<SessionV1.WithParts>
    readonly discard: (message: SessionV1.WithParts) => Effect.Effect<void>
  }) => Effect.Effect<
    | { readonly kind: "direct"; readonly message: SessionV1.WithParts }
    | { readonly kind: "queued"; readonly own: Item; readonly prepared?: P },
    E
  >
  readonly list: (sessionID: SessionID) => Effect.Effect<Item[]>
  /**
   * Withdraws an item for editing so it cannot be delivered mid-edit, even while
   * its message is being prepared. None tells the editor that delivery won.
   */
  readonly withdraw: (sessionID: SessionID, itemID: ItemID) => Effect.Effect<Option.Option<Item>>
  /** Cancels an edit: the item regains its place, since its seq is kept. */
  readonly restore: (sessionID: SessionID, itemID: ItemID) => Effect.Effect<Option.Option<Item>>
  readonly update: (input: {
    readonly sessionID: SessionID
    readonly itemID: ItemID
    readonly delivery: Delivery
  }) => Effect.Effect<Option.Option<Item>>
  /**
   * Turns pending items into V1 user messages, and reports whether the queue
   * changed. `prepare` resolves a message with no lock held, since it may read
   * files or MCP resources; `write` then stores it under the session lock, and
   * only while the item is still reserved, so a withdraw that lands meanwhile
   * wins. Steers wait while a compaction is pending or running, so they never
   * become its parent or input, and queued items go one per call so the loop
   * reevaluates between their turns. An item that cannot become a message is
   * dropped: the caller's `own` item fails the call, any other goes to
   * `rejected`, so one prompt's failure never lands on another.
   */
  readonly promote: <P, E>(input: {
    readonly sessionID: SessionID
    readonly delivery: Delivery
    readonly prepare: (input: PromotedInput) => Effect.Effect<P, E>
    readonly write: (prepared: P) => Effect.Effect<unknown>
    readonly rejected: (cause: Cause.Cause<E>) => Effect.Effect<void>
    readonly own?: ItemID
    /** Stops once the session has any message, for a new session's first promotion. */
    readonly untilStarted?: boolean
  }) => Effect.Effect<boolean, E>
  /**
   * Called by a drain before it reads history; returns the wake count that read
   * reflects, for `park`. A promoted row outlives its promotion until then so
   * that a joiner of a finishing run can see the prompt still needs a drain.
   * Another connection's write lock can outlast one attempt: an attempt whose
   * transaction never began, on the lock or past its acquisition backstop, is
   * made again, and the last such attempt dies with ConsumeLockedError.
   */
  readonly consume: (sessionID: SessionID) => Effect.Effect<number>
  /**
   * A run that stopped on an abort or error must not restart until something
   * wakes the session. Given the wake count its last history read reflected, it
   * parks only if nothing has woken the session since, so an admission its
   * decision never saw is not parked by it.
   */
  readonly park: (sessionID: SessionID, seen?: number) => Effect.Effect<void>
  /** Whether a stopped run parked the session and nothing has woken it since. */
  readonly isParked: (sessionID: SessionID) => Effect.Effect<boolean>
  /** The joiner re-check's signal: admitted work no drain has read yet, on a session that is not parked. */
  readonly awaitingDrain: (sessionID: SessionID) => Effect.Effect<boolean>
  readonly withdrawn: (sessionID: SessionID, itemID: ItemID) => Effect.Effect<boolean>
  /** Whether no drain has read this item yet (withdrawn items excluded). */
  readonly unread: (sessionID: SessionID, itemID: ItemID) => Effect.Effect<boolean>
  /**
   * Ends a turn: `reply`, the turn's final message, answers every waiting caller
   * whose prompt became a message at or before `turn`, the user message the turn
   * answered, and that no earlier turn answered. Those are the prompt that began
   * the turn, the steers that joined it, and a queued prompt whose turn it was.
   */
  readonly answer: (sessionID: SessionID, turn: MessageID, reply: SessionV1.WithParts) => Effect.Effect<void>
  /**
   * The answer to an item's prompt, once. Each caller's answer channel is opened
   * at admission and closed when the caller `forget`s it, however it returns, so
   * an interrupted one leaves nothing behind.
   */
  readonly reply: (itemID: ItemID) => Effect.Effect<SessionV1.WithParts | undefined>
  readonly forget: (itemID: ItemID) => Effect.Effect<void>
  /** Whether the session has no message at all yet. */
  readonly empty: (sessionID: SessionID) => Effect.Effect<boolean>
  /** For writers of user messages, such as compaction, that a promotion must not interleave with. */
  readonly exclusive: <A, E, R>(sessionID: SessionID, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  /** Runs a compaction; no steer is promoted until it and its follow-up messages are written. */
  readonly whileCompacting: <A, E, R>(sessionID: SessionID, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  /**
   * Starts or renews `holder`'s review hold (#212). While a session has a live
   * hold none of its items is promoted, including one whose promotion was
   * already being prepared, so the returned pending list stays as it is until a
   * release. `started` reports that this call began the session's hold.
   */
  readonly hold: (
    sessionID: SessionID,
    holder: string,
  ) => Effect.Effect<{ readonly started: boolean; readonly items: Item[] }>
  /** Ends `holder`'s hold; true when that ended the session's hold. */
  readonly release: (sessionID: SessionID, holder: string) => Effect.Effect<boolean>
  /**
   * Waits until the session has no live hold, and reports how it ended: by a
   * release, or by its last lease lapsing.
   */
  readonly released: (sessionID: SessionID) => Effect.Effect<HoldEnd>
  /** Deletes a pending item for good. None tells the caller it was not pending. */
  readonly remove: (sessionID: SessionID, itemID: ItemID) => Effect.Effect<Option.Option<Item>>
  /**
   * Gives the listed pending items their seqs back in the listed order, so they
   * promote in that order; a category still decides when an item promotes. None,
   * with nothing changed, when any of them is not pending.
   */
  readonly reorder: (sessionID: SessionID, itemIDs: readonly ItemID[]) => Effect.Effect<Option.Option<Item[]>>
  /**
   * Keeps the first of identical pending items, then merges similar commands of
   * one category into the first of them as numbered steps; see `consolidation`.
   * `markers` are the configured machine-message markers, whose messages never
   * merge as commands. Returns the resulting pending list.
   */
  readonly consolidate: (
    sessionID: SessionID,
    markers?: ConfigV1.Info["machine_message_markers"],
  ) => Effect.Effect<Item[]>
  /**
   * The messages with a synthetic note, on each one a queued prompt became, of
   * when that prompt was queued and delivered (#212). Only the model request
   * carries it; the stored message stays as the sender sent it.
   */
  readonly annotate: (
    sessionID: SessionID,
    messages: readonly SessionV1.WithParts[],
  ) => Effect.Effect<SessionV1.WithParts[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionQueue") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const database = yield* Database.Service
    const { db } = database
    const scope = yield* Scope.Scope
    const events = yield* EventV2Bridge.Service
    // `locks` guard every mutation and its publication and are held only for
    // bounded database work. `order` keeps a session's promotions in seq order
    // across their unlocked preparation; nothing else waits on it.
    const locks = new Map<SessionID, Semaphore.Semaphore>()
    const order = new Map<SessionID, Semaphore.Semaphore>()
    const parked = new Set<SessionID>()
    const wakes = new Map<SessionID, number>()
    const running = new Set<SessionID>()
    // Waiting callers' answer channels, by item: the message the item became,
    // then the final reply of the turn that message joined.
    const channels = new Map<
      ItemID,
      { readonly sessionID: SessionID; message?: MessageID; reply?: SessionV1.WithParts }
    >()
    // A waiter whose item was merged or superseded, or a retry's waiter, follows
    // the item that now carries its request (#212): waiter id to that item.
    const follows = new Map<ItemID, ItemID>()
    const carrier = (itemID: ItemID) => follows.get(itemID) ?? itemID
    // Review holds by session (#212), process-local like the drains they hold:
    // each holder's lease deadline, and the signal that the hold ended.
    const holds = new Map<
      SessionID,
      { readonly leases: Map<string, number>; readonly ended: Deferred.Deferred<HoldEnd> }
    >()

    const semaphore = (map: Map<SessionID, Semaphore.Semaphore>, sessionID: SessionID) => {
      const hit = map.get(sessionID)
      if (hit) return hit
      const next = Semaphore.makeUnsafe(1)
      map.set(sessionID, next)
      return next
    }

    // The write lock is taken at BEGIN. A deferred transaction would pin a read
    // snapshot in assertWritableIn, and a commit by another writer while its
    // write waits on the lock would leave that snapshot stale.
    const writable = <A, E>(sessionID: SessionID, effect: (tx: DatabaseTransaction) => Effect.Effect<A, E>) =>
      db.transaction(
        (tx) =>
          Effect.gen(function* () {
            yield* EventV2.assertWritableIn(tx, sessionID)
            return yield* effect(tx)
          }),
        { behavior: "immediate" },
      )

    const exclusive: Interface["exclusive"] = (sessionID, effect) => semaphore(locks, sessionID).withPermit(effect)

    // Admission, restore and send-now wake the session; called under its lock.
    const wake = (sessionID: SessionID) => {
      parked.delete(sessionID)
      wakes.set(sessionID, (wakes.get(sessionID) ?? 0) + 1)
    }

    const pending = (sessionID: SessionID) =>
      and(
        eq(SessionPromptQueueTable.session_id, sessionID),
        isNull(SessionPromptQueueTable.time_promoted),
        isNull(SessionPromptQueueTable.time_withdrawn),
      )

    const list = Effect.fn("SessionQueue.list")(function* (sessionID: SessionID) {
      const rows = yield* db
        .transaction((tx) =>
          Effect.gen(function* () {
            yield* EventV2.assertReplayableIn(tx, sessionID)
            return yield* tx
              .select()
              .from(SessionPromptQueueTable)
              .where(pending(sessionID))
              .orderBy(asc(SessionPromptQueueTable.seq))
              .all()
              .pipe(Effect.orDie)
          }),
        )
        .pipe(Effect.orDie)
      return [...rows.filter((row) => row.delivery === "steer"), ...rows.filter((row) => row.delivery !== "steer")].map(
        fromRow,
      )
    })

    const publish = Effect.fn("SessionQueue.publish")(function* (sessionID: SessionID) {
      yield* events.publish(Event.Updated, { sessionID, items: yield* list(sessionID) })
    })

    // Every mutation publishes its resulting list under the session's lock, so
    // listeners see the lists in the order the mutations happened.
    const admit = Effect.fn("SessionQueue.admit")(function* (input: AdmitInput) {
      const admitted = yield* request(input).pipe(Effect.orDie)
      if (admitted.kind === "retry")
        return yield* Effect.die(new Error(`Request ${input.requestID} was already admitted to ${input.sessionID}`))
      return admitted.own
    })

    const admitLocked = Effect.fnUntraced(function* (
      input: AdmitInput,
      markers?: ConfigV1.Info["machine_message_markers"],
    ) {
      const now = Date.now()
      const admitted = yield* writable(input.sessionID, (tx) =>
        Effect.gen(function* () {
          const allocated = yield* tx
            .insert(SessionPromptQueueSequenceTable)
            .values({ session_id: input.sessionID, seq: 1 })
            .onConflictDoUpdate({
              target: SessionPromptQueueSequenceTable.session_id,
              set: { seq: sql`${SessionPromptQueueSequenceTable.seq} + 1` },
            })
            .returning({ seq: SessionPromptQueueSequenceTable.seq })
            .get()
          if (!allocated) return yield* Effect.die(new Error(`No queue seq allocated for ${input.sessionID}`))
          const row = yield* tx
            .insert(SessionPromptQueueTable)
            .values({
              id: ItemID.create(),
              session_id: input.sessionID,
              seq: allocated.seq,
              delivery: input.delivery ?? "steer",
              request_id: input.requestID,
              input: encodeInput(Struct.omit(input, ["sessionID", "noReply", "delivery", "requestID"])),
              time_created: now,
              origins: [input.requestID ? { time: now, requestID: input.requestID } : { time: now }],
            })
            .returning()
            .get()
          if (row && input.requestID)
            yield* tx
              .insert(SessionPromptReceiptTable)
              .values({
                session_id: input.sessionID,
                request_id: input.requestID,
                state: "pending",
                item_id: row.id,
                fingerprint: fingerprint(input),
                time_created: now,
                time_updated: now,
              })
              .run()
          if (!row) return undefined
          return { row, ...(yield* replace(tx, input.sessionID, row, markers)) }
        }),
      ).pipe(Effect.orDie)
      if (!admitted) return yield* Effect.die(new Error(`Queue admission for ${input.sessionID} stored nothing`))
      channels.set(admitted.row.id, { sessionID: input.sessionID })
      retire(admitted.retired, admitted.row.id)
      if (admitted.into) retire([admitted.row.id], admitted.into)
      finish(admitted.finished)
      wake(input.sessionID)
      yield* publish(input.sessionID)
      return fromRow(admitted.row)
    })

    // Applies `admission` for a just-admitted row, in its transaction: the row
    // takes in identical copies and repeated peer requests and retires the
    // notices it supersedes, taking the place of the oldest of them, and then
    // joins a command it continues as that command's next step.
    const replace = Effect.fnUntraced(function* (
      tx: DatabaseTransaction,
      sessionID: SessionID,
      added: typeof SessionPromptQueueTable.$inferSelect,
      markers?: ConfigV1.Info["machine_message_markers"],
    ) {
      const pendingOf = () =>
        tx
          .select()
          .from(SessionPromptQueueTable)
          .where(and(pending(sessionID), eq(SessionPromptQueueTable.delivery, added.delivery)))
          .orderBy(asc(SessionPromptQueueTable.seq))
          .all()
          .pipe(Effect.orDie)
      const first = admission((yield* pendingOf()).map(fromRow), fromRow(added), markers)
      if (first.absorbed.length + first.superseded.length === 0 && !first.into)
        return {
          retired: [] as ItemID[],
          into: undefined,
          finished: [] as { readonly id: ItemID; readonly message: MessageID }[],
        }
      // A promotion being prepared for any of them must prepare again.
      const finished = yield* invalidate(tx, sessionID)
      const rows = yield* pendingOf()
      const plan = admission(rows.map(fromRow), fromRow(added), markers)
      const retired = [...plan.absorbed, ...plan.superseded]
      if (retired.length > 0) {
        yield* tx
          .delete(SessionPromptQueueTable)
          .where(inArray(SessionPromptQueueTable.id, retired))
          .run()
          .pipe(Effect.orDie)
        const absorbed = rows.filter((row) => plan.absorbed.includes(row.id))
        yield* tx
          .update(SessionPromptQueueTable)
          .set({
            seq: Math.min(added.seq, ...rows.filter((row) => retired.includes(row.id)).map((row) => row.seq)),
            origins: [...absorbed, added].map(origins).reduce(repeated),
          })
          .where(eq(SessionPromptQueueTable.id, added.id))
          .run()
          .pipe(Effect.orDie)
        if (plan.superseded.length > 0)
          yield* settleReceipts(tx, sessionID, plan.superseded, { kind: "superseded", by: added.request_id! })
        if (absorbed.length > 0)
          yield* settleReceipts(tx, sessionID, plan.absorbed, { kind: "merged", into: (yield* rowOf(tx, added.id))! })
      }
      const survivor = (yield* rowOf(tx, added.id))!
      const head = plan.into ? rows.find((row) => row.id === plan.into) : undefined
      if (!head) return { retired, into: undefined, finished, row: survivor }
      const merged = consolidation([fromRow(head), fromRow(survivor)], markers).changed.find(
        (item) => item.id === head.id,
      )!
      yield* tx
        .update(SessionPromptQueueTable)
        .set({ input: encodeInput(merged.input), origins: [...merged.origins], time_created: merged.time.created })
        .where(eq(SessionPromptQueueTable.id, head.id))
        .run()
        .pipe(Effect.orDie)
      yield* tx
        .delete(SessionPromptQueueTable)
        .where(eq(SessionPromptQueueTable.id, survivor.id))
        .run()
        .pipe(Effect.orDie)
      yield* settleReceipts(tx, sessionID, [survivor.id], { kind: "merged", into: (yield* rowOf(tx, head.id))! })
      return { retired, into: head.id, finished, row: survivor }
    })

    const rowOf = (tx: DatabaseTransaction, itemID: ItemID) =>
      tx.select().from(SessionPromptQueueTable).where(eq(SessionPromptQueueTable.id, itemID)).get().pipe(Effect.orDie)

    const receiptRow = (sessionID: SessionID, requestID: RequestID) =>
      db
        .select()
        .from(SessionPromptReceiptTable)
        .where(
          and(eq(SessionPromptReceiptTable.session_id, sessionID), eq(SessionPromptReceiptTable.request_id, requestID)),
        )
        .get()
        .pipe(Effect.orDie)

    const receipt: Interface["receipt"] = (sessionID, requestID) =>
      receiptRow(sessionID, requestID).pipe(Effect.map((row) => Option.fromNullishOr(row).pipe(Option.map(toReceipt))))

    const request: Interface["request"] = (input, markers) =>
      exclusive(
        input.sessionID,
        Effect.gen(function* () {
          const prior = input.requestID ? yield* receiptRow(input.sessionID, input.requestID) : undefined
          if (!prior) return { kind: "admitted" as const, own: yield* admitLocked(input, markers) }
          if (prior.fingerprint !== fingerprint(input))
            return yield* new RequestConflictError({ sessionID: input.sessionID, requestID: prior.request_id })
          return { kind: "retry" as const, own: yield* follow(prior), receipt: toReceipt(prior) }
        }),
      )

    // A retry's waiter: it follows the item carrying the request, or a newer
    // request that superseded it, or takes the message the request became.
    const follow = Effect.fnUntraced(function* (start: typeof SessionPromptReceiptTable.$inferSelect) {
      const own = ItemID.create()
      const channel: { readonly sessionID: SessionID; message?: MessageID } = { sessionID: start.session_id }
      channels.set(own, channel)
      const seen = new Set<string>()
      let row: typeof SessionPromptReceiptTable.$inferSelect | undefined = start
      while (row && !seen.has(row.request_id)) {
        seen.add(row.request_id)
        if (row.message_id) channel.message = row.message_id
        if (!row.message_id && row.item_id && (row.state === "pending" || row.state === "merged"))
          follows.set(own, row.item_id)
        if (row.state !== "superseded" || !row.by_request_id) return own
        row = yield* receiptRow(start.session_id, row.by_request_id)
      }
      return own
    })

    const check: Interface["check"] = (input) =>
      input.requestID
        ? receiptRow(input.sessionID, input.requestID).pipe(
            Effect.flatMap((prior) =>
              prior && prior.fingerprint !== fingerprint(input)
                ? Effect.fail(new RequestConflictError({ sessionID: input.sessionID, requestID: prior.request_id }))
                : Effect.void,
            ),
          )
        : Effect.void

    const delivered: Interface["delivered"] = (input, messageID) =>
      input.requestID
        ? writable(input.sessionID, (tx) =>
            tx
              .insert(SessionPromptReceiptTable)
              .values({
                session_id: input.sessionID,
                request_id: input.requestID!,
                state: "delivered",
                message_id: messageID,
                fingerprint: fingerprint(input),
                time_created: Date.now(),
                time_updated: Date.now(),
              })
              .onConflictDoNothing()
              .run(),
          ).pipe(Effect.orDie, Effect.asVoid)
        : Effect.void

    // Every receipt riding on an item takes its fate. A merge makes the
    // survivor's request the one they were merged into; a survivor that has none
    // takes over the first request merged into it, so it still has a receipt.
    const settleReceipts = Effect.fnUntraced(function* (
      tx: DatabaseTransaction,
      sessionID: SessionID,
      items: readonly ItemID[],
      fate:
        | { readonly kind: "merged"; readonly into: typeof SessionPromptQueueTable.$inferSelect }
        | { readonly kind: "superseded"; readonly by: RequestID }
        | { readonly kind: "deleted" },
    ) {
      if (items.length === 0) return
      const now = Date.now()
      const riding = and(
        eq(SessionPromptReceiptTable.session_id, sessionID),
        inArray(SessionPromptReceiptTable.item_id, [...items]),
      )
      if (fate.kind === "deleted") {
        yield* tx
          .update(SessionPromptReceiptTable)
          .set({ state: "deleted", item_id: null, time_updated: now })
          .where(riding)
          .run()
          .pipe(Effect.orDie)
        return
      }
      if (fate.kind === "superseded") {
        yield* tx
          .update(SessionPromptReceiptTable)
          .set({ state: "superseded", by_request_id: fate.by, item_id: null, time_updated: now })
          .where(riding)
          .run()
          .pipe(Effect.orDie)
        return
      }
      const own =
        fate.into.request_id ??
        (yield* tx
          .select({ request_id: SessionPromptReceiptTable.request_id })
          .from(SessionPromptReceiptTable)
          .where(and(riding, eq(SessionPromptReceiptTable.state, "pending")))
          .orderBy(asc(SessionPromptReceiptTable.time_created))
          .limit(1)
          .get()
          .pipe(Effect.orDie))?.request_id
      if (!own) return
      if (!fate.into.request_id)
        yield* tx
          .update(SessionPromptQueueTable)
          .set({ request_id: own })
          .where(eq(SessionPromptQueueTable.id, fate.into.id))
          .run()
          .pipe(Effect.orDie)
      yield* tx
        .update(SessionPromptReceiptTable)
        .set({ state: "pending", item_id: fate.into.id, into_request_id: null, time_updated: now })
        .where(and(eq(SessionPromptReceiptTable.session_id, sessionID), eq(SessionPromptReceiptTable.request_id, own)))
        .run()
        .pipe(Effect.orDie)
      yield* tx
        .update(SessionPromptReceiptTable)
        .set({ state: "merged", into_request_id: own, item_id: fate.into.id, time_updated: now })
        .where(and(riding, ne(SessionPromptReceiptTable.request_id, own)))
        .run()
        .pipe(Effect.orDie)
    })

    // A waiter on a retired item now waits on its survivor, and so does anyone
    // already following it.
    const retire = (items: readonly ItemID[], survivor: ItemID) =>
      items.forEach((item) => {
        follows.forEach((target, waiter) => {
          if (target === item) follows.set(waiter, survivor)
        })
        if (channels.has(item)) follows.set(item, survivor)
      })

    const writeOrAdmit: Interface["writeOrAdmit"] = (input) =>
      exclusive(
        input.admission.sessionID,
        Effect.gen(function* () {
          const busy = input.forceQueue === true || (yield* input.isBusy)
          const existingRow = input.admission.messageID
            ? yield* db
                .select({ session_id: MessageTable.session_id })
                .from(MessageTable)
                .where(eq(MessageTable.id, input.admission.messageID))
                .get()
                .pipe(Effect.orDie)
            : undefined
          if (existingRow) {
            const prepared = yield* input.prepare()
            const existing =
              existingRow.session_id === input.admission.sessionID && input.admission.messageID
                ? yield* MessageV2.get({
                    sessionID: input.admission.sessionID,
                    messageID: input.admission.messageID,
                  }).pipe(
                    Effect.provideService(Database.Service, database),
                    Effect.catchTag("NotFoundError", () => Effect.succeed(undefined)),
                  )
                : undefined
            if (existing && sameMessage(existing, prepared)) return { kind: "direct" as const, message: existing }
            const duplicate = input.admission.messageID
              ? (yield* list(input.admission.sessionID)).find(
                  (item) =>
                    item.input.messageID === input.admission.messageID && sameQueuedInput(item.input, input.admission),
                )
              : undefined
            if (duplicate) return { kind: "queued" as const, own: duplicate, prepared }
            const active = busy || (yield* input.isBusy)
            if (existing && !active) return { kind: "direct" as const, message: existing }
            if (existing && active)
              return {
                kind: "queued" as const,
                own: yield* admitLocked(input.admission),
                prepared,
              }
            const messageID = MessageID.ascending()
            const admission = { ...input.admission, messageID }
            const rekeyed = rekeyMessage(prepared, messageID)
            if (active) return { kind: "queued" as const, own: yield* admitLocked(admission), prepared: rekeyed }
            const message = yield* input.write(rekeyed)
            if (yield* input.isBusy) {
              yield* input.discard(message)
              return { kind: "queued" as const, own: yield* admitLocked(admission), prepared: rekeyed }
            }
            return { kind: "direct" as const, message }
          }
          if (busy)
            return {
              kind: "queued" as const,
              own: yield* admitLocked(input.admission),
            }
          const prepared = yield* input.prepare()
          if (yield* input.isBusy)
            return {
              kind: "queued" as const,
              own: yield* admitLocked(input.admission),
              prepared,
            }
          const message = yield* input.write(prepared)
          if (yield* input.isBusy) {
            yield* input.discard(message)
            return {
              kind: "queued" as const,
              own: yield* admitLocked(input.admission),
              prepared,
            }
          }
          return { kind: "direct" as const, message }
        }),
      )

    const withdraw = Effect.fn("SessionQueue.withdraw")(function* (sessionID: SessionID, itemID: ItemID) {
      return yield* exclusive(
        sessionID,
        Effect.gen(function* () {
          const row = yield* writable(sessionID, (tx) =>
            tx
              .update(SessionPromptQueueTable)
              .set({ time_withdrawn: Date.now(), message_id: null })
              .where(and(eq(SessionPromptQueueTable.id, itemID), pending(sessionID)))
              .returning()
              .get()
              .pipe(Effect.orDie),
          ).pipe(Effect.orDie)
          if (!row) return Option.none()
          yield* publish(sessionID)
          return Option.some(fromRow(row))
        }),
      )
    })

    const restore = Effect.fn("SessionQueue.restore")(function* (sessionID: SessionID, itemID: ItemID) {
      return yield* exclusive(sessionID, restoreLocked(sessionID, itemID))
    })

    const restoreLocked = Effect.fnUntraced(function* (sessionID: SessionID, itemID: ItemID) {
      const row = yield* writable(sessionID, (tx) =>
        tx
          .update(SessionPromptQueueTable)
          .set({ time_withdrawn: null })
          .where(
            and(
              eq(SessionPromptQueueTable.id, itemID),
              eq(SessionPromptQueueTable.session_id, sessionID),
              isNotNull(SessionPromptQueueTable.time_withdrawn),
            ),
          )
          .returning()
          .get()
          .pipe(Effect.orDie),
      ).pipe(Effect.orDie)
      if (!row) return Option.none()
      wake(sessionID)
      yield* publish(sessionID)
      return Option.some(fromRow(row))
    })

    const update = Effect.fn("SessionQueue.update")(function* (input: {
      readonly sessionID: SessionID
      readonly itemID: ItemID
      readonly delivery: Delivery
    }) {
      return yield* exclusive(input.sessionID, updateLocked(input))
    })

    const updateLocked = Effect.fnUntraced(function* (input: {
      readonly sessionID: SessionID
      readonly itemID: ItemID
      readonly delivery: Delivery
    }) {
      const row = yield* writable(input.sessionID, (tx) =>
        tx
          .update(SessionPromptQueueTable)
          .set({ delivery: input.delivery })
          .where(and(eq(SessionPromptQueueTable.id, input.itemID), pending(input.sessionID)))
          .returning()
          .get()
          .pipe(Effect.orDie),
      ).pipe(Effect.orDie)
      if (!row) return Option.none()
      wake(input.sessionID)
      yield* publish(input.sessionID)
      return Option.some(fromRow(row))
    })

    const held = (sessionID: SessionID) =>
      Clock.currentTimeMillis.pipe(
        Effect.map((now) => [...(holds.get(sessionID)?.leases.values() ?? [])].some((until) => until > now)),
      )

    // Drops lapsed leases and ends a hold left without a live one; under the lock.
    const settle = Effect.fnUntraced(function* (sessionID: SessionID, end: HoldEnd) {
      const current = holds.get(sessionID)
      if (!current) return undefined
      const now = yield* Clock.currentTimeMillis
      current.leases.forEach((until, holder) => {
        if (until <= now) current.leases.delete(holder)
      })
      if (current.leases.size > 0) return { ended: current.ended, wait: Math.min(...current.leases.values()) - now }
      holds.delete(sessionID)
      yield* Deferred.succeed(current.ended, end)
      return undefined
    })

    // Drops every pending row's reservation, so a promotion prepared before a
    // hold, reorder or consolidation can no longer commit; its loop reserves
    // again under the new state. A reservation whose message already landed, left
    // by a stopped process, is finished instead. Under the session lock, in `tx`.
    const invalidate = Effect.fnUntraced(function* (tx: DatabaseTransaction, sessionID: SessionID) {
      const reserved = yield* tx
        .select()
        .from(SessionPromptQueueTable)
        .where(and(pending(sessionID), isNotNull(SessionPromptQueueTable.message_id)))
        .all()
        .pipe(Effect.orDie)
      return yield* Effect.forEach(reserved, (row) =>
        Effect.gen(function* () {
          const done = yield* landed(tx, row.message_id!)
          if (done) yield* promoted(tx, row, row.message_id!)
          if (!done)
            yield* tx
              .update(SessionPromptQueueTable)
              .set({ message_id: null })
              .where(eq(SessionPromptQueueTable.id, row.id))
              .run()
              .pipe(Effect.orDie)
          return done ? [{ id: row.id, message: row.message_id! }] : []
        }),
      ).pipe(Effect.map((rows) => rows.flat()))
    })

    const finish = (finished: readonly { readonly id: ItemID; readonly message: MessageID }[]) =>
      finished.forEach((row) => deliver(row.id, row.message))

    // The message an item became answers its own waiter and every waiter following it.
    const deliver = (itemID: ItemID, message: MessageID) =>
      channels.forEach((channel, waiter) => {
        if (carrier(waiter) === itemID) channel.message = message
      })

    const hold: Interface["hold"] = (sessionID, holder) =>
      exclusive(
        sessionID,
        Effect.gen(function* () {
          const current = (yield* settle(sessionID, "lapsed")) ? holds.get(sessionID) : undefined
          const until = (yield* Clock.currentTimeMillis) + Duration.toMillis(holdLease)
          if (current) current.leases.set(holder, until)
          if (!current) {
            holds.set(sessionID, { leases: new Map([[holder, until]]), ended: yield* Deferred.make<HoldEnd>() })
            const finished = yield* writable(sessionID, (tx) => invalidate(tx, sessionID)).pipe(Effect.orDie)
            finish(finished)
            if (finished.length > 0) yield* publish(sessionID)
          }
          return { started: !current, items: yield* list(sessionID) }
        }),
      ).pipe(Effect.withSpan("SessionQueue.hold"))

    const release: Interface["release"] = (sessionID, holder) =>
      exclusive(
        sessionID,
        Effect.gen(function* () {
          const current = holds.get(sessionID)
          if (!current?.leases.delete(holder)) return false
          return (yield* settle(sessionID, "released")) === undefined
        }),
      ).pipe(Effect.withSpan("SessionQueue.release"))

    // A renewed lease outlasts the deadline this waits for, so it settles again.
    const released: Interface["released"] = (sessionID) =>
      Effect.gen(function* () {
        while (true) {
          const current = holds.get(sessionID)
          const live = yield* exclusive(sessionID, settle(sessionID, "lapsed"))
          if (!live) return current ? yield* Deferred.await(current.ended) : ("released" as const)
          yield* Deferred.await(live.ended).pipe(Effect.timeoutOption(Duration.millis(live.wait)))
        }
      })

    const remove = Effect.fn("SessionQueue.remove")(function* (sessionID: SessionID, itemID: ItemID) {
      return yield* exclusive(
        sessionID,
        Effect.gen(function* () {
          const row = yield* writable(sessionID, (tx) =>
            Effect.gen(function* () {
              const row = yield* tx
                .delete(SessionPromptQueueTable)
                .where(and(eq(SessionPromptQueueTable.id, itemID), pending(sessionID)))
                .returning()
                .get()
                .pipe(Effect.orDie)
              if (row) yield* settleReceipts(tx, sessionID, [row.id], { kind: "deleted" })
              return row
            }),
          ).pipe(Effect.orDie)
          if (!row) return Option.none()
          yield* publish(sessionID)
          return Option.some(fromRow(row))
        }),
      )
    })

    const reorder = Effect.fn("SessionQueue.reorder")(function* (sessionID: SessionID, itemIDs: readonly ItemID[]) {
      return yield* exclusive(
        sessionID,
        Effect.gen(function* () {
          const reordered = yield* writable(sessionID, (tx) =>
            Effect.gen(function* () {
              const rows = yield* tx
                .select()
                .from(SessionPromptQueueTable)
                .where(and(pending(sessionID), inArray(SessionPromptQueueTable.id, [...itemIDs])))
                .all()
                .pipe(Effect.orDie)
              if (rows.length !== itemIDs.length || new Set(itemIDs).size !== itemIDs.length) return undefined
              const finished = yield* invalidate(tx, sessionID)
              const seqs = rows.map((row) => row.seq).toSorted((a, b) => a - b)
              // (session_id, seq) is unique, so every row first moves to a free negative seq.
              yield* Effect.forEach(rows, (row) =>
                tx
                  .update(SessionPromptQueueTable)
                  .set({ seq: -row.seq })
                  .where(eq(SessionPromptQueueTable.id, row.id))
                  .run()
                  .pipe(Effect.orDie),
              )
              yield* Effect.forEach(itemIDs, (id, index) =>
                tx
                  .update(SessionPromptQueueTable)
                  .set({ seq: seqs[index] })
                  .where(eq(SessionPromptQueueTable.id, id))
                  .run()
                  .pipe(Effect.orDie),
              )
              return finished
            }),
          ).pipe(Effect.orDie)
          if (!reordered) return Option.none()
          finish(reordered)
          yield* publish(sessionID)
          return Option.some(yield* list(sessionID))
        }),
      )
    })

    // A promotion being prepared loses its reservation, so its item takes part
    // like any other and its stale message never lands.
    const consolidate = Effect.fn("SessionQueue.consolidate")(function* (
      sessionID: SessionID,
      markers?: ConfigV1.Info["machine_message_markers"],
    ) {
      return yield* exclusive(
        sessionID,
        Effect.gen(function* () {
          const changed = yield* writable(sessionID, (tx) =>
            Effect.gen(function* () {
              const finished = yield* invalidate(tx, sessionID)
              const rows = yield* tx
                .select()
                .from(SessionPromptQueueTable)
                .where(pending(sessionID))
                .orderBy(asc(SessionPromptQueueTable.seq))
                .all()
                .pipe(Effect.orDie)
              const plan = consolidation(
                [
                  ...rows.filter((row) => row.delivery === "steer"),
                  ...rows.filter((row) => row.delivery !== "steer"),
                ].map(fromRow),
                markers,
              )
              if (plan.removed.length > 0)
                yield* tx
                  .delete(SessionPromptQueueTable)
                  .where(inArray(SessionPromptQueueTable.id, plan.removed))
                  .run()
                  .pipe(Effect.orDie)
              yield* Effect.forEach(plan.changed, (changed) =>
                tx
                  .update(SessionPromptQueueTable)
                  .set({
                    input: encodeInput(changed.input),
                    origins: [...changed.origins],
                    time_created: changed.time.created,
                  })
                  .where(eq(SessionPromptQueueTable.id, changed.id))
                  .run()
                  .pipe(Effect.orDie),
              )
              yield* Effect.forEach(plan.into, (merge) =>
                Effect.gen(function* () {
                  const into = yield* tx
                    .select()
                    .from(SessionPromptQueueTable)
                    .where(eq(SessionPromptQueueTable.id, merge.into))
                    .get()
                    .pipe(Effect.orDie)
                  if (into) yield* settleReceipts(tx, sessionID, merge.items, { kind: "merged", into })
                }),
              )
              finish(finished)
              return { changed: plan.removed.length > 0 || finished.length > 0, into: plan.into }
            }),
          ).pipe(Effect.orDie)
          changed.into.forEach((merge) => retire(merge.items, merge.into))
          if (changed.changed) yield* publish(sessionID)
          return yield* list(sessionID)
        }),
      )
    })

    const compacting = (sessionID: SessionID) =>
      running.has(sessionID)
        ? Effect.succeed(true)
        : Effect.gen(function* () {
            const history = yield* MessageV2.snapshot(sessionID).pipe(Effect.provideService(Database.Service, database))
            return openTasks(history.messages, {
              admissionOrder: history.admissionOrder,
              excludeNoReply: true,
            }).some((task) => task.type === "compaction")
          })

    const whileCompacting: Interface["whileCompacting"] = (sessionID, effect) =>
      exclusive(
        sessionID,
        Effect.sync(() => void running.add(sessionID)),
      ).pipe(
        Effect.andThen(effect),
        Effect.ensuring(
          exclusive(
            sessionID,
            Effect.sync(() => void running.delete(sessionID)),
          ),
        ),
      )

    const oldest = (query: DatabaseAccess, sessionID: SessionID, delivery: Delivery) =>
      query
        .select()
        .from(SessionPromptQueueTable)
        .where(and(pending(sessionID), eq(SessionPromptQueueTable.delivery, delivery)))
        .orderBy(asc(SessionPromptQueueTable.seq))
        .limit(1)
        .get()
        .pipe(Effect.orDie)

    // A promoted message sorts after the session's history: V1 clients order
    // messages by id, so a supplied id is kept only while it is still the newest.
    const messageID = Effect.fnUntraced(function* (
      query: DatabaseAccess,
      sessionID: SessionID,
      supplied: MessageID | undefined,
    ) {
      if (!supplied) return MessageID.ascending()
      const newest = yield* query
        .select({ id: MessageTable.id })
        .from(MessageTable)
        .where(eq(MessageTable.session_id, sessionID))
        .orderBy(desc(MessageTable.id))
        .limit(1)
        .get()
        .pipe(Effect.orDie)
      return !newest || supplied > newest.id ? supplied : MessageID.ascending()
    })

    const landed = (query: DatabaseAccess, id: MessageID) =>
      query
        .select({ id: MessageTable.id })
        .from(MessageTable)
        .where(eq(MessageTable.id, id))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => row !== undefined),
        )

    // A promoted row's queue times outlive it as its message's provenance.
    const promoted = (tx: DatabaseTransaction, row: typeof SessionPromptQueueTable.$inferSelect, message: MessageID) =>
      Effect.gen(function* () {
        yield* tx
          .update(SessionPromptQueueTable)
          .set({ time_promoted: Date.now() })
          .where(eq(SessionPromptQueueTable.id, row.id))
          .run()
          .pipe(Effect.orDie)
        yield* tx
          .insert(SessionPromptProvenanceTable)
          .values({ message_id: message, session_id: row.session_id, origins: origins(row) })
          .onConflictDoNothing()
          .run()
          .pipe(Effect.orDie)
        const riding = and(
          eq(SessionPromptReceiptTable.session_id, row.session_id),
          eq(SessionPromptReceiptTable.item_id, row.id),
        )
        yield* tx
          .update(SessionPromptReceiptTable)
          .set({ message_id: message, time_updated: Date.now() })
          .where(riding)
          .run()
          .pipe(Effect.orDie)
        yield* tx
          .update(SessionPromptReceiptTable)
          .set({ state: "delivered" })
          .where(and(riding, eq(SessionPromptReceiptTable.state, "pending")))
          .run()
          .pipe(Effect.orDie)
      })

    const markPromoted = (sessionID: SessionID, itemID: ItemID, id: MessageID) =>
      writable(sessionID, (tx) =>
        Effect.gen(function* () {
          const row = yield* tx
            .select()
            .from(SessionPromptQueueTable)
            .where(eq(SessionPromptQueueTable.id, itemID))
            .get()
            .pipe(Effect.orDie)
          if (row) yield* promoted(tx, row, id)
        }),
      ).pipe(
        Effect.orDie,
        Effect.tap(() => Effect.sync(() => deliver(itemID, id))),
      )

    const drop = (sessionID: SessionID, itemID: ItemID) =>
      writable(sessionID, (tx) =>
        Effect.gen(function* () {
          yield* tx.delete(SessionPromptQueueTable).where(eq(SessionPromptQueueTable.id, itemID)).run()
          yield* settleReceipts(tx, sessionID, [itemID], { kind: "deleted" })
        }),
      ).pipe(Effect.orDie)

    const unreserve = (sessionID: SessionID, itemID: ItemID) =>
      writable(sessionID, (tx) =>
        tx
          .update(SessionPromptQueueTable)
          .set({ message_id: null })
          .where(eq(SessionPromptQueueTable.id, itemID))
          .run(),
      ).pipe(Effect.orDie)

    // Picks the oldest eligible row and reserves its message id, under the lock.
    // A reservation whose message already landed (a process stopped between the
    // write and the mark) is finished instead of prepared again.
    const reserve = (sessionID: SessionID, delivery: Delivery) =>
      exclusive(
        sessionID,
        Effect.gen(function* () {
          if (yield* held(sessionID)) return undefined
          if (delivery === "steer") {
            const hasPendingSteer = yield* db
              .transaction((tx) =>
                Effect.gen(function* () {
                  return yield* tx
                    .select({ id: SessionPromptQueueTable.id })
                    .from(SessionPromptQueueTable)
                    .where(pending(sessionID))
                    .orderBy(asc(SessionPromptQueueTable.seq))
                    .limit(1)
                    .get()
                    .pipe(
                      Effect.map((row) => row !== undefined),
                      Effect.orDie,
                    )
                }),
              )
              .pipe(Effect.orDie)
            if (!hasPendingSteer || (yield* compacting(sessionID))) return undefined
          }
          const result = yield* writable(sessionID, (tx) =>
            Effect.gen(function* () {
              const row = yield* oldest(tx, sessionID, delivery)
              if (!row) return undefined
              if (row.message_id && (yield* landed(tx, row.message_id))) {
                yield* promoted(tx, row, row.message_id)
                return { kind: "finished" as const, row }
              }
              const decoded = Schema.decodeUnknownExit(SessionPromptQueue.QueuedInput)(row.input)
              if (Exit.isFailure(decoded)) {
                yield* tx.delete(SessionPromptQueueTable).where(eq(SessionPromptQueueTable.id, row.id)).run()
                yield* settleReceipts(tx, sessionID, [row.id], { kind: "deleted" })
                return { kind: "invalid" as const, row, cause: decoded.cause }
              }
              const id = yield* messageID(tx, sessionID, decoded.value.messageID)
              yield* tx
                .update(SessionPromptQueueTable)
                .set({ message_id: id })
                .where(eq(SessionPromptQueueTable.id, row.id))
                .run()
                .pipe(Effect.orDie)
              return { kind: "reserved" as const, row, input: decoded.value, id }
            }),
          ).pipe(Effect.orDie)
          if (!result) return undefined
          if (result.kind === "finished") {
            deliver(result.row.id, result.row.message_id!)
            yield* publish(sessionID)
            return result
          }
          if (result.kind === "invalid") {
            yield* publish(sessionID)
            return result
          }
          return result
        }),
      )

    // Writes the prepared message only while its item is still pending with this
    // reservation; a withdraw or a compaction that began meanwhile wins.
    const commit = <P>(
      sessionID: SessionID,
      delivery: Delivery,
      itemID: ItemID,
      id: MessageID,
      prepared: P,
      write: (prepared: P) => Effect.Effect<unknown>,
    ) =>
      exclusive(
        sessionID,
        Effect.gen(function* () {
          const current = yield* db
            .select({ id: SessionPromptQueueTable.id })
            .from(SessionPromptQueueTable)
            .where(
              and(
                eq(SessionPromptQueueTable.id, itemID),
                pending(sessionID),
                eq(SessionPromptQueueTable.message_id, id),
              ),
            )
            .get()
            .pipe(Effect.orDie)
          if (!current) return false
          // A hold that began while this message was being prepared wins too.
          if ((yield* held(sessionID)) || (delivery === "steer" && (yield* compacting(sessionID)))) {
            yield* unreserve(sessionID, itemID)
            return false
          }
          yield* write(prepared).pipe(
            Effect.onExit((exit) =>
              Exit.isSuccess(exit)
                ? markPromoted(sessionID, itemID, id)
                : landed(db, id).pipe(
                    Effect.flatMap((exists) =>
                      exists ? markPromoted(sessionID, itemID, id) : unreserve(sessionID, itemID),
                    ),
                  ),
            ),
            Effect.ensuring(publish(sessionID)),
          )
          return true
        }),
      )

    const promote: Interface["promote"] = <P, E>(input: {
      readonly sessionID: SessionID
      readonly delivery: Delivery
      readonly prepare: (input: PromotedInput) => Effect.Effect<P, E>
      readonly write: (prepared: P) => Effect.Effect<unknown>
      readonly rejected: (cause: Cause.Cause<E>) => Effect.Effect<void>
      readonly own?: ItemID
      readonly untilStarted?: boolean
    }) =>
      semaphore(order, input.sessionID)
        .withPermit(
          Effect.gen(function* () {
            let changed = false
            while (true) {
              if (input.untilStarted && !(yield* empty(input.sessionID))) return changed
              const next = yield* reserve(input.sessionID, input.delivery)
              if (!next) return changed
              changed = true
              if (next.kind === "finished") {
                if (input.delivery === "queue") return true
                continue
              }
              if (next.kind === "invalid") {
                if (next.row.id === input.own) return yield* Effect.die(Cause.squash(next.cause))
                yield* input.rejected(Cause.die(Cause.squash(next.cause)))
                continue
              }
              const prepared = yield* input
                .prepare({ ...next.input, sessionID: input.sessionID, messageID: next.id })
                .pipe(Effect.exit)
              if (Exit.isFailure(prepared)) {
                // A prompt that cannot become a message is consumed, as a failed prompt was before the queue.
                yield* exclusive(
                  input.sessionID,
                  (Cause.hasInterruptsOnly(prepared.cause)
                    ? unreserve(input.sessionID, next.row.id)
                    : drop(input.sessionID, next.row.id)
                  ).pipe(Effect.andThen(publish(input.sessionID))),
                )
                if (Cause.hasInterruptsOnly(prepared.cause) || next.row.id === input.own)
                  return yield* Effect.failCause(prepared.cause)
                yield* input.rejected(prepared.cause)
                continue
              }
              const written = yield* commit(
                input.sessionID,
                input.delivery,
                next.row.id,
                next.id,
                prepared.value,
                input.write,
              )
              // Queued items run one per turn; the loop reevaluates before the next.
              if (written && input.delivery === "queue") return true
              if (!written && input.delivery === "steer" && (yield* compacting(input.sessionID))) return changed
            }
          }),
        )
        .pipe(Effect.withSpan("SessionQueue.promote"))

    // Only an attempt whose delete never ran is made again, so no executed
    // transaction body is replayed. Success is the attempt's own commit.
    const consume = Effect.fn("SessionQueue.consume")(function* (sessionID: SessionID) {
      for (let attempts = 1; ; attempts++) {
        const attempt = yield* consumeAttempt(sessionID)
        if (attempt.kind === "consumed") return attempt.seen
        yield* Effect.logWarning(
          attempt.kind === "locked"
            ? "queue consume met a held write lock"
            : "queue consume waited past its acquisition backstop",
          { "session.id": sessionID, attempts },
        )
        if (attempts === consumeAttempts)
          return yield* Effect.die(
            new ConsumeLockedError({
              sessionID,
              attempts: consumeAttempts,
              message: `SessionQueue.consume could not begin its write in ${consumeAttempts} attempts; session ${sessionID} stopped, and its queued prompts stay pending until it is prompted again`,
            }),
          )
      }
    })

    // The driver waits for its connection and retries BEGIN uninterruptibly, so
    // the attempt runs in its own fiber and the backstop stops waiting for it
    // rather than interrupting it. An abandoned attempt that later reaches its
    // body rolls back without deleting anything.
    const consumeAttempt = Effect.fnUntraced(function* (sessionID: SessionID) {
      const state = { began: false, abandoned: false }
      // Whether the abandon won: a body that began first is waited for instead,
      // so a committed consume is never reported as a backstop.
      const abandon = (fiber: Fiber.Fiber<unknown, unknown>) =>
        Effect.suspend(() => {
          if (state.began) return Effect.succeed(false)
          state.abandoned = true
          return Fiber.interrupt(fiber).pipe(Effect.forkIn(scope), Effect.as(true))
        })
      const fiber = yield* exclusive(
        sessionID,
        writable(sessionID, (tx) =>
          Effect.suspend(() => {
            if (state.abandoned) return Effect.interrupt
            state.began = true
            return tx
              .delete(SessionPromptQueueTable)
              .where(
                and(
                  eq(SessionPromptQueueTable.session_id, sessionID),
                  isNotNull(SessionPromptQueueTable.time_promoted),
                ),
              )
              .run()
              .pipe(Effect.map(() => wakes.get(sessionID) ?? 0))
          }),
        ),
      ).pipe(Effect.forkIn(scope, { startImmediately: true }))
      const waited = yield* Fiber.await(fiber).pipe(
        Effect.timeoutOption(consumeAcquireBackstop),
        Effect.onInterrupt(() => abandon(fiber)),
      )
      if (Option.isNone(waited) && (yield* abandon(fiber))) return { kind: "backstop" as const }
      const exit = Option.isSome(waited) ? waited.value : yield* Fiber.await(fiber)
      if (Exit.isSuccess(exit)) return { kind: "consumed" as const, seen: exit.value }
      if (!state.began && sqliteLockMessage(Cause.squash(exit.cause)) !== undefined) return { kind: "locked" as const }
      return yield* Effect.failCause(exit.cause).pipe(Effect.orDie)
    })

    const park = (sessionID: SessionID, seen?: number) =>
      exclusive(
        sessionID,
        Effect.sync(() => {
          if (seen === undefined || (wakes.get(sessionID) ?? 0) === seen) parked.add(sessionID)
        }),
      )

    const isParked: Interface["isParked"] = (sessionID) => Effect.sync(() => parked.has(sessionID))

    // A held pending item is not awaiting a drain: none could promote it, and the
    // hold's end wakes the session instead.
    const awaitingDrain = Effect.fn("SessionQueue.awaitingDrain")(function* (sessionID: SessionID) {
      if (parked.has(sessionID)) return false
      const row = yield* db
        .select({ id: SessionPromptQueueTable.id })
        .from(SessionPromptQueueTable)
        .where(
          and(
            eq(SessionPromptQueueTable.session_id, sessionID),
            isNull(SessionPromptQueueTable.time_withdrawn),
            (yield* held(sessionID)) ? isNotNull(SessionPromptQueueTable.time_promoted) : undefined,
          ),
        )
        .limit(1)
        .get()
        .pipe(Effect.orDie)
      return row !== undefined
    })

    const unread = Effect.fn("SessionQueue.unread")(function* (sessionID: SessionID, itemID: ItemID) {
      const row = yield* db
        .select({ id: SessionPromptQueueTable.id })
        .from(SessionPromptQueueTable)
        .where(
          and(
            eq(SessionPromptQueueTable.id, carrier(itemID)),
            eq(SessionPromptQueueTable.session_id, sessionID),
            isNull(SessionPromptQueueTable.time_withdrawn),
          ),
        )
        .get()
        .pipe(Effect.orDie)
      return row !== undefined
    })

    const withdrawn = Effect.fn("SessionQueue.withdrawn")(function* (sessionID: SessionID, itemID: ItemID) {
      const row = yield* db
        .select({ id: SessionPromptQueueTable.id })
        .from(SessionPromptQueueTable)
        .where(
          and(
            eq(SessionPromptQueueTable.id, carrier(itemID)),
            eq(SessionPromptQueueTable.session_id, sessionID),
            isNotNull(SessionPromptQueueTable.time_withdrawn),
          ),
        )
        .get()
        .pipe(Effect.orDie)
      return row !== undefined
    })

    const empty = Effect.fn("SessionQueue.empty")(function* (sessionID: SessionID) {
      const row = yield* db
        .select({ id: MessageTable.id })
        .from(MessageTable)
        .where(eq(MessageTable.session_id, sessionID))
        .limit(1)
        .get()
        .pipe(Effect.orDie)
      return row === undefined
    })

    // Admission order, not caller-supplied message IDs, defines which prompts a turn answered.
    const answer: Interface["answer"] = (sessionID, turn, reply) =>
      Effect.gen(function* () {
        const target = yield* db
          .select({ seq: MessageTable.admission_seq })
          .from(MessageTable)
          .where(and(eq(MessageTable.session_id, sessionID), eq(MessageTable.id, turn)))
          .get()
          .pipe(Effect.orDie)
        if (!target) return
        const pending = [...channels.entries()].filter(
          ([, channel]) => channel.sessionID === sessionID && !channel.reply && channel.message !== undefined,
        )
        const messageIDs = pending.flatMap(([, channel]) => (channel.message ? [channel.message] : []))
        if (messageIDs.length === 0) return
        const rows = yield* db
          .select({ id: MessageTable.id, seq: MessageTable.admission_seq })
          .from(MessageTable)
          .where(and(eq(MessageTable.session_id, sessionID), inArray(MessageTable.id, messageIDs)))
          .all()
          .pipe(Effect.orDie)
        const order = new Map(rows.map((row) => [row.id, row.seq]))
        pending.forEach(([, channel]) => {
          const seq = channel.message ? order.get(channel.message) : undefined
          if (seq !== undefined && seq <= target.seq) channel.reply = reply
        })
      })

    const forget = (itemID: ItemID) =>
      Effect.sync(() => {
        channels.delete(itemID)
        follows.delete(itemID)
      })

    const reply = (itemID: ItemID) =>
      Effect.sync(() => {
        const answered = channels.get(itemID)?.reply
        channels.delete(itemID)
        return answered
      })

    const annotate: Interface["annotate"] = (sessionID, messages) =>
      db
        .select()
        .from(SessionPromptProvenanceTable)
        .where(eq(SessionPromptProvenanceTable.session_id, sessionID))
        .all()
        .pipe(
          Effect.orDie,
          Effect.map((rows) => {
            const queued = new Map(rows.map((row) => [row.message_id, row.origins]))
            return messages.map((message) => {
              const found = message.info.role === "user" ? queued.get(message.info.id) : undefined
              if (!found) return message
              const note: SessionV1.TextPart = {
                id: PartID.ascending(),
                sessionID,
                messageID: message.info.id,
                type: "text",
                text: deliveryNote(found, message.info.time.created),
                synthetic: true,
              }
              return { ...message, parts: [note, ...message.parts] }
            })
          }),
        )

    return Service.of({
      admit,
      request,
      receipt,
      check,
      delivered,
      writeOrAdmit,
      list,
      withdraw,
      restore,
      update,
      promote,
      consume,
      park,
      isParked,
      awaitingDrain,
      withdrawn,
      unread,
      answer,
      reply,
      forget,
      empty,
      exclusive,
      whileCompacting,
      hold,
      release,
      released,
      remove,
      reorder,
      consolidate,
      annotate,
    })
  }),
)

/**
 * The compaction and subtask parts the loop still has to run. A compaction whose
 * summary turn stopped on an abort or error is over, the way any stopped turn is,
 * so neither a steer waits on it nor does the loop rerun it.
 *
 * This deliberately changes the loop's earlier behaviour, which retried such a
 * compaction on the next prompt with that prompt as its parent, so the prompt was
 * summarised instead of answered and a steer held behind it waited forever.
 * Stopping here matches how a stopped run parks; if the context is still too
 * large, the loop's overflow check starts a new compaction.
 */
export function openTasks(msgs: SessionV1.WithParts[], options: Parameters<typeof MessageV2.latest>[1]) {
  const stopped = new Set(
    msgs.flatMap((msg) => (msg.info.role === "assistant" && msg.info.error !== undefined ? [msg.info.parentID] : [])),
  )
  return MessageV2.latest(msgs, options).tasks.filter(
    (task) => task.type !== "compaction" || !stopped.has(task.messageID),
  )
}

// Rows hold the encoded input; `format`, for one, only becomes its class again
// through the schema.
const encodeInput = Schema.encodeSync(SessionPromptQueue.QueuedInput)
const decodeInput = Schema.decodeUnknownSync(SessionPromptQueue.QueuedInput)

function fromRow(row: typeof SessionPromptQueueTable.$inferSelect): Item {
  return {
    id: row.id,
    sessionID: row.session_id,
    seq: row.seq,
    delivery: row.delivery,
    ...(row.request_id ? { requestID: row.request_id } : {}),
    input: decodeInput(row.input),
    time: { created: row.time_created },
    origins: origins(row),
  }
}

function toReceipt(row: typeof SessionPromptReceiptTable.$inferSelect): Receipt {
  if (row.state === "pending") return { state: "pending", itemID: row.item_id! }
  if (row.state === "delivered") return { state: "delivered", messageID: row.message_id! }
  if (row.state === "merged")
    return { state: "merged", into: row.into_request_id!, ...(row.message_id ? { messageID: row.message_id } : {}) }
  if (row.state === "superseded") return { state: "superseded", by: row.by_request_id! }
  return { state: "deleted" }
}

// What makes a prompt the same prompt, as a digest: not its transport IDs, the
// model and variant it selected, how it was delivered, nor the request it names.
function fingerprint(input: AdmitInput) {
  const prompt = Struct.omit(input, ["sessionID", "requestID", "messageID", "model", "variant", "delivery"])
  return new Bun.CryptoHasher("sha256")
    .update(
      JSON.stringify(withoutUndefined({ ...prompt, parts: prompt.parts.map((part) => Struct.omit(part, ["id"])) })),
    )
    .digest("hex")
}

// A row stored before origins were kept has one: the time it was admitted.
function origins(row: typeof SessionPromptQueueTable.$inferSelect): SessionPromptQueue.Origin[] {
  return row.origins ?? [{ time: row.time_created }]
}

/**
 * What a model is told of a prompt delivered from the queue: when each of its
 * instructions was first queued, when identical copies of it were queued again,
 * and when it was delivered. Built from stored times only, so the request for a
 * turn reads the same every time it is sent.
 */
export function deliveryNote(origins: readonly SessionPromptQueue.Origin[], delivered: number) {
  const at = (time: number) => new Date(time).toISOString().replace(/\.\d{3}Z$/, "Z")
  const again = (origin: SessionPromptQueue.Origin) =>
    origin.repeats?.length ? `, and again at ${origin.repeats.map((repeat) => at(repeat.time)).join(", ")}` : ""
  const first = Math.min(...origins.map((origin) => origin.time))
  const waited = `${elapsed(delivered - first)} after it was first queued`
  // Each request it carries is still owed its own reply (#212).
  const requests = [
    ...new Set(
      origins.flatMap((origin) => [origin, ...(origin.repeats ?? [])].flatMap((copy) => copy.requestID ?? [])),
    ),
  ]
  const owed =
    requests.length > 1
      ? ` It carries ${requests.length} requests, each owed its own reply: ${requests.join(", ")}.`
      : ""
  if (origins.length === 1)
    return `[Queued prompt: queued at ${at(origins[0]!.time)}${again(origins[0]!)}; delivered at ${at(delivered)}, ${waited}.${owed}]`
  const steps = origins.map((origin, index) => `step ${index + 1} at ${at(origin.time)}${again(origin)}`)
  return `[Queued prompt merged from ${origins.length} queued instructions: ${steps.join("; ")}; delivered at ${at(delivered)}, ${waited}.${owed}]`
}

function elapsed(ms: number) {
  const seconds = Math.max(0, Math.round(ms / 1000))
  const parts = [
    [Math.floor(seconds / 86400), "d"],
    [Math.floor(seconds / 3600) % 24, "h"],
    [Math.floor(seconds / 60) % 60, "m"],
    [seconds % 60, "s"],
  ] as const
  const shown = parts.filter(([value]) => value > 0)
  return shown.length === 0 ? "0s" : shown.map(([value, unit]) => `${value}${unit}`).join("")
}

/**
 * The consolidate button's plan over pending items in promotion order (#212).
 * Identical items, in any category, collapse into the first: it keeps its
 * place, takes the newest copy's admission time, and keeps every copy's queue
 * time. Then commands that share a category, a leading verb (any case) and a
 * target, the next word, merge into the first of them as `<verb> <target>:` and
 * numbered steps in promotion order, each step keeping its own queue times. A
 * command is a prompt of one plain text part, `<verb> <target> [step]`; a bare
 * `<verb> <target>` is its own step, and a merged command parses back into its
 * own steps, so a later consolidation adds to them. A machine message (one
 * `markers` or the built-in envelopes mark) is never a command. Commands merge
 * only with the same agent and options, which their steps then share. The model
 * and variant a prompt was sent with decide nothing, since a pending prompt runs
 * at the session's selection when it is delivered.
 */
export function consolidation(items: readonly Item[], markers?: ConfigV1.Info["machine_message_markers"]) {
  const copies = Map.groupBy(items, copyKey)
  const kept = [...copies.values()].map((group) => ({
    item: {
      ...group[0]!,
      time: { created: Math.max(...group.map((copy) => copy.time.created)) },
      origins: group.slice(1).reduce((origins, copy) => repeated(origins, copy.origins), group[0]!.origins),
    },
    collapsed: group.length > 1,
  }))
  const groups = Map.groupBy(
    kept.flatMap((entry) => {
      const command = marked(entry.item.input, markers) ? undefined : parseCommand(entry.item.input)
      if (!command) return []
      return [{ item: entry.item, command, key: commandKey(entry.item, command) }]
    }),
    (entry) => entry.key,
  )
  const merges = [...groups.values()].filter((group) => group.length > 1)
  const mergedAway = new Set(merges.flatMap((group) => group.slice(1).map((entry) => entry.item.id)))
  const heads = new Map(
    merges.map((group) => {
      const first = group[0]!
      const text = [
        `${first.command.verb} ${first.command.target}:`,
        ...group.flatMap((entry) => entry.command.steps).map((step, index) => `${index + 1}. ${step}`),
      ].join("\n")
      return [
        first.item.id,
        {
          ...first.item,
          time: { created: Math.max(...group.map((entry) => entry.item.time.created)) },
          input: {
            ...first.item.input,
            parts: [{ ...first.item.input.parts[0]!, text }],
          } as SessionPromptQueue.QueuedInput,
          origins: group.flatMap((entry) =>
            entry.item.origins.length === entry.command.steps.length
              ? entry.item.origins
              : entry.command.steps.map(() => entry.item.origins.reduce(combine)),
          ),
        },
      ]
    }),
  )
  const survivors = new Set(kept.map((entry) => entry.item.id))
  return {
    // Which items each survivor took in, copies first, then merged commands.
    into: [
      ...[...copies.values()]
        .filter((group) => group.length > 1)
        .map((group) => ({ into: group[0]!.id, items: group.slice(1).map((copy) => copy.id) })),
      ...merges.map((group) => ({ into: group[0]!.item.id, items: group.slice(1).map((entry) => entry.item.id) })),
    ],
    removed: items.filter((item) => !survivors.has(item.id) || mergedAway.has(item.id)).map((item) => item.id),
    changed: kept.flatMap((entry) => {
      const head = heads.get(entry.item.id)
      if (head) return [head]
      return entry.collapsed && !mergedAway.has(entry.item.id) ? [entry.item] : []
    }),
  }
}

/**
 * What admitting `added` replaces among the pending items of its delivery
 * (#212), each only on positive evidence and never by age:
 *
 * - `absorbed`: identical copies, from any sender, whose text the survivor
 *   repeats exactly; and earlier sends of the same peer request (same peer,
 *   level, key and text, under another msgid). The added item takes them in,
 *   keeping each one's queue time and request.
 * - `superseded`: earlier notices with the added one's supersede key
 *   (`[fm-level:nudge:<key>]`) from the same sender. They retire undelivered.
 * - `into`: the earlier command `added` continues, under the verb-and-target
 *   rule, which takes it in as its next step.
 *
 * Every rule but the identical copy hides what the sender sent, so it applies
 * only when each item involved names a request, whose receipt then says what
 * became of it. A critical message, a reply, and a message from another sender
 * or with another reply target are never replaced.
 */
export function admission(pending: readonly Item[], added: Item, markers?: ConfigV1.Info["machine_message_markers"]) {
  const others = pending.filter((item) => item.id !== added.id && item.delivery === added.delivery)
  const own = envelopeOf(added)
  const absorbed = others.filter((item) => {
    if (copyKey(item) === copyKey(added)) return own?.level !== "critical"
    if (!item.requestID || !added.requestID || !own?.sender.startsWith("peer") || own.reply) return false
    if (own.level === "critical") return false
    const other = envelopeOf(item)
    return (
      other?.sender === own.sender &&
      other.level === own.level &&
      other.key === own.key &&
      !other.reply &&
      other.body === own.body
    )
  })
  const superseded =
    own?.level === "nudge" && own.key && added.requestID
      ? others.filter((item) => {
          if (!item.requestID || absorbed.includes(item)) return false
          const other = envelopeOf(item)
          return other?.sender === own.sender && other.level === "nudge" && other.key === own.key
        })
      : []
  const command = added.requestID && !marked(added.input, markers) ? parseCommand(added.input) : undefined
  const into = command
    ? others.find((item) => {
        if (!item.requestID || absorbed.includes(item) || marked(item.input, markers)) return false
        const other = parseCommand(item.input)
        return other !== undefined && commandKey(item, other) === commandKey(added, command)
      })
    : undefined
  return { absorbed: absorbed.map((item) => item.id), superseded: superseded.map((item) => item.id), into: into?.id }
}

function envelopeOf(item: Item) {
  return MachineMessage.envelope(
    item.input.parts.flatMap((part) => (part.type === "text" && !part.synthetic ? [part.text] : [])).join(""),
  )
}

function copyKey(item: Item) {
  return JSON.stringify(withoutUndefined(comparable(item.input)))
}

// Commands merge within one delivery, verb (any case) and target, and with the
// same agent and options.
function commandKey(item: Item, command: { readonly verb: string; readonly target: string }) {
  const normalized = comparable(item.input)
  const context = withoutUndefined({ ...normalized, parts: [{ ...normalized.parts[0], text: "" }] })
  return JSON.stringify([item.delivery, command.verb.toLowerCase(), command.target, context])
}

// What two inputs are compared by: never their transport IDs, nor the model and
// variant they were sent with, which only selected the session's.
function comparable(input: SessionPromptQueue.QueuedInput) {
  return Struct.omit(withoutIDs(input), ["model", "variant"])
}

function marked(input: SessionPromptQueue.QueuedInput, markers?: ConfigV1.Info["machine_message_markers"]) {
  const text = input.parts.flatMap((part) => (part.type === "text" && !part.synthetic ? [part.text] : [])).join("")
  return MachineMessage.classify(text, markers) !== undefined
}

// A copy's queue times join the survivor's: step by step when both carry the
// same instructions, otherwise all on the first.
function repeated(origins: readonly SessionPromptQueue.Origin[], copy: readonly SessionPromptQueue.Origin[]) {
  if (origins.length === copy.length) return origins.map((origin, index) => combine(origin, copy[index]!))
  return [copy.reduce(combine, origins[0]!), ...origins.slice(1)]
}

function combine(left: SessionPromptQueue.Origin, right: SessionPromptQueue.Origin): SessionPromptQueue.Origin {
  const copies = [left, ...(left.repeats ?? []), right, ...(right.repeats ?? [])]
    .map((copy) => (copy.requestID ? { time: copy.time, requestID: copy.requestID } : { time: copy.time }))
    .sort((a, b) => a.time - b.time)
  return { ...copies[0]!, repeats: copies.slice(1) }
}

function parseCommand(input: SessionPromptQueue.QueuedInput) {
  const part = input.parts[0]
  if (input.parts.length !== 1 || part?.type !== "text" || part.synthetic) return undefined
  const match = /^\s*(\S+)\s+(\S+?)[:,]?(?:\s+(\S[\s\S]*?))?\s*$/.exec(part.text)
  if (!match) return undefined
  // A bare `<verb> <target>` is a step of its own.
  if (match[3] === undefined) return { verb: match[1]!, target: match[2]!, steps: [`${match[1]} ${match[2]}`] }
  const rest = match[3].trim()
  const lines = rest.split("\n")
  const numbered = lines.length > 1 && lines.every((line) => /^\d+\.\s+\S/.test(line))
  return {
    verb: match[1]!,
    target: match[2]!,
    steps: numbered ? lines.map((line) => line.replace(/^\d+\.\s+/, "")) : [rest],
  }
}

function withoutIDs(input: SessionPromptQueue.QueuedInput) {
  return { ...Struct.omit(input, ["messageID"]), parts: input.parts.map((part) => Struct.omit(part, ["id"])) }
}

function sameMessage(left: SessionV1.WithParts, right: SessionV1.WithParts) {
  // Prompt preparation assigns fresh part IDs and timestamps for an identical retry.
  return isDeepStrictEqual(
    withoutUndefined({
      info: Struct.omit(left.info, ["time"]),
      parts: left.parts.map((part) => Struct.omit(part, ["id"])),
    }),
    withoutUndefined({
      info: Struct.omit(right.info, ["time"]),
      parts: right.parts.map((part) => Struct.omit(part, ["id"])),
    }),
  )
}

function sameQueuedInput(left: SessionPromptQueue.QueuedInput, right: AdmitInput) {
  return isDeepStrictEqual(
    withoutUndefined(Struct.omit(left, ["messageID"])),
    withoutUndefined(Struct.omit(right, ["sessionID", "messageID", "noReply", "delivery"])),
  )
}

function withoutUndefined(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(withoutUndefined)
  if (value === null || typeof value !== "object") return value
  return Object.fromEntries(
    Object.entries(value)
      .filter(([, item]) => item !== undefined)
      .map(([key, item]) => [key, withoutUndefined(item)]),
  )
}

function rekeyMessage<P extends SessionV1.WithParts>(message: P, messageID: MessageID): P {
  return {
    ...message,
    info: { ...message.info, id: messageID },
    parts: message.parts.map((part) => ({ ...part, messageID })),
  } as P
}

export const node = LayerNode.make({ service: Service, layer, deps: [Database.node, EventV2Bridge.node] })

export * as SessionQueue from "./queue"
