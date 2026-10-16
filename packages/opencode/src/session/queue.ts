import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { DatabaseTransaction } from "@opencode-ai/core/database/transaction"
import {
  SessionPromptQueueAliasTable,
  SessionPromptProvenanceTable,
  SessionPromptReceiptTable,
  SessionPromptQueueSequenceTable,
  SessionPromptQueueTable,
} from "@opencode-ai/core/session/prompt-queue.sql"
import { MessageTable, PartTable } from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { and, asc, desc, eq, gt, inArray, isNotNull, isNull, lt, ne, or, sql } from "drizzle-orm"
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
import { createHash } from "node:crypto"
import { EventV2Bridge } from "@/event-v2-bridge"
import { sqliteLockMessage } from "@/util/sqlite-error"
import { MachineMessage } from "./machine-message"
import { MessageV2 } from "./message-v2"
import { MessageID, PartID, SessionID } from "./schema"

type DatabaseService = Database.Interface["db"]
type DatabaseTransaction = Parameters<Parameters<DatabaseService["transaction"]>[0]>[0]
type DatabaseAccess = DatabaseService | DatabaseTransaction

// Direct noReply writes need their own original before preparation can ever run again.
const NoReplyOriginal = Schema.Struct({
  version: Schema.Literal(1),
  sessionID: SessionID,
  input: Schema.Struct({ ...SessionPromptQueue.Input.fields, noReply: Schema.Literal(true) }),
})
const NoReplyWitness = Schema.Struct({
  type: Schema.Literal("text"),
  text: Schema.Literal(""),
  ignored: Schema.Literal(true),
  synthetic: Schema.Literal(true),
  metadata: Schema.Struct({ sessionQueueNoReply: NoReplyOriginal }),
})

// Durable V1 prompt queue (swxtchio/swx-opencode#68). Every prompt except noReply
// is admitted here before it becomes a V1 user message, so the loop decides when
// it reaches the model: a steer at the next safe step, a queued item only when
// the run would otherwise go idle, with compatible queued peers also eligible at safe provider continuation.

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
  readonly group?: SessionPromptQueue.Group
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

export class HistoricalIdentityError extends Schema.TaggedErrorClass<HistoricalIdentityError>()(
  "SessionQueueHistoricalIdentityError",
  { sessionID: SessionID, messageID: MessageID, message: Schema.String },
) {}

// Each consume attempt waits out the statement lock window, so a lock released
// within about this many windows lets the run go on.
const consumeAttempts = 5
// Bounds an attempt's wait for the session permit, the shared connection and
// BEGIN together: its own statement window plus one other statement's window
// ahead of it on the connection. The body, once begun, is not timed.
const consumeAcquireBackstop = Duration.seconds(20)
// A review hold's lease (#212). Its client renews it on activity and closes its
// view after 5 minutes without any, so a live view always renews in time; the
// lease only ends a hold whose client vanished without releasing it.
export const holdLease = Duration.minutes(6)
export type HoldEnd = "released" | "lapsed"

export interface Interface {
  readonly admit: (input: AdmitInput) => Effect.Effect<Item>
  /** Reconcile represented input before preparation, hooks or other side effects. */
  readonly reconcile: (
    input: AdmitInput,
  ) => Effect.Effect<
    | { readonly kind: "direct"; readonly message: SessionV1.WithParts }
    | { readonly kind: "queued"; readonly own: Item }
    | undefined
  >
  /** Executable command/shell input has no retained expansion provenance; existing identity cannot run again. */
  readonly assertFreshIdentity: (sessionID: SessionID, messageID?: MessageID) => Effect.Effect<void>
  /** Reserve only the executable identity across unlocked IO, then recheck and admit under the session lock. */
  readonly prepareExecutable: <P extends AdmitInput, E>(input: {
    readonly sessionID: SessionID
    readonly messageID?: MessageID
    readonly prepare: () => Effect.Effect<P, E>
  }) => Effect.Effect<{ readonly input: P; readonly own: Item }, E>
  readonly message: (itemID: ItemID) => Effect.Effect<SessionV1.WithParts | undefined>
  readonly writeDirect: <P extends SessionV1.WithParts, E>(input: {
    readonly admission: AdmitInput
    readonly prepare: () => Effect.Effect<P, E>
    readonly write: (prepared: P) => Effect.Effect<SessionV1.WithParts>
  }) => Effect.Effect<SessionV1.WithParts, E>
  /** Reject ambiguous projected identity before command template execution; admission validates known originals. */
  readonly assertIdentity: (sessionID: SessionID, messageID?: MessageID) => Effect.Effect<void>
  /**
   * Admits a prompt, unless it names a request this session already has (#212).
   * The same prompt again is a retry: nothing is admitted, and `own` is a new
   * waiter's id that follows whatever carries the request now. A different
   * prompt under that request fails with RequestConflictError. An admitted
   * prompt then retires the pending items it positively replaces; see
   * `admission`. `markers` are the configured machine-message markers.
   * A `preparing` prompt is admitted before its sender has done any other work
   * on it (#249): it is listed and can be withdrawn at once, but nothing
   * promotes or merges it until the sender calls `prepared`, or `unprepared`
   * when that work fails. One whose transport identity the session already
   * holds is reconciled to it instead, as `reconcile` does, and admits nothing;
   * its delivery is not compared yet, so its sender reconciles again once it
   * has classified it.
   */
  readonly request: (
    input: AdmitInput,
    markers?: ConfigV1.Info["machine_message_markers"],
    preparing?: boolean,
  ) => Effect.Effect<
    | { readonly kind: "admitted"; readonly own: Item }
    | { readonly kind: "retry"; readonly own: ItemID; readonly receipt: Receipt }
    | { readonly kind: "reconciled"; readonly own: Item }
    | { readonly kind: "direct"; readonly message: SessionV1.WithParts },
    RequestConflictError
  >
  /**
   * Ends a `preparing` admission once its sender has prepared the session for
   * it: the item takes the delivery its sender decided, then merges and is
   * promoted like any other admission. Nothing when it is gone meanwhile.
   */
  readonly prepared: (input: {
    readonly sessionID: SessionID
    readonly itemID: ItemID
    readonly delivery?: Delivery
    readonly markers?: ConfigV1.Info["machine_message_markers"]
  }) => Effect.Effect<void>
  /**
   * Takes back a `preparing` admission whose sender failed to prepare it: the
   * item and its request go as if never sent, so an exact retry is admitted again.
   */
  readonly unprepared: (
    sessionID: SessionID,
    itemID: ItemID,
    markers?: ConfigV1.Info["machine_message_markers"],
  ) => Effect.Effect<void>
  /** Waits, without the session's lock, until a `preparing` item's sender has prepared it or taken it back. */
  readonly awaitPrepared: (itemID: ItemID) => Effect.Effect<void>
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
   * wins. Steers and peer continuations wait while a compaction is pending or running, so they never
   * become its parent or input. Delivery boundaries reevaluate continuation.
   * Native rejection retains its existing behavior. A failed machine group is
   * isolated to individual originals; a failing original is preserved in a
   * reported withdrawn hold for explicit restore, without failing unrelated callers.
   */
  readonly promote: <P, E>(input: {
    readonly sessionID: SessionID
    readonly delivery: Delivery
    readonly prepare: (input: PromotedInput) => Effect.Effect<P, E>
    readonly write: (prepared: P) => Effect.Effect<unknown>
    readonly valid?: (prepared: P) => Effect.Effect<boolean>
    readonly rejected: (cause: Cause.Cause<E>) => Effect.Effect<void>
    readonly own?: ItemID
    /** Stops once the session has any message, for a new session's first promotion. */
    readonly untilStarted?: boolean
    /** Safe active continuation selects peers only; native holds and automation remain queued. */
    readonly peersOnly?: boolean
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
   * Exact retry callers share the recorded answer, with one owner per admission.
   * Reading does not consume another caller's answer; each caller forgets only
   * its own ownership when its scope ends.
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
  /**
   * What became of a waiting caller's own item once the run it joined ended
   * (#215): `answered` when its turn recorded a reply, `delivered` when it became
   * a message whose turn has not, and `pending` while it is still queued. A
   * pending item first waits out any live review hold, ended by a release or a
   * lapse, and is observed again after it, so a hold that ended after the
   * caller's earlier checks is seen as ended. Fails with WithdrawnError when the
   * item was withdrawn or deleted without becoming a message.
   */
  readonly disposition: (
    sessionID: SessionID,
    itemID: ItemID,
  ) => Effect.Effect<"answered" | "delivered" | "pending", WithdrawnError>
  /** Deletes a pending item for good. None tells the caller it was not pending. */
  readonly remove: (sessionID: SessionID, itemID: ItemID) => Effect.Effect<Option.Option<Item>>
  /**
   * Gives the listed pending items their seqs back in the listed order, so they
   * promote in that order; a category still decides when an item promotes. None,
   * with nothing changed, when any of them is not pending.
   */
  readonly reorder: (sessionID: SessionID, itemIDs: readonly ItemID[]) => Effect.Effect<Option.Option<Item[]>>
  /** Keeps related pending work together without crossing delivery classes; see `consolidation`. */
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
    const preparations = new Map<string, symbol>()
    const parked = new Set<SessionID>()
    const wakes = new Map<SessionID, number>()
    const running = new Set<SessionID>()
    // Items this process admitted as `preparing` whose sender has not finished
    // with them, each with the signal that it has. A preparing row missing here
    // was left by a stopped process.
    const preparing = new Map<ItemID, Deferred.Deferred<void>>()
    // Ends an item's preparation here, so it bounds no admission from now on;
    // its waiters are told once the change it makes is committed.
    const endPreparing = (itemID: ItemID) => {
      const done = preparing.get(itemID)
      preparing.delete(itemID)
      return Effect.sync(() => {
        if (done) Deferred.doneUnsafe(done, Effect.void)
      })
    }
    // Waiting callers' answer channels, by item: the message the item became,
    // then the final reply of the turn that message joined.
    const channels = new Map<
      ItemID,
      {
        readonly sessionID: SessionID
        owners: Map<symbol, Scope.Scope | undefined>
        message?: MessageID
        reply?: SessionV1.WithParts
      }
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

    // A pending row its sender is still preparing is never promoted or merged.
    const promotable = (sessionID: SessionID) => and(pending(sessionID), isNull(SessionPromptQueueTable.time_preparing))

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
      if (admitted.kind !== "admitted")
        return yield* Effect.die(new Error(`Request ${input.requestID} was already admitted to ${input.sessionID}`))
      return admitted.own
    })

    const openChannel = (item: Item, message: MessageID | undefined, callerScope: Scope.Scope | undefined) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          const existing = channels.get(item.id)
          const channel = existing ?? {
            sessionID: item.sessionID,
            owners: new Map<symbol, Scope.Scope | undefined>(),
            message,
            reply: undefined as SessionV1.WithParts | undefined,
          }
          if (!existing) channels.set(item.id, channel)
          channel.message ??= message
          const owner = Symbol()
          channel.owners.set(owner, callerScope)
          if (callerScope)
            yield* Scope.addFinalizer(
              callerScope,
              Effect.sync(() => {
                if (!channel.owners.delete(owner)) return
                if (!channel.owners.size && channels.get(item.id) === channel) channels.delete(item.id)
              }),
            )
        }),
      )

    const indexAliases = (tx: DatabaseTransaction, sessionID: SessionID) =>
      Effect.gen(function* () {
        const sequence = yield* tx
          .select()
          .from(SessionPromptQueueSequenceTable)
          .where(eq(SessionPromptQueueSequenceTable.session_id, sessionID))
          .get()
        if (sequence?.aliases_indexed) return
        const rows = yield* tx
          .select({
            id: SessionPromptQueueTable.id,
            seq: SessionPromptQueueTable.seq,
            input: sql<string>`${SessionPromptQueueTable.input}`,
          })
          .from(SessionPromptQueueTable)
          .where(eq(SessionPromptQueueTable.session_id, sessionID))
          .all()
        const aliases = rows.flatMap((row) => {
          const raw = Schema.decodeUnknownOption(Schema.UnknownFromJsonString)(row.input)
          const decoded = Schema.decodeUnknownOption(SessionPromptQueue.QueuedInput)(Option.getOrUndefined(raw))
          const supplied = Schema.decodeUnknownOption(Schema.Struct({ messageID: MessageID }))(
            Option.getOrUndefined(raw),
          )
          const keys = Option.isSome(decoded)
            ? [
                ...constituentIDs(sessionID, decoded.value),
                ...decoded.value.parts.flatMap((part) => (pendingOriginal(part) && part.id ? [part.id] : [])),
              ]
            : Option.isSome(supplied)
              ? constituentIDs(sessionID, { messageID: supplied.value.messageID, parts: [] })
              : []
          return keys.map((id) => ({ id, item_id: row.id }))
        })
        // Backfill batches stay below SQLite's parameter bound; subsequent admissions use only indexed keys.
        yield* Effect.forEach(
          Array.from({ length: Math.ceil(aliases.length / 500) }, (_, index) =>
            aliases.slice(index * 500, (index + 1) * 500),
          ),
          (batch) => tx.insert(SessionPromptQueueAliasTable).values(batch).onConflictDoNothing().run(),
          { discard: true },
        )
        yield* tx
          .insert(SessionPromptQueueSequenceTable)
          .values({
            session_id: sessionID,
            seq: sequence?.seq ?? rows.reduce((seq, row) => Math.max(seq, row.seq), 0),
            aliases_indexed: true,
          })
          .onConflictDoUpdate({ target: SessionPromptQueueSequenceTable.session_id, set: { aliases_indexed: true } })
          .run()
      })

    const rememberAliases = (
      tx: DatabaseTransaction,
      item: Item,
      messageID: MessageID,
      kind: string,
      keys: readonly string[],
    ) =>
      Effect.gen(function* () {
        if (!keys.length) return
        const existing = yield* tx
          .select({ id: PartTable.id })
          .from(PartTable)
          .where(
            inArray(
              PartTable.id,
              keys.map((id) => PartID.make(id)),
            ),
          )
          .all()
        yield* Effect.forEach(
          keys.filter((id) => !existing.some((part) => part.id === id)),
          (id) =>
            events.publish(SessionV1.Event.PartUpdated, {
              sessionID: item.sessionID,
              part: {
                type: "text",
                id: PartID.make(id),
                sessionID: item.sessionID,
                messageID,
                text: "",
                ignored: true,
                synthetic: true,
                metadata: {
                  sessionQueue: Schema.encodeSync(SessionPromptQueue.Group)({
                    version: 1,
                    sessionID: item.sessionID,
                    kind,
                    constituents: [item],
                  }),
                },
              },
              time: Date.now(),
            }),
          { discard: true },
        )
      })

    const assertNotPreparing = (sessionID: SessionID, messageID?: MessageID) =>
      Effect.sync(() => {
        if (messageID && preparations.has(JSON.stringify([sessionID, messageID])))
          throw new HistoricalIdentityError({
            sessionID,
            messageID,
            message: `Executable identity ${messageID} is still preparing; competing admission refused before side effects`,
          })
      })

    const assertIdentityIn = (
      query: DatabaseAccess,
      sessionID: SessionID,
      messageID?: MessageID,
      checkCustody = true,
    ) =>
      Effect.gen(function* () {
        yield* assertNotPreparing(sessionID, messageID)
        if (!messageID) return
        const projected = yield* query
          .select({ id: MessageTable.id })
          .from(MessageTable)
          .where(eq(MessageTable.id, messageID))
          .get()
        if (!projected) return
        if (checkCustody) {
          const keys = constituentIDs(sessionID, { messageID, parts: [] })
          const witnesses = yield* query
            .select({
              id: PartTable.id,
              session_id: PartTable.session_id,
              message_id: PartTable.message_id,
              data: sql<string>`${PartTable.data}`,
            })
            .from(PartTable)
            .where(inArray(PartTable.id, keys))
            .all()
          const owners = witnesses.flatMap((row) => {
            const raw = Schema.decodeUnknownOption(Schema.UnknownFromJsonString)(row.data)
            const original = deliveredOriginal({ ...row, data: Option.getOrUndefined(raw) }, sessionID)
            return original ? [original.item.id] : []
          })
          const sequence = yield* query
            .select({ indexed: SessionPromptQueueSequenceTable.aliases_indexed })
            .from(SessionPromptQueueSequenceTable)
            .where(eq(SessionPromptQueueSequenceTable.session_id, sessionID))
            .get()
          const retained = yield* query
            .select({ id: SessionPromptQueueTable.id, input: sql<string>`${SessionPromptQueueTable.input}` })
            .from(SessionPromptQueueTable)
            .where(
              and(
                eq(SessionPromptQueueTable.session_id, sessionID),
                or(
                  inArray(
                    SessionPromptQueueTable.id,
                    query
                      .select({ id: SessionPromptQueueAliasTable.item_id })
                      .from(SessionPromptQueueAliasTable)
                      .where(inArray(SessionPromptQueueAliasTable.id, keys)),
                  ),
                  // Only an unindexed legacy Session needs canonical-input fallback; warm checks use custody indexes.
                  ...(!sequence?.indexed
                    ? [
                        sql`CASE WHEN json_valid(${SessionPromptQueueTable.input}) THEN json_extract(${SessionPromptQueueTable.input}, '$.messageID') END = ${messageID}`,
                      ]
                    : []),
                ),
              ),
            )
            .limit(2)
            .all()
          const originals = retained.flatMap((row) => {
            const raw = Schema.decodeUnknownOption(Schema.UnknownFromJsonString)(row.input)
            return Option.isSome(Schema.decodeUnknownOption(SessionPromptQueue.QueuedInput)(Option.getOrUndefined(raw)))
              ? [row.id]
              : []
          })
          if (
            owners.length === witnesses.length &&
            originals.length === retained.length &&
            new Set([...owners, ...originals]).size === 1
          )
            return
        }
        return yield* Effect.die(
          new HistoricalIdentityError({
            sessionID,
            messageID,
            message: `Existing projected message ${messageID} lacks unambiguous queue original or transport-witness custody; stable-ID admission refused without replay`,
          }),
        )
      }).pipe(Effect.orDie)

    const assertIdentity: Interface["assertIdentity"] = (sessionID, messageID) =>
      !messageID
        ? Effect.void
        : exclusive(
            sessionID,
            db
              .transaction((tx) =>
                EventV2.assertReplayableIn(tx, sessionID).pipe(
                  Effect.andThen(assertIdentityIn(tx, sessionID, messageID)),
                ),
              )
              .pipe(Effect.orDie),
          )

    const reconcileAdmission = Effect.fnUntraced(function* (
      input: AdmitInput,
      existingOnly = false,
      markers?: ConfigV1.Info["machine_message_markers"],
      // A sender that has not classified its prompt yet (#249) cannot be held to
      // a delivery here; it reconciles again with one once it has.
      deferred = false,
    ) {
      const now = Date.now()
      if (input.parts.some((part) => part.type === "text" && part.metadata?.sessionQueuePendingOriginal))
        return yield* Effect.die(new Error("Pending peer custody metadata is queue-owned"))
      yield* assertNotPreparing(input.sessionID, input.messageID)
      const callerScope = Option.getOrUndefined(yield* Effect.serviceOption(Scope.Scope))
      const result = yield* writable(input.sessionID, (tx) =>
        Effect.gen(function* () {
          const keys = constituentIDs(input.sessionID, input)
          if (keys.length) yield* indexAliases(tx, input.sessionID)
          const delivered = keys.length
            ? yield* tx.select().from(PartTable).where(inArray(PartTable.id, keys)).all()
            : []
          const originals = delivered.map((row) => {
            const original = deliveredOriginal(row, input.sessionID)
            if (
              !original ||
              !sameQueuedInput(original.item.input, input) ||
              (!deferred && original.item.delivery !== (input.delivery ?? "steer"))
            )
              throw new Error(`Conflicting delivered constituent: ${row.id}`)
            return original
          })
          const matches = keys.length
            ? yield* tx
                .select({ row: SessionPromptQueueTable })
                .from(SessionPromptQueueAliasTable)
                .innerJoin(
                  SessionPromptQueueTable,
                  eq(SessionPromptQueueAliasTable.item_id, SessionPromptQueueTable.id),
                )
                .where(inArray(SessionPromptQueueAliasTable.id, keys))
                .all()
            : []
          const rows = [...new Map(matches.map(({ row }) => [row.id, row])).values()]
          const projected = input.messageID
            ? yield* tx
                .select({ id: MessageTable.id, session_id: MessageTable.session_id })
                .from(MessageTable)
                .where(eq(MessageTable.id, input.messageID))
                .get()
            : undefined
          if (projected && (originals.length || rows.length)) {
            const deliveredOwner = originals.every((original) => original.messageID === projected.id)
            const retainedOwner = rows.every((row) => row.time_promoted !== null && row.message_id === projected.id)
            if (projected.session_id !== input.sessionID || !deliveredOwner || !retainedOwner)
              return yield* Effect.die(
                new HistoricalIdentityError({
                  sessionID: input.sessionID,
                  messageID: input.messageID!,
                  message: `Presented transport ID ${input.messageID} belongs to another projected message`,
                }),
              )
          }
          for (const row of rows) {
            const decoded = Schema.decodeUnknownOption(SessionPromptQueue.QueuedInput)(row.input)
            if (
              row.session_id !== input.sessionID ||
              Option.isNone(decoded) ||
              !originalsOf(row).some((original) => sameQueuedInput(original.input, input)) ||
              (!deferred && row.delivery !== (input.delivery ?? "steer"))
            )
              return yield* Effect.die(new Error(`Conflicting prompt message ID: ${input.messageID ?? keys[0]}`))
          }
          if (originals.length) {
            const original = originals[0]
            if (
              originals.some((item) => item.item.id !== original.item.id || item.messageID !== original.messageID) ||
              rows.some((row) => row.id !== original.item.id)
            )
              return yield* Effect.die(new Error(`Conflicting prompt identity owners: ${input.messageID ?? keys[0]}`))
            yield* rememberAliases(tx, original.item, original.messageID, original.groupKind, keys)
            return { kind: "delivered" as const, ...original }
          }
          if (rows.length) {
            const row = rows[0]
            const aliases = yield* tx
              .select()
              .from(SessionPromptQueueAliasTable)
              .where(eq(SessionPromptQueueAliasTable.item_id, row.id))
              .all()
            const added = keys.filter((id) => !aliases.some((alias) => alias.id === id))
            if (added.length)
              yield* tx
                .insert(SessionPromptQueueAliasTable)
                .values(added.map((id) => ({ id, item_id: row.id })))
                .run()
            if (added.length && decodeInput(row.input).parts.some(pendingOriginal)) {
              const original = originalsOf(row).find((original) => sameQueuedInput(original.input, input))!
              yield* tx
                .update(SessionPromptQueueTable)
                .set({
                  input: encodeInput({
                    ...decodeInput(row.input),
                    parts: [
                      ...decodeInput(row.input).parts,
                      ...added.map((id) => originalWitness(original, PartID.make(id), true)),
                    ],
                  }),
                })
                .where(eq(SessionPromptQueueTable.id, row.id))
                .run()
            }
            // A new alias accepted during preparation must be included in the eventual committed witness parts.
            if (row.message_id && !row.time_promoted && added.length)
              yield* tx
                .update(SessionPromptQueueTable)
                .set({ message_id: null })
                .where(and(eq(SessionPromptQueueTable.message_id, row.message_id), pending(input.sessionID)))
                .run()
            if (row.time_promoted && row.message_id) {
              const machine = dispatch(decodeInput(row.input))
              yield* rememberAliases(
                tx,
                fromRow(row),
                row.message_id,
                machine?.kind === "peer" ? "peer" : (machine?.type ?? "individual"),
                keys,
              )
            }
            return { kind: "row" as const, row }
          }
          // Known originals have already reconciled above; a bare projection is never executed again.
          yield* assertIdentityIn(tx, input.sessionID, input.messageID, false)
          if (existingOnly) return { kind: "absent" as const }
          const allocated = yield* tx
            .insert(SessionPromptQueueSequenceTable)
            .values({ session_id: input.sessionID, seq: 1, aliases_indexed: true })
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
              time_preparing: deferred ? now : null,
            })
            .returning()
            .get()
          if (!row) return yield* Effect.die(new Error(`Queue admission for ${input.sessionID} stored nothing`))
          if (keys.length)
            yield* tx
              .insert(SessionPromptQueueAliasTable)
              .values(keys.map((id) => ({ id, item_id: row.id })))
              .run()
          if (input.requestID)
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
          // A row its sender still prepares merges with nothing until `prepared`.
          if (deferred) return { kind: "row" as const, row, replacement: undefined }
          return { kind: "row" as const, row, replacement: yield* replace(tx, input.sessionID, row, markers) }
        }).pipe(
          Effect.tap((accepted) => {
            if (!input.requestID || accepted.kind === "absent") return Effect.void
            const delivered = accepted.kind === "delivered" || accepted.row.time_promoted !== null
            const original = accepted.kind === "row" ? accepted.row.request_id : undefined
            return tx
              .insert(SessionPromptReceiptTable)
              .values({
                session_id: input.sessionID,
                request_id: input.requestID,
                state: original && original !== input.requestID ? "merged" : delivered ? "delivered" : "pending",
                item_id: accepted.kind === "row" && !delivered ? accepted.row.id : null,
                message_id:
                  accepted.kind === "delivered" ? accepted.messageID : delivered ? accepted.row.message_id : null,
                into_request_id: original && original !== input.requestID ? original : null,
                fingerprint: fingerprint(input),
                time_created: now,
                time_updated: now,
              })
              .onConflictDoNothing()
              .run()
          }),
          Effect.tap((accepted) =>
            accepted.kind === "absent"
              ? Effect.void
              : // The retry owns the existing answer once committed, before alias observers can suspend admission.
                DatabaseTransaction.afterCommit(
                  accepted.kind === "delivered"
                    ? openChannel(accepted.item, accepted.messageID, callerScope)
                    : openChannel(
                        fromRow(accepted.row),
                        accepted.row.time_promoted && accepted.row.message_id ? accepted.row.message_id : undefined,
                        callerScope,
                      ).pipe(
                        Effect.andThen(
                          Effect.sync(() => {
                            if ("replacement" in accepted && accepted.replacement) {
                              retire(accepted.replacement.retired, accepted.row.id)
                              if (accepted.replacement.into) retire([accepted.row.id], accepted.replacement.into)
                              finish(accepted.replacement.finished)
                            }
                          }),
                        ),
                      ),
                  true,
                ),
          ),
        ),
      ).pipe(Effect.orDie)
      if (result.kind === "absent") return undefined
      if (result.kind === "delivered") return result.item
      const item = fromRow(result.row)
      if ("replacement" in result && result.replacement) {
        retire(result.replacement.retired, item.id)
        if (result.replacement.into) retire([item.id], result.replacement.into)
        finish(result.replacement.finished)
      }
      if (result.row.time_preparing !== null && !preparing.has(item.id)) preparing.set(item.id, Deferred.makeUnsafe())
      if (!result.row.time_promoted && result.row.time_preparing === null) wake(input.sessionID)
      yield* publish(input.sessionID)
      return item
    })

    const admitLocked = (input: AdmitInput, markers?: ConfigV1.Info["machine_message_markers"], deferred = false) =>
      reconcileAdmission(input, false, markers, deferred).pipe(
        Effect.flatMap((item) =>
          item ? Effect.succeed(item) : Effect.die(new Error("admission did not allocate an original")),
        ),
      )

    const reconcileLocked = Effect.fnUntraced(
      function* (input: AdmitInput, deferred = false) {
        yield* assertNotPreparing(input.sessionID, input.messageID)
        const keys = constituentIDs(input.sessionID, input)
        if (!keys.length) return undefined
        const parts = yield* db.select().from(PartTable).where(inArray(PartTable.id, keys)).all().pipe(Effect.orDie)
        const direct = parts.flatMap((part) => {
          const decoded = Schema.decodeUnknownOption(NoReplyWitness)(part.data)
          return Option.isSome(decoded) ? [{ part, original: decoded.value.metadata.sessionQueueNoReply }] : []
        })
        if (direct.length) {
          if (
            direct.length !== parts.length ||
            direct.some(
              ({ part, original }) =>
                part.session_id !== input.sessionID ||
                part.message_id !== direct[0].part.message_id ||
                original.sessionID !== input.sessionID ||
                original.input.messageID !== part.message_id ||
                !isDeepStrictEqual(
                  withoutUndefined({
                    ...Struct.omit(original.input, ["messageID"]),
                    delivery: deferred ? undefined : (original.input.delivery ?? "steer"),
                  }),
                  withoutUndefined({
                    ...Struct.omit(input, ["sessionID", "messageID"]),
                    delivery: deferred ? undefined : (input.delivery ?? "steer"),
                  }),
                ),
            )
          )
            return yield* Effect.die(
              new HistoricalIdentityError({
                sessionID: input.sessionID,
                messageID: input.messageID ?? MessageID.make(direct[0].part.message_id),
                message: `Conflicting direct noReply original: ${input.messageID}`,
              }),
            )
          const aliases = yield* db
            .select({ id: SessionPromptQueueAliasTable.id })
            .from(SessionPromptQueueAliasTable)
            .where(inArray(SessionPromptQueueAliasTable.id, keys))
            .limit(1)
            .get()
          const collision = input.messageID
            ? yield* db
                .select({ id: MessageTable.id })
                .from(MessageTable)
                .where(eq(MessageTable.id, input.messageID))
                .get()
            : undefined
          if (aliases || (collision && collision.id !== direct[0].part.message_id))
            return yield* Effect.die(
              new HistoricalIdentityError({
                sessionID: input.sessionID,
                messageID: input.messageID ?? MessageID.make(direct[0].part.message_id),
                message: "Conflicting direct noReply identity owner",
              }),
            )
          const missing = keys.filter((id) => !parts.some((part) => part.id === id))
          if (missing.length)
            yield* writable(input.sessionID, () =>
              Effect.forEach(
                missing,
                (id) =>
                  events.publish(SessionV1.Event.PartUpdated, {
                    sessionID: input.sessionID,
                    part: {
                      ...direct[0].part.data,
                      id,
                      sessionID: input.sessionID,
                      messageID: MessageID.make(direct[0].part.message_id),
                    } as SessionV1.TextPart,
                    time: Date.now(),
                  }),
                { discard: true },
              ),
            )
          return {
            kind: "direct" as const,
            message: yield* MessageV2.get({ sessionID: input.sessionID, messageID: direct[0].part.message_id }).pipe(
              Effect.provideService(Database.Service, database),
              Effect.orDie,
            ),
          }
        }
        const own = yield* reconcileAdmission(input, true, undefined, deferred)
        return own ? { kind: "queued" as const, own } : undefined
      },
      (effect) => effect.pipe(Effect.orDie),
    )
    const reconcile: Interface["reconcile"] = (input) => exclusive(input.sessionID, reconcileLocked(input))

    const assertFreshIdentityLocked = (sessionID: SessionID, messageID?: MessageID) =>
      !messageID
        ? Effect.void
        : Effect.gen(function* () {
            yield* assertNotPreparing(sessionID, messageID)
            const projected = yield* db
              .select({ id: MessageTable.id })
              .from(MessageTable)
              .where(eq(MessageTable.id, messageID))
              .get()
            const keys = constituentIDs(sessionID, { messageID, parts: [] })
            const aliases = yield* db
              .select({ id: SessionPromptQueueAliasTable.id })
              .from(SessionPromptQueueAliasTable)
              .where(inArray(SessionPromptQueueAliasTable.id, keys))
              .limit(1)
              .get()
            const sequence = yield* db
              .select({ indexed: SessionPromptQueueSequenceTable.aliases_indexed })
              .from(SessionPromptQueueSequenceTable)
              .where(eq(SessionPromptQueueSequenceTable.session_id, sessionID))
              .get()
            const pending = sequence?.indexed
              ? undefined
              : yield* db
                  .select({ id: SessionPromptQueueTable.id })
                  .from(SessionPromptQueueTable)
                  .where(
                    and(
                      eq(SessionPromptQueueTable.session_id, sessionID),
                      sql`CASE WHEN json_valid(${SessionPromptQueueTable.input}) THEN json_extract(${SessionPromptQueueTable.input}, '$.messageID') END = ${messageID}`,
                    ),
                  )
                  .limit(1)
                  .get()
            if (projected || aliases || pending)
              return yield* Effect.die(
                new HistoricalIdentityError({
                  sessionID,
                  messageID,
                  message: `Executable retry ${messageID} lacks retained command/shell provenance; refused before expansion`,
                }),
              )
          }).pipe(Effect.orDie)
    const assertFreshIdentity: Interface["assertFreshIdentity"] = (sessionID, messageID) =>
      exclusive(sessionID, assertFreshIdentityLocked(sessionID, messageID))
    const prepareExecutable: Interface["prepareExecutable"] = (input) => {
      const key = input.messageID ? JSON.stringify([input.sessionID, input.messageID]) : undefined
      const release = (owner: symbol) =>
        Effect.sync(() => {
          if (key && preparations.get(key) === owner) preparations.delete(key)
        })
      return Effect.acquireUseRelease(
        exclusive(
          input.sessionID,
          Effect.gen(function* () {
            yield* assertFreshIdentityLocked(input.sessionID, input.messageID)
            const owner = Symbol()
            if (key) preparations.set(key, owner)
            return owner
          }),
        ),
        (owner) =>
          Effect.gen(function* () {
            // A caller interrupt must not abandon native IO or hook promises and reopen the identity.
            const prepared = yield* input.prepare().pipe(Effect.uninterruptible)
            return yield* exclusive(
              input.sessionID,
              Effect.gen(function* () {
                // Handoff to durable custody is serialized, but shell and hook IO never hold the session permit.
                yield* release(owner)
                yield* assertFreshIdentityLocked(input.sessionID, input.messageID)
                const own = yield* admitLocked(prepared)
                return { input: prepared, own }
              }),
            )
          }),
        release,
      )
    }

    const writeOriginal = <P extends SessionV1.WithParts>(
      input: { admission: AdmitInput; write: (prepared: P) => Effect.Effect<SessionV1.WithParts> },
      prepared: P,
    ) =>
      Effect.gen(function* () {
        const message = yield* input.write(prepared)
        if (input.admission.noReply !== true) return message
        const original = { ...input.admission, messageID: message.info.id }
        const parts: SessionV1.TextPart[] = constituentIDs(input.admission.sessionID, original).map((id) => ({
          type: "text",
          id,
          sessionID: input.admission.sessionID,
          messageID: message.info.id,
          text: "",
          ignored: true,
          synthetic: true,
          metadata: {
            sessionQueueNoReply: Schema.encodeSync(NoReplyOriginal)({
              version: 1,
              sessionID: input.admission.sessionID,
              input: { ...original, noReply: true },
            }),
          },
        }))
        yield* writable(input.admission.sessionID, () =>
          Effect.forEach(
            parts,
            (part) =>
              events.publish(SessionV1.Event.PartUpdated, {
                sessionID: input.admission.sessionID,
                part,
                time: Date.now(),
              }),
            { discard: true },
          ),
        )
        return { ...message, parts: [...message.parts, ...parts] }
      }).pipe(Effect.orDie)

    const writeDirect: Interface["writeDirect"] = (input) =>
      exclusive(
        input.admission.sessionID,
        Effect.gen(function* () {
          const retry = yield* reconcileLocked(input.admission)
          if (retry?.kind === "direct") return retry.message
          if (retry?.kind === "queued") {
            const projected = yield* message(retry.own.id)
            if (projected) return projected
            return yield* Effect.die(new Error("noReply retry has no projected original"))
          }
          return yield* writeOriginal(input, yield* input.prepare())
        }),
      )

    // Applies `admission` for a just-admitted row, in its transaction: the row
    // takes in identical copies and repeated peer requests and retires the
    // notices it supersedes, taking the place of the oldest of them, and then
    // joins a command it continues as that command's next step.
    // The newest row of the session admitted before `before` whose sender is
    // still preparing it here: no admission after it may reach past it.
    const barrier = (tx: DatabaseTransaction, sessionID: SessionID, before: ItemID) =>
      preparing.size === 0
        ? Effect.succeed(undefined)
        : tx
            .select({ id: SessionPromptQueueTable.id })
            .from(SessionPromptQueueTable)
            .where(
              and(
                eq(SessionPromptQueueTable.session_id, sessionID),
                isNotNull(SessionPromptQueueTable.time_preparing),
                lt(SessionPromptQueueTable.id, before),
                inArray(SessionPromptQueueTable.id, [...preparing.keys()]),
              ),
            )
            .orderBy(desc(SessionPromptQueueTable.id))
            .limit(1)
            .get()
            .pipe(
              Effect.orDie,
              Effect.map((row) => row?.id),
            )

    // Once a preparation ends, each ready row admitted after it, up to the next
    // preparation, takes its admission again in admission order: it was bounded
    // by that preparation and can now reach past it.
    const readmit = Effect.fnUntraced(function* (
      tx: DatabaseTransaction,
      sessionID: SessionID,
      after: ItemID,
      markers?: ConfigV1.Info["machine_message_markers"],
    ) {
      const next =
        preparing.size === 0
          ? undefined
          : yield* tx
              .select({ id: SessionPromptQueueTable.id })
              .from(SessionPromptQueueTable)
              .where(
                and(
                  eq(SessionPromptQueueTable.session_id, sessionID),
                  isNotNull(SessionPromptQueueTable.time_preparing),
                  gt(SessionPromptQueueTable.id, after),
                  inArray(SessionPromptQueueTable.id, [...preparing.keys()]),
                ),
              )
              .orderBy(asc(SessionPromptQueueTable.id))
              .limit(1)
              .get()
              .pipe(Effect.orDie)
      const later = yield* tx
        .select({ id: SessionPromptQueueTable.id })
        .from(SessionPromptQueueTable)
        .where(
          and(
            promotable(sessionID),
            gt(SessionPromptQueueTable.id, after),
            next ? lt(SessionPromptQueueTable.id, next.id) : undefined,
          ),
        )
        .orderBy(asc(SessionPromptQueueTable.id))
        .all()
        .pipe(Effect.orDie)
      const readmitted = yield* Effect.forEach(later, (row) =>
        Effect.gen(function* () {
          const current = yield* rowOf(tx, row.id)
          if (!current || current.time_withdrawn !== null || current.time_promoted !== null) return []
          return [{ id: current.id, ...(yield* replace(tx, sessionID, current, markers)) }]
        }),
      )
      return readmitted.flat()
    })

    const replace = Effect.fnUntraced(function* (
      tx: DatabaseTransaction,
      sessionID: SessionID,
      added: typeof SessionPromptQueueTable.$inferSelect,
      markers?: ConfigV1.Info["machine_message_markers"],
    ) {
      // Admission order is the ascending item id: seq is a delivery slot a
      // survivor inherits. A row takes in only rows admitted before it, and none
      // from before a still-preparing row admitted before it (#249).
      const bound = yield* barrier(tx, sessionID, added.id)
      const pendingOf = () =>
        tx
          .select()
          .from(SessionPromptQueueTable)
          .where(
            and(
              promotable(sessionID),
              eq(SessionPromptQueueTable.delivery, added.delivery),
              lt(SessionPromptQueueTable.id, added.id),
              bound ? gt(SessionPromptQueueTable.id, bound) : undefined,
            ),
          )
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
      const proposed = admission(rows.map(fromRow), fromRow(added), markers)
      const peer = dispatch(decodeInput(added.input))?.kind === "peer"
      const bounded = proposed.absorbed.reduce(
        (kept, id) => {
          const row = rows.find((row) => row.id === id)!
          const size = originalsOf(row).length
          return !peer || kept.size + size <= 10 ? { size: kept.size + size, ids: [...kept.ids, id] } : kept
        },
        { size: originalsOf(added).length, ids: [] as ItemID[] },
      )
      const plan = { ...proposed, absorbed: bounded.ids }
      const retired = [...plan.absorbed, ...plan.superseded]
      if (retired.length > 0) {
        if (plan.absorbed.length)
          yield* retainOriginals(
            tx,
            added,
            rows.filter((row) => plan.absorbed.includes(row.id)),
          )
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
      yield* retainOriginals(tx, head, [survivor])
      const retained = (yield* rowOf(tx, head.id))!
      yield* tx
        .update(SessionPromptQueueTable)
        .set({
          input: encodeInput({
            ...merged.input,
            parts: [...merged.input.parts, ...decodeInput(retained.input).parts.filter(pendingOriginal)],
          }),
          origins: [...merged.origins],
          time_created: merged.time.created,
        })
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

    // Retired carriers move their exact original/alias custody into the survivor before cascade deletion.
    const retainOriginals = Effect.fnUntraced(function* (
      tx: DatabaseTransaction,
      target: typeof SessionPromptQueueTable.$inferSelect,
      sources: readonly (typeof SessionPromptQueueTable.$inferSelect)[],
    ) {
      const carriers = [...sources, target]
      const aliases = yield* tx
        .select()
        .from(SessionPromptQueueAliasTable)
        .where(
          inArray(
            SessionPromptQueueAliasTable.item_id,
            carriers.map((row) => row.id),
          ),
        )
        .all()
      const parts = carriers.flatMap((row) =>
        originalWitnesses(
          row,
          aliases.filter((alias) => alias.item_id === row.id),
          true,
        ),
      )
      yield* tx
        .update(SessionPromptQueueTable)
        .set({
          input: encodeInput({
            ...decodeInput(target.input),
            parts: [...decodeInput(target.input).parts.filter((part) => !pendingOriginal(part)), ...parts],
          }),
        })
        .where(eq(SessionPromptQueueTable.id, target.id))
        .run()
      yield* tx
        .update(SessionPromptQueueAliasTable)
        .set({ item_id: target.id })
        .where(
          inArray(
            SessionPromptQueueAliasTable.item_id,
            sources.map((row) => row.id),
          ),
        )
        .run()
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

    const request: Interface["request"] = (input, markers, deferred) =>
      exclusive(
        input.sessionID,
        Effect.gen(function* () {
          yield* sweep(input.sessionID)
          const prior = input.requestID ? yield* receiptRow(input.sessionID, input.requestID) : undefined
          // Identity reconciliation runs here, in the same section as the
          // admission, so a preparing prompt needs nothing read before it.
          const reconciled = !prior && deferred ? yield* reconcileLocked(input, true) : undefined
          if (reconciled?.kind === "direct") return reconciled
          if (reconciled) return { kind: "reconciled" as const, own: reconciled.own }
          if (!prior) return { kind: "admitted" as const, own: yield* admitLocked(input, markers, deferred) }
          if (prior.fingerprint !== fingerprint(input))
            return yield* new RequestConflictError({ sessionID: input.sessionID, requestID: prior.request_id })
          // A receipt never grants ownership of a foreign or conflicting native/peer transport identity.
          yield* reconcileLocked(input, deferred)
          return { kind: "retry" as const, own: yield* follow(prior), receipt: toReceipt(prior) }
        }),
      )

    const finishPreparing: Interface["prepared"] = (input) =>
      exclusive(
        input.sessionID,
        Effect.gen(function* () {
          const ended = endPreparing(input.itemID)
          const admitted = yield* writable(input.sessionID, (tx) =>
            Effect.gen(function* () {
              const row = yield* tx
                .update(SessionPromptQueueTable)
                .set({ time_preparing: null, ...(input.delivery ? { delivery: input.delivery } : {}) })
                .where(
                  and(
                    eq(SessionPromptQueueTable.id, input.itemID),
                    eq(SessionPromptQueueTable.session_id, input.sessionID),
                    isNotNull(SessionPromptQueueTable.time_preparing),
                  ),
                )
                .returning()
                .get()
                .pipe(Effect.orDie)
              if (!row) return undefined
              // A withdrawn one merges with nothing; a restore makes it pending as it is.
              const own =
                row.time_withdrawn === null
                  ? [{ id: row.id, ...(yield* replace(tx, input.sessionID, row, input.markers)) }]
                  : []
              return [...own, ...(yield* readmit(tx, input.sessionID, row.id, input.markers))]
            }),
          ).pipe(Effect.orDie)
          yield* ended
          if (!admitted) return
          admitted.forEach((admission) => {
            retire(admission.retired, admission.id)
            if (admission.into) retire([admission.id], admission.into)
            finish(admission.finished)
          })
          wake(input.sessionID)
          yield* publish(input.sessionID)
        }),
      )

    const awaitPrepared: Interface["awaitPrepared"] = (itemID) => {
      const done = preparing.get(itemID)
      return done ? Deferred.await(done).pipe(Effect.withSpan("SessionQueue.awaitPrepared")) : Effect.void
    }

    const unprepared: Interface["unprepared"] = (sessionID, itemID, markers) =>
      exclusive(
        sessionID,
        Effect.gen(function* () {
          const ended = endPreparing(itemID)
          const dropped = yield* dropPreparing(sessionID, [itemID])
          const readmitted = yield* writable(sessionID, (tx) => readmit(tx, sessionID, itemID, markers)).pipe(
            Effect.orDie,
          )
          yield* ended
          readmitted.forEach((admission) => {
            retire(admission.retired, admission.id)
            if (admission.into) retire([admission.id], admission.into)
            finish(admission.finished)
          })
          if (dropped || readmitted.some((admission) => admission.retired.length > 0 || admission.into))
            yield* publish(sessionID)
        }),
      )

    // Drops preparing rows a stopped process left: their senders are gone, so
    // they never become ready. Under the session's lock. Process-local, like the
    // queue's drains (swxtchio/swx-opencode#254).
    const sweep = Effect.fnUntraced(function* (sessionID: SessionID) {
      const left = (yield* db
        .select({ id: SessionPromptQueueTable.id })
        .from(SessionPromptQueueTable)
        .where(
          and(
            eq(SessionPromptQueueTable.session_id, sessionID),
            isNotNull(SessionPromptQueueTable.time_preparing),
            isNull(SessionPromptQueueTable.time_withdrawn),
          ),
        )
        .all()
        .pipe(Effect.orDie))
        .map((row) => row.id)
        .filter((id) => !preparing.has(id))
      if (left.length > 0 && (yield* dropPreparing(sessionID, left))) yield* publish(sessionID)
    })

    // Removes preparing rows with their requests' receipts, as if never admitted.
    // A withdrawn one stays, with its receipt: its text is the user's now, and
    // its request must still answer as withdrawn rather than be admitted again.
    const dropPreparing = (sessionID: SessionID, itemIDs: readonly ItemID[]) =>
      writable(sessionID, (tx) =>
        Effect.gen(function* () {
          const rows = yield* tx
            .delete(SessionPromptQueueTable)
            .where(
              and(
                eq(SessionPromptQueueTable.session_id, sessionID),
                inArray(SessionPromptQueueTable.id, [...itemIDs]),
                isNotNull(SessionPromptQueueTable.time_preparing),
                isNull(SessionPromptQueueTable.time_withdrawn),
              ),
            )
            .returning({ id: SessionPromptQueueTable.id })
            .all()
            .pipe(Effect.orDie)
          if (rows.length === 0) return false
          yield* tx
            .delete(SessionPromptReceiptTable)
            .where(
              and(
                eq(SessionPromptReceiptTable.session_id, sessionID),
                inArray(
                  SessionPromptReceiptTable.item_id,
                  rows.map((row) => row.id),
                ),
              ),
            )
            .run()
            .pipe(Effect.orDie)
          return true
        }),
      ).pipe(Effect.orDie)

    // A retry's waiter: it follows the item carrying the request, or a newer
    // request that superseded it, or takes the message the request became.
    const follow = Effect.fnUntraced(function* (start: typeof SessionPromptReceiptTable.$inferSelect) {
      const own = ItemID.create()
      const channel = {
        sessionID: start.session_id,
        owners: new Map<symbol, Scope.Scope | undefined>(),
        message: undefined as MessageID | undefined,
        reply: undefined as SessionV1.WithParts | undefined,
      }
      channels.set(own, channel)
      const callerScope = Option.getOrUndefined(yield* Effect.serviceOption(Scope.Scope))
      const owner = Symbol()
      channel.owners.set(owner, callerScope)
      if (callerScope)
        yield* Scope.addFinalizer(
          callerScope,
          Effect.sync(() => {
            channel.owners.delete(owner)
            if (!channel.owners.size) {
              channels.delete(own)
              follows.delete(own)
            }
          }),
        )
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

    // The request an item carries is withdrawn with it and pending again when
    // it is restored (#249). Requests merged into it keep naming that request.
    const carried = (
      tx: DatabaseTransaction,
      sessionID: SessionID,
      itemID: ItemID,
      from: "pending" | "withdrawn",
      to: "pending" | "withdrawn",
    ) =>
      tx
        .update(SessionPromptReceiptTable)
        .set({ state: to, time_updated: Date.now() })
        .where(
          and(
            eq(SessionPromptReceiptTable.session_id, sessionID),
            eq(SessionPromptReceiptTable.item_id, itemID),
            eq(SessionPromptReceiptTable.state, from),
          ),
        )
        .run()
        .pipe(Effect.orDie)

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
          const retry = yield* reconcileLocked(input.admission)
          if (retry) return retry
          yield* assertIdentityIn(db, input.admission.sessionID, input.admission.messageID)
          const keys = constituentIDs(input.admission.sessionID, input.admission)
          const witnessed = keys.length
            ? yield* db
                .select({ id: PartTable.id })
                .from(PartTable)
                .where(inArray(PartTable.id, keys))
                .limit(1)
                .get()
                .pipe(Effect.orDie)
            : undefined
          if (witnessed) return { kind: "queued" as const, own: yield* admitLocked(input.admission) }
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
            if (duplicate) {
              const callerScope = Option.getOrUndefined(yield* Effect.serviceOption(Scope.Scope))
              yield* openChannel(duplicate, undefined, callerScope)
              return { kind: "queued" as const, own: duplicate, prepared }
            }
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
            const message = yield* writeOriginal({ ...input, admission }, rekeyed)
            if (yield* input.isBusy) {
              yield* input.discard(message)
              return { kind: "queued" as const, own: yield* admitLocked(admission), prepared: rekeyed }
            }
            return { kind: "direct" as const, message }
          }
          if (dispatch(input.admission)) return { kind: "queued" as const, own: yield* admitLocked(input.admission) }
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
          const message = yield* writeOriginal(input, prepared)
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
            Effect.gen(function* () {
              const row = yield* tx
                .update(SessionPromptQueueTable)
                .set({ time_withdrawn: Date.now(), message_id: null })
                .where(and(eq(SessionPromptQueueTable.id, itemID), pending(sessionID)))
                .returning()
                .get()
                .pipe(Effect.orDie)
              if (row) yield* carried(tx, sessionID, itemID, "pending", "withdrawn")
              return row
            }),
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
        Effect.gen(function* () {
          const row = yield* tx
            .update(SessionPromptQueueTable)
            .set({ time_withdrawn: null })
            .where(
              and(
                eq(SessionPromptQueueTable.id, itemID),
                eq(SessionPromptQueueTable.session_id, sessionID),
                isNotNull(SessionPromptQueueTable.time_withdrawn),
                // One whose sender stopped preparing it would never be prepared.
                preparing.has(itemID) ? undefined : isNull(SessionPromptQueueTable.time_preparing),
              ),
            )
            .returning()
            .get()
            .pipe(Effect.orDie)
          if (row) yield* carried(tx, sessionID, itemID, "withdrawn", "pending")
          return row
        }),
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
      // A promotion prepared under the old delivery must not commit under it (#240).
      const updated = yield* writable(input.sessionID, (tx) =>
        Effect.gen(function* () {
          const finished = yield* invalidate(tx, input.sessionID)
          const row = yield* tx
            .update(SessionPromptQueueTable)
            .set({ delivery: input.delivery })
            .where(and(eq(SessionPromptQueueTable.id, input.itemID), pending(input.sessionID)))
            .returning()
            .get()
            .pipe(Effect.orDie)
          return { row, finished }
        }),
      ).pipe(Effect.orDie)
      finish(updated.finished)
      const row = updated.row
      if (!row && updated.finished.length > 0) yield* publish(input.sessionID)
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

    // The run a held caller joined could not deliver its item, so it ended on
    // the turn before; that turn's reply is not this item's answer.
    const disposition: Interface["disposition"] = Effect.fn("SessionQueue.disposition")(function* (sessionID, itemID) {
      const before = yield* observe(sessionID, itemID)
      if (before !== "pending") return before
      yield* released(sessionID)
      return yield* observe(sessionID, itemID)
    })

    const observe = Effect.fnUntraced(function* (sessionID: SessionID, itemID: ItemID) {
      const own = channels.get(itemID)
      const carried = channels.get(carrier(itemID))
      if (own?.reply ?? carried?.reply) return "answered" as const
      if (own?.message ?? carried?.message) return "delivered" as const
      const row = yield* db
        .select({ promoted: SessionPromptQueueTable.time_promoted, withdrawn: SessionPromptQueueTable.time_withdrawn })
        .from(SessionPromptQueueTable)
        .where(and(eq(SessionPromptQueueTable.id, carrier(itemID)), eq(SessionPromptQueueTable.session_id, sessionID)))
        .get()
        .pipe(Effect.orDie)
      if (!row || row.withdrawn !== null) return yield* new WithdrawnError({ sessionID, itemID })
      return row.promoted === null ? ("pending" as const) : ("delivered" as const)
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
                .where(promotable(sessionID))
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
              // Receipts settle while every carrier still exists, copies before the
              // command they then join, so a request follows each hop to the last.
              yield* Effect.forEach(plan.into, (merge) =>
                Effect.gen(function* () {
                  const into = yield* rowOf(tx, merge.into)
                  if (into) {
                    const sources = yield* tx
                      .select()
                      .from(SessionPromptQueueTable)
                      .where(inArray(SessionPromptQueueTable.id, merge.items))
                      .all()
                    yield* retainOriginals(tx, into, sources)
                    yield* settleReceipts(tx, sessionID, merge.items, { kind: "merged", into })
                  }
                }),
              )
              if (plan.removed.length > 0)
                yield* tx
                  .delete(SessionPromptQueueTable)
                  .where(inArray(SessionPromptQueueTable.id, plan.removed))
                  .run()
                  .pipe(Effect.orDie)
              yield* Effect.forEach(plan.changed, (changed) =>
                Effect.gen(function* () {
                  const current = (yield* rowOf(tx, changed.id))!
                  return yield* tx
                    .update(SessionPromptQueueTable)
                    .set({
                      input: encodeInput({
                        ...changed.input,
                        parts: [
                          ...changed.input.parts.filter((part) => !pendingOriginal(part)),
                          ...decodeInput(current.input).parts.filter(pendingOriginal),
                        ],
                      }),
                      origins: [...changed.origins],
                      time_created: changed.time.created,
                    })
                    .where(eq(SessionPromptQueueTable.id, changed.id))
                    .run()
                    .pipe(Effect.orDie)
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

    const eligible = (query: DatabaseAccess, sessionID: SessionID, delivery: Delivery, peersOnly = false) =>
      query
        .select()
        .from(SessionPromptQueueTable)
        .where(and(promotable(sessionID), eq(SessionPromptQueueTable.delivery, delivery)))
        .orderBy(asc(SessionPromptQueueTable.seq))
        .all()
        .pipe(
          Effect.orDie,
          Effect.map((rows) =>
            peersOnly
              ? rows.filter((row) => {
                  const decoded = Schema.decodeUnknownOption(SessionPromptQueue.QueuedInput)(row.input)
                  return Option.isSome(decoded) && dispatch(decoded.value, row.delivery)?.kind === "peer"
                })
              : rows,
          ),
        )

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
        const prior = yield* tx
          .select()
          .from(SessionPromptProvenanceTable)
          .where(eq(SessionPromptProvenanceTable.message_id, message))
          .get()
        yield* tx
          .insert(SessionPromptProvenanceTable)
          .values({
            message_id: message,
            session_id: row.session_id,
            origins: [...(prior?.origins ?? []), ...origins(row)],
          })
          .onConflictDoUpdate({
            target: SessionPromptProvenanceTable.message_id,
            set: { origins: [...(prior?.origins ?? []), ...origins(row)] },
          })
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

    // A shared reservation ID fixes membership across process restart. Preparation holds no DB/session lock.
    const reserve = (sessionID: SessionID, delivery: Delivery, isolate = new Set<ItemID>(), peersOnly = false) =>
      exclusive(
        sessionID,
        Effect.gen(function* () {
          if (yield* held(sessionID)) return undefined
          yield* sweep(sessionID)
          if ((delivery === "steer" || peersOnly) && (yield* compacting(sessionID))) return undefined
          const result = yield* writable(sessionID, (tx) =>
            Effect.gen(function* () {
              yield* indexAliases(tx, sessionID)
              const rows = yield* eligible(tx, sessionID, delivery, peersOnly)
              if (!rows.length) return undefined
              const picked = selection(rows)
              const selected = picked.some((row) => isolate.has(row.id)) ? [picked[0]] : picked
              const row = selected[0]
              const decoded = Schema.decodeUnknownExit(SessionPromptQueue.QueuedInput)(row.input)
              if (Exit.isFailure(decoded)) {
                yield* tx.delete(SessionPromptQueueTable).where(eq(SessionPromptQueueTable.id, row.id)).run()
                yield* settleReceipts(tx, sessionID, [row.id], { kind: "deleted" })
                return { kind: "invalid" as const, row, cause: decoded.cause }
              }
              const id =
                row.message_id ??
                (yield* messageID(tx, sessionID, selected.length === 1 ? decoded.value.messageID : undefined))
              const ids = selected.map((item) => item.id)
              const aliases = yield* tx
                .select()
                .from(SessionPromptQueueAliasTable)
                .where(inArray(SessionPromptQueueAliasTable.item_id, ids))
                .all()
              if (yield* landed(tx, id)) {
                for (const original of selected) {
                  const item = fromRow(original)
                  const machine = dispatch(item.input)
                  yield* rememberAliases(
                    tx,
                    item,
                    id,
                    machine?.kind === "peer" ? "peer" : (machine?.type ?? "individual"),
                    aliases.filter((alias) => alias.item_id === item.id).map((alias) => alias.id),
                  )
                }
                yield* Effect.forEach(selected, (row) => promoted(tx, row, id))
                return { kind: "finished" as const, rows: selected, id }
              }
              yield* tx
                .update(SessionPromptQueueTable)
                .set({ message_id: id })
                .where(inArray(SessionPromptQueueTable.id, ids))
                .run()
              return { kind: "reserved" as const, rows: selected, input: grouped(selected, aliases), id }
            }),
          ).pipe(Effect.orDie)
          if (!result) return undefined
          if (result.kind === "finished")
            result.rows.forEach((row) => {
              deliver(row.id, result.id)
            })
          if (result.kind !== "reserved") yield* publish(sessionID)
          return result
        }),
      )

    const releaseReservation = (
      sessionID: SessionID,
      rows: readonly (typeof SessionPromptQueueTable.$inferSelect)[],
      id: MessageID,
    ) =>
      writable(sessionID, (tx) =>
        tx
          .update(SessionPromptQueueTable)
          .set({ message_id: null })
          .where(
            and(
              inArray(
                SessionPromptQueueTable.id,
                rows.map((row) => row.id),
              ),
              eq(SessionPromptQueueTable.message_id, id),
              pending(sessionID),
            ),
          )
          .run(),
      ).pipe(Effect.orDie)

    // The prepared message, all its parts and every constituent's promotion commit together.
    const commit = <P>(
      sessionID: SessionID,
      delivery: Delivery,
      rows: readonly (typeof SessionPromptQueueTable.$inferSelect)[],
      id: MessageID,
      prepared: P,
      write: (prepared: P) => Effect.Effect<unknown>,
      valid?: (prepared: P) => Effect.Effect<boolean>,
      peersOnly = false,
    ) =>
      exclusive(
        sessionID,
        Effect.gen(function* () {
          if ((yield* held(sessionID)) || ((delivery === "steer" || peersOnly) && (yield* compacting(sessionID)))) {
            yield* releaseReservation(sessionID, rows, id)
            return false
          }
          const written = yield* writable(sessionID, (tx) =>
            Effect.gen(function* () {
              const current = yield* eligible(tx, sessionID, delivery, peersOnly)
              const members = current.filter((row) => row.message_id === id)
              // A withdrawn/edited constituent cancels the whole preparation. Concurrent arrivals stay outside it.
              if (
                members.length !== rows.length ||
                rows.some(
                  (row) =>
                    !members.some(
                      (member) =>
                        member.id === row.id && member.seq === row.seq && isDeepStrictEqual(member.input, row.input),
                    ),
                )
              ) {
                yield* tx
                  .update(SessionPromptQueueTable)
                  .set({ message_id: null })
                  .where(and(eq(SessionPromptQueueTable.message_id, id), pending(sessionID)))
                  .run()
                return false
              }
              const picked = selection(current)
              if (picked[0].message_id !== id) {
                yield* tx
                  .update(SessionPromptQueueTable)
                  .set({ message_id: null })
                  .where(and(eq(SessionPromptQueueTable.message_id, id), pending(sessionID)))
                  .run()
                return false
              }
              if (valid && !(yield* valid(prepared))) {
                yield* tx
                  .update(SessionPromptQueueTable)
                  .set({ message_id: null })
                  .where(and(eq(SessionPromptQueueTable.message_id, id), pending(sessionID)))
                  .run()
                return false
              }
              yield* write(prepared)
              yield* Effect.forEach(rows, (row) => promoted(tx, row, id))
              yield* DatabaseTransaction.afterCommit(
                Effect.sync(() => {
                  rows.forEach((row) => {
                    deliver(row.id, id)
                  })
                }),
                true,
              )
              yield* DatabaseTransaction.afterCommit(publish(sessionID))
              return true
            }),
          ).pipe(
            Effect.orDie,
            Effect.onError(() => releaseReservation(sessionID, rows, id)),
          )
          if (!written) yield* publish(sessionID)
          return written
        }),
      )

    const holdFailedOriginal = (
      sessionID: SessionID,
      rows: readonly (typeof SessionPromptQueueTable.$inferSelect)[],
      id: MessageID,
    ) =>
      exclusive(
        sessionID,
        writable(sessionID, (tx) =>
          tx
            .update(SessionPromptQueueTable)
            .set({ message_id: null, time_withdrawn: Date.now() })
            .where(
              and(
                inArray(
                  SessionPromptQueueTable.id,
                  rows.map((row) => row.id),
                ),
                or(eq(SessionPromptQueueTable.message_id, id), isNull(SessionPromptQueueTable.message_id)),
                pending(sessionID),
              ),
            )
            .run(),
        ).pipe(Effect.orDie, Effect.andThen(publish(sessionID))),
      )

    const promote: Interface["promote"] = <P, E>(input: {
      readonly sessionID: SessionID
      readonly delivery: Delivery
      readonly prepare: (input: PromotedInput) => Effect.Effect<P, E>
      readonly write: (prepared: P) => Effect.Effect<unknown>
      readonly valid?: (prepared: P) => Effect.Effect<boolean>
      readonly rejected: (cause: Cause.Cause<E>) => Effect.Effect<void>
      readonly own?: ItemID
      readonly untilStarted?: boolean
      readonly peersOnly?: boolean
    }) =>
      semaphore(order, input.sessionID)
        .withPermit(
          Effect.gen(function* () {
            let changed = false
            const isolate = new Set<ItemID>()
            while (true) {
              if (input.untilStarted && !(yield* empty(input.sessionID))) return changed
              const next = yield* reserve(input.sessionID, input.delivery, isolate, input.peersOnly)
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
              const attempt: Exit.Exit<boolean, E> = Exit.isFailure(prepared)
                ? Exit.failCause(prepared.cause)
                : yield* commit(
                    input.sessionID,
                    input.delivery,
                    next.rows,
                    next.id,
                    prepared.value,
                    input.write,
                    input.valid,
                    input.peersOnly,
                  ).pipe(Effect.exit)
              if (Exit.isFailure(attempt)) {
                const current = yield* exclusive(
                  input.sessionID,
                  db
                    .select({ id: SessionPromptQueueTable.id })
                    .from(SessionPromptQueueTable)
                    .where(
                      and(
                        pending(input.sessionID),
                        eq(SessionPromptQueueTable.message_id, next.id),
                        inArray(
                          SessionPromptQueueTable.id,
                          next.rows.map((row) => row.id),
                        ),
                      ),
                    )
                    .all()
                    .pipe(Effect.orDie),
                )
                // A review or receipt merge owns the changed carrier; a stale preparation cannot drop or hold it.
                if (
                  Exit.isFailure(prepared) &&
                  current.length !== next.rows.length &&
                  !Cause.hasInterrupts(attempt.cause)
                )
                  continue
                // A committed message must never be retried because one of its observers interrupted.
                if (
                  Cause.hasInterrupts(attempt.cause) ||
                  (yield* landed(db, next.id)) ||
                  (Exit.isSuccess(prepared) && sqliteLockMessage(Cause.squash(attempt.cause)))
                ) {
                  yield* exclusive(input.sessionID, releaseReservation(input.sessionID, next.rows, next.id))
                  return yield* Effect.failCause(attempt.cause)
                }
                if (Exit.isSuccess(prepared) && !("group" in next.input)) return yield* Effect.failCause(attempt.cause)
                if ("group" in next.input) {
                  if (next.rows.length > 1) {
                    yield* exclusive(input.sessionID, releaseReservation(input.sessionID, next.rows, next.id))
                    next.rows.forEach((row) => isolate.add(row.id))
                    continue
                  }
                  // Keep the failed original in an explicit, reported hold. Only the existing restore control retries it.
                  yield* holdFailedOriginal(input.sessionID, next.rows, next.id)
                  yield* input.rejected(
                    Cause.die(
                      new Error(
                        `Queue original ${next.rows[0].id} retained in withdrawn hold; restore after resolving failure: ${Cause.pretty(attempt.cause)}`,
                      ),
                    ),
                  )
                  if (next.rows[0].id === input.own) return yield* Effect.failCause(attempt.cause)
                  continue
                }
                yield* exclusive(
                  input.sessionID,
                  writable(input.sessionID, (tx) =>
                    Effect.gen(function* () {
                      yield* tx
                        .delete(SessionPromptQueueTable)
                        .where(eq(SessionPromptQueueTable.id, next.rows[0].id))
                        .run()
                      yield* settleReceipts(tx, input.sessionID, [next.rows[0].id], { kind: "deleted" })
                    }),
                  ).pipe(Effect.orDie, Effect.andThen(publish(input.sessionID))),
                )
                if (next.rows[0].id === input.own) return yield* Effect.failCause(attempt.cause)
                yield* input.rejected(attempt.cause)
                continue
              }
              if (attempt.value && input.delivery === "queue") return true
              if (!attempt.value && input.delivery === "steer" && (yield* compacting(input.sessionID))) return changed
            }
          }),
        )
        .pipe(Effect.withSpan("SessionQueue.promote"))

    // Only an attempt whose consume never ran is made again, so no executed
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
    // hold's end wakes the session instead. Nor is one its sender still prepares,
    // whose `prepared` wakes it.
    const awaitingDrain = Effect.fn("SessionQueue.awaitingDrain")(function* (sessionID: SessionID) {
      if (parked.has(sessionID)) return false
      const row = yield* db
        .select({ id: SessionPromptQueueTable.id })
        .from(SessionPromptQueueTable)
        .where(
          and(
            eq(SessionPromptQueueTable.session_id, sessionID),
            isNull(SessionPromptQueueTable.time_withdrawn),
            isNull(SessionPromptQueueTable.time_preparing),
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
      Effect.gen(function* () {
        const callerScope = Option.getOrUndefined(yield* Effect.serviceOption(Scope.Scope))
        yield* Effect.sync(() => {
          const channel = channels.get(itemID)
          if (!channel) return
          const owned = [...channel.owners].find(([, scope]) => scope === callerScope)
          if (!owned) return
          channel.owners.delete(owned[0])
          if (!channel.owners.size) {
            channels.delete(itemID)
            follows.delete(itemID)
          }
        })
      })

    const reply = (itemID: ItemID) =>
      Effect.sync(() => {
        const answered = channels.get(itemID)?.reply ?? channels.get(carrier(itemID))?.reply
        return answered
      })

    const message: Interface["message"] = (itemID) =>
      Effect.gen(function* () {
        const messageID = channels.get(itemID)?.message ?? channels.get(carrier(itemID))?.message
        const sessionID = channels.get(itemID)?.sessionID
        if (!messageID || !sessionID) return undefined
        return yield* MessageV2.get({ sessionID, messageID }).pipe(
          Effect.provideService(Database.Service, database),
          Effect.orDie,
        )
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
                text: deliveryNote(
                  found,
                  message.info.time.created,
                  !message.parts.some((part) => {
                    if (part.type !== "text" || part.ignored) return false
                    const group = Schema.decodeUnknownOption(SessionPromptQueue.Group)(part.metadata?.sessionQueue)
                    return Option.isSome(group) && group.value.kind !== "individual"
                  }),
                ),
                synthetic: true,
              }
              return { ...message, parts: [note, ...message.parts] }
            })
          }),
        )

    return Service.of({
      admit,
      reconcile,
      assertFreshIdentity,
      prepareExecutable,
      writeDirect,
      message,
      assertIdentity,
      request,
      prepared: finishPreparing,
      unprepared,
      awaitPrepared,
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
      disposition,
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

function deliveredOriginal(
  row: Pick<typeof PartTable.$inferSelect, "id" | "session_id" | "message_id"> & { data: unknown },
  sessionID: SessionID,
) {
  const part = Schema.decodeUnknownOption(SessionV1.TextPart)({
    ...(row.data !== null && typeof row.data === "object" ? row.data : {}),
    id: row.id,
    sessionID: row.session_id,
    messageID: row.message_id,
  })
  const group = Schema.decodeUnknownOption(SessionPromptQueue.Group)(
    Option.getOrUndefined(part)?.metadata?.sessionQueue,
  )
  if (
    Option.isNone(part) ||
    !part.value.ignored ||
    !part.value.synthetic ||
    Option.isNone(group) ||
    group.value.sessionID !== sessionID ||
    row.session_id !== sessionID ||
    group.value.constituents.length !== 1 ||
    group.value.constituents[0].sessionID !== sessionID
  )
    return
  return {
    item: { ...group.value.constituents[0], origins: [{ time: group.value.constituents[0].time.created }] },
    messageID: MessageID.make(row.message_id),
    groupKind: group.value.kind,
  }
}

function constituentIDs(sessionID: SessionID, input: SessionPromptQueue.QueuedInput) {
  const machine = dispatch(input)
  const identities = [
    ...(input.messageID ? [["message", input.messageID]] : []),
    ...(machine?.kind === "peer" ? [["peer", machine.sender, machine.home, machine.messageID]] : []),
  ]
  return identities.map((identity) =>
    PartID.make(
      "prt_queue_" +
        createHash("sha256")
          .update(JSON.stringify([sessionID, identity]))
          .digest("hex"),
    ),
  )
}

function pendingOriginal(part: SessionPromptQueue.QueuedInput["parts"][number]) {
  if (part.type !== "text" || !part.ignored || !part.synthetic) return
  const group = Schema.decodeUnknownOption(SessionPromptQueue.Group)(part.metadata?.sessionQueuePendingOriginal)
  if (Option.isSome(group) && group.value.constituents.length === 1) return group.value.constituents[0]
}

function originalsOf(row: typeof SessionPromptQueueTable.$inferSelect) {
  const stored = decodeInput(row.input).parts.flatMap((part) => pendingOriginal(part) ?? [])
  return stored.length ? [...new Map(stored.map((original) => [original.id, original])).values()] : [fromRow(row)]
}

function originalWitness(original: SessionPromptQueue.Group["constituents"][number], id: PartID, pending = false) {
  return {
    type: "text" as const,
    id,
    text: "",
    ignored: true,
    synthetic: true,
    metadata: {
      [pending ? "sessionQueuePendingOriginal" : "sessionQueue"]: Schema.encodeSync(SessionPromptQueue.Group)({
        version: 1,
        sessionID: original.sessionID,
        kind: dispatch(original.input)?.kind === "peer" ? "peer" : "individual",
        constituents: [original],
      }),
    },
  }
}

function originalWitnesses(
  row: typeof SessionPromptQueueTable.$inferSelect,
  aliases: readonly (typeof SessionPromptQueueAliasTable.$inferSelect)[],
  pending = false,
) {
  const parts = decodeInput(row.input).parts
  const originals = originalsOf(row)
  return aliases.flatMap((alias) => {
    const recorded = parts.find((part) => part.id === alias.id && pendingOriginal(part))
    const original = recorded
      ? pendingOriginal(recorded)
      : (originals.find((original) =>
          constituentIDs(original.sessionID, original.input).includes(PartID.make(alias.id)),
        ) ?? (originals.length === 1 ? originals[0] : undefined))
    if (!original) throw new Error(`Peer alias ${alias.id} lacks original custody in ${row.id}`)
    return [originalWitness(original, PartID.make(alias.id), pending)]
  })
}

function dispatch(input: SessionPromptQueue.QueuedInput, delivery?: Delivery) {
  input = { ...input, parts: input.parts.filter((part) => !pendingOriginal(part)) }
  if (!input.parts.length || input.parts.some((part) => part.type === "text" && (part.synthetic || part.ignored)))
    return
  // Incompatible execution controls cannot be absorbed into another sender's prompt.
  if (input.format || input.system || input.tools) return
  const kind = MachineMessage.dispatch(
    input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join(""),
  )
  if (input.parts.some((part) => part.type !== "text" && (kind?.kind !== "peer" || part.type !== "file"))) return
  // Configured critical markers and an editor's send-now keep their immediate, individual meaning.
  return kind?.kind === "automation" && delivery === "steer" ? undefined : kind
}

function selection(rows: readonly (typeof SessionPromptQueueTable.$inferSelect)[]) {
  const classified = rows.map((row) => {
    const decoded = Schema.decodeUnknownOption(SessionPromptQueue.QueuedInput)(row.input)
    return { row, kind: Option.isSome(decoded) ? dispatch(decoded.value, row.delivery) : undefined }
  })
  // Native/unknown work remains individual and keeps human authority. Peers precede every automation type.
  const first =
    classified.find((item) => !item.kind) ?? classified.find((item) => item.kind?.kind === "peer") ?? classified[0]
  if (first.row.message_id) return rows.filter((row) => row.message_id === first.row.message_id)
  if (!first.kind) return [first.row]
  if (first.kind.kind === "peer")
    return classified
      .filter((item) => item.kind?.kind === "peer" && !item.row.message_id)
      .reduce(
        (picked, item) => {
          const size = originalsOf(item.row).length
          return !picked.blocked && picked.size + size <= 10
            ? { size: picked.size + size, rows: [...picked.rows, item.row], blocked: false }
            : { ...picked, blocked: true }
        },
        { size: 0, rows: [] as (typeof SessionPromptQueueTable.$inferSelect)[], blocked: false },
      ).rows
  const type = first.kind.type
  return classified
    .filter((item) => item.kind?.kind === "automation" && item.kind.type === type && !item.row.message_id)
    .map((item) => item.row)
}

function grouped(
  rows: readonly (typeof SessionPromptQueueTable.$inferSelect)[],
  aliases: readonly (typeof SessionPromptQueueAliasTable.$inferSelect)[] = [],
) {
  const items = rows.flatMap(originalsOf)
  const kind = dispatch(items[0].input)
  const group: SessionPromptQueue.Group = {
    version: 1,
    sessionID: items[0].sessionID,
    kind: !kind ? "individual" : kind.kind === "peer" ? "peer" : kind.type,
    constituents: items,
  }
  const witnesses = rows.some((row) => decodeInput(row.input).parts.some(pendingOriginal))
    ? rows.flatMap((row) =>
        originalWitnesses(
          row,
          aliases.filter((alias) => alias.item_id === row.id),
        ),
      )
    : items.flatMap((item) => {
        const recorded = aliases.filter((alias) => alias.item_id === item.id).map((alias) => PartID.make(alias.id))
        return (recorded.length ? recorded : constituentIDs(item.sessionID, item.input)).map((id) => ({
          type: "text" as const,
          id,
          text: "",
          ignored: true,
          synthetic: true,
          metadata: { sessionQueue: Schema.encodeSync(SessionPromptQueue.Group)({ ...group, constituents: [item] }) },
        }))
      })
  if (!kind) {
    const visible = decodeInput(rows[0].input)
    return { ...visible, parts: [...visible.parts.filter((part) => !pendingOriginal(part)), ...witnesses] }
  }
  const latest = new Map<string, ItemID>()
  items.forEach((item) => {
    const machine = dispatch(item.input)
    if (machine?.kind === "automation" && machine.episode) latest.set(machine.episode, item.id)
  })
  const shown =
    kind.kind === "peer"
      ? items
      : items.filter((item) => {
          const machine = dispatch(item.input)
          return machine?.kind !== "automation" || !machine.episode || latest.get(machine.episode) === item.id
        })
  const heading =
    kind.kind === "peer"
      ? "Peer messages (lower authority than captain/human instructions). Read every complete original envelope. Reply only to each response-owed original msgid using re:<original-msgid>; informational entries create no reply obligation."
      : `Queued automation: ${kind.type}. Historical producer observations, not current measurements. Latest admitted text per identified episode is shown; the producer owns fresh measurements and pacing. Original constituents remain in sessionQueue metadata.`
  return {
    ...items[0].input,
    model: undefined,
    variant: undefined,
    group,
    parts: [
      {
        type: "text" as const,
        text:
          heading +
          "\n\n" +
          shown
            .map(
              (item, index) =>
                `${index + 1}. [Admitted ${new Date(item.time.created).toISOString()}; queue ${item.id}]\n${item.input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")}`,
            )
            .join("\n\n"),
        metadata: { sessionQueue: Schema.encodeSync(SessionPromptQueue.Group)(group) },
      },
      ...shown.flatMap((item, index) => {
        const files = item.input.parts.filter((part) => part.type === "file")
        return files.length
          ? [
              { type: "text" as const, synthetic: true, text: `Files for numbered peer ${index + 1} (${item.id}):` },
              ...files,
            ]
          : []
      }),
      ...witnesses,
    ],
  }
}

function toReceipt(row: typeof SessionPromptReceiptTable.$inferSelect): Receipt {
  if (row.state === "pending") return { state: "pending", itemID: row.item_id! }
  if (row.state === "withdrawn") return { state: "withdrawn", itemID: row.item_id! }
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
export function deliveryNote(origins: readonly SessionPromptQueue.Origin[], delivered: number, owesReplies = true) {
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
    owesReplies && requests.length > 1
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
 * Keep each delivery class and recipient context intact; the earliest item
 * keeps its place, and merged command steps follow admission order.
 */
export function consolidation(items: readonly Item[], markers?: ConfigV1.Info["machine_message_markers"]) {
  const copies = copyGroups(items, markers)
  const kept = copies.map((group) => ({ item: group.item, collapsed: group.sources.length > 1 }))
  const commands = kept.flatMap((entry) => {
    if (marked(entry.item.input, markers)) return []
    const command = parseCommand(entry.item.input)
    return command ? [{ item: entry.item, command, key: commandKey(entry.item, command) }] : []
  })
  const groups = Map.groupBy(commands, (entry) => entry.key)
  const merges = [...groups.values()].flatMap((group) =>
    group
      .reduce<Array<(typeof group)[number][]>>((batches, entry) => {
        const batch = batches.find((candidate) =>
          candidate.every((other) => sameCommand(other.item, other.command, entry.item, entry.command)),
        )
        if (batch) batch.push(entry)
        else batches.push([entry])
        return batches
      }, [])
      .filter((batch) => batch.length > 1),
  )
  const mergedAway = new Set(merges.flatMap((group) => group.slice(1).map((entry) => entry.item.id)))
  const heads = new Map(
    merges.map((group) => {
      const first = group[0]!
      const text = [
        `${first.command.verb} ${first.command.target}:`,
        ...group
          .flatMap((entry) =>
            entry.command.steps.map((step) => {
              if (normalizeTarget(entry.command.target) === normalizeTarget(first.command.target)) return step
              if (normalizeText(step) === normalizeText(plainText(entry.item.input) ?? "")) return step
              const target = `${entry.command.target}:`
              if (normalizeText(step).startsWith(normalizeText(target))) return step
              return `${target} ${step}`
            }),
          )
          .map((step, index) => `${index + 1}. ${step}`),
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
      ...copies
        .filter((group) => group.sources.length > 1)
        .map((group) => ({ into: group.item.id, items: group.sources.slice(1).map((source) => source.id) })),
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

function copyGroups(items: readonly Item[], markers?: ConfigV1.Info["machine_message_markers"]) {
  const exact = [...Map.groupBy(items, copyKey).values()].map((sources) => ({
    item: {
      ...sources[0]!,
      time: { created: Math.max(...sources.map((source) => source.time.created)) },
      origins: sources.slice(1).reduce((origins, source) => repeated(origins, source.origins), sources[0]!.origins),
    },
    sources,
    order: items.indexOf(sources[0]!),
  }))
  const candidates = exact.flatMap((entry) => {
    const text = plainText(entry.item.input)
    if (!text || marked(entry.item.input, markers)) return []
    const normalized = normalizeText(text)
    if (!normalized) return []
    return [{ entry, text, normalized, context: copyContextKey(entry.item), command: parseCommand(entry.item.input) }]
  })
  const grouped = new Set<(typeof exact)[number]>()
  const merged = [...Map.groupBy(candidates, (candidate) => candidate.context).values()].flatMap((group) => {
    const ordered = group.toSorted(
      (left, right) => right.normalized.length - left.normalized.length || left.entry.order - right.entry.order,
    )
    return ordered.flatMap((candidate) => {
      if (grouped.has(candidate.entry)) return []
      // Same-job command prefixes keep ordered steps; other prefixes retain their longest text.
      const matching = ordered.filter((other) => {
        if (grouped.has(other.entry) || !textPrefix(other.normalized, candidate.normalized)) return false
        if (other.normalized === candidate.normalized) return true
        if (!other.command || !candidate.command) return true
        if (sameCommand(other.entry.item, other.command, candidate.entry.item, candidate.command)) return false
        return targetText(targetSubject(other.command.target)) === targetText(targetSubject(candidate.command.target))
      })
      matching.forEach((other) => grouped.add(other.entry))
      if (matching.length === 1) return [candidate.entry]
      const originals = matching.toSorted((left, right) => left.entry.order - right.entry.order)
      const first = originals[0]!.entry
      return [
        {
          item: {
            ...first.item,
            input: {
              ...first.item.input,
              parts: first.item.input.parts.map((part) =>
                part.type === "text" && !part.synthetic && !pendingOriginal(part)
                  ? { ...part, text: candidate.text }
                  : part,
              ),
            },
            time: { created: Math.max(...originals.map((original) => original.entry.item.time.created)) },
            origins: originals.flatMap((original) => original.entry.item.origins),
          },
          sources: originals.flatMap((original) => original.entry.sources),
          order: first.order,
        },
      ]
    })
  })
  return [...exact.filter((entry) => !grouped.has(entry)), ...merged].toSorted(
    (left, right) => left.order - right.order,
  )
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
    own?.level === "nudge" && own.key && !own.reply && added.requestID
      ? others.filter((item) => {
          if (!item.requestID || absorbed.includes(item)) return false
          const other = envelopeOf(item)
          return other?.sender === own.sender && other.level === "nudge" && other.key === own.key && !other.reply
        })
      : []
  const command = added.requestID && !marked(added.input, markers) ? parseCommand(added.input) : undefined
  const into = command
    ? others.find((item) => {
        if (!item.requestID || absorbed.includes(item) || marked(item.input, markers)) return false
        const other = parseCommand(item.input)
        return other !== undefined && sameCommand(item, other, added, command)
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
  return JSON.stringify([item.delivery, withoutUndefined(comparable(item.input))])
}

// The key keeps commands with different delivery or request context apart.
function commandKey(item: Item, command: { readonly verb: string; readonly target: string }) {
  const normalized = comparable(item.input)
  const context = withoutUndefined({ ...normalized, parts: [{ ...normalized.parts[0], text: "" }] })
  return JSON.stringify([item.delivery, command.verb.toLowerCase(), context])
}

function sameCommand(
  leftItem: Item,
  left: { readonly verb: string; readonly target: string },
  rightItem: Item,
  right: { readonly verb: string; readonly target: string },
) {
  return commandKey(leftItem, left) === commandKey(rightItem, right) && targetMatches(left.target, right.target)
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

const conversationalLeads = new Set([
  "a",
  "an",
  "can",
  "could",
  "do",
  "does",
  "did",
  "how",
  "i",
  "it",
  "please",
  "should",
  "that",
  "the",
  "these",
  "they",
  "this",
  "those",
  "we",
  "what",
  "when",
  "where",
  "which",
  "who",
  "why",
  "would",
  "you",
])

// Common nouns and grammatical words do not identify a target by themselves.
const genericTargetWords = new Set([
  "a",
  "about",
  "after",
  "all",
  "also",
  "additional",
  "although",
  "am",
  "an",
  "any",
  "and",
  "app",
  "application",
  "area",
  "are",
  "as",
  "at",
  "bad",
  "be",
  "because",
  "been",
  "before",
  "being",
  "between",
  "both",
  "but",
  "by",
  "broken",
  "bug",
  "buggy",
  "can",
  "cannot",
  "could",
  "change",
  "changes",
  "class",
  "code",
  "codebase",
  "component",
  "current",
  "did",
  "do",
  "does",
  "deprecated",
  "directory",
  "doc",
  "docs",
  "documentation",
  "duplicate",
  "during",
  "each",
  "either",
  "existing",
  "extra",
  "else",
  "ever",
  "every",
  "few",
  "failure",
  "failures",
  "feature",
  "flaky",
  "for",
  "file",
  "files",
  "from",
  "full",
  "further",
  "had",
  "has",
  "have",
  "he",
  "her",
  "here",
  "hers",
  "herself",
  "him",
  "himself",
  "his",
  "failing",
  "failed",
  "flow",
  "folder",
  "function",
  "general",
  "get",
  "got",
  "hence",
  "how",
  "inaccurate",
  "in",
  "into",
  "is",
  "it",
  "its",
  "itself",
  "incorrect",
  "index",
  "issue",
  "invalid",
  "i",
  "if",
  "just",
  "line",
  "lines",
  "many",
  "may",
  "main",
  "me",
  "might",
  "method",
  "more",
  "most",
  "missing",
  "must",
  "my",
  "myself",
  "neither",
  "module",
  "much",
  "no",
  "nor",
  "not",
  "nothing",
  "now",
  "new",
  "of",
  "once",
  "on",
  "or",
  "other",
  "others",
  "our",
  "ours",
  "ourselves",
  "old",
  "package",
  "part",
  "per",
  "primary",
  "problem",
  "project",
  "prompt",
  "rather",
  "repo",
  "repository",
  "related",
  "same",
  "shall",
  "she",
  "secondary",
  "section",
  "simple",
  "since",
  "slow",
  "so",
  "source",
  "specific",
  "src",
  "stale",
  "such",
  "step",
  "steps",
  "than",
  "that",
  "system",
  "suite",
  "themselves",
  "them",
  "then",
  "there",
  "task",
  "these",
  "the",
  "they",
  "this",
  "test",
  "tests",
  "those",
  "thing",
  "things",
  "though",
  "through",
  "ts",
  "tsx",
  "too",
  "under",
  "until",
  "up",
  "upon",
  "js",
  "jsx",
  "json",
  "md",
  "us",
  "very",
  "was",
  "we",
  "were",
  "what",
  "whatever",
  "when",
  "whenever",
  "where",
  "whereas",
  "wherever",
  "whether",
  "which",
  "whichever",
  "while",
  "who",
  "whoever",
  "whom",
  "whose",
  "why",
  "py",
  "rs",
  "go",
  "java",
  "kt",
  "swift",
  "css",
  "html",
  "xml",
  "yaml",
  "yml",
  "toml",
  "sh",
  "sql",
  "will",
  "with",
  "within",
  "without",
  "would",
  "you",
  "your",
  "yours",
  "yourself",
  "yourselves",
  "to",
  "unused",
  "updated",
  "vulnerable",
  "work",
  "wrong",
])

function plainText(input: SessionPromptQueue.QueuedInput) {
  const parts = input.parts.filter((part) => !pendingOriginal(part))
  if (parts.length !== 1 || parts[0]?.type !== "text" || parts[0].synthetic) return
  return parts[0].text
}

function copyContextKey(item: Item) {
  const input = comparable(item.input)
  return JSON.stringify([
    item.delivery,
    withoutUndefined({
      ...input,
      parts: input.parts.map((part) => (part.type === "text" ? { ...part, text: "" } : part)),
    }),
  ])
}

function normalizeText(text: string) {
  return text.trim().toLowerCase().replace(/\s+/g, " ")
}

function textPrefix(shorter: string, longer: string) {
  return shorter === longer || (longer.startsWith(shorter) && !/[a-z0-9_]/i.test(longer[shorter.length]!))
}

function normalizeTarget(target: string) {
  return target
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[.,;:!?]+$/g, "")
    .replace(/^(?:(?:a|an|the)\s+)+/, "")
}

const targetComplements = new Set([
  "about",
  "after",
  "as",
  "at",
  "before",
  "by",
  "for",
  "from",
  "in",
  "into",
  "of",
  "on",
  "over",
  "through",
  "to",
  "under",
  "with",
  "within",
  "without",
])

const targetClauseConjunctions = new Set(["and", "but", "or"])
const targetClauseOpeners = new Set(["how", "what", "when", "where", "whether", "which", "who", "why"])

const targetFileSuffixes = new Set([
  "ts",
  "tsx",
  "js",
  "jsx",
  "mjs",
  "cjs",
  "py",
  "go",
  "rs",
  "sh",
  "md",
  "json",
  "yaml",
  "yml",
  "toml",
  "proto",
  "sql",
  "c",
  "h",
  "cpp",
  "hpp",
  "rb",
  "php",
  "java",
  "kt",
  "swift",
  "vue",
  "svelte",
  "css",
  "scss",
  "html",
  "env",
  "cfg",
  "ini",
  "lock",
])

function targetSubject(target: string) {
  const normalized = normalizeTarget(target)
  if (targetPaths(target).length > 0) return normalized
  const subject = target
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[.,;:!?]+$/g, "")
    .replace(/^(?:(?:a|an|the)\s+)+/i, "")
  const words = normalized.split(" ")
  const boundary = words.findIndex(
    (word, index) =>
      targetComplements.has(word) || (targetClauseConjunctions.has(word) && targetClauseOpeners.has(words[index + 1]!)),
  )
  if (boundary <= 0) return subject
  return subject.split(" ").slice(0, boundary).join(" ")
}

function targetText(target: string) {
  return normalizeTarget(target)
}

function targetWords(target: string) {
  const paths = new Set(targetPaths(target).map((path) => path.join("/")))
  return (
    targetText(target)
      .match(/[a-z0-9_.-]+(?:[\\/][a-z0-9_.-]+)*/g)
      ?.flatMap((part) => {
        const word = paths.has(part.replace(/\\/g, "/")) ? part.slice(0, part.lastIndexOf(".")) : part
        return word.match(/[a-z0-9]+/g) ?? []
      }) ?? []
  )
}

function targetMatches(left: string, right: string) {
  const leftPaths = targetPaths(left)
  const rightPaths = targetPaths(right)
  if (Boolean(leftPaths.length) !== Boolean(rightPaths.length)) return false
  if (leftPaths.length > 0 && rightPaths.length > 0) {
    const samePaths =
      leftPaths.every((leftPath) =>
        rightPaths.some((rightPath) => pathSuffix(leftPath, rightPath) || pathSuffix(rightPath, leftPath)),
      ) &&
      rightPaths.every((rightPath) =>
        leftPaths.some((leftPath) => pathSuffix(leftPath, rightPath) || pathSuffix(rightPath, leftPath)),
      )
    if (!samePaths) return false
    if (directPathTarget(left, leftPaths) && directPathTarget(right, rightPaths)) return true
  }
  const leftText = targetText(left)
  const rightText = targetText(right)
  if (leftText === rightText) return true
  const leftSubject = targetSubject(left)
  const rightSubject = targetSubject(right)
  const leftSubjectText = targetText(leftSubject)
  const rightSubjectText = targetText(rightSubject)
  const leftIdentity = targetIdentity(leftSubject)
  const rightIdentity = targetIdentity(rightSubject)
  if (containsTarget(leftSubjectText, rightSubjectText) && rightIdentity.nonGeneric.length > 0) return true
  if (containsTarget(rightSubjectText, leftSubjectText) && leftIdentity.nonGeneric.length > 0) return true
  if (!leftIdentity.head || leftIdentity.head !== rightIdentity.head) return false
  if (leftIdentity.evidence.length === 0 || rightIdentity.evidence.length === 0) return false
  return (
    leftIdentity.evidence.every((word) => rightIdentity.evidence.includes(word)) ||
    rightIdentity.evidence.every((word) => leftIdentity.evidence.includes(word))
  )
}

function targetPaths(target: string) {
  const tokens =
    target
      .trim()
      .replace(/[.,;:!?]+$/g, "")
      .match(/[a-z0-9_.-]+(?:[\\/][a-z0-9_.-]+)*/gi) ?? []
  return tokens.filter(targetFileToken).map((token) => normalizeTarget(token).split(/[\\/]/))
}

function targetFileSuffix(target: string) {
  const component = target.trim().split(/[\\/]/).at(-1)!
  const dot = component.lastIndexOf(".")
  if (dot <= 0 || dot === component.length - 1) return undefined
  const suffix = component.slice(dot + 1).toLowerCase()
  return targetFileSuffixes.has(suffix) ? suffix : undefined
}

function targetFileToken(target: string) {
  const component = target.trim().split(/[\\/]/).at(-1)!
  return Boolean(targetFileSuffix(target)) && (/[\\/]/.test(target) || !/[A-Z]/.test(component))
}

function directPathTarget(target: string, paths: readonly (readonly string[])[]) {
  const normalized = normalizeTarget(target).replace(/\\/g, "/")
  return paths.length === 1 && normalized === paths[0]!.join("/")
}

function targetIdentity(target: string) {
  const words = targetWords(target).map(normalizeTargetWord)
  const nonGeneric = words.filter((word) => !genericTargetWords.has(word))
  return {
    head: words.at(-1),
    evidence: words.slice(0, -1).filter((word) => !genericTargetWords.has(word)),
    nonGeneric,
  }
}

function pathSuffix(longer: readonly string[], shorter: readonly string[]) {
  return (
    shorter.length <= longer.length &&
    shorter.every((component, index) => component === longer[longer.length - shorter.length + index])
  )
}

function containsTarget(longer: string, shorter: string) {
  const longerWords = targetWords(longer).map(normalizeTargetWord)
  const shorterWords = targetWords(shorter).map(normalizeTargetWord)
  if (shorterWords.length === 0 || shorterWords.length >= longerWords.length) return false
  return longerWords.some((_, index) => shorterWords.every((word, offset) => word === longerWords[index + offset]))
}

function normalizeTargetWord(word: string) {
  if (word.length > 5 && word.endsWith("ion")) return word.slice(0, -3)
  if (word.length > 5 && word.endsWith("ate")) return word.slice(0, -1)
  if (word.length > 4 && word.endsWith("ies")) return `${word.slice(0, -3)}y`
  if (/(?:ses|xes|zes|ches|shes)$/.test(word)) return word.slice(0, -2)
  if (word.length > 3 && word.endsWith("s") && !word.endsWith("ss") && !word.endsWith("us") && !word.endsWith("is"))
    return word.slice(0, -1)
  return word
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
  input = { ...input, parts: input.parts.filter((part) => !pendingOriginal(part)) }
  const part = input.parts[0]
  if (input.parts.length !== 1 || part?.type !== "text" || part.synthetic) return undefined
  const match = /^\s*(\S+)\s+([\s\S]*?)\s*$/.exec(part.text)
  if (!match || conversationalLeads.has(match[1]!.toLowerCase())) return undefined
  const verb = match[1]!
  const rest = match[2]!.trim()
  if (!rest) return undefined
  // Keep the whole subject phrase; punctuation, options, and a leading file token separate later instructions.
  const separator = /:\s|:$|\.\s+|[!?]\s+|;\s+|\n|\s+(?=--\S)/.exec(rest)
  const candidate = separator ? undefined : /^(?:[^\s\\/]+[\\/])*[^\s\\/]+\.[a-z0-9]+(?=\s+\S)/i.exec(rest)
  const fileCandidate = candidate && targetFileToken(candidate[0]) ? candidate : undefined
  const fileEnd = fileCandidate ? fileCandidate.index + fileCandidate[0].length : undefined
  const end = separator?.index ?? fileEnd ?? rest.length
  const commandTarget = rest.slice(0, end).trim()
  const remainder = separator
    ? rest.slice(separator.index + separator[0].length).trim()
    : fileEnd !== undefined
      ? rest.slice(fileEnd).trim()
      : undefined
  if (!commandTarget) return undefined
  const step = remainder
  if (!step) return { verb, target: commandTarget, steps: [`${verb} ${commandTarget}`] }
  const lines = step.split("\n")
  const numbered = lines.length > 1 && lines.every((line) => /^\d+\.\s+\S/.test(line))
  return {
    verb,
    target: commandTarget,
    steps: numbered ? lines.map((line) => line.replace(/^\d+\.\s+/, "")) : [step],
  }
}

function withoutIDs(input: SessionPromptQueue.QueuedInput) {
  return {
    ...Struct.omit(input, ["messageID"]),
    parts: input.parts.filter((part) => !pendingOriginal(part)).map((part) => Struct.omit(part, ["id"])),
  }
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
  left = { ...left, parts: left.parts.filter((part) => !pendingOriginal(part)) }
  return isDeepStrictEqual(
    withoutUndefined(Struct.omit(left, ["messageID"])),
    withoutUndefined(Struct.omit(right, ["sessionID", "messageID", "noReply", "delivery", "requestID"])),
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
