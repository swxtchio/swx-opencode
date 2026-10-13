import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { DatabaseTransaction } from "@opencode-ai/core/database/transaction"
import {
  SessionPromptQueueAliasTable,
  SessionPromptQueueSequenceTable,
  SessionPromptQueueTable,
} from "@opencode-ai/core/session/prompt-queue.sql"
import { MessageTable, PartTable } from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { and, asc, desc, eq, inArray, isNotNull, isNull, or, sql } from "drizzle-orm"
import { Cause, Context, Duration, Effect, Exit, Fiber, Layer, Option, Schema, Scope, Semaphore, Struct } from "effect"
import { isDeepStrictEqual } from "node:util"
import { createHash } from "node:crypto"
import { EventV2Bridge } from "@/event-v2-bridge"
import { sqliteLockMessage } from "@/util/sqlite-error"
import { MessageV2 } from "./message-v2"
import { MachineMessage } from "./machine-message"
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
const consumeAcquireBackstop = Duration.seconds(10)

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
      return yield* exclusive(input.sessionID, admitLocked(input))
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
            ? constituentIDs(sessionID, decoded.value)
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

    const admission = Effect.fnUntraced(function* (input: AdmitInput, existingOnly = false) {
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
              original.item.delivery !== (input.delivery ?? "steer")
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
              !sameQueuedInput(decoded.value, input) ||
              row.delivery !== (input.delivery ?? "steer")
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
              input: encodeInput(Struct.omit(input, ["sessionID", "noReply", "delivery"])),
              time_created: Date.now(),
            })
            .returning()
            .get()
          if (!row) return yield* Effect.die(new Error(`Queue admission for ${input.sessionID} stored nothing`))
          if (keys.length)
            yield* tx
              .insert(SessionPromptQueueAliasTable)
              .values(keys.map((id) => ({ id, item_id: row.id })))
              .run()
          return { kind: "row" as const, row }
        }).pipe(
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
                      ),
                  true,
                ),
          ),
        ),
      ).pipe(Effect.orDie)
      if (result.kind === "absent") return undefined
      if (result.kind === "delivered") return result.item
      const item = fromRow(result.row)
      if (!result.row.time_promoted) wake(input.sessionID)
      yield* publish(input.sessionID)
      return item
    })

    const admitLocked = (input: AdmitInput) =>
      admission(input).pipe(
        Effect.flatMap((item) =>
          item ? Effect.succeed(item) : Effect.die(new Error("admission did not allocate an original")),
        ),
      )

    const reconcileLocked = Effect.fnUntraced(function* (input: AdmitInput) {
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
                  delivery: original.input.delivery ?? "steer",
                }),
                withoutUndefined({
                  ...Struct.omit(input, ["sessionID", "messageID"]),
                  delivery: input.delivery ?? "steer",
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
      const own = yield* admission(input, true)
      return own ? { kind: "queued" as const, own } : undefined
    }, (effect) => effect.pipe(Effect.orDie))
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
            const prepared = yield* input.prepare()
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
        .where(and(pending(sessionID), eq(SessionPromptQueueTable.delivery, delivery)))
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

    // A shared reservation ID fixes membership across process restart. Preparation holds no DB/session lock.
    const reserve = (sessionID: SessionID, delivery: Delivery, isolate = new Set<ItemID>(), peersOnly = false) =>
      exclusive(
        sessionID,
        Effect.gen(function* () {
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
                yield* tx
                  .update(SessionPromptQueueTable)
                  .set({ time_promoted: Date.now() })
                  .where(inArray(SessionPromptQueueTable.id, ids))
                  .run()
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
              const channel = channels.get(row.id)
              if (channel) channel.message = result.id
            })
          if (result.kind !== "reserved") yield* publish(sessionID)
          return result
        }),
      )

    const release = (
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
          if ((delivery === "steer" || peersOnly) && (yield* compacting(sessionID))) {
            yield* release(sessionID, rows, id)
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
              yield* tx
                .update(SessionPromptQueueTable)
                .set({ time_promoted: Date.now() })
                .where(
                  inArray(
                    SessionPromptQueueTable.id,
                    rows.map((row) => row.id),
                  ),
                )
                .run()
              yield* DatabaseTransaction.afterCommit(
                Effect.sync(() => {
                  rows.forEach((row) => {
                    const channel = channels.get(row.id)
                    if (channel) channel.message = id
                  })
                }),
                true,
              )
              yield* DatabaseTransaction.afterCommit(publish(sessionID))
              return true
            }),
          ).pipe(
            Effect.orDie,
            Effect.onError(() => release(sessionID, rows, id)),
          )
          if (!written) yield* publish(sessionID)
          return written
        }),
      )

    const hold = (
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
                // A committed message must never be retried because one of its observers interrupted.
                if (
                  Cause.hasInterrupts(attempt.cause) ||
                  (yield* landed(db, next.id)) ||
                  (Exit.isSuccess(prepared) && sqliteLockMessage(Cause.squash(attempt.cause)))
                ) {
                  yield* exclusive(input.sessionID, release(input.sessionID, next.rows, next.id))
                  return yield* Effect.failCause(attempt.cause)
                }
                if (Exit.isSuccess(prepared) && !("group" in next.input)) return yield* Effect.failCause(attempt.cause)
                if ("group" in next.input) {
                  if (next.rows.length > 1) {
                    yield* exclusive(input.sessionID, release(input.sessionID, next.rows, next.id))
                    next.rows.forEach((row) => isolate.add(row.id))
                    continue
                  }
                  // Keep the failed original in an explicit, reported hold. Only the existing restore control retries it.
                  yield* hold(input.sessionID, next.rows, next.id)
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
                    tx.delete(SessionPromptQueueTable).where(eq(SessionPromptQueueTable.id, next.rows[0].id)).run(),
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

    const awaitingDrain = Effect.fn("SessionQueue.awaitingDrain")(function* (sessionID: SessionID) {
      if (parked.has(sessionID)) return false
      const row = yield* db
        .select({ id: SessionPromptQueueTable.id })
        .from(SessionPromptQueueTable)
        .where(and(eq(SessionPromptQueueTable.session_id, sessionID), isNull(SessionPromptQueueTable.time_withdrawn)))
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
            eq(SessionPromptQueueTable.id, itemID),
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
            eq(SessionPromptQueueTable.id, itemID),
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
          if (!channel.owners.size) channels.delete(itemID)
        })
      })

    const reply = (itemID: ItemID) =>
      Effect.sync(() => {
        const answered = channels.get(itemID)?.reply
        return answered
      })

    const message: Interface["message"] = (itemID) =>
      Effect.gen(function* () {
        const messageID = channels.get(itemID)?.message
        const sessionID = channels.get(itemID)?.sessionID
        if (!messageID || !sessionID) return undefined
        return yield* MessageV2.get({ sessionID, messageID }).pipe(
          Effect.provideService(Database.Service, database),
          Effect.orDie,
        )
      })

    return Service.of({
      admit,
      reconcile,
      assertFreshIdentity,
      prepareExecutable,
      writeDirect,
      message,
      assertIdentity,
      writeOrAdmit,
      list,
      withdraw,
      restore,
      update,
      promote,
      consume,
      park,
      awaitingDrain,
      withdrawn,
      unread,
      answer,
      reply,
      forget,
      empty,
      exclusive,
      whileCompacting,
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
    input: decodeInput(row.input),
    time: { created: row.time_created },
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
  return { item: group.value.constituents[0], messageID: MessageID.make(row.message_id), groupKind: group.value.kind }
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

function dispatch(input: SessionPromptQueue.QueuedInput, delivery?: Delivery) {
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
      .slice(0, 10)
      .map((item) => item.row)
  const type = first.kind.type
  return classified
    .filter((item) => item.kind?.kind === "automation" && item.kind.type === type && !item.row.message_id)
    .map((item) => item.row)
}

function grouped(
  rows: readonly (typeof SessionPromptQueueTable.$inferSelect)[],
  aliases: readonly (typeof SessionPromptQueueAliasTable.$inferSelect)[] = [],
) {
  const items = rows.map(fromRow)
  const kind = dispatch(items[0].input)
  const group: SessionPromptQueue.Group = {
    version: 1,
    sessionID: items[0].sessionID,
    kind: !kind ? "individual" : kind.kind === "peer" ? "peer" : kind.type,
    constituents: items,
  }
  const witnesses = items.flatMap((item) => {
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
  if (!kind) return { ...items[0].input, parts: [...items[0].input.parts, ...witnesses] }
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
