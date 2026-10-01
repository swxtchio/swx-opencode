import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { SessionPromptQueueSequenceTable, SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import { MessageTable } from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { and, asc, desc, eq, inArray, isNotNull, isNull, sql } from "drizzle-orm"
import { Cause, Context, Effect, Exit, Layer, Option, Schema, Semaphore, Struct } from "effect"
import { isDeepStrictEqual } from "node:util"
import { EventV2Bridge } from "@/event-v2-bridge"
import { MessageV2 } from "./message-v2"
import { MessageID, SessionID } from "./schema"

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

export interface Interface {
  readonly admit: (input: AdmitInput) => Effect.Effect<Item>
  /**
   * Prepare under the session lock, recheck run state around a direct write,
   * and queue it if a run starts while the message is being persisted.
   */
  readonly writeOrAdmit: <P extends SessionV1.WithParts, E>(input: {
    readonly admission: AdmitInput
    readonly isBusy: Effect.Effect<boolean>
    readonly prepare: Effect.Effect<P, E>
    readonly write: (prepared: P) => Effect.Effect<SessionV1.WithParts>
    readonly discard: (message: SessionV1.WithParts) => Effect.Effect<void>
  }) => Effect.Effect<
    | { readonly kind: "direct"; readonly message: SessionV1.WithParts }
    | { readonly kind: "queued"; readonly own: Item; readonly prepared: P },
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
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionQueue") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const database = yield* Database.Service
    const { db } = database
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

    const semaphore = (map: Map<SessionID, Semaphore.Semaphore>, sessionID: SessionID) => {
      const hit = map.get(sessionID)
      if (hit) return hit
      const next = Semaphore.makeUnsafe(1)
      map.set(sessionID, next)
      return next
    }

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
        .select()
        .from(SessionPromptQueueTable)
        .where(pending(sessionID))
        .orderBy(asc(SessionPromptQueueTable.seq))
        .all()
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

    const admitLocked = Effect.fnUntraced(function* (input: AdmitInput) {
      const row = yield* db
        .transaction((tx) =>
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
            return yield* tx
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
          }),
        )
        .pipe(Effect.orDie)
      if (!row) return yield* Effect.die(new Error(`Queue admission for ${input.sessionID} stored nothing`))
      channels.set(row.id, { sessionID: input.sessionID })
      wake(input.sessionID)
      yield* publish(input.sessionID)
      return fromRow(row)
    })

    const writeOrAdmit: Interface["writeOrAdmit"] = (input) =>
      exclusive(
        input.admission.sessionID,
        Effect.gen(function* () {
          const prepared = yield* input.prepare
          const busy = yield* input.isBusy
          const existingRow = input.admission.messageID
            ? yield* db
                .select({ session_id: MessageTable.session_id })
                .from(MessageTable)
                .where(eq(MessageTable.id, input.admission.messageID))
                .get()
                .pipe(Effect.orDie)
            : undefined
          const existing =
            existingRow?.session_id === input.admission.sessionID && input.admission.messageID
              ? yield* MessageV2.get({
                  sessionID: input.admission.sessionID,
                  messageID: input.admission.messageID,
                }).pipe(
                  Effect.provideService(Database.Service, database),
                  Effect.catchTag("NotFoundError", () => Effect.succeed(undefined)),
                )
              : undefined
          if (existingRow) {
            if (existing && sameMessage(existing, prepared)) return { kind: "direct" as const, message: existing }
            const messageID = MessageID.ascending()
            const admission = { ...input.admission, messageID }
            const queued = rekeyMessage(prepared, messageID)
            return {
              kind: "queued" as const,
              own: yield* admitLocked(admission),
              prepared: queued,
            }
          }
          if (busy)
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
          const row = yield* db
            .update(SessionPromptQueueTable)
            .set({ time_withdrawn: Date.now(), message_id: null })
            .where(and(eq(SessionPromptQueueTable.id, itemID), pending(sessionID)))
            .returning()
            .get()
            .pipe(Effect.orDie)
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
      const row = yield* db
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
        .pipe(Effect.orDie)
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
      const row = yield* db
        .update(SessionPromptQueueTable)
        .set({ delivery: input.delivery })
        .where(and(eq(SessionPromptQueueTable.id, input.itemID), pending(input.sessionID)))
        .returning()
        .get()
        .pipe(Effect.orDie)
      if (!row) return Option.none()
      wake(input.sessionID)
      yield* publish(input.sessionID)
      return Option.some(fromRow(row))
    })

    const compacting = (sessionID: SessionID) =>
      running.has(sessionID)
        ? Effect.succeed(true)
        : Effect.gen(function* () {
            const history = yield* MessageV2.snapshot(sessionID).pipe(
              Effect.provideService(Database.Service, database),
            )
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

    const oldest = (sessionID: SessionID, delivery: Delivery) =>
      db
        .select()
        .from(SessionPromptQueueTable)
        .where(and(pending(sessionID), eq(SessionPromptQueueTable.delivery, delivery)))
        .orderBy(asc(SessionPromptQueueTable.seq))
        .limit(1)
        .get()
        .pipe(Effect.orDie)

    // A promoted message sorts after the session's history: V1 clients order
    // messages by id, so a supplied id is kept only while it is still the newest.
    const messageID = Effect.fnUntraced(function* (sessionID: SessionID, supplied: MessageID | undefined) {
      if (!supplied) return MessageID.ascending()
      const newest = yield* db
        .select({ id: MessageTable.id })
        .from(MessageTable)
        .where(eq(MessageTable.session_id, sessionID))
        .orderBy(desc(MessageTable.id))
        .limit(1)
        .get()
        .pipe(Effect.orDie)
      return !newest || supplied > newest.id ? supplied : MessageID.ascending()
    })

    const landed = (id: MessageID) =>
      db
        .select({ id: MessageTable.id })
        .from(MessageTable)
        .where(eq(MessageTable.id, id))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => row !== undefined),
        )

    const markPromoted = (itemID: ItemID, id: MessageID) =>
      db
        .update(SessionPromptQueueTable)
        .set({ time_promoted: Date.now() })
        .where(eq(SessionPromptQueueTable.id, itemID))
        .run()
        .pipe(
          Effect.orDie,
          Effect.tap(() =>
            Effect.sync(() => {
              const channel = channels.get(itemID)
              if (channel) channel.message = id
            }),
          ),
        )

    const drop = (itemID: ItemID) =>
      db.delete(SessionPromptQueueTable).where(eq(SessionPromptQueueTable.id, itemID)).run().pipe(Effect.orDie)

    const unreserve = (itemID: ItemID) =>
      db
        .update(SessionPromptQueueTable)
        .set({ message_id: null })
        .where(eq(SessionPromptQueueTable.id, itemID))
        .run()
        .pipe(Effect.orDie)

    // Picks the oldest eligible row and reserves its message id, under the lock.
    // A reservation whose message already landed (a process stopped between the
    // write and the mark) is finished instead of prepared again.
    const reserve = (sessionID: SessionID, delivery: Delivery) =>
      exclusive(
        sessionID,
        Effect.gen(function* () {
          if (delivery === "steer" && !(yield* oldest(sessionID, "steer"))) return undefined
          if (delivery === "steer" && (yield* compacting(sessionID))) return undefined
          const row = yield* oldest(sessionID, delivery)
          if (!row) return undefined
          if (row.message_id && (yield* landed(row.message_id))) {
            yield* markPromoted(row.id, row.message_id)
            yield* publish(sessionID)
            return { kind: "finished" as const, row }
          }
          const decoded = Schema.decodeUnknownExit(SessionPromptQueue.QueuedInput)(row.input)
          if (Exit.isFailure(decoded)) {
            yield* drop(row.id)
            yield* publish(sessionID)
            return { kind: "invalid" as const, row, cause: decoded.cause }
          }
          const id = yield* messageID(sessionID, decoded.value.messageID)
          yield* db
            .update(SessionPromptQueueTable)
            .set({ message_id: id })
            .where(eq(SessionPromptQueueTable.id, row.id))
            .run()
            .pipe(Effect.orDie)
          return { kind: "reserved" as const, row, input: decoded.value, id }
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
          if (delivery === "steer" && (yield* compacting(sessionID))) {
            yield* unreserve(itemID)
            return false
          }
          yield* write(prepared).pipe(
            Effect.onExit((exit) =>
              Exit.isSuccess(exit)
                ? markPromoted(itemID, id)
                : landed(id).pipe(Effect.flatMap((exists) => (exists ? markPromoted(itemID, id) : unreserve(itemID)))),
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
                  (Cause.hasInterruptsOnly(prepared.cause) ? unreserve(next.row.id) : drop(next.row.id)).pipe(
                    Effect.andThen(publish(input.sessionID)),
                  ),
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

    const consume = Effect.fn("SessionQueue.consume")(function* (sessionID: SessionID) {
      return yield* exclusive(
        sessionID,
        db
          .delete(SessionPromptQueueTable)
          .where(
            and(eq(SessionPromptQueueTable.session_id, sessionID), isNotNull(SessionPromptQueueTable.time_promoted)),
          )
          .run()
          .pipe(
            Effect.orDie,
            Effect.map(() => wakes.get(sessionID) ?? 0),
          ),
      )
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

    const forget = (itemID: ItemID) => Effect.sync(() => void channels.delete(itemID))

    const reply = (itemID: ItemID) =>
      Effect.sync(() => {
        const answered = channels.get(itemID)?.reply
        channels.delete(itemID)
        return answered
      })

    return Service.of({
      admit,
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
  return MessageV2.latest(msgs, options).tasks.filter((task) => task.type !== "compaction" || !stopped.has(task.messageID))
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
