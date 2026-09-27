import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { SessionPromptQueueSequenceTable, SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import { MessageTable } from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { and, asc, desc, eq, inArray, isNotNull, isNull, sql } from "drizzle-orm"
import { Cause, Context, Effect, Exit, Layer, Option, Schema, Semaphore, Struct } from "effect"
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

export interface Interface {
  readonly admit: (input: AdmitInput) => Effect.Effect<Item>
  readonly list: (sessionID: SessionID) => Effect.Effect<Item[]>
  /**
   * Withdraws an item for editing so it cannot be delivered mid-edit. None
   * tells the editor that delivery won the race.
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
   * Turns pending items into V1 user messages through `create`, and reports
   * whether the queue changed. Steers wait while a compaction task is pending,
   * so they never become that compaction's parent or input, and queued items go
   * one per call so the loop reevaluates between their turns. An item that
   * cannot become a message is dropped: the caller's `own` item fails the call,
   * any other goes to `rejected`, so one prompt's failure never lands on another.
   */
  readonly promote: <E>(input: {
    readonly sessionID: SessionID
    readonly delivery: Delivery
    readonly create: (input: PromotedInput) => Effect.Effect<unknown, E>
    readonly rejected: (cause: Cause.Cause<E>) => Effect.Effect<void>
    readonly own?: ItemID
  }) => Effect.Effect<boolean, E>
  /**
   * Called by a drain before it reads history. A promoted row outlives its
   * promotion until then so that a joiner of a finishing run can see the prompt
   * still needs a drain.
   */
  readonly consume: (sessionID: SessionID) => Effect.Effect<void>
  /** A run that stopped on an abort or error must not restart until something wakes the session. */
  readonly park: (sessionID: SessionID) => Effect.Effect<void>
  /** The joiner re-check's signal: admitted work no drain has read yet, on a session that is not parked. */
  readonly awaitingDrain: (sessionID: SessionID) => Effect.Effect<boolean>
  /** For writers of user messages, such as compaction, that a promotion must not interleave with. */
  readonly exclusive: <A, E, R>(sessionID: SessionID, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionQueue") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const database = yield* Database.Service
    const { db } = database
    const events = yield* EventV2Bridge.Service
    const locks = new Map<SessionID, Semaphore.Semaphore>()
    const parked = new Set<SessionID>()

    const lock = (sessionID: SessionID) => {
      const hit = locks.get(sessionID)
      if (hit) return hit
      const next = Semaphore.makeUnsafe(1)
      locks.set(sessionID, next)
      return next
    }

    const exclusive: Interface["exclusive"] = (sessionID, effect) => lock(sessionID).withPermit(effect)

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
      parked.delete(input.sessionID)
      yield* publish(input.sessionID)
      return fromRow(row)
    })

    const withdraw = Effect.fn("SessionQueue.withdraw")(function* (sessionID: SessionID, itemID: ItemID) {
      return yield* exclusive(
        sessionID,
        Effect.gen(function* () {
          const row = yield* db
            .update(SessionPromptQueueTable)
            .set({ time_withdrawn: Date.now() })
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
      parked.delete(sessionID)
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
      parked.delete(input.sessionID)
      yield* publish(input.sessionID)
      return Option.some(fromRow(row))
    })

    const compacting = (sessionID: SessionID) =>
      MessageV2.filterCompactedEffect(sessionID).pipe(
        Effect.provideService(Database.Service, database),
        Effect.map((msgs) => openTasks(msgs).some((task) => task.type === "compaction")),
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

    const markPromoted = (itemID: ItemID) =>
      db
        .update(SessionPromptQueueTable)
        .set({ time_promoted: Date.now() })
        .where(eq(SessionPromptQueueTable.id, itemID))
        .run()
        .pipe(Effect.orDie)

    const drop = (itemID: ItemID) =>
      db.delete(SessionPromptQueueTable).where(eq(SessionPromptQueueTable.id, itemID)).run().pipe(Effect.orDie)

    // The item stays listed until its message exists: the message id is only
    // reserved first, and the row is marked promoted once `create` has written it.
    const promoteOne = <E>(
      sessionID: SessionID,
      row: typeof SessionPromptQueueTable.$inferSelect,
      create: (input: PromotedInput) => Effect.Effect<unknown, E>,
    ) =>
      Effect.gen(function* () {
        if (row.message_id && (yield* landed(row.message_id))) {
          yield* markPromoted(row.id)
          yield* publish(sessionID)
          return true
        }
        const input = yield* Schema.decodeUnknownEffect(SessionPromptQueue.QueuedInput)(row.input).pipe(
          Effect.tapError(() => drop(row.id).pipe(Effect.andThen(publish(sessionID)))),
          Effect.orDie,
        )
        const id = yield* messageID(sessionID, input.messageID)
        const reserved = yield* db
          .update(SessionPromptQueueTable)
          .set({ message_id: id })
          .where(and(eq(SessionPromptQueueTable.id, row.id), pending(sessionID)))
          .returning({ id: SessionPromptQueueTable.id })
          .get()
          .pipe(Effect.orDie)
        if (!reserved) return false
        yield* create({ ...input, sessionID, messageID: id }).pipe(
          Effect.onExit((exit) => {
            if (Exit.isSuccess(exit)) return markPromoted(row.id)
            // A prompt that cannot become a message is consumed, as a failed prompt was before the queue.
            if (!Cause.hasInterruptsOnly(exit.cause)) return drop(row.id)
            return landed(id).pipe(
              Effect.flatMap((exists) =>
                exists
                  ? markPromoted(row.id)
                  : db
                      .update(SessionPromptQueueTable)
                      .set({ message_id: null })
                      .where(eq(SessionPromptQueueTable.id, row.id))
                      .run()
                      .pipe(Effect.orDie),
              ),
            )
          }),
          Effect.ensuring(publish(sessionID)),
        )
        return true
      })

    const promote: Interface["promote"] = <E>(input: {
      readonly sessionID: SessionID
      readonly delivery: Delivery
      readonly create: (input: PromotedInput) => Effect.Effect<unknown, E>
      readonly rejected: (cause: Cause.Cause<E>) => Effect.Effect<void>
      readonly own?: ItemID
    }) =>
      exclusive(
        input.sessionID,
        Effect.gen(function* () {
          if (input.delivery === "steer" && !(yield* oldest(input.sessionID, "steer"))) return false
          if (input.delivery === "steer" && (yield* compacting(input.sessionID))) return false
          let changed = false
          while (true) {
            const row = yield* oldest(input.sessionID, input.delivery)
            if (!row) return changed
            const exit = yield* promoteOne(input.sessionID, row, input.create).pipe(Effect.exit)
            if (Exit.isSuccess(exit) && !exit.value) continue
            changed = true
            // Queued items run one per turn; the loop reevaluates before the next.
            if (Exit.isSuccess(exit) && input.delivery === "queue") return true
            if (Exit.isSuccess(exit)) continue
            if (Cause.hasInterruptsOnly(exit.cause) || row.id === input.own) return yield* Effect.failCause(exit.cause)
            yield* input.rejected(exit.cause)
          }
        }),
      ).pipe(Effect.withSpan("SessionQueue.promote"))

    const consume = Effect.fn("SessionQueue.consume")(function* (sessionID: SessionID) {
      yield* exclusive(
        sessionID,
        db
          .delete(SessionPromptQueueTable)
          .where(
            and(eq(SessionPromptQueueTable.session_id, sessionID), isNotNull(SessionPromptQueueTable.time_promoted)),
          )
          .run()
          .pipe(Effect.orDie),
      )
    })

    const park = (sessionID: SessionID) => Effect.sync(() => void parked.add(sessionID))

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

    return Service.of({
      admit,
      list,
      withdraw,
      restore,
      update,
      promote,
      consume,
      park,
      awaitingDrain,
      exclusive,
    })
  }),
)

/**
 * The compaction and subtask parts the loop still has to run. A compaction whose
 * summary turn stopped on an abort or error is over, the way any stopped turn is,
 * so neither a steer waits on it nor does the loop rerun it.
 */
export function openTasks(msgs: SessionV1.WithParts[]) {
  const stopped = new Set(
    msgs.flatMap((msg) => (msg.info.role === "assistant" && msg.info.error !== undefined ? [msg.info.parentID] : [])),
  )
  return MessageV2.latest(msgs).tasks.filter((task) => task.type !== "compaction" || !stopped.has(task.messageID))
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

export const node = LayerNode.make({ service: Service, layer, deps: [Database.node, EventV2Bridge.node] })

export * as SessionQueue from "./queue"
