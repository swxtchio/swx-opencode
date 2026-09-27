import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { SessionPromptQueueSequenceTable, SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import { MessageTable } from "@opencode-ai/core/session/sql"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { and, asc, desc, eq, inArray, isNotNull, isNull, sql } from "drizzle-orm"
import { Cause, Context, Effect, Exit, Layer, Option, Semaphore, Struct } from "effect"
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
   * Turns pending items into V1 user messages through `create`. Steers wait
   * while a compaction task is pending, so they never become that compaction's
   * parent or input, and queued items go one per call so the loop reevaluates
   * between their turns.
   */
  readonly promote: <E>(input: {
    readonly sessionID: SessionID
    readonly delivery: Delivery
    readonly create: (input: PromotedInput) => Effect.Effect<unknown, E>
  }) => Effect.Effect<boolean, E>
  /**
   * Called by a drain before it reads history. A promoted row outlives its
   * promotion until then so that a joiner of a finishing run can see the prompt
   * still needs a drain, and so that a promotion interrupted by a crash is
   * delivered once rather than lost or repeated.
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

    const admit = Effect.fn("SessionQueue.admit")(function* (input: AdmitInput) {
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
                input: Struct.omit(input, ["sessionID", "noReply", "delivery"]),
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
    })

    const restore = Effect.fn("SessionQueue.restore")(function* (sessionID: SessionID, itemID: ItemID) {
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
        Effect.map((msgs) => MessageV2.latest(msgs).tasks.some((task) => task.type === "compaction")),
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

    const promoteOne = <E>(
      sessionID: SessionID,
      row: typeof SessionPromptQueueTable.$inferSelect,
      create: (input: PromotedInput) => Effect.Effect<unknown, E>,
    ) =>
      Effect.gen(function* () {
        const id = yield* messageID(sessionID, row.input.messageID)
        const claimed = yield* db
          .update(SessionPromptQueueTable)
          .set({ time_promoted: Date.now(), message_id: id })
          .where(and(eq(SessionPromptQueueTable.id, row.id), pending(sessionID)))
          .returning()
          .get()
          .pipe(Effect.orDie)
        // Withdrawn or promoted by someone else since it was read.
        if (!claimed) return false
        yield* create({ ...claimed.input, sessionID, messageID: id }).pipe(
          // A prompt that cannot become a message is consumed, as a failed prompt was before the queue.
          Effect.onExit((exit) =>
            Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
              ? db
                  .delete(SessionPromptQueueTable)
                  .where(eq(SessionPromptQueueTable.id, row.id))
                  .run()
                  .pipe(Effect.orDie)
              : Effect.void,
          ),
          Effect.ensuring(publish(sessionID)),
        )
        return true
      })

    const promote: Interface["promote"] = <E>(input: {
      readonly sessionID: SessionID
      readonly delivery: Delivery
      readonly create: (input: PromotedInput) => Effect.Effect<unknown, E>
    }) =>
      exclusive(
        input.sessionID,
        Effect.gen(function* () {
          if (input.delivery === "steer" && !(yield* oldest(input.sessionID, "steer"))) return false
          if (input.delivery === "steer" && (yield* compacting(input.sessionID))) return false
          let promoted = false
          while (true) {
            const row = yield* oldest(input.sessionID, input.delivery)
            if (!row) return promoted
            if (!(yield* promoteOne(input.sessionID, row, input.create))) continue
            promoted = true
            // Queued items run one per turn; the loop reevaluates before the next.
            if (input.delivery === "queue") return true
          }
        }),
      ).pipe(Effect.withSpan("SessionQueue.promote"))

    const consume = Effect.fn("SessionQueue.consume")(function* (sessionID: SessionID) {
      yield* exclusive(
        sessionID,
        Effect.gen(function* () {
          const rows = yield* db
            .select()
            .from(SessionPromptQueueTable)
            .where(
              and(eq(SessionPromptQueueTable.session_id, sessionID), isNotNull(SessionPromptQueueTable.time_promoted)),
            )
            .all()
            .pipe(Effect.orDie)
          if (rows.length === 0) return
          const landed = new Set(
            (yield* db
              .select({ id: MessageTable.id })
              .from(MessageTable)
              .where(
                inArray(
                  MessageTable.id,
                  rows.flatMap((row) => (row.message_id ? [row.message_id] : [])),
                ),
              )
              .all()
              .pipe(Effect.orDie)).map((row) => row.id),
          )
          const consumed = rows.filter((row) => row.message_id && landed.has(row.message_id)).map((row) => row.id)
          const lost = rows.filter((row) => !row.message_id || !landed.has(row.message_id)).map((row) => row.id)
          if (consumed.length > 0)
            yield* db
              .delete(SessionPromptQueueTable)
              .where(inArray(SessionPromptQueueTable.id, consumed))
              .run()
              .pipe(Effect.orDie)
          if (lost.length === 0) return
          yield* db
            .update(SessionPromptQueueTable)
            .set({ time_promoted: null, message_id: null })
            .where(inArray(SessionPromptQueueTable.id, lost))
            .run()
            .pipe(Effect.orDie)
          yield* publish(sessionID)
        }),
      )
    })

    const park = Effect.fn("SessionQueue.park")(function* (sessionID: SessionID) {
      parked.add(sessionID)
    })

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

function fromRow(row: typeof SessionPromptQueueTable.$inferSelect): Item {
  return {
    id: row.id,
    sessionID: row.session_id,
    seq: row.seq,
    delivery: row.delivery,
    input: row.input,
    time: { created: row.time_created },
  }
}

export const node = LayerNode.make({ service: Service, layer, deps: [Database.node, EventV2Bridge.node] })

export * as SessionQueue from "./queue"
