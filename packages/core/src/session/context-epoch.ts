export * as SessionContextEpoch from "./context-epoch"

import { eq } from "drizzle-orm"
import { DateTime, Effect, Schema } from "effect"
import type { Database } from "../database/database"
import { EventV2 } from "../event"
import { EventSequenceTable } from "../event/sql"
import { SystemContext } from "../system-context/index"
import { ContextSnapshotDecodeError } from "./error"
import { SessionEvent } from "./event"
import { SessionHistory } from "./history"
import { SessionInput } from "./input"
import { SessionMessage } from "./message"
import { SessionSchema } from "./schema"
import { SessionContextEpochTable } from "./sql"

type DatabaseService = Database.Interface["db"]
type DatabaseTransaction = Parameters<Parameters<DatabaseService["transaction"]>[0]>[0]
type DatabaseAccess = DatabaseService | DatabaseTransaction

interface Prepared {
  readonly baseline: string
  readonly baselineSeq: number
}

export function initialize(
  db: DatabaseService,
  context: Effect.Effect<SystemContext.SystemContext>,
  sessionID: SessionSchema.ID,
): Effect.Effect<Prepared | undefined, SystemContext.InitializationBlocked> {
  return initializeOnce(db, context, sessionID).pipe(Effect.withSpan("SessionContextEpoch.initialize"))
}

export function prepare(
  db: DatabaseService,
  events: EventV2.Interface,
  context: Effect.Effect<SystemContext.SystemContext>,
  sessionID: SessionSchema.ID,
): Effect.Effect<Prepared, SystemContext.InitializationBlocked | ContextSnapshotDecodeError> {
  return prepareOnce(db, events, context, sessionID).pipe(Effect.withSpan("SessionContextEpoch.prepare"))
}

const prepareOnce = Effect.fnUntraced(function* (
  db: DatabaseService,
  events: EventV2.Interface,
  context: Effect.Effect<SystemContext.SystemContext>,
  sessionID: SessionSchema.ID,
) {
  const [value, [stored, compaction]] = yield* Effect.all(
    [
      context,
      db
        .transaction((tx) =>
          Effect.gen(function* () {
            yield* EventV2.assertWritableIn(tx, sessionID)
            return yield* Effect.all([find(tx, sessionID), SessionHistory.latestCompaction(tx, sessionID)], {
              concurrency: "unbounded",
            })
          }),
        )
        .pipe(Effect.orDie),
    ],
    { concurrency: "unbounded" },
  )
  if (!stored) {
    const generation = yield* SystemContext.initialize(value)
    const baselineSeq = yield* insert(db, sessionID, generation)
    return { baseline: generation.baseline, baselineSeq }
  }

  const snapshot = yield* Schema.decodeUnknownEffect(SystemContext.Snapshot)(stored.snapshot).pipe(
    Effect.mapError((error) => new ContextSnapshotDecodeError({ sessionID, details: String(error) })),
  )
  const replacementSeq = compaction !== undefined && compaction.seq > stored.baseline_seq ? compaction.seq : undefined
  const result = replacementSeq
    ? yield* SystemContext.replace(value, snapshot)
    : yield* SystemContext.reconcile(value, snapshot)
  if (result._tag === "Unchanged" || result._tag === "ReplacementBlocked") {
    return { baseline: stored.baseline, baselineSeq: stored.baseline_seq }
  }
  if (result._tag === "ReplacementReady") {
    const baselineSeq = replacementSeq ?? (yield* EventV2.latestSequence(db, sessionID))
    yield* replace(db, sessionID, baselineSeq, result.generation)
    return { baseline: result.generation.baseline, baselineSeq }
  }

  yield* events.publish(
    SessionEvent.ContextUpdated,
    { sessionID, messageID: SessionMessage.ID.create(), timestamp: yield* DateTime.now, text: result.text },
    { commit: () => advance(db, sessionID, result.snapshot).pipe(Effect.orDie) },
  )
  return { baseline: stored.baseline, baselineSeq: stored.baseline_seq }
})

const initializeOnce = Effect.fnUntraced(function* (
  db: DatabaseService,
  context: Effect.Effect<SystemContext.SystemContext>,
  sessionID: SessionSchema.ID,
) {
  if (yield* exists(db, sessionID)) return
  const generation = yield* context.pipe(Effect.flatMap(SystemContext.initialize))
  const baselineSeq = yield* insert(db, sessionID, generation)
  return { baseline: generation.baseline, baselineSeq }
})

const exists = Effect.fn("SessionContextEpoch.exists")(function* (db: DatabaseAccess, sessionID: SessionSchema.ID) {
  return (
    (yield* db
      .select({ sessionID: SessionContextEpochTable.session_id })
      .from(SessionContextEpochTable)
      .where(eq(SessionContextEpochTable.session_id, sessionID))
      .get()
      .pipe(Effect.orDie)) !== undefined
  )
})

const find = Effect.fn("SessionContextEpoch.find")(function* (db: DatabaseAccess, sessionID: SessionSchema.ID) {
  return yield* db
    .select()
    .from(SessionContextEpochTable)
    .where(eq(SessionContextEpochTable.session_id, sessionID))
    .get()
    .pipe(Effect.orDie)
})

export const reset = Effect.fn("SessionContextEpoch.reset")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
) {
  yield* db
    .transaction((tx) =>
      Effect.gen(function* () {
        yield* EventV2.assertWritableIn(tx, sessionID)
        yield* tx
          .delete(SessionContextEpochTable)
          .where(eq(SessionContextEpochTable.session_id, sessionID))
          .run()
          .pipe(Effect.orDie)
      }),
    )
    .pipe(Effect.orDie)
})

const insert = Effect.fnUntraced(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  generation: SystemContext.Generation,
) {
  return yield* db
    .transaction((tx) =>
      Effect.gen(function* () {
        yield* EventV2.assertWritableIn(tx, sessionID)
        const sequence = yield* tx
          .select({ seq: EventSequenceTable.seq })
          .from(EventSequenceTable)
          .where(eq(EventSequenceTable.aggregate_id, sessionID))
          .get()
          .pipe(Effect.orDie)
        const baselineSeq = sequence?.seq ?? -1
        yield* tx
          .insert(SessionContextEpochTable)
          .values({
            session_id: sessionID,
            baseline: generation.baseline,
            snapshot: generation.snapshot,
            baseline_seq: baselineSeq,
          })
          .run()
          .pipe(Effect.orDie)
        return baselineSeq
      }),
    )
    .pipe(Effect.orDie)
})

const replace = Effect.fnUntraced(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  baselineSeq: number,
  generation: SystemContext.Generation,
) {
  yield* db
    .transaction((tx) =>
      Effect.gen(function* () {
        yield* EventV2.assertWritableIn(tx, sessionID)
        const updated = yield* tx
          .update(SessionContextEpochTable)
          .set({
            baseline: generation.baseline,
            snapshot: generation.snapshot,
            baseline_seq: baselineSeq,
          })
          .where(eq(SessionContextEpochTable.session_id, sessionID))
          .returning({ sessionID: SessionContextEpochTable.session_id })
          .get()
          .pipe(Effect.orDie)
        if (!updated) return yield* Effect.die("Context Epoch not found")
      }),
    )
    .pipe(Effect.orDie)
})

const advance = Effect.fnUntraced(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  snapshot: SystemContext.Snapshot,
) {
  const updated = yield* db
    .update(SessionContextEpochTable)
    .set({ snapshot })
    .where(eq(SessionContextEpochTable.session_id, sessionID))
    .returning({ sessionID: SessionContextEpochTable.session_id })
    .get()
    .pipe(Effect.orDie)
  if (!updated) return yield* Effect.die("Context Epoch not found")
})
