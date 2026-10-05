import { and, asc, desc, eq, gt, gte, ne, or } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { Database } from "../database/database"
import { EventV2 } from "../event"
import { MessageDecodeError } from "./error"
import { SessionMessage } from "./message"
import { SessionSchema } from "./schema"
import { SessionContextEpochTable, SessionMessageTable } from "./sql"

type DatabaseService = Database.Interface["db"]
type DatabaseTransaction = Parameters<Parameters<DatabaseService["transaction"]>[0]>[0]
type DatabaseAccess = DatabaseService | DatabaseTransaction

const decode = Schema.decodeUnknownEffect(SessionMessage.Message)

export const latestCompaction = Effect.fnUntraced(function* (db: DatabaseAccess, sessionID: SessionSchema.ID) {
  return yield* db
    .select({ seq: SessionMessageTable.seq })
    .from(SessionMessageTable)
    .where(and(eq(SessionMessageTable.session_id, sessionID), eq(SessionMessageTable.type, "compaction")))
    .orderBy(desc(SessionMessageTable.seq))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
})

const messageRows = Effect.fnUntraced(function* (
  db: DatabaseAccess,
  sessionID: SessionSchema.ID,
  compaction: { readonly seq: number } | undefined,
  baselineSeq?: number,
) {
  const rows = yield* db
    .select()
    .from(SessionMessageTable)
    .where(
      and(
        eq(SessionMessageTable.session_id, sessionID),
        compaction
          ? or(
              gte(SessionMessageTable.seq, compaction.seq),
              baselineSeq === undefined
                ? undefined
                : and(eq(SessionMessageTable.type, "system"), gt(SessionMessageTable.seq, baselineSeq)),
            )
          : undefined,
        baselineSeq === undefined
          ? undefined
          : or(ne(SessionMessageTable.type, "system"), gt(SessionMessageTable.seq, baselineSeq)),
      ),
    )
    .orderBy(asc(SessionMessageTable.seq))
    .all()
    .pipe(Effect.orDie)
  return rows
})

const decodeMessageRow = (row: typeof SessionMessageTable.$inferSelect) =>
  decode({ ...row.data, id: row.id, type: row.type }).pipe(
    Effect.mapError(
      () =>
        new MessageDecodeError({
          sessionID: SessionSchema.ID.make(row.session_id),
          messageID: SessionMessage.ID.make(row.id),
        }),
    ),
  )

export const load = Effect.fn("SessionHistory.load")(function* (db: DatabaseService, sessionID: SessionSchema.ID) {
  const rows = yield* db
    .transaction((tx) =>
      Effect.gen(function* () {
        yield* EventV2.assertReplayableIn(tx, sessionID)
        const [epoch, compaction] = yield* Effect.all(
          [
            tx
              .select({ baselineSeq: SessionContextEpochTable.baseline_seq })
              .from(SessionContextEpochTable)
              .where(eq(SessionContextEpochTable.session_id, sessionID))
              .get()
              .pipe(Effect.orDie),
            latestCompaction(tx, sessionID),
          ],
          { concurrency: "unbounded" },
        )
        return yield* messageRows(tx, sessionID, compaction, epoch?.baselineSeq)
      }),
    )
    .pipe(Effect.orDie)
  return yield* Effect.forEach(rows, decodeMessageRow)
})

export const loadForRunner = Effect.fn("SessionHistory.loadForRunner")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  baselineSeq: number,
) {
  return (yield* entriesForRunner(db, sessionID, baselineSeq)).map((entry) => entry.message)
})

export const entriesForRunner = Effect.fn("SessionHistory.entriesForRunner")(function* (
  db: DatabaseAccess,
  sessionID: SessionSchema.ID,
  baselineSeq: number,
) {
  const rows = yield* db
    .transaction((tx) =>
      Effect.gen(function* () {
        yield* EventV2.assertReplayableIn(tx, sessionID)
        return yield* messageRows(tx, sessionID, yield* latestCompaction(tx, sessionID), baselineSeq)
      }),
    )
    .pipe(Effect.orDie)
  return yield* Effect.forEach(rows, (row) =>
    decodeMessageRow(row).pipe(Effect.map((message) => ({ seq: row.seq, message }))),
  )
})

export * as SessionHistory from "./history"
