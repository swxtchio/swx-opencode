import { Effect } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { EventRetentionTable } from "@opencode-ai/core/event/sql"
import { DbRetention, type RetentionEvidence, type SqliteAccess } from "@/cli/cmd/db-retention"

const access = (db: unknown) => db as SqliteAccess

export function applyRetentionFixture(sessionID: string) {
  return applyRetentionFixtures([sessionID]).pipe(
    Effect.map((applied) => ({ database: applied.database, ...applied.trees[0]! })),
  )
}

/**
 * Prunes each listed Session, each its own tree, through a real retention apply on an exported copy of the store,
 * then copies the markers apply wrote back into the store, which is what makes those Sessions unreplayable there.
 */
export function applyRetentionFixtures(sessionIDs: readonly string[]) {
  return Effect.gen(function* () {
    const { db } = yield* Database.Service
    // Past the inactivity window for Sessions the test just created.
    const now = Date.now() + DbRetention.inactivityPolicy.windowMs + 60_000
    const finalSequence: Record<string, number> = {}
    for (const sessionID of sessionIDs) finalSequence[sessionID] = yield* EventV2.latestSequence(db, sessionID)
    const evidence: RetentionEvidence = {
      customerBinding: {
        proofID: "fixture-unbound-session-classification",
        durable: true,
        sessionIDs: [...sessionIDs],
        customerBoundSessionIDs: [],
        nonCustomerSessionIDs: [...sessionIDs],
      },
      readerContract: { reviewed: true, id: "fixture-reader-contract" },
      handoff: {
        receiptID: "fixture-handoff",
        durable: true,
        sessionIDs: [...sessionIDs],
        finalSequence,
        axes: {
          billing: { status: "retained" },
          provider: { status: "retained" },
          servingModel: { status: "retained" },
          routeAttribution: { status: "retained" },
          reportedCost: { status: "retained" },
          inputTokens: { status: "retained" },
          outputTokens: { status: "retained" },
          reasoningTokens: { status: "retained" },
          cacheReadTokens: { status: "retained" },
          cacheWriteTokens: { status: "retained" },
          correctness: { status: "unavailable", cause: "fixture has no correctness producer" },
          performance: { status: "unavailable", cause: "fixture has no performance producer" },
        },
        report: {
          windowStart: "2026-10-01T00:00:00Z",
          windowEnd: "2026-10-02T00:00:00Z",
          resultDigest: "fixture-model-report",
          denominators: { sessions: sessionIDs.length },
          unavailableCauses: {
            correctness: "fixture has no correctness producer",
            performance: "fixture has no performance producer",
          },
          rawHistoryInaccessible: true,
        },
      },
    }
    const image = yield* (db.$client as unknown as { export: Effect.Effect<Uint8Array> }).export
    // An in-memory copy: no other process can hold it, so its holder scans need not walk procfs, which keeps
    // pruning hundreds of Sessions fast. The apply code is the same as on a file.
    const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
    const database = sqlite.Database.deserialize(image)
    // The copy stands for the store after the test process, which wrote these Sessions through the real
    // producer, has exited: its registered identity no longer names a live process.
    database.query("UPDATE event_writer_process SET start_ticks = 'exited-writer'").run()
    const proof: RetentionEvidence = { ...evidence, holders: DbRetention.scanDatabaseHolders(database.filename) }
    yield* Effect.addFinalizer(() => Effect.sync(() => database.close()))
    const inventoried = DbRetention.inventory(access(database), proof, now).trees
    const trees = new Array<{
      tree: DbRetention.RetentionTree
      result: DbRetention.RetentionApplyResult
      eventIdentitiesBefore: { id: string; seq: number }[]
    }>()
    for (const sessionID of sessionIDs) {
      const tree = inventoried.find((item) => item.rootSessionID === sessionID)
      if (!tree?.eligible)
        return yield* Effect.die(new Error(`Retention fixture is not eligible: ${tree?.reasons.join(",")}`))
      const eventIdentitiesBefore = database
        .query<{ id: string; seq: number }, [string]>("SELECT id, seq FROM event WHERE aggregate_id = ? ORDER BY seq")
        .all(sessionID)
      const result = DbRetention.apply(access(database), { tree, evidence: () => proof, now: () => now })
      if (result.state !== "complete")
        return yield* Effect.die(new Error(`Retention fixture did not complete: ${result.reasons.join(",")}`))
      const marker = database
        .query<
          {
            aggregate_id: string
            state: "scanning" | "redacting" | "complete"
            progress_table: string | null
            progress_id: string | null
            evidence: string
            time_started: number
            time_updated: number
          },
          [string]
        >("SELECT * FROM event_retention WHERE aggregate_id = ?")
        .get(sessionID)
      if (!marker) return yield* Effect.die(new Error(`Retention fixture did not persist a marker for ${sessionID}`))
      yield* db
        .insert(EventRetentionTable)
        .values({ ...marker, evidence: JSON.parse(marker.evidence) as Record<string, unknown> })
        .run()
      trees.push({ tree, result, eventIdentitiesBefore })
    }
    return { database, trees }
  })
}
