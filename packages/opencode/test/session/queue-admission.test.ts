import { afterAll, expect } from "bun:test"
import { randomUUID } from "node:crypto"
import { rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { eq } from "drizzle-orm"
import { Cause, Effect, Exit, Fiber, Layer } from "effect"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { newStatementAttempts, patchBunQuery } from "@opencode-ai/core/database/sqlite-attempts"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import { Session } from "@/session/session"
import { SessionQueue } from "@/session/queue"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceStore } from "@/project/instance-store"
import { InstanceBootstrap } from "@/project/bootstrap"
import { pollWithTimeout, testEffect } from "../lib/effect"

// A file-backed store, so a second connection can hold the write lock on the
// database the queue writes to. The file is removed after the suite.
const databasePath = path.join(os.tmpdir(), `opencode-queue-admission-${randomUUID()}.db`)

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Session.node,
      SessionQueue.node,
      EventV2Bridge.node,
      SessionProjector.node,
      Database.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
    ]),
    [
      [Database.node, Database.layerFromPath(databasePath)],
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalWorkspaces: false })],
      [
        InstanceBootstrap.node,
        Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void })),
      ],
    ],
  ),
)

afterAll(async () => {
  await Promise.all(
    [databasePath, `${databasePath}-wal`, `${databasePath}-shm`].map((file) => rm(file, { force: true })),
  )
})

// The admission's own statements: its transaction begin, and any statement on the queue tables. A busy attempt on
// one of these is the admission meeting the held lock.
const admissionSQL = /^begin\b|session_prompt_queue/i

it.instance(
  "admits a prompt whose write waits on another connection's commit instead of failing on a stale read",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const { db } = yield* Database.Service
      const session = yield* sessions.create({ title: "Admission under a concurrent writer" })
      const attempts = newStatementAttempts()
      yield* patchBunQuery((sql) => admissionSQL.test(sql), attempts)

      const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
      const holder = new sqlite.Database(databasePath)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (holder.inTransaction) holder.run("ROLLBACK")
          holder.close()
        }),
      )
      holder.run("BEGIN IMMEDIATE")
      holder.run("CREATE TABLE admission_lock_probe (id INTEGER PRIMARY KEY)")

      const admission = yield* queue
        .admit({
          sessionID: session.id,
          delivery: "queue",
          parts: [{ type: "text", text: "admitted under contention" }],
        })
        .pipe(Effect.forkChild)

      // Commit only after the admission has met the held lock. Waiting on its busy
      // attempt, not on a clock, means a late-starting admission cannot run after
      // the commit. The timeout is only a backstop.
      yield* pollWithTimeout(
        Effect.sync(() => (attempts.busy.length > 0 ? true : undefined)),
        "admission never met SQLITE_BUSY",
        "10 seconds",
      )
      // It is still pending while the lock is held.
      expect(admission.pollUnsafe()).toBeUndefined()

      // The commit then changes the database after the admission's read, which is
      // what makes a deferred read stale.
      holder.run("COMMIT")

      const exit = yield* Fiber.await(admission)
      if (Exit.isFailure(exit))
        throw new Error(
          `${Cause.pretty(exit.cause)}\nadmission attempts: ${attempts.count}; busy codes: ${attempts.busy.join(", ")}`,
        )
      const rows = yield* db
        .select({ id: SessionPromptQueueTable.id })
        .from(SessionPromptQueueTable)
        .where(eq(SessionPromptQueueTable.session_id, session.id))
        .all()
      expect(rows).toHaveLength(1)
    }),
  30_000,
)
