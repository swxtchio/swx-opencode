import { afterAll, expect } from "bun:test"
import { randomUUID } from "node:crypto"
import { rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { eq } from "drizzle-orm"
import { Cause, Effect, Exit, Fiber, Layer } from "effect"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
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
import { testEffect } from "../lib/effect"

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

it.instance(
  "admits a prompt whose write waits on another connection's commit instead of failing on a stale read",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const { db } = yield* Database.Service
      const session = yield* sessions.create({ title: "Admission under a concurrent writer" })

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

      // Hold the write lock past the admission's read. The commit then changes
      // the database after that read, which is what makes a deferred read stale.
      yield* Effect.sleep("1500 millis")
      holder.run("COMMIT")

      const exit = yield* Fiber.await(admission)
      if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
      const rows = yield* db
        .select({ id: SessionPromptQueueTable.id })
        .from(SessionPromptQueueTable)
        .where(eq(SessionPromptQueueTable.session_id, session.id))
        .all()
      expect(rows).toHaveLength(1)
    }),
  30_000,
)
