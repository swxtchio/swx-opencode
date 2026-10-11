import { afterAll, expect } from "bun:test"
import { randomUUID } from "node:crypto"
import { rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { eq } from "drizzle-orm"
import { Deferred, Effect, Exit, Fiber, Layer, Option } from "effect"
import * as TestClock from "effect/testing/TestClock"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import { Session } from "@/session/session"
import { SessionQueue } from "@/session/queue"
import { SessionID } from "@/session/schema"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceStore } from "@/project/instance-store"
import { InstanceBootstrap } from "@/project/bootstrap"
import { awaitWithTimeout, testEffect } from "../lib/effect"

// The review view's queue operations (swxtchio/swx-opencode#212), against real
// queue rows. Delivery through a running session is covered over HTTP in
// httpapi-session.test.ts.
const databasePath = path.join(os.tmpdir(), `opencode-queue-review-${randomUUID()}.db`)

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

const text = (item: SessionQueue.Item) =>
  item.input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")

const admit = (sessionID: SessionID, delivery: SessionQueue.Delivery, value: string) =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    return yield* queue.admit({ sessionID, delivery, parts: [{ type: "text", text: value }] })
  })

// Promotes with the queue's own reserve and commit; `written` receives each
// message the queue lets through, in order.
const promoter = (sessionID: SessionID, written: string[]) => (delivery: SessionQueue.Delivery) =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    return yield* queue.promote({
      sessionID,
      delivery,
      prepare: (input) => Effect.succeed(input),
      write: (input) =>
        Effect.sync(() =>
          written.push(input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")),
        ),
      rejected: () => Effect.void,
    })
  })

it.instance(
  "a review hold stops both categories, even a promotion already being prepared, until it is released",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "review hold" })
      const written: string[] = []
      const promote = promoter(session.id, written)
      yield* admit(session.id, "steer", "steer while reviewed")
      yield* admit(session.id, "queue", "queued while reviewed")

      // A steer promotion is mid-preparation when the view opens.
      const preparing = yield* Deferred.make<void>()
      const gate = yield* Deferred.make<void>()
      const racing = yield* queue
        .promote({
          sessionID: session.id,
          delivery: "steer",
          prepare: (input) =>
            Deferred.succeed(preparing, undefined).pipe(Effect.andThen(Deferred.await(gate)), Effect.as(input)),
          write: (input) =>
            Effect.sync(() =>
              written.push(input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")),
            ),
          rejected: () => Effect.void,
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(preparing), "steer promotion never began preparing")
      const opened = yield* queue.hold(session.id, "view-1")
      expect(opened.started).toBe(true)
      expect(opened.items.map((item) => [item.delivery, text(item)])).toEqual([
        ["steer", "steer while reviewed"],
        ["queue", "queued while reviewed"],
      ])
      yield* Deferred.succeed(gate, undefined)
      yield* awaitWithTimeout(Fiber.join(racing), "held steer promotion never returned")

      // Neither the prepared steer nor any later attempt of either category lands.
      yield* promote("steer")
      yield* promote("queue")
      expect(written).toEqual([])
      expect((yield* queue.list(session.id)).map(text)).toEqual(["steer while reviewed", "queued while reviewed"])
      expect(yield* queue.awaitingDrain(session.id)).toBe(false)

      // A second viewer's hold outlives the first one's release.
      expect((yield* queue.hold(session.id, "view-2")).started).toBe(false)
      expect(yield* queue.release(session.id, "view-1")).toBe(false)
      yield* promote("steer")
      expect(written).toEqual([])

      expect(yield* queue.release(session.id, "view-2")).toBe(true)
      expect(yield* queue.awaitingDrain(session.id)).toBe(true)
      yield* promote("steer")
      yield* promote("queue")
      expect(written).toEqual(["steer while reviewed", "queued while reviewed"])
      expect(yield* queue.list(session.id)).toEqual([])
    }),
  30_000,
)

it.instance(
  "a review hold whose lease is not renewed lapses and reports it",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "review lease" })
      const written: string[] = []
      const promote = promoter(session.id, written)
      yield* admit(session.id, "queue", "kept by the lease")
      // A hold nobody watches lapses for promotion all the same.
      const unwatched = yield* sessions.create({ title: "review lease, unwatched" })
      const unwatchedWritten: string[] = []
      yield* admit(unwatched.id, "queue", "freed by its own deadline")
      yield* queue.hold(unwatched.id, "view")

      yield* queue.hold(session.id, "view")
      const ended = yield* queue.released(session.id).pipe(Effect.forkChild)
      yield* TestClock.adjust("5 minutes")
      // Renewed before it lapses, the hold lasts a full lease from the renewal.
      expect((yield* queue.hold(session.id, "view")).started).toBe(false)
      yield* TestClock.adjust("5 minutes")
      yield* promote("queue")
      expect(written).toEqual([])
      expect(ended.pollUnsafe()).toBeUndefined()
      yield* promoter(unwatched.id, unwatchedWritten)("queue")
      expect(unwatchedWritten).toEqual(["freed by its own deadline"])

      yield* TestClock.adjust("1 minute")
      // A timeout would run on the test clock too, so a bounded number of turns
      // lets the woken watcher finish instead.
      for (let turn = 0; turn < 100 && ended.pollUnsafe() === undefined; turn++) yield* Effect.yieldNow
      const end = ended.pollUnsafe()
      expect(end && Exit.isSuccess(end) ? end.value : undefined).toBe("lapsed")
      yield* promote("queue")
      expect(written).toEqual(["kept by the lease"])
    }).pipe(Effect.provide(TestClock.layer())),
  30_000,
)

it.instance(
  "remove deletes only a pending row, for good",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const { db } = yield* Database.Service
      const session = yield* sessions.create({ title: "review remove" })
      const other = yield* sessions.create({ title: "review remove, other session" })
      const written: string[] = []
      const promote = promoter(session.id, written)
      const delivered = yield* admit(session.id, "queue", "delivered first")
      const doomed = yield* admit(session.id, "queue", "deleted")
      yield* admit(session.id, "queue", "kept")
      yield* promote("queue")
      expect(written).toEqual(["delivered first"])

      expect(Option.isNone(yield* queue.remove(other.id, doomed.id))).toBe(true)
      expect(Option.isNone(yield* queue.remove(session.id, delivered.id))).toBe(true)
      expect(Option.getOrUndefined(yield* queue.remove(session.id, doomed.id))?.id).toBe(doomed.id)
      const rows = yield* db
        .select({ id: SessionPromptQueueTable.id })
        .from(SessionPromptQueueTable)
        .where(eq(SessionPromptQueueTable.id, doomed.id))
        .all()
      expect(rows).toEqual([])
      expect(Option.isNone(yield* queue.restore(session.id, doomed.id))).toBe(true)
      expect(Option.isNone(yield* queue.remove(session.id, doomed.id))).toBe(true)

      yield* promote("queue")
      yield* promote("queue")
      expect(written).toEqual(["delivered first", "kept"])
    }),
  30_000,
)

it.instance(
  "reorder changes promotion order within each category and never a category",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "review reorder" })
      const written: string[] = []
      const promote = promoter(session.id, written)
      const first = yield* admit(session.id, "queue", "queue one")
      const second = yield* admit(session.id, "queue", "queue two")
      const steer = yield* admit(session.id, "steer", "steer one")
      const third = yield* admit(session.id, "queue", "queue three")

      const reordered = yield* queue.reorder(session.id, [third.id, steer.id, first.id, second.id])
      expect(Option.getOrThrow(reordered).map((item) => [item.delivery, text(item)])).toEqual([
        ["steer", "steer one"],
        ["queue", "queue three"],
        ["queue", "queue one"],
        ["queue", "queue two"],
      ])

      // A stale reference fails the whole reorder and changes nothing.
      yield* promote("steer")
      expect(Option.isNone(yield* queue.reorder(session.id, [second.id, steer.id, first.id]))).toBe(true)
      expect(Option.isNone(yield* queue.reorder(session.id, [second.id, second.id]))).toBe(true)
      expect((yield* queue.list(session.id)).map(text)).toEqual(["queue three", "queue one", "queue two"])

      yield* promote("queue")
      yield* promote("queue")
      yield* promote("queue")
      expect(written).toEqual(["steer one", "queue three", "queue one", "queue two"])
    }),
  30_000,
)

it.instance(
  "consolidate keeps one of identical prompts and merges similar commands into ordered steps",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "review consolidate" })
      const written: string[] = []
      const promote = promoter(session.id, written)
      yield* admit(session.id, "steer", "fix auth.ts: handle a null token")
      yield* admit(session.id, "queue", "run the full suite")
      yield* admit(session.id, "steer", "review auth.ts: check the naming")
      yield* admit(session.id, "queue", "run the full suite")
      yield* admit(session.id, "steer", "Fix auth.ts add a regression test")
      yield* admit(session.id, "steer", "fix login.ts: tidy the imports")
      yield* admit(session.id, "queue", "fix auth.ts: queued step")
      yield* admit(session.id, "steer", "run the full suite")

      expect((yield* queue.consolidate(session.id)).map((item) => [item.delivery, text(item)])).toEqual([
        ["steer", "fix auth.ts:\n1. handle a null token\n2. add a regression test"],
        ["steer", "review auth.ts: check the naming"],
        ["steer", "fix login.ts: tidy the imports"],
        ["steer", "run the full suite"],
        ["queue", "fix auth.ts: queued step"],
      ])

      // A later similar command adds a step to the merged one.
      yield* admit(session.id, "steer", "fix auth.ts: update the docs")
      expect((yield* queue.consolidate(session.id)).map(text)[0]).toBe(
        "fix auth.ts:\n1. handle a null token\n2. add a regression test\n3. update the docs",
      )

      yield* promote("steer")
      yield* promote("queue")
      expect(written).toEqual([
        "fix auth.ts:\n1. handle a null token\n2. add a regression test\n3. update the docs",
        "review auth.ts: check the naming",
        "fix login.ts: tidy the imports",
        "run the full suite",
        "fix auth.ts: queued step",
      ])
    }),
  30_000,
)
