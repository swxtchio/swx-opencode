import { afterAll, expect } from "bun:test"
import { randomUUID } from "node:crypto"
import { rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { eq } from "drizzle-orm"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option } from "effect"
import * as TestClock from "effect/testing/TestClock"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import { Session } from "@/session/session"
import { SessionQueue } from "@/session/queue"
import { MessageID, PartID, SessionID } from "@/session/schema"
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

// A promotion of `delivery` whose first preparation waits on `gate`; later ones
// run straight through. `written` receives what the queue lets it commit.
const gated = (sessionID: SessionID, delivery: SessionQueue.Delivery, written: string[]) =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const preparing = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    const prepared: string[] = []
    const fiber = yield* queue
      .promote({
        sessionID,
        delivery,
        prepare: (input) =>
          Effect.sync(() =>
            prepared.push(input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")),
          ).pipe(
            Effect.andThen(Deferred.isDone(preparing)),
            Effect.flatMap((again) =>
              again ? Effect.void : Deferred.succeed(preparing, undefined).pipe(Effect.andThen(Deferred.await(gate))),
            ),
            Effect.as(input),
          ),
        write: (input) =>
          Effect.sync(() =>
            written.push(input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")),
          ),
        rejected: () => Effect.void,
      })
      .pipe(Effect.forkChild)
    yield* awaitWithTimeout(Deferred.await(preparing), "the gated promotion never began preparing")
    return {
      prepared,
      // Lets the gated preparation finish, and waits for its promotion to return.
      finish: Deferred.succeed(gate, undefined).pipe(
        Effect.andThen(awaitWithTimeout(Fiber.join(fiber), "the gated promotion never returned")),
      ),
    }
  })

// Like `gated`, but the first preparation then ends as `outcome` says: it fails,
// or it is interrupted. `finish` returns how the promotion ended.
const stale = (
  sessionID: SessionID,
  delivery: SessionQueue.Delivery,
  written: string[],
  outcome: "fail" | "interrupt",
) =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const preparing = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    const fiber = yield* queue
      .promote({
        sessionID,
        delivery,
        prepare: (input) =>
          Deferred.isDone(preparing).pipe(
            Effect.flatMap((again) =>
              again
                ? Effect.succeed(input)
                : Deferred.succeed(preparing, undefined).pipe(
                    Effect.andThen(Deferred.await(gate)),
                    Effect.andThen(outcome === "fail" ? Effect.fail("stale failure") : Effect.interrupt),
                  ),
            ),
          ),
        write: (input) =>
          Effect.sync(() =>
            written.push(input.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")),
          ),
        rejected: () => Effect.sync(() => written.push("rejected")),
      })
      .pipe(Effect.forkChild)
    yield* awaitWithTimeout(Deferred.await(preparing), "the gated promotion never began preparing")
    return Deferred.succeed(gate, undefined).pipe(
      Effect.andThen(awaitWithTimeout(Fiber.await(fiber), "the gated promotion never returned")),
    )
  })

const request = (sessionID: SessionID, delivery: SessionQueue.Delivery, requestID: string, value: string) =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    return yield* queue.request({ sessionID, delivery, requestID, parts: [{ type: "text", text: value }] })
  })

it.instance(
  "a preparation that fails or is interrupted after a merge made its reservation stale removes nothing",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const cases = (["fail", "interrupt"] as const).flatMap((outcome) =>
        (["steer", "queue"] as const).flatMap((delivery) =>
          (["consolidated", "admitted"] as const).map((merge) => ({ outcome, delivery, merge })),
        ),
      )
      yield* Effect.forEach(cases, ({ outcome, delivery, merge }) =>
        Effect.gen(function* () {
          const session = yield* sessions.create({ title: `${merge} while preparing, then ${outcome}, ${delivery}` })
          const written: string[] = []
          const named = merge === "admitted"
          yield* named ? request(session.id, delivery, "req-a", "run lint") : admit(session.id, delivery, "run lint")
          const finish = yield* stale(session.id, delivery, written, outcome)
          // The merge takes the second command into the row being prepared.
          if (named) yield* request(session.id, delivery, "req-b", "run lint --fix")
          if (!named) yield* admit(session.id, delivery, "run lint --fix")
          if (!named) yield* queue.consolidate(session.id)
          expect((yield* queue.list(session.id)).map(text)).toEqual(["run lint:\n1. run lint\n2. --fix"])
          const exit = yield* finish
          expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(outcome === "interrupt")
          yield* promoter(session.id, written)(delivery)
          expect(written).toEqual(["run lint:\n1. run lint\n2. --fix"])
          expect(yield* queue.list(session.id)).toEqual([])
          if (named)
            expect(Option.getOrThrow(yield* queue.receipt(session.id, "req-b"))).toMatchObject({
              state: "merged",
              into: "req-a",
            })
        }),
      )
    }),
  60_000,
)

it.instance(
  "a copy consolidated into a prompt that then joins a command follows it to the command",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "copy, then command" })
      yield* admit(session.id, "steer", "run lint")
      yield* request(session.id, "steer", "req-b", "run lint --fix")
      yield* request(session.id, "queue", "req-c", "run lint --fix")
      const [merged, ...rest] = yield* queue.consolidate(session.id)
      expect([text(merged!), rest]).toEqual(["run lint:\n1. run lint\n2. --fix", []])
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "req-b"))).toEqual({
        state: "pending",
        itemID: merged!.id,
      })
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "req-c"))).toEqual({ state: "merged", into: "req-b" })
      yield* promoter(session.id, [])("steer")
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "req-c"))).toMatchObject({
        state: "merged",
        into: "req-b",
        messageID: expect.any(String),
      })
    }),
  30_000,
)

it.instance(
  "a keyed notice never supersedes a reply, nor a reply one",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "keyed replies" })
      const status = (text: string) => `[fm-from-firstmate]\x1f[fm-level:nudge:status]\x1f ${text}`
      const notices = [
        ["r1", status("re:p1760000000-1-aaaa done")],
        ["r2", status("re:p1760000000-2-bbbb done")],
        ["s1", status("status: building")],
        ["r3", status("re:p1760000000-3-cccc done")],
        ["s2", status("status: tests running")],
      ] as const
      for (const [requestID, value] of notices) yield* request(session.id, "queue", requestID, value)
      expect((yield* queue.list(session.id)).map(text)).toEqual([
        status("re:p1760000000-1-aaaa done"),
        status("re:p1760000000-2-bbbb done"),
        status("status: tests running"),
        status("re:p1760000000-3-cccc done"),
      ])
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "s1"))).toEqual({ state: "superseded", by: "s2" })
      for (const requestID of ["r1", "r2", "r3"])
        expect(Option.getOrThrow(yield* queue.receipt(session.id, requestID))).toMatchObject({ state: "pending" })
    }),
  30_000,
)

it.instance(
  "a prompt whose delivery changes while it is being prepared is not committed under the old one",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "delivery changed in preparation" })
      const written: string[] = []
      const item = yield* admit(session.id, "steer", "steer, then queue")
      const racing = yield* gated(session.id, "steer", written)
      yield* queue.update({ sessionID: session.id, itemID: item.id, delivery: "queue" })
      yield* racing.finish
      expect(written).toEqual([])
      yield* promoter(session.id, written)("queue")
      expect(written).toEqual(["steer, then queue"])
    }),
  30_000,
)

it.instance(
  "a promotion prepared before a hold or a reorder never commits its stale reservation",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service

      // Reordered and closed while the first item's message is still being prepared.
      const closed = yield* sessions.create({ title: "reorder, closed before the preparation ends" })
      const closedWritten: string[] = []
      const first = yield* admit(closed.id, "queue", "admitted first")
      const second = yield* admit(closed.id, "queue", "admitted second")
      const racing = yield* gated(closed.id, "queue", closedWritten)
      yield* queue.hold(closed.id, "view")
      yield* queue.reorder(closed.id, [second.id, first.id])
      yield* queue.release(closed.id, "view")
      yield* racing.finish
      yield* promoter(closed.id, closedWritten)("queue")
      expect(closedWritten).toEqual(["admitted second", "admitted first"])

      // Reordered with no review open at all.
      const unheld = yield* sessions.create({ title: "reorder, no review open" })
      const unheldWritten: string[] = []
      const one = yield* admit(unheld.id, "queue", "admitted first")
      const two = yield* admit(unheld.id, "queue", "admitted second")
      const reordering = yield* gated(unheld.id, "queue", unheldWritten)
      yield* queue.reorder(unheld.id, [two.id, one.id])
      yield* reordering.finish
      yield* promoter(unheld.id, unheldWritten)("queue")
      expect(unheldWritten).toEqual(["admitted second", "admitted first"])

      // Held and closed with nothing changed: the preparation that began before
      // the hold is refused all the same, and the item prepared again.
      const untouched = yield* sessions.create({ title: "held and closed, nothing changed" })
      const untouchedWritten: string[] = []
      yield* admit(untouched.id, "queue", "admitted first")
      const holding = yield* gated(untouched.id, "queue", untouchedWritten)
      yield* queue.hold(untouched.id, "view")
      yield* queue.release(untouched.id, "view")
      yield* holding.finish
      expect(holding.prepared).toEqual(["admitted first", "admitted first"])
      expect(untouchedWritten).toEqual(["admitted first"])
    }),
  30_000,
)

it.instance(
  "a duplicate being prepared when the queue is consolidated is delivered once, in either category, held or not",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const cases = (["steer", "queue"] as const).flatMap((delivery) => [
        { delivery, held: true },
        { delivery, held: false },
      ])
      yield* Effect.forEach(cases, ({ delivery, held }) =>
        Effect.gen(function* () {
          const session = yield* sessions.create({ title: `consolidate a ${delivery} in preparation, held ${held}` })
          const written: string[] = []
          yield* admit(session.id, delivery, "the same instruction")
          yield* admit(session.id, delivery, "the same instruction")
          yield* admit(session.id, delivery, "another instruction")
          const racing = yield* gated(session.id, delivery, written)
          if (held) yield* queue.hold(session.id, "view")
          expect((yield* queue.consolidate(session.id)).map(text)).toEqual([
            "the same instruction",
            "another instruction",
          ])
          if (held) yield* queue.release(session.id, "view")
          yield* racing.finish
          yield* promoter(session.id, written)(delivery)
          yield* promoter(session.id, written)(delivery)
          expect(written).toEqual(["the same instruction", "another instruction"])
        }),
      )
    }),
  30_000,
)

it.instance(
  "a command being prepared when it is consolidated delivers every merged step",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "consolidate into a command in preparation" })
      const written: string[] = []
      yield* admit(session.id, "queue", "run lint")
      yield* admit(session.id, "queue", "run lint --fix")
      const racing = yield* gated(session.id, "queue", written)
      expect((yield* queue.consolidate(session.id)).map(text)).toEqual(["run lint:\n1. run lint\n2. --fix"])
      yield* racing.finish
      expect(written).toEqual(["run lint:\n1. run lint\n2. --fix"])
    }),
  30_000,
)

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

it.instance(
  "a bare command merges with a similar one as its own step, whatever its message and part IDs",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "review consolidate, bare command" })
      const written: string[] = []
      const identified = (value: string) =>
        queue.admit({
          sessionID: session.id,
          delivery: "queue",
          messageID: MessageID.ascending(),
          parts: [{ id: PartID.ascending(), type: "text", text: value }],
        })
      yield* identified("run lint")
      yield* identified("write the changelog")
      yield* identified("run lint --fix")

      expect((yield* queue.consolidate(session.id)).map(text)).toEqual([
        "run lint:\n1. run lint\n2. --fix",
        "write the changelog",
      ])
      yield* promoter(session.id, written)("queue")
      yield* promoter(session.id, written)("queue")
      expect(written).toEqual(["run lint:\n1. run lint\n2. --fix", "write the changelog"])
    }),
  30_000,
)

// swxtchio/swx-opencode#249: UP in the TUI withdraws a pending prompt to edit it.
it.instance(
  "withdrawing a prompt withdraws its request, and restoring it makes it pending again",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "review withdraw receipts" })
      const written: string[] = []
      const receipt = (requestID: string) =>
        queue.receipt(session.id, SessionQueue.RequestID.make(requestID)).pipe(Effect.map(Option.getOrUndefined))
      yield* request(session.id, "queue", "first", "check the build")
      // An identical copy takes the first in; the first now names it.
      const second = yield* request(session.id, "queue", "second", "check the build")
      if (second.kind !== "admitted") throw new Error("the copy was not admitted")
      expect(yield* receipt("first")).toEqual({ state: "merged", into: "second" })

      expect(Option.isSome(yield* queue.withdraw(session.id, second.own.id))).toBe(true)
      expect(yield* receipt("second")).toEqual({ state: "withdrawn", itemID: second.own.id })
      expect(yield* receipt("first")).toEqual({ state: "merged", into: "second" })
      // An exact retry of the withdrawn request is told so and admits nothing.
      const retried = yield* request(session.id, "queue", "second", "check the build")
      expect(retried.kind === "retry" ? retried.receipt : undefined).toEqual({
        state: "withdrawn",
        itemID: second.own.id,
      })
      expect(yield* queue.list(session.id)).toEqual([])
      const conflict = yield* request(session.id, "queue", "second", "check the lint").pipe(Effect.exit)
      expect(Exit.isFailure(conflict) && Cause.squash(conflict.cause)).toBeInstanceOf(SessionQueue.RequestConflictError)

      expect(Option.isSome(yield* queue.restore(session.id, second.own.id))).toBe(true)
      expect(yield* receipt("second")).toEqual({ state: "pending", itemID: second.own.id })
      yield* promoter(session.id, written)("queue")
      expect(written).toEqual(["check the build"])
      expect((yield* receipt("second"))?.state).toBe("delivered")
    }),
  30_000,
)

it.instance(
  "a prompt is pending before the session work its delivery waits on, and no promotion takes it until that is done",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "review admitted first" })
      const written: string[] = []
      const entered = yield* Deferred.make<void>()
      const gate = yield* Deferred.make<void>()
      const sent = yield* queue
        .request(
          { sessionID: session.id, delivery: "steer", requestID: "first", parts: [{ type: "text", text: "go" }] },
          undefined,
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(gate))),
        )
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(entered), "the session work never began")
      expect((yield* queue.list(session.id)).map(text)).toEqual(["go"])
      const promotion = yield* promoter(session.id, written)("steer").pipe(Effect.forkChild)
      // The promotion waits on the session's lock, which the session work holds.
      expect(yield* Fiber.join(promotion).pipe(Effect.timeoutOption("100 millis"))).toEqual(Option.none())
      expect(written).toEqual([])
      yield* Deferred.succeed(gate, undefined)
      yield* awaitWithTimeout(Fiber.join(sent), "the request never returned")
      yield* awaitWithTimeout(Fiber.join(promotion), "the promotion never returned")
      expect(written).toEqual(["go"])
    }),
  30_000,
)
