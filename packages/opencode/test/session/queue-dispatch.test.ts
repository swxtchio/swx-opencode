import { afterAll, expect } from "bun:test"
import { randomUUID } from "node:crypto"
import { rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Schema } from "effect"
import { eq } from "drizzle-orm"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionPromptQueueTable } from "@opencode-ai/core/session/prompt-queue.sql"
import { EventV2 } from "@opencode-ai/core/event"
import { EventWriter } from "@opencode-ai/core/event/writer"
import { EventAggregateWriterTable } from "@opencode-ai/core/event/writer.sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { Session } from "@/session/session"
import { SessionQueue } from "@/session/queue"
import { MessageV2 } from "@/session/message-v2"
import { MessageID, PartID } from "@/session/schema"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceStore } from "@/project/instance-store"
import { InstanceBootstrap } from "@/project/bootstrap"
import { testEffect } from "../lib/effect"

const filename = path.join(os.tmpdir(), `opencode-queue-dispatch-${randomUUID()}.db`)
const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Session.node,
      SessionQueue.node,
      EventV2.node,
      EventV2Bridge.node,
      SessionProjector.node,
      Database.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
    ]),
    [
      [Database.node, Database.layerFromPath(filename)],
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalWorkspaces: false })],
      [
        InstanceBootstrap.node,
        Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void })),
      ],
    ],
  ),
)
afterAll(async () => {
  await Promise.all([filename, `${filename}-wal`, `${filename}-shm`].map((file) => rm(file, { force: true })))
})

const peer = (n: number, level = "request") =>
  `[fm-from-peer]\x1f saas\x1f /scratch/saas\x1f p1791809000-${n}-abcd\x1f [fm-level:${level}]\x1f Complete peer ${n}\nre:p1791800000-${n}-def0\nDo not omit this last line ${n}`
const check = (episode: string, state: string) =>
  `[fm-level:nudge:watcher.wake]\x1f WATCHER FIRED [wake-1] - handle wake.\n\ncheck: /scratch/state/${episode}.check.sh: ${state}`
const prepare = (input: SessionQueue.PromotedInput) =>
  Effect.sync(() => ({
    info: {
      id: input.messageID,
      sessionID: input.sessionID,
      role: "user" as const,
      time: { created: Date.now() },
      agent: "build",
      model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
    },
    parts: input.parts.flatMap((part) =>
      part.type === "text"
        ? [{ ...part, id: part.id ?? PartID.ascending(), sessionID: input.sessionID, messageID: input.messageID }]
        : [],
    ),
  }))
const writer = (sessions: Session.Interface) => (message: SessionV1.WithParts) =>
  Effect.gen(function* () {
    yield* sessions.updateMessage(message.info)
    yield* Effect.forEach(message.parts, (part) => sessions.updatePart(part), { discard: true })
  })
const metadata = (message: SessionV1.WithParts) =>
  Schema.decodeUnknownSync(SessionPromptQueue.Group)(
    message.parts.find((part) => part.type === "text")?.metadata?.sessionQueue,
  )
const text = (message: SessionV1.WithParts) =>
  message.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")

it.instance(
  "peers precede older automation, ten complete envelopes share one delivery and overflow precedes automation",
  () =>
    Effect.gen(function* () {
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      yield* queue.admit({
        sessionID: session.id,
        delivery: "queue",
        parts: [{ type: "text", text: check("old", "stalled") }],
      })
      const originals = yield* Effect.forEach(
        Array.from({ length: 11 }, (_, n) => n + 1),
        (n) =>
          queue.admit({
            sessionID: session.id,
            delivery: "queue",
            parts: [{ type: "text", text: peer(n, n === 2 ? "info" : "request") }],
          }),
      )
      const promote = () =>
        queue.promote({
          sessionID: session.id,
          delivery: "queue",
          prepare,
          write: writer(sessions),
          rejected: () => Effect.void,
        })
      yield* promote()
      const first = (yield* MessageV2.stream(session.id))[0]
      expect(metadata(first).kind).toBe("peer")
      expect(metadata(first).constituents.map((item) => item.id)).toEqual(originals.slice(0, 10).map((item) => item.id))
      originals.slice(0, 10).forEach((item, n) => {
        expect(text(first)).toContain(`${n + 1}. [Admitted`)
        expect(text(first)).toContain(peer(n + 1, n === 1 ? "info" : "request"))
      })
      expect(text(first)).not.toContain(peer(11))
      expect(yield* queue.list(session.id)).toHaveLength(2)
      yield* queue.consume(session.id)
      expect(
        metadata(yield* MessageV2.get({ sessionID: session.id, messageID: first.info.id })).constituents,
      ).toHaveLength(10)
      yield* promote()
      expect(metadata((yield* MessageV2.stream(session.id))[0]).constituents[0].id).toBe(originals[10].id)
      expect(yield* queue.list(session.id)).toHaveLength(1)
      yield* promote()
      expect(metadata((yield* MessageV2.stream(session.id))[0]).kind).toBe("watcher.check")
    }),
)

it.instance(
  "same automation type delivers once with newest repeated check and every distinct task plus original metadata",
  () =>
    Effect.gen(function* () {
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const inputs = [check("one", "stalled"), check("two", "pending decision"), check("one", "converged")]
      const originals = yield* Effect.forEach(inputs, (value) =>
        queue.admit({ sessionID: session.id, delivery: "queue", parts: [{ type: "text", text: value }] }),
      )
      yield* queue.admit({
        sessionID: session.id,
        delivery: "queue",
        parts: [
          {
            type: "text",
            text: "[fm-level:nudge:watcher.wake]\x1f WATCHER FIRED [wake-2] - wake.\n\nsignal: another current task",
          },
        ],
      })
      yield* queue.promote({
        sessionID: session.id,
        delivery: "queue",
        prepare,
        write: writer(sessions),
        rejected: () => Effect.void,
      })
      const message = (yield* MessageV2.stream(session.id))[0]
      expect(text(message)).not.toContain("stalled")
      expect(text(message)).toContain("Historical producer observations, not current measurements")
      expect(text(message)).toContain("the producer owns fresh measurements and pacing")
      expect(text(message)).toContain("converged")
      expect(text(message)).toContain("pending decision")
      expect(metadata(message).constituents.map((item) => item.id)).toEqual(originals.map((item) => item.id))
      expect(metadata(message).constituents.map((item) => item.time.created)).toEqual(
        originals.map((item) => item.time.created),
      )
      expect(metadata(message).constituents[0].input.parts[0]).toEqual({ type: "text", text: inputs[0] })
      expect(yield* queue.list(session.id)).toHaveLength(1)
    }),
)

it.instance("concurrent peer arrival stays pending and withdrawal invalidates complete prepared group", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const one = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: peer(1) }],
    })
    const two = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: peer(2) }],
    })
    const ready = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    let preparations = 0
    const promotion = yield* queue
      .promote({
        sessionID: session.id,
        delivery: "queue",
        prepare: (input) =>
          Effect.gen(function* () {
            if (++preparations === 1) {
              yield* Deferred.succeed(ready, undefined)
              yield* Deferred.await(release)
            }
            return yield* prepare(input)
          }),
        write: writer(sessions),
        rejected: () => Effect.void,
      })
      .pipe(Effect.forkChild)
    yield* Deferred.await(ready)
    const three = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: peer(3) }],
    })
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(promotion)
    expect(metadata((yield* MessageV2.stream(session.id))[0]).constituents.map((item) => item.id)).toEqual([
      one.id,
      two.id,
    ])
    expect((yield* queue.list(session.id)).map((item) => item.id)).toEqual([three.id])
    const held = yield* queue.withdraw(session.id, three.id)
    expect(Option.isSome(held)).toBe(true)
    expect(yield* queue.list(session.id)).toEqual([])
    yield* queue.restore(session.id, three.id)
    const next = yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare: (input) =>
        Effect.gen(function* () {
          yield* queue.withdraw(session.id, three.id)
          return yield* prepare(input)
        }),
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    expect(next).toBe(true)
    expect((yield* MessageV2.stream(session.id)).length).toBe(1)
  }),
)

it.instance(
  "failed grouped write rolls back message, parts, promotions and notifications, then exact retry after consume reuses original",
  () =>
    Effect.gen(function* () {
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const events = yield* EventV2.Service
      const database = yield* Database.Service
      const session = yield* sessions.create({})
      const admission = {
        sessionID: session.id,
        delivery: "queue" as const,
        messageID: MessageID.ascending(),
        parts: [{ type: "text" as const, text: peer(1) }],
      }
      const original = yield* queue.admit(admission)
      expect((yield* queue.admit({ ...admission, messageID: undefined })).id).toBe(original.id)
      expect((yield* queue.admit({ ...admission, messageID: MessageID.ascending() })).id).toBe(original.id)
      // PR228 records event writers inside nested event transactions; a failed outer group must roll that back too.
      yield* database.db
        .update(EventAggregateWriterTable)
        .set({ time_last_append: 1 })
        .where(eq(EventAggregateWriterTable.aggregate_id, session.id))
        .run()
      const writing = () =>
        database.db
          .select()
          .from(EventAggregateWriterTable)
          .where(eq(EventAggregateWriterTable.aggregate_id, session.id))
          .all()
      const seen: string[] = []
      const unsubscribe = yield* events.listen((event) =>
        Effect.sync(() => {
          if (event.type === SessionV1.Event.MessageUpdated.type) seen.push(event.id)
        }),
      )
      const failed = yield* queue
        .promote({
          sessionID: session.id,
          delivery: "queue",
          prepare,
          write: (message) =>
            Effect.gen(function* () {
              yield* sessions.updateMessage(message.info)
              return yield* Effect.die("part write rejected")
            }),
          rejected: () => Effect.void,
        })
        .pipe(Effect.exit)
      expect(Exit.isFailure(failed)).toBe(true)
      expect(seen).toEqual([])
      expect(yield* writing()).toEqual([{ aggregate_id: session.id, writer_id: EventWriter.id(), time_last_append: 1 }])
      expect(yield* MessageV2.stream(session.id)).toEqual([])
      expect((yield* queue.list(session.id)).map((item) => item.id)).toEqual([original.id])
      yield* queue.promote({
        sessionID: session.id,
        delivery: "queue",
        prepare,
        write: writer(sessions),
        rejected: () => Effect.void,
      })
      expect(seen).toHaveLength(1)
      expect((yield* writing())[0]?.time_last_append).toBeGreaterThan(1)
      yield* unsubscribe
      yield* queue.consume(session.id)
      const retry = yield* queue.admit(admission)
      expect(retry.id).toBe(original.id)
      expect((yield* queue.admit({ ...admission, messageID: undefined })).id).toBe(original.id)
      expect((yield* queue.admit({ ...admission, messageID: MessageID.ascending() })).id).toBe(original.id)
      expect(yield* queue.list(session.id)).toEqual([])
      const stored = yield* database.db
        .select()
        .from(SessionPromptQueueTable)
        .where(eq(SessionPromptQueueTable.id, original.id))
        .get()
      expect(stored).toBeUndefined()
      const conflict = yield* queue.admit({ ...admission, parts: [{ type: "text", text: peer(2) }] }).pipe(Effect.exit)
      expect(Exit.isFailure(conflict)).toBe(true)
      if (Exit.isFailure(conflict)) expect(Cause.pretty(conflict.cause)).toContain("Conflicting")
    }),
)

it.instance(
  "manual, unknown machine and critical automation stay separate; immediate machine noReply uses durable selection",
  () =>
    Effect.gen(function* () {
      const queue = yield* SessionQueue.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const values = [
        "Human command",
        "OBSERVER: unknown meaningful directive",
        "[fm-level:critical]\x1f WATCHER FIRED [wake-1] emergency",
      ]
      yield* Effect.forEach(values, (value) =>
        queue.admit({ sessionID: session.id, delivery: "queue", parts: [{ type: "text", text: value }] }),
      )
      yield* queue.promote({
        sessionID: session.id,
        delivery: "queue",
        prepare,
        write: writer(sessions),
        rejected: () => Effect.void,
      })
      expect(text((yield* MessageV2.stream(session.id))[0])).toBe(values[0])
      expect(yield* queue.list(session.id)).toHaveLength(2)
      const input = {
        sessionID: session.id,
        delivery: "queue" as const,
        parts: [{ type: "text" as const, text: peer(4) }],
      }
      const entry = yield* queue.writeOrAdmit({
        admission: input,
        isBusy: Effect.succeed(false),
        prepare: () => prepare({ ...input, messageID: MessageID.ascending() }),
        write: (message) => writer(sessions)(message).pipe(Effect.as(message)),
        discard: () => Effect.void,
      })
      expect(entry.kind).toBe("queued")
    }),
)

it.instance("heartbeat duties survive type coalescing and malformed peer frames remain individual", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const heartbeat = (duty: string, receipt: string) =>
      `[fm-level:nudge:heartbeat]\x1f Fleet heartbeat. Run one supervision cycle from live state, not memory: read the live fleet, backlog, and open work fresh; identify who is blocked only on firstmate, and take the highest-value in-scope action that advances convergence. Escalate destructive, irreversible, or security-sensitive decisions to the captain.${duty ? ` Periodic nudge: ${duty}` : ""} See docs/fleet-operating-process.md. [fm-heartbeat-receipt:${receipt}]`
    const values = [
      heartbeat("check task A", "a1"),
      heartbeat("check task B", "b1"),
      heartbeat("", "telemetry1"),
      heartbeat("", "telemetry2"),
    ]
    yield* Effect.forEach(values, (value) =>
      queue.admit({ sessionID: session.id, delivery: "queue", parts: [{ type: "text", text: value }] }),
    )
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    const message = (yield* MessageV2.stream(session.id))[0]
    expect(text(message)).toContain("check task A")
    expect(text(message)).toContain("check task B")
    expect(text(message)).toContain("telemetry2")
    expect(metadata(message).constituents).toHaveLength(4)
    const malformed = "[fm-from-peer]\x1f invalid sender\x1f relative-home\x1f bad-id\x1f meaningful unknown content"
    yield* queue.admit({ sessionID: session.id, delivery: "queue", parts: [{ type: "text", text: malformed }] })
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    expect(text((yield* MessageV2.stream(session.id))[0])).toBe(malformed)
  }),
)

it.instance("failed native preparation is rejected without poisoning subsequent peer delivery", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const bad = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: "invalid native request" }],
    })
    const good = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: peer(1) }],
    })
    const rejected: unknown[] = []
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare: (input) =>
        input.parts.some((part) => part.type === "text" && part.text === "invalid native request")
          ? Effect.die("bad native")
          : prepare(input),
      write: writer(sessions),
      rejected: (cause) =>
        Effect.sync(() => {
          rejected.push(cause)
        }),
    })
    expect(rejected).toHaveLength(1)
    expect((yield* queue.list(session.id)).some((item) => item.id === bad.id)).toBe(false)
    expect(metadata((yield* MessageV2.stream(session.id))[0]).constituents.map((item) => item.id)).toEqual([good.id])
  }),
)

it.instance("fresh coordinator recovers original reserved membership and peer identity retry after consume", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const input = {
      sessionID: session.id,
      delivery: "queue" as const,
      parts: [{ type: "text" as const, text: peer(1) }],
    }
    const one = yield* queue.admit(input)
    const two = yield* queue.admit({ ...input, parts: [{ type: "text", text: peer(2) }] })
    const ready = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const original = yield* queue
      .promote({
        sessionID: session.id,
        delivery: "queue",
        prepare: (input) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(ready, undefined)
            yield* Deferred.await(release)
            return yield* prepare(input)
          }),
        write: writer(sessions),
        rejected: () => Effect.void,
      })
      .pipe(Effect.forkChild)
    yield* Deferred.await(ready)
    const three = yield* queue.admit({ ...input, parts: [{ type: "text", text: peer(3) }] })
    const fresh = SessionQueue.node.implementation as Layer.Layer<
      SessionQueue.Service,
      never,
      Database.Service | EventV2Bridge.Service
    >
    yield* Effect.gen(function* () {
      const restarted = yield* SessionQueue.Service
      yield* restarted.promote({
        sessionID: session.id,
        delivery: "queue",
        prepare,
        write: writer(sessions),
        rejected: () => Effect.void,
      })
      expect(metadata((yield* MessageV2.stream(session.id))[0]).constituents.map((item) => item.id)).toEqual([
        one.id,
        two.id,
      ])
      expect((yield* restarted.list(session.id)).map((item) => item.id)).toEqual([three.id])
      yield* restarted.consume(session.id)
      expect((yield* restarted.admit(input)).id).toBe(one.id)
      expect((yield* restarted.list(session.id)).map((item) => item.id)).toEqual([three.id])
    }).pipe(Effect.provide(Layer.fresh(fresh)))
    yield* Fiber.interrupt(original)
    expect((yield* MessageV2.stream(session.id)).length).toBe(1)
  }),
)

it.instance("peer retry cannot borrow another constituent transport identity before or after consume", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const one = {
      sessionID: session.id,
      delivery: "steer" as const,
      messageID: MessageID.ascending(),
      parts: [{ type: "text" as const, text: peer(1) }],
    }
    const two = {
      ...one,
      delivery: "queue" as const,
      messageID: MessageID.ascending(),
      parts: [{ type: "text" as const, text: peer(2) }],
    }
    yield* queue.admit(one)
    yield* queue.admit(two)
    const conflict = () => queue.admit({ ...one, messageID: two.messageID }).pipe(Effect.exit)
    expect(Exit.isFailure(yield* conflict())).toBe(true)
    expect(yield* queue.list(session.id)).toHaveLength(2)
    yield* queue.promote({
      sessionID: session.id,
      delivery: "steer",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    expect(Exit.isFailure(yield* conflict())).toBe(true)
    expect(yield* queue.list(session.id)).toHaveLength(1)
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    yield* queue.consume(session.id)
    expect(Exit.isFailure(yield* conflict())).toBe(true)
    expect(yield* queue.list(session.id)).toEqual([])
  }),
)

it.instance("peer batching retains complete file input alongside its literal envelope and constituent metadata", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const file = {
      type: "file" as const,
      mime: "text/plain",
      filename: "peer-evidence.txt",
      url: "data:text/plain;base64,Y29tcGxldGUgZXZpZGVuY2U=",
    }
    const original = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: peer(1) }, file],
    })
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare: (input) => {
        expect(input.parts.filter((part) => part.type === "file")).toEqual([file])
        return prepare(input)
      },
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    const message = (yield* MessageV2.stream(session.id))[0]
    expect(text(message)).toContain(peer(1))
    expect(metadata(message).constituents[0].id).toBe(original.id)
    expect(metadata(message).constituents[0].input.parts[1]).toEqual(file)
  }),
)

it.instance("emits grouped consumer checkpoint through admission promotion and message reads", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const capture = process.env.OPENCODE_WRITE_GROUPED_FIXTURE === "1"
    const git = (args: string[]) =>
      Effect.promise(async () => {
        const child = Bun.spawn(["git", ...args], { cwd: import.meta.dirname, stdout: "pipe", stderr: "pipe" })
        const output = await new Response(child.stdout).text()
        if (await child.exited) throw new Error(await new Response(child.stderr).text())
        return output.trim()
      })
    const sha = yield* git(["rev-parse", "HEAD"])
    if (capture) expect(yield* git(["status", "--porcelain"])).toBe("")
    const model = {
      id: ModelV2.ID.make("test"),
      providerID: ProviderV2.ID.make("test"),
      name: "Synthetic receiver",
      api: { id: "test", url: "http://127.0.0.1:1", npm: "@ai-sdk/openai-compatible" },
      capabilities: {
        temperature: false,
        reasoning: false,
        attachment: false,
        toolcall: true,
        input: { text: true, audio: false, image: false, video: false, pdf: false },
        output: { text: true, audio: false, image: false, video: false, pdf: false },
        interleaved: false as const,
      },
      cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
      limit: { context: 100000, input: 100000, output: 10000 },
      status: "active" as const,
      options: {},
      headers: {},
      release_date: "2026-01-01",
    }
    const scenarios = yield* Effect.forEach(["peer", "automation"] as const, (name) =>
      Effect.gen(function* () {
        const session = yield* sessions.create({ title: `Synthetic grouped checkpoint: ${name}` })
        const inputs: SessionQueue.AdmitInput[] =
          name === "peer"
            ? [
                {
                  sessionID: session.id,
                  delivery: "queue",
                  noReply: true,
                  messageID: MessageID.make("msg_checkpoint_peer_request"),
                  agent: "build",
                  model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("synthetic-sender-model") },
                  variant: "low",
                  parts: [{ type: "text", text: peer(1, "request") }],
                },
                { sessionID: session.id, delivery: "queue", parts: [{ type: "text", text: peer(2, "info") }] },
              ]
            : [
                {
                  sessionID: session.id,
                  delivery: "queue",
                  messageID: MessageID.make("msg_checkpoint_notice_old"),
                  parts: [{ type: "text", text: check("one", "historical stalled observation") }],
                },
                {
                  sessionID: session.id,
                  delivery: "queue",
                  messageID: MessageID.make("msg_checkpoint_notice_distinct"),
                  parts: [{ type: "text", text: check("two", "historical pending decision observation") }],
                },
                {
                  sessionID: session.id,
                  delivery: "queue",
                  messageID: MessageID.make("msg_checkpoint_notice_later"),
                  parts: [{ type: "text", text: check("one", "historical converged observation") }],
                },
              ]
        const originals = yield* Effect.forEach(inputs, (input) => queue.admit(input))
        const pending = yield* queue.list(session.id)
        yield* queue.promote({
          sessionID: session.id,
          delivery: "queue",
          prepare,
          write: writer(sessions),
          rejected: () => Effect.void,
        })
        const latest = (yield* MessageV2.stream(session.id))[0]
        const returned = yield* MessageV2.get({ sessionID: session.id, messageID: latest.info.id })
        const group = metadata(returned)
        expect(group.constituents).toEqual(originals)
        expect(group.constituents.map((item) => item.time.created)).toEqual(originals.map((item) => item.time.created))
        const encoded = originals.map((item) => Schema.encodeSync(SessionPromptQueue.QueuedInput)(item.input))
        expect(group.constituents.map((item) => Schema.encodeSync(SessionPromptQueue.QueuedInput)(item.input))).toEqual(
          encoded,
        )
        const witnesses = returned.parts.flatMap((part) =>
          part.type === "text" && part.metadata?.sessionQueue
            ? [
                {
                  partID: part.id,
                  messageID: part.messageID,
                  ignored: part.ignored === true,
                  synthetic: part.synthetic === true,
                  constituentIDs: Schema.decodeUnknownSync(SessionPromptQueue.Group)(
                    part.metadata.sessionQueue,
                  ).constituents.map((item) => item.id),
                },
              ]
            : [],
        )
        expect([...new Set(witnesses.flatMap((part) => part.constituentIDs))].sort()).toEqual(
          originals.map((item) => item.id).sort(),
        )
        expect(
          witnesses
            .filter((part) => part.ignored)
            .every((part) => part.synthetic && part.messageID === returned.info.id),
        ).toBe(true)
        expect(witnesses.filter((part) => !part.ignored)).toHaveLength(1)
        expect(witnesses.filter((part) => part.ignored)).toHaveLength(3)
        if (name === "peer") {
          expect(
            witnesses.filter((part) => part.ignored && part.constituentIDs.includes(originals[0].id)),
          ).toHaveLength(2)
          expect(Object.hasOwn(group.constituents[0].input, "noReply")).toBe(false)
          expect(Object.hasOwn(group.constituents[0].input, "delivery")).toBe(false)
          expect(group.constituents[0].delivery).toBe("queue")
          expect(group.constituents[0].input.messageID).toBe(inputs[0].messageID)
          expect(group.constituents[0].input.variant).toBe("low")
        }
        if (name === "automation") {
          expect(text(returned)).not.toContain("historical stalled observation")
          expect(text(returned)).toContain("historical pending decision observation")
          expect(text(returned)).toContain("historical converged observation")
          expect(text(returned)).toContain("not current measurements")
        }
        const modelInput = yield* Effect.promise(() => MessageV2.toModelMessages([returned], model))
        expect(modelInput).toHaveLength(1)
        expect(modelInput[0].role).toBe("user")
        expect(modelInput[0].content).toHaveLength(1)
        expect(JSON.stringify(modelInput)).not.toContain('"sessionQueue":')
        expect(JSON.stringify(modelInput)).not.toContain("prt_queue_")
        const marker = "SYNTHETIC_IGNORED_WITNESS_PROBE"
        const probe = {
          ...returned,
          parts: returned.parts.map((part) =>
            part.type === "text" && part.ignored ? { ...part, text: marker } : part,
          ),
        }
        const excluded = yield* Effect.promise(() => MessageV2.toModelMessages([probe], model))
        const control = yield* Effect.promise(() =>
          MessageV2.toModelMessages(
            [
              {
                ...probe,
                parts: probe.parts.map((part) =>
                  part.type === "text" && part.ignored ? { ...part, ignored: false } : part,
                ),
              },
            ],
            model,
          ),
        )
        expect(JSON.stringify(excluded)).not.toContain(marker)
        expect(JSON.stringify(control)).toContain(marker)
        yield* queue.consume(session.id)
        const afterConsume = yield* MessageV2.get({ sessionID: session.id, messageID: returned.info.id })
        expect(afterConsume).toEqual(returned)
        expect(yield* queue.list(session.id)).toEqual([])
        return {
          name,
          originalCalls: inputs,
          originals,
          encodedOriginalInputs: encoded,
          pendingBeforePromotion: pending,
          returnedMessage: returned,
          returnedAfterConsume: afterConsume,
          witnesses,
          modelInput,
          modelExclusionProbe: { marker, excluded, unignoredControl: control },
        }
      }),
    )
    const fixture = {
      provenance: {
        producerSHA: sha,
        generator: "packages/opencode/test/session/queue-dispatch.test.ts",
        test: "emits grouped consumer checkpoint through admission promotion and message reads",
        runtime: `Bun ${Bun.version}`,
        syntheticOnly: true,
        clockAndIDs: "Actual admission times and generated IDs are captured unchanged; reruns produce a new capture.",
      },
      scenarios,
    }
    if (capture)
      yield* Effect.promise(() =>
        Bun.write(
          new URL("../fixture/grouped-delivery/checkpoint.json", import.meta.url),
          JSON.stringify(fixture, null, 2) + "\n",
        ),
      )
  }),
)
