import { afterAll, expect } from "bun:test"
import { randomUUID } from "node:crypto"
import { rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Scope, Struct } from "effect"
import { eq, sql } from "drizzle-orm"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import {
  SessionPromptQueueAliasTable,
  SessionPromptQueueSequenceTable,
  SessionPromptQueueTable,
} from "@opencode-ai/core/session/prompt-queue.sql"
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
import { TestInstance } from "../fixture/fixture"

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
  message.parts
    .flatMap((part) => {
      if (part.type !== "text" || !part.metadata?.sessionQueue) return []
      const group = Schema.decodeUnknownOption(SessionPromptQueue.Group)(part.metadata.sessionQueue)
      return Option.isSome(group) ? [group.value] : []
    })
    .toSorted((a, b) => b.constituents.length - a.constituents.length)[0]!
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
            ...(n === 1 ? { origin: "tui-composer" as const } : {}),
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

it.instance("queued TUI composer prompts form one ordered message and retain receipts around an injection", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const firstText = "A\nA keeps its newline"
    const lastText = "composer batch B text"
    const first = yield* queue.admit({
      sessionID: session.id,
      requestID: "composer-a",
      delivery: "queue",
      origin: "tui-composer",
      parts: [{ type: "text", text: firstText }],
    })
    const injectedText = check("between", "stalled")
    const injected = yield* queue.admit({
      sessionID: session.id,
      requestID: "injected-between",
      delivery: "queue",
      origin: "tui-composer",
      parts: [{ type: "text", text: injectedText }],
    })
    const second = yield* queue.admit({
      sessionID: session.id,
      requestID: "composer-b",
      delivery: "queue",
      origin: "tui-composer",
      parts: [{ type: "text", text: lastText }],
    })
    const promote = () =>
      queue.promote({
        sessionID: session.id,
        delivery: "queue",
        prepare,
        write: writer(sessions),
        rejected: () => Effect.void,
      })

    yield* promote()
    const combined = (yield* MessageV2.stream(session.id))[0]
    const group = metadata(combined)
    expect(text(combined)).toBe(`${firstText}\n${lastText}`)
    expect(group.kind).toBe("human")
    expect(group.constituents.map((item) => item.id)).toEqual([first.id, second.id])
    expect(group.constituents.map((item) => item.input.parts[0])).toEqual([
      { type: "text", text: firstText },
      { type: "text", text: lastText },
    ])
    expect(yield* queue.list(session.id)).toEqual([injected])
    for (const requestID of ["composer-a", "composer-b"]) {
      const receipt = yield* queue.receipt(session.id, requestID)
      expect(Option.isSome(receipt) ? receipt.value : undefined).toEqual({
        state: "delivered",
        messageID: combined.info.id,
      })
    }

    yield* promote()
    const injectedMessage = (yield* MessageV2.stream(session.id)).find((message) =>
      text(message).includes(injectedText),
    )
    expect(injectedMessage).toBeDefined()
    expect(metadata(injectedMessage!).kind).toBe("watcher.check")
    expect(text(injectedMessage!)).toContain(injectedText)
    expect(text(injectedMessage!)).not.toContain(firstText)
    expect(text(injectedMessage!)).not.toContain(lastText)
    const injectedReceipt = yield* queue.receipt(session.id, "injected-between")
    expect(Option.isSome(injectedReceipt) ? injectedReceipt.value : undefined).toEqual({
      state: "delivered",
      messageID: injectedMessage!.info.id,
    })
  }),
)

it.instance("a single queued TUI composer prompt keeps its own text and delivery", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    yield* queue.admit({
      sessionID: session.id,
      requestID: "composer-alone",
      delivery: "queue",
      origin: "tui-composer",
      parts: [{ type: "text", text: "one queued prompt" }],
    })
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    const message = (yield* MessageV2.stream(session.id))[0]
    expect(text(message)).toBe("one queued prompt")
    expect(metadata(message).kind).toBe("individual")
    expect(yield* queue.list(session.id)).toEqual([])
  }),
)

it.instance("duplicate and command-shaped human sends survive admission and consolidation for one batch", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const admit = (requestID: string, text: string) =>
      queue.admit({
        sessionID: session.id,
        requestID,
        delivery: "queue",
        origin: "tui-composer",
        parts: [{ type: "text", text }],
      })
    const originals = yield* Effect.forEach(
      [
        ["composer-a-1", "A"],
        ["composer-a-2", "A"],
        ["composer-command-1", "build API"],
        ["composer-command-2", "build API"],
      ] as const,
      ([requestID, text]) => admit(requestID, text),
    )
    expect(yield* queue.list(session.id)).toHaveLength(4)

    yield* queue.consolidate(session.id)
    expect(yield* queue.list(session.id)).toHaveLength(4)

    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    const message = (yield* MessageV2.stream(session.id))[0]
    const group = metadata(message)
    expect(text(message)).toBe("A\nA\nbuild API\nbuild API")
    expect(group.kind).toBe("human")
    expect(group.constituents.map((item) => item.id)).toEqual(originals.map((item) => item.id))
    for (const requestID of ["composer-a-1", "composer-a-2", "composer-command-1", "composer-command-2"]) {
      const receipt = yield* queue.receipt(session.id, requestID)
      expect(Option.isSome(receipt) ? receipt.value : undefined).toEqual({
        state: "delivered",
        messageID: message.info.id,
      })
    }
  }),
)

it.instance("human batching preserves synthetic context and typed-part provenance", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    yield* queue.admit({
      sessionID: session.id,
      requestID: "composer-context-a",
      delivery: "queue",
      origin: "tui-composer",
      parts: [
        { type: "text", text: "Selected editor context", synthetic: true },
        { type: "text", text: "A", metadata: { source: "typed-part-a" } },
      ],
    })
    yield* queue.admit({
      sessionID: session.id,
      requestID: "composer-context-b",
      delivery: "queue",
      origin: "tui-composer",
      parts: [{ type: "text", text: "B\ncontinued B", metadata: { source: "typed-part-b" } }],
    })
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })

    const message = (yield* MessageV2.stream(session.id))[0]
    const group = metadata(message)
    const visibleText = message.parts.filter(
      (part): part is Extract<SessionV1.Part, { type: "text" }> =>
        part.type === "text" && !part.synthetic && !part.ignored,
    )
    expect(visibleText.map((part) => part.text).join("")).toBe("A\nB\ncontinued B")
    expect(
      message.parts.some((part) => part.type === "text" && part.text === "Selected editor context" && part.synthetic),
    ).toBe(true)
    expect(group.kind).toBe("human")
    expect(group.constituents[0]!.input.parts).toMatchObject([
      { type: "text", text: "Selected editor context", synthetic: true },
      { type: "text", text: "A", metadata: { source: "typed-part-a" } },
    ])
    expect(visibleText[0]?.metadata).toMatchObject({ source: "typed-part-a", sessionQueue: { kind: "human" } })
    expect(visibleText.find((part) => part.text.startsWith("B"))?.metadata).toMatchObject({ source: "typed-part-b" })
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
          own: original.id,
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
      expect(yield* queue.list(session.id)).toEqual([])
      expect(Option.isSome(yield* queue.restore(session.id, original.id))).toBe(true)
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
      ...["duty-one", "duty-two"].map(
        (duty) =>
          `[fm-level:nudge:heartbeat]\x1f Heartbeat summary: ${heartbeat(duty, duty).split("\x1f ")[1].slice(0, 240)}… Full message: /scratch/${duty}.txt [fm-heartbeat-receipt:${duty}]`,
      ),
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
    expect(text(message)).toContain("Full message: /scratch/duty-one.txt")
    expect(text(message)).toContain("Full message: /scratch/duty-two.txt")
    expect(message.parts.find((part) => part.type === "text")?.ignored).not.toBe(true)
    expect(metadata(message).constituents).toHaveLength(6)
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

it.instance("interrupting message observer cannot lose committed parts or answer binding", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const events = yield* EventV2.Service
    const session = yield* sessions.create({})
    const original = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      messageID: MessageID.ascending(),
      parts: [{ type: "text", text: peer(900) }],
    })
    const seen: string[] = []
    const unsubscribe = yield* events.listen((event) => {
      if (event.type === SessionV1.Event.MessageUpdated.type) return Effect.interrupt
      if (event.type === SessionV1.Event.PartUpdated.type)
        return Effect.sync(() => {
          seen.push(Schema.decodeUnknownSync(Schema.Struct({ part: SessionV1.Part }))(event.data).part.id)
        })
      return Effect.void
    })
    const result = yield* queue
      .promote({
        sessionID: session.id,
        delivery: "queue",
        prepare,
        write: writer(sessions),
        rejected: () => Effect.void,
      })
      .pipe(Effect.exit)
    yield* unsubscribe
    expect(Exit.isFailure(result)).toBe(true)
    const [message] = yield* MessageV2.stream(session.id)
    expect(message.parts.length).toBeGreaterThan(1)
    expect(seen.toSorted()).toEqual(message.parts.map((part) => part.id).toSorted())
    yield* queue.answer(session.id, message.info.id, message)
    expect(yield* queue.reply(original.id)).toEqual(message)
  }),
)

it.instance("caller cancellation after commit cannot discard part notifications or binding", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const events = yield* EventV2.Service
    const session = yield* sessions.create({})
    const original = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: peer(901) }],
    })
    const notified = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const completed = yield* Deferred.make<void>()
    const seen: string[] = []
    const unsubscribe = yield* events.listen((event) => {
      if (event.type === SessionV1.Event.MessageUpdated.type)
        return Deferred.succeed(notified, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.asVoid)
      if (event.type === SessionV1.Event.PartUpdated.type)
        return Effect.sync(() => {
          seen.push(Schema.decodeUnknownSync(Schema.Struct({ part: SessionV1.Part }))(event.data).part.id)
        }).pipe(
          Effect.andThen(
            Effect.suspend(() =>
              seen.length === 2 ? Deferred.succeed(completed, undefined).pipe(Effect.asVoid) : Effect.void,
            ),
          ),
        )
      return Effect.void
    })
    const running = yield* queue
      .promote({
        sessionID: session.id,
        delivery: "queue",
        prepare,
        write: writer(sessions),
        rejected: () => Effect.void,
      })
      .pipe(Effect.forkChild)
    yield* Deferred.await(notified)
    const [committed] = yield* MessageV2.stream(session.id)
    yield* queue.answer(session.id, committed.info.id, committed)
    expect(yield* queue.reply(original.id)).toEqual(committed)
    const stopping = yield* Fiber.interrupt(running).pipe(Effect.forkChild)
    const ended = yield* Fiber.join(stopping).pipe(
      Effect.timeoutOption("1 second"),
      Effect.ensuring(Deferred.succeed(release, undefined)),
    )
    expect(Option.isSome(ended)).toBe(true)
    yield* Deferred.await(completed)
    yield* unsubscribe
    const [message] = yield* MessageV2.stream(session.id)
    expect(seen.toSorted()).toEqual(message.parts.map((part) => part.id).toSorted())
    yield* queue.answer(session.id, message.info.id, message)
    expect(yield* queue.reply(original.id)).toEqual(message)
  }),
)

it.instance("accepted changed transport aliases cannot be borrowed in pending delivered or consumed state", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    for (const state of ["pending", "delivered", "consumed"] as const) {
      const session = yield* sessions.create({})
      const originalInput = {
        sessionID: session.id,
        delivery: "queue" as const,
        messageID: MessageID.ascending(),
        parts: [{ type: "text" as const, text: peer(910) }],
      }
      const original = yield* queue.admit(originalInput)
      if (state !== "pending")
        yield* queue.promote({
          sessionID: session.id,
          delivery: "queue",
          prepare,
          write: writer(sessions),
          rejected: () => Effect.void,
        })
      if (state === "consumed") yield* queue.consume(session.id)
      const accepted = MessageID.ascending()
      expect((yield* queue.admit({ ...originalInput, messageID: accepted })).id).toBe(original.id)
      const conflict = yield* queue
        .admit({ ...originalInput, messageID: accepted, parts: [{ type: "text", text: peer(911) }] })
        .pipe(Effect.exit)
      expect(Exit.isFailure(conflict)).toBe(true)
      if (state === "pending") {
        yield* queue.promote({
          sessionID: session.id,
          delivery: "queue",
          prepare,
          write: writer(sessions),
          rejected: () => Effect.void,
        })
        yield* queue.consume(session.id)
        expect((yield* queue.admit({ ...originalInput, messageID: accepted })).id).toBe(original.id)
        expect(
          Exit.isFailure(
            yield* queue
              .admit({ ...originalInput, messageID: accepted, parts: [{ type: "text", text: peer(912) }] })
              .pipe(Effect.exit),
          ),
        ).toBe(true)
      }
    }
  }),
)

it.instance("exact retry preserves recorded answer and independently released callers", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const input = {
      sessionID: session.id,
      delivery: "queue" as const,
      parts: [{ type: "text" as const, text: peer(920) }],
    }
    const first = yield* queue.admit(input)
    const second = yield* queue.admit(input)
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    const [message] = yield* MessageV2.stream(session.id)
    yield* queue.consume(session.id)
    yield* queue.answer(session.id, message.info.id, message)
    const third = yield* queue.admit(input)
    yield* queue.forget(first.id)
    expect(yield* queue.reply(second.id)).toEqual(message)
    expect(yield* queue.reply(third.id)).toEqual(message)
    yield* queue.forget(second.id)
    expect(yield* queue.reply(third.id)).toEqual(message)
    yield* queue.forget(third.id)
    expect(yield* queue.reply(first.id)).toBeUndefined()
  }),
)

it.instance("malformed withdrawn input cannot poison unrelated peer admission but retains its transport claim", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const session = yield* sessions.create({})
    const messageID = MessageID.ascending()
    const old = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      messageID,
      parts: [{ type: "text", text: "editor intent" }],
    })
    yield* queue.withdraw(session.id, old.id)
    const malformed = { messageID, parts: [{ type: "corrupted" }] }
    yield* database.db.run(sql`UPDATE session_prompt_queue SET input=${JSON.stringify(malformed)} WHERE id=${old.id}`)
    yield* database.db
      .delete(SessionPromptQueueAliasTable)
      .where(eq(SessionPromptQueueAliasTable.item_id, old.id))
      .run()
    yield* database.db
      .update(SessionPromptQueueSequenceTable)
      .set({ aliases_indexed: false })
      .where(eq(SessionPromptQueueSequenceTable.session_id, session.id))
      .run()
    const good = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: peer(930) }],
    })
    expect(good.id).not.toBe(old.id)
    const retained = yield* database.db
      .select()
      .from(SessionPromptQueueTable)
      .where(eq(SessionPromptQueueTable.id, old.id))
      .get()
    expect(retained?.input as unknown).toEqual(malformed)
    expect(retained?.time_withdrawn).toBeGreaterThan(0)
    expect(
      Exit.isFailure(
        yield* queue
          .admit({ sessionID: session.id, messageID, delivery: "queue", parts: [{ type: "text", text: peer(931) }] })
          .pipe(Effect.exit),
      ),
    ).toBe(true)
  }),
)

it.instance("individual stable transport retry reconciles through normal consume", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    for (const parts of [
      [{ type: "text" as const, text: "ordinary request" }],
      [{ type: "text" as const, text: peer(940), synthetic: true }],
    ]) {
      const session = yield* sessions.create({})
      const input = { sessionID: session.id, delivery: "queue" as const, messageID: MessageID.ascending(), parts }
      const original = yield* queue.admit(input)
      yield* queue.promote({
        sessionID: session.id,
        delivery: "queue",
        prepare,
        write: writer(sessions),
        rejected: () => Effect.void,
      })
      yield* queue.consume(session.id)
      expect((yield* queue.admit(input)).id).toBe(original.id)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* MessageV2.stream(session.id)).toHaveLength(1)
    }
  }),
)

it.instance("historical marked noReply retry without custody is refused despite matching projection", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    for (const system of [undefined, "individual execution context"]) {
      const session = yield* sessions.create({})
      const input = {
        sessionID: session.id,
        delivery: "queue" as const,
        noReply: true,
        system,
        messageID: MessageID.ascending(),
        parts: [{ type: "text" as const, text: peer(950) }],
      }
      const prepared = yield* prepare(input)
      yield* writer(sessions)(prepared)
      for (const busy of [false, true]) {
        const retry = yield* queue
          .writeOrAdmit({
            admission: input,
            isBusy: Effect.succeed(busy),
            prepare: () => prepare(input),
            write: (value) => writer(sessions)(value).pipe(Effect.as(value)),
            discard: () => Effect.void,
          })
          .pipe(Effect.exit)
        expect(Exit.isFailure(retry)).toBe(true)
        if (!Exit.isFailure(retry)) throw new Error("unwitnessed projection was silently accepted")
        expect(Cause.squash(retry.cause)).toBeInstanceOf(SessionQueue.HistoricalIdentityError)
        expect(yield* queue.list(session.id)).toEqual([])
      }
      expect(yield* MessageV2.stream(session.id)).toHaveLength(1)
    }
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
    const scenarios = yield* Effect.forEach(
      ["peer", "automation", "heartbeat", "individual", "peerContinuation"] as const,
      (name) =>
        Effect.gen(function* () {
          const session = yield* sessions.create({ title: `Synthetic grouped checkpoint: ${name}` })
          const peerScenario = name === "peer" || name === "peerContinuation"
          const inputs: SessionQueue.AdmitInput[] = peerScenario
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
            : name === "automation"
              ? [
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
              : name === "heartbeat"
                ? ["one", "two"].map((duty) => ({
                    sessionID: session.id,
                    delivery: "queue" as const,
                    messageID: MessageID.make(`msg_checkpoint_heartbeat_${duty}`),
                    parts: [
                      {
                        type: "text" as const,
                        text: `[fm-level:nudge:heartbeat]\x1f Heartbeat summary: ${"Fleet heartbeat. Run one supervision cycle from live state, not memory: read the live fleet, backlog, and open work fresh; identify who is blocked only on firstmate, and take the highest-value in-scope action that advances convergence. Escalate destructive, irreversible, or security-sensitive decisions to the captain.".slice(0, 240)}… Full message: /scratch/duty-${duty}.txt [fm-heartbeat-receipt:${duty}]`,
                      },
                    ],
                  }))
                : [
                    {
                      sessionID: session.id,
                      delivery: "queue",
                      messageID: MessageID.make("msg_checkpoint_individual"),
                      model: {
                        providerID: ProviderV2.ID.make("test"),
                        modelID: ModelV2.ID.make("individual-model"),
                      },
                      variant: "high",
                      parts: [{ type: "text", text: "Synthetic individual original with stable transport identity" }],
                    },
                  ]
          if (name === "peerContinuation")
            inputs.push(
              {
                sessionID: session.id,
                delivery: "queue",
                parts: [{ type: "text", text: check("deferred", "historical pending observation") }],
              },
              {
                sessionID: session.id,
                delivery: "queue",
                parts: [{ type: "text", text: "Held human original remains pending" }],
              },
            )
          const originals = yield* Effect.forEach(inputs, (input) => queue.admit(input))
          const selected = name === "peerContinuation" ? originals.slice(0, 2) : originals
          const retryCalls = peerScenario
            ? [{ ...inputs[0], messageID: MessageID.make("msg_checkpoint_peer_retry_alias") }]
            : []
          yield* Effect.forEach(retryCalls, (input) => queue.admit(input))
          const pending = yield* queue.list(session.id)
          const preparedInputs: SessionQueue.PromotedInput[] = []
          yield* queue.promote({
            sessionID: session.id,
            delivery: "queue",
            peersOnly: name === "peerContinuation",
            prepare: (input) =>
              Effect.gen(function* () {
                if (name === "individual") preparedInputs.push(input)
                return yield* prepare(input)
              }),
            write: writer(sessions),
            rejected: () => Effect.void,
          })
          const latest = (yield* MessageV2.stream(session.id))[0]
          const returned = yield* MessageV2.get({ sessionID: session.id, messageID: latest.info.id })
          const group =
            name === "individual"
              ? Schema.decodeUnknownSync(SessionPromptQueue.Group)(
                  returned.parts.find(
                    (part): part is SessionV1.TextPart => part.type === "text" && !!part.metadata?.sessionQueue,
                  )?.metadata?.sessionQueue,
                )
              : metadata(returned)
          const afterPromotion = yield* queue.list(session.id)
          expect(afterPromotion).toEqual(name === "peerContinuation" ? originals.slice(2) : [])
          expect(group.constituents).toEqual(selected.map((item) => Struct.omit(item, ["origins", "requestID"])))
          expect(group.constituents.map((item) => item.time.created)).toEqual(selected.map((item) => item.time.created))
          const encoded = originals.map((item) => Schema.encodeSync(SessionPromptQueue.QueuedInput)(item.input))
          expect(
            group.constituents.map((item) => Schema.encodeSync(SessionPromptQueue.QueuedInput)(item.input)),
          ).toEqual(selected.map((item) => Schema.encodeSync(SessionPromptQueue.QueuedInput)(item.input)))
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
            selected.map((item) => item.id).sort(),
          )
          expect(
            witnesses
              .filter((part) => part.ignored)
              .every((part) => part.synthetic && part.messageID === returned.info.id),
          ).toBe(true)
          expect(witnesses.filter((part) => !part.ignored)).toHaveLength(1)
          expect(witnesses.filter((part) => part.ignored)).toHaveLength(
            name === "individual" ? 1 : name === "heartbeat" ? 2 : peerScenario ? 4 : 3,
          )
          if (peerScenario) {
            expect(
              witnesses.filter((part) => part.ignored && part.constituentIDs.includes(originals[0].id)),
            ).toHaveLength(3)
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
          if (name === "heartbeat") {
            expect(JSON.stringify(modelInput)).toContain("Full message: /scratch/duty-one.txt")
            expect(JSON.stringify(modelInput)).toContain("Full message: /scratch/duty-two.txt")
          }
          if (name === "individual") {
            expect(group.kind).toBe("individual")
            expect(text(returned)).toBe(inputs[0].parts[0].type === "text" ? inputs[0].parts[0].text : "")
            expect(preparedInputs).toHaveLength(1)
            expect(preparedInputs[0]?.group).toBeUndefined()
            expect(preparedInputs[0]?.model).toEqual({
              providerID: ProviderV2.ID.make("test"),
              modelID: ModelV2.ID.make("individual-model"),
            })
            expect(preparedInputs[0]?.variant).toBe("high")
            expect(group.constituents[0]?.input.model).toEqual(preparedInputs[0]?.model)
            expect(group.constituents[0]?.input.variant).toBe("high")
          }
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
          expect(yield* queue.list(session.id)).toEqual(afterPromotion)
          const afterConsumeRetryCalls = peerScenario
            ? [{ ...inputs[0], messageID: MessageID.make("msg_checkpoint_peer_late_alias") }]
            : []
          yield* Effect.forEach(afterConsumeRetryCalls, (input) => queue.admit(input))
          const afterRetry = yield* MessageV2.get({ sessionID: session.id, messageID: returned.info.id })
          const modelAfterRetry = yield* Effect.promise(() => MessageV2.toModelMessages([afterRetry], model))
          expect(modelAfterRetry).toEqual(modelInput)
          return {
            name,
            originalCalls: inputs,
            retryCalls,
            afterConsumeRetryCalls,
            returnedAfterRetry: afterRetry,
            modelAfterRetry,
            originals,
            encodedOriginalInputs: encoded,
            pendingBeforePromotion: pending,
            promotion: { delivery: "queue", peersOnly: name === "peerContinuation" },
            deliveredOriginalIDs: selected.map((item) => item.id),
            pendingAfterPromotion: afterPromotion,
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

it.instance("removed binary constituent is held without poisoning complete peers or automation", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const session = yield* sessions.create({})
    const instance = yield* TestInstance
    const removed = path.join(instance.directory, "removed-peer-obligation.bin")
    yield* Effect.promise(() => Bun.write(removed, new Uint8Array([1, 2, 3])))
    const bad = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [
        { type: "text", text: peer(970) },
        {
          type: "file",
          mime: "application/octet-stream",
          url: pathToFileURL(removed).href,
          filename: "removed.bin",
        },
      ],
    })
    yield* Effect.promise(() => rm(removed))
    const good = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: peer(971) }],
    })
    const notice = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: check("unrelated", "historical notice") }],
    })
    const rejected: string[] = []
    const options = {
      sessionID: session.id,
      delivery: "queue" as const,
      prepare: (input: SessionQueue.PromotedInput) =>
        Effect.forEach(
          input.parts.filter((part) => part.type === "file"),
          (part) => Effect.promise(() => Bun.file(new URL(part.url)).arrayBuffer()),
        ).pipe(Effect.andThen(prepare(input))),
      write: writer(sessions),
      rejected: (cause: Cause.Cause<unknown>) =>
        Effect.sync(() => {
          rejected.push(Cause.pretty(cause))
        }),
    }
    const result = yield* queue.promote({ ...options, own: good.id }).pipe(Effect.exit)
    expect(Exit.isSuccess(result)).toBe(true)
    expect(metadata((yield* MessageV2.stream(session.id))[0]).constituents.map((item) => item.id)).toEqual([good.id])
    const held = yield* database.db
      .select()
      .from(SessionPromptQueueTable)
      .where(eq(SessionPromptQueueTable.id, bad.id))
      .get()
    expect(held?.input).toEqual(Schema.encodeSync(SessionPromptQueue.QueuedInput)(bad.input))
    expect(held?.time_withdrawn).toBeGreaterThan(0)
    expect(held?.time_created).toBe(bad.time.created)
    expect(rejected.join(" ")).toContain(bad.id)
    yield* queue.consume(session.id)
    yield* queue.promote(options)
    expect(metadata((yield* MessageV2.stream(session.id))[0]).constituents.map((item) => item.id)).toEqual([notice.id])
    yield* queue.consume(session.id)
    expect(yield* queue.list(session.id)).toEqual([])
    yield* Effect.promise(() => Bun.write(removed, new Uint8Array([1, 2, 3])))
    const restored = yield* queue.restore(session.id, bad.id)
    expect(Option.getOrThrow(restored).seq).toBe(bad.seq)
    yield* queue.promote(options)
    expect(metadata((yield* MessageV2.stream(session.id))[0]).constituents.map((item) => item.id)).toEqual([bad.id])
    expect(yield* MessageV2.stream(session.id)).toHaveLength(3)
  }),
)

it.instance("deterministic constituent write rollback is isolated without committing or dropping its obligation", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const session = yield* sessions.create({})
    const bad = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: peer(980) }],
    })
    const good = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: peer(981) }],
    })
    const rejected: string[] = []
    const result = yield* queue
      .promote({
        sessionID: session.id,
        delivery: "queue",
        own: good.id,
        prepare,
        write: (message) =>
          metadata(message).constituents.some((item) => item.id === bad.id)
            ? sessions.updateMessage(message.info).pipe(Effect.andThen(Effect.die("deterministic bad part")))
            : writer(sessions)(message),
        rejected: (cause) =>
          Effect.sync(() => {
            rejected.push(Cause.pretty(cause))
          }),
      })
      .pipe(Effect.exit)
    expect(Exit.isSuccess(result)).toBe(true)
    const messages = yield* MessageV2.stream(session.id)
    expect(messages).toHaveLength(1)
    expect(metadata(messages[0]).constituents.map((item) => item.id)).toEqual([good.id])
    const held = yield* database.db
      .select()
      .from(SessionPromptQueueTable)
      .where(eq(SessionPromptQueueTable.id, bad.id))
      .get()
    expect(held?.time_withdrawn).toBeGreaterThan(0)
    expect(held?.time_promoted).toBeNull()
    expect(held?.message_id).toBeNull()
    expect(rejected.join(" ")).toContain(bad.id)
  }),
)

it.instance("admission scope release and repeated forget cannot steal a different caller answer", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const oneScope = yield* Scope.make()
    const twoScope = yield* Scope.make()
    const input = {
      sessionID: session.id,
      delivery: "queue" as const,
      parts: [{ type: "text" as const, text: peer(990) }],
    }
    const first = yield* queue.admit(input).pipe(Effect.provideService(Scope.Scope, oneScope))
    const second = yield* queue.admit(input).pipe(Effect.provideService(Scope.Scope, twoScope))
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    const [message] = yield* MessageV2.stream(session.id)
    yield* queue.answer(session.id, message.info.id, message)
    yield* Scope.close(oneScope, Exit.void)
    yield* queue.forget(first.id).pipe(Effect.provideService(Scope.Scope, oneScope))
    yield* queue.forget(first.id).pipe(Effect.provideService(Scope.Scope, oneScope))
    expect(yield* queue.reply(second.id)).toEqual(message)
    yield* Scope.close(twoScope, Exit.void)
    expect(yield* queue.reply(first.id)).toBeUndefined()
  }),
)

it.instance("accepted alias binds answer ownership before a held committed listener", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const events = yield* EventV2.Service
    for (const state of ["delivered", "consumed"] as const) {
      for (const route of ["admit", "writeOrAdmit"] as const) {
        const session = yield* sessions.create({})
        const firstScope = yield* Effect.acquireRelease(Scope.make(), (scope) => Scope.close(scope, Exit.void))
        const retryScope = yield* Effect.acquireRelease(Scope.make(), (scope) => Scope.close(scope, Exit.void))
        const input = {
          sessionID: session.id,
          delivery: "queue" as const,
          messageID: MessageID.ascending(),
          parts: [{ type: "text" as const, text: peer(995) }],
        }
        const original = yield* queue.admit(input).pipe(Effect.provideService(Scope.Scope, firstScope))
        yield* queue.promote({
          sessionID: session.id,
          delivery: "queue",
          prepare,
          write: writer(sessions),
          rejected: () => Effect.void,
        })
        const [message] = yield* MessageV2.stream(session.id)
        if (state === "consumed") yield* queue.consume(session.id)
        yield* queue.answer(session.id, message.info.id, message)
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
        const unsubscribe = yield* events.listen((event) =>
          event.type === SessionV1.Event.PartUpdated.type
            ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.asVoid)
            : Effect.void,
        )
        const retryInput = { ...input, messageID: MessageID.ascending() }
        const retry = yield* (
          route === "admit"
            ? queue.admit(retryInput)
            : queue
                .writeOrAdmit({
                  admission: retryInput,
                  isBusy: Effect.succeed(false),
                  prepare: () => Effect.die("witnessed retry must not prepare another original"),
                  write: () => Effect.die("witnessed retry must not write another original"),
                  discard: () => Effect.die("witnessed retry must not discard its original"),
                })
                .pipe(
                  Effect.map((result) => {
                    if (result.kind !== "queued") throw new Error("expected witnessed original")
                    return result.own
                  }),
                )
        ).pipe(Effect.provideService(Scope.Scope, retryScope), Effect.forkChild)
        yield* Deferred.await(entered)
        yield* Scope.close(firstScope, Exit.void)
        expect(yield* queue.reply(original.id)).toEqual(message)
        yield* Deferred.succeed(release, undefined)
        expect((yield* Fiber.join(retry)).id).toBe(original.id)
        expect(yield* queue.reply(original.id)).toEqual(message)
        yield* unsubscribe
        yield* Scope.close(retryScope, Exit.void)
        expect(yield* queue.reply(original.id)).toBeUndefined()
      }
    }
  }),
)

it.instance("accepted alias ownership respects SQL rollback and cancellation during publication", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const events = yield* EventV2.Service
    const database = yield* Database.Service
    for (const boundary of ["rollback", "cancel"] as const) {
      const session = yield* sessions.create({})
      const firstScope = yield* Effect.acquireRelease(Scope.make(), (scope) => Scope.close(scope, Exit.void))
      const input = {
        sessionID: session.id,
        delivery: "queue" as const,
        messageID: MessageID.ascending(),
        parts: [{ type: "text" as const, text: peer(996) }],
      }
      const original = yield* queue.admit(input).pipe(Effect.provideService(Scope.Scope, firstScope))
      yield* queue.promote({
        sessionID: session.id,
        delivery: "queue",
        prepare,
        write: writer(sessions),
        rejected: () => Effect.void,
      })
      const [message] = yield* MessageV2.stream(session.id)
      yield* queue.consume(session.id)
      yield* queue.answer(session.id, message.info.id, message)
      const retryInput = { ...input, messageID: MessageID.ascending() }
      if (boundary === "rollback") {
        yield* database.db.run(
          sql`CREATE TRIGGER fail_alias_insert BEFORE INSERT ON part BEGIN SELECT RAISE(ABORT,'controlled alias rollback'); END`,
        )
        const attempt = yield* queue
          .admit(retryInput)
          .pipe(
            Effect.scoped,
            Effect.exit,
            Effect.ensuring(database.db.run(sql`DROP TRIGGER fail_alias_insert`).pipe(Effect.orDie)),
          )
        expect(Exit.isFailure(attempt)).toBe(true)
        expect((yield* MessageV2.stream(session.id))[0].parts).toEqual(message.parts)
      }
      if (boundary === "cancel") {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const completed = yield* Deferred.make<void>()
        yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
        const unsubscribe = yield* events.listen((event) =>
          event.type === SessionV1.Event.PartUpdated.type
            ? Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(Deferred.succeed(completed, undefined)),
                Effect.asVoid,
              )
            : Effect.void,
        )
        const retry = yield* queue.admit(retryInput).pipe(Effect.scoped, Effect.forkChild)
        yield* Deferred.await(entered)
        const stopped = yield* Fiber.interrupt(retry).pipe(Effect.timeoutOption("1 second"))
        expect(Option.isSome(stopped)).toBe(true)
        expect(yield* queue.reply(original.id)).toEqual(message)
        yield* Deferred.succeed(release, undefined)
        yield* Deferred.await(completed)
        yield* unsubscribe
      }
      yield* Scope.close(firstScope, Exit.void)
      expect(yield* queue.reply(original.id)).toBeUndefined()
    }
  }),
)

it.instance("new accepted alias during preparation invalidates reservation and commits its witness once", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const input = {
      sessionID: session.id,
      delivery: "queue" as const,
      messageID: MessageID.ascending(),
      parts: [{ type: "text" as const, text: peer(991) }],
    }
    const original = yield* queue.admit(input)
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    let preparations = 0
    const promoting = yield* queue
      .promote({
        sessionID: session.id,
        delivery: "queue",
        prepare: (value) =>
          Effect.gen(function* () {
            preparations++
            if (preparations === 1) {
              yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(release)
            }
            return yield* prepare(value)
          }),
        write: writer(sessions),
        rejected: () => Effect.void,
      })
      .pipe(Effect.forkChild)
    yield* Deferred.await(entered)
    const alias = { ...input, messageID: MessageID.ascending() }
    expect((yield* queue.admit(alias)).id).toBe(original.id)
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(promoting)
    expect(preparations).toBe(2)
    expect(yield* MessageV2.stream(session.id)).toHaveLength(1)
    yield* queue.consume(session.id)
    expect((yield* queue.admit(alias)).id).toBe(original.id)
    expect(
      Exit.isFailure(yield* queue.admit({ ...alias, parts: [{ type: "text", text: peer(992) }] }).pipe(Effect.exit)),
    ).toBe(true)
  }),
)

it.instance("already projected retained original transfers identity before ordinary consume", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const session = yield* sessions.create({})
    const input = {
      sessionID: session.id,
      delivery: "queue" as const,
      messageID: MessageID.ascending(),
      parts: [{ type: "text" as const, text: peer(993) }],
    }
    const original = yield* queue.admit(input)
    const legacy = yield* prepare(input)
    yield* writer(sessions)(legacy)
    yield* database.db
      .update(SessionPromptQueueTable)
      .set({ message_id: legacy.info.id })
      .where(eq(SessionPromptQueueTable.id, original.id))
      .run()
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    yield* queue.consume(session.id)
    expect((yield* queue.admit(input)).id).toBe(original.id)
    expect(yield* MessageV2.stream(session.id)).toHaveLength(1)
    expect(yield* queue.list(session.id)).toEqual([])
  }),
)

it.instance("historical projected stable IDs without originals are refused without inventing custody", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    for (const system of [undefined, "native excluded execution controls"]) {
      const session = yield* sessions.create({})
      const input = {
        sessionID: session.id,
        messageID: MessageID.ascending(),
        system,
        delivery: "queue" as const,
        parts: [{ type: "text" as const, text: "pre-upgrade original native intent" }],
      }
      const projected = yield* prepare(input)
      yield* writer(sessions)(projected)
      const before = yield* MessageV2.stream(session.id)
      for (const retry of [input, { ...input, parts: [{ type: "text" as const, text: "conflicting intent" }] }]) {
        const exit = yield* queue.admit(retry).pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
        expect(yield* queue.list(session.id)).toEqual([])
        expect(yield* MessageV2.stream(session.id)).toEqual(before)
      }
      const other = yield* sessions.create({})
      expect(Exit.isFailure(yield* queue.admit({ ...input, sessionID: other.id }).pipe(Effect.exit))).toBe(true)
      expect(yield* queue.list(other.id)).toEqual([])
    }
  }),
)

it.instance("peer-only continuation promotion preserves pending compaction custody and deferred inputs", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const human = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: "explicit human hold" }],
    })
    const automation = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: check("held", "historical observation") }],
    })
    const original = yield* queue.admit({
      sessionID: session.id,
      delivery: "queue",
      parts: [{ type: "text", text: peer(997) }],
    })
    const root = yield* prepare({
      sessionID: session.id,
      messageID: MessageID.ascending(),
      parts: [{ type: "text", text: "original compaction input" }],
    })
    yield* writer(sessions)(root)
    yield* sessions.updatePart({
      id: PartID.ascending(),
      sessionID: session.id,
      messageID: root.info.id,
      type: "compaction",
      auto: false,
    })
    const promote = () =>
      queue.promote({
        sessionID: session.id,
        delivery: "queue",
        peersOnly: true,
        prepare,
        write: writer(sessions),
        rejected: () => Effect.void,
      })
    expect(yield* promote()).toBe(false)
    expect(yield* queue.list(session.id)).toEqual([human, automation, original])
    // A terminal summary clears pending compaction eligibility through the existing history owner.
    yield* sessions.updateMessage({
      id: MessageID.ascending(),
      sessionID: session.id,
      role: "assistant",
      parentID: root.info.id,
      agent: "build",
      mode: "build",
      modelID: ModelV2.ID.make("test"),
      providerID: ProviderV2.ID.make("test"),
      path: { cwd: "/scratch", root: "/scratch" },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      time: { created: Date.now(), completed: Date.now() },
      finish: "stop",
      summary: true,
    })
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
    const running = yield* queue
      .whileCompacting(session.id, Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))))
      .pipe(Effect.forkChild)
    yield* Deferred.await(entered)
    expect(yield* promote()).toBe(false)
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(running)
    expect(yield* promote()).toBe(true)
    expect(metadata((yield* MessageV2.stream(session.id))[0]).constituents).toEqual([
      Struct.omit(original, ["origins", "requestID"]),
    ])
    expect(yield* queue.list(session.id)).toEqual([human, automation])
  }),
)

it.instance("identity preflight keeps represented custody read-only and rejects contradictory owners", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const session = yield* sessions.create({})
    const input = {
      sessionID: session.id,
      messageID: MessageID.ascending(),
      delivery: "queue" as const,
      parts: [{ type: "text" as const, text: "represented native input" }],
    }
    const original = yield* queue.admit(input)
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    const before = yield* MessageV2.stream(session.id)
    const retained = yield* database.db
      .select()
      .from(SessionPromptQueueTable)
      .where(eq(SessionPromptQueueTable.id, original.id))
      .get()
    if (!retained) throw new Error("expected retained original")
    yield* queue.assertIdentity(session.id, input.messageID)
    expect(yield* MessageV2.stream(session.id)).toEqual(before)
    yield* queue.consume(session.id)
    yield* queue.assertIdentity(session.id, input.messageID)
    expect(yield* MessageV2.stream(session.id)).toEqual(before)
    expect(yield* queue.list(session.id)).toEqual([])
    // A second durable claim must not reach command expansion, even if one ignored witness remains valid.
    const contradictory = SessionQueue.ItemID.create()
    yield* database.db
      .insert(SessionPromptQueueTable)
      .values({ ...retained, id: contradictory, seq: retained.seq + 1, time_promoted: null, message_id: null })
      .run()
    yield* database.db
      .insert(SessionPromptQueueAliasTable)
      .values({ id: before[0].parts.find((part) => part.type === "text" && part.ignored)!.id, item_id: contradictory })
      .run()
    const exit = yield* queue.assertIdentity(session.id, input.messageID).pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (!Exit.isFailure(exit)) throw new Error("contradictory identity was accepted")
    expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionQueue.HistoricalIdentityError)
    expect((yield* queue.list(session.id))[0].id).toBe(contradictory)
    expect(yield* MessageV2.stream(session.id)).toEqual(before)
  }),
)

it.instance("round5 peer aliases cannot borrow bare projected IDs in pending promoted or consumed custody", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    for (const custody of ["pending", "promoted", "consumed"] as const) {
      for (const foreign of [false, true]) {
        const session = yield* sessions.create({})
        const input = {
          sessionID: session.id,
          messageID: MessageID.ascending(),
          delivery: "queue" as const,
          parts: [{ type: "text" as const, text: peer(987) }],
        }
        const original = yield* queue.admit(input)
        if (custody !== "pending")
          yield* queue.promote({
            sessionID: session.id,
            delivery: "queue",
            prepare,
            write: writer(sessions),
            rejected: () => Effect.void,
          })
        if (custody === "consumed") yield* queue.consume(session.id)
        const other = foreign ? yield* sessions.create({}) : session
        const bare = yield* sessions.updateMessage({
          id: MessageID.ascending(),
          sessionID: other.id,
          role: "user",
          agent: "build",
          model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
          time: { created: 123 },
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: other.id,
          messageID: bare.id,
          type: "text",
          text: "unrelated bare owner",
        })
        const before = yield* sessions.messages({ sessionID: session.id })
        const bareBefore = yield* MessageV2.get({ sessionID: other.id, messageID: bare.id })
        const aliases = yield* database.db
          .select()
          .from(SessionPromptQueueAliasTable)
          .where(eq(SessionPromptQueueAliasTable.item_id, original.id))
          .all()
          .pipe(Effect.orDie)
        const retry = { ...input, messageID: bare.id }
        const admitted = yield* Effect.exit(queue.admit(retry))
        const direct = yield* Effect.exit(
          queue.writeOrAdmit({
            admission: { ...retry, noReply: true },
            isBusy: Effect.succeed(false),
            prepare: () => Effect.die(new Error("borrowed alias reached preparation")),
            write: () => Effect.die(new Error("borrowed alias reached write")),
            discard: () => Effect.void,
          }),
        )
        expect(Exit.isFailure(admitted)).toBe(true)
        expect(Exit.isFailure(direct)).toBe(true)
        expect(yield* sessions.messages({ sessionID: session.id })).toEqual(before)
        expect(yield* MessageV2.get({ sessionID: other.id, messageID: bare.id })).toEqual(bareBefore)
        expect(
          yield* database.db
            .select()
            .from(SessionPromptQueueAliasTable)
            .where(eq(SessionPromptQueueAliasTable.item_id, original.id))
            .all()
            .pipe(Effect.orDie),
        ).toEqual(aliases)
      }
    }
  }),
)

it.instance("round6 in-flight executable identity refuses competing entries before their preparation", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const input = {
      sessionID: session.id,
      messageID: MessageID.ascending(),
      delivery: "queue" as const,
      parts: [{ type: "text" as const, text: "original executable input" }],
    }
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const first = yield* queue
      .prepareExecutable({
        ...input,
        prepare: () =>
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.as(input)),
      })
      .pipe(Effect.forkChild)
    yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
    yield* Deferred.await(entered)
    const forbidden = () => Effect.die(new Error("competing input reached preparation"))
    const exits = yield* Effect.all(
      [
        queue.prepareExecutable({ ...input, prepare: forbidden }),
        queue.admit(input),
        queue.reconcile(input),
        queue.assertFreshIdentity(session.id, input.messageID),
        queue.writeDirect({ admission: { ...input, noReply: true }, prepare: forbidden, write: forbidden }),
        queue.writeOrAdmit({
          admission: { ...input, noReply: true },
          isBusy: Effect.succeed(false),
          prepare: forbidden,
          write: forbidden,
          discard: () => Effect.void,
        }),
      ].map((effect) =>
        effect.pipe(
          Effect.timeoutOrElse({
            duration: "2 seconds",
            orElse: () => Effect.die(new Error("competing identity waited on preparation")),
          }),
          Effect.exit,
        ),
      ),
      { concurrency: "unbounded" },
    )
    yield* Deferred.succeed(release, undefined)
    const completed = yield* Fiber.await(first)
    exits.forEach((exit) => {
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionQueue.HistoricalIdentityError)
    })
    expect(Exit.isSuccess(completed)).toBe(true)
    if (Exit.isSuccess(completed))
      expect((yield* queue.list(session.id)).map((item) => item.id)).toEqual([completed.value.own.id])
    expect(yield* sessions.messages({ sessionID: session.id })).toEqual([])
  }),
)

it.instance("round6 executable reservation is released after failed or cancelled preparation settles", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const input = {
      sessionID: session.id,
      messageID: MessageID.ascending(),
      parts: [{ type: "text" as const, text: "failed preparation has no admitted original" }],
    }
    const failure = yield* Effect.exit(
      queue.prepareExecutable({ ...input, prepare: () => Effect.fail("preparation failed") }),
    )
    expect(Exit.isFailure(failure)).toBe(true)
    if (Exit.isFailure(failure)) expect(Cause.squash(failure.cause)).toBe("preparation failed")
    yield* queue.assertFreshIdentity(session.id, input.messageID)
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const first = yield* queue
      .prepareExecutable({
        ...input,
        prepare: () =>
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.as(input)),
      })
      .pipe(Effect.forkChild)
    yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
    yield* Deferred.await(entered)
    const stopping = yield* Fiber.interrupt(first).pipe(Effect.forkChild({ startImmediately: true }))
    yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
    const blocked = yield* Effect.exit(queue.assertFreshIdentity(session.id, input.messageID))
    const held = first.pollUnsafe()
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(stopping)
    const interrupted = yield* Fiber.await(first)
    expect(Exit.isFailure(blocked)).toBe(true)
    if (Exit.isFailure(blocked))
      expect(Cause.squash(blocked.cause)).toBeInstanceOf(SessionQueue.HistoricalIdentityError)
    expect(held).toBeUndefined()
    expect(Exit.isFailure(interrupted) && Cause.hasInterrupts(interrupted.cause)).toBe(true)
    expect(yield* queue.list(session.id)).toEqual([])
    yield* queue.assertFreshIdentity(session.id, input.messageID)
    const admitted = yield* queue.prepareExecutable({ ...input, prepare: () => Effect.succeed(input) })
    expect(admitted.input).toEqual(input)
    expect((yield* queue.list(session.id)).map((item) => item.id)).toEqual([admitted.own.id])
  }),
)

it.instance("receipt peer carriers preserve every original and bound delivery by ten constituents", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const inputs = Array.from({ length: 11 }, (_, index) => ({
      sessionID: session.id,
      delivery: "queue" as const,
      requestID: `p1791809000-${index + 1}-abcd`,
      messageID: MessageID.ascending(),
      parts: [{ type: "text" as const, text: peer(1).replace("p1791809000-1-abcd", `p1791809000-${index + 1}-abcd`) }],
    }))
    const admitted = yield* Effect.forEach(inputs, (input) => queue.request(input))
    expect(yield* queue.list(session.id)).toHaveLength(2)
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
    expect(metadata(first).constituents.map((item) => item.input)).toEqual(
      inputs.slice(0, 10).map((input) => Struct.omit(input, ["sessionID", "delivery", "requestID"])),
    )
    inputs.slice(0, 10).forEach((input) => expect(text(first)).toContain(input.parts[0].text))
    expect(yield* queue.list(session.id)).toHaveLength(1)
    expect(Option.getOrThrow(yield* queue.receipt(session.id, inputs[0].requestID))).toEqual({
      state: "merged",
      into: inputs[9].requestID,
      messageID: first.info.id,
    })
    yield* queue.consume(session.id)
    const original = yield* queue.reconcile({ ...inputs[0], requestID: undefined })
    expect(original?.kind).toBe("queued")
    if (original?.kind === "queued" && admitted[0].kind === "admitted") expect(original.own.id).toBe(admitted[0].own.id)
    yield* promote()
    const messages = yield* MessageV2.stream(session.id)
    expect(messages).toHaveLength(2)
    const overflow = messages.find((message) => message.info.id !== first.info.id)!
    expect(metadata(overflow).constituents).toHaveLength(1)
    expect(text(overflow)).toContain(inputs[10].parts[0].text)
    expect(Option.getOrThrow(yield* queue.receipt(session.id, inputs[10].requestID))).toEqual({
      state: "delivered",
      messageID: overflow.info.id,
    })
  }),
)

it.instance("a receipt retry cannot borrow a foreign projected transport identity", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const input = {
      sessionID: session.id,
      delivery: "queue" as const,
      requestID: "peer-receipt",
      parts: [{ type: "text" as const, text: peer(1100) }],
    }
    yield* queue.request(input)
    const other = yield* sessions.create({})
    const foreign = yield* prepare({
      sessionID: other.id,
      messageID: MessageID.ascending(),
      parts: [{ type: "text", text: "other owner" }],
    })
    yield* writer(sessions)(foreign)
    const retry = yield* queue.request({ ...input, messageID: foreign.info.id }).pipe(Effect.exit)
    expect(Exit.isFailure(retry)).toBe(true)
    if (Exit.isFailure(retry)) expect(Cause.pretty(retry.cause)).toContain("belongs to another projected message")
    expect(yield* queue.list(session.id)).toHaveLength(1)
    expect(Option.getOrThrow(yield* queue.receipt(session.id, input.requestID)).state).toBe("pending")
  }),
)

it.instance("receipt command consolidation retains each stable-ID original after consume", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const inputs = ["run lint", "run lint --fix"].map((text, index) => ({
      sessionID: session.id,
      delivery: "queue" as const,
      requestID: `lint-${index}`,
      messageID: MessageID.ascending(),
      parts: [{ type: "text" as const, text }],
    }))
    const admitted = yield* Effect.forEach(inputs, (input) => queue.request(input))
    expect(yield* queue.list(session.id)).toHaveLength(1)
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    const message = (yield* MessageV2.stream(session.id))[0]
    expect(text(message)).toBe("run lint:\n1. run lint\n2. --fix")
    yield* queue.consume(session.id)
    const retry = yield* queue.reconcile({ ...inputs[1], requestID: undefined })
    expect(retry?.kind).toBe("queued")
    if (retry?.kind === "queued" && admitted[1].kind === "admitted") expect(retry.own.id).toBe(admitted[1].own.id)
    expect(yield* queue.list(session.id)).toEqual([])
    expect(Option.getOrThrow(yield* queue.receipt(session.id, inputs[1].requestID))).toEqual({
      state: "merged",
      into: inputs[0].requestID,
      messageID: message.info.id,
    })
  }),
)

it.instance("a new receipt name on an exact stable-ID retry follows the retained original", () =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const input = {
      sessionID: session.id,
      delivery: "queue" as const,
      requestID: "original",
      messageID: MessageID.ascending(),
      parts: [{ type: "text" as const, text: "held original" }],
    }
    const original = yield* queue.request(input)
    const retried = yield* queue.reconcile({ ...input, requestID: "alias" })
    expect(retried?.kind).toBe("queued")
    if (original.kind === "admitted" && retried?.kind === "queued") expect(retried.own.id).toBe(original.own.id)
    expect(yield* queue.list(session.id)).toHaveLength(1)
    expect(Option.getOrThrow(yield* queue.receipt(session.id, "alias"))).toEqual({ state: "merged", into: "original" })
    yield* queue.promote({
      sessionID: session.id,
      delivery: "queue",
      prepare,
      write: writer(sessions),
      rejected: () => Effect.void,
    })
    const message = (yield* MessageV2.stream(session.id))[0]
    expect(Option.getOrThrow(yield* queue.receipt(session.id, "alias"))).toEqual({
      state: "merged",
      into: "original",
      messageID: message.info.id,
    })
  }),
)
