// Writes prune fixtures through the Session, queue and durable-event producers.
import { rename, rm } from "node:fs/promises"
import { DateTime, Effect, Fiber, Layer, Schema } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { SessionV2 } from "@opencode-ai/core/session"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionContextEpoch } from "@opencode-ai/core/session/context-epoch"
import { SessionInput } from "@opencode-ai/core/session/input"
import { SystemContext } from "@opencode-ai/core/system-context/index"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceStore } from "@/project/instance-store"
import { InstanceBootstrap } from "@/project/bootstrap"
import { Session } from "@/session/session"
import { SessionQueue } from "@/session/queue"
import { Todo } from "@/session/todo"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { AskEmit } from "@/ask/emit"
import { Question } from "@/question"

const [filename, directory, serialized] = process.argv.slice(2)
if (!filename || !directory || !serialized) throw new Error("usage: <db> <project directory> <operation JSON>")

type Create = {
  parentID?: string
  payloadBytes?: number
  eventCount?: number
  pendingV1?: boolean
  preparingV1?: boolean
  withdrawnV1?: boolean
  pendingV2?: boolean
  unfinished?: boolean
  runningTool?: boolean
  // A question answered under `--questions-command`, leaving its settled ledger row.
  ask?: boolean
  completeStorage?: boolean
}

const input = JSON.parse(serialized) as {
  create?: Create | Create[]
  bulkMessages?: { sessionID: string; count: number }
  touch?: string
  crossPart?: { sessionID: string; messageID: string }
  queue?: string
  queueAfterSignal?: {
    sessionID: string
    startFile: string
    readyFile: string
    completedFile?: string
    registerBeforeSignal?: boolean
    appendEvent?: boolean
    readinessWriteGate?: { createdFile: string; releaseFile: string }
  }
  preparing?: string
  pendingV2?: string
  holdSessionID?: string
  hold?: boolean
  holdAfterSignal?: { startFile: string; readyFile: string }
  readerAfterSignal?: { readyFile: string }
}

if (input.queueAfterSignal && !input.queueAfterSignal.registerBeforeSignal) {
  console.log("PRUNE_QUEUE_WAITING")
  const deadline = Date.now() + 20_000
  while (!(await Bun.file(input.queueAfterSignal.startFile).exists())) {
    if (Date.now() >= deadline) throw new Error("pending prompt producer never received its start signal")
    await Bun.sleep(5)
  }
}

if (input.holdAfterSignal) {
  const sqlite = await import("bun:sqlite")
  console.log("PRUNE_HOLDER_WAITING")
  const deadline = Date.now() + 20_000
  while (!(await Bun.file(input.holdAfterSignal.startFile).exists())) {
    if (Date.now() >= deadline) throw new Error("late database reader never received its start signal")
    await Bun.sleep(5)
  }
  const db = new sqlite.Database(filename, { readonly: true })
  db.query("SELECT count(*) FROM session").get()
  await Bun.write(input.holdAfterSignal.readyFile, "ready\n")
  console.log(`PRUNE_HOLDER_READY ${process.pid}`)
  await new Promise<void>((resolve) => {
    process.stdin.once("end", resolve)
    process.stdin.once("close", resolve)
    process.stdin.resume()
  })
  db.close()
  process.exit(0)
}

const produce = Effect.gen(function* () {
  const sessions = yield* Session.Service
  const question = yield* Question.Service
  const v2 = yield* SessionV2.Service
  const queue = yield* SessionQueue.Service
  const todo = yield* Todo.Service
  const events = yield* EventV2Bridge.Service
  const { db } = yield* Database.Service
  if (input.bulkMessages) {
    const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
    yield* Effect.promise(async () => {
      const bulk = new sqlite.Database(filename)
      try {
        const insert = bulk.query(
          "INSERT INTO message (id, session_id, admission_seq, time_created, time_updated, data) VALUES (?, ?, 0, ?, ?, ?)",
        )
        const start = Date.now() - input.bulkMessages!.count - 20_000
        bulk.transaction(() => {
          Array.from({ length: input.bulkMessages!.count }, (_, index) => index).forEach((index) => {
            const timestamp = start + index
            insert.run(
              `msg-large-history-${String(index).padStart(6, "0")}`,
              input.bulkMessages!.sessionID,
              timestamp,
              timestamp,
              JSON.stringify({
                role: "user",
                sessionID: input.bulkMessages!.sessionID,
                time: { created: timestamp, updated: timestamp },
              }),
            )
          })
        })()
      } finally {
        bulk.close()
      }
    })
  }
  if (input.readerAfterSignal) {
    const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
    const reader = new sqlite.Database(filename, { readonly: true })
    try {
      reader.exec("BEGIN")
      reader.query("SELECT count(*) FROM session").get()
      const readyFile = input.readerAfterSignal.readyFile
      yield* Effect.promise(async () => {
        await Bun.write(readyFile, "ready\n")
        console.log(`PRUNE_READER_READY ${process.pid}`)
        await new Promise<void>((resolve) => {
          process.stdin.once("end", resolve)
          process.stdin.once("close", resolve)
          process.stdin.resume()
        })
      })
    } finally {
      if (reader.inTransaction) reader.exec("ROLLBACK")
      reader.close()
    }
  }
  if (input.queueAfterSignal?.registerBeforeSignal) {
    console.log(`PRUNE_REGISTERED ${process.pid}`)
    yield* Effect.promise(async () => {
      const deadline = Date.now() + 20_000
      while (!(await Bun.file(input.queueAfterSignal!.startFile).exists())) {
        if (Date.now() >= deadline) throw new Error("registered prompt producer never received its start signal")
        await Bun.sleep(5)
      }
    })
  }
  const creates = Array.isArray(input.create) ? input.create : input.create ? [input.create] : []
  const created = yield* Effect.forEach(creates, (create) =>
    Effect.gen(function* () {
      const info = yield* sessions.create({
        title: "prune fixture",
        ...(create.parentID ? { parentID: create.parentID as SessionID } : {}),
      })
      const sessionID = info.id
      const text = "Important: preserve the customer recovery plan. " + "x".repeat(create.payloadBytes ?? 0)
      const userID = MessageID.ascending()
      const partID = PartID.ascending()
      yield* sessions.updateMessage({
        id: userID,
        sessionID,
        role: "user",
        time: { created: Date.now() },
        agent: "build",
        model: { providerID: "provider-a", modelID: "requested-model" },
      } as SessionV1.User)
      yield* sessions.updatePart({ id: partID, sessionID, messageID: userID, type: "text", text } as SessionV1.TextPart)

      const assistantID = MessageID.ascending()
      yield* sessions.updateMessage({
        id: assistantID,
        sessionID,
        role: "assistant",
        time: { created: Date.now(), ...(!create.unfinished ? { completed: Date.now() } : {}) },
        parentID: userID,
        modelID: "requested-model",
        providerID: "provider-a",
        mode: "build",
        agent: "build",
        path: { cwd: info.directory, root: info.directory },
        responseModelIDs: ["served-model"],
        cost: 0.25,
        tokens: { total: 32, input: 12, output: 17, reasoning: 3, cache: { read: 5, write: 7 } },
      } as SessionV1.Assistant)
      yield* sessions.updatePart({
        id: PartID.ascending(),
        sessionID,
        messageID: assistantID,
        type: "text",
        text,
      } as SessionV1.TextPart)

      if (create.runningTool) {
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: assistantID,
          type: "tool",
          tool: "prune-custody-fixture",
          callID: "prune-custody-fixture",
          state: { status: "running", input: {}, time: { start: Date.now() } },
        } as SessionV1.ToolPart)
      }

      const timestamp = yield* DateTime.now
      const assistantMessageID = SessionMessage.ID.create()
      yield* events.publish(SessionEvent.Step.Started, {
        sessionID: SessionV2.ID.make(sessionID),
        timestamp,
        assistantMessageID,
        agent: "build",
        model: { id: ModelV2.ID.make("requested-model"), providerID: ProviderV2.ID.make("provider-a") },
      })
      if (create.eventCount) {
        const callID = `prune-validation-${sessionID}`
        yield* events.publish(SessionEvent.Tool.Called, {
          sessionID: SessionV2.ID.make(sessionID),
          timestamp,
          assistantMessageID,
          callID,
          tool: "prune-validation-fixture",
          input: {},
          provider: { executed: false },
        })
        yield* Effect.forEach(
          Array.from({ length: create.eventCount }, (_, index) => index),
          (index) =>
            events.publish(SessionEvent.Tool.Progress, {
              sessionID: SessionV2.ID.make(sessionID),
              timestamp,
              assistantMessageID,
              callID,
              structured: { step: index },
              content: [],
            }),
          { concurrency: 1 },
        )
        yield* events.publish(SessionEvent.Tool.Success, {
          sessionID: SessionV2.ID.make(sessionID),
          timestamp,
          assistantMessageID,
          callID,
          structured: { steps: create.eventCount },
          content: [],
          provider: { executed: false },
        })
      }
      if (!create.unfinished) {
        yield* events.publish(SessionEvent.Step.Ended, {
          sessionID: SessionV2.ID.make(sessionID),
          timestamp,
          assistantMessageID,
          finish: "stop",
          cost: 0.25,
          tokens: { input: 12, output: 17, reasoning: 3, cache: { read: 5, write: 7 } },
        })
      }
      yield* events.publish(SessionEvent.ContextUpdated, {
        sessionID: SessionV2.ID.make(sessionID),
        messageID: SessionMessage.ID.create(),
        timestamp,
        text,
      })
      yield* todo.update({
        sessionID,
        todos: [{ content: "todo produced for prune fixture", status: "pending", priority: "high" }],
      })
      yield* SessionContextEpoch.initialize(
        db,
        Effect.succeed(
          SystemContext.make({
            key: Schema.decodeSync(SystemContext.Key)("prune/fixture"),
            codec: Schema.toCodecJson(Schema.String),
            load: Effect.succeed("prune fixture context"),
            baseline: String,
            update: (_previous, current) => current,
          }),
        ),
        SessionV2.ID.make(sessionID),
      )

      if (create.ask) {
        // The ask goes nowhere; the ledger records it the same.
        AskEmit.configure({ command: Bun.which("true")! })
        const asked = yield* question
          .ask({
            sessionID,
            questions: [{ question: "Prune this?", header: "Prune", options: [] }],
            tool: { messageID: assistantID, callID: "prune-ask" },
            producer: "question",
          })
          .pipe(Effect.forkScoped)
        const [pending] = yield* question.list().pipe(
          Effect.filterOrFail(
            (items) => items.length === 1,
            () => new Error("the question was not recorded"),
          ),
          Effect.retry({ times: 1000 }),
        )
        yield* question.reply({ requestID: pending!.id, answers: [["yes"]] })
        yield* Fiber.join(asked)
        AskEmit.configure(undefined)
      }
      if (create.pendingV1) {
        yield* queue.admit({
          sessionID,
          delivery: "queue",
          parts: [{ type: "text", text: "queued work" }],
        })
      }
      if (create.preparingV1) {
        yield* queue.request(
          { sessionID, delivery: "queue", parts: [{ type: "text", text: "preparing queued work" }] },
          undefined,
          true,
        )
      }
      if (create.withdrawnV1) {
        const requested = yield* queue.request({
          sessionID,
          messageID: MessageID.ascending(),
          requestID: Schema.decodeSync(SessionQueue.RequestID)("prune-withdrawn"),
          delivery: "queue",
          parts: [{ type: "text", text: "withdrawn work retained for history" }],
        })
        if (requested.kind === "admitted") yield* queue.withdraw(sessionID, requested.own.id)
      }
      if (create.completeStorage) {
        yield* queue.request({
          sessionID,
          messageID: MessageID.ascending(),
          requestID: Schema.decodeSync(SessionQueue.RequestID)("prune-promoted"),
          delivery: "queue",
          parts: [{ type: "text", text: "delivered history" }],
        })
        yield* queue.promote({
          sessionID,
          delivery: "queue",
          prepare: (input) => Effect.succeed(input.messageID),
          write: (id) =>
            sessions.updateMessage({
              id,
              sessionID,
              role: "user",
              time: { created: Date.now() },
              agent: "build",
              model: { providerID: "provider-a", modelID: "requested-model" },
            } as SessionV1.User),
          rejected: (cause) => Effect.failCause(cause),
        })
        yield* v2.prompt({
          sessionID: SessionV2.ID.make(sessionID),
          prompt: { text: "promoted durable history" },
          delivery: "queue",
          resume: false,
        })
        yield* SessionInput.promoteNextQueued(db, events, SessionV2.ID.make(sessionID))
      }
      if (create.pendingV2) {
        yield* v2.prompt({
          sessionID: SessionV2.ID.make(sessionID),
          prompt: { text: "durable pending work" },
          resume: false,
        })
      }
      return { id: sessionID, parentID: create.parentID ?? null, time: Date.now(), messageID: userID, partID }
    }),
  )
  const heldSessionID = created.at(-1)?.id as SessionID | undefined

  if (input.touch) {
    const timestamp = yield* DateTime.now
    yield* events.publish(SessionEvent.ContextUpdated, {
      sessionID: SessionV2.ID.make(input.touch),
      messageID: SessionMessage.ID.create(),
      timestamp,
      text: "Important: preserve the customer recovery plan.",
    })
  }

  if (input.queue) {
    yield* queue.admit({
      sessionID: input.queue as SessionID,
      delivery: "queue",
      parts: [{ type: "text", text: "queued work after inventory" }],
    })
  }

  if (input.queueAfterSignal) {
    yield* queue.admit({
      sessionID: input.queueAfterSignal.sessionID as SessionID,
      delivery: "queue",
      parts: [{ type: "text", text: "queued work after inventory" }],
    })
    if (input.queueAfterSignal.appendEvent) {
      const timestamp = yield* DateTime.now
      yield* events.publish(SessionEvent.ContextUpdated, {
        sessionID: SessionV2.ID.make(input.queueAfterSignal.sessionID),
        messageID: SessionMessage.ID.create(),
        timestamp,
        text: "online reclaim concurrent writer acknowledgment",
      })
    }
    if (input.queueAfterSignal.completedFile) {
      const completedFile = input.queueAfterSignal.completedFile
      yield* Effect.promise(() => Bun.write(completedFile, "committed\n"))
    }
  }

  if (input.preparing) {
    yield* queue.request(
      { sessionID: input.preparing as SessionID, delivery: "queue", parts: [{ type: "text", text: "preparing work" }] },
      undefined,
      true,
    )
  }

  if (input.pendingV2) {
    yield* v2.prompt({
      sessionID: SessionV2.ID.make(input.pendingV2),
      prompt: { text: "durable pending work after inventory" },
      resume: false,
    })
  }

  if (input.crossPart) {
    const sessionID = SessionID.make(input.crossPart.sessionID)
    yield* events.publish(SessionV1.Event.PartUpdated, {
      sessionID,
      part: {
        id: PartID.ascending(),
        sessionID,
        messageID: MessageID.make(input.crossPart.messageID),
        type: "text",
        text: "PartUpdated fixture spanning two Sessions",
      } as SessionV1.TextPart,
      time: Date.now(),
    })
  }

  if (input.hold && (heldSessionID ?? input.holdSessionID)) {
    const id = heldSessionID ?? (input.holdSessionID as SessionID)
    yield* queue.hold(id, "prune-fixture-review")
    console.log(`PRUNE_HOLD_READY ${JSON.stringify({ ready: true, heldSessionID: id })}`)
    yield* Effect.promise(() => new Promise<void>((resolve) => process.stdin.once("data", () => resolve())))
  }

  return { created, heldSessionID }
})

const layer = AppNodeBuilder.build(
  LayerNode.group([
    Session.node,
    Question.node,
    SessionQueue.node,
    SessionV2.node,
    SessionStore.node,
    Todo.node,
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
    [SessionExecution.node, SessionExecution.noopLayer],
  ],
)

const result = await Effect.runPromise(
  Effect.scoped(
    InstanceStore.Service.use((store) => store.provide({ directory }, produce)).pipe(Effect.provide(layer)),
  ),
).catch(async (error) => {
  if (input.queueAfterSignal) {
    await publishReadiness(
      input.queueAfterSignal.readyFile,
      { ready: false, error: error instanceof Error ? error.message : String(error) },
      input.queueAfterSignal.readinessWriteGate,
    )
  }
  throw error
})
if (input.queueAfterSignal)
  await publishReadiness(input.queueAfterSignal.readyFile, { ready: true }, input.queueAfterSignal.readinessWriteGate)
if (!input.hold) console.log(JSON.stringify(result))

async function publishReadiness(
  readyFile: string,
  result: { ready: boolean; error?: string },
  gate?: { createdFile: string; releaseFile: string },
) {
  const temporaryFile = `${readyFile}.${process.pid}.tmp`
  try {
    await Bun.write(temporaryFile, "")
    if (gate) {
      await Bun.write(gate.createdFile, "created\n")
      const deadline = Date.now() + 10_000
      while (!(await Bun.file(gate.releaseFile).exists())) {
        if (Date.now() >= deadline) throw new Error("readiness writer did not receive its release signal")
        await Bun.sleep(5)
      }
    }
    await Bun.write(temporaryFile, JSON.stringify(result))
    await rename(temporaryFile, readyFile)
  } catch (error) {
    await rm(temporaryFile, { force: true })
    throw error
  }
}
