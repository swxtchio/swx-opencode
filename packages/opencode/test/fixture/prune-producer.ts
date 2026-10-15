// Writes prune fixtures through the Session, queue and durable-event producers.
import { DateTime, Effect, Layer, Schema } from "effect"
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
import { SystemContext } from "@opencode-ai/core/system-context/index"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceStore } from "@/project/instance-store"
import { InstanceBootstrap } from "@/project/bootstrap"
import { Session } from "@/session/session"
import { SessionQueue } from "@/session/queue"
import { Todo } from "@/session/todo"
import { MessageID, PartID, type SessionID } from "@/session/schema"

const [filename, directory, serialized] = process.argv.slice(2)
if (!filename || !directory || !serialized) throw new Error("usage: <db> <project directory> <operation JSON>")

const input = JSON.parse(serialized) as {
  create?: {
    parentID?: string
    payloadBytes?: number
    pendingV1?: boolean
    preparingV1?: boolean
    pendingV2?: boolean
    unfinished?: boolean
  }
  touch?: string
  queue?: string
  preparing?: string
  pendingV2?: string
  holdSessionID?: string
  hold?: boolean
}

const produce = Effect.gen(function* () {
  const sessions = yield* Session.Service
  const v2 = yield* SessionV2.Service
  const queue = yield* SessionQueue.Service
  const todo = yield* Todo.Service
  const events = yield* EventV2Bridge.Service
  const { db } = yield* Database.Service
  const created = [] as { id: string; parentID: string | null; time: number; messageID: string; partID: string }[]
  let heldSessionID: SessionID | undefined

  if (input.create) {
    const info = yield* sessions.create({
      title: "prune fixture",
      ...(input.create.parentID ? { parentID: input.create.parentID as SessionID } : {}),
    })
    const sessionID = info.id
    heldSessionID = sessionID
    const text = "Important: preserve the customer recovery plan. " + "x".repeat(input.create.payloadBytes ?? 0)
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
      time: { created: Date.now(), ...(!input.create.unfinished ? { completed: Date.now() } : {}) },
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

    const timestamp = yield* DateTime.now
    const assistantMessageID = SessionMessage.ID.create()
    yield* events.publish(SessionEvent.Step.Started, {
      sessionID: SessionV2.ID.make(sessionID),
      timestamp,
      assistantMessageID,
      agent: "build",
      model: { id: ModelV2.ID.make("requested-model"), providerID: ProviderV2.ID.make("provider-a") },
    })
    if (!input.create.unfinished) {
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

    if (input.create.pendingV1) {
      yield* queue.admit({
        sessionID,
        delivery: "queue",
        parts: [{ type: "text", text: "queued work" }],
      })
    }
    if (input.create.preparingV1) {
      yield* queue.request(
        { sessionID, delivery: "queue", parts: [{ type: "text", text: "preparing queued work" }] },
        undefined,
        true,
      )
    }
    if (input.create.pendingV2) {
      yield* v2.prompt({ sessionID: SessionV2.ID.make(sessionID), prompt: { text: "durable pending work" }, resume: false })
    }
    created.push({ id: sessionID, parentID: input.create.parentID ?? null, time: Date.now(), messageID: userID, partID })
  }

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
    [InstanceBootstrap.node, Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void }))],
    [SessionExecution.node, SessionExecution.noopLayer],
  ],
)

const result = await Effect.runPromise(
  Effect.scoped(InstanceStore.Service.use((store) => store.provide({ directory }, produce)).pipe(Effect.provide(layer))),
)
if (!input.hold) console.log(JSON.stringify(result))
