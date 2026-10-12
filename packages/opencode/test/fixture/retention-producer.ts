// A separate OS process that writes Session trees into a store through the real producers, then exits.
//   argv: <db> <project directory> <tree sizes JSON>, for example [2, 1] for a root with one child and a lone root.
// The store is opened through the production Database layer, so it carries the migrated schema and this
// process's writer registration, which names a process that is gone once the script exits.
// Each Session gets a V1 user and assistant turn with text and step-finish parts, a V2 step with usage, a
// V2 context update, a todo list and a Context Epoch, each holding a "sentinel" string retention must redact.
// It prints one JSON line naming the trees it wrote, the usage each step reported, and the clock window the
// writes fall in.
import { DateTime, Effect, Layer, Schema } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionContextEpoch } from "@opencode-ai/core/session/context-epoch"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SystemContext } from "@opencode-ai/core/system-context/index"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceBootstrap } from "@/project/bootstrap"
import { InstanceStore } from "@/project/instance-store"
import { Session } from "@/session/session"
import { MessageID, PartID, type SessionID } from "@/session/schema"
import { Todo } from "@/session/todo"

const [filename, directory, spec] = process.argv.slice(2)
if (!filename || !directory || !spec) throw new Error("usage: <db> <project directory> <tree sizes JSON>")
const sizes = JSON.parse(spec) as number[]

const usage = { cost: 0.25, tokens: { input: 12, output: 17, reasoning: 3, cache: { read: 5, write: 7 } } }

const produce = Effect.gen(function* () {
  const session = yield* Session.Service
  const todo = yield* Todo.Service
  const events = yield* EventV2Bridge.Service
  const { db } = yield* Database.Service

  const write = Effect.fn("RetentionProducer.write")(function* (parentID?: SessionID) {
    const info = yield* session.create({ title: "retention title sentinel", parentID })
    const userID = MessageID.ascending()
    yield* session.updateMessage({
      id: userID,
      sessionID: info.id,
      role: "user",
      time: { created: Date.now() },
      agent: "build",
      model: { providerID: "provider-a", modelID: "requested-model" },
    } as SessionV1.User)
    yield* session.updatePart({
      id: PartID.ascending(),
      sessionID: info.id,
      messageID: userID,
      type: "text",
      text: "user prompt sentinel",
    } as SessionV1.TextPart)
    const assistantID = MessageID.ascending()
    yield* session.updateMessage({
      id: assistantID,
      sessionID: info.id,
      role: "assistant",
      time: { created: Date.now(), completed: Date.now() },
      parentID: userID,
      modelID: ModelV2.ID.make("requested-model"),
      providerID: ProviderV2.ID.make("provider-a"),
      mode: "build",
      agent: "build",
      path: { cwd: info.directory, root: info.directory },
      responseModelIDs: ["served-model"],
      cost: usage.cost,
      tokens: { total: 32, ...usage.tokens },
    } as SessionV1.Assistant)
    yield* session.updatePart({
      id: PartID.ascending(),
      sessionID: info.id,
      messageID: assistantID,
      type: "text",
      text: "assistant text sentinel",
    } as SessionV1.TextPart)
    yield* session.updatePart({
      id: PartID.ascending(),
      sessionID: info.id,
      messageID: assistantID,
      type: "step-finish",
      reason: "stop",
      responseModelID: "served-model",
      cost: usage.cost,
      tokens: { total: 32, ...usage.tokens },
    } as SessionV1.StepFinishPart)

    const sessionID = SessionV2.ID.make(info.id)
    const timestamp = yield* DateTime.now
    const assistantMessageID = SessionMessage.ID.create()
    yield* events.publish(SessionEvent.Step.Started, {
      sessionID,
      timestamp,
      assistantMessageID,
      agent: "build",
      model: { id: ModelV2.ID.make("requested-model"), providerID: ProviderV2.ID.make("provider-a") },
    })
    yield* events.publish(SessionEvent.Step.Ended, {
      sessionID,
      timestamp,
      assistantMessageID,
      finish: "stop",
      cost: usage.cost,
      tokens: usage.tokens,
    })
    yield* events.publish(SessionEvent.ContextUpdated, {
      sessionID,
      messageID: SessionMessage.ID.create(),
      timestamp,
      text: "v2 context sentinel",
    })
    yield* todo.update({
      sessionID: info.id,
      todos: [{ content: "todo sentinel", status: "pending", priority: "high" }],
    })
    yield* SessionContextEpoch.initialize(
      db,
      Effect.succeed(
        SystemContext.make({
          key: Schema.decodeSync(SystemContext.Key)("retention/fixture"),
          codec: Schema.toCodecJson(Schema.String),
          load: Effect.succeed("context epoch sentinel"),
          baseline: String,
          update: (_previous, current) => current,
        }),
      ),
      sessionID,
    )
    return info.id
  })

  const producedFrom = Date.now()
  const trees = new Array<{ rootSessionID: string; sessionIDs: string[] }>()
  for (const size of sizes) {
    const rootSessionID = yield* write()
    const sessionIDs = [rootSessionID]
    for (let child = 1; child < size; child++) sessionIDs.push(yield* write(rootSessionID))
    trees.push({ rootSessionID, sessionIDs: sessionIDs.toSorted() })
  }
  return { trees, usage, producedFrom, producedUntil: Date.now() }
})

const layer = AppNodeBuilder.build(
  LayerNode.group([
    Session.node,
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
  ],
)

const result = await Effect.runPromise(
  Effect.scoped(
    InstanceStore.Service.use((store) => store.provide({ directory }, produce)).pipe(Effect.provide(layer)),
  ),
)
console.log(JSON.stringify(result))
process.exit(0)
