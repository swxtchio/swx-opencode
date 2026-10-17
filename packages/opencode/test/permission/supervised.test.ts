// Permission requests under `--questions emit` (swxtchio/swx-opencode#286): an ask that the rules leave to a human is
// recorded and emitted, and its once/always/reject reply keeps its existing effect while being settled exactly once.
import { describe, expect } from "bun:test"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Cause, Effect, Exit, Fiber, Layer } from "effect"
import { AskEmit } from "../../src/ask/emit"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { Permission } from "../../src/permission"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import { Session } from "../../src/session/session"
import { provideInstance, tmpdirScoped } from "../fixture/fixture"
import { writeReceiver } from "../lib/ask-receiver"
import { testEffect } from "../lib/effect"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Permission.node,
      Session.node,
      EventV2Bridge.node,
      SessionProjector.node,
      Database.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
    ]),
    [
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalWorkspaces: false })],
      [
        InstanceBootstrap.node,
        Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void })),
      ],
    ],
  ),
)

const supervised = Effect.fn("supervised")(function* () {
  const home = yield* tmpdirScoped()
  const receiver = yield* writeReceiver(home, "cat >> '$OUT'")
  AskEmit.configure({ command: receiver.path })
  yield* Effect.addFinalizer(() => Effect.sync(() => AskEmit.configure(undefined)))
  const session = yield* (yield* Session.Service).create({ title: "supervised" })
  return { receiver, sessionID: session.id }
})

const ask = (sessionID: Session.Info["id"], command: string, callID: string, ruleset: PermissionV1.Ruleset = []) =>
  Permission.Service.use((permission) =>
    permission.ask({
      sessionID,
      permission: "bash",
      patterns: [command],
      metadata: { command },
      always: [command.split(" ")[0] + " *"],
      tool: { messageID: "msg_supervised", callID },
      ruleset,
    }),
  ).pipe(Effect.forkScoped)

const failure = <A, E, R>(self: Effect.Effect<A, E, R>) =>
  self.pipe(
    Effect.exit,
    Effect.map((exit) => (Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined)),
  )

describe("permission requests under --questions emit", () => {
  it.live("emits the request with its approval scope, and a once reply lets only that call proceed", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      yield* Effect.gen(function* () {
        const permission = yield* Permission.Service
        const { receiver, sessionID } = yield* supervised()
        const first = yield* ask(sessionID, "git status", "call_once")
        const [emitted] = yield* receiver.read(1)

        expect(emitted).toMatchObject({
          kind: "permission",
          sessionID,
          directory: dir,
          origin: { messageID: "msg_supervised", callID: "call_once" },
          permission: {
            id: emitted.id,
            permission: "bash",
            patterns: ["git status"],
            always: ["git *"],
            metadata: { command: "git status" },
          },
          answer: { path: `/permission/${emitted.id}/answer`, query: { directory: dir } },
        })
        expect((yield* permission.list()).map((item) => item.id)).toEqual([emitted.id])
        expect(first.pollUnsafe()).toBeUndefined()

        const resolution = yield* permission.reply({ requestID: emitted.id, reply: "once" })
        expect(resolution).toMatchObject({ disposition: "settled", decision: { type: "replied", reply: "once" } })
        yield* Fiber.join(first)

        // once grants nothing beyond the call: the next matching call is asked again.
        const second = yield* ask(sessionID, "git log", "call_again")
        const [, again] = yield* receiver.read(2)
        expect(again.permission.patterns).toEqual(["git log"])
        const duplicate = yield* permission.reply({ requestID: emitted.id, reply: "reject" })
        expect(duplicate).toMatchObject({ disposition: "duplicate", decision: { type: "replied", reply: "once" } })
        yield* permission.reply({ requestID: again.id, reply: "once" })
        yield* Fiber.join(second)
      }).pipe(provideInstance(dir))
    }),
  )

  it.live("always approves the waiting matching request with it, settling that request too", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      yield* Effect.gen(function* () {
        const permission = yield* Permission.Service
        const { receiver, sessionID } = yield* supervised()
        const first = yield* ask(sessionID, "git status", "call_first")
        const second = yield* ask(sessionID, "git diff", "call_second")
        const [a, b] = yield* receiver.read(2)

        yield* permission.reply({ requestID: a.id, reply: "always" })
        yield* Fiber.join(first)
        yield* Fiber.join(second)
        const late = yield* permission.reply({ requestID: b.id, reply: "reject" })
        expect(late).toMatchObject({ disposition: "duplicate", decision: { type: "replied", reply: "always" } })

        // The grant stands: a later matching call proceeds without being emitted.
        yield* Fiber.join(yield* ask(sessionID, "git show", "call_later"))
        expect(yield* receiver.read(2)).toHaveLength(2)
      }).pipe(provideInstance(dir))
    }),
  )

  it.live("reject refuses the call and the Session's other waiting request, with feedback when given", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      yield* Effect.gen(function* () {
        const permission = yield* Permission.Service
        const { receiver, sessionID } = yield* supervised()
        const first = yield* ask(sessionID, "rm notes.md", "call_reject")
        const second = yield* ask(sessionID, "rm todo.txt", "call_sibling")
        const [a, b] = yield* receiver.read(2)

        yield* permission.reply({ requestID: a.id, reply: "reject", message: "keep the notes" })
        const refused = yield* failure(Fiber.join(first))
        expect(refused).toBeInstanceOf(PermissionV1.CorrectedError)
        expect((refused as PermissionV1.CorrectedError).feedback).toBe("keep the notes")
        expect(yield* failure(Fiber.join(second))).toBeInstanceOf(PermissionV1.RejectedError)
        const late = yield* permission.reply({ requestID: b.id, reply: "once" })
        expect(late).toMatchObject({ disposition: "duplicate", decision: { type: "replied", reply: "reject" } })
        expect(yield* permission.list()).toEqual([])
      }).pipe(provideInstance(dir))
    }),
  )

  it.live("allowed and denied operations are decided by the rules and never emitted", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      yield* Effect.gen(function* () {
        const { receiver, sessionID } = yield* supervised()
        yield* Fiber.join(
          yield* ask(sessionID, "ls", "call_allowed", [{ permission: "bash", pattern: "*", action: "allow" }]),
        )
        const denied = yield* failure(
          Fiber.join(
            yield* ask(sessionID, "rm -rf /", "call_denied", [{ permission: "bash", pattern: "*", action: "deny" }]),
          ),
        )
        expect(denied).toBeInstanceOf(PermissionV1.DeniedError)

        const waiting = yield* ask(sessionID, "make", "call_asked")
        const emitted = yield* receiver.read(1)
        expect(emitted.map((item) => item.permission.patterns)).toEqual([["make"]])
        yield* Permission.Service.use((permission) => permission.reply({ requestID: emitted[0].id, reply: "once" }))
        yield* Fiber.join(waiting)
      }).pipe(provideInstance(dir))
    }),
  )

  it.live("an unknown request is not found and leaves the waiting request untouched", () =>
    Effect.gen(function* () {
      const dir = yield* tmpdirScoped({ git: true })
      yield* Effect.gen(function* () {
        const permission = yield* Permission.Service
        const { receiver, sessionID } = yield* supervised()
        const waiting = yield* ask(sessionID, "make", "call_unknown")
        const [emitted] = yield* receiver.read(1)
        const unknown = yield* failure(permission.reply({ requestID: PermissionV1.ID.ascending(), reply: "once" }))
        expect(unknown).toBeInstanceOf(PermissionV1.NotFoundError)
        expect((yield* permission.list()).map((item) => item.id)).toEqual([emitted.id])
        expect(waiting.pollUnsafe()).toBeUndefined()
        yield* permission.reply({ requestID: emitted.id, reply: "once" })
        yield* Fiber.join(waiting)
      }).pipe(provideInstance(dir))
    }),
  )
})
