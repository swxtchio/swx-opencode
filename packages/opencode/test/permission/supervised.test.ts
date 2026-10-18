// Permission requests under `--questions-command` (swxtchio/swx-opencode#286): an ask that the rules leave to a human
// is recorded and emitted, and its once/always/reject reply keeps its existing effect while being settled exactly
// once.
import { describe, expect } from "bun:test"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Layer } from "effect"
import { SessionAskTable } from "@opencode-ai/core/session/ask.sql"
import { eq } from "drizzle-orm"
import { AskLedger } from "../../src/ask/ledger"
import { AskEmit } from "../../src/ask/emit"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { Permission } from "../../src/permission"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import { LLM } from "../../src/session/llm"
import { Session } from "../../src/session/session"
import { provideInstance, tmpdirScoped } from "../fixture/fixture"
import { writeReceiver } from "../lib/ask-receiver"
import { deadline } from "../lib/deadline"
import { testEffect } from "../lib/effect"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Permission.node,
      AskLedger.node,
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

// Beyond the receiver's read deadline, so a red run fails on that deadline rather than on Bun's default.
const timeout = deadline(30_000)

// Waits on a fiber or a signal with a bound, so a decision that never arrives fails here and names what is missing.
const within = <A, E, R>(self: Effect.Effect<A, E, R>, missing = "the waiting call never received its decision") =>
  self.pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(deadline(10_000)),
      orElse: () => Effect.die(new Error(missing)),
    }),
  )

// Records the request of every permission.replied event published from here on.
const repliedEvents = Effect.fn("repliedEvents")(function* () {
  const events = yield* EventV2Bridge.Service
  const replied: string[] = []
  const unsubscribe = yield* events.listen((event) =>
    event.type === Permission.Event.Replied.type
      ? Effect.sync(() => {
          replied.push((event.data as { requestID: string }).requestID)
        })
      : Effect.void,
  )
  yield* Effect.addFinalizer(() => unsubscribe)
  return replied
})

const failure = <A, E, R>(self: Effect.Effect<A, E, R>) =>
  self.pipe(
    Effect.exit,
    Effect.map((exit) => (Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined)),
  )

describe("permission requests under --questions-command", () => {
  it.live(
    "emits the request with its approval scope, and a once reply lets only that call proceed",
    () =>
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
          yield* within(Fiber.join(first))

          // once grants nothing beyond the call: the next matching call is asked again.
          const second = yield* ask(sessionID, "git log", "call_again")
          const [, again] = yield* receiver.read(2)
          expect(again.permission.patterns).toEqual(["git log"])
          const duplicate = yield* permission.reply({ requestID: emitted.id, reply: "reject" })
          expect(duplicate).toMatchObject({ disposition: "duplicate", decision: { type: "replied", reply: "once" } })
          yield* permission.reply({ requestID: again.id, reply: "once" })
          yield* within(Fiber.join(second))
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "always approves the waiting matching request with it, settling that request too",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const permission = yield* Permission.Service
          const { receiver, sessionID } = yield* supervised()
          const first = yield* ask(sessionID, "git status", "call_first")
          const second = yield* ask(sessionID, "git diff", "call_second")
          const [a, b] = yield* receiver.read(2)

          yield* permission.reply({ requestID: a.id, reply: "always" })
          yield* within(Fiber.join(first))
          yield* within(Fiber.join(second))
          const late = yield* permission.reply({ requestID: b.id, reply: "reject" })
          expect(late).toMatchObject({ disposition: "duplicate", decision: { type: "replied", reply: "always" } })

          // The grant stands: a later matching call proceeds without being emitted.
          yield* within(Fiber.join(yield* ask(sessionID, "git show", "call_later")))
          expect(yield* receiver.read(2)).toHaveLength(2)
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "reject refuses the call and the Session's other waiting request, with feedback when given",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const permission = yield* Permission.Service
          const { receiver, sessionID } = yield* supervised()
          const first = yield* ask(sessionID, "rm notes.md", "call_reject")
          const second = yield* ask(sessionID, "rm todo.txt", "call_sibling")
          const [a, b] = yield* receiver.read(2)

          yield* permission.reply({ requestID: a.id, reply: "reject", message: "keep the notes" })
          const refused = yield* failure(within(Fiber.join(first)))
          expect(refused).toBeInstanceOf(PermissionV1.CorrectedError)
          expect((refused as PermissionV1.CorrectedError).feedback).toBe("keep the notes")
          expect(yield* failure(within(Fiber.join(second)))).toBeInstanceOf(PermissionV1.RejectedError)
          const late = yield* permission.reply({ requestID: b.id, reply: "once" })
          expect(late).toMatchObject({ disposition: "duplicate", decision: { type: "replied", reply: "reject" } })
          expect(yield* permission.list()).toEqual([])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "allowed and denied operations are decided by the rules and never emitted",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const { receiver, sessionID } = yield* supervised()
          yield* within(
            Fiber.join(
              yield* ask(sessionID, "ls", "call_allowed", [{ permission: "bash", pattern: "*", action: "allow" }]),
            ),
          )
          const denied = yield* failure(
            within(
              Fiber.join(
                yield* ask(sessionID, "rm -rf /", "call_denied", [
                  { permission: "bash", pattern: "*", action: "deny" },
                ]),
              ),
            ),
          )
          expect(denied).toBeInstanceOf(PermissionV1.DeniedError)

          const waiting = yield* ask(sessionID, "make", "call_asked")
          const emitted = yield* receiver.read(1)
          expect(emitted.map((item) => item.permission.patterns)).toEqual([["make"]])
          yield* Permission.Service.use((permission) => permission.reply({ requestID: emitted[0].id, reply: "once" }))
          yield* within(Fiber.join(waiting))
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "an unknown request is not found and leaves the waiting request untouched",
    () =>
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
          yield* within(Fiber.join(waiting))
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "a request interrupted while its event is published is cancelled at once",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const permission = yield* Permission.Service
          const replied = yield* repliedEvents()
          const ledger = yield* AskLedger.Service
          const events = yield* EventV2Bridge.Service
          const { receiver, sessionID } = yield* supervised()
          const entered = yield* Deferred.make<void>()
          const unsubscribe = yield* events.listen((event) =>
            event.type === Permission.Event.Asked.type
              ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
              : Effect.void,
          )
          const fiber = yield* ask(sessionID, "make", "call_publishing")
          yield* within(Deferred.await(entered), "the ask event was never published")
          const [emitted] = yield* receiver.read(1)
          yield* Fiber.interrupt(fiber)
          yield* unsubscribe

          expect(yield* ledger.find(emitted.id, "permission")).toMatchObject({ decision: { type: "cancelled" } })
          expect(yield* permission.list()).toEqual([])
          // The close is announced once, as a refusal, so attached clients drop the request.
          expect(replied.filter((id) => id === emitted.id)).toHaveLength(1)
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "a reply whose event cannot be published still reaches the waiting call",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const permission = yield* Permission.Service
          const events = yield* EventV2Bridge.Service
          const { receiver, sessionID } = yield* supervised()
          const waiting = yield* ask(sessionID, "make", "call_unannounced")
          const [emitted] = yield* receiver.read(1)
          const unsubscribe = yield* events.listen((event) =>
            event.type === Permission.Event.Replied.type ? Effect.die(new Error("publication refused")) : Effect.void,
          )
          expect(yield* failure(permission.reply({ requestID: emitted.id, reply: "once" }))).toBeDefined()
          yield* unsubscribe

          yield* within(Fiber.join(waiting))
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "a supervised request without a tool call is refused and never recorded",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const permission = yield* Permission.Service
          const db = (yield* Database.Service).db
          const { sessionID } = yield* supervised()
          const refused = yield* failure(
            permission.ask({
              sessionID,
              permission: "custom_approval",
              patterns: ["deploy"],
              metadata: {},
              always: ["deploy"],
              ruleset: [],
            }),
          )
          expect(refused).toBeInstanceOf(PermissionV1.RejectedError)
          expect(
            yield* db.select().from(SessionAskTable).where(eq(SessionAskTable.session_id, sessionID)).all(),
          ).toEqual([])
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "a GitLab Duo workflow approval is emitted with its message as origin and settled by its answer",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const permission = yield* Permission.Service
          const { receiver, sessionID } = yield* supervised()
          const id = PermissionV1.ID.ascending()
          const request = LLM.workflowApproval({ sessionID, messageID: "msg_workflow" }, id, [
            { name: "deploy", args: JSON.stringify({ title: "production" }) },
          ])
          const waiting = yield* permission.ask(request).pipe(Effect.forkScoped)
          const [emitted] = yield* receiver.read(1)

          expect(emitted).toMatchObject({
            kind: "permission",
            id,
            origin: { messageID: "msg_workflow", callID: id },
            permission: { permission: "workflow_tool_approval", patterns: ["deploy: production"] },
          })
          expect(yield* permission.reply({ requestID: id, reply: "once" })).toMatchObject({ disposition: "settled" })
          yield* within(Fiber.join(waiting))
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  it.live(
    "competing replies to one request settle it once: one event, the winner's outcome",
    () =>
      Effect.gen(function* () {
        const dir = yield* tmpdirScoped({ git: true })
        yield* Effect.gen(function* () {
          const permission = yield* Permission.Service
          const replied = yield* repliedEvents()
          const { receiver, sessionID } = yield* supervised()
          const waiting = yield* ask(sessionID, "make", "call_race")
          const [emitted] = yield* receiver.read(1)

          const results = yield* Effect.all(
            [
              permission.reply({ requestID: emitted.id, reply: "once" }),
              permission.reply({ requestID: emitted.id, reply: "reject" }),
            ],
            { concurrency: "unbounded" },
          )
          expect(results.map((result) => result.disposition).sort()).toEqual(["duplicate", "settled"])
          const winner = results.find((result) => result.disposition === "settled")!.decision
          expect(results.map((result) => result.decision)).toEqual([winner, winner])
          const outcome = yield* within(Fiber.join(waiting)).pipe(Effect.exit)
          expect(Exit.isSuccess(outcome)).toBe(winner.type === "replied" && winner.reply === "once")
          expect(replied.filter((id) => id === emitted.id)).toHaveLength(1)
        }).pipe(provideInstance(dir))
      }),
    timeout,
  )

  for (const cascade of ["reject", "always"] as const)
    it.live(
      `a reply racing an ${cascade} cascade settles each request once, as its waiter saw it`,
      () =>
        Effect.gen(function* () {
          const dir = yield* tmpdirScoped({ git: true })
          yield* Effect.gen(function* () {
            const permission = yield* Permission.Service
            const ledger = yield* AskLedger.Service
            const replied = yield* repliedEvents()
            const { receiver, sessionID } = yield* supervised()
            const first = yield* ask(sessionID, "git status", "call_cascading")
            const second = yield* ask(sessionID, "git diff", "call_claimed")
            const [a, b] = yield* receiver.read(2)

            const results = yield* Effect.all(
              [
                permission.reply({ requestID: a.id, reply: cascade }),
                permission.reply({ requestID: b.id, reply: cascade === "reject" ? "once" : "reject" }),
              ],
              { concurrency: "unbounded" },
            )
            // Either the cascade or the request's own reply won it, and the ledger records that reply.
            const recorded = (yield* ledger.find(b.id, "permission"))!.decision
            expect(recorded?.type).toBe("replied")
            expect(results[1].decision).toEqual(recorded!)
            const outcome = yield* within(Fiber.join(second)).pipe(Effect.exit)
            expect(Exit.isSuccess(outcome)).toBe(recorded?.type === "replied" && recorded.reply !== "reject")
            yield* within(Fiber.join(first)).pipe(Effect.exit)
            expect(replied.filter((id) => id === a.id)).toHaveLength(1)
            expect(replied.filter((id) => id === b.id)).toHaveLength(1)
          }).pipe(provideInstance(dir))
        }),
      timeout,
    )
})
