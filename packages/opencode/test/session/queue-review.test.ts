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

const request = (
  sessionID: SessionID,
  delivery: SessionQueue.Delivery,
  requestID: string,
  value: string,
  origin?: "tui-composer",
) =>
  Effect.gen(function* () {
    const queue = yield* SessionQueue.Service
    return yield* queue.request({
      sessionID,
      delivery,
      requestID,
      ...(origin ? { origin } : {}),
      parts: [{ type: "text", text: value }],
    })
  })

const jobClassControls = [
  { title: "handler", first: "fix authentication handler", second: "fix billing handler" },
  { title: "crash", first: "fix the parser crash", second: "fix the tokenizer crash" },
]

const sharedFirstNounControls = [
  { title: "session queue", first: "refactor the session queue", second: "refactor the session projector" },
  { title: "workspace queue", first: "rename the workspace queue", second: "rename the workspace projector" },
]

const differentModifierSameHeadTargets = [
  {
    title: "intermittent failures",
    first: "fix intermittent authentication failures",
    second: "fix intermittent billing failures",
  },
  {
    title: "profile pages",
    first: "update user profile page",
    second: "update admin profile page",
  },
  {
    title: "redirect loops",
    first: "fix login redirect loop",
    second: "fix signup redirect loop",
  },
  {
    title: "connection pooling",
    first: "add postgres connection pooling",
    second: "add redis connection pooling",
  },
  {
    title: "service clients",
    first: "refactor billing service client",
    second: "refactor auth service client",
  },
  {
    title: "receipt routing",
    first: "review authentication receipt routing: preserve origin IDs",
    second: "review billing receipt routing: verify settlement",
  },
  {
    title: "dependent clauses retain conflicting subject modifiers",
    first: "investigate auth deployment and when it runs",
    second: "investigate billing deployment and why it runs",
  },
  {
    title: "lowercase node.js retains different subsystem subjects",
    first: "fix node.js authentication handler",
    second: "fix node.js billing handler",
  },
  {
    title: "comma clauses retain conflicting subject modifiers",
    first: "investigate auth deployment, how it works",
    second: "investigate billing deployment, how it works",
  },
  {
    title: "comma clauses retain conflicting subject nouns",
    first: "investigate CI deployment, how it works",
    second: "investigate CI rollback, how it works",
  },
]

const literalFilenameTargets = [
  {
    title: "tool file siblings",
    first: "update packages/core/src/tool/tool.ts: preserve the singular module",
    second: "update packages/core/src/tool/tools.ts: preserve the plural module",
  },
  {
    title: "route handler siblings",
    first: "update packages/opencode/src/server/routes/api/session.ts: preserve route identity",
    second: "update packages/opencode/src/server/routes/api/sessions.ts: preserve route identity",
  },
  {
    title: "short file target",
    first: "update packages/core/src/tool/tool.ts: preserve the singular module",
    second: "update tools.ts: preserve the standalone module",
  },
  {
    title: "file extensions",
    first: "update src/session/queue.ts: preserve the TypeScript module",
    second: "update src/session/queue.tsx: preserve the JSX module",
  },
]

const suffixFilenameTargets = [
  {
    title: "unlisted suffix basenames use literal identity after case normalization",
    first: "fix auth tool.test: preserve the module",
    second: "fix auth tools.TEST: verify the schema",
  },
  {
    title: "unlisted suffix basenames stay literal under a path prefix",
    first: "fix src/tool.test: preserve the module",
    second: "fix src/tools.test: verify the schema",
  },
  {
    title: "unlisted suffix basenames do not singularize before identity",
    first: "fix auth.graphql: preserve the schema",
    second: "fix auths.graphql: verify the schema",
  },
  {
    title: "an ing suffix does not singularize an unlisted dotted basename",
    first: "fix auth redirecting.test: preserve the route",
    second: "fix auth redirect.test: verify the route",
  },
  {
    title: "an unlisted dotted filename retains literal path containment",
    first: "update src/File.TEST: preserve the test identity",
    second: "update file.test: document the fixture",
    merged: "update src/File.TEST:\n1. preserve the test identity\n2. file.test: document the fixture",
  },
  {
    title: "curated bare suffix matches a slash-qualified filename",
    first: "update src/file.proto: preserve the document identity",
    second: "update file.proto: document details",
    merged: "update src/file.proto:\n1. preserve the document identity\n2. file.proto: document details",
  },
  {
    title: "curated suffix matches a shortened path",
    first: "update src/auth.proto: preserve schema identity",
    second: "update auth.proto: document the schema",
    merged: "update src/auth.proto:\n1. preserve schema identity\n2. auth.proto: document the schema",
  },
  {
    title: "different curated suffixes stay distinct",
    first: "update auth.proto: preserve protocol identity",
    second: "update auth.ts: preserve implementation identity",
  },
  {
    title: "an unlisted dotted word stays distinct from a curated suffix",
    first: "fix auth.proto: preserve the wire schema",
    second: "fix auth.protos: preserve generated documentation",
  },
  {
    title: "different unlisted dotted suffixes stay literal",
    first: "fix auth.graphql: preserve the query schema",
    second: "fix auth.graphqls: preserve the schema documentation",
  },
]

const bareDottedTargets = [
  {
    title: "Node.js natural subject",
    first: "fix Node.js runtime checks: keep async test",
    second: "fix Node runtime checks: cover async test",
    merged: "fix Node.js runtime checks:\n1. keep async test\n2. Node runtime checks: cover async test",
  },
  {
    title: "Next.js remains a natural target name",
    first: "fix Next.js routing bug: preserve fallback",
    second: "fix Next routing bug: add retry",
    merged: "fix Next.js routing bug:\n1. preserve fallback\n2. Next routing bug: add retry",
  },
  {
    title: "vue.js remains a natural target name",
    first: "fix vue.js reactivity bug: preserve the proxy",
    second: "fix vue reactivity bug: trace the observer",
    merged: "fix vue.js reactivity bug:\n1. preserve the proxy\n2. vue reactivity bug: trace the observer",
  },
  {
    title: "v1.2 remains part of its natural target",
    first: "upgrade v1.2 runtime",
    second: "upgrade runtime",
    merged: "upgrade v1.2 runtime:\n1. upgrade v1.2 runtime\n2. upgrade runtime",
  },
  {
    title: "2.5s remains part of its natural target",
    first: "upgrade 2.5s runtime",
    second: "upgrade runtime",
    merged: "upgrade 2.5s runtime:\n1. upgrade 2.5s runtime\n2. upgrade runtime",
  },
  {
    title: "different version tokens do not become file identities",
    first: "upgrade v1.2",
    second: "upgrade 2.5s",
  },
  {
    title: "different bare filename extensions",
    first: "fix queue.ts",
    second: "fix queue.tsx",
  },
  {
    title: "bare dotted suffix changes a word-boundary target",
    first: "fix login",
    second: "fix login.tsx",
  },
  {
    title: "plain-text filename suffix is not a word prefix",
    first: "Please summarize queue.ts",
    second: "Please summarize queue.tsx",
  },
]

const runtimeNameTarget = {
  first: "upgrade node.js runtime",
  second: "upgrade node runtime",
  merged: "upgrade node.js runtime:\n1. upgrade node.js runtime\n2. upgrade node runtime",
}

const dottedRuntimeControls = [
  {
    title: "express.js authentication and billing handlers stay separate",
    first: "fix express.js authentication handler",
    second: "fix express.js billing handler",
  },
  {
    title: "nuxt.js authentication and billing handlers stay separate",
    first: "fix nuxt.js authentication handler",
    second: "fix nuxt.js billing handler",
  },
  {
    title: "d3.js chart and axis handlers stay separate",
    first: "fix d3.js chart handler",
    second: "fix d3.js axis handler",
  },
]

const dottedRuntimeSameJobTargets = [
  {
    title: "express.js and express keep the same subject",
    first: "fix express.js authentication handler",
    second: "fix express authentication handler",
    merged:
      "fix express.js authentication handler:\n1. fix express.js authentication handler\n2. fix express authentication handler",
  },
  {
    title: "nuxt.js and nuxt keep the same subject",
    first: "fix nuxt.js authentication handler",
    second: "fix nuxt authentication handler",
    merged:
      "fix nuxt.js authentication handler:\n1. fix nuxt.js authentication handler\n2. fix nuxt authentication handler",
  },
  {
    title: "d3.js and d3 keep the same subject",
    first: "fix d3.js chart handler",
    second: "fix d3 chart handler",
    merged: "fix d3.js chart handler:\n1. fix d3.js chart handler\n2. fix d3 chart handler",
  },
]

const caseInsensitiveFilenameTargets = [
  {
    title: "no-colon uppercase filename retains its identity",
    first: "update Auth.ts preserve the schema",
    second: "update auth.ts verify the route",
    merged: "update Auth.ts:\n1. preserve the schema\n2. verify the route",
  },
  {
    title: "uppercase path suffix matches its lowercase basename",
    first: "update src/Auth.ts: preserve origin IDs",
    second: "update auth.ts: verify settlement",
    merged: "update src/Auth.ts:\n1. preserve origin IDs\n2. auth.ts: verify settlement",
  },
  {
    title: "uppercase extension keeps different files distinct",
    first: "update Auth.TS: preserve the TypeScript module",
    second: "update Auth.TSX: preserve the JSX module",
  },
  {
    title: "a slash-qualified and bare JavaScript path share file identity",
    first: "update src/node.js: parse options",
    second: "update node.js: format tokens",
    merged: "update src/node.js:\n1. parse options\n2. node.js: format tokens",
  },
]

const packageNameTarget = {
  first: "upgrade socket.io",
  second: "upgrade socket",
  merged: "upgrade socket.io:\n1. upgrade socket.io\n2. upgrade socket",
}

const containedCommandTargets = [
  {
    title: "parser",
    first: "fix the parser",
    second: "fix parser crash",
    merged: "fix the parser:\n1. fix the parser\n2. fix parser crash",
  },
  {
    title: "authentication redirect",
    first: "fix authentication",
    second: "fix authentication redirect",
    merged: "fix authentication:\n1. fix authentication\n2. fix authentication redirect",
  },
  {
    title: "login page",
    first: "fix the login page",
    second: "fix login",
    merged: "fix the login page:\n1. fix the login page\n2. fix login",
  },
  {
    title: "login page complement",
    first: "fix the login page for mobile",
    second: "fix login",
    merged: "fix the login page for mobile:\n1. fix the login page for mobile\n2. fix login",
  },
  {
    title: "dotted prose target",
    first: "fix v1.2 authentication handler",
    second: "fix authentication handler",
    merged: "fix v1.2 authentication handler:\n1. fix v1.2 authentication handler\n2. fix authentication handler",
  },
  {
    title: "trailing punctuation",
    first: "Fix the flaky auth test.",
    second: "fix flaky auth test",
    merged: "Fix the flaky auth test.:\n1. Fix the flaky auth test.\n2. fix flaky auth test",
  },
  {
    title: "punctuation-normalized containment",
    first: "fix the flaky auth-test page.",
    second: "fix auth test",
    merged: "fix the flaky auth-test page.:\n1. fix the flaky auth-test page.\n2. fix auth test",
  },
]

const punctuationNormalizedTargets = [
  {
    first: "Fix the test suite.",
    second: "fix test suite",
    merged: "Fix the test suite.:\n1. Fix the test suite.\n2. fix test suite",
  },
]

const sameWorkProseFileReference = {
  first: "fix login.ts",
  second: "fix the login handler in login.ts",
  merged: "fix login.ts:\n1. fix login.ts\n2. fix the login handler in login.ts",
}

const embeddedPathTargets = {
  first: "fix the auth handler in src/login.ts",
  second: "fix the auth handler in src/billing.ts",
}

const differentInvestigationPathReferences = {
  first: "investigate why the CI is slow in src/auth.ts",
  second: "investigate login delays in auth.ts",
}

const sameJobTailTargets = [
  {
    title: "investigation subject with a different tail",
    first: "investigate CI deployment and how it works",
    second: "investigate CI deployment and how it proceeds",
    merged:
      "investigate CI deployment and how it works:\n1. investigate CI deployment and how it works\n2. investigate CI deployment and how it proceeds",
  },
  {
    title: "different interrogative tails leave the same cache subject intact",
    first: "review API caching and when it expires",
    second: "review API caching and why it refreshes",
    merged:
      "review API caching and when it expires:\n1. review API caching and when it expires\n2. review API caching and why it refreshes",
  },
  {
    title: "comma-led dependent clauses leave the same deployment subject intact",
    first: "investigate CI deployment, how it works",
    second: "investigate CI deployment, how it proceeds",
    merged:
      "investigate CI deployment, how it works:\n1. investigate CI deployment, how it works\n2. investigate CI deployment, how it proceeds",
  },
]

const sameJobWordForms = [
  {
    title: "websocket reconnecting and reconnection share a root",
    first: "fix websocket reconnecting",
    second: "fix websocket reconnection",
    merged: "fix websocket reconnecting:\n1. fix websocket reconnecting\n2. fix websocket reconnection",
  },
  {
    title: "a past verb form and its base share a root",
    first: "investigate network failed",
    second: "investigate network fail",
    merged: "investigate network failed:\n1. investigate network failed\n2. investigate network fail",
  },
  {
    title: "activation and activate share a bounded -ate/-ion root",
    first: "fix user activation",
    second: "fix user activate",
    merged: "fix user activation:\n1. fix user activation\n2. fix user activate",
  },
  {
    title: "normalized singleton migration roots match",
    first: "fix migration",
    second: "fix migrate",
    merged: "fix migration:\n1. fix migration\n2. fix migrate",
  },
  {
    title: "reconnecting and reconnection match with a trailing subject noun",
    first: "fix websocket reconnecting handler",
    second: "fix websocket reconnection handler",
    merged:
      "fix websocket reconnecting handler:\n1. fix websocket reconnecting handler\n2. fix websocket reconnection handler",
  },
]

const boundedStemControls = [
  {
    title: "express parser and expression parser",
    first: "fix auth express parser",
    second: "fix auth expression parser",
  },
  { title: "quest parser and question parser", first: "fix auth quest parser", second: "fix auth question parser" },
  { title: "port parser and portion parser", first: "fix auth port parser", second: "fix auth portion parser" },
  { title: "edit page and edition page", first: "fix auth edit page", second: "fix auth edition page" },
  { title: "region cache and reg cache", first: "fix auth region cache", second: "fix auth reg cache" },
]

const inflectionTargets = [
  {
    title: "redirects",
    first: "fix authentication redirect: trace callback",
    second: "fix authentication redirects: inspect login",
    merged: "fix authentication redirect:\n1. trace callback\n2. authentication redirects: inspect login",
  },
  {
    title: "handlers",
    first: "fix authentication handler: trace callback",
    second: "fix authentication handlers: inspect login",
    merged: "fix authentication handler:\n1. trace callback\n2. authentication handlers: inspect login",
  },
  {
    title: "forms",
    first: "update user profile form: validate fields",
    second: "update user profile forms: map settings",
    merged: "update user profile form:\n1. validate fields\n2. user profile forms: map settings",
  },
  {
    title: "reconnects",
    first: "refactor reconnect queue: preserve order",
    second: "refactor reconnects queue: preserve retry order",
    merged: "refactor reconnect queue:\n1. preserve order\n2. reconnects queue: preserve retry order",
  },
  {
    title: "queues",
    first: "refactor session queue: preserve order",
    second: "refactor session queues: preserve receipts",
    merged: "refactor session queue:\n1. preserve order\n2. session queues: preserve receipts",
  },
]

const consolidateAdmitted = (title: string, values: readonly { delivery: SessionQueue.Delivery; text: string }[]) =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title })
    const admitted = yield* Effect.forEach(values, (value) => admit(session.id, value.delivery, value.text))
    expect(yield* queue.list(session.id)).toHaveLength(admitted.length)
    return yield* queue.consolidate(session.id)
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
  "a copy in another delivery class stays pending while same-class commands consolidate",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "copy, then command" })
      yield* admit(session.id, "steer", "run lint")
      yield* request(session.id, "steer", "req-b", "run lint --fix")
      yield* request(session.id, "queue", "req-c", "run lint --fix")
      const result = yield* queue.consolidate(session.id)
      expect(result.map((item) => [item.delivery, text(item)])).toEqual([
        ["steer", "run lint:\n1. run lint\n2. --fix"],
        ["queue", "run lint --fix"],
      ])
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "req-b"))).toEqual({
        state: "pending",
        itemID: result[0]!.id,
      })
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "req-c"))).toEqual({
        state: "pending",
        itemID: result[1]!.id,
      })
      yield* promoter(session.id, [])("steer")
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "req-c"))).toEqual({
        state: "pending",
        itemID: result[1]!.id,
      })
    }),
  30_000,
)

it.instance("command targets match across verb and whitespace case changes", () =>
  Effect.gen(function* () {
    const items = yield* consolidateAdmitted("case and whitespace", [
      { delivery: "queue", text: "  RUN   AUTH  " },
      { delivery: "queue", text: "run auth --fix" },
    ])
    expect(items.map(text)).toEqual(["RUN AUTH:\n1. RUN AUTH\n2. --fix"])
  }),
)

it.instance("command targets ignore leading articles and adjective differences", () =>
  Effect.gen(function* () {
    const items = yield* consolidateAdmitted("articles and adjectives", [
      { delivery: "queue", text: "fix the flaky authentication handler: keep null guards" },
      { delivery: "queue", text: "fix an authentication handler: add regression coverage" },
    ])
    expect(items.map(text)).toEqual([
      "fix the flaky authentication handler:\n1. keep null guards\n2. an authentication handler: add regression coverage",
    ])
  }),
)

it.instance("no-colon commands match whole subjects across unlisted modifiers", () =>
  Effect.gen(function* () {
    const cases = [
      {
        title: "intermittent authentication failures",
        prompts: ["fix intermittent authentication failures", "fix authentication failures"],
        merged:
          "fix intermittent authentication failures:\n1. fix intermittent authentication failures\n2. fix authentication failures",
      },
      {
        title: "sporadic login redirects",
        prompts: ["repair sporadic login redirects", "repair login redirects"],
        merged: "repair sporadic login redirects:\n1. repair sporadic login redirects\n2. repair login redirects",
      },
    ]
    for (const test of cases) {
      const items = yield* consolidateAdmitted(
        test.title,
        test.prompts.map((text) => ({ delivery: "queue" as const, text })),
      )
      expect(items.map(text)).toEqual([test.merged])
    }
  }),
)

it.instance("request admission matches whole no-colon subjects and keeps shared modifiers separate", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const positive = yield* sessions.create({ title: "no-colon request subject match" })
    yield* request(positive.id, "queue", "subject-a", "fix intermittent authentication failures")
    yield* request(positive.id, "queue", "subject-b", "fix authentication failures")
    expect((yield* queue.list(positive.id)).map(text)).toEqual([
      "fix intermittent authentication failures:\n1. fix intermittent authentication failures\n2. fix authentication failures",
    ])
    expect(Option.getOrThrow(yield* queue.receipt(positive.id, "subject-b"))).toMatchObject({
      state: "merged",
      into: "subject-a",
    })

    const different = yield* sessions.create({ title: "no-colon shared modifier control" })
    yield* request(different.id, "queue", "different-a", "fix intermittent authentication failures")
    yield* request(different.id, "queue", "different-b", "fix intermittent billing timeouts")
    expect((yield* queue.list(different.id)).map(text)).toEqual([
      "fix intermittent authentication failures",
      "fix intermittent billing timeouts",
    ])
    expect(Option.getOrThrow(yield* queue.receipt(different.id, "different-a"))).toMatchObject({ state: "pending" })
    expect(Option.getOrThrow(yield* queue.receipt(different.id, "different-b"))).toMatchObject({ state: "pending" })
  }),
)

it.instance("shared first nouns with different job classes stay separate", () =>
  Effect.gen(function* () {
    const prompts = [
      "refactor the session queue",
      "refactor the session projector",
      "rename the workspace queue",
      "rename the workspace projector",
    ]
    for (const delivery of ["steer", "queue"] as const) {
      const items = yield* consolidateAdmitted(
        `shared modifiers and first nouns ${delivery}`,
        prompts.map((text) => ({ delivery, text })),
      )
      expect(items.map(text)).toEqual(prompts)
    }
  }),
)

it.instance("request admission keeps different job classes apart on shared first nouns", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of sharedFirstNounControls) {
        const session = yield* sessions.create({ title: `${delivery} ${pair.title} control` })
        yield* request(session.id, delivery, "shared-a", pair.first)
        yield* request(session.id, delivery, "shared-b", pair.second)
        expect((yield* queue.list(session.id)).map(text)).toEqual([pair.first, pair.second])
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "shared-a"))).toMatchObject({ state: "pending" })
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "shared-b"))).toMatchObject({ state: "pending" })
      }
    }
  }),
)

it.instance("a shared job-class head noun does not merge different work during consolidation", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of jobClassControls) {
        const consolidated = yield* consolidateAdmitted(`${delivery} ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        expect(consolidated.map(text)).toEqual([pair.first, pair.second])
      }
    }
  }),
)

it.instance("request admission does not merge different work on a shared job-class head noun", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of jobClassControls) {
        const session = yield* sessions.create({ title: `${delivery} request ${pair.title}` })
        yield* request(session.id, delivery, "head-a", pair.first)
        yield* request(session.id, delivery, "head-b", pair.second)
        expect((yield* queue.list(session.id)).map(text)).toEqual([pair.first, pair.second])
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "head-a"))).toMatchObject({ state: "pending" })
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "head-b"))).toMatchObject({ state: "pending" })
      }
    }
  }),
)

it.instance("consolidation keeps containment ahead of job-class head equality", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of containedCommandTargets) {
        const items = yield* consolidateAdmitted(`${delivery} contained ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        expect(items.map(text)).toEqual([pair.merged])
      }
    }
  }),
)

it.instance("request admission keeps containment ahead of job-class head equality", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of containedCommandTargets) {
        const session = yield* sessions.create({ title: `${delivery} contained ${pair.title}` })
        yield* request(session.id, delivery, "contained-a", pair.first)
        yield* request(session.id, delivery, "contained-b", pair.second)
        expect((yield* queue.list(session.id)).map(text)).toEqual([pair.merged])
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "contained-b"))).toMatchObject({
          state: "merged",
          into: "contained-a",
        })
      }
    }
  }),
)

it.instance("consolidation normalizes trailing punctuation on an exact generic target", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of punctuationNormalizedTargets) {
        const items = yield* consolidateAdmitted(`${delivery} punctuation target`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        expect(items.map(text)).toEqual([pair.merged])
      }
    }
  }),
)

it.instance("request admission normalizes trailing punctuation on an exact generic target", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of punctuationNormalizedTargets) {
        const session = yield* sessions.create({ title: `${delivery} punctuation target` })
        yield* request(session.id, delivery, "punctuation-a", pair.first)
        yield* request(session.id, delivery, "punctuation-b", pair.second)
        expect((yield* queue.list(session.id)).map(text)).toEqual([pair.merged])
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "punctuation-b"))).toMatchObject({
          state: "merged",
          into: "punctuation-a",
        })
      }
    }
  }),
)

it.instance("consolidation keeps different modifiers apart when the job-class head is shared", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of differentModifierSameHeadTargets) {
        const items = yield* consolidateAdmitted(`${delivery} ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        expect(items.map(text)).toEqual([pair.first, pair.second])
      }
    }
  }),
)

it.instance("request admission keeps different modifiers apart when the job-class head is shared", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of differentModifierSameHeadTargets) {
        const session = yield* sessions.create({ title: `${delivery} ${pair.title} control` })
        yield* request(session.id, delivery, "modifier-a", pair.first)
        yield* request(session.id, delivery, "modifier-b", pair.second)
        expect((yield* queue.list(session.id)).map(text)).toEqual([pair.first, pair.second])
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "modifier-a"))).toMatchObject({
          state: "pending",
        })
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "modifier-b"))).toMatchObject({
          state: "pending",
        })
      }
    }
  }),
)

it.instance("consolidation matches shared identifiers across inflections in both delivery classes", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of inflectionTargets) {
        const consolidated = yield* consolidateAdmitted(pair.title, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        expect(consolidated.map(text)).toEqual([pair.merged])
      }
    }
  }),
)

it.instance("request admission matches shared identifiers across inflections in both delivery classes", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of inflectionTargets) {
        const session = yield* sessions.create({ title: `${delivery} request ${pair.title}` })
        yield* request(session.id, delivery, "inflection-a", pair.first)
        yield* request(session.id, delivery, "inflection-b", pair.second)
        expect((yield* queue.list(session.id)).map(text)).toEqual([pair.merged])
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "inflection-b"))).toMatchObject({
          state: "merged",
          into: "inflection-a",
        })
      }
    }
  }),
)

it.instance("colon targets stop at the short subject before a trailing note", () =>
  Effect.gen(function* () {
    const cases = [
      {
        title: "different parser duties",
        prompts: [
          "add tests for the parser. Note: preserve AST coverage",
          "add docs for the parser. Note: update usage guidance",
        ],
      },
      {
        title: "different sentence subjects",
        prompts: ["add parser tests. Note: preserve AST coverage", "add router docs. Note: update usage guidance"],
      },
    ]
    for (const test of cases) {
      const items = yield* consolidateAdmitted(
        test.title,
        test.prompts.map((text) => ({ delivery: "queue" as const, text })),
      )
      expect(items.map(text)).toEqual(test.prompts)
    }
  }),
)

it.instance("grammatical target words do not merge unrelated work in consolidation", () =>
  Effect.gen(function* () {
    const cases = [
      ["investigate how CI deploys: check runner", "investigate how login redirects: trace callback"],
      [
        "investigate CI deployment and how it works: inspect runner",
        "investigate login redirects and how it fails: trace callback",
      ],
      ["investigate why the CI is red:", "investigate why the login is slow:"],
      ["investigate how CI deployment works: inspect runner", "investigate how login routing works: trace callback"],
      ["investigate why CI deployment fails: inspect runner", "investigate why login routing fails: trace callback"],
      [
        "investigate where CI deployment breaks: inspect runner",
        "investigate where login routing breaks: trace callback",
      ],
    ]
    for (const delivery of ["steer", "queue"] as const) {
      for (const [first, second] of cases) {
        const items = yield* consolidateAdmitted(`different interrogative targets ${delivery}`, [
          { delivery, text: first! },
          { delivery, text: second! },
        ])
        expect(items.map(text)).toEqual([first, second])
      }
    }

    const positive = yield* consolidateAdmitted("same deployment target", [
      { delivery: "queue", text: "investigate how CI deployment works: inspect runner" },
      { delivery: "queue", text: "investigate whether CI deployment works: trace callback" },
    ])
    expect(positive.map(text)).toEqual([
      "investigate how CI deployment works:\n1. inspect runner\n2. whether CI deployment works: trace callback",
    ])
  }),
)

it.instance("request admission ignores grammatical target overlap and keeps a same-job positive", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const requests = [
      {
        first: "investigate CI deployment and how it works: inspect runner",
        second: "investigate login redirects and how it fails: trace callback",
      },
      {
        first: "investigate how CI deployment works: inspect runner",
        second: "investigate how login routing works: trace callback",
      },
      {
        first: "investigate why CI deployment fails: inspect runner",
        second: "investigate why login routing fails: trace callback",
      },
      {
        first: "investigate where CI deployment breaks: inspect runner",
        second: "investigate where login routing breaks: trace callback",
      },
    ]
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of requests) {
        const different = yield* sessions.create({ title: `${delivery} grammatical request boundary ${pair.first}` })
        yield* request(different.id, delivery, "grammar-a", pair.first)
        yield* request(different.id, delivery, "grammar-b", pair.second)
        expect((yield* queue.list(different.id)).map(text)).toEqual([pair.first, pair.second])
        expect(Option.getOrThrow(yield* queue.receipt(different.id, "grammar-a"))).toMatchObject({ state: "pending" })
        expect(Option.getOrThrow(yield* queue.receipt(different.id, "grammar-b"))).toMatchObject({ state: "pending" })
      }
    }

    for (const delivery of ["steer", "queue"] as const) {
      const same = yield* sessions.create({ title: `${delivery} grammatical target positive` })
      yield* request(same.id, delivery, "same-a", "investigate how CI deployment works: inspect runner")
      yield* request(same.id, delivery, "same-b", "investigate whether CI deployment works: trace callback")
      expect((yield* queue.list(same.id)).map(text)).toEqual([
        "investigate how CI deployment works:\n1. inspect runner\n2. whether CI deployment works: trace callback",
      ])
      expect(Option.getOrThrow(yield* queue.receipt(same.id, "same-b"))).toMatchObject({
        state: "merged",
        into: "same-a",
      })
    }
  }),
)

it.instance("command targets match when one path contains the other", () =>
  Effect.gen(function* () {
    const items = yield* consolidateAdmitted("path suffix", [
      { delivery: "queue", text: "update packages/opencode/src/session/queue.ts: retain receipts" },
      { delivery: "queue", text: "UPDATE src/session/queue.ts: preserve original IDs" },
    ])
    expect(items.map(text)).toEqual([
      "update packages/opencode/src/session/queue.ts:\n1. retain receipts\n2. src/session/queue.ts: preserve original IDs",
    ])
  }),
)

it.instance("consolidation keeps path-like filenames literal", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of literalFilenameTargets) {
        const items = yield* consolidateAdmitted(`${delivery} ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        expect(items.map(text)).toEqual([pair.first, pair.second])
      }
    }
  }),
)

it.instance("request admission keeps path-like filenames literal", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of literalFilenameTargets) {
        const session = yield* sessions.create({ title: `${delivery} ${pair.title}` })
        yield* request(session.id, delivery, "file-a", pair.first)
        yield* request(session.id, delivery, "file-b", pair.second)
        expect((yield* queue.list(session.id)).map(text)).toEqual([pair.first, pair.second])
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "file-a"))).toMatchObject({ state: "pending" })
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "file-b"))).toMatchObject({ state: "pending" })
      }
    }
  }),
)

it.instance("consolidation normalizes case before classifying file identity", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of caseInsensitiveFilenameTargets) {
        const items = yield* consolidateAdmitted(`${delivery} ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        expect(items.map(text)).toEqual(pair.merged ? [pair.merged] : [pair.first, pair.second])
      }
    }
  }),
)

it.instance("request admission normalizes case before classifying file identity", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of caseInsensitiveFilenameTargets) {
        const session = yield* sessions.create({ title: `${delivery} ${pair.title}` })
        yield* request(session.id, delivery, "case-file-a", pair.first)
        yield* request(session.id, delivery, "case-file-b", pair.second)
        const expected = pair.merged ? [pair.merged] : [pair.first, pair.second]
        expect((yield* queue.list(session.id)).map(text)).toEqual(expected)
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "case-file-b"))).toMatchObject(
          pair.merged ? { state: "merged", into: "case-file-a" } : { state: "pending" },
        )
      }
    }
  }),
)

it.instance("consolidation matches curated filenames and keeps other suffixes distinct", () =>
  Effect.gen(function* () {
    const actual: string[][] = []
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of suffixFilenameTargets) {
        const items = yield* consolidateAdmitted(`${delivery} ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        actual.push(items.map(text))
      }
    }
    expect(actual).toEqual(
      ["steer", "queue"].flatMap(() =>
        suffixFilenameTargets.map((pair) => (pair.merged ? [pair.merged] : [pair.first, pair.second])),
      ),
    )
  }),
)

it.instance("request admission matches curated filenames and keeps other suffixes distinct", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const actual: string[][] = []
    const receipts: string[] = []
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of suffixFilenameTargets) {
        const session = yield* sessions.create({ title: `${delivery} ${pair.title}` })
        yield* request(session.id, delivery, "suffix-a", pair.first)
        yield* request(session.id, delivery, "suffix-b", pair.second)
        actual.push((yield* queue.list(session.id)).map(text))
        const receipt = Option.getOrThrow(yield* queue.receipt(session.id, "suffix-b"))
        receipts.push(receipt.state === "merged" ? `${receipt.state}:${receipt.into}` : receipt.state)
      }
    }
    expect(actual).toEqual(
      ["steer", "queue"].flatMap(() =>
        suffixFilenameTargets.map((pair) => (pair.merged ? [pair.merged] : [pair.first, pair.second])),
      ),
    )
    expect(receipts).toEqual(
      ["steer", "queue"].flatMap(() =>
        suffixFilenameTargets.map((pair) => (pair.merged ? "merged:suffix-a" : "pending")),
      ),
    )
  }),
)

it.instance("bare dotted targets match on natural words, while filename extensions stay distinct", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of bareDottedTargets) {
        const items = yield* consolidateAdmitted(`${delivery} ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        expect(items.map(text)).toEqual(pair.merged ? [pair.merged] : [pair.first, pair.second])
      }
    }
  }),
)

it.instance("request admission uses natural words for bare dotted targets without merging filename extensions", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of bareDottedTargets) {
        const session = yield* sessions.create({ title: `${delivery} ${pair.title}` })
        yield* request(session.id, delivery, "bare-dotted-a", pair.first)
        yield* request(session.id, delivery, "bare-dotted-b", pair.second)
        const expected = pair.merged ? [pair.merged] : [pair.first, pair.second]
        expect((yield* queue.list(session.id)).map(text)).toEqual(expected)
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "bare-dotted-b"))).toMatchObject(
          pair.merged ? { state: "merged", into: "bare-dotted-a" } : { state: "pending" },
        )
      }
    }
  }),
)

it.instance("consolidation keeps a dotted runtime name as a natural target", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      const items = yield* consolidateAdmitted(`${delivery} runtime name`, [
        { delivery, text: runtimeNameTarget.first },
        { delivery, text: runtimeNameTarget.second },
      ])
      expect(items.map(text)).toEqual([runtimeNameTarget.merged])
    }
  }),
)

it.instance("request admission keeps a dotted runtime name as a natural target", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      const session = yield* sessions.create({ title: `${delivery} runtime name` })
      yield* request(session.id, delivery, "runtime-a", runtimeNameTarget.first)
      yield* request(session.id, delivery, "runtime-b", runtimeNameTarget.second)
      expect((yield* queue.list(session.id)).map(text)).toEqual([runtimeNameTarget.merged])
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "runtime-b"))).toMatchObject({
        state: "merged",
        into: "runtime-a",
      })
    }
  }),
)

it.instance("dotted runtime names retain different subjects in every delivery class", () =>
  Effect.gen(function* () {
    const actual: string[][] = []
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of dottedRuntimeControls) {
        const items = yield* consolidateAdmitted(`${delivery} ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        actual.push(items.map(text))
      }
    }
    expect(actual).toEqual(
      ["steer", "queue"].flatMap(() => dottedRuntimeControls.map((pair) => [pair.first, pair.second])),
    )
  }),
)

it.instance("request admission retains dotted runtime subjects in every delivery class", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const actual: string[][] = []
    const receipts: string[][] = []
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of dottedRuntimeControls) {
        const session = yield* sessions.create({ title: `${delivery} ${pair.title}` })
        yield* request(session.id, delivery, "runtime-control-a", pair.first)
        yield* request(session.id, delivery, "runtime-control-b", pair.second)
        actual.push((yield* queue.list(session.id)).map(text))
        receipts.push([
          Option.getOrThrow(yield* queue.receipt(session.id, "runtime-control-a")).state,
          Option.getOrThrow(yield* queue.receipt(session.id, "runtime-control-b")).state,
        ])
      }
    }
    expect(actual).toEqual(
      ["steer", "queue"].flatMap(() => dottedRuntimeControls.map((pair) => [pair.first, pair.second])),
    )
    expect(receipts).toEqual(["steer", "queue"].flatMap(() => dottedRuntimeControls.map(() => ["pending", "pending"])))
  }),
)

it.instance("dotted runtime names match their undotted subject in every delivery class", () =>
  Effect.gen(function* () {
    const actual: string[][] = []
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of dottedRuntimeSameJobTargets) {
        const items = yield* consolidateAdmitted(`${delivery} ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        actual.push(items.map(text))
      }
    }
    expect(actual).toEqual(["steer", "queue"].flatMap(() => dottedRuntimeSameJobTargets.map((pair) => [pair.merged])))
  }),
)

it.instance("request admission matches dotted runtime subjects in every delivery class", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const actual: string[][] = []
    const receipts: string[] = []
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of dottedRuntimeSameJobTargets) {
        const session = yield* sessions.create({ title: `${delivery} ${pair.title}` })
        yield* request(session.id, delivery, "runtime-match-a", pair.first)
        yield* request(session.id, delivery, "runtime-match-b", pair.second)
        actual.push((yield* queue.list(session.id)).map(text))
        const receipt = Option.getOrThrow(yield* queue.receipt(session.id, "runtime-match-b"))
        receipts.push(receipt.state === "merged" ? `${receipt.state}:${receipt.into}` : receipt.state)
      }
    }
    expect(actual).toEqual(["steer", "queue"].flatMap(() => dottedRuntimeSameJobTargets.map((pair) => [pair.merged])))
    expect(receipts).toEqual(
      ["steer", "queue"].flatMap(() => dottedRuntimeSameJobTargets.map(() => "merged:runtime-match-a")),
    )
  }),
)

it.instance("consolidation matches a dotted package name through natural containment", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      const items = yield* consolidateAdmitted(`${delivery} package name`, [
        { delivery, text: packageNameTarget.first },
        { delivery, text: packageNameTarget.second },
      ])
      expect(items.map(text)).toEqual([packageNameTarget.merged])
    }
  }),
)

it.instance("request admission matches a dotted package name through natural containment", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      const session = yield* sessions.create({ title: `${delivery} package name` })
      yield* request(session.id, delivery, "package-a", packageNameTarget.first)
      yield* request(session.id, delivery, "package-b", packageNameTarget.second)
      expect((yield* queue.list(session.id)).map(text)).toEqual([packageNameTarget.merged])
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "package-b"))).toMatchObject({
        state: "merged",
        into: "package-a",
      })
    }
  }),
)

it.instance("prose path references do not consolidate different command work", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      const items = yield* consolidateAdmitted(`${delivery} investigation path references`, [
        { delivery, text: differentInvestigationPathReferences.first },
        { delivery, text: differentInvestigationPathReferences.second },
      ])
      expect(items.map(text)).toEqual([
        differentInvestigationPathReferences.first,
        differentInvestigationPathReferences.second,
      ])
    }
  }),
)

it.instance("embedded slash paths remain evidence when filenames differ", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      const items = yield* consolidateAdmitted(`${delivery} embedded path evidence`, [
        { delivery, text: embeddedPathTargets.first },
        { delivery, text: embeddedPathTargets.second },
      ])
      expect(items.map(text)).toEqual([embeddedPathTargets.first, embeddedPathTargets.second])
    }
  }),
)

it.instance("request admission keeps embedded slash paths as differing file evidence", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      const session = yield* sessions.create({ title: `${delivery} embedded path evidence` })
      yield* request(session.id, delivery, "embedded-path-a", embeddedPathTargets.first)
      yield* request(session.id, delivery, "embedded-path-b", embeddedPathTargets.second)
      expect((yield* queue.list(session.id)).map(text)).toEqual([embeddedPathTargets.first, embeddedPathTargets.second])
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "embedded-path-a"))).toMatchObject({ state: "pending" })
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "embedded-path-b"))).toMatchObject({ state: "pending" })
    }
  }),
)

it.instance("request admission does not consolidate different work because prose paths share a suffix", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      const session = yield* sessions.create({ title: `${delivery} separate investigation path references` })
      yield* request(session.id, delivery, "investigation-a", differentInvestigationPathReferences.first)
      yield* request(session.id, delivery, "investigation-b", differentInvestigationPathReferences.second)
      expect((yield* queue.list(session.id)).map(text)).toEqual([
        differentInvestigationPathReferences.first,
        differentInvestigationPathReferences.second,
      ])
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "investigation-a"))).toMatchObject({ state: "pending" })
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "investigation-b"))).toMatchObject({ state: "pending" })
    }
  }),
)

it.instance("consolidation matches a prose file reference from its other target words", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      const items = yield* consolidateAdmitted(`${delivery} login file target words`, [
        { delivery, text: sameWorkProseFileReference.first },
        { delivery, text: sameWorkProseFileReference.second },
      ])
      expect(items.map(text)).toEqual([sameWorkProseFileReference.merged])
    }
  }),
)

it.instance("request admission matches a prose file reference from its other target words", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      const session = yield* sessions.create({ title: `${delivery} login file target words` })
      yield* request(session.id, delivery, "login-file-a", sameWorkProseFileReference.first)
      yield* request(session.id, delivery, "login-file-b", sameWorkProseFileReference.second)
      expect((yield* queue.list(session.id)).map(text)).toEqual([sameWorkProseFileReference.merged])
      expect(Option.getOrThrow(yield* queue.receipt(session.id, "login-file-b"))).toMatchObject({
        state: "merged",
        into: "login-file-a",
      })
    }
  }),
)

it.instance("consolidation extracts the same subject across dependent-clause tails", () =>
  Effect.gen(function* () {
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of sameJobTailTargets) {
        const items = yield* consolidateAdmitted(`${delivery} ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        expect(items.map(text)).toEqual([pair.merged])
      }
    }
  }),
)

it.instance("request admission extracts the same subject across dependent-clause tails", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of sameJobTailTargets) {
        const session = yield* sessions.create({ title: `${delivery} ${pair.title}` })
        yield* request(session.id, delivery, "same-job-a", pair.first)
        yield* request(session.id, delivery, "same-job-b", pair.second)
        expect((yield* queue.list(session.id)).map(text)).toEqual([pair.merged])
        expect(Option.getOrThrow(yield* queue.receipt(session.id, "same-job-b"))).toMatchObject({
          state: "merged",
          into: "same-job-a",
        })
      }
    }
  }),
)

it.instance("consolidation matches word forms through shared lexical roots", () =>
  Effect.gen(function* () {
    const actual: string[][] = []
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of sameJobWordForms) {
        const items = yield* consolidateAdmitted(`${delivery} ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        actual.push(items.map(text))
      }
    }
    expect(actual).toEqual(["steer", "queue"].flatMap(() => sameJobWordForms.map((pair) => [pair.merged])))
  }),
)

it.instance("request admission matches word forms through shared lexical roots", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const actual: string[][] = []
    const receipts: string[] = []
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of sameJobWordForms) {
        const session = yield* sessions.create({ title: `${delivery} ${pair.title}` })
        yield* request(session.id, delivery, "word-form-a", pair.first)
        yield* request(session.id, delivery, "word-form-b", pair.second)
        actual.push((yield* queue.list(session.id)).map(text))
        const receipt = Option.getOrThrow(yield* queue.receipt(session.id, "word-form-b"))
        receipts.push(receipt.state === "merged" ? `${receipt.state}:${receipt.into}` : receipt.state)
      }
    }
    expect(actual).toEqual(["steer", "queue"].flatMap(() => sameJobWordForms.map((pair) => [pair.merged])))
    expect(receipts).toEqual(["steer", "queue"].flatMap(() => sameJobWordForms.map(() => "merged:word-form-a")))
  }),
)

it.instance("consolidation keeps short false-friend stems distinct", () =>
  Effect.gen(function* () {
    const actual: string[][] = []
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of boundedStemControls) {
        const items = yield* consolidateAdmitted(`${delivery} ${pair.title}`, [
          { delivery, text: pair.first },
          { delivery, text: pair.second },
        ])
        actual.push(items.map(text))
      }
    }
    expect(actual).toEqual(
      ["steer", "queue"].flatMap(() => boundedStemControls.map((pair) => [pair.first, pair.second])),
    )
  }),
)

it.instance("request admission keeps short false-friend stems distinct", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const actual: string[][] = []
    const receipts: string[][] = []
    for (const delivery of ["steer", "queue"] as const) {
      for (const pair of boundedStemControls) {
        const session = yield* sessions.create({ title: `${delivery} ${pair.title}` })
        yield* request(session.id, delivery, "bounded-a", pair.first)
        yield* request(session.id, delivery, "bounded-b", pair.second)
        actual.push((yield* queue.list(session.id)).map(text))
        receipts.push([
          Option.getOrThrow(yield* queue.receipt(session.id, "bounded-a")).state,
          Option.getOrThrow(yield* queue.receipt(session.id, "bounded-b")).state,
        ])
      }
    }
    expect(actual).toEqual(
      ["steer", "queue"].flatMap(() => boundedStemControls.map((pair) => [pair.first, pair.second])),
    )
    expect(receipts).toEqual(["steer", "queue"].flatMap(() => boundedStemControls.map(() => ["pending", "pending"])))
  }),
)

it.instance("command delivery retains the later target when similar paths consolidate", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "preserve every command target" })
    yield* admit(session.id, "queue", "update packages/app/src/auth/login.ts: keep redirect checks")
    yield* admit(session.id, "queue", "update src/auth/login.ts: add login fallback")

    const items = yield* queue.consolidate(session.id)
    const merged =
      "update packages/app/src/auth/login.ts:\n1. keep redirect checks\n2. src/auth/login.ts: add login fallback"
    const written: string[] = []
    yield* promoter(session.id, written)("queue")
    expect(written).toEqual([merged])
    expect(items.map(text)).toEqual([merged])
  }),
)

it.instance("command targets match on a distinctive token when neither contains the other", () =>
  Effect.gen(function* () {
    const items = yield* consolidateAdmitted("distinctive target", [
      { delivery: "queue", text: "review authentication receipt routing: preserve origin IDs" },
      { delivery: "queue", text: "review receipt routing in authentication: verify settlement" },
    ])
    expect(items.map(text)).toEqual([
      "review authentication receipt routing:\n1. preserve origin IDs\n2. receipt routing in authentication: verify settlement",
    ])
  }),
)

it.instance("generic target words alone do not merge different command work", () =>
  Effect.gen(function* () {
    const items = yield* consolidateAdmitted("different targets", [
      { delivery: "queue", text: "fix the login bug: disable the redirect loop" },
      { delivery: "queue", text: "fix the signup bug: add the welcome email" },
    ])
    expect(items.map(text)).toEqual([
      "fix the login bug: disable the redirect loop",
      "fix the signup bug: add the welcome email",
    ])
  }),
)

it.instance("command target containment ignores incomplete suffix windows", () =>
  Effect.gen(function* () {
    const prompts = ["fix epsilon beta", "fix gamma delta epsilon"]
    const items = yield* consolidateAdmitted(
      "different command targets with different word counts",
      prompts.map((text) => ({ delivery: "queue" as const, text })),
    )
    expect(items.map(text)).toEqual(prompts)
  }),
)

it.instance("request admission bounds command target containment windows", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "different request targets with different word counts" })
    const prompts = ["fix epsilon beta", "fix gamma delta epsilon"]
    yield* Effect.forEach(prompts, (text, index) => request(session.id, "queue", `target-${index}`, text))
    expect((yield* queue.list(session.id)).map(text)).toEqual(prompts)
    const receipts = yield* Effect.forEach(prompts, (_, index) => queue.receipt(session.id, `target-${index}`))
    expect(receipts.map((receipt) => Option.getOrThrow(receipt).state)).toEqual(["pending", "pending"])
  }),
)

it.instance("an exact generic command target still matches after article normalization", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "generic target equality" })
    yield* admit(session.id, "queue", "fix the bug: preserve the first request")
    yield* admit(session.id, "queue", "FIX a bug: preserve the second request")

    const items = yield* queue.consolidate(session.id)
    const merged = "fix the bug:\n1. preserve the first request\n2. preserve the second request"
    const written: string[] = []
    yield* promoter(session.id, written)("queue")
    expect(written).toEqual([merged])
    expect(items.map(text)).toEqual([merged])
  }),
)

it.instance("a shared path directory does not match different file targets", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "different files under a shared directory" })
    const expected = [
      "update src/login/auth.ts: keep redirect checks",
      "update tests/login/signup.ts: add signup coverage",
    ]
    yield* admit(session.id, "queue", expected[0]!)
    yield* admit(session.id, "queue", expected[1]!)

    const items = yield* queue.consolidate(session.id)
    const written: string[] = []
    yield* promoter(session.id, written)("queue")
    yield* promoter(session.id, written)("queue")
    expect(written).toEqual(expected)
    expect(items.map(text)).toEqual(expected)
  }),
)

it.instance("a shared file extension alone does not identify a command target", () =>
  Effect.gen(function* () {
    const items = yield* consolidateAdmitted("different files", [
      { delivery: "queue", text: "update alpha.ts: preserve the first module" },
      { delivery: "queue", text: "update beta.ts: preserve the second module" },
    ])
    expect(items.map(text)).toEqual([
      "update alpha.ts: preserve the first module",
      "update beta.ts: preserve the second module",
    ])
  }),
)

it.instance("plain text copies match after case and whitespace normalization", () =>
  Effect.gen(function* () {
    const items = yield* consolidateAdmitted("normalized text", [
      { delivery: "queue", text: "Please summarize the queue" },
      { delivery: "queue", text: " please   SUMMARIZE THE queue " },
    ])
    expect(items.map(text)).toEqual(["Please summarize the queue"])
    expect(items[0]!.origins).toHaveLength(2)
  }),
)

it.instance("normalized command copies consolidate before command parsing", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "normalized command copies" })
    yield* admit(session.id, "queue", "RUN   THE   SUITE")
    yield* admit(session.id, "queue", " run the suite ")

    expect(yield* queue.list(session.id)).toHaveLength(2)
    const items = yield* queue.consolidate(session.id)
    const written: string[] = []
    yield* promoter(session.id, written)("queue")
    expect(written).toEqual(["RUN   THE   SUITE"])
    expect(items.map(text)).toEqual(["RUN   THE   SUITE"])
  }),
)

it.instance("same-target command prefixes retain ordered steps through delivery", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "same-target command prefix" })
    yield* admit(session.id, "queue", "run lint")
    yield* admit(session.id, "queue", "run lint --fix")

    expect(yield* queue.list(session.id)).toHaveLength(2)
    const items = yield* queue.consolidate(session.id)
    const merged = "run lint:\n1. run lint\n2. --fix"
    const written: string[] = []
    yield* promoter(session.id, written)("queue")
    expect(written).toEqual([merged])
    expect(items.map(text)).toEqual([merged])
  }),
)

it.instance("command text prefixes retain the longer instruction through delivery", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "command text prefix" })
    yield* admit(session.id, "queue", "run the suite")
    yield* admit(session.id, "queue", "run the suite with coverage")

    expect(yield* queue.list(session.id)).toHaveLength(2)
    const items = yield* queue.consolidate(session.id)
    const written: string[] = []
    yield* promoter(session.id, written)("queue")
    expect(written).toEqual(["run the suite with coverage"])
    expect(items.map(text)).toEqual(["run the suite with coverage"])
  }),
)

it.instance("plain text prefixes retain the longer instruction and both queue origins", () =>
  Effect.gen(function* () {
    const prompts = ["Please summarize the queue", "please summarize the queue in detail"]
    const items = yield* consolidateAdmitted(
      "prefix text",
      prompts.map((text) => ({ delivery: "queue" as const, text })),
    )
    expect(items.map(text)).toEqual([prompts[1]])
    expect(items[0]!.origins).toHaveLength(2)
  }),
)

it.instance("prefix text consolidation settles request receipts through delivery", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "prefix text receipts" })
    const prompts = ["Please summarize the queue", "Please summarize the queue in detail"]
    yield* request(session.id, "queue", "prefix-a", prompts[0]!)
    yield* request(session.id, "queue", "prefix-b", prompts[1]!)
    expect(yield* queue.list(session.id)).toHaveLength(2)
    const [merged] = yield* queue.consolidate(session.id)
    expect([text(merged!), merged!.origins.length]).toEqual([prompts[1], 2])
    expect(Option.getOrThrow(yield* queue.receipt(session.id, "prefix-b"))).toEqual({
      state: "merged",
      into: "prefix-a",
    })
    const written: string[] = []
    yield* promoter(session.id, written)("queue")
    expect(written).toEqual([prompts[1]])
    expect(Option.getOrThrow(yield* queue.receipt(session.id, "prefix-b"))).toMatchObject({
      state: "merged",
      into: "prefix-a",
      messageID: expect.any(String),
    })
  }),
)

it.instance("plain text with distinct work stays separate without a prefix relation", () =>
  Effect.gen(function* () {
    const items = yield* consolidateAdmitted("different text", [
      { delivery: "queue", text: "Please summarize the queue" },
      { delivery: "queue", text: "Please summarize the prompt" },
    ])
    expect(items.map(text)).toEqual(["Please summarize the queue", "Please summarize the prompt"])
  }),
)

it.instance("exact copies stay in their delivery class while normalized same-class prompts still merge", () =>
  Effect.gen(function* () {
    const separate = yield* consolidateAdmitted("exact delivery separation", [
      { delivery: "steer", text: "Please check the queue" },
      { delivery: "queue", text: "Please check the queue" },
    ])
    expect(separate.map((item) => [item.delivery, text(item)])).toEqual([
      ["steer", "Please check the queue"],
      ["queue", "Please check the queue"],
    ])
    const merged = yield* consolidateAdmitted("normalized same-class copies", [
      { delivery: "steer", text: "Please check the queue" },
      { delivery: "steer", text: " please   CHECK THE queue " },
    ])
    expect(merged.map((item) => [item.delivery, text(item)])).toEqual([["steer", "Please check the queue"]])
  }),
)

it.instance("normalized prompt equality stays in its delivery class", () =>
  Effect.gen(function* () {
    const items = yield* consolidateAdmitted("normalized delivery separation", [
      { delivery: "steer", text: "Please summarize the queue" },
      { delivery: "queue", text: " please   SUMMARIZE THE queue " },
    ])
    expect(items.map((item) => [item.delivery, text(item)])).toEqual([
      ["steer", "Please summarize the queue"],
      ["queue", " please   SUMMARIZE THE queue "],
    ])
  }),
)

it.instance("prompt prefixes stay in their delivery class", () =>
  Effect.gen(function* () {
    const items = yield* consolidateAdmitted("prefix delivery separation", [
      { delivery: "steer", text: "Please summarize the queue" },
      { delivery: "queue", text: "Please summarize the queue in detail" },
    ])
    expect(items.map((item) => [item.delivery, text(item)])).toEqual([
      ["steer", "Please summarize the queue"],
      ["queue", "Please summarize the queue in detail"],
    ])
  }),
)

it.instance("similar commands stay in their delivery class", () =>
  Effect.gen(function* () {
    const items = yield* consolidateAdmitted("command delivery separation", [
      { delivery: "steer", text: "update packages/opencode/src/session/queue.ts: retain provenance" },
      { delivery: "queue", text: "UPDATE src/session/queue.ts: preserve receipts" },
    ])
    expect(items.map((item) => [item.delivery, text(item)])).toEqual([
      ["steer", "update packages/opencode/src/session/queue.ts: retain provenance"],
      ["queue", "UPDATE src/session/queue.ts: preserve receipts"],
    ])
  }),
)

it.instance("receipt-bearing requests keep different modifiers apart on a shared target", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "receipt command match" })
    yield* request(session.id, "queue", "auth-a", "review authentication receipt routing: preserve origin IDs")
    yield* request(session.id, "queue", "auth-b", "review billing receipt routing: verify settlement")
    const items = yield* queue.list(session.id)
    expect(items.map(text)).toEqual([
      "review authentication receipt routing: preserve origin IDs",
      "review billing receipt routing: verify settlement",
    ])
    expect(Option.getOrThrow(yield* queue.receipt(session.id, "auth-a"))).toMatchObject({ state: "pending" })
    expect(Option.getOrThrow(yield* queue.receipt(session.id, "auth-b"))).toMatchObject({ state: "pending" })
  }),
)

it.instance("receipt-bearing requests merge contained targets for the same job", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "receipt command containment" })
    yield* request(session.id, "queue", "auth-a", "review authentication receipt routing: preserve origin IDs")
    yield* request(session.id, "queue", "auth-b", "review receipt routing in authentication: verify settlement")
    expect((yield* queue.list(session.id)).map(text)).toEqual([
      "review authentication receipt routing:\n1. preserve origin IDs\n2. receipt routing in authentication: verify settlement",
    ])
    expect(Option.getOrThrow(yield* queue.receipt(session.id, "auth-b"))).toMatchObject({
      state: "merged",
      into: "auth-a",
    })
  }),
)

it.instance("request admission ignores a shared path directory when file targets differ", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "receipt command path boundary" })
    yield* request(session.id, "queue", "path-a", "update src/login/auth.ts: preserve the redirect")
    yield* request(session.id, "queue", "path-b", "update tests/login/signup.ts: cover registration")

    expect((yield* queue.list(session.id)).map(text)).toEqual([
      "update src/login/auth.ts: preserve the redirect",
      "update tests/login/signup.ts: cover registration",
    ])
    expect(Option.getOrThrow(yield* queue.receipt(session.id, "path-a"))).toMatchObject({ state: "pending" })
    expect(Option.getOrThrow(yield* queue.receipt(session.id, "path-b"))).toMatchObject({ state: "pending" })
    const expected = [
      "update src/login/auth.ts: preserve the redirect",
      "update tests/login/signup.ts: cover registration",
    ]
    expect((yield* queue.consolidate(session.id)).map(text)).toEqual(expected)
    const written: string[] = []
    yield* promoter(session.id, written)("queue")
    yield* promoter(session.id, written)("queue")
    expect(written).toEqual(expected)
  }),
)

it.instance("configured critical markers still protect matching receipt-bearing commands", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "protected command marker" })
    const markers = { critical: [{ type: "prefix" as const, value: "fix auth.ts" }] }
    const inputs = [
      {
        sessionID: session.id,
        delivery: "queue" as const,
        requestID: "protected-a",
        parts: [{ type: "text" as const, text: "fix auth.ts: preserve the first origin" }],
      },
      {
        sessionID: session.id,
        delivery: "queue" as const,
        requestID: "protected-b",
        parts: [{ type: "text" as const, text: "fix auth.ts: preserve the second origin" }],
      },
    ]
    yield* Effect.forEach(inputs, (input) => queue.request(input, markers))
    expect((yield* queue.list(session.id)).map(text)).toEqual(inputs.map((input) => input.parts[0]!.text))
    expect((yield* queue.consolidate(session.id, markers)).map(text)).toEqual(
      inputs.map((input) => input.parts[0]!.text),
    )
    expect(Option.getOrThrow(yield* queue.receipt(session.id, "protected-a")).state).toBe("pending")
    expect(Option.getOrThrow(yield* queue.receipt(session.id, "protected-b")).state).toBe("pending")
  }),
)

it.instance("configured critical markers also protect free-text prefix matching", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const queue = yield* SessionQueue.Service
    const session = yield* sessions.create({ title: "protected text marker" })
    const markers = { critical: [{ type: "prefix" as const, value: "Please" }] }
    const prompts = ["Please summarize the queue", "Please summarize the queue in detail"]
    yield* Effect.forEach(prompts, (text) => admit(session.id, "queue", text))
    expect((yield* queue.consolidate(session.id, markers)).map(text)).toEqual(prompts)
  }),
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
      for (const [requestID, value] of notices) yield* request(session.id, "queue", requestID, value, "tui-composer")
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

// swxtchio/swx-opencode#215: a held sync caller waits for its own item until the hold ends.
it.instance(
  "a caller waits for its held item until the lease lapses, and ends withdrawn if it is deleted",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "held caller" })
      const item = yield* admit(session.id, "steer", "waits for the lapse")
      expect(yield* queue.disposition(session.id, item.id)).toBe("pending")

      yield* queue.hold(session.id, "view")
      const waiting = yield* queue.disposition(session.id, item.id).pipe(Effect.forkChild({ startImmediately: true }))
      yield* TestClock.adjust("5 minutes")
      expect((yield* queue.hold(session.id, "view")).started).toBe(false)
      yield* TestClock.adjust("5 minutes")
      for (let turn = 0; turn < 100; turn++) yield* Effect.yieldNow
      expect(waiting.pollUnsafe()).toBeUndefined()
      yield* TestClock.adjust("1 minute")
      // A timeout would run on the test clock too, so a bounded number of turns
      // lets the woken waiter finish instead.
      for (let turn = 0; turn < 100 && waiting.pollUnsafe() === undefined; turn++) yield* Effect.yieldNow
      const lapsed = waiting.pollUnsafe()
      expect(lapsed && Exit.isSuccess(lapsed) ? lapsed.value : undefined).toBe("pending")
      expect((yield* queue.list(session.id)).map(text)).toEqual(["waits for the lapse"])

      const doomed = yield* admit(session.id, "queue", "deleted while held")
      yield* queue.hold(session.id, "view")
      const deleted = yield* queue.disposition(session.id, doomed.id).pipe(Effect.forkChild({ startImmediately: true }))
      expect(Option.isSome(yield* queue.remove(session.id, doomed.id))).toBe(true)
      expect(yield* queue.release(session.id, "view")).toBe(true)
      for (let turn = 0; turn < 100 && deleted.pollUnsafe() === undefined; turn++) yield* Effect.yieldNow
      const ended = deleted.pollUnsafe()
      expect(ended && Exit.isFailure(ended) && Cause.squash(ended.cause)).toBeInstanceOf(SessionQueue.WithdrawnError)
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
  "consolidate keeps copies within their delivery class and merges similar commands into ordered steps",
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
        ["queue", "run the full suite"],
        ["queue", "fix auth.ts: queued step"],
      ])

      // A later similar command adds a step to the merged one.
      yield* admit(session.id, "steer", "fix auth.ts: update the docs")
      expect((yield* queue.consolidate(session.id)).map(text)[0]).toBe(
        "fix auth.ts:\n1. handle a null token\n2. add a regression test\n3. update the docs",
      )

      yield* promote("steer")
      yield* promote("queue")
      yield* promote("queue")
      expect(written).toEqual([
        "fix auth.ts:\n1. handle a null token\n2. add a regression test\n3. update the docs",
        "review auth.ts: check the naming",
        "fix login.ts: tidy the imports",
        "run the full suite",
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
  "a prompt admitted while its sender prepares it is listed at once, holds nothing up, and is promoted only once prepared",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "review admitted first" })
      const written: string[] = []
      const promote = promoter(session.id, written)
      const receipt = (requestID: string) =>
        queue.receipt(session.id, SessionQueue.RequestID.make(requestID)).pipe(Effect.map(Option.getOrUndefined))
      const first = yield* queue.request(
        { sessionID: session.id, delivery: "steer", requestID: "first", parts: [{ type: "text", text: "go" }] },
        undefined,
        true,
      )
      if (first.kind !== "admitted") throw new Error("the prompt was not admitted")
      expect((yield* queue.list(session.id)).map(text)).toEqual(["go"])
      expect(yield* receipt("first")).toEqual({ state: "pending", itemID: first.own.id })
      // No drain could promote it yet; its `prepared` wakes the session.
      expect(yield* queue.awaitingDrain(session.id)).toBe(false)
      // Nothing waits on its preparation: another prompt is admitted and
      // promoted past it, and an identical copy is not merged into it.
      yield* request(session.id, "steer", "second", "lint")
      yield* request(session.id, "steer", "copy", "go")
      expect((yield* queue.list(session.id)).map(text)).toEqual(["go", "lint", "go"])
      yield* promote("steer")
      expect(written).toEqual(["lint", "go"])
      expect((yield* queue.list(session.id)).map(text)).toEqual(["go"])
      // Prepared, it takes the delivery its sender decided.
      yield* queue.prepared({ sessionID: session.id, itemID: first.own.id, delivery: "queue" })
      expect((yield* queue.list(session.id)).map((item) => item.delivery)).toEqual(["queue"])
      yield* promote("queue")
      expect(written).toEqual(["lint", "go", "go"])
      expect((yield* receipt("first"))?.state).toBe("delivered")
    }),
  30_000,
)

it.instance(
  "a prompt that finishes preparing after later ones arrived is admitted as the earlier of them",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const session = yield* sessions.create({ title: "late preparation" })
      const receipt = (requestID: string) =>
        queue.receipt(session.id, SessionQueue.RequestID.make(requestID)).pipe(Effect.map(Option.getOrUndefined))
      const preparingRequest = (requestID: string, value: string) =>
        queue.request(
          {
            sessionID: session.id,
            delivery: "queue",
            requestID: SessionQueue.RequestID.make(requestID),
            parts: [{ type: "text", text: value }],
          },
          undefined,
          true,
        )
      const status = (text: string) => `[fm-from-firstmate]\x1f[fm-level:nudge:status]\x1f ${text}`

      // A newer keyed notice that arrived while the older one prepared still supersedes it.
      const older = yield* preparingRequest("older", status("status: building"))
      if (older.kind !== "admitted") throw new Error("the older notice was not admitted")
      yield* request(session.id, "queue", "newer", status("status: tests running"))
      yield* queue.prepared({ sessionID: session.id, itemID: older.own.id, delivery: "queue" })
      expect((yield* queue.list(session.id)).map(text)).toEqual([status("status: tests running")])
      expect(yield* receipt("older")).toEqual({ state: "superseded", by: "newer" })
      expect((yield* receipt("newer"))?.state).toBe("pending")

      // A later identical copy takes the earlier one in, as it would have had it been ready.
      const first = yield* preparingRequest("first", "check the build")
      if (first.kind !== "admitted") throw new Error("the first copy was not admitted")
      yield* request(session.id, "queue", "second", "check the build")
      yield* queue.prepared({ sessionID: session.id, itemID: first.own.id, delivery: "queue" })
      expect((yield* queue.list(session.id)).map(text)).toEqual([status("status: tests running"), "check the build"])
      expect(yield* receipt("first")).toEqual({ state: "merged", into: "second" })
      expect((yield* receipt("second"))?.state).toBe("pending")
    }),
  30_000,
)

it.instance(
  "a late preparation keeps admission order when a later prompt took an earlier delivery slot",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      // Ready A, then B whose sender is still preparing it, then ready C, all
      // queued under requests; then B is prepared.
      const late = (title: string, values: readonly [string, string, string], fails = false) =>
        Effect.gen(function* () {
          const session = yield* sessions.create({ title })
          const a = yield* request(session.id, "queue", "a", values[0])
          if (a.kind !== "admitted") throw new Error("A was not admitted")
          const b = yield* queue.request(
            {
              sessionID: session.id,
              delivery: "queue",
              requestID: SessionQueue.RequestID.make("b"),
              parts: [{ type: "text", text: values[1] }],
            },
            undefined,
            true,
          )
          if (b.kind !== "admitted") throw new Error("B was not admitted")
          yield* request(session.id, "queue", "c", values[2])
          yield* fails
            ? queue.unprepared(session.id, b.own.id)
            : queue.prepared({ sessionID: session.id, itemID: b.own.id, delivery: "queue" })
          const receipt = (requestID: string) =>
            queue.receipt(session.id, SessionQueue.RequestID.make(requestID)).pipe(Effect.map(Option.getOrUndefined))
          return {
            session,
            a: a.own,
            b: b.own,
            items: yield* queue.list(session.id),
            receipts: [yield* receipt("a"), yield* receipt("b"), yield* receipt("c")],
          }
        })
      const status = (text: string) => `[fm-from-firstmate]\x1f[fm-level:nudge:status]\x1f ${text}`

      // The newest notice survives, and each one is superseded by the next.
      const notices = yield* late("late supersede", [
        status("status: building"),
        status("status: compiling"),
        status("status: tests running"),
      ])
      expect(notices.items.map(text)).toEqual([status("status: tests running")])
      expect(notices.receipts.map((receipt) => receipt && receipt.state)).toEqual([
        "superseded",
        "superseded",
        "pending",
      ])
      expect(notices.receipts.slice(0, 2)).toEqual([
        { state: "superseded", by: "b" },
        { state: "superseded", by: "c" },
      ])
      // B's own caller follows what took it in.
      expect(Option.isSome(yield* queue.withdraw(notices.session.id, notices.items[0].id))).toBe(true)
      expect(yield* queue.withdrawn(notices.session.id, notices.b.id)).toBe(true)

      // Had B's preparation failed, C still supersedes A once B is gone.
      const dropped = yield* late(
        "late supersede, failed",
        [status("status: building"), status("status: compiling"), status("status: tests running")],
        true,
      )
      expect(dropped.items.map(text)).toEqual([status("status: tests running")])
      expect(dropped.receipts).toEqual([
        { state: "superseded", by: "c" },
        undefined,
        { state: "pending", itemID: dropped.items[0].id },
      ])
      expect(Option.isSome(yield* queue.withdraw(dropped.session.id, dropped.items[0].id))).toBe(true)
      expect(yield* queue.withdrawn(dropped.session.id, dropped.a.id)).toBe(true)

      // The newest identical copy takes the earlier ones in.
      const copies = yield* late("late copies", ["check the build", "check the build", "check the build"])
      expect(copies.items.map((item) => [text(item), item.requestID])).toEqual([["check the build", "c"]])
      expect(copies.receipts.slice(1)).toEqual([
        { state: "merged", into: "c" },
        { state: "pending", itemID: copies.items[0].id },
      ])

      // A command's steps stay in the order they were sent.
      const steps = yield* late("late steps", [
        "fix auth.ts: handle a null token",
        "fix auth.ts: add a regression test",
        "fix auth.ts: update the docs",
      ])
      expect(steps.items.map(text)).toEqual([
        "fix auth.ts:\n1. handle a null token\n2. add a regression test\n3. update the docs",
      ])
    }),
  30_000,
)

it.instance(
  "a prompt withdrawn while its sender prepares it stays withdrawn whether the preparation succeeds or fails",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const { db } = yield* Database.Service
      const session = yield* sessions.create({ title: "withdrawn in preparation" })
      const written: string[] = []
      const receipt = (requestID: string) =>
        queue.receipt(session.id, SessionQueue.RequestID.make(requestID)).pipe(Effect.map(Option.getOrUndefined))
      const input = (requestID: string, value: string) => ({
        sessionID: session.id,
        delivery: "steer" as const,
        requestID: SessionQueue.RequestID.make(requestID),
        parts: [{ type: "text" as const, text: value }],
      })

      // Prepared after UP took it: nothing is delivered, and its request stays withdrawn.
      const prepared = yield* queue.request(input("prepared", "prepared after"), undefined, true)
      if (prepared.kind !== "admitted") throw new Error("the prompt was not admitted")
      expect(Option.isSome(yield* queue.withdraw(session.id, prepared.own.id))).toBe(true)
      yield* queue.prepared({ sessionID: session.id, itemID: prepared.own.id, delivery: "steer" })
      yield* promoter(session.id, written)("steer")
      expect(written).toEqual([])
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* receipt("prepared")).toEqual({ state: "withdrawn", itemID: prepared.own.id })

      // Its preparation failed after UP took it: still withdrawn, so an exact
      // retry is told so, and nothing would prepare it if it were put back.
      const failed = yield* queue.request(input("failed", "failed after"), undefined, true)
      if (failed.kind !== "admitted") throw new Error("the prompt was not admitted")
      expect(Option.isSome(yield* queue.withdraw(session.id, failed.own.id))).toBe(true)
      yield* queue.unprepared(session.id, failed.own.id)
      expect(yield* receipt("failed")).toEqual({ state: "withdrawn", itemID: failed.own.id })
      const retried = yield* queue.request(input("failed", "failed after"), undefined, true)
      expect(retried.kind === "retry" ? retried.receipt : retried.kind).toEqual({
        state: "withdrawn",
        itemID: failed.own.id,
      })
      expect(Option.isNone(yield* queue.restore(session.id, failed.own.id))).toBe(true)
      expect(yield* queue.list(session.id)).toEqual([])

      // Left preparing by a stopped process after UP took it: the sweep keeps it withdrawn.
      const left = yield* request(session.id, "steer", "left", "left after")
      if (left.kind !== "admitted") throw new Error("the prompt was not admitted")
      expect(Option.isSome(yield* queue.withdraw(session.id, left.own.id))).toBe(true)
      yield* db
        .update(SessionPromptQueueTable)
        .set({ time_preparing: Date.now() })
        .where(eq(SessionPromptQueueTable.id, left.own.id))
        .run()
      yield* promoter(session.id, written)("steer")
      expect(yield* receipt("left")).toEqual({ state: "withdrawn", itemID: left.own.id })
      expect(Option.isNone(yield* queue.restore(session.id, left.own.id))).toBe(true)
      expect(written).toEqual([])
    }),
  30_000,
)

it.instance(
  "a prompt whose preparation failed goes as if never sent, and so does one a stopped process left preparing",
  () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const queue = yield* SessionQueue.Service
      const { db } = yield* Database.Service
      const session = yield* sessions.create({ title: "review unprepared" })
      const written: string[] = []
      const receipt = (requestID: string) =>
        queue.receipt(session.id, SessionQueue.RequestID.make(requestID)).pipe(Effect.map(Option.getOrUndefined))
      const input = {
        sessionID: session.id,
        delivery: "steer" as const,
        requestID: SessionQueue.RequestID.make("failed"),
        parts: [{ type: "text" as const, text: "go" }],
      }
      const failed = yield* queue.request(input, undefined, true)
      if (failed.kind !== "admitted") throw new Error("the prompt was not admitted")
      yield* queue.unprepared(session.id, failed.own.id)
      expect(yield* queue.list(session.id)).toEqual([])
      expect(yield* receipt("failed")).toBeUndefined()
      // Its exact retry is admitted again rather than told it was withdrawn.
      const retried = yield* queue.request(input, undefined, true)
      if (retried.kind !== "admitted") throw new Error("the retry was not admitted")
      expect(retried.own.id).not.toBe(failed.own.id)

      // A row still preparing that this process does not know of was left by a
      // stopped process, whose sender is gone.
      const left = yield* request(session.id, "steer", "left", "lint")
      if (left.kind !== "admitted") throw new Error("the prompt was not admitted")
      yield* db
        .update(SessionPromptQueueTable)
        .set({ time_preparing: Date.now() })
        .where(eq(SessionPromptQueueTable.id, left.own.id))
        .run()
      yield* promoter(session.id, written)("steer")
      expect(written).toEqual([])
      expect((yield* queue.list(session.id)).map((item) => item.id)).toEqual([retried.own.id])
      expect(yield* receipt("left")).toBeUndefined()
    }),
  30_000,
)
