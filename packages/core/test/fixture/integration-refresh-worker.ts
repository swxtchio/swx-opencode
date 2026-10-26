import { createInterface } from "node:readline"
import { Cause, Context, Effect, Exit, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Credential } from "@opencode-ai/core/credential"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { Global } from "@opencode-ai/core/global"
import { Integration } from "@opencode-ai/core/integration"

type Input = {
  readonly databasePath: string
  readonly lockRoot: string
  readonly integrationID: string
  readonly methodID: string
  readonly credentialID: string
  readonly label: string
  readonly role: "first" | "second"
  readonly firstFails: boolean
}

type Control = { readonly type: string }

const input = JSON.parse(process.argv[2]!) as Input
const lines = createInterface({ input: process.stdin, crlfDelay: Infinity })
const controls = new Map<string, Control[]>()
const waiters = new Map<string, ((value: Control) => void)[]>()

lines.on("line", (line) => {
  const value = JSON.parse(line) as Control
  const waiter = waiters.get(value.type)?.shift()
  if (waiter) waiter(value)
  else controls.set(value.type, [...(controls.get(value.type) ?? []), value])
})

function send(value: object) {
  process.stdout.write(`${JSON.stringify(value)}\n`)
}

function waitFor(type: string) {
  return new Promise<Control>((resolve) => {
    const queued = controls.get(type)
    if (queued?.length) {
      const [value, ...rest] = queued
      controls.set(type, rest)
      resolve(value!)
      return
    }
    waiters.set(type, [...(waiters.get(type) ?? []), resolve])
  })
}

const program = Effect.gen(function* () {
  const storage = yield* Layer.build(
    AppNodeBuilder.build(LayerNode.group([Credential.node, EventV2.node]), [
      [Database.node, Database.layerFromPath(input.databasePath)],
      [Global.node, Global.layerWith({ state: input.lockRoot })],
    ]),
  )
  const credentials = Context.get(storage, Credential.Service)
  const events = Context.get(storage, EventV2.Service)
  const credentialID = Credential.ID.make(input.credentialID)
  const credential = yield* credentials.get(credentialID)
  if (!credential) {
    const rows = yield* credentials.all()
    return yield* Effect.die(
      new Error(
        `Shared credential ${input.credentialID} was not visible at ${input.databasePath}; rows: ${rows.map((row) => row.id).join(",")}`,
      ),
    )
  }

  const observed = Credential.Service.of({
    all: credentials.all,
    list: credentials.list,
    get: credentials.get,
    lock: (id, operation) =>
      Effect.promise(async () => {
        send({ type: "lock-entered", credentialID: id })
        await waitFor("continue-lock")
        send({ type: "lock-started" })
      }).pipe(Effect.andThen(credentials.lock(id, operation))),
    create: credentials.create,
    update: credentials.update,
    remove: credentials.remove,
  })
  const integrationContext = yield* Layer.build(
    AppNodeBuilder.build(Integration.node, [
      [Credential.node, Layer.succeed(Credential.Service, observed)],
      [EventV2.node, Layer.succeed(EventV2.Service, events)],
    ]),
  )
  const integration = Context.get(integrationContext, Integration.Service)
  const integrationID = Integration.ID.make(input.integrationID)
  const methodID = Integration.MethodID.make(input.methodID)

  yield* integration.transform((editor) =>
    editor.method.update({
      integrationID,
      method: { id: methodID, type: "oauth", label: "OAuth" },
      authorize: () => Effect.die("unused sign-in"),
      refresh: (value) =>
        Effect.gen(function* () {
          send({ type: "refresh-entered", role: input.role, refresh: value.refresh })
          if (input.role === "first") {
            yield* Effect.promise(() => waitFor("release-refresh"))
            if (input.firstFails) return yield* Effect.fail(new Error("controlled OAuth refresh failure"))
          }
          return Credential.OAuth.make({
            ...value,
            access: `${input.role}-access`,
            refresh: `${input.role}-refresh`,
            expires: Number.MAX_SAFE_INTEGER,
          })
        }),
    }),
  )
  send({ type: "ready", access: credential.value.type === "oauth" ? credential.value.access : undefined })
  yield* Effect.promise(() => waitFor("resolve"))
  send({ type: "resolver-started" })
  const connection = {
    type: "credential" as const,
    id: credentialID,
    label: input.label,
    method: "oauth" as const,
  }
  const result = yield* Effect.exit(integration.connection.resolve(connection))
  if (Exit.isSuccess(result)) {
    send({ type: "result", success: true, value: result.value })
    return
  }
  const error = Cause.squash(result.cause)
  send({
    type: "result",
    success: false,
    errorType: error instanceof Integration.AuthorizationError ? error._tag : "unknown",
    message: error instanceof Error ? error.message : String(error),
  })
})

await Effect.runPromise(program.pipe(Effect.scoped))
  .catch((error) => {
    const message = error instanceof Error ? (error.stack ?? error.message) : String(error)
    process.stderr.write(message)
    process.exitCode = 1
  })
  .finally(() => lines.close())
