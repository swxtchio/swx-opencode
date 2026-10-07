export * as Database from "./database"

import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { layer as sqliteLayer } from "#sqlite"
import { Context, Effect, Layer } from "effect"
import { Global } from "../global"
import { Flag } from "../flag/flag"
import { DatabaseMigration } from "./migration"
import { resolvePath } from "./resolve-path"
import { InstallationChannel } from "../installation/version"
import { makeGlobalNode } from "../effect/app-node"

const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()
const nativeBusyTimeoutMs = 5
type DatabaseShape = Effect.Success<typeof makeDatabase>

export interface Interface {
  db: DatabaseShape
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/storage/Database") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = yield* makeDatabase

    yield* db.run(`PRAGMA busy_timeout = ${nativeBusyTimeoutMs}`)
    yield* db.run("PRAGMA cache_size = -64000")
    yield* db.run("PRAGMA foreign_keys = ON")
    yield* DatabaseMigration.apply(db)
    // Let fresh files configure auto-vacuum while table-less before WAL initialization.
    yield* db.run("PRAGMA journal_mode = WAL")
    yield* db.run("PRAGMA synchronous = NORMAL")
    yield* db.run("PRAGMA wal_checkpoint(PASSIVE)")

    return { db }
  }).pipe(Effect.orDie),
)

export function layerFromPath(filename: string) {
  return layer.pipe(Layer.provide(sqliteLayer({ filename, disableWAL: true })))
}

export function path() {
  return resolvePath({
    database: Flag.OPENCODE_DB,
    data: Global.Path.data,
    channel: InstallationChannel,
    disableChannelDb: process.env.OPENCODE_DISABLE_CHANNEL_DB,
  })
}

export const node = makeGlobalNode({ service: Service, layer: layerFromPath(path()), deps: [Global.node] })
