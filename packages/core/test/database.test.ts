import { describe, expect, test } from "bun:test"
import { chmod, mkdir, stat, symlink } from "node:fs/promises"
import path from "path"
import { Context, Effect, Layer } from "effect"
import { Database } from "../src/database/database"
import { Flag } from "../src/flag/flag"
import { createPrivateDatabaseFile, restrictToOwner } from "../src/database/file-permissions"
import { Global } from "../src/global"
import { tmpdir } from "./fixture/tmpdir"

async function startLockContender(filename: string) {
  const child = Bun.spawn(["bun", "-e", contenderScript], {
    cwd: import.meta.dir,
    env: { ...process.env, SQLITE_PERMISSION_DB: filename },
    stdin: "pipe",
    stdout: "pipe",
    stderr: "inherit",
  })
  const reader = child.stdout.getReader()
  const decoder = new TextDecoder()
  let output = ""
  let released = false
  const readLine = async () => {
    while (!output.includes("\n")) {
      const chunk = await reader.read()
      if (chunk.done) throw new Error(`SQLite lock contender exited: ${output}`)
      output += decoder.decode(chunk.value, { stream: true })
    }
    const boundary = output.indexOf("\n")
    const line = output.slice(0, boundary)
    output = output.slice(boundary + 1)
    return line
  }
  const command = async (value: string) => {
    child.stdin.write(`${value}\n`)
    await child.stdin.flush()
    return readLine()
  }

  if ((await readLine()) !== "ready") throw new Error("SQLite lock contender did not become ready")
  return {
    probe: () => command("try"),
    release: async () => {
      if (released) return
      released = true
      child.stdin.write("exit\n")
      await child.stdin.flush()
      child.stdin.end()
      if ((await readLine()) !== "closed") throw new Error("SQLite lock contender did not close")
      await child.exited
      reader.releaseLock()
    },
  }
}

const contenderScript = `
import { Database } from "bun:sqlite"

const db = new Database(process.env.SQLITE_PERMISSION_DB!)
db.run("PRAGMA busy_timeout = 0")
console.log("ready")
const readCommand = () => new Promise((resolve) => process.stdin.once("data", resolve))
while (String(await readCommand()).trim() !== "exit") {
  try {
    db.run("BEGIN IMMEDIATE")
    db.run("ROLLBACK")
    console.log("acquired")
  } catch (error) {
    const code = typeof error === "object" && error !== null && "code" in error ? error.code : "unknown"
    console.log("blocked:" + code)
  }
}
db.close()
console.log("closed")
`

const seedExistingPermissionDatabase = (filename: string) =>
  Effect.gen(function* () {
    const context = yield* Layer.build(Database.layerFromPath(filename))
    const client = Context.get(context, Database.Service).db.$client
    yield* client.unsafe("CREATE TABLE permission_symlink (value TEXT NOT NULL)").raw
    yield* client.unsafe("INSERT INTO permission_symlink (value) VALUES ('preserved')").raw
    yield* Effect.promise(() =>
      Promise.all(["", "-wal", "-shm"].map((suffix) => chmod(filename + suffix, 0o644))),
    )
  })

describe.skipIf(process.platform === "win32")("Database file permissions", () => {
  test("creates a fresh SQLite file owner-only before later tightening", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "private-at-birth.sqlite")

    expect(createPrivateDatabaseFile(filename)).toBe(true)
    expect((await stat(filename)).mode & 0o777).toBe(0o600)
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
          const native = new sqlite.Database(filename)
          yield* Effect.addFinalizer(() => Effect.sync(() => native.close()))
          native.run("CREATE TABLE birth_probe (value TEXT NOT NULL)")
        }),
      ),
    )
  })

  test("creates an explicit database and its sidecars owner-only while open", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "explicit.sqlite")

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(Database.layerFromPath(filename))
          const client = Context.get(context, Database.Service).db.$client
          yield* client.unsafe("CREATE TABLE permission_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
          yield* client.unsafe("INSERT INTO permission_probe (value) VALUES ('fresh')").raw

          expect(
            yield* Effect.promise(() =>
              Promise.all(["", "-wal", "-shm"].map(async (suffix) => (await stat(filename + suffix)).mode & 0o777)),
            ),
          ).toEqual([0o600, 0o600, 0o600])
        }),
      ),
    )
  })

  test.serial("tightens real sidecars reached through a live OPENCODE_DB symlink", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "real.sqlite")
    const alias = path.join(tmp.path, "explicit.sqlite")
    const previousDatabase = Flag.OPENCODE_DB
    const previousDatabaseEnv = process.env.OPENCODE_DB

    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            yield* seedExistingPermissionDatabase(filename)
            yield* Effect.promise(() => symlink(filename, alias))
            Flag.OPENCODE_DB = alias
            process.env.OPENCODE_DB = alias
            const selected = Database.path()
            expect(selected).toBe(alias)

            const context = yield* Layer.build(Database.layerFromPath(selected))
            const client = Context.get(context, Database.Service).db.$client
            expect(yield* client.unsafe("SELECT value FROM permission_symlink").values).toEqual([[
              "preserved",
            ]])
            expect(
              yield* Effect.promise(() =>
                Promise.all(["", "-wal", "-shm"].map(async (suffix) => (await stat(filename + suffix)).mode & 0o777)),
              ),
            ).toEqual([0o600, 0o600, 0o600])
          }),
        ),
      )
    } finally {
      Flag.OPENCODE_DB = previousDatabase
      if (previousDatabaseEnv === undefined) delete process.env.OPENCODE_DB
      else process.env.OPENCODE_DB = previousDatabaseEnv
    }
  })

  test.serial("tightens real sidecars reached through a symlinked data-directory opencode.db", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "real.sqlite")
    const data = path.join(tmp.path, "data")
    const alias = path.join(data, "opencode.db")
    const previousDatabase = Flag.OPENCODE_DB
    const previousData = Global.Path.data
    const previousDatabaseEnv = process.env.OPENCODE_DB
    const previousChannelSetting = process.env.OPENCODE_DISABLE_CHANNEL_DB
    await mkdir(data, { recursive: true })

    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            yield* seedExistingPermissionDatabase(filename)
            yield* Effect.promise(() => symlink(filename, alias))
            Flag.OPENCODE_DB = undefined
            delete process.env.OPENCODE_DB
            Global.Path.data = data
            process.env.OPENCODE_DISABLE_CHANNEL_DB = "1"
            const selected = Database.path()
            expect(selected).toBe(alias)

            const context = yield* Layer.build(Database.layerFromPath(selected))
            const client = Context.get(context, Database.Service).db.$client
            expect(yield* client.unsafe("SELECT value FROM permission_symlink").values).toEqual([[
              "preserved",
            ]])
            expect(
              yield* Effect.promise(() =>
                Promise.all(["", "-wal", "-shm"].map(async (suffix) => (await stat(filename + suffix)).mode & 0o777)),
              ),
            ).toEqual([0o600, 0o600, 0o600])
          }),
        ),
      )
    } finally {
      Flag.OPENCODE_DB = previousDatabase
      Global.Path.data = previousData
      if (previousDatabaseEnv === undefined) delete process.env.OPENCODE_DB
      else process.env.OPENCODE_DB = previousDatabaseEnv
      if (previousChannelSetting === undefined) delete process.env.OPENCODE_DISABLE_CHANNEL_DB
      else process.env.OPENCODE_DISABLE_CHANNEL_DB = previousChannelSetting
    }
  })

  test.serial("opens a dangling OPENCODE_DB symlink with owner-only target files", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "dangling-target.sqlite")
    const alias = path.join(tmp.path, "selected.sqlite")
    const previousDatabase = Flag.OPENCODE_DB
    const previousDatabaseEnv = process.env.OPENCODE_DB
    await symlink(path.basename(filename), alias)

    try {
      Flag.OPENCODE_DB = alias
      process.env.OPENCODE_DB = alias
      expect(Database.path()).toBe(alias)
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(Database.layerFromPath(Database.path()))
            const client = Context.get(context, Database.Service).db.$client
            yield* client.unsafe("CREATE TABLE dangling_probe (value TEXT NOT NULL)").raw
            yield* client.unsafe("INSERT INTO dangling_probe (value) VALUES ('created')").raw
            expect(yield* client.unsafe("SELECT value FROM dangling_probe").values).toEqual([["created"]])
            expect(
              yield* Effect.promise(() =>
                Promise.all(["", "-wal", "-shm"].map(async (suffix) => (await stat(filename + suffix)).mode & 0o777)),
              ),
            ).toEqual([0o600, 0o600, 0o600])
          }),
        ),
      )
    } finally {
      Flag.OPENCODE_DB = previousDatabase
      if (previousDatabaseEnv === undefined) delete process.env.OPENCODE_DB
      else process.env.OPENCODE_DB = previousDatabaseEnv
    }
  })

  test("tightens existing SQLite files and keeps their data", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "existing.sqlite")

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const context = yield* Layer.build(Database.layerFromPath(filename))
              const client = Context.get(context, Database.Service).db.$client
              yield* client.unsafe("CREATE TABLE permission_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").raw
              yield* client.unsafe("INSERT INTO permission_probe (value) VALUES ('preserved')").raw
            }),
          )

          const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
          const native = new sqlite.Database(filename)
          yield* Effect.addFinalizer(() => Effect.sync(() => native.close()))
          native.run("PRAGMA journal_mode = WAL")
          native.run("INSERT INTO permission_probe (value) VALUES ('also preserved')")
          yield* Effect.promise(() =>
            Promise.all(["", "-wal", "-shm"].map((suffix) => chmod(filename + suffix, 0o644))),
          )
          expect(
            yield* Effect.promise(() =>
              Promise.all(["", "-wal", "-shm"].map(async (suffix) => (await stat(filename + suffix)).mode & 0o777)),
            ),
          ).toEqual([0o644, 0o644, 0o644])

          const context = yield* Layer.build(Database.layerFromPath(filename))
          const client = Context.get(context, Database.Service).db.$client
          expect(yield* client.unsafe("SELECT value FROM permission_probe ORDER BY id").values).toEqual([
            ["preserved"],
            ["also preserved"],
          ])
          expect(
            yield* Effect.promise(() =>
              Promise.all(["", "-wal", "-shm"].map(async (suffix) => (await stat(filename + suffix)).mode & 0o777)),
            ),
          ).toEqual([0o600, 0o600, 0o600])
        }),
      ),
    )
  })

  test("tightens an existing database without releasing a same-process SQLite lock", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "locked.sqlite")

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
          const holder = new sqlite.Database(filename)
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              if (holder.inTransaction) holder.run("ROLLBACK")
              holder.close()
            }),
          )
          holder.run("CREATE TABLE permission_lock (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
          holder.run("BEGIN IMMEDIATE")
          holder.run("INSERT INTO permission_lock (value) VALUES ('holder')")
          const contender = yield* Effect.promise(() => startLockContender(filename))
          yield* Effect.addFinalizer(() =>
            Effect.promise(() => contender.release()).pipe(
              Effect.timeoutOrElse({
                duration: "2 seconds",
                orElse: () => Effect.fail(new Error("SQLite lock contender did not exit")),
              }),
              Effect.orDie,
            ),
          )

          expect(yield* Effect.promise(() => contender.probe())).toMatch(/^blocked:SQLITE_BUSY/)
          yield* Effect.promise(() => chmod(filename, 0o644))
          yield* Effect.promise(() => restrictToOwner(filename))
          expect((yield* Effect.promise(() => stat(filename))).mode & 0o777).toBe(0o600)
          expect(yield* Effect.promise(() => contender.probe())).toMatch(/^blocked:SQLITE_BUSY/)

          holder.run("ROLLBACK")
          expect(yield* Effect.promise(() => contender.probe())).toBe("acquired")
        }),
      ),
    )
  })
})
