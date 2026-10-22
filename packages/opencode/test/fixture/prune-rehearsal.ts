// Private on-disk rehearsal; the production CLI owns deletion and compaction.
import { Database } from "bun:sqlite"
import { mkdir, mkdtemp, rm, stat } from "node:fs/promises"
import path from "node:path"
import assert from "node:assert/strict"

const packageRoot = path.resolve(import.meta.dir, "../..")
const worktreeRoot = path.resolve(packageRoot, "../..")
const scratch = path.join(worktreeRoot, ".scratch")
await mkdir(scratch, { recursive: true, mode: 0o700 })
const directory = await mkdtemp(path.join(scratch, "dead-retirement-rehearsal-"))
const filename = path.join(directory, "opencode.db")
const dead = path.join(directory, "dead-worktree")
const live = path.join(directory, "live-worktree")
const env = {
  ...process.env,
  HOME: directory,
  XDG_DATA_HOME: path.join(directory, "data"),
  XDG_CONFIG_HOME: path.join(directory, "config"),
  XDG_CACHE_HOME: path.join(directory, "cache"),
  XDG_STATE_HOME: path.join(directory, "state"),
  OPENCODE_DB: filename,
  OPENCODE_DISABLE_CHANNEL_DB: "1",
  OPENCODE_PURE: "1",
}

function run(args: string[]) {
  const result = Bun.spawnSync([process.execPath, ...args], { cwd: packageRoot, env, timeout: 60000 })
  assert.equal(result.exitCode, 0, result.stderr.toString() + result.stdout.toString())
  return result.stdout.toString()
}

async function allocation() {
  return Promise.all(
    [filename, `${filename}-wal`, `${filename}-shm`, `${filename}-journal`].map(async (file) => {
      const entry = await stat(file).catch((error: NodeJS.ErrnoException) => {
        if (error.code === "ENOENT") return undefined
        throw error
      })
      return { path: path.basename(file), allocatedBytes: (entry?.blocks ?? 0) * 512, sizeBytes: entry?.size ?? 0 }
    }),
  )
}

try {
  await mkdir(dead)
  await mkdir(live)
  const producer = path.join(import.meta.dir, "prune-producer.ts")
  run([
    producer,
    filename,
    dead,
    JSON.stringify({ create: { payloadBytes: 4 * 1024 * 1024, withdrawnV1: true, ask: true, completeStorage: true } }),
  ])
  run([producer, filename, live, JSON.stringify({ create: { payloadBytes: 128 } })])
  const setup = new Database(filename)
  setup.exec("PRAGMA auto_vacuum=NONE; VACUUM")
  assert.deepEqual(setup.query("PRAGMA auto_vacuum").get(), { auto_vacuum: 0 })
  const liveBefore = setup.query("SELECT * FROM session WHERE directory = ?").all(live)
  const livePartsBefore = setup
    .query("SELECT * FROM part WHERE session_id IN (SELECT id FROM session WHERE directory = ?) ORDER BY id")
    .all(live)
  setup.close()
  await rm(dead, { recursive: true })
  const before = await allocation()
  const entrypoint = path.join(packageRoot, "src/index.ts")
  const inventory = JSON.parse(run([entrypoint, "db", "prune", "--database", filename]))
  assert.equal(inventory.selectedCounts.sessions, 1)
  const applied = JSON.parse(run([entrypoint, "db", "prune", "--database", filename, "--apply"]))
  assert.equal(applied.action, "applied")
  assert.deepEqual(applied.deletedCounts, {
    sessions: inventory.selectedCounts.sessions,
    ownedRows: inventory.selectedCounts.ownedRows,
    byTable: inventory.selectedSessions[0].ownedRows,
  })
  const after = await allocation()
  assert(
    after.reduce((sum, item) => sum + item.allocatedBytes, 0) <
      before.reduce((sum, item) => sum + item.allocatedBytes, 0),
  )
  const verify = new Database(filename, { readonly: true })
  assert.equal(
    verify.query<{ rows: number }, [string]>("SELECT count(*) AS rows FROM session WHERE directory = ?").get(dead)
      ?.rows,
    0,
  )
  assert.deepEqual(verify.query("SELECT * FROM session WHERE directory = ?").all(live), liveBefore)
  assert.deepEqual(
    verify
      .query("SELECT * FROM part WHERE session_id IN (SELECT id FROM session WHERE directory = ?) ORDER BY id")
      .all(live),
    livePartsBefore,
  )
  const integrity = verify.query("PRAGMA integrity_check").all()
  const foreignKeys = verify.query("PRAGMA foreign_key_check").all()
  verify.close()
  assert.deepEqual(integrity, [{ integrity_check: "ok" }])
  assert.deepEqual(foreignKeys, [])
  const source = Bun.spawnSync(["git", "rev-parse", "HEAD"], { cwd: worktreeRoot })
  assert.equal(source.exitCode, 0)
  const evidence = {
    observedAt: new Date().toISOString(),
    sourceCommit: source.stdout.toString().trim(),
    platform: `${process.platform}-${process.arch}`,
    bun: Bun.version,
    autoVacuum: "NONE",
    privateDatabaseOnly: true,
    before,
    after,
    deletedCounts: applied.deletedCounts,
    liveSessionRetained: true,
    livePartsRetained: true,
    integrity,
    foreignKeys,
    checkpoint: applied.reclamation.checkpoint,
    vacuum: applied.reclamation.vacuum,
    vacuumCheckpoint: applied.reclamation.vacuumCheckpoint,
  }
  await Bun.write(
    path.join(worktreeRoot, "docs/verification/dead-retirement-2026-10-22.json"),
    JSON.stringify(evidence, null, 2) + "\n",
  )
  console.log(JSON.stringify(evidence, null, 2))
} finally {
  await rm(directory, { recursive: true, force: true })
}
