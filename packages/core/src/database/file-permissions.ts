import { closeSync, lstatSync, openSync, readlinkSync, realpathSync, statSync } from "node:fs"
import { chmod } from "node:fs/promises"
import path from "path"

export async function restrictToOwner(filename: string) {
  if (filename === ":memory:" || process.platform === "win32") return
  const target = resolveDatabaseFilename(filename)

  if (!createPrivateDatabaseFile(target)) return

  await chmod(target, 0o600)
  // SQLite derives new sidecar modes from the database but does not tighten existing sidecars.
  await Promise.all(
    [`${target}-wal`, `${target}-shm`].map((sidecar) =>
      chmod(sidecar, 0o600).catch((error: NodeJS.ErrnoException) => {
        if (error.code !== "ENOENT") throw error
      }),
    ),
  )
}

export function createPrivateDatabaseFile(filename: string) {
  // Exclusive creation sets the first mode without ever opening an existing SQLite database.
  try {
    closeSync(openSync(filename, "wx", 0o600))
    return true
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error) || error.code !== "EEXIST") throw error
    return statSync(filename).isFile()
  }
}

function resolveDatabaseFilename(filename: string) {
  // SQLite follows the final-component symlink before choosing database sidecar paths.
  let current = path.resolve(filename)
  const visited = new Set<string>()
  for (;;) {
    const parent = realpathSync(path.dirname(current))
    const candidate = path.join(parent, path.basename(current))
    if (visited.has(candidate)) throw new Error("database path contains a symbolic-link cycle")
    visited.add(candidate)

    let entry
    try {
      entry = lstatSync(candidate)
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT") return candidate
      throw error
    }
    if (!entry.isSymbolicLink()) return candidate

    const target = readlinkSync(candidate)
    current = path.isAbsolute(target) ? target : path.resolve(parent, target)
  }
}
