import { closeSync, openSync, statSync } from "node:fs"
import { chmod } from "node:fs/promises"

export async function restrictToOwner(filename: string) {
  if (filename === ":memory:" || process.platform === "win32") return

  // Exclusive creation avoids closing an existing database descriptor and dropping this process's POSIX locks.
  try {
    closeSync(openSync(filename, "wx", 0o600))
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error) || error.code !== "EEXIST") throw error
    if (!statSync(filename).isFile()) return
  }

  await chmod(filename, 0o600)
  // SQLite derives new sidecar modes from the database but does not tighten existing sidecars.
  await Promise.all(
    [`${filename}-wal`, `${filename}-shm`].map((sidecar) =>
      chmod(sidecar, 0o600).catch((error: NodeJS.ErrnoException) => {
        if (error.code !== "ENOENT") throw error
      }),
    ),
  )
}
