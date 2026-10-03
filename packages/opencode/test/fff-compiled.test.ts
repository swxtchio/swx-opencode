import { describe, expect, test } from "bun:test"
import { getNpmPackageName } from "@ff-labs/fff-bun"
import fs from "node:fs/promises"
import { createRequire } from "node:module"
import path from "node:path"

const targetPlatform = process.platform === "win32" ? "windows" : process.platform
const libc = process.platform === "linux" && getNpmPackageName().endsWith("-musl") ? "musl" : "gnu"
const target =
  `bun-${targetPlatform}-${process.arch}${process.platform === "linux" && libc === "musl" ? "-musl" : ""}` as Bun.Build.CompileTarget
const require = createRequire(import.meta.resolve("@ff-labs/fff-bun"))
const executablePath = process.env.OPENCODE_FFF_TEST_BUN_EXECUTABLE
const library = Buffer.from(await Bun.file(require.resolve(getNpmPackageName())).arrayBuffer())
const extension = process.platform === "darwin" ? ".dylib" : process.platform === "win32" ? ".dll" : ".so"

describe("compiled FFF extraction", () => {
  test("reuses one matching native allocation across normal and forced exits", async () => {
    const dist = path.join(import.meta.dir, "../dist")
    await fs.mkdir(dist, { recursive: true })
    const root = await fs.mkdtemp(path.join(dist, "fff-compiled-"))
    try {
      const temporary = path.join(root, "tmp")
      const workspace = path.join(root, "workspace")
      const binary = path.join(root, "opencode")
      await fs.mkdir(path.join(workspace, "src", "nested"), { recursive: true })
      await fs.mkdir(temporary)
      await fs.writeFile(path.join(workspace, "src", "nested", "needle-target.ts"), "picker-content\n")

      const build = await Bun.build({
        entrypoints: [path.join(import.meta.dir, "fixture/fff-compiled.ts")],
        format: "esm",
        minify: true,
        splitting: true,
        define: { FFF_LIBC: JSON.stringify(libc) },
        compile: {
          autoloadBunfig: false,
          autoloadDotenv: false,
          target,
          outfile: binary,
          ...(executablePath ? { executablePath } : {}),
        },
      })
      expect(build.success).toBe(true)

      const allocations = async () => {
        const entries = await fs.readdir(temporary, { withFileTypes: true })
        const candidates = (
          await Promise.all(
            entries.map(async (entry) => {
              const directory = path.join(temporary, entry.name)
              if (entry.isDirectory()) {
                return (await fs.readdir(directory, { withFileTypes: true }))
                  .filter((child) => child.isFile() && child.name.endsWith(extension))
                  .map((child) => path.join(directory, child.name))
              }
              return entry.isFile() && entry.name.endsWith(extension) ? [directory] : []
            }),
          )
        ).flat()

        return (
          await Promise.all(
            candidates.map(async (file) => ({ file, contentMatches: (await fs.readFile(file)).equals(library) })),
          )
        )
          .filter((entry) => entry.contentMatches)
          .map((entry) => entry.file)
      }

      const launch = (mode: "complete" | "hold") =>
        Bun.spawn([binary, workspace, mode], {
          cwd: root,
          env: { ...process.env, BUN_TMPDIR: temporary, TMPDIR: temporary },
          stdout: "pipe",
          stderr: "pipe",
        })

      const before = await allocations()
      expect(before).toEqual([])

      const complete = async () => {
        const child = launch("complete")
        const timeout = setTimeout(() => child.kill("SIGKILL"), 15_000)
        try {
          const [stdout, stderr, status] = await Promise.all([
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
            child.exited,
          ])
          expect(status, stderr).toBe(0)
          expect(stdout).toContain("ready")
        } finally {
          clearTimeout(timeout)
        }
      }

      await complete()
      const afterFirstNormalExit = await allocations()
      expect(afterFirstNormalExit).toHaveLength(1)
      await complete()
      const afterNormalExits = await allocations()
      expect(afterNormalExits).toEqual(afterFirstNormalExit)

      const forceExit = async () => {
        const child = launch("hold")
        const reader = child.stdout.getReader()
        const timeout = setTimeout(() => child.kill("SIGKILL"), 15_000)
        try {
          let output = ""
          while (!output.includes("\n")) {
            const chunk = await reader.read()
            if (chunk.done) throw new Error("compiled FFF process exited before its readiness signal")
            output += new TextDecoder().decode(chunk.value)
          }
          expect(output.split("\n", 1)[0]).toBe("ready")
          expect(await allocations()).toEqual(afterNormalExits)
        } finally {
          clearTimeout(timeout)
          child.kill("SIGKILL")
        }
        expect(await child.exited).not.toBe(0)
        expect(await allocations()).toEqual(afterNormalExits)
      }

      await forceExit()
      await forceExit()
      expect(await allocations()).toEqual(afterNormalExits)
    } finally {
      await fs.rm(root, { recursive: true, force: true })
    }
  }, 60_000)
})
