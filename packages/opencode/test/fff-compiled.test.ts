import { describe, expect, test } from "bun:test"
import { getNpmPackageName } from "@ff-labs/fff-bun"
import { fffBuildDefines, fffLibraryForTarget } from "@opencode-ai/script/fff-native"
import fs from "node:fs/promises"
import { createRequire } from "node:module"
import os from "node:os"
import path from "node:path"

const targetPlatform = process.platform === "win32" ? "windows" : process.platform
const libc = process.platform === "linux" && getNpmPackageName().endsWith("-musl") ? "musl" : "gnu"
const target =
  `bun-${targetPlatform}-${process.arch}${process.platform === "linux" && libc === "musl" ? "-musl" : ""}` as Bun.Build.CompileTarget
const require = createRequire(import.meta.resolve("@ff-labs/fff-bun"))
const executablePath = process.env.OPENCODE_FFF_TEST_BUN_EXECUTABLE
const nativeLibrary = await fffLibraryForTarget(
  {
    os: process.platform === "win32" ? "win32" : process.platform === "darwin" ? "darwin" : "linux",
    arch: process.arch as "arm64" | "x64",
    ...(process.platform === "linux" && libc === "musl" ? { abi: "musl" as const } : {}),
  },
  (specifier) => require.resolve(specifier),
)
const library = Buffer.from(await Bun.file(nativeLibrary.sourcePath).arrayBuffer())
const extension = process.platform === "darwin" ? ".dylib" : process.platform === "win32" ? ".dll" : ".so"

describe("compiled FFF extraction", () => {
  test("keeps native search usable without temp allocations in the sidecar build", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-fff-compiled-"))
    try {
      const temporary = path.join(root, "tmp")
      const workspace = path.join(root, "workspace")
      const binary = path.join(root, "opencode")
      await fs.mkdir(path.join(workspace, "src", "nested"), { recursive: true })
      await fs.mkdir(temporary)
      await fs.writeFile(path.join(workspace, "src", "nested", "needle-target.ts"), "picker-content\n")

      const build = async (outfile: string, externalLibrary?: string, executable?: string) =>
        Bun.build({
          entrypoints: [path.join(import.meta.dir, "fixture/fff-compiled.ts")],
          format: "esm",
          minify: true,
          splitting: true,
          define: {
            ...(externalLibrary ? fffBuildDefines({ filename: externalLibrary }) : {}),
            FFF_LIBC: JSON.stringify(libc),
          },
          compile: {
            autoloadBunfig: false,
            autoloadDotenv: false,
            target,
            outfile,
            ...(executable ? { executablePath: executable } : {}),
          },
        })

      const allocations = async (temporaryDirectory: string) => {
        const findLibraries = async (directory: string): Promise<string[]> => {
          const entries = await fs.readdir(directory, { withFileTypes: true })
          return (
            await Promise.all(
              entries.map(async (entry) => {
                const file = path.join(directory, entry.name)
                if (entry.isDirectory()) return findLibraries(file)
                return entry.isFile() && entry.name.endsWith(extension) ? [file] : []
              }),
            )
          ).flat()
        }

        const candidates = await findLibraries(temporaryDirectory)
        return Promise.all(
          candidates.map(async (file) => {
            const content = await fs.readFile(file)
            return { file, byteLength: content.byteLength, contentMatches: content.equals(library) }
          }),
        )
      }

      const launch = (executable: string, temporaryDirectory: string, mode: "complete" | "hold") =>
        Bun.spawn([executable, workspace, mode], {
          cwd: root,
          env: { ...process.env, BUN_TMPDIR: temporaryDirectory, TMPDIR: temporaryDirectory },
          stdout: "pipe",
          stderr: "pipe",
        })

      const complete = async (executable: string, temporaryDirectory: string) => {
        const child = launch(executable, temporaryDirectory, "complete")
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

      const forceExit = async (
        executable: string,
        temporaryDirectory: string,
        expected: Awaited<ReturnType<typeof allocations>>,
      ) => {
        const child = launch(executable, temporaryDirectory, "hold")
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
          expect(await allocations(temporaryDirectory)).toEqual(expected)
        } finally {
          clearTimeout(timeout)
          child.kill("SIGKILL")
        }
        expect(await child.exited).not.toBe(0)
        expect(await allocations(temporaryDirectory)).toEqual(expected)
      }

      const before = await allocations(temporary)
      expect(before).toEqual([])

      const embeddedBuild = await build(binary, undefined, executablePath)
      expect(embeddedBuild.success).toBe(true)
      await complete(binary, temporary)
      const afterFirstNormalExit = await allocations(temporary)
      expect(afterFirstNormalExit).toHaveLength(1)
      expect(afterFirstNormalExit[0].contentMatches).toBe(true)
      await complete(binary, temporary)
      const afterNormalExits = await allocations(temporary)
      expect(afterNormalExits).toEqual(afterFirstNormalExit)

      const sidecarTemporary = path.join(root, "sidecar-tmp")
      const sidecarBinary = path.join(root, "opencode-sidecar")
      await fs.mkdir(sidecarTemporary)
      await Bun.write(path.join(root, nativeLibrary.filename), Bun.file(nativeLibrary.sourcePath))
      const sidecarBuild = await build(sidecarBinary, nativeLibrary.filename)
      expect(sidecarBuild.success).toBe(true)
      expect(await allocations(sidecarTemporary)).toEqual([])

      await complete(sidecarBinary, sidecarTemporary)
      expect(await allocations(sidecarTemporary)).toEqual([])
      await complete(sidecarBinary, sidecarTemporary)
      expect(await allocations(sidecarTemporary)).toEqual([])
      await forceExit(sidecarBinary, sidecarTemporary, [])
      await forceExit(sidecarBinary, sidecarTemporary, [])
      expect(await allocations(sidecarTemporary)).toEqual([])
    } finally {
      await fs.rm(root, { recursive: true, force: true })
    }
  }, 180_000)
})
