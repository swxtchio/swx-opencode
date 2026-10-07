import { describe, expect, test } from "bun:test"
import { fffLibraryForTarget } from "@opencode-ai/script/fff-native"
import { createRequire } from "node:module"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"

const targetOS = process.platform as "linux" | "darwin" | "win32"
const targetArch = process.arch as "arm64" | "x64"
const targetName = targetOS === "win32" ? "windows" : targetOS
const require = createRequire(import.meta.resolve("@ff-labs/fff-bun"))
const nativeLibrary = await fffLibraryForTarget({ os: targetOS, arch: targetArch }, (specifier) =>
  require.resolve(specifier),
)
const nativeBytes = Buffer.from(await Bun.file(nativeLibrary.sourcePath).arrayBuffer())
const nativeDigest = new Bun.CryptoHasher("sha256").update(nativeBytes).digest("hex")
const fffDownload = await import(new URL("./download.ts", import.meta.resolve("@ff-labs/fff-bun")).href)

describe("compiled OpenCode FFF distribution", () => {
  test("loads the packaged native sidecar and leaves no temp addon allocations across exits", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-fff-distribution-"))
    const dist = path.join(root, "dist")
    const temporaryRoot = path.join(root, "tmp")
    const workspace = path.join(root, "workspace")
    const home = path.join(root, "home")
    const buildTemporary = path.join(root, "build-tmp")

    try {
      await Promise.all([
        fs.mkdir(temporaryRoot, { recursive: true }),
        fs.mkdir(workspace, { recursive: true }),
        fs.mkdir(home, { recursive: true }),
        fs.mkdir(buildTemporary, { recursive: true }),
      ])
      await fs.mkdir(path.join(workspace, "src", "nested"), { recursive: true })
      await fs.writeFile(path.join(workspace, "src", "nested", "needle-target.ts"), "picker-content\n")

      const build = Bun.spawn(
        ["bun", "run", "script/build.ts", "--single", "--skip-install", "--skip-embed-web-ui"],
        {
          cwd: path.resolve(import.meta.dir, ".."),
          env: {
            ...process.env,
            OPENCODE_BUILD_DIST: dist,
            BUN_TMPDIR: buildTemporary,
            TMPDIR: buildTemporary,
          },
          stdout: "pipe",
          stderr: "pipe",
        },
      )
      const buildTimeout = setTimeout(() => build.kill("SIGKILL"), 180_000)
      const [buildStdout, buildStderr, buildStatus] = await Promise.all([
        new Response(build.stdout).text(),
        new Response(build.stderr).text(),
        build.exited,
      ])
      clearTimeout(buildTimeout)
      expect(buildStatus, `${buildStdout}\n${buildStderr}`).toBe(0)

      const output = path.join(dist, `opencode-${targetName}-${targetArch}`, "bin")
      const binary = [path.join(output, "opencode"), path.join(output, "opencode.exe")].find((file) =>
        Bun.file(file).size > 0,
      )
      expect(binary).toBeDefined()
      const sidecar = path.join(output, nativeLibrary.filename)
      expect(await fs.readFile(sidecar)).toEqual(nativeBytes)
      expect(nativeLibrary.filename).toContain(nativeDigest)
      console.log(`FFF sidecar target=${targetOS}-${targetArch} filename=${nativeLibrary.filename} sha256=${nativeDigest}`)
      expect(await addonAllocations(temporaryRoot)).toEqual([])

      const normalTemporary = path.join(temporaryRoot, "normal")
      await fs.mkdir(normalTemporary)
      const normal = await run(
        [binary!, "debug", "file", "search", "needle-target"],
        workspace,
        childEnvironment(root, normalTemporary),
      )
      expect(normal.status, `${normal.stdout}\n${normal.stderr}`).toBe(0)
      expect(normal.stdout).toContain("needle-target.ts")
      expect(await addonAllocations(temporaryRoot)).toEqual([])

      const missingSidecar = `${sidecar}.missing`
      await fs.rename(sidecar, missingSidecar)
      try {
        const missingTemporary = path.join(temporaryRoot, "missing")
        await fs.mkdir(missingTemporary)
        const missing = await run(
          [binary!, "debug", "file", "search", "needle-target"],
          workspace,
          childEnvironment(root, missingTemporary),
        )
        expect(missing.status, `${missing.stdout}\n${missing.stderr}`).not.toBe(0)
      } finally {
        await fs.rename(missingSidecar, sidecar)
      }
      expect(await addonAllocations(temporaryRoot)).toEqual([])

      for (const index of [0, 1]) {
        const tmp = path.join(temporaryRoot, `forced-${index}`)
        await fs.mkdir(tmp)
        const server = Bun.spawn([binary!, "serve", "--hostname", "127.0.0.1", "--port", "0"], {
          cwd: workspace,
          env: childEnvironment(root, tmp),
          stdout: "pipe",
          stderr: "pipe",
        })
        let serverStopped = false
        const stderr = new Response(server.stderr).text()
        try {
          const address = await waitForServer(server, stderr)
          const files = await findFile(address, workspace)
          expect(files.some((file) => file.includes("needle-target.ts"))).toBe(true)
          expect(await nativeLibraryIsMapped(server.pid, sidecar)).toBe(true)
          expect(await addonAllocations(temporaryRoot)).toEqual([])
          server.kill("SIGKILL")
          expect(await server.exited).not.toBe(0)
          serverStopped = true
          expect(await addonAllocations(temporaryRoot)).toEqual([])
        } finally {
          if (!serverStopped) {
            server.kill("SIGKILL")
            await server.exited
          }
          await stderr
        }
      }
    } finally {
      await fs.rm(root, { recursive: true, force: true })
    }
  }, 300_000)

  test("resolves sidecars beside Windows compiled roots", () => {
    const filename = "fff_c-identity.dll"
    const executablePath = path.join("B:/~BUN/root", "opencode.exe")
    const expected = path.join("B:/~BUN/root", filename)
    const resolved = fffDownload.resolveCompiledLibraryPath(
      executablePath,
      filename,
      (candidate: string) => candidate === expected,
    )
    expect(resolved).toBe(expected)
  })
})

async function run(command: string[], cwd: string, env: Record<string, string | undefined>) {
  const child = Bun.spawn(command, { cwd, env, stdout: "pipe", stderr: "pipe" })
  const timeout = setTimeout(() => child.kill("SIGKILL"), 30_000)
  try {
    const [stdout, stderr, status] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ])
    return { stdout, stderr, status }
  } finally {
    clearTimeout(timeout)
  }
}

function childEnvironment(root: string, temporary: string) {
  const home = path.join(root, "home")
  const env: Record<string, string | undefined> = {
    ...process.env,
    BUN_TMPDIR: temporary,
    TMPDIR: temporary,
    TMP: temporary,
    TEMP: temporary,
    HOME: home,
    USERPROFILE: home,
    APPDATA: path.join(home, "AppData", "Roaming"),
    LOCALAPPDATA: path.join(home, "AppData", "Local"),
    OPENCODE_TEST_HOME: home,
    OPENCODE_CONFIG_DIR: path.join(home, "config"),
    XDG_CONFIG_HOME: path.join(home, ".config"),
    XDG_DATA_HOME: path.join(home, ".local", "share"),
    XDG_CACHE_HOME: path.join(home, ".cache"),
    OPENCODE_DISABLE_AUTOUPDATE: "1",
    OPENCODE_DISABLE_MODELS_FETCH: "1",
    OPENCODE_DISABLE_PROJECT_CONFIG: "1",
    OPENCODE_PURE: "1",
  }
  delete env.OPENCODE_DISABLE_FFF
  return env
}

async function waitForServer(server: Bun.Subprocess, stderr: Promise<string>) {
  const stdout = server.stdout
  if (!stdout || typeof stdout === "number") throw new Error("server stdout is not piped")
  const reader = stdout.getReader()
  const timeout = setTimeout(() => server.kill("SIGKILL"), 45_000)
  let output = ""
  try {
    while (true) {
      const chunk = await reader.read()
      if (chunk.done) {
        await server.exited
        throw new Error(`server exited before its listening signal: ${output}\n${await stderr}`)
      }
      output += new TextDecoder().decode(chunk.value)
      const match = output.match(/opencode server listening on (https?:\/\/\S+)/)
      if (!match) continue
      void drain(reader)
      return match[1]
    }
  } finally {
    clearTimeout(timeout)
  }
}

async function drain(reader: ReadableStreamDefaultReader<Uint8Array>) {
  while (!(await reader.read()).done) {}
}

async function findFile(address: string, directory: string) {
  const url = new URL("/find/file", address)
  url.searchParams.set("query", "needle-target")
  url.searchParams.set("directory", directory)
  url.searchParams.set("limit", "10")
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    const response = await fetch(url, { signal: AbortSignal.timeout(5_000) })
    if (!response.ok) throw new Error(`file search returned HTTP ${response.status}`)
    const files = (await response.json()) as string[]
    if (files.some((file) => file.includes("needle-target.ts"))) return files
    await Bun.sleep(50)
  }
  throw new Error("compiled file search did not find its fixture before the failure deadline")
}

async function nativeLibraryIsMapped(pid: number, sidecar: string) {
  const name = path.basename(sidecar).toLowerCase()
  if (process.platform === "linux") {
    return (await fs.readFile(`/proc/${pid}/maps`, "utf8")).toLowerCase().includes(name)
  }
  const command =
    process.platform === "win32"
      ? [
          "powershell.exe",
          "-NoProfile",
          "-NonInteractive",
          "-ExecutionPolicy",
          "Bypass",
          "-Command",
          `[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(); (Get-Process -Id ${pid}).Modules | ForEach-Object { $_.FileName }`,
        ]
      : ["vmmap", String(pid)]
  const result = await run(command, process.cwd(), process.env)
  expect(result.status, result.stderr).toBe(0)
  return result.stdout.toLowerCase().includes(name)
}

async function addonAllocations(directory: string) {
  const nativeExtensions = new Set([".so", ".dylib", ".dll"])
  const found: { path: string; byteLength: number; digest: string; matchesFFF: boolean }[] = []
  const visit = async (current: string): Promise<void> => {
    for (const entry of await fs.readdir(current, { withFileTypes: true })) {
      const file = path.join(current, entry.name)
      if (entry.isDirectory()) {
        await visit(file)
        continue
      }
      if (!entry.isFile() || !nativeExtensions.has(path.extname(entry.name).toLowerCase())) continue
      const contents = await fs.readFile(file)
      const matchesFFF =
        contents.byteLength === 0 ||
        (contents.byteLength <= nativeBytes.byteLength && nativeBytes.subarray(0, contents.byteLength).equals(contents))
      if (!matchesFFF) continue
      found.push({
        path: file,
        byteLength: contents.byteLength,
        digest: new Bun.CryptoHasher("sha256").update(contents).digest("hex"),
        matchesFFF: contents.equals(nativeBytes),
      })
    }
  }
  await visit(directory)
  return found
}
