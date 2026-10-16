import { expect, test } from "bun:test"
import { $ } from "bun"
import fs from "node:fs/promises"
import path from "node:path"
import { tmpdir } from "../fixture/fixture"

const fixture = path.join(import.meta.dir, "../fixture/fff-effective-search.ts")

test("an omitted watcher.fff keeps the default native watcher enabled", async () => {
  const result = await run({ global: {} })
  expect(result.watcherConfig?.fff).toBeUndefined()
  expectSearch(result)
  if (process.platform === "linux") expect(result.nativeWatcher).toBe(true)
})

test("OPENCODE_CONFIG_CONTENT disables the FFF watcher while keeping native search useful", async () => {
  const result = await run({
    global: { watcher: { fff: true, ignore: ["global-ignore"] } },
    content: { watcher: { fff: false, ignore: ["content-ignore"] } },
  })
  expect(result.watcherConfig?.fff).toBe(false)
  expect(result.watcherConfig?.ignore).toContain("content-ignore")
  expect(result.watcherStatus).toEqual([])
  expectSearch(result)
  if (process.platform === "linux") expect(result.nativeWatcher).toBe(false)
})

test("OPENCODE_CONFIG_CONTENT watcher.ignore reaches the active file watcher", async () => {
  const result = await run({
    global: { watcher: { fff: true, ignore: ["global-ignore"] } },
    content: { watcher: { fff: true, ignore: [".git"] } },
    git: true,
  })
  expect(result.watcherStatus).toEqual([])
  expect(result.watcherConfig?.ignore).toContain(".git")
  expectSearch(result)
})

test("OPENCODE_DISABLE_PROJECT_CONFIG excludes project watcher overrides", async () => {
  const result = await run({
    global: { watcher: { fff: false, ignore: ["global-ignore"] } },
    project: { watcher: { fff: true, ignore: [".git", "project-ignore"] } },
    projectOpenCode: { watcher: { fff: true, ignore: [".git", "project-opencode-ignore"] } },
    disableProjectConfig: true,
    git: true,
  })
  expect(result.watcherConfig?.fff).toBe(false)
  expect(result.watcherConfig?.ignore).toContain("global-ignore")
  expect(result.watcherConfig?.ignore).not.toContain("project-ignore")
  expect(result.watcherConfig?.ignore).not.toContain("project-opencode-ignore")
  expect(result.watcherConfig?.ignore).not.toContain(".git")
  expect(result.watcherStatus.some((item) => item.watch === "git")).toBe(true)
  expect(result.watcherStatus.some((item) => item.watch === "git")).toBe(true)
  expectSearch(result)
  if (process.platform === "linux") expect(result.nativeWatcher).toBe(false)
})

test("a higher-priority watcher.fff true override re-enables the native watcher", async () => {
  const result = await run({
    global: { watcher: { fff: false } },
    content: { watcher: { fff: true } },
  })
  expect(result.watcherConfig?.fff).toBe(true)
  expectSearch(result)
  if (process.platform === "linux") expect(result.nativeWatcher).toBe(true)
})

async function run(input: {
  global: object
  content?: object
  project?: object
  projectOpenCode?: object
  disableProjectConfig?: boolean
  git?: boolean
}) {
  const tmp = await tmpdir()
  try {
    const root = path.join(tmp.path, "project")
    const global = path.join(tmp.path, "global")
    await fs.mkdir(path.join(root, ".opencode"), { recursive: true })
    await fs.mkdir(global, { recursive: true })
    await fs.mkdir(path.join(root, "src", "nested"), { recursive: true })
    if (input.git) await $`git init`.cwd(root).quiet()
    await fs.writeFile(path.join(root, "src", "nested", "needle-target.ts"), "picker-content\n")
    await fs.writeFile(path.join(global, "opencode.json"), JSON.stringify(input.global))
    if (input.project) await fs.writeFile(path.join(root, "opencode.json"), JSON.stringify(input.project))
    if (input.projectOpenCode)
      await fs.writeFile(path.join(root, ".opencode", "opencode.json"), JSON.stringify(input.projectOpenCode))

    const env = { ...process.env }
    delete env.OPENCODE_CONFIG
    delete env.OPENCODE_CONFIG_DIR
    delete env.OPENCODE_CONFIG_CONTENT
    delete env.OPENCODE_DISABLE_PROJECT_CONFIG
    delete env.OPENCODE_DISABLE_FFF
    if (input.content) env.OPENCODE_CONFIG_CONTENT = JSON.stringify(input.content)
    if (input.disableProjectConfig) env.OPENCODE_DISABLE_PROJECT_CONFIG = "true"

    const child = Bun.spawn([process.execPath, fixture, root, global, input.git ? "git" : ""], {
      env,
      stdout: "pipe",
      stderr: "pipe",
    })
    let timedOut = false
    const timer = setTimeout(() => {
      timedOut = true
      child.kill()
    }, 20_000)
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ])
    clearTimeout(timer)
    if (timedOut) throw new Error(`FFF config fixture timed out\n${stderr}`)
    if (exitCode !== 0) throw new Error(`FFF config fixture exited ${exitCode}\n${stderr}\n${stdout}`)
    const line = stdout.split("\n").find((value) => value.startsWith("FFF_REPORT "))
    if (!line) throw new Error(`FFF config fixture did not report results\n${stderr}\n${stdout}`)
    return JSON.parse(line.slice("FFF_REPORT ".length)) as {
      files: string[]
      directories: string[]
      glob: string[]
      matches: string[]
      watcherConfig?: { fff?: boolean; ignore?: string[] }
      watcherStatus: { watch: string }[]
      nativeWatcher?: boolean
    }
  } finally {
    await tmp[Symbol.asyncDispose]()
  }
}

function expectSearch(result: Awaited<ReturnType<typeof run>>) {
  expect(result.files).toContain("src/nested/needle-target.ts")
  expect(result.directories.some((item) => item.includes("nested"))).toBe(true)
  expect(result.glob).toContain("src/nested/needle-target.ts")
  expect(result.matches.some((item) => item.includes("picker-content"))).toBe(true)
}
