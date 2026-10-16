import { expect, test } from "bun:test"
import { $ } from "bun"
import fs from "node:fs/promises"
import path from "node:path"
import { tmpdir } from "../fixture/fixture"

const fixture = path.join(import.meta.dir, "../fixture/fff-effective-search.ts")

test("production location-map control distinguishes enabled and no-watch FFF startup", async () => {
  const enabled = await run({ global: {}, watcher: true })
  const disabled = await run({
    global: { watcher: { fff: true, ignore: ["global-ignore"] } },
    content: { watcher: { fff: false, ignore: ["content-ignore"] } },
    watcher: false,
  })
  expectSearch(enabled)
  expectSearch(disabled)
  expect(disabled.watcherStatus).toEqual([])
  if (process.platform === "linux") {
    expect(enabled.nativeWatcher).toBe(true)
    expect(disabled.nativeWatcher).toBe(false)
  }
})

test("OPENCODE_CONFIG_CONTENT watcher.ignore reaches the active file watcher", async () => {
  const result = await run({
    global: { watcher: { fff: true, ignore: ["global-ignore"] } },
    content: { watcher: { fff: true, ignore: [".git"] } },
    git: true,
    watcher: true,
  })
  expect(result.watcherStatus).toEqual([])
  expectSearch(result)
  if (process.platform === "linux") expect(result.nativeWatcher).toBe(true)
})

test("OPENCODE_DISABLE_PROJECT_CONFIG excludes project watcher overrides", async () => {
  const result = await run({
    global: { watcher: { fff: false, ignore: ["global-ignore"] } },
    project: { watcher: { fff: true, ignore: [".git", "project-ignore"] } },
    projectOpenCode: { watcher: { fff: true, ignore: [".git", "project-opencode-ignore"] } },
    disableProjectConfig: true,
    git: true,
    watcher: false,
  })
  expect(result.watcherStatus.some((item) => item.watch === "git")).toBe(true)
  expectSearch(result)
  if (process.platform === "linux") expect(result.nativeWatcher).toBe(false)
})

test("a higher-priority watcher.fff true override re-enables the native watcher", async () => {
  const result = await run({
    global: { watcher: { fff: false } },
    content: { watcher: { fff: true } },
    watcher: true,
  })
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
  watcher: boolean
}) {
  const tmp = await tmpdir()
  try {
    const root = path.join(tmp.path, "project")
    const global = path.join(tmp.path, "global")
    await fs.mkdir(path.join(root, ".opencode"), { recursive: true })
    await fs.mkdir(global, { recursive: true })
    await fs.mkdir(path.join(root, "src", "nested"), { recursive: true })
    const environment = {
      home: path.join(tmp.path, "home"),
      config: path.join(tmp.path, "xdg-config"),
      data: path.join(tmp.path, "xdg-data"),
      cache: path.join(tmp.path, "xdg-cache"),
      state: path.join(tmp.path, "xdg-state"),
      managed: path.join(tmp.path, "managed"),
    }
    await Promise.all(Object.values(environment).map((directory) => fs.mkdir(directory, { recursive: true })))
    if (input.git) await $`git init`.cwd(root).quiet()
    await fs.mkdir(path.join(root, "src"), { recursive: true })
    await fs.writeFile(path.join(root, "src", "a-first.ts"), "first-marker\n")
    await fs.writeFile(path.join(root, "src", "nested", "needle-target.ts"), "picker-content\n")
    await fs.writeFile(path.join(root, "z-last.ts"), "last-marker\n")
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
    env.HOME = environment.home
    env.OPENCODE_TEST_HOME = environment.home
    env.OPENCODE_TEST_MANAGED_CONFIG_DIR = environment.managed
    env.XDG_CONFIG_HOME = environment.config
    env.XDG_DATA_HOME = environment.data
    env.XDG_CACHE_HOME = environment.cache
    env.XDG_STATE_HOME = environment.state
    if (input.content) env.OPENCODE_CONFIG_CONTENT = JSON.stringify(input.content)
    if (input.disableProjectConfig) env.OPENCODE_DISABLE_PROJECT_CONFIG = "true"

    const child = Bun.spawn([process.execPath, fixture, root, global, input.watcher ? "watch" : "no-watch"], {
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
