import { expect, test } from "bun:test"
import fsSync from "node:fs"
import fs from "fs/promises"
import os from "os"
import path from "path"

test("global path inspection leaves default directories for runtime initialization", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-global-path-test-"))
  await using _ = {
    async [Symbol.asyncDispose]() {
      await fs.rm(root, { recursive: true, force: true })
    },
  }

  const environment = {
    ...process.env,
    HOME: path.join(root, "home"),
    XDG_DATA_HOME: path.join(root, "xdg-data"),
    XDG_CONFIG_HOME: path.join(root, "xdg-config"),
    XDG_STATE_HOME: path.join(root, "xdg-state"),
    XDG_CACHE_HOME: path.join(root, "xdg-cache"),
    TMPDIR: path.join(root, "tmp"),
    TMP: path.join(root, "tmp"),
    TEMP: path.join(root, "tmp"),
  }
  const data = path.join(environment.XDG_DATA_HOME, "opencode")
  const cache = path.join(environment.XDG_CACHE_HOME, "opencode")
  const expectedTmp = path.join(environment.TMPDIR, "opencode")
  const defaults = [
    data,
    path.join(data, "log"),
    path.join(data, "repos"),
    path.join(environment.XDG_CONFIG_HOME, "opencode"),
    path.join(environment.XDG_STATE_HOME, "opencode"),
    cache,
    path.join(cache, "bin"),
    expectedTmp,
  ]
  const run = async (script: string) => {
    const child = Bun.spawn(["bun", "-e", script], {
      cwd: path.resolve(import.meta.dir, ".."),
      env: environment,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    })
    const stdout = new Response(child.stdout).text()
    const stderr = new Response(child.stderr).text()
    const timeout = setTimeout(() => child.kill(), 10_000)
    const exitCode = await child.exited
    clearTimeout(timeout)
    return { exitCode, stdout: await stdout, stderr: await stderr }
  }

  const inspected = await run(
    `const { Global } = await import("@opencode-ai/core/global"); console.log(JSON.stringify([Global.Path.tmp, Global.make().tmp]))`,
  )
  expect(inspected.exitCode).toBe(0)
  expect(inspected.stderr).toBe("")
  expect(inspected.stdout.trim()).toBe(JSON.stringify([expectedTmp, expectedTmp]))
  expect(defaults.filter(fsSync.existsSync)).toEqual([])

  const initialized = await run(`
    import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
    import { Global } from "@opencode-ai/core/global"
    import { Effect, ManagedRuntime } from "effect"

    const runtime = ManagedRuntime.make(AppNodeBuilder.build(Global.node))
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* Global.Service
      }),
    )
    await runtime.dispose()
  `)
  expect(initialized.exitCode).toBe(0)
  expect(initialized.stderr).toBe("")
  expect(defaults.filter(fsSync.existsSync)).toEqual(defaults)
}, 30_000)
