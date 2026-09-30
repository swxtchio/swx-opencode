import { $ } from "bun"
import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { build } from "vite"
import { resolveConfig } from "electron-vite"

// Builds the desktop main process exactly as `electron-vite build` does and runs
// its sidecar.js under Node, so root file events are observed from the shipped
// sidecar output rather than from the intermediate opencode dist/node bundle.
const desktop = path.join(import.meta.dir, "../..")
const canRun = process.platform !== "win32" && !!Bun.which("node") && !process.env.CI
const describeSidecar = canRun ? describe : describe.skip

// Electron's utility process provides process.parentPort; Node's IPC channel
// stands in for it here.
const parentPortShim = `data:text/javascript,${encodeURIComponent(`
process.parentPort = {
  postMessage: (message) => process.send(message),
  on: (event, listener) => process.on("message", (data) => listener({ data })),
}
`)}`

async function buildMain(outDir: string) {
  await $`bun script/build-node.ts`.cwd(path.join(desktop, "../opencode")).quiet()
  const config = await resolveConfig({ root: desktop }, "build", "production")
  const main = config.config?.main
  if (!main) throw new Error("electron-vite config has no main build")
  await build({ ...main, logLevel: "silent", build: { ...main.build, outDir, emptyOutDir: true } })
}

describeSidecar("desktop sidecar file watcher", () => {
  const built = { dir: "" }
  // Building both bundles is setup, not the behaviour under test, so it gets its own
  // generous bound: on a loaded box it can take minutes.
  beforeAll(async () => {
    // Under the package's out/, like the real out/main, so externalized dependencies
    // resolve from its node_modules. out/ is gitignored, so a clean checkout lacks it.
    await fs.mkdir(path.join(desktop, "out"), { recursive: true })
    built.dir = await fs.mkdtemp(path.join(desktop, "out", "sidecar-test-"))
    await buildMain(path.join(built.dir, "main"))
  }, 900_000)
  afterAll(() => (built.dir ? fs.rm(built.dir, { recursive: true, force: true }) : undefined))

  test("delivers root file create, update and delete events from the built sidecar", async () => {
    await using tmp = await fs.mkdtemp(path.join(os.tmpdir(), "desktop-sidecar-")).then((dir) => ({
      dir,
      [Symbol.asyncDispose]: () => fs.rm(dir, { recursive: true, force: true }),
    }))
    const outDir = path.join(built.dir, "main")
    const repo = path.join(tmp.dir, "repo")
    const home = path.join(tmp.dir, "home")
    await fs.mkdir(repo, { recursive: true })
    await $`git init -q`.cwd(repo).quiet()
    await $`git -c user.email=test@opencode.test -c user.name=Test commit -q --allow-empty -m root`.cwd(repo).quiet()

    const port = 20000 + Math.floor(Math.random() * 20000)
    const password = "sidecar-test"
    const started = Promise.withResolvers<void>()
    const sidecar = Bun.spawn(["node", "--import", parentPortShim, path.join(outDir, "sidecar.js")], {
      env: {
        PATH: process.env.PATH,
        HOME: home,
        XDG_CONFIG_HOME: path.join(home, ".config"),
        XDG_DATA_HOME: path.join(home, ".local/share"),
        XDG_STATE_HOME: path.join(home, ".local/state"),
        XDG_CACHE_HOME: path.join(home, ".cache"),
        OPENCODE_DISABLE_AUTOUPDATE: "1",
        OPENCODE_DISABLE_MODELS_FETCH: "1",
        OPENCODE_EXPERIMENTAL_FILEWATCHER: "true",
      },
      stdout: "ignore",
      stderr: "ignore",
      serialization: "json",
      ipc: (message: { type: string; error?: { message: string } }) => {
        if (message.type === "ready") started.resolve()
        if (message.type === "error") started.reject(new Error(message.error?.message))
      },
    })
    try {
      void sidecar.exited.then((code) => started.reject(new Error(`sidecar exited ${code}`)))
      sidecar.send({ type: "start", hostname: "127.0.0.1", port, password, userDataPath: home })
      await started.promise

      const directory = await fs.realpath(repo)
      const query = `directory=${encodeURIComponent(directory)}`
      const url = (route: string) => `http://127.0.0.1:${port}${route}${route.includes("?") ? "&" : "?"}${query}`
      const events = await fetch(url("/event"), { headers: auth(password) })
      const seen: { file: string; event: string }[] = []
      void (async () => {
        const decoder = new TextDecoder()
        for await (const chunk of events.body!) {
          for (const line of decoder.decode(chunk).split("\n")) {
            if (!line.startsWith("data: ")) continue
            const payload = JSON.parse(line.slice(6))
            if (payload.type === "file.watcher.updated") seen.push(payload.properties)
          }
        }
      })().catch(() => {})
      const observed = async (file: string, event: string, trigger: () => Promise<unknown>) => {
        const deadline = Date.now() + 15_000
        await trigger()
        while (!seen.some((item) => item.file === file && item.event === event)) {
          if (Date.now() > deadline) throw new Error(`no ${event} event for ${file}; saw ${JSON.stringify(seen)}`)
          await Bun.sleep(100)
        }
      }

      // Boots the location, which starts its watches.
      expect((await fetch(url("/find/file?query=x"), { headers: auth(password) })).status).toBe(200)
      // The root watch confirms asynchronously: rewrite a probe until its first event arrives.
      const probe = path.join(directory, "sidecar-probe.txt")
      const deadline = Date.now() + 15_000
      while (!seen.some((item) => item.file === probe)) {
        if (Date.now() > deadline) throw new Error(`root watch never delivered; saw ${JSON.stringify(seen)}`)
        await fs.writeFile(probe, String(Math.random()))
        await Bun.sleep(250)
      }

      const file = path.join(directory, "sidecar-root.txt")
      await observed(file, "add", () => fs.writeFile(file, "a"))
      await observed(file, "change", () => fs.writeFile(file, "b"))
      await observed(file, "unlink", () => fs.rm(file))
    } finally {
      sidecar.kill("SIGKILL")
      await sidecar.exited
    }
  }, 120_000)
})

function auth(password: string) {
  return { authorization: `Basic ${btoa(`opencode:${password}`)}` }
}
