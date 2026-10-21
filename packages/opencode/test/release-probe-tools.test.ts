import { describe, expect, test } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import process from "node:process"
import { releaseInput, resolveReleaseIdentity } from "../script/release-probe-target"
import { runProcess } from "../script/release-probe-process"
import { startRequest } from "../script/release-probe-requests"

const releaseTag = "v1.18.32-swxtch.0ff0dfbc"
const sourceCommit = "0ff0dfbca1f01765f6bd41d0379d9b97f1cf8d23"
const version = "1.18.32-swxtch.0ff0dfbc"
const bunVersion = "1.4.2"

describe("release proof input resolution", () => {
  test("uses the released tag as the pull request default and derives its verified identity", () => {
    const request = releaseInput("pull_request", { pull_request: {} }, releaseTag)
    expect(
      resolveReleaseIdentity({
        request,
        sourceCommit,
        packageVersion: "1.18.32",
        tagCommit: sourceCommit,
        bunVersion,
        expectedBunVersion: bunVersion,
      }),
    ).toEqual({ releaseTag, sourceCommit, version, bunVersion })
  })

  test("resolves workflow_dispatch inputs and validates supplied commit and version", () => {
    const request = releaseInput(
      "workflow_dispatch",
      { inputs: { release_tag: releaseTag, source_commit: sourceCommit, version } },
      "v0.0.0-swxtch.00000000",
    )
    expect(
      resolveReleaseIdentity({
        request,
        sourceCommit,
        packageVersion: "1.18.32",
        tagCommit: sourceCommit,
        bunVersion,
        expectedBunVersion: bunVersion,
      }).version,
    ).toBe(version)
  })

  test("uses the release-tag default for a one-click workflow_dispatch rerun", () => {
    const request = releaseInput("workflow_dispatch", { inputs: {} }, releaseTag)
    expect(
      resolveReleaseIdentity({
        request,
        sourceCommit,
        packageVersion: "1.18.32",
        tagCommit: sourceCommit,
        bunVersion,
        expectedBunVersion: bunVersion,
      }),
    ).toEqual({ releaseTag, sourceCommit, version, bunVersion })
  })

  test("resolves repository_dispatch client payload with the same identity checks", () => {
    const request = releaseInput(
      "repository_dispatch",
      { client_payload: { release_tag: releaseTag, source_commit: sourceCommit, version } },
      "v0.0.0-swxtch.00000000",
    )
    expect(
      resolveReleaseIdentity({
        request,
        sourceCommit,
        packageVersion: "1.18.32",
        tagCommit: sourceCommit,
        bunVersion,
        expectedBunVersion: bunVersion,
      }),
    ).toEqual({ releaseTag, sourceCommit, version, bunVersion })
  })

  test("rejects a requested source commit that does not match the checked-out tag", () => {
    const request = { releaseTag, sourceCommit: "f".repeat(40), version }
    expect(() =>
      resolveReleaseIdentity({
        request,
        sourceCommit,
        packageVersion: "1.18.32",
        tagCommit: request.sourceCommit === sourceCommit ? "f".repeat(40) : sourceCommit,
        bunVersion,
        expectedBunVersion: bunVersion,
      }),
    ).toThrow()
  })

  test("rejects a requested version that does not match the checked-out tag", () => {
    const request = { releaseTag, sourceCommit, version: "1.18.31-swxtch.0ff0dfbc" }
    expect(() =>
      resolveReleaseIdentity({
        request,
        sourceCommit,
        packageVersion: "1.18.32",
        tagCommit: sourceCommit,
        bunVersion,
        expectedBunVersion: bunVersion,
      }),
    ).toThrow()
  })

  test("rejects a release tag that resolves to a different source commit", () => {
    const request = { releaseTag, sourceCommit, version }
    expect(() =>
      resolveReleaseIdentity({
        request,
        sourceCommit,
        packageVersion: "1.18.32",
        tagCommit: "f".repeat(40),
        bunVersion,
        expectedBunVersion: bunVersion,
      }),
    ).toThrow()
  })

  test("rejects a Bun runtime that differs from the selected source pin", () => {
    const request = { releaseTag, sourceCommit, version }
    expect(() =>
      resolveReleaseIdentity({
        request,
        sourceCommit,
        packageVersion: "1.18.32",
        tagCommit: sourceCommit,
        bunVersion: "1.4.1",
        expectedBunVersion: bunVersion,
      }),
    ).toThrow()
  })

  test("resolves an unpublished candidate from a real Git source and refuses conflicting tags", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-release-candidate-"))
    const source = path.join(root, "source")
    const remote = path.join(root, "remote.git")
    const event = path.join(root, "event.json")
    const output = path.join(root, "github-env")
    const script = path.resolve(import.meta.dir, "../script/release-probe-target.ts")
    const env = {
      ...process.env,
      RELEASE_SOURCE_DIR: source,
      GITHUB_EVENT_PATH: event,
      GITHUB_ENV: output,
      GITHUB_EVENT_NAME: "workflow_dispatch",
      RELEASE_PROBE_DEFAULT_TAG: releaseTag,
    }
    const git = (...args: string[]) => {
      const result = Bun.spawnSync(["git", ...args], { stdout: "pipe", stderr: "pipe", timeout: 10_000 })
      if (result.exitCode !== 0) throw new Error(result.stderr.toString())
      return result.stdout.toString().trim()
    }
    const resolve = (candidate = true) =>
      runProcess([process.execPath, script, ...(candidate ? ["--candidate"] : [])], {
        cwd: root,
        env,
        timeoutMs: 15_000,
        label: "candidate source identity",
      })
    try {
      git("clone", "--bare", "--shared", path.resolve(import.meta.dir, "../../.."), remote)
      git("clone", "--shared", remote, source)
      const commit = git("-C", source, "rev-parse", "HEAD")
      const packageVersion = (await Bun.file(path.join(source, "packages/opencode/package.json")).json()).version
      const version = `${packageVersion}-swxtch.${commit.slice(0, 8)}`
      const tag = `v${version}`
      await Bun.write(event, JSON.stringify({ inputs: { release_tag: tag, source_commit: commit, version } }))
      const candidate = await resolve()
      expect(candidate.exit).toBe(0)
      expect(JSON.parse(candidate.stdout)).toEqual({ releaseTag: tag, sourceCommit: commit, version, bunVersion: Bun.version })
      expect(await Bun.file(output).text()).toContain(`SOURCE_COMMIT=${commit}\nOPENCODE_VERSION=${version}\n`)

      const proof = await resolve(false)
      expect(proof.exit).not.toBe(0)
      expect(proof.stderr).toContain("Could not resolve release tag")

      git("--git-dir", remote, "tag", tag, git("-C", source, "rev-parse", "HEAD^"))
      const conflicting = await resolve()
      expect(conflicting.exit).not.toBe(0)
      expect(conflicting.stderr).toContain(`Tag ${tag} resolves to`)

      await Bun.write(event, JSON.stringify({ inputs: { release_tag: tag, version } }))
      const implicit = await resolve()
      expect(implicit.exit).not.toBe(0)
      expect(implicit.stderr).toContain("A full explicit source_commit is required")

      await Bun.write(event, JSON.stringify({ inputs: { release_tag: tag, source_commit: commit, version } }))
      git("-C", source, "remote", "set-url", "origin", path.join(root, "unavailable.git"))
      const unavailable = await resolve()
      expect(unavailable.exit).not.toBe(0)
      expect(unavailable.stderr).toContain("Could not resolve release tag")
    } finally {
      await fs.rm(root, { recursive: true, force: true })
    }
  }, 60_000)
})

describe("release probe request ownership", () => {
  test("aborts a timed-out fetch that is blocked in the producer", async () => {
    if (process.platform !== "linux") return
    let received = () => {}
    let aborted = () => {}
    let respond = (_response: Response) => {}
    const requestReceived = new Promise<void>((resolve) => (received = resolve))
    const requestAborted = new Promise<void>((resolve) => (aborted = resolve))
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch(request) {
        received()
        // Keep the producer response open until the owned client request aborts.
        return new Promise<Response>((resolve) => {
          respond = resolve
          request.signal.addEventListener(
            "abort",
            () => {
              aborted()
              resolve(new Response("aborted", { status: 499 }))
            },
            { once: true },
          )
        })
      },
    })
    const handle = startRequest(`http://127.0.0.1:${server.port}/wait`, { method: "POST" }, 250, "wait request test")
    try {
      await within(requestReceived, 1_000, "producer received test request")
      await expect(within(handle.result, 1_000, "owned request timeout")).rejects.toThrow(/timed out or was aborted/)
      await handle.done
      await within(requestAborted, 1_000, "producer observed request abort")
    } finally {
      handle.abort()
      respond(new Response("cleanup", { status: 200 }))
      await handle.done
      await server.stop(true)
    }
  })

  test("reaps a timed-out child and its inherited process group", async () => {
    if (process.platform !== "linux") return
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-release-probe-process-"))
    const pidsFile = path.join(root, "children")
    let processGroup: number | undefined
    try {
      // Both shells ignore TERM so cleanup must reach the whole process group with SIGKILL.
      const command = [
        "bash",
        "-c",
        `trap '' TERM; (trap '' TERM; sleep 30) & child=$!; printf '%s %s\\n' "$$" "$child" > '${pidsFile}'; wait`,
      ]
      await expect(
        runProcess(command, { cwd: root, env: process.env, timeoutMs: 250, label: "release probe child lifecycle test" }),
      ).rejects.toThrow(/Timed out waiting for release probe child lifecycle test/)
      const [parent, grandchild] = (await fs.readFile(pidsFile, "utf8")).trim().split(/\s+/).map(Number)
      processGroup = parent
      expect(processAlive(parent)).toBe(false)
      expect(processAlive(grandchild)).toBe(false)
      expect(groupAlive(parent)).toBe(false)
    } finally {
      if (processGroup !== undefined && groupAlive(processGroup)) {
        try {
          process.kill(-processGroup, "SIGKILL")
        } catch {}
      }
      await fs.rm(root, { recursive: true, force: true })
    }
  })
})

function processAlive(pid: number) {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    if (missingProcess(error)) return false
    throw error
  }
}

function groupAlive(pid: number) {
  try {
    process.kill(-pid, 0)
    return true
  } catch (error) {
    if (missingProcess(error)) return false
    throw error
  }
}

function missingProcess(error: unknown) {
  return error instanceof Error && "code" in error && error.code === "ESRCH"
}

async function within<T>(promise: Promise<T>, budgetMs: number, label: string) {
  let timer: Timer | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}`)), budgetMs)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}
