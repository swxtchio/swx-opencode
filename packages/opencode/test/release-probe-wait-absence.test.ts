import { test, expect } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import process from "node:process"
import { runProcess } from "../script/release-probe-process"

const binary = Bun.env.RELEASE_PROBE_BINARY
const version = Bun.env.OPENCODE_VERSION
const targetArch = Bun.env.RELEASE_PROBE_ARCH ?? process.arch
const evidenceDir = Bun.env.RELEASE_PROBE_RED_EVIDENCE_DIR

if (!binary) {
  test.skip("held-wait dispatch suppression integration (requires extracted release binary)", () => {})
} else {
  test("rejects selectively delayed main wait requests for completed, failed, and interrupted turns", async () => {
    if (process.platform !== "linux" || (targetArch !== "arm64" && targetArch !== "x64"))
      throw new Error(`Expected a native linux target runner, got ${process.platform}-${targetArch}`)
    if (!version) throw new Error("OPENCODE_VERSION is required for the held-wait absence proof")

    const script = path.resolve(import.meta.dir, "../script/release-probe.ts")
    const preload = path.resolve(import.meta.dir, "../script/release-probe-delay-wait.preload.ts")
    const attacks = [
      { scenario: "completed", phase: "the local Bash tool is held" },
      { scenario: "failed", phase: "the failed tool's provider continuation is held" },
      { scenario: "interrupted", phase: "the interruptible Bash tool is held" },
    ] as const
    for (const attack of attacks) {
      const proofRoot = await fs.mkdtemp(path.join(os.tmpdir(), `release-probe-wait-${attack.scenario}-`))
      const result = await runProcess(
        [process.execPath, "--preload", preload, script, targetArch, path.resolve(binary), proofRoot],
        {
          cwd: path.resolve(import.meta.dir, ".."),
          env: {
            ...process.env,
            OPENCODE_VERSION: version,
            RELEASE_PROBE_WAIT_SUPPRESS_SCENARIO: attack.scenario,
          },
          timeoutMs: 120_000,
          label: `main wait dispatch suppression proof for ${attack.scenario}`,
        },
      ).finally(() => fs.rm(proofRoot, { recursive: true, force: true }))
      const output = `${result.stdout}\n${result.stderr}`
      if (evidenceDir) {
        await fs.mkdir(evidenceDir, { recursive: true })
        await fs.writeFile(
          path.join(evidenceDir, `native-${targetArch}-wait-red-${attack.scenario}.log`),
          output,
        )
      }
      expect(result.exit, output).not.toBe(0)
      expect(output).toContain(`No producer wait registration before releasing the hold for sessionID=`)
    }
  }, 180_000)

  test("rejects a client-fabricated partial wait receipt", async () => {
    if (process.platform !== "linux" || (targetArch !== "arm64" && targetArch !== "x64"))
      throw new Error(`Expected a native linux target runner, got ${process.platform}-${targetArch}`)
    if (!version) throw new Error("OPENCODE_VERSION is required for the partial wait-prefix absence proof")

    const proofRoot = await fs.mkdtemp(path.join(os.tmpdir(), "release-probe-wait-partial-prefix-"))
    try {
      const script = path.resolve(import.meta.dir, "../script/release-probe.ts")
      const preload = path.resolve(import.meta.dir, "../script/release-probe-delay-wait.preload.ts")
      const result = await runProcess(
        [process.execPath, "--preload", preload, script, targetArch, path.resolve(binary), proofRoot],
        {
          cwd: path.resolve(import.meta.dir, ".."),
          env: {
            ...process.env,
            OPENCODE_VERSION: version,
            RELEASE_PROBE_WAIT_SUPPRESS_SCENARIO: "completed",
            RELEASE_PROBE_WAIT_ATTACK: "partial-prefix",
            RELEASE_PROBE_SERVER_TRACE: "1",
          },
          timeoutMs: 120_000,
          label: "partial client wait receipt attack",
        },
      )
      const output = `${result.stdout}\n${result.stderr}`
      if (evidenceDir) {
        await fs.mkdir(evidenceDir, { recursive: true })
        await fs.writeFile(path.join(evidenceDir, `native-${targetArch}-wait-red-partial-prefix.log`), output)
      }
      expect(result.exit, output).not.toBe(0)
      expect(output).toContain("No producer wait registration before releasing the hold for sessionID=")
      const sessionID = /error: No producer wait registration before releasing the hold for sessionID=([^\s;]+)/.exec(output)?.[1]
      if (!sessionID) throw new Error(`partial-prefix rejection omitted the main session ID: ${output}`)

      const trace = await fs.readFile(path.join(proofRoot, "opencode-server-0.trace"), "utf8")
      const prefix = trace
        .split(/\r?\n/)
        .find((line) => line.includes(`POST /api/session/${sessionID}/wait HTTP/1.1`))
      if (!prefix) throw new Error(`producer trace omitted the fabricated partial wait prefix for ${sessionID}`)
      const traceLine = prefix
        .replace(/Authorization: Basic [^\\"]+/i, "Authorization: Basic <redacted>")
        .replace(/x-opencode-directory: [^\\"]+/i, "x-opencode-directory: <redacted>")
      if (evidenceDir)
        await fs.writeFile(path.join(evidenceDir, `native-${targetArch}-wait-red-partial-prefix-trace.log`), `${traceLine}\n`)
      expect(prefix).toContain("X-Release-Probe-Wait-Receipt: client-fabricated")
      expect(prefix).not.toContain("\\r\\n\\r\\n")
      const serverLog = await fs.readFile(path.join(proofRoot, "xdg-data/opencode/log/opencode.log"), "utf8")
      expect(serverLog).not.toContain(`session_id=${sessionID}`)
    } finally {
      await fs.rm(proofRoot, { recursive: true, force: true })
    }
  }, 120_000)
}
