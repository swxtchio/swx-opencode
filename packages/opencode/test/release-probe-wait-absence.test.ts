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
      expect(output).toContain(`No producer-side wait request receipt during ${attack.phase}`)
    }
  }, 180_000)
}
