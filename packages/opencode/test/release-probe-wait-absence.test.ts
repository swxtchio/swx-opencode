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
  test("requires producer wait receipts before releasing each completed-turn hold", async () => {
    if (process.platform !== "linux" || (targetArch !== "arm64" && targetArch !== "x64"))
      throw new Error(`Expected a native linux target runner, got ${process.platform}-${targetArch}`)
    if (!version) throw new Error("OPENCODE_VERSION is required for the held-wait absence proof")

    const script = path.resolve(import.meta.dir, "../script/release-probe.ts")
    const preload = path.resolve(import.meta.dir, "../script/release-probe-delay-wait.preload.ts")
    for (const phase of ["tool", "continuation"] as const) {
      const proofRoot = await fs.mkdtemp(path.join(os.tmpdir(), `release-probe-wait-${phase}-`))
      const markers =
        phase === "tool"
          ? { start: "completed-tool-held", release: "completed-tool-release" }
          : { start: "completed-continuation-held", release: "completed-continuation-release" }
      const result = await runProcess(
        [process.execPath, "--preload", preload, script, targetArch, path.resolve(binary), proofRoot],
        {
          cwd: path.resolve(import.meta.dir, ".."),
          env: {
            ...process.env,
            OPENCODE_VERSION: version,
            RELEASE_PROBE_WAIT_START_FILE: path.join(proofRoot, "workspace", markers.start),
            RELEASE_PROBE_WAIT_RELEASE_FILE: path.join(proofRoot, "workspace", markers.release),
          },
          timeoutMs: 120_000,
          label: `wait dispatch suppression proof during ${phase} hold`,
        },
      ).finally(() => fs.rm(proofRoot, { recursive: true, force: true }))
      const output = `${result.stdout}\n${result.stderr}`
      if (evidenceDir) {
        await fs.mkdir(evidenceDir, { recursive: true })
        await fs.writeFile(
          path.join(evidenceDir, `native-${targetArch}-wait-red-${phase}.log`),
          output,
        )
      }
      expect(result.exit, output).not.toBe(0)
      expect(output).toContain(`No producer-side pending wait receipt during`)
      expect(output).toContain(phase === "tool" ? "the local Bash tool is held" : "the final provider continuation is held")
    }
  }, 180_000)
}
