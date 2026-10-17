import { expect, test } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import process from "node:process"
import { runProcess } from "../script/release-probe-process"

const binary = Bun.env.RELEASE_PROBE_SOCKET_ONLY_BINARY
const version = Bun.env.OPENCODE_VERSION
const targetArch = Bun.env.RELEASE_PROBE_ARCH ?? process.arch
const evidenceDir = Bun.env.RELEASE_PROBE_RED_EVIDENCE_DIR

if (!binary) {
  test.skip("published-byte socket-only receipt attack (requires downloaded release binary)", () => {})
} else {
  test("rejects a complete producer socket request without the wait-handler registration", async () => {
    if (process.platform !== "linux" || (targetArch !== "arm64" && targetArch !== "x64"))
      throw new Error(`Expected a native linux target runner, got ${process.platform}-${targetArch}`)
    if (!version) throw new Error("OPENCODE_VERSION is required for the published-byte socket-only proof")

    const proofRoot = await fs.mkdtemp(path.join(os.tmpdir(), "release-probe-wait-socket-only-"))
    try {
      const script = path.resolve(import.meta.dir, "../script/release-probe.ts")
      const result = await runProcess(
        [process.execPath, "run", script, targetArch, path.resolve(binary), proofRoot],
        {
          cwd: path.resolve(import.meta.dir, ".."),
          env: {
            ...process.env,
            OPENCODE_VERSION: version,
            RELEASE_PROBE_SERVER_TRACE: "1",
          },
          timeoutMs: 120_000,
          label: "socket-only wait receipt attack against published bytes",
        },
      )
      const output = `${result.stdout}\n${result.stderr}`
      if (evidenceDir) {
        await fs.mkdir(evidenceDir, { recursive: true })
        await fs.writeFile(path.join(evidenceDir, `native-${targetArch}-wait-socket-only.log`), output)
      }
      expect(result.exit, output).not.toBe(0)
      expect(output).toContain("No producer wait registration before releasing the hold for sessionID=")
      const sessionID = /error: No producer wait registration before releasing the hold for sessionID=([^\s;]+)/.exec(output)?.[1]
      if (!sessionID) throw new Error(`socket-only rejection omitted the main session ID: ${output}`)

      const trace = await fs.readFile(path.join(proofRoot, "opencode-server-0.trace"), "utf8")
      const request = trace
        .split(/\r?\n/)
        .find((line) => line.includes(`POST /api/session/${sessionID}/wait HTTP/1.1`))
      if (!request) throw new Error(`producer trace did not contain the main wait POST for ${sessionID}`)
      const traceLine = request
        .replace(/Authorization: Basic [^\\"]+/i, "Authorization: Basic <redacted>")
        .replace(/x-opencode-directory: [^\\"]+/i, "x-opencode-directory: <redacted>")
      if (evidenceDir) await fs.writeFile(path.join(evidenceDir, `native-${targetArch}-socket-only-producer-trace.log`), `${traceLine}\n`)
      expect(request).toContain("Content-Length: 0\\r\\n\\r\\n")
      const serverLog = await fs.readFile(path.join(proofRoot, "xdg-data/opencode/log/opencode.log"), "utf8")
      expect(serverLog).not.toContain(`event=session.wait.registered session_id=${sessionID}`)
    } finally {
      await fs.rm(proofRoot, { recursive: true, force: true })
    }
  }, 180_000)
}
