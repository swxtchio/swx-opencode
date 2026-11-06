import { EventWriter } from "@opencode-ai/core/event/writer"
import { runMini } from "../../../src/cli/cmd/run"

const attach = process.env.SWX_MINI_ATTACH_URL
const readyFile = process.env.SWX_MINI_READY_FILE
const requestFile = process.env.SWX_MINI_REQUEST_FILE
const controlURL = process.env.SWX_MINI_CONTROL_URL
const controlFile = process.env.SWX_MINI_CONTROL_FILE
if (!attach || !readyFile || !requestFile || !controlURL || !controlFile)
  throw new Error("mini attach worker inputs are missing")

const identity = EventWriter.readIdentity(process.pid)
if (!identity?.bootID || !identity.startTicks) throw new Error("mini attach worker has no start identity")
await Bun.write(
  readyFile,
  JSON.stringify({
    pid: identity.pid,
    bootID: identity.bootID,
    startTicks: identity.startTicks,
    terminal: process.stdin.isTTY === true && process.stdout.isTTY === true,
  }),
)

const originalFetch = globalThis.fetch
let controlStarted = false
globalThis.fetch = (async (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
  const requestURL = input instanceof Request ? input.url : String(input)
  const method = init?.method ?? (input instanceof Request ? input.method : "GET")
  const timeoutDisabled = init !== undefined && "timeout" in init && init.timeout === false
  await Bun.write(requestFile, JSON.stringify({ url: new URL(requestURL).pathname, method, timeoutDisabled }))
  const request = originalFetch(input, init)
  if (!controlStarted && new URL(requestURL).pathname.includes("ses_transport_missing")) {
    controlStarted = true
    void Bun.sleep(2_500).then(async () => {
      try {
        const response = await originalFetch(controlURL)
        await response.text()
        await Bun.write(controlFile, JSON.stringify({ type: "completed" }))
      } catch (error) {
        await Bun.write(
          controlFile,
          JSON.stringify({ type: "failed", name: error instanceof Error ? error.name : "unknown" }),
        )
      }
    })
  }
  return request
}) as typeof globalThis.fetch

await runMini({ attach, session: "ses_transport_missing", prompt: "attach transport probe" })
