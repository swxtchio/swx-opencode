#!/usr/bin/env bun

import fs from "node:fs/promises"
import path from "node:path"
import { abortAllRequests, requestText, startRequest } from "./release-probe-requests"
import { runProcess, spawnProcess } from "./release-probe-process"

type Scenario = "completed" | "failed" | "interrupted"
type Message = {
  id: string
  type: string
  content?: Array<{
    type: string
    text?: string
    name?: string
    state?: { status: string; error?: { message?: string } }
  }>
  error?: { message?: string; origin?: string }
}
type HistoryEvent = { id: string; type: string; durable?: { seq: number } }
type ApiResult = { status: number; body: unknown; text: string }
type AdmissionReceipt = { status: number; sessionID: string; id: string; admittedSeq: number }
type WaitLogInput = {
  event: "session.wait.registered" | "session.wait.resolved"
  sessionID: string
  admittedSeq: number
  messageID: string
  waitID?: string
  outcome?: string
}
type WaitResult = {
  type: string
  admittedSeq?: number
  messageID?: string
  assistantMessageID?: string
}

const [targetArch, binaryArg, proofRootArg] = Bun.argv.slice(2)
if (!targetArch || !binaryArg || !proofRootArg) {
  throw new Error("Usage: bun release-probe.ts <arm64|x64> <extracted-opencode> <proof-root>")
}
if (targetArch !== "arm64" && targetArch !== "x64") throw new Error(`Unsupported target architecture: ${targetArch}`)
if (process.platform !== "linux" || process.arch !== targetArch)
  throw new Error(`Expected native linux-${targetArch} runtime, got ${process.platform}-${process.arch}`)

const binary = path.resolve(binaryArg)
const proofRoot = path.resolve(proofRootArg)
const workspace = path.join(proofRoot, "workspace")
const home = path.join(proofRoot, "home")
const data = path.join(proofRoot, "xdg-data")
const state = path.join(proofRoot, "xdg-state")
const cache = path.join(proofRoot, "xdg-cache")
const temporary = path.join(proofRoot, "tmp")
const database = path.join(proofRoot, "session.sqlite")
const serverLogFile = path.join(data, "opencode", "log", "opencode.log")
const expectedVersion = Bun.env.OPENCODE_VERSION
if (!expectedVersion) throw new Error("OPENCODE_VERSION is required for the release attestation")

for (const directory of [workspace, home, data, state, cache, temporary, path.join(workspace, "src/nested")])
  await fs.mkdir(directory, { recursive: true })
const git = Bun.spawnSync(["git", "-C", workspace, "init", "--quiet"], { stdout: "pipe", stderr: "pipe" })
if (git.exitCode !== 0) throw new Error(`git init failed: ${git.stderr.toString()}`)
await Bun.write(path.join(workspace, "src/nested/needle-target.ts"), "export const releaseProof = true\n")

const password = "opencode-release-proof"
const authorization = `Basic ${Buffer.from(`opencode:${password}`).toString("base64")}`
const stateful: {
  counts: Record<Scenario, number>
  completedContinuationStarted: boolean
  failedContinuationStarted: boolean
  titleCalls: number
  otherCalls: number
  requests: Array<{ scenario: Scenario; ordinal: number; hasToolResult: boolean }>
} = {
  counts: { completed: 0, failed: 0, interrupted: 0 },
  completedContinuationStarted: false,
  failedContinuationStarted: false,
  titleCalls: 0,
  otherCalls: 0,
  requests: [],
}

const completedGate = gate()
const failedGate = gate()
const llm = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  async fetch(request) {
    const url = new URL(request.url)
    if (url.pathname === "/control/state")
      return new Response(JSON.stringify(stateful), { headers: { "content-type": "application/json" } })
    if (url.pathname === "/control/release/completed" && request.method === "POST") {
      completedGate.release()
      return new Response(null, { status: 204 })
    }
    if (url.pathname === "/control/release/failed" && request.method === "POST") {
      failedGate.release()
      return new Response(null, { status: 204 })
    }
    if (request.method !== "POST" || !url.pathname.endsWith("/chat/completions"))
      return new Response("not found", { status: 404 })

    const body = (await request.json()) as { messages?: Array<{ role?: string }> }
    const serialized = JSON.stringify(body)
    if (serialized.includes("Generate a title for this conversation")) {
      stateful.titleCalls++
      return textResponse("Native release proof")
    }

    const scenario = (["completed", "failed", "interrupted"] as const).find((item) =>
      serialized.includes(`case:${item}`),
    )
    if (!scenario) {
      stateful.otherCalls++
      return textResponse("ok")
    }

    const ordinal = ++stateful.counts[scenario]
    stateful.requests.push({
      scenario,
      ordinal,
      hasToolResult: body.messages?.some((message) => message.role === "tool") ?? false,
    })
    if (ordinal === 1) return toolResponse(scenario)
    if (ordinal === 2 && scenario === "completed") {
      stateful.completedContinuationStarted = true
      await completedGate.promise
      return textResponse("completed after the released tool")
    }
    if (ordinal === 2 && scenario === "failed") {
      stateful.failedContinuationStarted = true
      await failedGate.promise
      return textResponse("continued after the failed tool")
    }
    return textResponse("terminal continuation")
  },
})

const providerConfig = {
  providers: {
    test: {
      name: "Release probe",
      api: {
        type: "aisdk",
        package: "@ai-sdk/openai-compatible",
        url: `http://127.0.0.1:${llm.port}/v1`,
        settings: {},
      },
      request: { body: { apiKey: "test-key" } },
      models: {
        "test-model": {
          name: "Release probe model",
          api: { id: "test-model" },
          capabilities: { tools: true, input: ["text"], output: ["text"] },
          limit: { context: 100_000, output: 10_000 },
          cost: { input: 0, output: 0 },
        },
      },
    },
  },
}
const providerConfigText = JSON.stringify(providerConfig)
const permissionConfigText = JSON.stringify({ permission: { bash: "allow", read: "allow" } })
const globalConfig = path.join(home, ".config/opencode")
await fs.mkdir(globalConfig, { recursive: true })
await Bun.write(path.join(globalConfig, "opencode.json"), permissionConfigText)
await Bun.write(path.join(globalConfig, "opencode.jsonc"), providerConfigText)
const stageProject = Bun.spawnSync(["git", "-C", workspace, "add", "src"])
if (stageProject.exitCode !== 0) throw new Error(`git add failed in the release probe project: ${stageProject.stderr.toString()}`)
const commitProject = Bun.spawnSync([
  "git",
  "-C",
  workspace,
  "-c",
  "user.name=OpenCode release probe",
  "-c",
  "user.email=release-probe@example.invalid",
  "commit",
  "--quiet",
  "-m",
  "release probe project",
])
if (commitProject.exitCode !== 0)
  throw new Error(`git commit failed in the release probe project: ${commitProject.stderr.toString()}`)

const opencodeEnvironment: Record<string, string | undefined> = {
  ...process.env,
  PATH: "/usr/bin:/bin",
  HOME: home,
  XDG_CONFIG_HOME: path.join(home, ".config"),
  XDG_DATA_HOME: data,
  XDG_STATE_HOME: state,
  XDG_CACHE_HOME: cache,
  TMPDIR: temporary,
  BUN_TMPDIR: temporary,
  OPENCODE_DB: database,
  OPENCODE_SERVER_PASSWORD: password,
  OPENCODE_DISABLE_MODELS_FETCH: "1",
  OPENCODE_DISABLE_AUTOUPDATE: "1",
  OPENCODE_PURE: "1",
  OPENCODE_LOG_LEVEL: "INFO",
}
delete opencodeEnvironment.OPENCODE_DISABLE_FFF
delete opencodeEnvironment.BUN_OPTIONS
delete opencodeEnvironment.RELEASE_PROBE_WAIT_START_FILE
delete opencodeEnvironment.RELEASE_PROBE_WAIT_RELEASE_FILE
delete opencodeEnvironment.RELEASE_PROBE_WAIT_SUPPRESS_SCENARIO
delete opencodeEnvironment.RELEASE_PROBE_SERVER_TRACE

let serverUrl = ""
let serverExitCode: number | undefined
let serverNumber = 0
const report: Record<string, unknown> = {
  architecture: targetArch,
  runtime_arch: process.arch,
  version: expectedVersion,
  database: database,
}
let server: Awaited<ReturnType<typeof startServer>> | undefined
try {
  const version = await runBinary(["--version"], workspace, opencodeEnvironment)
  if (version.stdout.trim() !== expectedVersion) throw new Error(`--version mismatch: ${version.stdout.trim()}`)
  report.version_output = version.stdout.trim()

  const search = await runBinary(["debug", "file", "search", "needle-target"], workspace, opencodeEnvironment)
  if (!search.stdout.includes("needle-target.ts")) throw new Error(`FFF sidecar search missed fixture: ${search.stdout}`)
  report.fff_search = search.stdout.trim()

  report.database_selection = await databaseSelection()

  server = await startServer()
  report.idle = await idleOutcome()
  report.pending = await pendingOutcome()
  report.completed = await completedOutcome()
  const settled = report.completed as Awaited<ReturnType<typeof completedOutcome>>
  await server.stop()
  server = await startServer()
  report.restart = await restartReadback(settled)
  report.failed = await failedOutcome()
  report.interrupted = await interruptedOutcome()
  report.provider_requests = await providerState()

  await Bun.write(path.join(proofRoot, "evidence.json"), JSON.stringify(report, null, 2) + "\n")
  console.log(JSON.stringify(report, null, 2))
} finally {
  for (const name of ["completed", "failed", "interrupted"])
    await fs.writeFile(path.join(workspace, `${name}-tool-release`), "release")
  await fs.writeFile(path.join(workspace, "completed-continuation-release"), "release")
  completedGate.release()
  failedGate.release()
  try {
    await abortAllRequests()
    if (server) await server.stop()
  } finally {
    await llm.stop(true)
  }
}

function gate() {
  let release = () => {}
  const promise = new Promise<void>((resolve) => {
    release = resolve
  })
  return { promise, release }
}

function chunk(delta: Record<string, unknown>, finish?: string) {
  return {
    id: "chatcmpl-release-probe",
    object: "chat.completion.chunk",
    choices: [{ index: 0, delta, ...(finish ? { finish_reason: finish } : {}) }],
  }
}

function streamResponse(parts: unknown[]) {
  return new Response(
    parts.map((part) => `data: ${JSON.stringify(part)}\n\n`).join("") + "data: [DONE]\n\n",
    { headers: { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" } },
  )
}

function textResponse(text: string) {
  return streamResponse([
    chunk({ role: "assistant" }),
    chunk({ content: text }),
    chunk({}, "stop"),
  ])
}

function toolResponse(scenario: Scenario) {
  const gateFile = path.join(workspace, `${scenario}-tool-release`)
  const toolName = scenario === "failed" ? "not_registered" : "bash"
  const input =
    scenario === "failed"
      ? {}
      : {
          command: `while [ ! -f "${gateFile}" ]; do sleep 0.05; done; printf '${scenario} tool released\\n'`,
          timeout: 120_000,
        }
  return streamResponse([
    chunk({ role: "assistant" }),
    chunk({
      tool_calls: [
        {
          index: 0,
          id: `call-${scenario}`,
          type: "function",
          function: { name: toolName, arguments: "" },
        },
      ],
    }),
    chunk({ tool_calls: [{ index: 0, function: { arguments: JSON.stringify(input) } }] }),
    chunk({}, "tool_calls"),
  ])
}

async function apiRaw(route: string, init: RequestInit = {}, timeoutMs = 15_000): Promise<ApiResult> {
  return parseApiResult(
    await requestText(`${serverUrl}${route}`, apiInit(init), timeoutMs, `${init.method ?? "GET"} ${route}`),
  )
}

function apiRequest(route: string, init: RequestInit, timeoutMs: number) {
  return startRequest(`${serverUrl}${route}`, apiInit(init), timeoutMs, `${init.method ?? "GET"} ${route}`)
}

function apiInit(init: RequestInit) {
  const headers = new Headers(init.headers)
  headers.set("authorization", authorization)
  headers.set("x-opencode-directory", workspace)
  return { ...init, headers }
}

function parseApiResult(response: { status: number; text: string }): ApiResult {
  let body: unknown
  try {
    body = response.text.length ? JSON.parse(response.text) : undefined
  } catch {
    body = undefined
  }
  return { status: response.status, body, text: response.text }
}

async function apiJson<T>(route: string, init: RequestInit = {}): Promise<T> {
  const result = await apiRaw(route, init)
  if (result.status < 200 || result.status >= 300)
    throw new Error(`${init.method ?? "GET"} ${route} returned HTTP ${result.status}: ${result.text}`)
  return result.body as T
}

async function until<T>(label: string, read: () => Promise<T | undefined>, budgetMs = 60_000): Promise<T> {
  const deadline = Date.now() + budgetMs
  while (Date.now() < deadline) {
    const value = await read()
    if (value !== undefined) return value
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`Timed out waiting for ${label}`)
}

async function waitLogRecord(input: WaitLogInput) {
  return until(
    `producer log ${input.event} for ${input.sessionID}`,
    async () => {
      const logs = await fs.readFile(serverLogFile, "utf8").catch(() => "")
      return logs.split(/\r?\n/).find(
        (line) =>
          line.includes(`event=${input.event}`) &&
          line.includes(`session_id=${input.sessionID}`) &&
          line.includes(`admitted_seq=${input.admittedSeq}`) &&
          line.includes(`message_id=${input.messageID}`) &&
          (input.waitID === undefined || line.includes(`wait_id=${input.waitID}`)) &&
          (input.outcome === undefined || line.includes(`outcome=${input.outcome}`)),
      )
    },
    10_000,
  )
}

async function startServer() {
  const probe = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response("probe") })
  const port = probe.port
  await probe.stop(true)
  const traceFile =
    Bun.env.RELEASE_PROBE_SERVER_TRACE === "1"
      ? path.join(proofRoot, `opencode-server-${serverNumber++}.trace`)
      : undefined
  const serverArgs = [binary, "serve", "--pure", "--print-logs", "--hostname", "127.0.0.1", "--port", String(port)]
  const command = traceFile
    ? ["strace", "-f", "-s", "8192", "-e", "trace=read,readv,recvfrom,recvmsg", "-o", traceFile, ...serverArgs]
    : serverArgs
  const serverProcess = spawnProcess(command, { cwd: workspace, env: opencodeEnvironment })
  let stdout = ""
  let stderr = ""
  let modelCatalog = "not requested yet"
  const stdoutDone = serverProcess.stdout.then((value) => (stdout = value))
  const stderrDone = serverProcess.stderr.then((value) => (stderr = value))
  serverUrl = `http://127.0.0.1:${port}`
  serverExitCode = undefined
  void serverProcess.child.exited.then((code) => (serverExitCode = code))
  try {
    await until(
      "authenticated configured V2 model",
      async () => {
        if (serverExitCode !== undefined) throw new Error(`OpenCode exited before readiness (${serverExitCode})`)
        try {
          const result = await apiRaw("/api/model", {}, 5_000)
          modelCatalog = `status=${result.status} body=${JSON.stringify(result.body).slice(0, 4000)}`
          if (result.status === 401) throw new Error("OpenCode server rejected Basic authentication")
          if (result.status !== 200) return undefined
          const models = (result.body as { data: Array<{ id: string; providerID: string }> }).data
          return models.find((model) => model.id === "test-model" && model.providerID === "test")
        } catch (error) {
          if (serverExitCode !== undefined) throw error
          if (error instanceof Error && error.message.includes("rejected Basic authentication")) throw error
          return undefined
        }
      },
      120_000,
    )
  } catch (error) {
    await serverProcess.stop("OpenCode startup cleanup")
    await Promise.all([stdoutDone, stderrDone])
    await Bun.write(path.join(proofRoot, "opencode-startup-failure.log"), `${stdout}\n${stderr}`)
    throw new Error(`${String(error)}\nmodel_catalog=${modelCatalog}\n${stderr}`)
  }
  return {
    traceFile,
    async stop() {
      await serverProcess.stop("OpenCode server shutdown")
      await Promise.all([stdoutDone, stderrDone])
      await fs.appendFile(path.join(proofRoot, "opencode-server.log"), stdout + stderr)
    },
  }
}

async function providerState() {
  const response = await requestText(`http://127.0.0.1:${llm.port}/control/state`, {}, 15_000, "provider control state")
  if (response.status < 200 || response.status >= 300)
    throw new Error(`provider control returned HTTP ${response.status}: ${response.text}`)
  return JSON.parse(response.text) as typeof stateful
}

async function releaseProviderGate(scenario: "completed" | "failed") {
  const response = await requestText(
    `http://127.0.0.1:${llm.port}/control/release/${scenario}`,
    { method: "POST" },
    15_000,
    `provider ${scenario} gate release`,
  )
  if (response.status !== 204)
    throw new Error(`provider ${scenario} gate release returned HTTP ${response.status}: ${response.text}`)
}

async function providerUntil(label: string, predicate: (value: typeof stateful) => boolean) {
  return until(label, async () => {
    const state = await providerState()
    return predicate(state) ? state : undefined
  })
}

async function createSession() {
  const response = await apiJson<{ data: { id: string } }>("/api/session", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ model: { id: "test-model", providerID: "test" }, location: { directory: workspace } }),
  })
  return response.data
}

async function admit(sessionID: string, text: string, resume = true): Promise<AdmissionReceipt> {
  const response = await apiRaw(`/api/session/${sessionID}/prompt`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ prompt: { text }, resume }),
  })
  const data = (response.body as { data?: { id?: unknown; admittedSeq?: unknown } } | undefined)?.data
  if (response.status !== 200 || typeof data?.id !== "string" || typeof data.admittedSeq !== "number")
    throw new Error(`Prompt admission receipt mismatch: ${JSON.stringify(response)}`)
  return { status: response.status, sessionID, id: data.id, admittedSeq: data.admittedSeq }
}

async function messages(sessionID: string) {
  const response = await apiJson<{ data: Message[] }>(`/api/session/${sessionID}/message`)
  return response.data
}

function findTool(messages: Message[], name?: string) {
  return messages
    .filter((message) => message.type === "assistant")
    .flatMap((message) => message.content ?? [])
    .find((content) => content.type === "tool" && (name === undefined || content.name === name))
}

function startWait(sessionID: string) {
  if (!server) throw new Error("Cannot start a wait request without a running producer")
  const state: { settled: boolean; response?: ApiResult; error?: string } = { settled: false }
  const request = apiRequest(`/api/session/${sessionID}/wait`, { method: "POST" }, 120_000)
  const promise = request.result
    .then(parseApiResult)
    .then((response) => {
      state.settled = true
      state.response = response
    })
    .catch((error) => {
      state.settled = true
      state.error = String(error)
    })
  return { state, promise, abort: request.abort, done: request.done }
}

async function assertRunningWaitPending(
  sessionID: string,
  admission: AdmissionReceipt,
  waiter: ReturnType<typeof startWait>,
  phase: string,
) {
  if (admission.status !== 200 || admission.sessionID !== sessionID || !admission.id || !Number.isInteger(admission.admittedSeq))
    throw new Error(`No producer admission receipt for ${phase}: ${JSON.stringify(admission)}`)
  const active = await apiRaw("/api/session/active")
  const activeData = (active.body as { data?: Record<string, { type: string }> } | undefined)?.data
  if (active.status !== 200 || activeData?.[sessionID]?.type !== "running")
    throw new Error(`Session was not positively active while ${phase}: ${JSON.stringify(active)}`)
  const messageReadback = await apiRaw(`/api/session/${sessionID}/message/${admission.id}`)
  const producerMessage = (messageReadback.body as { data?: Message } | undefined)?.data
  if (messageReadback.status !== 200 || producerMessage?.id !== admission.id || producerMessage.type !== "user")
    throw new Error(`Producer message readback did not match the held admission during ${phase}: ${JSON.stringify(messageReadback)}`)
  let registrationLine: string
  try {
    registrationLine = await waitLogRecord({
      event: "session.wait.registered",
      sessionID,
      admittedSeq: admission.admittedSeq,
      messageID: admission.id,
    })
  } catch (error) {
    throw new Error(`No producer wait registration before releasing the hold for sessionID=${sessionID} admitted_seq=${admission.admittedSeq} during ${phase}: ${String(error)}`)
  }
  const waitID = /(?:^|\s)wait_id=([^\s]+)/.exec(registrationLine)?.[1]
  if (!waitID) throw new Error(`Producer wait registration omitted wait_id for ${sessionID}: ${registrationLine}`)
  if (waiter.state.settled) throw new Error(`wait returned before ${phase}: ${JSON.stringify(waiter.state.response)}`)
  return {
    phase,
    main_session_id: sessionID,
    active_status: active.status,
    active_type: activeData[sessionID].type,
    admission_receipt: admission,
    producer_message_readback: {
      status: messageReadback.status,
      id: producerMessage.id,
      type: producerMessage.type,
    },
    producer_wait_registration: {
      wait_id: waitID,
      sessionID,
      messageID: admission.id,
      admittedSeq: admission.admittedSeq,
      record: registrationLine,
    },
    main_wait_client_unsettled: waiter.state.settled === false,
  }
}

async function idleOutcome() {
  const session = await createSession()
  const response = await apiRaw(`/api/session/${session.id}/wait`, { method: "POST" })
  if (response.status !== 200 || (response.body as { data: WaitResult }).data.type !== "idle")
    throw new Error(`new native Session did not return idle: ${JSON.stringify(response)}`)
  return { sessionID: session.id, status: response.status, result: (response.body as { data: WaitResult }).data }
}

async function pendingOutcome(text = "case:pending do not resume") {
  return pendingOutcomeWithText(text)
}

async function pendingOutcomeWithText(text: string) {
  const session = await createSession()
  const admission = await admit(session.id, text, false)
  const response = await apiRaw(`/api/session/${session.id}/wait`, { method: "POST" })
  const result = (response.body as { data?: WaitResult } | undefined)?.data
  if (
    response.status !== 200 ||
    result?.type !== "pending" ||
    result.admittedSeq !== admission.admittedSeq ||
    result.messageID !== admission.id
  )
    throw new Error(`native admit-only wait mismatch: ${JSON.stringify({ response, admission })}`)
  return { sessionID: session.id, admission, status: response.status, result }
}

async function completedOutcome() {
  const session = await createSession()
  const admission = await admit(session.id, "case:completed run the gated bash tool then answer")
  await providerUntil("first completed tool request", (state) => state.counts.completed >= 1)
  const heldTool = await until("bash tool to enter running state", async () => {
    const tool = findTool(await messages(session.id), "bash")
    return tool?.state?.status === "running" ? tool : undefined
  })
  await fs.writeFile(path.join(workspace, "completed-tool-held"), "held")
  const waiter = startWait(session.id)
  const toolHoldReceipt = await assertRunningWaitPending(
    session.id,
    admission,
    waiter,
    "the local Bash tool is held",
  )

  await fs.writeFile(path.join(workspace, "completed-tool-release"), "release")
  const continuation = await providerUntil("final provider continuation to be held", (state) => state.completedContinuationStarted)
  const settledTool = await until("Bash tool settlement before provider continuation", async () => {
    const tool = findTool(await messages(session.id), "bash")
    return tool?.state?.status === "completed" ? tool : undefined
  })
  if (!continuation.requests.some((request) => request.scenario === "completed" && request.ordinal === 2 && request.hasToolResult))
    throw new Error("provider continuation did not receive the settled tool result")
  await fs.writeFile(path.join(workspace, "completed-continuation-held"), "held")
  const continuationHoldReceipt = await assertRunningWaitPending(
    session.id,
    admission,
    waiter,
    "the final provider continuation is held",
  )
  if (toolHoldReceipt.producer_wait_registration.wait_id !== continuationHoldReceipt.producer_wait_registration.wait_id)
    throw new Error("completed wait registered different producer wait IDs across its held phases")
  await fs.writeFile(path.join(workspace, "completed-continuation-release"), "release")
  await releaseProviderGate("completed")

  await waiter.promise
  if (waiter.state.error) throw new Error(`completed wait request failed: ${waiter.state.error}`)
  const response = waiter.state.response
  if (!response || response.status !== 200) throw new Error(`completed wait HTTP response mismatch: ${JSON.stringify(response)}`)
  const result = (response.body as { data: WaitResult }).data
  if (result.type !== "completed" || result.admittedSeq !== admission.admittedSeq || !result.assistantMessageID)
    throw new Error(`completed wait result mismatch: ${JSON.stringify({ result, admission })}`)
  const waitResolution = await waitLogRecord({
    event: "session.wait.resolved",
    sessionID: session.id,
    admittedSeq: admission.admittedSeq,
    messageID: admission.id,
    waitID: toolHoldReceipt.producer_wait_registration.wait_id,
    outcome: result.type,
  })

  const allMessages = await messages(session.id)
  const user = allMessages.find((message) => message.id === admission.id)
  const assistant = allMessages.find((message) => message.id === result.assistantMessageID)
  if (user?.type !== "user") throw new Error("admitted user message was not projected")
  if (assistant?.type !== "assistant" || !assistant.content?.some((content) => content.type === "text" && content.text?.includes("completed after the released tool")))
    throw new Error("terminal assistant message was not projected")
  const history = await apiJson<{ data: HistoryEvent[]; hasMore: boolean }>(`/api/session/${session.id}/history?limit=100`)
  if (history.hasMore) throw new Error("completed Session history unexpectedly has another page")
  const events = history.data.map((event) => ({ id: event.id, seq: event.durable?.seq, type: event.type }))
  if (events.some((event) => event.seq === undefined)) throw new Error("history returned an event without a durable sequence")
  for (let index = 1; index < events.length; index++)
    if (events[index].seq! <= events[index - 1].seq!) throw new Error("history sequences are not strictly increasing")
  if (!events.some((event) => event.seq === admission.admittedSeq)) throw new Error("admission sequence missing from history")
  return {
    sessionID: session.id,
    admission,
    result,
    held_tool_status: heldTool.state?.status,
    settled_tool_status: settledTool.state?.status,
    held_phase_wait_evidence: [toolHoldReceipt, continuationHoldReceipt],
    wait_resolution: waitResolution,
    userMessageID: user.id,
    assistantMessageID: assistant.id,
    history: events,
    hasMore: history.hasMore,
  }
}

async function restartReadback(completed: Awaited<ReturnType<typeof completedOutcome>>) {
  const user = await apiJson<{ data: Message }>(`/api/session/${completed.sessionID}/message/${completed.userMessageID}`)
  const assistant = await apiJson<{ data: Message }>(`/api/session/${completed.sessionID}/message/${completed.assistantMessageID}`)
  if (user.data.id !== completed.userMessageID || user.data.type !== "user") throw new Error("user message did not survive restart")
  if (assistant.data.id !== completed.assistantMessageID || assistant.data.type !== "assistant")
    throw new Error("assistant message did not survive restart")
  const after = await apiJson<{ data: HistoryEvent[]; hasMore: boolean }>(
    `/api/session/${completed.sessionID}/history?after=${completed.admission.admittedSeq}&limit=100`,
  )
  const expected = completed.history.filter((event) => event.seq! > completed.admission.admittedSeq)
  const observed = after.data.map((event) => ({ id: event.id, seq: event.durable?.seq, type: event.type }))
  if (after.hasMore || JSON.stringify(observed) !== JSON.stringify(expected))
    throw new Error(`exclusive history replay mismatch after restart: ${JSON.stringify({ observed, expected, hasMore: after.hasMore })}`)
  return {
    userMessageID: user.data.id,
    assistantMessageID: assistant.data.id,
    cursor: completed.admission.admittedSeq,
    exclusiveOrdering: true,
    replayedEvents: observed,
  }
}

async function failedOutcome() {
  const session = await createSession()
  const admission = await admit(session.id, "case:failed call the unavailable test tool")
  await providerUntil("failed-tool continuation to be held", (value) => value.failedContinuationStarted)
  const failedTool = await until("unrecovered tool failure projection", async () => {
    const tool = findTool(await messages(session.id), "not_registered")
    return tool?.state?.status === "error" ? tool : undefined
  })
  const waiter = startWait(session.id)
  const waitEvidence = await assertRunningWaitPending(
    session.id,
    admission,
    waiter,
    "the failed tool's provider continuation is held",
  )
  await releaseProviderGate("failed")
  await waiter.promise
  if (waiter.state.error) throw new Error(`failed wait request failed: ${waiter.state.error}`)
  const response = waiter.state.response
  const result = response?.body ? (response.body as { data: WaitResult }).data : undefined
  if (response?.status !== 200 || result?.type !== "failed" || result.admittedSeq !== admission.admittedSeq)
    throw new Error(`failed wait result mismatch: ${JSON.stringify({ response, admission })}`)
  const waitResolution = await waitLogRecord({
    event: "session.wait.resolved",
    sessionID: session.id,
    admittedSeq: admission.admittedSeq,
    messageID: admission.id,
    waitID: waitEvidence.producer_wait_registration.wait_id,
    outcome: result.type,
  })
  const history = await apiJson<{ data: HistoryEvent[]; hasMore: boolean }>(`/api/session/${session.id}/history?limit=100`)
  if (!history.data.some((event) => event.type.toLowerCase().includes("tool.failed")))
    throw new Error("failed tool terminal event was not persisted")
  return {
    sessionID: session.id,
    admission,
    result,
    wait_evidence: waitEvidence,
    wait_resolution: waitResolution,
    failed_tool_status: failedTool.state?.status,
    terminal_event_persisted: true,
  }
}

async function interruptedOutcome() {
  const session = await createSession()
  const admission = await admit(session.id, "case:interrupted run the interruptible gated tool")
  await providerUntil("interrupted tool provider request", (value) => value.counts.interrupted >= 1)
  await until("interruptible Bash tool running state", async () => {
    const tool = findTool(await messages(session.id), "bash")
    return tool?.state?.status === "running" ? tool : undefined
  })
  await fs.writeFile(path.join(workspace, "interrupted-tool-held"), "held")
  const waiter = startWait(session.id)
  const waitEvidence = await assertRunningWaitPending(
    session.id,
    admission,
    waiter,
    "the interruptible Bash tool is held",
  )
  const interrupt = await apiRaw(`/api/session/${session.id}/interrupt`, { method: "POST" })
  if (interrupt.status !== 204) throw new Error(`interrupt returned HTTP ${interrupt.status}: ${interrupt.text}`)
  await waiter.promise
  if (waiter.state.error) throw new Error(`interrupted wait request failed: ${waiter.state.error}`)
  const response = waiter.state.response
  const result = response?.body ? (response.body as { data: WaitResult }).data : undefined
  if (response?.status !== 200 || result?.type !== "interrupted" || result.admittedSeq !== admission.admittedSeq)
    throw new Error(`interrupted wait result mismatch: ${JSON.stringify({ response, admission })}`)
  const waitResolution = await waitLogRecord({
    event: "session.wait.resolved",
    sessionID: session.id,
    admittedSeq: admission.admittedSeq,
    messageID: admission.id,
    waitID: waitEvidence.producer_wait_registration.wait_id,
    outcome: result.type,
  })
  const allMessages = await messages(session.id)
  const tool = findTool(allMessages, "bash")
  if (tool?.state?.status !== "error" || !tool.state.error?.message?.toLowerCase().includes("interrupt"))
    throw new Error(`terminal tool cleanup was not persisted: ${JSON.stringify(tool)}`)
  const history = await apiJson<{ data: HistoryEvent[]; hasMore: boolean }>(`/api/session/${session.id}/history?limit=100`)
  if (!history.data.some((event) => event.type.toLowerCase().includes("tool.failed")))
    throw new Error("interrupted tool cleanup event was not persisted")
  return {
    sessionID: session.id,
    admission,
    result,
    wait_evidence: waitEvidence,
    wait_resolution: waitResolution,
    tool_cleanup: tool.state.error.message,
    terminal_event_persisted: true,
  }
}

async function databaseSelection() {
  const root = path.join(proofRoot, "database-selection")
  const conflictingData = path.join(root, "conflicting-xdg")
  await fs.mkdir(conflictingData, { recursive: true })
  const outputs: Array<Record<string, unknown>> = []
  for (const label of ["first", "second"]) {
    const directory = path.join(root, label)
    const database = path.join(directory, "selected.sqlite")
    const env = {
      ...opencodeEnvironment,
      OPENCODE_DB: database,
      XDG_DATA_HOME: conflictingData,
      TMPDIR: path.join(directory, "tmp"),
      BUN_TMPDIR: path.join(directory, "tmp"),
    }
    await fs.mkdir(env.TMPDIR, { recursive: true })
    const selected = await runBinary(["db", "path"], workspace, env)
    if (selected.stdout.trim() !== database) throw new Error(`${label} db path mismatch: ${selected.stdout}`)
    const query = await runBinary(["db", "SELECT 1"], workspace, env)
    if (!query.stdout.includes("1")) throw new Error(`${label} SELECT 1 returned unexpected output: ${query.stdout}`)
    const stat = await fs.stat(database)
    const conflictingStore = path.join(conflictingData, "opencode", "opencode.db")
    if (await exists(conflictingStore)) throw new Error(`explicit database selector opened conflicting XDG store ${conflictingStore}`)
    outputs.push({ selector: label, dbPath: selected.stdout.trim(), query: query.stdout.trim(), size: stat.size, device: stat.dev, inode: stat.ino })
  }
  const [first, second] = outputs as [{ device: number; inode: number }, { device: number; inode: number }]
  if (first.device === second.device && first.inode === second.inode)
    throw new Error("two absolute OPENCODE_DB selectors resolved to one physical store")
  const unsetEnv = { ...opencodeEnvironment, XDG_DATA_HOME: path.join(root, "unset-xdg") }
  delete (unsetEnv as Record<string, string | undefined>).OPENCODE_DB
  delete (unsetEnv as Record<string, string | undefined>).OPENCODE_DISABLE_CHANNEL_DB
  const defaultPath = path.join(unsetEnv.XDG_DATA_HOME, "opencode", "opencode.db")
  const defaultSelection = await runBinary(["db", "path"], workspace, unsetEnv)
  if (defaultSelection.stdout.trim() !== defaultPath)
    throw new Error(`unset selector did not use the latest-channel default: ${defaultSelection.stdout}`)
  return { explicit: outputs, conflicting_xdg_database_opened: false, unset_latest_channel_path: defaultSelection.stdout.trim() }
}

async function runBinary(args: string[], cwd: string, env: Record<string, string | undefined>) {
  const result = await runProcess([binary, ...args], {
    cwd,
    env,
    timeoutMs: 120_000,
    label: `binary command ${args.join(" ")}`,
  })
  if (result.exit !== 0) throw new Error(`binary command ${args.join(" ")} exited ${result.exit}: ${result.stderr}`)
  return result
}

async function exists(filename: string) {
  return fs.stat(filename).then(() => true, () => false)
}
