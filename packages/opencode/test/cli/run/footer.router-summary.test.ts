import { expect, test } from "bun:test"
import { createDefaultOpenTuiKeymap } from "@opentui/keymap/opentui"
import { MockTreeSitterClient, createTestRenderer, type TestRenderer } from "@opentui/core/testing"
import type { Event } from "@opencode-ai/sdk/v2"
import { RunFooter } from "@/cli/cmd/run/footer"
import { createSessionData, reduceSessionData } from "@/cli/cmd/run/session-data"
import type { SessionMessages } from "@/cli/cmd/run/session.shared"
import { RUN_THEME_FALLBACK } from "@/cli/cmd/run/theme"
import type { RunProvider } from "@/cli/cmd/run/types"
import { registerOpencodeKeymap } from "@opencode-ai/tui/keymap"
import { createTuiResolvedConfig } from "../../fixture/tui-runtime"

type OutputSnapshot = {
  getRealCharBytes(addLineBreaks?: boolean): Uint8Array
  destroy(): void
}

function message(id: string, parentID: string, responseModelIDs: string[]): SessionMessages[number] {
  return {
    info: {
      id,
      sessionID: "session-1",
      role: "assistant",
      time: { created: 1, completed: 1001 },
      parentID,
      providerID: "llmrouter",
      modelID: "auto",
      responseModelIDs: [...new Set(responseModelIDs)],
      mode: "chat",
      agent: "build",
      path: { cwd: "/tmp", root: "/tmp" },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    },
    parts: responseModelIDs.map((responseModelID, index) => ({
      id: `${id}-step-${index}`,
      sessionID: "session-1",
      messageID: id,
      type: "step-finish",
      reason: "stop",
      responseModelID,
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    })),
  }
}

function output(renderer: TestRenderer) {
  const queue = Reflect.get(renderer, "externalOutputQueue")
  if (!queue || typeof queue !== "object" || !("claim" in queue) || typeof queue.claim !== "function") {
    throw new Error("renderer missing external output queue")
  }
  const commits = queue.claim()
  if (!Array.isArray(commits)) throw new Error("renderer returned invalid output commits")
  return commits as Array<{ snapshot: OutputSnapshot }>
}

async function renderSummary(input: {
  sessionID?: string
  messages?: SessionMessages
  failHistory?: boolean
}) {
  const renderer = await createTestRenderer({
    width: 100,
    screenMode: "split-footer",
    footerHeight: 6,
    externalOutputMode: "capture-stdout",
    consoleMode: "disabled",
  })
  const treeSitterClient = new MockTreeSitterClient({ autoResolveTimeout: 0 })
  treeSitterClient.setMockResult({ highlights: [] })
  const current = message("m2", "user-2", ["glm-5.3-flash"])
  let reads = 0
  const providers = [
    {
      id: "llmrouter",
      name: "LLMRouter",
      source: "api",
      env: [],
      options: {},
      models: {
        auto: { name: "Auto" },
        "luna-max": { name: "luna-max" },
        "glm-5.3-flash": { name: "glm-5.3-flash" },
        "sol-high": { name: "sol-high" },
      },
    },
  ] as unknown as RunProvider[]
  const config = createTuiResolvedConfig()
  const keymap = createDefaultOpenTuiKeymap(renderer.renderer)
  const unregister = registerOpencodeKeymap(keymap, renderer.renderer, config)
  const footer = new RunFooter(renderer.renderer, {
    directory: "/tmp",
    findFiles: async () => [],
    agents: [],
    resources: [],
    sessionID: () => input.sessionID,
    getSessionMessages: async (sessionID) => {
      reads++
      if (input.failHistory) throw new Error("history unavailable")
      if (sessionID !== "session-1") return undefined
      return input.messages
    },
    agentLabel: "Build",
    modelLabel: "Auto",
    model: { providerID: "llmrouter", modelID: "auto" },
    variant: undefined,
    first: false,
    theme: RUN_THEME_FALLBACK,
    keymap,
    tuiConfig: config,
    backgroundSubagents: false,
    diffStyle: "auto",
    onPermissionReply: () => {},
    onQuestionReply: () => {},
    onQuestionReject: () => {},
    onEditorOpen: async () => undefined,
    treeSitterClient,
  })

  try {
    footer.event({ type: "models", providers })
    footer.event({ type: "turn.send", queue: 0 })
    const sessionData = reduceSessionData({
      data: createSessionData(),
      event: {
        type: "message.updated",
        properties: { sessionID: "session-1", info: current.info },
      } as Event,
      sessionID: "session-1",
      thinking: true,
      limits: {},
    })
    if (!sessionData.footer?.patch?.turnModel) throw new Error("session data did not report the assistant model")
    footer.event({
      type: "stream.patch",
      patch: sessionData.footer.patch,
    })
    footer.event({ type: "turn.duration", duration: "1s" })
    const flushing = Reflect.get(footer, "flushing")
    if (!(flushing instanceof Promise)) throw new Error("footer is missing its pending output chain")
    await flushing
    const flushError = Reflect.get(footer, "flushError")
    if (flushError) throw flushError

    const commits = output(renderer.renderer)
    try {
      return {
        reads,
        text: commits
        .map((commit) => new TextDecoder().decode(commit.snapshot.getRealCharBytes(true)))
          .join(""),
      }
    } finally {
      for (const commit of commits) commit.snapshot.destroy()
    }
  } finally {
    footer.destroy()
    unregister()
    renderer.renderer.destroy()
  }
}

test("live run footer summary renders lifetime router usage through scrollback", async () => {
  const result = await renderSummary({
    sessionID: "session-1",
    messages: [message("m1", "user-1", ["luna-max", "luna-max"]), message("m2", "user-2", ["glm-5.3-flash"]), message("m3", "user-3", ["sol-high"])],
  })
  expect(result.reads).toBe(1)
  expect(result.text).toContain("▣ Build · Auto (luna-max:2/67%, glm-5.3-flash:1/33%, sol-high:0/0%) · 1s")
})

test("live footer keeps its served-ID summary when session history is unavailable", async () => {
  const unavailable = "▣ Build · Auto (glm-5.3-flash) · 1s"
  const cases = [
    { sessionID: undefined, messages: undefined, reads: 0 },
    { sessionID: "session-1", messages: [message("other-message", "user-1", ["luna-max"])], reads: 1 },
    { sessionID: "session-1", messages: undefined, failHistory: true, reads: 1 },
  ]
  for (const input of cases) {
    const result = await renderSummary(input)
    expect(result.reads).toBe(input.reads)
    expect(result.text).toContain(unavailable)
  }
})
