import { afterEach, expect, mock, test } from "bun:test"
import { createTestRenderer, type TestRenderer } from "@opentui/core/testing"
import type { SessionMessages } from "@/cli/cmd/run/session.shared"
import type { RunProvider } from "@/cli/cmd/run/types"
import { createTuiResolvedConfig } from "../../fixture/tui-runtime"

type OutputSnapshot = {
  getRealCharBytes(addLineBreaks?: boolean): Uint8Array
  destroy(): void
}

function message(id: string, parentID: string, responseModelID: string): SessionMessages[number] {
  return {
    info: {
      id,
      sessionID: "session-1",
      role: "assistant",
      time: { created: id === "prior" ? 1 : 2, completed: id === "prior" ? 2 : 3 },
      parentID,
      providerID: "llmrouter",
      modelID: "auto",
      responseModelIDs: [responseModelID],
      mode: "chat",
      agent: "build",
      path: { cwd: "/tmp", root: "/tmp" },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    },
    parts: [
      {
        id: `${id}-step`,
        sessionID: "session-1",
        messageID: id,
        type: "step-finish",
        reason: "stop",
        responseModelID,
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      },
    ],
  }
}

function scrollbackText(renderer: TestRenderer) {
  const queue = Reflect.get(renderer, "externalOutputQueue")
  if (!queue || typeof queue !== "object" || !("claim" in queue) || typeof queue.claim !== "function") {
    throw new Error("renderer missing external output queue")
  }
  const commits = queue.claim()
  if (!Array.isArray(commits)) throw new Error("renderer returned invalid output commits")
  const snapshots = commits as Array<{ snapshot: OutputSnapshot }>
  try {
    return snapshots.map((commit) => new TextDecoder().decode(commit.snapshot.getRealCharBytes(true))).join("")
  } finally {
    for (const commit of snapshots) commit.snapshot.destroy()
  }
}

afterEach(() => mock.restore())

test("runtime lifecycle forwards session history into the run footer summary", async () => {
  const setup = await createTestRenderer({
    width: 100,
    screenMode: "split-footer",
    footerHeight: 4,
    externalOutputMode: "capture-stdout",
    consoleMode: "disabled",
  })
  const core = await import("@opentui/core")
  let rendererCreated = false
  await mock.module("@opentui/core", () => ({
    ...core,
    createCliRenderer: async () => {
      rendererCreated = true
      return setup.renderer
    },
  }))
  await mock.module("@/cli/cmd/run/runtime.stdin", () => ({
    resolveInteractiveStdin: () => ({ stdin: process.stdin }),
  }))

  let lifecycle: Awaited<ReturnType<typeof import("@/cli/cmd/run/runtime.lifecycle").createRuntimeLifecycle>> | undefined
  try {
    const { createRuntimeLifecycle } = await import("@/cli/cmd/run/runtime.lifecycle")
    const readSessionIDs: string[] = []
    const history = [message("prior", "user-1", "luna-max"), message("current", "user-2", "glm-5.3-flash")]
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
    lifecycle = await createRuntimeLifecycle({
      directory: "/tmp",
      findFiles: async () => [],
      agents: [],
      resources: [],
      sessionID: "session-1",
      getSessionMessages: async (sessionID) => {
        readSessionIDs.push(sessionID)
        return history
      },
      first: false,
      history: [],
      agent: "build",
      model: { providerID: "llmrouter", modelID: "auto" },
      variant: undefined,
      tuiConfig: createTuiResolvedConfig(),
      backgroundSubagents: false,
      onPermissionReply: () => {},
      onQuestionReply: () => {},
      onQuestionReject: () => {},
    })

    lifecycle.footer.event({ type: "models", providers })
    lifecycle.footer.event({ type: "turn.send", queue: 0 })
    lifecycle.footer.event({
      type: "stream.patch",
      patch: {
        turnModel: {
          providerID: "llmrouter",
          modelID: "auto",
          served: ["glm-5.3-flash"],
          messageID: "current",
        },
      },
    })
    lifecycle.footer.event({ type: "turn.duration", duration: "1s" })
    const flushing = Reflect.get(lifecycle.footer, "flushing")
    if (!(flushing instanceof Promise)) throw new Error("lifecycle footer is missing its pending output chain")
    await flushing

    expect(rendererCreated).toBe(true)
    expect(scrollbackText(setup.renderer)).toContain(
      "▣ Build · Auto (luna-max:1/50%, glm-5.3-flash:1/50%, sol-high:0/0%) · 1s",
    )
    expect(readSessionIDs).toContain("session-1")
  } finally {
    if (lifecycle) await lifecycle.close({ showExit: false })
    else if (!setup.renderer.isDestroyed) setup.renderer.destroy()
    mock.restore()
  }
})
