import { expect, test } from "bun:test"
import { messageTurnSummaryCommit } from "@/cli/cmd/run/turn-summary"
import { replaySession } from "@/cli/cmd/run/session-replay"
import type { SessionMessages } from "@/cli/cmd/run/session.shared"
import type { RunProvider } from "@/cli/cmd/run/types"

const providers = [
  {
    id: "llmrouter",
    name: "LLMRouter",
    models: {
      auto: { name: "Auto" },
      "luna-max": { name: "luna-max" },
      "glm-5.3-flash": { name: "glm-5.3-flash" },
      "sol-high": { name: "sol-high" },
    },
  },
] as unknown as RunProvider[]

function message(
  id: string,
  sessionID: string,
  parentID: string,
  responseModelIDs: string[],
  extra: Record<string, unknown> = {},
): SessionMessages[number] {
  return {
    info: {
      id,
      sessionID,
      role: "assistant",
      time: { created: 1, completed: 1001 },
      parentID,
      providerID: "llmrouter",
      modelID: "auto",
      mode: "chat",
      agent: "build",
      path: { cwd: "/tmp", root: "/tmp" },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      ...extra,
    } as SessionMessages[number]["info"],
    parts: responseModelIDs.map((responseModelID, index) => ({
      id: `${id}-step-${index}`,
      sessionID,
      messageID: id,
      type: "step-finish",
      reason: "stop",
      responseModelID,
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    })),
  }
}

function user(id: string): SessionMessages[number] {
  return {
    info: {
      id,
      sessionID: "session-1",
      role: "user",
      time: { created: 1 },
      agent: "build",
      model: { providerID: "llmrouter", modelID: "auto" },
    },
    parts: [
      {
        id: `${id}-text`,
        sessionID: "session-1",
        messageID: id,
        type: "text",
        text: "hello",
      },
    ],
  }
}

test("replay summary counts prior steps through each assistant message only", () => {
  const earlier = message("m1", "session-1", "user-1", ["luna-max", "luna-max"])
  const compaction = message("m-summary", "session-1", "user-1", ["glm-5.3-flash"], { summary: true })
  const current = message("m2", "session-1", "user-2", ["glm-5.3-flash"])
  const later = message("m3", "session-1", "user-3", ["sol-high"])
  const otherSession = message("m4", "session-2", "user-4", ["sol-high"])
  const all = [earlier, compaction, current, later, otherSession]

  expect(messageTurnSummaryCommit(earlier, providers, all)?.text).toContain(
    "▣ Build · Auto (luna-max:2/100%, glm-5.3-flash:0/0%, sol-high:0/0%)",
  )
  expect(messageTurnSummaryCommit(current, providers, all)?.text).toContain(
    "▣ Build · Auto (luna-max:2/67%, glm-5.3-flash:1/33%, sol-high:0/0%)",
  )
  expect(messageTurnSummaryCommit(otherSession, providers, all)?.text).toContain(
    "▣ Build · Auto (luna-max:0/0%, glm-5.3-flash:0/0%, sol-high:1/100%)",
  )

  const replay = replaySession({
    messages: [
      user("user-1"),
      earlier,
      compaction,
      user("user-2"),
      current,
      user("user-3"),
      later,
    ],
    permissions: [],
    questions: [],
    thinking: true,
    limits: {},
    providers,
  })
  expect(replay.commits.filter((commit) => commit.summary).map((commit) => commit.summary?.model)).toEqual([
    "Auto (luna-max:2/100%, glm-5.3-flash:0/0%, sol-high:0/0%)",
    "Auto (luna-max:2/67%, glm-5.3-flash:1/33%, sol-high:0/0%)",
    "Auto (luna-max:2/50%, glm-5.3-flash:1/25%, sol-high:1/25%)",
  ])
})
