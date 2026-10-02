/** @jsxImportSource @opentui/solid */
import { describe, expect, test } from "bun:test"
import path from "node:path"
import { Schema } from "effect"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import type { AssistantMessage, Message, Part, Session, SessionStatus } from "@opencode-ai/sdk/v2"
import { LocationProvider } from "../../../src/context/location"
import { useSync } from "../../../src/context/sync"
import { ThemeProvider } from "../../../src/context/theme"
import { TuiConfigProvider, resolve } from "../../../src/config"
import { SessionContext, ReasoningPartView, ToolPartView } from "../../../src/routes/session"
import { SPINNER_FRAMES } from "../../../src/component/spinner"
import { mount, wait } from "../../cli/cmd/tui/sync-fixture"
import { tmpdir } from "../../fixture/fixture"

const ActivityStatus = Schema.Union([
  Schema.Struct({ type: Schema.Literal("busy") }),
  Schema.Struct({ type: Schema.Literal("idle") }),
])
const ActivityArm = Schema.Struct({
  messages: Schema.Array(SessionV1.WithParts),
  status: ActivityStatus,
})
type ActivityArmInfo = Schema.Schema.Type<typeof ActivityArm>
const ActivitySnapshotSchema = Schema.Struct({
  session: SessionV1.SessionInfo,
  oldAssistantID: Schema.String,
  shellAssistantID: Schema.String,
  currentAssistantID: Schema.String,
  completedAssistantID: Schema.String,
  active: ActivityArm,
  completed: ActivityArm,
})
type ActivitySnapshot = Schema.Schema.Type<typeof ActivitySnapshotSchema>

function assistant(rows: ActivitySnapshot["active"]["messages"], id: string) {
  const row = rows.find((item) => item.info.id === id)
  if (!row || row.info.role !== "assistant") throw new Error(`Missing assistant ${id}`)
  // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- the producer payload was schema-decoded and is passed to the TUI's current SDK consumer
  return { info: row.info as unknown as AssistantMessage, parts: row.parts as unknown as Part[] }
}

function reasoning(parts: Part[], openOnly = true) {
  const part = parts.findLast(
    (item): item is Extract<Part, { type: "reasoning" }> =>
      item.type === "reasoning" && (!openOnly || item.time.end === undefined) && item.metadata !== undefined,
  )
  if (!part) throw new Error("Missing production reasoning part")
  return part
}

function tool(parts: Part[], name: string) {
  const part = parts.find((item): item is Extract<Part, { type: "tool" }> => item.type === "tool" && item.tool === name)
  if (!part) throw new Error(`Missing production ${name} tool part`)
  return part
}

function History(props: {
  sessionID: string
  old: ReturnType<typeof assistant>
  shell: ReturnType<typeof assistant>
  current: ReturnType<typeof assistant>
  oldReasoning: Extract<Part, { type: "reasoning" }>
  oldRead: Extract<Part, { type: "tool" }>
  oldShell: Extract<Part, { type: "tool" }>
  currentReasoning: Extract<Part, { type: "reasoning" }>
  currentRead?: Extract<Part, { type: "tool" }>
}) {
  const sync = useSync()
  const config = resolve({}, { terminalSuspend: false })
  const sessionContext = {
    width: 100,
    sessionID: props.sessionID,
    conceal: () => false,
    thinkingMode: () => "show" as const,
    showThinking: () => true,
    showTimestamps: () => false,
    showDetails: () => true,
    showGenericToolOutput: () => false,
    diffWrapMode: () => "word" as const,
    providers: () => new Map(),
    sync,
    tui: config,
  }

  return (
    <TuiConfigProvider config={config}>
      <ThemeProvider mode="dark">
        <LocationProvider>
          <SessionContext.Provider value={sessionContext}>
            <box flexDirection="column">
              <ReasoningPartView last={true} message={props.old.info} part={props.oldReasoning} />
              <ToolPartView last={true} message={props.old.info} part={props.oldRead} />
              <ToolPartView last={true} message={props.shell.info} part={props.oldShell} />
              <ReasoningPartView last={true} message={props.current.info} part={props.currentReasoning} />
              {props.currentRead && <ToolPartView last={true} message={props.current.info} part={props.currentRead} />}
            </box>
          </SessionContext.Provider>
        </LocationProvider>
      </ThemeProvider>
    </TuiConfigProvider>
  )
}

function spinnerCount(frame: string) {
  return Array.from(frame).filter((char) => SPINNER_FRAMES.includes(char)).length
}

function populate(
  sync: ReturnType<typeof useSync>,
  snapshot: ActivitySnapshot,
  arm: ActivityArmInfo,
  includeStatus = true,
) {
  // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- the producer payload is schema-decoded and shares the SDK session shape
  sync.set("session", [snapshot.session as unknown as Session])
  sync.set(
    "message",
    snapshot.session.id,
    arm.messages.map((row) => {
      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- V1 producer info is consumed through the current SDK sync store
      return row.info as unknown as Message
    }),
  )
  arm.messages.forEach((row) => {
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- V1 producer parts are consumed through the current SDK sync store
    sync.set("part", row.info.id, row.parts as unknown as Part[])
  })
  if (includeStatus) sync.set("session_status", snapshot.session.id, arm.status as SessionStatus)
}

async function render(snapshot: ActivitySnapshot, arm: ActivityArmInfo, state: string, includeStatus = true) {
  const old = assistant(arm.messages, snapshot.oldAssistantID)
  const shell = assistant(arm.messages, snapshot.shellAssistantID)
  const current = assistant(
    arm.messages,
    arm.status.type === "busy" ? snapshot.currentAssistantID : snapshot.completedAssistantID,
  )
  const app = await mount(undefined, state, true, () => (
    <History
      sessionID={snapshot.session.id}
      old={old}
      shell={shell}
      current={current}
      oldReasoning={reasoning(old.parts)}
      oldRead={tool(old.parts, "read")}
      oldShell={tool(shell.parts, "bash")}
      currentReasoning={reasoning(current.parts, arm.status.type === "busy")}
      currentRead={current.parts.find(
        (part): part is Extract<Part, { type: "tool" }> => part.type === "tool" && part.tool === "read",
      )}
    />
  ))

  populate(app.sync, snapshot, arm, includeStatus)
  await wait(() => app.app.captureCharFrame().includes("Thinking"))
  await app.app.renderOnce()
  return { app: app.app, frame: app.app.captureCharFrame() }
}

describe("assistant activity rendering", () => {
  test("superseded production reasoning and tools stay neutral while the latest assistant is working and after it completes", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const output = path.join(tmp.path, "assistant-activity.json")
    const packageRoot = path.resolve(import.meta.dir, "../../../../opencode")
    const env = Object.fromEntries(
      Object.entries(process.env).flatMap(([key, value]) => (value === undefined ? [] : [[key, value]])),
    )
    env.OPENCODE_DANGLING_ASSISTANT_SNAPSHOT = output
    const child = Bun.spawn(
      [
        process.execPath,
        "test",
        "test/session/prompt.test.ts",
        "--test-name-pattern=dangling-assistant-production-integration",
      ],
      { cwd: packageRoot, env, stdout: "pipe", stderr: "pipe" },
    )
    try {
      const [code, stdout, stderr] = await Promise.all([
        child.exited,
        Bun.readableStreamToText(child.stdout),
        Bun.readableStreamToText(child.stderr),
      ])
      expect(code, `${stdout}\n${stderr}`).toBe(0)

      const snapshot = Schema.decodeUnknownSync(ActivitySnapshotSchema)(
        Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(await Bun.file(output).text()),
      )
      const active = await render(snapshot, snapshot.active, tmp.path)
      try {
        expect(active.frame).toContain("Thinking status unknown")
        expect(active.frame).toContain("Thinking")
        expect(active.frame).toContain("Read /tmp/unfinished.ts")
        expect(active.frame).toContain("started")
        expect(spinnerCount(active.frame)).toBe(2)
      } finally {
        active.app.renderer.destroy()
      }

      const nonOwner = await render(snapshot, snapshot.active, tmp.path, false)
      try {
        expect(nonOwner.frame).toContain("Thinking status unknown")
        expect(nonOwner.frame).not.toContain("Tool execution aborted")
        expect(spinnerCount(nonOwner.frame)).toBe(0)
      } finally {
        nonOwner.app.renderer.destroy()
      }

      const completed = await render(snapshot, snapshot.completed, tmp.path)
      try {
        expect(completed.frame).toContain("Thinking status unknown")
        expect(completed.frame).toContain("Thought")
        expect(completed.frame).toContain("started")
        expect(spinnerCount(completed.frame)).toBe(0)
      } finally {
        completed.app.renderer.destroy()
      }
    } finally {
      if (child.exitCode === null) child.kill()
    }
  }, 120_000)
})
