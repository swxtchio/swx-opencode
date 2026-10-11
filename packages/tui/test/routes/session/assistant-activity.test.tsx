/** @jsxImportSource @opentui/solid */
import { createDefaultOpenTuiKeymap } from "@opentui/keymap/opentui"
import { describe, expect, test } from "bun:test"
import path from "node:path"
import { Schema } from "effect"
import { onCleanup } from "solid-js"
import type { JSX } from "@opentui/solid"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import type { AssistantMessage, Message, Part, Session, SessionStatus } from "@opencode-ai/sdk/v2"
import { LocalProvider } from "../../../src/context/local"
import { LocationProvider } from "../../../src/context/location"
import { RouteProvider } from "../../../src/context/route"
import { useSync } from "../../../src/context/sync"
import { ThemeProvider } from "../../../src/context/theme"
import { TuiConfigProvider, resolve } from "../../../src/config"
import { OpencodeKeymapProvider, registerOpencodeKeymap } from "../../../src/keymap"
import {
  activeForegroundTasks,
  activeTaskRetry,
  assistantStatus,
  SessionContext,
  AssistantMessageView,
  ToolPartView,
} from "../../../src/routes/session"
import { SPINNER_FRAMES } from "../../../src/component/spinner"
import { useRenderer } from "@opentui/solid"
import { DialogProvider } from "../../../src/ui/dialog"
import { ToastProvider } from "../../../src/ui/toast"
import { mount, wait } from "../../cli/cmd/tui/sync-fixture"
import { tmpdir } from "../../fixture/fixture"

const ActivityStatus = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("busy"),
    activeAssistantMessageID: Schema.optional(Schema.NullOr(SessionV1.MessageID)),
  }),
  Schema.Struct({ type: Schema.Literal("idle") }),
  Schema.Struct({
    type: Schema.Literal("retry"),
    attempt: Schema.Number,
    message: Schema.String,
    next: Schema.Number,
    activeAssistantMessageID: Schema.optional(Schema.NullOr(SessionV1.MessageID)),
    action: Schema.optional(Schema.Unknown),
  }),
])
const ActivityArm = Schema.Struct({
  messages: Schema.Array(SessionV1.WithParts),
  status: ActivityStatus,
})
type ActivityArmInfo = Schema.Schema.Type<typeof ActivityArm>
const PreAssistantActivity = Schema.Struct({
  session: SessionV1.SessionInfo,
  messages: Schema.Array(SessionV1.WithParts),
  oldAssistantID: SessionV1.MessageID,
  shellAssistantID: SessionV1.MessageID,
  taskAssistantID: SessionV1.MessageID,
  status: ActivityStatus,
})
const ShellActivity = Schema.Struct({
  session: SessionV1.SessionInfo,
  messages: Schema.Array(SessionV1.WithParts),
  assistantID: SessionV1.MessageID,
  status: ActivityStatus,
  statusEvent: ActivityStatus,
})
const DirectSubtaskActivity = Schema.Struct({
  session: SessionV1.SessionInfo,
  messages: Schema.Array(SessionV1.WithParts),
  assistantID: SessionV1.MessageID,
  childSessionID: Schema.String,
  childMessages: Schema.Array(SessionV1.WithParts),
  childStatus: ActivityStatus,
  parentStatus: ActivityStatus,
})
const ActivitySnapshotSchema = Schema.Struct({
  session: SessionV1.SessionInfo,
  oldAssistantID: SessionV1.MessageID,
  shellAssistantID: SessionV1.MessageID,
  taskAssistantID: SessionV1.MessageID,
  taskProducerSessionID: Schema.String,
  taskProducerSession: SessionV1.SessionInfo,
  taskProducerMessages: Schema.Array(SessionV1.WithParts),
  taskChildSessionID: Schema.String,
  taskChildMessages: Schema.Array(SessionV1.WithParts),
  taskChildStatus: ActivityStatus,
  taskParentStatus: ActivityStatus,
  directSubtask: DirectSubtaskActivity,
  shellProducer: ShellActivity,
  preAssistant: PreAssistantActivity,
  currentAssistantID: SessionV1.MessageID,
  completedAssistantID: SessionV1.MessageID,
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
        <TaskInteractionProviders sessionID={props.sessionID}>
          <LocalProvider>
            <LocationProvider>
              <SessionContext.Provider value={sessionContext}>
                <box flexDirection="column">
                  <AssistantMessageView
                    last={true}
                    message={props.old.info}
                    parts={[props.oldReasoning]}
                    modelMessages={undefined}
                  />
                  <ToolPartView last={true} message={props.old.info} part={props.oldRead} />
                  <ToolPartView last={true} message={props.shell.info} part={props.oldShell} />
                  <AssistantMessageView
                    last={true}
                    message={props.current.info}
                    parts={[props.currentReasoning]}
                    modelMessages={undefined}
                  />
                  {props.currentRead && (
                    <ToolPartView last={true} message={props.current.info} part={props.currentRead} />
                  )}
                </box>
              </SessionContext.Provider>
            </LocationProvider>
          </LocalProvider>
        </TaskInteractionProviders>
      </ThemeProvider>
    </TuiConfigProvider>
  )
}

function PreAssistantHistory(props: {
  sessionID: string
  old: ReturnType<typeof assistant>
  shell: ReturnType<typeof assistant>
  task: ReturnType<typeof assistant>
  oldReasoning: Extract<Part, { type: "reasoning" }>
  oldRead: Extract<Part, { type: "tool" }>
  shellPart: Extract<Part, { type: "tool" }>
  taskPart: Extract<Part, { type: "tool" }>
  prepare: (sync: ReturnType<typeof useSync>) => void
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
            <TaskInteractionProviders sessionID={props.sessionID}>
              <TaskSeed prepare={props.prepare}>
                <LocalProvider>
                  <box flexDirection="column">
                    <AssistantMessageView
                      last={true}
                      message={props.old.info}
                      parts={[props.oldReasoning]}
                      modelMessages={undefined}
                    />
                    <ToolPartView last={true} message={props.old.info} part={props.oldRead} />
                    <ToolPartView last={true} message={props.shell.info} part={props.shellPart} />
                    <ToolPartView last={true} message={props.task.info} part={props.taskPart} />
                  </box>
                </LocalProvider>
              </TaskSeed>
            </TaskInteractionProviders>
          </SessionContext.Provider>
        </LocationProvider>
      </ThemeProvider>
    </TuiConfigProvider>
  )
}

function TaskInteractionProviders(props: { sessionID: string; children: JSX.Element }) {
  const renderer = useRenderer()
  const keymap = createDefaultOpenTuiKeymap(renderer)
  const config = resolve({}, { terminalSuspend: false })
  const off = registerOpencodeKeymap(keymap, renderer, config)
  onCleanup(off)

  return (
    <RouteProvider initialRoute={{ type: "session", sessionID: props.sessionID }}>
      <OpencodeKeymapProvider keymap={keymap}>
        <TuiConfigProvider config={config}>
          <ToastProvider>
            <DialogProvider>{props.children}</DialogProvider>
          </ToastProvider>
        </TuiConfigProvider>
      </OpencodeKeymapProvider>
    </RouteProvider>
  )
}

function TaskSeed(props: { prepare: (sync: ReturnType<typeof useSync>) => void; children: JSX.Element }) {
  props.prepare(useSync())
  return props.children
}

function TaskActivity(props: {
  sessionID: string
  message: ReturnType<typeof assistant>
  part: Extract<Part, { type: "tool" }>
  prepare: (sync: ReturnType<typeof useSync>) => void
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
              <TaskInteractionProviders sessionID={props.sessionID}>
                <TaskSeed prepare={props.prepare}>
                  <ToolPartView last={true} message={props.message.info} part={props.part} />
                </TaskSeed>
              </TaskInteractionProviders>
            </box>
          </SessionContext.Provider>
        </LocationProvider>
      </ThemeProvider>
    </TuiConfigProvider>
  )
}

function hasSpinner(line: string) {
  return Array.from(line).some((char) => SPINNER_FRAMES.includes(char))
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
  populateTaskChild(sync, snapshot)
  sync.set("capabilities", { ...sync.data.capabilities, experimentalBackgroundSubagents: true })
}

function populateTaskChild(sync: ReturnType<typeof useSync>, snapshot: ActivitySnapshot) {
  sync.set(
    "message",
    snapshot.taskChildSessionID,
    snapshot.taskChildMessages.map((row) => {
      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer child rows are decoded and passed through the current SDK sync store
      return row.info as unknown as Message
    }),
  )
  snapshot.taskChildMessages.forEach((row) => {
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer child parts are decoded and passed through the current SDK sync store
    sync.set("part", row.info.id, row.parts as unknown as Part[])
  })
  sync.set("session_status", snapshot.taskChildSessionID, snapshot.taskChildStatus as SessionStatus)
}

function populatePreAssistant(sync: ReturnType<typeof useSync>, snapshot: ActivitySnapshot) {
  // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer session is decoded and passed through the current SDK sync store
  sync.set("session", [snapshot.preAssistant.session as unknown as Session])
  sync.set(
    "message",
    snapshot.preAssistant.session.id,
    snapshot.preAssistant.messages.map((row) => {
      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer rows are decoded and passed through the current SDK sync store
      return row.info as unknown as Message
    }),
  )
  snapshot.preAssistant.messages.forEach((row) => {
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer parts are decoded and passed through the current SDK sync store
    sync.set("part", row.info.id, row.parts as unknown as Part[])
  })
  sync.set("session_status", snapshot.preAssistant.session.id, snapshot.preAssistant.status as SessionStatus)
  populateTaskChild(sync, snapshot)
  sync.set("capabilities", { ...sync.data.capabilities, experimentalBackgroundSubagents: true })
}

function populateShellOwner(sync: ReturnType<typeof useSync>, snapshot: ActivitySnapshot) {
  // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer session is decoded and passed through the current SDK sync store
  sync.set("session", [snapshot.shellProducer.session as unknown as Session])
  sync.set(
    "message",
    snapshot.shellProducer.session.id,
    snapshot.shellProducer.messages.map((row) => {
      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer rows are decoded and passed through the current SDK sync store
      return row.info as unknown as Message
    }),
  )
  snapshot.shellProducer.messages.forEach((row) => {
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer parts are decoded and passed through the current SDK sync store
    sync.set("part", row.info.id, row.parts as unknown as Part[])
  })
  sync.set("session_status", snapshot.shellProducer.session.id, snapshot.shellProducer.statusEvent as SessionStatus)
}

function populateDirectSubtask(sync: ReturnType<typeof useSync>, snapshot: ActivitySnapshot) {
  const direct = snapshot.directSubtask
  // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer session is decoded and passed through the current SDK sync store
  sync.set("session", [direct.session as unknown as Session])
  sync.set(
    "message",
    direct.session.id,
    direct.messages.map((row) => {
      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer rows are decoded and passed through the current SDK sync store
      return row.info as unknown as Message
    }),
  )
  direct.messages.forEach((row) => {
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer parts are decoded and passed through the current SDK sync store
    sync.set("part", row.info.id, row.parts as unknown as Part[])
  })
  sync.set("session_status", direct.session.id, direct.parentStatus as SessionStatus)
  sync.set(
    "message",
    direct.childSessionID,
    direct.childMessages.map((row) => {
      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer child rows are decoded and passed through the current SDK sync store
      return row.info as unknown as Message
    }),
  )
  direct.childMessages.forEach((row) => {
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer child parts are decoded and passed through the current SDK sync store
    sync.set("part", row.info.id, row.parts as unknown as Part[])
  })
  sync.set("session_status", direct.childSessionID, direct.childStatus as SessionStatus)
  sync.set("capabilities", { ...sync.data.capabilities, experimentalBackgroundSubagents: true })
}

async function renderShellOwner(snapshot: ActivitySnapshot, state: string) {
  const shellAssistant = assistant(snapshot.shellProducer.messages, snapshot.shellProducer.assistantID)
  const part = tool(shellAssistant.parts, "bash")
  const app = await mount(undefined, state, true, () => (
    <TaskActivity
      sessionID={snapshot.shellProducer.session.id}
      message={shellAssistant}
      part={part}
      prepare={(sync) => populateShellOwner(sync, snapshot)}
    />
  ))
  await wait(() => app.app.captureCharFrame().includes("started"))
  await app.app.renderOnce()
  return { app: app.app, frame: app.app.captureCharFrame() }
}

async function renderDirectSubtask(snapshot: ActivitySnapshot, state: string) {
  const direct = snapshot.directSubtask
  const message = assistant(direct.messages, direct.assistantID)
  const part = tool(message.parts, "task")
  const app = await mount(undefined, state, true, () => (
    <TaskActivity
      sessionID={direct.session.id}
      message={message}
      part={part}
      prepare={(sync) => populateDirectSubtask(sync, snapshot)}
    />
  ))
  await wait(() => app.app.captureCharFrame().includes("Inspect direct subtask ownership"))
  await app.app.renderOnce()
  return { app: app.app, frame: app.app.captureCharFrame(), sync: app.sync, message }
}

function populateHistoricalSubtask(sync: ReturnType<typeof useSync>, snapshot: ActivitySnapshot, arm: ActivityArmInfo) {
  const latestAssistant = assistant(
    arm.messages,
    arm.status.type === "busy" ? snapshot.currentAssistantID : snapshot.completedAssistantID,
  )
  populateDirectSubtask(sync, snapshot)
  sync.set("message", snapshot.directSubtask.session.id, [
    ...snapshot.directSubtask.messages.map((row) => {
      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer rows are decoded and passed through the current SDK sync store
      return row.info as unknown as Message
    }),
    latestAssistant.info,
  ])
  sync.set("session_status", snapshot.directSubtask.session.id, arm.status as SessionStatus)
}

async function renderHistoricalSubtask(snapshot: ActivitySnapshot, arm: ActivityArmInfo, state: string) {
  const direct = snapshot.directSubtask
  const message = assistant(direct.messages, direct.assistantID)
  const part = tool(message.parts, "task")
  const app = await mount(undefined, state, true, () => (
    <TaskActivity
      sessionID={direct.session.id}
      message={message}
      part={part}
      prepare={(sync) => populateHistoricalSubtask(sync, snapshot, arm)}
    />
  ))
  await wait(() => app.app.captureCharFrame().includes("Inspect direct subtask ownership"))
  await app.app.renderOnce()
  return { app: app.app, frame: app.app.captureCharFrame(), sync: app.sync, message }
}

async function renderPreAssistant(snapshot: ActivitySnapshot, state: string) {
  const old = assistant(snapshot.preAssistant.messages, snapshot.preAssistant.oldAssistantID)
  const shell = assistant(snapshot.preAssistant.messages, snapshot.preAssistant.shellAssistantID)
  const task = assistant(snapshot.preAssistant.messages, snapshot.preAssistant.taskAssistantID)
  const app = await mount(undefined, state, true, () => (
    <PreAssistantHistory
      sessionID={snapshot.preAssistant.session.id}
      old={old}
      shell={shell}
      task={task}
      oldReasoning={reasoning(old.parts)}
      oldRead={tool(old.parts, "read")}
      shellPart={tool(shell.parts, "bash")}
      taskPart={tool(task.parts, "task")}
      prepare={(sync) => populatePreAssistant(sync, snapshot)}
    />
  ))
  await wait(() => app.app.captureCharFrame().includes("Inspect task ownership"))
  await app.app.renderOnce()
  return { app: app.app, frame: app.app.captureCharFrame(), sync: app.sync }
}

function populateTask(sync: ReturnType<typeof useSync>, snapshot: ActivitySnapshot) {
  const rows = snapshot.taskProducerMessages
  // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer session is decoded and passed through the current SDK sync store
  sync.set("session", [snapshot.taskProducerSession as unknown as Session])
  sync.set(
    "message",
    snapshot.taskProducerSessionID,
    rows.map((row) => {
      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer rows are decoded and passed through the current SDK sync store
      return row.info as unknown as Message
    }),
  )
  rows.forEach((row) => {
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer parts are decoded and passed through the current SDK sync store
    sync.set("part", row.info.id, row.parts as unknown as Part[])
  })
  sync.set("session_status", snapshot.taskProducerSessionID, snapshot.taskParentStatus as SessionStatus)
  populateTaskChild(sync, snapshot)
  sync.set("capabilities", { ...sync.data.capabilities, experimentalBackgroundSubagents: true })
}

function populateHistoricalTask(
  sync: ReturnType<typeof useSync>,
  snapshot: ActivitySnapshot,
  arm: ActivityArmInfo,
  includeStatus: boolean,
) {
  const latestAssistant = assistant(
    arm.messages,
    arm.status.type === "busy" ? snapshot.currentAssistantID : snapshot.completedAssistantID,
  )
  populateTaskChild(sync, snapshot)
  // Both assistant rows and the child retry status are producer output; this sync projection joins the Task row with the later produced row to test row ownership, not database recovery.
  sync.set("session", [snapshot.taskProducerSession as unknown as Session])
  sync.set("message", snapshot.taskProducerSessionID, [
    ...snapshot.taskProducerMessages.map((row) => {
      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer row is decoded and passed through the current SDK sync store
      return row.info as unknown as Message
    }),
    latestAssistant.info,
  ])
  snapshot.taskProducerMessages.forEach((row) => {
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- producer parts are decoded and passed through the current SDK sync store
    sync.set("part", row.info.id, row.parts as unknown as Part[])
  })
  if (includeStatus) sync.set("session_status", snapshot.taskProducerSessionID, arm.status as SessionStatus)
  sync.set("capabilities", { ...sync.data.capabilities, experimentalBackgroundSubagents: true })
}

async function renderHistoricalTask(
  snapshot: ActivitySnapshot,
  arm: ActivityArmInfo,
  state: string,
  includeStatus = true,
) {
  const message = assistant(snapshot.taskProducerMessages, snapshot.taskAssistantID)
  const part = tool(message.parts, "task")
  const app = await mount(undefined, state, true, () => (
    <TaskActivity
      sessionID={snapshot.taskProducerSessionID}
      message={message}
      part={part}
      prepare={(sync) => populateHistoricalTask(sync, snapshot, arm, includeStatus)}
    />
  ))
  await wait(() => app.app.captureCharFrame().includes("Inspect task ownership"))
  await app.app.renderOnce()
  return { app: app.app, frame: app.app.captureCharFrame(), sync: app.sync, message }
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

async function renderTask(snapshot: ActivitySnapshot, state: string) {
  const message = assistant(snapshot.taskProducerMessages, snapshot.taskAssistantID)
  const part = tool(message.parts, "task")
  const app = await mount(undefined, state, true, () => (
    <TaskActivity
      sessionID={snapshot.taskProducerSessionID}
      message={message}
      part={part}
      prepare={(sync) => populateTask(sync, snapshot)}
    />
  ))
  await wait(() => app.app.captureCharFrame().includes("Inspect task ownership"))
  await app.app.renderOnce()
  return { app: app.app, frame: app.app.captureCharFrame(), sync: app.sync, message }
}

describe("assistant activity rendering", () => {
  test("keeps superseded production activity neutral after a later assistant row is persisted", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const output = path.join(tmp.path, "assistant-activity.json")
    const packageRoot = path.resolve(import.meta.dir, "../../../../opencode")
    const env = Object.fromEntries(
      Object.entries(process.env).flatMap(([key, value]) => (value === undefined ? [] : [[key, value]])),
    )
    env.OPENCODE_DANGLING_ASSISTANT_SNAPSHOT = output
    // Reasoning and tool rows cross JSON into a fresh test database before later production turns; the Task rendering joins its producer row and child retry status with that later assistant in the sync projection. Neither fixture reopens the original worker database.
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
      if (snapshot.active.status.type !== "busy") throw new Error("expected the active production owner status")
      expect(snapshot.active.status.activeAssistantMessageID).toBe(snapshot.currentAssistantID)
      if (snapshot.taskParentStatus.type !== "busy") throw new Error("expected the Task producer owner status")
      expect(snapshot.taskParentStatus.activeAssistantMessageID).toBe(snapshot.taskAssistantID)
      if (snapshot.shellProducer.status.type !== "busy") throw new Error("expected the running shell owner status")
      expect(snapshot.shellProducer.status.activeAssistantMessageID).toBe(snapshot.shellProducer.assistantID)
      if (snapshot.shellProducer.statusEvent.type !== "busy") throw new Error("expected the running shell owner event")
      expect(snapshot.shellProducer.statusEvent.activeAssistantMessageID).toBe(snapshot.shellProducer.assistantID)
      expect(snapshot.taskProducerMessages.at(-1)?.info.role).toBe("user")
      if (snapshot.preAssistant.status.type !== "busy") throw new Error("expected the pre-assistant busy status")
      expect(snapshot.preAssistant.status.activeAssistantMessageID).toBeNull()
      expect(snapshot.preAssistant.messages.at(-1)?.info.role).toBe("user")
      expect(snapshot.preAssistant.messages.findLast((message) => message.info.role === "assistant")?.info.id).toBe(
        snapshot.preAssistant.taskAssistantID,
      )

      const shellOwner = await renderShellOwner(snapshot, tmp.path)
      try {
        const shellLine = shellOwner.frame.split("\n").find(hasSpinner)
        expect(shellLine).toBeDefined()
        expect(shellOwner.frame).toContain("started")
      } finally {
        shellOwner.app.renderer.destroy()
      }

      if (snapshot.directSubtask.parentStatus.type !== "busy")
        throw new Error("expected the direct subtask parent owner status")
      expect(snapshot.directSubtask.parentStatus.activeAssistantMessageID).toBe(snapshot.directSubtask.assistantID)
      if (snapshot.directSubtask.childStatus.type !== "retry")
        throw new Error("expected the direct subtask child retry status")
      expect(snapshot.directSubtask.messages.at(-1)?.info.role).toBe("user")
      const directChildAssistant = snapshot.directSubtask.childMessages.findLast(
        (message) => message.info.role === "assistant",
      )
      if (!directChildAssistant || directChildAssistant.info.role !== "assistant")
        throw new Error("the direct subtask snapshot has no child assistant")
      expect(snapshot.directSubtask.childStatus.activeAssistantMessageID).toBe(directChildAssistant.info.id)

      const directSubtask = await renderDirectSubtask(snapshot, tmp.path)
      try {
        const taskLine = directSubtask.frame
          .split("\n")
          .find((line) => line.includes("Inspect direct subtask ownership"))
        expect(assistantStatus(directSubtask.sync, directSubtask.message.info)).toBe("working")
        if (!taskLine) throw new Error(`direct subtask row missing from rendered frame:\n${directSubtask.frame}`)
        expect(hasSpinner(taskLine ?? "")).toBe(true)
        expect(directSubtask.frame).toContain("Retrying")
        expect(activeForegroundTasks(directSubtask.sync, [directSubtask.message.info])).toHaveLength(1)
      } finally {
        directSubtask.app.renderer.destroy()
      }

      const preAssistant = await renderPreAssistant(snapshot, tmp.path)
      try {
        const lines = preAssistant.frame.split("\n")
        const old = assistant(snapshot.preAssistant.messages, snapshot.preAssistant.oldAssistantID)
        const shell = assistant(snapshot.preAssistant.messages, snapshot.preAssistant.shellAssistantID)
        const task = assistant(snapshot.preAssistant.messages, snapshot.preAssistant.taskAssistantID)
        const oldReasoning = lines.find((line) => line.includes("Thinking status unknown"))
        const oldRead = lines.find((line) => line.includes("→ Read /tmp/unfinished.ts"))
        const shellLine = lines.find((line) => line.includes("hold-shell."))
        const taskLine = lines.find((line) => line.includes("Inspect task ownership"))
        if (!oldReasoning || !oldRead || !shellLine || !taskLine)
          throw new Error(`pre-assistant rows missing from rendered frame:\n${preAssistant.frame}`)
        const preAssistantStatus = preAssistant.sync.data.session_status[snapshot.preAssistant.session.id]
        if (!preAssistantStatus) throw new Error("pre-assistant status was not projected to the TUI store")
        expect(preAssistantStatus).toMatchObject({
          type: "busy",
          activeAssistantMessageID: null,
        })
        expect(assistantStatus(preAssistant.sync, old.info)).toBe("unknown")
        expect(assistantStatus(preAssistant.sync, shell.info)).toBe("unknown")
        expect(assistantStatus(preAssistant.sync, task.info)).toBe("unknown")
        expect(preAssistant.sync.session.status(snapshot.preAssistant.session.id)).toBe("working")
        expect(hasSpinner(oldReasoning ?? "")).toBe(false)
        expect(hasSpinner(oldRead ?? "")).toBe(false)
        expect(hasSpinner(shellLine ?? "")).toBe(false)
        expect(hasSpinner(taskLine ?? "")).toBe(false)
        expect(taskLine).not.toContain("Retrying")
        expect(preAssistant.frame).not.toContain("task-child.txt")
        expect(activeTaskRetry(snapshot.taskChildStatus as SessionStatus, false)).toBeUndefined()
      } finally {
        preAssistant.app.renderer.destroy()
      }

      const taskActive = await renderTask(snapshot, tmp.path)
      try {
        const taskLine = taskActive.frame.split("\n").find((line) => line.includes("Inspect task ownership"))
        expect(assistantStatus(taskActive.sync, taskActive.message.info)).toBe("working")
        expect(taskLine).toBeDefined()
        expect(hasSpinner(taskLine ?? "")).toBe(true)
        expect(taskActive.frame).toContain("Retrying")
        expect(activeTaskRetry(snapshot.taskChildStatus as SessionStatus, true)?.type).toBe("retry")
        expect(activeForegroundTasks(taskActive.sync, [taskActive.message.info])).toHaveLength(1)
      } finally {
        taskActive.app.renderer.destroy()
      }

      const active = await render(snapshot, snapshot.active, tmp.path)
      try {
        const lines = active.frame.split("\n")
        const oldReasoning = lines.find((line) => line.includes("Thinking status unknown"))
        const oldRead = lines.find((line) => line.includes("→ Read /tmp/unfinished.ts"))
        const oldShell = lines.find((line) => line.includes("hold-shell."))
        const currentReasoning = lines.find((line) => hasSpinner(line) && line.includes("Thinking"))
        const currentRead = lines.find((line) => hasSpinner(line) && line.includes("Read /tmp/unfinished.ts"))
        expect(oldReasoning, active.frame).toBeDefined()
        expect(oldRead, active.frame).toBeDefined()
        expect(oldShell, active.frame).toBeDefined()
        expect(currentReasoning, active.frame).toBeDefined()
        expect(currentRead, active.frame).toBeDefined()
        expect(hasSpinner(oldReasoning ?? "")).toBe(false)
        expect(hasSpinner(oldRead ?? "")).toBe(false)
        expect(hasSpinner(oldShell ?? "")).toBe(false)
      } finally {
        active.app.renderer.destroy()
      }

      const legacy = await render(snapshot, { ...snapshot.active, status: { type: "busy" } }, tmp.path)
      try {
        const lines = legacy.frame.split("\n")
        const oldReasoning = lines.find((line) => line.includes("Thinking status unknown"))
        const oldRead = lines.find((line) => line.includes("→ Read /tmp/unfinished.ts"))
        const activeReasoning = lines.find((line) => hasSpinner(line) && line.includes("Thinking"))
        const activeRead = lines.find((line) => hasSpinner(line) && line.includes("Read /tmp/unfinished.ts"))
        expect(oldReasoning).toBeDefined()
        expect(oldRead).toBeDefined()
        expect(activeReasoning).toBeDefined()
        expect(activeRead).toBeDefined()
      } finally {
        legacy.app.renderer.destroy()
      }

      const historicalTask = await renderHistoricalTask(snapshot, snapshot.active, tmp.path)
      try {
        const historicalTaskStatus = historicalTask.sync.data.session_status[snapshot.taskProducerSessionID]
        if (!historicalTaskStatus) throw new Error("historical Task status was not projected to the TUI store")
        expect(historicalTaskStatus).toMatchObject({ activeAssistantMessageID: snapshot.currentAssistantID })
        expect(historicalTask.sync.data.message[snapshot.taskProducerSessionID]?.at(-1)?.id).toBe(
          snapshot.currentAssistantID,
        )
        expect(assistantStatus(historicalTask.sync, historicalTask.message.info)).toBe("unknown")
        expect(activeForegroundTasks(historicalTask.sync, [historicalTask.message.info])).toEqual([])
        expect(historicalTask.frame).toContain("Inspect task ownership")
        expect(hasSpinner(historicalTask.frame)).toBe(false)
        expect(historicalTask.frame).not.toContain("Retrying")
        expect(historicalTask.frame).not.toContain("task-child.txt")
        expect(historicalTask.frame).not.toContain("↳")
        expect(activeTaskRetry(snapshot.taskChildStatus as SessionStatus, false)).toBeUndefined()
      } finally {
        historicalTask.app.renderer.destroy()
      }

      const historicalSubtask = await renderHistoricalSubtask(snapshot, snapshot.active, tmp.path)
      try {
        expect(assistantStatus(historicalSubtask.sync, historicalSubtask.message.info)).toBe("unknown")
        expect(activeForegroundTasks(historicalSubtask.sync, [historicalSubtask.message.info])).toEqual([])
        if (!historicalSubtask.frame.includes("Inspect direct subtask ownership"))
          throw new Error(`historical direct subtask row missing from rendered frame:\n${historicalSubtask.frame}`)
        expect(hasSpinner(historicalSubtask.frame)).toBe(false)
        expect(historicalSubtask.frame).not.toContain("Retrying")
        expect(historicalSubtask.frame).not.toContain("direct-subtask-child.txt")
        expect(historicalSubtask.frame).not.toContain("↳")
      } finally {
        historicalSubtask.app.renderer.destroy()
      }

      const nonOwner = await render(snapshot, snapshot.active, tmp.path, false)
      try {
        expect(nonOwner.frame).not.toContain("Tool execution aborted")
        const lines = nonOwner.frame.split("\n")
        const oldReasoning = lines.find((line) => line.includes("Thinking status unknown"))
        const oldRead = lines.find((line) => line.includes("→ Read /tmp/unfinished.ts"))
        const oldShell = lines.find((line) => line.includes("hold-shell."))
        expect(oldReasoning).toBeDefined()
        expect(oldRead).toBeDefined()
        expect(oldShell).toBeDefined()
        expect(hasSpinner(oldReasoning ?? "")).toBe(false)
        expect(hasSpinner(oldRead ?? "")).toBe(false)
        expect(hasSpinner(oldShell ?? "")).toBe(false)
      } finally {
        nonOwner.app.renderer.destroy()
      }

      const completed = await render(snapshot, snapshot.completed, tmp.path)
      try {
        const lines = completed.frame.split("\n")
        const oldReasoning = lines.find((line) => line.includes("Thinking status unknown"))
        const oldRead = lines.find((line) => line.includes("→ Read /tmp/unfinished.ts"))
        const oldShell = lines.find((line) => line.includes("hold-shell."))
        expect(oldReasoning).toBeDefined()
        expect(oldRead).toBeDefined()
        expect(oldShell).toBeDefined()
        expect(hasSpinner(oldReasoning ?? "")).toBe(false)
        expect(hasSpinner(oldRead ?? "")).toBe(false)
        expect(hasSpinner(oldShell ?? "")).toBe(false)
      } finally {
        completed.app.renderer.destroy()
      }
    } finally {
      if (child.exitCode === null) child.kill()
    }
  }, 120_000)
})
