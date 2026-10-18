import {
  BoxRenderable,
  RGBA,
  TextareaRenderable,
  MouseEvent,
  PasteEvent,
  decodePasteBytes,
  type KeyEvent,
  type Renderable,
} from "@opentui/core"
import type { CommandContext } from "@opentui/keymap"
import { createEffect, createMemo, onMount, createSignal, onCleanup, on, Show, Switch, Match } from "solid-js"
import { registerOpencodeSpinner } from "../register-spinner"
import path from "path"
import { fileURLToPath } from "url"
import { useLocal } from "../../context/local"
import { Flag } from "@opencode-ai/core/flag/flag"
import { tint, useTheme } from "../../context/theme"
import { EmptyBorder, SplitBorder } from "../../ui/border"
import { useTuiPaths, useTuiTerminalEnvironment } from "../../context/runtime"
import { useClipboard } from "../../context/clipboard"
import { Spinner } from "../spinner"
import { useSDK } from "../../context/sdk"
import { useRoute } from "../../context/route"
import { useProject } from "../../context/project"
import { carries, useSync } from "../../context/sync"
import { useEvent } from "../../context/event"
import { editorSelectionKey, useEditorContext, type EditorSelection } from "../../context/editor"
import { normalizePromptContent, openEditor } from "../../editor"
import { useExit } from "../../context/exit"
import { promptOffsetWidth } from "../../prompt/display"
import { createStore, produce, unwrap } from "solid-js/store"
import { usePromptHistory, type PromptInfo } from "../../prompt/history"
import { computePromptTraits } from "../../prompt/traits"
import {
  expandedOffset,
  expandPastedTextPlaceholders,
  expandTrackedPastedText,
  recalledPrompt,
} from "../../prompt/part"
import { usePromptStash } from "../../prompt/stash"
import { DialogStash } from "../dialog-stash"
import { type AutocompleteRef, Autocomplete } from "./autocomplete"
import { useRenderer, useTerminalDimensions, type JSX } from "@opentui/solid"
import type { AssistantMessage, FilePart, SessionPromptQueueReceipt, UserMessage } from "@opencode-ai/sdk/v2"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { Locale } from "../../util/locale"
import { errorMessage } from "../../util/error"
import {
  promptEnhancementContext,
  type MessageWithParts,
  type TranscriptOptions,
  type TranscriptPresentation,
} from "../../util/transcript"
import { isRecord } from "../../util/record"
import { formatDuration } from "../../util/format"
import { createColors, createFrames } from "../../ui/spinner"
import { useDialog } from "../../ui/dialog"
import { DialogProvider as DialogProviderConnect } from "../dialog-provider"
import { DialogAlert } from "../../ui/dialog-alert"
import { useToast } from "../../ui/toast"
import { useKV } from "../../context/kv"
import { createFadeIn } from "../../util/signal"
import { DialogSkill } from "../dialog-skill"
import { DialogWorkspaceUnavailable } from "../dialog-workspace-unavailable"
import { useArgs } from "../../context/args"
import { OPENCODE_BASE_MODE, useBindings, useCommandShortcut, useLeaderActive, useOpencodeKeymap } from "../../keymap"
import { useTuiConfig } from "../../config"
import { usePromptWorkspace } from "./workspace"
import { pendingPrompts, PromptQueueRow, type PendingPrompt } from "./queue"
import { usePromptMove } from "./move"
import { readLocalAttachment } from "./local-attachment"
import { useLocation } from "../../context/location"

registerOpencodeSpinner()

export type PromptProps = {
  sessionID?: string
  enhancementContext?: () => {
    messages: MessageWithParts[]
    options: Pick<TranscriptOptions, "thinking" | "toolDetails" | "assistantMetadata" | "providers">
    presentation: TranscriptPresentation
  }
  visible?: boolean
  disabled?: boolean
  onSubmit?: () => void
  ref?: (ref: PromptRef | undefined) => void
  hint?: JSX.Element
  right?: JSX.Element
  showPlaceholder?: boolean
  placeholders?: {
    normal?: string[]
    shell?: string[]
  }
}

function pastedFilepath(value: string, platform: string) {
  const raw = value.replace(/^['"]+|['"]+$/g, "")
  if (raw.startsWith("file://")) {
    try {
      return fileURLToPath(raw)
    } catch {}
  }
  if (platform === "win32") return raw
  return raw.replace(/\\(.)/g, "$1")
}

export type PromptRef = {
  focused: boolean
  current: PromptInfo
  set(prompt: PromptInfo): void
  reset(): void
  blur(): void
  focus(): void
  submit(): void
}

const money = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
})

// A recall asks the server where a prompt is this often, until this backstop:
// whether one still being sent is queued yet, which the server does on
// arrival, and where one it could not keep went.
const RECALL_POLL_MS = 50
const RECALL_BACKSTOP_MS = 10_000

function randomIndex(count: number) {
  if (count <= 0) return 0
  return Math.floor(Math.random() * count)
}

function fadeColor(color: RGBA, alpha: number) {
  return RGBA.fromValues(color.r, color.g, color.b, color.a * alpha)
}

function hasEditorRangeSelection(selection: EditorSelection["ranges"][number]) {
  return (
    selection.selection.start.line !== selection.selection.end.line ||
    selection.selection.start.character !== selection.selection.end.character
  )
}

function getEditorRangeLabel(selection: EditorSelection["ranges"][number]) {
  if (!hasEditorRangeSelection(selection)) return
  if (selection.selection.start.line === selection.selection.end.line) return `#${selection.selection.start.line}`
  return `#${selection.selection.start.line}-${selection.selection.end.line}`
}

function formatEditorContext(selection: EditorSelection) {
  const selected = selection.ranges.filter(hasEditorRangeSelection)
  if (selected.length === 0)
    return `<system-reminder>Note: The user opened the file "${selection.filePath}". This may or may not be relevant to the current task.</system-reminder>\n`

  const ranges = selected.map((range, index) => {
    const prefix = selected.length > 1 ? `Selection ${index + 1}: ` : ""
    return `Note: The user selected ${prefix}${getEditorRangeLabel(range)} from "${selection.filePath}". \`\`\`${range.text}\`\`\`\n\n`
  })

  return `<system-reminder>${ranges.join("\n")} This may or may not be relevant to the current task.</system-reminder>\n`
}

let stashed: { prompt: PromptInfo; cursor: number } | undefined

export function Prompt(props: PromptProps) {
  let input: TextareaRenderable
  let anchor: BoxRenderable
  const [inputTarget, setInputTarget] = createSignal<TextareaRenderable | undefined>()

  const leader = useLeaderActive()
  const local = useLocal()
  const args = useArgs()
  const paths = useTuiPaths()
  const location = useLocation()
  const terminalEnvironment = useTuiTerminalEnvironment()
  const clipboard = useClipboard()
  const sdk = useSDK()
  const editor = useEditorContext()
  const route = useRoute()
  const project = useProject()
  const sync = useSync()
  const tuiConfig = useTuiConfig()
  const dialog = useDialog()
  const toast = useToast()
  const status = createMemo(() => sync.data.session_status?.[props.sessionID ?? ""] ?? { type: "idle" })
  const history = usePromptHistory()
  const stash = usePromptStash()
  const keymap = useOpencodeKeymap()
  const agentShortcut = useCommandShortcut("agent.cycle")
  const paletteShortcut = useCommandShortcut("command.palette.show")
  const renderer = useRenderer()
  const exit = useExit()
  const dimensions = useTerminalDimensions()
  const { theme, syntax } = useTheme()
  const kv = useKV()
  const animationsEnabled = createMemo(() => kv.get("animations_enabled", true))
  const list = createMemo(() => props.placeholders?.normal ?? [])
  const shell = createMemo(() => props.placeholders?.shell ?? [])
  const fileContextEnabled = createMemo(() => kv.get("file_context_enabled", true))
  const [dismissedEditorSelectionKey, setDismissedEditorSelectionKey] = createSignal<string>()
  const editorContext = createMemo(() => {
    const selection = fileContextEnabled() ? editor.selection() : undefined
    if (!selection) return
    return editorSelectionKey(selection) === dismissedEditorSelectionKey() ? undefined : selection
  })
  const editorPath = createMemo(() => editorContext()?.filePath)
  const editorSelectionLabel = createMemo(() => {
    const ranges = editorContext()?.ranges
    if (!ranges) return
    const first = ranges.find(hasEditorRangeSelection) ?? ranges[0]
    if (!first) return
    return [getEditorRangeLabel(first), ranges.length > 1 ? `+${ranges.length - 1}` : undefined]
      .filter(Boolean)
      .join(" ")
  })
  const editorFileLabel = createMemo(() => {
    const value = editorPath()
    if (!value) return
    const filename = path.basename(value)
    const file = /^index\.[^./]+$/.test(filename)
      ? [path.basename(path.dirname(value)), filename].filter(Boolean).join("/")
      : filename
    return `${file.split(path.sep).join("/")}${editorSelectionLabel() ?? ""}`
  })
  const editorFileLabelDisplay = createMemo(() => {
    const file = editorFileLabel()
    if (!file) return
    return Locale.truncateMiddle(file, Math.max(12, Math.min(48, Math.floor(dimensions().width / 3))))
  })
  const editorContextLabelState = createMemo(() => editor.labelState())
  const [auto, setAuto] = createSignal<AutocompleteRef>()
  const workspace = usePromptWorkspace(props.sessionID)
  const move = usePromptMove({ projectID: project.project, sessionID: () => props.sessionID })
  const [cursorVersion, setCursorVersion] = createSignal(0)
  const currentProviderLabel = createMemo(() => local.model.parsed().provider)
  const hasRightContent = createMemo(() => Boolean(props.right))

  function promptModelWarning() {
    toast.show({
      variant: "warning",
      message: "Connect a provider to send prompts",
      duration: 3000,
    })
    if (sync.data.provider.length === 0) {
      dialog.replace(() => <DialogProviderConnect />)
    }
  }

  function dismissEditorContext() {
    setDismissedEditorSelectionKey(editorSelectionKey(editorContext()))
    editor.clearSelection()
  }
  const fileStyleId = syntax().getStyleId("extmark.file")!
  const agentStyleId = syntax().getStyleId("extmark.agent")!
  const pasteStyleId = syntax().getStyleId("extmark.paste")!
  let promptPartTypeId = 0
  const event = useEvent()

  event.on("tui.prompt.append", (evt, { workspace }) => {
    if (workspace !== project.workspace.current()) return
    if (!input || input.isDestroyed) return
    setStore("prompt", "machineAppended", true)
    input.insertText(evt.properties.text)
    setTimeout(() => {
      // setTimeout is a workaround and needs to be addressed properly
      if (!input || input.isDestroyed) return
      input.getLayoutNode().markDirty()
      input.gotoBufferEnd()
      renderer.requestRender()
    }, 0)
  })

  createEffect(() => {
    if (!input || input.isDestroyed) return
    if (props.disabled) input.cursorColor = theme.backgroundElement
    if (!props.disabled) input.cursorColor = theme.text
    if (tuiConfig.cursor) input.cursorStyle = tuiConfig.cursor
  })

  const lastUserMessage = createMemo(() => {
    if (!props.sessionID) return undefined
    const messages = sync.data.message[props.sessionID]
    if (!messages) return undefined
    return messages.findLast((m): m is UserMessage => m.role === "user")
  })

  const usage = createMemo(() => {
    if (!props.sessionID) return
    const session = sync.session.get(props.sessionID)
    const msg = sync.data.message[props.sessionID] ?? []
    const last = msg.findLast((item): item is AssistantMessage => item.role === "assistant" && item.tokens.output > 0)
    if (!last) return

    const tokens =
      last.tokens.input + last.tokens.output + last.tokens.reasoning + last.tokens.cache.read + last.tokens.cache.write
    if (tokens <= 0) return

    const model = sync.data.provider.find((item) => item.id === last.providerID)?.models[last.modelID]
    const pct = model?.limit.context ? `${Math.round((tokens / model.limit.context) * 100)}%` : undefined
    const cost = session?.cost ?? 0
    return {
      context: pct ? `${Locale.number(tokens)} (${pct})` : Locale.number(tokens),
      cost: cost > 0 ? money.format(cost) : undefined,
    }
  })

  const [store, setStore] = createStore<{
    prompt: PromptInfo
    mode: "normal" | "shell"
    extmarkToPartIndex: Map<number, number>
    interrupt: number
    placeholder: number
  }>({
    placeholder: randomIndex(list().length),
    prompt: {
      input: "",
      parts: [],
    },
    mode: "normal",
    extmarkToPartIndex: new Map(),
    interrupt: 0,
  })
  const [enhancing, setEnhancing] = createSignal(false)
  let promptRevision = 0
  let promptClearRevision = 0

  createEffect(
    on(
      () => props.sessionID,
      () => history.reset(),
    ),
  )

  createEffect(
    on(
      () => props.sessionID,
      () => {
        setStore("placeholder", randomIndex(list().length))
      },
      { defer: true },
    ),
  )

  // Initialize agent/model/variant from last user message when session changes
  let syncedSessionID: string | undefined
  createEffect(() => {
    const sessionID = props.sessionID
    const msg = lastUserMessage()

    if (sessionID !== syncedSessionID) {
      if (!sessionID || !msg) return

      syncedSessionID = sessionID

      // Only set agent if it's a primary agent (not a subagent)
      const isPrimaryAgent = local.agent.list().some((x) => x.name === msg.agent)
      if (msg.agent && isPrimaryAgent) {
        // Keep command line --agent if specified.
        if (!args.agent) local.agent.set(msg.agent)
        if (msg.model) {
          local.model.set(msg.model)
          // Restore rather than set, so a command line --effort keeps applying where the
          // session's model declares it, like --agent above.
          local.model.variant.restore(msg.model.variant)
        }
      }
    }
  })

  // The picker is the open session's selection, which its pending prompts run
  // at when they are delivered (swxtchio/swx-opencode#212).
  createEffect(
    on(
      local.model.chosen,
      () => {
        const model = local.model.current()
        if (!props.sessionID || !model) return
        void sdk.client.session
          .update({
            sessionID: props.sessionID,
            model: {
              providerID: model.providerID,
              id: model.modelID,
              variant: local.model.variant.current() ?? "default",
            },
          })
          .then((result) => {
            if (result.error)
              toast.show({ variant: "error", message: "Could not select this model for the session's queued prompts" })
          })
      },
      { defer: true },
    ),
  )

  const promptCommands = createMemo(() =>
    [
      {
        title: "Clear prompt",
        name: "prompt.clear",
        category: "Prompt",
        hidden: true,
        run: () => {
          clearPrompt()
          dialog.clear()
        },
      },
      {
        title: "Submit prompt",
        name: "prompt.submit",
        category: "Prompt",
        hidden: true,
        run: async () => {
          if (!input.focused) return
          const handled = await submit()
          if (!handled) return

          dialog.clear()
        },
      },
      {
        title: "Enhance prompt",
        name: "prompt.enhance",
        category: "Prompt",
        enabled: !!store.prompt.input && !enhancing(),
        run: () => void enhancePrompt(),
      },
      {
        title: "Remove editor context",
        name: "prompt.editor_context.clear",
        category: "Prompt",
        enabled: Boolean(editorContext()),
        run: () => {
          dismissEditorContext()
          dialog.clear()
        },
      },
      {
        title: "Paste",
        name: "prompt.paste",
        category: "Prompt",
        hidden: true,
        run: async (ctx: CommandContext<Renderable, KeyEvent>) => {
          ctx.event.preventDefault()
          ctx.event.stopPropagation()
          const content = await clipboard.read?.()
          if (content?.mime.startsWith("image/")) {
            await pasteAttachment({
              filename: "clipboard",
              mime: content.mime,
              content: content.data,
            })
            return
          }
          if (content?.mime === "text/plain") {
            await pasteInputText(content.data)
          }
        },
      },
      {
        title: "Interrupt session",
        name: "session.interrupt",
        category: "Session",
        hidden: true,
        enabled: status().type !== "idle",
        run: () => {
          if (auto()?.visible) return
          if (!input.focused) return
          // TODO: this should be its own command
          if (store.mode === "shell") {
            setStore("mode", "normal")
            return
          }
          if (!props.sessionID) return

          setStore("interrupt", store.interrupt + 1)

          setTimeout(() => {
            setStore("interrupt", 0)
          }, 5000)

          if (store.interrupt >= 2) {
            void sdk.client.session.abort({
              sessionID: props.sessionID,
            })
            setStore("interrupt", 0)
          }
          dialog.clear()
        },
      },
      {
        title: "Open editor",
        category: "Session",
        name: "prompt.editor",
        slashName: "editor",
        run: async () => {
          dialog.clear()

          // replace summarized text parts with the actual text
          const text = store.prompt.parts
            .filter((p) => p.type === "text")
            .reduce((acc, p) => {
              if (!p.source) return acc
              return acc.replace(p.source.text.value, p.text)
            }, store.prompt.input)

          const nonTextParts = store.prompt.parts.filter((p) => p.type !== "text")

          const value = text
          const content = await openEditor({
            renderer,
            value,
            cwd:
              (project.instance.path().worktree === "/" ? undefined : project.instance.path().worktree) ||
              project.instance.directory() ||
              paths.cwd,
          })
          if (!content) return
          const normalized = normalizePromptContent(content)

          input.setText(normalized)

          // Update positions for nonTextParts based on their location in new content
          // Filter out parts whose virtual text was deleted
          // this handles a case where the user edits the text in the editor
          // such that the virtual text moves around or is deleted
          const updatedNonTextParts = nonTextParts
            .map((part) => {
              let virtualText = ""
              if (part.type === "file" && part.source?.text) {
                virtualText = part.source.text.value
              } else if (part.type === "agent" && part.source) {
                virtualText = part.source.value
              }

              if (!virtualText) return part

              const newStart = normalized.indexOf(virtualText)
              // if the virtual text is deleted, remove the part
              if (newStart === -1) return null

              const newEnd = newStart + virtualText.length

              if (part.type === "file" && part.source?.text) {
                return {
                  ...part,
                  source: {
                    ...part.source,
                    text: {
                      ...part.source.text,
                      start: newStart,
                      end: newEnd,
                    },
                  },
                }
              }

              if (part.type === "agent" && part.source) {
                return {
                  ...part,
                  source: {
                    ...part.source,
                    start: newStart,
                    end: newEnd,
                  },
                }
              }

              return part
            })
            .filter((part) => part !== null)

          setStore("prompt", {
            input: normalized,
            // keep only the non-text parts because the text parts were
            // already expanded inline
            parts: updatedNonTextParts,
          })
          restoreExtmarksFromParts(updatedNonTextParts)
          input.cursorOffset = Bun.stringWidth(normalized)
        },
      },
      {
        title: "Skills",
        name: "prompt.skills",
        category: "Prompt",
        slashName: "skills",
        run: () => {
          dialog.replace(() => (
            <DialogSkill
              onSelect={(skill) => {
                input.setText(`/${skill} `)
                setStore("prompt", {
                  input: `/${skill} `,
                  parts: [],
                })
                input.gotoBufferEnd()
              }}
            />
          ))
        },
      },
      {
        title: "Warp",
        desc: "Change the workspace for the session",
        name: "workspace.set",
        category: "Session",
        enabled: Flag.OPENCODE_EXPERIMENTAL_WORKSPACES,
        slashName: "warp",
        run: () => {
          workspace.open()
        },
      },
      {
        title: "Move session",
        desc: "Move to another project dir",
        name: "session.move",
        category: "Session",
        slashName: "move",
        run: () => {
          move.open()
        },
      },
    ].map((entry) => ({
      namespace: "palette",
      ...entry,
    })),
  )

  useBindings(() => ({
    commands: promptCommands(),
  }))

  useBindings(() => ({
    mode: OPENCODE_BASE_MODE,
    bindings: tuiConfig.keybinds.gather("prompt.palette", [
      "prompt.submit",
      "prompt.editor",
      "prompt.editor_context.clear",
      "prompt.discard",
      "prompt.stash.pop",
      "prompt.stash.list",
      "prompt.skills",
      "session.interrupt",
      "workspace.set",
      "session.move",
    ]),
  }))

  useBindings(() => ({
    mode: OPENCODE_BASE_MODE,
    enabled: () => !!input && !input.isDestroyed && input.focused && store.mode === "normal",
    priority: 2,
    bindings: tuiConfig.keybinds.get("prompt.enhance"),
  }))

  const ref: PromptRef = {
    get focused() {
      return input.focused
    },
    get current() {
      return store.prompt
    },
    focus() {
      input.focus()
    },
    blur() {
      input.blur()
    },
    set(prompt) {
      if (!prompt.input) promptClearRevision++
      input.setText(prompt.input)
      setStore("prompt", prompt)
      restoreExtmarksFromParts(prompt.parts)
      input.gotoBufferEnd()
    },
    reset() {
      promptClearRevision++
      input.clear()
      input.extmarks.clear()
      setStore("prompt", {
        input: "",
        parts: [],
      })
      setStore("extmarkToPartIndex", new Map())
    },
    submit() {
      void submit()
    },
  }

  onMount(() => {
    const saved = stashed
    stashed = undefined
    if (store.prompt.input) return
    if (saved && saved.prompt.input) {
      input.setText(saved.prompt.input)
      setStore("prompt", saved.prompt)
      restoreExtmarksFromParts(saved.prompt.parts)
      input.cursorOffset = saved.cursor
    }
  })

  onCleanup(() => {
    if (store.prompt.input && input.plainText) {
      stashed = { prompt: unwrap(store.prompt), cursor: input.cursorOffset }
    }
    setInputTarget(undefined)
    props.ref?.(undefined)
  })

  createEffect(() => {
    if (!input || input.isDestroyed) return
    if (props.visible === false || dialog.stack.length > 0) {
      if (input.focused) input.blur()
      return
    }

    // Slot/plugin updates can remount the background prompt while a dialog is open.
    // Keep focus with the dialog and let the prompt reclaim it after the dialog closes.
    if (!input.focused) input.focus()
  })

  createEffect(() => {
    if (!input || input.isDestroyed) return
    input.traits = {
      ...input.traits,
      ...computePromptTraits({
        mode: store.mode,
        autocompleteVisible: !!auto()?.visible,
      }),
    }
  })

  function restoreExtmarksFromParts(parts: PromptInfo["parts"]) {
    input.extmarks.clear()
    setStore("extmarkToPartIndex", new Map())

    parts.forEach((part, partIndex) => {
      let start = 0
      let end = 0
      let virtualText = ""
      let styleId: number | undefined

      if (part.type === "file" && part.source?.text) {
        start = part.source.text.start
        end = part.source.text.end
        virtualText = part.source.text.value
        styleId = fileStyleId
      } else if (part.type === "agent" && part.source) {
        start = part.source.start
        end = part.source.end
        virtualText = part.source.value
        styleId = agentStyleId
      } else if (part.type === "text" && part.source?.text) {
        start = part.source.text.start
        end = part.source.text.end
        virtualText = part.source.text.value
        styleId = pasteStyleId
      }

      if (virtualText) {
        const extmarkId = input.extmarks.create({
          start,
          end,
          virtual: true,
          styleId,
          typeId: promptPartTypeId,
        })
        setStore("extmarkToPartIndex", (map: Map<number, number>) => {
          const newMap = new Map(map)
          newMap.set(extmarkId, partIndex)
          return newMap
        })
      }
    })
  }

  function syncExtmarksWithPromptParts() {
    const allExtmarks = input.extmarks.getAllForTypeId(promptPartTypeId)
    setStore(
      produce((draft) => {
        const newMap = new Map<number, number>()
        const newParts: typeof draft.prompt.parts = []

        for (const extmark of allExtmarks) {
          const partIndex = draft.extmarkToPartIndex.get(extmark.id)
          if (partIndex !== undefined) {
            const part = draft.prompt.parts[partIndex]
            if (part) {
              if (part.type === "agent" && part.source) {
                part.source.start = extmark.start
                part.source.end = extmark.end
              } else if (part.type === "file" && part.source?.text) {
                part.source.text.start = extmark.start
                part.source.text.end = extmark.end
              } else if (part.type === "text" && part.source?.text) {
                part.source.text.start = extmark.start
                part.source.text.end = extmark.end
              }
              newMap.set(extmark.id, newParts.length)
              newParts.push(part)
            }
          }
        }

        draft.extmarkToPartIndex = newMap
        draft.prompt.parts = newParts
      }),
    )
  }

  const stashCommands = createMemo(() =>
    [
      {
        title: "Discard draft",
        name: "prompt.discard",
        category: "Prompt",
        enabled: !!store.prompt.input,
        run: () => {
          if (!store.prompt.input) return
          clearPrompt()
          dialog.clear()
        },
      },
      {
        title: "Stash pop",
        name: "prompt.stash.pop",
        category: "Prompt",
        enabled: stash.list().length > 0,
        run: () => {
          const entry = stash.pop()
          if (entry) {
            input.setText(entry.input)
            setStore("prompt", {
              input: entry.input,
              parts: entry.parts,
              ...(entry.machineAppended ? { machineAppended: true } : {}),
            })
            restoreExtmarksFromParts(entry.parts)
            input.gotoBufferEnd()
          }
          dialog.clear()
        },
      },
      {
        title: "Stash list",
        name: "prompt.stash.list",
        category: "Prompt",
        enabled: stash.list().length > 0,
        run: () => {
          dialog.replace(() => (
            <DialogStash
              onSelect={(entry) => {
                input.setText(entry.input)
                setStore("prompt", {
                  input: entry.input,
                  parts: entry.parts,
                  ...(entry.machineAppended ? { machineAppended: true } : {}),
                })
                restoreExtmarksFromParts(entry.parts)
                input.gotoBufferEnd()
              }}
            />
          ))
        },
      },
    ].map((entry) => ({
      namespace: "palette",
      ...entry,
    })),
  )

  useBindings(() => ({
    commands: stashCommands(),
  }))

  useBindings(() => {
    return {
      target: inputTarget,
      enabled: inputTarget() !== undefined && !props.disabled,
      bindings: tuiConfig.keybinds.get("prompt.paste"),
    }
  })

  useBindings(() => {
    return {
      target: inputTarget,
      enabled: inputTarget() !== undefined && !props.disabled && store.prompt.input !== "",
      bindings: tuiConfig.keybinds.get("prompt.clear"),
    }
  })

  useBindings(() => {
    return {
      target: inputTarget,
      enabled: (() => {
        cursorVersion()
        return (
          inputTarget() !== undefined &&
          !props.disabled &&
          store.mode === "normal" &&
          !auto()?.visible &&
          input?.visualCursor.offset === 0
        )
      })(),
      bindings: [
        {
          key: "!",
          desc: "Shell mode",
          group: "Prompt",
          cmd: () => {
            setStore("placeholder", randomIndex(shell().length))
            setStore("mode", "shell")
          },
        },
      ],
    }
  })

  useBindings(() => {
    return {
      target: inputTarget,
      enabled: inputTarget() !== undefined && store.mode === "shell",
      bindings: [{ key: "escape", desc: "Exit shell mode", group: "Prompt", cmd: () => setStore("mode", "normal") }],
    }
  })

  useBindings(() => {
    return {
      target: inputTarget,
      enabled: (() => {
        cursorVersion()
        return inputTarget() !== undefined && store.mode === "shell" && input?.visualCursor.offset === 0
      })(),
      bindings: [{ key: "backspace", desc: "Exit shell mode", group: "Prompt", cmd: () => setStore("mode", "normal") }],
    }
  })

  useBindings(() => {
    return {
      target: inputTarget,
      enabled: (() => {
        cursorVersion()
        return inputTarget() !== undefined && !props.disabled && !auto()?.visible && input !== undefined
      })(),
      commands: [
        {
          name: "prompt.history.previous",
          title: "Previous prompt history",
          category: "Prompt",
          run() {
            if (input.cursorOffset !== 0) {
              if (input.scrollY + input.visualCursor.visualRow === 0) input.cursorOffset = 0
              return false
            }

            // While a recall holds a prompt, UP does nothing: its withdrawal may
            // already have emptied the pending list, and history would copy the
            // prompt it is taking back.
            if (recalling) return true
            // An empty composer takes back the last pending prompt instead (#249).
            const pending = !input.plainText && store.mode === "normal" ? pendingPrompts(sync, props.sessionID) : []
            if (pending.length > 0) {
              void recall(pending.at(-1)!)
              return true
            }

            const item = history.move(props.sessionID, -1, input.plainText, sync.data.config.machine_message_markers)
            if (!item) return false
            input.setText(item.input)
            setStore("prompt", item)
            setStore("mode", item.mode ?? "normal")
            restoreExtmarksFromParts(item.parts)
            input.cursorOffset = 0
          },
        },
      ],
      bindings: tuiConfig.keybinds.get("prompt.history.previous"),
    }
  })

  useBindings(() => {
    return {
      target: inputTarget,
      enabled: (() => {
        cursorVersion()
        return inputTarget() !== undefined && !props.disabled && !auto()?.visible && input !== undefined
      })(),
      commands: [
        {
          name: "prompt.history.next",
          title: "Next prompt history",
          category: "Prompt",
          run() {
            if (input.cursorOffset !== input.plainText.length) {
              if (
                input.scrollY + input.visualCursor.visualRow ===
                Math.max(0, input.editorView.getTotalVirtualLineCount() - 1)
              )
                input.cursorOffset = input.plainText.length
              return false
            }

            const item = history.move(props.sessionID, 1, input.plainText, sync.data.config.machine_message_markers)
            if (!item) return false
            input.setText(item.input)
            setStore("prompt", item)
            setStore("mode", item.mode ?? "normal")
            restoreExtmarksFromParts(item.parts)
            input.cursorOffset = input.plainText.length
          },
        },
      ],
      bindings: tuiConfig.keybinds.get("prompt.history.next"),
    }
  })

  // UP takes back the last pending prompt (#249). Only a withdrawal the server
  // confirms hands it to the composer, so one promoted meanwhile is never
  // copied. One recall runs at a time, through any restore it needs, so a
  // second UP never takes a prompt the first is still putting back.
  let recalling = false
  async function recall(prompt: PendingPrompt) {
    const sessionID = props.sessionID
    if (recalling || !sessionID) return
    recalling = true
    await takeBack(sessionID, prompt).finally(() => (recalling = false))
  }

  async function takeBack(sessionID: string, prompt: PendingPrompt) {
    const admission = prompt.item ? { itemID: prompt.item.id } : await admitted(sessionID, prompt.requestID)
    if (admission === "sending") return toast.show({ variant: "info", message: "That prompt is still being sent" })
    if (!admission) return toast.show({ variant: "info", message: "That prompt is no longer pending" })
    const itemID = admission.itemID
    const withdrawn = await sdk.client.session.queue.withdraw({ sessionID, itemID }).catch(() => undefined)
    if (withdrawn?.response?.status === 404)
      return toast.show({ variant: "info", message: "That prompt is no longer pending" })
    const requestID = prompt.item?.requestID ?? prompt.requestID
    // Any other failure may still have withdrawn it.
    if (!withdrawn?.data)
      return giveBack(
        { sessionID, itemID, requestID },
        prompt.item
          ? recalledPrompt(prompt.item)
          : { input: prompt.text, parts: [], ...(prompt.machineAppended ? { machineAppended: true } : {}) },
        "Could not take that prompt back",
      )
    sync.queue.withdrawn(sessionID, withdrawn.data)
    const recalled = recalledPrompt(withdrawn.data)
    if (input.isDestroyed || input.plainText || props.sessionID !== sessionID)
      return giveBack({ sessionID, itemID, requestID }, recalled, "The composer was in use")
    input.setText(recalled.input)
    setStore("prompt", recalled)
    setStore("mode", "normal")
    restoreExtmarksFromParts(recalled.parts)
    input.gotoBufferEnd()
  }

  // The item a send still on its way became, once the server admits it, which
  // it does before any other work on the prompt; nothing when the send settled
  // without leaving one pending.
  async function admitted(sessionID: string, requestID: string | undefined) {
    if (!requestID) return undefined
    const deadline = Date.now() + RECALL_BACKSTOP_MS
    while (Date.now() < deadline) {
      const listed = sync.data.session_queue[sessionID]?.find((item) => carries(item, requestID))
      if (listed) return { itemID: listed.id }
      const receipt = await carried(sessionID, requestID)
      if (receipt) return receipt.state === "pending" ? { itemID: receipt.itemID } : undefined
      if (!sync.data.session_sending[sessionID]?.some((sending) => sending.requestID === requestID)) return undefined
      await Bun.sleep(RECALL_POLL_MS)
    }
    return "sending" as const
  }

  // A request's receipt, following any merge to the request that now carries
  // it (#228): null when the server has no such request, undefined when it
  // could not be read.
  async function carried(
    sessionID: string,
    requestID: string,
    seen = new Set<string>(),
  ): Promise<SessionPromptQueueReceipt | null | undefined> {
    const read = await sdk.client.session.queue.receipt({ sessionID, requestID }).catch(() => undefined)
    if (!read?.data) return read?.response?.status === 404 ? null : undefined
    if (read.data.state !== "merged" || read.data.messageID || seen.has(read.data.into)) return read.data
    return carried(sessionID, read.data.into, seen.add(requestID))
  }

  // Puts back a prompt this recall may have withdrawn, with one restore, then
  // says where it is only once the server confirms it. Its text becomes the
  // user's to resend only when the server confirms it is still withdrawn, so
  // the queue and the user never both hold it to send.
  async function giveBack(
    taken: { sessionID: string; itemID: string; requestID?: string },
    prompt: PromptInfo,
    reason: string,
  ) {
    const restored = await sdk.client.session.queue
      .restore({ sessionID: taken.sessionID, id: taken.itemID })
      .catch(() => undefined)
    // Answered either way, the queue holds it: put back, or never withdrawn.
    const answered = restored?.data !== undefined || restored?.response?.status === 404
    const deadline = Date.now() + RECALL_BACKSTOP_MS
    while (Date.now() < deadline) {
      const where = await custody(taken, answered)
      if (where === "pending") return toast.show({ variant: "info", message: `${reason}; it stays queued` })
      if (where === "gone")
        return toast.show({
          variant: "info",
          message: restored?.data
            ? `${reason}; it went back to the queue and is no longer pending`
            : "That prompt is no longer pending",
        })
      if (where === "withdrawn") {
        stash.push({
          input: prompt.input,
          parts: prompt.parts,
          ...(prompt.machineAppended ? { machineAppended: true } : {}),
        })
        return toast.show({
          variant: "error",
          message: `${reason}, and it could not be put back in the queue; its text is in the prompt stash`,
        })
      }
      await Bun.sleep(RECALL_POLL_MS)
    }
    toast.show({
      variant: "error",
      message: `${reason}, and could not confirm where it is; nothing was copied`,
    })
  }

  // Where a taken prompt is now, from what the server says after the restore:
  // the queue's list, then its request's receipt. undefined while neither can
  // tell.
  async function custody(taken: { sessionID: string; itemID: string; requestID?: string }, answered: boolean) {
    const listed = await sdk.client.session.queue.list({ sessionID: taken.sessionID }).catch(() => undefined)
    if (listed?.data) sync.queue.listed(taken.sessionID, listed.data)
    if (listed?.data?.some((item) => item.id === taken.itemID)) return "pending" as const
    const receipt = taken.requestID ? await carried(taken.sessionID, taken.requestID) : undefined
    if (receipt?.state === "pending") return "pending" as const
    if (receipt?.state === "withdrawn" && receipt.itemID === taken.itemID) return "withdrawn" as const
    if (receipt || receipt === null) return "gone" as const
    // Not listed, and the restore's answer says the queue held it after.
    if (listed?.data && answered) return "gone" as const
    return undefined
  }

  let submitting = false
  async function submit() {
    // Prevent overlapping invocations (e.g. a double-pressed Enter, or the
    // input's native onSubmit racing another dispatch). Without this guard,
    // a second call slips past the empty-input check before the first call
    // clears `store.prompt.input`, then awaits its own `session.create` and
    // ultimately reads the now-empty store — sending a phantom empty prompt
    // to a freshly created session.
    if (submitting) return false
    submitting = true
    try {
      return await submitInner()
    } finally {
      submitting = false
    }
  }

  async function submitInner() {
    workspace.clearNotice()

    // IME: double-defer may fire before onContentChange flushes the last
    // composed character (e.g. Korean hangul) to the store, so read
    // plainText directly and sync before any downstream reads.
    if (input && !input.isDestroyed && input.plainText !== store.prompt.input) {
      setStore("prompt", "input", input.plainText)
      syncExtmarksWithPromptParts()
    }
    if (props.disabled) return false
    if (workspace.creating() || move.creating()) return false
    if (auto()?.visible) return false
    if (!store.prompt.input) return false
    const agent = local.agent.current()
    if (!agent) return false
    const trimmed = store.prompt.input.trim()
    if (trimmed === "exit" || trimmed === "quit" || trimmed === ":q") {
      void exit()
      return true
    }
    const selectedModel = local.model.current()
    if (!selectedModel) {
      void promptModelWarning()
      return false
    }

    const workspaceSession = props.sessionID ? sync.session.get(props.sessionID) : undefined
    const workspaceID = workspaceSession?.workspaceID
    const workspaceStatus = workspaceID ? (project.workspace.status(workspaceID) ?? "error") : undefined
    if (props.sessionID && workspaceID && workspaceStatus !== "connected") {
      dialog.replace(() => (
        <DialogWorkspaceUnavailable
          onRestore={() => {
            workspace.open()
            return false
          }}
        />
      ))
      return false
    }

    const variant = local.model.variant.current()
    let sessionID = props.sessionID
    let finishMoveProgress = false
    if (sessionID == null) {
      const selectedWorkspace = workspace.selection()
      const workspaceID = selectedWorkspace?.type === "existing" ? selectedWorkspace.workspaceID : undefined

      const directory = await move.getDirectory(store.prompt.input)
      if (move.pending() && !directory) return false
      finishMoveProgress = Boolean(move.progress())

      const res = await sdk.client.session.create({
        directory,
        workspace: workspaceID,
        agent: agent.name,
        model: {
          providerID: selectedModel.providerID,
          id: selectedModel.modelID,
          variant,
        },
      })

      if (res.error) {
        if (finishMoveProgress) move.finishSubmit()
        console.log("Creating a session failed:", res.error)

        toast.show({
          message: "Creating a session failed. Open console for more details.",
          variant: "error",
        })

        return true
      }

      sessionID = res.data.id
    }

    const pasted = input.extmarks.getAllForTypeId(promptPartTypeId).flatMap((extmark) => {
      const partIndex = store.extmarkToPartIndex.get(extmark.id)
      const part = partIndex === undefined ? undefined : store.prompt.parts[partIndex]
      if (part?.type !== "text") return []
      return [{ start: extmark.start, end: extmark.end, text: part.text }]
    })
    const inputText = expandTrackedPastedText(store.prompt.input, pasted)

    // Filter out text parts (pasted content) since they're now expanded inline,
    // and mark each mention where the expanded text holds it, so a recall of
    // the queued prompt finds it there (#249).
    const nonTextParts = store.prompt.parts.flatMap((part): PromptInfo["parts"] => {
      const at = <At extends { start: number; end: number }>(mention: At) => ({
        ...mention,
        start: expandedOffset(mention.start, pasted),
        end: expandedOffset(mention.end, pasted),
      })
      if (part.type === "text") return []
      if (part.type === "agent") return [part.source ? { ...part, source: at(part.source) } : part]
      return [part.source ? { ...part, source: { ...part.source, text: at(part.source.text) } } : part]
    })

    // Capture mode before it gets reset
    const currentMode = store.mode
    const editorSelection = editorContext()
    const editorParts =
      editorSelection && editor.labelState() === "pending"
        ? [
            {
              type: "text" as const,
              text: formatEditorContext(editorSelection),
              synthetic: true,
              metadata: {
                kind: "editor_context",
                source: editorSelection.source ?? "editor",
                filePath: editorSelection.filePath,
                ranges: editorSelection.ranges,
              },
            },
          ]
        : []

    if (store.mode === "shell") {
      move.startSubmit()
      void sdk.client.session.shell({
        sessionID,
        agent: agent.name,
        model: {
          providerID: selectedModel.providerID,
          modelID: selectedModel.modelID,
        },
        command: inputText,
      })
      setStore("mode", "normal")
    } else if (
      inputText.startsWith("/") &&
      sync.data.command.some((x) => x.name === inputText.split("\n")[0].split(" ")[0].slice(1))
    ) {
      move.startSubmit()
      // Parse command from first line, preserve multi-line content in arguments
      const firstLineEnd = inputText.indexOf("\n")
      const firstLine = firstLineEnd === -1 ? inputText : inputText.slice(0, firstLineEnd)
      const [command, ...firstLineArgs] = firstLine.split(" ")
      const restOfInput = firstLineEnd === -1 ? "" : inputText.slice(firstLineEnd + 1)
      const args = firstLineArgs.join(" ") + (restOfInput ? "\n" + restOfInput : "")

      void sdk.client.session.command({
        sessionID,
        command: command.slice(1),
        arguments: args,
        agent: agent.name,
        model: `${selectedModel.providerID}/${selectedModel.modelID}`,
        variant,
        parts: nonTextParts.filter((x) => x.type === "file"),
      })
    } else {
      move.startSubmit()
      // Names this send, so the server can say what became of it (#212), and
      // so the pending row shows it until the queue lists it (#249).
      const requestID = crypto.randomUUID()
      const settled = sync.queue.sending(sessionID, {
        requestID,
        text: inputText,
        ...(store.prompt.machineAppended ? { machineAppended: true } : {}),
      })
      sdk.client.session
        .prompt(
          {
            sessionID,
            requestID,
            ...selectedModel,
            agent: agent.name,
            delivery: status().type === "idle" ? "steer" : "queue",
            origin: "tui-composer",
            model: selectedModel,
            variant,
            parts: [
              ...editorParts,
              {
                type: "text",
                text: inputText,
                ...(store.prompt.machineAppended
                  ? { metadata: { [SessionPromptQueue.TuiPromptAppendMetadataKey]: true } }
                  : {}),
              },
              ...nonTextParts,
            ],
          },
          { throwOnError: true },
        )
        .catch((error) => {
          // A withdrawn prompt was taken back on purpose, by a recall or a review.
          if (
            error instanceof Error &&
            isRecord(error.cause) &&
            isRecord(error.cause.body) &&
            error.cause.body._tag === "PromptWithdrawn"
          )
            return
          toast.show({
            title: "Failed to send prompt",
            message: errorMessage(error),
            variant: "error",
          })
        })
        .finally(settled)
      if (editorParts.length > 0) editor.markSelectionSent()
    }
    if (!store.prompt.machineAppended)
      history.append(sessionID, { ...store.prompt, mode: currentMode }, sync.data.config.machine_message_markers)
    input.extmarks.clear()
    setStore("prompt", {
      input: "",
      parts: [],
    })
    setStore("extmarkToPartIndex", new Map())
    props.onSubmit?.()

    // temporary hack to make sure the message is sent
    if (!props.sessionID) {
      if (editorParts.length > 0) editor.preserveSelectionFromNewSession()
      setTimeout(() => {
        route.navigate({
          type: "session",
          sessionID,
        })
      }, 50)
    }
    input.clear()
    if (finishMoveProgress) move.finishSubmit()
    return true
  }

  function pasteText(text: string, virtualText: string) {
    const currentOffset = input.cursorOffset
    const extmarkStart = currentOffset
    const extmarkEnd = extmarkStart + promptOffsetWidth(virtualText)

    input.insertText(virtualText + " ")

    const extmarkId = input.extmarks.create({
      start: extmarkStart,
      end: extmarkEnd,
      virtual: true,
      styleId: pasteStyleId,
      typeId: promptPartTypeId,
    })

    setStore(
      produce((draft) => {
        const partIndex = draft.prompt.parts.length
        draft.prompt.parts.push({
          type: "text" as const,
          text,
          source: {
            text: {
              start: extmarkStart,
              end: extmarkEnd,
              value: virtualText,
            },
          },
        })
        draft.extmarkToPartIndex.set(extmarkId, partIndex)
      }),
    )
  }

  async function pasteInputText(text: string) {
    const clearRevision = promptClearRevision
    const normalizedText = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n")
    const pastedContent = normalizedText.trim()
    const filepath = pastedFilepath(pastedContent, terminalEnvironment.platform)
    const isUrl = /^(https?):\/\//.test(filepath)
    if (!isUrl) {
      const attachment = await readLocalAttachment(filepath)
      // A clear while inspecting a pasted path must not reinsert the old draft.
      if (input.isDestroyed || promptClearRevision !== clearRevision) return
      const filename = path.basename(filepath)
      if (attachment?.type === "text") {
        pasteText(attachment.content, `[SVG: ${filename ?? "image"}]`)
        return
      }
      if (attachment?.type === "binary") {
        await pasteAttachment({
          filename,
          filepath,
          mime: attachment.mime,
          content: Buffer.from(attachment.content).toString("base64"),
        })
        return
      }
    }

    const lineCount = (pastedContent.match(/\n/g)?.length ?? 0) + 1
    if (
      (lineCount >= 3 || pastedContent.length > 150) &&
      kv.get("paste_summary_enabled", !sync.data.config.experimental?.disable_paste_summary)
    ) {
      pasteText(pastedContent, `[Pasted ~${lineCount} lines]`)
      return
    }

    input.insertText(normalizedText)

    setTimeout(() => {
      if (!input || input.isDestroyed) return
      input.getLayoutNode().markDirty()
      renderer.requestRender()
    }, 0)
  }

  async function pasteAttachment(file: { filename?: string; filepath?: string; content: string; mime: string }) {
    const currentOffset = input.cursorOffset
    const extmarkStart = currentOffset
    const pdf = file.mime === "application/pdf"
    const count = store.prompt.parts.filter((x) => {
      if (x.type !== "file") return false
      if (pdf) return x.mime === "application/pdf"
      return x.mime.startsWith("image/")
    }).length
    const virtualText = pdf ? `[PDF ${count + 1}]` : `[Image ${count + 1}]`
    const extmarkEnd = extmarkStart + virtualText.length
    const textToInsert = virtualText + " "

    input.insertText(textToInsert)

    const extmarkId = input.extmarks.create({
      start: extmarkStart,
      end: extmarkEnd,
      virtual: true,
      styleId: pasteStyleId,
      typeId: promptPartTypeId,
    })

    const part: Omit<FilePart, "id" | "messageID" | "sessionID"> = {
      type: "file" as const,
      mime: file.mime,
      filename: file.filename,
      url: `data:${file.mime};base64,${file.content}`,
      source: {
        type: "file",
        path: file.filepath ?? file.filename ?? "",
        text: {
          start: extmarkStart,
          end: extmarkEnd,
          value: virtualText,
        },
      },
    }
    setStore(
      produce((draft) => {
        const partIndex = draft.prompt.parts.length
        draft.prompt.parts.push(part)
        draft.extmarkToPartIndex.set(extmarkId, partIndex)
      }),
    )
    return
  }

  function clearPrompt() {
    promptClearRevision++
    input.extmarks.clear()
    setStore("prompt", {
      input: "",
      parts: [],
    })
    setStore("extmarkToPartIndex", new Map())
    input.setText("")
  }

  async function enhancePrompt() {
    if (!input || input.isDestroyed || store.mode !== "normal" || enhancing()) return
    const requestInput = input
    const sessionID = props.sessionID
    const revision = promptRevision
    const draft = requestInput.plainText
    if (!draft.trim()) return
    setStore("prompt", "input", draft)
    const requestParts = store.prompt.parts
    const requestPartsSnapshot = JSON.stringify(requestParts)
    const placeholders = requestParts.flatMap((part) => {
      if (part.type === "file" && part.source?.text) return [part.source.text.value]
      if (part.type === "agent" && part.source) return [part.source.value]
      if (part.type === "text" && part.source?.text) return [part.source.text.value]
      return []
    })
    setEnhancing(true)
    try {
      const transcript = props.enhancementContext?.()
      const context = transcript
        ? promptEnhancementContext(transcript.messages, transcript.options, transcript.presentation)
        : ""
      const currentModel = local.model.current()
      const response = await sdk.client.instance.prompt.enhance(
        {
          model:
            tuiConfig.prompt_enhance_model ??
            (currentModel ? `${currentModel.providerID}/${currentModel.modelID}` : "google/gemini-3.8-flash"),
          draft,
          context,
        },
        { throwOnError: true },
      )
      if (
        input !== requestInput ||
        requestInput.isDestroyed ||
        props.sessionID !== sessionID ||
        promptRevision !== revision ||
        requestInput.plainText !== draft ||
        JSON.stringify(store.prompt.parts) !== requestPartsSnapshot
      ) {
        toast.show({ message: "Prompt changed while enhancement was running", variant: "info" })
        return
      }
      const enhanced = response.data.trim()
      if (!enhanced) throw new Error("The enhancement model returned an empty draft.")
      if (placeholders.some((placeholder) => !enhanced.includes(placeholder)))
        throw new Error("The enhancement omitted an attachment or reference placeholder.")
      const parts = requestParts.map((part) => {
        const value =
          part.type === "file"
            ? part.source?.text?.value
            : part.type === "agent"
              ? part.source?.value
              : part.type === "text"
                ? part.source?.text?.value
                : undefined
        if (!value) return part
        const start = enhanced.indexOf(value)
        if (start === -1) return part
        const end = start + value.length
        if (part.type === "file" && part.source?.text)
          return { ...part, source: { ...part.source, text: { ...part.source.text, start, end } } }
        if (part.type === "agent" && part.source) return { ...part, source: { ...part.source, start, end } }
        if (part.type === "text" && part.source?.text)
          return { ...part, source: { ...part.source, text: { ...part.source.text, start, end } } }
        return part
      })
      input.setText(enhanced)
      setStore("prompt", { input: enhanced, parts })
      restoreExtmarksFromParts(parts)
      input.gotoBufferEnd()
      toast.show({ message: "Prompt enhanced", variant: "success" })
    } catch (error) {
      toast.show({ title: "Failed to enhance prompt", message: errorMessage(error), variant: "error" })
    } finally {
      setEnhancing(false)
    }
  }

  const highlight = createMemo(() => {
    if (leader()) return theme.border
    if (store.mode === "shell") return theme.primary
    const agent = local.agent.current()
    if (!agent) return theme.border
    return local.agent.color(agent.name)
  })

  const showVariant = createMemo(() => {
    const variants = local.model.variant.list()
    if (variants.length === 0) return false
    const current = local.model.variant.current()
    return !!current
  })

  const agentMetaAlpha = createFadeIn(() => !!local.agent.current(), animationsEnabled)
  const modelMetaAlpha = createFadeIn(() => !!local.agent.current() && store.mode === "normal", animationsEnabled)
  const variantMetaAlpha = createFadeIn(
    () => !!local.agent.current() && store.mode === "normal" && showVariant(),
    animationsEnabled,
  )
  const borderHighlight = createMemo(() => tint(theme.border, highlight(), agentMetaAlpha()))

  const placeholderText = createMemo(() => {
    if (props.showPlaceholder === false) return undefined
    if (store.mode === "shell") {
      if (!shell().length) return undefined
      const example = shell()[store.placeholder % shell().length]
      return `Run a command… "${example}"`
    }
    if (!list().length) return undefined
    return `Ask anything… "${list()[store.placeholder % list().length]}"`
  })

  const spinnerDef = createMemo(() => {
    const agent =
      status().type !== "idle"
        ? (local.agent.list().find((a) => a.name === lastUserMessage()?.agent) ?? local.agent.current())
        : local.agent.current()
    const color = agent ? local.agent.color(agent.name) : theme.border
    return {
      frames: createFrames({
        color,
        style: "blocks",
        inactiveFactor: 0.6,
        // enableFading: false,
        minAlpha: 0.3,
      }),
      color: createColors({
        color,
        style: "blocks",
        inactiveFactor: 0.6,
        // enableFading: false,
        minAlpha: 0.3,
      }),
    }
  })
  const maxHeight = createMemo(() => tuiConfig.prompt?.max_height ?? Math.max(6, Math.floor(dimensions().height / 3)))
  const moveLabelWidth = createMemo(() => Math.max(12, Math.min(44, dimensions().width - 48)))

  return (
    <>
      <box ref={(r: BoxRenderable) => (anchor = r)} visible={props.visible !== false} width="100%">
        <box
          width="100%"
          border={["left"]}
          borderColor={borderHighlight()}
          customBorderChars={{
            ...SplitBorder.customBorderChars,
            bottomLeft: "╹",
          }}
        >
          <box
            paddingLeft={2}
            paddingRight={2}
            paddingTop={1}
            flexShrink={0}
            backgroundColor={theme.backgroundElement}
            flexGrow={1}
            width="100%"
          >
            <textarea
              width="100%"
              placeholder={placeholderText()}
              placeholderColor={theme.textMuted}
              textColor={leader() ? theme.textMuted : theme.text}
              focusedTextColor={leader() ? theme.textMuted : theme.text}
              minHeight={1}
              maxHeight={maxHeight()}
              onContentChange={() => {
                promptRevision++
                const value = input.plainText
                if (!value && store.prompt.machineAppended) {
                  setStore(
                    "prompt",
                    produce((draft) => {
                      delete draft.machineAppended
                    }),
                  )
                }
                setStore("prompt", "input", value)
                auto()?.onInput(value)
                syncExtmarksWithPromptParts()
                setCursorVersion((value) => value + 1)
              }}
              onCursorChange={() => setCursorVersion((value) => value + 1)}
              onKeyDown={(e: { preventDefault(): void }) => {
                if (props.disabled) {
                  e.preventDefault()
                  return
                }
              }}
              onSubmit={() => {
                // IME: double-defer so the last composed character (e.g. Korean
                // hangul) is flushed to plainText before we read it for submission.
                setTimeout(() => setTimeout(() => submit(), 0), 0)
              }}
              onPaste={async (event: PasteEvent) => {
                if (props.disabled) {
                  event.preventDefault()
                  return
                }

                // Normalize line endings at the boundary
                // Windows ConPTY/Terminal often sends CR-only newlines in bracketed paste
                // Replace CRLF first, then any remaining CR
                const normalizedText = decodePasteBytes(event.bytes).replace(/\r\n/g, "\n").replace(/\r/g, "\n")
                const pastedContent = normalizedText.trim()

                // Windows Terminal <1.25 can surface image-only clipboard as an
                // empty bracketed paste. Windows Terminal 1.25+ does not.
                if (!pastedContent) {
                  keymap.dispatchCommand("prompt.paste")
                  return
                }

                // Once we cross an async boundary below, the terminal may perform its
                // default paste unless we suppress it first and handle insertion ourselves.
                event.preventDefault()

                await pasteInputText(normalizedText)
              }}
              ref={(r: TextareaRenderable) => {
                input = r
                Object.assign(r, {
                  getClipboardText: (text: string) => expandPastedTextPlaceholders(text, store.prompt.parts),
                })
                setInputTarget(r)
                if (promptPartTypeId === 0) {
                  promptPartTypeId = input.extmarks.registerType("prompt-part")
                }
                props.ref?.(ref)
                setTimeout(() => {
                  // setTimeout is a workaround and needs to be addressed properly
                  if (!input || input.isDestroyed) return
                  input.cursorColor = theme.text
                  if (tuiConfig.cursor) input.cursorStyle = tuiConfig.cursor
                }, 0)
              }}
              onMouseDown={(r: MouseEvent) => r.target?.focus()}
              focusedBackgroundColor={theme.backgroundElement}
              cursorColor={props.disabled ? theme.backgroundElement : theme.text}
              cursorStyle={tuiConfig.cursor}
              syntaxStyle={syntax()}
            />
            <box flexDirection="row" flexShrink={0} paddingTop={1} gap={1} justifyContent="space-between">
              <box flexDirection="row" gap={1}>
                <Show when={local.agent.current()} fallback={<box height={1} />}>
                  {(agent) => (
                    <>
                      <text fg={fadeColor(highlight(), agentMetaAlpha())}>
                        {store.mode === "shell" ? "Shell" : Locale.titlecase(agent().name)}
                      </text>
                      <Show when={store.mode === "normal" && local.permission.mode === "auto"}>
                        <text fg={fadeColor(theme.textMuted, agentMetaAlpha())}>auto</text>
                      </Show>
                      <Show when={store.mode === "normal"}>
                        <box flexDirection="row" gap={1}>
                          <text fg={fadeColor(theme.textMuted, modelMetaAlpha())}>·</text>
                          <text
                            flexShrink={0}
                            fg={fadeColor(leader() ? theme.textMuted : theme.text, modelMetaAlpha())}
                          >
                            {local.model.parsed().model}
                          </text>
                          <text fg={fadeColor(theme.textMuted, modelMetaAlpha())}>{currentProviderLabel()}</text>
                          <Show when={showVariant()}>
                            <text fg={fadeColor(theme.textMuted, variantMetaAlpha())}>·</text>
                            <text>
                              <span style={{ fg: fadeColor(theme.warning, variantMetaAlpha()), bold: true }}>
                                {local.model.variant.current()}
                              </span>
                            </text>
                          </Show>
                        </box>
                      </Show>
                    </>
                  )}
                </Show>
              </box>
              <Show when={hasRightContent()}>
                <box flexDirection="row" gap={1} alignItems="center">
                  {props.right}
                </box>
              </Show>
            </box>
          </box>
        </box>
        <box
          height={1}
          border={["left"]}
          borderColor={borderHighlight()}
          customBorderChars={{
            ...EmptyBorder,
            vertical: theme.backgroundElement.a !== 0 ? "╹" : " ",
          }}
        >
          <box
            height={1}
            border={["bottom"]}
            borderColor={theme.backgroundElement}
            customBorderChars={
              theme.backgroundElement.a !== 0
                ? {
                    ...EmptyBorder,
                    horizontal: "▀",
                  }
                : {
                    ...EmptyBorder,
                    horizontal: " ",
                  }
            }
          />
        </box>
        <box width="100%" flexDirection="row" justifyContent="space-between">
          <Switch>
            <Match when={status().type !== "idle"}>
              <box
                flexDirection="row"
                gap={1}
                flexGrow={1}
                justifyContent={status().type === "retry" ? "space-between" : "flex-start"}
              >
                <box flexShrink={0} flexDirection="row" gap={1}>
                  <box marginLeft={1}>
                    <Show when={kv.get("animations_enabled", true)} fallback={<text fg={theme.textMuted}>[⋯]</text>}>
                      <spinner color={spinnerDef().color} frames={spinnerDef().frames} interval={40} />
                    </Show>
                  </box>
                  <box flexDirection="row" gap={1} flexShrink={0}>
                    {(() => {
                      const retry = createMemo(() => {
                        const s = status()
                        if (s.type !== "retry") return
                        return s
                      })
                      const message = createMemo(() => {
                        const r = retry()
                        if (!r) return
                        if (r.message.includes("exceeded your current quota") && r.message.includes("gemini"))
                          return "gemini is way too hot right now"
                        if (r.message.length > 80) return r.message.slice(0, 80) + "…"
                        return r.message
                      })
                      const isTruncated = createMemo(() => {
                        const r = retry()
                        if (!r) return false
                        return r.message.length > 120
                      })
                      const [seconds, setSeconds] = createSignal(0)
                      onMount(() => {
                        const timer = setInterval(() => {
                          const next = retry()?.next
                          if (next) setSeconds(Math.round((next - Date.now()) / 1000))
                        }, 1000)

                        onCleanup(() => {
                          clearInterval(timer)
                        })
                      })
                      const handleMessageClick = () => {
                        const r = retry()
                        if (!r) return
                        if (isTruncated()) {
                          void DialogAlert.show(dialog, "Retry Error", r.message)
                        }
                      }

                      const retryText = () => {
                        const r = retry()
                        if (!r) return ""
                        const baseMessage = message()
                        const truncatedHint = isTruncated() ? " (click to expand)" : ""
                        const duration = formatDuration(seconds())
                        const retryInfo = ` [retrying ${duration ? `in ${duration} ` : ""}attempt #${r.attempt}]`
                        return baseMessage + truncatedHint + retryInfo
                      }

                      return (
                        <Show when={retry()}>
                          <box onMouseUp={handleMessageClick}>
                            <text fg={theme.error}>{retryText()}</text>
                          </box>
                        </Show>
                      )
                    })()}
                  </box>
                </box>
                <text fg={store.interrupt > 0 ? theme.primary : theme.text}>
                  esc{" "}
                  <span style={{ fg: store.interrupt > 0 ? theme.primary : theme.textMuted }}>
                    {store.interrupt > 0 ? "again to interrupt" : "interrupt"}
                  </span>
                </text>
              </box>
            </Match>
            <Match when={workspace.notice()}>
              {(notice) => (
                <box paddingLeft={3}>
                  <text fg={theme.accent}>{notice()}</text>
                </box>
              )}
            </Match>
            <Match when={workspace.label()}>
              {(label) => (
                <box paddingLeft={3} flexDirection="row" gap={1}>
                  <Show when={workspace.creating()}>
                    <Spinner color={theme.accent} />
                  </Show>
                  <text fg={workspace.creating() ? theme.accent : theme.text}>
                    {(() => {
                      const item = label()
                      if (item.type === "new") {
                        if (workspace.creating())
                          return `Creating ${item.workspaceType}${".".repeat(workspace.creatingDots())}`
                        return (
                          <>
                            Workspace <span style={{ fg: theme.textMuted }}>(new {item.workspaceType})</span>
                          </>
                        )
                      }
                      return (
                        <>
                          Workspace <span style={{ fg: theme.textMuted }}>{item.workspaceName}</span>
                        </>
                      )
                    })()}
                  </text>
                </box>
              )}
            </Match>
            <Match when={move.progress()}>
              {(progress) => (
                <box paddingLeft={3}>
                  <Spinner color={theme.accent}>
                    {progress()}
                    <span style={{ fg: theme.textMuted }}>{".".repeat(move.creatingDots())}</span>
                  </Spinner>
                </box>
              )}
            </Match>
            <Match when={move.pendingNew()}>
              <box paddingLeft={3}>
                <text fg={theme.accent}>(new working copy)</text>
              </box>
            </Match>
            <Match when={true}>
              {props.hint ?? (
                <Show when={props.sessionID} fallback={<text />}>
                  <box marginLeft={1}>
                    <text fg={theme.textMuted}>{location()?.directory ?? paths.cwd}</text>
                  </box>
                </Show>
              )}
            </Match>
          </Switch>
          <Show when={status().type !== "retry"}>
            <box gap={2} flexDirection="row">
              <Show when={editorContextLabelState() !== "none" ? editorFileLabelDisplay() : undefined}>
                {(file) => (
                  <text fg={editorContextLabelState() === "pending" ? theme.secondary : theme.textMuted}>{file()}</text>
                )}
              </Show>
              <Switch>
                <Match when={store.mode === "normal"}>
                  <Switch>
                    <Match when={usage()}>
                      {(item) => (
                        <text fg={theme.textMuted} wrapMode="none">
                          {[item().context, item().cost].filter(Boolean).join(" · ")}
                        </text>
                      )}
                    </Match>
                    <Match when={true}>
                      <text fg={theme.text}>
                        {agentShortcut()} <span style={{ fg: theme.textMuted }}>agents</span>
                      </text>
                    </Match>
                  </Switch>
                  <text fg={theme.text}>
                    {paletteShortcut()} <span style={{ fg: theme.textMuted }}>commands</span>
                  </text>
                </Match>
                <Match when={store.mode === "shell"}>
                  <text fg={theme.text}>
                    esc <span style={{ fg: theme.textMuted }}>exit shell mode</span>
                  </text>
                </Match>
              </Switch>
            </box>
          </Show>
        </box>
        <PromptQueueRow sessionID={props.sessionID} />
      </box>
      <Autocomplete
        sessionID={props.sessionID}
        ref={(r) => {
          setAuto(() => r)
        }}
        anchor={() => anchor}
        input={() => input}
        setPrompt={(cb) => {
          setStore("prompt", produce(cb))
        }}
        setExtmark={(partIndex, extmarkId) => {
          setStore("extmarkToPartIndex", (map: Map<number, number>) => {
            const newMap = new Map(map)
            newMap.set(extmarkId, partIndex)
            return newMap
          })
        }}
        value={store.prompt.input}
        fileStyleId={fileStyleId}
        agentStyleId={agentStyleId}
        promptPartTypeId={() => promptPartTypeId}
      />
    </>
  )
}
