import type { SessionPromptQueueItem } from "@opencode-ai/sdk/v2"
import { SessionPromptQueue } from "@opencode-ai/schema/session-prompt-queue"
import { createMemo, For, Show } from "solid-js"
import { useSync } from "../../context/sync"
import { selectedForeground, useTheme } from "../../context/theme"
import { useCommandShortcut } from "../../keymap"
import { useDialog } from "../../ui/dialog"
import { Locale } from "../../util/locale"
import type { PromptEnhancement } from "../../util/transcript"
import { DialogQueue, preview } from "../dialog-queue"

// Only the most recently sent pending prompts are listed under the prompt; the
// review view shows them all.
const SHOWN = 3

// A session's pending prompts as the user sees them, oldest sent first: the
// queue's items by when each was last sent, then prompts this client is still
// sending, which the queue has not listed yet (#249). Delivery order differs,
// since pending steers go before queued prompts; the review view shows that.
// `item` is set only on what the queue holds, which alone can be withdrawn;
// `requestID` names a send still on its way to the queue.
export type PendingPrompt = { text: string; item?: SessionPromptQueueItem; requestID?: string; machineAppended?: true }

export function pendingPrompts(sync: ReturnType<typeof useSync>, sessionID: string | undefined): PendingPrompt[] {
  if (!sessionID) return []
  return [
    ...(sync.data.session_queue[sessionID] ?? [])
      .toSorted((a, b) => a.time.created - b.time.created || a.seq - b.seq)
      .map((item) => ({
        text: preview(item),
        item,
        ...(item.input.origin !== "tui-composer" ||
        item.input.parts.some(
          (part) => part.type === "text" && part.metadata?.[SessionPromptQueue.TuiPromptAppendMetadataKey] === true,
        )
          ? { machineAppended: true as const }
          : {}),
      })),
    ...(sync.data.session_sending[sessionID] ?? []).map((prompt) => ({
      text: prompt.text,
      requestID: prompt.requestID,
      ...(prompt.machineAppended ? { machineAppended: true as const } : {}),
    })),
  ]
}

// The session's pending prompts under the prompt, each marked QUEUED from the
// moment it is sent until it becomes a message (swxtchio/swx-opencode#212,
// #249). They are not messages yet, so the transcript cannot show them;
// activating the row opens their review view.
export function PromptQueueRow(props: { sessionID?: string; enhancement?: PromptEnhancement }) {
  const sync = useSync()
  const dialog = useDialog()
  const { theme } = useTheme()
  const shortcut = useCommandShortcut("session.queued_prompts")
  const prompts = createMemo(() => pendingPrompts(sync, props.sessionID))
  // A prompt still being sent goes as a steer, the default.
  const queued = createMemo(() => prompts().filter((prompt) => prompt.item?.delivery === "queue").length)

  return (
    <Show when={props.sessionID && prompts().length > 0 ? props.sessionID : undefined}>
      {(sessionID) => (
        <box
          paddingLeft={1}
          flexDirection="column"
          onMouseUp={() => DialogQueue.show(dialog, sessionID(), props.enhancement)}
        >
          <Show when={prompts().length > SHOWN}>
            <text fg={theme.textMuted} wrapMode="none">
              … {prompts().length - SHOWN} sent earlier
            </text>
          </Show>
          <For each={prompts().slice(-SHOWN)}>
            {(prompt) => (
              <text fg={theme.text} wrapMode="none">
                <span style={{ bg: theme.warning, fg: selectedForeground(theme, theme.warning), bold: true }}>
                  {" QUEUED "}
                </span>{" "}
                {Locale.truncate(prompt.text.split("\n")[0]!.trim(), 100)}
              </text>
            )}
          </For>
          <text fg={theme.warning} wrapMode="none">
            {prompts().length} pending{" "}
            <span style={{ fg: theme.textMuted }}>
              · {prompts().length - queued()} steer · {queued()} queue · {shortcut() ? `${shortcut()} ` : ""}review
            </span>
          </text>
        </box>
      )}
    </Show>
  )
}
