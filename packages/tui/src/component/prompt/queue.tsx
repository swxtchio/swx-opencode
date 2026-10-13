import { createMemo, Show } from "solid-js"
import { useSync } from "../../context/sync"
import { useTheme } from "../../context/theme"
import { useCommandShortcut } from "../../keymap"
import { useDialog } from "../../ui/dialog"
import { DialogQueue } from "../dialog-queue"

// The row under the prompt counting the session's pending prompts by category
// (swxtchio/swx-opencode#212). They are not messages yet, so the transcript
// cannot show them; activating the row opens their review view.
export function PromptQueueRow(props: { sessionID?: string }) {
  const sync = useSync()
  const dialog = useDialog()
  const { theme } = useTheme()
  const shortcut = useCommandShortcut("session.queued_prompts")
  const items = createMemo(() => (props.sessionID ? (sync.data.session_queue[props.sessionID] ?? []) : []))
  const steer = createMemo(() => items().filter((item) => item.delivery === "steer").length)

  return (
    <Show when={props.sessionID && items().length > 0 ? props.sessionID : undefined}>
      {(sessionID) => (
        <box paddingLeft={1} flexDirection="row" onMouseUp={() => DialogQueue.show(dialog, sessionID())}>
          <text fg={theme.warning} wrapMode="none">
            {items().length} pending{" "}
            <span style={{ fg: theme.textMuted }}>
              · {steer()} steer · {items().length - steer()} queue · {shortcut() ? `${shortcut()} ` : ""}review
            </span>
          </text>
        </box>
      )}
    </Show>
  )
}
