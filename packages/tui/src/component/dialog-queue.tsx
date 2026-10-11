import { useKeyboard } from "@opentui/solid"
import type { SessionPromptQueueItem } from "@opencode-ai/sdk/v2"
import { createMemo, onCleanup, onMount } from "solid-js"
import { useSDK } from "../context/sdk"
import { useSync } from "../context/sync"
import { useDialog, type DialogContext } from "../ui/dialog"
import { DialogSelect } from "../ui/dialog-select"
import { Locale } from "../util/locale"

// The review view of a session's pending prompts (swxtchio/swx-opencode#212).
// While it is open the server holds every promotion for the session, so the
// list stays as shown; closing it, by any route, releases the hold.

/** Without user activity for this long, the view closes itself and releases its hold. */
export const REVIEW_IDLE_MS = 5 * 60 * 1000
// The hold is a server lease, renewed on activity at most this often. It runs
// a minute longer than REVIEW_IDLE_MS, so a renewal always lands in time.
const RENEW_MS = 30 * 1000

export function DialogQueue(props: { sessionID: string }) {
  const dialog = useDialog()
  const sdk = useSDK()
  const sync = useSync()
  const holder = `tui_${crypto.randomUUID()}`
  const items = createMemo(() => sync.data.session_queue[props.sessionID] ?? [])
  // Hold requests and the release go out in order, so a quick close can never
  // release before the hold it ends has landed.
  let requests = Promise.resolve()
  const send = (request: () => Promise<unknown>) => {
    requests = requests.then(request).then(
      () => undefined,
      () => undefined,
    )
  }
  const hold = () => send(() => sdk.client.session.queue.hold({ sessionID: props.sessionID, holder }))
  let closed = false
  let renewed = Date.now()
  let idle: ReturnType<typeof setTimeout> | undefined

  const active = () => {
    if (closed) return
    clearTimeout(idle)
    idle = setTimeout(() => dialog.clear(), REVIEW_IDLE_MS)
    if (Date.now() - renewed < RENEW_MS) return
    renewed = Date.now()
    hold()
  }

  onMount(() => {
    dialog.setSize("large")
    hold()
    active()
  })
  onCleanup(() => {
    closed = true
    clearTimeout(idle)
    send(() => sdk.client.session.queue.release({ sessionID: props.sessionID, holder }))
  })
  useKeyboard(active)

  // Steers always promote before queued prompts, so an item moves only past a
  // neighbour of its own category.
  const move = (id: string, step: -1 | 1) => {
    const list = items()
    const index = list.findIndex((item) => item.id === id)
    const neighbour = list[index + step]
    if (index < 0 || !neighbour || neighbour.delivery !== list[index]!.delivery) return
    const order = list.map((item) => item.id)
    order[index] = neighbour.id
    order[index + step] = id
    void sdk.client.session.queue.reorder({
      sessionID: props.sessionID,
      items: order.filter((_, position) => list[position]!.delivery === neighbour.delivery),
    })
  }

  const options = createMemo(() =>
    items().map((item) => {
      const lines = preview(item).split("\n")
      return {
        title: Locale.truncate(lines[0]!.trim(), 60),
        value: item.id,
        description: item.delivery,
        footer: lines.length > 1 ? `+${lines.length - 1} lines` : undefined,
      }
    }),
  )

  return (
    <DialogSelect
      title="Queued prompts (delivery paused)"
      options={options()}
      renderFilter={false}
      preserveSelection
      emptyView={<text>Nothing is waiting</text>}
      onMove={active}
      actions={[
        {
          command: "dialog.queue.delete",
          title: "delete",
          onTrigger: (option) => {
            active()
            void sdk.client.session.queue.remove({ sessionID: props.sessionID, id: option.value })
          },
        },
        {
          command: "dialog.queue.move_up",
          title: "earlier",
          onTrigger: (option) => {
            active()
            move(option.value, -1)
          },
        },
        {
          command: "dialog.queue.move_down",
          title: "later",
          onTrigger: (option) => {
            active()
            move(option.value, 1)
          },
        },
        {
          command: "dialog.queue.consolidate",
          title: "consolidate",
          onTrigger: () => {
            active()
            void sdk.client.session.queue.consolidate({ sessionID: props.sessionID })
          },
        },
      ]}
    />
  )
}

DialogQueue.show = (dialog: DialogContext, sessionID: string) =>
  dialog.replace(() => <DialogQueue sessionID={sessionID} />)

// What a pending item will say: its typed text, or the kinds of its parts.
function preview(item: SessionPromptQueueItem) {
  const text = item.input.parts.flatMap((part) => (part.type === "text" && !part.synthetic ? [part.text] : []))
  if (text.length > 0) return text.join("\n")
  return item.input.parts.map((part) => `[${part.type}]`).join(" ")
}
