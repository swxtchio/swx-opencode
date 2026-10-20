import { useKeyboard } from "@opentui/solid"
import type { SessionPromptQueueItem } from "@opencode-ai/sdk/v2"
import { createMemo, createSignal, onCleanup, onMount } from "solid-js"
import { useSDK } from "../context/sdk"
import { useSync } from "../context/sync"
import { useDialog, type DialogContext } from "../ui/dialog"
import { DialogSelect } from "../ui/dialog-select"
import { useToast } from "../ui/toast"
import { Locale } from "../util/locale"
import { promptEnhancementContext, promptEnhancementModel, type PromptEnhancement } from "../util/transcript"

// The review view of a session's pending prompts (swxtchio/swx-opencode#212).
// While it is open the server holds every promotion for the session, so the
// list stays as shown; closing it, by any route, releases the hold.

/** Without user activity for this long, the view closes itself and releases its hold. */
export const REVIEW_IDLE_MS = 5 * 60 * 1000
// The hold is a server lease, renewed on activity at most this often. It runs
// a minute longer than REVIEW_IDLE_MS, so a renewal always lands in time.
const RENEW_MS = 30 * 1000
// No hold or release request waits longer than this for its response.
const REQUEST_MS = 10 * 1000

export function DialogQueue(props: { sessionID: string; enhancement?: PromptEnhancement }) {
  const dialog = useDialog()
  const sdk = useSDK()
  const sync = useSync()
  const toast = useToast()
  const holder = `tui_${crypto.randomUUID()}`
  const items = createMemo(() => sync.data.session_queue[props.sessionID] ?? [])
  // Delivery is paused only while the server confirms the hold: a renewal it
  // refuses may let the lease lapse, so the view says so and locks its edits
  // until a later renewal is confirmed.
  const [hold, setHold] = createSignal<"pending" | "held" | "failed">("pending")
  const [enhancing, setEnhancing] = createSignal(false)
  let closed = false
  let renewed = Date.now()
  let idle: ReturnType<typeof setTimeout> | undefined

  const checked = async <R extends { error?: unknown; response?: Response }>(call: Promise<R>) => {
    const result = await call
    if (result.error !== undefined) throw result.error
    if (result.response && !result.response.ok)
      throw new Error(`${result.response.status} ${result.response.statusText}`)
    return result
  }
  const failed = (action: string) => (error: unknown) =>
    toast.show({
      variant: "error",
      message: `Could not ${action} the queued prompts: ${error instanceof Error ? error.message : String(error)}`,
    })
  const signal = () => AbortSignal.timeout(REQUEST_MS)

  const release = () =>
    void checked(sdk.client.session.queue.release({ sessionID: props.sessionID, holder }, { signal: signal() })).catch(
      failed("resume delivery of"),
    )

  // A close releases at once, and again as each hold request still in flight
  // settles, so no slow response delays the release and a hold the server
  // accepts after the close never outlives it.
  const request = (renewal: boolean) =>
    void checked(sdk.client.session.queue.hold({ sessionID: props.sessionID, holder }, { signal: signal() }))
      .then((result) => {
        if (closed) return
        setHold("held")
        if (result.data) sync.queue.listed(props.sessionID, result.data)
      })
      .catch((error) => {
        if (closed) return
        setHold("failed")
        failed(renewal ? "keep pausing delivery of" : "pause delivery of")(error)
      })
      .finally(() => {
        if (closed) release()
      })

  const active = () => {
    if (closed) return
    clearTimeout(idle)
    idle = setTimeout(() => dialog.clear(), REVIEW_IDLE_MS)
    if (Date.now() - renewed < RENEW_MS) return
    renewed = Date.now()
    request(true)
  }

  onMount(() => {
    dialog.setSize("large")
    request(false)
    active()
  })
  onCleanup(() => {
    closed = true
    clearTimeout(idle)
    release()
  })
  useKeyboard(active)

  // Steers always promote before queued prompts, so an item moves only past a
  // neighbour of its own category.
  const move = (id: string, step: -1 | 1) => {
    const list = items()
    const index = list.findIndex((item) => item.id === id)
    const neighbour = list[index + step]
    if (index < 0) {
      toast.show({ variant: "info", message: "That queued prompt is no longer available." })
      return
    }
    if (!neighbour) {
      toast.show({
        variant: "info",
        message:
          step < 0
            ? "This prompt is already earliest in its delivery group."
            : "This prompt is already latest in its delivery group.",
      })
      return
    }
    if (neighbour.delivery !== list[index]!.delivery) {
      toast.show({ variant: "info", message: "Prompts cannot move across delivery types." })
      return
    }
    const order = list.map((item) => item.id)
    order[index] = neighbour.id
    order[index + step] = id
    void checked(
      sdk.client.session.queue.reorder({
        sessionID: props.sessionID,
        items: order.filter((_, position) => list[position]!.delivery === neighbour.delivery),
      }),
    ).catch(failed("reorder"))
  }

  // Ages count on while the view is open.
  const [now, setNow] = createSignal(Date.now())
  const tick = setInterval(() => setNow(Date.now()), 30 * 1000)
  onCleanup(() => clearInterval(tick))

  const options = createMemo(() =>
    items().map((item) => {
      const lines = preview(item).split("\n")
      return {
        title: Locale.truncate(lines[0]!.trim(), 60),
        value: item.id,
        description: [item.delivery, ...freshness(item, now())].join(" · "),
        details: lines.slice(1),
        footer: lines.length > 1 ? `+${lines.length - 1} lines` : undefined,
      }
    }),
  )

  // The list may still change until the hold is confirmed, so it is edited only then.
  const locked = () => hold() !== "held"
  const explainLocked = () => {
    active()
    toast.show({
      variant: "warning",
      message:
        hold() === "pending"
          ? "Delivery is not paused yet; wait for the hold to be confirmed."
          : "Delivery is NOT paused because the hold failed.",
    })
  }
  const explainUnavailable = (message: string) => () => {
    active()
    toast.show({ variant: "info", message })
  }

  // Mouse use anywhere on the view, scrolling included, counts as activity.
  return (
    <box onMouse={active}>
      <DialogSelect
        title={
          hold() === "held"
            ? "Queued prompts (delivery paused)"
            : hold() === "pending"
              ? "Queued prompts (pausing delivery…)"
              : "Queued prompts (delivery NOT paused)"
        }
        options={options()}
        renderFilter={false}
        preserveSelection
        emptyView={<text>Nothing is waiting</text>}
        onMove={active}
        actions={[
          {
            command: "dialog.queue.force",
            title: "force",
            disabled: locked,
            onDisabledTrigger: explainLocked,
            onUnavailableTrigger: explainUnavailable("No queued prompt is selected."),
            onTrigger: (option) => {
              active()
              void checked(
                sdk.client.session.queue.force(
                  { sessionID: props.sessionID, itemID: option.value, holder },
                  { signal: signal() },
                ),
              )
                .then((result) => {
                  const data = result.data
                  if (!data) {
                    toast.show({ variant: "error", message: "The force request returned no queue state." })
                    return
                  }
                  sync.queue.listed(props.sessionID, data.items)
                  if (data.outcome === "forced") {
                    toast.show({ variant: "info", message: "The selected prompt will reach the next safe step." })
                    dialog.clear()
                    return
                  }
                  if (data.outcome === "another-review") {
                    toast.show({
                      variant: "warning",
                      message: "Another review is holding delivery; the selected prompt was not forced.",
                    })
                    return
                  }
                  if (data.outcome === "hold-unconfirmed") {
                    setHold("failed")
                    toast.show({
                      variant: "warning",
                      message: "The review hold is no longer confirmed; the selected prompt was not forced.",
                    })
                    return
                  }
                  if (data.outcome === "preparing") {
                    toast.show({
                      variant: "warning",
                      message: "This prompt is still preparing; it was not forced.",
                    })
                    return
                  }
                  toast.show({
                    variant: "info",
                    message: "The selected prompt is no longer pending; it was not forced.",
                  })
                })
                .catch(failed("force"))
            },
          },
          {
            command: "dialog.queue.enhance",
            title: "enh",
            side: "right",
            disabled: () => locked() || enhancing(),
            onDisabledTrigger: () => {
              if (locked()) {
                explainLocked()
                return
              }
              active()
              toast.show({ variant: "info", message: "A queued prompt enhancement is already running." })
            },
            onUnavailableTrigger: explainUnavailable("No queued prompt is selected."),
            onTrigger: (option) => {
              active()
              const selected = items().find((item) => item.id === option.value)
              if (!selected) {
                toast.show({ variant: "info", message: "That queued prompt is no longer available." })
                return
              }
              const draft = selected.input.parts
                .flatMap((part) => (part.type === "text" && !part.synthetic && !part.ignored ? [part.text] : []))
                .join("")
              if (!draft.trim()) {
                toast.show({ variant: "info", message: "The selected prompt has no text to enhance." })
                return
              }
              const placeholders = selected.input.parts.flatMap((part) => {
                if (part.type === "file" && part.source?.text) return [part.source.text.value]
                if (part.type === "agent" && part.source) return [part.source.value]
                return []
              })
              const transcript = props.enhancement?.context?.()
              setEnhancing(true)
              void checked(
                sdk.client.instance.prompt.enhance({
                  model: props.enhancement?.model() ?? promptEnhancementModel(undefined, undefined),
                  draft,
                  context: transcript
                    ? promptEnhancementContext(transcript.messages, transcript.options, transcript.presentation)
                    : "",
                }),
              )
                .then((result) => {
                  const text = result.data?.trim()
                  if (!text) throw new Error("The enhancement model returned an empty draft.")
                  if (placeholders.some((placeholder) => !text.includes(placeholder)))
                    throw new Error("The enhancement omitted an attachment or reference placeholder.")
                  return checked(
                    sdk.client.session.queue.rewrite({
                      sessionID: props.sessionID,
                      itemID: selected.id,
                      expectedText: draft,
                      text,
                    }),
                  )
                })
                .then((result) => {
                  const data = result.data
                  if (!data) {
                    toast.show({ variant: "error", message: "The rewrite request returned no queue state." })
                    return
                  }
                  sync.queue.listed(props.sessionID, data.items)
                  if (data.outcome === "rewritten") {
                    toast.show({ variant: "success", message: "Queued prompt enhanced." })
                    return
                  }
                  if (data.outcome === "stale") {
                    toast.show({ variant: "warning", message: "The selected prompt changed; no rewrite was applied." })
                    return
                  }
                  if (data.outcome === "preparing") {
                    toast.show({ variant: "warning", message: "The selected prompt is still preparing." })
                    return
                  }
                  if (data.outcome === "unavailable") {
                    toast.show({ variant: "info", message: "The selected prompt is no longer pending." })
                    return
                  }
                  if (data.outcome === "no-text") {
                    toast.show({ variant: "info", message: "The selected prompt has no text to enhance." })
                    return
                  }
                  if (data.outcome === "empty") {
                    toast.show({ variant: "warning", message: "The enhancement returned empty text." })
                    return
                  }
                  toast.show({
                    variant: "warning",
                    message: "The enhancement omitted an attachment or reference placeholder.",
                  })
                })
                .catch(failed("enhance"))
                .finally(() => setEnhancing(false))
            },
          },
          {
            command: "dialog.queue.delete",
            title: "delete",
            disabled: locked,
            onDisabledTrigger: explainLocked,
            onUnavailableTrigger: explainUnavailable("No queued prompt is selected."),
            onTrigger: (option) => {
              active()
              void checked(sdk.client.session.queue.remove({ sessionID: props.sessionID, id: option.value })).catch(
                failed("delete one of"),
              )
            },
          },
          {
            command: "dialog.queue.move_up",
            title: "earlier",
            disabled: locked,
            onDisabledTrigger: explainLocked,
            onUnavailableTrigger: explainUnavailable("No queued prompt is selected."),
            onTrigger: (option) => {
              active()
              move(option.value, -1)
            },
          },
          {
            command: "dialog.queue.move_down",
            title: "later",
            disabled: locked,
            onDisabledTrigger: explainLocked,
            onUnavailableTrigger: explainUnavailable("No queued prompt is selected."),
            onTrigger: (option) => {
              active()
              move(option.value, 1)
            },
          },
          {
            command: "dialog.queue.consolidate",
            title: "consolidate",
            disabled: locked,
            onDisabledTrigger: explainLocked,
            onUnavailableTrigger: explainUnavailable("Nothing to consolidate."),
            onTrigger: (option) => {
              active()
              void checked(
                sdk.client.session.queue.consolidateSelected({
                  sessionID: props.sessionID,
                  itemID: option.value,
                }),
              )
                .then((result) => {
                  const data = result.data
                  if (!data) {
                    toast.show({ variant: "error", message: "The consolidate request returned no queue state." })
                    return
                  }
                  sync.queue.listed(props.sessionID, data.items)
                  if (data.outcome === "consolidated") return
                  if (data.outcome === "preparing") {
                    toast.show({ variant: "warning", message: "A human prompt is still preparing; nothing changed." })
                    return
                  }
                  if (data.outcome === "incompatible") {
                    toast.show({
                      variant: "warning",
                      message: "Human prompts with different delivery or execution context cannot be consolidated.",
                    })
                    return
                  }
                  if (data.outcome === "unavailable") {
                    toast.show({ variant: "info", message: "The selected prompt is no longer available." })
                    return
                  }
                  toast.show({ variant: "info", message: "Nothing to consolidate." })
                })
                .catch(failed("consolidate"))
            },
          },
        ]}
      />
    </box>
  )
}

DialogQueue.show = (dialog: DialogContext, sessionID: string, enhancement?: PromptEnhancement) =>
  dialog.replace(() => <DialogQueue sessionID={sessionID} enhancement={enhancement} />)

// When the item's oldest instruction was queued and how long ago, how often
// identical copies were sent, and how many queued instructions it merges.
function freshness(item: SessionPromptQueueItem, now: number) {
  const first = Math.min(...item.origins.map((origin) => origin.time))
  const sent = item.origins.length === 1 ? 1 + (item.origins[0]!.repeats?.length ?? 0) : 0
  return [
    `queued ${Locale.todayTimeOrDateTime(first)}`,
    `${age(now - first)} ago`,
    ...(sent > 1 ? [`sent ${sent}×`] : []),
    ...(item.origins.length > 1 ? [`${item.origins.length} merged`] : []),
  ]
}

function age(ms: number) {
  const minutes = Math.floor(Math.max(0, ms) / 60000)
  if (minutes < 1) return "<1m"
  if (minutes < 60) return `${minutes}m`
  if (minutes < 1440) return `${Math.floor(minutes / 60)}h ${minutes % 60}m`
  return `${Math.floor(minutes / 1440)}d ${Math.floor((minutes % 1440) / 60)}h`
}

// What a pending item will say: its typed text, or the kinds of its parts.
export function preview(item: SessionPromptQueueItem) {
  const text = item.input.parts.flatMap((part) =>
    part.type === "text" && !part.synthetic && part.text.length > 0 ? [part.text] : [],
  )
  if (text.length > 0) return text.join("\n")
  return item.input.parts.map((part) => `[${part.type}]`).join(" ")
}
