import { base64Encode } from "@opencode-ai/core/util/encode"
import { expect, test, type Page } from "@playwright/test"
import { mockOpenCodeServer } from "../utils/mock-server"
import { expectSessionTitle } from "../utils/waits"

const directory = "C:/OpenCode/TodoDockNavigation"
const projectID = "proj_todo_dock_navigation"
const sourceID = "ses_todo_dock_source"
const otherID = "ses_todo_dock_other"
const sourceTitle = "Todo dock animation"
const otherTitle = "Separate session"

const activeTodos = [
  { id: "todo-1", content: "Receive todos in the active session", status: "completed", priority: "high" },
  { id: "todo-2", content: "Keep the dock visible across tabs", status: "completed", priority: "high" },
  { id: "todo-3", content: "Close after the final todo", status: "in_progress", priority: "high" },
]

type EventPayload = {
  directory: string
  payload: Record<string, unknown>
}

test.use({ viewport: { width: 1440, height: 900 }, reducedMotion: "no-preference" })

/**
 * How long to watch for the dock after switching back to its tab.
 *
 * Has to comfortably outlast the tab switch plus a render, or the window closes
 * before the dock reappears and the assertion sees no samples at all.
 */
const DOCK_SAMPLE_BUDGET_MS = 8_000

/**
 * How long to watch for a dock that should NOT come back.
 *
 * Absence is a claim about an interval rather than an event to wait for, so
 * this one stays a bounded window. The "appears" condition still applies, so a
 * dock that wrongly returns ends the sampling immediately and fails the
 * assertion rather than costing the full budget.
 */
const DOCK_ABSENCE_WINDOW_MS = 1_500

test("animates todo lifecycle without replaying it across session tabs", async ({ page }) => {
  test.setTimeout(90_000)
  const events: EventPayload[] = []
  const todos: Record<string, typeof activeTodos> = { [sourceID]: [], [otherID]: [] }
  // Seeded busy, which is what the duplicate `sessionStatus` key below used to
  // say before it was silently discarded. The mock reads this map through a
  // getter, so the test can still mutate it later.
  const sessionStatus: Record<string, { type: "busy" | "idle" }> = { [sourceID]: { type: "busy" } }

  await mockOpenCodeServer(page, {
    directory,
    project: {
      id: projectID,
      worktree: directory,
      vcs: "git",
      name: "todo-dock-navigation",
      time: { created: 1700000000000, updated: 1700000000000 },
      sandboxes: [],
    },
    provider: {
      all: [
        {
          id: "opencode",
          name: "OpenCode",
          models: {
            "claude-opus-4-6": {
              id: "claude-opus-4-6",
              name: "Claude Opus 4.6",
              limit: { context: 200_000 },
            },
          },
        },
      ],
      connected: ["opencode"],
      default: { providerID: "opencode", modelID: "claude-opus-4-6" },
    },
    sessions: [session(sourceID, sourceTitle, 1700000000000), session(otherID, otherTitle, 1700000001000)],
    pageMessages: () => ({ items: [] }),
    events: () => events.splice(0, 1),
    eventRetry: 16,
    sessionStatus: () => sessionStatus,
    todos: (sessionID) => todos[sessionID] ?? [],
  })
  await configurePage(page)

  await page.goto(sessionHref(sourceID))
  await expectSessionTitle(page, sourceTitle)
  const dock = page.locator('[data-component="session-todo-dock"]')
  await expect(dock).toHaveCount(0)

  // Already busy in the seeded map above; the event is what tells the page.
  events.push(statusEvent(sourceID, "busy"))
  await expect(page.getByRole("button", { name: "Stop" })).toBeVisible()

  await page.waitForTimeout(700)
  const opening = sampleDock(page, "opaque")
  todos[sourceID] = activeTodos
  events.push(todoEvent(sourceID, activeTodos))
  await expect(dock).toBeVisible()
  await expect(dock.locator('[data-state="in_progress"]')).toHaveCount(1)
  expectFadedIn(await opening, "opening")

  await switchSession(page, otherID, otherTitle)
  await expect(dock).toHaveCount(0)

  // 700ms was not reliably long enough to contain the dock reappearing after a
  // tab switch: measured a run where this window captured ZERO present samples
  // and failed on `openSamples.length > 0`. Only present samples are read, so a
  // longer window cannot weaken the assertion - it just stops the window
  // closing before the thing it is meant to observe.
  const returningOpen = sampleDock(page, "appears")
  await switchSession(page, sourceID, sourceTitle)
  const openSamples = (await returningOpen).filter((sample) => sample.present)
  expect(openSamples.length).toBeGreaterThan(0)
  expect(openSamples[0]!.opacity).toBeGreaterThan(0.98)
  expect(openSamples[0]!.height).toBeGreaterThan(70)
  await expect(dock.locator('[data-state="in_progress"]')).toHaveCount(1)

  const completedTodos = activeTodos.map((todo) => ({ ...todo, status: "completed" }))
  const closing = sampleDock(page, "absent")
  todos[sourceID] = completedTodos
  events.push(todoEvent(sourceID, completedTodos))
  await expect(dock).toHaveCount(0)
  expectFaded(await closing, "closing")
  todos[sourceID] = []
  events.push(todoEvent(sourceID, []))

  await switchSession(page, otherID, otherTitle)
  const returningEmpty = sampleDock(page, "appears", DOCK_ABSENCE_WINDOW_MS)
  await switchSession(page, sourceID, sourceTitle)
  await expect(dock).toHaveCount(0)
  expect((await returningEmpty).every((sample) => !sample.present)).toBe(true)
})

type DockSample = { present: boolean; height: number; opacity: number }

/**
 * Assert the dock's opacity RAMPED rather than appearing fully formed.
 *
 * The previous form required a sample to land inside `opacity > 0.05 && < 0.95`
 * - a race between this sampler and a spring animation, with no bound on
 *   either. Measured on this branch: the old assertion failed 1 run in 5
 *   locally, at exactly this line, and it is the failure CI hit three attempts
 *   in a row.
 *
 * The intent is preserved, because the regression this spec guards is "it
 * animates on open and does NOT replay on return". What is dropped is the
 * requirement to observe a specific instant. Only samples where the dock
 * exists are considered: the sampler reports opacity 0 for an absent dock, and
 * counting those was what forced the arbitrary 0.05 floor in the first place.
 *
 * Deliberately no "reached full opacity" assertion. A first attempt required
 * max > 0.98 and it FAILED, measured at 0.878927: the spring has not settled
 * when the sampling window closes, so that is not observable here and adding
 * it made the spec fail more often, not less. Direction is what is checkable -
 * it started faint and rose.
 */
function expectFadedIn(samples: DockSample[], label: string) {
  const present = samples.filter((sample) => sample.present)
  expect(present.length, `${label}: dock never appeared`).toBeGreaterThan(0)
  const opacities = present.map((sample) => sample.opacity)
  expect(Math.min(...opacities), `${label}: dock appeared already opaque, so nothing faded in`).toBeLessThan(0.95)
  expect(opacities.at(-1)!, `${label}: dock opacity did not rise`).toBeGreaterThan(opacities[0]!)
}

/** As above, for a dock that fades out: it must not vanish in one frame. */
function expectFaded(samples: DockSample[], label: string) {
  const present = samples.filter((sample) => sample.present)
  expect(present.length, `${label}: dock was never present`).toBeGreaterThan(0)
  const opacities = present.map((sample) => sample.opacity)
  expect(Math.min(...opacities), `${label}: dock disappeared without fading`).toBeLessThan(0.95)
}

function session(id: string, title: string, created: number) {
  return {
    id,
    slug: id,
    projectID,
    directory,
    title,
    version: "dev",
    time: { created, updated: created },
  }
}

function statusEvent(sessionID: string, type: "busy" | "idle"): EventPayload {
  return {
    directory,
    payload: { type: "session.status", properties: { sessionID, status: { type } } },
  }
}

function todoEvent(sessionID: string, next: typeof activeTodos): EventPayload {
  return {
    directory,
    payload: { type: "todo.updated", properties: { sessionID, todos: next } },
  }
}

async function configurePage(page: Page) {
  const server = `http://${process.env.PLAYWRIGHT_SERVER_HOST ?? "127.0.0.1"}:${process.env.PLAYWRIGHT_SERVER_PORT ?? "4096"}`
  await page.addInitScript(
    ({ directory, dirBase64, server, sessionIDs }) => {
      localStorage.setItem("settings.v3", JSON.stringify({ general: { newLayoutDesigns: true } }))
      localStorage.setItem(
        "opencode.global.dat:server",
        JSON.stringify({
          projects: { local: [{ worktree: directory, expanded: true }] },
          lastProject: { local: directory },
        }),
      )
      localStorage.setItem(
        "opencode.window.browser.dat:tabs",
        JSON.stringify(sessionIDs.map((sessionId) => ({ type: "session", server, dirBase64, sessionId }))),
      )
    },
    { directory, dirBase64: base64Encode(directory), server, sessionIDs: [sourceID, otherID] },
  )
}

function sessionHref(sessionID: string) {
  const server = `http://${process.env.PLAYWRIGHT_SERVER_HOST ?? "127.0.0.1"}:${process.env.PLAYWRIGHT_SERVER_PORT ?? "4096"}`
  return `/server/${base64Encode(server)}/session/${sessionID}`
}

async function switchSession(page: Page, sessionID: string, title: string) {
  const href = sessionHref(sessionID)
  const tab = page.locator(`[data-slot="titlebar-tabs"] a[href="${href}"]`).first()
  await expect(tab).toBeVisible()
  await tab.click()
  await expectSessionTitle(page, title)
}

function sampleDock(page: Page, until: "appears" | "opaque" | "absent", budgetMs = DOCK_SAMPLE_BUDGET_MS) {
  return page.evaluate(
    async ({ until, budgetMs }) => {
      const samples: DockSample[] = []
      const start = performance.now()
      let satisfied = false
      let everAppeared = false

      const read = () => {
        const dock = document.querySelector<HTMLElement>('[data-component="session-todo-dock"]')
        const clip = dock?.parentElement?.parentElement
        const labelEl = dock?.querySelector<HTMLElement>('[data-action="session-todo-toggle"] span[aria-label]')
        return {
          present: !!dock,
          height: clip?.getBoundingClientRect().height ?? 0,
          opacity: labelEl ? Number.parseFloat(getComputedStyle(labelEl).opacity) : 0,
        }
      }

      // Sample until the thing being waited for has actually happened, with
      // time only as a backstop. A fixed window raced SSE delivery and the
      // spring: measured windows that closed before the dock appeared
      // ("dock never appeared") and before the fade began ("disappeared
      // without fading"), both of which failed on the absence of data rather
      // than on the dock's behaviour.
      //
      // Sampling continues for a few frames PAST the condition so the caller
      // sees the settled state as well as the transition into it.
      let after = 0
      while (performance.now() - start < budgetMs) {
        const sample = read()
        samples.push(sample)
        everAppeared ||= sample.present
        if (!satisfied) {
          if (until === "appears") satisfied = sample.present
          else if (until === "opaque") satisfied = sample.present && sample.opacity > 0.9
          else satisfied = !sample.present && everAppeared
        }
        if (satisfied && ++after > 3) break
        await new Promise(requestAnimationFrame)
      }

      return samples
    },
    { until, budgetMs },
  )
}
