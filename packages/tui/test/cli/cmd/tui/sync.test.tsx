/** @jsxImportSource @opentui/solid */
import { describe, expect, test } from "bun:test"
import { tmpdir } from "../../../fixture/fixture"
import { json, mount, wait } from "./sync-fixture"
import type { GlobalEvent, Message, Session, SessionStatus } from "@opencode-ai/sdk/v2"

function branchEvent(branch: string, workspace?: string): GlobalEvent {
  return {
    directory: "/tmp/other",
    project: "proj_test",
    workspace,
    payload: {
      id: `evt_vcs_${branch}`,
      type: "vcs.branch.updated",
      properties: { branch },
    },
  }
}

describe("tui sync", () => {
  test("refresh scopes sessions by default and lists project sessions when disabled", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const { app, kv, sync, session } = await mount(undefined, tmp.path)

    try {
      expect(kv.get("session_directory_filter_enabled", true)).toBe(true)
      expect(session.at(-1)?.searchParams.get("roots")).toBeNull()
      expect(session.at(-1)?.searchParams.get("scope")).toBeNull()
      expect(session.at(-1)?.searchParams.get("path")).toBe("packages/tui")

      kv.set("session_directory_filter_enabled", false)
      await sync.session.refresh()

      expect(session.at(-1)?.searchParams.get("scope")).toBe("project")
      expect(session.at(-1)?.searchParams.get("path")).toBeNull()
      expect(session.at(-1)?.searchParams.get("roots")).toBeNull()
    } finally {
      app.renderer.destroy()
    }
  })

  test("vcs branch updates only apply for the active workspace", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const { app, emit, project, sync } = await mount(undefined, tmp.path)

    try {
      expect(sync.data.vcs?.branch).toBe("main")

      project.workspace.set("ws_a")
      emit(branchEvent("other", "ws_b"))
      await Bun.sleep(30)

      expect(sync.data.vcs?.branch).toBe("main")

      emit(branchEvent("feature", "ws_a"))
      await wait(() => sync.data.vcs?.branch === "feature")

      expect(sync.data.vcs?.branch).toBe("feature")
    } finally {
      app.renderer.destroy()
    }
  })

  test("restarted sessions distinguish unknown status from failed and active work", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    let resolveStatus!: (response: Response) => void
    let statusRequested!: () => void
    const statusResponse = new Promise<Response>((resolve) => {
      resolveStatus = resolve
    })
    const requested = new Promise<void>((resolve) => {
      statusRequested = resolve
    })
    const { app, sync } = await mount(
      (url) => {
        if (url.pathname !== "/session/status") return
        statusRequested()
        return statusResponse
      },
      tmp.path,
      false,
    )

    try {
      const sessionID = "ses_incomplete"
      const session = { id: sessionID, time: { created: 1, updated: 1 } } as unknown as Session
      const assistant = {
        id: "msg_incomplete",
        sessionID,
        role: "assistant",
        time: { created: 2 },
      } as unknown as Message
      sync.set("session", [session])
      sync.set("message", sessionID, [assistant])

      expect(sync.data.session_status).toEqual({})
      await requested
      expect(sync.status).not.toBe("complete")
      expect(sync.session.status(sessionID)).toBe("unknown")

      resolveStatus(json({}))
      await wait(() => sync.status === "complete")
      expect(sync.data.session_status).toEqual({})
      expect(sync.session.status(sessionID)).toBe("unknown")

      sync.set("session_status", sessionID, { type: "idle" } as SessionStatus)
      expect(sync.session.status(sessionID)).toBe("failed")

      sync.set("session_status", sessionID, { type: "busy" } as SessionStatus)
      expect(sync.session.status(sessionID)).toBe("working")

      sync.set("session_status", sessionID, {
        type: "retry",
        attempt: 1,
        message: "retrying",
        next: Date.now(),
      } as SessionStatus)
      expect(sync.session.status(sessionID)).toBe("working")

      sync.set("message", sessionID, [{ ...assistant, time: { created: 2, completed: 3 } } as Message])
      sync.set("session_status", sessionID, { type: "idle" } as SessionStatus)
      expect(sync.session.status(sessionID)).toBe("idle")
    } finally {
      resolveStatus(json({}))
      app.renderer.destroy()
    }
  })
})
