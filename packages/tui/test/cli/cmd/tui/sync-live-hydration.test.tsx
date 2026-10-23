/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import type { GlobalEvent } from "@opencode-ai/sdk/v2"
import { tmpdir } from "../../../fixture/fixture"
import { createTuiResolvedConfig } from "../../../fixture/tui-runtime"
import { json, mount, wait } from "./sync-fixture"

const sessionID = "ses_hydration_race"
const messageID = "msg_hydration_race"
const partID = "prt_hydration_race"
const session = {
  id: sessionID,
  title: "race",
  time: { created: 0, updated: 0 },
  version: "1.15.13",
  directory: "/tmp/opencode/packages/opencode",
}
const assistant = {
  id: messageID,
  sessionID,
  role: "assistant" as const,
  agent: "build",
  modelID: "model",
  providerID: "test",
  mode: "build",
  parentID: "msg_user",
  path: { cwd: session.directory, root: session.directory },
  cost: 0,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  time: { created: 1, completed: 2 },
}

function global(payload: GlobalEvent["payload"]): GlobalEvent {
  return { directory: "/tmp/other", project: "proj_test", payload }
}

test("live messages use creation time with an ID tie-break", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")
  const { app, emit, sync } = await mount(undefined, tmp.path)
  const messages = [
    { ...assistant, id: "msg_a", time: { created: 30, completed: 31 } },
    { ...assistant, id: "msg_z", time: { created: 10, completed: 11 } },
    { ...assistant, id: "msg_m", time: { created: 20, completed: 21 } },
    { ...assistant, id: "msg_b", time: { created: 20, completed: 21 } },
  ]

  try {
    for (const info of messages) {
      emit(global({ id: `evt_${info.id}`, type: "message.updated", properties: { sessionID, info } }))
    }
    await wait(() => sync.data.message[sessionID]?.length === messages.length)

    expect(sync.data.message[sessionID].map((message) => message.id)).toEqual(["msg_z", "msg_b", "msg_m", "msg_a"])
  } finally {
    app.renderer.destroy()
  }
})

test("stale session hydration does not overwrite live message parts", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")

  let resolveMessages!: (response: Response) => void
  const messages = new Promise<Response>((resolve) => {
    resolveMessages = resolve
  })
  let requested = false
  const { app, emit, sync } = await mount((url) => {
    if (url.pathname === `/session/${sessionID}`) return json(session)
    if (url.pathname === `/session/${sessionID}/message`) {
      requested = true
      return messages
    }
    if (url.pathname === `/session/${sessionID}/todo` || url.pathname === `/session/${sessionID}/diff`) return json([])
    return undefined
  }, tmp.path)

  try {
    const hydrate = sync.session.sync(sessionID)
    await wait(() => requested)
    emit(global({ id: "evt_message", type: "message.updated", properties: { sessionID, info: assistant } }))
    emit(
      global({
        id: "evt_part",
        type: "message.part.updated",
        properties: {
          sessionID,
          time: 2,
          part: { id: partID, sessionID, messageID, type: "text", text: "visible live content" },
        },
      }),
    )
    await wait(() => sync.data.part[messageID]?.[0]?.type === "text")

    resolveMessages(
      json([
        {
          info: assistant,
          parts: [{ id: partID, sessionID, messageID, type: "text", text: "" }],
        },
      ]),
    )
    await hydrate

    expect(sync.data.part[messageID][0]).toMatchObject({ text: "visible live content" })
  } finally {
    app.renderer.destroy()
  }
})

test("orphan live deltas do not suppress hydrated parts", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")

  let resolveMessages!: (response: Response) => void
  const messages = new Promise<Response>((resolve) => {
    resolveMessages = resolve
  })
  let requested = false
  const { app, emit, sync } = await mount((url) => {
    if (url.pathname === `/session/${sessionID}`) return json(session)
    if (url.pathname === `/session/${sessionID}/message`) {
      requested = true
      return messages
    }
    if (url.pathname === `/session/${sessionID}/todo` || url.pathname === `/session/${sessionID}/diff`) return json([])
    return undefined
  }, tmp.path)

  try {
    const hydrate = sync.session.sync(sessionID)
    await wait(() => requested)
    emit(
      global({
        id: "evt_delta",
        type: "message.part.delta",
        properties: { sessionID, messageID, partID, field: "text", delta: "ignored until part exists" },
      }),
    )
    resolveMessages(
      json([{ info: assistant, parts: [{ id: partID, sessionID, messageID, type: "text", text: "hydrated" }] }]),
    )
    await hydrate

    expect(sync.data.part[messageID][0]).toMatchObject({ text: "hydrated" })
  } finally {
    app.renderer.destroy()
  }
})

test("hydration does not clear text streamed before it starts", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")

  let resolveMessages!: (response: Response) => void
  const messages = new Promise<Response>((resolve) => {
    resolveMessages = resolve
  })
  let requested = false
  const { app, emit, sync } = await mount((url) => {
    if (url.pathname === `/session/${sessionID}`) return json(session)
    if (url.pathname === `/session/${sessionID}/message`) {
      requested = true
      return messages
    }
    if (url.pathname === `/session/${sessionID}/todo` || url.pathname === `/session/${sessionID}/diff`) return json([])
    return undefined
  }, tmp.path)

  try {
    emit(global({ id: "evt_message", type: "message.updated", properties: { sessionID, info: assistant } }))
    emit(
      global({
        id: "evt_part",
        type: "message.part.updated",
        properties: {
          sessionID,
          time: 1,
          part: { id: partID, sessionID, messageID, type: "text", text: "" },
        },
      }),
    )
    emit(
      global({
        id: "evt_delta",
        type: "message.part.delta",
        properties: { sessionID, messageID, partID, field: "text", delta: "visible streamed content" },
      }),
    )
    await wait(() => sync.data.part[messageID]?.[0]?.type === "text" && sync.data.part[messageID][0].text !== "")
    const hydrate = sync.session.sync(sessionID)
    await wait(() => requested)
    resolveMessages(json([{ info: assistant, parts: [{ id: partID, sessionID, messageID, type: "text", text: "" }] }]))
    await hydrate

    expect(sync.data.part[messageID][0]).toMatchObject({ text: "visible streamed content" })
  } finally {
    app.renderer.destroy()
  }
})

test("live messages merged during hydration retain the 100 message window", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")

  let resolveMessages!: (response: Response) => void
  const messages = new Promise<Response>((resolve) => {
    resolveMessages = resolve
  })
  let requested = false
  const { app, emit, sync } = await mount((url) => {
    if (url.pathname === `/session/${sessionID}`) return json(session)
    if (url.pathname === `/session/${sessionID}/message`) {
      requested = true
      return messages
    }
    if (url.pathname === `/session/${sessionID}/todo` || url.pathname === `/session/${sessionID}/diff`) return json([])
    return undefined
  }, tmp.path)

  try {
    const hydrate = sync.session.sync(sessionID)
    await wait(() => requested)
    const live = { ...assistant, id: "msg_z_live" }
    emit(global({ id: "evt_live", type: "message.updated", properties: { sessionID, info: live } }))
    await wait(() => sync.data.message[sessionID]?.some((message) => message.id === live.id) ?? false)
    resolveMessages(
      json(
        Array.from({ length: 100 }, (_, index) => {
          const id = `msg_${String(index).padStart(3, "0")}`
          return {
            info: { ...assistant, id },
            parts: [{ id: `prt_${id}`, sessionID, messageID: id, type: "text", text: id }],
          }
        }),
      ),
    )
    await hydrate

    expect(sync.data.message[sessionID]).toHaveLength(100)
    expect(sync.data.message[sessionID].at(-1)?.id).toBe(live.id)
    expect(sync.data.message[sessionID].some((message) => message.id === "msg_000")).toBe(false)
    expect(sync.data.part.msg_000).toBeUndefined()
  } finally {
    app.renderer.destroy()
  }
})

test("configured message window reconciles live updates during hydration", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")

  const messageWindow = 135
  let resolveMessages!: (response: Response) => void
  const messages = new Promise<Response>((resolve) => {
    resolveMessages = resolve
  })
  let requested = false
  let requestedLimit = 0
  const { app, emit, sync } = await mount(
    (url) => {
      if (url.pathname === `/session/${sessionID}`) return json(session)
      if (url.pathname === `/session/${sessionID}/message`) {
        requested = true
        requestedLimit = Number(url.searchParams.get("limit"))
        return messages
      }
      if (url.pathname === `/session/${sessionID}/todo` || url.pathname === `/session/${sessionID}/diff`) return json([])
      return undefined
    },
    tmp.path,
    true,
    undefined,
    createTuiResolvedConfig({ message_window: messageWindow }),
  )

  try {
    const hydrate = sync.session.sync(sessionID)
    await wait(() => requested)
    const live = {
      ...assistant,
      id: "msg_z_live",
    }
    emit(global({ id: "evt_live", type: "message.updated", properties: { sessionID, info: live } }))
    emit(
      global({
        id: "evt_live_part",
        type: "message.part.updated",
        properties: {
          sessionID,
          time: 2,
          part: { id: "prt_msg_z_live", sessionID, messageID: live.id, type: "text", text: "live part" },
        },
      }),
    )
    await wait(() => sync.data.part[live.id]?.[0]?.type === "text")

    const history = Array.from({ length: messageWindow }, (_, index) => {
      const id = `msg_${String(index).padStart(3, "0")}`
      return {
        info: { ...assistant, id },
        parts: [{ id: `prt_${id}`, sessionID, messageID: id, type: "text", text: id }],
      }
    })
    resolveMessages(json(history.slice(-requestedLimit)))
    await hydrate

    expect(requestedLimit).toBe(messageWindow)
    expect(sync.data.message[sessionID].map((message) => message.id)).toEqual([
      ...history.slice(1).map((message) => message.info.id),
      live.id,
    ])
    expect(sync.data.part[history[0]!.info.id]).toBeUndefined()
    expect(sync.data.part[history[1]!.info.id]?.[0]).toMatchObject({ text: history[1]!.info.id })
    expect(sync.data.part[live.id]?.[0]).toMatchObject({ text: "live part" })

    const newest = {
      ...assistant,
      id: "msg_zz_live",
    }
    emit(global({ id: "evt_newest", type: "message.updated", properties: { sessionID, info: newest } }))
    emit(
      global({
        id: "evt_newest_part",
        type: "message.part.updated",
        properties: {
          sessionID,
          time: 3,
          part: { id: "prt_msg_zz_live", sessionID, messageID: newest.id, type: "text", text: "newest part" },
        },
      }),
    )
    await wait(() => sync.data.part[newest.id]?.[0]?.type === "text")

    expect(sync.data.message[sessionID]).toHaveLength(messageWindow)
    expect(sync.data.message[sessionID].map((message) => message.id)).toEqual([
      ...history.slice(2).map((message) => message.info.id),
      live.id,
      newest.id,
    ])
    expect(sync.data.part[history[1]!.info.id]).toBeUndefined()
    expect(sync.data.part[newest.id]?.[0]).toMatchObject({ text: "newest part" })
  } finally {
    app.renderer.destroy()
  }
})

test("live growth keeps messages beyond the old cap inside a larger configured window", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")

  const messageWindow = 130
  const initial = Array.from({ length: 90 }, (_, index) => {
    const id = `msg_${String(index).padStart(3, "0")}`
    return {
      info: { ...assistant, id },
      parts: [{ id: `prt_${id}`, sessionID, messageID: id, type: "text", text: id }],
    }
  })
  const live = Array.from({ length: 20 }, (_, index) => {
    const id = `msg_${String(initial.length + index).padStart(3, "0")}`
    return {
      info: { ...assistant, id },
      part: { id: `prt_${id}`, sessionID, messageID: id, type: "text" as const, text: id },
    }
  })
  let requestedLimit = 0
  const { app, emit, sync } = await mount(
    (url) => {
      if (url.pathname === `/session/${sessionID}`) return json(session)
      if (url.pathname === `/session/${sessionID}/message`) {
        requestedLimit = Number(url.searchParams.get("limit"))
        return json(initial.slice(-requestedLimit))
      }
      if (url.pathname === `/session/${sessionID}/todo` || url.pathname === `/session/${sessionID}/diff`) return json([])
      return undefined
    },
    tmp.path,
    true,
    undefined,
    createTuiResolvedConfig({ message_window: messageWindow }),
  )

  try {
    await sync.session.sync(sessionID)
    expect(requestedLimit).toBe(messageWindow)
    expect(sync.data.message[sessionID]).toHaveLength(initial.length)

    live.forEach((message) => {
      emit(
        global({
          id: `evt_${message.info.id}`,
          type: "message.updated",
          properties: { sessionID, info: message.info },
        }),
      )
      emit(
        global({
          id: `evt_${message.info.id}_part`,
          type: "message.part.updated",
          properties: { sessionID, time: 2, part: message.part },
        }),
      )
    })
    const oldestPastDefault = initial[9]!
    const newest = live.at(-1)!
    await wait(
      () =>
        sync.data.message[sessionID]?.length === initial.length + live.length &&
        sync.data.part[newest.info.id]?.[0]?.type === "text",
    )

    expect(sync.data.message[sessionID].map((message) => message.id)).toEqual([
      ...initial.map((message) => message.info.id),
      ...live.map((message) => message.info.id),
    ])
    expect(sync.data.message[sessionID].some((message) => message.id === oldestPastDefault.info.id)).toBe(true)
    expect(sync.data.part[oldestPastDefault.info.id]?.[0]).toMatchObject({ text: oldestPastDefault.info.id })
    expect(sync.data.part[newest.info.id]?.[0]).toMatchObject({ text: newest.info.id })
  } finally {
    app.renderer.destroy()
  }
})

test("a message removed during hydration does not regain stale parts", async () => {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")

  let resolveMessages!: (response: Response) => void
  const messages = new Promise<Response>((resolve) => {
    resolveMessages = resolve
  })
  let requested = false
  const { app, emit, sync } = await mount((url) => {
    if (url.pathname === `/session/${sessionID}`) return json(session)
    if (url.pathname === `/session/${sessionID}/message`) {
      requested = true
      return messages
    }
    if (url.pathname === `/session/${sessionID}/todo` || url.pathname === `/session/${sessionID}/diff`) return json([])
    return undefined
  }, tmp.path)

  try {
    emit(global({ id: "evt_message", type: "message.updated", properties: { sessionID, info: assistant } }))
    await wait(() => sync.data.message[sessionID]?.length === 1)
    const hydrate = sync.session.sync(sessionID)
    await wait(() => requested)
    emit(global({ id: "evt_removed", type: "message.removed", properties: { sessionID, messageID } }))
    await wait(() => sync.data.message[sessionID]?.length === 0)
    resolveMessages(
      json([{ info: assistant, parts: [{ id: partID, sessionID, messageID, type: "text", text: "stale" }] }]),
    )
    await hydrate

    expect(sync.data.message[sessionID]).toEqual([])
    expect(sync.data.part[messageID]).toBeUndefined()
  } finally {
    app.renderer.destroy()
  }
})
