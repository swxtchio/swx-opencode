import { createOpenAICompatible } from "@ai-sdk/openai-compatible"

export async function openAICompatibleStreamError(error: string | { message: string; type: string; code: string }) {
  const chunk = {
    id: "chatcmpl-test",
    object: "chat.completion.chunk",
    created: 1790550000,
    model: "test-model",
    choices: [{ index: 0, delta: { role: "assistant", content: "partial response" }, finish_reason: null }],
  }
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: () =>
      new Response(`data: ${JSON.stringify(chunk)}\n\ndata: ${JSON.stringify({ error })}\n\n`, {
        headers: { "content-type": "text/event-stream" },
      }),
  })
  try {
    const model = createOpenAICompatible({
      name: "test",
      baseURL: `http://127.0.0.1:${server.port}/v1`,
      apiKey: "test",
    })("test-model")
    const response = await model.doStream({
      prompt: [{ role: "user", content: [{ type: "text", text: "trigger" }] }],
    })
    const reader = response.stream.getReader()
    while (true) {
      const item = await reader.read()
      if (item.done) return undefined
      if (item.value.type === "error") return item.value.error
    }
  } finally {
    server.stop(true)
  }
}
