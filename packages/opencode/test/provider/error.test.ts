import { describe, expect, test } from "bun:test"
import { ProviderError } from "@/provider/error"
import { openAICompatibleStreamError } from "../lib/openai-compatible-stream-error"

describe("provider stream errors", () => {
  test("classifies a top-level context overflow code after SDK stream parsing", async () => {
    const input = {
      message: "Request failed",
      type: "invalid_request_error",
      code: "context_length_exceeded",
    }
    const error = await openAICompatibleStreamError(input)

    expect(error).toEqual(input)
    expect(ProviderError.parseStreamError(error)).toEqual({
      type: "context_overflow",
      message: "Input exceeds context window of this model",
      responseBody: JSON.stringify(input),
    })
  })

  test("classifies a top-level context overflow message after SDK stream parsing", async () => {
    const input = {
      message: "Your input exceeds the context window of this model",
      type: "invalid_request_error",
      code: "invalid_request_error",
    }
    const error = await openAICompatibleStreamError(input)

    expect(error).toEqual(input)
    expect(ProviderError.parseStreamError(error)).toEqual({
      type: "context_overflow",
      message: input.message,
      responseBody: JSON.stringify(input),
    })
  })

  test("leaves top-level stream errors without an overflow signal unclassified", async () => {
    const input = {
      message: "The provider request failed",
      type: "invalid_request_error",
      code: "provider_error",
    }
    const error = await openAICompatibleStreamError(input)

    expect(error).toEqual(input)
    expect(ProviderError.parseStreamError(error)).toBeUndefined()
  })

  test("retries provider stream errors without a code", () => {
    const messages = [
      "The model is currently at capacity due to high demand. Please try again in a few minutes, or use a higher service tier for priority processing: https://docs.x.ai/developers/advanced-api-usage/priority-processing",
      "The model is temporarily unavailable.",
    ]

    for (const message of messages)
      expect(
        ProviderError.parseStreamError({
          type: "error",
          error: { message },
        }),
      ).toEqual({
        type: "api_error",
        message,
        isRetryable: true,
        responseBody: JSON.stringify({ type: "error", error: { message } }),
      })
  })
})
