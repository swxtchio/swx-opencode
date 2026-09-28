import { describe, expect, test } from "bun:test"
import { ProviderError } from "@/provider/error"

describe("provider stream errors", () => {
  test("classifies a top-level context overflow code", () => {
    const input = {
      message: "Request failed",
      type: "invalid_request_error",
      code: "context_length_exceeded",
    }

    expect(ProviderError.parseStreamError(input)).toEqual({
      type: "context_overflow",
      message: "Input exceeds context window of this model",
      responseBody: JSON.stringify(input),
    })
  })

  test("classifies a top-level context overflow message", () => {
    const input = {
      message: "Your input exceeds the context window of this model",
      type: "invalid_request_error",
      code: "invalid_request_error",
    }

    expect(ProviderError.parseStreamError(input)).toEqual({
      type: "context_overflow",
      message: input.message,
      responseBody: JSON.stringify(input),
    })
  })

  test("leaves top-level stream errors without an overflow signal unclassified", () => {
    expect(
      ProviderError.parseStreamError({
        message: "The provider request failed",
        type: "invalid_request_error",
        code: "provider_error",
      }),
    ).toBeUndefined()
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
