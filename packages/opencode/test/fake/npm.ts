import { Npm } from "@opencode-ai/core/npm"
import { Effect, Layer } from "effect"

export const noop = Layer.mock(Npm.Service)({
  install: () => Effect.void,
})

// Records every install request and can fail the ones carrying a pinned
// version, which is how an unpublished build behaves against npm: the exact
// version 404s while an unpinned request resolves. Used to pin the fallback in
// config.ts - see swxtchio/swx-opencode#16.
export function recording(input?: { failPinned?: boolean }) {
  const calls: (string | undefined)[] = []
  const layer = Layer.mock(Npm.Service)({
    install: (_dir: string, add?: { add: { name: string; version?: string }[] }) => {
      const version = add?.add?.[0]?.version
      calls.push(version)
      if (input?.failPinned && version !== undefined) {
        return Effect.fail(
          new Npm.InstallFailedError({
            dir: _dir,
            add: ["@opencode-ai/plugin"],
            cause: `No matching version found for @opencode-ai/plugin@${version}.`,
          }),
        )
      }
      return Effect.void
    },
  })
  return { layer, calls }
}

export * as NpmTest from "./npm"
