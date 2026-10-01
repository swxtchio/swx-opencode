export * as ParcelBinding from "./parcel-binding"

// @ts-ignore
import { createWrapper } from "@parcel/watcher/wrapper"
import { lazy } from "../util/lazy"

declare const OPENCODE_LIBC: string | undefined

export const load = lazy((): typeof import("@parcel/watcher") | undefined => {
  try {
    const libc = typeof OPENCODE_LIBC === "undefined" ? undefined : OPENCODE_LIBC
    const binding = require(
      `@parcel/watcher-${process.platform}-${process.arch}${process.platform === "linux" ? `-${libc || "glibc"}` : ""}`,
    )
    return createWrapper(binding) as typeof import("@parcel/watcher")
  } catch {
    return
  }
})
